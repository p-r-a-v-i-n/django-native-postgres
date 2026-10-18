use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::{FutureExt, TryStreamExt};
use pyo3::prelude::*;
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};

use crate::cancellation::CancellationGuard;
use crate::error::NativeError;
use crate::parameter::QueryParameters;
use crate::postgres::{self, PostgresPool, QueryResult};
use crate::query_stream::{
    self, QueryStreamCommand, QueryStreamFetchResult, QueryStreamHandle, QueryStreamOpenResult,
    StartedQueryStream,
};
use crate::tls::TlsConnector;
use crate::transaction_options::TransactionOptions;
use deadpool_postgres::Object;
use std::process;
use tokio_postgres::{Row, RowStream};

enum TransactionStreamExit {
    Finished,
    Queued(TransactionCommand),
    Stop,
}

enum StreamNextRow {
    Row(Result<Option<Row>, tokio_postgres::Error>),
    Cancelled,
}

pub(crate) enum TransactionCommand {
    Execute {
        sql: String,
        params: QueryParameters,
        cancellation: oneshot::Receiver<()>,
        response: oneshot::Sender<Result<QueryResult, NativeError>>,
    },
    Commit {
        response: oneshot::Sender<Result<(), NativeError>>,
    },
    Rollback {
        response: oneshot::Sender<Result<(), NativeError>>,
    },
    OpenQueryStream {
        sql: String,
        params: QueryParameters,
        cancellation: oneshot::Receiver<()>,
        response: oneshot::Sender<Result<QueryStreamOpenResult, NativeError>>,
    },
}

#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct TransactionHandle {
    process_id: u32,
    sender: mpsc::Sender<TransactionCommand>,
    closed: Arc<AtomicBool>,
}

impl TransactionHandle {
    pub(crate) fn new(sender: mpsc::Sender<TransactionCommand>) -> Self {
        Self {
            process_id: process::id(),
            sender,
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn try_mark_closed(&self) -> bool {
        self.closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn validate_process(&self) -> Result<(), NativeError> {
        if self.process_id != process::id() {
            return Err(NativeError::TransactionHandleProcessMismatch);
        }
        Ok(())
    }
}

pub(crate) async fn begin(
    pool: &PostgresPool,
    options: TransactionOptions,
    mut cancellation: oneshot::Receiver<()>,
) -> Result<TransactionHandle, NativeError> {
    let client = tokio::select! {
        biased;
        _ = &mut cancellation => {
            return Err(NativeError::TransactionStartCancelled);
        }
        result = pool.pool().get() => {
            result.map_err(|error| NativeError::PostgresPoolAcquire(error.to_string()))?
        }
    };

    let (sender, receiver) = mpsc::channel(1);
    let (ready_sender, ready_receiver) = oneshot::channel();

    tokio::spawn(run_transaction(
        client,
        pool.tls().clone(),
        options,
        cancellation,
        receiver,
        ready_sender,
    ));

    ready_receiver
        .await
        .map_err(|_| NativeError::ResponseChannelClosed)??;

    Ok(TransactionHandle::new(sender))
}

pub(crate) async fn execute(
    handle: TransactionHandle,
    sql: String,
    params: QueryParameters,
) -> Result<QueryResult, NativeError> {
    handle.validate_process()?;
    if handle.is_closed() {
        return Err(NativeError::TransactionHandleClosed);
    }

    let (mut cancellation_guard, cancellation) = CancellationGuard::new();
    let (response_tx, response_rx) = oneshot::channel();

    if handle
        .sender
        .send(TransactionCommand::Execute {
            sql,
            params,
            cancellation,
            response: response_tx,
        })
        .await
        .is_err()
    {
        handle.mark_closed();
        return Err(NativeError::TransactionHandleClosed);
    }

    let response = response_rx.await;
    cancellation_guard.disarm();

    match response {
        Ok(result) => result,
        Err(_) => {
            handle.mark_closed();
            Err(NativeError::TransactionHandleClosed)
        }
    }
}

pub(crate) async fn commit(handle: TransactionHandle) -> Result<(), NativeError> {
    handle.validate_process()?;
    if !handle.try_mark_closed() {
        return Err(NativeError::TransactionHandleClosed);
    }

    let (response_tx, response_rx) = oneshot::channel();

    handle
        .sender
        .send(TransactionCommand::Commit {
            response: response_tx,
        })
        .await
        .map_err(|_| NativeError::TransactionHandleClosed)?;

    response_rx
        .await
        .map_err(|_| NativeError::TransactionHandleClosed)?
}

pub(crate) async fn rollback(handle: TransactionHandle) -> Result<(), NativeError> {
    handle.validate_process()?;
    if !handle.try_mark_closed() {
        return Err(NativeError::TransactionHandleClosed);
    }

    let (response_tx, response_rx) = oneshot::channel();

    handle
        .sender
        .send(TransactionCommand::Rollback {
            response: response_tx,
        })
        .await
        .map_err(|_| NativeError::TransactionHandleClosed)?;

    response_rx
        .await
        .map_err(|_| NativeError::TransactionHandleClosed)?
}

async fn run_transaction(
    mut client: Object,
    tls: TlsConnector,
    options: TransactionOptions,
    mut cancellation: oneshot::Receiver<()>,
    mut receiver: mpsc::Receiver<TransactionCommand>,
    ready: oneshot::Sender<Result<(), NativeError>>,
) {
    let cancel_token = client.cancel_token();
    let mut builder = client.build_transaction();

    if let Some(isolation_level) = options.isolation_level() {
        builder = builder.isolation_level(isolation_level);
    }
    if let Some(read_only) = options.read_only() {
        builder = builder.read_only(read_only);
    }
    if let Some(deferrable) = options.deferrable() {
        builder = builder.deferrable(deferrable);
    }

    let mut start = Box::pin(builder.start());

    let transaction = tokio::select! {
        biased;
        result = &mut start => {
            match result {
                Ok(transaction) => transaction,
                Err(source) => {
                    let _ = ready.send(Err(NativeError::PostgresTransaction {
                        operation: "begin",
                        source,
                    }));
                    return;
                }
            }
        }
        _ = &mut cancellation => {
            let cancellation_result = tls.cancel_query(&cancel_token).await;
            let start_result = start.as_mut().await;
            drop(start);

            // A successfully started transaction is rolled back explicitly.
            // On an error, tokio-postgres already queues a rollback before
            // the start future returns.
            clean_up_cancelled_start(start_result).await;

            let error = match cancellation_result {
                Ok(()) => NativeError::TransactionStartCancelled,
                Err(source) => NativeError::PostgresCancel(source),
            };
            let _ = ready.send(Err(error));
            return;
        }
    };

    if ready.send(Ok(())).is_err() {
        return;
    }

    let mut queued_command = None;

    loop {
        let command = match queued_command.take() {
            Some(command) => command,
            None => {
                let Some(command) = receiver.recv().await else {
                    return;
                };
                command
            }
        };

        match command {
            TransactionCommand::Execute {
                sql,
                params,
                mut cancellation,
                response,
            } => {
                let cancel_token = transaction.cancel_token();
                let result = postgres::execute_on_client(
                    &*transaction,
                    cancel_token,
                    &tls,
                    &sql,
                    params,
                    &mut cancellation,
                )
                .await;
                let connection_can_be_reused = !transaction.client().is_closed()
                    && !matches!(
                        &result,
                        Err(NativeError::PostgresCancel(_))
                            | Err(NativeError::QueryCancelled {
                                connection_can_be_reused: false,
                            })
                    );
                let _ = response.send(result);

                if !connection_can_be_reused {
                    return;
                }
            }
            TransactionCommand::OpenQueryStream {
                sql,
                params,
                mut cancellation,
                response,
            } => {
                let cancel_token = transaction.cancel_token();
                let started = query_stream::start_on_client(
                    &*transaction,
                    cancel_token,
                    &tls,
                    &sql,
                    params,
                    &mut cancellation,
                )
                .await;

                let StartedQueryStream { rows, columns } = match started {
                    Ok(started) => started,
                    Err(error) => {
                        let connection_can_be_reused = transaction_error_allows_reuse(&error)
                            && !transaction.client().is_closed();
                        let _ = response.send(Err(error));
                        if !connection_can_be_reused {
                            return;
                        }
                        continue;
                    }
                };

                if columns.is_empty() {
                    let result = finish_transaction_non_row_query(
                        &transaction,
                        rows,
                        &tls,
                        &mut cancellation,
                    )
                    .await;
                    let connection_can_be_reused = result
                        .as_ref()
                        .map_or_else(transaction_error_allows_reuse, |_| true)
                        && !transaction.client().is_closed();
                    let _ = response.send(result);
                    if !connection_can_be_reused {
                        return;
                    }
                    continue;
                }

                let (stream_sender, stream_receiver) = mpsc::channel(1);
                let open_result = QueryStreamOpenResult {
                    handle: QueryStreamHandle::new(stream_sender),
                    rows_affected: None,
                    columns,
                };

                if response.send(Ok(open_result)).is_err() {
                    let mut rows = Box::pin(rows);
                    let _ = drain_rows(rows.as_mut()).await;
                    continue;
                }

                match run_transaction_query_stream(
                    &transaction,
                    rows,
                    &tls,
                    stream_receiver,
                    &mut receiver,
                )
                .await
                {
                    TransactionStreamExit::Finished => {}
                    TransactionStreamExit::Queued(command) => {
                        queued_command = Some(command);
                    }
                    TransactionStreamExit::Stop => return,
                }
            }
            TransactionCommand::Commit { response } => {
                let result =
                    transaction
                        .commit()
                        .await
                        .map_err(|source| NativeError::PostgresTransaction {
                            operation: "commit",
                            source,
                        });
                let _ = response.send(result);
                return;
            }
            TransactionCommand::Rollback { response } => {
                let result = transaction.rollback().await.map_err(|source| {
                    NativeError::PostgresTransaction {
                        operation: "roll back",
                        source,
                    }
                });
                let _ = response.send(result);
                return;
            }
        }
    }
}

async fn finish_transaction_non_row_query(
    transaction: &deadpool_postgres::Transaction<'_>,
    rows: RowStream,
    tls: &TlsConnector,
    cancellation: &mut oneshot::Receiver<()>,
) -> Result<QueryStreamOpenResult, NativeError> {
    let mut rows = Box::pin(rows);

    loop {
        let next_row = {
            let mut pinned_rows = rows.as_mut();
            let next_row = pinned_rows.try_next();
            tokio::pin!(next_row);

            tokio::select! {
                biased;
                _ = &mut *cancellation => StreamNextRow::Cancelled,
                result = &mut next_row => StreamNextRow::Row(result),
            }
        };

        match next_row {
            StreamNextRow::Row(Ok(Some(_))) => {}
            StreamNextRow::Row(Ok(None)) => {
                let rows_affected = rows.as_ref().rows_affected().unwrap_or(0);
                let (sender, receiver) = mpsc::channel(1);
                drop(receiver);
                let handle = QueryStreamHandle::new(sender);
                handle.mark_exhausted();

                return Ok(QueryStreamOpenResult {
                    handle,
                    rows_affected: Some(rows_affected),
                    columns: Vec::new(),
                });
            }
            StreamNextRow::Row(Err(error)) => {
                return Err(NativeError::PostgresQuery(error));
            }
            StreamNextRow::Cancelled => {
                return Err(cancel_transaction_rows(transaction, tls, rows.as_mut()).await);
            }
        }
    }
}

async fn run_transaction_query_stream(
    transaction: &deadpool_postgres::Transaction<'_>,
    rows: RowStream,
    tls: &TlsConnector,
    mut stream_receiver: mpsc::Receiver<QueryStreamCommand>,
    transaction_receiver: &mut mpsc::Receiver<TransactionCommand>,
) -> TransactionStreamExit {
    let mut rows = Box::pin(rows);
    let mut pending_row = None;

    loop {
        let command = tokio::select! {
            biased;
            command = stream_receiver.recv() => {
                let Some(command) = command else {
                    return finish_abandoned_transaction_stream(rows.as_mut()).await;
                };
                command
            }
            command = transaction_receiver.recv() => {
                let Some(command) = command else {
                    return finish_abandoned_transaction_stream(rows.as_mut()).await;
                };

                return match drain_rows(rows.as_mut()).await {
                    Ok(_) => TransactionStreamExit::Queued(command),
                    Err(error) => {
                        let connection_can_be_reused = transaction_error_allows_reuse(&error)
                            && !transaction.client().is_closed();
                        send_transaction_command_error(command, error);
                        if connection_can_be_reused {
                            TransactionStreamExit::Finished
                        } else {
                            TransactionStreamExit::Stop
                        }
                    }
                };
            }
        };

        match command {
            QueryStreamCommand::Fetch {
                size,
                mut cancellation,
                response,
            } => {
                let mut decoded_rows = Vec::with_capacity(size);
                let mut exhausted = false;

                while decoded_rows.len() < size {
                    let next_row = if let Some(row) = pending_row.take() {
                        StreamNextRow::Row(Ok(Some(row)))
                    } else {
                        let mut pinned_rows = rows.as_mut();
                        let next_row = pinned_rows.try_next();
                        tokio::pin!(next_row);

                        tokio::select! {
                            biased;
                            _ = &mut cancellation => StreamNextRow::Cancelled,
                            result = &mut next_row => StreamNextRow::Row(result),
                        }
                    };

                    match next_row {
                        StreamNextRow::Row(Ok(Some(row))) => match postgres::decode_row(row) {
                            Ok(row) => decoded_rows.push(row),
                            Err(error) => {
                                let drain_result = drain_rows(rows.as_mut()).await;
                                let connection_can_be_reused = drain_result
                                    .as_ref()
                                    .map_or_else(transaction_error_allows_reuse, |_| true)
                                    && !transaction.client().is_closed();
                                let _ = response.send(Err(error));
                                return if connection_can_be_reused {
                                    TransactionStreamExit::Finished
                                } else {
                                    TransactionStreamExit::Stop
                                };
                            }
                        },
                        StreamNextRow::Row(Ok(None)) => {
                            exhausted = true;
                            break;
                        }
                        StreamNextRow::Row(Err(error)) => {
                            let connection_can_be_reused =
                                postgres::query_error_allows_connection_reuse(&error)
                                    && !transaction.client().is_closed();
                            let _ = response.send(Err(NativeError::PostgresQuery(error)));
                            return if connection_can_be_reused {
                                TransactionStreamExit::Finished
                            } else {
                                TransactionStreamExit::Stop
                            };
                        }
                        StreamNextRow::Cancelled => {
                            let error =
                                cancel_transaction_rows(transaction, tls, rows.as_mut()).await;
                            let connection_can_be_reused = transaction_error_allows_reuse(&error)
                                && !transaction.client().is_closed();
                            let _ = response.send(Err(error));
                            return if connection_can_be_reused {
                                TransactionStreamExit::Finished
                            } else {
                                TransactionStreamExit::Stop
                            };
                        }
                    }
                }

                if !exhausted && decoded_rows.len() == size {
                    match rows.as_mut().try_next().now_or_never() {
                        Some(Ok(Some(row))) => pending_row = Some(row),
                        Some(Ok(None)) => exhausted = true,
                        Some(Err(error)) => {
                            let connection_can_be_reused =
                                postgres::query_error_allows_connection_reuse(&error)
                                    && !transaction.client().is_closed();
                            let _ = response.send(Err(NativeError::PostgresQuery(error)));
                            return if connection_can_be_reused {
                                TransactionStreamExit::Finished
                            } else {
                                TransactionStreamExit::Stop
                            };
                        }
                        None => {}
                    }
                }

                if response
                    .send(Ok(QueryStreamFetchResult {
                        rows: decoded_rows,
                        exhausted,
                    }))
                    .is_err()
                {
                    return finish_abandoned_transaction_stream(rows.as_mut()).await;
                }

                if exhausted {
                    return TransactionStreamExit::Finished;
                }
            }
            QueryStreamCommand::Close { response } => {
                let result = drain_rows(rows.as_mut()).await.map(|_| ());
                let connection_can_be_reused = result
                    .as_ref()
                    .map_or_else(transaction_error_allows_reuse, |_| true)
                    && !transaction.client().is_closed();
                let _ = response.send(result);
                return if connection_can_be_reused {
                    TransactionStreamExit::Finished
                } else {
                    TransactionStreamExit::Stop
                };
            }
        }
    }
}

async fn finish_abandoned_transaction_stream(rows: Pin<&mut RowStream>) -> TransactionStreamExit {
    match drain_rows(rows).await {
        Ok(_) => TransactionStreamExit::Finished,
        Err(error) if transaction_error_allows_reuse(&error) => TransactionStreamExit::Finished,
        Err(_) => TransactionStreamExit::Stop,
    }
}

async fn drain_rows(mut rows: Pin<&mut RowStream>) -> Result<u64, NativeError> {
    loop {
        match rows.as_mut().try_next().await {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(rows.as_ref().rows_affected().unwrap_or(0)),
            Err(error) => return Err(NativeError::PostgresQuery(error)),
        }
    }
}

async fn cancel_transaction_rows(
    transaction: &deadpool_postgres::Transaction<'_>,
    tls: &TlsConnector,
    mut rows: Pin<&mut RowStream>,
) -> NativeError {
    let cancel_token = transaction.cancel_token();
    let cancellation = tls.cancel_query(&cancel_token).await;
    let operation_allows_reuse = loop {
        match rows.as_mut().try_next().await {
            Ok(Some(_)) => {}
            Ok(None) => break false,
            Err(error) => break postgres::query_error_confirms_cancellation(&error),
        }
    };
    let connection_can_be_reused = cancellation.is_ok() && operation_allows_reuse;

    match cancellation {
        Ok(()) => NativeError::QueryCancelled {
            connection_can_be_reused,
        },
        Err(error) => NativeError::PostgresCancel(error),
    }
}

fn transaction_error_allows_reuse(error: &NativeError) -> bool {
    match error {
        NativeError::PostgresQuery(error) => postgres::query_error_allows_connection_reuse(error),
        NativeError::PostgresCancel(_) => false,
        NativeError::QueryCancelled {
            connection_can_be_reused,
        } => *connection_can_be_reused,
        _ => true,
    }
}

fn send_transaction_command_error(command: TransactionCommand, error: NativeError) {
    match command {
        TransactionCommand::Execute { response, .. } => {
            let _ = response.send(Err(error));
        }
        TransactionCommand::OpenQueryStream { response, .. } => {
            let _ = response.send(Err(error));
        }
        TransactionCommand::Commit { response } | TransactionCommand::Rollback { response } => {
            let _ = response.send(Err(error));
        }
    }
}

async fn clean_up_cancelled_start(
    result: Result<deadpool_postgres::Transaction<'_>, tokio_postgres::Error>,
) {
    if let Ok(transaction) = result {
        let _ = transaction.rollback().await;
    }
}

pub(crate) async fn open_query_stream(
    handle: TransactionHandle,
    sql: String,
    params: QueryParameters,
) -> Result<QueryStreamOpenResult, NativeError> {
    handle.validate_process()?;

    if handle.is_closed() {
        return Err(NativeError::TransactionHandleClosed);
    }

    let (mut cancellation_guard, cancellation) = CancellationGuard::new();
    let (response_tx, response_rx) = oneshot::channel();

    if handle
        .sender
        .send(TransactionCommand::OpenQueryStream {
            sql,
            params,
            cancellation,
            response: response_tx,
        })
        .await
        .is_err()
    {
        handle.mark_closed();
        return Err(NativeError::TransactionHandleClosed);
    }

    let response = response_rx.await;
    cancellation_guard.disarm();

    match response {
        Ok(result) => result,
        Err(_) => {
            handle.mark_closed();
            Err(NativeError::TransactionHandleClosed)
        }
    }
}
