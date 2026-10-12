use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::cancellation::CancellationGuard;
use crate::error::NativeError;
use crate::parameter::QueryParameters;
use crate::postgres::{self, QueryColumns, QueryRows};
use deadpool_postgres::{Object, Pool};
use futures_util::{FutureExt, TryStreamExt};
use pyo3::prelude::*;
use std::pin::Pin;
use std::process;
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{NoTls, Row, RowStream};

enum OperationOutcome<T> {
    Completed(Result<T, tokio_postgres::Error>),
    Cancelled {
        cancellation: Result<(), tokio_postgres::Error>,
        operation: Result<T, tokio_postgres::Error>,
    },
}

enum NextRowOutcome {
    Row(Result<Option<Row>, tokio_postgres::Error>),
    Cancelled,
}

pub(crate) enum QueryStreamCommand {
    Fetch {
        size: usize,
        cancellation: oneshot::Receiver<()>,
        response: oneshot::Sender<Result<QueryStreamFetchResult, NativeError>>,
    },
    Close {
        response: oneshot::Sender<Result<(), NativeError>>,
    },
}

pub(crate) struct QueryStreamFetchResult {
    rows: QueryRows,
    exhausted: bool,
}

#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct QueryStreamHandle {
    process_id: u32,
    sender: mpsc::Sender<QueryStreamCommand>,
    closed: Arc<AtomicBool>,
    exhausted: Arc<AtomicBool>,
}

impl QueryStreamHandle {
    pub(crate) fn new(sender: mpsc::Sender<QueryStreamCommand>) -> Self {
        Self {
            process_id: process::id(),
            sender,
            closed: Arc::new(AtomicBool::new(false)),
            exhausted: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn is_exhausted(&self) -> bool {
        self.exhausted.load(Ordering::Acquire)
    }

    pub(crate) fn mark_exhausted(&self) {
        self.exhausted.store(true, Ordering::Release);
    }

    pub(crate) fn try_mark_closed(&self) -> bool {
        self.closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn validate_process(&self) -> Result<(), NativeError> {
        if self.process_id != process::id() {
            return Err(NativeError::QueryStreamHandleProcessMismatch);
        }
        Ok(())
    }
}

pub(crate) struct QueryStreamOpenResult {
    pub(crate) handle: QueryStreamHandle,
    pub(crate) rows_affected: Option<u64>,
    pub(crate) columns: QueryColumns,
}

pub(crate) async fn open(
    pool: &Pool,
    sql: String,
    params: QueryParameters,
    mut cancellation: oneshot::Receiver<()>,
) -> Result<QueryStreamOpenResult, NativeError> {
    let client = tokio::select! {
        biased;
        _ = &mut cancellation => {
            return Err(NativeError::QueryCancelled {
                connection_can_be_reused: true,
            });
        }
        result = pool.get() => {
            result.map_err(|error| NativeError::PostgresPoolAcquire(error.to_string()))?
        }
    };

    let (postgres_sql, params) = params.prepare(&sql)?;
    let parameter_types: Vec<Type> = params
        .iter()
        .map(|parameter| parameter.postgres_type())
        .collect();

    let prepare_outcome = {
        let prepare = client.prepare_typed(&postgres_sql, &parameter_types);
        tokio::pin!(prepare);

        tokio::select! {
            biased;
            _ = &mut cancellation => {
                let cancellation = client.cancel_token().cancel_query(NoTls).await;
                let operation = prepare.as_mut().await;
                OperationOutcome::Cancelled {
                    cancellation,
                    operation,
                }
            }
            result = &mut prepare => OperationOutcome::Completed(result)
        }
    };

    let statement = match prepare_outcome {
        OperationOutcome::Completed(Ok(statement)) => statement,
        OperationOutcome::Completed(Err(error)) => {
            let connection_can_be_reused =
                !client.is_closed() && postgres::query_error_allows_connection_reuse(&error);
            discard_if_needed(client, connection_can_be_reused);
            return Err(NativeError::PostgresQuery(error));
        }
        OperationOutcome::Cancelled {
            cancellation,
            operation,
        } => {
            let connection_can_be_reused = !client.is_closed()
                && cancellation.is_ok()
                && match operation {
                    Ok(_) => false,
                    Err(ref error) => postgres::query_error_confirms_cancellation(error),
                };
            discard_if_needed(client, connection_can_be_reused);

            return match cancellation {
                Ok(()) => Err(NativeError::QueryCancelled {
                    connection_can_be_reused,
                }),
                Err(error) => Err(NativeError::PostgresCancel(error)),
            };
        }
    };

    let columns: QueryColumns = statement
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect();
    let postgres_params: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|parameter| parameter.as_postgres())
        .collect();
    let query_outcome = {
        let query = client.query_raw(&statement, postgres_params);
        tokio::pin!(query);

        tokio::select! {
            biased;
            _ = &mut cancellation => {
                let cancellation = client.cancel_token().cancel_query(NoTls).await;
                let operation = query.as_mut().await;
                OperationOutcome::Cancelled {
                    cancellation,
                    operation,
                }
            }
            result = &mut query => OperationOutcome::Completed(result)
        }
    };

    let rows = match query_outcome {
        OperationOutcome::Completed(Ok(rows)) => rows,
        OperationOutcome::Completed(Err(error)) => {
            let connection_can_be_reused =
                !client.is_closed() && postgres::query_error_allows_connection_reuse(&error);
            discard_if_needed(client, connection_can_be_reused);
            return Err(NativeError::PostgresQuery(error));
        }
        OperationOutcome::Cancelled {
            cancellation,
            operation,
        } => {
            let operation_allows_reuse = match operation {
                Ok(rows) => {
                    let mut rows = Box::pin(rows);
                    drain_cancelled_rows(rows.as_mut()).await
                }
                Err(ref error) => postgres::query_error_confirms_cancellation(error),
            };
            let connection_can_be_reused =
                !client.is_closed() && cancellation.is_ok() && operation_allows_reuse;
            discard_if_needed(client, connection_can_be_reused);

            return match cancellation {
                Ok(()) => Err(NativeError::QueryCancelled {
                    connection_can_be_reused,
                }),
                Err(error) => Err(NativeError::PostgresCancel(error)),
            };
        }
    };

    if columns.is_empty() {
        return finish_non_row_query(client, rows, cancellation).await;
    }

    let (sender, receiver) = mpsc::channel(1);
    tokio::spawn(run_query_stream(client, rows, receiver));

    Ok(QueryStreamOpenResult {
        handle: QueryStreamHandle::new(sender),
        rows_affected: None,
        columns,
    })
}

async fn finish_non_row_query(
    client: Object,
    rows: RowStream,
    mut cancellation: oneshot::Receiver<()>,
) -> Result<QueryStreamOpenResult, NativeError> {
    let mut rows = Box::pin(rows);

    loop {
        let next_row = {
            let mut pinned_rows = rows.as_mut();
            let next_row = pinned_rows.try_next();
            tokio::pin!(next_row);

            tokio::select! {
                biased;
                _ = &mut cancellation => NextRowOutcome::Cancelled,
                result = &mut next_row => NextRowOutcome::Row(result),
            }
        };

        match next_row {
            NextRowOutcome::Row(Ok(Some(_))) => continue,
            NextRowOutcome::Row(Ok(None)) => {
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
            NextRowOutcome::Row(Err(error)) => {
                let connection_can_be_reused =
                    postgres::query_error_allows_connection_reuse(&error);
                discard_if_needed(client, connection_can_be_reused);
                return Err(NativeError::PostgresQuery(error));
            }
            NextRowOutcome::Cancelled => {
                let cleanup = cancel_and_drain(&client, rows.as_mut()).await;
                let connection_can_be_reused = cleanup.as_ref().is_ok_and(|reusable| *reusable);
                let error = match cleanup {
                    Ok(_) => NativeError::QueryCancelled {
                        connection_can_be_reused,
                    },
                    Err(error) => error,
                };
                discard_if_needed(client, connection_can_be_reused);
                return Err(error);
            }
        }
    }
}

pub(crate) async fn fetch(
    handle: QueryStreamHandle,
    size: usize,
) -> Result<QueryRows, NativeError> {
    handle.validate_process()?;
    if size == 0 {
        return Err(NativeError::InvalidQueryStreamFetchSize);
    }
    if handle.is_closed() {
        return Err(NativeError::QueryStreamHandleClosed);
    }
    if handle.is_exhausted() {
        return Ok(Vec::new());
    }

    let (mut cancellation_guard, cancellation) = CancellationGuard::new();
    let (response_tx, response_rx) = oneshot::channel();

    if handle
        .sender
        .send(QueryStreamCommand::Fetch {
            size,
            cancellation,
            response: response_tx,
        })
        .await
        .is_err()
    {
        handle.mark_closed();
        return Err(NativeError::QueryStreamHandleClosed);
    }

    let response = response_rx.await;
    cancellation_guard.disarm();

    match response {
        Ok(Ok(result)) => {
            if result.exhausted {
                handle.mark_exhausted();
            }
            Ok(result.rows)
        }
        Ok(Err(error)) => {
            handle.mark_closed();
            Err(error)
        }
        Err(_) => {
            handle.mark_closed();
            Err(NativeError::QueryStreamHandleClosed)
        }
    }
}

pub(crate) async fn close(handle: QueryStreamHandle) -> Result<(), NativeError> {
    handle.validate_process()?;
    if !handle.try_mark_closed() {
        return Err(NativeError::QueryStreamHandleClosed);
    }
    if handle.is_exhausted() {
        return Ok(());
    }

    let (response_tx, response_rx) = oneshot::channel();
    handle
        .sender
        .send(QueryStreamCommand::Close {
            response: response_tx,
        })
        .await
        .map_err(|_| NativeError::QueryStreamHandleClosed)?;

    response_rx
        .await
        .map_err(|_| NativeError::QueryStreamHandleClosed)?
}

async fn run_query_stream(
    client: Object,
    rows: RowStream,
    mut receiver: mpsc::Receiver<QueryStreamCommand>,
) {
    let mut rows = Box::pin(rows);
    let mut pending_row = None;
    let connection_can_be_reused = loop {
        let Some(command) = receiver.recv().await else {
            break cancel_and_drain(&client, rows.as_mut())
                .await
                .is_ok_and(|reusable| reusable);
        };

        match command {
            QueryStreamCommand::Fetch {
                size,
                mut cancellation,
                response,
            } => {
                let mut decoded_rows = Vec::with_capacity(size);
                let mut exhausted = false;
                let mut fetch_error = None;

                while decoded_rows.len() < size {
                    let next_row = if let Some(row) = pending_row.take() {
                        NextRowOutcome::Row(Ok(Some(row)))
                    } else {
                        let mut pinned_rows = rows.as_mut();
                        let next_row = pinned_rows.try_next();
                        tokio::pin!(next_row);

                        tokio::select! {
                            biased;
                            _ = &mut cancellation => NextRowOutcome::Cancelled,
                            result = &mut next_row => NextRowOutcome::Row(result),
                        }
                    };

                    let row = match next_row {
                        NextRowOutcome::Row(row) => row,
                        NextRowOutcome::Cancelled => {
                            let cleanup = cancel_and_drain(&client, rows.as_mut()).await;
                            let connection_can_be_reused =
                                cleanup.as_ref().is_ok_and(|reusable| *reusable);
                            let error = match cleanup {
                                Ok(_) => NativeError::QueryCancelled {
                                    connection_can_be_reused,
                                },
                                Err(error) => error,
                            };
                            let _ = response.send(Err(error));
                            return discard_if_needed(client, connection_can_be_reused);
                        }
                    };

                    match row {
                        Ok(Some(row)) => match postgres::decode_row(row) {
                            Ok(row) => decoded_rows.push(row),
                            Err(error) => {
                                fetch_error = Some(error);
                                break;
                            }
                        },
                        Ok(None) => {
                            exhausted = true;
                            break;
                        }
                        Err(error) => {
                            let connection_can_be_reused =
                                postgres::query_error_allows_connection_reuse(&error);
                            let _ = response.send(Err(NativeError::PostgresQuery(error)));
                            return discard_if_needed(client, connection_can_be_reused);
                        }
                    }
                }

                if let Some(error) = fetch_error {
                    let connection_can_be_reused = cancel_and_drain(&client, rows.as_mut())
                        .await
                        .is_ok_and(|reusable| reusable);
                    let _ = response.send(Err(error));
                    break connection_can_be_reused;
                }

                if !exhausted && decoded_rows.len() == size {
                    let immediately_available_row = {
                        let mut pinned_rows = rows.as_mut();
                        pinned_rows.try_next().now_or_never()
                    };

                    match immediately_available_row {
                        Some(Ok(Some(row))) => pending_row = Some(row),
                        Some(Ok(None)) => exhausted = true,
                        Some(Err(error)) => {
                            let connection_can_be_reused =
                                postgres::query_error_allows_connection_reuse(&error);
                            let _ = response.send(Err(NativeError::PostgresQuery(error)));
                            return discard_if_needed(client, connection_can_be_reused);
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
                    break cancel_and_drain(&client, rows.as_mut())
                        .await
                        .is_ok_and(|reusable| reusable);
                }

                if exhausted {
                    break true;
                }
            }
            QueryStreamCommand::Close { response } => {
                let cleanup = cancel_and_drain(&client, rows.as_mut()).await;
                let connection_can_be_reused = cleanup.as_ref().is_ok_and(|reusable| *reusable);
                let result = cleanup.map(|_| ());
                let _ = response.send(result);
                break connection_can_be_reused;
            }
        }
    };

    discard_if_needed(client, connection_can_be_reused);
}

async fn cancel_and_drain(
    client: &Object,
    mut rows: Pin<&mut RowStream>,
) -> Result<bool, NativeError> {
    client
        .cancel_token()
        .cancel_query(NoTls)
        .await
        .map_err(NativeError::PostgresCancel)?;

    Ok(drain_cancelled_rows(rows.as_mut()).await)
}

async fn drain_cancelled_rows(mut rows: Pin<&mut RowStream>) -> bool {
    loop {
        match rows.as_mut().try_next().await {
            Ok(Some(_)) => continue,
            Ok(None) => return false,
            Err(error) => return postgres::query_error_confirms_cancellation(&error),
        }
    }
}

fn discard_if_needed(client: Object, connection_can_be_reused: bool) {
    if !connection_can_be_reused || client.is_closed() {
        drop(Object::take(client));
    }
}
