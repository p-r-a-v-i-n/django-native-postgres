use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::prelude::*;
use tokio::sync::{mpsc, oneshot};

use crate::cancellation::CancellationGuard;
use crate::error::NativeError;
use crate::parameter::QueryParameter;
use crate::postgres::{self, QueryRows};
use deadpool_postgres::{Object, Pool};
use std::process;

pub(crate) enum TransactionCommand {
    Execute {
        sql: String,
        params: Vec<QueryParameter>,
        cancellation: oneshot::Receiver<()>,
        response: oneshot::Sender<Result<QueryRows, NativeError>>,
    },
    Commit {
        response: oneshot::Sender<Result<(), NativeError>>,
    },
    Rollback {
        response: oneshot::Sender<Result<(), NativeError>>,
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

pub(crate) async fn begin(pool: &Pool) -> Result<TransactionHandle, NativeError> {
    let client = pool
        .get()
        .await
        .map_err(|error| NativeError::PostgresPoolAcquire(error.to_string()))?;

    let (sender, receiver) = mpsc::channel(1);
    let (ready_sender, ready_receiver) = oneshot::channel();

    tokio::spawn(run_transaction(client, receiver, ready_sender));

    ready_receiver
        .await
        .map_err(|_| NativeError::ResponseChannelClosed)??;

    Ok(TransactionHandle::new(sender))
}

pub(crate) async fn execute(
    handle: TransactionHandle,
    sql: String,
    params: Vec<QueryParameter>,
) -> Result<QueryRows, NativeError> {
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
    mut receiver: mpsc::Receiver<TransactionCommand>,
    ready: oneshot::Sender<Result<(), NativeError>>,
) {
    let transaction = match client.transaction().await {
        Ok(transaction) => transaction,
        Err(source) => {
            let _ = ready.send(Err(NativeError::PostgresTransaction {
                operation: "begin",
                source,
            }));
            return;
        }
    };

    if ready.send(Ok(())).is_err() {
        return;
    }

    while let Some(command) = receiver.recv().await {
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
                    &sql,
                    &params,
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
