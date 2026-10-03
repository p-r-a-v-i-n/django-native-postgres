use std::collections::HashMap;
use std::process;
use std::sync::{Arc, Mutex as SyncMutex, OnceLock};
use std::thread;
use std::time::Duration;

use crate::cancellation::CancellationGuard;
use crate::error::NativeError;
use crate::parameter::QueryParameter;
use crate::pool::PoolHandle;
use crate::postgres::{self, QueryRows};
use crate::transaction::{self, TransactionHandle};
use deadpool_postgres::Pool;
use tokio::sync::{Mutex, mpsc, oneshot};

type PoolRegistry = Arc<Mutex<HashMap<u64, Pool>>>;

enum Command {
    Probe {
        delay_ms: u64,
        response: oneshot::Sender<u64>,
    },
    Execute {
        pool: PoolHandle,
        sql: String,
        params: Vec<QueryParameter>,
        cancellation: oneshot::Receiver<()>,
        response: oneshot::Sender<Result<QueryRows, NativeError>>,
    },
    BeginTransaction {
        pool: PoolHandle,
        response: oneshot::Sender<Result<TransactionHandle, NativeError>>,
    },
    ClosePools {
        response: oneshot::Sender<()>,
    },
    ClosePool {
        pool_id: u64,
        response: oneshot::Sender<()>,
    },
}

pub(crate) async fn close_pool(pool: PoolHandle) -> Result<(), NativeError> {
    pool.mark_closed();

    let service = RuntimeService::start();
    let (response_tx, response_rx) = oneshot::channel();

    service
        .sender
        .send(Command::ClosePool {
            pool_id: pool.id,
            response: response_tx,
        })
        .await
        .map_err(|_| NativeError::CommandChannelClosed)?;

    response_rx
        .await
        .map_err(|_| NativeError::ResponseChannelClosed)
}

pub(crate) async fn probe(delay_ms: u64) -> Result<u64, NativeError> {
    let service = RuntimeService::start();
    let (response_tx, response_rx) = oneshot::channel();

    service
        .sender
        .send(Command::Probe {
            delay_ms,
            response: response_tx,
        })
        .await
        .map_err(|_| NativeError::CommandChannelClosed)?;

    response_rx
        .await
        .map_err(|_| NativeError::ResponseChannelClosed)
}

pub(crate) async fn execute(
    pool: PoolHandle,
    sql: String,
    params: Vec<QueryParameter>,
) -> Result<QueryRows, NativeError> {
    let (mut cancellation_guard, cancellation) = CancellationGuard::new();
    let service = RuntimeService::start();
    let (response_tx, response_rx) = oneshot::channel();

    service
        .sender
        .send(Command::Execute {
            pool,
            sql,
            params,
            cancellation,
            response: response_tx,
        })
        .await
        .map_err(|_| NativeError::CommandChannelClosed)?;

    let response = response_rx.await;
    cancellation_guard.disarm();

    response.map_err(|_| NativeError::ResponseChannelClosed)?
}

pub(crate) async fn begin_transaction(pool: PoolHandle) -> Result<TransactionHandle, NativeError> {
    let service = RuntimeService::start();
    let (response_tx, response_rx) = oneshot::channel();

    service
        .sender
        .send(Command::BeginTransaction {
            pool,
            response: response_tx,
        })
        .await
        .map_err(|_| NativeError::CommandChannelClosed)?;

    response_rx
        .await
        .map_err(|_| NativeError::ResponseChannelClosed)?
}

pub(crate) async fn close_pools() -> Result<(), NativeError> {
    let service = RuntimeService::start();
    let (response_tx, response_rx) = oneshot::channel();

    service
        .sender
        .send(Command::ClosePools {
            response: response_tx,
        })
        .await
        .map_err(|_| NativeError::CommandChannelClosed)?;

    response_rx
        .await
        .map_err(|_| NativeError::ResponseChannelClosed)
}

// Handle to the command queue owned by the background runtime.
#[derive(Clone)]
struct RuntimeService {
    process_id: u32,
    sender: mpsc::Sender<Command>,
}

// Shared by all callers in the current process.
static RUNTIME_SERVICE: OnceLock<SyncMutex<Option<RuntimeService>>> = OnceLock::new();

impl RuntimeService {
    fn start() -> Self {
        let process_id = process::id();
        let service = RUNTIME_SERVICE.get_or_init(|| SyncMutex::new(None));
        let mut service = service.lock().unwrap_or_else(|error| error.into_inner());

        let needs_new_runtime = match service.as_ref() {
            Some(service) => service.process_id != process_id,
            None => true,
        };

        if needs_new_runtime {
            *service = Some(Self::new(process_id));
        }

        service
            .as_ref()
            .expect("runtime service must be initialized")
            .clone()
    }
    fn new(process_id: u32) -> Self {
        let (sender, mut receiver) = mpsc::channel::<Command>(32);

        thread::Builder::new()
            .name("tokio-runtime".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create Tokio runtime");

                runtime.block_on(async move {
                    let pools = Arc::new(Mutex::new(HashMap::new()));

                    while let Some(command) = receiver.recv().await {
                        tokio::spawn(handle_command(command, Arc::clone(&pools)));
                    }
                });
            })
            .expect("failed to spawn Tokio runtime thread");

        RuntimeService { process_id, sender }
    }
}

async fn get_or_create_pool(
    pools: &PoolRegistry,
    handle: &PoolHandle,
) -> Result<Pool, NativeError> {
    let mut pools = pools.lock().await;

    if handle.is_closed() {
        return Err(NativeError::PoolHandleClosed);
    }

    if let Some(pool) = pools.get(&handle.id) {
        return Ok(pool.clone());
    }

    let pool = postgres::create_pool(
        &handle.database_url,
        handle.max_size,
        handle.wait_timeout_ms,
    )?;

    // Closing can race with the first pool creation.
    if handle.is_closed() {
        pool.close();
        return Err(NativeError::PoolHandleClosed);
    }

    pools.insert(handle.id, pool.clone());
    Ok(pool)
}

async fn handle_command(command: Command, pools: PoolRegistry) {
    match command {
        Command::Probe { delay_ms, response } => {
            // The timer yields the runtime thread while the probe is pending.
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;

            // Cancellation may drop the receiver before this task completes.
            let _ = response.send(delay_ms);
        }
        Command::Execute {
            pool,
            sql,
            params,
            mut cancellation,
            response,
        } => {
            let result = match get_or_create_pool(&pools, &pool).await {
                Ok(pool) => postgres::execute(&pool, &sql, &params, &mut cancellation).await,
                Err(error) => Err(error),
            };
            // Cancellation may drop the receiver before this task completes.
            let _ = response.send(result);
        }
        Command::BeginTransaction { pool, response } => {
            let result = match get_or_create_pool(&pools, &pool).await {
                Ok(pool) => transaction::begin(&pool).await,
                Err(error) => Err(error),
            };
            let _ = response.send(result);
        }
        Command::ClosePools { response } => {
            let mut pools = pools.lock().await;
            for (_, pool) in pools.drain() {
                pool.close();
            }
            let _ = response.send(());
        }
        Command::ClosePool { pool_id, response } => {
            let pool = pools.lock().await.remove(&pool_id);
            if let Some(pool) = pool {
                pool.close();
            }
            let _ = response.send(());
        }
    }
}
