use std::collections::HashMap;
use std::process;
use std::sync::{Arc, Mutex as SyncMutex, OnceLock};
use std::thread;
use std::time::Duration;

use crate::error::NativeError;
use crate::parameter::QueryParameter;
use crate::postgres::{self, QueryRows};
use deadpool_postgres::Pool;
use tokio::sync::{Mutex, mpsc, oneshot};

type PoolRegistry = Arc<Mutex<HashMap<String, Pool>>>;

enum Command {
    Probe {
        delay_ms: u64,
        response: oneshot::Sender<u64>,
    },
    Execute {
        database_url: String,
        pool_max_size: usize,
        pool_wait_timeout_ms: u64,
        sql: String,
        params: Vec<QueryParameter>,
        response: oneshot::Sender<Result<QueryRows, NativeError>>,
    },
    ClosePools {
        response: oneshot::Sender<()>,
    },
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
    database_url: String,
    sql: String,
    params: Vec<QueryParameter>,
    pool_max_size: usize,
    pool_wait_timeout_ms: u64,
) -> Result<QueryRows, NativeError> {
    let service = RuntimeService::start();
    let (response_tx, response_rx) = oneshot::channel();

    service
        .sender
        .send(Command::Execute {
            database_url,
            pool_max_size,
            pool_wait_timeout_ms,
            sql,
            params,
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
    database_url: &str,
    pool_max_size: usize,
    pool_wait_timeout_ms: u64,
) -> Result<Pool, NativeError> {
    if pool_max_size == 0 {
        return Err(NativeError::InvalidPoolMaxSize);
    }
    let mut pools = pools.lock().await;

    if let Some(pool) = pools.get(database_url) {
        return Ok(pool.clone());
    }

    let pool = postgres::create_pool(database_url, pool_max_size, pool_wait_timeout_ms)?;
    pools.insert(database_url.to_string(), pool.clone());
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
            database_url,
            pool_max_size,
            pool_wait_timeout_ms,
            sql,
            params,
            response,
        } => {
            let result = match get_or_create_pool(
                &pools,
                &database_url,
                pool_max_size,
                pool_wait_timeout_ms,
            )
            .await
            {
                Ok(pool) => postgres::execute(&pool, &sql, &params).await,
                Err(error) => Err(error),
            };
            // Cancellation may drop the receiver before this task completes.
            let _ = response.send(result);
        }
        Command::ClosePools { response } => {
            let mut pools = pools.lock().await;
            for (_, pool) in pools.drain() {
                pool.close();
            }
            let _ = response.send(());
        }
    }
}
