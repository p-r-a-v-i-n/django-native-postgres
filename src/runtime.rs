use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

use crate::error::NativeError;
use crate::postgres::{self, TextRows};
use tokio::sync::{mpsc, oneshot};

enum Command {
    Probe {
        delay_ms: u64,
        response: oneshot::Sender<u64>,
    },
    Execute {
        database_url: String,
        sql: String,
        response: oneshot::Sender<Result<TextRows, NativeError>>,
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

pub(crate) async fn execute(database_url: String, sql: String) -> Result<TextRows, NativeError> {
    let service = RuntimeService::start();
    let (response_tx, response_rx) = oneshot::channel();

    service
        .sender
        .send(Command::Execute {
            database_url,
            sql,
            response: response_tx,
        })
        .await
        .map_err(|_| NativeError::CommandChannelClosed)?;

    response_rx
        .await
        .map_err(|_| NativeError::ResponseChannelClosed)?
}

// Handle to the command queue owned by the background runtime.
struct RuntimeService {
    sender: mpsc::Sender<Command>,
}

// Shared by all callers in the current process.
static RUNTIME_SERVICE: OnceLock<RuntimeService> = OnceLock::new();

impl RuntimeService {
    fn start() -> &'static RuntimeService {
        RUNTIME_SERVICE.get_or_init(|| {
            // A bounded queue applies backpressure during submission bursts.
            let (sender, mut receiver) = mpsc::channel::<Command>(32);

            // Database I/O runs on a dedicated current-thread Tokio runtime.
            thread::Builder::new()
                .name("tokio-runtime".to_string())
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to create Tokio runtime");

                    runtime.block_on(async move {
                        while let Some(command) = receiver.recv().await {
                            // Spawning prevents one request from serializing the command loop.
                            tokio::spawn(handle_command(command));
                        }
                    });
                })
                .expect("failed to spawn Tokio runtime thread");

            RuntimeService { sender }
        })
    }
}

async fn handle_command(command: Command) {
    match command {
        Command::Probe { delay_ms, response } => {
            // The timer yields the runtime thread while the probe is pending.
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;

            // Cancellation may drop the receiver before this task completes.
            let _ = response.send(delay_ms);
        }
        Command::Execute {
            database_url,
            sql,
            response,
        } => {
            let result = postgres::execute(&database_url, &sql).await;
            // Cancellation may drop the receiver before this task completes.
            let _ = response.send(result);
        }
    }
}
