use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

use crate::error::NativeError;
use crate::postgres::{self, TextRows};
use tokio::sync::{mpsc, oneshot};

// A command that can be sent to our Tokio runtime.
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

// This is the object that Python-facing code uses to talk
// to the background Tokio runtime.
struct RuntimeService {
    sender: mpsc::Sender<Command>,
}

// There will only ever be one RuntimeService.
static RUNTIME_SERVICE: OnceLock<RuntimeService> = OnceLock::new();

impl RuntimeService {
    fn start() -> &'static RuntimeService {
        RUNTIME_SERVICE.get_or_init(|| {
            // Create the bounded command queue.
            let (sender, mut receiver) = mpsc::channel::<Command>(32);

            // Create exactly ONE OS thread.
            thread::Builder::new()
                .name("tokio-runtime".to_string())
                .spawn(move || {
                    // Create ONE current-thread Tokio runtime.
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to create Tokio runtime");

                    // Run the command loop forever.
                    runtime.block_on(async move {
                        while let Some(command) = receiver.recv().await {
                            // Each command gets its own Tokio task.
                            tokio::spawn(handle_command(command));
                        }
                    });
                })
                .expect("failed to spawn Tokio runtime thread");

            // Give the caller the sending side of the queue.
            RuntimeService { sender }
        })
    }
}

// Handle one command.
async fn handle_command(command: Command) {
    match command {
        Command::Probe { delay_ms, response } => {
            // Wait without blocking the OS thread.
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;

            // Send the result back through the oneshot channel.
            let _ = response.send(delay_ms);
        }
        Command::Execute {
            database_url,
            sql,
            response,
        } => {
            let result = postgres::execute(&database_url, &sql).await;
            let _ = response.send(result);
        }
    }
}
