use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::NativeError;
use crate::parameter::QueryParameters;
use crate::postgres::{self, QueryResult};
use pyo3::prelude::*;
use std::process;
use tokio::sync::{mpsc, oneshot};

pub(crate) enum QueryStreamCommand {
    Fetch {
        size: usize,
        response: oneshot::Sender<Result<QueryResult, NativeError>>,
    },
    Close {
        response: oneshot::Sender<Result<(), NativeError>>,
    },
}

#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct QueryStreamHandle {
    process_id: u32,
    sender: mpsc::Sender<QueryStreamCommand>,
    closed: Arc<AtomicBool>,
}

impl QueryStreamHandle {
    pub(crate) fn new(sender: mpsc::Sender<QueryStreamCommand>) -> Self {
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
            return Err(NativeError::QueryStreamHandleProcessMismatch);
        }
        Ok(())
    }
}

pub(crate) async fn fetch(handle: QueryStreamHandle, sql: String, params: QueryParameters) {
    todo!("not implemented yet");
}
