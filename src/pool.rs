use crate::error::NativeError;
use pyo3::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PoolHandle {
    pub(crate) id: u64,
    pub(crate) database_url: String,
    pub(crate) max_size: usize,
    pub(crate) wait_timeout_ms: u64,
    closed: Arc<AtomicBool>,
}

impl PoolHandle {
    pub(crate) fn new(
        database_url: String,
        max_size: usize,
        wait_timeout_ms: u64,
    ) -> Result<Self, NativeError> {
        if max_size == 0 {
            return Err(NativeError::InvalidPoolMaxSize);
        }

        Ok(Self {
            id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
            database_url,
            max_size,
            wait_timeout_ms,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
    }
}
