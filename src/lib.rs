//! Minimal native packaging probe for the project foundation.

use crate::error::NativeError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

mod error;
mod parameter;
mod placeholders;
mod postgres;
mod runtime;

#[pyfunction]
fn build_info() -> (&'static str, &'static str) {
    (env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
}

#[pyfunction]
async fn runtime_probe(delay_ms: u64) -> PyResult<u64> {
    runtime::probe(delay_ms).await.map_err(to_python_error)
}

#[pyfunction(signature = (database_url, sql, params=None, pool_max_size=16, pool_wait_timeout_ms=30_000))]
async fn execute(
    database_url: String,
    sql: String,
    params: Option<Vec<parameter::QueryParameter>>,
    pool_max_size: usize,
    pool_wait_timeout_ms: u64,
) -> PyResult<postgres::QueryRows> {
    runtime::execute(
        database_url,
        sql,
        params.unwrap_or_default(),
        pool_max_size,
        pool_wait_timeout_ms,
    )
    .await
    .map_err(to_python_error)
}

#[pyfunction]
async fn close_pools() -> PyResult<()> {
    runtime::close_pools().await.map_err(to_python_error)
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(build_info, module)?)?;
    module.add_function(wrap_pyfunction!(runtime_probe, module)?)?;
    module.add_function(wrap_pyfunction!(execute, module)?)?;
    module.add_function(wrap_pyfunction!(close_pools, module)?)?;
    Ok(())
}

fn to_python_error(error: NativeError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
