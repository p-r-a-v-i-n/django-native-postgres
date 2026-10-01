//! Minimal native packaging probe for the project foundation.

use crate::error::NativeError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

mod error;
mod parameter;
mod placeholders;
mod pool;
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

#[pyfunction(signature=(database_url, max_size=16, wait_timeout_ms=30_000))]
fn create_pool(
    database_url: String,
    max_size: usize,
    wait_timeout_ms: i64,
) -> PyResult<pool::PoolHandle> {
    pool::PoolHandle::new(database_url, max_size, wait_timeout_ms).map_err(to_python_error)
}

#[pyfunction]
async fn close_pool(pool: Py<pool::PoolHandle>) -> PyResult<()> {
    runtime::close_pool(pool.get().clone())
        .await
        .map_err(to_python_error)
}

#[pyfunction(signature = (pool, sql, params=None))]
async fn execute(
    pool: Py<pool::PoolHandle>,
    sql: String,
    params: Option<Vec<parameter::QueryParameter>>,
) -> PyResult<postgres::QueryRows> {
    runtime::execute(pool.get().clone(), sql, params.unwrap_or_default())
        .await
        .map_err(to_python_error)
}

#[pyfunction]
async fn close_pools() -> PyResult<()> {
    runtime::close_pools().await.map_err(to_python_error)
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<pool::PoolHandle>()?;

    module.add_function(wrap_pyfunction!(build_info, module)?)?;
    module.add_function(wrap_pyfunction!(runtime_probe, module)?)?;
    module.add_function(wrap_pyfunction!(execute, module)?)?;
    module.add_function(wrap_pyfunction!(close_pools, module)?)?;
    module.add_function(wrap_pyfunction!(create_pool, module)?)?;
    module.add_function(wrap_pyfunction!(close_pool, module)?)?;
    Ok(())
}

fn to_python_error(error: NativeError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
