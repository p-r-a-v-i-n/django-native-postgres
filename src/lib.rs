//! Minimal native packaging probe for the project foundation.

use crate::error::NativeError;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

pyo3::create_exception!(_native, PostgresIntegrityError, PyRuntimeError);

mod cancellation;
mod error;
mod parameter;
mod placeholders;
mod pool;
mod postgres;
mod runtime;
mod transaction;
mod transaction_options;

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

#[pyfunction(signature = (
    pool,
    isolation_level=None,
    read_only=None,
    deferrable=None
))]
async fn begin_transaction(
    pool: Py<pool::PoolHandle>,
    isolation_level: Option<String>,
    read_only: Option<bool>,
    deferrable: Option<bool>,
) -> PyResult<transaction::TransactionHandle> {
    let options =
        transaction_options::TransactionOptions::new(isolation_level, read_only, deferrable)
            .map_err(to_python_error)?;

    runtime::begin_transaction(pool.get().clone(), options)
        .await
        .map_err(to_python_error)
}

#[pyfunction(signature = (transaction, sql, params=None))]
async fn execute_transaction(
    transaction: Py<transaction::TransactionHandle>,
    sql: String,
    params: Option<Vec<parameter::QueryParameter>>,
) -> PyResult<postgres::QueryRows> {
    transaction::execute(transaction.get().clone(), sql, params.unwrap_or_default())
        .await
        .map(|result| result.rows)
        .map_err(to_python_error)
}

#[pyfunction(signature = (transaction, sql, params=None))]
async fn execute_transaction_result(
    transaction: Py<transaction::TransactionHandle>,
    sql: String,
    params: Option<Vec<parameter::QueryParameter>>,
) -> PyResult<(postgres::QueryRows, u64)> {
    transaction::execute(transaction.get().clone(), sql, params.unwrap_or_default())
        .await
        .map(|result| (result.rows, result.rows_affected))
        .map_err(to_python_error)
}

#[pyfunction(signature = (transaction, sql, params=None))]
async fn execute_transaction_with_metadata(
    transaction: Py<transaction::TransactionHandle>,
    sql: String,
    params: Option<Vec<parameter::QueryParameter>>,
) -> PyResult<(postgres::QueryRows, u64, postgres::QueryColumns)> {
    transaction::execute(transaction.get().clone(), sql, params.unwrap_or_default())
        .await
        .map(|result| (result.rows, result.rows_affected, result.columns))
        .map_err(to_python_error)
}

#[pyfunction]
async fn commit_transaction(transaction: Py<transaction::TransactionHandle>) -> PyResult<()> {
    transaction::commit(transaction.get().clone())
        .await
        .map_err(to_python_error)
}

#[pyfunction]
async fn rollback_transaction(transaction: Py<transaction::TransactionHandle>) -> PyResult<()> {
    transaction::rollback(transaction.get().clone())
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
        .map(|result| result.rows)
        .map_err(to_python_error)
}

#[pyfunction(signature = (pool, sql, params=None))]
async fn execute_result(
    pool: Py<pool::PoolHandle>,
    sql: String,
    params: Option<Vec<parameter::QueryParameter>>,
) -> PyResult<(postgres::QueryRows, u64)> {
    runtime::execute(pool.get().clone(), sql, params.unwrap_or_default())
        .await
        .map(|result| (result.rows, result.rows_affected))
        .map_err(to_python_error)
}

#[pyfunction(signature = (pool, sql, params=None))]
async fn execute_with_metadata(
    pool: Py<pool::PoolHandle>,
    sql: String,
    params: Option<Vec<parameter::QueryParameter>>,
) -> PyResult<(postgres::QueryRows, u64, postgres::QueryColumns)> {
    runtime::execute(pool.get().clone(), sql, params.unwrap_or_default())
        .await
        .map(|result| (result.rows, result.rows_affected, result.columns))
        .map_err(to_python_error)
}

#[pyfunction]
async fn close_pools() -> PyResult<()> {
    runtime::close_pools().await.map_err(to_python_error)
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add(
        "PostgresIntegrityError",
        module.py().get_type::<PostgresIntegrityError>(),
    )?;
    module.add_class::<pool::PoolHandle>()?;
    module.add_class::<transaction::TransactionHandle>()?;

    module.add_function(wrap_pyfunction!(build_info, module)?)?;
    module.add_function(wrap_pyfunction!(runtime_probe, module)?)?;
    module.add_function(wrap_pyfunction!(execute, module)?)?;
    module.add_function(wrap_pyfunction!(execute_result, module)?)?;
    module.add_function(wrap_pyfunction!(execute_with_metadata, module)?)?;
    module.add_function(wrap_pyfunction!(close_pools, module)?)?;
    module.add_function(wrap_pyfunction!(create_pool, module)?)?;
    module.add_function(wrap_pyfunction!(close_pool, module)?)?;
    module.add_function(wrap_pyfunction!(begin_transaction, module)?)?;
    module.add_function(wrap_pyfunction!(execute_transaction, module)?)?;
    module.add_function(wrap_pyfunction!(execute_transaction_result, module)?)?;
    module.add_function(wrap_pyfunction!(execute_transaction_with_metadata, module)?)?;
    module.add_function(wrap_pyfunction!(commit_transaction, module)?)?;
    module.add_function(wrap_pyfunction!(rollback_transaction, module)?)?;
    Ok(())
}

fn to_python_error(error: NativeError) -> PyErr {
    if matches!(
        &error,
        NativeError::PostgresQuery(source)
            | NativeError::PostgresTransaction { source, .. }
            if source
                .code()
                .is_some_and(|code| code.code().starts_with("23"))
    ) {
        return PostgresIntegrityError::new_err(error.to_string());
    }
    match error {
        error @ NativeError::InvalidTransactionIsolationLevel(_) => {
            PyValueError::new_err(error.to_string())
        }
        error => PyRuntimeError::new_err(error.to_string()),
    }
}
