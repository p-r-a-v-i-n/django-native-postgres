//! Minimal native packaging probe for the project foundation.

use crate::error::NativeError;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

pyo3::create_exception!(_native, PostgresDatabaseError, PyRuntimeError);
pyo3::create_exception!(_native, PostgresIntegrityError, PostgresDatabaseError);
pyo3::create_exception!(_native, PostgresDataError, PyRuntimeError);
pyo3::create_exception!(_native, PostgresInterfaceError, PyRuntimeError);
pyo3::create_exception!(_native, PostgresNotSupportedError, PyRuntimeError);
pyo3::create_exception!(_native, PostgresOperationalError, PyRuntimeError);
pyo3::create_exception!(_native, PostgresProgrammingError, PyRuntimeError);

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
    params: Option<parameter::QueryParameters>,
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
    params: Option<parameter::QueryParameters>,
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
    params: Option<parameter::QueryParameters>,
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
    params: Option<parameter::QueryParameters>,
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
    params: Option<parameter::QueryParameters>,
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
    params: Option<parameter::QueryParameters>,
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
        "PostgresDatabaseError",
        module.py().get_type::<PostgresDatabaseError>(),
    )?;
    module.add(
        "PostgresIntegrityError",
        module.py().get_type::<PostgresIntegrityError>(),
    )?;
    module.add(
        "PostgresDataError",
        module.py().get_type::<PostgresDataError>(),
    )?;
    module.add(
        "PostgresInterfaceError",
        module.py().get_type::<PostgresInterfaceError>(),
    )?;
    module.add(
        "PostgresNotSupportedError",
        module.py().get_type::<PostgresNotSupportedError>(),
    )?;
    module.add(
        "PostgresOperationalError",
        module.py().get_type::<PostgresOperationalError>(),
    )?;
    module.add(
        "PostgresProgrammingError",
        module.py().get_type::<PostgresProgrammingError>(),
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

fn postgres_error_source(error: &NativeError) -> Option<&tokio_postgres::Error> {
    match error {
        NativeError::PostgresConnect(source)
        | NativeError::PostgresQuery(source)
        | NativeError::PostgresCancel(source)
        | NativeError::PostgresTransaction { source, .. }
        | NativeError::PostgresDecode { source, .. } => Some(source),
        _ => None,
    }
}

fn postgres_database_error_message(
    error: &NativeError,
    database_error: &tokio_postgres::error::DbError,
) -> String {
    let message = database_error.message();

    match error {
        NativeError::PostgresConnect(_) => {
            format!("failed to connect to PostgreSQL: {message}")
        }
        NativeError::PostgresQuery(_) => format!("PostgreSQL query failed: {message}"),
        NativeError::PostgresCancel(_) => {
            format!("failed to cancel PostgreSQL query: {message}")
        }
        NativeError::PostgresTransaction { operation, .. } => {
            format!("failed to {operation} PostgreSQL transaction: {message}")
        }
        NativeError::PostgresDecode { column, .. } => {
            format!("failed to decode PostgreSQL column {column}: {message}")
        }
        _ => message.to_owned(),
    }
}

fn new_postgres_database_error(
    error: &NativeError,
    database_error: &tokio_postgres::error::DbError,
) -> PyErr {
    let message = postgres_database_error_message(error, database_error);
    let sqlstate = database_error.code().code().to_owned();
    let severity = database_error.severity().to_owned();
    let message_primary = database_error.message().to_owned();
    let detail = database_error.detail().map(str::to_owned);
    let hint = database_error.hint().map(str::to_owned);
    let schema_name = database_error.schema().map(str::to_owned);
    let table_name = database_error.table().map(str::to_owned);
    let column_name = database_error.column().map(str::to_owned);
    let datatype_name = database_error.datatype().map(str::to_owned);
    let constraint_name = database_error.constraint().map(str::to_owned);
    let is_integrity_error = sqlstate.starts_with("23");

    Python::attach(move |python| {
        let error = if is_integrity_error {
            PostgresIntegrityError::new_err(message)
        } else {
            PostgresDatabaseError::new_err(message)
        };

        let value = error.value(python);
        let attributes = [
            value.setattr("sqlstate", sqlstate),
            value.setattr("severity", severity),
            value.setattr("message_primary", message_primary),
            value.setattr("detail", detail),
            value.setattr("hint", hint),
            value.setattr("schema_name", schema_name),
            value.setattr("table_name", table_name),
            value.setattr("column_name", column_name),
            value.setattr("datatype_name", datatype_name),
            value.setattr("constraint_name", constraint_name),
        ];

        for result in attributes {
            if let Err(attribute_error) = result {
                return attribute_error;
            }
        }

        error
    })
}

fn to_python_error(error: NativeError) -> PyErr {
    if let Some(source) = postgres_error_source(&error) {
        if let Some(database_error) = source.as_db_error() {
            return new_postgres_database_error(&error, database_error);
        }
    }

    match error {
        error @ (NativeError::PostgresConnect(_)
        | NativeError::PostgresPoolBuild(_)
        | NativeError::PostgresPoolAcquire(_)
        | NativeError::PostgresQuery(_)
        | NativeError::PostgresCancel(_)
        | NativeError::PostgresTransaction { .. }
        | NativeError::TransactionStartCancelled
        | NativeError::QueryCancelled { .. }) => {
            PostgresOperationalError::new_err(error.to_string())
        }
        error @ (NativeError::CommandChannelClosed
        | NativeError::ResponseChannelClosed
        | NativeError::PoolHandleClosed
        | NativeError::TransactionHandleClosed
        | NativeError::TransactionHandleProcessMismatch) => {
            PostgresInterfaceError::new_err(error.to_string())
        }
        error @ NativeError::PostgresDecode { .. } => PostgresDataError::new_err(error.to_string()),
        error @ (NativeError::PlaceholderCountMismatch(_)
        | NativeError::MissingNamedParameter(_)) => {
            PostgresProgrammingError::new_err(error.to_string())
        }
        error @ NativeError::UnsupportedPostgresType { .. } => {
            PostgresNotSupportedError::new_err(error.to_string())
        }
        error @ NativeError::InvalidTransactionIsolationLevel(_) => {
            PyValueError::new_err(error.to_string())
        }
        error => PyRuntimeError::new_err(error.to_string()),
    }
}
