use crate::error::NativeError;
use crate::parameter::QueryParameter;
use crate::placeholders::rewrite_django_placeholders;
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod};
use pyo3::prelude::*;
use std::time::Duration;
use tokio_postgres::error::Severity;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{GenericClient, NoTls};

#[derive(Debug, IntoPyObject)]
pub(crate) enum QueryValue {
    #[pyo3(transparent)]
    Text(String),

    #[pyo3(transparent)]
    Integer(i64),
}

pub(crate) type QueryRows = Vec<Vec<Option<QueryValue>>>;

pub(crate) fn create_pool(
    database_url: &str,
    pool_max_size: usize,
    pool_wait_timeout_ms: u64,
) -> Result<Pool, NativeError> {
    let postgres_config = database_url.parse().map_err(NativeError::PostgresConnect)?;
    let manager = Manager::from_config(
        postgres_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );

    Pool::builder(manager)
        .max_size(pool_max_size)
        .wait_timeout(Some(Duration::from_millis(pool_wait_timeout_ms)))
        .runtime(deadpool_postgres::Runtime::Tokio1)
        .build()
        .map_err(|error| NativeError::PostgresPoolBuild(error.to_string()))
}

pub(crate) async fn execute_on_client<C>(
    client: &C,
    sql: &str,
    params: &[QueryParameter],
) -> Result<QueryRows, NativeError>
where
    C: GenericClient + Sync,
{
    let postgres_params: Vec<(&(dyn ToSql + Sync), Type)> = params
        .iter()
        .map(|parameter| (parameter.as_postgres(), parameter.postgres_type()))
        .collect();

    let postgres_sql = rewrite_django_placeholders(sql, params.len())?;

    let rows = client
        .query_typed(postgres_sql.as_str(), &postgres_params)
        .await
        .map_err(NativeError::PostgresQuery)?;

    let mut decoded_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let mut decoded_row = Vec::with_capacity(row.len());

        for column in 0..row.len() {
            let column_type = row.columns()[column].type_();

            let value = if column_type == &Type::TEXT
                || column_type == &Type::VARCHAR
                || column_type == &Type::BPCHAR
                || column_type == &Type::NAME
            {
                row.try_get::<usize, Option<String>>(column)
                    .map(|value| value.map(QueryValue::Text))
                    .map_err(|source| NativeError::PostgresDecode { column, source })?
            } else if column_type == &Type::INT2 {
                row.try_get::<usize, Option<i16>>(column)
                    .map(|value| value.map(|value| QueryValue::Integer(i64::from(value))))
                    .map_err(|source| NativeError::PostgresDecode { column, source })?
            } else if column_type == &Type::INT4 {
                row.try_get::<usize, Option<i32>>(column)
                    .map(|value| value.map(|value| QueryValue::Integer(i64::from(value))))
                    .map_err(|source| NativeError::PostgresDecode { column, source })?
            } else if column_type == &Type::INT8 {
                row.try_get::<usize, Option<i64>>(column)
                    .map(|value| value.map(QueryValue::Integer))
                    .map_err(|source| NativeError::PostgresDecode { column, source })?
            } else {
                return Err(NativeError::UnsupportedPostgresType {
                    column,
                    type_name: column_type.name().to_string(),
                });
            };

            decoded_row.push(value);
        }

        decoded_rows.push(decoded_row);
    }

    Ok(decoded_rows)
}

pub(crate) async fn execute(
    pool: &Pool,
    sql: &str,
    params: &[QueryParameter],
) -> Result<QueryRows, NativeError> {
    let client = pool
        .get()
        .await
        .map_err(|error| NativeError::PostgresPoolAcquire(error.to_string()))?;

    let result = execute_on_client(&**client, sql, params).await;

    if let Err(NativeError::PostgresQuery(error)) = &result {
        let connection_can_be_reused = !client.is_closed()
            && error
                .as_db_error()
                .and_then(|error| error.parsed_severity())
                == Some(Severity::Error);
        if !connection_can_be_reused {
            drop(Object::take(client));
        }
    }
    result
}
