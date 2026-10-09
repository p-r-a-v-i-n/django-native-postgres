use crate::error::NativeError;
use crate::parameter::QueryParameters;
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod};
use futures_util::TryStreamExt;
use pyo3::prelude::*;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_postgres::error::Severity;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{CancelToken, GenericClient, NoTls};

#[derive(Debug, IntoPyObject)]
pub(crate) enum QueryValue {
    #[pyo3(transparent)]
    Text(String),

    #[pyo3(transparent)]
    Integer(i64),
}

pub(crate) type QueryRows = Vec<Vec<Option<QueryValue>>>;
pub(crate) type QueryColumns = Vec<String>;

pub(crate) struct QueryResult {
    pub(crate) rows: QueryRows,
    pub(crate) rows_affected: u64,
    pub(crate) columns: QueryColumns,
}

fn query_error_allows_connection_reuse(error: &tokio_postgres::Error) -> bool {
    error
        .as_db_error()
        .and_then(|error| error.parsed_severity())
        == Some(Severity::Error)
}

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
    cancel_token: CancelToken,
    sql: &str,
    params: QueryParameters,
    cancellation: &mut oneshot::Receiver<()>,
) -> Result<QueryResult, NativeError>
where
    C: GenericClient + Sync,
{
    let (postgres_sql, params) = params.prepare(sql)?;
    let postgres_params: Vec<(&(dyn ToSql + Sync), Type)> = params
        .iter()
        .map(|parameter| (parameter.as_postgres(), parameter.postgres_type()))
        .collect();

    let query = client.query_typed_raw(
        postgres_sql.as_str(),
        postgres_params
            .iter()
            .map(|(value, postgres_type)| (*value, postgres_type.clone())),
    );
    tokio::pin!(query);

    let rows = tokio::select! {
        biased;
        result = &mut query => {
            result.map_err(NativeError::PostgresQuery)?
        }
        _ = &mut *cancellation => {
            cancel_token
                .cancel_query(NoTls)
                .await
                .map_err(NativeError::PostgresCancel)?;

            // PostgreSQL sends the cancellation result through the original
            // connection. Drain it before that connection can be reused.
            let connection_can_be_reused = match query.await {
                Ok(rows) => {
                    tokio::pin!(rows);
                    loop {
                        match rows.try_next().await {
                            Ok(Some(_)) => continue,
                            Ok(None) => break true,
                            Err(error) => break query_error_allows_connection_reuse(&error),
                        }
                    }
                }
                Err(error) => query_error_allows_connection_reuse(&error),
            };
            return Err(NativeError::QueryCancelled {
                connection_can_be_reused,
            });
        }
    };

    tokio::pin!(rows);
    let mut postgres_rows = Vec::new();

    loop {
        let row = tokio::select! {
            biased;
            result = rows.try_next() => {
                result.map_err(NativeError::PostgresQuery)?
            }
            _ = &mut *cancellation => {
                cancel_token
                    .cancel_query(NoTls)
                    .await
                    .map_err(NativeError::PostgresCancel)?;

                let connection_can_be_reused = loop {
                    match rows.try_next().await {
                        Ok(Some(_)) => continue,
                        Ok(None) => break true,
                        Err(error) => break query_error_allows_connection_reuse(&error),
                    }
                };
                return Err(NativeError::QueryCancelled {
                    connection_can_be_reused,
                });
            }
        };

        match row {
            Some(row) => postgres_rows.push(row),
            None => break,
        }
    }

    let rows_affected = rows.rows_affected().unwrap_or(0);
    let columns = postgres_rows
        .first()
        .map(|row| {
            row.columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect()
        })
        .unwrap_or_default();

    let mut decoded_rows = Vec::with_capacity(postgres_rows.len());

    for row in postgres_rows {
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

    Ok(QueryResult {
        rows: decoded_rows,
        rows_affected,
        columns,
    })
}

pub(crate) async fn execute(
    pool: &Pool,
    sql: &str,
    params: QueryParameters,
    cancellation: &mut oneshot::Receiver<()>,
) -> Result<QueryResult, NativeError> {
    let client = tokio::select! {
        result = pool.get() => {
            result.map_err(|error| NativeError::PostgresPoolAcquire(error.to_string()))?
        }
        _ = &mut *cancellation => {
            return Err(NativeError::QueryCancelled {
                connection_can_be_reused: true,
            });
        }
    };

    let cancel_token = client.cancel_token();
    let result = execute_on_client(&**client, cancel_token, sql, params, cancellation).await;

    let connection_can_be_reused = !client.is_closed()
        && match &result {
            Err(NativeError::PostgresCancel(_)) => false,
            Err(NativeError::PostgresQuery(error)) => query_error_allows_connection_reuse(error),
            Err(NativeError::QueryCancelled {
                connection_can_be_reused,
            }) => *connection_can_be_reused,
            _ => true,
        };

    if !connection_can_be_reused {
        drop(Object::take(client));
    }

    result
}
