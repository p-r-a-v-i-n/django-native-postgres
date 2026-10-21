use crate::error::NativeError;
use crate::parameter::QueryParameters;
use crate::tls::{SslMode, TlsConnector};
use deadpool_postgres::{Hook, HookError, Manager, ManagerConfig, Object, Pool, RecyclingMethod};
use futures_util::TryStreamExt;
use pyo3::prelude::*;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_postgres::error::{Severity, SqlState};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{CancelToken, GenericClient, NoTls};

#[derive(Clone)]
pub(crate) struct PostgresPool {
    pool: Pool,
    tls: TlsConnector,
}

impl PostgresPool {
    pub(crate) fn pool(&self) -> &Pool {
        &self.pool
    }

    pub(crate) fn tls(&self) -> &TlsConnector {
        &self.tls
    }

    pub(crate) fn close(&self) {
        self.pool.close();
    }
}

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

pub(crate) fn decode_row(row: tokio_postgres::Row) -> Result<Vec<Option<QueryValue>>, NativeError> {
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

    Ok(decoded_row)
}

pub(crate) fn query_error_allows_connection_reuse(error: &tokio_postgres::Error) -> bool {
    error
        .as_db_error()
        .and_then(|error| error.parsed_severity())
        == Some(Severity::Error)
}

pub(crate) fn query_error_confirms_cancellation(error: &tokio_postgres::Error) -> bool {
    error.code() == Some(&SqlState::QUERY_CANCELED)
}

pub(crate) fn create_pool(
    database_url: &str,
    pool_max_size: usize,
    pool_wait_timeout_ms: u64,
    ssl_mode: SslMode,
    ssl_root_cert: Option<&str>,
    time_zone: Option<&str>,
) -> Result<PostgresPool, NativeError> {
    let mut postgres_config: tokio_postgres::Config =
        database_url.parse().map_err(NativeError::PostgresConnect)?;
    postgres_config.ssl_mode(ssl_mode.postgres_mode());

    let tls = TlsConnector::new(ssl_mode, ssl_root_cert)?;
    let manager = match &tls {
        TlsConnector::Disabled => Manager::from_config(
            postgres_config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        ),
        TlsConnector::Rustls(connector) => Manager::from_config(
            postgres_config,
            connector.clone(),
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        ),
    };

    let mut pool_builder = Pool::builder(manager)
        .max_size(pool_max_size)
        .wait_timeout(Some(Duration::from_millis(pool_wait_timeout_ms)))
        .runtime(deadpool_postgres::Runtime::Tokio1);

    if let Some(time_zone) = time_zone {
        let time_zone = time_zone.to_owned();
        pool_builder = pool_builder.post_create(Hook::async_fn(move |client, _metrics| {
            let time_zone = time_zone.clone();
            Box::pin(async move {
                client
                    .query_one("SELECT set_config('TimeZone', $1, false)", &[&time_zone])
                    .await
                    .map(|_row| ())
                    .map_err(HookError::Backend)
            })
        }));
    }

    let pool = pool_builder
        .build()
        .map_err(|error| NativeError::PostgresPoolBuild(error.to_string()))?;

    Ok(PostgresPool { pool, tls })
}

pub(crate) async fn execute_on_client<C>(
    client: &C,
    cancel_token: CancelToken,
    tls: &TlsConnector,
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
            tls
                .cancel_query(&cancel_token)
                .await
                .map_err(NativeError::PostgresCancel)?;

            // PostgreSQL sends the cancellation result through the original
            // connection. Reuse it only when that result confirms the
            // cancellation reached this query rather than a later query.
            let connection_can_be_reused = match query.await {
                Ok(rows) => {
                    tokio::pin!(rows);
                    loop {
                        match rows.try_next().await {
                            Ok(Some(_)) => continue,
                            Ok(None) => break false,
                            Err(error) => break query_error_confirms_cancellation(&error),
                        }
                    }
                }
                Err(error) => query_error_confirms_cancellation(&error),
            };
            return Err(NativeError::QueryCancelled {
                connection_can_be_reused,
            });
        }
    };

    tokio::pin!(rows);
    let mut decoded_rows = Vec::new();
    let mut columns = None;
    let mut decode_error = None;

    loop {
        let row = tokio::select! {
            biased;
            result = rows.try_next() => {
                result.map_err(NativeError::PostgresQuery)?
            }
            _ = &mut *cancellation => {
                tls
                    .cancel_query(&cancel_token)
                    .await
                    .map_err(NativeError::PostgresCancel)?;

                let connection_can_be_reused = loop {
                    match rows.try_next().await {
                        Ok(Some(_)) => continue,
                        Ok(None) => break false,
                        Err(error) => break query_error_confirms_cancellation(&error),
                    }
                };
                return Err(NativeError::QueryCancelled {
                    connection_can_be_reused,
                });
            }
        };

        match row {
            Some(row) => {
                if columns.is_none() {
                    columns = Some(
                        row.columns()
                            .iter()
                            .map(|column| column.name().to_string())
                            .collect(),
                    );
                }

                if decode_error.is_none() {
                    match decode_row(row) {
                        Ok(row) => decoded_rows.push(row),
                        Err(error) => {
                            decoded_rows.clear();
                            decode_error = Some(error);
                        }
                    }
                }
            }
            None => break,
        }
    }

    let rows_affected = rows.rows_affected().unwrap_or(0);
    if let Some(error) = decode_error {
        return Err(error);
    }

    Ok(QueryResult {
        rows: decoded_rows,
        rows_affected,
        columns: columns.unwrap_or_default(),
    })
}

pub(crate) async fn execute(
    pool: &PostgresPool,
    sql: &str,
    params: QueryParameters,
    cancellation: &mut oneshot::Receiver<()>,
) -> Result<QueryResult, NativeError> {
    let client = tokio::select! {
        result = pool.pool().get() => {
            result.map_err(|error| NativeError::PostgresPoolAcquire(error.to_string()))?
        }
        _ = &mut *cancellation => {
            return Err(NativeError::QueryCancelled {
                connection_can_be_reused: true,
            });
        }
    };

    let cancel_token = client.cancel_token();
    let result = execute_on_client(
        &**client,
        cancel_token,
        pool.tls(),
        sql,
        params,
        cancellation,
    )
    .await;

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
