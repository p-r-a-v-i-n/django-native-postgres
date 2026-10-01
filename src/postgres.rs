use crate::error::NativeError;
use crate::parameter::QueryParameter;
use crate::placeholders::rewrite_django_placeholders;
use pyo3::prelude::*;
use tokio_postgres::NoTls;
use tokio_postgres::types::{ToSql, Type};

#[derive(Debug, IntoPyObject)]
pub(crate) enum QueryValue {
    #[pyo3(transparent)]
    Text(String),

    #[pyo3(transparent)]
    Integer(i64),
}

pub(crate) type QueryRows = Vec<Vec<Option<QueryValue>>>;

pub(crate) async fn execute(
    database_url: &str,
    sql: &str,
    params: &[QueryParameter],
) -> Result<QueryRows, NativeError> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .map_err(NativeError::PostgresConnect)?;

    let _connection_task = tokio::spawn(connection);

    let postgres_params: Vec<&(dyn ToSql + Sync)> =
        params.iter().map(QueryParameter::as_postgres).collect();

    let postgres_sql = rewrite_django_placeholders(sql, params.len())?;

    let rows = client
        .query(postgres_sql.as_str(), &postgres_params)
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
