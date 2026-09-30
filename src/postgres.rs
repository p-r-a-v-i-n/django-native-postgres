use crate::error::NativeError;
use tokio_postgres::NoTls;

pub(crate) type TextRows = Vec<Vec<Option<String>>>;

pub(crate) async fn execute(database_url: &str, sql: &str) -> Result<TextRows, NativeError> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .map_err(NativeError::PostgresConnect)?;

    let _connection_task = tokio::spawn(connection);

    let rows = client
        .query(sql, &[])
        .await
        .map_err(NativeError::PostgresQuery)?;

    let mut decoded_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let mut decoded_row = Vec::with_capacity(row.len());

        for column in 0..row.len() {
            let value = row
                .try_get::<usize, Option<String>>(column)
                .map_err(|source| NativeError::PostgresDecode { column, source })?;

            decoded_row.push(value);
        }

        decoded_rows.push(decoded_row);
    }

    Ok(decoded_rows)
}
