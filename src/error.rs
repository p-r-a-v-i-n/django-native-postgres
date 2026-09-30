#[derive(Debug)]
pub(crate) enum NativeError {
    CommandChannelClosed,
    ResponseChannelClosed,
    PostgresConnect(tokio_postgres::Error),
    PostgresQuery(tokio_postgres::Error),
    PostgresDecode {
        column: usize,
        source: tokio_postgres::Error,
    },
}

impl std::fmt::Display for NativeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommandChannelClosed => {
                write!(formatter, "Tokio runtime command channel closed")
            }
            Self::ResponseChannelClosed => {
                write!(formatter, "Tokio runtime response channel closed")
            }
            Self::PostgresConnect(error) => {
                write!(formatter, "failed to connect to PostgreSQL: {error}")
            }
            Self::PostgresQuery(error) => {
                write!(formatter, "PostgreSQL query failed: {error}")
            }
            Self::PostgresDecode { column, source } => {
                write!(
                    formatter,
                    "failed to decode PostgreSQL column {column}: {source}"
                )
            }
        }
    }
}

impl std::error::Error for NativeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PostgresConnect(error) | Self::PostgresQuery(error) => Some(error),
            Self::PostgresDecode { source, .. } => Some(source),
            _ => None,
        }
    }
}
