#[derive(Debug)]
pub(crate) enum NativeError {
    CommandChannelClosed,
    ResponseChannelClosed,
    PostgresConnect(tokio_postgres::Error),
    PostgresPoolBuild(String),
    PostgresPoolAcquire(String),
    PostgresQuery(tokio_postgres::Error),
    PostgresTransaction {
        operation: &'static str,
        source: tokio_postgres::Error,
    },
    PostgresDecode {
        column: usize,
        source: tokio_postgres::Error,
    },
    PlaceholderCountMismatch(crate::placeholders::PlaceholderCountMismatch),
    UnsupportedPostgresType {
        column: usize,
        type_name: String,
    },
    InvalidPoolMaxSize,
    InvalidPoolWaitTimeout,
    PoolHandleClosed,
    TransactionHandleClosed,
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
            Self::PostgresPoolBuild(error) => {
                write!(
                    formatter,
                    "failed to build PostgreSQL connection pool: {error}"
                )
            }
            Self::PostgresPoolAcquire(error) => {
                write!(
                    formatter,
                    "failed to acquire PostgreSQL connection: {error}"
                )
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
            Self::PlaceholderCountMismatch(error) => error.fmt(formatter),
            Self::UnsupportedPostgresType { column, type_name } => {
                write!(
                    formatter,
                    "unsupported PostgreSQL type {type_name} at column {column}"
                )
            }
            Self::InvalidPoolMaxSize => {
                write!(
                    formatter,
                    "PostgreSQL pool max size must be greater than zero"
                )
            }
            Self::InvalidPoolWaitTimeout => {
                write!(
                    formatter,
                    "PostgreSQL pool wait timeout must be a positive integer"
                )
            }
            Self::PoolHandleClosed => {
                write!(formatter, "PostgreSQL pool handle is closed")
            }
            Self::PostgresTransaction { operation, source } => {
                write! {
                    formatter,
                    "failed to {operation} PostgreSQL transaction: {source}"
                }
            }
            Self::TransactionHandleClosed => {
                write!(formatter, "PostgreSQL transaction handle is closed")
            }
        }
    }
}

impl std::error::Error for NativeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PostgresConnect(error) | Self::PostgresQuery(error) => Some(error),
            Self::PostgresTransaction { source, .. } | Self::PostgresDecode { source, .. } => {
                Some(source)
            }
            Self::PlaceholderCountMismatch(error) => Some(error),
            _ => None,
        }
    }
}

impl From<crate::placeholders::PlaceholderCountMismatch> for NativeError {
    fn from(error: crate::placeholders::PlaceholderCountMismatch) -> Self {
        Self::PlaceholderCountMismatch(error)
    }
}
