use crate::error::NativeError;
use tokio_postgres::IsolationLevel;

#[derive(Clone, Copy)]
enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl TransactionIsolationLevel {
    const READ_UNCOMMITTED: &'static str = "read_uncommitted";
    const READ_COMMITTED: &'static str = "read_committed";
    const REPEATABLE_READ: &'static str = "repeatable_read";
    const SERIALIZABLE: &'static str = "serializable";

    fn parse(value: &str) -> Option<Self> {
        match value {
            Self::READ_UNCOMMITTED => Some(Self::ReadUncommitted),
            Self::READ_COMMITTED => Some(Self::ReadCommitted),
            Self::REPEATABLE_READ => Some(Self::RepeatableRead),
            Self::SERIALIZABLE => Some(Self::Serializable),
            _ => None,
        }
    }
}

impl From<TransactionIsolationLevel> for IsolationLevel {
    fn from(value: TransactionIsolationLevel) -> Self {
        match value {
            TransactionIsolationLevel::ReadUncommitted => Self::ReadUncommitted,
            TransactionIsolationLevel::ReadCommitted => Self::ReadCommitted,
            TransactionIsolationLevel::RepeatableRead => Self::RepeatableRead,
            TransactionIsolationLevel::Serializable => Self::Serializable,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TransactionOptions {
    isolation_level: Option<TransactionIsolationLevel>,
    read_only: Option<bool>,
    deferrable: Option<bool>,
}

impl TransactionOptions {
    pub(crate) fn new(
        isolation_level: Option<String>,
        read_only: Option<bool>,
        deferrable: Option<bool>,
    ) -> Result<Self, NativeError> {
        let isolation_level = isolation_level
            .map(|value| {
                TransactionIsolationLevel::parse(&value)
                    .ok_or(NativeError::InvalidTransactionIsolationLevel(value))
            })
            .transpose()?;

        Ok(Self {
            isolation_level,
            read_only,
            deferrable,
        })
    }

    pub(crate) fn isolation_level(&self) -> Option<IsolationLevel> {
        self.isolation_level.map(Into::into)
    }

    pub(crate) fn read_only(&self) -> Option<bool> {
        self.read_only
    }

    pub(crate) fn deferrable(&self) -> Option<bool> {
        self.deferrable
    }
}
