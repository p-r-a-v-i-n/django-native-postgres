use pyo3::prelude::*;
use tokio_postgres::types::{ToSql, Type};

#[derive(Debug, FromPyObject)]
pub(crate) enum QueryParameter {
    #[pyo3(transparent)]
    Text(String),

    #[pyo3(transparent)]
    Integer(i64),

    #[pyo3(transparent)]
    Null(Option<String>),
}

impl QueryParameter {
    pub(crate) fn as_postgres(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Text(value) => value,
            Self::Integer(value) => value,
            Self::Null(value) => value,
        }
    }

    pub(crate) fn postgres_type(&self) -> Type {
        match self {
            Self::Text(_) => Type::TEXT,
            Self::Integer(_) => Type::INT8,
            Self::Null(_) => Type::UNKNOWN,
        }
    }
}
