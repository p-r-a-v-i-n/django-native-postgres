use pyo3::prelude::*;
use tokio_postgres::types::ToSql;

#[derive(Debug, FromPyObject)]
pub(crate) enum QueryParameter {
    #[pyo3(transparent)]
    Text(String),

    #[pyo3(transparent)]
    Integer(i64),
}

impl QueryParameter {
    pub(crate) fn as_postgres(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Text(value) => value,
            Self::Integer(value) => value,
        }
    }
}
