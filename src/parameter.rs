use std::collections::HashMap;

use pyo3::prelude::*;
use tokio_postgres::types::{ToSql, Type};

use crate::error::NativeError;
use crate::placeholders::{rewrite_django_named_placeholders, rewrite_django_placeholders};

#[derive(Debug, FromPyObject)]
pub(crate) enum QueryParameter {
    #[pyo3(transparent)]
    Text(String),

    #[pyo3(transparent)]
    Integer(i64),

    #[pyo3(transparent)]
    Null(Option<String>),
}

#[derive(Debug, FromPyObject)]
pub(crate) enum QueryParameters {
    #[pyo3(transparent)]
    Named(HashMap<String, QueryParameter>),

    #[pyo3(transparent)]
    Positional(Vec<QueryParameter>),
}

impl Default for QueryParameters {
    fn default() -> Self {
        Self::Positional(Vec::new())
    }
}

impl QueryParameters {
    pub(crate) fn prepare(self, sql: &str) -> Result<(String, Vec<QueryParameter>), NativeError> {
        match self {
            Self::Positional(parameters) => {
                let sql = rewrite_django_placeholders(sql, parameters.len())?;
                Ok((sql, parameters))
            }
            Self::Named(mut parameters) => {
                let (sql, parameter_names) = rewrite_django_named_placeholders(sql);
                let mut ordered_parameters = Vec::with_capacity(parameter_names.len());

                for name in parameter_names {
                    let parameter = parameters
                        .remove(&name)
                        .ok_or_else(|| NativeError::MissingNamedParameter(name.clone()))?;
                    ordered_parameters.push(parameter);
                }

                Ok((sql, ordered_parameters))
            }
        }
    }
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
