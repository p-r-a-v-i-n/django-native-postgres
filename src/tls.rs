use crate::error::NativeError;
use tokio_postgres::config::SslMode as PostgresSslMode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SslMode {
    Disable,
    Prefer,
    Require,
}

impl SslMode {
    const DISABLE: &'static str = "disable";
    const PREFER: &'static str = "prefer";
    const REQUIRE: &'static str = "require";

    pub(crate) fn parse(value: String) -> Result<Self, NativeError> {
        match value.as_str() {
            Self::DISABLE => Ok(Self::Disable),
            Self::PREFER => Ok(Self::Prefer),
            Self::REQUIRE => Ok(Self::Require),
            _ => Err(NativeError::InvalidSslMode(value)),
        }
    }

    pub(crate) fn postgres_mode(self) -> PostgresSslMode {
        match self {
            Self::Disable => PostgresSslMode::Disable,
            Self::Prefer => PostgresSslMode::Prefer,
            Self::Require => PostgresSslMode::Require,
        }
    }
}
