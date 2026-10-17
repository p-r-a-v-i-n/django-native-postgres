use crate::error::NativeError;
use tokio_postgres::config::SslMode as PostgresSslMode;
use tokio_postgres_rustls::MakeRustlsConnect;

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

#[derive(Clone)]
pub(crate) enum TlsConnector {
    Disabled,
    Rustls(MakeRustlsConnect),
}

impl TlsConnector {
    pub(crate) fn new(ssl_mode: SslMode) -> Result<Self, NativeError> {
        match ssl_mode {
            SslMode::Disable => Ok(Self::Disabled),
            SslMode::Prefer | SslMode::Require => {
                let (connector, _certificate_errors) = MakeRustlsConnect::with_native_certs()
                    .map_err(|errors| {
                        NativeError::TlsConfiguration(format!(
                            "could not load native root certificates: {errors:?}"
                        ))
                    })?;
                Ok(Self::Rustls(connector))
            }
        }
    }
    pub(crate) async fn cancel_query(
        &self,
        cancel_token: &tokio_postgres::CancelToken,
    ) -> Result<(), tokio_postgres::Error> {
        match self {
            Self::Disabled => cancel_token.cancel_query(tokio_postgres::NoTls).await,
            Self::Rustls(connector) => cancel_token.cancel_query(connector.clone()).await,
        }
    }
}
