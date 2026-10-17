use crate::error::NativeError;
use rustls::{ClientConfig, RootCertStore};
use std::fs::File;
use std::io::BufReader;
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
    pub(crate) fn new(ssl_mode: SslMode, ssl_root_cert: Option<&str>) -> Result<Self, NativeError> {
        match ssl_mode {
            SslMode::Disable => Ok(Self::Disabled),
            SslMode::Prefer | SslMode::Require => {
                let connector = match ssl_root_cert {
                    Some(path) => connector_with_root_cert(path)?,
                    None => {
                        let (connector, _certificate_errors) =
                            MakeRustlsConnect::with_native_certs().map_err(|errors| {
                                NativeError::TlsConfiguration(format!(
                                    "could not load native root certificates: {errors:?}"
                                ))
                            })?;
                        connector
                    }
                };
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

fn connector_with_root_cert(path: &str) -> Result<MakeRustlsConnect, NativeError> {
    let file = File::open(path).map_err(|error| {
        NativeError::TlsConfiguration(format!(
            "could not open root certificate file {path:?}: {error}"
        ))
    })?;
    let mut reader = BufReader::new(file);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            NativeError::TlsConfiguration(format!(
                "could not read root certificate file {path:?}: {error}"
            ))
        })?;

    if certificates.is_empty() {
        return Err(NativeError::TlsConfiguration(format!(
            "root certificate file {path:?} contains no certificates"
        )));
    }

    let mut roots = RootCertStore::empty();
    let (valid_certificates, _) = roots.add_parsable_certificates(certificates);
    if valid_certificates == 0 {
        return Err(NativeError::TlsConfiguration(format!(
            "root certificate file {path:?} contains no valid certificates"
        )));
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(MakeRustlsConnect::new(config))
}
