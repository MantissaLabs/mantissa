use mantissa_client::config::ClientConfig;
use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

const DEFAULT_BIND_PORT: u16 = 6579;
const ENV_BIND_ADDR: &str = "MANTISSA_REST_ADDR";
const ENV_TLS_CERT: &str = "MANTISSA_REST_TLS_CERT";
const ENV_TLS_KEY: &str = "MANTISSA_REST_TLS_KEY";
const ENV_CLIENT_CA: &str = "MANTISSA_REST_CLIENT_CA";
const ENV_CLIENT_CERT_SHA256: &str = "MANTISSA_REST_CLIENT_CERT_SHA256";

/// Runtime configuration for the local REST gateway.
#[derive(Clone, Debug)]
pub struct RestConfig {
    pub bind_addr: SocketAddr,
    pub socket: Option<PathBuf>,
    pub tls: RestTlsConfig,
}

/// TLS file paths used by the REST listener.
///
/// Server certificate and key are optional for loopback-only HTTP. A configured
/// client CA enables mTLS and is mandatory for non-loopback direct binds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RestTlsConfig {
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub client_ca_path: Option<PathBuf>,
    pub client_cert_sha256: Vec<String>,
}

impl RestConfig {
    /// Builds REST configuration from environment variables and defaults.
    pub fn from_env() -> Result<Self, RestConfigError> {
        let config = Self::from_env_unvalidated()?;
        config.validate()?;
        Ok(config)
    }

    /// Builds REST configuration from environment variables without validating it.
    pub fn from_env_unvalidated() -> Result<Self, RestConfigError> {
        let bind_addr = match env::var(ENV_BIND_ADDR) {
            Ok(value) => value
                .parse()
                .map_err(|source| RestConfigError::InvalidBindAddr { value, source })?,
            Err(env::VarError::NotPresent) => default_bind_addr(),
            Err(env::VarError::NotUnicode(_)) => {
                return Err(RestConfigError::InvalidEnvUnicode {
                    name: ENV_BIND_ADDR,
                });
            }
        };

        Ok(Self {
            bind_addr,
            socket: None,
            tls: RestTlsConfig {
                cert_path: read_optional_path_env(ENV_TLS_CERT)?,
                key_path: read_optional_path_env(ENV_TLS_KEY)?,
                client_ca_path: read_optional_path_env(ENV_CLIENT_CA)?,
                client_cert_sha256: read_optional_list_env(ENV_CLIENT_CERT_SHA256)?,
            },
        })
    }

    /// Converts REST configuration into the local Mantissa client config.
    pub fn client_config(&self) -> ClientConfig {
        ClientConfig {
            socket: self.socket.clone(),
            ..ClientConfig::default()
        }
    }

    /// Validates REST listener configuration before binding.
    pub fn validate(&self) -> Result<(), RestConfigError> {
        self.tls.validate()?;
        if !self.bind_addr.ip().is_loopback() {
            if !self.tls.server_tls_enabled() {
                return Err(RestConfigError::NonLoopbackRequiresTls {
                    bind_addr: self.bind_addr,
                });
            }
            if !self.tls.client_cert_required() {
                return Err(RestConfigError::NonLoopbackRequiresClientCa {
                    bind_addr: self.bind_addr,
                });
            }
        }
        Ok(())
    }

    /// Returns the URL scheme used by this REST listener.
    pub fn scheme(&self) -> &'static str {
        if self.tls.server_tls_enabled() {
            "https"
        } else {
            "http"
        }
    }
}

impl RestTlsConfig {
    /// Returns true when a server certificate/key pair is configured.
    pub fn server_tls_enabled(&self) -> bool {
        self.cert_path.is_some() && self.key_path.is_some()
    }

    /// Returns true when TLS client certificates are required.
    pub fn client_cert_required(&self) -> bool {
        self.client_ca_path.is_some()
    }

    /// Validates the local consistency of TLS path settings.
    fn validate(&self) -> Result<(), RestConfigError> {
        match (&self.cert_path, &self.key_path) {
            (Some(_), Some(_)) | (None, None) => {}
            (Some(cert_path), None) => {
                return Err(RestConfigError::TlsCertWithoutKey {
                    cert_path: cert_path.clone(),
                });
            }
            (None, Some(key_path)) => {
                return Err(RestConfigError::TlsKeyWithoutCert {
                    key_path: key_path.clone(),
                });
            }
        }

        if self.client_ca_path.is_some() && !self.server_tls_enabled() {
            return Err(RestConfigError::ClientCaWithoutTls);
        }
        if !self.client_cert_sha256.is_empty() && !self.client_cert_required() {
            return Err(RestConfigError::ClientCertFingerprintWithoutClientCa);
        }
        for value in &self.client_cert_sha256 {
            normalize_client_cert_sha256(value)?;
        }

        Ok(())
    }
}

/// Returns the loopback bind address used when no environment override exists.
fn default_bind_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_BIND_PORT)
}

/// Reads one optional path environment variable.
fn read_optional_path_env(name: &'static str) -> Result<Option<PathBuf>, RestConfigError> {
    match env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Path::new(trimmed).to_path_buf()))
            }
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(RestConfigError::InvalidEnvUnicode { name }),
    }
}

/// Reads one comma-separated optional environment variable list.
fn read_optional_list_env(name: &'static str) -> Result<Vec<String>, RestConfigError> {
    match env::var(name) {
        Ok(value) => Ok(value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect()),
        Err(env::VarError::NotPresent) => Ok(Vec::new()),
        Err(env::VarError::NotUnicode(_)) => Err(RestConfigError::InvalidEnvUnicode { name }),
    }
}

/// Normalizes one configured client certificate SHA-256 fingerprint.
pub fn normalize_client_cert_sha256(raw: &str) -> Result<String, RestConfigError> {
    let trimmed = raw.trim();
    let without_prefix = trimmed
        .strip_prefix("sha256:")
        .or_else(|| trimmed.strip_prefix("SHA256:"))
        .unwrap_or(trimmed);
    let normalized: String = without_prefix
        .chars()
        .filter(|character| *character != ':')
        .flat_map(char::to_lowercase)
        .collect();
    if normalized.len() != 64 {
        return Err(RestConfigError::InvalidClientCertFingerprint {
            value: raw.to_string(),
            reason: "expected 64 hexadecimal SHA-256 characters".to_string(),
        });
    }
    if !normalized.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(RestConfigError::InvalidClientCertFingerprint {
            value: raw.to_string(),
            reason: "fingerprint must contain only hexadecimal characters".to_string(),
        });
    }
    Ok(normalized)
}

/// Configuration errors detected before the REST listener starts.
#[derive(Debug)]
pub enum RestConfigError {
    InvalidBindAddr {
        value: String,
        source: std::net::AddrParseError,
    },
    InvalidEnvUnicode {
        name: &'static str,
    },
    TlsCertWithoutKey {
        cert_path: PathBuf,
    },
    TlsKeyWithoutCert {
        key_path: PathBuf,
    },
    ClientCaWithoutTls,
    ClientCertFingerprintWithoutClientCa,
    InvalidClientCertFingerprint {
        value: String,
        reason: String,
    },
    NonLoopbackRequiresTls {
        bind_addr: SocketAddr,
    },
    NonLoopbackRequiresClientCa {
        bind_addr: SocketAddr,
    },
}

impl std::fmt::Display for RestConfigError {
    /// Formats configuration errors for CLI startup output.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBindAddr { value, source } => {
                write!(
                    formatter,
                    "invalid {ENV_BIND_ADDR} value '{value}': {source}"
                )
            }
            Self::InvalidEnvUnicode { name } => {
                write!(
                    formatter,
                    "REST environment variable {name} contains non-Unicode data"
                )
            }
            Self::TlsCertWithoutKey { cert_path } => {
                write!(
                    formatter,
                    "REST TLS certificate {} requires --rest-tls-key or {ENV_TLS_KEY}",
                    cert_path.display()
                )
            }
            Self::TlsKeyWithoutCert { key_path } => {
                write!(
                    formatter,
                    "REST TLS key {} requires --rest-tls-cert or {ENV_TLS_CERT}",
                    key_path.display()
                )
            }
            Self::ClientCaWithoutTls => {
                write!(
                    formatter,
                    "REST client CA requires server TLS certificate and key"
                )
            }
            Self::ClientCertFingerprintWithoutClientCa => {
                write!(
                    formatter,
                    "REST client certificate fingerprints require --rest-client-ca or {ENV_CLIENT_CA}"
                )
            }
            Self::InvalidClientCertFingerprint { value, reason } => {
                write!(
                    formatter,
                    "invalid REST client certificate SHA-256 fingerprint '{value}': {reason}"
                )
            }
            Self::NonLoopbackRequiresTls { bind_addr } => {
                write!(
                    formatter,
                    "REST bind address {bind_addr} is not loopback; configure --rest-tls-cert and --rest-tls-key"
                )
            }
            Self::NonLoopbackRequiresClientCa { bind_addr } => {
                write!(
                    formatter,
                    "REST bind address {bind_addr} is not loopback; configure --rest-client-ca to require mTLS"
                )
            }
        }
    }
}

impl std::error::Error for RestConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a REST config with loopback defaults and no TLS paths.
    fn config() -> RestConfig {
        RestConfig {
            bind_addr: "127.0.0.1:6579".parse().unwrap(),
            socket: None,
            tls: RestTlsConfig::default(),
        }
    }

    #[test]
    fn loopback_allows_plain_http() {
        let config = config();

        config.validate().unwrap();
        assert_eq!(config.scheme(), "http");
    }

    #[test]
    fn loopback_allows_tls_without_client_ca() {
        let mut config = config();
        config.tls.cert_path = Some("/tmp/rest.crt".into());
        config.tls.key_path = Some("/tmp/rest.key".into());

        config.validate().unwrap();
        assert_eq!(config.scheme(), "https");
    }

    #[test]
    fn non_loopback_requires_tls_and_client_ca() {
        let mut config = config();
        config.bind_addr = "0.0.0.0:6579".parse().unwrap();

        assert!(matches!(
            config.validate(),
            Err(RestConfigError::NonLoopbackRequiresTls { .. })
        ));

        config.tls.cert_path = Some("/tmp/rest.crt".into());
        config.tls.key_path = Some("/tmp/rest.key".into());
        assert!(matches!(
            config.validate(),
            Err(RestConfigError::NonLoopbackRequiresClientCa { .. })
        ));

        config.tls.client_ca_path = Some("/tmp/rest-clients.pem".into());
        config.validate().unwrap();
    }

    #[test]
    fn accepts_normalized_client_certificate_fingerprints() {
        let mut config = config();
        config.tls.cert_path = Some("/tmp/rest.crt".into());
        config.tls.key_path = Some("/tmp/rest.key".into());
        config.tls.client_ca_path = Some("/tmp/rest-clients.pem".into());
        config.tls.client_cert_sha256 = vec![
            "sha256:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99".into(),
        ];

        config.validate().unwrap();
        assert_eq!(
            normalize_client_cert_sha256(&config.tls.client_cert_sha256[0]).unwrap(),
            "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
        );
    }

    #[test]
    fn rejects_incomplete_tls_pairs() {
        let mut config = config();
        config.tls.cert_path = Some("/tmp/rest.crt".into());
        assert!(matches!(
            config.validate(),
            Err(RestConfigError::TlsCertWithoutKey { .. })
        ));

        config.tls.cert_path = None;
        config.tls.key_path = Some("/tmp/rest.key".into());
        assert!(matches!(
            config.validate(),
            Err(RestConfigError::TlsKeyWithoutCert { .. })
        ));
    }

    #[test]
    fn rejects_client_ca_without_server_tls() {
        let mut config = config();
        config.tls.client_ca_path = Some("/tmp/rest-clients.pem".into());

        assert!(matches!(
            config.validate(),
            Err(RestConfigError::ClientCaWithoutTls)
        ));
    }

    #[test]
    fn rejects_client_certificate_fingerprints_without_client_ca() {
        let mut config = config();
        config.tls.cert_path = Some("/tmp/rest.crt".into());
        config.tls.key_path = Some("/tmp/rest.key".into());
        config.tls.client_cert_sha256 = vec!["aabb".into()];

        assert!(matches!(
            config.validate(),
            Err(RestConfigError::ClientCertFingerprintWithoutClientCa)
        ));
    }

    #[test]
    fn rejects_invalid_client_certificate_fingerprint_format() {
        let mut config = config();
        config.tls.cert_path = Some("/tmp/rest.crt".into());
        config.tls.key_path = Some("/tmp/rest.key".into());
        config.tls.client_ca_path = Some("/tmp/rest-clients.pem".into());
        config.tls.client_cert_sha256 = vec!["not-a-sha256".into()];

        assert!(matches!(
            config.validate(),
            Err(RestConfigError::InvalidClientCertFingerprint { .. })
        ));
    }
}
