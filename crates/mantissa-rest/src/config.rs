use crate::auth::RestAuthConfig;
use mantissa_client::config::ClientConfig;
use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

const DEFAULT_BIND_PORT: u16 = 6579;
const ENV_BIND_ADDR: &str = "MANTISSA_REST_ADDR";
const ENV_SOCKET: &str = "MANTISSA_REST_SOCKET";
const ENV_TOKEN: &str = "MANTISSA_REST_TOKEN";
const ENV_INSECURE_NO_AUTH: &str = "MANTISSA_REST_INSECURE_NO_AUTH";

/// Runtime configuration for the local REST gateway.
#[derive(Clone, Debug)]
pub struct RestConfig {
    pub bind_addr: SocketAddr,
    pub socket: Option<PathBuf>,
    pub auth: RestAuthConfig,
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
            Err(env::VarError::NotUnicode(_)) => return Err(RestConfigError::InvalidEnvUnicode),
        };

        let socket = match env::var(ENV_SOCKET) {
            Ok(value) => Some(PathBuf::from(value)),
            Err(env::VarError::NotPresent) => None,
            Err(env::VarError::NotUnicode(_)) => return Err(RestConfigError::InvalidEnvUnicode),
        };

        let token = match env::var(ENV_TOKEN) {
            Ok(value) if !value.trim().is_empty() => Some(value),
            Ok(_) | Err(env::VarError::NotPresent) => None,
            Err(env::VarError::NotUnicode(_)) => return Err(RestConfigError::InvalidEnvUnicode),
        };

        let insecure_no_auth = match env::var(ENV_INSECURE_NO_AUTH) {
            Ok(value) => matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"),
            Err(env::VarError::NotPresent) => false,
            Err(env::VarError::NotUnicode(_)) => return Err(RestConfigError::InvalidEnvUnicode),
        };

        let auth = if insecure_no_auth {
            RestAuthConfig::Disabled
        } else {
            RestAuthConfig::Bearer { token }
        };

        Ok(Self {
            bind_addr,
            socket,
            auth,
        })
    }

    /// Converts REST configuration into the local Mantissa client config.
    pub fn client_config(&self) -> ClientConfig {
        ClientConfig {
            socket: self.socket.clone(),
            ..ClientConfig::default()
        }
    }

    /// Rejects configurations that would expose unauthenticated control routes.
    pub fn validate(&self) -> Result<(), RestConfigError> {
        if !self.bind_addr.ip().is_loopback() && !self.auth.has_bearer_token() {
            return Err(RestConfigError::NonLoopbackWithoutBearerToken {
                bind_addr: self.bind_addr,
            });
        }
        Ok(())
    }
}

/// Returns the loopback bind address used when no environment override exists.
fn default_bind_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_BIND_PORT)
}

/// Configuration errors detected before the REST listener starts.
#[derive(Debug)]
pub enum RestConfigError {
    InvalidBindAddr {
        value: String,
        source: std::net::AddrParseError,
    },
    InvalidEnvUnicode,
    NonLoopbackWithoutBearerToken {
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
            Self::InvalidEnvUnicode => {
                write!(formatter, "REST environment contains non-Unicode data")
            }
            Self::NonLoopbackWithoutBearerToken { bind_addr } => write!(
                formatter,
                "refusing to bind REST API to {bind_addr} without {ENV_TOKEN}"
            ),
        }
    }
}

impl std::error::Error for RestConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_loopback_without_token() {
        let config = RestConfig {
            bind_addr: default_bind_addr(),
            socket: None,
            auth: RestAuthConfig::Bearer { token: None },
        };

        config.validate().unwrap();
    }

    #[test]
    fn validate_rejects_non_loopback_without_token() {
        let config = RestConfig {
            bind_addr: "0.0.0.0:6579".parse().unwrap(),
            socket: None,
            auth: RestAuthConfig::Bearer { token: None },
        };

        assert!(matches!(
            config.validate(),
            Err(RestConfigError::NonLoopbackWithoutBearerToken { .. })
        ));
    }
}
