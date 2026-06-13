use mantissa_client::config::ClientConfig;
use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

const DEFAULT_BIND_PORT: u16 = 6579;
const ENV_BIND_ADDR: &str = "MANTISSA_REST_ADDR";
const ENV_SOCKET: &str = "MANTISSA_REST_SOCKET";

/// Runtime configuration for the local REST gateway.
#[derive(Clone, Debug)]
pub struct RestConfig {
    pub bind_addr: SocketAddr,
    pub socket: Option<PathBuf>,
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

        Ok(Self { bind_addr, socket })
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
        }
    }
}

impl std::error::Error for RestConfigError {}
