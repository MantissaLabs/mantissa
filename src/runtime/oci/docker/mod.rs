//! # Docker Runtime Backend
//!
//! This module provides the Docker-backed implementation of the generic
//! runtime backend using the Bollard Docker API.

use std::env;
use std::future::Future;
use std::path::PathBuf;

use bollard::Docker;
use bollard::errors::Error as BollardError;
use log::info;

use crate::config;
use crate::runtime::types::{RuntimeError, RuntimeResult};

mod conversions;
mod credentials;
mod images;
mod interactive;
#[cfg(test)]
mod registry_tests;
mod runtime;
mod sandbox;
#[cfg(test)]
mod tests;

/// Label key used to persist workload ownership onto runtime instances.
pub(super) const WORKLOAD_ID_LABEL: &str = "mantissa.workload_id";

/// Default operator-facing profile name for standard OCI Docker workloads.
pub(super) const DOCKER_STANDARD_PROFILE: &str = "default";

/// Default operator-facing profile name for sandboxed OCI Docker workloads.
pub(super) const DOCKER_SANDBOXED_PROFILE: &str = "oci-default";

/// Explicit operator-facing profile alias for the future `nono`-backed sandbox contract.
pub(super) const DOCKER_NONO_PROFILE: &str = "nono-default";

/// Binary name used when Mantissa injects the sandbox init helper into containers.
pub const MANTISSA_SANDBOX_HELPER_BINARY_NAME: &str = "mantissa-sandbox-init";

/// Environment variable used to pass the serialized sandbox policy to the helper.
pub const MANTISSA_SANDBOX_POLICY_ENV_VAR: &str = "MANTISSA_SANDBOX_POLICY";

/// Environment variable that overrides the host-side path to the helper binary.
pub const MANTISSA_SANDBOX_HELPER_HOST_ENV_VAR: &str = "MANTISSA_SANDBOX_HELPER_PATH";

/// Container-local path where the helper binary is bind-mounted for sandboxed workloads.
pub(super) const MANTISSA_SANDBOX_HELPER_CONTAINER_PATH: &str = "/mantissa-sandbox-init";

/// Label that marks one container as running through the `nono` helper boundary.
pub(super) const MANTISSA_SANDBOX_ENABLED_LABEL: &str = "mantissa.sandbox.enabled";

/// Optional read-only roots commonly needed so sandboxed OCI images can start.
///
/// Some image layouts omit specific entries such as `/lib64`. The helper treats
/// these as optional bootstrap allowances and skips only the missing ones.
pub const NONO_EXEC_READONLY_DIRS: &[&str] = &[
    "/bin", "/sbin", "/usr", "/lib", "/lib64", "/etc", "/dev", "/proc",
];

/// Host-side availability decision for the `nono`-backed Docker sandbox contract.
#[derive(Clone, Debug, PartialEq, Eq)]
enum NonoSandboxBackendAvailability {
    /// The host can register the sandboxed Docker backend with the provided helper path.
    Available(PathBuf),
    /// The current host platform cannot run the `nono` sandbox contract.
    UnsupportedHost,
    /// The sandbox helper binary could not be resolved on an otherwise supported host.
    MissingHelper,
}

impl NonoSandboxBackendAvailability {
    /// Detects whether the current host can advertise the `nono` Docker sandbox contract.
    fn detect() -> Self {
        Self::from_parts(
            DockerRuntimeBackend::host_supports_nono_sandbox(),
            DockerRuntimeBackend::resolve_nono_helper_host_path(),
        )
    }

    /// Collapses host capability and helper discovery into one registration decision.
    fn from_parts(host_supported: bool, helper_host_path: Option<PathBuf>) -> Self {
        if !host_supported {
            return Self::UnsupportedHost;
        }

        match helper_host_path {
            Some(path) => Self::Available(path),
            None => Self::MissingHelper,
        }
    }

    /// Returns the helper path when the sandboxed Docker backend can be registered.
    fn helper_host_path(&self) -> Option<&PathBuf> {
        match self {
            Self::Available(path) => Some(path),
            Self::UnsupportedHost | Self::MissingHelper => None,
        }
    }

    /// Returns one operator-facing reason why the sandboxed Docker backend is unavailable.
    fn unavailable_reason(&self) -> Option<String> {
        match self {
            Self::Available(_) => None,
            Self::UnsupportedHost => {
                Some("sandboxed Docker backend requires a Linux or macOS host".to_string())
            }
            Self::MissingHelper => Some(format!(
                "sandboxed Docker backend requires helper binary {}; set {} or place it next to the mantissa executable",
                MANTISSA_SANDBOX_HELPER_BINARY_NAME, MANTISSA_SANDBOX_HELPER_HOST_ENV_VAR
            )),
        }
    }
}

/// Docker runtime backend implementation.
#[derive(Clone)]
pub struct DockerRuntimeBackend {
    docker: Docker,
    nono_helper_host_path: Option<PathBuf>,
    docker_config_path: Option<PathBuf>,
}

impl DockerRuntimeBackend {
    /// Connects one Docker daemon and enables sandbox contracts when its helper is available.
    pub async fn new() -> RuntimeResult<Self> {
        let (docker, endpoint) = Self::connect_verified().await?;
        let availability = NonoSandboxBackendAvailability::detect();
        let nono_helper_host_path = availability.helper_host_path().cloned();
        if let Some(reason) = availability.unavailable_reason() {
            info!(target: "task", "Docker sandbox support is unavailable: {reason}");
        }

        info!(target: "task", "Connected to Docker endpoint {endpoint}");
        if let Some(path) = &nono_helper_host_path {
            info!(target: "task", "Resolved Docker sandbox helper: {}", path.display());
        }

        Ok(Self {
            docker,
            nono_helper_host_path,
            docker_config_path: credentials::docker_config_path(),
        })
    }

    /// Returns whether the current host can run the Docker sandbox helper.
    fn host_supports_nono_sandbox() -> bool {
        cfg!(any(target_os = "linux", target_os = "macos"))
    }

    /// Connects to Docker and verifies the daemon is reachable before the backend is registered.
    async fn connect_verified() -> RuntimeResult<(Docker, String)> {
        let (docker, endpoint) =
            Self::connect().map_err(|err| RuntimeError::backend(None, err.to_string()))?;

        docker
            .ping()
            .await
            .map_err(|err| RuntimeError::OperationFailed(format!("docker ping failed: {err}")))?;

        Ok((docker, endpoint))
    }

    fn connect() -> Result<(Docker, String), bollard::errors::Error> {
        if let Some(host) = config::oci_runtime_host() {
            return Self::connect_with_host(&host).map(|docker| (docker, host));
        }

        if let Ok(host) = env::var("DOCKER_HOST") {
            return Self::connect_with_host(&host).map(|docker| (docker, host));
        }

        let docker = Docker::connect_with_defaults()?;
        Ok((docker, "(defaults)".to_string()))
    }

    fn connect_with_host(host: &str) -> Result<Docker, bollard::errors::Error> {
        if host.starts_with("tcp://") || host.starts_with("http://") {
            Docker::connect_with_http(host, 120, bollard::API_DEFAULT_VERSION)
        } else if host.starts_with("unix://") || host.starts_with('/') {
            Docker::connect_with_unix(host, 120, bollard::API_DEFAULT_VERSION)
        } else {
            Docker::connect_with_defaults()
        }
    }

    /// Executes one container-scoped Docker API call and normalizes not-found
    /// failures.
    async fn run_runtime_call<T, F>(&self, runtime_id: &str, call: F) -> RuntimeResult<T>
    where
        F: Future<Output = Result<T, BollardError>>,
    {
        call.await
            .map_err(|err| conversions::classify_runtime_error(runtime_id, err))
    }

    /// Executes one unit-returning runtime operation with standard post-success
    /// logging.
    async fn run_unit_runtime_call<F>(
        &self,
        runtime_id: &str,
        success_message: &'static str,
        call: F,
    ) -> RuntimeResult<()>
    where
        F: Future<Output = Result<(), BollardError>>,
    {
        self.run_runtime_call(runtime_id, call).await?;
        info!("{success_message}: {runtime_id}");
        Ok(())
    }
}
