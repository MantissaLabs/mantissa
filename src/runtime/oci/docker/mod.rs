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
mod sandbox_check;
#[cfg(test)]
mod tests;

/// Label key used to persist workload ownership onto runtime instances.
pub(super) const WORKLOAD_ID_LABEL: &str = "mantissa.workload_id";

/// Default operator-facing profile name for standard OCI Docker workloads.
pub(super) const DOCKER_STANDARD_PROFILE: &str = "default";

/// Default operator-facing profile name for sandboxed OCI Docker workloads.
pub(super) const DOCKER_SANDBOXED_PROFILE: &str = "oci-default";

/// Explicit profile name for Docker workloads restricted by the Nono helper.
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

/// Helper argument used to validate a policy without executing the workload.
pub const MANTISSA_SANDBOX_CHECK_ARGUMENT: &str = "--check";

/// Marks disposable checks so inherited image labels cannot make them look like workloads.
pub(super) const SANDBOX_CHECK_LABEL: &str = "mantissa.sandbox.check";

/// Docker runtime backend implementation.
#[derive(Clone)]
pub struct DockerRuntimeBackend {
    docker: Docker,
    nono_helper_host_path: Option<PathBuf>,
    docker_config_path: Option<PathBuf>,
}

impl DockerRuntimeBackend {
    /// Connects to Docker and checks helper compatibility before offering sandboxed execution.
    pub async fn new() -> RuntimeResult<Self> {
        let (docker, endpoint) = Self::connect_verified().await?;
        let nono_helper_host_path = match Self::find_sandbox_helper(&docker).await {
            Ok(path) => Some(path),
            Err(reason) => {
                info!(target: "task", "Docker sandbox support is unavailable: {reason}");
                None
            }
        };

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
