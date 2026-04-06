//! # Docker Runtime Backend
//!
//! This module provides the Docker-backed implementation of the generic
//! runtime backend using the Bollard Docker API.

use std::env;
use std::future::Future;

use bollard::Docker;
use bollard::errors::Error as BollardError;
use log::info;

use crate::config;
use crate::runtime::types::{
    RuntimeCapabilities, RuntimeError, RuntimeResult, RuntimeSupportContract, RuntimeSupportProfile,
};
use crate::workload::model::{ExecutionPlatform, IsolationMode};

mod conversions;
mod images;
mod interactive;
mod runtime;
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

/// Binary name used when Mantissa injects the `nono` init helper into containers.
pub const MANTISSA_NONO_HELPER_BINARY_NAME: &str = "mantissa-nono-init";

/// Environment variable used to pass the serialized sandbox policy to the helper.
pub const MANTISSA_NONO_POLICY_ENV_VAR: &str = "MANTISSA_NONO_POLICY";

/// One exact Docker runtime contract exposed through the node-local runtime registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DockerRuntimeMode {
    /// Standard Docker-backed OCI execution with no elevated sandbox contract.
    Standard,
    /// Dedicated sandboxed OCI execution slot that will be backed by `nono`.
    NonoSandbox,
}

impl DockerRuntimeMode {
    /// Returns the exact runtime support profile exposed by this backend registration.
    fn advertised_support(self, capabilities: RuntimeCapabilities) -> RuntimeSupportProfile {
        match self {
            Self::Standard => RuntimeSupportProfile::from_exact_contracts(
                [
                    RuntimeSupportContract::new(
                        ExecutionPlatform::Oci,
                        IsolationMode::Standard,
                        None,
                    ),
                    RuntimeSupportContract::new(
                        ExecutionPlatform::Oci,
                        IsolationMode::Standard,
                        Some(DOCKER_STANDARD_PROFILE),
                    ),
                ],
                capabilities.feature_flags(),
            ),
            Self::NonoSandbox => RuntimeSupportProfile::from_exact_contracts(
                [
                    RuntimeSupportContract::new(
                        ExecutionPlatform::Oci,
                        IsolationMode::Sandboxed,
                        None,
                    ),
                    RuntimeSupportContract::new(
                        ExecutionPlatform::Oci,
                        IsolationMode::Sandboxed,
                        Some(DOCKER_SANDBOXED_PROFILE),
                    ),
                    RuntimeSupportContract::new(
                        ExecutionPlatform::Oci,
                        IsolationMode::Sandboxed,
                        Some(DOCKER_NONO_PROFILE),
                    ),
                ],
                capabilities.feature_flags(),
            ),
        }
    }
}

/// Docker runtime backend implementation.
#[derive(Clone)]
pub struct DockerRuntimeBackend {
    docker: Docker,
    mode: DockerRuntimeMode,
}

impl DockerRuntimeBackend {
    /// Creates one Docker-backed runtime backend after verifying daemon
    /// connectivity.
    pub async fn new() -> RuntimeResult<Self> {
        Self::new_with_mode(DockerRuntimeMode::Standard).await
    }

    /// Creates one Docker-backed runtime backend for the provided exact contract.
    pub async fn new_with_mode(mode: DockerRuntimeMode) -> RuntimeResult<Self> {
        let (docker, endpoint) = Self::connect_verified().await?;
        Ok(Self::from_client(docker, mode, &endpoint))
    }

    /// Creates the standard and sandboxed Docker runtime registrations from one verified daemon.
    pub async fn new_pair() -> RuntimeResult<(Self, Self)> {
        let (docker, endpoint) = Self::connect_verified().await?;
        Ok((
            Self::from_client(docker.clone(), DockerRuntimeMode::Standard, &endpoint),
            Self::from_client(docker, DockerRuntimeMode::NonoSandbox, &endpoint),
        ))
    }

    /// Builds one Docker runtime backend around one already-verified client.
    fn from_client(docker: Docker, mode: DockerRuntimeMode, endpoint: &str) -> Self {
        info!(
            target: "task",
            "Connected to Docker endpoint {endpoint} for {:?} OCI backend",
            mode
        );

        Self { docker, mode }
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
