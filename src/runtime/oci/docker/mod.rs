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
use crate::runtime::types::{
    RuntimeCapabilities, RuntimeError, RuntimeResult, RuntimeSupportContract, RuntimeSupportProfile,
};
use crate::workload::model::{ExecutionPlatform, IsolationMode};

mod conversions;
mod images;
mod interactive;
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

/// One exact Docker runtime contract exposed through the node-local runtime registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DockerRuntimeMode {
    /// Standard Docker-backed OCI execution with no elevated sandbox contract.
    Standard,
    /// Dedicated sandboxed OCI execution slot that will be backed by `nono`.
    NonoSandbox,
}

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
    nono_helper_host_path: Option<PathBuf>,
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
        let nono_helper_host_path = match mode {
            DockerRuntimeMode::Standard => None,
            DockerRuntimeMode::NonoSandbox => {
                let availability = NonoSandboxBackendAvailability::detect();
                match availability.helper_host_path() {
                    Some(path) => Some(path.clone()),
                    None => {
                        return Err(RuntimeError::OperationFailed(
                            availability.unavailable_reason().unwrap_or_else(|| {
                                "sandboxed Docker backend is unavailable".to_string()
                            }),
                        ));
                    }
                }
            }
        };
        Ok(Self::from_client(
            docker,
            mode,
            &endpoint,
            nono_helper_host_path,
        ))
    }

    /// Creates the standard Docker runtime plus the optional sandboxed registration.
    pub async fn new_pair() -> RuntimeResult<(Self, Option<Self>)> {
        let (docker, endpoint) = Self::connect_verified().await?;
        let sandbox_availability = NonoSandboxBackendAvailability::detect();
        let standard =
            Self::from_client(docker.clone(), DockerRuntimeMode::Standard, &endpoint, None);
        let sandboxed = match sandbox_availability.helper_host_path() {
            Some(path) => Some(Self::from_client(
                docker,
                DockerRuntimeMode::NonoSandbox,
                &endpoint,
                Some(path.clone()),
            )),
            None => {
                if let Some(reason) = sandbox_availability.unavailable_reason() {
                    info!(
                        target: "task",
                        "Skipping sandboxed Docker backend registration: {reason}"
                    );
                }
                None
            }
        };

        Ok((standard, sandboxed))
    }

    /// Returns whether the current Mantissa host can run `nono`-sandboxed workloads.
    fn host_supports_nono_sandbox() -> bool {
        cfg!(any(target_os = "linux", target_os = "macos"))
    }

    /// Builds one Docker runtime backend around one already-verified client.
    fn from_client(
        docker: Docker,
        mode: DockerRuntimeMode,
        endpoint: &str,
        nono_helper_host_path: Option<PathBuf>,
    ) -> Self {
        info!(
            target: "task",
            "Connected to Docker endpoint {endpoint} for {:?} OCI backend",
            mode
        );
        if matches!(mode, DockerRuntimeMode::NonoSandbox)
            && let Some(path) = nono_helper_host_path.as_ref()
        {
            info!(
                target: "task",
                "Resolved sandbox helper for sandboxed Docker backend: {}",
                path.display()
            );
        }

        Self {
            docker,
            mode,
            nono_helper_host_path,
        }
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
