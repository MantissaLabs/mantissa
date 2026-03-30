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
use crate::runtime::types::{RuntimeError, RuntimeResult};

mod conversions;
mod images;
mod interactive;
mod runtime;
#[cfg(test)]
mod tests;

/// Label key used to persist workload ownership onto runtime instances.
pub(super) const WORKLOAD_ID_LABEL: &str = "mantissa.workload_id";

/// Docker runtime backend implementation.
#[derive(Clone)]
pub struct DockerRuntimeBackend {
    docker: Docker,
}

impl DockerRuntimeBackend {
    /// Creates one Docker-backed runtime backend after verifying daemon
    /// connectivity.
    pub async fn new() -> RuntimeResult<Self> {
        let (docker, endpoint) =
            Self::connect().map_err(|err| RuntimeError::backend(None, err.to_string()))?;

        docker
            .ping()
            .await
            .map_err(|err| RuntimeError::OperationFailed(format!("docker ping failed: {err}")))?;

        info!(
            target: "task",
            "Connected to Docker endpoint {endpoint}",
        );

        Ok(Self { docker })
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
