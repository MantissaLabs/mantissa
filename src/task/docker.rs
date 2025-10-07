//! # Container Manager
//!
//! This module provides functionality to manage container lifecycle operations
//! using the Bollard Docker API.

use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bollard::Docker;
use bollard::container::{
    Config, CreateContainerOptions, InspectContainerOptions, ListContainersOptions,
    RemoveContainerOptions, RestartContainerOptions, StartContainerOptions, StopContainerOptions,
};
use bollard::models::{HostConfig, RestartPolicy, RestartPolicyNameEnum};
use bollard::service::ContainerInspectResponse;

use async_trait::async_trait;
use log::{debug, error, info};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur during container operations
#[derive(Error, Debug)]
pub enum ContainerError {
    #[error("Docker API error: {0}")]
    DockerAPI(#[from] bollard::errors::Error),

    #[error("Container not found: {0}")]
    NotFound(String),

    #[error("Container operation timeout")]
    Timeout,

    #[error("Operation failed: {0}")]
    OperationFailed(String),
}

/// Result type for container operations
pub type ContainerResult<T> = Result<T, ContainerError>;

/// Interface for container management operations
#[async_trait]
pub trait ContainerManager {
    /// Create a new container
    async fn create_container(
        &self,
        name: &str,
        image: &str,
        command: Option<Vec<String>>,
        env_vars: Option<Vec<String>>,
        ports: Option<HashMap<String, Vec<HashMap<String, String>>>>,
        volumes: Option<Vec<String>>,
        restart_policy: Option<RestartPolicyConfig>,
    ) -> ContainerResult<String>;

    /// Start a container
    async fn start_container(&self, container_id: &str) -> ContainerResult<()>;

    /// Stop a container
    async fn stop_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> ContainerResult<()>;

    /// Restart a container
    async fn restart_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> ContainerResult<()>;

    /// Remove a container
    async fn remove_container(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> ContainerResult<()>;

    /// List containers with optional filters
    async fn list_containers(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> ContainerResult<Vec<ContainerInfo>>;

    /// Get container details
    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> ContainerResult<ContainerInspectResponse>;

    // Pull an image
    async fn pull_image(&self, image: &str) -> ContainerResult<()>;
}

/// Configuration for container restart policy
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestartPolicyConfig {
    pub name: RestartPolicyType,
    pub max_retry_count: Option<i32>,
}

/// Types of restart policies
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RestartPolicyType {
    No,
    Always,
    OnFailure,
    UnlessStopped,
}

/// Container information returned from listing containers
#[derive(Debug, Clone)]
pub struct ContainerInfo {
    pub id: String,
    pub name: String,
    pub image: String,
    pub status: String,
    pub state: String,
    pub created: i64,
}

/// Docker container manager implementation
#[derive(Clone)]
pub struct DockerContainerManager {
    docker: Docker,
}

impl DockerContainerManager {
    /// Create a new Docker container manager
    pub async fn new() -> ContainerResult<Self> {
        let (docker, endpoint) = Self::connect().map_err(ContainerError::DockerAPI)?;

        docker
            .ping()
            .await
            .map_err(|e| ContainerError::OperationFailed(format!("docker ping failed: {e}")))?;

        info!(
            target: "task",
            "Connected to Docker endpoint {endpoint}",
        );

        Ok(Self { docker })
    }

    fn connect() -> Result<(Docker, String), bollard::errors::Error> {
        if let Ok(host) = env::var("MANTISSA_DOCKER_HOST") {
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
}

static CONTAINER_MANAGER_OVERRIDE: Lazy<Mutex<Option<Arc<dyn ContainerManager + Send + Sync>>>> =
    Lazy::new(|| Mutex::new(None));

pub fn container_manager_override() -> Option<Arc<dyn ContainerManager + Send + Sync>> {
    CONTAINER_MANAGER_OVERRIDE
        .lock()
        .expect("container manager override mutex poisoned")
        .as_ref()
        .cloned()
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn set_container_manager_override(manager: Arc<dyn ContainerManager + Send + Sync>) {
    *CONTAINER_MANAGER_OVERRIDE
        .lock()
        .expect("container manager override mutex poisoned") = Some(manager);
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn clear_container_manager_override() {
    CONTAINER_MANAGER_OVERRIDE
        .lock()
        .expect("container manager override mutex poisoned")
        .take();
}

#[async_trait]
impl ContainerManager for DockerContainerManager {
    async fn create_container(
        &self,
        name: &str,
        image: &str,
        command: Option<Vec<String>>,
        env_vars: Option<Vec<String>>,
        ports: Option<HashMap<String, Vec<HashMap<String, String>>>>,
        volumes: Option<Vec<String>>,
        restart_policy: Option<RestartPolicyConfig>,
    ) -> ContainerResult<String> {
        // Configure host settings
        let mut host_config = HostConfig::default();

        // Set restart policy if provided
        if let Some(policy) = restart_policy {
            let name = match policy.name {
                RestartPolicyType::No => RestartPolicyNameEnum::NO,
                RestartPolicyType::Always => RestartPolicyNameEnum::ALWAYS,
                RestartPolicyType::OnFailure => RestartPolicyNameEnum::ON_FAILURE,
                RestartPolicyType::UnlessStopped => RestartPolicyNameEnum::UNLESS_STOPPED,
            };

            host_config.restart_policy = Some(RestartPolicy {
                name: Some(name),
                maximum_retry_count: policy.max_retry_count.map(|value| i64::from(value)),
            });
        }

        // Set volumes if provided
        if let Some(vols) = volumes {
            host_config.binds = Some(vols);
        }

        // Create container config
        let config = Config {
            image: Some(image.to_string()),
            env: env_vars,
            cmd: command,
            exposed_ports: if let Some(ports_map) = ports {
                // Convert from HashMap<String, Vec<HashMap<String, String>>> to HashMap<String, HashMap<(), ()>>
                let mut exposed = HashMap::new();
                for port in ports_map.keys() {
                    exposed.insert(port.clone(), HashMap::new());
                }
                Some(exposed)
            } else {
                None
            },
            host_config: Some(host_config),
            ..Default::default()
        };

        // Set container name options
        let options = Some(CreateContainerOptions {
            name,
            platform: None,
        });

        debug!("Creating container '{}' with image '{}'", name, image);

        // Create the container
        let response = self
            .docker
            .create_container(options, config)
            .await
            .map_err(ContainerError::DockerAPI)?;

        if !response.warnings.is_empty() {
            for warning in response.warnings {
                debug!("Container creation warning: {warning}");
            }
        }

        info!("Container '{}' created with ID: {}", name, response.id);

        Ok(response.id)
    }

    async fn start_container(&self, container_id: &str) -> ContainerResult<()> {
        debug!("Starting container: {}", container_id);

        self.docker
            .start_container(container_id, None::<StartContainerOptions<String>>)
            .await
            .map_err(ContainerError::DockerAPI)?;

        info!("Container started: {}", container_id);

        Ok(())
    }

    async fn stop_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> ContainerResult<()> {
        let seconds = timeout.map(|t| t.as_secs() as i64);
        debug!(
            "Stopping container: {} (timeout: {:?}s)",
            container_id, seconds
        );

        let options = StopContainerOptions {
            t: seconds.unwrap_or(10),
        };

        self.docker
            .stop_container(container_id, Some(options))
            .await
            .map_err(ContainerError::DockerAPI)?;

        info!("Container stopped: {}", container_id);

        Ok(())
    }

    async fn restart_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> ContainerResult<()> {
        let seconds = timeout.map(|t| t.as_secs() as i64);
        debug!(
            "Restarting container: {} (timeout: {:?}s)",
            container_id, seconds
        );

        let options = RestartContainerOptions {
            t: seconds.unwrap_or(10) as isize,
        };

        self.docker
            .restart_container(container_id, Some(options))
            .await
            .map_err(ContainerError::DockerAPI)?;

        info!("Container restarted: {}", container_id);

        Ok(())
    }

    async fn remove_container(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> ContainerResult<()> {
        debug!(
            "Removing container: {} (force: {}, remove volumes: {})",
            container_id, force, remove_volumes
        );

        let options = RemoveContainerOptions {
            force,
            v: remove_volumes,
            link: false,
        };

        self.docker
            .remove_container(container_id, Some(options))
            .await
            .map_err(ContainerError::DockerAPI)?;

        info!("Container removed: {}", container_id);

        Ok(())
    }

    async fn list_containers(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> ContainerResult<Vec<ContainerInfo>> {
        debug!("Listing containers with filters: {:?}", filters);

        let options = ListContainersOptions {
            all: true,
            filters: filters.unwrap_or_default(),
            ..Default::default()
        };

        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .map_err(ContainerError::DockerAPI)?;

        let result = containers
            .into_iter()
            .map(|c| {
                let id = c.id.unwrap_or_default();
                let name = c
                    .names
                    .unwrap_or_default()
                    .first()
                    .cloned()
                    .unwrap_or_default()
                    .trim_start_matches('/')
                    .to_string();
                let image = c.image.unwrap_or_default();
                let status = c.status.unwrap_or_default();
                let state = c.state.unwrap_or_default();
                let created = c.created.unwrap_or_default();

                ContainerInfo {
                    id,
                    name,
                    image,
                    status,
                    state,
                    created,
                }
            })
            .collect();

        Ok(result)
    }

    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> ContainerResult<ContainerInspectResponse> {
        debug!("Inspecting container: {}", container_id);

        let options = Some(InspectContainerOptions { size: false });

        let container_info = self
            .docker
            .inspect_container(container_id, options)
            .await
            .map_err(ContainerError::DockerAPI)?;

        Ok(container_info)
    }

    async fn pull_image(&self, image: &str) -> ContainerResult<()> {
        debug!("Pulling image: {}", image);

        use futures::StreamExt;

        let options = Some(bollard::image::CreateImageOptions {
            from_image: image,
            ..Default::default()
        });

        let mut stream = self.docker.create_image(options, None, None);

        // Process the stream of updates
        while let Some(result) = stream.next().await {
            match result {
                Ok(update) => {
                    if let Some(status) = update.status {
                        debug!("Pull status: {status}");
                    }
                    if let Some(error) = update.error {
                        return Err(ContainerError::OperationFailed(error));
                    }
                }
                Err(err) => return Err(ContainerError::DockerAPI(err)),
            }
        }

        info!("Image pulled: {}", image);
        Ok(())
    }
}
