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
use bollard::errors::Error as BollardError;
use bollard::models::{
    DeviceRequest, EventMessageTypeEnum, HostConfig, RestartPolicy, RestartPolicyNameEnum,
};
use bollard::service::ContainerInspectResponse;
use bollard::system::EventsOptions;

use crate::config;
use async_trait::async_trait;
use futures::StreamExt;
use log::{debug, info, trace, warn};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;

/// Errors that can occur during container operations
#[derive(Error, Debug)]
pub enum ContainerError {
    #[error("Docker API error: {0}")]
    DockerAPI(#[from] bollard::errors::Error),

    #[allow(dead_code)]
    #[error("Container not found: {0}")]
    NotFound(String),

    #[allow(dead_code)]
    #[error("Container operation timeout")]
    Timeout,

    #[error("Operation failed: {0}")]
    OperationFailed(String),
}

/// Result type for container operations
pub type ContainerResult<T> = Result<T, ContainerError>;

/// Normalizes low-level Docker API errors into stable container error variants.
fn classify_container_error(container_id: &str, err: BollardError) -> ContainerError {
    match &err {
        BollardError::DockerResponseServerError { status_code, .. } if *status_code == 404 => {
            ContainerError::NotFound(container_id.to_string())
        }
        _ => ContainerError::DockerAPI(err),
    }
}

/// Parameters describing how to launch a container.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContainerCreateRequest {
    pub name: String,
    pub image: String,
    pub command: Option<Vec<String>>,
    pub env_vars: Option<Vec<String>>,
    pub ports: Option<HashMap<String, Vec<HashMap<String, String>>>>,
    pub volumes: Option<Vec<String>>,
    pub restart_policy: Option<RestartPolicyConfig>,
    pub resource_limits: ResourceLimits,
    pub dns_servers: Option<Vec<String>>,
    pub gpu_device_ids: Option<Vec<String>>,
}

/// Interface for container management operations
#[async_trait]
pub trait ContainerManager {
    /// Create a new container
    async fn create_container(&self, request: ContainerCreateRequest) -> ContainerResult<String>;

    /// Start a container
    async fn start_container(&self, container_id: &str) -> ContainerResult<()>;

    /// Stop a container
    async fn stop_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> ContainerResult<()>;

    /// Restart a container
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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

    /// Indicates whether the runtime supports lifecycle event streaming.
    fn supports_runtime_events(&self) -> bool {
        false
    }

    /// Streams runtime lifecycle events into the provided queue until the stream ends.
    async fn watch_runtime_events(
        &self,
        _events_tx: UnboundedSender<ContainerRuntimeEvent>,
    ) -> ContainerResult<()> {
        Ok(())
    }
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

/// Resource limits that should be enforced by the container engine.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceLimits {
    pub memory_bytes: Option<i64>,
    pub nano_cpus: Option<i64>,
    pub cpu_shares: Option<i64>,
}

impl ResourceLimits {
    const MIN_CPU_SHARES: i64 = 2;
    const MAX_CPU_SHARES: i64 = 262_144;

    /// Builds resource limits from scheduler requests expressed in milli-CPU and bytes.
    pub fn from_requests(cpu_millis: u64, memory_bytes: u64) -> Self {
        let memory_bytes = if memory_bytes == 0 {
            None
        } else {
            Some(Self::saturating_i64(memory_bytes as u128))
        };

        let nano_cpus = if cpu_millis == 0 {
            None
        } else {
            let nanos = (cpu_millis as u128).saturating_mul(1_000_000u128);
            Some(Self::saturating_i64(nanos))
        };

        let cpu_shares = if cpu_millis == 0 {
            None
        } else {
            let shares = (cpu_millis as u128).saturating_mul(1024u128) / 1_000u128;
            let shares = shares
                .max(Self::MIN_CPU_SHARES as u128)
                .min(Self::MAX_CPU_SHARES as u128);
            Some(Self::saturating_i64(shares))
        };

        Self {
            memory_bytes,
            nano_cpus,
            cpu_shares,
        }
    }

    fn saturating_i64(value: u128) -> i64 {
        if value > i64::MAX as u128 {
            i64::MAX
        } else {
            value as i64
        }
    }
}

/// Container information returned from listing containers
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ContainerInfo {
    pub id: String,
    pub name: String,
    pub image: String,
    pub status: String,
    pub state: String,
    pub created: i64,
}

/// Normalized container runtime events used by task reconciliation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContainerRuntimeEvent {
    ContainerStateChanged,
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
        if let Some(host) = config::docker_host() {
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
    async fn create_container(&self, request: ContainerCreateRequest) -> ContainerResult<String> {
        let ContainerCreateRequest {
            name,
            image,
            command,
            env_vars,
            ports,
            volumes,
            restart_policy,
            resource_limits,
            dns_servers,
            gpu_device_ids,
        } = request;

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
                maximum_retry_count: policy.max_retry_count.map(i64::from),
            });
        }

        if let Some(memory) = resource_limits.memory_bytes {
            host_config.memory = Some(memory);
            host_config.memory_swap = Some(-1);
        }

        if let Some(nano_cpus) = resource_limits.nano_cpus {
            host_config.nano_cpus = Some(nano_cpus);
        }

        if let Some(cpu_shares) = resource_limits.cpu_shares {
            host_config.cpu_shares = Some(cpu_shares);
        }

        if let Some(device_ids) = gpu_device_ids {
            if !device_ids.is_empty() {
                host_config.device_requests = Some(vec![DeviceRequest {
                    driver: Some("nvidia".to_string()),
                    count: None,
                    device_ids: Some(device_ids),
                    capabilities: Some(vec![vec![
                        "gpu".to_string(),
                        "compute".to_string(),
                        "utility".to_string(),
                    ]]),
                    options: None,
                }]);
            }
        }

        // Set volumes if provided
        if let Some(vols) = volumes {
            host_config.binds = Some(vols);
        }

        if let Some(servers) = dns_servers {
            host_config.dns = Some(servers.clone());
            info!(target: "task", "configured container dns for {name}: {servers:?}");
        } else {
            warn!(
                target: "task",
                "no custom dns configured for {name}; falling back to docker defaults"
            );
        }

        // Create container config
        let config = Config {
            image: Some(image.clone()),
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
            name: &name,
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
            .map_err(|err| classify_container_error(container_id, err))?;

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
            .map_err(|err| classify_container_error(container_id, err))?;

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
            .map_err(|err| classify_container_error(container_id, err))?;

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
            .map_err(|err| classify_container_error(container_id, err))?;

        info!("Container removed: {}", container_id);

        Ok(())
    }

    async fn list_containers(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> ContainerResult<Vec<ContainerInfo>> {
        tracing::trace!(target: "task::docker", ?filters, "listing containers");

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
        trace!("Inspecting container: {}", container_id);

        let options = Some(InspectContainerOptions { size: false });

        let container_info = self
            .docker
            .inspect_container(container_id, options)
            .await
            .map_err(|err| classify_container_error(container_id, err))?;

        Ok(container_info)
    }

    async fn pull_image(&self, image: &str) -> ContainerResult<()> {
        debug!("Pulling image: {}", image);

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

    fn supports_runtime_events(&self) -> bool {
        true
    }

    async fn watch_runtime_events(
        &self,
        events_tx: UnboundedSender<ContainerRuntimeEvent>,
    ) -> ContainerResult<()> {
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert("type".to_string(), vec!["container".to_string()]);
        let options = EventsOptions::<String> {
            since: None,
            until: None,
            filters,
        };

        let mut stream = self.docker.events(Some(options));
        while let Some(next) = stream.next().await {
            let event = next.map_err(ContainerError::DockerAPI)?;
            if event.typ != Some(EventMessageTypeEnum::CONTAINER) {
                continue;
            }
            let Some(action) = event.action.as_deref() else {
                continue;
            };
            // Only forward lifecycle edges that materially change convergence state.
            // `kill`/`stop` can fire repeatedly while a stop is already in progress and would
            // amplify reconcile churn without adding useful state information.
            if !matches!(action, "start" | "die" | "destroy" | "rename") {
                continue;
            }

            let name = event
                .actor
                .as_ref()
                .and_then(|actor| actor.attributes.as_ref())
                .and_then(|attrs| attrs.get("name"));
            if name.map(|value| value.starts_with("mantissa-")) != Some(true) {
                continue;
            }

            if events_tx
                .send(ContainerRuntimeEvent::ContainerStateChanged)
                .is_err()
            {
                return Ok(());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Default)]
    struct NullContainerManager;

    #[async_trait]
    impl ContainerManager for NullContainerManager {
        async fn create_container(
            &self,
            _request: ContainerCreateRequest,
        ) -> ContainerResult<String> {
            Ok(String::from("noop"))
        }

        async fn start_container(&self, _container_id: &str) -> ContainerResult<()> {
            Ok(())
        }

        async fn stop_container(
            &self,
            _container_id: &str,
            _timeout: Option<Duration>,
        ) -> ContainerResult<()> {
            Ok(())
        }

        async fn restart_container(
            &self,
            _container_id: &str,
            _timeout: Option<Duration>,
        ) -> ContainerResult<()> {
            Ok(())
        }

        async fn remove_container(
            &self,
            _container_id: &str,
            _force: bool,
            _remove_volumes: bool,
        ) -> ContainerResult<()> {
            Ok(())
        }

        async fn list_containers(
            &self,
            _filters: Option<HashMap<String, Vec<String>>>,
        ) -> ContainerResult<Vec<ContainerInfo>> {
            Ok(Vec::new())
        }

        async fn inspect_container(
            &self,
            _container_id: &str,
        ) -> ContainerResult<ContainerInspectResponse> {
            Err(ContainerError::OperationFailed("noop".into()))
        }

        async fn pull_image(&self, _image: &str) -> ContainerResult<()> {
            Ok(())
        }
    }

    #[test]
    fn container_manager_override_round_trip() {
        let previous = container_manager_override();
        clear_container_manager_override();

        let manager: Arc<dyn ContainerManager + Send + Sync> = Arc::new(NullContainerManager);
        set_container_manager_override(manager.clone());

        let current = container_manager_override().expect("override installed");
        assert!(Arc::ptr_eq(&current, &manager));

        clear_container_manager_override();

        if let Some(previous_manager) = previous {
            set_container_manager_override(previous_manager);
        }
    }

    #[test]
    fn classify_container_error_maps_404_to_not_found() {
        let error = bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            message: "No such container".to_string(),
        };
        let mapped = classify_container_error("demo-container", error);
        assert!(matches!(mapped, ContainerError::NotFound(ref id) if id == "demo-container"));
    }

    #[test]
    fn classify_container_error_preserves_non_404_as_docker_api() {
        let error = bollard::errors::Error::DockerResponseServerError {
            status_code: 409,
            message: "Conflict".to_string(),
        };
        let mapped = classify_container_error("demo-container", error);
        assert!(matches!(
            mapped,
            ContainerError::DockerAPI(bollard::errors::Error::DockerResponseServerError {
                status_code: 409,
                ..
            })
        ));
    }
}
