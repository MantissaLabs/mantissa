//! RuntimeBackend implementation for Docker.
//!
//! This file holds the high-level lifecycle operations exposed through the
//! generic runtime trait. The helpers it calls live in sibling modules so the
//! trait implementation stays focused on the scheduler-facing behavior.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use bollard::container::AttachContainerResults;
use bollard::errors::Error as BollardError;
use bollard::models::{
    ContainerCreateBody, DeviceRequest, EventMessageTypeEnum, HostConfig, RestartPolicy,
    RestartPolicyNameEnum,
};
use bollard::query_parameters::{
    AttachContainerOptionsBuilder, CreateContainerOptions, CreateImageOptions, EventsOptions,
    InspectContainerOptions, ListContainersOptions, LogsOptionsBuilder, RemoveContainerOptions,
    RestartContainerOptions, StartContainerOptions, StopContainerOptions,
};
use futures::StreamExt;
use log::{debug, info, trace, warn};
use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender, UnboundedSender};

use crate::runtime::types::{
    RestartPolicyType, RuntimeAttachOptions, RuntimeBackend, RuntimeCapabilities,
    RuntimeCreateRequest, RuntimeError, RuntimeEvent, RuntimeExecOptions, RuntimeExecResult,
    RuntimeInfo, RuntimeLogFrame, RuntimeLogsOptions, RuntimeResult, RuntimeSupportProfile,
};
use crate::workload::model::ExecutionPlatform;

use super::conversions::{
    classify_runtime_error, runtime_info_from_inspect, runtime_info_from_list_entry,
    runtime_log_frame_from_output,
};
use super::images::PullProgressLogState;
use super::interactive::AttachBridgeIo;
use super::{DockerRuntimeBackend, WORKLOAD_ID_LABEL};

#[async_trait]
impl RuntimeBackend for DockerRuntimeBackend {
    /// Creates one Docker container from the generic runtime create request.
    async fn create_instance(&self, request: RuntimeCreateRequest) -> RuntimeResult<String> {
        let RuntimeCreateRequest {
            name,
            image,
            execution_platform: _execution_platform,
            isolation_mode: _isolation_mode,
            isolation_profile: _isolation_profile,
            labels,
            command,
            tty,
            open_stdin,
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

        if let Some(device_ids) = gpu_device_ids
            && !device_ids.is_empty()
        {
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
        let config = ContainerCreateBody {
            image: Some(image.clone()),
            labels,
            tty: Some(tty),
            open_stdin: Some(open_stdin),
            env: env_vars,
            cmd: command,
            exposed_ports: ports.map(|ports_map| ports_map.into_keys().collect()),
            host_config: Some(host_config),
            ..Default::default()
        };

        // Set container name options
        let options = Some(CreateContainerOptions {
            name: Some(name.clone()),
            ..Default::default()
        });

        debug!("Creating container '{name}' with image '{image}'");

        // Create the container
        let response = self
            .docker
            .create_container(options, config)
            .await
            .map_err(|err| classify_runtime_error(&name, err))?;

        if !response.warnings.is_empty() {
            for warning in response.warnings {
                debug!("Container creation warning: {warning}");
            }
        }

        info!("Container '{name}' created with ID: {}", response.id);

        Ok(response.id)
    }

    /// Starts one existing Docker container.
    async fn start_instance(&self, container_id: &str) -> RuntimeResult<()> {
        debug!("Starting container: {container_id}");

        self.run_unit_runtime_call(
            container_id,
            "Container started",
            self.docker
                .start_container(container_id, None::<StartContainerOptions>),
        )
        .await
    }

    /// Stops one Docker container.
    async fn stop_instance(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        let seconds = timeout.map(|value| value.as_secs() as i64);
        debug!("Stopping container: {container_id} (timeout: {seconds:?}s)");
        let effective_seconds = Self::timeout_seconds_or_default(timeout, 10);

        self.run_unit_runtime_call(
            container_id,
            "Container stopped",
            self.docker.stop_container(
                container_id,
                Some(StopContainerOptions {
                    t: Some(effective_seconds),
                    ..Default::default()
                }),
            ),
        )
        .await
    }

    /// Executes one non-interactive command inside one Docker container.
    async fn exec_instance(
        &self,
        container_id: &str,
        command: &[String],
        timeout: Option<Duration>,
    ) -> RuntimeResult<RuntimeExecResult> {
        if command.is_empty() {
            return Err(RuntimeError::OperationFailed(
                "pre-stop command must contain at least one argument".to_string(),
            ));
        }

        debug!("Executing command in container: {container_id} ({command:?})");

        let exec_future = self.run_exec(container_id, command);
        match timeout {
            Some(limit) => match tokio::time::timeout(limit, exec_future).await {
                Ok(result) => result,
                Err(_) => Err(RuntimeError::Timeout),
            },
            None => exec_future.await,
        }
    }

    /// Starts one interactive exec session inside one Docker container.
    async fn exec_instance_stream(
        &self,
        container_id: &str,
        options: &RuntimeExecOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<RuntimeExecResult> {
        self.exec_container_interactive(container_id, options, output_tx, input_rx)
            .await
    }

    /// Restarts one Docker container.
    async fn restart_instance(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        let seconds = timeout.map(|value| value.as_secs() as i64);
        debug!("Restarting container: {container_id} (timeout: {seconds:?}s)");
        let effective_seconds = Self::timeout_seconds_or_default(timeout, 10);

        self.run_unit_runtime_call(
            container_id,
            "Container restarted",
            self.docker.restart_container(
                container_id,
                Some(RestartContainerOptions {
                    t: Some(effective_seconds),
                    ..Default::default()
                }),
            ),
        )
        .await
    }

    /// Removes one Docker container.
    async fn remove_instance(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> RuntimeResult<()> {
        debug!(
            "Removing container: {container_id} (force: {force}, remove volumes: {remove_volumes})"
        );

        self.run_unit_runtime_call(
            container_id,
            "Container removed",
            self.docker.remove_container(
                container_id,
                Some(RemoveContainerOptions {
                    force,
                    v: remove_volumes,
                    link: false,
                }),
            ),
        )
        .await
    }

    /// Lists Docker containers through the generic runtime info shape.
    async fn list_instances(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> RuntimeResult<Vec<RuntimeInfo>> {
        tracing::trace!(target: "task::docker", ?filters, "listing containers");

        let options = ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        };

        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .map_err(|err| RuntimeError::backend(None, err.to_string()))?;

        let result = containers
            .into_iter()
            .map(runtime_info_from_list_entry)
            .collect();

        Ok(result)
    }

    /// Returns inspect-level Docker metadata through the generic runtime info
    /// shape.
    async fn inspect_instance(&self, container_id: &str) -> RuntimeResult<RuntimeInfo> {
        trace!("Inspecting container: {container_id}");
        let inspect = self
            .run_runtime_call(
                container_id,
                self.docker
                    .inspect_container(container_id, Some(InspectContainerOptions { size: false })),
            )
            .await?;
        Ok(runtime_info_from_inspect(inspect))
    }

    /// Reports whether one Docker image already exists locally.
    async fn image_present(&self, image: &str) -> RuntimeResult<bool> {
        trace!("Inspecting image: {image}");
        match self.docker.inspect_image(image).await {
            Ok(_) => Ok(true),
            Err(BollardError::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(false),
            Err(err) => Err(RuntimeError::backend(None, err.to_string())),
        }
    }

    /// Pulls one image from the configured Docker registry.
    async fn pull_image(&self, image: &str) -> RuntimeResult<()> {
        debug!("Pulling image: {image}");

        let options = Some(CreateImageOptions {
            from_image: Some(image.to_string()),
            ..Default::default()
        });

        let mut stream = self.docker.create_image(options, None, None);
        let mut last_updates: HashMap<Option<String>, PullProgressLogState> = HashMap::new();

        // Process the stream of updates
        while let Some(result) = stream.next().await {
            match result {
                Ok(update) => {
                    if Self::should_log_pull_update(&mut last_updates, &update)
                        && let Some(status) = Self::format_pull_status(&update)
                    {
                        debug!("Pull status: {status}");
                    }
                    if let Some(error) = update
                        .error_detail
                        .as_ref()
                        .and_then(|detail| detail.message.as_deref())
                    {
                        return Err(RuntimeError::OperationFailed(error.to_string()));
                    }
                }
                Err(err) => return Err(RuntimeError::backend(None, err.to_string())),
            }
        }

        info!("Image pulled: {image}");
        Ok(())
    }

    /// Streams Docker log frames while preserving stream identity and follow
    /// semantics.
    async fn stream_instance_logs(
        &self,
        container_id: &str,
        options: &RuntimeLogsOptions,
        logs_tx: MpscSender<RuntimeLogFrame>,
    ) -> RuntimeResult<()> {
        let options = options.normalized();
        let mut stream = self.docker.logs(
            container_id,
            Some(
                LogsOptionsBuilder::new()
                    .follow(options.follow)
                    .stdout(options.stdout)
                    .stderr(options.stderr)
                    .timestamps(options.timestamps)
                    .tail(&options.tail)
                    .build(),
            ),
        );

        while let Some(next) = stream.next().await {
            let frame = next.map_err(|err| classify_runtime_error(container_id, err))?;
            if logs_tx
                .send(runtime_log_frame_from_output(frame))
                .await
                .is_err()
            {
                return Ok(());
            }
        }

        Ok(())
    }

    /// Attaches to one Docker container and bridges both stdout and stderr
    /// output together with stdin input.
    async fn attach_instance(
        &self,
        container_id: &str,
        options: &RuntimeAttachOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<()> {
        if options.tty {
            return self
                .attach_tty_container_raw(container_id, options, output_tx, input_rx)
                .await;
        }

        let mut builder = AttachContainerOptionsBuilder::new()
            .logs(options.logs)
            .stream(options.stream)
            .stdin(options.stdin)
            .stdout(options.stdout)
            .stderr(options.stderr);
        if let Some(detach_keys) = options.detach_keys.as_deref() {
            builder = builder.detach_keys(detach_keys);
        }

        self.ensure_container_running_for_stream(container_id)
            .await?;
        let AttachContainerResults {
            mut output,
            mut input,
        } = self
            .run_runtime_call(
                container_id,
                self.docker
                    .attach_container(container_id, Some(builder.build())),
            )
            .await?;

        self.bridge_attached_io(
            container_id,
            &mut output,
            &mut input,
            options,
            AttachBridgeIo {
                output_tx,
                input_rx,
                saw_output: false,
            },
        )
        .await
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            exec: true,
            interactive_exec: true,
            logs: true,
            attach: true,
            lifecycle_events: true,
        }
    }

    /// Advertises Docker-backed OCI execution in both standard and sandboxed isolation modes.
    fn advertised_support(&self) -> RuntimeSupportProfile {
        RuntimeSupportProfile::new(
            [ExecutionPlatform::Oci],
            [
                crate::workload::model::IsolationMode::Standard,
                crate::workload::model::IsolationMode::Sandboxed,
            ],
            ["default", "oci-default"],
            self.capabilities().feature_flags(),
        )
    }

    /// Watches Docker container events and forwards task-relevant lifecycle
    /// edges.
    async fn watch_runtime_events(
        &self,
        events_tx: UnboundedSender<RuntimeEvent>,
    ) -> RuntimeResult<()> {
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert("type".to_string(), vec!["container".to_string()]);
        let options = EventsOptions {
            since: None,
            until: None,
            filters: Some(filters),
        };

        let mut stream = self.docker.events(Some(options));
        while let Some(next) = stream.next().await {
            let event = next.map_err(|err| RuntimeError::backend(None, err.to_string()))?;
            if event.typ != Some(EventMessageTypeEnum::CONTAINER) {
                continue;
            }
            let Some(action) = event.action.as_deref() else {
                continue;
            };
            // Only forward lifecycle edges that materially change convergence
            // state. `kill` and `stop` can fire repeatedly while a stop is
            // already in progress and would amplify reconcile churn without
            // adding useful state information.
            if !matches!(action, "start" | "die" | "destroy" | "rename") {
                continue;
            }

            let attributes = event
                .actor
                .as_ref()
                .and_then(|actor| actor.attributes.as_ref());
            let workload_id = attributes
                .and_then(|attrs| attrs.get(WORKLOAD_ID_LABEL))
                .and_then(|value| uuid::Uuid::parse_str(value).ok());
            if workload_id.is_none() {
                continue;
            }

            if action == "die" {
                let exit_code = event
                    .actor
                    .as_ref()
                    .and_then(|actor| actor.attributes.as_ref())
                    .and_then(|attrs| attrs.get("exitCode"))
                    .and_then(|value| value.parse::<i32>().ok())
                    .unwrap_or(1);

                if let Some(task_id) = workload_id
                    && events_tx
                        .send(RuntimeEvent::TaskExited { task_id, exit_code })
                        .is_err()
                {
                    return Ok(());
                }
            }

            if events_tx.send(RuntimeEvent::InstanceStateChanged).is_err() {
                return Ok(());
            }
        }

        Ok(())
    }
}
