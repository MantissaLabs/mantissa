use anyhow::anyhow;
use std::collections::HashMap;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::runtime::types::{
    ResourceLimits, RestartPolicyConfig, RestartPolicyType, RuntimeCreateRequest,
};
use crate::workload::model::{
    RuntimeClass, WorkloadEnvironmentVariable as TaskEnvironmentVariable,
    WorkloadSecretFile as TaskSecretFile, WorkloadVolumeMount as TaskVolumeMount,
};
use crate::workload::types::{
    WorkloadRestartPolicy as TaskRestartPolicy, WorkloadRestartPolicyKind as TaskRestartPolicyKind,
};

use super::secrets::ResolvedTaskSecrets;
use super::{
    WorkloadManager, instance_already_running, is_name_conflict, wrap_create_error,
    wrap_existing_inspect_error, wrap_start_error,
};

/// Shared launch inputs used by single-task and batch local startup paths.
pub(super) struct InstanceLaunchRequest<'a> {
    pub task_id: Uuid,
    pub task_name: &'a str,
    pub instance_name: &'a str,
    pub image: &'a str,
    pub runtime_class: RuntimeClass,
    pub sandbox_profile: Option<&'a str>,
    pub command: &'a [String],
    pub tty: bool,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub gpu_device_ids: &'a [String],
    pub truncate_gpu_device_ids: bool,
    pub restart_policy: Option<&'a TaskRestartPolicy>,
    pub env: &'a [TaskEnvironmentVariable],
    pub secret_files: &'a [TaskSecretFile],
    pub volume_mounts: &'a [TaskVolumeMount],
    pub networks: &'a [Uuid],
}

impl WorkloadManager {
    /// Builds one runtime instance launch request and guarantees the process is started.
    ///
    /// Both single-task and batch startup paths call this helper so create/start behavior cannot
    /// drift between the two code paths.
    pub(super) async fn launch_task_instance(
        &self,
        request: &InstanceLaunchRequest<'_>,
    ) -> Result<String, anyhow::Error> {
        let restart_policy = request.restart_policy.map(restart_policy_to_config);
        let resource_limits =
            ResourceLimits::from_requests(request.cpu_millis, request.memory_bytes);

        debug!(
            target: "task",
            task = %request.task_id,
            instance = %request.instance_name,
            networks = ?request.networks,
            "launching runtime instance with networks"
        );

        let dns_servers = self.resolve_dns_servers(request.networks).await?;
        let dns_servers = if dns_servers.is_empty() {
            None
        } else {
            Some(dns_servers)
        };

        let mut resolved = self
            .resolve_runtime_secrets(request.task_id, request.env, request.secret_files)
            .await?;
        let mut env_vars = if resolved.env.is_empty() {
            None
        } else {
            Some(resolved.env.clone())
        };
        let volumes = if resolved.mounts.is_empty() {
            None
        } else {
            Some(resolved.mounts.clone())
        };
        let volume_mounts = self
            .resolve_runtime_volume_mounts(request.task_id, request.volume_mounts)
            .await?;
        let volumes = match (volumes, volume_mounts.is_empty()) {
            (Some(mut mounts), false) => {
                mounts.extend(volume_mounts);
                Some(mounts)
            }
            (Some(mounts), true) => Some(mounts),
            (None, false) => Some(volume_mounts),
            (None, true) => None,
        };

        let gpu_device_ids = if request.gpu_count > 0 {
            let mut ids = request.gpu_device_ids.to_vec();
            if ids.len() < request.gpu_count as usize {
                cleanup_launch_artifacts(request.task_id, &mut resolved, "insufficient gpus").await;
                return Err(anyhow!(
                    "task {} requested {} GPU(s) but only {} GPU device(s) were reserved",
                    request.task_name,
                    request.gpu_count,
                    ids.len()
                ));
            }
            if request.truncate_gpu_device_ids && ids.len() > request.gpu_count as usize {
                ids.truncate(request.gpu_count as usize);
            }
            Some(ids)
        } else {
            None
        };

        if let Some(device_ids) = gpu_device_ids.as_ref() {
            if let Err(err) = self.ensure_gpu_runtime_ready(device_ids).await {
                cleanup_launch_artifacts(request.task_id, &mut resolved, "gpu runtime check").await;
                return Err(err);
            }
            super::append_nvidia_visible_devices(&mut env_vars, device_ids);
        }

        let mut labels = HashMap::from([
            (
                "mantissa.workload_id".to_string(),
                request.task_id.to_string(),
            ),
            (
                "mantissa.runtime_class".to_string(),
                request.runtime_class.as_str().to_string(),
            ),
        ]);
        if let Some(profile) = request
            .sandbox_profile
            .filter(|value| !value.trim().is_empty())
        {
            labels.insert(
                "mantissa.sandbox_profile".to_string(),
                profile.trim().to_string(),
            );
        }
        let create_request = RuntimeCreateRequest {
            name: request.instance_name.to_string(),
            image: request.image.to_string(),
            runtime_class: request.runtime_class,
            sandbox_profile: request.sandbox_profile.map(str::to_string),
            labels: Some(labels),
            command: if request.command.is_empty() {
                None
            } else {
                Some(request.command.to_vec())
            },
            tty: request.tty,
            // Keep stdin open so later `tasks attach` sessions can forward input into shells and
            // other interactive entrypoints after the runtime instance has already been started.
            open_stdin: true,
            env_vars,
            ports: None,
            volumes,
            restart_policy,
            resource_limits,
            dns_servers,
            gpu_device_ids,
        };
        let retry_create_request = create_request.clone();

        let (instance_id, created_fresh) = match self
            .runtime
            .runtime_backend
            .create_instance(create_request)
            .await
        {
            Ok(id) => (id, true),
            Err(err) => {
                if is_name_conflict(&err) {
                    match self
                        .resolve_existing_instance_id(request.instance_name)
                        .await
                    {
                        Ok(Some(existing_id)) => (existing_id, false),
                        Ok(None) => {
                            debug!(
                                target: "task",
                                task = %request.task_id,
                                instance = %request.instance_name,
                                "name conflict had no resolvable existing instance; retrying create once"
                            );
                            match self
                                .runtime
                                .runtime_backend
                                .create_instance(retry_create_request)
                                .await
                            {
                                Ok(id) => (id, true),
                                Err(retry_err) => {
                                    cleanup_launch_artifacts(
                                        request.task_id,
                                        &mut resolved,
                                        "create retry failed",
                                    )
                                    .await;
                                    return Err(wrap_create_error(request.task_name, retry_err));
                                }
                            }
                        }
                        Err(inspect_err) => {
                            cleanup_launch_artifacts(
                                request.task_id,
                                &mut resolved,
                                "inspect existing after name conflict",
                            )
                            .await;
                            return Err(wrap_existing_inspect_error(
                                request.task_name,
                                inspect_err,
                            ));
                        }
                    }
                } else {
                    cleanup_launch_artifacts(request.task_id, &mut resolved, "create failed").await;
                    return Err(wrap_create_error(request.task_name, err));
                }
            }
        };

        match self
            .runtime
            .runtime_backend
            .start_instance(&instance_id)
            .await
        {
            Ok(_) => {}
            Err(err) => {
                if instance_already_running(&err) {
                    debug!(
                        target: "task",
                        "instance {} already running while starting task {}",
                        instance_id,
                        request.task_id
                    );
                } else {
                    if created_fresh
                        && let Err(remove_err) = self
                            .runtime
                            .runtime_backend
                            .remove_instance(&instance_id, true, true)
                            .await
                    {
                        warn!(
                            target: "task",
                            "failed to remove instance {} after start failure: {remove_err}",
                            instance_id
                        );
                    }
                    return Err(wrap_start_error(request.task_name, err));
                }
            }
        }

        Ok(instance_id)
    }
}

/// Maps a task restart policy into the runtime restart-policy payload.
fn restart_policy_to_config(policy: &TaskRestartPolicy) -> RestartPolicyConfig {
    RestartPolicyConfig {
        name: match policy.name {
            TaskRestartPolicyKind::No => RestartPolicyType::No,
            TaskRestartPolicyKind::Always => RestartPolicyType::Always,
            TaskRestartPolicyKind::OnFailure => RestartPolicyType::OnFailure,
            TaskRestartPolicyKind::UnlessStopped => RestartPolicyType::UnlessStopped,
        },
        max_retry_count: policy.max_retry_count,
    }
}

/// Removes staged secret artifacts produced during one failed launch attempt.
async fn cleanup_launch_artifacts(task_id: Uuid, resolved: &mut ResolvedTaskSecrets, phase: &str) {
    if let Some(artifacts) = resolved.artifacts.take()
        && let Err(clean_err) = artifacts.cleanup().await
    {
        warn!(
            target: "task",
            task = %task_id,
            phase,
            "failed to cleanup staged secrets after launch failure: {clean_err}"
        );
    }
}
