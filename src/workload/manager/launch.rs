use anyhow::anyhow;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::agents::types::{
    AGENT_ALLOW_NETWORK_ENV_VAR, AGENT_ALLOW_WRITE_ENV_VAR, AGENT_WORKDIR_ENV_VAR,
};
use crate::runtime::types::{
    ResourceLimits, RestartPolicyConfig, RestartPolicyType, RuntimeCreateRequest,
    RuntimeInstanceRef, RuntimePortBinding, RuntimePortProtocol, RuntimeSandboxAccessMode,
    RuntimeSandboxNetworkMode, RuntimeSandboxPathRule, RuntimeSandboxPolicy,
};
use crate::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadEnvironmentVariable as TaskEnvironmentVariable,
    WorkloadOwner, WorkloadSecretFile, WorkloadVolumeMount as TaskVolumeMount,
};
use crate::workload::types::{
    WorkloadPortBinding, WorkloadPortProtocol, WorkloadRestartPolicy, WorkloadRestartPolicyKind,
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
    pub execution_platform: ExecutionPlatform,
    pub isolation_mode: IsolationMode,
    pub isolation_profile: Option<&'a str>,
    pub command: &'a [String],
    pub tty: bool,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub gpu_device_ids: &'a [String],
    pub truncate_gpu_device_ids: bool,
    pub restart_policy: Option<&'a WorkloadRestartPolicy>,
    pub env: &'a [TaskEnvironmentVariable],
    pub secret_files: &'a [WorkloadSecretFile],
    pub volume_mounts: &'a [TaskVolumeMount],
    pub networks: &'a [Uuid],
    pub ports: &'a [WorkloadPortBinding],
    pub owner: Option<&'a WorkloadOwner>,
}

impl WorkloadManager {
    /// Builds one runtime instance launch request and guarantees the process is started.
    ///
    /// Both single-task and batch startup paths call this helper so create/start behavior cannot
    /// drift between the two code paths.
    pub(super) async fn launch_task_instance(
        &self,
        request: &InstanceLaunchRequest<'_>,
    ) -> Result<RuntimeInstanceRef, anyhow::Error> {
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
                "mantissa.execution_platform".to_string(),
                request.execution_platform.as_str().to_string(),
            ),
            (
                "mantissa.isolation_mode".to_string(),
                request.isolation_mode.as_str().to_string(),
            ),
        ]);
        if let Some(profile) = request
            .isolation_profile
            .filter(|value| !value.trim().is_empty())
        {
            labels.insert(
                "mantissa.isolation_profile".to_string(),
                profile.trim().to_string(),
            );
        }
        let create_request = RuntimeCreateRequest {
            name: request.instance_name.to_string(),
            image: request.image.to_string(),
            execution_platform: request.execution_platform,
            isolation_mode: request.isolation_mode,
            isolation_profile: request.isolation_profile.map(str::to_string),
            sandbox_policy: build_runtime_sandbox_policy(request),
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
            ports: request
                .ports
                .iter()
                .map(runtime_port_binding_from_workload)
                .collect(),
            volumes,
            restart_policy,
            resource_limits,
            dns_servers,
            gpu_device_ids,
        };
        let retry_create_request = create_request.clone();
        let execution_platform = request.execution_platform;
        let isolation_mode = request.isolation_mode;
        let isolation_profile = request.isolation_profile;

        let (instance_id, created_fresh) = match self
            .runtime
            .runtime_set
            .create_instance(create_request)
            .await
        {
            Ok(id) => (id, true),
            Err(err) => {
                if is_name_conflict(&err) {
                    match self
                        .resolve_existing_runtime_instance(
                            request.instance_name,
                            execution_platform,
                            isolation_mode,
                            isolation_profile,
                        )
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
                                .runtime_set
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

        match self.runtime.runtime_set.start_instance(&instance_id).await {
            Ok(_) => {}
            Err(err) => {
                if instance_already_running(&err) {
                    debug!(
                        target: "task",
                        "instance {} already running while starting task {}",
                        instance_id.handle,
                        request.task_id
                    );
                } else {
                    if created_fresh
                        && let Err(remove_err) = self
                            .runtime
                            .runtime_set
                            .remove_instance(&instance_id, true, true)
                            .await
                    {
                        warn!(
                            target: "task",
                            "failed to remove instance {} after start failure: {remove_err}",
                            instance_id.handle
                        );
                    }
                    return Err(wrap_start_error(request.task_name, err));
                }
            }
        }

        Ok(instance_id)
    }
}

/// Builds the runtime-enforced sandbox policy for one launch request when the owner needs it.
fn build_runtime_sandbox_policy(
    request: &InstanceLaunchRequest<'_>,
) -> Option<RuntimeSandboxPolicy> {
    if request.execution_platform != ExecutionPlatform::Oci
        || request.isolation_mode != IsolationMode::Sandboxed
        || !matches!(request.owner, Some(WorkloadOwner::AgentRun(_)))
    {
        return None;
    }

    Some(build_agent_runtime_sandbox_policy(
        request.env,
        request.secret_files,
        request.volume_mounts,
    ))
}

/// Translates persisted agent policy hints into the concrete runtime sandbox contract.
fn build_agent_runtime_sandbox_policy(
    env: &[TaskEnvironmentVariable],
    secret_files: &[WorkloadSecretFile],
    volume_mounts: &[TaskVolumeMount],
) -> RuntimeSandboxPolicy {
    let allow_network = lookup_agent_bool_env(env, AGENT_ALLOW_NETWORK_ENV_VAR);
    let allow_write = lookup_agent_bool_env(env, AGENT_ALLOW_WRITE_ENV_VAR);
    let working_directory = lookup_agent_path_env(env, AGENT_WORKDIR_ENV_VAR);
    let mut filesystem = Vec::new();

    for mount in volume_mounts {
        add_or_widen_sandbox_rule(
            &mut filesystem,
            RuntimeSandboxPathRule::directory(
                mount.target.clone(),
                sandbox_access_for_mount(mount.read_only, allow_write),
            ),
        );
    }

    for secret in secret_files {
        add_or_widen_sandbox_rule(
            &mut filesystem,
            RuntimeSandboxPathRule::file(secret.path.clone(), RuntimeSandboxAccessMode::Read),
        );
    }

    if allow_write {
        add_or_widen_sandbox_rule(
            &mut filesystem,
            RuntimeSandboxPathRule::directory("/tmp", RuntimeSandboxAccessMode::ReadWrite),
        );
        add_or_widen_sandbox_rule(
            &mut filesystem,
            RuntimeSandboxPathRule::directory("/var/tmp", RuntimeSandboxAccessMode::ReadWrite),
        );
    }

    if let Some(path) = working_directory.as_ref() {
        add_or_widen_sandbox_rule(
            &mut filesystem,
            RuntimeSandboxPathRule::directory(
                path.clone(),
                if allow_write {
                    RuntimeSandboxAccessMode::ReadWrite
                } else {
                    RuntimeSandboxAccessMode::Read
                },
            ),
        );
    }

    RuntimeSandboxPolicy {
        working_directory,
        filesystem,
        network: if allow_network {
            RuntimeSandboxNetworkMode::AllowAll
        } else {
            RuntimeSandboxNetworkMode::Blocked
        },
    }
}

/// Returns the effective access mode one mounted path should receive under the sandbox.
fn sandbox_access_for_mount(read_only: bool, allow_write: bool) -> RuntimeSandboxAccessMode {
    if read_only || !allow_write {
        RuntimeSandboxAccessMode::Read
    } else {
        RuntimeSandboxAccessMode::ReadWrite
    }
}

/// Looks up one agent boolean env var from the execution template using Mantissa semantics.
fn lookup_agent_bool_env(env: &[TaskEnvironmentVariable], name: &str) -> bool {
    lookup_agent_env(env, name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Looks up one agent path env var from the execution template after trimming empty values.
fn lookup_agent_path_env(env: &[TaskEnvironmentVariable], name: &str) -> Option<PathBuf> {
    lookup_agent_env(env, name).map(PathBuf::from)
}

/// Returns the last literal env value declared for one agent runtime hint.
fn lookup_agent_env<'a>(env: &'a [TaskEnvironmentVariable], name: &str) -> Option<&'a str> {
    env.iter()
        .rev()
        .find(|entry| entry.name == name)
        .and_then(|entry| entry.value.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// Adds one path rule to the sandbox policy, widening exact duplicates instead of repeating them.
fn add_or_widen_sandbox_rule(
    filesystem: &mut Vec<RuntimeSandboxPathRule>,
    candidate: RuntimeSandboxPathRule,
) {
    if let Some(existing) = filesystem
        .iter_mut()
        .find(|rule| rule.kind == candidate.kind && rule.path == candidate.path)
    {
        existing.access = widen_sandbox_access(existing.access, candidate.access);
        return;
    }

    filesystem.push(candidate);
}

/// Collapses two access modes so the resulting rule preserves the broadest required permission.
fn widen_sandbox_access(
    current: RuntimeSandboxAccessMode,
    candidate: RuntimeSandboxAccessMode,
) -> RuntimeSandboxAccessMode {
    use RuntimeSandboxAccessMode::{Read, ReadWrite, Write};

    match (current, candidate) {
        (ReadWrite, _) | (_, ReadWrite) => ReadWrite,
        (Read, Write) | (Write, Read) => ReadWrite,
        (Write, Write) => Write,
        _ => Read,
    }
}

/// Converts one workload port binding into the runtime backend create request format.
fn runtime_port_binding_from_workload(binding: &WorkloadPortBinding) -> RuntimePortBinding {
    RuntimePortBinding {
        target_port: binding.target_port,
        host_port: binding.host_port,
        host_ip: binding.host_ip.clone(),
        protocol: match binding.protocol {
            WorkloadPortProtocol::Tcp => RuntimePortProtocol::Tcp,
            WorkloadPortProtocol::Udp => RuntimePortProtocol::Udp,
        },
    }
}

/// Maps a task restart policy into the runtime restart-policy payload.
fn restart_policy_to_config(policy: &WorkloadRestartPolicy) -> RestartPolicyConfig {
    RestartPolicyConfig {
        name: match policy.name {
            WorkloadRestartPolicyKind::No => RestartPolicyType::No,
            WorkloadRestartPolicyKind::Always => RestartPolicyType::Always,
            WorkloadRestartPolicyKind::OnFailure => RestartPolicyType::OnFailure,
            WorkloadRestartPolicyKind::UnlessStopped => RestartPolicyType::UnlessStopped,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::model::{WorkloadSecretReference, WorkloadVolumeMount};

    #[test]
    fn agent_runtime_sandbox_policy_downscopes_rw_mounts_without_write_access() {
        let policy = build_agent_runtime_sandbox_policy(
            &[
                TaskEnvironmentVariable {
                    name: AGENT_ALLOW_NETWORK_ENV_VAR.to_string(),
                    value: Some("false".to_string()),
                    secret: None,
                },
                TaskEnvironmentVariable {
                    name: AGENT_ALLOW_WRITE_ENV_VAR.to_string(),
                    value: Some("false".to_string()),
                    secret: None,
                },
                TaskEnvironmentVariable {
                    name: AGENT_WORKDIR_ENV_VAR.to_string(),
                    value: Some("/workspace".to_string()),
                    secret: None,
                },
            ],
            &[WorkloadSecretFile {
                path: "/run/secrets/token".to_string(),
                secret: WorkloadSecretReference {
                    name: "token".to_string(),
                    version_id: None,
                },
                mode: None,
                ownership: crate::volumes::types::LocalVolumeOwnership::Daemon,
                path_env_name: None,
            }],
            &[
                WorkloadVolumeMount {
                    volume_id: Uuid::new_v4(),
                    volume_name: "workspace".to_string(),
                    target: "/workspace".to_string(),
                    read_only: false,
                },
                WorkloadVolumeMount {
                    volume_id: Uuid::new_v4(),
                    volume_name: "cache".to_string(),
                    target: "/cache".to_string(),
                    read_only: true,
                },
            ],
        );

        assert_eq!(policy.network, RuntimeSandboxNetworkMode::Blocked);
        assert_eq!(policy.working_directory, Some(PathBuf::from("/workspace")));
        assert!(policy.filesystem.iter().any(|rule| {
            rule.path == std::path::Path::new("/workspace")
                && rule.kind == crate::runtime::types::RuntimeSandboxPathKind::Directory
                && rule.access == RuntimeSandboxAccessMode::Read
        }));
        assert!(policy.filesystem.iter().any(|rule| {
            rule.path == std::path::Path::new("/cache")
                && rule.kind == crate::runtime::types::RuntimeSandboxPathKind::Directory
                && rule.access == RuntimeSandboxAccessMode::Read
        }));
        assert!(policy.filesystem.iter().any(|rule| {
            rule.path == std::path::Path::new("/run/secrets/token")
                && rule.kind == crate::runtime::types::RuntimeSandboxPathKind::File
                && rule.access == RuntimeSandboxAccessMode::Read
        }));
        assert!(!policy.filesystem.iter().any(|rule| {
            rule.path == std::path::Path::new("/tmp")
                && rule.access == RuntimeSandboxAccessMode::ReadWrite
        }));
    }

    #[test]
    fn agent_runtime_sandbox_policy_enables_workdir_and_temp_writes() {
        let policy = build_agent_runtime_sandbox_policy(
            &[
                TaskEnvironmentVariable {
                    name: AGENT_ALLOW_NETWORK_ENV_VAR.to_string(),
                    value: Some("true".to_string()),
                    secret: None,
                },
                TaskEnvironmentVariable {
                    name: AGENT_ALLOW_WRITE_ENV_VAR.to_string(),
                    value: Some("true".to_string()),
                    secret: None,
                },
                TaskEnvironmentVariable {
                    name: AGENT_WORKDIR_ENV_VAR.to_string(),
                    value: Some("/workspace".to_string()),
                    secret: None,
                },
            ],
            &[],
            &[],
        );

        assert_eq!(policy.network, RuntimeSandboxNetworkMode::AllowAll);
        assert!(policy.filesystem.iter().any(|rule| {
            rule.path == std::path::Path::new("/workspace")
                && rule.kind == crate::runtime::types::RuntimeSandboxPathKind::Directory
                && rule.access == RuntimeSandboxAccessMode::ReadWrite
        }));
        assert!(policy.filesystem.iter().any(|rule| {
            rule.path == std::path::Path::new("/tmp")
                && rule.kind == crate::runtime::types::RuntimeSandboxPathKind::Directory
                && rule.access == RuntimeSandboxAccessMode::ReadWrite
        }));
        assert!(policy.filesystem.iter().any(|rule| {
            rule.path == std::path::Path::new("/var/tmp")
                && rule.kind == crate::runtime::types::RuntimeSandboxPathKind::Directory
                && rule.access == RuntimeSandboxAccessMode::ReadWrite
        }));
    }
}
