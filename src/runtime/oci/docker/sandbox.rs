//! Docker-side helper wiring for `nono` sandbox launches.
//!
//! This module stays on the Mantissa side of the boundary. It never applies
//! `nono` directly. Instead, it resolves the host helper path, rewrites Docker
//! create and exec requests to enter through the helper, and persists the
//! serialized sandbox policy on the container so later `docker exec` calls can
//! use the same kernel policy.

use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};

use bollard::query_parameters::InspectContainerOptions;

use crate::runtime::types::{RuntimeError, RuntimeResult, RuntimeSandboxPolicy};

use super::{
    DockerRuntimeBackend, DockerRuntimeMode, MANTISSA_NONO_ENABLED_LABEL,
    MANTISSA_NONO_HELPER_BINARY_NAME, MANTISSA_NONO_HELPER_CONTAINER_PATH,
    MANTISSA_NONO_HELPER_HOST_ENV_VAR, MANTISSA_NONO_POLICY_ENV_VAR,
};

/// Fully prepared Docker create settings after optional `nono` helper injection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PreparedSandboxedCreate {
    pub(super) entrypoint: Option<Vec<String>>,
    pub(super) command: Option<Vec<String>>,
    pub(super) env_vars: Option<Vec<String>>,
    pub(super) labels: Option<HashMap<String, String>>,
    pub(super) volumes: Option<Vec<String>>,
}

/// Fully prepared Docker exec settings after optional `nono` helper injection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PreparedSandboxedExec {
    pub(super) command: Vec<String>,
    pub(super) env_vars: Option<Vec<String>>,
    pub(super) working_dir: Option<String>,
}

/// Container-local metadata needed to route later exec calls back through the helper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SandboxedContainerMetadata {
    pub(super) encoded_policy: String,
    pub(super) working_dir: Option<String>,
}

impl DockerRuntimeBackend {
    /// Resolves the host-side helper binary path from explicit overrides or nearby binaries.
    pub(super) fn resolve_nono_helper_host_path() -> Option<PathBuf> {
        let mut candidates = Vec::new();
        if let Ok(path) = env::var(MANTISSA_NONO_HELPER_HOST_ENV_VAR) {
            candidates.push(PathBuf::from(path));
        }

        if let Ok(current_exe) = env::current_exe() {
            candidates.push(current_exe.with_file_name(MANTISSA_NONO_HELPER_BINARY_NAME));
            if let Some(parent) = current_exe.parent() {
                candidates.push(parent.join(MANTISSA_NONO_HELPER_BINARY_NAME));
                if parent.file_name().and_then(|value| value.to_str()) == Some("deps")
                    && let Some(grandparent) = parent.parent()
                {
                    candidates.push(grandparent.join(MANTISSA_NONO_HELPER_BINARY_NAME));
                }
            }
        }

        candidates
            .into_iter()
            .find_map(Self::canonical_nono_helper_path)
    }

    /// Rewrites one container create request so the helper becomes PID 1 when sandboxing is active.
    pub(super) async fn prepare_sandboxed_create(
        &self,
        image: &str,
        command: Option<Vec<String>>,
        env_vars: Option<Vec<String>>,
        labels: Option<HashMap<String, String>>,
        volumes: Option<Vec<String>>,
        sandbox_policy: Option<RuntimeSandboxPolicy>,
    ) -> RuntimeResult<PreparedSandboxedCreate> {
        let Some(policy) = sandbox_policy else {
            return Ok(PreparedSandboxedCreate {
                entrypoint: None,
                command,
                env_vars,
                labels,
                volumes,
            });
        };

        if !matches!(self.mode, DockerRuntimeMode::NonoSandbox) {
            return Ok(PreparedSandboxedCreate {
                entrypoint: None,
                command,
                env_vars,
                labels,
                volumes,
            });
        }

        let helper_path = self.ensure_nono_helper_host_path()?;
        let encoded_policy = policy
            .encode_env_value()
            .map_err(|err| RuntimeError::OperationFailed(err.to_string()))?;
        let target_command = self
            .resolve_effective_sandbox_command(image, command.as_deref())
            .await?;

        Ok(PreparedSandboxedCreate {
            entrypoint: Some(vec![MANTISSA_NONO_HELPER_CONTAINER_PATH.to_string()]),
            command: Some(target_command),
            env_vars: Some(merge_env_var(
                env_vars.unwrap_or_default(),
                MANTISSA_NONO_POLICY_ENV_VAR,
                encoded_policy,
            )),
            labels: Some(merge_label(
                labels.unwrap_or_default(),
                MANTISSA_NONO_ENABLED_LABEL,
                "true",
            )),
            volumes: Some(merge_bind_mount(
                volumes.unwrap_or_default(),
                helper_path,
                MANTISSA_NONO_HELPER_CONTAINER_PATH,
            )),
        })
    }

    /// Rewrites one Docker exec request so late commands re-enter through the helper when needed.
    pub(super) async fn prepare_sandboxed_exec(
        &self,
        container_id: &str,
        command: &[String],
    ) -> RuntimeResult<PreparedSandboxedExec> {
        if !matches!(self.mode, DockerRuntimeMode::NonoSandbox) {
            return Ok(PreparedSandboxedExec {
                command: command.to_vec(),
                env_vars: None,
                working_dir: None,
            });
        }

        let Some(metadata) = self
            .inspect_sandboxed_container_metadata(container_id)
            .await?
        else {
            return Ok(PreparedSandboxedExec {
                command: command.to_vec(),
                env_vars: None,
                working_dir: None,
            });
        };

        let mut wrapped_command = Vec::with_capacity(command.len() + 1);
        wrapped_command.push(MANTISSA_NONO_HELPER_CONTAINER_PATH.to_string());
        wrapped_command.extend_from_slice(command);

        Ok(PreparedSandboxedExec {
            command: wrapped_command,
            env_vars: Some(vec![format!(
                "{MANTISSA_NONO_POLICY_ENV_VAR}={}",
                metadata.encoded_policy
            )]),
            working_dir: metadata.working_dir,
        })
    }

    /// Returns the helper bind mount source path or a clear operator-facing error when it is missing.
    fn ensure_nono_helper_host_path(&self) -> RuntimeResult<&Path> {
        self.nono_helper_host_path.as_deref().ok_or_else(|| {
            RuntimeError::OperationFailed(format!(
                "sandboxed docker backend requires helper binary {}; set {} or place it next to the mantissa executable",
                MANTISSA_NONO_HELPER_BINARY_NAME,
                MANTISSA_NONO_HELPER_HOST_ENV_VAR
            ))
        })
    }

    /// Resolves the effective process command Docker would have launched without helper injection.
    async fn resolve_effective_sandbox_command(
        &self,
        image: &str,
        requested_command: Option<&[String]>,
    ) -> RuntimeResult<Vec<String>> {
        let inspect = self
            .docker
            .inspect_image(image)
            .await
            .map_err(|err| RuntimeError::backend(None, err.to_string()))?;
        let config = inspect.config.unwrap_or_default();

        resolve_effective_sandbox_command_parts(
            image,
            config.entrypoint.as_deref(),
            config.cmd.as_deref(),
            requested_command,
        )
    }

    /// Inspects one container and extracts the persisted helper metadata needed for later exec calls.
    async fn inspect_sandboxed_container_metadata(
        &self,
        container_id: &str,
    ) -> RuntimeResult<Option<SandboxedContainerMetadata>> {
        let inspect = self
            .run_runtime_call(
                container_id,
                self.docker
                    .inspect_container(container_id, Some(InspectContainerOptions { size: false })),
            )
            .await?;
        let Some(config) = inspect.config.as_ref() else {
            return Ok(None);
        };

        parse_sandboxed_container_metadata(
            config.labels.as_ref(),
            config.env.as_deref(),
            config.working_dir.as_deref(),
        )
    }

    /// Canonicalizes one helper path candidate and rejects missing or non-file results.
    fn canonical_nono_helper_path(path: PathBuf) -> Option<PathBuf> {
        let canonical = path.canonicalize().ok()?;
        canonical.is_file().then_some(canonical)
    }
}

/// Resolves the final target command the helper must `exec` once Docker has started it.
pub(super) fn resolve_effective_sandbox_command_parts(
    image: &str,
    image_entrypoint: Option<&[String]>,
    image_cmd: Option<&[String]>,
    requested_command: Option<&[String]>,
) -> RuntimeResult<Vec<String>> {
    let mut effective = collect_non_empty_command_parts(image_entrypoint);
    if let Some(command) = requested_command.filter(|value| !value.is_empty()) {
        effective.extend(command.iter().cloned());
    } else {
        effective.extend(collect_non_empty_command_parts(image_cmd));
    }

    if effective.is_empty() {
        return Err(RuntimeError::OperationFailed(format!(
            "sandboxed docker workload image {image} does not resolve to a runnable command"
        )));
    }

    Ok(effective)
}

/// Parses one container config into the helper metadata needed for later exec wrapping.
pub(super) fn parse_sandboxed_container_metadata(
    labels: Option<&HashMap<String, String>>,
    env_vars: Option<&[String]>,
    working_dir: Option<&str>,
) -> RuntimeResult<Option<SandboxedContainerMetadata>> {
    if labels
        .and_then(|entries| entries.get(MANTISSA_NONO_ENABLED_LABEL))
        .map(String::as_str)
        != Some("true")
    {
        return Ok(None);
    }

    let Some(encoded_policy) = find_env_var(env_vars, MANTISSA_NONO_POLICY_ENV_VAR) else {
        return Err(RuntimeError::OperationFailed(format!(
            "sandboxed container is missing {} in its persisted environment",
            MANTISSA_NONO_POLICY_ENV_VAR
        )));
    };

    Ok(Some(SandboxedContainerMetadata {
        encoded_policy,
        working_dir: normalize_optional_text(working_dir),
    }))
}

/// Collects non-empty command parts so image defaults with `[""]` reset markers do not leak through.
fn collect_non_empty_command_parts(parts: Option<&[String]>) -> Vec<String> {
    parts
        .into_iter()
        .flatten()
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .collect()
}

/// Adds or replaces one environment variable in Docker's `NAME=value` list representation.
fn merge_env_var(mut env_vars: Vec<String>, name: &str, value: String) -> Vec<String> {
    let prefix = format!("{name}=");
    if let Some(entry) = env_vars
        .iter_mut()
        .find(|entry| entry.starts_with(&prefix) || entry.as_str() == name)
    {
        *entry = format!("{prefix}{value}");
        return env_vars;
    }

    env_vars.push(format!("{prefix}{value}"));
    env_vars
}

/// Adds or replaces one label inside Docker's string map representation.
fn merge_label(
    mut labels: HashMap<String, String>,
    name: &str,
    value: &str,
) -> HashMap<String, String> {
    labels.insert(name.to_string(), value.to_string());
    labels
}

/// Adds the helper bind mount when the container does not already expose that target path.
fn merge_bind_mount(
    mut mounts: Vec<String>,
    host_path: &Path,
    container_path: &str,
) -> Vec<String> {
    if mounts
        .iter()
        .any(|mount| bind_mount_targets_path(mount, container_path))
    {
        return mounts;
    }

    mounts.push(format!("{}:{container_path}:ro", host_path.display()));
    mounts
}

/// Returns whether one Docker bind mount string already targets the provided container path.
fn bind_mount_targets_path(mount: &str, container_path: &str) -> bool {
    mount.split(':').nth(1) == Some(container_path)
}

/// Returns the value of one `NAME=value` environment entry when present.
fn find_env_var(env_vars: Option<&[String]>, name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    env_vars?
        .iter()
        .find_map(|entry| entry.strip_prefix(&prefix).map(str::to_string))
}

/// Normalizes one optional string by trimming and dropping empty values.
fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    (!value.is_empty()).then(|| value.to_string())
}
