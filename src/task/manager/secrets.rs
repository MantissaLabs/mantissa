use super::TaskManager;
use crate::task::types::{TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference};
use anyhow::{Context, Result, anyhow};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::warn;
use uuid::Uuid;

/// On-disk artifacts populated when staging secret material for a container launch.
#[derive(Clone)]
pub(super) struct TaskSecretArtifacts {
    root_dir: PathBuf,
}

impl TaskSecretArtifacts {
    /// Deletes the staging directory associated with a task secret injection.
    pub async fn cleanup(self) -> Result<()> {
        match fs::remove_dir_all(&self.root_dir).await {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(anyhow!(
                "failed to remove secret staging directory {}: {err}",
                self.root_dir.display()
            )),
        }
    }
}

/// Fully resolved secret metadata ready to be supplied to the container runtime.
pub(super) struct ResolvedTaskSecrets {
    pub env: Vec<String>,
    pub mounts: Vec<String>,
    pub artifacts: Option<TaskSecretArtifacts>,
}

impl TaskManager {
    /// Resolves environment variables and secret file projections into concrete runtime artifacts.
    ///
    /// This is invoked when the scheduler hands control to the TaskManager for a local launch.
    /// It performs validation, decrypts referenced secrets and stages any file material on disk
    /// so the Docker integration can bind-mount them into the new container.
    pub(super) async fn resolve_runtime_secrets(
        &self,
        task_id: Uuid,
        env: &[TaskEnvironmentVariable],
        secret_files: &[TaskSecretFile],
    ) -> Result<ResolvedTaskSecrets> {
        // Clear any stale staging content in case a previous attempt failed.
        self.cleanup_secret_artifacts(task_id).await;

        let mut resolved_env = Vec::with_capacity(env.len());
        for var in env {
            resolved_env.push(self.build_env_assignment(var)?);
        }

        let (mounts, artifacts) = self.stage_secret_files(task_id, secret_files).await?;

        Ok(ResolvedTaskSecrets {
            env: resolved_env,
            mounts,
            artifacts,
        })
    }

    /// Removes secret staging directories for `task_id` if present.
    ///
    /// This is used when tasks stop or fail so we do not leave decrypted material behind.
    pub(super) async fn cleanup_secret_artifacts(&self, task_id: Uuid) {
        let artifacts = {
            let mut guard = self.secret_artifacts.lock().await;
            guard.remove(&task_id)
        };

        if let Some(artifacts) = artifacts {
            if let Err(err) = artifacts.cleanup().await {
                warn!(
                    target: "task",
                    "failed to cleanup secret artifacts for task {}: {err}",
                    task_id
                );
            }
        }
    }

    /// Constructs an environment variable assignment, decrypting a referenced secret when needed.
    fn build_env_assignment(&self, var: &TaskEnvironmentVariable) -> Result<String> {
        let name = var.name.trim();
        if name.is_empty() {
            return Err(anyhow!(
                "environment variable name cannot be empty when launching task"
            ));
        }
        if name.contains('=') {
            return Err(anyhow!(
                "environment variable name '{name}' cannot contain '='"
            ));
        }

        match (&var.value, &var.secret) {
            (Some(value), None) => Ok(format!("{name}={value}")),
            (None, Some(secret_ref)) => {
                let plaintext = self.load_secret_plaintext(secret_ref)?;
                let value = String::from_utf8(plaintext).map_err(|_| {
                    anyhow!(
                        "secret '{}' contains non UTF-8 data and cannot populate env var '{}'",
                        secret_ref.name,
                        name
                    )
                })?;
                Ok(format!("{name}={value}"))
            }
            (Some(_), Some(_)) => Err(anyhow!(
                "environment variable '{name}' cannot specify both value and secret reference"
            )),
            (None, None) => Err(anyhow!(
                "environment variable '{name}' missing value or secret reference"
            )),
        }
    }

    /// Stages secret file payloads on disk and builds the bind mount descriptors for Docker.
    async fn stage_secret_files(
        &self,
        task_id: Uuid,
        files: &[TaskSecretFile],
    ) -> Result<(Vec<String>, Option<TaskSecretArtifacts>)> {
        if files.is_empty() {
            return Ok((Vec::new(), None));
        }

        let root_dir = self.secret_runtime_root.join(task_id.to_string());
        if let Err(err) = fs::remove_dir_all(&root_dir).await {
            if err.kind() != ErrorKind::NotFound {
                return Err(anyhow!(
                    "failed to reset secret staging directory {}: {err}",
                    root_dir.display()
                ));
            }
        }

        fs::create_dir_all(&root_dir).await.with_context(|| {
            format!(
                "failed to create secret staging directory {}",
                root_dir.display()
            )
        })?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let metadata = fs::metadata(&root_dir)
                .await
                .with_context(|| format!("failed to inspect {}", root_dir.display()))?;
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o700);
            fs::set_permissions(&root_dir, permissions)
                .await
                .with_context(|| format!("failed to secure {}", root_dir.display()))?;
        }

        let mut mounts = Vec::with_capacity(files.len());

        for (idx, file) in files.iter().enumerate() {
            let target = file.path.trim();
            if target.is_empty() {
                cleanup_dir_quietly(&root_dir).await;
                return Err(anyhow!(
                    "secret file projection requires a non-empty container path"
                ));
            }
            if !target.starts_with('/') {
                cleanup_dir_quietly(&root_dir).await;
                return Err(anyhow!(
                    "secret file target '{}' must be an absolute container path",
                    target
                ));
            }

            let plaintext = match self.load_secret_plaintext(&file.secret) {
                Ok(bytes) => bytes,
                Err(err) => {
                    cleanup_dir_quietly(&root_dir).await;
                    return Err(err);
                }
            };

            let host_path = root_dir.join(format!("secret-{idx}"));
            let mut options = OpenOptions::new();
            options.write(true).create(true).truncate(true);

            #[cfg(unix)]
            {
                options.mode(file.mode.unwrap_or(0o400));
            }

            let mut handle = match options.open(&host_path).await {
                Ok(file) => file,
                Err(err) => {
                    cleanup_dir_quietly(&root_dir).await;
                    return Err(anyhow!(
                        "failed to open secret staging file {}: {err}",
                        host_path.display()
                    ));
                }
            };

            if let Err(err) = handle.write_all(&plaintext).await {
                cleanup_dir_quietly(&root_dir).await;
                return Err(anyhow!(
                    "failed to write secret '{}' into {}: {err}",
                    file.secret.name,
                    host_path.display()
                ));
            }

            if let Err(err) = handle.flush().await {
                cleanup_dir_quietly(&root_dir).await;
                return Err(anyhow!(
                    "failed to flush secret staging file {}: {err}",
                    host_path.display()
                ));
            }

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = file.mode.unwrap_or(0o400);
                if let Err(err) =
                    fs::set_permissions(&host_path, std::fs::Permissions::from_mode(mode)).await
                {
                    cleanup_dir_quietly(&root_dir).await;
                    return Err(anyhow!(
                        "failed to set permissions on {}: {err}",
                        host_path.display()
                    ));
                }
            }

            let host = match host_path.to_str() {
                Some(value) => value.to_string(),
                None => {
                    cleanup_dir_quietly(&root_dir).await;
                    return Err(anyhow!(
                        "secret staging path '{}' contains invalid UTF-8",
                        host_path.display()
                    ));
                }
            };

            mounts.push(format!("{host}:{target}:ro"));
        }

        Ok((mounts, Some(TaskSecretArtifacts { root_dir })))
    }

    /// Loads and decrypts the secret referenced by `reference`.
    fn load_secret_plaintext(&self, reference: &TaskSecretReference) -> Result<Vec<u8>> {
        let name = reference.name.trim();
        if name.is_empty() {
            return Err(anyhow!("secret reference name cannot be empty"));
        }

        let value = self
            .secret_registry
            .get_by_name(name)
            .map_err(|e| anyhow!("failed to lookup secret '{name}': {e}"))?
            .ok_or_else(|| anyhow!("secret '{name}' not found"))?;

        let current_version = value.current_version.version_id;
        if let Some(expected) = reference.version_id {
            if expected != current_version {
                return Err(anyhow!(
                    "secret '{name}' version mismatch: expected {}, found {}",
                    expected,
                    current_version
                ));
            }
        }

        self.secret_keyring
            .decrypt(value.id, current_version, &value.current_version.ciphertext)
            .map_err(|e| anyhow!("failed to decrypt secret '{name}': {e}"))
    }
}

async fn cleanup_dir_quietly(path: &Path) {
    match fs::remove_dir_all(path).await {
        Ok(_) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            warn!(
                target: "task",
                "failed to remove secret staging directory {} during rollback: {err}",
                path.display()
            );
        }
    }
}
