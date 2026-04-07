use super::{WorkloadManager, WorkloadStartRequest};
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::types::SecretValue;
use crate::volumes::types::LocalVolumeOwnership;
use crate::workload::model::{
    WorkloadEnvironmentVariable as TaskEnvironmentVariable, WorkloadSecretFile,
};
use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::warn;
use uuid::Uuid;

/// On-disk artifacts populated when staging secret material for a runtime launch.
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

/// Fully resolved secret metadata ready to be supplied to the runtime backend.
pub(super) struct ResolvedTaskSecrets {
    pub env: Vec<String>,
    pub mounts: Vec<String>,
    pub artifacts: Option<TaskSecretArtifacts>,
}

impl WorkloadManager {
    /// Ensures every task start request references secrets that exist locally with compatible versions.
    pub(super) fn ensure_secret_dependencies(
        &self,
        requests: &[WorkloadStartRequest],
    ) -> Result<()> {
        for request in requests {
            for var in &request.env {
                if let Some(secret) = &var.secret {
                    self.load_secret_value(&secret.name)?;
                }
            }
            for file in &request.secret_files {
                self.load_secret_value(&file.secret.name)?;
            }
        }
        Ok(())
    }

    /// Resolves environment variables and secret file projections into concrete runtime artifacts.
    ///
    /// This is invoked when the scheduler hands control to the WorkloadManager for a local launch.
    /// It performs validation, decrypts referenced secrets and stages any file material on disk
    /// so the runtime backend can bind-mount them into the new instance.
    pub(super) async fn resolve_runtime_secrets(
        &self,
        task_id: Uuid,
        env: &[TaskEnvironmentVariable],
        secret_files: &[WorkloadSecretFile],
    ) -> Result<ResolvedTaskSecrets> {
        // Clear any stale staging content in case a previous attempt failed.
        self.cleanup_secret_artifacts(task_id).await;

        let keyring = { self.secrets.secret_keyring.read().await.clone() };
        let mut value_cache: HashMap<String, SecretValue> = HashMap::new();
        let mut plaintext_cache: HashMap<Uuid, Arc<[u8]>> = HashMap::new();

        let mut resolved_env = Vec::with_capacity(env.len());
        for var in env {
            resolved_env.push(self.build_env_assignment(
                var,
                &mut value_cache,
                &mut plaintext_cache,
                &keyring,
            )?);
        }

        let (mounts, path_env, artifacts) = self
            .stage_secret_files(
                task_id,
                secret_files,
                &mut value_cache,
                &mut plaintext_cache,
                &keyring,
            )
            .await?;
        resolved_env.extend(path_env);

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
        let root_dir = self.secrets.secret_runtime_root.join(task_id.to_string());
        match fs::remove_dir_all(&root_dir).await {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                warn!(
                    target: "task",
                    "failed to cleanup secret staging directory {} for task {}: {err}",
                    root_dir.display(),
                    task_id
                );
            }
        }
    }

    /// Constructs an environment variable assignment, decrypting a referenced secret when needed.
    fn build_env_assignment(
        &self,
        var: &TaskEnvironmentVariable,
        value_cache: &mut HashMap<String, SecretValue>,
        plaintext_cache: &mut HashMap<Uuid, Arc<[u8]>>,
        keyring: &SecretKeyring,
    ) -> Result<String> {
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
                let plaintext = self.decrypt_secret_cached(
                    &secret_ref.name,
                    value_cache,
                    plaintext_cache,
                    keyring,
                )?;
                let value = String::from_utf8(plaintext.as_ref().to_vec()).map_err(|_| {
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

    /// Constructs one plain environment variable assignment that points at a mounted secret path.
    fn build_path_env_assignment(&self, name: &str, path: &str) -> Result<String> {
        let name = name.trim();
        if name.is_empty() {
            return Err(anyhow!(
                "secret file path_env_name cannot be empty when launching task"
            ));
        }
        if name.contains('=') {
            return Err(anyhow!(
                "secret file path_env_name '{name}' cannot contain '='"
            ));
        }
        Ok(format!("{name}={path}"))
    }

    /// Stages secret file payloads on disk and builds the bind mount descriptors for Docker.
    async fn stage_secret_files(
        &self,
        task_id: Uuid,
        files: &[WorkloadSecretFile],
        value_cache: &mut HashMap<String, SecretValue>,
        plaintext_cache: &mut HashMap<Uuid, Arc<[u8]>>,
        keyring: &SecretKeyring,
    ) -> Result<(Vec<String>, Vec<String>, Option<TaskSecretArtifacts>)> {
        if files.is_empty() {
            return Ok((Vec::new(), Vec::new(), None));
        }

        let root_dir = self.secrets.secret_runtime_root.join(task_id.to_string());
        if let Err(err) = fs::remove_dir_all(&root_dir).await
            && err.kind() != ErrorKind::NotFound
        {
            return Err(anyhow!(
                "failed to reset secret staging directory {}: {err}",
                root_dir.display()
            ));
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

            if let Err(err) =
                fs::set_permissions(&root_dir, std::fs::Permissions::from_mode(0o700)).await
            {
                cleanup_dir_quietly(&root_dir).await;
                return Err(anyhow!("failed to secure {}: {err}", root_dir.display()));
            }
        }

        let mut mounts = Vec::with_capacity(files.len());
        let mut path_env = Vec::new();

        for (idx, file) in files.iter().enumerate() {
            let target = file.path.trim();
            if target.is_empty() {
                cleanup_dir_quietly(&root_dir).await;
                return Err(anyhow!(
                    "secret file projection requires a non-empty runtime filesystem path"
                ));
            }
            if !target.starts_with('/') {
                cleanup_dir_quietly(&root_dir).await;
                return Err(anyhow!(
                    "secret file target '{}' must be an absolute runtime filesystem path",
                    target
                ));
            }
            let path_env_assignment = if let Some(name) = file.path_env_name.as_deref() {
                match self.build_path_env_assignment(name, target) {
                    Ok(value) => Some(value),
                    Err(err) => {
                        cleanup_dir_quietly(&root_dir).await;
                        return Err(err);
                    }
                }
            } else {
                None
            };

            let plaintext = match self.decrypt_secret_cached(
                &file.secret.name,
                value_cache,
                plaintext_cache,
                keyring,
            ) {
                Ok(bytes) => bytes,
                Err(err) => {
                    cleanup_dir_quietly(&root_dir).await;
                    return Err(err);
                }
            };

            let host_path = root_dir.join(format!("secret-{idx}"));
            match fs::remove_file(&host_path).await {
                Ok(_) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => {
                    cleanup_dir_quietly(&root_dir).await;
                    return Err(anyhow!(
                        "failed to reset secret staging file {}: {err}",
                        host_path.display()
                    ));
                }
            }

            let mut options = OpenOptions::new();
            options.write(true).create_new(true);

            #[cfg(unix)]
            {
                // Use a permissive mode during creation to avoid permission races on filesystems
                // that reject immediate read-only creation, then tighten permissions below.
                options.mode(0o600);
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

            if let Err(err) = handle.write_all(plaintext.as_ref()).await {
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
                if let Err(err) = normalize_staged_secret_file_permissions(&host_path, file) {
                    cleanup_dir_quietly(&root_dir).await;
                    return Err(anyhow!(
                        "failed to apply ownership or permissions on {}: {err}",
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
            if let Some(path_env_assignment) = path_env_assignment {
                path_env.push(path_env_assignment);
            }
        }

        Ok((mounts, path_env, Some(TaskSecretArtifacts { root_dir })))
    }

    /// Loads the current value for a secret by logical name.
    fn load_secret_value(&self, name: &str) -> Result<SecretValue> {
        let name = name.trim();
        if name.is_empty() {
            return Err(anyhow!("secret reference name cannot be empty"));
        }

        self.secrets
            .secret_registry
            .get_by_name(name)
            .map_err(|e| anyhow!("failed to lookup secret '{name}': {e}"))?
            .ok_or_else(|| anyhow!("secret '{name}' not found"))
    }

    /// Decrypts a secret reference, caching metadata and plaintext so repeated lookups stay cheap.
    fn decrypt_secret_cached(
        &self,
        name: &str,
        value_cache: &mut HashMap<String, SecretValue>,
        plaintext_cache: &mut HashMap<Uuid, Arc<[u8]>>,
        keyring: &SecretKeyring,
    ) -> Result<Arc<[u8]>> {
        let key = name.trim().to_string();
        if key.is_empty() {
            return Err(anyhow!("secret reference name cannot be empty"));
        }

        let value = if let Some(value) = value_cache.get(&key) {
            value.clone()
        } else {
            let value = self.load_secret_value(&key)?;
            value_cache.insert(key.clone(), value.clone());
            value
        };

        let version_id = value.current_version.version_id;
        if let Some(bytes) = plaintext_cache.get(&version_id) {
            return Ok(bytes.clone());
        }

        let plaintext = keyring
            .decrypt(value.id, version_id, &value.current_version.ciphertext)
            .map_err(|e| anyhow!("failed to decrypt secret '{}': {e}", key))?;

        let arc: Arc<[u8]> = Arc::from(plaintext.into_boxed_slice());
        plaintext_cache.insert(version_id, arc.clone());
        Ok(arc)
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

/// Applies the declared ownership and mode to one staged secret file on Unix hosts.
#[cfg(unix)]
fn normalize_staged_secret_file_permissions(path: &Path, file: &WorkloadSecretFile) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = std::fs::metadata(path).with_context(|| {
        format!(
            "failed to read staged secret file metadata for {}",
            path.display()
        )
    })?;
    let (daemon_uid, daemon_gid) = current_process_ids();
    let (desired_uid, desired_gid) = file.ownership.resolve_ids(daemon_uid, daemon_gid);
    if metadata.uid() != desired_uid || metadata.gid() != desired_gid {
        chown_path(path, desired_uid, desired_gid).with_context(|| {
            format!(
                "failed to set staged secret file owner {desired_uid}:{desired_gid} on {}",
                path.display()
            )
        })?;
    }

    let desired_mode = file
        .mode
        .unwrap_or(default_secret_file_mode(file.ownership));
    let current_mode = metadata.permissions().mode() & 0o7777;
    if current_mode != desired_mode {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(desired_mode))
            .with_context(|| {
                format!(
                    "failed to set staged secret file mode {:o} on {}",
                    desired_mode,
                    path.display()
                )
            })?;
    }
    Ok(())
}

/// Leaves staged secret ownership normalization as a no-op on non-Unix hosts.
#[cfg(not(unix))]
fn normalize_staged_secret_file_permissions(
    _path: &Path,
    _file: &WorkloadSecretFile,
) -> Result<()> {
    Ok(())
}

/// Returns the default POSIX mode Mantissa should use for one staged secret file.
fn default_secret_file_mode(ownership: LocalVolumeOwnership) -> u32 {
    match ownership {
        LocalVolumeOwnership::Daemon | LocalVolumeOwnership::User { .. } => 0o400,
        LocalVolumeOwnership::FsGroup { .. } => 0o440,
    }
}

/// Returns the uid and gid of the running Mantissa daemon process.
#[cfg(unix)]
fn current_process_ids() -> (u32, u32) {
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    (uid, gid)
}

/// Changes the uid and gid of one staged secret file in place.
#[cfg(unix)]
fn chown_path(path: &Path, uid: u32, gid: u32) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| anyhow!("invalid path for chown: {}", path.display()))?;
    let result = unsafe { libc::chown(c_path.as_ptr(), uid, gid) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
            .with_context(|| format!("failed to chown staged secret file path {}", path.display()))
    }
}
