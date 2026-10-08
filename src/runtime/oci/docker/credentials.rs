//! Resolves node-local Docker credentials before the runtime downloads an image.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use bollard::auth::DockerCredentials;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::timeout;
use zeroize::Zeroizing;

use crate::runtime::registry_auth::{RegistryAuth, RegistryAuthConfig, image_registry};
use crate::runtime::types::{RuntimeError, RuntimeResult};

const HELPER_TIMEOUT: Duration = Duration::from_secs(10);
const HELPER_OUTPUT_MAX_BYTES: u64 = 64 * 1024;

/// Finds the daemon user's Docker config so image pulls can reuse that user's registry logins.
pub(super) fn docker_config_path() -> Option<PathBuf> {
    std::env::var_os("DOCKER_CONFIG")
        .filter(|directory| !directory.is_empty())
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".docker")))
        .map(|directory| directory.join("config.json"))
}

/// Reloads credentials for each pull so login, logout, and rotation need no daemon restart.
pub(super) async fn resolve(
    config_path: Option<&Path>,
    image: &str,
) -> RuntimeResult<Option<RegistryAuth>> {
    let Some(path) = config_path else {
        return Ok(None);
    };

    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => Zeroizing::new(bytes),
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(RuntimeError::OperationFailed(format!(
                "cannot read Docker configuration {}: {error}",
                path.display()
            )));
        }
    };
    if bytes.is_empty() {
        return Ok(None);
    }

    // Deserialization errors can quote credential values, so report only the file location.
    let config = RegistryAuthConfig::from_bytes(&bytes).map_err(|_| {
        RuntimeError::OperationFailed(format!(
            "invalid Docker credential configuration in {}",
            path.display()
        ))
    })?;
    let registry = image_registry(image);
    let helper = config.helper(&registry);

    if !helper.is_empty() {
        // Docker helpers are found as docker-credential-<name> on PATH.
        // Reject path separators so configuration cannot select an arbitrary file.
        if !helper
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        {
            return Err(RuntimeError::OperationFailed(
                "invalid Docker credential helper name".to_string(),
            ));
        }

        let command = Command::new(format!("docker-credential-{helper}"));
        return credentials_from_helper(command, &registry, HELPER_TIMEOUT).await;
    }

    config.auth_for_registry(&registry)
}

/// Exchanges the helper's `get` request with bounded output and cancels stalled helper processes.
async fn credentials_from_helper(
    mut command: Command,
    registry: &str,
    deadline: Duration,
) -> RuntimeResult<Option<RegistryAuth>> {
    let program = command
        .as_std()
        .get_program()
        .to_string_lossy()
        .into_owned();
    let helper_error = |reason: &str| {
        RuntimeError::OperationFailed(format!("Docker credential helper {program}: {reason}"))
    };
    let mut child = command
        .arg("get")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| helper_error(&format!("cannot start: {error}")))?;

    let request = async {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| helper_error("stdin unavailable"))?;
        stdin
            .write_all(registry.as_bytes())
            .await
            .map_err(|_| helper_error("cannot write registry address"))?;
        // Helpers read the registry address until EOF before answering the get request.
        drop(stdin);

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| helper_error("stdout unavailable"))?;
        let mut output = Zeroizing::new(Vec::new());
        stdout
            .take(HELPER_OUTPUT_MAX_BYTES + 1)
            .read_to_end(&mut output)
            .await
            .map_err(|_| helper_error("cannot read credentials"))?;
        if output.len() as u64 > HELPER_OUTPUT_MAX_BYTES {
            return Err(helper_error("response exceeds 64 KiB"));
        }

        let status = child
            .wait()
            .await
            .map_err(|_| helper_error("cannot wait for process"))?;
        if !status.success() {
            // Docker helpers use this standard response when a registry has no saved login.
            if String::from_utf8_lossy(&output).contains("credentials not found in native keychain")
            {
                return Ok(None);
            }

            // A failing helper may print passwords or tokens, so errors must omit its output.
            return Err(helper_error("credential lookup failed"));
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct HelperCredentials {
            username: String,
            secret: String,
        }

        let response: HelperCredentials = serde_json::from_slice(&output)
            .map_err(|_| helper_error("invalid credential response"))?;
        if response.username.is_empty() || response.secret.is_empty() {
            return Err(helper_error("empty username or secret"));
        }

        let mut credentials = RegistryAuth {
            registry: registry.to_string(),
            username: None,
            password: None,
            identity_token: None,
            registry_token: None,
        };
        if response.username == "<token>" {
            // Docker helpers use this username to distinguish an identity token from a password.
            credentials.identity_token = Some(response.secret);
        } else {
            credentials.username = Some(response.username);
            credentials.password = Some(response.secret);
        }

        Ok(Some(credentials))
    };

    timeout(deadline, request)
        .await
        .map_err(|_| helper_error("credential lookup timed out"))?
}

impl From<&RegistryAuth> for DockerCredentials {
    /// Converts shared credentials only at the Docker API boundary.
    fn from(auth: &RegistryAuth) -> Self {
        Self {
            username: auth.username.clone(),
            password: auth.password.clone(),
            serveraddress: Some(auth.registry.clone()),
            identitytoken: auth.identity_token.clone(),
            registrytoken: auth.registry_token.clone(),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests;
