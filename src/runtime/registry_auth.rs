//! Registry credential parsing shared by container runtime providers.

use std::collections::BTreeMap;
use std::fmt;

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use zeroize::{Zeroize, Zeroizing};

use super::types::{RuntimeError, RuntimeResult};

pub(crate) const DOCKER_HUB_ADDRESS: &str = "https://index.docker.io/v1/";

/// Login data for one image registry, passed to a runtime only for image pulls.
#[derive(Clone, Default)]
pub struct RegistryAuth {
    pub registry: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub identity_token: Option<String>,
    pub registry_token: Option<String>,
}

impl fmt::Debug for RegistryAuth {
    /// Keeps passwords and tokens out of runtime request diagnostics.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistryAuth")
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

impl Drop for RegistryAuth {
    /// Clears credential strings when the image pull no longer needs them.
    fn drop(&mut self) {
        self.username.zeroize();
        self.password.zeroize();
        self.identity_token.zeroize();
        self.registry_token.zeroize();
    }
}

impl RegistryAuth {
    /// Selects the image's credentials from a registry_auth secret containing inline auths.
    pub fn from_config(bytes: &[u8], image: &str) -> RuntimeResult<Self> {
        let config = RegistryAuthConfig::from_bytes(bytes)?;
        if !config.creds_store.is_empty() || !config.cred_helpers.is_empty() {
            return Err(RuntimeError::OperationFailed(
                "registry_auth secrets must contain inline auths. Configure credential helpers on the node"
                    .to_string(),
            ));
        }

        let registry = image_registry(image);
        config.auth_for_registry(&registry)?.ok_or_else(|| {
            RuntimeError::OperationFailed(format!(
                "registry_auth has no credentials for registry {registry}"
            ))
        })
    }

    /// Prevents a runtime from sending explicitly supplied credentials to another registry.
    pub fn validate_image(&self, image: &str) -> RuntimeResult<()> {
        if canonical_registry(&self.registry) != image_registry(image) {
            return Err(RuntimeError::OperationFailed(
                "registry_auth does not match the image registry".to_string(),
            ));
        }

        Ok(())
    }
}

/// Docker configuration used to select credentials or a helper for the image's registry.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct RegistryAuthConfig {
    auths: BTreeMap<String, StoredRegistryAuth>,
    #[serde(rename = "credsStore")]
    pub(crate) creds_store: String,
    #[serde(rename = "credHelpers")]
    pub(crate) cred_helpers: BTreeMap<String, String>,
}

/// One auths entry, kept separate from runtime credentials until its registry is selected.
#[derive(Default, Deserialize)]
struct StoredRegistryAuth {
    auth: Option<String>,
    username: Option<String>,
    password: Option<String>,
    identitytoken: Option<String>,
    registrytoken: Option<String>,
}

impl Drop for StoredRegistryAuth {
    /// Clears parsed configuration entries after selecting credentials for the requested registry.
    fn drop(&mut self) {
        self.auth.zeroize();
        self.username.zeroize();
        self.password.zeroize();
        self.identitytoken.zeroize();
        self.registrytoken.zeroize();
    }
}

impl RegistryAuthConfig {
    /// Suppresses JSON parser details that could quote secret values in an error.
    pub(crate) fn from_bytes(bytes: &[u8]) -> RuntimeResult<Self> {
        serde_json::from_slice(bytes).map_err(|_| {
            RuntimeError::OperationFailed(
                "invalid registry authentication configuration".to_string(),
            )
        })
    }

    /// Selects a registry override before the node's default credential helper.
    pub(crate) fn helper(&self, registry: &str) -> &str {
        registry_entry(&self.cred_helpers, registry)
            .map(String::as_str)
            .unwrap_or(&self.creds_store)
    }

    /// Decodes the selected registry's auths entry without forwarding unrelated logins.
    pub(crate) fn auth_for_registry(&self, registry: &str) -> RuntimeResult<Option<RegistryAuth>> {
        let Some(stored) = registry_entry(&self.auths, registry) else {
            return Ok(None);
        };

        let mut credentials = RegistryAuth {
            registry: registry.to_string(),
            username: stored.username.clone(),
            password: stored.password.clone(),
            identity_token: stored.identitytoken.clone(),
            registry_token: stored.registrytoken.clone(),
        };

        if let Some(auth) = stored.auth.as_deref().filter(|auth| !auth.is_empty()) {
            let decoded =
                Zeroizing::new(STANDARD.decode(auth).map_err(|_| invalid_auth(registry))?);
            let decoded = std::str::from_utf8(&decoded).map_err(|_| invalid_auth(registry))?;
            let (username, password) = decoded
                .split_once(':')
                .filter(|(username, _)| !username.is_empty())
                .ok_or_else(|| invalid_auth(registry))?;

            credentials.username = Some(username.to_string());
            credentials.password = Some(password.trim_matches('\0').to_string());
        }

        let has_credentials = [
            credentials.username.as_deref(),
            credentials.identity_token.as_deref(),
            credentials.registry_token.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|value| !value.is_empty());

        Ok(has_credentials.then_some(credentials))
    }
}

/// Selects the image's registry, treating unqualified names as Docker Hub references.
pub(crate) fn image_registry(image: &str) -> String {
    let registry = match image.split_once('/') {
        Some((host, _))
            if host.contains(['.', ':'])
                || host == "localhost"
                || host.bytes().any(|byte| byte.is_ascii_uppercase()) =>
        {
            host
        }
        _ => "docker.io",
    };

    canonical_registry(registry)
}

/// Makes registry keys comparable and uses Docker Hub's index URL for its credential helpers.
fn canonical_registry(address: &str) -> String {
    let address = address
        .strip_prefix("https://")
        .or_else(|| address.strip_prefix("http://"))
        .unwrap_or(address);
    let host = address
        .split('/')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();

    match host.as_str() {
        "docker.io" | "index.docker.io" | "registry-1.docker.io" => DOCKER_HUB_ADDRESS.to_string(),
        _ => host,
    }
}

/// Prefers an exact credential key before checking equivalent registry addresses.
fn registry_entry<'a, T>(entries: &'a BTreeMap<String, T>, registry: &str) -> Option<&'a T> {
    entries.get(registry).or_else(|| {
        entries
            .iter()
            .find_map(|(address, entry)| (canonical_registry(address) == registry).then_some(entry))
    })
}

/// Reports malformed authentication without exposing its encoded or decoded value.
fn invalid_auth(registry: &str) -> RuntimeError {
    RuntimeError::OperationFailed(format!("invalid registry authentication for {registry}"))
}

#[cfg(test)]
mod tests;
