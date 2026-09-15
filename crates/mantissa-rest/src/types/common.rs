use crate::types::volumes::FilesystemOwnership;
use mantissa_client::host_ports::HostPortView;
use mantissa_client::services::manifest as config;
use mantissa_protocol::{volumes::filesystem_ownership, workload};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

/// REST-facing host-port binding shared by jobs and services.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct HostPort {
    pub name: String,
    pub target_port: u16,
    pub host_port: u16,
    pub host_ip: String,
    pub protocol: String,
}

impl From<HostPortView> for HostPort {
    /// Converts a client host-port view into the REST JSON shape.
    fn from(value: HostPortView) -> Self {
        Self {
            name: value.name,
            target_port: value.target_port,
            host_port: value.host_port,
            host_ip: value.host_ip,
            protocol: value.protocol.as_str().to_string(),
        }
    }
}

/// Converts Rust debug enum labels into lowercase snake-case strings.
pub(crate) fn debug_variant_label(value: impl std::fmt::Debug) -> String {
    camel_to_snake(&format!("{value:?}"))
}

/// Converts a CamelCase variant label into snake_case.
fn camel_to_snake(value: &str) -> String {
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if idx != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// Resolves a named volume mount without requiring a separate volume lookup.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct TaskVolumeMount {
    pub volume_name: String,
    pub volume_id: String,
    pub target: String,
    pub read_only: bool,
}

/// Describes a projected secret file while keeping its contents out of inspection responses.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct TaskSecretFile {
    pub path: String,
    pub secret: config::SecretReference,
    /// Numeric Unix permissions; null means the secret policy default.
    pub mode: Option<u32>,
    pub ownership: FilesystemOwnership,
    pub path_env_name: Option<String>,
}

/// Preserves volume identifiers and mount settings for task and service inspection.
pub(crate) fn volumes(
    values: capnp::struct_list::Reader<workload::volume_mount::Owned>,
) -> capnp::Result<Vec<TaskVolumeMount>> {
    values
        .iter()
        .map(|mount| {
            Ok(TaskVolumeMount {
                volume_name: mount.get_volume_name()?.to_str()?.to_string(),
                volume_id: uuid_to_string(mount.get_volume_id()?)?,
                target: mount.get_target()?.to_str()?.to_string(),
                read_only: mount.get_read_only(),
            })
        })
        .collect()
}

/// Keeps secret references separate from literal values in inspection responses.
pub(crate) fn env(
    values: capnp::struct_list::Reader<workload::environment_var::Owned>,
) -> capnp::Result<Vec<config::EnvironmentVariable>> {
    values
        .iter()
        .map(|variable| {
            // A secret reference takes precedence even if a literal value is also present on the wire.
            let (value, secret) = if variable.has_secret() {
                (None, Some(secret_reference(variable.get_secret()?)?))
            } else {
                (Some(variable.get_value()?.to_str()?.to_string()), None)
            };

            Ok(config::EnvironmentVariable {
                name: variable.get_name()?.to_str()?.to_string(),
                value,
                secret,
            })
        })
        .collect()
}

/// Describes secret projections without retrieving their contents.
pub(crate) fn secret_files(
    values: capnp::struct_list::Reader<workload::secret_file::Owned>,
) -> capnp::Result<Vec<TaskSecretFile>> {
    values
        .iter()
        .map(|file| {
            let ownership = match file.get_ownership()?.which()? {
                filesystem_ownership::Which::Daemon(()) => FilesystemOwnership {
                    kind: "daemon".into(),
                    uid: None,
                    gid: None,
                },
                filesystem_ownership::Which::User(user) => {
                    let user = user?;
                    FilesystemOwnership {
                        kind: "user".into(),
                        uid: Some(user.get_uid()),
                        gid: Some(user.get_gid()),
                    }
                }
                filesystem_ownership::Which::FsGroup(group) => FilesystemOwnership {
                    kind: "fs_group".into(),
                    uid: None,
                    gid: Some(group?.get_gid()),
                },
            };

            let path_env_name = file.get_path_env_name()?.to_str()?;
            Ok(TaskSecretFile {
                path: file.get_path()?.to_str()?.to_string(),
                secret: secret_reference(file.get_secret()?)?,
                mode: (file.get_mode() != 0).then_some(file.get_mode()),
                ownership,
                path_env_name: (!path_env_name.is_empty()).then(|| path_env_name.to_string()),
            })
        })
        .collect()
}

/// Represents latest-version references with null instead of an artificial version identifier.
pub(crate) fn secret_reference(
    secret: workload::secret_ref::Reader<'_>,
) -> capnp::Result<config::SecretReference> {
    let version = secret.get_version_id()?;
    Ok(config::SecretReference {
        name: secret.get_name()?.to_str()?.to_string(),
        version: if version.is_empty() {
            None
        } else {
            Some(uuid_to_string(version)?)
        },
    })
}

/// Retains exact text and argument boundaries when converting Cap'n Proto lists to JSON arrays.
pub(crate) fn text_list(values: capnp::text_list::Reader<'_>) -> capnp::Result<Vec<String>> {
    values
        .iter()
        .map(|value| Ok(value?.to_str()?.to_string()))
        .collect()
}

/// Requires full UUIDs so inspection references can be used in other REST requests.
pub(crate) fn uuid_to_string(bytes: &[u8]) -> capnp::Result<String> {
    Uuid::from_slice(bytes)
        .map(|id| id.to_string())
        .map_err(|error| capnp::Error::failed(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    enum Example {
        VolumeUnavailable,
    }

    #[test]
    fn debug_variant_label_returns_snake_case() {
        assert_eq!(
            debug_variant_label(Example::VolumeUnavailable),
            "volume_unavailable"
        );
    }
}
