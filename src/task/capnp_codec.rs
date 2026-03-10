use crate::task::types::{
    TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference, TaskVolumeMount,
};
use capnp::{Error, struct_list};
use protocol::task::{environment_var, secret_file, secret_ref, volume_mount};
use uuid::Uuid;

/// Encodes one secret reference into the task schema payload.
pub fn encode_secret_ref(mut builder: secret_ref::Builder<'_>, reference: &TaskSecretReference) {
    builder.set_name(&reference.name);
    if let Some(version_id) = reference.version_id {
        builder.set_version_id(version_id.as_bytes());
    } else {
        builder.set_version_id(&[]);
    }
}

/// Decodes one secret reference from the task schema payload.
pub fn decode_secret_ref(reader: secret_ref::Reader<'_>) -> Result<TaskSecretReference, Error> {
    let name = reader.get_name()?.to_str()?.to_string();
    let data = reader.get_version_id()?;
    let version_id = if data.len() == 16 {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(data);
        Some(Uuid::from_bytes(bytes))
    } else {
        None
    };

    Ok(TaskSecretReference { name, version_id })
}

/// Encodes task environment variables into the task schema list.
pub fn encode_env_vars(
    builder: &mut struct_list::Builder<environment_var::Owned>,
    vars: &[TaskEnvironmentVariable],
) {
    for (idx, var) in vars.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_name(&var.name);
        if let Some(value) = &var.value {
            entry.set_value(value);
        }
        if let Some(secret) = &var.secret {
            let secret_builder = entry.reborrow().init_secret();
            encode_secret_ref(secret_builder, secret);
        }
    }
}

/// Decodes task environment variables from the task schema list.
pub fn decode_env_vars(
    list: struct_list::Reader<environment_var::Owned>,
) -> Result<Vec<TaskEnvironmentVariable>, Error> {
    let mut env = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        let name = entry.get_name()?.to_str()?.to_string();
        let value = if entry.has_value() {
            Some(entry.get_value()?.to_str()?.to_string())
        } else {
            None
        };
        let secret = if entry.has_secret() {
            Some(decode_secret_ref(entry.get_secret()?)?)
        } else {
            None
        };
        env.push(TaskEnvironmentVariable {
            name,
            value,
            secret,
        });
    }
    Ok(env)
}

/// Encodes task secret files into the task schema list.
pub fn encode_secret_files(
    builder: &mut struct_list::Builder<secret_file::Owned>,
    files: &[TaskSecretFile],
) {
    for (idx, file) in files.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_path(&file.path);
        let secret_builder = entry.reborrow().init_secret();
        encode_secret_ref(secret_builder, &file.secret);
        entry.set_mode(file.mode.unwrap_or(0));
    }
}

/// Decodes task secret files from the task schema list.
pub fn decode_secret_files(
    list: struct_list::Reader<secret_file::Owned>,
) -> Result<Vec<TaskSecretFile>, Error> {
    let mut files = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        let path = entry.get_path()?.to_str()?.to_string();
        let secret = decode_secret_ref(entry.get_secret()?)?;
        let mode = match entry.get_mode() {
            0 => None,
            value => Some(value),
        };
        files.push(TaskSecretFile { path, secret, mode });
    }
    Ok(files)
}

/// Encodes task volume mounts into the task schema list.
pub fn encode_volume_mounts(
    builder: &mut struct_list::Builder<volume_mount::Owned>,
    mounts: &[TaskVolumeMount],
) {
    for (idx, mount) in mounts.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_volume_id(mount.volume_id.as_bytes());
        entry.set_volume_name(&mount.volume_name);
        entry.set_target(&mount.target);
        entry.set_read_only(mount.read_only);
    }
}

/// Decodes task volume mounts from the task schema list.
pub fn decode_volume_mounts(
    list: struct_list::Reader<volume_mount::Owned>,
) -> Result<Vec<TaskVolumeMount>, Error> {
    let mut mounts = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        let volume_id = {
            let data = entry.get_volume_id()?;
            if data.len() != 16 {
                return Err(Error::failed("invalid volume id length".to_string()));
            }
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(data);
            Uuid::from_bytes(bytes)
        };
        mounts.push(TaskVolumeMount {
            volume_id,
            volume_name: entry.get_volume_name()?.to_str()?.to_string(),
            target: entry.get_target()?.to_str()?.to_string(),
            read_only: entry.get_read_only(),
        });
    }
    Ok(mounts)
}
