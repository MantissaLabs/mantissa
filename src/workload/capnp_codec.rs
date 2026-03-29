use crate::task::types::{
    TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference, TaskVolumeMount,
};
use crate::workload::types::{
    WorkloadLivenessProbe, WorkloadLivenessProbeKind, WorkloadRestartPolicy,
    WorkloadRestartPolicyKind,
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

/// Encodes one task liveness probe into the task wire payload.
pub fn encode_task_liveness_probe(
    mut builder: protocol::task::liveness_probe::Builder<'_>,
    probe: &WorkloadLivenessProbe,
) {
    let kind = match probe.kind {
        WorkloadLivenessProbeKind::Exec => protocol::task::LivenessProbeKind::Exec,
        WorkloadLivenessProbeKind::Http => protocol::task::LivenessProbeKind::Http,
        WorkloadLivenessProbeKind::Tcp => protocol::task::LivenessProbeKind::Tcp,
    };
    builder.set_kind(kind);
    let mut command_builder = builder.reborrow().init_command(probe.command.len() as u32);
    for (idx, arg) in probe.command.iter().enumerate() {
        command_builder.set(idx as u32, arg);
    }
    builder.set_port(probe.port);
    builder.set_path(probe.path.as_deref().unwrap_or(""));
    builder.set_interval_ms(probe.interval_ms);
    builder.set_timeout_ms(probe.timeout_ms);
    builder.set_failure_threshold(probe.failure_threshold);
    builder.set_start_period_ms(probe.start_period_ms);
}

/// Decodes one task liveness probe from the task wire payload.
pub fn decode_task_liveness_probe(
    reader: protocol::task::liveness_probe::Reader<'_>,
) -> Result<WorkloadLivenessProbe, Error> {
    let kind = match reader.get_kind()? {
        protocol::task::LivenessProbeKind::Exec => WorkloadLivenessProbeKind::Exec,
        protocol::task::LivenessProbeKind::Http => WorkloadLivenessProbeKind::Http,
        protocol::task::LivenessProbeKind::Tcp => WorkloadLivenessProbeKind::Tcp,
    };
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        let text = arg?.to_str()?.to_string();
        if !text.is_empty() {
            command.push(text);
        }
    }
    let path = reader.get_path()?.to_str()?.trim().to_string();

    Ok(WorkloadLivenessProbe {
        kind,
        command,
        port: reader.get_port(),
        path: (!path.is_empty()).then_some(path),
        interval_ms: reader.get_interval_ms(),
        timeout_ms: reader.get_timeout_ms(),
        failure_threshold: reader.get_failure_threshold(),
        start_period_ms: reader.get_start_period_ms(),
    })
}

/// Encodes one service liveness probe into the service wire payload.
pub fn encode_service_liveness_probe(
    mut builder: protocol::services::liveness_probe::Builder<'_>,
    probe: &WorkloadLivenessProbe,
) {
    let kind = match probe.kind {
        WorkloadLivenessProbeKind::Exec => protocol::services::LivenessProbeKind::Exec,
        WorkloadLivenessProbeKind::Http => protocol::services::LivenessProbeKind::Http,
        WorkloadLivenessProbeKind::Tcp => protocol::services::LivenessProbeKind::Tcp,
    };
    builder.set_kind(kind);
    let mut command_builder = builder.reborrow().init_command(probe.command.len() as u32);
    for (idx, arg) in probe.command.iter().enumerate() {
        command_builder.set(idx as u32, arg);
    }
    builder.set_port(probe.port);
    builder.set_path(probe.path.as_deref().unwrap_or(""));
    builder.set_interval_ms(probe.interval_ms);
    builder.set_timeout_ms(probe.timeout_ms);
    builder.set_failure_threshold(probe.failure_threshold);
    builder.set_start_period_ms(probe.start_period_ms);
}

/// Decodes one service liveness probe from the service wire payload.
pub fn decode_service_liveness_probe(
    reader: protocol::services::liveness_probe::Reader<'_>,
) -> Result<WorkloadLivenessProbe, Error> {
    let kind = match reader.get_kind()? {
        protocol::services::LivenessProbeKind::Exec => WorkloadLivenessProbeKind::Exec,
        protocol::services::LivenessProbeKind::Http => WorkloadLivenessProbeKind::Http,
        protocol::services::LivenessProbeKind::Tcp => WorkloadLivenessProbeKind::Tcp,
    };
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        let text = arg?.to_str()?.to_string();
        if !text.is_empty() {
            command.push(text);
        }
    }
    let path = reader.get_path()?.to_str()?.trim().to_string();

    Ok(WorkloadLivenessProbe {
        kind,
        command,
        port: reader.get_port(),
        path: (!path.is_empty()).then_some(path),
        interval_ms: reader.get_interval_ms(),
        timeout_ms: reader.get_timeout_ms(),
        failure_threshold: reader.get_failure_threshold(),
        start_period_ms: reader.get_start_period_ms(),
    })
}

/// Encodes one task restart policy into the task wire payload.
pub fn encode_task_restart_policy(
    mut builder: protocol::task::restart_policy::Builder<'_>,
    policy: &WorkloadRestartPolicy,
) {
    let name = match policy.name {
        WorkloadRestartPolicyKind::No => protocol::task::RestartPolicyName::No,
        WorkloadRestartPolicyKind::Always => protocol::task::RestartPolicyName::Always,
        WorkloadRestartPolicyKind::OnFailure => protocol::task::RestartPolicyName::OnFailure,
        WorkloadRestartPolicyKind::UnlessStopped => {
            protocol::task::RestartPolicyName::UnlessStopped
        }
    };
    builder.set_name(name);
    builder.set_max_retry_count(policy.max_retry_count.unwrap_or(-1));
}

/// Decodes one task restart policy from the task wire payload.
pub fn decode_task_restart_policy(
    reader: protocol::task::restart_policy::Reader<'_>,
) -> Result<WorkloadRestartPolicy, Error> {
    let name = match reader.get_name()? {
        protocol::task::RestartPolicyName::No => WorkloadRestartPolicyKind::No,
        protocol::task::RestartPolicyName::Always => WorkloadRestartPolicyKind::Always,
        protocol::task::RestartPolicyName::OnFailure => WorkloadRestartPolicyKind::OnFailure,
        protocol::task::RestartPolicyName::UnlessStopped => {
            WorkloadRestartPolicyKind::UnlessStopped
        }
    };

    let max_retry_count = match reader.get_max_retry_count() {
        value if value < 0 => None,
        value => Some(value),
    };

    Ok(WorkloadRestartPolicy {
        name,
        max_retry_count,
    })
}

/// Encodes one service restart policy into the service wire payload.
pub fn encode_service_restart_policy(
    mut builder: protocol::services::restart_policy::Builder<'_>,
    policy: &WorkloadRestartPolicy,
) {
    let name = match policy.name {
        WorkloadRestartPolicyKind::No => protocol::services::RestartPolicyName::No,
        WorkloadRestartPolicyKind::Always => protocol::services::RestartPolicyName::Always,
        WorkloadRestartPolicyKind::OnFailure => protocol::services::RestartPolicyName::OnFailure,
        WorkloadRestartPolicyKind::UnlessStopped => {
            protocol::services::RestartPolicyName::UnlessStopped
        }
    };
    builder.set_name(name);
    builder.set_max_retry_count(policy.max_retry_count.unwrap_or(-1));
}

/// Decodes one service restart policy from the service wire payload.
pub fn decode_service_restart_policy(
    reader: protocol::services::restart_policy::Reader<'_>,
) -> Result<WorkloadRestartPolicy, Error> {
    let name = match reader.get_name()? {
        protocol::services::RestartPolicyName::No => WorkloadRestartPolicyKind::No,
        protocol::services::RestartPolicyName::Always => WorkloadRestartPolicyKind::Always,
        protocol::services::RestartPolicyName::OnFailure => WorkloadRestartPolicyKind::OnFailure,
        protocol::services::RestartPolicyName::UnlessStopped => {
            WorkloadRestartPolicyKind::UnlessStopped
        }
    };

    let max_retry_count = match reader.get_max_retry_count() {
        value if value < 0 => None,
        value => Some(value),
    };

    Ok(WorkloadRestartPolicy {
        name,
        max_retry_count,
    })
}
