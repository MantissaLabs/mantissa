use crate::volumes::types::LocalVolumeOwnership;
use crate::workload::model::{
    WorkloadEnvironmentVariable, WorkloadSecretFile, WorkloadSecretReference, WorkloadVolumeMount,
};
use crate::workload::types::{
    WorkloadLivenessProbe, WorkloadLivenessProbeKind, WorkloadPortBinding, WorkloadPortProtocol,
    WorkloadRestartPolicy, WorkloadRestartPolicyKind,
};
use capnp::{Error, struct_list};
use mantissa_protocol::volumes::local_volume_ownership;
use mantissa_protocol::workload::{
    environment_var, port_binding, secret_file, secret_ref, volume_mount,
};
use uuid::Uuid;

/// Encodes one secret reference into the task schema payload.
pub fn encode_secret_ref(
    mut builder: secret_ref::Builder<'_>,
    reference: &WorkloadSecretReference,
) {
    builder.set_name(&reference.name);
    if let Some(version_id) = reference.version_id {
        builder.set_version_id(version_id.as_bytes());
    } else {
        builder.set_version_id(&[]);
    }
}

/// Decodes one secret reference from the task schema payload.
pub fn decode_secret_ref(reader: secret_ref::Reader<'_>) -> Result<WorkloadSecretReference, Error> {
    let name = reader.get_name()?.to_str()?.to_string();
    let data = reader.get_version_id()?;
    let version_id = if data.len() == 16 {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(data);
        Some(Uuid::from_bytes(bytes))
    } else {
        None
    };

    Ok(WorkloadSecretReference { name, version_id })
}

/// Encodes task environment variables into the task schema list.
pub fn encode_env_vars(
    builder: &mut struct_list::Builder<environment_var::Owned>,
    vars: &[WorkloadEnvironmentVariable],
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
) -> Result<Vec<WorkloadEnvironmentVariable>, Error> {
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
        env.push(WorkloadEnvironmentVariable {
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
    files: &[WorkloadSecretFile],
) {
    for (idx, file) in files.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_path(&file.path);
        let secret_builder = entry.reborrow().init_secret();
        encode_secret_ref(secret_builder, &file.secret);
        entry.set_mode(file.mode.unwrap_or(0));
        write_local_volume_ownership(entry.reborrow().init_ownership(), file.ownership);
        entry.set_path_env_name(file.path_env_name.as_deref().unwrap_or(""));
    }
}

/// Decodes task secret files from the task schema list.
pub fn decode_secret_files(
    list: struct_list::Reader<secret_file::Owned>,
) -> Result<Vec<WorkloadSecretFile>, Error> {
    let mut files = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        let path = entry.get_path()?.to_str()?.to_string();
        let secret = decode_secret_ref(entry.get_secret()?)?;
        let mode = match entry.get_mode() {
            0 => None,
            value => Some(value),
        };
        let ownership = if entry.has_ownership() {
            read_local_volume_ownership(entry.get_ownership()?)?
        } else {
            LocalVolumeOwnership::Daemon
        };
        let path_env_name = if entry.has_path_env_name() {
            let name = entry.get_path_env_name()?.to_str()?.trim().to_string();
            (!name.is_empty()).then_some(name)
        } else {
            None
        };
        files.push(WorkloadSecretFile {
            path,
            secret,
            mode,
            ownership,
            path_env_name,
        });
    }
    Ok(files)
}

/// Encodes one uid/gid ownership policy into the shared workload wire contract.
fn write_local_volume_ownership(
    mut builder: local_volume_ownership::Builder<'_>,
    ownership: LocalVolumeOwnership,
) {
    match ownership {
        LocalVolumeOwnership::Daemon => {
            builder.set_daemon(());
        }
        LocalVolumeOwnership::User { uid, gid } => {
            let mut user = builder.reborrow().init_user();
            user.set_uid(uid);
            user.set_gid(gid);
        }
        LocalVolumeOwnership::FsGroup { gid } => {
            let mut fs_group = builder.reborrow().init_fs_group();
            fs_group.set_gid(gid);
        }
    }
}

/// Decodes one uid/gid ownership policy from the shared workload wire contract.
fn read_local_volume_ownership(
    reader: local_volume_ownership::Reader<'_>,
) -> Result<LocalVolumeOwnership, Error> {
    match reader.which()? {
        local_volume_ownership::Which::Daemon(()) => Ok(LocalVolumeOwnership::Daemon),
        local_volume_ownership::Which::User(Ok(user)) => Ok(LocalVolumeOwnership::User {
            uid: user.get_uid(),
            gid: user.get_gid(),
        }),
        local_volume_ownership::Which::User(Err(err)) => Err(err),
        local_volume_ownership::Which::FsGroup(Ok(fs_group)) => Ok(LocalVolumeOwnership::FsGroup {
            gid: fs_group.get_gid(),
        }),
        local_volume_ownership::Which::FsGroup(Err(err)) => Err(err),
    }
}

/// Encodes task volume mounts into the task schema list.
pub fn encode_volume_mounts(
    builder: &mut struct_list::Builder<volume_mount::Owned>,
    mounts: &[WorkloadVolumeMount],
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
) -> Result<Vec<WorkloadVolumeMount>, Error> {
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
        mounts.push(WorkloadVolumeMount {
            volume_id,
            volume_name: entry.get_volume_name()?.to_str()?.to_string(),
            target: entry.get_target()?.to_str()?.to_string(),
            read_only: entry.get_read_only(),
        });
    }
    Ok(mounts)
}

/// Encodes task host port bindings into the shared workload schema list.
pub fn encode_port_bindings(
    builder: &mut struct_list::Builder<port_binding::Owned>,
    ports: &[WorkloadPortBinding],
) {
    for (idx, port) in ports.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_name(&port.name);
        entry.set_target_port(port.target_port);
        entry.set_host_port(port.host_port);
        entry.set_host_ip(&port.host_ip);
        let protocol = match port.protocol {
            WorkloadPortProtocol::Tcp => mantissa_protocol::workload::PortProtocol::Tcp,
            WorkloadPortProtocol::Udp => mantissa_protocol::workload::PortProtocol::Udp,
        };
        entry.set_protocol(protocol);
    }
}

/// Decodes task host port bindings from the shared workload schema list.
pub fn decode_port_bindings(
    list: struct_list::Reader<port_binding::Owned>,
) -> Result<Vec<WorkloadPortBinding>, Error> {
    let mut ports = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        let protocol = match entry.get_protocol()? {
            mantissa_protocol::workload::PortProtocol::Tcp => WorkloadPortProtocol::Tcp,
            mantissa_protocol::workload::PortProtocol::Udp => WorkloadPortProtocol::Udp,
        };
        ports.push(WorkloadPortBinding {
            name: entry.get_name()?.to_str()?.to_string(),
            target_port: entry.get_target_port(),
            host_port: entry.get_host_port(),
            host_ip: entry.get_host_ip()?.to_str()?.to_string(),
            protocol,
        });
    }
    Ok(ports)
}

/// Encodes one task liveness probe into the task wire payload.
pub fn encode_task_liveness_probe(
    mut builder: mantissa_protocol::workload::liveness_probe::Builder<'_>,
    probe: &WorkloadLivenessProbe,
) {
    let kind = match probe.kind {
        WorkloadLivenessProbeKind::Exec => mantissa_protocol::workload::LivenessProbeKind::Exec,
        WorkloadLivenessProbeKind::Http => mantissa_protocol::workload::LivenessProbeKind::Http,
        WorkloadLivenessProbeKind::Tcp => mantissa_protocol::workload::LivenessProbeKind::Tcp,
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
    reader: mantissa_protocol::workload::liveness_probe::Reader<'_>,
) -> Result<WorkloadLivenessProbe, Error> {
    let kind = match reader.get_kind()? {
        mantissa_protocol::workload::LivenessProbeKind::Exec => WorkloadLivenessProbeKind::Exec,
        mantissa_protocol::workload::LivenessProbeKind::Http => WorkloadLivenessProbeKind::Http,
        mantissa_protocol::workload::LivenessProbeKind::Tcp => WorkloadLivenessProbeKind::Tcp,
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
    mut builder: mantissa_protocol::services::liveness_probe::Builder<'_>,
    probe: &WorkloadLivenessProbe,
) {
    let kind = match probe.kind {
        WorkloadLivenessProbeKind::Exec => mantissa_protocol::services::LivenessProbeKind::Exec,
        WorkloadLivenessProbeKind::Http => mantissa_protocol::services::LivenessProbeKind::Http,
        WorkloadLivenessProbeKind::Tcp => mantissa_protocol::services::LivenessProbeKind::Tcp,
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
    reader: mantissa_protocol::services::liveness_probe::Reader<'_>,
) -> Result<WorkloadLivenessProbe, Error> {
    let kind = match reader.get_kind()? {
        mantissa_protocol::services::LivenessProbeKind::Exec => WorkloadLivenessProbeKind::Exec,
        mantissa_protocol::services::LivenessProbeKind::Http => WorkloadLivenessProbeKind::Http,
        mantissa_protocol::services::LivenessProbeKind::Tcp => WorkloadLivenessProbeKind::Tcp,
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
    mut builder: mantissa_protocol::workload::restart_policy::Builder<'_>,
    policy: &WorkloadRestartPolicy,
) {
    let name = match policy.name {
        WorkloadRestartPolicyKind::No => mantissa_protocol::workload::RestartPolicyName::No,
        WorkloadRestartPolicyKind::Always => mantissa_protocol::workload::RestartPolicyName::Always,
        WorkloadRestartPolicyKind::OnFailure => {
            mantissa_protocol::workload::RestartPolicyName::OnFailure
        }
        WorkloadRestartPolicyKind::UnlessStopped => {
            mantissa_protocol::workload::RestartPolicyName::UnlessStopped
        }
    };
    builder.set_name(name);
    builder.set_max_retry_count(policy.max_retry_count.unwrap_or(-1));
}

/// Decodes one task restart policy from the task wire payload.
pub fn decode_task_restart_policy(
    reader: mantissa_protocol::workload::restart_policy::Reader<'_>,
) -> Result<WorkloadRestartPolicy, Error> {
    let name = match reader.get_name()? {
        mantissa_protocol::workload::RestartPolicyName::No => WorkloadRestartPolicyKind::No,
        mantissa_protocol::workload::RestartPolicyName::Always => WorkloadRestartPolicyKind::Always,
        mantissa_protocol::workload::RestartPolicyName::OnFailure => {
            WorkloadRestartPolicyKind::OnFailure
        }
        mantissa_protocol::workload::RestartPolicyName::UnlessStopped => {
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
    mut builder: mantissa_protocol::services::restart_policy::Builder<'_>,
    policy: &WorkloadRestartPolicy,
) {
    let name = match policy.name {
        WorkloadRestartPolicyKind::No => mantissa_protocol::services::RestartPolicyName::No,
        WorkloadRestartPolicyKind::Always => mantissa_protocol::services::RestartPolicyName::Always,
        WorkloadRestartPolicyKind::OnFailure => {
            mantissa_protocol::services::RestartPolicyName::OnFailure
        }
        WorkloadRestartPolicyKind::UnlessStopped => {
            mantissa_protocol::services::RestartPolicyName::UnlessStopped
        }
    };
    builder.set_name(name);
    builder.set_max_retry_count(policy.max_retry_count.unwrap_or(-1));
}

/// Decodes one service restart policy from the service wire payload.
pub fn decode_service_restart_policy(
    reader: mantissa_protocol::services::restart_policy::Reader<'_>,
) -> Result<WorkloadRestartPolicy, Error> {
    let name = match reader.get_name()? {
        mantissa_protocol::services::RestartPolicyName::No => WorkloadRestartPolicyKind::No,
        mantissa_protocol::services::RestartPolicyName::Always => WorkloadRestartPolicyKind::Always,
        mantissa_protocol::services::RestartPolicyName::OnFailure => {
            WorkloadRestartPolicyKind::OnFailure
        }
        mantissa_protocol::services::RestartPolicyName::UnlessStopped => {
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
