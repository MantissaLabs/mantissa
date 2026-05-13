use crate::scheduler::placement::{
    PlacementConstraint, PlacementConstraintOperator, PlacementConstraintSelector, PlacementPolicy,
    PlacementStrategy,
};
use crate::volumes::types::LocalVolumeOwnership;
use crate::workload::model::{
    WorkloadEnvironmentVariable, WorkloadSecretFile, WorkloadSecretReference, WorkloadVolumeMount,
};
use crate::workload::network_prerequisites::{WorkloadNetworkIpFamily, WorkloadNetworkRequirement};
use crate::workload::types::{
    WorkloadAdmissionMode, WorkloadAdmissionPolicy, WorkloadLivenessProbe,
    WorkloadLivenessProbeKind, WorkloadPortBinding, WorkloadPortProtocol, WorkloadRestartPolicy,
    WorkloadRestartPolicyKind,
};
use capnp::{Error, struct_list};
use mantissa_protocol::volumes::local_volume_ownership;
use mantissa_protocol::workload::{
    admission_policy, environment_var, network_requirement, placement_constraint,
    placement_constraint_selector, placement_policy, port_binding, secret_file, secret_ref,
    volume_mount,
};
use uuid::Uuid;

/// Encodes the workload admission policy selected by a higher-level controller.
pub fn encode_admission_policy(
    mut builder: admission_policy::Builder<'_>,
    policy: &WorkloadAdmissionPolicy,
) {
    let mode = match policy.mode {
        WorkloadAdmissionMode::Incremental => {
            mantissa_protocol::workload::AdmissionMode::Incremental
        }
        WorkloadAdmissionMode::Gang => mantissa_protocol::workload::AdmissionMode::Gang,
    };
    builder.set_mode(mode);
}

/// Decodes one workload admission policy, defaulting absent modes to incremental admission.
pub fn decode_admission_policy(
    reader: admission_policy::Reader<'_>,
) -> Result<WorkloadAdmissionPolicy, Error> {
    let mode = match reader.get_mode() {
        Ok(mantissa_protocol::workload::AdmissionMode::Incremental) => {
            WorkloadAdmissionMode::Incremental
        }
        Ok(mantissa_protocol::workload::AdmissionMode::Gang) => WorkloadAdmissionMode::Gang,
        Err(_) => WorkloadAdmissionMode::Incremental,
    };
    Ok(WorkloadAdmissionPolicy { mode })
}

/// Encodes one generic workload placement policy into the shared wire shape.
pub fn encode_placement_policy(
    mut builder: placement_policy::Builder<'_>,
    policy: &PlacementPolicy,
) {
    let mut constraints = builder
        .reborrow()
        .init_constraints(policy.constraints.len() as u32);
    for (idx, constraint) in policy.constraints.iter().enumerate() {
        encode_placement_constraint(constraints.reborrow().get(idx as u32), constraint);
    }
    builder.set_strategy(placement_strategy_to_proto(policy.strategy));
}

/// Decodes one generic workload placement policy from the shared wire shape.
pub fn decode_placement_policy(
    reader: placement_policy::Reader<'_>,
) -> Result<PlacementPolicy, Error> {
    let constraints = match reader.get_constraints() {
        Ok(entries) => {
            let mut constraints = Vec::with_capacity(entries.len() as usize);
            for entry in entries.iter() {
                constraints.push(decode_placement_constraint(entry)?);
            }
            constraints
        }
        Err(_) => Vec::new(),
    };
    let strategy = match reader.get_strategy() {
        Ok(strategy) => placement_strategy_from_proto(strategy),
        Err(_) => PlacementStrategy::Spread,
    };

    Ok(PlacementPolicy {
        constraints,
        strategy,
    })
}

/// Encodes one hard placement constraint into the shared workload wire shape.
fn encode_placement_constraint(
    mut builder: placement_constraint::Builder<'_>,
    constraint: &PlacementConstraint,
) {
    encode_placement_constraint_selector(builder.reborrow().init_selector(), constraint.selector());
    builder.set_operator(placement_constraint_operator_to_proto(
        constraint.operator(),
    ));
    builder.set_value(constraint.value());
}

/// Decodes one hard placement constraint from the shared workload wire shape.
fn decode_placement_constraint(
    reader: placement_constraint::Reader<'_>,
) -> Result<PlacementConstraint, Error> {
    let selector = decode_placement_constraint_selector(reader.get_selector()?)?;
    let operator = match reader.get_operator() {
        Ok(operator) => placement_constraint_operator_from_proto(operator),
        Err(_) => PlacementConstraintOperator::Eq,
    };
    let value = reader.get_value()?.to_str()?.to_string();

    PlacementConstraint::new(selector, operator, value)
        .map_err(|err| Error::failed(err.to_string()))
}

/// Encodes one internal placement selector into the shared workload wire union.
fn encode_placement_constraint_selector(
    mut builder: placement_constraint_selector::Builder<'_>,
    selector: &PlacementConstraintSelector,
) {
    match selector {
        PlacementConstraintSelector::NodeId => builder.set_node_id(()),
        PlacementConstraintSelector::NodeHostname => builder.set_node_hostname(()),
        PlacementConstraintSelector::NodeIp => builder.set_node_ip(()),
        PlacementConstraintSelector::NodeAddress => builder.set_node_address(()),
        PlacementConstraintSelector::NodePlatformOs => builder.set_node_platform_os(()),
        PlacementConstraintSelector::NodePlatformArch => builder.set_node_platform_arch(()),
        PlacementConstraintSelector::NodeLabel { key } => builder.set_node_label(key),
    }
}

/// Decodes one typed placement selector from the shared workload wire union.
fn decode_placement_constraint_selector(
    reader: placement_constraint_selector::Reader<'_>,
) -> Result<PlacementConstraintSelector, Error> {
    match reader.which()? {
        placement_constraint_selector::Which::NodeId(()) => Ok(PlacementConstraintSelector::NodeId),
        placement_constraint_selector::Which::NodeHostname(()) => {
            Ok(PlacementConstraintSelector::NodeHostname)
        }
        placement_constraint_selector::Which::NodeIp(()) => Ok(PlacementConstraintSelector::NodeIp),
        placement_constraint_selector::Which::NodeAddress(()) => {
            Ok(PlacementConstraintSelector::NodeAddress)
        }
        placement_constraint_selector::Which::NodePlatformOs(()) => {
            Ok(PlacementConstraintSelector::NodePlatformOs)
        }
        placement_constraint_selector::Which::NodePlatformArch(()) => {
            Ok(PlacementConstraintSelector::NodePlatformArch)
        }
        placement_constraint_selector::Which::NodeLabel(Ok(key)) => Ok(
            PlacementConstraintSelector::node_label(key.to_str()?.to_string()),
        ),
        placement_constraint_selector::Which::NodeLabel(Err(err)) => Err(err),
    }
}

/// Decodes the placement comparison operator stored in the shared wire payload.
fn placement_constraint_operator_from_proto(
    operator: mantissa_protocol::workload::PlacementConstraintOperator,
) -> PlacementConstraintOperator {
    match operator {
        mantissa_protocol::workload::PlacementConstraintOperator::Eq => {
            PlacementConstraintOperator::Eq
        }
        mantissa_protocol::workload::PlacementConstraintOperator::Ne => {
            PlacementConstraintOperator::Ne
        }
    }
}

/// Encodes the internal placement comparison operator into the shared wire enum.
fn placement_constraint_operator_to_proto(
    operator: PlacementConstraintOperator,
) -> mantissa_protocol::workload::PlacementConstraintOperator {
    match operator {
        PlacementConstraintOperator::Eq => {
            mantissa_protocol::workload::PlacementConstraintOperator::Eq
        }
        PlacementConstraintOperator::Ne => {
            mantissa_protocol::workload::PlacementConstraintOperator::Ne
        }
    }
}

/// Decodes the placement strategy stored in the shared wire payload.
fn placement_strategy_from_proto(
    strategy: mantissa_protocol::workload::PlacementStrategy,
) -> PlacementStrategy {
    match strategy {
        mantissa_protocol::workload::PlacementStrategy::Spread => PlacementStrategy::Spread,
        mantissa_protocol::workload::PlacementStrategy::Binpack => PlacementStrategy::Binpack,
    }
}

/// Encodes the internal placement strategy into the shared wire enum.
fn placement_strategy_to_proto(
    strategy: PlacementStrategy,
) -> mantissa_protocol::workload::PlacementStrategy {
    match strategy {
        PlacementStrategy::Spread => mantissa_protocol::workload::PlacementStrategy::Spread,
        PlacementStrategy::Binpack => mantissa_protocol::workload::PlacementStrategy::Binpack,
    }
}

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

/// Decodes manifest-level network requirements from the shared workload schema list.
pub fn decode_network_requirements(
    list: struct_list::Reader<network_requirement::Owned>,
) -> Result<Vec<WorkloadNetworkRequirement>, Error> {
    let mut required = Vec::with_capacity(list.len() as usize);
    for network in list.iter() {
        required.push(decode_network_requirement(network)?);
    }
    Ok(required)
}

/// Decodes one manifest-level network requirement from the shared workload schema.
pub fn decode_network_requirement(
    reader: network_requirement::Reader<'_>,
) -> Result<WorkloadNetworkRequirement, Error> {
    let name = reader.get_name()?.to_str()?.trim().to_string();
    if name.is_empty() {
        return Err(Error::failed(
            "required network name cannot be empty".to_string(),
        ));
    }

    let driver = crate::network::types::NetworkDriver::from_proto(reader.get_driver()?);
    let ip_family = match reader.get_ip_family() {
        Ok(mantissa_protocol::workload::NetworkRequirementIpFamily::Default) | Err(_) => {
            WorkloadNetworkIpFamily::Default
        }
        Ok(mantissa_protocol::workload::NetworkRequirementIpFamily::Ipv4) => {
            WorkloadNetworkIpFamily::Ipv4
        }
        Ok(mantissa_protocol::workload::NetworkRequirementIpFamily::Ipv6) => {
            WorkloadNetworkIpFamily::Ipv6
        }
    };

    Ok(WorkloadNetworkRequirement {
        name,
        driver,
        ip_family,
    })
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
