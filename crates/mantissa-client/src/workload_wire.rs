use crate::jobs::manifest::{
    EnvironmentVariable, LivenessKind, LivenessProbe, SecretFileProjection, SecretReference,
};
use crate::volumes::LocalVolumeOwnership;
use crate::volumes::ResolvedVolumeMount;
use crate::workload_submit::{
    DeploymentPolicySpec, ManifestPortBinding, ManifestPortProtocol, PlacementConstraint,
    PlacementConstraintOperator, PlacementConstraintSelector, PlacementSpec, PlacementStrategy,
    RequestedNetworkSpec, WorkloadAdmissionMode, WorkloadAdmissionPolicy,
};
use capnp::struct_list;
use mantissa_protocol::volumes::local_volume_ownership;
use mantissa_protocol::workload::{
    admission_policy, deployment_policy, environment_var, liveness_probe, network_requirement,
    placement_constraint, placement_constraint_selector, placement_policy, port_binding,
    secret_file, secret_ref, volume_mount,
};
use uuid::Uuid;

/// One prepared named volume mount resolved to the cluster volume identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedVolumeMount {
    pub volume_id: Uuid,
    pub volume_name: String,
    pub target: String,
    pub read_only: bool,
}

/// Encodes one manifest-selected workload admission policy into the shared wire shape.
pub fn write_admission_policy(
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

/// Encodes one manifest-selected deployment policy into the shared workload wire shape.
pub fn write_deployment_policy(
    mut builder: deployment_policy::Builder<'_>,
    policy: &DeploymentPolicySpec,
) {
    builder.set_progress_deadline_secs(policy.progress_deadline_secs);
    builder.set_healthy_deadline_secs(policy.healthy_deadline_secs);
    builder.set_min_healthy_secs(policy.min_healthy_secs);
}

/// Encodes one generic workload placement policy into the shared wire shape.
pub fn write_placement_policy(builder: placement_policy::Builder<'_>, policy: &PlacementSpec) {
    write_placement_policy_parts(builder, &policy.constraints, policy.strategy);
}

/// Encodes generic placement parts for callers with controller-specific placement wrappers.
pub fn write_placement_policy_parts(
    mut builder: placement_policy::Builder<'_>,
    constraints: &[PlacementConstraint],
    strategy: PlacementStrategy,
) {
    let mut constraints_builder = builder
        .reborrow()
        .init_constraints(constraints.len() as u32);
    for (idx, constraint) in constraints.iter().enumerate() {
        write_placement_constraint(constraints_builder.reborrow().get(idx as u32), constraint);
    }
    let strategy = match strategy {
        PlacementStrategy::Spread => mantissa_protocol::workload::PlacementStrategy::Spread,
        PlacementStrategy::Binpack => mantissa_protocol::workload::PlacementStrategy::Binpack,
    };
    builder.set_strategy(strategy);
}

/// Writes one typed placement constraint into the generic workload payload.
fn write_placement_constraint(
    mut builder: placement_constraint::Builder<'_>,
    constraint: &PlacementConstraint,
) {
    write_placement_constraint_selector(builder.reborrow().init_selector(), &constraint.selector);
    let operator = match constraint.operator {
        PlacementConstraintOperator::Eq => {
            mantissa_protocol::workload::PlacementConstraintOperator::Eq
        }
        PlacementConstraintOperator::Ne => {
            mantissa_protocol::workload::PlacementConstraintOperator::Ne
        }
    };
    builder.set_operator(operator);
    builder.set_value(constraint.value.trim());
}

/// Writes one typed placement selector into the generic workload payload.
fn write_placement_constraint_selector(
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
        PlacementConstraintSelector::NodeLabel { key } => builder.set_node_label(key.trim()),
    }
}

/// Rebuilds one resolved CLI volume mount into the generic workload submit payload shape.
pub fn prepared_volume_mount_from_resolved(mount: &ResolvedVolumeMount) -> PreparedVolumeMount {
    PreparedVolumeMount {
        volume_id: mount.volume_id,
        volume_name: mount.volume_name.clone(),
        target: mount.target.clone(),
        read_only: mount.read_only,
    }
}

/// Encodes one manifest secret reference into the workload wire builder.
pub fn write_secret_reference(mut builder: secret_ref::Builder<'_>, reference: &SecretReference) {
    builder.set_name(&reference.name);
    if let Some(version) = reference.version {
        builder.set_version_id(version.as_bytes());
    } else {
        builder.set_version_id(&[]);
    }
}

/// Encodes one manifest environment variable list into the workload wire builder.
pub fn write_env_vars(
    builder: &mut struct_list::Builder<environment_var::Owned>,
    vars: &[EnvironmentVariable],
) {
    for (index, var) in vars.iter().enumerate() {
        let mut entry = builder.reborrow().get(index as u32);
        entry.set_name(&var.name);
        if let Some(value) = var.value.as_deref() {
            entry.set_value(value);
        }
        if let Some(secret) = var.secret.as_ref() {
            write_secret_reference(entry.reborrow().init_secret(), secret);
        }
    }
}

/// Encodes one manifest secret file list into the workload wire builder.
pub fn write_secret_files(
    builder: &mut struct_list::Builder<secret_file::Owned>,
    files: &[SecretFileProjection],
) {
    for (index, file) in files.iter().enumerate() {
        let mut entry = builder.reborrow().get(index as u32);
        entry.set_path(&file.path);
        write_secret_reference(entry.reborrow().init_secret(), &file.secret);
        entry.set_mode(file.mode.unwrap_or(0));
        write_local_volume_ownership(entry.reborrow().init_ownership(), &file.ownership);
        entry.set_path_env_name(file.path_env_name.as_deref().unwrap_or(""));
    }
}

/// Encodes one managed-filesystem ownership policy into the shared workload wire builder.
pub fn write_local_volume_ownership(
    mut builder: local_volume_ownership::Builder<'_>,
    ownership: &LocalVolumeOwnership,
) {
    match ownership {
        LocalVolumeOwnership::Daemon => {
            builder.set_daemon(());
        }
        LocalVolumeOwnership::User { uid, gid } => {
            let mut user = builder.reborrow().init_user();
            user.set_uid(*uid);
            user.set_gid(*gid);
        }
        LocalVolumeOwnership::FsGroup { gid } => {
            let mut fs_group = builder.reborrow().init_fs_group();
            fs_group.set_gid(*gid);
        }
    }
}

/// Encodes one resolved volume mount list into the workload wire builder.
pub fn write_volume_mounts(
    builder: &mut struct_list::Builder<volume_mount::Owned>,
    mounts: &[PreparedVolumeMount],
) {
    for (index, mount) in mounts.iter().enumerate() {
        let mut entry = builder.reborrow().get(index as u32);
        entry.set_volume_id(mount.volume_id.as_bytes());
        entry.set_volume_name(&mount.volume_name);
        entry.set_target(&mount.target);
        entry.set_read_only(mount.read_only);
    }
}

/// Encodes manifest network requirements into the shared workload wire builder.
pub fn write_network_requirements(
    builder: &mut struct_list::Builder<network_requirement::Owned>,
    requirements: &[RequestedNetworkSpec],
) {
    for (index, network) in requirements.iter().enumerate() {
        let mut entry = builder.reborrow().get(index as u32);
        entry.set_name(&network.name);
        entry.set_driver(network.driver.into());
        let family = match network.ip_family {
            Some(crate::config::NetworkIpFamily::Ipv4) => {
                mantissa_protocol::workload::NetworkRequirementIpFamily::Ipv4
            }
            Some(crate::config::NetworkIpFamily::Ipv6) => {
                mantissa_protocol::workload::NetworkRequirementIpFamily::Ipv6
            }
            None => mantissa_protocol::workload::NetworkRequirementIpFamily::Default,
        };
        entry.set_ip_family(family);
        if let Some(realization) = network.realization {
            entry.set_realization(realization.into());
        }
    }
}

/// Encodes one manifest host port list into the shared workload wire builder.
pub fn write_port_bindings(
    builder: &mut struct_list::Builder<port_binding::Owned>,
    ports: &[ManifestPortBinding],
) {
    for (index, port) in ports.iter().enumerate() {
        let mut entry = builder.reborrow().get(index as u32);
        entry.set_name(port.name.trim());
        entry.set_target_port(port.target);
        entry.set_host_port(port.host);
        entry.set_host_ip(port.host_ip.trim());
        let protocol = match port.protocol {
            ManifestPortProtocol::Tcp => mantissa_protocol::workload::PortProtocol::Tcp,
            ManifestPortProtocol::Udp => mantissa_protocol::workload::PortProtocol::Udp,
        };
        entry.set_protocol(protocol);
    }
}

/// Writes one optional single mount into the workspace or checkpoint payload.
pub fn write_optional_volume_mount(
    builder: volume_mount::Builder<'_>,
    mount: Option<&PreparedVolumeMount>,
) {
    match mount {
        Some(mount) => {
            let mut builder = builder;
            builder.set_volume_id(mount.volume_id.as_bytes());
            builder.set_volume_name(&mount.volume_name);
            builder.set_target(&mount.target);
            builder.set_read_only(mount.read_only);
        }
        None => {
            let mut builder = builder;
            builder.set_volume_id(&[]);
            builder.set_volume_name("");
            builder.set_target("");
            builder.set_read_only(false);
        }
    }
}

/// Encodes one manifest liveness probe into the workload wire builder.
pub fn write_liveness_probe(mut builder: liveness_probe::Builder<'_>, probe: &LivenessProbe) {
    let kind = match probe.kind {
        LivenessKind::Exec => mantissa_protocol::workload::LivenessProbeKind::Exec,
        LivenessKind::Http => mantissa_protocol::workload::LivenessProbeKind::Http,
        LivenessKind::Tcp => mantissa_protocol::workload::LivenessProbeKind::Tcp,
    };
    builder.set_kind(kind);

    let mut command = builder.reborrow().init_command(probe.command.len() as u32);
    for (index, arg) in probe.command.iter().enumerate() {
        command.set(index as u32, arg);
    }

    builder.set_port(probe.port);
    builder.set_path(probe.path.as_deref().unwrap_or(""));
    builder.set_interval_ms(probe.interval_ms);
    builder.set_timeout_ms(probe.timeout_ms);
    builder.set_failure_threshold(probe.failure_threshold);
    builder.set_start_period_ms(probe.start_period_ms);
}
