use crate::jobs::manifest::{
    EnvironmentVariable, LivenessKind, LivenessProbe, SecretFileProjection, SecretReference,
};
use crate::volumes::LocalVolumeOwnership;
use crate::volumes::ResolvedVolumeMount;
use capnp::struct_list;
use protocol::volumes::local_volume_ownership;
use protocol::workload::{environment_var, liveness_probe, secret_file, secret_ref, volume_mount};
use uuid::Uuid;

/// One prepared named volume mount resolved to the cluster volume identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparedVolumeMount {
    pub volume_id: Uuid,
    pub volume_name: String,
    pub target: String,
    pub read_only: bool,
}

/// Rebuilds one resolved CLI volume mount into the generic workload submit payload shape.
pub(crate) fn prepared_volume_mount_from_resolved(
    mount: &ResolvedVolumeMount,
) -> PreparedVolumeMount {
    PreparedVolumeMount {
        volume_id: mount.volume_id,
        volume_name: mount.volume_name.clone(),
        target: mount.target.clone(),
        read_only: mount.read_only,
    }
}

/// Encodes one manifest secret reference into the workload wire builder.
pub(crate) fn write_secret_reference(
    mut builder: secret_ref::Builder<'_>,
    reference: &SecretReference,
) {
    builder.set_name(&reference.name);
    if let Some(version) = reference.version {
        builder.set_version_id(version.as_bytes());
    } else {
        builder.set_version_id(&[]);
    }
}

/// Encodes one manifest environment variable list into the workload wire builder.
pub(crate) fn write_env_vars(
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
pub(crate) fn write_secret_files(
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
pub(crate) fn write_local_volume_ownership(
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
pub(crate) fn write_volume_mounts(
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

/// Writes one optional single mount into the workspace or checkpoint payload.
pub(crate) fn write_optional_volume_mount(
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
pub(crate) fn write_liveness_probe(
    mut builder: liveness_probe::Builder<'_>,
    probe: &LivenessProbe,
) {
    let kind = match probe.kind {
        LivenessKind::Exec => protocol::workload::LivenessProbeKind::Exec,
        LivenessKind::Http => protocol::workload::LivenessProbeKind::Http,
        LivenessKind::Tcp => protocol::workload::LivenessProbeKind::Tcp,
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
