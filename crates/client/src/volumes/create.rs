use super::{
    LocalVolumeOwnership, VolumeBindingMode, VolumeLabel, VolumeSpec, parse_volume_labels,
    resolve_node_selector,
};
use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result, anyhow};

/// Data required to create one managed local volume object.
#[derive(Debug, Clone)]
pub struct VolumeCreateRequest {
    pub name: String,
    pub ownership: LocalVolumeOwnership,
    pub binding_mode: VolumeBindingMode,
    pub reclaim_policy: super::VolumeReclaimPolicy,
    pub requested_bytes: Option<u64>,
    pub labels: Vec<VolumeLabel>,
    pub node_selector: Option<String>,
}

/// Submits one managed local volume create request and returns the persisted spec.
pub async fn create_raw(cfg: &ClientConfig, request: &VolumeCreateRequest) -> Result<VolumeSpec> {
    let session = connection::get_local_session(cfg).await?;
    let volumes_cap = session.get_volumes_request();
    let volumes = volumes_cap.send().pipeline.get_volumes();
    let mut create = volumes.create_request();

    let bound_node = if let Some(selector) = &request.node_selector {
        Some(resolve_node_selector(cfg, selector).await?)
    } else {
        None
    };

    {
        let mut inner = create.get().init_request();
        inner.set_name(&request.name);
        let mut driver = inner.reborrow().init_driver();
        let mut local = driver.reborrow().init_local();
        local.set_source_kind(protocol::volumes::LocalVolumeSourceKind::Managed);
        local.set_imported_path("");
        match &request.ownership {
            LocalVolumeOwnership::Daemon => {
                local.reborrow().init_ownership().set_daemon(());
            }
            LocalVolumeOwnership::User { uid, gid } => {
                let mut user = local.reborrow().init_ownership().init_user();
                user.set_uid(*uid);
                user.set_gid(*gid);
            }
            LocalVolumeOwnership::FsGroup { gid } => {
                let mut fs_group = local.reborrow().init_ownership().init_fs_group();
                fs_group.set_gid(*gid);
            }
        }
        inner.set_access_mode(protocol::volumes::VolumeAccessMode::ReadWriteOnce);
        inner.set_binding_mode(match request.binding_mode {
            VolumeBindingMode::Immediate => protocol::volumes::VolumeBindingMode::Immediate,
            VolumeBindingMode::WaitForFirstConsumer => {
                protocol::volumes::VolumeBindingMode::WaitForFirstConsumer
            }
        });
        inner.set_reclaim_policy(match request.reclaim_policy {
            super::VolumeReclaimPolicy::Retain => protocol::volumes::VolumeReclaimPolicy::Retain,
            super::VolumeReclaimPolicy::Delete => protocol::volumes::VolumeReclaimPolicy::Delete,
        });
        inner.set_requested_bytes(request.requested_bytes.unwrap_or(0));
        let mut labels = inner.reborrow().init_labels(request.labels.len() as u32);
        for (idx, label) in request.labels.iter().enumerate() {
            let mut entry = labels.reborrow().get(idx as u32);
            entry.set_key(&label.key);
            entry.set_value(&label.value);
        }
        if let Some((node_id, _node_name)) = &bound_node {
            inner.set_bound_node_id(node_id.as_bytes());
        } else {
            inner.set_bound_node_id(&[]);
        }
    }

    let response = create
        .send()
        .promise
        .await
        .context("volume create request failed")?;
    let reader = response.get()?.get_volume()?;
    VolumeSpec::from_reader(reader)
}

/// Creates one managed local volume and renders the result for CLI usage.
pub async fn create(
    cfg: &ClientConfig,
    request: VolumeCreateRequest,
    labels: &[String],
) -> Result<()> {
    if matches!(request.binding_mode, VolumeBindingMode::Immediate)
        && request.node_selector.is_none()
    {
        return Err(anyhow!("immediate local volumes require --node"));
    }

    let request = VolumeCreateRequest {
        labels: parse_volume_labels(labels)?,
        ..request
    };
    let volume = create_raw(cfg, &request).await?;
    output::emit_line(format!(
        "volume '{}' created with id {} ({}, {})",
        volume.name, volume.id, volume.driver, volume.access_mode
    ));
    Ok(())
}
