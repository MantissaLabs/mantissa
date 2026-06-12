use mantissa_client::volumes::{
    LocalVolumeOwnership as ClientLocalVolumeOwnership, VolumeDriver as ClientVolumeDriver,
    VolumeInspect as ClientVolumeInspect, VolumeLabel as ClientVolumeLabel,
    VolumeNodeStatus as ClientVolumeNodeStatus, VolumeSpec as ClientVolumeSpec,
    VolumeSummary as ClientVolumeSummary,
};
use serde::Serialize;

/// REST-facing volume summary row.
#[derive(Clone, Debug, Serialize)]
pub struct VolumeSummary {
    pub id: String,
    pub name: String,
    pub driver: VolumeDriver,
    pub local_ownership: Option<LocalVolumeOwnership>,
    pub access_mode: String,
    pub binding_mode: String,
    pub reclaim_policy: String,
    pub status: String,
    pub bound_node_id: Option<String>,
    pub bound_node_name: Option<String>,
    pub requested_bytes: Option<u64>,
    pub in_use: bool,
    pub reason: Option<String>,
    pub updated_at: String,
}

impl From<ClientVolumeSummary> for VolumeSummary {
    /// Converts the client volume summary into the REST JSON shape.
    fn from(value: ClientVolumeSummary) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            driver: value.driver.into(),
            local_ownership: value.local_ownership.map(LocalVolumeOwnership::from),
            access_mode: value.access_mode.to_string(),
            binding_mode: value.binding_mode.to_string(),
            reclaim_policy: value.reclaim_policy.to_string(),
            status: value.status.to_string(),
            bound_node_id: value.bound_node_id.map(|id| id.to_string()),
            bound_node_name: value.bound_node_name,
            requested_bytes: value.requested_bytes,
            in_use: value.in_use,
            reason: value.reason,
            updated_at: value.updated_at,
        }
    }
}

/// REST-facing volume driver.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VolumeDriver {
    pub kind: String,
    pub path: Option<String>,
    pub driver_name: Option<String>,
    pub handle: Option<String>,
}

impl From<ClientVolumeDriver> for VolumeDriver {
    /// Converts the client volume driver into an explicit JSON shape.
    fn from(value: ClientVolumeDriver) -> Self {
        match value {
            ClientVolumeDriver::LocalManaged => Self {
                kind: "local_managed".to_string(),
                path: None,
                driver_name: None,
                handle: None,
            },
            ClientVolumeDriver::LocalImportedPath(path) => Self {
                kind: "local_imported_path".to_string(),
                path: Some(path),
                driver_name: None,
                handle: None,
            },
            ClientVolumeDriver::External {
                driver_name,
                handle,
            } => Self {
                kind: "external".to_string(),
                path: None,
                driver_name: Some(driver_name),
                handle: Some(handle),
            },
        }
    }
}

/// REST-facing ownership policy for local volume materialization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LocalVolumeOwnership {
    pub kind: String,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

impl From<ClientLocalVolumeOwnership> for LocalVolumeOwnership {
    /// Converts the client local-volume ownership into an explicit JSON shape.
    fn from(value: ClientLocalVolumeOwnership) -> Self {
        match value {
            ClientLocalVolumeOwnership::Daemon => Self {
                kind: "daemon".to_string(),
                uid: None,
                gid: None,
            },
            ClientLocalVolumeOwnership::User { uid, gid } => Self {
                kind: "user".to_string(),
                uid: Some(uid),
                gid: Some(gid),
            },
            ClientLocalVolumeOwnership::FsGroup { gid } => Self {
                kind: "fs_group".to_string(),
                uid: None,
                gid: Some(gid),
            },
        }
    }
}

/// REST-facing persisted volume specification.
#[derive(Clone, Debug, Serialize)]
pub struct VolumeSpec {
    pub id: String,
    pub name: String,
    pub driver: VolumeDriver,
    pub local_ownership: Option<LocalVolumeOwnership>,
    pub access_mode: String,
    pub binding_mode: String,
    pub reclaim_policy: String,
    pub requested_bytes: Option<u64>,
    pub labels: Vec<VolumeLabel>,
    pub status: String,
    pub bound_node_id: Option<String>,
    pub bound_node_name: Option<String>,
    pub volume_epoch: u64,
    pub phase_version: u64,
    pub created_at: String,
    pub updated_at: String,
    pub reason: Option<String>,
    pub message: Option<String>,
}

impl From<ClientVolumeSpec> for VolumeSpec {
    /// Converts the client volume spec into the REST JSON shape.
    fn from(value: ClientVolumeSpec) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            driver: value.driver.into(),
            local_ownership: value.local_ownership.map(LocalVolumeOwnership::from),
            access_mode: value.access_mode.to_string(),
            binding_mode: value.binding_mode.to_string(),
            reclaim_policy: value.reclaim_policy.to_string(),
            requested_bytes: value.requested_bytes,
            labels: value.labels.into_iter().map(VolumeLabel::from).collect(),
            status: value.status.to_string(),
            bound_node_id: value.bound_node_id.map(|id| id.to_string()),
            bound_node_name: value.bound_node_name,
            volume_epoch: value.volume_epoch,
            phase_version: value.phase_version,
            created_at: value.created_at,
            updated_at: value.updated_at,
            reason: value.reason,
            message: value.message,
        }
    }
}

/// REST-facing volume label.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VolumeLabel {
    pub key: String,
    pub value: String,
}

impl From<ClientVolumeLabel> for VolumeLabel {
    /// Converts the client volume label into the REST JSON shape.
    fn from(value: ClientVolumeLabel) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}

/// REST-facing node-local volume status row.
#[derive(Clone, Debug, Serialize)]
pub struct VolumeNodeStatus {
    pub id: String,
    pub volume_id: String,
    pub node_id: String,
    pub node_name: String,
    pub local_path: Option<String>,
    pub state: String,
    pub capacity_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub published_task_ids: Vec<String>,
    pub updated_at: String,
    pub last_error: Option<String>,
}

impl From<ClientVolumeNodeStatus> for VolumeNodeStatus {
    /// Converts the client node-local volume status into the REST JSON shape.
    fn from(value: ClientVolumeNodeStatus) -> Self {
        Self {
            id: value.id.to_string(),
            volume_id: value.volume_id.to_string(),
            node_id: value.node_id.to_string(),
            node_name: value.node_name,
            local_path: value.local_path,
            state: value.state.to_string(),
            capacity_bytes: value.capacity_bytes,
            used_bytes: value.used_bytes,
            published_task_ids: value
                .published_task_ids
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            updated_at: value.updated_at,
            last_error: value.last_error,
        }
    }
}

/// REST-facing volume inspection payload.
#[derive(Clone, Debug, Serialize)]
pub struct VolumeInspect {
    pub spec: VolumeSpec,
    pub node_states: Vec<VolumeNodeStatus>,
}

impl From<ClientVolumeInspect> for VolumeInspect {
    /// Converts the client volume inspect view into the REST JSON shape.
    fn from(value: ClientVolumeInspect) -> Self {
        Self {
            spec: value.spec.into(),
            node_states: value
                .node_states
                .into_iter()
                .map(VolumeNodeStatus::from)
                .collect(),
        }
    }
}
