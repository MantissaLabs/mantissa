use mantissa_client::volumes::{
    FilesystemOwnership as ClientFilesystemOwnership,
    ReplicatedVolumeGroupStatus as ClientReplicatedVolumeGroupStatus,
    ReplicatedVolumePlan as ClientReplicatedVolumePlan,
    VolumeBindingMode as ClientVolumeBindingMode, VolumeCreateDriver as ClientVolumeCreateDriver,
    VolumeCreateRequest as ClientVolumeCreateRequest,
    VolumeDeleteDisposition as ClientVolumeDeleteDisposition, VolumeDeleteResult,
    VolumeDriver as ClientVolumeDriver, VolumeImportRequest as ClientVolumeImportRequest,
    VolumeInspect as ClientVolumeInspect, VolumeLabel as ClientVolumeLabel,
    VolumeNodeStatus as ClientVolumeNodeStatus, VolumeReclaimPolicy as ClientVolumeReclaimPolicy,
    VolumeSpec as ClientVolumeSpec, VolumeSummary as ClientVolumeSummary,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// REST request body for creating one Mantissa-managed volume.
#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct VolumeCreateRequest {
    pub name: String,
    #[serde(default)]
    pub driver: VolumeCreateDriver,
    #[serde(default)]
    pub ownership: VolumeOwnershipRequest,
    #[serde(default = "default_binding_mode")]
    pub binding_mode: String,
    #[serde(default = "default_reclaim_policy")]
    pub reclaim_policy: String,
    #[serde(default)]
    pub requested_bytes: Option<u64>,
    #[serde(default)]
    pub labels: Vec<VolumeLabel>,
    #[serde(default)]
    pub node_selector: Option<String>,
}

impl VolumeCreateRequest {
    /// Converts this REST request into the reusable client request.
    pub fn into_client(self) -> Result<ClientVolumeCreateRequest, String> {
        let binding_mode = parse_binding_mode(&self.binding_mode)?;
        let request = ClientVolumeCreateRequest {
            name: self.name,
            driver: match self.driver {
                VolumeCreateDriver::Local => ClientVolumeCreateDriver::Local,
                VolumeCreateDriver::Replicated => ClientVolumeCreateDriver::Replicated,
            },
            ownership: self.ownership.into_client(),
            binding_mode,
            reclaim_policy: parse_reclaim_policy(&self.reclaim_policy)?,
            requested_bytes: self.requested_bytes,
            labels: self
                .labels
                .into_iter()
                .map(ClientVolumeLabel::from)
                .collect(),
            node_selector: self.node_selector,
        };
        request.validate().map_err(|error| error.to_string())?;
        Ok(request)
    }
}

/// Storage driver selected by a REST volume create request.
#[derive(Clone, Copy, Debug, Default, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VolumeCreateDriver {
    #[default]
    Local,
    Replicated,
}

/// REST request body for importing an existing local path as a volume.
#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct VolumeImportRequest {
    pub name: String,
    pub node_selector: String,
    pub path: String,
    #[serde(default)]
    pub requested_bytes: Option<u64>,
    #[serde(default)]
    pub labels: Vec<VolumeLabel>,
}

impl From<VolumeImportRequest> for ClientVolumeImportRequest {
    /// Converts the REST import request into the reusable client request.
    fn from(value: VolumeImportRequest) -> Self {
        Self {
            name: value.name,
            node_selector: value.node_selector,
            path: value.path,
            requested_bytes: value.requested_bytes,
            labels: value
                .labels
                .into_iter()
                .map(ClientVolumeLabel::from)
                .collect(),
        }
    }
}

/// REST request ownership policy for Mantissa-managed filesystems.
#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum VolumeOwnershipRequest {
    #[default]
    Daemon,
    User {
        uid: u32,
        gid: u32,
    },
    FsGroup {
        gid: u32,
    },
}

impl VolumeOwnershipRequest {
    /// Converts this REST ownership request into the reusable client enum.
    fn into_client(self) -> ClientFilesystemOwnership {
        match self {
            Self::Daemon => ClientFilesystemOwnership::Daemon,
            Self::User { uid, gid } => ClientFilesystemOwnership::User { uid, gid },
            Self::FsGroup { gid } => ClientFilesystemOwnership::FsGroup { gid },
        }
    }
}

/// Returns the default binding mode for REST volume create requests.
fn default_binding_mode() -> String {
    "wait_for_first_consumer".to_string()
}

/// Returns the default reclaim policy for REST volume create requests.
fn default_reclaim_policy() -> String {
    "retain".to_string()
}

/// Parses one REST binding mode into the reusable client enum.
fn parse_binding_mode(value: &str) -> Result<ClientVolumeBindingMode, String> {
    match value {
        "immediate" => Ok(ClientVolumeBindingMode::Immediate),
        "wait_for_first_consumer" => Ok(ClientVolumeBindingMode::WaitForFirstConsumer),
        _ => Err(format!("invalid volume binding_mode '{value}'")),
    }
}

/// Parses one REST reclaim policy into the reusable client enum.
fn parse_reclaim_policy(value: &str) -> Result<ClientVolumeReclaimPolicy, String> {
    match value {
        "retain" => Ok(ClientVolumeReclaimPolicy::Retain),
        "delete" => Ok(ClientVolumeReclaimPolicy::Delete),
        _ => Err(format!("invalid volume reclaim_policy '{value}'")),
    }
}

/// REST-facing volume summary row.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct VolumeSummary {
    pub id: String,
    pub name: String,
    pub driver: VolumeDriver,
    pub filesystem_ownership: Option<FilesystemOwnership>,
    pub access_mode: String,
    pub binding_mode: String,
    pub reclaim_policy: String,
    pub status: String,
    pub state: String,
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
        let state = value.state.to_string();
        Self {
            id: value.id.to_string(),
            name: value.name,
            driver: value.driver.into(),
            filesystem_ownership: value.filesystem_ownership.map(FilesystemOwnership::from),
            access_mode: value.access_mode.to_string(),
            binding_mode: value.binding_mode.to_string(),
            reclaim_policy: value.reclaim_policy.to_string(),
            status: value.status.to_string(),
            state,
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
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
            ClientVolumeDriver::Replicated => Self {
                kind: "replicated".to_string(),
                path: None,
                driver_name: None,
                handle: None,
            },
        }
    }
}

/// REST-facing ownership policy for one managed filesystem.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct FilesystemOwnership {
    pub kind: String,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

impl From<ClientFilesystemOwnership> for FilesystemOwnership {
    /// Converts the client filesystem ownership into an explicit JSON shape.
    fn from(value: ClientFilesystemOwnership) -> Self {
        match value {
            ClientFilesystemOwnership::Daemon => Self {
                kind: "daemon".to_string(),
                uid: None,
                gid: None,
            },
            ClientFilesystemOwnership::User { uid, gid } => Self {
                kind: "user".to_string(),
                uid: Some(uid),
                gid: Some(gid),
            },
            ClientFilesystemOwnership::FsGroup { gid } => Self {
                kind: "fs_group".to_string(),
                uid: None,
                gid: Some(gid),
            },
        }
    }
}

/// REST-facing persisted volume specification.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct VolumeSpec {
    pub id: String,
    pub name: String,
    pub driver: VolumeDriver,
    pub filesystem_ownership: Option<FilesystemOwnership>,
    pub access_mode: String,
    pub binding_mode: String,
    pub reclaim_policy: String,
    pub requested_bytes: Option<u64>,
    pub labels: Vec<VolumeLabel>,
    pub bound_node_id: Option<String>,
    pub bound_node_name: Option<String>,
    pub volume_epoch: u64,
    pub lifecycle_revision: u64,
    pub lifecycle_request_id: String,
    pub desired_disposition: String,
    pub remove_data: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl From<ClientVolumeSpec> for VolumeSpec {
    /// Converts the client volume spec into the REST JSON shape.
    fn from(value: ClientVolumeSpec) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            driver: value.driver.into(),
            filesystem_ownership: value.filesystem_ownership.map(FilesystemOwnership::from),
            access_mode: value.access_mode.to_string(),
            binding_mode: value.binding_mode.to_string(),
            reclaim_policy: value.reclaim_policy.to_string(),
            requested_bytes: value.requested_bytes,
            labels: value.labels.into_iter().map(VolumeLabel::from).collect(),
            bound_node_id: value.bound_node_id.map(|id| id.to_string()),
            bound_node_name: value.bound_node_name,
            volume_epoch: value.volume_epoch,
            lifecycle_revision: value.lifecycle_revision,
            lifecycle_request_id: value.lifecycle_request_id.to_string(),
            desired_disposition: value.desired_disposition.to_string(),
            remove_data: value.remove_data,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

/// REST-facing volume label.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct VolumeLabel {
    pub key: String,
    pub value: String,
}

impl From<VolumeLabel> for ClientVolumeLabel {
    /// Converts the REST volume label into the reusable client label.
    fn from(value: VolumeLabel) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
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
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct VolumeNodeStatus {
    pub id: String,
    pub volume_id: String,
    pub node_id: String,
    pub node_name: String,
    pub local_path: Option<String>,
    pub state: String,
    pub health: String,
    pub capacity_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub published_task_ids: Vec<String>,
    pub updated_at: String,
    pub last_error: Option<String>,
    pub volume_epoch: u64,
    pub group_id: Option<String>,
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
            health: value.health.to_string(),
            capacity_bytes: value.capacity_bytes,
            used_bytes: value.used_bytes,
            published_task_ids: value
                .published_task_ids
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            updated_at: value.updated_at,
            last_error: value.last_error,
            volume_epoch: value.volume_epoch,
            group_id: value.group_id.map(|id| id.to_string()),
        }
    }
}

/// REST-facing immutable genesis input for a replicated volume.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ReplicatedVolumePlan {
    pub id: String,
    pub volume_id: String,
    pub volume_epoch: u64,
    pub bootstrap_id: String,
    pub workload_node_id: String,
    pub replica_node_ids: Vec<String>,
    pub generation: u64,
    pub capacity_bytes: u64,
    pub logical_sector_bytes: u32,
    pub physical_block_bytes: u32,
    pub minimum_io_bytes: u32,
    pub data_block_bytes: u32,
}

impl From<ClientReplicatedVolumePlan> for ReplicatedVolumePlan {
    /// Converts immutable replica genesis input into the REST JSON shape.
    fn from(value: ClientReplicatedVolumePlan) -> Self {
        Self {
            id: value.id.to_string(),
            volume_id: value.volume_id.to_string(),
            volume_epoch: value.volume_epoch,
            bootstrap_id: value.bootstrap_id.to_string(),
            workload_node_id: value.workload_node_id.to_string(),
            replica_node_ids: value
                .replica_node_ids
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            generation: value.generation,
            capacity_bytes: value.capacity_bytes,
            logical_sector_bytes: value.logical_sector_bytes,
            physical_block_bytes: value.physical_block_bytes,
            minimum_io_bytes: value.minimum_io_bytes,
            data_block_bytes: value.data_block_bytes,
        }
    }
}

/// REST-facing status copied from the replicated volume's Raft state.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ReplicatedVolumeGroupStatus {
    pub id: String,
    pub volume_id: String,
    pub volume_epoch: u64,
    pub group_id: String,
    pub reporter_node_id: String,
    pub status: String,
    pub committed_index: u64,
    pub leader_node_id: Option<String>,
    pub attached_node_id: Option<String>,
    pub updated_at: String,
    pub message: Option<String>,
    pub control_revision: u64,
    pub fence: Option<u64>,
    pub copy_node_ids: Vec<String>,
    pub voter_node_ids: Vec<String>,
    pub replacement_id: Option<String>,
    pub replacement_old_node_id: Option<String>,
    pub replacement_new_node_id: Option<String>,
    pub degraded: bool,
}

impl From<ClientReplicatedVolumeGroupStatus> for ReplicatedVolumeGroupStatus {
    /// Converts replicated Raft status into the REST JSON shape.
    fn from(value: ClientReplicatedVolumeGroupStatus) -> Self {
        Self {
            id: value.id.to_string(),
            volume_id: value.volume_id.to_string(),
            volume_epoch: value.volume_epoch,
            group_id: value.group_id.to_string(),
            reporter_node_id: value.reporter_node_id.to_string(),
            status: value.status.to_string(),
            committed_index: value.committed_index,
            leader_node_id: value.leader_node_id.map(|id| id.to_string()),
            attached_node_id: value.attached_node_id.map(|id| id.to_string()),
            updated_at: value.updated_at,
            message: value.message,
            control_revision: value.control_revision,
            fence: value.fence,
            copy_node_ids: value
                .copy_node_ids
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            voter_node_ids: value
                .voter_node_ids
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            replacement_id: value.replacement_id.map(|id| id.to_string()),
            replacement_old_node_id: value.replacement_old_node_id.map(|id| id.to_string()),
            replacement_new_node_id: value.replacement_new_node_id.map(|id| id.to_string()),
            degraded: value.degraded,
        }
    }
}

/// REST-facing volume inspection payload.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct VolumeInspect {
    pub state: String,
    pub state_message: Option<String>,
    pub spec: VolumeSpec,
    pub node_states: Vec<VolumeNodeStatus>,
    pub plan: Option<ReplicatedVolumePlan>,
    pub group_status: Option<ReplicatedVolumeGroupStatus>,
}

impl From<ClientVolumeInspect> for VolumeInspect {
    /// Converts the client volume inspect view into the REST JSON shape.
    fn from(value: ClientVolumeInspect) -> Self {
        let state = value.state.to_string();
        Self {
            state,
            state_message: value.state_message,
            spec: value.spec.into(),
            node_states: value
                .node_states
                .into_iter()
                .map(VolumeNodeStatus::from)
                .collect(),
            plan: value.plan.map(ReplicatedVolumePlan::from),
            group_status: value.group_status.map(ReplicatedVolumeGroupStatus::from),
        }
    }
}

/// REST response returned after deleting one volume.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct VolumeDeleteResponse {
    pub preserved_path: Option<String>,
    pub disposition: VolumeDeleteDisposition,
}

/// Accepted logical result of a delete request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VolumeDeleteDisposition {
    Deleted,
    Retained,
}

/// Optional controls for a REST volume delete request.
#[derive(Clone, Copy, Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct VolumeDeleteQuery {
    /// Permanently removes managed backing data instead of applying retain policy.
    #[serde(default)]
    pub delete_data: bool,
}

impl From<VolumeDeleteResult> for VolumeDeleteResponse {
    /// Converts the client delete result into the REST JSON shape.
    fn from(value: VolumeDeleteResult) -> Self {
        Self {
            preserved_path: value.preserved_path,
            disposition: match value.disposition {
                ClientVolumeDeleteDisposition::Deleted => VolumeDeleteDisposition::Deleted,
                ClientVolumeDeleteDisposition::Retained => VolumeDeleteDisposition::Retained,
            },
        }
    }
}
