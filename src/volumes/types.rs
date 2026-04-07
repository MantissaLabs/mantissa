use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One user-defined key/value pair attached to a volume object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VolumeLabel {
    pub key: String,
    pub value: String,
}

/// Access modes supported by Mantissa-managed volumes.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VolumeAccessMode {
    ReadWriteOnce,
}

impl VolumeAccessMode {
    /// Converts one protocol enum into the internal representation.
    pub fn from_proto(mode: protocol::volumes::VolumeAccessMode) -> Self {
        match mode {
            protocol::volumes::VolumeAccessMode::ReadWriteOnce => Self::ReadWriteOnce,
        }
    }

    /// Converts the internal representation into the protocol enum.
    pub fn to_proto(self) -> protocol::volumes::VolumeAccessMode {
        match self {
            Self::ReadWriteOnce => protocol::volumes::VolumeAccessMode::ReadWriteOnce,
        }
    }
}

/// Binding modes supported by Mantissa-managed volumes.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VolumeBindingMode {
    Immediate,
    WaitForFirstConsumer,
}

impl VolumeBindingMode {
    /// Converts one protocol enum into the internal representation.
    pub fn from_proto(mode: protocol::volumes::VolumeBindingMode) -> Self {
        match mode {
            protocol::volumes::VolumeBindingMode::Immediate => Self::Immediate,
            protocol::volumes::VolumeBindingMode::WaitForFirstConsumer => {
                Self::WaitForFirstConsumer
            }
        }
    }

    /// Converts the internal representation into the protocol enum.
    pub fn to_proto(self) -> protocol::volumes::VolumeBindingMode {
        match self {
            Self::Immediate => protocol::volumes::VolumeBindingMode::Immediate,
            Self::WaitForFirstConsumer => {
                protocol::volumes::VolumeBindingMode::WaitForFirstConsumer
            }
        }
    }
}

/// Reclaim policies supported by Mantissa-managed volumes.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VolumeReclaimPolicy {
    Retain,
    Delete,
}

impl VolumeReclaimPolicy {
    /// Converts one protocol enum into the internal representation.
    pub fn from_proto(policy: protocol::volumes::VolumeReclaimPolicy) -> Self {
        match policy {
            protocol::volumes::VolumeReclaimPolicy::Retain => Self::Retain,
            protocol::volumes::VolumeReclaimPolicy::Delete => Self::Delete,
        }
    }

    /// Converts the internal representation into the protocol enum.
    pub fn to_proto(self) -> protocol::volumes::VolumeReclaimPolicy {
        match self {
            Self::Retain => protocol::volumes::VolumeReclaimPolicy::Retain,
            Self::Delete => protocol::volumes::VolumeReclaimPolicy::Delete,
        }
    }
}

/// Lifecycle states exposed for one volume object.
#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum VolumeStatus {
    #[default]
    Pending,
    Bound,
    Ready,
    InUse,
    Deleting,
    Failed,
}

impl VolumeStatus {
    /// Converts one protocol enum into the internal representation.
    pub fn from_proto(status: protocol::volumes::VolumeStatus) -> Self {
        match status {
            protocol::volumes::VolumeStatus::Pending => Self::Pending,
            protocol::volumes::VolumeStatus::Bound => Self::Bound,
            protocol::volumes::VolumeStatus::Ready => Self::Ready,
            protocol::volumes::VolumeStatus::InUse => Self::InUse,
            protocol::volumes::VolumeStatus::Deleting => Self::Deleting,
            protocol::volumes::VolumeStatus::Failed => Self::Failed,
        }
    }

    /// Converts the internal representation into the protocol enum.
    pub fn to_proto(self) -> protocol::volumes::VolumeStatus {
        match self {
            Self::Pending => protocol::volumes::VolumeStatus::Pending,
            Self::Bound => protocol::volumes::VolumeStatus::Bound,
            Self::Ready => protocol::volumes::VolumeStatus::Ready,
            Self::InUse => protocol::volumes::VolumeStatus::InUse,
            Self::Deleting => protocol::volumes::VolumeStatus::Deleting,
            Self::Failed => protocol::volumes::VolumeStatus::Failed,
        }
    }
}

/// Node-local lifecycle states exposed for one realized volume row.
#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum VolumeNodeState {
    #[default]
    Pending,
    Provisioning,
    Ready,
    Published,
    Deleting,
    Error,
}

impl VolumeNodeState {
    /// Converts one protocol enum into the internal representation.
    pub fn from_proto(state: protocol::volumes::VolumeNodeState) -> Self {
        match state {
            protocol::volumes::VolumeNodeState::Pending => Self::Pending,
            protocol::volumes::VolumeNodeState::Provisioning => Self::Provisioning,
            protocol::volumes::VolumeNodeState::Ready => Self::Ready,
            protocol::volumes::VolumeNodeState::Published => Self::Published,
            protocol::volumes::VolumeNodeState::Deleting => Self::Deleting,
            protocol::volumes::VolumeNodeState::Error => Self::Error,
        }
    }

    /// Converts the internal representation into the protocol enum.
    pub fn to_proto(self) -> protocol::volumes::VolumeNodeState {
        match self {
            Self::Pending => protocol::volumes::VolumeNodeState::Pending,
            Self::Provisioning => protocol::volumes::VolumeNodeState::Provisioning,
            Self::Ready => protocol::volumes::VolumeNodeState::Ready,
            Self::Published => protocol::volumes::VolumeNodeState::Published,
            Self::Deleting => protocol::volumes::VolumeNodeState::Deleting,
            Self::Error => protocol::volumes::VolumeNodeState::Error,
        }
    }
}

/// The source kind used by the built-in local driver.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum LocalVolumeSource {
    Managed,
    ImportedPath(String),
}

/// Explicit ownership policy applied to Mantissa-managed local volume directories.
#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum LocalVolumeOwnership {
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

impl LocalVolumeOwnership {
    /// Resolves the uid and gid Mantissa should apply on the bound node for one managed volume.
    pub fn resolve_ids(self, daemon_uid: u32, daemon_gid: u32) -> (u32, u32) {
        match self {
            Self::Daemon => (daemon_uid, daemon_gid),
            Self::User { uid, gid } => (uid, gid),
            Self::FsGroup { gid } => (daemon_uid, gid),
        }
    }

    /// Returns the directory mode Mantissa applies to the managed volume root for this policy.
    pub fn directory_mode(self) -> u32 {
        match self {
            Self::Daemon | Self::User { .. } => 0o750,
            Self::FsGroup { .. } => 0o2770,
        }
    }
}

/// Built-in local driver specification stored on one volume object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LocalVolumeSpec {
    pub source: LocalVolumeSource,
    pub ownership: LocalVolumeOwnership,
}

/// Future external driver specification stored on one volume object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExternalVolumeSpec {
    pub driver_name: String,
    pub handle: String,
}

/// Driver configuration stored on one volume object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VolumeDriver {
    Local(LocalVolumeSpec),
    External(ExternalVolumeSpec),
}

/// Desired-state row replicated for one volume object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VolumeSpecValue {
    pub id: Uuid,
    pub name: String,
    pub driver: VolumeDriver,
    pub access_mode: VolumeAccessMode,
    pub binding_mode: VolumeBindingMode,
    pub reclaim_policy: VolumeReclaimPolicy,
    pub requested_bytes: Option<u64>,
    pub labels: Vec<VolumeLabel>,
    pub status: VolumeStatus,
    pub bound_node_id: Option<Uuid>,
    pub bound_node_name: Option<String>,
    pub volume_epoch: u64,
    pub phase_version: u64,
    pub created_at: String,
    pub updated_at: String,
    pub reason: Option<String>,
    pub message: Option<String>,
}

impl VolumeSpecValue {
    /// Builds one new volume object from the provided desired-state draft.
    pub fn new(draft: VolumeSpecDraft) -> Self {
        let now = current_timestamp();
        let status = match (&draft.driver, draft.bound_node_id) {
            (
                VolumeDriver::Local(LocalVolumeSpec {
                    source: LocalVolumeSource::ImportedPath(_),
                    ..
                }),
                Some(_),
            ) => VolumeStatus::Ready,
            (_, Some(_)) => VolumeStatus::Bound,
            _ => VolumeStatus::Pending,
        };

        Self {
            id: compute_volume_id(&draft.name),
            name: draft.name,
            driver: draft.driver,
            access_mode: draft.access_mode,
            binding_mode: draft.binding_mode,
            reclaim_policy: draft.reclaim_policy,
            requested_bytes: draft.requested_bytes,
            labels: normalize_labels(draft.labels),
            status,
            bound_node_id: draft.bound_node_id,
            bound_node_name: draft.bound_node_name,
            volume_epoch: 0,
            phase_version: 0,
            created_at: now.clone(),
            updated_at: now,
            reason: None,
            message: None,
        }
    }
}

/// Node-local replicated row for one volume on one node.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VolumeNodeStateValue {
    pub id: Uuid,
    pub volume_id: Uuid,
    pub node_id: Uuid,
    pub node_name: String,
    pub local_path: Option<String>,
    pub state: VolumeNodeState,
    pub capacity_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub published_task_ids: Vec<Uuid>,
    pub updated_at: String,
    pub last_error: Option<String>,
}

impl VolumeNodeStateValue {
    /// Builds one new node-local volume row for the selected volume and node.
    pub fn new(
        volume_id: Uuid,
        node_id: Uuid,
        node_name: impl Into<String>,
        local_path: Option<String>,
        state: VolumeNodeState,
        capacity_bytes: Option<u64>,
    ) -> Self {
        Self {
            id: compute_volume_node_state_id(volume_id, node_id),
            volume_id,
            node_id,
            node_name: node_name.into(),
            local_path,
            state,
            capacity_bytes,
            used_bytes: None,
            published_task_ids: Vec::new(),
            updated_at: current_timestamp(),
            last_error: None,
        }
    }
}

/// Draft inputs used to create one new volume object.
#[derive(Clone, Debug)]
pub struct VolumeSpecDraft {
    pub name: String,
    pub driver: VolumeDriver,
    pub access_mode: VolumeAccessMode,
    pub binding_mode: VolumeBindingMode,
    pub reclaim_policy: VolumeReclaimPolicy,
    pub requested_bytes: Option<u64>,
    pub labels: Vec<VolumeLabel>,
    pub bound_node_id: Option<Uuid>,
    pub bound_node_name: Option<String>,
}

/// Gossip event used to replicate volume updates immediately.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum VolumeEvent {
    Upsert(Box<VolumeSpecValue>),
    Remove(Uuid),
    NodeUpsert(Box<VolumeNodeStateValue>),
    NodeRemove(Uuid),
}

/// Computes one stable volume identifier from its logical name.
pub fn compute_volume_id(name: &str) -> Uuid {
    let digest = blake3::hash(name.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Computes one stable node-state identifier from the volume and node identifiers.
pub fn compute_volume_node_state_id(volume_id: Uuid, node_id: Uuid) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(volume_id.as_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Returns one RFC3339 timestamp for replicated volume metadata.
fn current_timestamp() -> String {
    Utc::now().to_rfc3339()
}

/// Returns volume labels sorted and deduplicated by key for deterministic MVReg ordering.
fn normalize_labels(mut labels: Vec<VolumeLabel>) -> Vec<VolumeLabel> {
    labels.sort_by(|a, b| a.key.cmp(&b.key).then(a.value.cmp(&b.value)));
    labels.dedup_by(|left, right| left.key == right.key);
    labels
}
