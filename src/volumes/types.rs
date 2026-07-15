use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
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
    pub fn from_proto(mode: mantissa_protocol::volumes::VolumeAccessMode) -> Self {
        match mode {
            mantissa_protocol::volumes::VolumeAccessMode::ReadWriteOnce => Self::ReadWriteOnce,
        }
    }

    /// Converts the internal representation into the protocol enum.
    pub fn to_proto(self) -> mantissa_protocol::volumes::VolumeAccessMode {
        match self {
            Self::ReadWriteOnce => mantissa_protocol::volumes::VolumeAccessMode::ReadWriteOnce,
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
    pub fn from_proto(mode: mantissa_protocol::volumes::VolumeBindingMode) -> Self {
        match mode {
            mantissa_protocol::volumes::VolumeBindingMode::Immediate => Self::Immediate,
            mantissa_protocol::volumes::VolumeBindingMode::WaitForFirstConsumer => {
                Self::WaitForFirstConsumer
            }
        }
    }

    /// Converts the internal representation into the protocol enum.
    pub fn to_proto(self) -> mantissa_protocol::volumes::VolumeBindingMode {
        match self {
            Self::Immediate => mantissa_protocol::volumes::VolumeBindingMode::Immediate,
            Self::WaitForFirstConsumer => {
                mantissa_protocol::volumes::VolumeBindingMode::WaitForFirstConsumer
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
    pub fn from_proto(policy: mantissa_protocol::volumes::VolumeReclaimPolicy) -> Self {
        match policy {
            mantissa_protocol::volumes::VolumeReclaimPolicy::Retain => Self::Retain,
            mantissa_protocol::volumes::VolumeReclaimPolicy::Delete => Self::Delete,
        }
    }

    /// Converts the internal representation into the protocol enum.
    pub fn to_proto(self) -> mantissa_protocol::volumes::VolumeReclaimPolicy {
        match self {
            Self::Retain => mantissa_protocol::volumes::VolumeReclaimPolicy::Retain,
            Self::Delete => mantissa_protocol::volumes::VolumeReclaimPolicy::Delete,
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
    Deleted,
}

/// Lifecycle precedence for concurrent rows within one volume generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum VolumeDeletionRank {
    Live,
    Deleting,
    Deleted,
}

impl VolumeStatus {
    /// Converts one protocol enum into the internal representation.
    pub fn from_proto(status: mantissa_protocol::volumes::VolumeStatus) -> Self {
        match status {
            mantissa_protocol::volumes::VolumeStatus::Pending => Self::Pending,
            mantissa_protocol::volumes::VolumeStatus::Bound => Self::Bound,
            mantissa_protocol::volumes::VolumeStatus::Ready => Self::Ready,
            mantissa_protocol::volumes::VolumeStatus::InUse => Self::InUse,
            mantissa_protocol::volumes::VolumeStatus::Deleting => Self::Deleting,
            mantissa_protocol::volumes::VolumeStatus::Failed => Self::Failed,
            mantissa_protocol::volumes::VolumeStatus::Deleted => Self::Deleted,
        }
    }

    /// Converts the internal representation into the protocol enum.
    pub fn to_proto(self) -> mantissa_protocol::volumes::VolumeStatus {
        match self {
            Self::Pending => mantissa_protocol::volumes::VolumeStatus::Pending,
            Self::Bound => mantissa_protocol::volumes::VolumeStatus::Bound,
            Self::Ready => mantissa_protocol::volumes::VolumeStatus::Ready,
            Self::InUse => mantissa_protocol::volumes::VolumeStatus::InUse,
            Self::Deleting => mantissa_protocol::volumes::VolumeStatus::Deleting,
            Self::Failed => mantissa_protocol::volumes::VolumeStatus::Failed,
            Self::Deleted => mantissa_protocol::volumes::VolumeStatus::Deleted,
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
    pub fn from_proto(state: mantissa_protocol::volumes::VolumeNodeState) -> Self {
        match state {
            mantissa_protocol::volumes::VolumeNodeState::Pending => Self::Pending,
            mantissa_protocol::volumes::VolumeNodeState::Provisioning => Self::Provisioning,
            mantissa_protocol::volumes::VolumeNodeState::Ready => Self::Ready,
            mantissa_protocol::volumes::VolumeNodeState::Published => Self::Published,
            mantissa_protocol::volumes::VolumeNodeState::Deleting => Self::Deleting,
            mantissa_protocol::volumes::VolumeNodeState::Error => Self::Error,
        }
    }

    /// Converts the internal representation into the protocol enum.
    pub fn to_proto(self) -> mantissa_protocol::volumes::VolumeNodeState {
        match self {
            Self::Pending => mantissa_protocol::volumes::VolumeNodeState::Pending,
            Self::Provisioning => mantissa_protocol::volumes::VolumeNodeState::Provisioning,
            Self::Ready => mantissa_protocol::volumes::VolumeNodeState::Ready,
            Self::Published => mantissa_protocol::volumes::VolumeNodeState::Published,
            Self::Deleting => mantissa_protocol::volumes::VolumeNodeState::Deleting,
            Self::Error => mantissa_protocol::volumes::VolumeNodeState::Error,
        }
    }
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
///
/// Imported host paths are not owned by Mantissa, so they cannot carry a managed-volume
/// ownership policy. Keeping the variants separate makes that invalid state unrepresentable
/// inside persisted volume rows.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum LocalVolumeSpec {
    Managed { ownership: LocalVolumeOwnership },
    ImportedPath { path: String },
}

impl LocalVolumeSpec {
    /// Builds one managed local volume spec with the selected ownership policy.
    pub fn managed(ownership: LocalVolumeOwnership) -> Self {
        Self::Managed { ownership }
    }

    /// Builds one imported host-path local volume spec.
    pub fn imported_path(path: impl Into<String>) -> Self {
        Self::ImportedPath { path: path.into() }
    }

    /// Returns the effective ownership policy applied by the local volume driver.
    pub fn ownership(&self) -> LocalVolumeOwnership {
        match self {
            Self::Managed { ownership } => *ownership,
            Self::ImportedPath { .. } => LocalVolumeOwnership::Daemon,
        }
    }
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
            (VolumeDriver::Local(LocalVolumeSpec::ImportedPath { .. }), Some(_)) => {
                VolumeStatus::Ready
            }
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

    /// Returns whether this volume generation is currently being removed.
    pub fn is_deleting(&self) -> bool {
        self.status == VolumeStatus::Deleting
    }

    /// Returns whether cleanup completed for this deleted volume generation.
    pub fn is_deleted(&self) -> bool {
        self.status == VolumeStatus::Deleted
    }

    /// Returns whether this row is retained deletion evidence rather than a live volume.
    pub fn is_delete_marker(&self) -> bool {
        self.is_deleting() || self.is_deleted()
    }

    /// Advances this generation to the terminal delete marker used during eventual convergence.
    pub fn mark_deleting(&mut self) {
        if !self.is_deleting() {
            self.phase_version = self.phase_version.saturating_add(1);
        }
        self.status = VolumeStatus::Deleting;
        self.updated_at = current_timestamp();
        self.reason = Some("delete_requested".to_string());
        self.message = Some("volume deletion requested".to_string());
    }

    /// Seals one generation after its node-local cleanup has completed.
    pub fn mark_deleted(&mut self) {
        if !self.is_deleted() {
            self.phase_version = self.phase_version.saturating_add(1);
        }
        self.status = VolumeStatus::Deleted;
        self.updated_at = current_timestamp();
        self.reason = Some("deleted".to_string());
        self.message = Some("volume deletion completed".to_string());
    }

    /// Starts a new live generation after an older generation reached its delete marker.
    pub fn recreate_after(&mut self, previous: &Self) {
        self.volume_epoch = previous.volume_epoch.saturating_add(1);
        self.phase_version = 0;
    }

    /// Compares two concurrent rows using the lifecycle order shared by reads and compaction.
    pub fn precedence_cmp(&self, other: &Self) -> Ordering {
        self.volume_epoch
            .cmp(&other.volume_epoch)
            // Deletion is terminal within an epoch. A peer may accept the delete from a view
            // whose phase version lags another writer by more than one transition.
            .then(self.deletion_rank().cmp(&other.deletion_rank()))
            .then(self.phase_version.cmp(&other.phase_version))
            .then(compare_volume_timestamps(
                &self.updated_at,
                &other.updated_at,
            ))
            .then(self.status.cmp(&other.status))
            .then(self.bound_node_id.cmp(&other.bound_node_id))
            .then(self.bound_node_name.cmp(&other.bound_node_name))
            .then(self.driver.cmp(&other.driver))
            .then(self.access_mode.cmp(&other.access_mode))
            .then(self.binding_mode.cmp(&other.binding_mode))
            .then(self.reclaim_policy.cmp(&other.reclaim_policy))
            .then(self.requested_bytes.cmp(&other.requested_bytes))
            .then(self.reason.cmp(&other.reason))
            .then(self.message.cmp(&other.message))
            // Match the existing compaction rank's Reverse<Value> final tie-breaker.
            .then_with(|| other.cmp(self))
    }

    /// Ranks live, deleting, and fully deleted rows within one immutable generation.
    pub(crate) fn deletion_rank(&self) -> VolumeDeletionRank {
        match self.status {
            VolumeStatus::Deleting => VolumeDeletionRank::Deleting,
            VolumeStatus::Deleted => VolumeDeletionRank::Deleted,
            _ => VolumeDeletionRank::Live,
        }
    }
}

/// Compares two RFC3339 volume timestamps and falls back to their stable raw representation.
pub(crate) fn compare_volume_timestamps(left: &str, right: &str) -> Ordering {
    match (
        DateTime::parse_from_rfc3339(left),
        DateTime::parse_from_rfc3339(right),
    ) {
        (Ok(left_ts), Ok(right_ts)) => left_ts
            .with_timezone(&Utc)
            .cmp(&right_ts.with_timezone(&Utc)),
        _ => left.cmp(right),
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
    pub volume_epoch: u64,
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
        volume_epoch: u64,
    ) -> Self {
        Self {
            id: compute_volume_node_state_id(volume_id, node_id, volume_epoch),
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
            volume_epoch,
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
pub fn compute_volume_node_state_id(volume_id: Uuid, node_id: Uuid, volume_epoch: u64) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(volume_id.as_bytes());
    hasher.update(node_id.as_bytes());
    hasher.update(&volume_epoch.to_be_bytes());
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
