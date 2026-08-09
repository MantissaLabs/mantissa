use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use thiserror::Error;
use uuid::Uuid;

/// Logical block size supported by the first replicated-volume profile.
pub const REPLICATED_VOLUME_BLOCK_SIZE: u64 = 4096;

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
    Retaining,
    Retained,
    Restoring,
    Failed,
    Deleted,
}

/// Terminal precedence for concurrent desired lifecycle requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum VolumeDeletionRank {
    Live,
    Deleted,
}

/// Requested outcome for one public volume generation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DesiredVolumeDisposition {
    Live,
    Retained,
    Deleted,
}

/// One convergent lifecycle request carried only by desired state.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VolumeLifecycleIntent {
    pub revision: u64,
    pub request_id: Uuid,
    pub disposition: DesiredVolumeDisposition,
    pub remove_data: bool,
}

impl VolumeLifecycleIntent {
    /// Builds the initial live intent for a newly requested generation.
    pub fn live() -> Self {
        Self {
            revision: 0,
            request_id: Uuid::new_v4(),
            disposition: DesiredVolumeDisposition::Live,
            remove_data: false,
        }
    }

    /// Creates the next explicit desired outcome without preserving progress state.
    pub fn next(
        self,
        disposition: DesiredVolumeDisposition,
        remove_data: bool,
    ) -> Result<Self, anyhow::Error> {
        let revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("volume lifecycle revision is exhausted"))?;
        Ok(Self {
            revision,
            request_id: Uuid::new_v4(),
            disposition,
            remove_data,
        })
    }
}

impl VolumeStatus {
    /// Converts one protocol enum into the internal representation.
    pub fn from_proto(status: mantissa_protocol::volumes::VolumeStatus) -> Self {
        match status {
            mantissa_protocol::volumes::VolumeStatus::Pending => Self::Pending,
            mantissa_protocol::volumes::VolumeStatus::Bound => Self::Bound,
            mantissa_protocol::volumes::VolumeStatus::Ready => Self::Ready,
            mantissa_protocol::volumes::VolumeStatus::InUse => Self::InUse,
            mantissa_protocol::volumes::VolumeStatus::Retaining => Self::Retaining,
            mantissa_protocol::volumes::VolumeStatus::Retained => Self::Retained,
            mantissa_protocol::volumes::VolumeStatus::Restoring => Self::Restoring,
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
            Self::Retaining => mantissa_protocol::volumes::VolumeStatus::Retaining,
            Self::Retained => mantissa_protocol::volumes::VolumeStatus::Retained,
            Self::Restoring => mantissa_protocol::volumes::VolumeStatus::Restoring,
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
    Retained,
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
            mantissa_protocol::volumes::VolumeNodeState::Retained => Self::Retained,
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
            Self::Retained => mantissa_protocol::volumes::VolumeNodeState::Retained,
            Self::Deleting => mantissa_protocol::volumes::VolumeNodeState::Deleting,
            Self::Error => mantissa_protocol::volumes::VolumeNodeState::Error,
        }
    }
}

/// Ownership and permissions applied to a Mantissa-managed filesystem.
#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemOwnership {
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

impl FilesystemOwnership {
    /// Resolves the user and group IDs Mantissa applies to one managed filesystem.
    pub fn resolve_ids(self, daemon_uid: u32, daemon_gid: u32) -> (u32, u32) {
        match self {
            Self::Daemon => (daemon_uid, daemon_gid),
            Self::User { uid, gid } => (uid, gid),
            Self::FsGroup { gid } => (daemon_uid, gid),
        }
    }

    /// Returns the directory mode Mantissa applies to the managed filesystem root.
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
    Managed { ownership: FilesystemOwnership },
    ImportedPath { path: String },
}

impl LocalVolumeSpec {
    /// Builds one managed local volume spec with the selected ownership policy.
    pub fn managed(ownership: FilesystemOwnership) -> Self {
        Self::Managed { ownership }
    }

    /// Builds one imported host-path local volume spec.
    pub fn imported_path(path: impl Into<String>) -> Self {
        Self::ImportedPath { path: path.into() }
    }

    /// Returns the effective ownership policy applied by the local volume driver.
    pub fn ownership(&self) -> FilesystemOwnership {
        match self {
            Self::Managed { ownership } => *ownership,
            Self::ImportedPath { .. } => FilesystemOwnership::Daemon,
        }
    }
}

/// Future external driver specification stored on one volume object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExternalVolumeSpec {
    pub driver_name: String,
    pub handle: String,
}

/// Driver settings for a volume stored on three Mantissa nodes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicatedVolumeSpec {
    /// Ownership and permissions applied after the filesystem is mounted.
    pub ownership: FilesystemOwnership,
}

/// Driver configuration stored on one volume object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VolumeDriver {
    Local(LocalVolumeSpec),
    External(ExternalVolumeSpec),
    Replicated(ReplicatedVolumeSpec),
}

impl VolumeDriver {
    /// Returns whether Mantissa stores this volume on a replicated Raft group.
    pub fn is_replicated(&self) -> bool {
        matches!(self, Self::Replicated(_))
    }
}

/// Reason a replicated-volume request cannot use the supported storage profile.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum VolumeRequestError {
    #[error("volume lifecycle request requires a non-zero request id")]
    MissingLifecycleRequestId,
    #[error("remove_data is valid only for terminal volume deletion")]
    InvalidRemoveData,
    #[error("replicated volumes require wait_for_first_consumer binding")]
    ImmediateBinding,
    #[error("replicated volumes require a non-zero capacity")]
    MissingCapacity,
    #[error("replicated volume capacity {requested_bytes} is not aligned to {block_size} bytes")]
    UnalignedCapacity {
        requested_bytes: u64,
        block_size: u64,
    },
    #[error("replicated volume capacity cannot fit in 64-bit replica accounting")]
    CapacityOverflow,
    #[error("replicated volumes require a non-zero bootstrap-plan coordinator")]
    MissingPlanCoordinator,
    #[error("only replicated volumes may name a bootstrap-plan coordinator")]
    UnexpectedPlanCoordinator,
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
    pub bound_node_id: Option<Uuid>,
    pub bound_node_name: Option<String>,
    /// Retry ID shared by volumes bound by the same workload placement.
    pub binding_operation_id: Option<Uuid>,
    /// Monotonic generation that makes a later workload rebind win in the CRDT.
    pub binding_revision: u64,
    /// Immutable node solely allowed to create this generation's random genesis plan.
    pub plan_coordinator_node_id: Option<Uuid>,
    pub volume_epoch: u64,
    pub lifecycle: VolumeLifecycleIntent,
    pub created_at: String,
    pub updated_at: String,
}

impl VolumeSpecValue {
    /// Builds one new volume object from the provided desired-state draft.
    pub fn new(draft: VolumeSpecDraft) -> Self {
        let now = current_timestamp();
        Self {
            id: compute_volume_id(&draft.name),
            name: draft.name,
            driver: draft.driver,
            access_mode: draft.access_mode,
            binding_mode: draft.binding_mode,
            reclaim_policy: draft.reclaim_policy,
            requested_bytes: draft.requested_bytes,
            labels: normalize_labels(draft.labels),
            bound_node_id: draft.bound_node_id,
            bound_node_name: draft.bound_node_name,
            binding_operation_id: None,
            binding_revision: 0,
            plan_coordinator_node_id: None,
            volume_epoch: 0,
            lifecycle: VolumeLifecycleIntent::live(),
            created_at: now.clone(),
            updated_at: now,
        }
    }

    /// Checks the settings that are fixed for the first replicated-volume profile.
    pub fn validate_request(&self) -> Result<(), VolumeRequestError> {
        if self.lifecycle.request_id.is_nil() {
            return Err(VolumeRequestError::MissingLifecycleRequestId);
        }
        if self.lifecycle.remove_data
            && self.lifecycle.disposition != DesiredVolumeDisposition::Deleted
        {
            return Err(VolumeRequestError::InvalidRemoveData);
        }
        if !self.driver.is_replicated() {
            if self.plan_coordinator_node_id.is_some() {
                return Err(VolumeRequestError::UnexpectedPlanCoordinator);
            }
            return Ok(());
        }
        if self.binding_mode != VolumeBindingMode::WaitForFirstConsumer {
            return Err(VolumeRequestError::ImmediateBinding);
        }
        let requested_bytes = self
            .requested_bytes
            .filter(|value| *value != 0)
            .ok_or(VolumeRequestError::MissingCapacity)?;
        if requested_bytes % REPLICATED_VOLUME_BLOCK_SIZE != 0 {
            return Err(VolumeRequestError::UnalignedCapacity {
                requested_bytes,
                block_size: REPLICATED_VOLUME_BLOCK_SIZE,
            });
        }
        let replica_space =
            mantissa_volume::storage_format::ReplicaSpace::for_capacity(requested_bytes)
                .map_err(|_| VolumeRequestError::CapacityOverflow)?;
        replica_space
            .total_bytes()
            .map_err(|_| VolumeRequestError::CapacityOverflow)?;
        if self
            .plan_coordinator_node_id
            .is_none_or(|node_id| node_id.is_nil())
        {
            return Err(VolumeRequestError::MissingPlanCoordinator);
        }
        Ok(())
    }

    /// Returns whether two rows describe the same immutable volume request.
    pub fn has_same_request(&self, other: &Self) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.driver == other.driver
            && self.access_mode == other.access_mode
            && self.binding_mode == other.binding_mode
            && self.reclaim_policy == other.reclaim_policy
            && self.requested_bytes == other.requested_bytes
            && self.labels == other.labels
            && self.plan_coordinator_node_id == other.plan_coordinator_node_id
            && self.volume_epoch == other.volume_epoch
    }

    /// Returns whether this generation has terminal delete intent.
    pub fn is_deleting(&self) -> bool {
        self.lifecycle.disposition == DesiredVolumeDisposition::Deleted
    }

    /// Returns whether this generation is requested to remain retained.
    pub fn is_retaining(&self) -> bool {
        self.lifecycle.disposition == DesiredVolumeDisposition::Retained
    }

    /// Returns whether this generation is requested to remain retained.
    pub fn is_retained(&self) -> bool {
        self.lifecycle.disposition == DesiredVolumeDisposition::Retained
    }

    /// Returns whether desired state permits a live writer.
    pub fn is_restoring(&self) -> bool {
        self.lifecycle.disposition == DesiredVolumeDisposition::Live
    }

    /// Returns whether this generation carries terminal desired deletion.
    pub fn is_deleted(&self) -> bool {
        self.lifecycle.disposition == DesiredVolumeDisposition::Deleted
    }

    /// Returns whether this row is retained deletion evidence rather than a live volume.
    pub fn is_delete_marker(&self) -> bool {
        self.is_deleting() || self.is_deleted()
    }

    /// Returns whether this generation has a saved request to remove its data.
    pub fn data_deletion_was_requested(&self) -> bool {
        self.lifecycle.disposition == DesiredVolumeDisposition::Deleted
            && self.lifecycle.remove_data
    }

    /// Requests retention without storing observed stopping progress.
    pub fn request_retained(&mut self) -> Result<(), anyhow::Error> {
        if !self.is_retained() {
            self.lifecycle = self
                .lifecycle
                .next(DesiredVolumeDisposition::Retained, false)?;
        }
        self.updated_at = current_timestamp();
        Ok(())
    }

    /// Requests live service for a retained generation.
    pub fn request_live(&mut self) -> Result<(), anyhow::Error> {
        if self.lifecycle.disposition != DesiredVolumeDisposition::Live {
            self.lifecycle = self.lifecycle.next(DesiredVolumeDisposition::Live, false)?;
        }
        self.updated_at = current_timestamp();
        Ok(())
    }

    /// Requests terminal deletion and records whether owned data may be removed.
    pub fn request_deleted(&mut self, remove_data: bool) -> Result<(), anyhow::Error> {
        if self.lifecycle.disposition != DesiredVolumeDisposition::Deleted {
            self.lifecycle = self
                .lifecycle
                .next(DesiredVolumeDisposition::Deleted, remove_data)?;
        } else if remove_data && !self.lifecycle.remove_data {
            self.lifecycle = self
                .lifecycle
                .next(DesiredVolumeDisposition::Deleted, true)?;
        }
        self.updated_at = current_timestamp();
        Ok(())
    }

    /// Moves a first-consumer binding as one causally newer desired-state level.
    pub fn move_binding(
        &mut self,
        node_id: Uuid,
        node_name: String,
        operation_id: Uuid,
    ) -> Result<(), anyhow::Error> {
        if self.bound_node_id == Some(node_id) {
            return Ok(());
        }
        self.binding_revision = self
            .binding_revision
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("volume binding revision is exhausted"))?;
        self.bound_node_id = Some(node_id);
        self.bound_node_name = Some(node_name);
        self.binding_operation_id = Some(operation_id);
        self.updated_at = current_timestamp();
        Ok(())
    }

    /// Starts a new live generation after an older generation reached its delete marker.
    pub fn recreate_after(&mut self, previous: &Self) -> Result<(), anyhow::Error> {
        self.volume_epoch = previous
            .volume_epoch
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("volume generation is exhausted"))?;
        self.lifecycle = VolumeLifecycleIntent::live();
        self.binding_operation_id = None;
        self.binding_revision = 0;
        self.plan_coordinator_node_id = None;
        Ok(())
    }

    /// Compares two concurrent rows using the lifecycle order shared by reads and compaction.
    pub fn precedence_cmp(&self, other: &Self) -> Ordering {
        self.volume_epoch
            .cmp(&other.volume_epoch)
            // Deletion is terminal within an epoch. A peer may accept the delete from a view
            // whose phase version lags another writer by more than one transition.
            .then(self.deletion_rank().cmp(&other.deletion_rank()))
            .then(self.lifecycle.revision.cmp(&other.lifecycle.revision))
            .then(self.lifecycle.request_id.cmp(&other.lifecycle.request_id))
            .then(self.binding_revision.cmp(&other.binding_revision))
            // Every volume in one workload placement uses the same ID. Comparing it before
            // progress makes concurrent placements choose the same winner on every shared row.
            .then(self.binding_operation_id.cmp(&other.binding_operation_id))
            .then(compare_volume_timestamps(
                &self.updated_at,
                &other.updated_at,
            ))
            .then(self.bound_node_id.cmp(&other.bound_node_id))
            .then(self.bound_node_name.cmp(&other.bound_node_name))
            .then(self.driver.cmp(&other.driver))
            .then(self.access_mode.cmp(&other.access_mode))
            .then(self.binding_mode.cmp(&other.binding_mode))
            .then(self.reclaim_policy.cmp(&other.reclaim_policy))
            .then(self.requested_bytes.cmp(&other.requested_bytes))
            .then(
                self.plan_coordinator_node_id
                    .cmp(&other.plan_coordinator_node_id),
            )
            // Match the existing compaction rank's Reverse<Value> final tie-breaker.
            .then_with(|| other.cmp(self))
    }

    /// Ranks live, deleting, and fully deleted rows within one immutable generation.
    pub(crate) fn deletion_rank(&self) -> VolumeDeletionRank {
        match self.lifecycle.disposition {
            DesiredVolumeDisposition::Deleted => VolumeDeletionRank::Deleted,
            DesiredVolumeDisposition::Live | DesiredVolumeDisposition::Retained => {
                VolumeDeletionRank::Live
            }
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
    /// Raft group this report belongs to, or none for non-replicated volumes.
    pub group_id: Option<Uuid>,
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
            group_id: None,
        }
    }

    /// Associates this node report with one replicated-volume Raft group.
    pub fn with_group_id(mut self, group_id: Uuid) -> Self {
        self.group_id = Some(group_id);
        self
    }
}

/// Exact replicated-volume descriptor saved with one immutable bootstrap plan.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SavedVolumeDescriptor {
    /// Stable volume UUID.
    pub volume_id: Uuid,
    /// Non-zero storage generation.
    pub generation: u64,
    /// Logical capacity in bytes.
    pub capacity_bytes: u64,
    /// Logical sector size reported by ublk.
    pub logical_sector_bytes: u32,
    /// Physical block size reported by ublk.
    pub physical_block_bytes: u32,
    /// Minimum aligned I/O size reported by ublk.
    pub minimum_io_bytes: u32,
    /// Size of one independently stored data block.
    pub data_block_bytes: u32,
}

impl SavedVolumeDescriptor {
    /// Builds the supported descriptor for one control-plane volume generation.
    pub fn for_volume(
        volume_id: Uuid,
        volume_epoch: u64,
        capacity_bytes: u64,
    ) -> Result<Self, anyhow::Error> {
        let generation = volume_epoch
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("replicated volume generation is exhausted"))?;
        let descriptor = mantissa_volume::VolumeDescriptor::new(
            mantissa_volume::VolumeId::new(volume_id)?,
            mantissa_volume::VolumeGeneration::new(generation)?,
            capacity_bytes,
            mantissa_volume::VolumeBlockSizes::supported(),
        )?;
        Ok(Self::from_storage(&descriptor))
    }

    /// Copies one checked storage descriptor into the immutable bootstrap plan.
    #[must_use]
    pub fn from_storage(descriptor: &mantissa_volume::VolumeDescriptor) -> Self {
        let block_sizes = descriptor.block_sizes();
        Self {
            volume_id: *descriptor.volume_id().as_uuid(),
            generation: descriptor.generation().get(),
            capacity_bytes: descriptor.capacity().bytes(),
            logical_sector_bytes: block_sizes.logical_sector().bytes(),
            physical_block_bytes: block_sizes.physical_block().bytes(),
            minimum_io_bytes: block_sizes.minimum_io().bytes(),
            data_block_bytes: block_sizes.data_block().bytes(),
        }
    }

    /// Rebuilds and validates the descriptor used by local storage and Raft.
    pub fn to_storage(self) -> Result<mantissa_volume::VolumeDescriptor, anyhow::Error> {
        let volume_id = mantissa_volume::VolumeId::new(self.volume_id)?;
        let generation = mantissa_volume::VolumeGeneration::new(self.generation)?;
        let block_sizes = mantissa_volume::VolumeBlockSizes::new(
            self.logical_sector_bytes,
            self.physical_block_bytes,
            self.minimum_io_bytes,
            self.data_block_bytes,
        )?;
        Ok(mantissa_volume::VolumeDescriptor::new(
            volume_id,
            generation,
            self.capacity_bytes,
            block_sizes,
        )?)
    }
}

/// Immutable genesis input shared by all three bootstrap members.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicatedVolumePlan {
    /// Stable ID used as the replicated-store key.
    pub id: Uuid,
    /// Volume this immutable bootstrap plan belongs to.
    pub volume_id: Uuid,
    /// Volume generation this immutable bootstrap plan belongs to.
    pub volume_epoch: u64,
    /// Stable bootstrap identity reused by every ensure retry.
    pub bootstrap_id: Uuid,
    /// Node selected to run the first workload.
    pub workload_node_id: Uuid,
    /// Three nodes selected to store the volume.
    pub replica_node_ids: [Uuid; 3],
    /// Exact identity, capacity, and block sizes used by all three replicas.
    pub descriptor: SavedVolumeDescriptor,
}

impl ReplicatedVolumePlan {
    /// Builds immutable bootstrap input for one volume generation.
    pub fn new(
        volume_id: Uuid,
        volume_epoch: u64,
        bootstrap_id: Uuid,
        workload_node_id: Uuid,
        replica_node_ids: [Uuid; 3],
        descriptor: SavedVolumeDescriptor,
    ) -> Self {
        Self {
            id: compute_replicated_volume_plan_id(volume_id, volume_epoch),
            volume_id,
            volume_epoch,
            bootstrap_id,
            workload_node_id,
            replica_node_ids,
            descriptor,
        }
    }

    /// Returns whether two concurrent values contain identical genesis input.
    pub fn has_same_plan(&self, other: &Self) -> bool {
        self == other
    }
}

/// Latest report copied from one replicated volume's committed Raft state.
///
/// This record is for status and scheduling. The storage runtime must still
/// check its local Raft state before it accepts an attachment or block request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicatedVolumeGroupStatusValue {
    /// Stable ID used as the replicated-store key.
    pub id: Uuid,
    /// Volume this report belongs to.
    pub volume_id: Uuid,
    /// Volume generation this report belongs to.
    pub volume_epoch: u64,
    /// Public ID derived from the volume ID and storage generation.
    pub group_id: Uuid,
    /// Node that produced this report.
    pub reporter_node_id: Uuid,
    /// Public state observed from committed group state.
    pub status: VolumeStatus,
    /// Highest committed Raft log index included in this report.
    pub committed_index: u64,
    /// Current Raft leader, if known.
    pub leader_node_id: Option<Uuid>,
    /// Node serving the volume, if attached.
    pub attached_node_id: Option<Uuid>,
    /// RFC3339 timestamp when this report was produced.
    pub updated_at: String,
    /// Short status or failure detail.
    pub message: Option<String>,
    /// Revision of the committed bounded volume control state.
    pub control_revision: u64,
    /// Current non-zero data fence, or none before initialization.
    pub fence: Option<u64>,
    /// Current active data copies from committed control state.
    pub copy_node_ids: Vec<Uuid>,
    /// Current committed Raft voters.
    pub voter_node_ids: Vec<Uuid>,
    /// Current replacement grant, when one is active.
    pub replacement_id: Option<Uuid>,
    /// Current copy being replaced, when the grant removes one.
    pub replacement_old_node_id: Option<Uuid>,
    /// Current inactive replacement target.
    pub replacement_new_node_id: Option<Uuid>,
    /// Whether committed control state or membership is outside steady state.
    pub degraded: bool,
}

impl ReplicatedVolumeGroupStatusValue {
    /// Builds one report copied from committed state for a replicated volume group.
    pub fn new(
        volume_id: Uuid,
        volume_epoch: u64,
        group_id: Uuid,
        reporter_node_id: Uuid,
        status: VolumeStatus,
        committed_index: u64,
    ) -> Self {
        Self {
            id: compute_replicated_volume_group_status_id(volume_id, volume_epoch, group_id),
            volume_id,
            volume_epoch,
            group_id,
            reporter_node_id,
            status,
            committed_index,
            leader_node_id: None,
            attached_node_id: None,
            updated_at: current_timestamp(),
            message: None,
            control_revision: 0,
            fence: None,
            copy_node_ids: Vec::new(),
            voter_node_ids: Vec::new(),
            replacement_id: None,
            replacement_old_node_id: None,
            replacement_new_node_id: None,
            degraded: false,
        }
    }

    /// Compares reports by committed progress before timestamps or local details.
    pub fn precedence_cmp(&self, other: &Self) -> Ordering {
        self.volume_epoch
            .cmp(&other.volume_epoch)
            .then(self.committed_index.cmp(&other.committed_index))
            .then(compare_volume_timestamps(
                &self.updated_at,
                &other.updated_at,
            ))
            .then(self.status.cmp(&other.status))
            .then(self.leader_node_id.cmp(&other.leader_node_id))
            .then(self.attached_node_id.cmp(&other.attached_node_id))
            .then_with(|| other.cmp(self))
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
    PlanUpsert(Box<ReplicatedVolumePlan>),
    PlanRemove(Uuid),
    GroupStatusUpsert(Box<ReplicatedVolumeGroupStatusValue>),
    GroupStatusRemove(Uuid),
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

/// Computes the stable immutable-plan ID for one volume generation.
pub fn compute_replicated_volume_plan_id(volume_id: Uuid, volume_epoch: u64) -> Uuid {
    compute_volume_record_id(b"replicated-volume-plan", volume_id, volume_epoch)
}

/// Gives every volume a stable pseudo-random node order for balanced storage placement.
pub(crate) fn compute_replicated_volume_node_score(volume_id: Uuid, node_id: Uuid) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"replicated-volume-node");
    hasher.update(volume_id.as_bytes());
    hasher.update(node_id.as_bytes());
    *hasher.finalize().as_bytes()
}

/// Computes the public ID for the Raft group of one volume generation.
pub fn compute_replicated_volume_group_id(volume_id: Uuid, generation: u64) -> Uuid {
    compute_volume_record_id(b"replicated-volume-group", volume_id, generation)
}

/// Computes the stable group-status-record ID for one volume generation and Raft group.
pub fn compute_replicated_volume_group_status_id(
    volume_id: Uuid,
    volume_epoch: u64,
    group_id: Uuid,
) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"replicated-volume-group-status");
    hasher.update(volume_id.as_bytes());
    hasher.update(&volume_epoch.to_be_bytes());
    hasher.update(group_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Computes one stable metadata-record ID from its type, volume, and generation.
fn compute_volume_record_id(kind: &[u8], volume_id: Uuid, volume_epoch: u64) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(kind);
    hasher.update(volume_id.as_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a replicated request with the selected logical capacity.
    fn replicated_request(capacity_bytes: u64) -> VolumeSpecValue {
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: "data".to_string(),
            driver: VolumeDriver::Replicated(ReplicatedVolumeSpec {
                ownership: FilesystemOwnership::Daemon,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Retain,
            requested_bytes: Some(capacity_bytes),
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        spec.plan_coordinator_node_id = Some(Uuid::from_u128(10));
        spec
    }

    /// Replicated genesis ownership is mandatory and invalid on other drivers.
    #[test]
    fn plan_coordinator_is_part_of_the_replicated_request() {
        let mut replicated = replicated_request(REPLICATED_VOLUME_BLOCK_SIZE);
        replicated.plan_coordinator_node_id = None;
        assert_eq!(
            replicated.validate_request(),
            Err(VolumeRequestError::MissingPlanCoordinator)
        );
        replicated.plan_coordinator_node_id = Some(Uuid::nil());
        assert_eq!(
            replicated.validate_request(),
            Err(VolumeRequestError::MissingPlanCoordinator)
        );
        replicated.plan_coordinator_node_id = Some(Uuid::from_u128(20));
        assert_eq!(replicated.validate_request(), Ok(()));

        let mut local = VolumeSpecValue::new(VolumeSpecDraft {
            name: "local".to_string(),
            driver: VolumeDriver::Local(LocalVolumeSpec::managed(FilesystemOwnership::Daemon)),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Retain,
            requested_bytes: None,
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        local.plan_coordinator_node_id = Some(Uuid::from_u128(20));
        assert_eq!(
            local.validate_request(),
            Err(VolumeRequestError::UnexpectedPlanCoordinator)
        );
    }

    /// Checks that capacity accounting rejects values whose replica files overflow.
    #[test]
    fn replicated_capacity_must_fit_replica_accounting() {
        assert_eq!(
            replicated_request(u64::MAX - 4095).validate_request(),
            Err(VolumeRequestError::CapacityOverflow)
        );
    }

    /// A later binding level wins independently of operation IDs and wall clocks.
    #[test]
    fn binding_revision_orders_replicated_volume_moves() {
        let mut previous = replicated_request(REPLICATED_VOLUME_BLOCK_SIZE);
        previous.bound_node_id = Some(Uuid::from_u128(1));
        previous.bound_node_name = Some("node-a".to_string());
        previous.binding_operation_id = Some(Uuid::from_u128(u128::MAX));
        previous.binding_revision = 1;
        previous.updated_at = "2099-01-01T00:00:00Z".to_string();

        let mut moved = previous.clone();
        moved
            .move_binding(Uuid::from_u128(2), "node-b".to_string(), Uuid::from_u128(1))
            .expect("binding revision must advance");
        moved.updated_at = "2000-01-01T00:00:00Z".to_string();

        assert!(moved.precedence_cmp(&previous).is_gt());
        assert_eq!(
            moved.plan_coordinator_node_id,
            previous.plan_coordinator_node_id
        );
    }
}
