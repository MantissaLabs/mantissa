use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::driver::{InvalidUblkSettings, UblkDeviceId, UblkQueueSettings, UblkSettings};
use crate::fs::volume::ReplicatedVolumeFilesystem;
use crate::{
    DriverSessionId, FenceEpoch, FilesystemId, OperationId, ReplacementId, VolumeDescriptor,
    VolumeGeneration, VolumeId,
};

/// Local catalog key for one volume generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReplicaKey {
    volume_id: VolumeId,
    generation: VolumeGeneration,
}

impl ReplicaKey {
    /// Creates the key stored for one descriptor.
    #[must_use]
    pub const fn new(volume_id: VolumeId, generation: VolumeGeneration) -> Self {
        Self {
            volume_id,
            generation,
        }
    }

    /// Returns the stable volume ID.
    #[must_use]
    pub const fn volume_id(self) -> VolumeId {
        self.volume_id
    }

    /// Returns the destructive-rebuild generation.
    #[must_use]
    pub const fn generation(self) -> VolumeGeneration {
        self.generation
    }
}

impl From<&VolumeDescriptor> for ReplicaKey {
    /// Builds the local key from a checked descriptor.
    fn from(descriptor: &VolumeDescriptor) -> Self {
        Self::new(descriptor.volume_id(), descriptor.generation())
    }
}

/// State of one local replica directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaState {
    /// Space is held while the first local files are created.
    Preparing,

    /// The local files are complete and may be opened.
    Ready,

    /// The local files are being removed.
    Deleting,

    /// Current control state removed this node and its files are being released.
    Retiring,

    /// The local files are fenced but kept for an operator.
    Retained,
}

impl ReplicaState {
    /// Checks the small node-local file lifecycle.
    pub(super) fn can_change_to(self, requested: Self) -> bool {
        self == requested
            || matches!(
                (self, requested),
                (
                    Self::Preparing,
                    Self::Ready | Self::Deleting | Self::Retiring
                ) | (
                    Self::Ready,
                    Self::Deleting | Self::Retiring | Self::Retained
                ) | (
                    Self::Retained,
                    Self::Ready | Self::Deleting | Self::Retiring
                )
            )
    }
}

/// Durable safety health of one local replica file.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReplicaHealth {
    /// The checked local file may serve when local admission also permits it.
    #[default]
    Healthy,

    /// Validation or uncertain I/O requires recovery before serving again.
    NeedsRecovery,
}

impl fmt::Display for ReplicaState {
    /// Writes the plain state name used in catalog errors.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Preparing => "preparing",
            Self::Ready => "ready",
            Self::Deleting => "deleting",
            Self::Retiring => "retiring",
            Self::Retained => "retained",
        };
        formatter.write_str(name)
    }
}

/// Data and metadata space held for one replica.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReservedSpace {
    data_bytes: u64,
    metadata_bytes: u64,
}

/// Durable reason this node created one local data copy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalReplicaOrigin {
    /// One immutable initial plan available from desired state.
    Bootstrap(OperationId),

    /// One committed replacement and voters able to confirm whether it remains current.
    Replacement {
        id: ReplacementId,
        voter_node_ids: BTreeSet<Uuid>,
    },
}

impl LocalReplicaOrigin {
    /// Creates one replacement origin with the safe voters that granted provisioning.
    pub fn replacement(
        id: ReplacementId,
        voter_node_ids: BTreeSet<Uuid>,
    ) -> Result<Self, InvalidLocalReplicaOrigin> {
        validate_origin_voters(&voter_node_ids)?;
        Ok(Self::Replacement { id, voter_node_ids })
    }

    /// Returns voters that can route a replacement obsolescence check.
    #[must_use]
    pub const fn voter_node_ids(&self) -> Option<&BTreeSet<Uuid>> {
        match self {
            Self::Bootstrap(_) => None,
            Self::Replacement { voter_node_ids, .. } => Some(voter_node_ids),
        }
    }

    /// Returns the replacement identity when this row was created by repair.
    #[must_use]
    pub const fn replacement_id(&self) -> Option<ReplacementId> {
        match self {
            Self::Bootstrap(_) => None,
            Self::Replacement { id, .. } => Some(*id),
        }
    }
}

/// Explains why local provisioning provenance cannot support reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidLocalReplicaOrigin {
    /// Replacement needs two or three voters.
    #[error("local replica origin has an invalid voter count {actual}")]
    InvalidVoterCount {
        /// Number of unique voter UUIDs supplied.
        actual: usize,
    },

    /// Node identities must never use the nil UUID.
    #[error("local replica origin contains a zero voter UUID")]
    ZeroVoter,
}

/// Checks one bounded voter route before it becomes durable local provenance.
fn validate_origin_voters(
    voter_node_ids: &BTreeSet<Uuid>,
) -> Result<(), InvalidLocalReplicaOrigin> {
    if !(2..=3).contains(&voter_node_ids.len()) {
        return Err(InvalidLocalReplicaOrigin::InvalidVoterCount {
            actual: voter_node_ids.len(),
        });
    }
    if voter_node_ids.iter().any(Uuid::is_nil) {
        return Err(InvalidLocalReplicaOrigin::ZeroVoter);
    }
    Ok(())
}

impl ReservedSpace {
    /// Rebuilds checked space read from the local catalog.
    pub(super) const fn new(data_bytes: u64, metadata_bytes: u64) -> Self {
        Self {
            data_bytes,
            metadata_bytes,
        }
    }

    /// Returns space promised to logical data.
    #[must_use]
    pub const fn data_bytes(self) -> u64 {
        self.data_bytes
    }

    /// Returns space promised to replica metadata.
    #[must_use]
    pub const fn metadata_bytes(self) -> u64 {
        self.metadata_bytes
    }
}

/// ublk device and attachment identity saved for one local replica.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SavedUblkDevice {
    id: UblkDeviceId,
    capacity: crate::VolumeCapacity,
    fence: FenceEpoch,
    session_id: DriverSessionId,
    queue_count: u16,
    queue_depth: u16,
    max_request_bytes: u32,
}

impl SavedUblkDevice {
    /// Creates one local record from a running device and its checked settings.
    #[must_use]
    pub const fn new(
        id: UblkDeviceId,
        fence: FenceEpoch,
        session_id: DriverSessionId,
        settings: UblkSettings,
    ) -> Self {
        Self {
            id,
            capacity: crate::VolumeCapacity::from_validated(settings.capacity_bytes()),
            fence,
            session_id,
            queue_count: settings.queue_count(),
            queue_depth: settings.queue_depth(),
            max_request_bytes: settings.max_request_bytes(),
        }
    }

    /// Rebuilds one record read from Cap'n Proto before settings are checked.
    pub(super) const fn from_stored_parts(
        id: UblkDeviceId,
        capacity: crate::VolumeCapacity,
        fence: FenceEpoch,
        session_id: DriverSessionId,
        queue_count: u16,
        queue_depth: u16,
        max_request_bytes: u32,
    ) -> Self {
        Self {
            id,
            capacity,
            fence,
            session_id,
            queue_count,
            queue_depth,
            max_request_bytes,
        }
    }

    /// Returns the kernel ublk device number.
    #[must_use]
    pub const fn id(self) -> UblkDeviceId {
        self.id
    }

    /// Returns the logical capacity fixed when this ublk device started.
    #[must_use]
    pub const fn capacity(self) -> crate::VolumeCapacity {
        self.capacity
    }

    /// Returns the committed attachment number served by the device.
    #[must_use]
    pub const fn fence(self) -> FenceEpoch {
        self.fence
    }

    /// Returns the driver session served by the device.
    #[must_use]
    pub const fn session_id(self) -> DriverSessionId {
        self.session_id
    }

    /// Returns the same kernel device serving a later fence for this session.
    pub(super) const fn with_fence(mut self, fence: FenceEpoch) -> Self {
        self.fence = fence;
        self
    }

    /// Rebuilds and checks the immutable ublk settings needed for recovery.
    pub fn settings(
        self,
        descriptor: &VolumeDescriptor,
    ) -> Result<UblkSettings, InvalidUblkSettings> {
        let memory_limit_bytes = u64::from(self.queue_count)
            .checked_mul(u64::from(self.queue_depth))
            .and_then(|value| value.checked_mul(u64::from(self.max_request_bytes)))
            .ok_or(InvalidUblkSettings::QueueMemoryOverflow)?;
        let device_descriptor = descriptor
            .with_capacity(self.capacity)
            .map_err(|_| InvalidUblkSettings::InvalidCapacity)?;
        UblkSettings::new(
            &device_descriptor,
            UblkQueueSettings {
                queue_count: self.queue_count,
                queue_depth: self.queue_depth,
                max_request_bytes: self.max_request_bytes,
                memory_limit_bytes,
            },
        )
    }

    /// Returns the saved queue count for Cap'n Proto.
    pub(super) const fn queue_count(self) -> u16 {
        self.queue_count
    }

    /// Returns the saved queue depth for Cap'n Proto.
    pub(super) const fn queue_depth(self) -> u16 {
        self.queue_depth
    }

    /// Returns the saved request size for Cap'n Proto.
    pub(super) const fn max_request_bytes(self) -> u32 {
        self.max_request_bytes
    }
}

/// Last saved step for one local filesystem mount.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SavedMountState {
    /// The mount is required but may not yet exist in the kernel.
    Mounting,

    /// The mount was found and its ownership was applied.
    Mounted,

    /// The mount must be removed before its mapped device and ublk device.
    Unmounting,
}

/// One filesystem mount saved so it can be restored or removed after a restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SavedVolumeMount {
    state: SavedMountState,
    fence: FenceEpoch,
    session_id: DriverSessionId,
    path: PathBuf,
    owner_uid: u32,
    owner_gid: u32,
    mode: u32,
    filesystem_expanded_to_bytes: u64,
    filesystem: ReplicatedVolumeFilesystem,
}

impl SavedVolumeMount {
    /// Creates the first saved step before the mount system call runs.
    pub fn mounting(
        fence: FenceEpoch,
        session_id: DriverSessionId,
        path: PathBuf,
        owner_uid: u32,
        owner_gid: u32,
        mode: u32,
        filesystem: ReplicatedVolumeFilesystem,
    ) -> Result<Self, InvalidSavedVolumeMount> {
        Self::from_stored_parts(
            SavedMountState::Mounting,
            fence,
            session_id,
            path,
            owner_uid,
            owner_gid,
            mode,
            0,
            filesystem,
        )
    }

    /// Rebuilds and checks one mount read from the local catalog.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_stored_parts(
        state: SavedMountState,
        fence: FenceEpoch,
        session_id: DriverSessionId,
        path: PathBuf,
        owner_uid: u32,
        owner_gid: u32,
        mode: u32,
        filesystem_expanded_to_bytes: u64,
        filesystem: ReplicatedVolumeFilesystem,
    ) -> Result<Self, InvalidSavedVolumeMount> {
        if !path.is_absolute() {
            return Err(InvalidSavedVolumeMount::PathNotAbsolute);
        }
        if mode & !0o7777 != 0 {
            return Err(InvalidSavedVolumeMount::InvalidMode { actual: mode });
        }
        Ok(Self {
            state,
            fence,
            session_id,
            path,
            owner_uid,
            owner_gid,
            mode,
            filesystem_expanded_to_bytes,
            filesystem,
        })
    }

    /// Returns the last saved mount or unmount step.
    #[must_use]
    pub const fn state(&self) -> SavedMountState {
        self.state
    }

    /// Returns the committed attachment number that owns the mount.
    #[must_use]
    pub const fn fence(&self) -> FenceEpoch {
        self.fence
    }

    /// Returns the driver session that owns the mount.
    #[must_use]
    pub const fn session_id(&self) -> DriverSessionId {
        self.session_id
    }

    /// Returns the exact daemon-owned mount directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the user ID applied to the mounted filesystem root.
    #[must_use]
    pub const fn owner_uid(&self) -> u32 {
        self.owner_uid
    }

    /// Returns the group ID applied to the mounted filesystem root.
    #[must_use]
    pub const fn owner_gid(&self) -> u32 {
        self.owner_gid
    }

    /// Returns the Unix permission bits applied to the mounted filesystem root.
    #[must_use]
    pub const fn mode(&self) -> u32 {
        self.mode
    }

    /// Returns the largest mapped capacity successfully passed to the grow tool.
    #[must_use]
    pub const fn filesystem_expanded_to_bytes(&self) -> u64 {
        self.filesystem_expanded_to_bytes
    }

    /// Returns the exact filesystem expected at the saved mount.
    #[must_use]
    pub const fn filesystem(&self) -> ReplicatedVolumeFilesystem {
        self.filesystem
    }

    /// Returns the same mounted filesystem owned by a later session fence.
    pub(super) fn with_fence(&self, fence: FenceEpoch) -> Self {
        let mut changed = self.clone();
        changed.fence = fence;
        changed
    }

    /// Returns the same mount after one later step was completed.
    pub fn with_state(&self, state: SavedMountState) -> Result<Self, InvalidSavedVolumeMount> {
        if !matches!(
            (self.state, state),
            (
                SavedMountState::Mounting,
                SavedMountState::Mounting | SavedMountState::Mounted | SavedMountState::Unmounting
            ) | (
                SavedMountState::Mounted,
                SavedMountState::Mounted | SavedMountState::Unmounting
            ) | (SavedMountState::Unmounting, SavedMountState::Unmounting)
        ) {
            return Err(InvalidSavedVolumeMount::StateMovedBackwards);
        }
        let mut changed = self.clone();
        changed.state = state;
        Ok(changed)
    }

    /// Returns the same mount with a monotonic filesystem-expansion receipt.
    pub fn with_filesystem_expanded_to(
        &self,
        capacity_bytes: u64,
    ) -> Result<Self, InvalidSavedVolumeMount> {
        if capacity_bytes < self.filesystem_expanded_to_bytes {
            return Err(InvalidSavedVolumeMount::FilesystemCapacityMovedBackwards);
        }
        let mut changed = self.clone();
        changed.filesystem_expanded_to_bytes = capacity_bytes;
        Ok(changed)
    }

    /// Checks that a replacement changes only monotonic mount progress.
    pub(super) fn can_advance_to(&self, next: &Self) -> bool {
        self.fence == next.fence
            && self.session_id == next.session_id
            && self.path == next.path
            && self.owner_uid == next.owner_uid
            && self.owner_gid == next.owner_gid
            && self.mode == next.mode
            && self.filesystem == next.filesystem
            && self
                .with_state(next.state)
                .is_ok_and(|state| state.state == next.state)
            && next.filesystem_expanded_to_bytes >= self.filesystem_expanded_to_bytes
    }
}

/// Explains why one saved mount cannot be trusted after a restart.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidSavedVolumeMount {
    /// Mount paths must be exact absolute Linux paths.
    #[error("saved volume mount path must be absolute")]
    PathNotAbsolute,

    /// Only Unix permission bits may be stored.
    #[error("saved volume mount mode {actual:#o} contains unsupported bits")]
    InvalidMode {
        /// Invalid mode read from the catalog.
        actual: u32,
    },

    /// Completed mount steps must not move back to earlier work.
    #[error("saved volume mount state cannot move backwards")]
    StateMovedBackwards,

    /// A successful filesystem expansion receipt is monotonic.
    #[error("saved filesystem expansion capacity cannot move backwards")]
    FilesystemCapacityMovedBackwards,
}

/// One unfinished filesystem format saved across a daemon restart.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SavedFilesystemFormat {
    filesystem_id: FilesystemId,
    profile_hash: [u8; 32],
    filesystem: ReplicatedVolumeFilesystem,
}

impl SavedFilesystemFormat {
    /// Saves the filesystem, UUID, and profile before mkfs starts.
    #[must_use]
    pub const fn new(
        filesystem: ReplicatedVolumeFilesystem,
        filesystem_id: FilesystemId,
        profile_hash: [u8; 32],
    ) -> Self {
        Self {
            filesystem_id,
            profile_hash,
            filesystem,
        }
    }

    /// Returns the exact filesystem whose format is in progress.
    #[must_use]
    pub const fn filesystem(self) -> ReplicatedVolumeFilesystem {
        self.filesystem
    }

    /// Returns the UUID that mkfs must write.
    #[must_use]
    pub const fn filesystem_id(self) -> FilesystemId {
        self.filesystem_id
    }

    /// Returns the hash of the exact filesystem format profile.
    #[must_use]
    pub const fn profile_hash(self) -> [u8; 32] {
        self.profile_hash
    }
}

/// Complete durable node-local record for one replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaRecord {
    descriptor: VolumeDescriptor,
    origin: LocalReplicaOrigin,
    directory_name: String,
    state: ReplicaState,
    health: ReplicaHealth,
    reserved: ReservedSpace,
    filesystem_format: Option<SavedFilesystemFormat>,
}

impl ReplicaRecord {
    /// Creates the first local row after its pool space is held.
    pub(super) fn new(
        descriptor: VolumeDescriptor,
        origin: LocalReplicaOrigin,
        directory_name: String,
        reserved: ReservedSpace,
    ) -> Self {
        Self {
            descriptor,
            origin,
            directory_name,
            state: ReplicaState::Preparing,
            health: ReplicaHealth::Healthy,
            reserved,
            filesystem_format: None,
        }
    }

    /// Returns the local catalog key.
    #[must_use]
    pub fn key(&self) -> ReplicaKey {
        ReplicaKey::from(&self.descriptor)
    }

    /// Returns the latest Raft descriptor applied to this local copy.
    #[must_use]
    pub const fn descriptor(&self) -> &VolumeDescriptor {
        &self.descriptor
    }

    /// Returns the immutable origin that owns this reservation.
    #[must_use]
    pub const fn origin(&self) -> &LocalReplicaOrigin {
        &self.origin
    }

    /// Returns the complete path below the checked pool root.
    #[must_use]
    pub fn path(&self, pool_root: &Path) -> PathBuf {
        pool_root.join("replicas").join(&self.directory_name)
    }

    /// Returns the node-local file state.
    #[must_use]
    pub const fn state(&self) -> ReplicaState {
        self.state
    }

    /// Returns whether the checked local file may be admitted to data work.
    #[must_use]
    pub const fn health(&self) -> ReplicaHealth {
        self.health
    }

    /// Returns the data and metadata space held by this row.
    #[must_use]
    pub const fn reserved_space(&self) -> ReservedSpace {
        self.reserved
    }

    /// Returns one unfinished filesystem format saved on this node.
    #[must_use]
    pub const fn filesystem_format(&self) -> Option<SavedFilesystemFormat> {
        self.filesystem_format
    }

    /// Stores a checked file-state change.
    pub(super) fn set_state(&mut self, state: ReplicaState) {
        self.state = state;
    }

    /// Stores one fail-closed local file-health observation.
    pub(super) fn set_health(&mut self, health: ReplicaHealth) {
        self.health = health;
    }

    /// Records the replacement grant that now owns this excluded local file slot.
    pub(super) fn set_origin(&mut self, origin: LocalReplicaOrigin) {
        self.origin = origin;
    }

    /// Records a capacity already committed by Raft for this generation.
    pub(super) fn set_descriptor(&mut self, descriptor: VolumeDescriptor) {
        self.descriptor = descriptor;
    }

    /// Records the complete pool space currently held for this copy.
    pub(super) fn set_reserved_space(&mut self, reserved: ReservedSpace) {
        self.reserved = reserved;
    }

    /// Saves one unfinished filesystem format before tool writes begin.
    pub(super) fn set_filesystem_format(&mut self, format: SavedFilesystemFormat) {
        self.filesystem_format = Some(format);
    }

    /// Clears a filesystem format after the committed state says it finished.
    pub(super) fn clear_filesystem_format(&mut self) {
        self.filesystem_format = None;
    }

    /// Returns the deterministic directory name stored in this row.
    pub(super) fn directory_name(&self) -> &str {
        &self.directory_name
    }
}

/// Durable local ownership of one writer session and its kernel resources.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalAttachmentRecord {
    descriptor: VolumeDescriptor,
    session_id: DriverSessionId,
    granted_fence: Option<FenceEpoch>,
    ublk_devices: Vec<SavedUblkDevice>,
    volume_mount: Option<SavedVolumeMount>,
    detaching: bool,
}

impl LocalAttachmentRecord {
    /// Saves a session before requesting distributed writer grant.
    pub fn new(descriptor: VolumeDescriptor, session_id: DriverSessionId) -> Self {
        Self {
            descriptor,
            session_id,
            granted_fence: None,
            ublk_devices: Vec::new(),
            volume_mount: None,
            detaching: false,
        }
    }

    /// Returns the catalog key for this attachment.
    pub fn key(&self) -> ReplicaKey {
        ReplicaKey::from(&self.descriptor)
    }

    /// Returns the descriptor currently used by the mapped device.
    pub const fn descriptor(&self) -> &VolumeDescriptor {
        &self.descriptor
    }

    /// Returns the driver session stored before its writer grant was requested.
    pub const fn session_id(&self) -> DriverSessionId {
        self.session_id
    }

    /// Returns the committed fence installed for the session, when granted.
    pub const fn granted_fence(&self) -> Option<FenceEpoch> {
        self.granted_fence
    }

    /// Returns every saved ublk device still owned by this attachment.
    pub fn ublk_devices(&self) -> &[SavedUblkDevice] {
        &self.ublk_devices
    }

    /// Returns the saved mount operation and path, when present.
    pub const fn volume_mount(&self) -> Option<&SavedVolumeMount> {
        self.volume_mount.as_ref()
    }

    /// Returns whether local cleanup must finish instead of resuming attachment.
    pub const fn is_detaching(&self) -> bool {
        self.detaching
    }

    /// Saves the exact fence committed for this session.
    pub(super) fn set_granted_fence(&mut self, fence: FenceEpoch) {
        self.granted_fence = Some(fence);
    }

    /// Records the descriptor proven active in the mapped device.
    pub(super) fn set_descriptor(&mut self, descriptor: VolumeDescriptor) {
        self.descriptor = descriptor;
    }

    /// Adds one checked device while preserving deterministic device order.
    pub(super) fn add_ublk_device(&mut self, device: SavedUblkDevice) {
        if !self.ublk_devices.contains(&device) {
            self.ublk_devices.push(device);
            self.ublk_devices.sort_by_key(|saved| saved.id());
        }
    }

    /// Replaces one exact device after its old kernel identity disappeared.
    pub(super) fn replace_ublk_device(
        &mut self,
        expected: SavedUblkDevice,
        device: SavedUblkDevice,
    ) {
        if let Some(saved) = self
            .ublk_devices
            .iter_mut()
            .find(|saved| **saved == expected)
        {
            *saved = device;
            self.ublk_devices.sort_by_key(|saved| saved.id());
        }
    }

    /// Removes one exact device only after its owner reaches terminal cleanup.
    pub(super) fn remove_ublk_device(&mut self, expected: SavedUblkDevice) {
        self.ublk_devices.retain(|saved| *saved != expected);
    }

    /// Advances every owned device to the same committed writer fence.
    pub(super) fn set_ublk_device_fence(&mut self, fence: FenceEpoch) {
        for device in &mut self.ublk_devices {
            *device = device.with_fence(fence);
        }
    }

    /// Saves one restart-safe mount transition.
    pub(super) fn set_volume_mount(&mut self, volume_mount: SavedVolumeMount) {
        self.volume_mount = Some(volume_mount);
    }

    /// Clears the mount only after kernel and directory cleanup finish.
    pub(super) fn clear_volume_mount(&mut self) {
        self.volume_mount = None;
    }

    /// Makes cleanup intent durable before any cancellable local or Raft work.
    pub(super) fn begin_detach(&mut self) {
        self.detaching = true;
    }
}

/// Compact local proof that bootstrap no longer owns a live generation copy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalReplicaRetirement {
    key: ReplicaKey,
}

impl LocalReplicaRetirement {
    /// Creates proof that current control state removed this node's old copy.
    pub const fn new(key: ReplicaKey) -> Self {
        Self { key }
    }

    /// Returns the retired storage generation.
    pub const fn key(self) -> ReplicaKey {
        self.key
    }
}

/// What the pool can safely start at its current free-space level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolSpaceState {
    /// New replicas and recovery work may start.
    Ready,

    /// New replicas stop; only work that fits the remaining space may start.
    NoNewReplicas,

    /// No allocation may start; deletion remains allowed.
    DeleteOnly,
}

/// Current durable reservations and actual filesystem free space.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolStatus {
    state: PoolSpaceState,
    available_bytes: u64,
    data_bytes: u64,
    metadata_bytes: u64,
}

impl PoolStatus {
    /// Builds one current view from durable totals and filesystem free space.
    pub(super) const fn new(
        state: PoolSpaceState,
        available_bytes: u64,
        data_bytes: u64,
        metadata_bytes: u64,
    ) -> Self {
        Self {
            state,
            available_bytes,
            data_bytes,
            metadata_bytes,
        }
    }

    /// Returns what work may start now.
    #[must_use]
    pub const fn state(self) -> PoolSpaceState {
        self.state
    }

    /// Returns space currently available from the filesystem.
    #[must_use]
    pub const fn available_bytes(self) -> u64 {
        self.available_bytes
    }

    /// Returns space promised to replica data.
    #[must_use]
    pub const fn data_bytes(self) -> u64 {
        self.data_bytes
    }

    /// Returns space promised to replica metadata.
    #[must_use]
    pub const fn metadata_bytes(self) -> u64 {
        self.metadata_bytes
    }
}
