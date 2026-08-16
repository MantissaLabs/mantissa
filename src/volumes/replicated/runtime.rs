//! Shared ownership, types, and invariants for the node-local replicated-volume runtime.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use mantissa_raft::catalog::{GroupActivation, GroupCatalog};
use mantissa_raft::runtime::{GroupRuntime, RuntimeError};
use mantissa_raft::transport::{
    AuthenticatedApplication, AuthenticatedStreamApplication, IncomingGroupStarter, TcpTransport,
    TcpTransportSettings, TransportError,
};
use mantissa_volume::catalog::{
    LocalAttachmentRecord, LocalReplicaOrigin, PoolSpaceState, ReplicaCatalog, ReplicaHealth,
    ReplicaKey, ReplicaPool, ReplicaRecord, ReplicaState, SavedFilesystemFormat, SavedMountState,
    SavedUblkDevice, SavedVolumeMount,
};
use mantissa_volume::control_state::{
    ExpectedVolumeRevision, FenceVolumeWriter, GrantVolumeWriter, VolumeCommand,
    VolumeCommandResponse, VolumeControlState, VolumeDisposition, WriterGrant,
};
use mantissa_volume::driver::{
    DriverLimits, MappedVolumeLayout, MappedVolumeSystem, UblkDeviceId, UblkDeviceInfo,
    UblkDeviceState, UblkOwnerId, UblkSystem,
};
use mantissa_volume::fs::{
    space::{self, FilesystemSpace},
    volume::{self, ReplicatedVolumeFilesystem as VolumeFilesystem},
};
use mantissa_volume::lifecycle_calls;
use mantissa_volume::protocol::{ReplicaKeyAdapter, UuidNodeIdAdapter, VolumeCommandAdapter};
use mantissa_volume::storage::replica_file::connection::{
    ReplicaDataConnection, ReplicaDataServer, read_connection_open,
};
use mantissa_volume::storage::replica_file::data_path::{
    FixedReplicaCopy, FixedReplicaPath, FixedReplicaPathSettings,
};
use mantissa_volume::storage::replica_file::io_admission::{
    AppliedVolumeStateRegistry, AppliedVolumeStateRemovalError, FenceAdmission,
};
use mantissa_volume::storage::replica_file::wire::{
    ReplicaDataConnectionOpen, ReplicaDataConnectionPurpose, ReplicaDataProgress,
};
use mantissa_volume::storage::replica_file::{
    ReplicaFile, ReplicaFileError, ReplicaFileSettings, ReplicaFileWorkerPool,
};
use mantissa_volume::{FenceEpoch, ReplacementId, VolumeCapacity, VolumeDescriptor, VolumeNodeId};
use openraft::{EmptyNode, Membership, StoredMembership};
use parking_lot::{Mutex, RwLock};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::rpc::StorageServiceFactory;
use super::split_validation::VolumeMembershipChangeBlocker;
use super::{REPLICATED_VOLUME_FORMAT_VERSION, ReplicatedVolumeSupport};
use crate::cluster::ClusterViewState;
use crate::config::{CheckedReplicatedVolumeConfig, ReplicatedVolumeConfig};
use crate::store::path::open_state_database;
use crate::store::replicated::peers::PeersStore;

#[path = "runtime/attachment_recovery.rs"]
mod attachment_recovery;
#[path = "runtime/capacity.rs"]
mod capacity;
#[path = "runtime/connections.rs"]
mod connections;
#[path = "runtime/data.rs"]
mod data;
#[path = "runtime/driver.rs"]
mod driver;
#[path = "runtime/group.rs"]
mod group;
#[path = "runtime/local_singleflight.rs"]
mod local_singleflight;
#[path = "runtime/maintenance.rs"]
mod maintenance;
#[path = "runtime/membership_lock.rs"]
mod membership_lock;
#[path = "runtime/mounted_volume.rs"]
mod mounted_volume;
#[path = "runtime/peers.rs"]
mod peers;
#[path = "runtime/raft_control.rs"]
mod raft_control;
#[path = "runtime/replica_storage.rs"]
mod replica_storage;
#[path = "runtime/shutdown.rs"]
mod shutdown;
#[path = "runtime/startup.rs"]
mod startup;
#[path = "runtime/writer_attachment.rs"]
mod writer_attachment;

use connections::DataConnections;
use driver::{DriverAttachment, DriverQuarantine, ReplicatedDriver, ReplicatedDriverSettings};
use group::VolumeGroupStarter;
use local_singleflight::VolumeSingleflightMap;
use maintenance::MaintenanceManager;
use membership_lock::VolumeMembershipLocks;
use peers::StoragePeerDirectory;

type VolumeApplication = mantissa_volume::state_machine::VolumeControlApplication<
    mantissa_volume::storage::VolumeControlStateStore,
>;
type VolumeTransport = TcpTransport<
    VolumeApplication,
    ReplicaKey,
    ReplicaKeyAdapter,
    UuidNodeIdAdapter,
    VolumeCommandAdapter,
    StoragePeerDirectory,
>;
type VolumeCatalog = GroupCatalog<ReplicaKey, Uuid, ReplicaKeyAdapter, UuidNodeIdAdapter>;
type VolumeGroups =
    GroupRuntime<ReplicaKey, Uuid, ReplicaKeyAdapter, UuidNodeIdAdapter, VolumeGroupStarter>;

const RECONCILE_RAFT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_WAKE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);

/// Keeps an explicitly woken voter active through two election windows and its wake attempt.
fn raft_group_idle_timeout(config: &openraft::Config) -> Duration {
    Duration::from_millis(config.election_timeout_max)
        .saturating_mul(2)
        .saturating_add(PEER_WAKE_ATTEMPT_TIMEOUT)
}

/// One opened copy waiting for a common data fence to be installed.
enum PreparedDriverCopy {
    Local(Arc<ReplicaFile>),
    Remote(Arc<ReplicaDataConnection>),
}

/// Marks replica progress that proves the current writer path requires recovery.
#[derive(Debug, thiserror::Error)]
#[error("writer replica progress requires recovery")]
struct WriterReplicaRecoveryRequired;

/// Selects the next local action for a saved attachment seen by a mount retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SavedAttachmentMountAction {
    Reuse,
    Clean,
    WaitForFenceHandoff,
    RestartConsumer,
}

/// Reports whether local detach finished or only its independent Raft fence remains pending.
enum AttachmentDetachProgress {
    Complete,
    FencePending(anyhow::Error),
}

/// Connects lazy authenticated group traffic to the recovered group runtime.
struct IncomingVolumeGroupStarter {
    groups: OnceLock<Weak<VolumeGroups>>,
    desired_generations: Arc<RwLock<HashSet<ReplicaKey>>>,
}

impl IncomingVolumeGroupStarter {
    /// Creates a lazy starter whose admission follows the current desired snapshot.
    fn new(desired_generations: Arc<RwLock<HashSet<ReplicaKey>>>) -> Self {
        Self {
            groups: OnceLock::new(),
            desired_generations,
        }
    }

    /// Installs the completed runtime exactly once before listening starts.
    fn set(&self, groups: &Arc<VolumeGroups>) -> Result<()> {
        self.groups
            .set(Arc::downgrade(groups))
            .map_err(|_| anyhow::anyhow!("incoming volume group starter was already configured"))
    }
}

impl IncomingGroupStarter<Uuid, ReplicaKey> for IncomingVolumeGroupStarter {
    /// Starts only a cataloged group whose saved voters include the peer.
    fn start(
        &self,
        peer: Uuid,
        group_id: ReplicaKey,
    ) -> futures::future::BoxFuture<'static, Result<(), TransportError>> {
        let groups = self.groups.get().cloned();
        let desired_generations = Arc::clone(&self.desired_generations);
        Box::pin(async move {
            if !desired_generations.read().contains(&group_id) {
                return Err(TransportError::UnknownGroup);
            }
            let groups = groups
                .and_then(|groups| groups.upgrade())
                .ok_or(TransportError::Stopped)?;
            let saved = groups
                .catalog()
                .group(&group_id)
                .map_err(|error| TransportError::StartGroup {
                    message: error.to_string(),
                })?
                .ok_or(TransportError::UnknownGroup)?;
            if !saved
                .membership()
                .is_some_and(|membership| membership.voter_ids().any(|node| node == peer))
            {
                return Err(TransportError::WrongPeer);
            }
            groups
                .activate(&group_id)
                .await
                .map_err(|error| TransportError::StartGroup {
                    message: error.to_string(),
                })?;
            Ok(())
        })
    }
}

/// Runtime ownership plus an immediately callable revocation control.
#[derive(Clone)]
struct TrackedDriver {
    owner: Arc<tokio::sync::Mutex<ReplicatedDriver>>,
    quarantine: DriverQuarantine,
}

/// Host checks completed before the local state database is opened.
pub(crate) struct PreparedReplicatedVolumeHost {
    checked: CheckedReplicatedVolumeConfig,
    pool: ReplicaPool,
    mapped_volumes: MappedVolumeSystem,
}

/// Durable local facts returned by idempotent preparation and inspection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalReplicaStatus {
    pub(crate) exists: bool,
    pub(crate) state: ReplicaState,
    pub(crate) health: ReplicaHealth,
    pub(crate) group_saved: bool,
    pub(crate) control_state_initialized: bool,
    pub(crate) applied_log_index: Option<u64>,
    pub(crate) leader_node_id: Option<Uuid>,
    pub(crate) voter_node_ids: BTreeSet<Uuid>,
    pub(crate) reserved_capacity_bytes: u64,
    pub(crate) prepared_capacity_bytes: u64,
    pub(crate) served_capacity_bytes: u64,
    pub(crate) device_capacity_bytes: Option<u64>,
    pub(crate) filesystem_expansion_pending: bool,
}

/// Local capacity facts read without starting or advancing expansion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplicaCapacityStatus {
    pub(crate) reserved_capacity_bytes: u64,
    pub(crate) prepared_capacity_bytes: u64,
    pub(crate) served_capacity_bytes: u64,
    pub(crate) healthy: bool,
    pub(crate) reason: String,
}

/// Membership predicate requested for one exact replacement authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReplacementMembershipGoal {
    Learner,
    FinalVoters,
    Absent { rollback_voters: BTreeSet<Uuid> },
}

/// Volume control state and complete membership returned by a running leader.
#[derive(Clone, Debug)]
pub(crate) struct LeaderVolumeGroupState {
    pub(crate) control_state: VolumeControlState,
    pub(crate) membership: RaftMembershipSnapshot,
}

/// Voters, learners, and joint-consensus state read from one Raft group.
#[derive(Clone, Debug)]
pub(crate) struct RaftMembershipSnapshot {
    pub(crate) voters: BTreeSet<Uuid>,
    pub(crate) members: BTreeSet<Uuid>,
    pub(crate) is_joint: bool,
}

/// Live filesystem space measured on the node serving the mounted writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WriterFilesystemSpace {
    pub(crate) writer_node_id: Uuid,
    pub(crate) space: FilesystemSpace,
}

/// Owns local replicas, volume Raft groups, data gates, and node-wide transport.
#[must_use = "the replicated-volume runtime must be shut down before it is dropped"]
pub struct ReplicatedVolumeRuntime {
    owner: OnceLock<Weak<ReplicatedVolumeRuntime>>,
    replicas: ReplicaCatalog,
    groups: Arc<VolumeGroups>,
    transport: Arc<VolumeTransport>,
    membership_change_blocker: VolumeMembershipChangeBlocker,
    volume_membership_locks: VolumeMembershipLocks,
    cluster_view: ClusterViewState,
    applied_volume_states: Arc<AppliedVolumeStateRegistry>,
    desired_generations: Arc<RwLock<HashSet<ReplicaKey>>>,
    gates: RwLock<BTreeMap<ReplicaKey, Arc<FenceAdmission>>>,
    replica_files: Mutex<BTreeMap<ReplicaKey, Arc<ReplicaFile>>>,
    replica_file_workers: ReplicaFileWorkerPool,
    replica_data_server: ReplicaDataServer,
    data_connections: DataConnections,
    maintenance: MaintenanceManager,
    fs: volume::Manager,
    mapped_volumes: MappedVolumeSystem,
    lifecycle_calls: lifecycle_calls::Tracker,
    node_id: Uuid,
    volume_node_id: mantissa_volume::VolumeNodeId,
    ublk_owner_id: UblkOwnerId,
    driver_limits: DriverLimits,
    fixed_path_settings: FixedReplicaPathSettings,
    drivers: Mutex<BTreeMap<ReplicaKey, TrackedDriver>>,
    volume_singleflight: VolumeSingleflightMap,
    driver_lifecycle: tokio::sync::RwLock<()>,
    protocol_limits: mantissa_raft::protocol::ProtocolLimits,
    replica_file_settings: ReplicaFileSettings,
    replica_data_limits: mantissa_volume::storage::replica_file::wire::ReplicaDataLimits,
    handshake_timeout: Duration,
    operation_timeout: Duration,
    stop_io_timeout: Duration,
    shutdown_timeout: Duration,
    raft_group_idle_timeout: Duration,
    repair_failure_grace: Duration,
    advertise_address: SocketAddr,
    max_saved_replicas: usize,
    max_saved_groups: usize,
    stopping: AtomicBool,
}

impl ReplicatedVolumeRuntime {
    /// Returns the local node identity used by control state and observations.
    #[must_use]
    pub(crate) const fn node_id(&self) -> Uuid {
        self.node_id
    }

    /// Returns the private address advertised for this node's storage listener.
    #[must_use]
    pub(crate) const fn advertise_address(&self) -> SocketAddr {
        self.advertise_address
    }

    /// Returns the delay before an unavailable copy may change data control state.
    #[must_use]
    pub(crate) const fn repair_failure_grace(&self) -> Duration {
        self.repair_failure_grace
    }

    /// Returns the configured bound for one replicated-storage operation.
    #[must_use]
    pub(crate) const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    /// Returns the transport protocol limits used by narrow storage RPCs.
    #[must_use]
    pub(crate) const fn protocol_limits(&self) -> mantissa_raft::protocol::ProtocolLimits {
        self.protocol_limits
    }

    /// Prevents reconcilers and attach requests from starting new local work.
    pub(crate) fn begin_shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
        self.maintenance.begin_shutdown();
        self.data_connections.begin_shutdown();
        for gate in self.gates.read().values() {
            gate.disable();
        }
        for driver in self.drivers.lock().values() {
            driver.quarantine.quarantine();
        }
    }

    /// Returns whether local shutdown has closed new lifecycle work.
    #[must_use]
    pub(crate) fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }

    /// Returns the wall-clock budget for one complete local shutdown attempt.
    #[must_use]
    pub(crate) const fn shutdown_attempt_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Replaces the CRDT-derived generation allow-list and revokes removed entries atomically.
    pub(crate) fn replace_desired_generations(&self, next: HashSet<ReplicaKey>) {
        let mut desired = self.desired_generations.write();
        let removed = desired.difference(&next).copied().collect::<Vec<_>>();
        *desired = next;
        self.maintenance.retain_desired(&desired);

        for key in removed {
            if let Some(gate) = self.gates.read().get(&key) {
                gate.disable();
            }
            self.data_connections.close(key);
            if let Some(driver) = self.drivers.lock().get(&key) {
                driver.quarantine.quarantine();
            }
        }
    }

    /// Returns whether current desired state still names this exact storage generation.
    pub(super) fn generation_is_desired(&self, key: ReplicaKey) -> bool {
        self.desired_generations.read().contains(&key)
    }

    /// Holds desired admission stable while a local resource crosses its start boundary.
    fn current_generation_guard(
        &self,
        key: ReplicaKey,
    ) -> Result<parking_lot::RwLockReadGuard<'_, HashSet<ReplicaKey>>> {
        let desired = self.desired_generations.read();
        if !desired.contains(&key) {
            anyhow::bail!("replicated-volume generation is not current desired state");
        }
        Ok(desired)
    }

    /// Rejects resource creation and control-state changes for stale generations.
    fn require_current_generation(&self, key: ReplicaKey) -> Result<()> {
        drop(self.current_generation_guard(key)?);
        Ok(())
    }

    /// Clones one runtime-owned driver without holding the registry across async work.
    fn tracked_driver(&self, key: ReplicaKey) -> Option<Arc<tokio::sync::Mutex<ReplicatedDriver>>> {
        self.drivers
            .lock()
            .get(&key)
            .map(|driver| Arc::clone(&driver.owner))
    }
}

/// Returns whether one command initializes, changes, or interrupts copy or Raft membership.
fn split_blocks_command(command: &VolumeCommand) -> bool {
    matches!(
        command,
        VolumeCommand::Initialize(_)
            | VolumeCommand::SetDisposition(_)
            | VolumeCommand::BeginRecovery(_)
            | VolumeCommand::BeginReplacement(_)
            | VolumeCommand::CancelReplacement(_)
            | VolumeCommand::AdoptReplacement(_)
    )
}

/// Creates or verifies the fixed file and makes local readiness durable.
fn prepare_empty_replica_file(
    replicas: &ReplicaCatalog,
    record: &ReplicaRecord,
    settings: ReplicaFileSettings,
    ready_file_is_open: bool,
) -> Result<()> {
    let directory = record.path(replicas.pool_root()).join("blocks");
    match record.state() {
        ReplicaState::Ready if !ready_file_is_open => {
            if record.health() == ReplicaHealth::NeedsRecovery {
                anyhow::bail!("ready local replica remains fenced for recovery");
            }
            if let Err(error) = ReplicaFile::open(directory, record.descriptor().clone(), settings)
            {
                if error.open_failure_requires_recovery() {
                    replicas.set_replica_health(record.key(), ReplicaHealth::NeedsRecovery)?;
                }
                return Err(error.into());
            }
        }
        ReplicaState::Ready if record.health() == ReplicaHealth::NeedsRecovery => {
            anyhow::bail!("open local replica remains fenced for recovery");
        }
        ReplicaState::Ready => {}
        ReplicaState::Preparing => {
            if let Err(error) = std::fs::remove_dir_all(&directory)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                return Err(error.into());
            }
            if let Err(error) = ReplicaFile::create(
                directory,
                record.descriptor().clone(),
                mantissa_volume::FenceEpoch::new(1)?,
                settings,
            ) {
                replicas.set_replica_health(record.key(), ReplicaHealth::NeedsRecovery)?;
                return Err(error.into());
            }
            replicas.set_replica_state(record.key(), ReplicaState::Ready)?;
            replicas.set_replica_health(record.key(), ReplicaHealth::Healthy)?;
        }
        state => anyhow::bail!("cannot prepare a replica while it is {state}"),
    }
    Ok(())
}

/// Removes one exact replica directory and durably records its parent change.
fn remove_replica_directory(path: &std::path::Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {
            let parent = path.parent().context("replica directory has no parent")?;
            std::fs::File::open(parent)?.sync_all()?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

/// Requires an idempotent volume command to reach its stated postcondition.
fn require_command_postcondition(response: VolumeCommandResponse) -> Result<()> {
    match response {
        VolumeCommandResponse::Applied { .. } | VolumeCommandResponse::Current { .. } => Ok(()),
        VolumeCommandResponse::Conflict { .. } => {
            anyhow::bail!("control-state changed while applying the local lifecycle level")
        }
        VolumeCommandResponse::Rejected(reason) => {
            anyhow::bail!("control state rejected the local lifecycle level: {reason:?}")
        }
    }
}

/// Validates one opened copy and folds its state into writer-path preparation.
fn record_driver_copy_progress(
    copy: mantissa_volume::VolumeNodeId,
    progress: ReplicaDataProgress,
    requested_epoch: mantissa_volume::FenceEpoch,
    common_progress: &mut Option<ReplicaDataProgress>,
    progress_matches: &mut bool,
    greatest_changed_generation: &mut u64,
) -> Result<()> {
    if progress.data_fence() > requested_epoch {
        anyhow::bail!(
            "writer replica {copy} is ahead of requested epoch {} at {}",
            requested_epoch.get(),
            progress.data_fence().get()
        );
    }
    if progress.stored_write_number() != progress.durable_write_number() {
        return Err(anyhow::Error::new(WriterReplicaRecoveryRequired)).with_context(|| {
            format!("writer replica {copy} contains writes that are not durable")
        });
    }
    if !progress.changed_regions_complete() {
        return Err(anyhow::Error::new(WriterReplicaRecoveryRequired)).with_context(|| {
            format!("writer replica {copy} has incomplete changed-region metadata")
        });
    }
    *greatest_changed_generation =
        (*greatest_changed_generation).max(progress.changed_region_generation());
    match common_progress {
        Some(expected) if !same_writer_durable_point(*expected, progress) => {
            *progress_matches = false;
        }
        Some(_) => {}
        None => *common_progress = Some(progress),
    }
    Ok(())
}

/// Compares data ordering while allowing complete changed-region generations to differ.
fn same_writer_durable_point(left: ReplicaDataProgress, right: ReplicaDataProgress) -> bool {
    left.data_fence() == right.data_fence()
        && left.flush_number() == right.flush_number()
        && left.durable_write_number() == right.durable_write_number()
        && left.stored_write_number() == right.stored_write_number()
}

/// Decides whether a mount retry may reuse, must clean, or must preserve one saved session.
fn saved_attachment_mount_action(
    local_node: VolumeNodeId,
    saved_session: mantissa_volume::DriverSessionId,
    saved_fence: Option<FenceEpoch>,
    saved_mount: Option<SavedMountState>,
    current_writer: Option<WriterGrant>,
    current_fence: FenceEpoch,
) -> SavedAttachmentMountAction {
    let exact_writer = current_writer
        .is_some_and(|writer| writer.node_id == local_node && writer.session_id == saved_session);
    match saved_fence {
        None if current_writer.is_none() || exact_writer => SavedAttachmentMountAction::Reuse,
        None => SavedAttachmentMountAction::Clean,
        Some(saved_fence) if exact_writer && saved_fence == current_fence => {
            SavedAttachmentMountAction::Reuse
        }
        Some(_) if exact_writer => SavedAttachmentMountAction::WaitForFenceHandoff,
        Some(_) if saved_mount == Some(SavedMountState::Mounted) => {
            SavedAttachmentMountAction::RestartConsumer
        }
        Some(_) => SavedAttachmentMountAction::Clean,
    }
}

/// Returns true only when copy progress proves that the granted writer needs recovery.
fn writer_path_failure_requires_recovery(error: &anyhow::Error) -> bool {
    error.is::<WriterReplicaRecoveryRequired>()
}

/// Allows a fence-only handoff only when every selected copy already agrees.
fn writer_fence_install_generation(
    progress_matches: bool,
    current_fence: FenceEpoch,
    requested_fence: FenceEpoch,
    greatest_changed_generation: u64,
) -> Result<Option<u64>> {
    if !progress_matches {
        return Err(WriterReplicaRecoveryRequired.into());
    }
    if current_fence == requested_fence {
        return Ok(None);
    }
    Ok(Some(
        greatest_changed_generation
            .checked_add(1)
            .context("changed-region generation is exhausted")?,
    ))
}

/// Checks immutable identity and that the local node may receive a writer.
fn validate_mount_eligibility(
    record: &ReplicaRecord,
    state: &VolumeControlState,
    local_node: mantissa_volume::VolumeNodeId,
) -> Result<()> {
    if state.descriptor() != Some(record.descriptor()) {
        anyhow::bail!("local replica descriptor differs from current control state");
    }
    if state.disposition() != VolumeDisposition::Live {
        anyhow::bail!("volume is not live and cannot be mounted");
    }
    if !state
        .data()
        .is_some_and(|data| data.copies.contains(&local_node))
    {
        anyhow::bail!("local replica is not an active writable copy");
    }
    Ok(())
}

/// Checks that a saved local session still owns the exact current fence.
fn validate_saved_writer(
    state: &VolumeControlState,
    local_node: mantissa_volume::VolumeNodeId,
    session_id: mantissa_volume::DriverSessionId,
    fence: mantissa_volume::FenceEpoch,
) -> Result<()> {
    if saved_writer_fence(state, local_node, session_id)? != fence {
        anyhow::bail!("saved attachment no longer owns current writer grant");
    }
    Ok(())
}

/// Returns the current fence owned by one exact saved local writer session.
fn saved_writer_fence(
    state: &VolumeControlState,
    local_node: mantissa_volume::VolumeNodeId,
    session_id: mantissa_volume::DriverSessionId,
) -> Result<mantissa_volume::FenceEpoch> {
    let data = state
        .data()
        .context("initialized volume has no data control state")?;
    if state.disposition() != VolumeDisposition::Live
        || data.writer
            != Some(WriterGrant {
                node_id: local_node,
                session_id,
            })
    {
        anyhow::bail!("saved attachment no longer owns current writer grant");
    }
    Ok(data.fence)
}

/// Derives the filesystem UUID solely from the immutable volume generation.
fn deterministic_filesystem_id(key: ReplicaKey) -> Result<mantissa_volume::FilesystemId> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mantissa replicated volume filesystem v1");
    hasher.update(key.volume_id().as_bytes());
    hasher.update(&key.generation().get().to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(mantissa_volume::FilesystemId::new(Uuid::from_bytes(bytes))?)
}

/// Compares one mount's immutable request while ignoring its progress state.
fn same_mount(left: &SavedVolumeMount, right: &SavedVolumeMount) -> bool {
    left.fence() == right.fence()
        && left.session_id() == right.session_id()
        && left.path() == right.path()
        && left.owner_uid() == right.owner_uid()
        && left.owner_gid() == right.owner_gid()
        && left.mode() == right.mode()
        && left.filesystem() == right.filesystem()
}

/// Requires every current copy to have published the committed served bound.
fn all_copies_serve_capacity(
    active_copy_count: usize,
    statuses: &[ReplicaCapacityStatus],
    target: VolumeCapacity,
) -> bool {
    active_copy_count != 0
        && statuses.len() == active_copy_count
        && statuses.iter().all(|status| {
            status.healthy
                && status.reserved_capacity_bytes >= target.bytes()
                && status.prepared_capacity_bytes >= target.bytes()
                && status.served_capacity_bytes >= target.bytes()
        })
}

/// Accepts only an older or equal capacity for the same replacement storage identity.
fn replacement_capacity_can_converge(
    saved: &VolumeDescriptor,
    committed: &VolumeDescriptor,
) -> bool {
    saved.has_same_storage_identity(committed) && saved.capacity() <= committed.capacity()
}

/// Returns true only for the selected deterministic filesystem signature.
fn is_exact_filesystem(
    signatures: &[volume::Signature],
    filesystem: VolumeFilesystem,
    filesystem_id: mantissa_volume::FilesystemId,
) -> bool {
    signatures.len() == 1 && signatures[0].matches(filesystem, filesystem_id)
}

/// Converts the desired-volume value into the filesystem manager value.
const fn volume_filesystem(
    filesystem: crate::volumes::types::ReplicatedVolumeFilesystem,
) -> VolumeFilesystem {
    match filesystem {
        crate::volumes::types::ReplicatedVolumeFilesystem::Ext4 => VolumeFilesystem::Ext4,
        crate::volumes::types::ReplicatedVolumeFilesystem::Xfs => VolumeFilesystem::Xfs,
    }
}

/// Accepts safe degraded, stable, or joint membership seen during replacement.
fn replacement_voter_hint_is_valid(voters: &BTreeSet<Uuid>, local_node_id: Uuid) -> bool {
    match voters.len() {
        2 => !voters.contains(&local_node_id),
        3 => true,
        4 => voters.contains(&local_node_id),
        _ => false,
    }
}

/// Selects obsolete learners that are neither data copies nor the current replacement target.
fn stale_replacement_learners(
    members: &BTreeSet<Uuid>,
    voters: &BTreeSet<Uuid>,
    copies: &BTreeSet<VolumeNodeId>,
    replacement_target: Uuid,
) -> Vec<Uuid> {
    members
        .difference(voters)
        .copied()
        .filter(|node_id| {
            *node_id != replacement_target && !copies.iter().any(|copy| copy.as_uuid() == node_id)
        })
        .collect()
}

/// Requires rollback membership to contain two or three exact active data copies.
fn validate_replacement_rollback_voters(
    copies: &BTreeSet<VolumeNodeId>,
    rollback_voters: &BTreeSet<Uuid>,
) -> Result<()> {
    if !(2..=3).contains(&rollback_voters.len())
        || !rollback_voters
            .iter()
            .all(|node_id| copies.iter().any(|copy| copy.as_uuid() == node_id))
    {
        anyhow::bail!("replacement rollback requires two or three active data copies");
    }
    Ok(())
}

/// Requires the exact applied grant and data exclusion before destructive file reset.
fn validate_replacement_target_reset(
    state: &VolumeControlState,
    replacement_id: ReplacementId,
    local_node_id: VolumeNodeId,
) -> Result<()> {
    if state.disposition() != VolumeDisposition::Live {
        anyhow::bail!("replacement target reset requires live control state");
    }
    let replacement = state
        .replacement()
        .filter(|replacement| {
            replacement.id == replacement_id && replacement.new_node_id == local_node_id
        })
        .context("local control state does not name this replacement target")?;
    let data = state
        .data()
        .context("replacement target reset has no initialized data state")?;
    if data.copies.contains(&replacement.new_node_id) {
        anyhow::bail!("refusing to reset a replica that remains in data control state");
    }
    Ok(())
}

/// Returns wall-clock milliseconds used only in peer diagnostics.
fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "runtime/tests.rs"]
mod tests;
