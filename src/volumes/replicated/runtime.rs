//! Node-local ownership and narrow Raft control state access for replicated volumes.

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
#[path = "runtime/peers.rs"]
mod peers;

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
pub(crate) struct PreparedStorage {
    checked: CheckedReplicatedVolumeConfig,
    pool: ReplicaPool,
    mapped_volumes: MappedVolumeSystem,
}

/// Durable local facts returned by idempotent ensure and inspect calls.
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
    /// Checks root access, local tools, block devices, and the replica pool.
    pub(crate) async fn prepare_host(
        config: &ReplicatedVolumeConfig,
        node_id: Uuid,
    ) -> Result<PreparedStorage> {
        let checked = config.checked()?;
        if !mantissa_net::paths::running_as_root() {
            anyhow::bail!(
                "replicated volumes need root access for ublk, device-mapper, and filesystem mounts"
            );
        }
        let fs_settings = checked.fs.clone();
        let pool_path = checked.pool_path.clone();
        let (pool, mapped_volumes) = tokio::task::spawn_blocking(move || -> Result<_> {
            volume::Manager::check_tools(&fs_settings)?;
            UblkSystem::system(UblkOwnerId::for_node(node_id.as_bytes()))
                .require_features()
                .context("required ublk kernel features are unavailable")?;
            let mapped_volumes =
                MappedVolumeSystem::new().context("open the device-mapper control device")?;
            mapped_volumes
                .require_features()
                .context("required device-mapper features are unavailable")?;
            let pool =
                ReplicaPool::check(&pool_path).context("replicated-volume pool check failed")?;
            Ok((pool, mapped_volumes))
        })
        .await
        .context("join replicated-volume host checks")??;
        Ok(PreparedStorage {
            checked,
            pool,
            mapped_volumes,
        })
    }

    /// Checks the host and opens durable local storage without listening.
    pub(crate) async fn open(
        config: ReplicatedVolumeConfig,
        node_id: Uuid,
        noise_keys: Arc<mantissa_net::noise::NoiseKeys>,
        peers: PeersStore,
        cluster_view: ClusterViewState,
        membership_change_blocker: VolumeMembershipChangeBlocker,
    ) -> Result<Arc<Self>> {
        let prepared = Self::prepare_host(&config, node_id).await?;
        Self::open_prepared(
            prepared,
            node_id,
            noise_keys,
            peers,
            cluster_view,
            membership_change_blocker,
        )
        .await
    }

    /// Opens catalogs, volume Raft groups, and bounded local workers.
    pub(crate) async fn open_prepared(
        prepared: PreparedStorage,
        node_id: Uuid,
        noise_keys: Arc<mantissa_net::noise::NoiseKeys>,
        peers: PeersStore,
        cluster_view: ClusterViewState,
        membership_change_blocker: VolumeMembershipChangeBlocker,
    ) -> Result<Arc<Self>> {
        let PreparedStorage {
            checked,
            pool,
            mapped_volumes,
        } = prepared;
        let fs = tokio::task::spawn_blocking({
            let settings = checked.fs.clone();
            let timeout = checked.operation_timeout;
            move || volume::Manager::prepare(&settings, timeout)
        })
        .await
        .context("join replicated-volume filesystem setup")??;
        let catalog_path = checked.catalog_path.clone().unwrap_or(
            crate::store::path::default_db_path()?.with_file_name("replicated-volumes.redb"),
        );
        let (database, replicas, group_catalog) = tokio::task::spawn_blocking({
            let protocol_limits = checked.protocol_limits;
            let max_saved_replicas = checked.max_saved_replicas;
            let max_saved_groups = checked.runtime_limits.max_saved_groups();
            move || -> Result<_> {
                let database = Arc::new(
                    open_state_database(&catalog_path)
                        .context("open the replicated-volume local database")?,
                );
                let replicas = ReplicaCatalog::open(Arc::clone(&database), pool)?;
                replicas.check_replica_slot_limit(max_saved_replicas)?;
                replicas.discover_replicas(max_saved_replicas)?;
                let groups = GroupCatalog::open(
                    Arc::clone(&database),
                    ReplicaKeyAdapter,
                    UuidNodeIdAdapter,
                    protocol_limits,
                )?;
                groups.discover_groups(max_saved_groups)?;
                Ok((database, replicas, groups))
            }
        })
        .await
        .context("join replicated-volume storage checks")??;

        let commands = Arc::new(VolumeCommandAdapter);
        let service_factory = Arc::new(StorageServiceFactory::new());
        let desired_generations = Arc::new(RwLock::new(HashSet::new()));
        let incoming = Arc::new(IncomingVolumeGroupStarter::new(Arc::clone(
            &desired_generations,
        )));
        let peer_directory = Arc::new(StoragePeerDirectory::new(peers, cluster_view.clone()));
        let transport_settings = TcpTransportSettings::new(
            node_id,
            checked.listen_address,
            Arc::clone(&noise_keys),
            peer_directory,
            Arc::new(ReplicaKeyAdapter),
            Arc::new(UuidNodeIdAdapter),
            Arc::clone(&commands),
            checked.protocol_limits,
            checked.transport_limits,
        )
        .with_application(service_factory.clone() as Arc<dyn AuthenticatedApplication<Uuid>>)
        .with_stream_application(
            service_factory.clone() as Arc<dyn AuthenticatedStreamApplication<Uuid>>
        )
        .with_incoming_group_starter(
            incoming.clone() as Arc<dyn IncomingGroupStarter<Uuid, ReplicaKey>>
        );
        let transport = Arc::new(VolumeTransport::prepare(transport_settings));
        let applied_volume_states = Arc::new(AppliedVolumeStateRegistry::new());
        let raft_group_idle_timeout = raft_group_idle_timeout(&checked.raft_config);
        let starter = VolumeGroupStarter {
            node_id,
            replicas: replicas.clone(),
            groups: group_catalog.clone(),
            database: Arc::clone(&database),
            transport: Arc::clone(&transport),
            commands,
            log_key_seed: noise_keys.to_private_bytes(),
            raft_config: checked.raft_config,
            protocol_limits: checked.protocol_limits,
            log_limits: checked.log_limits,
            max_state_bytes: checked.max_state_bytes,
            applied_volume_states: Arc::clone(&applied_volume_states),
            replay_batch_entries: checked.protocol_limits.max_append_entries() as usize,
        };
        let groups = Arc::new(GroupRuntime::new(
            group_catalog,
            starter,
            checked.runtime_limits,
        ));
        incoming.set(&groups)?;
        let volume_node_id = mantissa_volume::VolumeNodeId::new(node_id)?;
        let worker_threads = checked.replica_file_worker_threads;
        let queue_capacity = checked.replica_file_queue_capacity;
        let replica_file_workers = tokio::task::spawn_blocking(move || {
            ReplicaFileWorkerPool::start(worker_threads, queue_capacity)
        })
        .await
        .context("join replicated-volume file-worker startup")??;
        let replica_data_server = ReplicaDataServer::new(
            replica_file_workers.clone(),
            checked.replica_data_limits,
            checked.replica_data_server_settings,
        );
        let runtime = Arc::new(Self {
            owner: OnceLock::new(),
            replicas,
            groups,
            transport,
            membership_change_blocker,
            volume_membership_locks: VolumeMembershipLocks::default(),
            cluster_view,
            applied_volume_states,
            desired_generations,
            gates: RwLock::new(BTreeMap::new()),
            replica_files: Mutex::new(BTreeMap::new()),
            replica_file_workers,
            replica_data_server,
            data_connections: DataConnections::default(),
            maintenance: MaintenanceManager::new(
                checked.max_parallel_repairs,
                checked.max_repair_chunk_bytes,
                checked.max_repair_bytes_per_second,
            ),
            fs,
            mapped_volumes,
            lifecycle_calls: lifecycle_calls::Tracker::default(),
            node_id,
            volume_node_id,
            ublk_owner_id: UblkOwnerId::for_node(node_id.as_bytes()),
            driver_limits: checked.driver_limits,
            fixed_path_settings: checked.fixed_path_settings,
            drivers: Mutex::new(BTreeMap::new()),
            volume_singleflight: VolumeSingleflightMap::default(),
            driver_lifecycle: tokio::sync::RwLock::new(()),
            protocol_limits: checked.protocol_limits,
            replica_file_settings: checked.replica_file_settings,
            replica_data_limits: checked.replica_data_limits,
            handshake_timeout: checked.transport_limits.handshake_timeout(),
            operation_timeout: checked.operation_timeout,
            stop_io_timeout: checked.stop_io_timeout,
            shutdown_timeout: checked.shutdown_timeout,
            raft_group_idle_timeout,
            repair_failure_grace: checked.repair_failure_grace,
            advertise_address: checked.advertise_address,
            max_saved_replicas: checked.max_saved_replicas,
            max_saved_groups: checked.runtime_limits.max_saved_groups(),
            stopping: AtomicBool::new(false),
        });
        runtime
            .owner
            .set(Arc::downgrade(&runtime))
            .map_err(|_| anyhow::anyhow!("replicated-volume runtime owner was already set"))?;
        service_factory.set_runtime(&runtime)?;
        Ok(runtime)
    }

    /// Recovers saved control state and local attachments before advertising readiness.
    pub async fn recover(self: &Arc<Self>) -> Result<()> {
        let records = self.replicas.discover_replicas(self.max_saved_replicas)?;
        let recovered_control_states = self.recover_saved_control_states().await?;
        for record in records {
            let key = record.key();
            if record.state() == ReplicaState::Ready
                && self.generation_is_desired(key)
                && recovered_control_states.contains(&key)
                && let Err(error) = self.ensure_local_gate(&record).await
            {
                warn!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "local replica remains quarantined after startup recovery"
                );
            }
        }
        self.recover_saved_attachments().await?;
        Ok(())
    }

    /// Replays each admitted volume group without retaining its runtime slot.
    async fn recover_saved_control_states(self: &Arc<Self>) -> Result<BTreeSet<ReplicaKey>> {
        let records = self
            .groups
            .catalog()
            .discover_groups(self.max_saved_groups)?;
        let mut recovered = BTreeSet::new();
        for record in records {
            let key = *record.group_id();
            if record.activation() != GroupActivation::Active || !self.generation_is_desired(key) {
                continue;
            }

            match tokio::time::timeout(RECONCILE_RAFT_ATTEMPT_TIMEOUT, self.groups.activate(&key))
                .await
            {
                Ok(Ok(group)) => {
                    drop(group);
                    recovered.insert(key);
                }
                Ok(Err(error)) => {
                    warn!(
                        target: "volumes",
                        ?key,
                        error = %error,
                        "saved volume control state remains quarantined after startup recovery"
                    );
                    continue;
                }
                Err(_) => {
                    warn!(
                        target: "volumes",
                        ?key,
                        timeout = ?RECONCILE_RAFT_ATTEMPT_TIMEOUT,
                        "saved volume control state recovery remains in progress"
                    );
                    continue;
                }
            }

            if let Err(error) = self.suspend_startup_group(key).await {
                warn!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "replayed volume group remains active after startup recovery"
                );
            }
        }
        Ok(recovered)
    }

    /// Releases one startup runtime slot while preserving its restart marker.
    async fn suspend_startup_group(&self, key: ReplicaKey) -> Result<()> {
        let suspended = tokio::time::timeout(
            RECONCILE_RAFT_ATTEMPT_TIMEOUT,
            self.groups.suspend_if_idle(&key, Duration::ZERO),
        )
        .await
        .context("startup volume group suspension timed out")??;
        if !suspended {
            anyhow::bail!("startup volume group is still in use");
        }
        Ok(())
    }

    /// Starts the authenticated storage listener needed by cross-node recovery.
    pub fn start_listening(&self) -> Result<()> {
        self.transport
            .start_listening()
            .context("start the replicated-volume storage listener")
    }

    /// Starts storage on a socket reserved before the rest of node bootstrap.
    pub(crate) fn start_listening_on(&self, listener: TcpListener) -> Result<()> {
        self.transport
            .start_listening_on(listener)
            .context("start the replicated-volume storage listener")
    }

    /// Builds the node's current storage support advertisement.
    pub fn support(&self, publication_generation: u64) -> Result<ReplicatedVolumeSupport> {
        let pool = self.replicas.pool_status()?;
        let below_replica_limit = self.replicas.replica_slot_count()?
            < u64::try_from(self.max_saved_replicas).unwrap_or(u64::MAX);
        let below_group_limit = self.groups.catalog().group_count()?
            < u64::try_from(self.max_saved_groups).unwrap_or(u64::MAX);
        Ok(ReplicatedVolumeSupport {
            address: self.advertise_address.to_string(),
            format_version: REPLICATED_VOLUME_FORMAT_VERSION,
            accepts_replicas: pool.state() == PoolSpaceState::Ready
                && below_replica_limit
                && below_group_limit,
            available_bytes: pool.available_bytes(),
            updated_at_unix_ms: unix_time_ms(),
            publication_generation,
        })
    }

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

    /// Returns the shared membership lock for one volume without retaining idle entries.
    fn volume_membership_lock(&self, key: ReplicaKey) -> Arc<tokio::sync::RwLock<()>> {
        self.volume_membership_locks.for_volume(key)
    }

    /// Holds one volume's membership work after a final durable split check.
    async fn lock_raft_membership_change(
        &self,
        key: ReplicaKey,
    ) -> Result<tokio::sync::OwnedRwLockReadGuard<()>> {
        let change = self.volume_membership_lock(key).read_owned().await;
        self.membership_change_blocker.ensure_changes_allowed()?;
        Ok(change)
    }

    /// Returns the transport protocol limits used by narrow storage RPCs.
    #[must_use]
    pub(crate) const fn protocol_limits(&self) -> mantissa_raft::protocol::ProtocolLimits {
        self.protocol_limits
    }

    /// Rejects an authenticated storage peer after a committed split excludes it.
    pub(super) fn ensure_peer_in_active_view(&self, peer: Uuid) -> Result<()> {
        if !self.cluster_view.includes_node(&peer) {
            anyhow::bail!("storage peer {peer} is outside the current cluster view");
        }
        Ok(())
    }

    /// Runs one narrow application RPC through the runtime-owned transport.
    pub(super) async fn call_storage_application<T, F>(
        &self,
        node_id: Uuid,
        size: usize,
        call: F,
    ) -> Result<T, TransportError>
    where
        T: Send + 'static,
        F: FnOnce(
                mantissa_protocol::raft::raft_transport::Client,
            ) -> futures::future::LocalBoxFuture<'static, Result<T, capnp::Error>>
            + Send
            + 'static,
    {
        self.transport.call_application(node_id, size, call).await
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
    fn desired_generation_guard(
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
    fn ensure_generation_desired(&self, key: ReplicaKey) -> Result<()> {
        drop(self.desired_generation_guard(key)?);
        Ok(())
    }

    /// Clones one runtime-owned driver without holding the registry across async work.
    fn tracked_driver(&self, key: ReplicaKey) -> Option<Arc<tokio::sync::Mutex<ReplicatedDriver>>> {
        self.drivers
            .lock()
            .get(&key)
            .map(|driver| Arc::clone(&driver.owner))
    }

    /// Lists complete node-local replica generations for level reconciliation.
    pub(crate) fn local_replica_keys(&self) -> Result<Vec<ReplicaKey>> {
        Ok(self
            .replicas
            .discover_replicas(self.max_saved_replicas)?
            .into_iter()
            .map(|record| record.key())
            .collect())
    }

    /// Returns whether control state already removed this node's former live copy.
    pub(crate) fn local_replica_retired(&self, key: ReplicaKey) -> Result<bool> {
        Ok(self.replicas.retirement(key)?.is_some())
    }

    /// Returns immutable local provisioning provenance for reconciliation.
    pub(crate) fn local_replica_origin(
        &self,
        key: ReplicaKey,
    ) -> Result<Option<LocalReplicaOrigin>> {
        Ok(self
            .replicas
            .replica(key)?
            .map(|record| record.origin().clone()))
    }

    /// Forgets retirement proof made obsolete by a newer desired generation.
    pub(crate) fn forget_superseded_replica_retirement(&self, current: ReplicaKey) -> Result<()> {
        self.replicas.forget_retirement_before(current)?;
        Ok(())
    }

    /// Converts a former-member proof into retryable local physical cleanup.
    pub(crate) async fn retire_local_replica(&self, key: ReplicaKey) -> Result<()> {
        self.ensure_generation_desired(key)?;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.ensure_generation_desired(key)?;
        self.lifecycle_calls
            .wait_for_idle(key, self.stop_io_timeout)
            .await
            .context("wait for earlier local work before replica retirement")?;
        let Some(record) = self.replicas.replica(key)? else {
            if self.replicas.retirement(key)?.is_some() {
                return Ok(());
            }
            if self.maintenance.cancel_other(key, None) {
                anyhow::bail!("replica maintenance is still stopping before local retirement");
            }
            self.close_replica_io(key).await?;
            self.stop_and_remove_group(key).await?;
            self.retire_local_gate_after_local_revocation(key)?;
            self.close_replica_file(key)?;
            if !self.data_connections.forget_closed(key) {
                anyhow::bail!("retired replica data connections became active during cleanup");
            }
            self.replicas
                .record_missing_replica_retirement(key, self.max_saved_replicas)?;
            return Ok(());
        };
        if record.state() != ReplicaState::Retiring {
            self.replicas
                .set_replica_state(key, ReplicaState::Retiring)?;
        }
        let record = self
            .replicas
            .replica(key)?
            .context("retiring replica disappeared before cleanup")?;
        self.finish_retired_replica(record).await
    }

    /// Forgets obsolete former-member proof after terminal desired deletion.
    pub(crate) fn delete_retired_replica(&self, key: ReplicaKey) -> Result<()> {
        self.replicas.remove_deleted_replica(key)?;
        Ok(())
    }

    /// Returns catalog and kernel mount facts without consulting public CRDT state.
    pub(crate) async fn local_volume_mount_paths(&self) -> Result<BTreeSet<PathBuf>> {
        let replicas = self.replicas.clone();
        let maximum = self.max_saved_replicas;
        let fs = self.fs.clone();
        tokio::task::spawn_blocking(move || {
            let mut paths = replicas
                .discover_attachments(maximum)?
                .into_iter()
                .filter_map(|record| {
                    record
                        .volume_mount()
                        .map(|mount| mount.path().to_path_buf())
                })
                .collect::<BTreeSet<_>>();
            paths.extend(fs.mounted_volume_paths()?);
            Result::<_, anyhow::Error>::Ok(paths)
        })
        .await
        .context("join local replicated-volume mount inventory")?
    }

    /// Ensures one bootstrap replica and exact common three-voter group locally.
    pub(crate) async fn ensure_replica_local(
        &self,
        descriptor: mantissa_volume::VolumeDescriptor,
        bootstrap_id: mantissa_volume::OperationId,
        voters: BTreeSet<Uuid>,
    ) -> Result<LocalReplicaStatus> {
        self.membership_change_blocker.ensure_changes_allowed()?;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        if voters.len() != 3 || !voters.contains(&self.node_id) {
            anyhow::bail!("local bootstrap requires an exact three-voter set containing this node");
        }
        let key = ReplicaKey::from(&descriptor);
        self.ensure_generation_desired(key)?;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        self.ensure_generation_desired(key)?;
        let existing_replica = self.replicas.replica(key)?;
        if existing_replica.is_none() && self.groups.catalog().group(&key)?.is_some() {
            anyhow::bail!(
                "refusing to create an empty bootstrap replica while local Raft state survives"
            );
        }
        let replica_was_ready = existing_replica
            .as_ref()
            .is_some_and(|record| record.state() == ReplicaState::Ready);
        let replicas = self.replicas.clone();
        let groups = self.groups.catalog().clone();
        let settings = self.replica_file_settings;
        let max_saved_replicas = self.max_saved_replicas;
        let max_saved_groups = self.max_saved_groups;
        let saved_voters = voters.clone();
        let ready_file_is_open = self.replica_files.lock().contains_key(&key);
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-bootstrap-replica",
                self.operation_timeout,
                move || -> Result<()> {
                    let record = replicas.reserve_replica_bounded(
                        descriptor,
                        LocalReplicaOrigin::Bootstrap(bootstrap_id),
                        max_saved_replicas,
                    )?;
                    prepare_empty_replica_file(&replicas, &record, settings, ready_file_is_open)?;
                    let group = groups.ensure_group_bounded(
                        &key,
                        GroupActivation::Active,
                        max_saved_groups,
                    )?;
                    match group.membership() {
                        None => {
                            let membership =
                                Membership::new(vec![saved_voters.clone()], saved_voters);
                            groups.save_membership(
                                &key,
                                &StoredMembership::<Uuid, EmptyNode>::new(None, membership),
                            )?;
                        }
                        Some(membership)
                            if membership.log_id().is_none()
                                && membership.membership().get_joint_config().len() == 1
                                && membership.voter_ids().collect::<BTreeSet<_>>()
                                    == saved_voters => {}
                        // Once Raft has committed membership, that membership is the
                        // control state and may legitimately have evolved through replica
                        // replacement. A delayed bootstrap ensure verifies the exact
                        // descriptor and bootstrap origin above but must not reinterpret
                        // or reset current OpenRaft membership.
                        Some(membership) if membership.log_id().is_some() => {}
                        Some(_) => {
                            anyhow::bail!("local group has conflicting bootstrap membership")
                        }
                    }
                    Ok(())
                },
            )
            .await
            .context("ensure local bootstrap replica")?;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime stopped during local replica ensure");
        }
        self.ensure_generation_desired(key)?;
        let record = self
            .replicas
            .replica(key)?
            .context("ensured replica disappeared from local catalog")?;
        if !replica_was_ready && record.state() == ReplicaState::Ready {
            info!(
                target: "mantissa::volumes::replicated",
                volume_id = %key.volume_id().as_uuid(),
                generation = key.generation().get(),
                local_node_id = %self.node_id,
                voter_node_ids = ?voters,
                "prepared local replicated-volume copy"
            );
        }
        self.ensure_local_gate(&record).await?;
        self.local_status(key).await
    }

    /// Initializes the exact bootstrap membership after all planned copies exist.
    pub(crate) async fn initialize_bootstrap(
        &self,
        key: ReplicaKey,
        voters: BTreeSet<Uuid>,
    ) -> Result<()> {
        let _membership_change = self.lock_raft_membership_change(key).await?;
        self.ensure_generation_desired(key)?;
        // OpenRaft starts an election as part of initializing a pristine
        // group. The replica ensure phase has saved the group on each voter
        // but intentionally left it idle, so start the remote endpoints before
        // the local initialize call can send its first vote requests.
        self.wake_voters(key, &voters).await;
        let group = self.groups.activate(&key).await?;
        if group.metrics().last_log_index.is_some() && group.voter_node_ids() == voters {
            return Ok(());
        }
        match group.initialize(voters.clone()).await {
            Ok(()) => Ok(()),
            Err(_error)
                if group.metrics().last_log_index.is_some() && group.voter_node_ids() == voters =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Ensures one replacement file and activates its saved non-voting group endpoint.
    pub(crate) async fn ensure_replacement_local(
        &self,
        descriptor: mantissa_volume::VolumeDescriptor,
        replacement_id: mantissa_volume::ReplacementId,
        voters: BTreeSet<Uuid>,
    ) -> Result<LocalReplicaStatus> {
        self.membership_change_blocker.ensure_changes_allowed()?;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        if !replacement_voter_hint_is_valid(&voters, self.node_id) {
            anyhow::bail!(
                "replacement ensure requires three stable voters or a four-voter joint \
                 configuration containing this target"
            );
        }
        let key = ReplicaKey::from(&descriptor);
        self.ensure_generation_desired(key)?;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        self.ensure_generation_desired(key)?;
        if voters.contains(&self.node_id) {
            // Degraded control state can rebuild a returned configured voter in
            // place. A durably unhealthy file is reset only after local
            // applied state proves this node is the exact inactive target.
            // The ensuing full repair still overwrites the complete file
            // before Raft may restore it to data control state.
            let mut record = self
                .replicas
                .replica(key)?
                .context("in-place replacement voter has no local replica")?;
            if !replacement_capacity_can_converge(record.descriptor(), &descriptor) {
                anyhow::bail!("in-place replacement voter has a conflicting descriptor");
            }
            if self.groups.catalog().group(&key)?.is_none() {
                anyhow::bail!("in-place replacement voter has no saved Raft group");
            }
            if record.health() == ReplicaHealth::NeedsRecovery {
                self.reset_unhealthy_replacement_file(&record, replacement_id)
                    .await?;
                record = self
                    .replicas
                    .replica(key)?
                    .context("reset in-place replacement replica disappeared")?;
            }
            if record.state() != ReplicaState::Ready || record.health() != ReplicaHealth::Healthy {
                anyhow::bail!("in-place replacement voter has no usable ready replica");
            }
            if record.descriptor().capacity() < descriptor.capacity() {
                self.apply_local_replica_capacity_locked(key, descriptor.capacity())
                    .await?;
                record = self
                    .replicas
                    .replica(key)?
                    .context("expanded in-place replacement replica disappeared")?;
            }
            if record.descriptor() != &descriptor {
                anyhow::bail!("in-place replacement voter did not reach current capacity");
            }
            if !self.data_connections.reopen(key) {
                anyhow::bail!("old in-place replacement streams are still closing");
            }
            self.ensure_local_gate(&record).await?;
            return self.local_status(key).await;
        }
        if let Some(mut record) = self.replicas.replica(key)? {
            // A node that returns after quorum removed it still owns its old
            // bootstrap row. Wake its saved Raft endpoint first, but do not
            // erase the file until caught-up applied state names this exact
            // node and replacement grant while excluding it from data work.
            if !replacement_capacity_can_converge(record.descriptor(), &descriptor) {
                anyhow::bail!("excluded replacement target has a conflicting descriptor");
            }
            if self.groups.catalog().group(&key)?.is_none() {
                anyhow::bail!("excluded replacement target has no saved Raft group");
            }
            self.close_replica_io(key).await?;
            let group = self
                .groups
                .activate(&key)
                .await
                .context("activate excluded replacement Raft endpoint")?;
            let applied = group.state();
            drop(group);
            if applied.descriptor() != Some(&descriptor)
                || validate_replacement_target_reset(&applied, replacement_id, self.volume_node_id)
                    .is_err()
            {
                return self.local_status(key).await;
            }

            self.close_replica_file(key)?;
            let replacement_origin =
                LocalReplicaOrigin::replacement(replacement_id, voters.clone())?;
            if record.origin() != &replacement_origin
                || record.state() == ReplicaState::Preparing
                || record.health() == ReplicaHealth::NeedsRecovery
            {
                let replicas = self.replicas.clone();
                let settings = self.replica_file_settings;
                self.lifecycle_calls
                    .run(
                        key,
                        "mantissa-volume-reuse-excluded-replica",
                        self.operation_timeout,
                        move || -> Result<()> {
                            replicas.reserve_replica_capacity(key, descriptor.capacity())?;
                            let record = replicas.begin_replica_replacement(
                                key,
                                descriptor,
                                replacement_origin,
                            )?;
                            prepare_empty_replica_file(&replicas, &record, settings, false)
                        },
                    )
                    .await
                    .context("rebuild excluded replica as the current replacement")?;
            } else {
                if record.descriptor().capacity() < descriptor.capacity() {
                    self.apply_local_replica_capacity_locked(key, descriptor.capacity())
                        .await?;
                    record = self
                        .replicas
                        .replica(key)?
                        .context("expanded replacement replica disappeared")?;
                }
                let replicas = self.replicas.clone();
                let settings = self.replica_file_settings;
                self.lifecycle_calls
                    .run(
                        key,
                        "mantissa-volume-verify-replacement-replica",
                        self.operation_timeout,
                        move || prepare_empty_replica_file(&replicas, &record, settings, false),
                    )
                    .await
                    .context("verify existing replacement replica")?;
            }
            if !self.data_connections.reopen(key) {
                anyhow::bail!("old excluded replacement streams are still closing");
            }
            let record = self
                .replicas
                .replica(key)?
                .context("rebuilt replacement replica disappeared before admission")?;
            self.ensure_local_gate(&record).await?;
            return self.local_status(key).await;
        }
        let replicas = self.replicas.clone();
        let groups = self.groups.catalog().clone();
        let settings = self.replica_file_settings;
        let max_saved_replicas = self.max_saved_replicas;
        let max_saved_groups = self.max_saved_groups;
        let saved_voters = voters.clone();
        let ready_file_is_open = self.replica_files.lock().contains_key(&key);
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-replacement-replica",
                self.operation_timeout,
                move || -> Result<()> {
                    let record = replicas.reserve_replica_bounded(
                        descriptor,
                        LocalReplicaOrigin::replacement(replacement_id, saved_voters.clone())?,
                        max_saved_replicas,
                    )?;
                    prepare_empty_replica_file(&replicas, &record, settings, ready_file_is_open)?;
                    let group = groups.ensure_group_bounded(
                        &key,
                        GroupActivation::Inactive,
                        max_saved_groups,
                    )?;
                    match group.membership() {
                        None => {
                            let membership =
                                Membership::new(vec![saved_voters.clone()], saved_voters);
                            groups.save_membership(
                                &key,
                                &StoredMembership::<Uuid, EmptyNode>::new(None, membership),
                            )?;
                        }
                        Some(membership)
                            if membership.log_id().is_none()
                                && membership.membership().get_joint_config().len() == 1
                                && membership.voter_ids().collect::<BTreeSet<_>>()
                                    == saved_voters => {}
                        // Current committed membership supersedes the pre-bootstrap
                        // voter hint after this replacement has joined or completed.
                        Some(membership) if membership.log_id().is_some() => {}
                        Some(_) => {
                            anyhow::bail!("replacement has conflicting saved membership")
                        }
                    }
                    Ok(())
                },
            )
            .await
            .context("ensure local replacement replica")?;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime stopped during replacement ensure");
        }
        self.ensure_generation_desired(key)?;
        drop(
            self.groups
                .activate(&key)
                .await
                .context("activate replacement Raft endpoint")?,
        );
        let record = self
            .replicas
            .replica(key)?
            .context("prepared replacement replica disappeared before admission")?;
        self.ensure_local_gate(&record).await?;
        self.local_status(key).await
    }

    /// Resets one unhealthy voter only while applied state names it as an inactive target.
    async fn reset_unhealthy_replacement_file(
        &self,
        record: &ReplicaRecord,
        replacement_id: ReplacementId,
    ) -> Result<()> {
        let key = record.key();
        let group = self
            .groups
            .activate(&key)
            .await
            .context("activate unhealthy in-place replacement voter")?;
        let control_state = group.state();
        drop(group);
        validate_replacement_target_reset(&control_state, replacement_id, self.volume_node_id)?;
        if !matches!(
            record.state(),
            ReplicaState::Ready | ReplicaState::Preparing
        ) {
            anyhow::bail!("unhealthy replacement replica is not locally rebuildable");
        }
        if record.state() == ReplicaState::Ready {
            self.replicas
                .set_replica_state(key, ReplicaState::Preparing)?;
        }
        self.close_replica_io(key).await?;
        self.close_replica_file(key)?;
        let replicas = self.replicas.clone();
        let settings = self.replica_file_settings;
        let current = self
            .replicas
            .replica(key)?
            .context("unhealthy replacement replica disappeared before reset")?;
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-reset-unhealthy-replica",
                self.operation_timeout,
                move || prepare_empty_replica_file(&replicas, &current, settings, false),
            )
            .await
            .context("reset unhealthy in-place replacement file")
    }

    /// Reads current locally applied state, starting its saved group if needed.
    pub(crate) async fn read_applied_state(&self, key: ReplicaKey) -> Result<VolumeControlState> {
        self.ensure_generation_desired(key)?;
        let state = self.groups.activate(&key).await?.state();
        if state.descriptor().is_some()
            && let Some(record) = self.replicas.replica(key)?
        {
            self.ensure_local_gate(&record).await?;
        }
        Ok(state)
    }

    /// Reads already-applied durable control state without starting the Raft group.
    pub(crate) fn applied_state(&self, key: ReplicaKey) -> Result<Option<VolumeControlState>> {
        self.ensure_generation_desired(key)?;
        self.applied_volume_states
            .cell(key)
            .map(|cell| cell.load().control_state().map_err(anyhow::Error::from))
            .transpose()
    }

    /// Returns quorum-confirmed control state only when this member is the leader.
    pub(crate) async fn local_quorum_state(
        &self,
        key: ReplicaKey,
    ) -> Result<Option<VolumeControlState>> {
        self.ensure_generation_desired(key)?;
        let group = self.activate_with_voters(key).await?;
        let leader = group.wait_for_leader(self.operation_timeout).await?;
        if leader != self.node_id {
            return Ok(None);
        }
        let state = group.leader_state(self.operation_timeout).await?;
        if let Some(record) = self.replicas.replica(key)? {
            self.ensure_local_gate(&record).await?;
        }
        Ok(Some(state))
    }

    /// Wakes saved voters and makes one bounded quorum-read attempt for reconciliation.
    pub(crate) async fn poll_local_quorum_state(
        &self,
        key: ReplicaKey,
    ) -> Result<Option<VolumeControlState>> {
        self.ensure_generation_desired(key)?;
        let group = self.activate_with_voters(key).await?;
        if group.metrics().leader != Some(self.node_id) {
            return Ok(None);
        }
        let control_state = match tokio::time::timeout(
            RECONCILE_RAFT_ATTEMPT_TIMEOUT,
            group.leader_state(self.operation_timeout),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => return Ok(None),
        };
        if let Some(record) = self.replicas.replica(key)? {
            self.ensure_local_gate(&record).await?;
        }
        Ok(Some(control_state))
    }

    /// Reads one running leader's applied state without requiring a live quorum.
    pub(crate) async fn poll_running_local_leader_observation(
        &self,
        key: ReplicaKey,
    ) -> Result<Option<LeaderVolumeGroupState>> {
        self.ensure_generation_desired(key)?;
        let group = match self.groups.group(&key).await {
            Ok(group) => group,
            Err(RuntimeError::GroupNotRunning) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if group.metrics().leader != Some(self.node_id) {
            return Ok(None);
        }
        // Group status is an advisory CRDT observation and never authorizes a
        // transition. Reading the already-applied state here lets every stable
        // voter, including the last leader, become idle. Actionable desired or
        // health facts take the separate quorum path above, which explicitly
        // wakes the saved voters before reading state or proposing a command.
        let control_state = group.state();
        if group.metrics().leader != Some(self.node_id) {
            return Ok(None);
        }
        if let Some(record) = self.replicas.replica(key)? {
            self.ensure_local_gate(&record).await?;
        }
        Ok(Some(LeaderVolumeGroupState {
            control_state,
            membership: RaftMembershipSnapshot {
                voters: group.voter_node_ids(),
                members: group.member_node_ids(),
                is_joint: group.membership_is_joint(),
            },
        }))
    }

    /// Returns quorum-confirmed control state and membership on this leader.
    pub(crate) async fn inspect_quorum_state_as_leader(
        &self,
        key: ReplicaKey,
    ) -> Result<LeaderVolumeGroupState> {
        // The exclusive side returns a snapshot that cannot overlap a local
        // membership change. During split validation, the Proposed row has
        // already converged: this waits for older calls, while later calls
        // fail their durable split check.
        let _membership_changes = self.volume_membership_lock(key).write_owned().await;
        self.ensure_generation_desired(key)?;
        let group = self.groups.activate(&key).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("local volume member is not the current Raft leader");
        }
        let control_state = group.leader_state(self.operation_timeout).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("volume leadership changed during quorum-state inspection");
        }
        Ok(LeaderVolumeGroupState {
            control_state,
            membership: RaftMembershipSnapshot {
                voters: group.voter_node_ids(),
                members: group.member_node_ids(),
                is_joint: group.membership_is_joint(),
            },
        })
    }

    /// Proposes through this member only while it remains the elected leader.
    pub(crate) async fn propose_as_leader(
        &self,
        key: ReplicaKey,
        command: VolumeCommand,
    ) -> Result<VolumeCommandResponse> {
        let _membership_change = if split_blocks_command(&command) {
            Some(self.lock_raft_membership_change(key).await?)
        } else {
            None
        };
        self.ensure_generation_desired(key)?;
        let group = self.groups.activate(&key).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("local volume member is not the current Raft leader");
        }
        let response = group.write(command).await?.response;
        if let Some(record) = self.replicas.replica(key)? {
            self.ensure_local_gate(&record).await?;
        }
        Ok(response)
    }

    /// Makes one bounded semantic proposal attempt for level reconciliation.
    ///
    /// Cancelling the response wait does not cancel ownership inside the Raft
    /// core. A command that commits after the deadline is recognized as current
    /// by the next pass, so the controller never waits on peer wakeup or stores
    /// an RPC completion phase.
    pub(crate) async fn propose_as_leader_for_reconcile(
        &self,
        key: ReplicaKey,
        command: VolumeCommand,
    ) -> Result<VolumeCommandResponse> {
        let _membership_change = if split_blocks_command(&command) {
            Some(self.lock_raft_membership_change(key).await?)
        } else {
            None
        };
        self.ensure_generation_desired(key)?;
        let group = self.groups.activate(&key).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("local volume member is not the current Raft leader");
        }
        let response = tokio::time::timeout(RECONCILE_RAFT_ATTEMPT_TIMEOUT, group.write(command))
            .await
            .context("volume control state reconciliation attempt timed out")??
            .response;
        if let Some(record) = self.replicas.replica(key)? {
            self.ensure_local_gate(&record).await?;
        }
        Ok(response)
    }

    /// Proposes one semantic compare-and-set control-state transition.
    pub(crate) async fn propose_volume_command(
        &self,
        key: ReplicaKey,
        command: VolumeCommand,
    ) -> Result<VolumeCommandResponse> {
        let _membership_change = if split_blocks_command(&command) {
            Some(self.lock_raft_membership_change(key).await?)
        } else {
            None
        };
        self.ensure_generation_desired(key)?;
        let descriptor = self
            .replicas
            .replica(key)?
            .context("volume control state proposal has no local replica")?
            .descriptor()
            .clone();
        let group = self.activate_with_voters(key).await?;
        let leader = group.wait_for_leader(self.operation_timeout).await?;
        let response = if leader == self.node_id {
            group.write(command).await?.response
        } else {
            self.propose_volume_command_on(leader, descriptor, command)
                .await?
        };
        if let Some(record) = self.replicas.replica(key)? {
            self.ensure_local_gate(&record).await?;
        }
        Ok(response)
    }

    /// Makes one bounded control state proposal from local attachment reconciliation.
    async fn propose_volume_command_for_reconcile(
        &self,
        key: ReplicaKey,
        command: VolumeCommand,
    ) -> Result<VolumeCommandResponse> {
        tokio::time::timeout(
            ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT,
            self.propose_volume_command(key, command),
        )
        .await
        .context("attachment control-state reconciliation attempt timed out")?
    }

    /// Returns current committed voter membership from the local group view.
    pub(crate) async fn membership(&self, key: ReplicaKey) -> Result<BTreeSet<Uuid>> {
        self.ensure_generation_desired(key)?;
        Ok(self.groups.activate(&key).await?.voter_node_ids())
    }

    /// Reads durable local membership without activating its Raft member.
    pub(crate) fn saved_membership(&self, key: ReplicaKey) -> Result<BTreeSet<Uuid>> {
        self.ensure_generation_desired(key)?;
        Ok(self
            .groups
            .catalog()
            .group(&key)?
            .and_then(|record| record.membership().cloned())
            .map(|membership| membership.voter_ids().collect())
            .unwrap_or_default())
    }

    /// Suspends process-idle groups while preserving restart replay markers.
    pub(crate) async fn suspend_idle_groups(&self) -> Result<usize> {
        let suspended = tokio::time::timeout(
            RECONCILE_RAFT_ATTEMPT_TIMEOUT,
            self.groups.suspend_idle(self.raft_group_idle_timeout),
        )
        .await
        .context("idle volume group suspension timed out")?
        .map_err(anyhow::Error::from)?;
        if suspended > 0 {
            debug!(
                target: "mantissa::volumes::raft",
                local_node_id = %self.node_id,
                suspended_group_count = suspended,
                "stopped idle volume Raft members"
            );
        }
        Ok(suspended)
    }

    /// Ensures the learner or final voters for one exact replacement on this leader.
    pub(crate) async fn ensure_replacement_membership_as_leader(
        &self,
        key: ReplicaKey,
        replacement_id: mantissa_volume::ReplacementId,
        coordinator_node_id: Uuid,
        goal: ReplacementMembershipGoal,
    ) -> Result<BTreeSet<Uuid>> {
        let _membership_change = self.lock_raft_membership_change(key).await?;
        self.ensure_generation_desired(key)?;
        let group = self.groups.activate(&key).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("local volume member is not the current Raft leader");
        }
        let state = group.leader_state(self.operation_timeout).await?;
        let replacement = state
            .replacement()
            .context("volume has no current replacement grant")?;
        if replacement.id != replacement_id
            || *replacement.coordinator_node_id.as_uuid() != coordinator_node_id
        {
            anyhow::bail!("replacement membership request does not match current control state");
        }
        let data = state
            .data()
            .context("initialized volume has no data control state")?;
        let mut voters = group.voter_node_ids();
        let new_node_id = *replacement.new_node_id.as_uuid();
        match goal {
            ReplacementMembershipGoal::Learner => {
                let members = group.member_node_ids();
                for stale in
                    stale_replacement_learners(&members, &voters, &data.copies, new_node_id)
                {
                    group.remove_learner(stale).await?;
                }
                if !group.member_node_ids().contains(&new_node_id) {
                    group.add_learner(new_node_id).await?;
                }
                return Ok(group.voter_node_ids());
            }
            ReplacementMembershipGoal::Absent { rollback_voters } => {
                validate_replacement_rollback_voters(&data.copies, &rollback_voters)?;
                for voter in &rollback_voters {
                    if !group.member_node_ids().contains(voter) {
                        group.add_learner(*voter).await?;
                    }
                }
                if voters != rollback_voters {
                    group.set_voters(rollback_voters.clone()).await?;
                    voters = group.voter_node_ids();
                }
                if group.member_node_ids().contains(&new_node_id) {
                    group.remove_learner(new_node_id).await?;
                }
                if voters != rollback_voters || group.member_node_ids().contains(&new_node_id) {
                    anyhow::bail!("replacement membership rollback has not converged");
                }
                return Ok(voters);
            }
            ReplacementMembershipGoal::FinalVoters => {}
        }
        let mut final_set = data
            .copies
            .iter()
            .map(|node_id| *node_id.as_uuid())
            .collect::<BTreeSet<_>>();
        if let Some(old_node_id) = replacement.old_node_id {
            final_set.remove(old_node_id.as_uuid());
        }
        final_set.insert(new_node_id);
        if final_set.len() != 3 {
            anyhow::bail!("replacement does not produce exactly three final voters");
        }
        if voters != final_set {
            group.set_voters(final_set.clone()).await?;
            voters = group.voter_node_ids();
        }
        if voters != final_set {
            anyhow::bail!("replacement voter change has not converged");
        }
        Ok(voters)
    }

    /// Removes only extra voters after committed control state still names all three data copies.
    pub(crate) async fn ensure_data_membership_as_leader(
        &self,
        key: ReplicaKey,
        expected_revision: u64,
        expected_voters: BTreeSet<Uuid>,
    ) -> Result<BTreeSet<Uuid>> {
        let _membership_change = self.lock_raft_membership_change(key).await?;
        self.ensure_generation_desired(key)?;
        let group = self.groups.activate(&key).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("local volume member is not the current Raft leader");
        }
        let state = group.leader_state(self.operation_timeout).await?;
        let data = state
            .data()
            .context("initialized volume has no data control state")?;
        let current_copies = data
            .copies
            .iter()
            .map(|node_id| *node_id.as_uuid())
            .collect::<BTreeSet<_>>();
        if state.disposition() != VolumeDisposition::Live
            || state.revision() != expected_revision
            || state.replacement().is_some()
            || data.recovery.is_some()
            || current_copies != expected_voters
            || expected_voters.len() != 3
        {
            anyhow::bail!("control-state changed before data membership reconciliation");
        }
        let mut voters = group.voter_node_ids();
        if !expected_voters.is_subset(&voters) {
            anyhow::bail!("current membership does not contain every active data copy");
        }
        if voters != expected_voters {
            group.set_voters(expected_voters.clone()).await?;
            voters = group.voter_node_ids();
        }
        if voters != expected_voters {
            anyhow::bail!("data membership reconciliation has not converged");
        }
        Ok(voters)
    }

    /// Routes one replacement membership ensure without changing Raft leadership.
    pub(crate) async fn ensure_replacement_membership(
        &self,
        descriptor: mantissa_volume::VolumeDescriptor,
        replacement_id: mantissa_volume::ReplacementId,
        goal: ReplacementMembershipGoal,
    ) -> Result<BTreeSet<Uuid>> {
        let key = ReplicaKey::from(&descriptor);
        let group = self.activate_with_voters(key).await?;
        let leader = group.wait_for_leader(self.operation_timeout).await?;
        if leader == self.node_id {
            self.ensure_replacement_membership_as_leader(key, replacement_id, self.node_id, goal)
                .await
        } else {
            self.ensure_replacement_membership_on(leader, descriptor, replacement_id, goal)
                .await
        }
    }

    /// Reports local catalog, group, and applied-state facts without advancing work.
    pub(crate) async fn local_status(&self, key: ReplicaKey) -> Result<LocalReplicaStatus> {
        let record = self.replicas.replica(key)?;
        let capacity = self.local_replica_capacity_status(key, None)?;
        let saved = self.groups.catalog().group(&key)?;
        let running = self.groups.group(&key).await.ok();
        let applied_state = self.applied_volume_states.cell(key).map(|cell| cell.load());
        let metrics = running.as_ref().map(|group| group.metrics());
        let voter_node_ids = saved
            .as_ref()
            .and_then(|group| group.membership())
            .map(|membership| membership.voter_ids().collect())
            .unwrap_or_default();
        let attachment = self.replicas.attachment(key)?;
        let device_capacity_bytes = match (
            attachment
                .as_ref()
                .and_then(LocalAttachmentRecord::volume_mount),
            self.tracked_driver(key),
        ) {
            (Some(_), Some(driver)) => Some(driver.lock().await.device_capacity_bytes()),
            _ => None,
        };
        let filesystem_expansion_pending = attachment
            .as_ref()
            .and_then(LocalAttachmentRecord::volume_mount)
            .zip(device_capacity_bytes)
            .is_some_and(|(mount, device)| mount.filesystem_expanded_to_bytes() < device);
        Ok(LocalReplicaStatus {
            exists: record.is_some(),
            state: record
                .as_ref()
                .map_or(ReplicaState::Preparing, ReplicaRecord::state),
            health: record
                .as_ref()
                .map_or(ReplicaHealth::Healthy, ReplicaRecord::health),
            group_saved: saved.is_some(),
            control_state_initialized: applied_state.is_some(),
            applied_log_index: applied_state
                .as_ref()
                .map(|applied_state| applied_state.applied.index())
                .or_else(|| {
                    saved
                        .as_ref()
                        .and_then(|group| group.applied_log_id())
                        .map(|log_id| log_id.index)
                }),
            leader_node_id: metrics.and_then(|metrics| metrics.leader),
            voter_node_ids,
            reserved_capacity_bytes: capacity.reserved_capacity_bytes,
            prepared_capacity_bytes: capacity.prepared_capacity_bytes,
            served_capacity_bytes: capacity.served_capacity_bytes,
            device_capacity_bytes,
            filesystem_expansion_pending,
        })
    }

    /// Reads local reservation, file coverage, and served bounds without advancing work.
    pub(crate) fn local_replica_capacity_status(
        &self,
        key: ReplicaKey,
        target: Option<VolumeCapacity>,
    ) -> Result<ReplicaCapacityStatus> {
        let Some(record) = self.replicas.replica(key)? else {
            return Ok(ReplicaCapacityStatus {
                reserved_capacity_bytes: 0,
                prepared_capacity_bytes: 0,
                served_capacity_bytes: 0,
                healthy: false,
                reason: "local replica is not present".to_owned(),
            });
        };
        let reserved_capacity_bytes = record.reserved_space().data_bytes();
        let open_file = self.replica_files.lock().get(&key).cloned();
        let prepared = if let Some(file) = open_file.as_ref() {
            file.prepared_capacity()
        } else {
            ReplicaFile::prepared_capacity_at(
                record.path(self.replicas.pool_root()).join("blocks"),
                record.descriptor(),
            )
        };
        let prepared_capacity_bytes = match prepared {
            Ok(prepared) => prepared.bytes().min(reserved_capacity_bytes),
            Err(error) => {
                return Ok(ReplicaCapacityStatus {
                    reserved_capacity_bytes,
                    prepared_capacity_bytes: 0,
                    served_capacity_bytes: 0,
                    healthy: false,
                    reason: format!("local replica file coverage is unavailable: {error}"),
                });
            }
        };
        let served_capacity_bytes = open_file.as_ref().map_or_else(
            || record.descriptor().capacity().bytes(),
            |file| file.served_capacity().bytes(),
        );
        let base_healthy = record.state() == ReplicaState::Ready
            && record.health() == ReplicaHealth::Healthy
            && served_capacity_bytes <= prepared_capacity_bytes
            && record.descriptor().capacity().bytes() <= served_capacity_bytes;
        let reason = if record.state() != ReplicaState::Ready {
            format!("local replica is {}", record.state())
        } else if record.health() != ReplicaHealth::Healthy {
            "local replica requires recovery".to_owned()
        } else if let Some(target) = target
            && reserved_capacity_bytes < target.bytes()
        {
            format!(
                "reserved capacity is {} bytes, below target {} bytes",
                reserved_capacity_bytes,
                target.bytes()
            )
        } else if let Some(target) = target
            && prepared_capacity_bytes < target.bytes()
        {
            format!(
                "prepared capacity is {} bytes, below target {} bytes",
                prepared_capacity_bytes,
                target.bytes()
            )
        } else if !base_healthy {
            "local served capacity is inconsistent with durable file coverage".to_owned()
        } else {
            String::new()
        };
        Ok(ReplicaCapacityStatus {
            reserved_capacity_bytes,
            prepared_capacity_bytes,
            served_capacity_bytes,
            healthy: base_healthy,
            reason,
        })
    }

    /// Reserves and durably prepares one desired capacity without exposing it.
    pub(crate) async fn reconcile_local_replica_capacity(
        &self,
        key: ReplicaKey,
        target: VolumeCapacity,
    ) -> Result<ReplicaCapacityStatus> {
        self.ensure_generation_desired(key)?;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.ensure_generation_desired(key)?;
        let record = self
            .replicas
            .replica(key)?
            .context("capacity preparation has no local replica")?;
        if record.state() != ReplicaState::Ready || record.health() != ReplicaHealth::Healthy {
            return self.local_replica_capacity_status(key, Some(target));
        }
        record.descriptor().with_capacity(target)?;
        if target < record.descriptor().capacity() {
            anyhow::bail!("desired replica capacity is below applied Raft capacity");
        }

        let replicas = self.replicas.clone();
        let settings = self.replica_file_settings;
        let directory = record.path(replicas.pool_root()).join("blocks");
        let open_file = self.replica_files.lock().get(&key).cloned();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-prepare-capacity",
                self.operation_timeout,
                move || -> Result<()> {
                    let current = replicas
                        .replica(key)?
                        .context("capacity preparation replica disappeared")?;
                    if current.reserved_space().data_bytes() < target.bytes() {
                        replicas.reserve_replica_capacity(key, target)?;
                    } else if current.reserved_space().data_bytes() > target.bytes() {
                        replicas.reduce_replica_reservation(key, target)?;
                    }
                    if let Some(file) = open_file.as_ref() {
                        file.prepare_capacity(target)?;
                    } else {
                        let file =
                            ReplicaFile::open(&directory, current.descriptor().clone(), settings)?;
                        file.prepare_capacity(target)?;
                    }
                    Ok(())
                },
            )
            .await
            .context("prepare local replica capacity")?;
        self.local_replica_capacity_status(key, Some(target))
    }

    /// Applies one committed capacity locally and then raises the live request bound.
    async fn apply_local_replica_capacity(
        &self,
        key: ReplicaKey,
        target: VolumeCapacity,
    ) -> Result<()> {
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.apply_local_replica_capacity_locked(key, target).await
    }

    /// Applies committed capacity while the caller holds this replica's exclusive lane.
    async fn apply_local_replica_capacity_locked(
        &self,
        key: ReplicaKey,
        target: VolumeCapacity,
    ) -> Result<()> {
        let record = self
            .replicas
            .replica(key)?
            .context("committed capacity has no local replica")?;
        if target < record.descriptor().capacity() {
            anyhow::bail!("committed replica capacity moved backwards");
        }
        if target == record.descriptor().capacity() {
            if let Some(file) = self.replica_files.lock().get(&key).cloned() {
                file.serve_capacity(target)?;
            }
            return Ok(());
        }

        let replicas = self.replicas.clone();
        let settings = self.replica_file_settings;
        let directory = record.path(replicas.pool_root()).join("blocks");
        let open_file = self.replica_files.lock().get(&key).cloned();
        let file_for_call = open_file.clone();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-apply-capacity",
                self.operation_timeout,
                move || -> Result<()> {
                    let current = replicas
                        .replica(key)?
                        .context("capacity application replica disappeared")?;
                    if current.reserved_space().data_bytes() < target.bytes() {
                        replicas.reserve_replica_capacity(key, target)?;
                    }
                    if let Some(file) = file_for_call.as_ref() {
                        file.prepare_capacity(target)?;
                    } else {
                        let file =
                            ReplicaFile::open(&directory, current.descriptor().clone(), settings)?;
                        file.prepare_capacity(target)?;
                    }
                    replicas.apply_replica_capacity(key, target)?;
                    Ok(())
                },
            )
            .await
            .context("apply committed local replica capacity")?;
        if let Some(file) = open_file {
            file.serve_capacity(target)?;
        }
        info!(
            target: "mantissa::volumes::replicated",
            volume_id = %key.volume_id().as_uuid(),
            generation = key.generation().get(),
            capacity_bytes = target.bytes(),
            "applied expanded replicated-volume capacity locally"
        );
        Ok(())
    }

    /// Reconciles every durable attachment from current control state and local resources.
    pub(crate) async fn reconcile_local_attachments(&self) -> Result<()> {
        let _lifecycle = self.driver_lifecycle.read().await;
        if self.is_stopping() {
            return Ok(());
        }
        let mut failed = 0_usize;
        let mut first_failure = None;
        for saved in self
            .replicas
            .discover_attachments(self.max_saved_replicas)?
        {
            let key = saved.key();
            if let Err(error) = self.reconcile_local_attachment(saved).await {
                failed += 1;
                if first_failure.is_none() {
                    first_failure = Some((key, error));
                }
            }
        }
        if let Some((key, error)) = first_failure {
            anyhow::bail!(
                "{failed} local attachment reconciliation attempt(s) remain pending; first \
                 failure for {key:?}: {error:#}"
            );
        }
        Ok(())
    }

    /// Reconciles one saved attachment without coupling its failure to later entries.
    async fn reconcile_local_attachment(&self, saved: LocalAttachmentRecord) -> Result<()> {
        let key = saved.key();
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if self.is_stopping() {
            return Ok(());
        }
        if !self.generation_is_desired(key) {
            self.quarantine_deleted_local(key).await;
            return self.cleanup_attachment(key).await;
        }
        if saved.is_detaching() {
            return match self.attempt_attachment_detach(key).await? {
                AttachmentDetachProgress::Complete => Ok(()),
                AttachmentDetachProgress::FencePending(error) => {
                    Err(error.context("detached writer fence remains pending"))
                }
            };
        }
        let Some(state) = self.applied_state(key)? else {
            debug!(
                target: "volumes",
                ?key,
                "local attachment waits for applied state recovery"
            );
            return Ok(());
        };
        let current = state.data().and_then(|data| data.writer);
        let exact_writer = current
            .filter(|writer| {
                writer.node_id == self.volume_node_id && writer.session_id == saved.session_id()
            })
            .and_then(|writer| state.data().map(|data| (writer, data.fence)));
        if let Some((_writer, fence)) = exact_writer {
            if saved.granted_fence().is_none() {
                self.replicas
                    .save_attachment_fence(key, saved.session_id(), fence)?;
                return Ok(());
            }
            if saved.granted_fence() != Some(fence) {
                if saved.granted_fence().is_some_and(|saved| saved < fence) {
                    let record = self
                        .replicas
                        .replica(key)?
                        .context("saved attachment has no local replica")?;
                    self.advance_local_driver_fence(&record, &saved, &state, fence)
                        .await?;
                    return Ok(());
                }
                // A mounted filesystem may still be owned by a workload.
                // Applied state already fences this older path; the
                // workload manager must stop its consumer before calling
                // unmount and advancing physical cleanup.
                if saved
                    .volume_mount()
                    .is_none_or(|mount| mount.state() != SavedMountState::Mounted)
                {
                    self.cleanup_attachment(key).await?;
                }
                return Ok(());
            }
            let record = self
                .replicas
                .replica(key)?
                .context("saved attachment has no local replica")?;
            if saved.descriptor().capacity() < record.descriptor().capacity() {
                if !self
                    .all_active_copies_serve_capacity(&state, record.descriptor().capacity())
                    .await
                {
                    return Ok(());
                }
                self.reconcile_writer_frontend_capacity(&record, &saved, &state)
                    .await?;
                return Ok(());
            }
            if saved.descriptor().capacity() > record.descriptor().capacity() {
                anyhow::bail!("saved mapped capacity exceeds the applied replica capacity");
            }
            let serving = if let Some(driver) = self.tracked_driver(key) {
                let mut driver = driver.lock().await;
                let attachment = DriverAttachment {
                    node_id: self.volume_node_id,
                    generation: key.generation(),
                    fence,
                    session_id: saved.session_id(),
                };
                if driver.attachment() == attachment
                    && driver.is_io_paused()
                    && state.replacement().is_none()
                {
                    driver.resume_io()?;
                }
                if driver.attachment() == attachment && driver.is_available() {
                    driver.stop_retiring_path().await?;
                    driver.stop_retiring_device().await?;
                    let active_device = driver.saved_device()?;
                    let backend_path = driver.backend_path()?.to_path_buf();
                    drop(driver);
                    if saved.volume_mount().is_some() {
                        let layout = MappedVolumeLayout::new(
                            self.volume_node_id,
                            saved.descriptor(),
                            backend_path,
                        )?;
                        let mapped_volumes = self.mapped_volumes.clone();
                        self.lifecycle_calls
                            .run(
                                key,
                                "mantissa-volume-resume-saved-mapping",
                                self.operation_timeout,
                                move || mapped_volumes.resume_exact(&layout).map(drop),
                            )
                            .await
                            .context("resume exact saved mapped volume")?;
                    }
                    for stale in saved
                        .ublk_devices()
                        .iter()
                        .copied()
                        .filter(|device| device.id() != active_device.id())
                    {
                        self.remove_untracked_device(key, stale).await?;
                        self.replicas.clear_ublk_device(key, stale)?;
                    }
                    true
                } else {
                    false
                }
            } else {
                saved.ublk_devices().is_empty()
            };
            if serving {
                if let Some(volume_mount) = saved.volume_mount()
                    && volume_mount.state() != SavedMountState::Mounted
                {
                    let record = self
                        .replicas
                        .replica(key)?
                        .context("saved attachment has no local replica")?;
                    let backend_path = {
                        let driver = self
                            .tracked_driver(key)
                            .context("serving attachment lost its tracked ublk backend")?;
                        driver.lock().await.backend_path()?.to_path_buf()
                    };
                    let mapped_path = self
                        .ensure_mapped_volume_device(saved.descriptor(), backend_path)
                        .await?;
                    self.recover_saved_mount(
                        &record,
                        &saved,
                        &state,
                        volume_mount,
                        saved.descriptor(),
                        mapped_path,
                    )
                    .await?;
                }
                self.reconcile_mounted_filesystem_capacity(key, record.descriptor())
                    .await?;
                return Ok(());
            }
            if saved.volume_mount().is_none() {
                debug!(
                    target: "volumes",
                    ?key,
                    session_id = ?saved.session_id(),
                    "failed unpublished driver will be fenced and cleaned"
                );
                return match self.attempt_attachment_detach(key).await? {
                    AttachmentDetachProgress::Complete => Ok(()),
                    AttachmentDetachProgress::FencePending(error) => {
                        Err(error.context("failed unpublished writer fence remains pending"))
                    }
                };
            }
            debug!(
                target: "volumes",
                ?key,
                session_id = ?saved.session_id(),
                "failed local driver requires control-state recovery"
            );
            let current_state = match tokio::time::timeout(
                ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT,
                self.read_quorum_state(key),
            )
            .await
            {
                Ok(Ok(state)) => state,
                Err(_) => {
                    debug!(
                        target: "volumes",
                        ?key,
                        "failed writer grant confirmation timed out"
                    );
                    return Ok(());
                }
                Ok(Err(error)) => {
                    warn!(
                        target: "volumes",
                        ?key,
                        error = %error,
                        "failed writer waits for current quorum state"
                    );
                    return Ok(());
                }
            };
            let Some(current_writer) =
                current_state
                    .data()
                    .and_then(|data| data.writer)
                    .filter(|current| {
                        current.node_id == self.volume_node_id
                            && current.session_id == saved.session_id()
                    })
            else {
                return Ok(());
            };
            self.begin_recovery_for_failed_writer(key, &current_state, current_writer)
                .await?;
            // Do not tear a mounted block device out from under its
            // consumer. The workload manager observes `is_mounted=false`,
            // stops the container, and then calls `unmount_volume`, which
            // owns the safe mount-before-device cleanup order.
            if saved
                .volume_mount()
                .is_some_and(|mount| mount.state() == SavedMountState::Mounted)
            {
                return Ok(());
            }
        } else if state.replacement().is_none()
            && let Some(driver) = self.tracked_driver(key)
        {
            let driver = driver.lock().await;
            if driver.is_io_paused() {
                // Once no replacement owns the pause, release held I/O
                // through the dynamically fenced old path. Its bounded
                // failure lets the workload stop and local cleanup run.
                driver.resume_io()?;
            }
        }
        if saved
            .volume_mount()
            .is_none_or(|mount| mount.state() != SavedMountState::Mounted)
        {
            self.cleanup_attachment(key).await?;
        }
        Ok(())
    }

    /// Confirms every current data copy serves the committed capacity before frontend exposure.
    async fn all_active_copies_serve_capacity(
        &self,
        state: &VolumeControlState,
        target: VolumeCapacity,
    ) -> bool {
        let Some(data) = state.data() else {
            return false;
        };
        let Some(key) = state.descriptor().map(ReplicaKey::from) else {
            return false;
        };
        let checks = data
            .copies
            .iter()
            .map(|copy| self.inspect_replica_capacity_on(*copy.as_uuid(), key, target));
        let checked = futures::future::join_all(checks).await;
        let Ok(statuses) = checked.into_iter().collect::<Result<Vec<_>>>() else {
            return false;
        };
        all_copies_serve_capacity(data.copies.len(), &statuses, target)
    }

    /// Switches one attached writer to the locally applied replicated capacity.
    async fn reconcile_writer_frontend_capacity(
        &self,
        record: &ReplicaRecord,
        saved: &LocalAttachmentRecord,
        state: &VolumeControlState,
    ) -> Result<()> {
        let key = record.key();
        let current = saved.descriptor().clone();
        let target = record.descriptor().clone();
        if !current.has_same_storage_identity(&target) || current.capacity() >= target.capacity() {
            anyhow::bail!("writer frontend expansion requires one larger compatible descriptor");
        }
        let fence = saved
            .granted_fence()
            .context("writer frontend expansion has no granted fence")?;
        let attachment = DriverAttachment {
            node_id: self.volume_node_id,
            generation: key.generation(),
            fence,
            session_id: saved.session_id(),
        };
        validate_saved_writer(state, self.volume_node_id, saved.session_id(), fence)?;
        let owner = self
            .tracked_driver(key)
            .context("writer frontend expansion has no tracked driver")?;

        let active_capacity_bytes = owner.lock().await.device_capacity_bytes();
        if active_capacity_bytes == current.capacity().bytes() {
            let needs_target_path = owner.lock().await.path_capacity() < target.capacity();
            let next_settings = ReplicatedDriverSettings::new(
                self.ublk_owner_id,
                self.driver_limits.ublk_settings(&target)?,
                self.stop_io_timeout,
            );
            {
                let mut driver = owner.lock().await;
                driver.start_larger_device(next_settings)?;
                driver.finish_larger_device_start().await?;
            }
            let (old_backend, larger_backend, larger_device) = {
                let driver = owner.lock().await;
                (
                    driver.backend_path()?.to_path_buf(),
                    driver.larger_backend_path()?.to_path_buf(),
                    driver.saved_larger_device()?,
                )
            };
            self.replicas.save_ublk_device(key, larger_device)?;
            let current_layout =
                MappedVolumeLayout::new(self.volume_node_id, &current, old_backend)?;
            let larger_layout =
                MappedVolumeLayout::new(self.volume_node_id, &target, larger_backend)?;

            let mapped_volumes = self.mapped_volumes.clone();
            let current_to_suspend = current_layout.clone();
            let larger_after_suspend = larger_layout.clone();
            self.lifecycle_calls
                .run(
                    key,
                    "mantissa-volume-suspend-mapping",
                    self.operation_timeout,
                    move || {
                        mapped_volumes
                            .suspend_for_backend_switch(&current_to_suspend, &larger_after_suspend)
                            .map(drop)
                    },
                )
                .await
                .context("suspend mapped volume before backend expansion")?;

            let pause = {
                let driver = owner.lock().await;
                if driver.attachment() != attachment {
                    anyhow::bail!("writer frontend expansion found another driver attachment");
                }
                driver.io_pause()?
            };
            pause.drain_and_flush().await?;

            // The old path must be fully drained before its replacement is
            // opened. A busy path normally has writes between its stored and
            // durable counters, so preparing first would make online
            // expansion wait forever for an accidental idle instant.
            let target_path = if needs_target_path {
                Some(self.prepare_driver_path(record, attachment, state).await?)
            } else {
                None
            };
            if let Some(path) = target_path {
                let mut driver = owner.lock().await;
                if driver.path_capacity() < target.capacity() {
                    if !driver.can_install_path() {
                        anyhow::bail!("writer data path is not held for capacity expansion");
                    }
                    driver.install_path(path, target.clone(), attachment);
                }
            }
            owner.lock().await.arm_larger_device()?;

            // Device-mapper resume may issue filesystem I/O before its ioctl
            // returns. Let those requests reach the already-installed path;
            // the suspended mapping still prevents them from arriving early.
            owner.lock().await.resume_io()?;

            let mapped_volumes = self.mapped_volumes.clone();
            self.lifecycle_calls
                .run(
                    key,
                    "mantissa-volume-expand-mapping",
                    self.operation_timeout,
                    move || {
                        mapped_volumes
                            .switch_backend(&current_layout, &larger_layout)
                            .map(drop)
                    },
                )
                .await
                .context("activate larger mapped volume backend")?;
            owner.lock().await.promote_larger_device()?;
        } else if active_capacity_bytes != target.capacity().bytes() {
            anyhow::bail!(
                "tracked writer device capacity {active_capacity_bytes} does not match the saved \
                 or applied capacity"
            );
        }

        self.replicas
            .advance_attachment_capacity(key, target.clone())?;
        {
            let driver = owner.lock().await;
            if driver.is_io_paused() {
                driver.resume_io()?;
            }
        }

        let old_devices = self
            .replicas
            .attachment(key)?
            .context("expanded writer attachment disappeared")?
            .ublk_devices()
            .iter()
            .copied()
            .filter(|device| device.capacity() < target.capacity())
            .collect::<Vec<_>>();
        {
            let mut driver = owner.lock().await;
            driver.stop_retiring_device().await?;
            driver.stop_retiring_path().await?;
        }
        for device in old_devices {
            self.remove_untracked_device(key, device).await?;
            self.replicas.clear_ublk_device(key, device)?;
        }
        self.reconcile_mounted_filesystem_capacity(key, &target)
            .await?;
        info!(
            target: "mantissa::volumes::replicated",
            volume_id = %key.volume_id().as_uuid(),
            generation = key.generation().get(),
            capacity_bytes = target.capacity().bytes(),
            "expanded replicated-volume writer frontend"
        );
        Ok(())
    }

    /// Runs and records idempotent online filesystem expansion for one saved mount.
    async fn reconcile_mounted_filesystem_capacity(
        &self,
        key: ReplicaKey,
        descriptor: &VolumeDescriptor,
    ) -> Result<()> {
        let Some(mount) = self
            .replicas
            .attachment(key)?
            .and_then(|attachment| attachment.volume_mount().cloned())
        else {
            return Ok(());
        };
        let target_bytes = descriptor.capacity().bytes();
        if mount.state() == SavedMountState::Unmounting
            || mount.filesystem_expanded_to_bytes() >= target_bytes
        {
            return Ok(());
        }
        let (progress, backend_path) = {
            let driver = self
                .tracked_driver(key)
                .context("filesystem expansion has no tracked block driver")?;
            let driver = driver.lock().await;
            (driver.progress(), driver.backend_path()?.to_path_buf())
        };
        let mapped_path = MappedVolumeLayout::new(self.volume_node_id, descriptor, backend_path)?
            .expected_path()
            .to_path_buf();
        let fs = self.fs.clone();
        let resize_path = mapped_path.clone();
        let mount_path = mount.path().to_path_buf();
        let filesystem = mount.filesystem();
        self.lifecycle_calls
            .run_async(
                key,
                "mantissa-volume-expand-filesystem",
                self.operation_timeout,
                async move {
                    fs.expand(filesystem, &resize_path, &mount_path, progress)
                        .await
                },
            )
            .await
            .context("expand mounted replicated-volume filesystem")?;
        let current = self
            .replicas
            .attachment(key)?
            .and_then(|attachment| attachment.volume_mount().cloned())
            .context("filesystem expansion mount disappeared")?;
        if current.filesystem_expanded_to_bytes() < target_bytes {
            let expanded = current.with_filesystem_expanded_to(target_bytes)?;
            self.replicas
                .replace_volume_mount(key, &current, expanded)?;
        }
        Ok(())
    }

    /// Converges one adopted fence behind the same backend, mapping, and mount.
    async fn advance_local_driver_fence(
        &self,
        record: &ReplicaRecord,
        saved: &LocalAttachmentRecord,
        state: &VolumeControlState,
        next_fence: FenceEpoch,
    ) -> Result<()> {
        let key = record.key();
        let previous_fence = saved
            .granted_fence()
            .context("saved attachment has no previous writer fence")?;
        let previous = DriverAttachment {
            node_id: self.volume_node_id,
            generation: key.generation(),
            fence: previous_fence,
            session_id: saved.session_id(),
        };
        let next = DriverAttachment {
            fence: next_fence,
            ..previous
        };
        let driver = self
            .tracked_driver(key)
            .context("adopted writer fence has no tracked local driver")?;
        let mut driver = driver.lock().await;
        if driver.attachment() == previous {
            if !driver.can_install_path() {
                anyhow::bail!("adopted writer path is not held for its fence handoff");
            }
            let path = match self.prepare_driver_path(record, next, state).await {
                Ok(path) => path,
                Err(error) if error.is::<WriterReplicaRecoveryRequired>() => {
                    drop(driver);
                    let writer = state
                        .data()
                        .and_then(|data| data.writer)
                        .context("mismatched writer copies have no current writer")?;
                    let recovery = self
                        .begin_recovery_for_failed_writer(key, state, writer)
                        .await;
                    return match recovery {
                        Ok(()) => Err(error.context(
                            "writer copies differ; recovery is authorized for the next retry",
                        )),
                        Err(recovery_error) => Err(anyhow::anyhow!(
                            "writer copies differ: {error:#}; failed-writer recovery remains \
                             pending: {recovery_error:#}"
                        )),
                    };
                }
                Err(error) => return Err(error),
            };
            driver.install_path(path, record.descriptor().clone(), next);
        } else if driver.attachment() != next {
            anyhow::bail!("tracked driver differs from the adopted writer session");
        }
        self.replicas.advance_attachment_fence(
            key,
            saved.session_id(),
            previous_fence,
            next_fence,
        )?;
        if driver.is_io_paused() {
            driver.resume_io()?;
        }
        driver.stop_retiring_path().await?;
        Ok(())
    }

    /// Finishes an adopted local writer handoff without making it a Raft phase.
    async fn converge_adopted_local_writer(
        &self,
        key: ReplicaKey,
        state: &VolumeControlState,
    ) -> Result<()> {
        let _lifecycle = self.driver_lifecycle.read().await;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        let saved = self
            .replicas
            .attachment(key)?
            .context("adopted local writer has no saved attachment")?;
        let record = self
            .replicas
            .replica(key)?
            .context("adopted local writer has no local replica")?;
        let next_fence = saved_writer_fence(state, self.volume_node_id, saved.session_id())?;
        match saved.granted_fence() {
            Some(previous) if previous < next_fence => {
                self.advance_local_driver_fence(&record, &saved, state, next_fence)
                    .await
            }
            Some(current) if current == next_fence => {
                let driver = self
                    .tracked_driver(key)
                    .context("adopted local writer has no tracked driver")?;
                let mut driver = driver.lock().await;
                if driver.is_io_paused() {
                    driver.resume_io()?;
                }
                driver.stop_retiring_path().await?;
                Ok(())
            }
            _ => anyhow::bail!("adopted writer fence moved behind its local attachment"),
        }
    }

    /// Fences one failed local writer by authorizing replay from its local copy.
    async fn begin_recovery_for_failed_writer(
        &self,
        key: ReplicaKey,
        state: &VolumeControlState,
        writer: WriterGrant,
    ) -> Result<()> {
        if writer.node_id != self.volume_node_id {
            anyhow::bail!("failed local driver does not own current writer grant");
        }
        let data = state.data().context("failed writer grant has no data")?;
        if data.writer != Some(writer) || !data.copies.contains(&self.volume_node_id) {
            anyhow::bail!("failed local driver no longer matches current writer grant");
        }
        let targets = self.reachable_recovery_targets(key, state, writer).await?;
        debug!(
            target: "volumes",
            ?key,
            ?targets,
            "proposing failed-writer recovery over reachable copies"
        );
        let recovery = mantissa_volume::control_state::RecoveryGrant {
            id: mantissa_volume::RecoveryId::new(Uuid::new_v4())?,
            coordinator_node_id: self.volume_node_id,
            source_node_id: self.volume_node_id,
            target_node_ids: targets,
        };
        let response = self
            .propose_volume_command_for_reconcile(
                key,
                VolumeCommand::BeginRecovery(mantissa_volume::control_state::BeginVolumeRecovery {
                    expected: ExpectedVolumeRevision {
                        generation: key.generation(),
                        revision: state.revision(),
                    },
                    expected_writer: Some(writer),
                    replaced_recovery_id: None,
                    recovery,
                }),
            )
            .await?;
        debug!(
            target: "volumes",
            ?key,
            ?response,
            "failed-writer recovery proposal completed"
        );
        require_command_postcondition(response)
    }

    /// Selects the local writer and every copy reachable through current control state.
    async fn reachable_recovery_targets(
        &self,
        key: ReplicaKey,
        state: &VolumeControlState,
        writer: WriterGrant,
    ) -> Result<BTreeSet<mantissa_volume::VolumeNodeId>> {
        let data = state.data().context("failed writer grant has no data")?;
        let descriptor = state
            .descriptor()
            .context("failed writer grant has no descriptor")?;
        let data_fence = data.fence;
        self.replica_file(key)?;

        let mut targets = BTreeSet::from([self.volume_node_id]);
        let mut failures = Vec::new();
        for copy in data
            .copies
            .iter()
            .copied()
            .filter(|copy| *copy != self.volume_node_id)
        {
            let open = ReplicaDataConnectionOpen::new(
                descriptor.clone(),
                data.fence,
                writer.session_id,
                ReplicaDataConnectionPurpose::Data,
            );
            let reachable =
                match tokio::time::timeout(ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT, async {
                    let connection = data::connect(
                        &self.transport,
                        copy,
                        &open,
                        self.replica_data_limits,
                        self.fixed_path_settings.max_in_flight_writes(),
                    )
                    .await?;
                    FixedReplicaCopy::remote(descriptor.clone(), data_fence, connection)
                        .await
                        .map(|_| ())
                        .map_err(anyhow::Error::from)
                })
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!("copy reachability probe timed out")),
                };
            match reachable {
                Ok(()) => {
                    targets.insert(copy);
                }
                Err(error) => failures.push(format!("{copy}: {error:#}")),
            }
        }
        if targets.len() < 2 {
            anyhow::bail!(
                "failed writer has fewer than two reachable copies; {}",
                failures.join(", ")
            );
        }
        Ok(targets)
    }

    /// Fails closed from terminal desired state without changing control state.
    pub(crate) async fn quarantine_deleted_local(&self, key: ReplicaKey) {
        self.data_connections.close(key);
        if let Some(gate) = self.gates.read().get(&key) {
            gate.disable();
        }
        let driver = self.tracked_driver(key);
        if let Some(driver) = driver {
            driver.lock().await.quarantine();
        }
    }

    /// Converges one replica row to current committed disposition using local facts.
    pub(crate) async fn reconcile_local_replica(
        &self,
        key: ReplicaKey,
        wanted: VolumeDisposition,
    ) -> Result<()> {
        self.ensure_generation_desired(key)?;
        if let Some(applied_state) = self.applied_state(key)?
            && applied_state
                .data()
                .is_some_and(|data| data.copies.contains(&self.volume_node_id))
            && let Some(descriptor) = applied_state.descriptor()
            && self
                .replicas
                .replica(key)?
                .is_some_and(|record| descriptor.capacity() >= record.descriptor().capacity())
        {
            self.apply_local_replica_capacity(key, descriptor.capacity())
                .await?;
        }
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.lifecycle_calls
            .wait_for_idle(key, self.stop_io_timeout)
            .await
            .context("wait for earlier local work before disposition reconciliation")?;
        let Some(record) = self.replicas.replica(key)? else {
            return Ok(());
        };

        if record.state() == ReplicaState::Deleting {
            anyhow::bail!("local deletion is waiting for terminal desired intent");
        }
        if record.state() == ReplicaState::Retiring {
            return self.finish_retired_replica(record).await;
        }

        // Local cleanup and admission follow applied state. Requiring a
        // fresh quorum read here would couple every level pass to a possibly
        // stale leader RPC even though a stale local value can only defer the
        // requested transition, never grant it.
        let Some(applied_state) = self.applied_state(key)? else {
            // Startup or an incoming Raft activation will publish the saved
            // application state. Until then this local copy stays fenced.
            return Ok(());
        };
        if applied_state.descriptor() != Some(record.descriptor()) {
            anyhow::bail!("local replica descriptor differs from committed control state");
        }
        if applied_state.disposition() != wanted {
            // Desired state may reach a follower before the corresponding
            // Raft entry. The periodic level reconciler will retry from the
            // newly applied state without needing an error or completion
            // notification.
            return Ok(());
        }
        match wanted {
            VolumeDisposition::Live => {
                if record.state() == ReplicaState::Retained {
                    self.replicas.set_replica_state(key, ReplicaState::Ready)?;
                }
                if !self.data_connections.reopen(key) {
                    anyhow::bail!("old replica data connections are still closing");
                }
                let current = self
                    .replicas
                    .replica(key)?
                    .context("local replica disappeared while restoring")?;
                self.ensure_local_gate(&current).await
            }
            VolumeDisposition::Retained => {
                self.close_replica_io(key).await?;
                self.replicas
                    .set_replica_state(key, ReplicaState::Retained)?;
                Ok(())
            }
        }
    }

    /// Converges terminal local cleanup from irreversible desired revocation.
    pub(crate) async fn reconcile_deleted_local_replica(
        &self,
        key: ReplicaKey,
        remove_data: bool,
    ) -> Result<()> {
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.lifecycle_calls
            .wait_for_idle(key, self.stop_io_timeout)
            .await
            .context("wait for earlier local work before terminal cleanup")?;
        let Some(record) = self.replicas.replica(key)? else {
            return Ok(());
        };

        self.quarantine_deleted_local(key).await;
        if record.state() == ReplicaState::Deleting {
            return self.finish_deleted_replica(record).await;
        }
        if record.state() == ReplicaState::Retiring {
            self.finish_retired_replica(record).await?;
            self.replicas.remove_deleted_replica(key)?;
            return Ok(());
        }
        if !remove_data {
            self.close_replica_io(key).await?;
            self.replicas
                .set_replica_state(key, ReplicaState::Retained)?;
            return Ok(());
        }

        self.replicas
            .set_replica_state(key, ReplicaState::Deleting)?;
        let deleting = self
            .replicas
            .replica(key)?
            .context("deleting replica disappeared before cleanup")?;
        self.finish_deleted_replica(deleting).await
    }

    /// Serves a stream through already-published state and its local gate.
    pub(super) async fn serve_replica_data_stream(
        &self,
        peer: Uuid,
        mut stream: mantissa_net::noise::NoiseStream,
    ) -> Result<()> {
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        let open = tokio::time::timeout(
            self.handshake_timeout,
            read_connection_open(&mut stream, self.replica_data_limits),
        )
        .await
        .context("replica data connection header timed out")??
        .context("replica data connection closed before its header")?;
        let key = ReplicaKey::from(open.descriptor());
        let singleflight = self.volume_singleflight.get(key);
        let singleflight = singleflight.lock().await;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        self.ensure_generation_desired(key)?;
        let record = self
            .replicas
            .replica(key)?
            .context("local replica data has no catalog record")?;
        self.ensure_local_gate(&record).await?;
        let admission = self
            .gates
            .read()
            .get(&key)
            .cloned()
            .context("local replica data admission is unavailable")?;
        let connection = self
            .data_connections
            .register(key)
            .context("local replica data connections are closed")?;
        // Register lifecycle ownership before opening the cached file. A
        // concurrent cleanup either closes this registration and waits for the
        // handler, or makes registration fail before a stale file can reopen.
        let file = self.replica_file(key)?;
        // Cleanup holds the same short local lane through closing, file removal,
        // and forgetting the terminal stream marker. Release it only after this
        // handler is registered and owns its exact file.
        drop(singleflight);
        let serving = self.replica_data_server.serve_authorized(
            stream,
            file,
            open,
            mantissa_volume::VolumeNodeId::new(peer)?,
            admission,
        );
        tokio::pin!(serving);
        tokio::select! {
            biased;
            () = connection.stopping() => {
                Ok(())
            }
            result = &mut serving => {
                result.context("serve dynamically fenced replica data")
            }
        }
    }

    /// Returns whether initialized control state can be confirmed by a live quorum.
    pub async fn volume_is_ready(&self, key: ReplicaKey) -> Result<bool> {
        if self.is_stopping() || !self.generation_is_desired(key) {
            return Ok(false);
        }
        if self.groups.catalog().group(&key)?.is_none() {
            return Ok(false);
        }
        Ok(self.read_quorum_state(key).await?.descriptor().is_some())
    }

    /// Confirms a live quorum has committed at least one required capacity.
    pub async fn volume_is_ready_for_capacity(
        &self,
        key: ReplicaKey,
        required_capacity_bytes: u64,
    ) -> Result<bool> {
        if self.is_stopping()
            || !self.generation_is_desired(key)
            || self.groups.catalog().group(&key)?.is_none()
        {
            return Ok(false);
        }
        let state = self.read_quorum_state(key).await?;
        let Some(descriptor) = state.descriptor() else {
            return Ok(false);
        };
        let required_capacity = VolumeCapacity::new(required_capacity_bytes)?;
        if descriptor.capacity() < required_capacity
            || !self
                .all_active_copies_serve_capacity(&state, required_capacity)
                .await
        {
            return Ok(false);
        }
        let Some(saved) = self.replicas.attachment(key)? else {
            return Ok(true);
        };
        let Some(mount) = saved.volume_mount() else {
            return Ok(true);
        };
        if saved.descriptor().capacity().bytes() < required_capacity_bytes
            || mount.filesystem_expanded_to_bytes() < required_capacity_bytes
        {
            return Ok(false);
        }
        let Some(driver) = self.tracked_driver(key) else {
            return Ok(false);
        };
        Ok(driver.lock().await.device_capacity_bytes() >= required_capacity_bytes)
    }

    /// Checks that one saved attachment still owns a serving driver and kernel mount.
    pub async fn volume_is_mounted(&self, key: ReplicaKey) -> Result<bool> {
        let _lifecycle = self.driver_lifecycle.read().await;
        if self.is_stopping() {
            return Ok(false);
        }
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.volume_is_mounted_locked(key).await
    }

    /// Checks one mount while its local lifecycle facts cannot be mid-transition.
    async fn volume_is_mounted_locked(&self, key: ReplicaKey) -> Result<bool> {
        if !self.generation_is_desired(key) {
            return Ok(false);
        }
        let Some(saved) = self.replicas.attachment(key)? else {
            return Ok(false);
        };
        let Some(volume_mount) = saved.volume_mount() else {
            return Ok(false);
        };
        if volume_mount.state() != SavedMountState::Mounted
            || volume_mount.path() != self.fs.mount_path(key)
        {
            return Ok(false);
        }
        let Some(fence) = saved.granted_fence() else {
            return Ok(false);
        };
        let Some(applied_state) = self.applied_state(key)? else {
            return Ok(false);
        };
        if validate_saved_writer(
            &applied_state,
            self.volume_node_id,
            saved.session_id(),
            fence,
        )
        .is_err()
            || !self
                .gates
                .read()
                .get(&key)
                .is_some_and(|gate| gate.is_enabled())
        {
            return Ok(false);
        }
        let Some(driver) = self.tracked_driver(key) else {
            return Ok(false);
        };
        let expected = DriverAttachment {
            node_id: self.volume_node_id,
            generation: key.generation(),
            fence,
            session_id: saved.session_id(),
        };
        let driver = driver.lock().await;
        if driver.attachment() != expected || !driver.is_available() {
            return Ok(false);
        }
        let backend_path = driver.backend_path()?.to_path_buf();
        drop(driver);
        let layout =
            MappedVolumeLayout::new(self.volume_node_id, saved.descriptor(), backend_path)?;
        let mapped_volumes = self.mapped_volumes.clone();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-map-check",
                self.operation_timeout,
                move || -> Result<()> {
                    if mapped_volumes.inspect(&layout)?.is_none() {
                        anyhow::bail!("mapped volume device is absent");
                    }
                    Ok(())
                },
            )
            .await
            .context("check mounted replicated-volume mapping")?;
        let fs = self.fs.clone();
        let path = volume_mount.path().to_path_buf();
        let filesystem = volume_mount.filesystem();
        Ok(
            tokio::task::spawn_blocking(move || fs.volume_path_is_writable(&path, filesystem))
                .await
                .context("join replicated-volume mount check")??,
        )
    }

    /// Measures filesystem space only while the local mounted writer remains owned.
    pub(crate) async fn measure_writer_filesystem_space(
        &self,
        key: ReplicaKey,
    ) -> Result<WriterFilesystemSpace> {
        let _lifecycle = self.driver_lifecycle.read().await;
        if self.is_stopping() {
            anyhow::bail!("the replicated-volume runtime is stopping");
        }
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if !self.volume_is_mounted_locked(key).await? {
            anyhow::bail!("replicated volume is not mounted on this writer node");
        }
        let path = self.fs.mount_path(key);
        let space = tokio::task::spawn_blocking(move || space::measure(&path))
            .await
            .context("join replicated-volume filesystem space measurement")??;
        Ok(WriterFilesystemSpace {
            writer_node_id: self.node_id,
            space,
        })
    }

    /// Describes applied, cataloged, and driver facts for lifecycle diagnostics.
    pub async fn attachment_diagnostics(&self, key: ReplicaKey) -> Result<String> {
        let control_state = self.read_applied_state(key).await?;
        let control_summary = control_state.data().map(|data| {
            (
                control_state.revision(),
                data.fence,
                data.writer,
                data.copies.clone(),
                control_state.replacement(),
            )
        });
        let saved = self.replicas.attachment(key)?;
        let gate = self.gates.read().get(&key).cloned().map(|gate| {
            let applied = gate.applied_volume_state().load();
            (
                gate.is_enabled(),
                gate.in_flight(),
                applied.applied.index(),
                applied.data.fence,
            )
        });
        let driver = match self.tracked_driver(key) {
            Some(driver) => Some(driver.lock().await.diagnostics()),
            None => None,
        };
        Ok(format!(
            "control_state={control_summary:?}, gate={gate:?}, saved={saved:?}, driver={driver:?}"
        ))
    }

    /// Converges one writer grant, private ublk backend, mapping, and filesystem mount.
    pub async fn mount_volume(
        &self,
        key: ReplicaKey,
        ownership: crate::volumes::types::FilesystemOwnership,
        filesystem: crate::volumes::types::ReplicatedVolumeFilesystem,
    ) -> Result<PathBuf> {
        self.ensure_generation_desired(key)?;
        let _lifecycle = self.driver_lifecycle.read().await;
        if self.is_stopping() {
            anyhow::bail!("the replicated-volume runtime is stopping");
        }
        self.ensure_generation_desired(key)?;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if self.is_stopping() {
            anyhow::bail!("the replicated-volume runtime is stopping");
        }
        let record = self
            .replicas
            .replica(key)?
            .context("cannot attach without a local replica")?;
        if record.state() != ReplicaState::Ready {
            anyhow::bail!(
                "cannot attach a local replica while it is {}",
                record.state()
            );
        }

        let mut state = self.read_quorum_state(key).await?;
        validate_mount_eligibility(&record, &state, self.volume_node_id)?;
        let attachment = self.ensure_mount_attachment(&record, &mut state).await?;
        let session_id = attachment.session_id();

        let writer = WriterGrant {
            node_id: self.volume_node_id,
            session_id,
        };
        if let Some(recovery) = state
            .data()
            .and_then(|data| data.recovery.as_ref())
            .cloned()
        {
            if recovery.coordinator_node_id != self.volume_node_id
                || recovery.source_node_id != self.volume_node_id
            {
                anyhow::bail!("volume recovery is coordinated by another active copy");
            }
            self.wait_for_recovery(&state, recovery.id).await?;
            state = self.read_quorum_state(key).await?;
            if state
                .data()
                .and_then(|data| data.recovery.as_ref())
                .is_some()
            {
                anyhow::bail!("volume recovery grant changed while data was aligning");
            }
        }
        if state.replacement().is_some() {
            anyhow::bail!("volume replacement must converge before a writer is granted");
        }
        if state.data().and_then(|data| data.writer) != Some(writer) {
            if let Some(current) = state.data().and_then(|data| data.writer) {
                anyhow::bail!(
                    "volume already has writer {} with another durable session",
                    current.node_id
                );
            }
            require_command_postcondition(
                self.propose_volume_command(
                    key,
                    VolumeCommand::GrantWriter(GrantVolumeWriter {
                        expected: ExpectedVolumeRevision {
                            generation: key.generation(),
                            revision: state.revision(),
                        },
                        writer,
                    }),
                )
                .await?,
            )?;
            state = self.read_quorum_state(key).await?;
        }
        let data = state
            .data()
            .context("initialized volume has no data control state")?;
        if data.writer != Some(writer) {
            anyhow::bail!("writer grant changed while the local attachment was starting");
        }
        if attachment.granted_fence().is_none() {
            self.replicas
                .save_attachment_fence(key, session_id, data.fence)?;
        } else if attachment.granted_fence() != Some(data.fence) {
            anyhow::bail!("local writer fence handoff is still in progress");
        }
        let attachment = self
            .replicas
            .attachment(key)?
            .context("saved attachment disappeared after its writer grant")?;
        let backend_path = match self.ensure_ublk_backend(&record, &attachment, &state).await {
            Ok(backend_path) => backend_path,
            Err(driver_error) if writer_path_failure_requires_recovery(&driver_error) => {
                let recovery = self
                    .begin_recovery_for_failed_writer(key, &state, writer)
                    .await;
                return match recovery {
                    Ok(()) => Err(driver_error.context(
                        "writer data path failed; recovery is authorized for the next retry",
                    )),
                    Err(recovery_error) => Err(anyhow::anyhow!(
                        "writer data path failed: {driver_error:#}; failed-writer recovery remains \
                         pending: {recovery_error:#}"
                    )),
                };
            }
            Err(driver_error) => return Err(driver_error),
        };
        let mapped_path = self
            .ensure_mapped_volume_device(record.descriptor(), backend_path)
            .await?;
        self.ensure_mounted_filesystem(
            &record,
            &attachment,
            &state,
            mapped_path,
            ownership,
            volume_filesystem(filesystem),
        )
        .await
    }

    /// Removes a fenced stale attachment before allocating or reusing one mount session.
    async fn ensure_mount_attachment(
        &self,
        record: &ReplicaRecord,
        state: &mut VolumeControlState,
    ) -> Result<LocalAttachmentRecord> {
        let key = record.key();
        loop {
            let Some(saved) = self.replicas.attachment(key)? else {
                let session_id = state
                    .data()
                    .and_then(|data| data.writer)
                    .filter(|writer| writer.node_id == self.volume_node_id)
                    .map_or_else(
                        || mantissa_volume::DriverSessionId::new(Uuid::new_v4()),
                        |writer| Ok(writer.session_id),
                    )?;
                return self
                    .replicas
                    .ensure_attachment(record.descriptor().clone(), session_id)
                    .map_err(Into::into);
            };
            if saved.is_detaching() {
                anyhow::bail!("the saved volume attachment is being removed");
            }
            let data = state
                .data()
                .context("initialized volume has no data control state")?;
            match saved_attachment_mount_action(
                self.volume_node_id,
                saved.session_id(),
                saved.granted_fence(),
                saved.volume_mount().map(SavedVolumeMount::state),
                data.writer,
                data.fence,
            ) {
                SavedAttachmentMountAction::Reuse => return Ok(saved),
                SavedAttachmentMountAction::WaitForFenceHandoff => {
                    anyhow::bail!("local writer fence handoff is still in progress");
                }
                SavedAttachmentMountAction::RestartConsumer => {
                    anyhow::bail!(
                        "the saved volume mount was fenced and its consumer must restart"
                    );
                }
                SavedAttachmentMountAction::Clean => {
                    self.cleanup_attachment(key).await?;
                    *state = self.read_quorum_state(key).await?;
                    validate_mount_eligibility(record, state, self.volume_node_id)?;
                }
            }
        }
    }

    /// Fences the current local writer first, then retries purely local cleanup.
    pub async fn unmount_volume(&self, key: ReplicaKey) -> Result<()> {
        let _lifecycle = self.driver_lifecycle.read().await;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        match self.attempt_attachment_detach(key).await? {
            AttachmentDetachProgress::Complete => {}
            AttachmentDetachProgress::FencePending(error) => {
                // Local admission, mount, mapping, and backend are already gone. Keep the durable
                // detaching record as the attachment reconciler's retry cursor, but do not make
                // workload deletion depend on quorum returning for an old writer that cannot
                // serve I/O.
                warn!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "local detach completed while writer fence remains pending"
                );
            }
        }
        Ok(())
    }

    /// Drives one durable detach intent through local cleanup and an independent writer fence.
    async fn attempt_attachment_detach(&self, key: ReplicaKey) -> Result<AttachmentDetachProgress> {
        self.replicas.begin_attachment_detach(key)?;
        self.quarantine_driver(key);
        let fence = async {
            if !self.generation_is_desired(key) {
                return Ok(());
            }
            tokio::time::timeout(
                ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT,
                self.fence_local_writer(key),
            )
            .await
            .context("writer fence reconciliation attempt timed out")?
        };
        let (fence, cleanup) = tokio::join!(fence, self.quiesce_attachment_resources(key));
        match (fence, cleanup) {
            (Ok(()), Ok(())) => {
                self.finish_quiesced_attachment(key).await?;
                Ok(AttachmentDetachProgress::Complete)
            }
            (Err(fence), Ok(())) => {
                let retry_owned = self
                    .replicas
                    .attachment(key)?
                    .is_some_and(|saved| saved.is_detaching());
                if !retry_owned {
                    return Err(fence.context(
                        "writer fence failed without a durable local detach retry cursor",
                    ));
                }
                Ok(AttachmentDetachProgress::FencePending(fence))
            }
            (Ok(()), Err(cleanup)) => Err(cleanup),
            (Err(fence), Err(cleanup)) => Err(anyhow::anyhow!(
                "writer fence remains pending: {fence:#}; local cleanup remains pending: \
                 {cleanup:#}"
            )),
        }
    }

    /// Stops and drains one path while retaining its ublk backend for cleanup.
    async fn quiesce_driver_requests(&self, key: ReplicaKey) -> Result<()> {
        let Some(driver) = self.tracked_driver(key) else {
            return Ok(());
        };
        driver
            .lock()
            .await
            .stop_requests()
            .await
            .context("drain replicated-volume path")
    }

    /// Rejects new local device requests synchronously before any cleanup wait.
    fn quarantine_driver(&self, key: ReplicaKey) {
        let quarantine = self
            .drivers
            .lock()
            .get(&key)
            .map(|driver| driver.quarantine.clone());
        if let Some(quarantine) = quarantine {
            quarantine.quarantine();
        }
    }

    /// Reads control state after obtaining a live quorum and applying its current state.
    async fn read_quorum_state(&self, key: ReplicaKey) -> Result<VolumeControlState> {
        self.ensure_generation_desired(key)?;
        let record = self
            .replicas
            .replica(key)?
            .context("volume has no local replica")?;
        let group = self.activate_with_voters(key).await?;
        let leader = group.wait_for_leader(self.operation_timeout).await?;
        if leader != self.node_id {
            let observed = self
                .inspect_replica_on(leader, record.descriptor().clone())
                .await?;
            if observed.leader_node_id != Some(leader) {
                anyhow::bail!("remote volume leader changed during quorum-state read");
            }
            let applied = observed
                .applied_log_index
                .context("remote volume leader has no applied state entry")?;
            group
                .wait_for_applied(applied, self.operation_timeout)
                .await?;
            if group.metrics().leader != Some(leader) {
                anyhow::bail!("volume leader changed while applying current control state");
            }
        } else {
            group.leader_state(self.operation_timeout).await?;
        }
        let state = group.state();
        self.ensure_local_gate(&record).await?;
        Ok(state)
    }

    /// Revokes exactly this node's current writer without waiting for cleanup.
    async fn fence_local_writer(&self, key: ReplicaKey) -> Result<()> {
        if self.groups.catalog().group(&key)?.is_none() {
            return Ok(());
        }
        let state = self.read_quorum_state(key).await?;
        let Some(writer) = state.data().and_then(|data| data.writer) else {
            return Ok(());
        };
        if writer.node_id != self.volume_node_id {
            return Ok(());
        }
        require_command_postcondition(
            self.propose_volume_command(
                key,
                VolumeCommand::FenceWriter(FenceVolumeWriter {
                    expected: ExpectedVolumeRevision {
                        generation: key.generation(),
                        revision: state.revision(),
                    },
                    writer,
                }),
            )
            .await?,
        )?;
        Ok(())
    }

    /// Opens every current copy, drains older fences, and starts one data path.
    async fn prepare_driver_path(
        &self,
        record: &ReplicaRecord,
        attachment: DriverAttachment,
        state: &VolumeControlState,
    ) -> Result<FixedReplicaPath> {
        let data_state = state
            .data()
            .context("initialized volume has no data control state")?;
        if data_state.writer
            != Some(WriterGrant {
                node_id: attachment.node_id,
                session_id: attachment.session_id,
            })
            || data_state.fence != attachment.fence
        {
            anyhow::bail!("driver attachment does not match current writer grant");
        }
        let data_fence = attachment.fence;
        let mut prepared = Vec::with_capacity(data_state.copies.len());
        let mut common_progress = None;
        let mut progress_matches = true;
        let mut greatest_changed_generation = 0_u64;
        for copy in data_state.copies.iter().copied() {
            if copy == self.volume_node_id {
                debug!(
                    target: "volumes",
                    ?copy,
                    fence = attachment.fence.get(),
                    "inspecting local replicated-volume writer copy"
                );
                let file = self.replica_file(record.key())?;
                let progress = ReplicaDataProgress::from_file(file.progress());
                prepared.push((copy, PreparedDriverCopy::Local(file)));
                record_driver_copy_progress(
                    copy,
                    progress,
                    data_fence,
                    &mut common_progress,
                    &mut progress_matches,
                    &mut greatest_changed_generation,
                )?;
            } else {
                debug!(
                    target: "volumes",
                    ?copy,
                    fence = attachment.fence.get(),
                    "opening remote replicated-volume writer path"
                );
                let open = ReplicaDataConnectionOpen::new(
                    record.descriptor().clone(),
                    attachment.fence,
                    attachment.session_id,
                    ReplicaDataConnectionPurpose::Data,
                );
                let connection = tokio::time::timeout(
                    self.operation_timeout,
                    data::connect(
                        &self.transport,
                        copy,
                        &open,
                        self.replica_data_limits,
                        self.fixed_path_settings.max_in_flight_writes(),
                    ),
                )
                .await
                .with_context(|| format!("connect to writer replica {copy} timed out"))??;
                let progress = tokio::time::timeout(
                    self.operation_timeout,
                    connection.progress(record.descriptor().clone(), data_fence),
                )
                .await
                .with_context(|| format!("inspect writer replica {copy} timed out"))??;
                record_driver_copy_progress(
                    copy,
                    progress,
                    data_fence,
                    &mut common_progress,
                    &mut progress_matches,
                    &mut greatest_changed_generation,
                )?;
                prepared.push((copy, PreparedDriverCopy::Remote(connection)));
            }
        }
        let current_progress = common_progress.context("writer grant has no data copies")?;
        let install_generation = writer_fence_install_generation(
            progress_matches,
            current_progress.data_fence(),
            data_fence,
            greatest_changed_generation,
        )?;

        let mut copies = Vec::with_capacity(prepared.len());
        for (copy, prepared) in prepared {
            match prepared {
                PreparedDriverCopy::Local(file) => {
                    if let Some(changed_generation) = install_generation {
                        let gate = self
                            .gates
                            .read()
                            .get(&record.key())
                            .cloned()
                            .context("local data gate is unavailable")?;
                        let install = tokio::time::timeout(
                            self.operation_timeout,
                            gate.prepare_fence_install(attachment.fence),
                        )
                        .await
                        .with_context(|| {
                            format!("local writer fence {} did not drain", attachment.fence)
                        })??;
                        let maintenance =
                            self.replica_file_workers.maintenance(Arc::clone(&file))?;
                        tokio::time::timeout(
                            self.operation_timeout,
                            maintenance.install_fence(data_fence, changed_generation, install),
                        )
                        .await
                        .context("local fence installation timed out")??;
                    }
                    copies.push(FixedReplicaCopy::local(file));
                }
                PreparedDriverCopy::Remote(connection) => {
                    if let Some(changed_generation) = install_generation {
                        tokio::time::timeout(
                            self.operation_timeout,
                            connection.install_fence(
                                record.descriptor().clone(),
                                data_fence,
                                changed_generation,
                            ),
                        )
                        .await
                        .with_context(|| {
                            format!("install writer fence on replica {copy} timed out")
                        })?
                        .with_context(|| format!("install writer fence on replica {copy}"))?;
                    }
                    copies.push(
                        tokio::time::timeout(
                            self.operation_timeout,
                            FixedReplicaCopy::remote(
                                record.descriptor().clone(),
                                data_fence,
                                connection,
                            ),
                        )
                        .await
                        .with_context(|| format!("check writer replica {copy} timed out"))?
                        .with_context(|| format!("check writer replica {copy}"))?,
                    );
                }
            }
            debug!(
                target: "volumes",
                ?copy,
                fence = attachment.fence.get(),
                "replicated-volume writer copy is ready"
            );
        }
        FixedReplicaPath::start_copies(
            record.descriptor().clone(),
            data_fence,
            self.fixed_path_settings,
            &self.replica_file_workers,
            copies,
        )
        .context("start fixed-file replicated data path")
    }

    /// Creates or recovers the private ublk backend for one writer attachment.
    async fn ensure_ublk_backend(
        &self,
        record: &ReplicaRecord,
        saved: &LocalAttachmentRecord,
        state: &VolumeControlState,
    ) -> Result<PathBuf> {
        let fence = saved
            .granted_fence()
            .context("local attachment has no committed writer fence")?;
        let attachment = DriverAttachment {
            node_id: self.volume_node_id,
            generation: record.descriptor().generation(),
            fence,
            session_id: saved.session_id(),
        };
        if let Some(driver) = self.tracked_driver(record.key()) {
            let driver = driver.lock().await;
            if driver.attachment() != attachment || !driver.is_serving() {
                anyhow::bail!(
                    "another local driver is still owned for this volume: {}",
                    driver.diagnostics()
                );
            }
            let saved_device = driver.saved_device()?;
            let backend_path = driver.backend_path()?.to_path_buf();
            drop(driver);
            self.replicas.save_ublk_device(record.key(), saved_device)?;
            let current = self.read_quorum_state(record.key()).await?;
            validate_saved_writer(&current, self.volume_node_id, saved.session_id(), fence)?;
            return Ok(backend_path);
        }
        if !saved.ublk_devices().is_empty() {
            anyhow::bail!("saved ublk device was not recovered before attachment retry");
        }
        let path = self.prepare_driver_path(record, attachment, state).await?;
        let settings = ReplicatedDriverSettings::new(
            self.ublk_owner_id,
            self.driver_limits.ublk_settings(record.descriptor())?,
            self.stop_io_timeout,
        );
        let gate = self
            .gates
            .read()
            .get(&record.key())
            .cloned()
            .context("local data gate is unavailable")?;
        let driver = ReplicatedDriver::prepare_start(
            path,
            record.descriptor().clone(),
            gate,
            attachment,
            settings,
        );
        let driver = self.register_starting_driver(record.key(), driver).await?;
        let start = {
            let mut driver = driver.lock().await;
            driver.finish_start().await
        };
        if let Err(start_error) = start {
            let cleanup = self.stop_tracked_driver(record.key()).await;
            return match cleanup {
                Ok(()) => Err(start_error.into()),
                Err(cleanup_error) => Err(anyhow::anyhow!(
                    "ublk startup failed: {start_error}; cleanup remains pending: {cleanup_error:#}"
                )),
            };
        }
        let (device, backend_path) = {
            let driver = driver.lock().await;
            (driver.saved_device()?, driver.backend_path()?.to_path_buf())
        };
        self.replicas.save_ublk_device(record.key(), device)?;
        let current = self.read_quorum_state(record.key()).await?;
        if current.data().is_none_or(|data| {
            data.fence != fence
                || data.writer
                    != Some(WriterGrant {
                        node_id: self.volume_node_id,
                        session_id: saved.session_id(),
                    })
        }) {
            self.stop_tracked_driver(record.key()).await?;
            anyhow::bail!("writer grant changed while ublk was starting");
        }
        Ok(backend_path)
    }

    /// Ensures the deterministic dm-linear device used by the filesystem.
    async fn ensure_mapped_volume_device(
        &self,
        descriptor: &VolumeDescriptor,
        backend_path: PathBuf,
    ) -> Result<PathBuf> {
        let key = ReplicaKey::from(descriptor);
        let layout = MappedVolumeLayout::new(self.volume_node_id, descriptor, backend_path)?;
        let expected_path = layout.expected_path().to_path_buf();
        let mapped_volumes = self.mapped_volumes.clone();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-map",
                self.operation_timeout,
                move || mapped_volumes.ensure(&layout).map(drop),
            )
            .await
            .context("ensure mapped replicated-volume device")?;
        info!(
            target: "volumes",
            ?key,
            mapped_device = %expected_path.display(),
            "replicated-volume mapped device is ready"
        );
        Ok(expected_path)
    }

    /// Registers an inert driver before starting its kernel owner thread.
    async fn register_starting_driver(
        &self,
        key: ReplicaKey,
        driver: ReplicatedDriver,
    ) -> Result<Arc<tokio::sync::Mutex<ReplicatedDriver>>> {
        let driver = Arc::new(tokio::sync::Mutex::new(driver));
        let start = {
            let _desired = self.desired_generation_guard(key)?;
            let mut drivers = self.drivers.lock();
            if self.is_stopping() {
                anyhow::bail!("replicated-volume runtime is stopping");
            }
            if drivers.contains_key(&key) {
                anyhow::bail!("another local driver started for this volume");
            }
            let mut owner = driver
                .try_lock()
                .context("new driver lock is unexpectedly busy before registration")?;
            drivers.insert(
                key,
                TrackedDriver {
                    owner: Arc::clone(&driver),
                    quarantine: owner.quarantine_handle(),
                },
            );
            // No await or fallible ownership transfer may occur between the
            // registry insertion and the first possible kernel effect.
            owner.start_device_owner()
        };
        if let Err(start_error) = start {
            let cleanup = self.stop_tracked_driver(key).await;
            return match cleanup {
                Ok(()) => Err(start_error.into()),
                Err(cleanup_error) => Err(anyhow::anyhow!(
                    "ublk owner startup failed: {start_error}; cleanup remains pending: \
                     {cleanup_error:#}"
                )),
            };
        }
        Ok(driver)
    }

    /// Formats the selected filesystem and saves each mount level locally.
    async fn ensure_mounted_filesystem(
        &self,
        record: &ReplicaRecord,
        attachment: &LocalAttachmentRecord,
        state: &VolumeControlState,
        mapped_path: PathBuf,
        ownership: crate::volumes::types::FilesystemOwnership,
        filesystem: VolumeFilesystem,
    ) -> Result<PathBuf> {
        let key = record.key();
        let fence = attachment
            .granted_fence()
            .context("local attachment has no committed writer fence")?;
        validate_saved_writer(state, self.volume_node_id, attachment.session_id(), fence)?;
        let path = self.fs.mount_path(key);
        let (owner_uid, owner_gid, mode) =
            crate::volumes::permissions::resolve_filesystem_ownership(ownership);
        let requested = SavedVolumeMount::mounting(
            fence,
            attachment.session_id(),
            path.clone(),
            owner_uid,
            owner_gid,
            mode,
            filesystem,
        )?;
        let saved = match attachment.volume_mount() {
            Some(current) if same_mount(current, &requested) => {
                if current.state() == SavedMountState::Unmounting {
                    anyhow::bail!("the saved volume mount is being removed");
                }
                current.clone()
            }
            Some(_) => anyhow::bail!("another mount is already saved for this volume"),
            None => {
                self.replicas.save_volume_mount(key, requested.clone())?;
                requested
            }
        };
        self.ensure_filesystem(record, &mapped_path, filesystem)
            .await?;
        let fs = self.fs.clone();
        let mount_path = path.clone();
        let mount_device = mapped_path;
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-mount",
                self.operation_timeout,
                move || -> Result<()> {
                    fs.mount(filesystem, &mount_device, &mount_path)?;
                    crate::volumes::permissions::apply_filesystem_ownership(
                        &mount_path,
                        owner_uid,
                        owner_gid,
                        mode,
                    )
                },
            )
            .await
            .context("mount replicated-volume filesystem")?;
        if let Err(error) = self.ensure_generation_desired(key) {
            self.remove_saved_mount(key, &saved).await?;
            return Err(error);
        }
        let current = self.read_quorum_state(key).await?;
        if validate_saved_writer(
            &current,
            self.volume_node_id,
            attachment.session_id(),
            fence,
        )
        .is_err()
        {
            self.remove_saved_mount(key, &saved).await?;
            anyhow::bail!("writer grant changed while the filesystem was mounting");
        }
        if saved.state() == SavedMountState::Mounting {
            let mounted = saved.with_state(SavedMountState::Mounted)?;
            self.replicas.replace_volume_mount(key, &saved, mounted)?;
        }
        self.reconcile_mounted_filesystem_capacity(key, record.descriptor())
            .await?;
        Ok(path)
    }

    /// Creates or verifies the selected deterministic filesystem per generation.
    async fn ensure_filesystem(
        &self,
        record: &ReplicaRecord,
        mapped_path: &std::path::Path,
        filesystem: VolumeFilesystem,
    ) -> Result<()> {
        let key = record.key();
        let filesystem_id = deterministic_filesystem_id(key)?;
        let profile_hash = self.fs.format_profile_hash(filesystem);
        let prior = self
            .replicas
            .replica(key)?
            .context("local replica disappeared before filesystem format")?
            .filesystem_format();
        if let Some(saved) = prior
            && (saved.filesystem() != filesystem
                || saved.filesystem_id() != filesystem_id
                || saved.profile_hash() != profile_hash)
        {
            anyhow::bail!("saved filesystem format conflicts with deterministic settings");
        }
        let signatures = self.fs.probe(mapped_path).await?;
        if prior.is_none() && is_exact_filesystem(&signatures, filesystem, filesystem_id) {
            return Ok(());
        }
        if prior.is_none() && !signatures.is_empty() {
            anyhow::bail!("unformatted volume contains a foreign filesystem signature");
        }
        // A saved format receipt means mkfs may have stopped after writing a
        // recognizable superblock. Reformat the still-unmounted device rather
        // than accepting a filesystem whose initialization did not finish.
        let saved = prior
            .unwrap_or_else(|| SavedFilesystemFormat::new(filesystem, filesystem_id, profile_hash));
        self.replicas.save_filesystem_format(key, saved)?;
        let progress = {
            let driver = self
                .tracked_driver(key)
                .context("volume driver disappeared while formatting")?;
            driver.lock().await.progress()
        };
        let fs = self.fs.clone();
        let format_device = mapped_path.to_path_buf();
        self.lifecycle_calls
            .run_async(
                key,
                "mantissa-volume-format",
                self.operation_timeout,
                async move {
                    fs.format(
                        filesystem,
                        &format_device,
                        filesystem_id,
                        prior.is_some(),
                        progress,
                    )
                    .await
                },
            )
            .await
            .context("format deterministic replicated-volume filesystem")?;
        let flush = {
            let driver = self
                .tracked_driver(key)
                .context("volume driver disappeared after formatting")?;
            driver.lock().await.flush_handle()?
        };
        flush.run().await?;
        let signatures = self.fs.probe(mapped_path).await?;
        if !is_exact_filesystem(&signatures, filesystem, filesystem_id) {
            anyhow::bail!(
                "{} format did not create the deterministic filesystem UUID",
                filesystem.name()
            );
        }
        self.replicas.clear_filesystem_format(key, saved)?;
        Ok(())
    }

    /// Completes every local cleanup level without discarding retry ownership.
    async fn cleanup_attachment(&self, key: ReplicaKey) -> Result<()> {
        self.quiesce_attachment_resources(key).await?;
        self.finish_quiesced_attachment(key).await
    }

    /// Clears retry markers only after mount, mapping, and backend are absent.
    async fn finish_quiesced_attachment(&self, key: ReplicaKey) -> Result<()> {
        if let Some(saved) = self.replicas.attachment(key)?
            && let Some(mount) = saved.volume_mount().cloned()
        {
            if mount.state() != SavedMountState::Unmounting {
                anyhow::bail!("quiesced attachment still records a live mount state");
            }
            self.replicas.clear_volume_mount(key, &mount)?;
        }
        if let Some(saved) = self.replicas.attachment(key)? {
            for device in saved.ublk_devices().to_vec() {
                self.remove_untracked_device(key, device).await?;
                self.replicas.clear_ublk_device(key, device)?;
            }
            self.replicas.remove_attachment(key, saved.session_id())?;
        }
        Ok(())
    }

    /// Closes streams and attachment resources while retaining retry ownership.
    async fn close_replica_io(&self, key: ReplicaKey) -> Result<()> {
        if let Some(gate) = self.gates.read().get(&key) {
            gate.disable();
        }
        tokio::time::timeout(
            self.stop_io_timeout,
            self.data_connections.close_and_wait(key),
        )
        .await
        .context("replica data connection cleanup timed out")?;
        self.cleanup_attachment(key).await
    }

    /// Completes terminal physical deletion from one durable `Deleting` row.
    async fn finish_deleted_replica(&self, record: ReplicaRecord) -> Result<()> {
        let key = record.key();
        if self.maintenance.cancel_other(key, None) {
            anyhow::bail!("replica maintenance is still stopping before local deletion");
        }
        self.close_replica_io(key).await?;
        self.stop_and_remove_group(key).await?;
        self.retire_local_gate_after_local_revocation(key)?;
        self.close_replica_file(key)?;

        self.remove_local_replica_storage(record.clone(), "mantissa-volume-delete-replica")
            .await?;
        if let Some(format) = self
            .replicas
            .replica(key)?
            .and_then(|record| record.filesystem_format())
        {
            self.replicas.clear_filesystem_format(key, format)?;
        }
        self.replicas.remove_deleted_replica(key)?;
        if !self.data_connections.forget_closed(key) {
            anyhow::bail!("closed replica data connections became active during deletion");
        }
        Ok(())
    }

    /// Releases a safely excluded copy while retaining compact bootstrap suppression.
    async fn finish_retired_replica(&self, record: ReplicaRecord) -> Result<()> {
        let key = record.key();
        if self.maintenance.cancel_other(key, None) {
            anyhow::bail!("replica maintenance is still stopping before local retirement");
        }
        self.close_replica_io(key).await?;
        self.stop_and_remove_group(key).await?;
        self.retire_local_gate_after_local_revocation(key)?;
        self.close_replica_file(key)?;

        self.remove_local_replica_storage(record.clone(), "mantissa-volume-retire-replica")
            .await?;
        if let Some(format) = self
            .replicas
            .replica(key)?
            .and_then(|record| record.filesystem_format())
        {
            self.replicas.clear_filesystem_format(key, format)?;
        }
        self.replicas.remove_retired_replica(key)?;
        if !self.data_connections.forget_closed(key) {
            anyhow::bail!("retired replica data connections became active during cleanup");
        }
        Ok(())
    }

    /// Removes stopped admission state and replica files through one retryable owner.
    async fn remove_local_replica_storage(
        &self,
        record: ReplicaRecord,
        call_name: &'static str,
    ) -> Result<()> {
        let key = record.key();
        let root = record.path(self.replicas.pool_root());
        let starter = self.groups.starter().clone();
        self.lifecycle_calls
            .run(
                key,
                call_name,
                self.stop_io_timeout,
                move || -> Result<()> {
                    starter.remove_closed_storage(&record)?;
                    remove_replica_directory(&root)
                },
            )
            .await
            .context("remove stopped local replica storage")
    }

    /// Retires a stale gate after the local catalog durably records revocation.
    fn retire_local_gate_after_local_revocation(&self, key: ReplicaKey) -> Result<()> {
        let gate = if let Some(gate) = self.gates.read().get(&key).cloned() {
            gate
        } else if let Some(cell) = self.applied_volume_states.cell(key) {
            let gate = FenceAdmission::new(cell, self.volume_node_id);
            self.gates.write().insert(key, Arc::clone(&gate));
            gate
        } else {
            return Ok(());
        };
        gate.disable();
        match self
            .applied_volume_states
            .remove_locally_revoked(key, &gate)
        {
            Ok(()) => {}
            Err(AppliedVolumeStateRemovalError::NotFound) if gate.in_flight().is_empty() => {}
            Err(error) => return Err(error.into()),
        }
        let mut gates = self.gates.write();
        if gates
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, &gate))
        {
            gates.remove(&key);
        }
        Ok(())
    }

    /// Drops the cache only when no connection, driver, or worker owns the file.
    fn close_replica_file(&self, key: ReplicaKey) -> Result<()> {
        let mut files = self.replica_files.lock();
        let Some(file) = files.get(&key) else {
            return Ok(());
        };
        if Arc::strong_count(file) != 1 {
            anyhow::bail!("local replica file is still in use");
        }
        files.remove(&key);
        Ok(())
    }

    /// Stops one runtime group and removes only its now-inactive catalog row.
    async fn stop_and_remove_group(&self, key: ReplicaKey) -> Result<()> {
        match self.groups.group(&key).await {
            Ok(_) => self
                .groups
                .deactivate(&key)
                .await
                .context("stop deleted local volume group")?,
            Err(RuntimeError::GroupNotRunning) => {
                if self.groups.catalog().group(&key)?.is_some() {
                    self.groups
                        .catalog()
                        .set_activation(&key, GroupActivation::Inactive)?;
                }
            }
            Err(error) => return Err(error).context("inspect deleted local volume group"),
        }
        if self.groups.catalog().group(&key)?.is_some() {
            self.groups.remove_inactive_group(&key)?;
        }
        Ok(())
    }

    /// Removes one saved mount through the cancellation-safe filesystem tracker.
    async fn remove_saved_mount(&self, key: ReplicaKey, saved: &SavedVolumeMount) -> Result<()> {
        let unmounting = saved.with_state(SavedMountState::Unmounting)?;
        if saved.state() != SavedMountState::Unmounting {
            self.replicas
                .replace_volume_mount(key, saved, unmounting.clone())?;
        }
        let fs = self.fs.clone();
        let path = unmounting.path().to_path_buf();
        let filesystem = unmounting.filesystem();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-unmount",
                self.stop_io_timeout,
                move || -> Result<()> {
                    fs.unmount_saved(filesystem, &path)?;
                    fs.remove_mount_path(&path)?;
                    Ok(())
                },
            )
            .await
            .context("unmount replicated-volume filesystem")?;
        self.replicas.clear_volume_mount(key, &unmounting)?;
        Ok(())
    }

    /// Removes every owned mapped device before its ublk backend can stop.
    async fn remove_mapped_volume_device(&self, key: ReplicaKey) -> Result<()> {
        let mapped_volumes = self.mapped_volumes.clone();
        let node_id = self.volume_node_id;
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-unmap",
                self.stop_io_timeout,
                move || mapped_volumes.remove(node_id, key),
            )
            .await
            .context("remove mapped replicated-volume device")?;
        debug!(
            target: "volumes",
            ?key,
            "replicated-volume mapped device is absent"
        );
        Ok(())
    }

    /// Lazily detaches a mount only after local driver admission is quarantined.
    async fn detach_quarantined_mount(
        &self,
        key: ReplicaKey,
        saved: &SavedVolumeMount,
    ) -> Result<()> {
        let unmounting = saved.with_state(SavedMountState::Unmounting)?;
        if saved.state() != SavedMountState::Unmounting {
            self.replicas
                .replace_volume_mount(key, saved, unmounting.clone())?;
        }
        let fs = self.fs.clone();
        let path = unmounting.path().to_path_buf();
        let filesystem = unmounting.filesystem();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-detach",
                self.stop_io_timeout,
                move || -> Result<()> {
                    fs.detach_saved(filesystem, &path)?;
                    fs.remove_mount_path(&path)?;
                    Ok(())
                },
            )
            .await
            .context("detach fenced replicated-volume filesystem")?;
        Ok(())
    }

    /// Stops a registry-owned driver and removes it only at terminal cleanup.
    async fn stop_tracked_driver(&self, key: ReplicaKey) -> Result<()> {
        let Some(driver) = self.tracked_driver(key) else {
            return Ok(());
        };
        let (result, stopped) = {
            let mut driver = driver.lock().await;
            let result = driver.stop().await;
            (result, driver.is_stopped())
        };
        if stopped {
            let mut drivers = self.drivers.lock();
            if drivers
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(&current.owner, &driver))
            {
                drivers.remove(&key);
            }
        }
        result.context("stop replicated-volume driver")
    }

    /// Removes a saved device that has no live owner thread in this process.
    async fn remove_untracked_device(&self, key: ReplicaKey, saved: SavedUblkDevice) -> Result<()> {
        let owner = self.ublk_owner_id;
        let mapped_volumes = self.mapped_volumes.clone();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-ublk-cleanup",
                self.stop_io_timeout,
                move || -> Result<()> {
                    let system = UblkSystem::system(owner);
                    match system.device(saved.id())? {
                        None => Ok(()),
                        Some(device)
                            if mapped_volumes.backend_is_referenced(device.block_path())? =>
                        {
                            anyhow::bail!(
                                "ublk backend {} is still referenced by device-mapper",
                                saved.id()
                            )
                        }
                        Some(device) if device.state() == UblkDeviceState::Running => {
                            anyhow::bail!("untracked ublk device {} is still running", saved.id())
                        }
                        Some(_) => system.remove(saved.id()).map_err(Into::into),
                    }
                },
            )
            .await
            .context("remove stale untracked ublk device")
    }

    /// Recovers only sessions confirmed by a live quorum and leaves others inert.
    async fn recover_saved_attachments(&self) -> Result<()> {
        let attachments = self
            .replicas
            .discover_attachments(self.max_saved_replicas)?;
        let expected_mappings = attachments
            .iter()
            .filter(|saved| !saved.ublk_devices().is_empty())
            .map(LocalAttachmentRecord::key)
            .collect::<BTreeSet<_>>();
        let mapped_volumes = self.mapped_volumes.clone();
        let node_id = self.volume_node_id;
        self.lifecycle_calls
            .run_global(
                "mantissa-volume-unexpected-mapping-cleanup",
                self.stop_io_timeout,
                move || mapped_volumes.remove_unexpected(node_id, &expected_mappings),
            )
            .await
            .context("remove unexpected mapped volume devices")?;
        let saved_ids = attachments
            .iter()
            .flat_map(LocalAttachmentRecord::ublk_devices)
            .map(|device| device.id())
            .collect::<Vec<_>>();
        let owner = self.ublk_owner_id;
        let inventory =
            tokio::task::spawn_blocking(move || UblkSystem::system(owner).check_devices(saved_ids))
                .await
                .context("join startup ublk inventory")??;
        for unexpected in inventory.unexpected() {
            if unexpected.state() == UblkDeviceState::Running {
                anyhow::bail!(
                    "unexpected Mantissa ublk device {} is still running",
                    unexpected.id()
                );
            }
            let owner = self.ublk_owner_id;
            let id = unexpected.id();
            let path = unexpected.block_path().to_path_buf();
            let mapped_volumes = self.mapped_volumes.clone();
            self.lifecycle_calls
                .run_global(
                    "mantissa-volume-unexpected-ublk-cleanup",
                    self.stop_io_timeout,
                    move || -> Result<()> {
                        if mapped_volumes.backend_is_referenced(&path)? {
                            anyhow::bail!(
                                "unexpected ublk backend {id} is still referenced by device-mapper"
                            );
                        }
                        UblkSystem::system(owner).remove(id)?;
                        Ok(())
                    },
                )
                .await
                .context("remove unexpected ublk device")?;
        }
        let states = inventory
            .found()
            .iter()
            .cloned()
            .map(|device| (device.id(), device))
            .collect::<BTreeMap<_, _>>();

        for saved in attachments {
            let key = saved.key();
            if let Err(error) = self.recover_saved_attachment(saved, &states).await {
                warn!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "saved attachment remains quarantined after startup recovery"
                );
            }
            if let Err(error) = self.suspend_startup_group(key).await {
                warn!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "attachment recovery left its volume group active"
                );
            }
        }
        Ok(())
    }

    /// Recovers one saved attachment while leaving unrelated startup work independent.
    async fn recover_saved_attachment(
        &self,
        mut saved: LocalAttachmentRecord,
        states: &BTreeMap<UblkDeviceId, UblkDeviceInfo>,
    ) -> Result<()> {
        let key = saved.key();
        let Some(record) = self.replicas.replica(key)? else {
            anyhow::bail!("saved attachment has no local replica");
        };
        if !self.generation_is_desired(key) {
            self.quarantine_deleted_local(key).await;
            return self.cleanup_attachment(key).await;
        }
        if saved.is_detaching() {
            if let AttachmentDetachProgress::FencePending(error) =
                self.attempt_attachment_detach(key).await?
            {
                debug!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "saved local detach waits for quorum writer fencing"
                );
            }
            return Ok(());
        }
        let state = match self.read_quorum_state(key).await {
            Ok(state) => state,
            Err(error) => {
                warn!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "saved attachment remains inert until quorum state is known"
                );
                return Ok(());
            }
        };
        let Some(fence) = saved.granted_fence() else {
            return Ok(());
        };
        let committed_fence =
            match saved_writer_fence(&state, self.volume_node_id, saved.session_id()) {
                Ok(committed_fence) if committed_fence >= fence => committed_fence,
                _ => return self.cleanup_attachment(key).await,
            };
        if committed_fence > fence {
            self.replicas.advance_attachment_fence(
                key,
                saved.session_id(),
                fence,
                committed_fence,
            )?;
            saved = self
                .replicas
                .attachment(key)?
                .context("advanced saved attachment disappeared")?;
        }
        if validate_saved_writer(
            &state,
            self.volume_node_id,
            saved.session_id(),
            committed_fence,
        )
        .is_err()
        {
            return self.cleanup_attachment(key).await;
        }
        if saved.ublk_devices().is_empty() {
            return Ok(());
        }
        let mut candidates = Vec::new();
        for device in saved.ublk_devices().iter().copied() {
            let Some(info) = states.get(&device.id()) else {
                continue;
            };
            let descriptor = saved.descriptor().with_capacity(device.capacity())?;
            let layout =
                MappedVolumeLayout::new(self.volume_node_id, &descriptor, info.block_path())?;
            candidates.push((device, info.clone(), descriptor, layout));
        }
        let layouts = candidates
            .iter()
            .map(|(_, _, _, layout)| layout.clone())
            .collect::<Vec<_>>();
        let mapped_volumes = self.mapped_volumes.clone();
        let selected = tokio::task::spawn_blocking(move || mapped_volumes.active_layout(&layouts))
            .await
            .context("join saved mapped-volume inspection")??;
        let mapping_exists = selected.is_some();
        let saved_device = if let Some(index) = selected {
            candidates
                .get(index)
                .map(|(device, _, _, _)| *device)
                .context("mapped-volume selection exceeded saved candidates")?
        } else {
            let active = saved
                .ublk_devices()
                .iter()
                .copied()
                .filter(|device| device.capacity() == saved.descriptor().capacity())
                .collect::<Vec<_>>();
            if active.len() != 1 {
                anyhow::bail!("saved attachment does not identify one active-capacity device");
            }
            active[0]
        };
        let frontend_descriptor = saved.descriptor().with_capacity(saved_device.capacity())?;
        if frontend_descriptor.capacity() < saved.descriptor().capacity()
            || frontend_descriptor.capacity() > record.descriptor().capacity()
        {
            anyhow::bail!("active mapped capacity conflicts with the local attachment catalog");
        }
        if frontend_descriptor.capacity() > saved.descriptor().capacity() {
            self.replicas
                .advance_attachment_capacity(key, frontend_descriptor.clone())?;
            saved = self
                .replicas
                .attachment(key)?
                .context("advanced mapped attachment disappeared")?;
        }
        let path = self
            .prepare_driver_path(
                &record,
                DriverAttachment {
                    node_id: self.volume_node_id,
                    generation: key.generation(),
                    fence: committed_fence,
                    session_id: saved.session_id(),
                },
                &state,
            )
            .await?;
        let ublk = saved_device.settings(&frontend_descriptor)?;
        self.check_saved_driver_limits(ublk)?;
        let settings =
            ReplicatedDriverSettings::new(self.ublk_owner_id, ublk, self.stop_io_timeout);
        let attachment = DriverAttachment {
            node_id: self.volume_node_id,
            generation: key.generation(),
            fence: committed_fence,
            session_id: saved.session_id(),
        };
        let gate = self
            .gates
            .read()
            .get(&key)
            .cloned()
            .context("local data gate is unavailable")?;
        let recover_existing = match states.get(&saved_device.id()).map(UblkDeviceInfo::state) {
            Some(UblkDeviceState::NeedsRecovery) => true,
            Some(UblkDeviceState::Running) => {
                anyhow::bail!("saved ublk device {} is still running", saved_device.id())
            }
            Some(_) => {
                self.remove_untracked_device(key, saved_device).await?;
                false
            }
            None => false,
        };
        let driver = if recover_existing {
            ReplicatedDriver::prepare_recovery(
                saved_device.id(),
                path,
                record.descriptor().clone(),
                Arc::clone(&gate),
                attachment,
                settings,
            )
        } else {
            ReplicatedDriver::prepare_start(
                path,
                record.descriptor().clone(),
                gate,
                attachment,
                settings,
            )
        };
        let driver = self.register_starting_driver(key, driver).await?;
        let start = {
            let mut driver = driver.lock().await;
            driver.finish_start().await
        };
        if let Err(start_error) = start {
            let cleanup = self.stop_tracked_driver(key).await;
            return match cleanup {
                Ok(()) => Err(start_error.into()),
                Err(cleanup_error) => Err(anyhow::anyhow!(
                    "ublk recovery failed: {start_error}; cleanup remains pending: \
                     {cleanup_error:#}"
                )),
            };
        }
        if !recover_existing {
            let replacement = driver.lock().await.saved_device()?;
            self.replicas
                .replace_ublk_device(key, saved_device, replacement)?;
            saved = self
                .replicas
                .attachment(key)?
                .context("recovered attachment disappeared")?;
        }
        let active_device = driver.lock().await.saved_device()?;
        if frontend_descriptor.capacity() < record.descriptor().capacity() {
            let larger = saved
                .ublk_devices()
                .iter()
                .copied()
                .find(|device| device.capacity() == record.descriptor().capacity());
            if let Some(larger) = larger {
                match states.get(&larger.id()).map(UblkDeviceInfo::state) {
                    Some(UblkDeviceState::NeedsRecovery) => {
                        let ublk = larger.settings(record.descriptor())?;
                        self.check_saved_driver_limits(ublk)?;
                        let settings = ReplicatedDriverSettings::new(
                            self.ublk_owner_id,
                            ublk,
                            self.stop_io_timeout,
                        );
                        let mut owner = driver.lock().await;
                        owner.recover_larger_device(larger.id(), settings)?;
                        owner.finish_larger_device_start().await?;
                    }
                    Some(UblkDeviceState::Running) => {
                        anyhow::bail!("saved ublk device {} is still running", larger.id());
                    }
                    Some(_) | None => {
                        self.remove_untracked_device(key, larger).await?;
                        self.replicas.clear_ublk_device(key, larger)?;
                        saved = self
                            .replicas
                            .attachment(key)?
                            .context("saved attachment disappeared during device cleanup")?;
                    }
                }
            }
        } else {
            for stale in saved
                .ublk_devices()
                .iter()
                .copied()
                .filter(|device| device.id() != active_device.id())
                .collect::<Vec<_>>()
            {
                self.remove_untracked_device(key, stale).await?;
                self.replicas.clear_ublk_device(key, stale)?;
            }
            saved = self
                .replicas
                .attachment(key)?
                .context("saved attachment disappeared during old-device cleanup")?;
        }
        let backend_path = driver.lock().await.backend_path()?.to_path_buf();
        let active_layout =
            MappedVolumeLayout::new(self.volume_node_id, &frontend_descriptor, backend_path)?;
        let mapped_path = if mapping_exists {
            if frontend_descriptor.capacity() == record.descriptor().capacity() {
                let mapped_volumes = self.mapped_volumes.clone();
                let layout = active_layout.clone();
                self.lifecycle_calls
                    .run(
                        key,
                        "mantissa-volume-resume-saved-mapping",
                        self.operation_timeout,
                        move || mapped_volumes.resume_exact(&layout).map(drop),
                    )
                    .await
                    .context("resume exact saved mapped volume")?;
            }
            active_layout.expected_path().to_path_buf()
        } else {
            self.ensure_mapped_volume_device(
                &frontend_descriptor,
                active_layout.backend_path().to_path_buf(),
            )
            .await?
        };
        if let Some(volume_mount) = saved.volume_mount().cloned() {
            self.recover_saved_mount(
                &record,
                &saved,
                &state,
                &volume_mount,
                &frontend_descriptor,
                mapped_path,
            )
            .await?;
        }
        Ok(())
    }

    /// Restores one exact saved mount after its driver and writer grant are current.
    async fn recover_saved_mount(
        &self,
        record: &ReplicaRecord,
        attachment: &LocalAttachmentRecord,
        state: &VolumeControlState,
        saved: &SavedVolumeMount,
        frontend_descriptor: &VolumeDescriptor,
        mapped_path: PathBuf,
    ) -> Result<()> {
        let key = record.key();
        if saved.state() == SavedMountState::Unmounting {
            if let AttachmentDetachProgress::FencePending(error) =
                self.attempt_attachment_detach(key).await?
            {
                return Err(error.context("saved local detach waits for writer fencing"));
            }
            return Ok(());
        }
        let fence = attachment
            .granted_fence()
            .context("saved mount has no granted fence")?;
        validate_saved_writer(state, self.volume_node_id, attachment.session_id(), fence)?;
        if saved.path() != self.fs.mount_path(key) {
            anyhow::bail!("saved mount path differs from the configured deterministic path");
        }
        let filesystem = saved.filesystem();
        self.ensure_filesystem(record, &mapped_path, filesystem)
            .await?;
        let fs = self.fs.clone();
        let path = saved.path().to_path_buf();
        let owner_uid = saved.owner_uid();
        let owner_gid = saved.owner_gid();
        let mode = saved.mode();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-mount",
                self.operation_timeout,
                move || -> Result<()> {
                    fs.mount(filesystem, &mapped_path, &path)?;
                    crate::volumes::permissions::apply_filesystem_ownership(
                        &path, owner_uid, owner_gid, mode,
                    )
                },
            )
            .await
            .context("recover saved replicated-volume mount")?;
        if saved.state() == SavedMountState::Mounting {
            self.replicas.replace_volume_mount(
                key,
                saved,
                saved.with_state(SavedMountState::Mounted)?,
            )?;
        }
        self.reconcile_mounted_filesystem_capacity(key, frontend_descriptor)
            .await?;
        Ok(())
    }

    /// Refuses recovery settings that exceed this process's checked bounds.
    fn check_saved_driver_limits(
        &self,
        settings: mantissa_volume::driver::UblkSettings,
    ) -> Result<()> {
        let current = self.driver_limits.queues();
        if settings.max_request_bytes() > current.max_request_bytes
            || settings.queue_buffer_bytes() > current.memory_limit_bytes
            || settings.max_request_bytes() as usize > self.driver_limits.max_pending_buffer_bytes()
        {
            anyhow::bail!("saved ublk device exceeds current driver memory limits");
        }
        Ok(())
    }

    /// Removes mount, mapping, and backend while retaining the writer session.
    async fn quiesce_attachment_resources(&self, key: ReplicaKey) -> Result<()> {
        let saved_attachment = self.replicas.attachment(key)?;
        if saved_attachment.is_none() && self.tracked_driver(key).is_none() {
            // A volume key identifies the same mapper name on every node. Do
            // not remove a mapping without a node-local attachment or driver
            // proving that this runtime owns it. Startup inventory cleanup is
            // the separate owner for local kernel orphans.
            return Ok(());
        }
        self.quiesce_driver_requests(key).await?;
        if let Some(saved) = saved_attachment
            && let Some(volume_mount) = saved.volume_mount().cloned()
        {
            if self.tracked_driver(key).is_none() {
                let mapped_volumes = self.mapped_volumes.clone();
                let owner = self.ublk_owner_id;
                let descriptor = saved.descriptor().clone();
                let devices = saved.ublk_devices().to_vec();
                let node_id = self.volume_node_id;
                self.lifecycle_calls
                    .run(
                        key,
                        "mantissa-volume-fail-dead-mapping",
                        self.stop_io_timeout,
                        move || -> Result<()> {
                            let system = UblkSystem::system(owner);
                            let mut layouts = Vec::with_capacity(devices.len());
                            for device in devices {
                                let Some(found) = system.device(device.id())? else {
                                    continue;
                                };
                                let device_descriptor =
                                    descriptor.with_capacity(device.capacity())?;
                                layouts.push(MappedVolumeLayout::new(
                                    node_id,
                                    &device_descriptor,
                                    found.block_path(),
                                )?);
                            }
                            mapped_volumes.fail_io_for_cleanup(node_id, key, &layouts)?;
                            Ok(())
                        },
                    )
                    .await
                    .context("make dead mapped backend fail before detaching filesystem")?;
            }
            self.detach_quarantined_mount(key, &volume_mount).await?;
        }
        self.remove_mapped_volume_device(key).await?;
        self.stop_tracked_driver(key).await?;
        if let Some(saved) = self.replicas.attachment(key)? {
            for device in saved.ublk_devices().to_vec() {
                self.remove_untracked_device(key, device).await?;
                if saved.volume_mount().is_none() {
                    self.replicas.clear_ublk_device(key, device)?;
                }
            }
        }
        Ok(())
    }

    /// Stops local resources within one wall-clock budget without consuming retry state.
    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown_with_timeout(self.shutdown_timeout).await
    }

    /// Stops local resources within the caller's remaining daemon-shutdown budget.
    pub(crate) async fn shutdown_with_timeout(&self, timeout: Duration) -> Result<()> {
        self.begin_shutdown();
        tokio::time::timeout(timeout, self.shutdown_attempt())
            .await
            .context("replicated-volume shutdown attempt timed out")?
    }

    /// Advances every owned resource toward terminal shutdown in dependency order.
    async fn shutdown_attempt(&self) -> Result<()> {
        let _lifecycle = self.driver_lifecycle.write().await;
        let attachments = self
            .replicas
            .discover_attachments(self.max_saved_replicas)?;
        let attachment_cleanup = async {
            let mut stopping = FuturesUnordered::new();
            for attachment in attachments {
                let key = attachment.key();
                stopping.push(async move { (key, self.quiesce_attachment_resources(key).await) });
            }
            let mut failed = 0_usize;
            let mut first_failure = None;
            while let Some((key, result)) = stopping.next().await {
                if let Err(error) = result {
                    failed += 1;
                    if first_failure.is_none() {
                        first_failure = Some((key, error));
                    }
                }
            }
            if let Some((key, error)) = first_failure {
                anyhow::bail!(
                    "{failed} replicated-volume attachment cleanup(s) remain pending; first \
                     failure for {key:?}: {error:#}"
                );
            }
            Ok(())
        };
        let (maintenance, attachments) = tokio::join!(self.stop_maintenance(), attachment_cleanup);
        match (maintenance, attachments) {
            (Ok(()), Ok(())) => {}
            (Err(maintenance), Ok(())) => return Err(maintenance),
            (Ok(()), Err(attachments)) => return Err(attachments),
            (Err(maintenance), Err(attachments)) => {
                return Err(anyhow::anyhow!(
                    "replica maintenance remains pending: {maintenance:#}; attachment cleanup \
                     remains pending: {attachments:#}"
                ));
            }
        }
        tokio::time::timeout(
            self.shutdown_timeout,
            self.data_connections.close_all_and_wait(),
        )
        .await
        .context("replica data connection shutdown timed out")?;
        self.lifecycle_calls.stop(self.shutdown_timeout).await?;
        self.replica_file_workers
            .stop(self.shutdown_timeout)
            .await?;
        self.groups.shutdown(self.shutdown_timeout).await?;
        tokio::time::timeout(self.shutdown_timeout, self.transport.shutdown())
            .await
            .context("replicated-volume transport shutdown timed out")??;
        Ok(())
    }

    /// Opens and caches the exact local fixed file for one generation.
    fn replica_file(&self, key: ReplicaKey) -> Result<Arc<ReplicaFile>> {
        if let Some(file) = self.replica_files.lock().get(&key).cloned() {
            return Ok(file);
        }
        let record = self
            .replicas
            .replica(key)?
            .context("local replica file has no catalog record")?;
        let file = Arc::new(ReplicaFile::open(
            record.path(self.replicas.pool_root()).join("blocks"),
            record.descriptor().clone(),
            self.replica_file_settings,
        )?);
        let mut files = self.replica_files.lock();
        Ok(files
            .entry(key)
            .or_insert_with(|| Arc::clone(&file))
            .clone())
    }

    /// Creates and enables one fail-closed gate for applicable durable state.
    async fn ensure_local_gate(&self, record: &ReplicaRecord) -> Result<()> {
        let key = record.key();
        let _desired = self.desired_generation_guard(key)?;
        let Some(cell) = self.applied_volume_states.cell(key) else {
            return Ok(());
        };
        let applied = cell.load().applied.index();
        let gate = {
            let mut gates = self.gates.write();
            gates
                .entry(key)
                .or_insert_with(|| FenceAdmission::new(Arc::clone(&cell), self.volume_node_id))
                .clone()
        };
        if self.is_stopping() {
            gate.disable();
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        let applied_state = cell.load();
        let local_belongs = applied_state.data.copies.contains(&self.volume_node_id)
            || applied_state
                .replacement
                .is_some_and(|replacement| replacement.new_node_id == self.volume_node_id);
        if record.state() != ReplicaState::Ready
            || applied_state.disposition != VolumeDisposition::Live
            || !local_belongs
        {
            gate.disable();
            return Ok(());
        }
        if record.health() == ReplicaHealth::NeedsRecovery {
            gate.disable();
            anyhow::bail!("local replica remains fenced until replacement recovery completes");
        }
        let file = match self.replica_file(key) {
            Ok(file) => file,
            Err(error) => {
                gate.disable();
                if error
                    .downcast_ref::<ReplicaFileError>()
                    .is_some_and(ReplicaFileError::open_failure_requires_recovery)
                {
                    self.replicas
                        .set_replica_health(key, ReplicaHealth::NeedsRecovery)
                        .context("mark unusable local replica for recovery")?;
                }
                return Err(error);
            }
        };
        if file.needs_recovery() {
            gate.disable();
            self.replicas
                .set_replica_health(key, ReplicaHealth::NeedsRecovery)
                .context("mark uncertain local replica for recovery")?;
            anyhow::bail!("local replica file requires recovery after uncertain I/O");
        }
        gate.enable_for(applied)?;
        if self.is_stopping() {
            gate.disable();
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        Ok(())
    }

    /// Wakes the saved voters before starting the local Raft member.
    async fn activate_with_voters(&self, key: ReplicaKey) -> Result<Arc<group::RunningVolumeNode>> {
        self.ensure_generation_desired(key)?;
        let saved = self
            .groups
            .catalog()
            .group(&key)?
            .context("volume has no saved Raft group")?;
        let voters = saved
            .membership()
            .into_iter()
            .flat_map(|membership| membership.voter_ids())
            .collect();
        // A saved leader starts replication as soon as OpenRaft opens. Starting
        // it first therefore races every idle follower and produces a failed
        // replication round before the explicit wake requests arrive. The wake
        // wait is bounded: an unavailable voter cannot block local activation.
        self.wake_voters(key, &voters).await;
        Ok(self.groups.activate(&key).await?)
    }

    /// Sends one bounded start request to every saved remote voter.
    async fn wake_voters(&self, key: ReplicaKey, voters: &BTreeSet<Uuid>) {
        let remote_voters = voters
            .iter()
            .copied()
            .filter(|node| *node != self.node_id)
            .collect::<Vec<_>>();
        let wakeups = remote_voters
            .iter()
            .copied()
            .map(|voter| async move { (voter, self.transport.start_group_on(key, voter).await) });
        match tokio::time::timeout(
            PEER_WAKE_ATTEMPT_TIMEOUT,
            futures::future::join_all(wakeups),
        )
        .await
        {
            Ok(results) => {
                let mut ready_voters = 0_usize;
                let mut failed_voter_wakeups = Vec::new();
                for (voter, result) in results {
                    match result {
                        Ok(()) => ready_voters += 1,
                        Err(error) => {
                            failed_voter_wakeups.push((voter, error.to_string()));
                        }
                    }
                }
                debug!(
                    target: "mantissa::volumes::raft",
                    volume_id = %key.volume_id().as_uuid(),
                    generation = key.generation().get(),
                    local_node_id = %self.node_id,
                    remote_voter_count = remote_voters.len(),
                    ready_voter_count = ready_voters,
                    failed_voter_wakeups = ?failed_voter_wakeups,
                    "finished waking remote volume Raft members"
                );
            }
            Err(_) => {
                debug!(
                    target: "mantissa::volumes::raft",
                    volume_id = %key.volume_id().as_uuid(),
                    generation = key.generation().get(),
                    local_node_id = %self.node_id,
                    remote_voter_count = remote_voters.len(),
                    timeout_ms = PEER_WAKE_ATTEMPT_TIMEOUT.as_millis(),
                    "timed out waking remote volume Raft members"
                );
            }
        }
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
mod tests {
    use super::{
        ReplicaCapacityStatus, SavedAttachmentMountAction, WriterReplicaRecoveryRequired,
        all_copies_serve_capacity, raft_group_idle_timeout, replacement_capacity_can_converge,
        replacement_voter_hint_is_valid, saved_attachment_mount_action, stale_replacement_learners,
        validate_replacement_rollback_voters, validate_replacement_target_reset,
        writer_fence_install_generation, writer_path_failure_requires_recovery,
    };
    use mantissa_volume::catalog::SavedMountState;
    use mantissa_volume::control_state::{
        BeginReplicaReplacement, BeginVolumeRecovery, ExpectedVolumeRevision, InitializeVolume,
        RecoveryGrant, ReplacementGrant, RevokeVolumeRecovery, VolumeCommand, VolumeControlState,
        WriterGrant,
    };
    use mantissa_volume::{
        DriverSessionId, FenceEpoch, RecoveryId, ReplacementId, VolumeBlockSizes, VolumeCapacity,
        VolumeDescriptor, VolumeGeneration, VolumeId, VolumeNodeId,
    };
    use std::collections::BTreeSet;
    use std::time::Duration;
    use uuid::Uuid;

    /// Creates one valid test volume-node identity.
    fn volume_node(value: u128) -> VolumeNodeId {
        VolumeNodeId::new(Uuid::from_u128(value)).expect("non-zero test volume node")
    }

    /// Applies one command whose validity is part of this test's setup.
    fn apply(state: &VolumeControlState, command: VolumeCommand) -> VolumeControlState {
        let result = state.evaluate(&command);
        assert!(matches!(
            result.response,
            mantissa_volume::control_state::VolumeCommandResponse::Applied { .. }
        ));
        result.state
    }

    /// The mapped writer waits until every active copy serves the committed range.
    #[test]
    fn writer_capacity_requires_every_copy_to_serve() {
        let target = VolumeCapacity::new(128 << 20).expect("valid test capacity");
        let ready = ReplicaCapacityStatus {
            reserved_capacity_bytes: target.bytes(),
            prepared_capacity_bytes: target.bytes(),
            served_capacity_bytes: target.bytes(),
            healthy: true,
            reason: String::new(),
        };
        assert!(!all_copies_serve_capacity(
            3,
            &[ready.clone(), ready.clone()],
            target
        ));
        assert!(all_copies_serve_capacity(
            3,
            &[ready.clone(), ready.clone(), ready.clone()],
            target
        ));

        let mut behind = ready.clone();
        behind.served_capacity_bytes = 64 << 20;
        assert!(!all_copies_serve_capacity(
            3,
            &[ready.clone(), ready.clone(), behind],
            target
        ));
        let mut unhealthy = ready.clone();
        unhealthy.healthy = false;
        assert!(!all_copies_serve_capacity(
            3,
            &[ready.clone(), ready, unhealthy],
            target
        ));
    }

    /// A returning voter may catch up from an older capacity but never from another identity.
    #[test]
    fn replacement_voter_accepts_only_convergent_capacity() {
        let initial = VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(10)).expect("test volume ID"),
            VolumeGeneration::new(1).expect("test volume generation"),
            64 << 20,
            VolumeBlockSizes::supported(),
        )
        .expect("test initial descriptor");
        let expanded = initial
            .with_capacity(VolumeCapacity::new(128 << 20).expect("expanded test capacity"))
            .expect("compatible expanded descriptor");
        let other_generation = VolumeDescriptor::new(
            initial.volume_id(),
            VolumeGeneration::new(2).expect("other test generation"),
            expanded.capacity().bytes(),
            VolumeBlockSizes::supported(),
        )
        .expect("other generation descriptor");

        assert!(replacement_capacity_can_converge(&initial, &expanded));
        assert!(replacement_capacity_can_converge(&expanded, &expanded));
        assert!(!replacement_capacity_can_converge(&expanded, &initial));
        assert!(!replacement_capacity_can_converge(
            &initial,
            &other_generation
        ));
    }

    /// A fence handoff cannot erase evidence that selected copies diverged.
    #[test]
    fn differing_writer_progress_requires_recovery() {
        let old_fence = FenceEpoch::new(4).expect("old test fence");
        let next_fence = FenceEpoch::new(5).expect("next test fence");
        assert!(
            writer_fence_install_generation(false, old_fence, next_fence, 9)
                .expect_err("differing progress must require recovery")
                .is::<WriterReplicaRecoveryRequired>()
        );
        assert_eq!(
            writer_fence_install_generation(true, old_fence, next_fence, 9)
                .expect("matching progress may advance one fence"),
            Some(10)
        );
        assert_eq!(
            writer_fence_install_generation(true, next_fence, next_fence, 10)
                .expect("matching current progress needs no fence install"),
            None
        );
    }

    /// Mount retries clean a fenced session instead of granting it another writer fence.
    #[test]
    fn mount_retry_cleans_fenced_unmounted_attachment() {
        let local_node = volume_node(1);
        let session = DriverSessionId::new(Uuid::from_u128(2)).expect("test driver session");
        let old_fence = FenceEpoch::new(4).expect("old test fence");
        let recovery_fence = FenceEpoch::new(5).expect("recovery test fence");

        assert_eq!(
            saved_attachment_mount_action(
                local_node,
                session,
                Some(old_fence),
                None,
                None,
                recovery_fence,
            ),
            SavedAttachmentMountAction::Clean
        );
        assert_eq!(
            saved_attachment_mount_action(
                local_node,
                session,
                Some(old_fence),
                Some(SavedMountState::Mounted),
                None,
                recovery_fence,
            ),
            SavedAttachmentMountAction::RestartConsumer
        );
    }

    /// Only an exact current session may be reused, and a newer fence waits for handoff.
    #[test]
    fn mount_retry_preserves_exact_writer_handoff() {
        let local_node = volume_node(3);
        let session = DriverSessionId::new(Uuid::from_u128(4)).expect("test driver session");
        let current_fence = FenceEpoch::new(6).expect("current test fence");
        let next_fence = FenceEpoch::new(7).expect("next test fence");
        let writer = Some(WriterGrant {
            node_id: local_node,
            session_id: session,
        });

        assert_eq!(
            saved_attachment_mount_action(
                local_node,
                session,
                Some(current_fence),
                None,
                writer,
                current_fence,
            ),
            SavedAttachmentMountAction::Reuse
        );
        assert_eq!(
            saved_attachment_mount_action(
                local_node,
                session,
                Some(current_fence),
                Some(SavedMountState::Mounted),
                writer,
                next_fence,
            ),
            SavedAttachmentMountAction::WaitForFenceHandoff
        );
    }

    /// Local ownership and transport failures do not invent recovery grant.
    #[test]
    fn only_proven_copy_divergence_requests_writer_recovery() {
        let local_failure = anyhow::anyhow!("another local driver is still owned");
        assert!(!writer_path_failure_requires_recovery(&local_failure));

        let divergence = anyhow::Error::new(WriterReplicaRecoveryRequired);
        assert!(writer_path_failure_requires_recovery(&divergence));
    }

    /// An unhealthy voter can reset only under its exact inactive replacement grant.
    #[test]
    fn unhealthy_replacement_reset_requires_applied_data_exclusion() {
        let descriptor = VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(10)).expect("test volume ID"),
            VolumeGeneration::new(1).expect("test volume generation"),
            64 << 20,
            VolumeBlockSizes::supported(),
        )
        .expect("test volume descriptor");
        let initial = apply(
            &VolumeControlState::default(),
            VolumeCommand::Initialize(InitializeVolume {
                descriptor,
                initial_copies: [volume_node(1), volume_node(2), volume_node(3)]
                    .into_iter()
                    .collect(),
            }),
        );
        let recovery_id = RecoveryId::new(Uuid::from_u128(20)).expect("test recovery ID");
        let recovering = apply(
            &initial,
            VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                expected: ExpectedVolumeRevision {
                    generation: initial
                        .descriptor()
                        .expect("initialized descriptor")
                        .generation(),
                    revision: initial.revision(),
                },
                expected_writer: None,
                replaced_recovery_id: None,
                recovery: RecoveryGrant {
                    id: recovery_id,
                    coordinator_node_id: volume_node(1),
                    source_node_id: volume_node(1),
                    target_node_ids: [volume_node(1), volume_node(2)].into_iter().collect(),
                },
            }),
        );
        let degraded = apply(
            &recovering,
            VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
                expected: ExpectedVolumeRevision {
                    generation: recovering
                        .descriptor()
                        .expect("recovering descriptor")
                        .generation(),
                    revision: recovering.revision(),
                },
                recovery_id,
            }),
        );
        let replacement_id = ReplacementId::new(Uuid::from_u128(30)).expect("test replacement ID");
        let replacing = apply(
            &degraded,
            VolumeCommand::BeginReplacement(BeginReplicaReplacement {
                expected: ExpectedVolumeRevision {
                    generation: degraded
                        .descriptor()
                        .expect("degraded descriptor")
                        .generation(),
                    revision: degraded.revision(),
                },
                replacement: ReplacementGrant {
                    id: replacement_id,
                    coordinator_node_id: volume_node(1),
                    old_node_id: None,
                    new_node_id: volume_node(3),
                    source_node_id: volume_node(1),
                },
            }),
        );

        validate_replacement_target_reset(&replacing, replacement_id, volume_node(3))
            .expect("the exact excluded replacement target may reset");
        assert!(
            validate_replacement_target_reset(
                &replacing,
                ReplacementId::new(Uuid::from_u128(31)).expect("other replacement ID"),
                volume_node(3),
            )
            .is_err()
        );
        assert!(
            validate_replacement_target_reset(&replacing, replacement_id, volume_node(2)).is_err()
        );
        assert!(
            validate_replacement_target_reset(&initial, replacement_id, volume_node(3)).is_err()
        );
    }

    #[test]
    fn idle_timeout_covers_two_election_windows_and_one_wake_attempt() {
        let config = openraft::Config {
            election_timeout_max: 12_000,
            ..Default::default()
        };

        assert_eq!(raft_group_idle_timeout(&config), Duration::from_secs(26));
    }

    #[test]
    fn replacement_rollback_accepts_exact_active_data_voters() {
        let copies = [volume_node(1), volume_node(2), volume_node(3)]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let original = [Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]
            .into_iter()
            .collect();
        let survivors = [Uuid::from_u128(1), Uuid::from_u128(2)]
            .into_iter()
            .collect();

        validate_replacement_rollback_voters(&copies, &original)
            .expect("all three active data copies are valid rollback voters");
        validate_replacement_rollback_voters(&copies, &survivors)
            .expect("two selected active survivors are valid rollback voters");
    }

    /// A later replacement removes an ungranted old learner before adding its target.
    #[test]
    fn replacement_prunes_only_obsolete_non_data_learners() {
        let voters = BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]);
        let members = BTreeSet::from([
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(4),
        ]);
        let copies = BTreeSet::from([volume_node(1), volume_node(2), volume_node(4)]);

        assert_eq!(
            stale_replacement_learners(&members, &voters, &copies, Uuid::from_u128(5)),
            Vec::<Uuid>::new(),
            "the old learner is still an active data copy"
        );

        let stable_copies = BTreeSet::from([volume_node(1), volume_node(2), volume_node(3)]);
        assert_eq!(
            stale_replacement_learners(&members, &voters, &stable_copies, Uuid::from_u128(5)),
            vec![Uuid::from_u128(4)]
        );
    }

    #[test]
    fn replacement_rollback_rejects_unsafe_or_foreign_voters() {
        let copies = [volume_node(1), volume_node(2), volume_node(3)]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let one_voter = BTreeSet::from([Uuid::from_u128(1)]);
        let foreign_voter = BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(4)]);

        assert!(validate_replacement_rollback_voters(&copies, &one_voter).is_err());
        assert!(validate_replacement_rollback_voters(&copies, &foreign_voter).is_err());
    }

    /// Replacement accepts every membership shape produced by safe rollback and promotion.
    #[test]
    fn replacement_target_accepts_reachable_membership_hints() {
        let target = Uuid::from_u128(4);
        let degraded = BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2)]);
        let stable = [Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]
            .into_iter()
            .collect();
        let promoted = BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), target]);
        let joint = [
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            target,
        ]
        .into_iter()
        .collect();
        let unrelated_four = [
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(5),
        ]
        .into_iter()
        .collect();

        assert!(replacement_voter_hint_is_valid(&degraded, target));
        assert!(replacement_voter_hint_is_valid(&stable, target));
        assert!(replacement_voter_hint_is_valid(&promoted, target));
        assert!(replacement_voter_hint_is_valid(&joint, target));
        assert!(!replacement_voter_hint_is_valid(&unrelated_four, target));
        assert!(!replacement_voter_hint_is_valid(
            &BTreeSet::from([Uuid::from_u128(1), target]),
            target
        ));
        assert!(!replacement_voter_hint_is_valid(
            &BTreeSet::from([Uuid::from_u128(1)]),
            target
        ));
    }
}
