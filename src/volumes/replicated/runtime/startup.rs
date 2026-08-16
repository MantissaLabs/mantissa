//! Startup and recovery of the node-local replicated-volume runtime.

use super::{
    AppliedVolumeStateRegistry, Arc, AtomicBool, AuthenticatedApplication,
    AuthenticatedStreamApplication, BTreeMap, BTreeSet, ClusterViewState, Context, DataConnections,
    Duration, GroupActivation, GroupCatalog, GroupRuntime, HashSet, IncomingGroupStarter,
    IncomingVolumeGroupStarter, MaintenanceManager, MappedVolumeSystem, Mutex, OnceLock,
    PeersStore, PoolSpaceState, PreparedReplicatedVolumeHost, RECONCILE_RAFT_ATTEMPT_TIMEOUT,
    REPLICATED_VOLUME_FORMAT_VERSION, ReplicaCatalog, ReplicaDataServer, ReplicaFileWorkerPool,
    ReplicaKey, ReplicaKeyAdapter, ReplicaPool, ReplicaState, ReplicatedVolumeConfig,
    ReplicatedVolumeRuntime, ReplicatedVolumeSupport, Result, RwLock, StoragePeerDirectory,
    StorageServiceFactory, TcpListener, TcpTransportSettings, UblkOwnerId, UblkSystem, Uuid,
    UuidNodeIdAdapter, VolumeCommandAdapter, VolumeGroupStarter, VolumeMembershipChangeBlocker,
    VolumeMembershipLocks, VolumeSingleflightMap, VolumeTransport, lifecycle_calls,
    open_state_database, raft_group_idle_timeout, unix_time_ms, volume, warn,
};

impl ReplicatedVolumeRuntime {
    /// Checks root access, local tools, block devices, and the replica pool.
    pub(crate) async fn prepare_host(
        config: &ReplicatedVolumeConfig,
        node_id: Uuid,
    ) -> Result<PreparedReplicatedVolumeHost> {
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
        Ok(PreparedReplicatedVolumeHost {
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
        prepared: PreparedReplicatedVolumeHost,
        node_id: Uuid,
        noise_keys: Arc<mantissa_net::noise::NoiseKeys>,
        peers: PeersStore,
        cluster_view: ClusterViewState,
        membership_change_blocker: VolumeMembershipChangeBlocker,
    ) -> Result<Arc<Self>> {
        let PreparedReplicatedVolumeHost {
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
                && let Err(error) = self.reconcile_replica_io_gate(&record).await
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
    pub(super) async fn suspend_startup_group(&self, key: ReplicaKey) -> Result<()> {
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
}
