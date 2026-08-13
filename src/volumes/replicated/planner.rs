//! Metadata-only creation of one immutable bootstrap plan per volume generation.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use mantissa_health::HealthMonitor;
use mantissa_volume::storage_format::ReplicaSpace;
use tokio::time::interval;
use tracing::warn;
use uuid::Uuid;

use super::split_validation::VolumeMembershipChangeBlocker;
use super::storage_peer_is_ready;
use crate::gossip::Message;
use crate::registry::Registry;
use crate::volumes::registry::VolumeRegistry;
use crate::volumes::types::{
    ReplicatedVolumePlan, SavedVolumeDescriptor, VolumeDriver, VolumeEvent, VolumeSpecValue,
    compute_replicated_volume_node_score,
};

const PLAN_RECONCILE_TICK_SECS: u64 = 2;

/// Creates random genesis input only on the immutable coordinator named by desired state.
#[derive(Clone)]
pub struct ReplicatedVolumePlanner {
    registry: VolumeRegistry,
    cluster_registry: Registry,
    membership_change_blocker: VolumeMembershipChangeBlocker,
    health_monitor: Arc<HealthMonitor>,
    gossip_tx: async_channel::Sender<Message>,
    node_id: Uuid,
}

impl ReplicatedVolumePlanner {
    /// Builds the metadata planner that runs even when this node stores no replicas.
    pub(crate) fn new(
        registry: VolumeRegistry,
        cluster_registry: Registry,
        membership_change_blocker: VolumeMembershipChangeBlocker,
        health_monitor: Arc<HealthMonitor>,
        gossip_tx: async_channel::Sender<Message>,
        node_id: Uuid,
    ) -> Self {
        Self {
            registry,
            cluster_registry,
            membership_change_blocker,
            health_monitor,
            gossip_tx,
            node_id,
        }
    }

    /// Repeats level reconciliation so lost notifications and transient capacity recover.
    pub async fn run(&self) {
        let mut tick = interval(Duration::from_secs(PLAN_RECONCILE_TICK_SECS));
        loop {
            if let Err(error) = self.reconcile().await {
                warn!(target: "volumes", "failed to reconcile replicated-volume plans: {error:#}");
            }
            let observed = self.registry.change_version();
            tokio::select! {
                _ = tick.tick() => {}
                _ = self.registry.wait_for_change(observed) => {}
            }
        }
    }

    /// Creates every locally owned missing plan whose workload binding is now known.
    pub async fn reconcile(&self) -> Result<()> {
        for spec in self.registry.list_reconcilable_specs()? {
            if !matches!(spec.driver, VolumeDriver::Replicated(_))
                || spec.plan_coordinator_node_id != Some(self.node_id)
                || spec.bound_node_id.is_none()
            {
                continue;
            }
            if let Err(error) = self.reconcile_volume(&spec).await {
                warn!(
                    target: "volumes",
                    volume_id = %spec.id,
                    "replicated-volume plan remains pending: {error:#}"
                );
            }
        }
        Ok(())
    }

    /// Saves exactly one random plan after checking that none exists locally.
    async fn reconcile_volume(&self, spec: &VolumeSpecValue) -> Result<()> {
        if self.registry.get_plan(spec.id)?.is_some() {
            return Ok(());
        }
        self.membership_change_blocker.ensure_changes_allowed()?;
        let plan = self.create_plan(spec)?;
        self.registry.upsert_plan(plan.clone()).await?;
        self.gossip_tx
            .send(Message::Volume {
                id: Uuid::new_v4(),
                event: VolumeEvent::PlanUpsert(Box::new(plan)),
            })
            .await
            .map_err(|error| anyhow::anyhow!("enqueue replicated-volume plan gossip: {error}"))
    }

    /// Creates genesis from one bound desired generation and the current eligible peer set.
    fn create_plan(&self, spec: &VolumeSpecValue) -> Result<ReplicatedVolumePlan> {
        let capacity = spec
            .initial_capacity_bytes
            .context("replicated volume has no initial capacity")?;
        let workload_node_id = spec
            .bound_node_id
            .context("replicated volume has no winning workload binding")?;
        let descriptor = SavedVolumeDescriptor::for_volume(spec.id, spec.volume_epoch, capacity)?;
        let replicas = self.select_replica_nodes(spec.id, workload_node_id, capacity)?;
        Ok(ReplicatedVolumePlan::new(
            spec.id,
            spec.volume_epoch,
            Uuid::new_v4(),
            workload_node_id,
            replicas,
            descriptor,
        ))
    }

    /// Selects the bound workload node and two stable capacity-ranked peers.
    fn select_replica_nodes(
        &self,
        volume_id: Uuid,
        workload_node_id: Uuid,
        capacity: u64,
    ) -> Result<[Uuid; 3]> {
        let required_bytes = ReplicaSpace::for_capacity(capacity)?.total_bytes()?;
        let health = self.health_monitor.snapshot();
        let mut candidates = Vec::new();
        let mut workload_ready = false;
        for (node_id, peer) in self.cluster_registry.peer_values_snapshot()? {
            if !storage_peer_is_ready(node_id, &peer, &health)
                || peer.scheduling.drain_requested
                || !peer.replicated_volumes.accepts_replicas
                || peer.replicated_volumes.available_bytes < required_bytes
            {
                continue;
            }
            if node_id == workload_node_id {
                workload_ready = true;
            } else {
                candidates.push(node_id);
            }
        }
        if !workload_ready {
            anyhow::bail!("the bound workload node is not ready to store a replica");
        }
        candidates.sort_by(|left, right| {
            compute_replicated_volume_node_score(volume_id, *right)
                .cmp(&compute_replicated_volume_node_score(volume_id, *left))
                .then(left.cmp(right))
        });
        if candidates.len() < 2 {
            anyhow::bail!("replicated volume requires two additional ready storage nodes");
        }
        Ok([workload_node_id, candidates[0], candidates[1]])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ClusterViewState, RootSchemaInfo};
    use crate::runtime::types::RuntimeSupportProfile;
    use crate::store::local::LocalSessionStore;
    use crate::store::replicated::cluster_operations::ClusterOperationStore;
    use crate::store::replicated::peers::open_peers_store;
    use crate::store::replicated::volumes::{
        open_replicated_volume_capacity_request_store, open_replicated_volume_group_status_store,
        open_replicated_volume_plan_store, open_volume_node_store, open_volume_spec_store,
    };
    use crate::topology::peers::{PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue};
    use crate::volumes::replicated::{REPLICATED_VOLUME_FORMAT_VERSION, ReplicatedVolumeSupport};
    use crate::volumes::types::{
        FilesystemOwnership, ReplicatedVolumeSpec, VolumeAccessMode, VolumeBindingMode,
        VolumeReclaimPolicy, VolumeSpecDraft,
    };
    use ed25519_dalek::SigningKey;
    use mantissa_net::noise::NoiseKeys;
    use mantissa_store::uuid_key::UuidKey;

    /// Builds one active peer that can accept a replica of the test volume.
    fn ready_storage_peer(node_id: Uuid, ordinal: u16) -> PeerValue {
        PeerValue {
            address: format!("10.0.0.{ordinal}:6578"),
            hostname: format!("node-{ordinal}"),
            platform_os: "linux".to_string(),
            platform_arch: "x86_64".to_string(),
            noise_static_pub: [ordinal as u8; 32],
            signing_pub: [ordinal.saturating_add(1) as u8; 32],
            identity_sig: vec![ordinal.saturating_add(2) as u8; 64],
            wireguard: None,
            scheduling: PeerSchedulingState::schedulable_default(node_id),
            readiness: Default::default(),
            labels: PeerLabelState::default(),
            runtime_support: RuntimeSupportProfile::default(),
            replicated_volumes: ReplicatedVolumeSupport {
                address: format!("10.0.0.{ordinal}:7578"),
                format_version: REPLICATED_VOLUME_FORMAT_VERSION,
                accepts_replicas: true,
                available_bytes: u64::MAX,
                updated_at_unix_ms: 1,
                publication_generation: 1,
            },
            root_schema: RootSchemaInfo::default(),
            membership: PeerMembership::active(1),
        }
    }

    /// A failed plan notification and later rebind must not authorize another genesis creator.
    #[tokio::test]
    async fn immutable_coordinator_survives_lost_gossip_and_rebinding() {
        let dir = tempfile::tempdir().expect("create planner tempdir");
        let db = Arc::new(
            redb::Database::create(dir.path().join("planner.redb"))
                .expect("create planner database"),
        );
        let coordinator = Uuid::from_u128(1);
        let workload = Uuid::from_u128(2);
        let rebound = Uuid::from_u128(3);
        let third = Uuid::from_u128(4);
        let fourth = Uuid::from_u128(5);
        let peers = open_peers_store(db.clone(), coordinator).expect("open planner peer store");
        let specs = open_volume_spec_store(db.clone(), coordinator).expect("open planner specs");
        let nodes = open_volume_node_store(db.clone(), coordinator).expect("open planner nodes");
        let plans =
            open_replicated_volume_plan_store(db.clone(), coordinator).expect("open planner plans");
        let statuses = open_replicated_volume_group_status_store(db.clone(), coordinator)
            .expect("open planner statuses");
        let capacity_requests =
            open_replicated_volume_capacity_request_store(db.clone(), coordinator)
                .expect("open planner capacity requests");
        peers
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild planner peer store");
        specs
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild planner spec store");
        nodes
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild planner node store");
        plans
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild planner plan store");
        statuses
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild planner status store");
        capacity_requests
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild planner capacity request store");
        for (ordinal, node_id) in [workload, rebound, third, fourth].into_iter().enumerate() {
            peers
                .upsert(
                    &UuidKey::from(node_id),
                    ready_storage_peer(node_id, ordinal as u16 + 2),
                )
                .await
                .expect("save ready planner peer");
        }

        let registry = VolumeRegistry::new(specs, nodes, plans, statuses, capacity_requests);
        let noise_keys = NoiseKeys::from_private_bytes([0x31; 32]);
        let sessions =
            LocalSessionStore::open(db.clone(), &noise_keys).expect("open planner session store");
        let cluster_view = ClusterViewState::legacy_default();
        let cluster_registry = Registry::new(
            peers.clone(),
            sessions,
            SigningKey::from_bytes(&[0x32; 32]),
            Arc::new(noise_keys),
            coordinator,
            HealthMonitor::new(coordinator),
            cluster_view.clone(),
        );
        let membership_change_blocker = VolumeMembershipChangeBlocker::new(
            ClusterOperationStore::new(db, coordinator).expect("open cluster operation store"),
            ClusterViewState::legacy_default(),
        );
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: "coordinator-window".to_string(),
            driver: VolumeDriver::Replicated(ReplicatedVolumeSpec {
                ownership: FilesystemOwnership::Daemon,
                filesystem: crate::volumes::types::ReplicatedVolumeFilesystem::Ext4,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Delete,
            initial_capacity_bytes: Some(64 * 1024 * 1024),
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        spec.plan_coordinator_node_id = Some(coordinator);
        spec.move_binding(workload, "workload".to_string(), Uuid::from_u128(10))
            .expect("bind test volume");
        registry
            .upsert_spec(spec.clone())
            .await
            .expect("save bound test volume");

        let (non_owner_tx, _non_owner_rx) = async_channel::bounded(4);
        let non_owner = ReplicatedVolumePlanner::new(
            registry.clone(),
            cluster_registry.clone(),
            membership_change_blocker.clone(),
            HealthMonitor::new(rebound),
            non_owner_tx,
            rebound,
        );
        non_owner.reconcile().await.expect("reconcile non-owner");
        assert_eq!(registry.get_plan(spec.id).expect("read absent plan"), None);

        let (closed_tx, closed_rx) = async_channel::bounded(1);
        drop(closed_rx);
        let owner = ReplicatedVolumePlanner::new(
            registry.clone(),
            cluster_registry,
            membership_change_blocker,
            HealthMonitor::new(coordinator),
            closed_tx,
            coordinator,
        );
        owner
            .reconcile()
            .await
            .expect("persist plan despite lost gossip acceleration");
        let saved = registry
            .get_plan(spec.id)
            .expect("read saved plan")
            .expect("coordinator saved plan before gossip");
        assert_eq!(saved.workload_node_id, workload);

        spec.move_binding(rebound, "rebound".to_string(), Uuid::from_u128(11))
            .expect("move workload binding");
        registry
            .upsert_spec(spec)
            .await
            .expect("save moved workload binding");
        non_owner
            .reconcile()
            .await
            .expect("reconcile rebound non-owner");
        assert_eq!(
            registry
                .get_plan(saved.volume_id)
                .expect("read stable plan"),
            Some(saved)
        );

        cluster_view.install(
            cluster_view.active_view(),
            std::collections::HashSet::from([third]),
        );
        let mut out_of_view_candidate_spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: "out-of-view-replica-candidate".to_string(),
            driver: VolumeDriver::Replicated(ReplicatedVolumeSpec {
                ownership: FilesystemOwnership::Daemon,
                filesystem: crate::volumes::types::ReplicatedVolumeFilesystem::Ext4,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Delete,
            initial_capacity_bytes: Some(64 * 1024 * 1024),
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        out_of_view_candidate_spec.plan_coordinator_node_id = Some(coordinator);
        out_of_view_candidate_spec
            .move_binding(workload, "workload".to_string(), Uuid::from_u128(12))
            .expect("bind out-of-view-candidate test volume");
        registry
            .upsert_spec(out_of_view_candidate_spec.clone())
            .await
            .expect("save out-of-view-candidate test volume");
        owner
            .reconcile()
            .await
            .expect("create plan without out-of-view candidate");
        let candidate_plan = registry
            .get_plan(out_of_view_candidate_spec.id)
            .expect("read out-of-view-candidate plan")
            .expect("out-of-view-candidate plan exists");
        assert!(!candidate_plan.replica_node_ids.contains(&third));
        assert!(candidate_plan.replica_node_ids.contains(&fourth));
    }
}
