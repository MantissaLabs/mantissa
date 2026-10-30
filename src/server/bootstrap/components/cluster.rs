use super::RuntimeBuilder;
use crate::config;
use crate::gossip::Message;
use crate::ingress::registry::IngressPoolRegistry;
use crate::network::registry::NetworkRegistry;
use crate::registry::Registry;
use crate::runtime::oci::DockerRuntimeBackend;
use crate::runtime::set::RuntimeSet;
use crate::runtime::testing::{
    IN_MEMORY_RUNTIME_BACKEND_KIND, new_in_memory_runtime_backend,
    use_in_memory_runtime_backend_from_env,
};
use crate::scheduler::Scheduler;
use crate::scheduler::digest::{
    SchedulerDigestPublisher, SchedulerDigestRegistry, SchedulerDigestReplicator,
};
use crate::secrets::master_key::replication::SecretMasterKeyPublisher;
use crate::server::bootstrap::{
    BootstrapResult, RuntimeRegistries, replicated_volumes::ReplicatedVolumeState,
    runtime::RuntimeReplication, stores::BootstrapStores,
};
use crate::services::{ServiceReconcileTrigger, ServiceRegistry};
use crate::store::replicated::registry::{ReplicatedStoreHandles, replicated_store_registry};
use crate::sync::{SyncRunner, SyncStores};
use crate::topology::{Keys, Topology, TopologyConfig, TopologyDependencies, TopologyStorage};
use crate::volumes::replicated::ReplicatedVolumeSupport;
use crate::workload::WorkloadRegistry;
use async_channel::Receiver;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::info;

/// Recovered cluster state and shared registries needed by later subsystem constructors.
pub(super) struct ClusterComponents {
    pub topology: Topology,
    pub scheduler: Rc<Scheduler>,
    pub registries: RuntimeRegistries,
    pub replication: RuntimeReplication,
    pub master_keys: MasterKeyComponents,
    pub network_sync: NetworkSyncNotifications,
    pub service_reconcile_trigger: ServiceReconcileTrigger,
    pub runtime_set: RuntimeSet,
}

/// Master-key publication and the notification shared with replication and reconciliation.
pub(super) struct MasterKeyComponents {
    pub publisher: SecretMasterKeyPublisher,
    pub notify: Arc<Notify>,
}

/// Sync wakes network reconciliation when repaired attachment or demand records change.
pub(super) struct NetworkSyncNotifications {
    pub attachments: Arc<Notify>,
    pub demand: Arc<Notify>,
}

impl RuntimeBuilder<'_> {
    /// Restores sync indexes and topology before later controllers inspect the active view.
    ///
    /// Topology needs the master-key publisher during recovery, before secret reconciliation runs.
    pub(super) async fn build_cluster(
        &self,
        volumes: &ReplicatedVolumeState,
        topology_receiver: Receiver<Message>,
    ) -> BootstrapResult<ClusterComponents> {
        let root_schema = self
            .stores
            .restore_root_schema_state(&self.ctx.db, self.options.root_schema_override)?;
        let replicated_volume_support = match volumes.runtime.as_ref() {
            Some(storage) => storage.support(root_schema.publication_generation())?,
            None => ReplicatedVolumeSupport::stopped(
                root_schema.publication_generation(),
                root_schema.info().updated_at_unix_ms,
            ),
        };

        let runtime_health = self.options.runtime_health;
        let health_monitor = mantissa_health::HealthMonitor::new(self.ctx.self_id);
        let secret_master_key_replication_notify = Arc::new(Notify::new());
        let secret_master_key_publisher = SecretMasterKeyPublisher::new(
            self.stores.secret_master_keys.clone(),
            self.gossip_sender.clone(),
            secret_master_key_replication_notify.clone(),
            self.ctx.self_id,
            self.ctx.noise_keys.clone(),
        );
        let topology_stores =
            build_topology_stores(self.stores, secret_master_key_publisher.clone());

        let sync_stores = build_sync_stores(self.stores);
        sync_stores
            .rebuild_msts_for_root_schema_version(root_schema.supported_version())
            .await
            .map_err(|error| std::io::Error::other(format!("rebuild sync MSTs: {error}")))?;

        let attachment_sync_notify = Arc::new(Notify::new());
        let network_demand_sync_notify = Arc::new(Notify::new());
        let sync_runner = SyncRunner::new(
            sync_stores.clone(),
            root_schema,
            Some(attachment_sync_notify.clone()),
            Some(network_demand_sync_notify.clone()),
            Some(secret_master_key_replication_notify.clone()),
        );
        let sync_gc_progress = sync_runner.gc_progress();

        let network_registry = NetworkRegistry::new(
            self.stores.networks.clone(),
            self.stores.network_peers.clone(),
            self.stores.network_attachments.clone(),
        );
        let workload_registry = WorkloadRegistry::new(self.stores.workloads.clone());
        let service_registry = ServiceRegistry::new(self.stores.services.clone());
        let service_reconcile_trigger = ServiceReconcileTrigger::new();
        let ingress_pool_registry = IngressPoolRegistry::new(self.stores.ingress_pools.clone());

        let registry = Registry::new(
            self.stores.peers.clone(),
            self.stores.local_sessions.clone(),
            self.ctx.signing_key.clone(),
            self.ctx.noise_keys.clone(),
            self.ctx.self_id,
            health_monitor.clone(),
            volumes.cluster_view.clone(),
        );

        let scheduler = self.build_scheduler(registry.clone()).await?;
        let runtime_set = self.build_runtime_set().await?;
        let runtime_support = runtime_set.advertised_support();

        let topology = Topology::new(TopologyConfig {
            addr: self.ctx.listen_addr.clone(),
            gossip_receiver: topology_receiver,
            gossip_sender: self.gossip_sender.clone(),
            node: self.ctx.node.clone(),
            cluster_view: volumes.cluster_view.clone(),
            root_schema,
            stores: topology_stores,
            crypto: Keys {
                noise_public_key: self.ctx.noise_keys.public,
                signing_key: self.ctx.signing_key.clone(),
            },
            deps: TopologyDependencies {
                registry: registry.clone(),
                network_registry: network_registry.clone(),
                workload_registry,
                service_registry: service_registry.clone(),
                service_reconcile_trigger: service_reconcile_trigger.clone(),
                volume_registry: volumes.registry.clone(),
                replicated_volume_split_validator: volumes.split_validator.clone(),
                scheduler: scheduler.clone(),
                sync: sync_runner,
                health_monitor,
                runtime_health,
            },
            runtime_support,
            replicated_volume_support,
        })?;

        restore_topology_derived_state(&topology).await?;

        Ok(ClusterComponents {
            topology,
            scheduler,
            registries: RuntimeRegistries {
                peers: registry,
                services: service_registry,
                networks: network_registry,
                ingress_pools: ingress_pool_registry,
                volumes: volumes.registry.clone(),
            },
            replication: RuntimeReplication {
                stores: sync_stores,
                gc_progress: sync_gc_progress,
                cluster_view: volumes.cluster_view.clone(),
                root_schema,
            },
            master_keys: MasterKeyComponents {
                publisher: secret_master_key_publisher,
                notify: secret_master_key_replication_notify,
            },
            network_sync: NetworkSyncNotifications {
                attachments: attachment_sync_notify,
                demand: network_demand_sync_notify,
            },
            service_reconcile_trigger,
            runtime_set,
        })
    }

    /// Attaches digest replication and queues initial capacity before workload admission.
    pub(super) async fn prepare_scheduler_digest_replication(
        &self,
        scheduler: &Scheduler,
        receiver: Receiver<Message>,
    ) -> SchedulerDigestReplicator {
        let registry = SchedulerDigestRegistry::new(self.stores.scheduler_digests.clone());
        let publisher = SchedulerDigestPublisher::new(
            registry.clone(),
            self.gossip_sender.clone(),
            self.ctx.self_id,
        );
        let replicator = SchedulerDigestReplicator::new(registry.clone(), receiver);
        scheduler.set_digest_publisher(publisher);
        scheduler.set_digest_registry(registry);

        scheduler.publish_current_digest().await;

        replicator
    }

    /// Initializes local scheduler slots before topology recovery or workload admission.
    async fn build_scheduler(&self, registry: Registry) -> BootstrapResult<Rc<Scheduler>> {
        let runtime_config = config::scheduler_runtime_config();
        let scheduler = Rc::new(
            Scheduler::new(
                self.stores.scheduler_store.clone(),
                registry,
                self.ctx.self_id,
            )
            .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })?,
        );
        scheduler
            .initialize_with_node(&self.ctx.node, runtime_config)
            .await
            .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })?;

        Ok(scheduler)
    }

    /// Resolves the injected or configured runtime so topology advertises its capabilities.
    async fn build_runtime_set(&self) -> BootstrapResult<RuntimeSet> {
        if let Some(runtime_set) = &self.options.runtime_set {
            return Ok(runtime_set.clone());
        }

        if use_in_memory_runtime_backend_from_env() {
            info!(
                target: "task",
                "using in-memory container runtime from env override"
            );
            return Ok(RuntimeSet::singleton(
                IN_MEMORY_RUNTIME_BACKEND_KIND,
                new_in_memory_runtime_backend(),
            ));
        }

        let docker = DockerRuntimeBackend::new()
            .await
            .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })?;

        Ok(RuntimeSet::singleton("docker", Arc::new(docker)))
    }
}

/// Shares the durable identity and secret stores used during topology recovery.
fn build_topology_stores(
    stores: &BootstrapStores,
    secret_master_key_publisher: SecretMasterKeyPublisher,
) -> TopologyStorage {
    TopologyStorage {
        local_credential_store: stores.local_creds.clone(),
        local_sessions: stores.local_sessions.clone(),
        session_auth: stores.session_auth.clone(),
        peers: stores.peers.clone(),
        cluster_operations: stores.cluster_operations.clone(),
        cluster_view_store: stores.cluster_view.clone(),
        token_store: stores.token_store.clone(),
        secret_master_store: stores.secret_master_store.clone(),
        secret_keyring: stores.secret_keyring.clone(),
        secrets: stores.secrets.clone(),
        secret_master_keys: stores.secret_master_keys.clone(),
        secret_master_key_publisher,
    }
}

/// Gives sync every replicated domain so recovery and RPC reads use the same stores.
fn build_sync_stores(stores: &BootstrapStores) -> SyncStores {
    replicated_store_registry(ReplicatedStoreHandles {
        peers: stores.peers.clone(),
        workloads: stores.workloads.clone(),
        services: stores.services.clone(),
        jobs: stores.jobs.clone(),
        agents: stores.agents.clone(),
        secrets: stores.secrets.clone(),
        secret_master_keys: stores.secret_master_keys.clone(),
        networks: stores.networks.clone(),
        network_peers: stores.network_peers.clone(),
        network_attachments: stores.network_attachments.clone(),
        cluster_views: stores.cluster_view.cluster_view_domain_store(),
        cluster_operations: stores.cluster_operations.domain_store(),
        volumes: stores.volumes.clone(),
        volume_nodes: stores.volume_nodes.clone(),
        volume_plans: stores.volume_plans.clone(),
        volume_group_statuses: stores.volume_group_statuses.clone(),
        volume_capacity_requests: stores.volume_capacity_requests.clone(),
        scheduler_digests: stores.scheduler_digests.clone(),
        ingress_pools: stores.ingress_pools.clone(),
    })
}

/// Replays durable cluster operations before controllers act on the recovered cluster view.
async fn restore_topology_derived_state(topology: &Topology) -> BootstrapResult<()> {
    let completed_retirements = topology.publish_pending_view_retirements().await?;
    if completed_retirements > 0 {
        info!(
            target: "cluster_view",
            completed_retirements,
            "completed pending source cluster view retirements during startup"
        );
    }

    match topology.restore_cluster_names_from_operations().await {
        Ok(restored) if restored > 0 => {
            info!(
                target: "cluster_view",
                restored,
                "restored cluster lineage names from durable operation history"
            );
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(
                target: "cluster_view",
                "failed to restore cluster lineage names from operation history: {error}"
            );
        }
    }

    let replayed = topology.replay_cluster_operations_on_startup().await?;
    if replayed > 0 {
        info!(
            target: "cluster_view",
            replayed,
            "replayed pending cluster operations during startup"
        );
    }

    topology
        .reconcile_finalized_cluster_transitions_for_active_view(
            "startup finalized-row reconciliation",
        )
        .await?;

    if let Err(error) = topology.publish_local_cluster_node_count().await {
        tracing::warn!(
            target: "cluster_view",
            "failed to publish local cluster node count during startup recovery: {error}"
        );
    }

    Ok(())
}
