use super::components::{SecretActors, VolumeActors};
use super::runtime::RuntimeComponents;
use crate::config;
use crate::gossip::{self, DedupeStateHandle, Message};
use crate::network::gossip::NetworkGossiper;
use crate::observability::metrics::{self as runtime_metrics, MetricsSamplerInputs};
use crate::scheduler::digest::SchedulerDigestReplicator;
use crate::store::path::default_db_path;
use crate::store::replicated::gc::StoreGcRunner;
use crate::topology::Topology;
use crate::volumes::replicated::{ReplicatedVolumeController, ReplicatedVolumePlanner};
use crate::volumes::{VolumeController, VolumeReplicator};
use async_channel::Receiver;
use ed25519_dalek::SigningKey;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task::{JoinHandle, spawn_local};
use tracing::error;

/// Join handles for the background actors spawned during bootstrap.
///
/// Headless tests keep these handles so they can shut down long-running
/// controllers before restarting the same node identity from durable state.
pub struct RuntimeTaskHandles {
    tasks: Vec<JoinHandle<()>>,
    gossip_control: async_channel::Sender<gossip::OutboundControl>,
}

impl RuntimeTaskHandles {
    /// Waits for the outbound worker to attempt every workload update queued before shutdown.
    pub async fn flush_gossip(&self, timeout: Duration) -> Result<(), anyhow::Error> {
        let (sent, received) = tokio::sync::oneshot::channel();

        self.gossip_control
            .send(gossip::OutboundControl::Flush(sent))
            .await
            .map_err(|_| anyhow::anyhow!("outbound gossip worker is unavailable"))?;

        tokio::time::timeout(timeout, received)
            .await
            .map_err(|_| anyhow::anyhow!("timed out while attempting final gossip updates"))?
            .map_err(|_| anyhow::anyhow!("outbound gossip worker stopped before final updates"))
    }

    /// Abort every runtime actor that bootstrap spawned.
    ///
    /// Headless restart tests use this after graceful subsystem cleanup so
    /// lingering controllers do not keep sockets, timers, or store watchers
    /// alive across the simulated daemon restart.
    pub fn abort(self) {
        for task in self.tasks {
            task.abort();
        }
    }

    /// Abort every runtime actor and wait until each task has fully stopped.
    ///
    /// Restart tests reuse the same durable state immediately after shutdown.
    /// Waiting for the cancelled tasks closes the window where a previous
    /// controller can still be unwinding while the replacement runtime starts
    /// reconciling the same local networking resources.
    pub async fn abort_and_wait(self) {
        for task in self.tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

/// Outbound gossip shares deduplication state with the ingress RPC capability.
pub(super) struct OutboundGossip {
    pub receiver: Receiver<Message>,
    pub dedupe: DedupeStateHandle,
    pub fanout: usize,
}

/// Constructed background work consumed when bootstrap starts the local runtime.
pub(super) struct RuntimeActors {
    pub secrets: SecretActors,
    pub volumes: VolumeActors,
    pub scheduler_digest_replicator: SchedulerDigestReplicator,
    pub network_gossiper: NetworkGossiper,
    pub gossip: OutboundGossip,
    pub store_gc: StoreGcRunner,
    pub signing_key: SigningKey,
}

/// Starts the prepared actors and retains their handles for daemon and headless shutdown.
pub(super) async fn spawn_runtime_tasks(
    components: &RuntimeComponents,
    actors: RuntimeActors,
) -> RuntimeTaskHandles {
    let VolumeActors {
        replicator: volume_replicator,
        controller: volume_controller,
        replicated_planner,
        replicated_controller,
    } = actors.volumes;
    let (gossip_control, gossip_controls) = async_channel::bounded(1);
    let mut tasks = Vec::new();

    tasks.extend(spawn_metrics(components));
    spawn_workload_controllers(components, &mut tasks);
    spawn_replication_tasks(
        actors.secrets,
        volume_replicator,
        actors.scheduler_digest_replicator,
        &mut tasks,
    );
    spawn_volume_controllers(
        volume_controller,
        replicated_planner,
        replicated_controller,
        &mut tasks,
    );
    tasks.push(spawn_local(async move {
        actors.network_gossiper.run().await;
    }));
    tasks.extend(actors.store_gc.spawn());

    let topology = components.controllers.topology.clone();
    let gossip_tick = topology.gossip_interval();
    tasks.push(spawn_local(async move {
        gossip::start(
            actors.gossip.receiver,
            gossip_controls,
            topology,
            actors.gossip.dedupe,
            Some(actors.gossip.fanout),
            gossip_tick,
        )
        .await;
    }));

    let topology = components.controllers.topology.clone();
    tasks.push(spawn_local(async move {
        topology.run().await;
    }));
    resume_cluster_participation(
        &components.controllers.topology,
        actors.signing_key,
        &mut tasks,
    )
    .await;
    tasks.extend(components.controllers.networks.spawn());

    RuntimeTaskHandles {
        tasks,
        gossip_control,
    }
}

/// Starts the optional metrics exporter and sampler against the runtime's shared state.
fn spawn_metrics(components: &RuntimeComponents) -> Option<JoinHandle<()>> {
    let metrics_config = match config::metrics_runtime_config() {
        Ok(config) => config,
        Err(error) => {
            error!(target: "metrics", "invalid metrics configuration: {error}");
            return None;
        }
    };
    let state_db_path = match default_db_path() {
        Ok(path) => path,
        Err(error) => {
            error!(target: "metrics", "failed to resolve state database path: {error}");
            PathBuf::from("state.redb")
        }
    };

    runtime_metrics::spawn_metrics(
        metrics_config,
        MetricsSamplerInputs {
            scheduler: components.controllers.scheduler.clone(),
            registry: components.registries.peers.clone(),
            nodeport: components.controllers.networks.nodeport_manager(),
            stores: components.replication.stores.clone(),
            progress: components.replication.gc_progress.clone(),
            cluster_view: components.replication.cluster_view.clone(),
            root_schema: components.replication.root_schema,
            state_db_path,
        },
    )
}

/// Starts workload execution before the controllers that submit work through it.
fn spawn_workload_controllers(components: &RuntimeComponents, tasks: &mut Vec<JoinHandle<()>>) {
    let mut workload_runner = components.controllers.workloads.clone();
    tasks.push(spawn_local(async move {
        workload_runner.run().await;
    }));

    let mut job_runner = components.controllers.jobs.clone();
    tasks.push(spawn_local(async move {
        job_runner.run().await;
    }));

    let mut agent_runner = components.controllers.agents.clone();
    tasks.push(spawn_local(async move {
        agent_runner.run().await;
    }));

    let mut service_runner = components.controllers.services.clone();
    tasks.push(spawn_local(async move {
        service_runner.run().await;
    }));
}

/// Starts inbound replication before gossip and topology begin contacting peers.
fn spawn_replication_tasks(
    secrets: SecretActors,
    volume_replicator: VolumeReplicator,
    scheduler_digest_replicator: SchedulerDigestReplicator,
    tasks: &mut Vec<JoinHandle<()>>,
) {
    tasks.push(spawn_local(async move {
        secrets.replicator.run().await;
    }));
    tasks.push(spawn_local(async move {
        secrets.master_key_replicator.run().await;
    }));
    tasks.push(spawn_local(async move {
        secrets
            .master_key_reconciler
            .run_on_notify(secrets.master_key_replication_notify)
            .await;
    }));
    tasks.push(spawn_local(async move {
        volume_replicator.run().await;
    }));
    tasks.push(spawn_local(async move {
        scheduler_digest_replicator.run().await;
    }));
}

/// Starts local volume reconciliation and the configured replicated-storage controllers.
fn spawn_volume_controllers(
    controller: VolumeController,
    planner: ReplicatedVolumePlanner,
    replicated_controller: Option<ReplicatedVolumeController>,
    tasks: &mut Vec<JoinHandle<()>>,
) {
    tasks.push(spawn_local(async move {
        controller.run().await;
    }));
    tasks.push(spawn_local(async move {
        planner.run().await;
    }));
    if let Some(controller) = replicated_controller {
        tasks.push(spawn_local(async move {
            controller.run().await;
        }));
    }
}

/// Resumes peer contact only for a node whose durable state records an existing join.
///
/// Topology owns its periodic loops because leave and rejoin must stop and restart them.
async fn resume_cluster_participation(
    topology: &Topology,
    signing_key: SigningKey,
    tasks: &mut Vec<JoinHandle<()>>,
) {
    if topology.already_joined().await.unwrap_or(false) {
        topology.ensure_cluster_background_tasks();

        let topology = topology.clone();
        tasks.push(spawn_local(async move {
            if let Err(error) = topology.connect_known_peers(Some(&signing_key)).await {
                error!(target: "server", "Startup connect failed: {error}");
            }
        }));
    }
}
