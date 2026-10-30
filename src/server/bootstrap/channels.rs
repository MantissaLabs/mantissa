use crate::cluster::ClusterViewState;
use crate::gossip::{self, Message};
use async_channel::{Receiver, Sender};

/// Gossip routes and their receiving queues, owned by bootstrap until each actor is built.
pub(super) struct RuntimeChannels {
    pub gossip: gossip::Gossip,
    pub outbound: Sender<Message>,
    pub outbound_receiver: Receiver<Message>,
    pub topology: Receiver<Message>,
    pub workloads: WorkloadChannels,
    pub networks: Receiver<Message>,
    pub secrets: SecretChannels,
    pub volumes: Receiver<Message>,
    pub scheduler_digests: Receiver<Message>,
}

/// Inbound queues consumed by the workload manager and the job, agent, and service controllers.
pub(super) struct WorkloadChannels {
    pub workloads: Receiver<Message>,
    pub jobs: Receiver<Message>,
    pub agents: Receiver<Message>,
    pub services: Receiver<Message>,
}

/// Separate queues let encrypted secrets and master-key records replicate independently.
pub(super) struct SecretChannels {
    pub secrets: Receiver<Message>,
    pub master_keys: Receiver<Message>,
}

impl RuntimeChannels {
    /// Creates bounded domain queues and gives their sender routes to gossip ingress.
    ///
    /// Receivers move into their actors so bootstrap never retains a competing consumer.
    pub fn new(channel_capacity: usize, cluster_view: ClusterViewState) -> Self {
        let capacity = channel_capacity.max(1);
        let (outbound, outbound_receiver) = async_channel::bounded(capacity);
        let (topology_tx, topology) = async_channel::bounded(capacity);
        let (workload_tx, workloads) = async_channel::bounded(capacity);
        let (job_tx, jobs) = async_channel::bounded(capacity);
        let (agent_tx, agents) = async_channel::bounded(capacity);
        let (service_tx, services) = async_channel::bounded(capacity);
        let (network_tx, networks) = async_channel::bounded(capacity);
        let (secret_tx, secrets) = async_channel::bounded(capacity);
        let (master_key_tx, master_keys) = async_channel::bounded(capacity);
        let (volume_tx, volumes) = async_channel::bounded(capacity);
        let (scheduler_digest_tx, scheduler_digests) = async_channel::bounded(capacity);

        let gossip = gossip::Gossip::new(
            gossip::Channels {
                topology_events: topology_tx,
                workload_events: workload_tx,
                job_events: job_tx,
                agent_events: agent_tx,
                service_events: service_tx,
                network_events: network_tx,
                secret_events: secret_tx,
                secret_master_key_events: master_key_tx,
                volume_events: volume_tx,
                scheduler_digest_events: scheduler_digest_tx,
                outbound_events: outbound.clone(),
            },
            cluster_view,
        );

        Self {
            gossip,
            outbound,
            outbound_receiver,
            topology,
            workloads: WorkloadChannels {
                workloads,
                jobs,
                agents,
                services,
            },
            networks,
            secrets: SecretChannels {
                secrets,
                master_keys,
            },
            volumes,
            scheduler_digests,
        }
    }
}
