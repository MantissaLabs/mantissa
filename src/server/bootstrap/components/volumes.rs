use super::{RuntimeBuilder, cluster::ClusterComponents};
use crate::config;
use crate::gossip::Message;
use crate::server::bootstrap::{BootstrapResult, replicated_volumes::ReplicatedVolumeState};
use crate::volumes::replicated::{ReplicatedVolumeController, ReplicatedVolumePlanner};
use crate::volumes::{VolumeController, VolumeReplicator};
use async_channel::Receiver;
use std::path::PathBuf;
use std::sync::Arc;

/// Local volume directory and the actors that reconcile volume specifications.
pub(super) struct VolumeComponents {
    pub local_root: PathBuf,
    pub actors: VolumeActors,
}

/// Volume replication and controllers consumed once runtime construction succeeds.
pub(in crate::server::bootstrap) struct VolumeActors {
    pub replicator: VolumeReplicator,
    pub controller: VolumeController,
    pub replicated_planner: ReplicatedVolumePlanner,
    pub replicated_controller: Option<ReplicatedVolumeController>,
}

impl RuntimeBuilder<'_> {
    /// Builds volume controllers with the same recovered membership rules used by topology.
    pub(super) fn build_volumes(
        &self,
        cluster: &ClusterComponents,
        volumes: &ReplicatedVolumeState,
        receiver: Receiver<Message>,
    ) -> BootstrapResult<VolumeComponents> {
        let volume_replicator = VolumeReplicator::new(
            cluster.registries.volumes.clone(),
            self.gossip_sender.clone(),
            receiver,
        );
        let local_volume_root = self.resolve_local_volume_root()?;
        let volume_controller = VolumeController::new(
            cluster.registries.volumes.clone(),
            self.gossip_sender.clone(),
            self.ctx.self_id,
            self.local_node_name.clone(),
            local_volume_root.clone(),
            config::local_volume_enforce_capacity(),
        );
        let replicated_volume_planner = ReplicatedVolumePlanner::new(
            cluster.registries.volumes.clone(),
            cluster.registries.peers.clone(),
            volumes.membership_change_blocker.clone(),
            cluster.registries.peers.health_monitor(),
            self.gossip_sender.clone(),
            self.ctx.self_id,
        );
        let replicated_volume_controller = volumes.runtime.as_ref().map(|runtime| {
            ReplicatedVolumeController::new(
                cluster.registries.volumes.clone(),
                cluster.registries.peers.clone(),
                volumes.membership_change_blocker.clone(),
                cluster.registries.peers.health_monitor(),
                self.gossip_sender.clone(),
                Arc::clone(runtime),
            )
        });

        Ok(VolumeComponents {
            local_root: local_volume_root,
            actors: VolumeActors {
                replicator: volume_replicator,
                controller: volume_controller,
                replicated_planner: replicated_volume_planner,
                replicated_controller: replicated_volume_controller,
            },
        })
    }

    /// Uses the caller's volume directory or resolves the daemon's configured local storage.
    fn resolve_local_volume_root(&self) -> BootstrapResult<PathBuf> {
        match &self.options.local_volume_root {
            Some(path) => Ok(path.clone()),
            None => config::local_volume_root().map_err(|error| -> Box<dyn std::error::Error> {
                Box::new(std::io::Error::other(error.to_string()))
            }),
        }
    }
}
