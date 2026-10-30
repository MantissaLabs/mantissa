use super::{RuntimeBuilder, cluster::ClusterComponents};
use crate::secrets::gossip::SecretReplicator;
use crate::secrets::master_key::reconciler::SecretMasterKeyReconciler;
use crate::secrets::master_key::replication::SecretMasterKeyReplicator;
use crate::secrets::registry::SecretRegistry;
use crate::server::bootstrap::channels::SecretChannels;
use std::sync::Arc;
use tokio::sync::Notify;

/// Secret access retained while RPC capabilities and workload controllers are constructed.
pub(super) struct SecretComponents {
    pub registry: SecretRegistry,
    pub actors: SecretActors,
}

/// Secret replication and reconciliation loops consumed when runtime tasks start.
pub(in crate::server::bootstrap) struct SecretActors {
    pub replicator: SecretReplicator,
    pub master_key_replicator: SecretMasterKeyReplicator,
    pub master_key_reconciler: SecretMasterKeyReconciler,
    pub master_key_replication_notify: Arc<Notify>,
}

impl RuntimeBuilder<'_> {
    /// Reconciles master keys against recovered topology before workloads can use secrets.
    pub(super) async fn build_secrets(
        &self,
        cluster: &ClusterComponents,
        channels: SecretChannels,
    ) -> SecretComponents {
        let secret_registry = SecretRegistry::new(self.stores.secrets.clone());
        let secret_replicator = SecretReplicator::new(
            secret_registry.clone(),
            self.gossip_sender.clone(),
            channels.secrets,
        );
        let secret_master_key_replicator = SecretMasterKeyReplicator::new(
            self.stores.secret_master_keys.clone(),
            channels.master_keys,
            cluster.master_keys.notify.clone(),
        );
        let secret_master_key_reconciler = SecretMasterKeyReconciler::new(
            self.ctx.self_id,
            self.ctx.noise_keys.clone(),
            cluster.registries.peers.clone(),
            self.stores.secret_master_keys.clone(),
            self.stores.secret_master_store.clone(),
            self.stores.secret_keyring.clone(),
            cluster.replication.cluster_view.clone(),
        );

        if let Err(error) = secret_master_key_reconciler.reconcile_active_view().await {
            tracing::warn!(
                target: "secrets",
                "failed to reconcile replicated secret master keys during startup: {error:#}"
            );
        }

        SecretComponents {
            registry: secret_registry,
            actors: SecretActors {
                replicator: secret_replicator,
                master_key_replicator: secret_master_key_replicator,
                master_key_reconciler: secret_master_key_reconciler,
                master_key_replication_notify: cluster.master_keys.notify.clone(),
            },
        }
    }
}
