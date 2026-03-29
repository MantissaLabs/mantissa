use super::{BootstrapContext, BootstrapResult};
use crate::cluster::{ClusterViewId, ClusterViewState};
use crate::secrets::crypto::SecretKeyring;
use crate::server::auth::AuthStore;
use crate::store::cluster_operation_store::ClusterOperationStore;
use crate::store::cluster_view_store::ClusterViewStore;
use crate::store::job_store::{JobStore, open_job_store};
use crate::store::local::{LocalCredentialStore, LocalSessionStore, SecretMasterStore};
use crate::store::network_store::{
    NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore, open_network_attachment_store,
    open_network_peer_store, open_network_spec_store,
};
use crate::store::peer_store::{PeersStore, open_peers_store};
use crate::store::scheduler_digest_store::{SchedulerDigestStore, open_scheduler_digest_store};
use crate::store::scheduler_store::{SchedulerStore, open_scheduler_store};
use crate::store::secret_store::{SecretStore, open_secret_store};
use crate::store::service_store::{ServiceStore, open_service_store};
use crate::store::task_store::{TaskStore, open_task_store};
use crate::store::volume_store::{
    VolumeNodeStore, VolumeSpecStore, open_volume_node_store, open_volume_spec_store,
};
use crate::token::TokenStore;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Wraps one bootstrap store error with a short startup-specific context.
///
/// This keeps storage open failures readable without introducing a custom error
/// enum for each individual store creation step.
fn store_error(context: &str, error: impl std::fmt::Display) -> Box<dyn std::error::Error> {
    Box::new(std::io::Error::other(format!("{context}: {error}")))
}

/// Durable stores opened during bootstrap.
///
/// This groups the persistent state needed by topology, scheduling, secrets,
/// services, networks, and volumes before the runtime actors are assembled.
pub(crate) struct BootstrapStores {
    pub peers: PeersStore,
    pub cluster_operations: ClusterOperationStore,
    pub cluster_view: ClusterViewStore,
    pub session_auth: AuthStore,
    pub local_sessions: LocalSessionStore,
    pub local_creds: LocalCredentialStore,
    pub token_store: TokenStore,
    pub secret_master_store: SecretMasterStore,
    pub tasks: TaskStore,
    pub jobs: JobStore,
    pub scheduler_store: SchedulerStore,
    pub scheduler_digests: SchedulerDigestStore,
    pub services: ServiceStore,
    pub secrets: SecretStore,
    pub networks: NetworkSpecStore,
    pub network_peers: NetworkPeerStore,
    pub network_attachments: NetworkAttachmentStore,
    pub volumes: VolumeSpecStore,
    pub volume_nodes: VolumeNodeStore,
    pub secret_keyring: Arc<RwLock<SecretKeyring>>,
}

impl BootstrapStores {
    /// Opens the durable stores and warms their on-disk indexes.
    ///
    /// This isolates storage concerns from runtime assembly so later bootstrap
    /// phases can work with a fully prepared persistent state view.
    pub(crate) async fn open(ctx: &BootstrapContext) -> BootstrapResult<Self> {
        let peers = open_peers_store(ctx.db.clone(), ctx.self_id)?;
        peers.rebuild_mst_from_disk().await?;

        let cluster_operations = ClusterOperationStore::new(ctx.db.clone())?;
        let cluster_view = ClusterViewStore::new(ctx.db.clone(), ctx.self_id)?;
        cluster_view.rebuild_cluster_view_domain_mst().await?;

        let session_auth = AuthStore::new(ctx.db.clone())?;
        let local_sessions = LocalSessionStore::open(ctx.db.clone(), &ctx.noise_keys)?;
        let local_creds = LocalCredentialStore::new(ctx.db.clone())?;

        let token_store = TokenStore::load(ctx.db.clone())
            .map_err(|error| store_error("load persistent join token", error))?;

        let secret_master_store = SecretMasterStore::new(ctx.db.clone())
            .map_err(|error| store_error("open secret master key store", error))?;
        let master_record = secret_master_store
            .ensure_current()
            .map_err(|error| store_error("ensure current secret master record", error))?;
        let secret_keyring = Arc::new(RwLock::new(SecretKeyring::new(
            secret_master_store.clone(),
            master_record,
        )));

        peers.debug_dump_root("peers").await;

        let tasks = open_task_store(ctx.db.clone(), ctx.self_id)?;
        tasks.rebuild_mst_from_disk().await?;

        let jobs = open_job_store(ctx.db.clone(), ctx.self_id)?;
        jobs.rebuild_mst_from_disk().await?;

        let scheduler_store = open_scheduler_store(ctx.db.clone(), ctx.self_id)?;
        scheduler_store.rebuild_mst_from_disk().await?;

        let scheduler_digests = open_scheduler_digest_store(ctx.db.clone(), ctx.self_id)?;
        scheduler_digests.rebuild_mst_from_disk().await?;

        let services = open_service_store(ctx.db.clone(), ctx.self_id)?;
        services.rebuild_mst_from_disk().await?;

        let secrets = open_secret_store(ctx.db.clone(), ctx.self_id)?;
        secrets.rebuild_mst_from_disk().await?;

        let networks = open_network_spec_store(ctx.db.clone(), ctx.self_id)?;
        networks.rebuild_mst_from_disk().await?;

        let network_peers = open_network_peer_store(ctx.db.clone(), ctx.self_id)?;
        network_peers.rebuild_mst_from_disk().await?;

        let network_attachments = open_network_attachment_store(ctx.db.clone(), ctx.self_id)?;
        network_attachments.rebuild_mst_from_disk().await?;

        let volumes = open_volume_spec_store(ctx.db.clone(), ctx.self_id)?;
        volumes.rebuild_mst_from_disk().await?;

        let volume_nodes = open_volume_node_store(ctx.db.clone(), ctx.self_id)?;
        volume_nodes.rebuild_mst_from_disk().await?;

        Ok(Self {
            peers,
            cluster_operations,
            cluster_view,
            session_auth,
            local_sessions,
            local_creds,
            token_store,
            secret_master_store,
            tasks,
            jobs,
            scheduler_store,
            scheduler_digests,
            services,
            secrets,
            networks,
            network_peers,
            network_attachments,
            volumes,
            volume_nodes,
            secret_keyring,
        })
    }

    /// Restores the last committed active cluster view from disk.
    ///
    /// The runtime uses this to rebuild cluster-scoped services before any
    /// network traffic is accepted.
    pub(crate) fn restore_active_view(&self) -> BootstrapResult<ClusterViewState> {
        let persisted_active_view = self
            .cluster_view
            .read_active_view()
            .map_err(|error| store_error("read persisted active cluster view", error))?;
        let active_view = persisted_active_view.unwrap_or_else(ClusterViewId::legacy_default);
        if persisted_active_view.is_some() {
            info!(
                target: "cluster_view",
                active_view = %active_view,
                "restored persisted active cluster view during startup"
            );
        }
        Ok(ClusterViewState::new(active_view))
    }
}
