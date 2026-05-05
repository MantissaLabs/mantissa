use super::{BootstrapContext, BootstrapResult, runtime::BootstrapOptions};
use crate::cluster::{
    ClusterViewId, ClusterViewState, MIN_SUPPORTED_ROOT_SCHEMA_VERSION, RootSchemaState,
    SUPPORTED_ROOT_SCHEMA_VERSION,
};
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::master_key_protector::PassphraseMasterKeyProtector;
use crate::server::auth::AuthStore;
use crate::store::agent_store::{AgentStore, open_agent_store};
use crate::store::cluster_operation_store::ClusterOperationStore;
use crate::store::cluster_view_store::ClusterViewStore;
use crate::store::job_store::{JobStore, open_job_store};
use crate::store::local::{
    LocalCredentialStore, LocalSessionStore, SecretMasterStore,
    next_root_schema_publication_generation,
};
use crate::store::network_store::{
    NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore, open_network_attachment_store,
    open_network_peer_store, open_network_spec_store,
};
use crate::store::peer_store::{PeersStore, open_peers_store};
use crate::store::scheduler_digest_store::{SchedulerDigestStore, open_scheduler_digest_store};
use crate::store::scheduler_store::{SchedulerStore, open_scheduler_store};
use crate::store::secret_store::{SecretStore, open_secret_store};
use crate::store::service_store::{ServiceStore, open_service_store};
use crate::store::volume_store::{
    VolumeNodeStore, VolumeSpecStore, open_volume_node_store, open_volume_spec_store,
};
use crate::store::workload_store::{WorkloadStore, open_workload_store};
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
pub struct BootstrapStores {
    pub peers: PeersStore,
    pub cluster_operations: ClusterOperationStore,
    pub cluster_view: ClusterViewStore,
    pub session_auth: AuthStore,
    pub local_sessions: LocalSessionStore,
    pub local_creds: LocalCredentialStore,
    pub token_store: TokenStore,
    pub secret_master_store: SecretMasterStore,
    pub workloads: WorkloadStore,
    pub jobs: JobStore,
    pub agents: AgentStore,
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
    pub(super) async fn open(
        ctx: &BootstrapContext,
        options: &BootstrapOptions,
    ) -> BootstrapResult<Self> {
        let peers = open_peers_store(ctx.db.clone(), ctx.self_id)?;
        peers.rebuild_mst_from_disk().await?;

        let cluster_operations = ClusterOperationStore::new(ctx.db.clone())?;
        let cluster_view = ClusterViewStore::new(ctx.db.clone(), ctx.self_id)?;
        cluster_view.rebuild_cluster_view_domain_mst().await?;

        let session_ticket_ttl_secs = crate::config::session_ticket_ttl_secs();
        let session_auth = AuthStore::with_ticket_ttl(ctx.db.clone(), session_ticket_ttl_secs)?;
        let local_sessions = LocalSessionStore::open_with_ticket_ttl(
            ctx.db.clone(),
            &ctx.noise_keys,
            session_ticket_ttl_secs,
        )?;
        let local_creds = LocalCredentialStore::new(ctx.db.clone())?;

        let token_store = TokenStore::load(ctx.db.clone())
            .map_err(|error| store_error("load persistent join token", error))?;

        let passphrase = options.master_key_passphrase.clone().ok_or_else(|| {
            store_error(
                "open secret master key store",
                "master key passphrase source is required",
            )
        })?;
        let secret_master_protector = Arc::new(PassphraseMasterKeyProtector::with_params(
            passphrase,
            options.master_key_kdf_params,
        ));
        let secret_master_store =
            SecretMasterStore::new(ctx.db.clone(), secret_master_protector)
                .map_err(|error| store_error("open secret master key store", error))?;
        let master_record = secret_master_store
            .ensure_current_for_node(ClusterViewId::legacy_default(), ctx.self_id)
            .map_err(|error| store_error("ensure current secret master record", error))?;
        let secret_keyring = Arc::new(RwLock::new(SecretKeyring::new(
            secret_master_store.clone(),
            master_record,
        )));

        peers.debug_dump_root("peers").await;

        let workloads = open_workload_store(ctx.db.clone(), ctx.self_id)?;
        workloads.rebuild_mst_from_disk().await?;

        let jobs = open_job_store(ctx.db.clone(), ctx.self_id)?;
        jobs.rebuild_mst_from_disk().await?;

        let agents = open_agent_store(ctx.db.clone(), ctx.self_id)?;
        agents.rebuild_mst_from_disk().await?;

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
            workloads,
            jobs,
            agents,
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
    pub(super) fn restore_active_view(&self) -> BootstrapResult<ClusterViewState> {
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

    /// Builds the local semantic root schema support range advertised at startup.
    pub(super) fn restore_root_schema_state(
        &self,
        db: &redb::Database,
        override_state: Option<RootSchemaState>,
    ) -> BootstrapResult<RootSchemaState> {
        let publication_generation = next_root_schema_publication_generation(db)
            .map_err(|error| store_error("advance root schema publication generation", error))?;

        if let Some(root_schema) = override_state {
            let root_schema = RootSchemaState::with_publication_generation(
                root_schema.minimum_supported_version(),
                root_schema.supported_version(),
                publication_generation,
            )
            .map_err(|error| store_error("build overridden root schema state", error))?;
            info!(
                target: "sync",
                minimum_root_schema_version = root_schema.minimum_supported_version(),
                supported_root_schema_version = root_schema.supported_version(),
                root_schema_publication_generation = root_schema.publication_generation(),
                "initialized overridden local root schema support range during startup"
            );
            return Ok(root_schema);
        }

        info!(
            target: "sync",
            minimum_root_schema_version = MIN_SUPPORTED_ROOT_SCHEMA_VERSION,
            supported_root_schema_version = SUPPORTED_ROOT_SCHEMA_VERSION,
            root_schema_publication_generation = publication_generation,
            "initialized local root schema support range during startup"
        );

        RootSchemaState::with_publication_generation(
            MIN_SUPPORTED_ROOT_SCHEMA_VERSION,
            SUPPORTED_ROOT_SCHEMA_VERSION,
            publication_generation,
        )
        .map_err(|error| store_error("build root schema state", error))
    }
}
