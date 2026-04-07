use std::sync::Arc;

use tokio::sync::RwLock;

use crate::secrets::crypto::SecretKeyring;
use crate::store::agent_store::AgentStore;
use crate::store::cluster_operation_store::ClusterOperationStore;
use crate::store::cluster_view_store::ClusterViewStore;
use crate::store::job_store::JobStore;
use crate::store::local::{LocalCredentialStore, LocalSessionStore, SecretMasterStore};
use crate::store::network_store::{NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore};
use crate::store::peer_store::PeersStore;
use crate::store::scheduler_digest_store::SchedulerDigestStore;
use crate::store::secret_store::SecretStore;
use crate::store::service_store::ServiceStore;
use crate::store::volume_store::{VolumeNodeStore, VolumeSpecStore};
use crate::store::workload_store::WorkloadStore;
use crate::token::TokenStore;

/// Bundles the store handles required to construct and operate a `Topology`.
#[derive(Clone)]
pub struct TopologyStorage {
    pub local_credential_store: LocalCredentialStore,
    pub local_sessions: LocalSessionStore,
    pub peers: PeersStore,
    pub cluster_operations: ClusterOperationStore,
    pub cluster_view_store: ClusterViewStore,
    pub token_store: TokenStore,
    pub secret_master_store: SecretMasterStore,
    pub workloads: WorkloadStore,
    pub jobs: JobStore,
    pub agents: AgentStore,
    pub services: ServiceStore,
    pub secrets: SecretStore,
    pub networks: NetworkSpecStore,
    pub network_peers: NetworkPeerStore,
    pub network_attachments: NetworkAttachmentStore,
    pub volumes: VolumeSpecStore,
    pub volume_nodes: VolumeNodeStore,
    pub scheduler_digests: SchedulerDigestStore,
    pub secret_keyring: Arc<RwLock<SecretKeyring>>,
}
