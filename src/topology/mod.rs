use crate::cluster::{ClusterViewId, ClusterViewState, RootSchemaInfo, RootSchemaState};
use crate::config;
use crate::gossip::{GossipContext, Message};
use crate::network::registry::NetworkRegistry;
use crate::node::Node;
use crate::node::address::compute_advertise_ip;
use crate::node::address::extract_port;
use crate::node::id::set_node_id;
use crate::registry::Registry;
use crate::runtime::types::RuntimeSupportProfile;
use crate::scheduler::Scheduler;
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::master_key::replication::SecretMasterKeyPublisher;
use crate::server::auth::AuthStore;
use crate::services::{ServiceReconcileTrigger, ServiceRegistry};
use crate::store::local::{LocalCredentialStore, LocalSessionStore, SecretMasterStore};
use crate::store::replicated::cluster_operations::ClusterOperationStore;
use crate::store::replicated::cluster_views::ClusterNameRecord;
use crate::store::replicated::cluster_views::ClusterViewStore;
use crate::store::replicated::peers::PeersStore;
use crate::store::replicated::secret_key_sync::SecretMasterKeyStore;
use crate::store::replicated::secrets::SecretStore;
use crate::sync::{SyncRunner, SyncTraceContext};
use crate::token::TokenStore;
use crate::topology::peers::{PeerMembership, PeerSchedulingState, PeerValue, WireGuardPeerValue};
use crate::volumes::VolumeRegistry;
use crate::workload::WorkloadRegistry;
use ::mantissa_health::HealthMonitor;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use capnp::Error;
use ed25519_dalek::{SigningKey, VerifyingKey};
use futures::stream::{FuturesUnordered, StreamExt};
use mantissa_net::noise::NoisePeerVerifier;
use mantissa_protocol::gossip::gossip::Client as GossipClient;
use mantissa_protocol::server::{self, ServerClient};
use mantissa_protocol::sync::Domain;
use mantissa_store::uuid_key::UuidKey;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;
use x25519_dalek::PublicKey;

use self::builders::drain_state_from_scheduling;
use self::local_state::LocalNodeState;
use self::peer_cache::{PeerCacheEntry, PeerSnapshot, PeerSnapshotCache};
use self::runtime::{
    GossipWarmSetState, ImmediateSyncState, TopologyRuntime, WorkloadRepairHintState,
};

mod builders;
mod cluster_operations;
mod drain;
mod event;
mod gossip;
pub mod health;
mod local_node;
mod local_state;
mod membership;
mod peer_cache;
mod peer_handle;
pub mod peer_provider;
pub mod peers;
mod runtime;
mod service;
mod swim;
mod sync;

pub use self::event::TopologyEvent;
pub use self::peer_handle::PeerHandle;
pub use builders::add_event;
pub use service::read_topology_event;

/// Default anti-entropy interval for periodic sync loops.
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(5);
/// Default number of peers sampled per anti-entropy sync tick.
const DEFAULT_SYNC_FANOUT: usize = 8;
/// Default maximum number of peers synchronized concurrently within one tick.
const DEFAULT_SYNC_PARALLELISM: usize = 3;
/// Number of view-scoped gossip peers kept warm relative to the hot-path fanout budget.
const DEFAULT_GOSSIP_WARM_SET_MULTIPLIER: usize = 4;
/// Hard cap applied to the warm peer set so gossip session reuse stays bounded.
const DEFAULT_GOSSIP_WARM_SET_MAX: usize = 32;
/// Number of peers rotated through the warm set on each refresh.
const DEFAULT_GOSSIP_WARM_ROTATION: usize = 1;
/// Max idle age before cached sessions and derived capabilities are discarded.
const DEFAULT_GOSSIP_CAPABILITY_MAX_IDLE: Duration = Duration::from_secs(30);
/// Hard cap for cached capability entries kept by the registry before idle eviction trims them.
const DEFAULT_GOSSIP_CAPABILITY_CACHE_MAX: usize = 256;
/// Default anti-entropy interval for cross-view cluster metadata sync.
const DEFAULT_GLOBAL_METADATA_SYNC_INTERVAL: Duration = Duration::from_secs(5);
/// Default number of peers sampled per metadata sync tick.
const DEFAULT_GLOBAL_METADATA_SYNC_FANOUT: usize = 8;
/// Default maximum concurrent cross-view metadata sync operations per tick.
const DEFAULT_GLOBAL_METADATA_SYNC_PARALLELISM: usize = 1;
/// Number of peers targeted by the low-rate workload-only repair path on each sync tick.
const DEFAULT_WORKLOAD_REPAIR_FANOUT: usize = 1;
/// Maximum queued peers that can be prioritized by deployment-aware workload repair.
const DEFAULT_WORKLOAD_REPAIR_HINT_MAX: usize = 256;
/// Cross-view domains synchronized by the global metadata anti-entropy loop.
const GLOBAL_METADATA_SYNC_DOMAINS: [Domain; 2] = [Domain::ClusterViews, Domain::ClusterOperations];
/// Selected domains synchronized by the targeted workload-only repair path.
const WORKLOAD_REPAIR_SYNC_DOMAINS: [Domain; 1] = [Domain::Workloads];

/// Reads the optional per-tick sync parallelism override from the environment.
fn sync_parallelism_from_env(default: usize) -> usize {
    std::env::var("MANTISSA_SYNC_PARALLELISM")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Reads the optional metadata sync parallelism override from the environment.
fn global_metadata_sync_parallelism_from_env(default: usize) -> usize {
    std::env::var("MANTISSA_GLOBAL_METADATA_SYNC_PARALLELISM")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Keys and signing material used by the topology service.
#[derive(Clone)]
pub struct Keys {
    pub noise_public_key: PublicKey,
    pub signing_key: SigningKey,
}

#[derive(Clone)]
pub struct Topology {
    local: LocalNodeState,
    stores: TopologyStorage,
    runtime: TopologyRuntime,
    deps: TopologyDependencies,
}

pub struct TopologyConfig {
    pub addr: String,
    pub gossip_receiver: Receiver<Message>,
    pub gossip_sender: Sender<Message>,
    pub node: Node,
    pub cluster_view: ClusterViewState,
    pub root_schema: RootSchemaState,
    pub stores: TopologyStorage,
    pub crypto: Keys,
    pub deps: TopologyDependencies,
    pub runtime_support: RuntimeSupportProfile,
}

/// Bundles the store handles required to construct and operate a `Topology`.
#[derive(Clone)]
pub struct TopologyStorage {
    pub local_credential_store: LocalCredentialStore,
    pub local_sessions: LocalSessionStore,
    pub session_auth: AuthStore,
    pub peers: PeersStore,
    pub cluster_operations: ClusterOperationStore,
    pub cluster_view_store: ClusterViewStore,
    pub token_store: TokenStore,
    pub secret_master_store: SecretMasterStore,
    pub secret_keyring: Arc<RwLock<SecretKeyring>>,
    pub secrets: SecretStore,
    pub secret_master_keys: SecretMasterKeyStore,
    pub secret_master_key_publisher: SecretMasterKeyPublisher,
}

/// Runtime collaborators used by topology but owned outside its durable stores.
#[derive(Clone)]
pub struct TopologyDependencies {
    pub registry: Registry,
    pub network_registry: NetworkRegistry,
    pub workload_registry: WorkloadRegistry,
    pub service_registry: ServiceRegistry,
    pub service_reconcile_trigger: ServiceReconcileTrigger,
    pub volume_registry: VolumeRegistry,
    pub scheduler: Rc<Scheduler>,
    pub sync: SyncRunner,
    pub health_monitor: Arc<HealthMonitor>,
    pub runtime_health: config::RuntimeHealthConfig,
}

impl Topology {
    pub fn new(config: TopologyConfig) -> Result<Self, Error> {
        let TopologyConfig {
            addr,
            gossip_receiver,
            gossip_sender,
            node,
            cluster_view,
            root_schema,
            stores,
            crypto,
            deps,
            runtime_support,
        } = config;
        let Keys {
            noise_public_key,
            signing_key,
        } = crypto;
        let probe_interval = deps.runtime_health.probe_interval;
        let topology = Self {
            local: LocalNodeState {
                node,
                cluster_view,
                root_schema,
                advertise: local_state::AdvertiseState::new(addr),
                server_handle: Rc::new(std::cell::OnceCell::new()),
                public_key: noise_public_key,
                signing_key,
                runtime_support,
            },
            stores,
            runtime: TopologyRuntime {
                gossip: runtime::GossipState::new(gossip_receiver, gossip_sender),
                peer_snapshot_cache: Arc::new(tokio::sync::Mutex::new(PeerSnapshotCache::new())),
                gossip_warm_set: Arc::new(tokio::sync::Mutex::new(GossipWarmSetState::default())),
                excluded_peers: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
                immediate_sync: ImmediateSyncState::new(),
                sync: runtime::SyncLoopState::new(DEFAULT_SYNC_INTERVAL, DEFAULT_SYNC_FANOUT),
                health_probe: runtime::ProbeLoopState::new(probe_interval),
                workload_repair_fanout: Arc::new(Mutex::new(DEFAULT_WORKLOAD_REPAIR_FANOUT)),
                workload_repair_cursor: Arc::new(Mutex::new(0)),
                workload_repair_hints: Arc::new(Mutex::new(WorkloadRepairHintState::default())),
                metadata_sync: runtime::SyncLoopState::new(
                    DEFAULT_GLOBAL_METADATA_SYNC_INTERVAL,
                    DEFAULT_GLOBAL_METADATA_SYNC_FANOUT,
                ),
                metadata_sync_cursor: Arc::new(Mutex::new(0)),
                cluster_operation_gate: runtime::ClusterOperationGate::new(),
            },
            deps,
        };

        info!(
            target: "cluster_view",
            active_view = %topology.active_cluster_view(),
            "initialized topology with active cluster view"
        );

        Ok(topology)
    }

    /// Returns the currently active cluster view identifier.
    pub fn active_cluster_view(&self) -> ClusterViewId {
        self.local.cluster_view.active_view()
    }

    /// Returns the highest semantic root schema version this binary supports.
    pub fn supported_root_schema_version(&self) -> u32 {
        self.local.root_schema.supported_version()
    }

    /// Returns the lowest semantic root schema version this binary still serves.
    pub fn minimum_root_schema_version(&self) -> u32 {
        self.local.root_schema.minimum_supported_version()
    }

    /// Returns the root-schema support metadata currently advertised by this node.
    pub fn root_schema_info(&self) -> RootSchemaInfo {
        self.local.root_schema.info()
    }

    /// Replaces the active cluster view identifier and returns the previous value.
    #[allow(dead_code)]
    pub fn set_active_cluster_view(&self, next: ClusterViewId) -> ClusterViewId {
        let previous = self.local.cluster_view.set_active_view(next);
        if previous == next {
            debug!(
                target: "cluster_view",
                active_view = %next,
                "active cluster view already current"
            );
            return previous;
        }

        info!(
            target: "cluster_view",
            previous = %previous,
            next = %next,
            "updated active cluster view"
        );
        previous
    }

    /// Returns a snapshot of peers currently excluded from active control-plane loops.
    pub async fn excluded_peers_snapshot(&self) -> HashSet<Uuid> {
        self.runtime.excluded_peers.lock().await.clone()
    }

    /// Replaces the excluded-peer set used to scope active control-plane loops.
    pub async fn set_excluded_peers(&self, excluded: HashSet<Uuid>) {
        let mut guard = self.runtime.excluded_peers.lock().await;
        if *guard != excluded {
            *guard = excluded;
        }
    }
}
