use std::collections::BTreeMap;

use anyhow::Result;
use mantissa_raft::transport::{RaftPeer, RaftPeerDirectory};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::cluster::ClusterViewState;
use crate::store::replicated::peers::PeersStore;
use crate::topology::peers::PeerValue;

/// Cached storage addresses and Noise keys read from the peer store.
pub(super) struct StoragePeerDirectory {
    peers: PeersStore,
    cluster_view: ClusterViewState,
    cache: RwLock<StoragePeerCache>,
}

#[derive(Default)]
struct StoragePeerCache {
    loaded: bool,
    generation: u64,
    by_node: BTreeMap<Uuid, RaftPeer<Uuid>>,
    by_noise_key: BTreeMap<[u8; 32], Uuid>,
}

impl StoragePeerDirectory {
    /// Creates an empty cache over the existing durable peer store.
    pub(super) fn new(peers: PeersStore, cluster_view: ClusterViewState) -> Self {
        Self {
            peers,
            cluster_view,
            cache: RwLock::new(StoragePeerCache::default()),
        }
    }

    /// Rebuilds both indexes only when the peer store changes.
    fn refresh(&self) -> Result<()> {
        let generation = self.peers.change_clock();
        let cache = self.cache.read();
        if cache.loaded && cache.generation == generation {
            return Ok(());
        }
        drop(cache);
        let (rows, _) = self.peers.load_all_regs()?;
        let mut by_node = BTreeMap::new();
        let mut by_noise_key = BTreeMap::new();
        for (key, register) in rows {
            let node_id = key.to_uuid();
            let Some(peer) = PeerValue::select_reg(&register) else {
                continue;
            };
            if !peer.is_active() || !peer.replicated_volumes.is_running() {
                continue;
            }
            let Ok(address) = peer.replicated_volumes.address.parse() else {
                continue;
            };
            let raft_peer = RaftPeer {
                node_id,
                address,
                noise_public_key: peer.noise_static_pub,
            };
            by_noise_key.insert(peer.noise_static_pub, node_id);
            by_node.insert(node_id, raft_peer);
        }
        *self.cache.write() = StoragePeerCache {
            loaded: true,
            generation,
            by_node,
            by_noise_key,
        };
        Ok(())
    }
}

impl RaftPeerDirectory<Uuid> for StoragePeerDirectory {
    /// Returns the current private address and Noise key for one member.
    fn peer(&self, node_id: &Uuid) -> Option<RaftPeer<Uuid>> {
        if !self.cluster_view.includes_node(node_id) {
            return None;
        }
        self.refresh().ok()?;
        self.cache.read().by_node.get(node_id).cloned()
    }

    /// Returns the active storage node that owns one proven Noise key.
    fn node_for_noise_key(&self, noise_public_key: &[u8; 32]) -> Option<Uuid> {
        self.refresh().ok()?;
        let node_id = self
            .cache
            .read()
            .by_noise_key
            .get(noise_public_key)
            .copied()?;
        self.cluster_view.includes_node(&node_id).then_some(node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::RootSchemaInfo;
    use crate::runtime::types::RuntimeSupportProfile;
    use crate::store::replicated::peers::open_peers_store;
    use crate::topology::peers::{PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue};
    use crate::volumes::replicated::{REPLICATED_VOLUME_FORMAT_VERSION, ReplicatedVolumeSupport};
    use mantissa_store::uuid_key::UuidKey;
    use std::sync::Arc;

    /// Builds one active storage peer for cluster-view tests.
    fn storage_peer(node_id: Uuid, noise_static_pub: [u8; 32]) -> PeerValue {
        PeerValue {
            address: "127.0.0.1:6578".to_string(),
            hostname: "storage-peer".to_string(),
            platform_os: "linux".to_string(),
            platform_arch: "x86_64".to_string(),
            noise_static_pub,
            signing_pub: [2; 32],
            identity_sig: vec![3; 64],
            wireguard: None,
            scheduling: PeerSchedulingState::schedulable_default(node_id),
            readiness: Default::default(),
            labels: PeerLabelState::default(),
            runtime_support: RuntimeSupportProfile::default(),
            replicated_volumes: ReplicatedVolumeSupport {
                address: "127.0.0.1:7578".to_string(),
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

    /// Moving a peer outside the view hides its address and Noise identity.
    #[tokio::test]
    async fn out_of_view_peer_cannot_be_returned_from_cache() {
        let directory = tempfile::tempdir().expect("create peer directory tempdir");
        let db = Arc::new(
            redb::Database::create(directory.path().join("peers.redb"))
                .expect("create peer directory database"),
        );
        let local_node_id = Uuid::from_u128(1);
        let peer_node_id = Uuid::from_u128(2);
        let noise_static_pub = [7; 32];
        let peers = open_peers_store(db, local_node_id).expect("open peer store");
        peers
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild peer store");
        peers
            .upsert(
                &UuidKey::from(peer_node_id),
                storage_peer(peer_node_id, noise_static_pub),
            )
            .await
            .expect("save storage peer");
        let cluster_view = ClusterViewState::legacy_default();
        let storage = StoragePeerDirectory::new(peers, cluster_view.clone());

        assert!(storage.peer(&peer_node_id).is_some());
        assert_eq!(
            storage.node_for_noise_key(&noise_static_pub),
            Some(peer_node_id)
        );

        cluster_view.install(
            cluster_view.active_view(),
            std::collections::HashSet::from([peer_node_id]),
        );
        assert!(storage.peer(&peer_node_id).is_none());
        assert_eq!(storage.node_for_noise_key(&noise_static_pub), None);

        cluster_view.install(cluster_view.active_view(), std::collections::HashSet::new());
        assert!(storage.peer(&peer_node_id).is_some());
        assert_eq!(
            storage.node_for_noise_key(&noise_static_pub),
            Some(peer_node_id)
        );
    }
}
