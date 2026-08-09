use std::collections::BTreeMap;

use anyhow::Result;
use mantissa_raft::transport::{RaftPeer, RaftPeerDirectory};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::store::replicated::peers::PeersStore;
use crate::topology::peers::PeerValue;

/// Cached storage addresses and Noise keys read from the peer store.
pub(super) struct StoragePeerDirectory {
    peers: PeersStore,
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
    pub(super) fn new(peers: PeersStore) -> Self {
        Self {
            peers,
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
        self.refresh().ok()?;
        self.cache.read().by_node.get(node_id).cloned()
    }

    /// Returns the active storage node that owns one proven Noise key.
    fn node_for_noise_key(&self, noise_public_key: &[u8; 32]) -> Option<Uuid> {
        self.refresh().ok()?;
        self.cache
            .read()
            .by_noise_key
            .get(noise_public_key)
            .copied()
    }
}
