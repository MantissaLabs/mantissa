use crate::{store::crdt::mvreg_snapshot::MvRegSnapshot, topology::peers::types::PeerValue};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PeerEntry {
    Active(MvRegSnapshot<PeerValue>),
    // A monotonic delete marker; ts can be lamport/time to break ties if you later re-activate
    Deleted { ts: u64 },
}

// Hash must be deterministic and cover the variant + payload
impl Hash for PeerEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            PeerEntry::Active(snap) => {
                0u8.hash(state); // variant tag
                snap.hash(state); // hashes sorted vals
            }
            PeerEntry::Deleted { ts } => {
                1u8.hash(state); // variant tag
                ts.hash(state);
            }
        }
    }
}
