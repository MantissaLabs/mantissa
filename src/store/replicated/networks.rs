use crate::network::types::{
    NetworkAttachmentStateRank, NetworkAttachmentValue, NetworkPeerStateRank,
    NetworkPeerStateValue, NetworkSpecValue,
};
use crate::store::replicated::open::open_arc_store;
use chrono::{DateTime, Utc};
use mantissa_store::adapter::{CompactingStoreMvRegAdapterSorted, MvRegCompactionRanker};
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::mvreg::MvRegEntry;
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use std::cmp::Reverse;
use std::sync::Arc;
use uuid::Uuid;

/// Redb table names for network specifications (desired state).
pub struct NetworkSpecTables;

impl TableSet for NetworkSpecTables {
    const VALUES: &'static str = "network_spec_values";
    const TOMBS: &'static str = "network_spec_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "network_spec_tombs_by_observed";
    const META: &'static str = "network_spec_meta";
}

/// Redb table names for replicated peer state entries.
pub struct NetworkPeerTables;

impl TableSet for NetworkPeerTables {
    const VALUES: &'static str = "network_peer_values";
    const TOMBS: &'static str = "network_peer_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "network_peer_tombs_by_observed";
    const META: &'static str = "network_peer_meta";
}

/// Redb table names for replicated network attachments.
pub struct NetworkAttachmentTables;

impl TableSet for NetworkAttachmentTables {
    const VALUES: &'static str = "network_attachment_values";
    const TOMBS: &'static str = "network_attachment_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "network_attachment_tombs_by_observed";
    const META: &'static str = "network_attachment_meta";
}

/// Network-spec ranker that preserves the registry's current canonical snapshot order.
pub struct NetworkSpecCompactionRank;

impl MvRegCompactionRanker<NetworkSpecValue, Uuid> for NetworkSpecCompactionRank {
    type Rank = NetworkSpecValue;

    /// Ranks one network spec by its full deterministic value ordering.
    fn rank(entry: &MvRegEntry<NetworkSpecValue, Uuid>) -> Self::Rank {
        entry.value().clone()
    }
}

/// Network peer-state compaction ranker used by the generic MVReg adapter.
pub struct NetworkPeerCompactionRank;

/// Total peer-state ordering key matching the registry's canonical selector.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct NetworkPeerRank {
    updated_at: String,
    state: NetworkPeerStateRank,
    tie_breaker: NetworkPeerStateValue,
}

impl MvRegCompactionRanker<NetworkPeerStateValue, Uuid> for NetworkPeerCompactionRank {
    type Rank = NetworkPeerRank;

    /// Ranks one peer-state row using the same timestamp and readiness order as the registry.
    fn rank(entry: &MvRegEntry<NetworkPeerStateValue, Uuid>) -> Self::Rank {
        let value = entry.value();
        NetworkPeerRank {
            updated_at: value.updated_at.clone(),
            state: value.state.precedence_rank(),
            tie_breaker: value.clone(),
        }
    }
}

/// Network attachment compaction ranker used by the generic MVReg adapter.
pub struct NetworkAttachmentCompactionRank;

/// Total attachment ordering key matching the registry's canonical selector.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct NetworkAttachmentRank {
    task_updated_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    state: NetworkAttachmentStateRank,
    traffic_published: bool,
    node_id: Uuid,
    tie_breaker: Reverse<NetworkAttachmentValue>,
}

impl MvRegCompactionRanker<NetworkAttachmentValue, Uuid> for NetworkAttachmentCompactionRank {
    type Rank = NetworkAttachmentRank;

    /// Ranks one attachment using task revision, attachment timestamp, and lifecycle state.
    fn rank(entry: &MvRegEntry<NetworkAttachmentValue, Uuid>) -> Self::Rank {
        let value = entry.value();
        NetworkAttachmentRank {
            task_updated_at: value.task_updated_at.as_deref().and_then(parse_rfc3339),
            updated_at: parse_attachment_timestamp(&value.updated_at, &value.created_at),
            state: value.state.precedence_rank(),
            traffic_published: value.traffic_published,
            node_id: value.node_id,
            tie_breaker: Reverse(value.clone()),
        }
    }
}

/// Parses the freshest available attachment timestamp used after task revision ordering ties.
fn parse_attachment_timestamp(updated_at: &str, created_at: &str) -> Option<DateTime<Utc>> {
    parse_rfc3339(updated_at).or_else(|| parse_rfc3339(created_at))
}

/// Parses one replicated RFC3339 timestamp into UTC for deterministic attachment ranking.
fn parse_rfc3339(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .ok()
}

/// Store adapter for network specification registers with compaction enabled.
pub type NetworkSpecRegAdapter =
    CompactingStoreMvRegAdapterSorted<UuidKey, NetworkSpecValue, Uuid, NetworkSpecCompactionRank>;

/// Specialized MST/CRDT store for network specifications.
pub type NetworkSpecStoreInner = CrdtMstStore<NetworkSpecRegAdapter, XXHash128, NetworkSpecTables>;

/// Shared handle to the network specification store.
pub type NetworkSpecStore = Arc<NetworkSpecStoreInner>;

/// Store adapter for network peer-state registers with compaction enabled.
pub type NetworkPeerRegAdapter = CompactingStoreMvRegAdapterSorted<
    UuidKey,
    NetworkPeerStateValue,
    Uuid,
    NetworkPeerCompactionRank,
>;

/// Specialized MST/CRDT store for per-peer network state.
pub type NetworkPeerStoreInner = CrdtMstStore<NetworkPeerRegAdapter, XXHash128, NetworkPeerTables>;

/// Shared handle to the network peer state store.
pub type NetworkPeerStore = Arc<NetworkPeerStoreInner>;

/// Store adapter for network attachment registers with compaction enabled.
pub type NetworkAttachmentRegAdapter = CompactingStoreMvRegAdapterSorted<
    UuidKey,
    NetworkAttachmentValue,
    Uuid,
    NetworkAttachmentCompactionRank,
>;

/// Specialized MST/CRDT store for network attachment state.
pub type NetworkAttachmentStoreInner =
    CrdtMstStore<NetworkAttachmentRegAdapter, XXHash128, NetworkAttachmentTables>;

/// Shared handle to the network attachment store.
pub type NetworkAttachmentStore = Arc<NetworkAttachmentStoreInner>;

/// Open or create the network specification store scoped to the provided actor.
pub fn open_network_spec_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<NetworkSpecStore> {
    open_arc_store(db, actor, NetworkSpecStoreInner::open)
}

/// Open or create the network peer state store scoped to the provided actor.
pub fn open_network_peer_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<NetworkPeerStore> {
    open_arc_store(db, actor, NetworkPeerStoreInner::open)
}

/// Open or create the network attachment store scoped to the provided actor.
pub fn open_network_attachment_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<NetworkAttachmentStore> {
    open_arc_store(db, actor, NetworkAttachmentStoreInner::open)
}
