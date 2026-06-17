use crate::ingress::types::IngressPoolSpecValue;
use crate::store::replicated::open::open_arc_store;
use mantissa_store::adapter::{CompactingStoreMvRegAdapterSorted, MvRegCompactionRanker};
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::mvreg::MvRegEntry;
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use std::cmp::Reverse;
use std::sync::Arc;
use uuid::Uuid;

/// Redb table names for replicated public ingress pool specifications.
pub struct IngressPoolTables;

impl TableSet for IngressPoolTables {
    const VALUES: &'static str = "ingress_pool_values";
    const TOMBS: &'static str = "ingress_pool_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "ingress_pool_tombs_by_observed";
    const META: &'static str = "ingress_pool_meta";
}

/// Ingress-pool compaction ranker used by the generic MVReg adapter.
pub struct IngressPoolCompactionRank;

/// Total ingress-pool ordering key matching the registry's canonical selector.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct IngressPoolRank {
    generation: u64,
    updated_at: String,
    name: String,
    min_nodes: u16,
    max_nodes: Option<u16>,
    placement: crate::scheduler::placement::PlacementPolicy,
    spread_by: Option<crate::ingress::types::IngressPoolSpreadKey>,
    created_at: String,
    id: Uuid,
    tie_breaker: Reverse<IngressPoolSpecValue>,
}

impl MvRegCompactionRanker<IngressPoolSpecValue, Uuid> for IngressPoolCompactionRank {
    type Rank = IngressPoolRank;

    /// Ranks one ingress-pool register with the same winner selected by the registry.
    fn rank(entry: &MvRegEntry<IngressPoolSpecValue, Uuid>) -> Self::Rank {
        let value = entry.value();
        IngressPoolRank {
            generation: value.generation,
            updated_at: value.updated_at.clone(),
            name: value.name.clone(),
            min_nodes: value.min_nodes,
            max_nodes: value.max_nodes,
            placement: value.placement.clone(),
            spread_by: value.spread_by.clone(),
            created_at: value.created_at.clone(),
            id: value.id,
            tie_breaker: Reverse(value.clone()),
        }
    }
}

/// Store adapter for ingress-pool registers with domain-aware compaction enabled.
pub type IngressPoolRegAdapter = CompactingStoreMvRegAdapterSorted<
    UuidKey,
    IngressPoolSpecValue,
    Uuid,
    IngressPoolCompactionRank,
>;

/// Specialized MST/CRDT store for public ingress pool specifications.
pub type IngressPoolStoreInner = CrdtMstStore<IngressPoolRegAdapter, XXHash128, IngressPoolTables>;

/// Shared handle to the public ingress pool specification store.
pub type IngressPoolStore = Arc<IngressPoolStoreInner>;

/// Open or create the public ingress pool specification store scoped to the provided actor.
pub fn open_ingress_pool_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<IngressPoolStore> {
    open_arc_store(db, actor, IngressPoolStoreInner::open)
}
