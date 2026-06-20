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
///
/// Delete markers rank before generation so compaction cannot retain a stale
/// active row over a concurrent delete, even if the stale row has a higher
/// user-facing generation counter.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct IngressPoolRank {
    deleted: bool,
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
            deleted: value.deleted,
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

#[cfg(test)]
mod tests {
    use super::{IngressPoolRegAdapter, IngressPoolSpecValue};
    use crate::ingress::types::IngressPoolSpecDraft;
    use crate::scheduler::placement::PlacementPolicy;
    use mantissa_store::adapter::RegAdapter;
    use mantissa_store::mvreg::{MvReg, MvRegEntry, VectorClock};
    use uuid::Uuid;

    /// Builds a deterministic UUID from a small integer for adapter tests.
    fn actor(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    /// Builds one valid ingress pool spec for compaction tests.
    fn pool() -> IngressPoolSpecValue {
        IngressPoolSpecValue::from_draft(IngressPoolSpecDraft {
            name: "public-web".to_string(),
            min_nodes: 1,
            max_nodes: Some(1),
            placement: PlacementPolicy::default(),
            spread_by: None,
        })
        .expect("valid ingress pool")
    }

    /// Builds a one-actor vector clock for deterministic MVReg fixtures.
    fn clock(actor: Uuid, counter: u64) -> VectorClock<Uuid> {
        let mut clock = VectorClock::new();
        clock.apply(actor, counter);
        clock
    }

    /// Builds one explicit MVReg entry for deterministic adapter fixtures.
    fn entry(
        actor: Uuid,
        counter: u64,
        value: IngressPoolSpecValue,
    ) -> MvRegEntry<IngressPoolSpecValue, Uuid> {
        MvRegEntry::new(clock(actor, counter), value)
    }

    /// Compaction should keep delete markers over stale active rows.
    #[test]
    fn ingress_pool_compaction_keeps_delete_marker_over_stale_active() {
        let active_actor = actor(1);
        let delete_actor = actor(2);
        let mut stale_active = pool();
        stale_active.generation = 100;
        stale_active.touch();
        let mut deleted = pool();
        deleted.mark_deleted();
        let reg = MvReg::from_entries(vec![
            entry(active_actor, 1, stale_active),
            entry(delete_actor, 1, deleted),
        ]);

        let compacted = IngressPoolRegAdapter::compact_reg(reg, 1)
            .expect("compact ingress pool register")
            .expect("register should compact");
        let values = compacted.read_values();

        assert_eq!(values.len(), 1);
        assert!(
            values[0].is_deleted(),
            "delete marker should outrank stale active rows during compaction"
        );

        let winner = compacted
            .entries()
            .iter()
            .find(|entry| entry.value().is_deleted())
            .expect("delete marker winner");
        assert_eq!(winner.clock().get(&active_actor), 1);
        assert_eq!(winner.clock().get(&delete_actor), 1);
    }
}
