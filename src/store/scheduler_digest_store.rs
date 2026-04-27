use crate::scheduler::digest::SchedulerDigestValue;
use crate::store::open::open_arc_store;
use crdt_store::adapter::{CompactingStoreMvRegAdapterSorted, MvRegCompactionRanker};
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::mvreg::MvRegEntry;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

/// Redb table names for replicated scheduler digest rows.
pub struct SchedulerDigestTables;

impl TableSet for SchedulerDigestTables {
    const VALUES: &'static str = "scheduler_digest_values";
    const TOMBS: &'static str = "scheduler_digest_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "scheduler_digest_tombs_by_observed";
    const META: &'static str = "scheduler_digest_meta";
}

/// Scheduler digest compaction ranker used by the generic MVReg adapter.
pub struct SchedulerDigestRank;

impl MvRegCompactionRanker<SchedulerDigestValue, Uuid> for SchedulerDigestRank {
    type Rank = (u64, u64, u32, u64, u64, u64, u64, u32, bool, Uuid);

    /// Ranks one scheduler digest entry by the existing canonical freshness order.
    fn rank(entry: &MvRegEntry<SchedulerDigestValue, Uuid>) -> Self::Rank {
        scheduler_digest_compaction_rank(entry.value())
    }
}

/// Builds the deterministic ordering key used for scheduler digest compaction.
fn scheduler_digest_compaction_rank(
    value: &SchedulerDigestValue,
) -> (u64, u64, u32, u64, u64, u64, u64, u32, bool, Uuid) {
    (
        value.snapshot_version,
        value.updated_at_unix_ms,
        value.free_slot_count,
        value.free_cpu_millis,
        value.free_memory_bytes,
        value.largest_free_slot_cpu_millis,
        value.largest_free_slot_memory_bytes,
        value.free_gpu_count,
        value.gpu_runtime_ready,
        value.node_id,
    )
}

/// Specialized MST/CRDT store for per-node scheduler digest rows.
pub type SchedulerDigestRegAdapter =
    CompactingStoreMvRegAdapterSorted<UuidKey, SchedulerDigestValue, Uuid, SchedulerDigestRank>;

/// Specialized MST/CRDT store for per-node scheduler digest rows.
pub type SchedulerDigestStoreInner =
    CrdtMstStore<SchedulerDigestRegAdapter, XXHash128, SchedulerDigestTables>;

/// Shared handle to the scheduler digest store.
pub type SchedulerDigestStore = Arc<SchedulerDigestStoreInner>;

/// Opens the replicated scheduler digest store backed by Redb.
pub fn open_scheduler_digest_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<SchedulerDigestStore> {
    open_arc_store(db, actor, SchedulerDigestStoreInner::open)
}

#[cfg(test)]
mod tests {
    use super::{SchedulerDigestRegAdapter, SchedulerDigestValue};
    use crdt_store::adapter::RegAdapter;
    use crdt_store::mvreg::{MvReg, MvRegEntry, VectorClock};
    use uuid::Uuid;

    /// Builds a deterministic UUID from a small integer for adapter tests.
    fn actor(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    /// Builds one scheduler digest value with enough fields to exercise ranking.
    fn digest(
        node_id: Uuid,
        snapshot_version: u64,
        updated_at_unix_ms: u64,
    ) -> SchedulerDigestValue {
        SchedulerDigestValue {
            node_id,
            snapshot_version,
            updated_at_unix_ms,
            free_slot_count: snapshot_version as u32,
            free_cpu_millis: snapshot_version.saturating_mul(1000),
            free_memory_bytes: snapshot_version.saturating_mul(2048),
            largest_free_slot_cpu_millis: snapshot_version.saturating_mul(500),
            largest_free_slot_memory_bytes: snapshot_version.saturating_mul(1024),
            free_gpu_count: 0,
            gpu_runtime_ready: true,
        }
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
        value: SchedulerDigestValue,
    ) -> MvRegEntry<SchedulerDigestValue, Uuid> {
        MvRegEntry::new(clock(actor, counter), value)
    }

    /// Scheduler digest registers should round-trip through the custom adapter codec.
    #[test]
    fn scheduler_digest_adapter_roundtrips_capnp_rows() {
        let node_id = actor(99);
        let reg = SchedulerDigestRegAdapter::upsert_reg(
            None,
            &actor(1),
            digest(node_id, 7, 1_776_000_000_001),
        );

        let encoded =
            SchedulerDigestRegAdapter::encode_reg(&reg).expect("encode scheduler digest register");
        let decoded = SchedulerDigestRegAdapter::decode_reg(&encoded)
            .expect("decode scheduler digest register");

        assert_eq!(decoded, reg);
        assert_eq!(
            SchedulerDigestRegAdapter::snapshot_reg(&decoded),
            SchedulerDigestRegAdapter::snapshot_reg(&reg)
        );
    }

    /// Compaction should keep the newest digest and absorb dropped clocks.
    #[test]
    fn scheduler_digest_compaction_keeps_newest_digest() {
        let node_id = actor(99);
        let dropped_actor = actor(1);
        let winner_actor = actor(2);
        let reg = MvReg::from_entries(vec![
            entry(dropped_actor, 1, digest(node_id, 1, 1_776_000_000_001)),
            entry(winner_actor, 1, digest(node_id, 2, 1_776_000_000_002)),
        ]);

        let compacted = SchedulerDigestRegAdapter::compact_reg(reg, 1)
            .expect("compact scheduler digest register")
            .expect("register should compact");
        let values = compacted.read_values();

        assert_eq!(values.len(), 1);
        assert_eq!(values[0].snapshot_version, 2);
        assert_eq!(values[0].updated_at_unix_ms, 1_776_000_000_002);

        let winner = compacted
            .entries()
            .iter()
            .find(|entry| entry.value().snapshot_version == 2)
            .expect("winner entry");
        assert_eq!(winner.clock().get(&dropped_actor), 1);
        assert_eq!(winner.clock().get(&winner_actor), 1);
    }
}
