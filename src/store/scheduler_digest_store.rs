use crate::scheduler::digest::SchedulerDigestValue;
use crate::store::open::open_arc_store;
use crdt_store::adapter::StoreMvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
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

/// Specialized MST/CRDT store for per-node scheduler digest rows.
pub type SchedulerDigestStoreInner = CrdtMstStore<
    StoreMvRegAdapterSorted<UuidKey, SchedulerDigestValue, Uuid>,
    XXHash128,
    SchedulerDigestTables,
>;

/// Shared handle to the scheduler digest store.
pub type SchedulerDigestStore = Arc<SchedulerDigestStoreInner>;

/// Opens the replicated scheduler digest store backed by Redb.
pub fn open_scheduler_digest_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<SchedulerDigestStore> {
    open_arc_store(db, actor, SchedulerDigestStoreInner::open)
}
