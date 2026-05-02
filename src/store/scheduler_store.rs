use std::sync::Arc;

use crate::store::open::open_arc_store;
use mantissa_store::adapter::StoreMvRegAdapterSorted;
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use uuid::Uuid;

use crate::scheduler::SchedulerSnapshot;

pub struct SchedulerTables;

impl TableSet for SchedulerTables {
    const VALUES: &'static str = "scheduler_values";
    const TOMBS: &'static str = "scheduler_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "scheduler_tombs_by_observed";
    const META: &'static str = "scheduler_meta";
}

pub type SchedulerStoreInner = CrdtMstStore<
    StoreMvRegAdapterSorted<UuidKey, SchedulerSnapshot, Uuid>,
    XXHash128,
    SchedulerTables,
>;

pub type SchedulerStore = Arc<SchedulerStoreInner>;

pub fn open_scheduler_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<SchedulerStore> {
    open_arc_store(db, actor, SchedulerStoreInner::open)
}
