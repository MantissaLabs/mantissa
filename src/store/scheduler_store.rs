use std::sync::Arc;

use crate::store::open::open_arc_store;
use crdt_store::adapter::MvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use uuid::Uuid;

use crate::scheduler::SchedulerSnapshot;

pub struct SchedulerTables;

impl TableSet for SchedulerTables {
    const VALUES: &'static str = "scheduler_values";
    const TOMBS: &'static str = "scheduler_tombs";
    const META: &'static str = "scheduler_meta";
}

pub type SchedulerStoreInner =
    CrdtMstStore<MvRegAdapterSorted<UuidKey, SchedulerSnapshot, Uuid>, XXHash128, SchedulerTables>;

pub type SchedulerStore = Arc<SchedulerStoreInner>;

pub fn open_scheduler_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<SchedulerStore> {
    open_arc_store(db, actor, SchedulerStoreInner::open)
}
