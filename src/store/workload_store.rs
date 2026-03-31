use crate::store::open::open_arc_store;
use crate::workload::model::WorkloadValue;
use crdt_store::adapter::MvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

pub struct WorkloadTables;

impl TableSet for WorkloadTables {
    const VALUES: &'static str = "workload_values";
    const TOMBS: &'static str = "workload_tombs";
    const META: &'static str = "workload_meta";
}

pub type WorkloadStoreInner =
    CrdtMstStore<MvRegAdapterSorted<UuidKey, WorkloadValue, Uuid>, XXHash128, WorkloadTables>;

pub type WorkloadStore = Arc<WorkloadStoreInner>;

pub fn open_workload_store(db: Arc<redb::Database>, actor: Uuid) -> std::io::Result<WorkloadStore> {
    open_arc_store(db, actor, |db, actor| {
        WorkloadStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}
