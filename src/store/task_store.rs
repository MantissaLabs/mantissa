use crate::store::open::open_arc_store;
use crate::task::types::TaskValue;
use crdt_store::adapter::MvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

pub struct TaskTables;

impl TableSet for TaskTables {
    const VALUES: &'static str = "task_values";
    const TOMBS: &'static str = "task_tombs";
    const META: &'static str = "task_meta";
}

pub type TaskStoreInner =
    CrdtMstStore<MvRegAdapterSorted<UuidKey, TaskValue, Uuid>, XXHash128, TaskTables>;

pub type TaskStore = Arc<TaskStoreInner>;

pub fn open_task_store(db: Arc<redb::Database>, actor: Uuid) -> std::io::Result<TaskStore> {
    open_arc_store(db, actor, |db, actor| {
        TaskStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}
