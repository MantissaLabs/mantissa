use crate::jobs::types::JobSpecValue;
use crate::store::open::open_arc_store;
use crdt_store::adapter::MvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

/// Redb table names used by the replicated job store.
pub struct JobTables;

impl TableSet for JobTables {
    const VALUES: &'static str = "job_values";
    const TOMBS: &'static str = "job_tombs";
    const META: &'static str = "job_meta";
}

pub type JobStoreInner =
    CrdtMstStore<MvRegAdapterSorted<UuidKey, JobSpecValue, Uuid>, XXHash128, JobTables>;

pub type JobStore = Arc<JobStoreInner>;

/// Opens the replicated job store for one local actor identifier.
pub fn open_job_store(db: Arc<redb::Database>, actor: Uuid) -> std::io::Result<JobStore> {
    open_arc_store(db, actor, JobStoreInner::open)
}
