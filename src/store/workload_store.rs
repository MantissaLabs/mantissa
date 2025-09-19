use crate::workload::container::ContainerState;
use crdt_store::adapter::MvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct WorkloadValue {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    pub created_at: String,
    pub command: Vec<String>,
    pub node_id: Uuid,
    pub node_name: String,
}

impl WorkloadValue {
    pub fn new(
        id: Uuid,
        name: impl Into<String>,
        image: impl Into<String>,
        state: ContainerState,
        created_at: impl Into<String>,
        command: Vec<String>,
        node_id: Uuid,
        node_name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            image: image.into(),
            state,
            created_at: created_at.into(),
            command,
            node_id,
            node_name: node_name.into(),
        }
    }
}

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
    let inner = WorkloadStoreInner::open(db, actor)?;
    Ok(Arc::new(inner))
}
