use crate::agents::types::AgentRecordValue;
use crate::store::open::open_arc_store;
use crdt_store::adapter::MvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

/// Redb table names used by the replicated agent store.
pub struct AgentTables;

impl TableSet for AgentTables {
    const VALUES: &'static str = "agent_values";
    const TOMBS: &'static str = "agent_tombs";
    const META: &'static str = "agent_meta";
}

pub type AgentStoreInner =
    CrdtMstStore<MvRegAdapterSorted<UuidKey, AgentRecordValue, Uuid>, XXHash128, AgentTables>;

pub type AgentStore = Arc<AgentStoreInner>;

/// Opens the replicated agent store for one local actor identifier.
pub fn open_agent_store(db: Arc<redb::Database>, actor: Uuid) -> std::io::Result<AgentStore> {
    open_arc_store(db, actor, AgentStoreInner::open)
}
