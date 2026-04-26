use crate::services::types::ServiceSpecValue;
use crate::store::open::open_arc_store;
use crdt_store::adapter::StoreMvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

pub struct ServiceTables;

impl TableSet for ServiceTables {
    const VALUES: &'static str = "service_values";
    const TOMBS: &'static str = "service_tombs";
    const META: &'static str = "service_meta";
}

pub type ServiceStoreInner = CrdtMstStore<
    StoreMvRegAdapterSorted<UuidKey, ServiceSpecValue, Uuid>,
    XXHash128,
    ServiceTables,
>;

pub type ServiceStore = Arc<ServiceStoreInner>;

pub fn open_service_store(db: Arc<redb::Database>, actor: Uuid) -> std::io::Result<ServiceStore> {
    open_arc_store(db, actor, ServiceStoreInner::open)
}
