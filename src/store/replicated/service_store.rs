use crate::services::types::ServiceSpecValue;
use crate::store::replicated::open::open_arc_store;
use mantissa_store::adapter::StoreMvRegAdapterSorted;
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

pub struct ServiceTables;

impl TableSet for ServiceTables {
    const VALUES: &'static str = "service_values";
    const TOMBS: &'static str = "service_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "service_tombs_by_observed";
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
