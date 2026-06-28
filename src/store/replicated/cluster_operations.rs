use crate::cluster::operations::ClusterOperationRecord;
use crate::store::replicated::open::open_arc_store;
use mantissa_store::adapter::StoreMvRegAdapterSorted;
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

/// Cluster-operation ledger tables replicated through the global metadata plane.
pub struct ClusterOperationTables;

impl TableSet for ClusterOperationTables {
    const VALUES: &'static str = "cluster_operation_values";
    const TOMBS: &'static str = "cluster_operation_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "cluster_operation_tombs_by_observed";
    const META: &'static str = "cluster_operation_meta";
}

/// Specialized MST/CRDT store for split/merge operation records.
pub type ClusterOperationDomainStoreInner = CrdtMstStore<
    StoreMvRegAdapterSorted<UuidKey, ClusterOperationRecord, Uuid>,
    XXHash128,
    ClusterOperationTables,
>;

/// Shared handle to the replicated split/merge operation ledger.
pub type ClusterOperationDomainStore = Arc<ClusterOperationDomainStoreInner>;

/// Opens the replicated split/merge operation ledger for one local actor.
pub fn open_cluster_operation_domain_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<ClusterOperationDomainStore> {
    open_arc_store(db, actor, ClusterOperationDomainStoreInner::open)
}
