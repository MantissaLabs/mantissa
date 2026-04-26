use crate::store::open::open_arc_store;
use crate::volumes::types::{VolumeNodeStateValue, VolumeSpecValue};
use crdt_store::adapter::StoreMvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

/// Redb table names for replicated volume specifications.
pub struct VolumeSpecTables;

impl TableSet for VolumeSpecTables {
    const VALUES: &'static str = "volume_spec_values";
    const TOMBS: &'static str = "volume_spec_tombs";
    const META: &'static str = "volume_spec_meta";
}

/// Redb table names for replicated node-local volume state rows.
pub struct VolumeNodeTables;

impl TableSet for VolumeNodeTables {
    const VALUES: &'static str = "volume_node_values";
    const TOMBS: &'static str = "volume_node_tombs";
    const META: &'static str = "volume_node_meta";
}

/// Specialized MST/CRDT store for volume specifications.
pub type VolumeSpecStoreInner = CrdtMstStore<
    StoreMvRegAdapterSorted<UuidKey, VolumeSpecValue, Uuid>,
    XXHash128,
    VolumeSpecTables,
>;

/// Shared handle to the volume specification store.
pub type VolumeSpecStore = Arc<VolumeSpecStoreInner>;

/// Specialized MST/CRDT store for per-node volume state.
pub type VolumeNodeStoreInner = CrdtMstStore<
    StoreMvRegAdapterSorted<UuidKey, VolumeNodeStateValue, Uuid>,
    XXHash128,
    VolumeNodeTables,
>;

/// Shared handle to the volume node-state store.
pub type VolumeNodeStore = Arc<VolumeNodeStoreInner>;

/// Open or create the volume specification store scoped to the provided actor.
pub fn open_volume_spec_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<VolumeSpecStore> {
    open_arc_store(db, actor, VolumeSpecStoreInner::open)
}

/// Open or create the volume node-state store scoped to the provided actor.
pub fn open_volume_node_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<VolumeNodeStore> {
    open_arc_store(db, actor, VolumeNodeStoreInner::open)
}
