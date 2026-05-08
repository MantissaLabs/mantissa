use crate::store::replicated::compaction::ParsedOrRawTimestampRank;
use crate::store::replicated::open::open_arc_store;
use crate::volumes::types::{
    VolumeAccessMode, VolumeBindingMode, VolumeDriver, VolumeNodeState, VolumeNodeStateValue,
    VolumeReclaimPolicy, VolumeSpecValue, VolumeStatus,
};
use mantissa_store::adapter::{CompactingStoreMvRegAdapterSorted, MvRegCompactionRanker};
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::mvreg::MvRegEntry;
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use std::cmp::Reverse;
use std::sync::Arc;
use uuid::Uuid;

/// Redb table names for replicated volume specifications.
pub struct VolumeSpecTables;

impl TableSet for VolumeSpecTables {
    const VALUES: &'static str = "volume_spec_values";
    const TOMBS: &'static str = "volume_spec_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "volume_spec_tombs_by_observed";
    const META: &'static str = "volume_spec_meta";
}

/// Redb table names for replicated node-local volume state rows.
pub struct VolumeNodeTables;

impl TableSet for VolumeNodeTables {
    const VALUES: &'static str = "volume_node_values";
    const TOMBS: &'static str = "volume_node_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "volume_node_tombs_by_observed";
    const META: &'static str = "volume_node_meta";
}

/// Volume-spec compaction ranker used by the generic MVReg adapter.
pub struct VolumeSpecCompactionRank;

/// Total volume-spec ordering key matching the registry's canonical selector.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct VolumeSpecRank {
    volume_epoch: u64,
    phase_version: u64,
    updated_at: ParsedOrRawTimestampRank,
    status: VolumeStatus,
    bound_node_id: Option<Uuid>,
    bound_node_name: Option<String>,
    driver: VolumeDriver,
    access_mode: VolumeAccessMode,
    binding_mode: VolumeBindingMode,
    reclaim_policy: VolumeReclaimPolicy,
    requested_bytes: Option<u64>,
    reason: Option<String>,
    message: Option<String>,
    tie_breaker: Reverse<VolumeSpecValue>,
}

impl MvRegCompactionRanker<VolumeSpecValue, Uuid> for VolumeSpecCompactionRank {
    type Rank = VolumeSpecRank;

    /// Ranks one volume spec with the same deterministic order as the registry selector.
    fn rank(entry: &MvRegEntry<VolumeSpecValue, Uuid>) -> Self::Rank {
        let value = entry.value();
        VolumeSpecRank {
            volume_epoch: value.volume_epoch,
            phase_version: value.phase_version,
            updated_at: ParsedOrRawTimestampRank::new(&value.updated_at),
            status: value.status,
            bound_node_id: value.bound_node_id,
            bound_node_name: value.bound_node_name.clone(),
            driver: value.driver.clone(),
            access_mode: value.access_mode,
            binding_mode: value.binding_mode,
            reclaim_policy: value.reclaim_policy,
            requested_bytes: value.requested_bytes,
            reason: value.reason.clone(),
            message: value.message.clone(),
            tie_breaker: Reverse(value.clone()),
        }
    }
}

/// Volume-node compaction ranker used by the generic MVReg adapter.
pub struct VolumeNodeCompactionRank;

/// Total volume-node ordering key matching the registry's canonical selector.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct VolumeNodeRank {
    updated_at: ParsedOrRawTimestampRank,
    state: VolumeNodeState,
    published_task_ids: Vec<Uuid>,
    capacity_bytes: Option<u64>,
    used_bytes: Option<u64>,
    last_error: Option<String>,
    local_path: Option<String>,
    tie_breaker: Reverse<VolumeNodeStateValue>,
}

impl MvRegCompactionRanker<VolumeNodeStateValue, Uuid> for VolumeNodeCompactionRank {
    type Rank = VolumeNodeRank;

    /// Ranks one per-node volume state with the same order as the registry selector.
    fn rank(entry: &MvRegEntry<VolumeNodeStateValue, Uuid>) -> Self::Rank {
        let value = entry.value();
        VolumeNodeRank {
            updated_at: ParsedOrRawTimestampRank::new(&value.updated_at),
            state: value.state,
            published_task_ids: value.published_task_ids.clone(),
            capacity_bytes: value.capacity_bytes,
            used_bytes: value.used_bytes,
            last_error: value.last_error.clone(),
            local_path: value.local_path.clone(),
            tie_breaker: Reverse(value.clone()),
        }
    }
}

/// Store adapter for volume spec registers with domain-aware compaction enabled.
pub type VolumeSpecRegAdapter =
    CompactingStoreMvRegAdapterSorted<UuidKey, VolumeSpecValue, Uuid, VolumeSpecCompactionRank>;

/// Specialized MST/CRDT store for volume specifications.
pub type VolumeSpecStoreInner = CrdtMstStore<VolumeSpecRegAdapter, XXHash128, VolumeSpecTables>;

/// Shared handle to the volume specification store.
pub type VolumeSpecStore = Arc<VolumeSpecStoreInner>;

/// Store adapter for volume node-state registers with domain-aware compaction enabled.
pub type VolumeNodeRegAdapter = CompactingStoreMvRegAdapterSorted<
    UuidKey,
    VolumeNodeStateValue,
    Uuid,
    VolumeNodeCompactionRank,
>;

/// Specialized MST/CRDT store for per-node volume state.
pub type VolumeNodeStoreInner = CrdtMstStore<VolumeNodeRegAdapter, XXHash128, VolumeNodeTables>;

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
