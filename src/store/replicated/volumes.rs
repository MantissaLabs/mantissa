use crate::store::replicated::compaction::ParsedOrRawTimestampRank;
use crate::store::replicated::open::open_arc_store;
use crate::volumes::types::{
    VolumeAccessMode, VolumeBindingMode, VolumeDeletionRank, VolumeDriver, VolumeNodeState,
    VolumeNodeStateValue, VolumeReclaimPolicy, VolumeSpecValue, VolumeStatus,
};
use mantissa_store::adapter::{
    CompactingStoreMvRegAdapterSorted, MvRegCompactionRanker, RegAdapter,
};
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::mvreg::{MvReg, MvRegEntry, MvRegSnapshot};
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use std::cmp::Reverse;
use std::io;
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
    deletion_rank: VolumeDeletionRank,
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
            deletion_rank: value.deletion_rank(),
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
    volume_epoch: u64,
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
            volume_epoch: value.volume_epoch,
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

/// Generic volume-spec adapter used for codec, merge, and compaction delegation.
type BaseVolumeSpecRegAdapter =
    CompactingStoreMvRegAdapterSorted<UuidKey, VolumeSpecValue, Uuid, VolumeSpecCompactionRank>;

/// Store adapter that prevents stale volume lifecycle writers from undoing deletion markers.
pub struct VolumeSpecRegAdapter;

impl RegAdapter for VolumeSpecRegAdapter {
    type Key = UuidKey;
    type Actor = Uuid;
    type Reg = MvReg<VolumeSpecValue, Uuid>;
    type Value = VolumeSpecValue;
    type Snapshot = MvRegSnapshot<VolumeSpecValue>;

    /// Writes only values that advance the canonical volume lifecycle.
    fn upsert_reg(
        current: Option<Self::Reg>,
        actor: &Self::Actor,
        value: Self::Value,
    ) -> Self::Reg {
        let reg = current.unwrap_or_default();
        if let Some(current) = select_replicated_volume_spec(reg.snapshot())
            && !value.precedence_cmp(&current).is_gt()
        {
            return reg;
        }
        <BaseVolumeSpecRegAdapter as RegAdapter>::upsert_reg(Some(reg), actor, value)
    }

    /// Projects one register into the stable volume-spec snapshot.
    fn snapshot_reg(reg: &Self::Reg) -> Self::Snapshot {
        <BaseVolumeSpecRegAdapter as RegAdapter>::snapshot_reg(reg)
    }

    /// Encodes one volume identifier into its durable key representation.
    fn key_to_bytes(key: &Self::Key) -> Vec<u8> {
        <BaseVolumeSpecRegAdapter as RegAdapter>::key_to_bytes(key)
    }

    /// Decodes one durable volume identifier.
    fn key_from_bytes(bytes: &[u8]) -> io::Result<Self::Key> {
        <BaseVolumeSpecRegAdapter as RegAdapter>::key_from_bytes(bytes)
    }

    /// Encodes the writer actor for tombstone metadata.
    fn actor_to_bytes(actor: &Self::Actor) -> Vec<u8> {
        <BaseVolumeSpecRegAdapter as RegAdapter>::actor_to_bytes(actor)
    }

    /// Decodes the writer actor from tombstone metadata.
    fn actor_from_bytes(bytes: &[u8]) -> io::Result<Self::Actor> {
        <BaseVolumeSpecRegAdapter as RegAdapter>::actor_from_bytes(bytes)
    }

    /// Encodes one volume MV-register for durable storage and Sync.
    fn encode_reg(reg: &Self::Reg) -> mantissa_store::Result<Vec<u8>> {
        <BaseVolumeSpecRegAdapter as RegAdapter>::encode_reg(reg)
    }

    /// Decodes one volume MV-register from durable storage or Sync.
    fn decode_reg(bytes: &[u8]) -> mantissa_store::Result<Self::Reg> {
        <BaseVolumeSpecRegAdapter as RegAdapter>::decode_reg(bytes)
    }

    /// Compacts concurrent values with the same precedence used by registry reads.
    fn compact_reg(reg: Self::Reg, max_values: usize) -> mantissa_store::Result<Option<Self::Reg>> {
        <BaseVolumeSpecRegAdapter as RegAdapter>::compact_reg(reg, max_values)
    }

    /// Merges an inbound register while retaining concurrent lifecycle evidence.
    fn merge_regs(current: Option<Self::Reg>, incoming: Self::Reg) -> Self::Reg {
        <BaseVolumeSpecRegAdapter as RegAdapter>::merge_regs(current, incoming)
    }
}

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
    open_arc_store(db, actor, |db, actor| {
        VolumeSpecStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}

/// Open or create the volume node-state store scoped to the provided actor.
pub fn open_volume_node_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<VolumeNodeStore> {
    open_arc_store(db, actor, |db, actor| {
        VolumeNodeStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}

/// Selects the deterministic winning volume spec from one merged register snapshot.
fn select_replicated_volume_spec(
    snapshot: MvRegSnapshot<VolumeSpecValue>,
) -> Option<VolumeSpecValue> {
    snapshot
        .as_slice()
        .iter()
        .cloned()
        .max_by(VolumeSpecValue::precedence_cmp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volumes::types::{LocalVolumeOwnership, LocalVolumeSpec, VolumeSpecDraft};

    /// Builds one live volume generation for replicated-store ordering tests.
    fn live_volume(name: &str) -> VolumeSpecValue {
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: name.to_string(),
            driver: VolumeDriver::Local(LocalVolumeSpec::managed(LocalVolumeOwnership::Daemon)),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Delete,
            requested_bytes: None,
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        spec.status = VolumeStatus::Ready;
        spec
    }

    /// Opens one isolated temporary volume-spec store for a selected actor.
    fn temporary_spec_store(actor: Uuid) -> (tempfile::TempDir, VolumeSpecStore) {
        let dir = tempfile::tempdir().expect("create volume store tempdir");
        let db = Arc::new(
            redb::Database::create(dir.path().join("volumes.redb"))
                .expect("create volume store database"),
        );
        let store = open_volume_spec_store(db, actor).expect("open volume spec store");
        (dir, store)
    }

    /// A late local controller write must not replace either stage of semantic deletion.
    #[tokio::test]
    async fn stale_local_write_cannot_replace_delete_marker() {
        let (_dir, store) = temporary_spec_store(Uuid::new_v4());
        let live = live_volume("stale-local");
        let key = UuidKey::from(live.id);
        store
            .upsert(&key, live.clone())
            .await
            .expect("persist live volume");

        let mut deleting = live.clone();
        deleting.mark_deleting();
        store
            .upsert(&key, deleting.clone())
            .await
            .expect("persist deleting marker");

        let mut stale = live.clone();
        stale.phase_version = u64::MAX;
        stale.updated_at = "9999-12-31T23:59:59Z".to_string();
        store
            .upsert(&key, stale.clone())
            .await
            .expect("attempt stale controller write");
        let snapshot = store
            .get_snapshot(&key)
            .expect("read deleting snapshot")
            .expect("deleting snapshot present");
        assert_eq!(
            select_replicated_volume_spec(snapshot),
            Some(deleting.clone())
        );

        let mut deleted = deleting;
        deleted.mark_deleted();
        store
            .upsert(&key, deleted.clone())
            .await
            .expect("persist deleted marker");
        store
            .upsert(&key, stale)
            .await
            .expect("attempt stale write after cleanup");
        let snapshot = store
            .get_snapshot(&key)
            .expect("read deleted snapshot")
            .expect("deleted snapshot present");
        assert_eq!(
            select_replicated_volume_spec(snapshot),
            Some(deleted.clone())
        );

        let mut recreated = live_volume("stale-local");
        recreated.recreate_after(&deleted);
        store
            .upsert(&key, recreated.clone())
            .await
            .expect("persist recreated generation");
        store
            .upsert(&key, deleted)
            .await
            .expect("attempt late old-generation deletion");
        let snapshot = store
            .get_snapshot(&key)
            .expect("read recreated snapshot")
            .expect("recreated snapshot present");
        assert_eq!(select_replicated_volume_spec(snapshot), Some(recreated));
    }

    /// Sync must retain a delete marker as canonical over a concurrent stale live register.
    #[tokio::test]
    async fn stale_remote_register_cannot_hide_delete_marker() {
        let (_source_dir, source) = temporary_spec_store(Uuid::new_v4());
        let (_target_dir, target) = temporary_spec_store(Uuid::new_v4());
        let live = live_volume("stale-remote");
        let key = UuidKey::from(live.id);

        let mut stale = live.clone();
        stale.phase_version = u64::MAX;
        stale.updated_at = "9999-12-31T23:59:59Z".to_string();
        source
            .upsert(&key, stale)
            .await
            .expect("persist remote stale row");

        let mut deleted = live;
        deleted.mark_deleting();
        deleted.mark_deleted();
        target
            .upsert(&key, deleted.clone())
            .await
            .expect("persist local delete marker");

        let (registers, tombstones) = source
            .load_all_regs()
            .expect("load remote source registers");
        target
            .apply_delta_chunk_update_mst(registers, tombstones)
            .await
            .expect("merge remote source registers");

        let snapshot = target
            .get_snapshot(&key)
            .expect("read merged snapshot")
            .expect("merged snapshot present");
        assert_eq!(select_replicated_volume_spec(snapshot), Some(deleted));
    }
}
