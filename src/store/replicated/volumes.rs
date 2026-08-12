use crate::store::replicated::compaction::ParsedOrRawTimestampRank;
use crate::store::replicated::open::open_arc_store;
use crate::volumes::types::{
    ReplicatedVolumeCapacityRequest, ReplicatedVolumeGroupStatusValue, ReplicatedVolumePlan,
    VolumeNodeState, VolumeNodeStateValue, VolumeSpecValue, VolumeStatus,
};
use mantissa_store::adapter::{
    CompactingStoreMvRegAdapterSorted, MvRegCompactionRanker, RegAdapter, StoreMvRegAdapterSorted,
};
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::mvreg::{MvReg, MvRegEntry, MvRegSnapshot};
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use std::cmp::{Ordering, Reverse};
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

/// Redb table names for immutable replicated-volume bootstrap plans.
pub struct ReplicatedVolumePlanTables;

impl TableSet for ReplicatedVolumePlanTables {
    const VALUES: &'static str = "replicated_volume_plan_values";
    const TOMBS: &'static str = "replicated_volume_plan_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "replicated_volume_plan_tombs_by_observed";
    const META: &'static str = "replicated_volume_plan_meta";
}

/// Redb table names for replicated-volume group-status reports.
pub struct ReplicatedVolumeGroupStatusTables;

impl TableSet for ReplicatedVolumeGroupStatusTables {
    const VALUES: &'static str = "replicated_volume_group_status_values";
    const TOMBS: &'static str = "replicated_volume_group_status_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "replicated_volume_group_status_tombs_by_observed";
    const META: &'static str = "replicated_volume_group_status_meta";
}

/// Redb table names for replicated-volume capacity requests.
pub struct ReplicatedVolumeCapacityRequestTables;

impl TableSet for ReplicatedVolumeCapacityRequestTables {
    const VALUES: &'static str = "replicated_volume_capacity_request_values";
    const TOMBS: &'static str = "replicated_volume_capacity_request_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "replicated_volume_capacity_request_tombs_by_observed";
    const META: &'static str = "replicated_volume_capacity_request_meta";
}

/// Volume-spec compaction ranker used by the generic MVReg adapter.
pub struct VolumeSpecCompactionRank;

/// Total volume-spec ordering key that delegates to the registry's canonical selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeSpecRank(VolumeSpecValue);

impl Ord for VolumeSpecRank {
    /// Uses the desired-row precedence rule directly so compaction cannot drift from reads.
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.precedence_cmp(&other.0)
    }
}

impl PartialOrd for VolumeSpecRank {
    /// Returns the total desired-row order used by both reads and compaction.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl MvRegCompactionRanker<VolumeSpecValue, Uuid> for VolumeSpecCompactionRank {
    type Rank = VolumeSpecRank;

    /// Ranks one volume spec with the same deterministic order as the registry selector.
    fn rank(entry: &MvRegEntry<VolumeSpecValue, Uuid>) -> Self::Rank {
        VolumeSpecRank(entry.value().clone())
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
    reserved_capacity_bytes: Option<u64>,
    prepared_capacity_bytes: Option<u64>,
    served_capacity_bytes: Option<u64>,
    device_capacity_bytes: Option<u64>,
    filesystem_expansion_pending: bool,
    used_bytes: Option<u64>,
    last_error: Option<String>,
    local_path: Option<String>,
    group_id: Option<Uuid>,
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
            reserved_capacity_bytes: value.reserved_capacity_bytes,
            prepared_capacity_bytes: value.prepared_capacity_bytes,
            served_capacity_bytes: value.served_capacity_bytes,
            device_capacity_bytes: value.device_capacity_bytes,
            filesystem_expansion_pending: value.filesystem_expansion_pending,
            used_bytes: value.used_bytes,
            last_error: value.last_error.clone(),
            local_path: value.local_path.clone(),
            group_id: value.group_id,
            tie_breaker: Reverse(value.clone()),
        }
    }
}

/// Group-status compaction ranker used by the generic MVReg adapter.
pub struct ReplicatedVolumeGroupStatusCompactionRank;

/// Total group-status ordering key matching registry reads.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ReplicatedVolumeGroupStatusRank {
    volume_epoch: u64,
    committed_index: u64,
    updated_at: ParsedOrRawTimestampRank,
    status: VolumeStatus,
    leader_node_id: Option<Uuid>,
    attached_node_id: Option<Uuid>,
    tie_breaker: Reverse<ReplicatedVolumeGroupStatusValue>,
}

impl MvRegCompactionRanker<ReplicatedVolumeGroupStatusValue, Uuid>
    for ReplicatedVolumeGroupStatusCompactionRank
{
    type Rank = ReplicatedVolumeGroupStatusRank;

    /// Ranks group reports with the same deterministic order as registry reads.
    fn rank(entry: &MvRegEntry<ReplicatedVolumeGroupStatusValue, Uuid>) -> Self::Rank {
        let value = entry.value();
        ReplicatedVolumeGroupStatusRank {
            volume_epoch: value.volume_epoch,
            committed_index: value.committed_index,
            updated_at: ParsedOrRawTimestampRank::new(&value.updated_at),
            status: value.status,
            leader_node_id: value.leader_node_id,
            attached_node_id: value.attached_node_id,
            tie_breaker: Reverse(value.clone()),
        }
    }
}

/// Capacity-request compaction ranker used by registry reads and MST sync.
pub struct ReplicatedVolumeCapacityRequestCompactionRank;

/// Total capacity-request ordering key matching the public winner selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicatedVolumeCapacityRequestRank(ReplicatedVolumeCapacityRequest);

impl Ord for ReplicatedVolumeCapacityRequestRank {
    /// Uses the desired-capacity precedence rule without a second ordering.
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.precedence_cmp(&other.0)
    }
}

impl PartialOrd for ReplicatedVolumeCapacityRequestRank {
    /// Returns the total desired-capacity order used by reads and compaction.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl MvRegCompactionRanker<ReplicatedVolumeCapacityRequest, Uuid>
    for ReplicatedVolumeCapacityRequestCompactionRank
{
    type Rank = ReplicatedVolumeCapacityRequestRank;

    /// Ranks one request by generation, revision, and deterministic request ID.
    fn rank(entry: &MvRegEntry<ReplicatedVolumeCapacityRequest, Uuid>) -> Self::Rank {
        ReplicatedVolumeCapacityRequestRank(entry.value().clone())
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
        if let Some(current) = select_replicated_volume_spec(reg.snapshot()) {
            if current.driver.is_replicated()
                && current.volume_epoch == value.volume_epoch
                && !current.has_same_request(&value)
            {
                return reg;
            }
            if !value.precedence_cmp(&current).is_gt() {
                return reg;
            }
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
        if winning_generation_has_request_conflict(&reg) {
            return Ok(None);
        }
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

/// Non-compacting plan registers retain conflicts so no side effects are guessed.
pub type ReplicatedVolumePlanRegAdapter =
    StoreMvRegAdapterSorted<UuidKey, ReplicatedVolumePlan, Uuid>;

/// Specialized MST/CRDT store for immutable bootstrap plans.
pub type ReplicatedVolumePlanStoreInner =
    CrdtMstStore<ReplicatedVolumePlanRegAdapter, XXHash128, ReplicatedVolumePlanTables>;

/// Shared handle to the replicated-volume plan store.
pub type ReplicatedVolumePlanStore = Arc<ReplicatedVolumePlanStoreInner>;

/// Store adapter for replicated-volume group-status reports.
pub type ReplicatedVolumeGroupStatusRegAdapter = CompactingStoreMvRegAdapterSorted<
    UuidKey,
    ReplicatedVolumeGroupStatusValue,
    Uuid,
    ReplicatedVolumeGroupStatusCompactionRank,
>;

/// Specialized MST/CRDT store for replicated-volume group-status reports.
pub type ReplicatedVolumeGroupStatusStoreInner = CrdtMstStore<
    ReplicatedVolumeGroupStatusRegAdapter,
    XXHash128,
    ReplicatedVolumeGroupStatusTables,
>;

/// Shared handle to the replicated-volume group-status store.
pub type ReplicatedVolumeGroupStatusStore = Arc<ReplicatedVolumeGroupStatusStoreInner>;

/// Store adapter that compacts capacity requests to their canonical winner.
pub type ReplicatedVolumeCapacityRequestRegAdapter = CompactingStoreMvRegAdapterSorted<
    UuidKey,
    ReplicatedVolumeCapacityRequest,
    Uuid,
    ReplicatedVolumeCapacityRequestCompactionRank,
>;

/// Specialized MST/CRDT store for desired replicated-volume capacity.
pub type ReplicatedVolumeCapacityRequestStoreInner = CrdtMstStore<
    ReplicatedVolumeCapacityRequestRegAdapter,
    XXHash128,
    ReplicatedVolumeCapacityRequestTables,
>;

/// Shared handle to the replicated-volume capacity-request store.
pub type ReplicatedVolumeCapacityRequestStore = Arc<ReplicatedVolumeCapacityRequestStoreInner>;

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

/// Opens the immutable bootstrap-plan store for replicated volumes.
pub fn open_replicated_volume_plan_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<ReplicatedVolumePlanStore> {
    open_arc_store(db, actor, |db, actor| {
        ReplicatedVolumePlanStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}

/// Opens the group-status store for replicated volumes.
pub fn open_replicated_volume_group_status_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<ReplicatedVolumeGroupStatusStore> {
    open_arc_store(db, actor, |db, actor| {
        ReplicatedVolumeGroupStatusStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}

/// Opens the desired-capacity store for replicated volumes.
pub fn open_replicated_volume_capacity_request_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<ReplicatedVolumeCapacityRequestStore> {
    open_arc_store(db, actor, |db, actor| {
        ReplicatedVolumeCapacityRequestStoreInner::builder(db, actor)
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

/// Returns whether active values disagree on immutable fields in the newest generation.
fn winning_generation_has_request_conflict(reg: &MvReg<VolumeSpecValue, Uuid>) -> bool {
    let Some(volume_epoch) = reg
        .entries()
        .iter()
        .map(|entry| entry.value().volume_epoch)
        .max()
    else {
        return false;
    };
    let mut current = reg
        .entries()
        .iter()
        .filter(|entry| entry.value().volume_epoch == volume_epoch)
        .map(MvRegEntry::value);
    let Some(first) = current.next() else {
        return false;
    };
    current.any(|value| !first.has_same_request(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volumes::types::{
        FilesystemOwnership, LocalVolumeSpec, VolumeAccessMode, VolumeBindingMode, VolumeDriver,
        VolumeReclaimPolicy, VolumeSpecDraft,
    };
    use mantissa_store::mvreg::VectorClock;

    /// Builds one live volume generation for replicated-store ordering tests.
    fn live_volume(name: &str) -> VolumeSpecValue {
        VolumeSpecValue::new(VolumeSpecDraft {
            name: name.to_string(),
            driver: VolumeDriver::Local(LocalVolumeSpec::managed(FilesystemOwnership::Daemon)),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Delete,
            initial_capacity_bytes: None,
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        })
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

    /// Compaction must retain the same later binding that ordinary registry reads select.
    #[test]
    fn compaction_preserves_the_canonical_binding_revision() {
        let mut older = live_volume("binding-compaction");
        older.binding_revision = 1;
        older.binding_operation_id = Some(Uuid::from_u128(u128::MAX));
        older.updated_at = "9999-12-31T23:59:59Z".to_string();

        let mut newer = older.clone();
        newer.binding_revision = 2;
        newer.binding_operation_id = Some(Uuid::from_u128(1));
        newer.updated_at = "1970-01-01T00:00:00Z".to_string();

        assert!(newer.precedence_cmp(&older).is_gt());

        let mut older_clock = VectorClock::new();
        older_clock.apply(Uuid::from_u128(1), 1);
        let mut newer_clock = VectorClock::new();
        newer_clock.apply(Uuid::from_u128(2), 1);
        let register = MvReg::from_entries(vec![
            MvRegEntry::new(older_clock, older),
            MvRegEntry::new(newer_clock, newer.clone()),
        ]);

        let compacted = VolumeSpecRegAdapter::compact_reg(register, 1)
            .expect("compact volume binding register")
            .expect("concurrent binding register requires compaction");
        assert_eq!(
            select_replicated_volume_spec(compacted.snapshot()),
            Some(newer)
        );
    }

    /// Garbage collection must not erase an immutable request conflict.
    #[test]
    fn compaction_preserves_winning_generation_request_conflicts() {
        let mut left = live_volume("request-conflict");
        left.plan_coordinator_node_id = None;
        let mut right = left.clone();
        right.initial_capacity_bytes = Some(4096);

        let mut left_clock = VectorClock::new();
        left_clock.apply(Uuid::from_u128(1), 1);
        let mut right_clock = VectorClock::new();
        right_clock.apply(Uuid::from_u128(2), 1);
        let register = MvReg::from_entries(vec![
            MvRegEntry::new(left_clock, left),
            MvRegEntry::new(right_clock, right),
        ]);

        assert!(winning_generation_has_request_conflict(&register));
        assert!(
            VolumeSpecRegAdapter::compact_reg(register, 1)
                .expect("inspect conflicting volume register")
                .is_none()
        );
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
        deleting
            .request_deleted(true)
            .expect("request terminal deletion");
        store
            .upsert(&key, deleting.clone())
            .await
            .expect("persist deleting marker");

        let mut stale = live.clone();
        stale.lifecycle.revision = u64::MAX;
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

        let mut recreated = live_volume("stale-local");
        recreated
            .recreate_after(&deleting)
            .expect("create next generation");
        store
            .upsert(&key, recreated.clone())
            .await
            .expect("persist recreated generation");
        store
            .upsert(&key, deleting)
            .await
            .expect("attempt late old-generation deletion");
        let snapshot = store
            .get_snapshot(&key)
            .expect("read recreated snapshot")
            .expect("recreated snapshot present");
        assert_eq!(select_replicated_volume_spec(snapshot), Some(recreated));
    }

    /// Normal attachment updates from an older retain cycle must not replace lifecycle work.
    #[tokio::test]
    async fn stale_normal_write_cannot_replace_retain_or_restore() {
        let (_dir, store) = temporary_spec_store(Uuid::new_v4());
        let live = live_volume("stale-retain");
        let key = UuidKey::from(live.id);
        store
            .upsert(&key, live.clone())
            .await
            .expect("persist live volume");

        let mut retaining = live.clone();
        retaining.request_retained().expect("request retention");
        store
            .upsert(&key, retaining.clone())
            .await
            .expect("persist retaining volume");

        let mut stale_live = live;
        stale_live.updated_at = "9999-12-31T23:59:59Z".to_string();
        store
            .upsert(&key, stale_live)
            .await
            .expect("attempt stale normal update");
        assert_eq!(
            store
                .get_snapshot(&key)
                .expect("read retaining snapshot")
                .and_then(select_replicated_volume_spec),
            Some(retaining.clone())
        );

        let mut restoring = retaining.clone();
        restoring.request_live().expect("request live service");
        store
            .upsert(&key, restoring.clone())
            .await
            .expect("persist restoring volume");

        let mut stale_retained = retaining;
        stale_retained.updated_at = "9999-12-31T23:59:59Z".to_string();
        store
            .upsert(&key, stale_retained)
            .await
            .expect("attempt stale retained update");
        assert_eq!(
            store
                .get_snapshot(&key)
                .expect("read restoring snapshot")
                .and_then(select_replicated_volume_spec),
            Some(restoring.clone())
        );

        assert_eq!(
            store
                .get_snapshot(&key)
                .expect("read restored snapshot")
                .and_then(select_replicated_volume_spec),
            Some(restoring)
        );
    }

    /// Sync must retain a delete marker as canonical over a concurrent stale live register.
    #[tokio::test]
    async fn stale_remote_register_cannot_hide_delete_marker() {
        let (_source_dir, source) = temporary_spec_store(Uuid::new_v4());
        let (_target_dir, target) = temporary_spec_store(Uuid::new_v4());
        let live = live_volume("stale-remote");
        let key = UuidKey::from(live.id);

        let mut stale = live.clone();
        stale.lifecycle.revision = u64::MAX;
        stale.updated_at = "9999-12-31T23:59:59Z".to_string();
        source
            .upsert(&key, stale)
            .await
            .expect("persist remote stale row");

        let mut deleted = live;
        deleted
            .request_deleted(true)
            .expect("request terminal deletion");
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

    /// Merging an older normal row must converge on a concurrent retain request everywhere.
    #[tokio::test]
    async fn stale_remote_register_cannot_hide_retain_request() {
        let (_left_dir, left) = temporary_spec_store(Uuid::new_v4());
        let (_right_dir, right) = temporary_spec_store(Uuid::new_v4());
        let live = live_volume("remote-retain");
        let key = UuidKey::from(live.id);

        let mut stale = live.clone();
        stale.updated_at = "9999-12-31T23:59:59Z".to_string();
        left.upsert(&key, stale).await.expect("persist stale row");

        let mut retaining = live;
        retaining.request_retained().expect("request retention");
        right
            .upsert(&key, retaining.clone())
            .await
            .expect("persist retain request");

        let (right_registers, right_tombstones) =
            right.load_all_regs().expect("load retain registers");
        left.apply_delta_chunk_update_mst(right_registers, right_tombstones)
            .await
            .expect("merge retain request into stale node");
        let (left_registers, left_tombstones) =
            left.load_all_regs().expect("load merged registers");
        right
            .apply_delta_chunk_update_mst(left_registers, left_tombstones)
            .await
            .expect("merge converged registers into retain node");

        for store in [&left, &right] {
            let snapshot = store
                .get_snapshot(&key)
                .expect("read converged retain snapshot")
                .expect("converged retain snapshot present");
            assert_eq!(
                select_replicated_volume_spec(snapshot),
                Some(retaining.clone())
            );
        }
    }
}
