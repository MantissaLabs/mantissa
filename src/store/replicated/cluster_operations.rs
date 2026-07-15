use crate::cluster::operations::ClusterOperationRecord;
use crate::store::replicated::open::open_arc_store;
use mantissa_store::adapter::RegAdapter;
use mantissa_store::codec::{MvRegStoreCodec, StoreActorCodec, StoreRegisterCodec};
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::mvreg::{MvReg, MvRegEntry, MvRegSnapshot};
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use std::io;
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

/// Store adapter for operation rows with domain-aware stale-write rejection.
pub struct ClusterOperationRegAdapter;

impl RegAdapter for ClusterOperationRegAdapter {
    type Key = UuidKey;
    type Actor = Uuid;
    type Reg = MvReg<ClusterOperationRecord, Uuid>;
    type Value = ClusterOperationRecord;
    type Snapshot = MvRegSnapshot<ClusterOperationRecord>;

    /// Merges one local operation write into the current register.
    ///
    /// The stale-write check runs inside the store write transaction via
    /// `CrdtMstStore::upsert`, so a late abort cannot dominate a committed or
    /// finalized row simply because it has a newer local vector-clock entry.
    fn upsert_reg(
        current: Option<Self::Reg>,
        actor: &Self::Actor,
        value: Self::Value,
    ) -> Self::Reg {
        let mut reg = current.unwrap_or_default();
        if let Some(current) = select_replicated_operation(reg.snapshot())
            && !value.supersedes(&current)
        {
            return reg;
        }

        reg.write(*actor, value);
        reg
    }

    /// Projects one register into the deterministic replicated operation snapshot.
    fn snapshot_reg(reg: &Self::Reg) -> Self::Snapshot {
        reg.snapshot()
    }

    /// Encodes an operation UUID key into its durable byte representation.
    fn key_to_bytes(key: &Self::Key) -> Vec<u8> {
        key.as_ref().to_vec()
    }

    /// Decodes an operation UUID key from its durable byte representation.
    fn key_from_bytes(bytes: &[u8]) -> io::Result<Self::Key> {
        UuidKey::try_from(bytes).map_err(Into::into)
    }

    /// Encodes the writer actor into stable tombstone metadata bytes.
    fn actor_to_bytes(actor: &Self::Actor) -> Vec<u8> {
        actor.encode_store_actor()
    }

    /// Decodes the writer actor from stable tombstone metadata bytes.
    fn actor_from_bytes(bytes: &[u8]) -> io::Result<Self::Actor> {
        Uuid::decode_store_actor(bytes).map_err(|error| io::Error::other(error.to_string()))
    }

    /// Encodes one operation MV-register row for durable storage and sync.
    fn encode_reg(reg: &Self::Reg) -> mantissa_store::Result<Vec<u8>> {
        MvRegStoreCodec::<ClusterOperationRecord, Uuid>::encode_store_reg(reg)
    }

    /// Decodes one operation MV-register row from durable storage or sync.
    fn decode_reg(bytes: &[u8]) -> mantissa_store::Result<Self::Reg> {
        MvRegStoreCodec::<ClusterOperationRecord, Uuid>::decode_store_reg(bytes)
    }

    /// Compacts concurrent operation values using the same winner order as reads.
    fn compact_reg(
        mut reg: Self::Reg,
        max_values: usize,
    ) -> mantissa_store::Result<Option<Self::Reg>> {
        Ok(reg
            .compact_with(max_values, cluster_operation_entry_rank)
            .then_some(reg))
    }

    /// Merges one inbound register with the current durable register.
    fn merge_regs(current: Option<Self::Reg>, incoming: Self::Reg) -> Self::Reg {
        match current {
            Some(mut current) => {
                current.merge(incoming);
                current
            }
            None => incoming,
        }
    }
}

/// Specialized MST/CRDT store for split/merge operation records.
pub type ClusterOperationDomainStoreInner =
    CrdtMstStore<ClusterOperationRegAdapter, XXHash128, ClusterOperationTables>;

/// Shared handle to the replicated split/merge operation ledger.
pub type ClusterOperationDomainStore = Arc<ClusterOperationDomainStoreInner>;

/// Topology-facing handle for the replicated split/merge operation ledger.
#[derive(Clone)]
pub struct ClusterOperationStore {
    domain: ClusterOperationDomainStore,
}

impl ClusterOperationStore {
    /// Opens the replicated split/merge operation ledger for the provided local actor.
    pub fn new(db: Arc<redb::Database>, actor: Uuid) -> io::Result<Self> {
        Ok(Self {
            domain: open_cluster_operation_domain_store(db, actor)?,
        })
    }

    /// Returns the replicated operation domain used by global metadata sync.
    pub fn domain_store(&self) -> ClusterOperationDomainStore {
        self.domain.clone()
    }

    /// Rebuilds the in-memory MST for the replicated operation ledger.
    pub async fn rebuild_mst_from_disk(&self) -> io::Result<()> {
        self.domain
            .rebuild_mst_from_disk()
            .await
            .map_err(io::Error::other)
    }

    /// Persists one operation record into the replicated ledger.
    pub async fn put_record(&self, operation: &ClusterOperationRecord) -> io::Result<()> {
        self.domain
            .upsert(&UuidKey::from(operation.id), operation.clone())
            .await
            .map_err(io::Error::other)
    }

    /// Loads the deterministic winning operation row by operation id.
    pub fn get_record(&self, id: Uuid) -> io::Result<Option<ClusterOperationRecord>> {
        let snapshot = self
            .domain
            .get_snapshot(&UuidKey::from(id))
            .map_err(io::Error::other)?;
        Ok(snapshot.and_then(select_replicated_operation))
    }

    /// Lists all deterministic winning operation rows currently present.
    pub fn list_records(&self) -> io::Result<Vec<ClusterOperationRecord>> {
        let (snapshots, _) = self.domain.load_all().map_err(io::Error::other)?;
        let mut out = Vec::with_capacity(snapshots.len());
        for (_key, snapshot) in snapshots {
            let Some(operation) = select_replicated_operation(snapshot) else {
                continue;
            };
            out.push(operation);
        }
        out.sort_by_key(|operation| operation.id);
        Ok(out)
    }

    /// Tombstones multiple operation rows and returns how many live rows were removed.
    pub async fn delete_many(&self, ids: &[Uuid]) -> io::Result<usize> {
        let mut removed = 0usize;
        for id in ids {
            let key = UuidKey::from(*id);
            if self
                .domain
                .get_snapshot(&key)
                .map_err(io::Error::other)?
                .is_none()
            {
                continue;
            }
            let _sequence = self.domain.remove(&key).await.map_err(io::Error::other)?;
            removed = removed.saturating_add(1);
        }
        Ok(removed)
    }
}

/// Opens the replicated split/merge operation ledger for one local actor.
pub fn open_cluster_operation_domain_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> io::Result<ClusterOperationDomainStore> {
    open_arc_store(db, actor, |db, actor| {
        ClusterOperationDomainStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}

/// Ranks one active operation register entry for deterministic MVReg compaction.
fn cluster_operation_entry_rank(
    entry: &MvRegEntry<ClusterOperationRecord, Uuid>,
) -> (u8, u64, Uuid, String, ClusterOperationRecord) {
    let operation = entry.value();
    (
        operation.stage.rank(),
        operation.updated_at_unix_ms,
        operation.id,
        operation.details.clone(),
        operation.clone(),
    )
}

/// Selects the winning replicated operation row from a merged MV-register snapshot.
fn select_replicated_operation(
    snapshot: MvRegSnapshot<ClusterOperationRecord>,
) -> Option<ClusterOperationRecord> {
    snapshot
        .as_slice()
        .iter()
        .cloned()
        .max_by(|left, right| left.precedence_cmp(right))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::operations::{
        ClusterOperationKind, ClusterOperationStage, MergeServicePolicy, SplitNetworkPolicy,
        SplitServicePolicy,
    };
    use crate::cluster::{ClusterId, ClusterViewId};
    use tempfile::tempdir;

    /// Builds one deterministic operation record for store adapter tests.
    fn operation(
        id: Uuid,
        stage: ClusterOperationStage,
        updated_at_unix_ms: u64,
    ) -> ClusterOperationRecord {
        let source = ClusterViewId::legacy_default();
        ClusterOperationRecord {
            id,
            submitted_by_node_id: Uuid::from_u128(0x700),
            kind: ClusterOperationKind::Merge,
            stage,
            dry_run: false,
            created_at_unix_ms: 1,
            depends_on_operation_id: None,
            source_views: vec![source],
            target_views: vec![ClusterViewId::new(
                ClusterId::from_uuid(Uuid::from_u128(0x500)),
                source.epoch.saturating_add(1),
            )],
            target_cluster_names: Vec::new(),
            split_assignments: Vec::new(),
            split_service_policy: SplitServicePolicy::default(),
            split_network_policy: SplitNetworkPolicy::default(),
            merge_service_policy: MergeServicePolicy::default(),
            updated_at_unix_ms,
            details: format!("stage={stage:?}"),
        }
    }

    /// Ensures a stale same-actor write cannot dominate a newer terminal stage.
    #[test]
    fn cluster_operation_adapter_rejects_stale_same_actor_write() {
        let actor = Uuid::from_u128(0xA);
        let operation_id = Uuid::from_u128(0xB);
        let finalized = operation(operation_id, ClusterOperationStage::Finalized, 10);
        let stale_abort = operation(operation_id, ClusterOperationStage::Aborted, 11);

        let reg = ClusterOperationRegAdapter::upsert_reg(None, &actor, finalized.clone());
        let reg = ClusterOperationRegAdapter::upsert_reg(Some(reg), &actor, stale_abort);
        let selected = select_replicated_operation(reg.snapshot()).expect("selected operation");

        assert_eq!(selected, finalized);
    }

    /// Ensures retention deletion writes replicated tombstones instead of local-only deletes.
    #[tokio::test]
    async fn cluster_operation_store_delete_many_writes_tombstones() {
        let dir = tempdir().expect("create temp dir");
        let db = Arc::new(
            redb::Database::create(dir.path().join("operations.redb"))
                .expect("create redb database"),
        );
        let operation_id = Uuid::from_u128(0xC);
        let store = ClusterOperationStore::new(db, Uuid::from_u128(0xD)).expect("open store");
        store
            .put_record(&operation(
                operation_id,
                ClusterOperationStage::Finalized,
                12,
            ))
            .await
            .expect("persist operation");

        let removed = store
            .delete_many(&[operation_id])
            .await
            .expect("delete operation");

        assert_eq!(removed, 1);
        assert!(
            store
                .get_record(operation_id)
                .expect("read deleted operation")
                .is_none()
        );
        assert!(
            store
                .domain_store()
                .has_tombstone(&UuidKey::from(operation_id))
                .expect("read operation tombstone")
        );
    }
}
