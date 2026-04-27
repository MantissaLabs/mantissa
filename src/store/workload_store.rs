use crate::store::open::open_arc_store;
use crate::workload::model::{WorkloadValue, parse_workload_timestamp, workload_phase_rank};
use chrono::{DateTime, Utc};
use crdt_store::adapter::{CompactingStoreMvRegAdapterSorted, MvRegCompactionRanker};
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::mvreg::MvRegEntry;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use std::cmp::Reverse;
use std::sync::Arc;
use uuid::Uuid;

pub struct WorkloadTables;

impl TableSet for WorkloadTables {
    const VALUES: &'static str = "workload_values";
    const TOMBS: &'static str = "workload_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "workload_tombs_by_observed";
    const META: &'static str = "workload_meta";
}

/// Workload compaction ranker used by the generic MVReg adapter.
pub struct WorkloadCompactionRank;

impl MvRegCompactionRanker<WorkloadValue, Uuid> for WorkloadCompactionRank {
    type Rank = (
        u64,
        u64,
        Option<DateTime<Utc>>,
        u8,
        bool,
        Uuid,
        Reverse<WorkloadValue>,
    );

    /// Ranks one workload value using the same causal order as workload selection.
    fn rank(entry: &MvRegEntry<WorkloadValue, Uuid>) -> Self::Rank {
        let value = entry.value();
        (
            value.task_epoch,
            value.phase_version,
            parse_workload_timestamp(&value.updated_at, &value.created_at),
            workload_phase_rank(&value.state),
            value.definition_complete,
            value.node_id,
            Reverse(value.clone()),
        )
    }
}

/// Store adapter for workload registers with domain-aware compaction enabled.
pub type WorkloadRegAdapter =
    CompactingStoreMvRegAdapterSorted<UuidKey, WorkloadValue, Uuid, WorkloadCompactionRank>;

pub type WorkloadStoreInner = CrdtMstStore<WorkloadRegAdapter, XXHash128, WorkloadTables>;

pub type WorkloadStore = Arc<WorkloadStoreInner>;

pub fn open_workload_store(db: Arc<redb::Database>, actor: Uuid) -> std::io::Result<WorkloadStore> {
    open_arc_store(db, actor, |db, actor| {
        WorkloadStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}
