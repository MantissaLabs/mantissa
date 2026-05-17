use crate::store::replicated::open::open_arc_store;
use crate::workload::model::{
    WorkloadStoreValue, admission_group_phase_rank, parse_workload_timestamp, workload_phase_rank,
};
use chrono::{DateTime, Utc};
use mantissa_store::adapter::{CompactingStoreMvRegAdapterSorted, MvRegCompactionRanker};
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::mvreg::MvRegEntry;
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
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

impl MvRegCompactionRanker<WorkloadStoreValue, Uuid> for WorkloadCompactionRank {
    type Rank = (
        u8,
        u64,
        u64,
        Option<DateTime<Utc>>,
        u8,
        bool,
        Uuid,
        Reverse<WorkloadStoreValue>,
    );

    /// Ranks one workload-domain value using its domain-specific convergence order.
    fn rank(entry: &MvRegEntry<WorkloadStoreValue, Uuid>) -> Self::Rank {
        match entry.value() {
            WorkloadStoreValue::Workload(value) => (
                0,
                value.task_epoch,
                value.phase_version,
                parse_workload_timestamp(&value.updated_at, &value.created_at),
                workload_phase_rank(&value.state),
                value.definition_complete,
                value.node_id,
                Reverse(entry.value().clone()),
            ),
            WorkloadStoreValue::AdmissionGroup(record) => (
                1,
                u64::from(admission_group_phase_rank(record.phase)),
                0,
                parse_workload_timestamp(&record.updated_at, &record.created_at),
                admission_group_phase_rank(record.phase),
                true,
                record.coordinator_node_id,
                Reverse(entry.value().clone()),
            ),
            WorkloadStoreValue::ServiceProgress(record) => (
                2,
                record.service_epoch,
                record.observed_total(),
                parse_workload_timestamp(&record.updated_at, &record.created_at),
                0,
                true,
                record.node_id,
                Reverse(entry.value().clone()),
            ),
        }
    }
}

/// Store adapter for workload registers with domain-aware compaction enabled.
pub type WorkloadRegAdapter =
    CompactingStoreMvRegAdapterSorted<UuidKey, WorkloadStoreValue, Uuid, WorkloadCompactionRank>;

pub type WorkloadStoreInner = CrdtMstStore<WorkloadRegAdapter, XXHash128, WorkloadTables>;

pub type WorkloadStore = Arc<WorkloadStoreInner>;

pub fn open_workload_store(db: Arc<redb::Database>, actor: Uuid) -> std::io::Result<WorkloadStore> {
    open_arc_store(db, actor, |db, actor| {
        WorkloadStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}
