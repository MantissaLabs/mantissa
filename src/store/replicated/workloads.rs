use crate::store::replicated::open::open_arc_store;
use crate::workload::model::{
    WorkloadAdmissionGroupPhaseRank, WorkloadPhaseRank, WorkloadStoreValue,
    parse_workload_timestamp,
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

/// Total workload-domain ordering key matching each record selector's precedence.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum WorkloadCompactionRankValue {
    /// Rank for replicated workload lifecycle rows.
    Workload(WorkloadRecordRank),
    /// Rank for grouped workload admission decisions.
    AdmissionGroup(WorkloadAdmissionGroupRank),
    /// Rank for service-generation progress rows.
    ServiceProgress(ServiceGenerationProgressRank),
}

/// Ordering fields for replicated workload lifecycle rows.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct WorkloadRecordRank {
    task_epoch: u64,
    phase_version: u64,
    updated_at: Option<DateTime<Utc>>,
    phase: WorkloadPhaseRank,
    definition_complete: bool,
    node_id: Uuid,
    tie_breaker: Box<Reverse<WorkloadStoreValue>>,
}

/// Ordering fields for replicated workload admission decisions.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct WorkloadAdmissionGroupRank {
    phase: WorkloadAdmissionGroupPhaseRank,
    updated_at: Option<DateTime<Utc>>,
    coordinator_node_id: Uuid,
    tie_breaker: Box<Reverse<WorkloadStoreValue>>,
}

/// Ordering fields for replicated service-generation progress rows.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ServiceGenerationProgressRank {
    service_epoch: u64,
    observed_total: u64,
    updated_at: Option<DateTime<Utc>>,
    node_id: Uuid,
    tie_breaker: Box<Reverse<WorkloadStoreValue>>,
}

impl MvRegCompactionRanker<WorkloadStoreValue, Uuid> for WorkloadCompactionRank {
    type Rank = WorkloadCompactionRankValue;

    /// Ranks one workload-domain value using its domain-specific convergence order.
    fn rank(entry: &MvRegEntry<WorkloadStoreValue, Uuid>) -> Self::Rank {
        match entry.value() {
            WorkloadStoreValue::Workload(value) => {
                WorkloadCompactionRankValue::Workload(WorkloadRecordRank {
                    task_epoch: value.task_epoch,
                    phase_version: value.phase_version,
                    updated_at: parse_workload_timestamp(&value.updated_at, &value.created_at),
                    phase: value.state.precedence_rank(),
                    definition_complete: value.definition_complete,
                    node_id: value.node_id,
                    tie_breaker: Box::new(Reverse(entry.value().clone())),
                })
            }
            WorkloadStoreValue::AdmissionGroup(record) => {
                WorkloadCompactionRankValue::AdmissionGroup(WorkloadAdmissionGroupRank {
                    phase: record.phase.precedence_rank(),
                    updated_at: parse_workload_timestamp(&record.updated_at, &record.created_at),
                    coordinator_node_id: record.coordinator_node_id,
                    tie_breaker: Box::new(Reverse(entry.value().clone())),
                })
            }
            WorkloadStoreValue::ServiceProgress(record) => {
                WorkloadCompactionRankValue::ServiceProgress(ServiceGenerationProgressRank {
                    service_epoch: record.service_epoch,
                    observed_total: record.observed_total(),
                    updated_at: parse_workload_timestamp(&record.updated_at, &record.created_at),
                    node_id: record.node_id,
                    tie_breaker: Box::new(Reverse(entry.value().clone())),
                })
            }
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
