use crate::jobs::types::{JobSpecValue, JobStatusRank, parse_timestamp};
use crate::store::replicated::open::open_arc_store;
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

/// Redb table names used by the replicated job store.
pub struct JobTables;

impl TableSet for JobTables {
    const VALUES: &'static str = "job_values";
    const TOMBS: &'static str = "job_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "job_tombs_by_observed";
    const META: &'static str = "job_meta";
}

/// Job compaction ranker used by the generic MVReg adapter.
pub struct JobCompactionRank;

/// Total job ordering key matching the registry's canonical selector.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct JobSpecRank {
    attempts_started: u32,
    phase_version: u64,
    status: JobStatusRank,
    updated_at: Option<DateTime<Utc>>,
    id: Uuid,
    tie_breaker: Reverse<JobSpecValue>,
}

impl MvRegCompactionRanker<JobSpecValue, Uuid> for JobCompactionRank {
    type Rank = JobSpecRank;

    /// Ranks one job entry using the same order as the registry's canonical job selector.
    fn rank(entry: &MvRegEntry<JobSpecValue, Uuid>) -> Self::Rank {
        let value = entry.value();
        JobSpecRank {
            attempts_started: value.attempts_started,
            phase_version: value.phase_version,
            status: value.status.precedence_rank(),
            updated_at: parse_timestamp(&value.updated_at),
            id: value.id,
            tie_breaker: Reverse(value.clone()),
        }
    }
}

/// Store adapter for job registers with domain-aware compaction enabled.
pub type JobRegAdapter =
    CompactingStoreMvRegAdapterSorted<UuidKey, JobSpecValue, Uuid, JobCompactionRank>;

pub type JobStoreInner = CrdtMstStore<JobRegAdapter, XXHash128, JobTables>;

pub type JobStore = Arc<JobStoreInner>;

/// Opens the replicated job store for one local actor identifier.
pub fn open_job_store(db: Arc<redb::Database>, actor: Uuid) -> std::io::Result<JobStore> {
    open_arc_store(db, actor, JobStoreInner::open)
}
