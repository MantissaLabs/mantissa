use crate::jobs::types::{JobSpecValue, JobStatus, parse_timestamp};
use crate::store::open::open_arc_store;
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

impl MvRegCompactionRanker<JobSpecValue, Uuid> for JobCompactionRank {
    type Rank = (
        u32,
        u64,
        u8,
        Option<DateTime<Utc>>,
        Uuid,
        Reverse<JobSpecValue>,
    );

    /// Ranks one job entry using the same order as the registry's canonical job selector.
    fn rank(entry: &MvRegEntry<JobSpecValue, Uuid>) -> Self::Rank {
        let value = entry.value();
        (
            value.attempts_started,
            value.phase_version,
            job_status_rank(value.status),
            parse_timestamp(&value.updated_at),
            value.id,
            Reverse(value.clone()),
        )
    }
}

/// Returns the stable lifecycle precedence used to compact concurrent job values.
fn job_status_rank(status: JobStatus) -> u8 {
    match status {
        JobStatus::Failed => 7,
        JobStatus::Cancelled => 6,
        JobStatus::Succeeded => 5,
        JobStatus::Cancelling => 4,
        JobStatus::Running => 3,
        JobStatus::Retrying => 2,
        JobStatus::Pending => 1,
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
