use crate::agents::types::{
    AgentRecordValue, AgentRunSpecValue, AgentRunStatus, AgentSessionSpecValue, AgentSessionStatus,
    parse_timestamp,
};
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

/// Redb table names used by the replicated agent store.
pub struct AgentTables;

impl TableSet for AgentTables {
    const VALUES: &'static str = "agent_values";
    const TOMBS: &'static str = "agent_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "agent_tombs_by_observed";
    const META: &'static str = "agent_meta";
}

/// Total rank used to compact agent session and run MVRegs.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum AgentRecordCompactionRank {
    /// Rank for durable session control-plane records.
    Session(
        u64,
        u64,
        u8,
        Option<DateTime<Utc>>,
        Uuid,
        Box<Reverse<AgentSessionSpecValue>>,
    ),
    /// Rank for durable run records launched from agent sessions.
    Run(
        u64,
        u8,
        Option<DateTime<Utc>>,
        Uuid,
        Box<Reverse<AgentRunSpecValue>>,
    ),
}

/// Agent compaction ranker used by the generic MVReg adapter.
pub struct AgentCompactionRank;

impl MvRegCompactionRanker<AgentRecordValue, Uuid> for AgentCompactionRank {
    type Rank = AgentRecordCompactionRank;

    /// Ranks one agent record using the same lifecycle fields as the registry selector.
    fn rank(entry: &MvRegEntry<AgentRecordValue, Uuid>) -> Self::Rank {
        match entry.value() {
            AgentRecordValue::Session(value) => AgentRecordCompactionRank::Session(
                value.event_sequence,
                value.phase_version,
                agent_session_status_rank(value.status),
                parse_timestamp(&value.updated_at),
                value.id,
                Box::new(Reverse(value.as_ref().clone())),
            ),
            AgentRecordValue::Run(value) => AgentRecordCompactionRank::Run(
                value.phase_version,
                agent_run_status_rank(value.status),
                parse_timestamp(&value.updated_at),
                value.id,
                Box::new(Reverse(value.as_ref().clone())),
            ),
        }
    }
}

/// Returns the stable lifecycle precedence used to compact concurrent session values.
fn agent_session_status_rank(status: AgentSessionStatus) -> u8 {
    match status {
        AgentSessionStatus::Closed => 6,
        AgentSessionStatus::Closing => 5,
        AgentSessionStatus::Failed => 4,
        AgentSessionStatus::Running => 3,
        AgentSessionStatus::Queued => 2,
        AgentSessionStatus::WaitingInput => 1,
    }
}

/// Returns the stable lifecycle precedence used to compact concurrent run values.
fn agent_run_status_rank(status: AgentRunStatus) -> u8 {
    match status {
        AgentRunStatus::Succeeded => 5,
        AgentRunStatus::Failed => 4,
        AgentRunStatus::Cancelled => 3,
        AgentRunStatus::Running => 2,
        AgentRunStatus::Pending => 1,
    }
}

/// Store adapter for agent registers with domain-aware compaction enabled.
pub type AgentRegAdapter =
    CompactingStoreMvRegAdapterSorted<UuidKey, AgentRecordValue, Uuid, AgentCompactionRank>;

pub type AgentStoreInner = CrdtMstStore<AgentRegAdapter, XXHash128, AgentTables>;

pub type AgentStore = Arc<AgentStoreInner>;

/// Opens the replicated agent store for one local actor identifier.
pub fn open_agent_store(db: Arc<redb::Database>, actor: Uuid) -> std::io::Result<AgentStore> {
    open_arc_store(db, actor, AgentStoreInner::open)
}
