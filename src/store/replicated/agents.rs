use crate::agents::types::{
    AgentRecordValue, AgentRunSpecValue, AgentRunStatusRank, AgentSessionSpecValue,
    AgentSessionStatusRank, parse_timestamp,
};
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
    Session(AgentSessionCompactionRankValue),
    /// Rank for durable run records launched from agent sessions.
    Run(AgentRunCompactionRankValue),
}

/// Ordering fields for replicated agent session rows.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct AgentSessionCompactionRankValue {
    event_sequence: u64,
    phase_version: u64,
    status: AgentSessionStatusRank,
    updated_at: Option<DateTime<Utc>>,
    id: Uuid,
    tie_breaker: Box<Reverse<AgentSessionSpecValue>>,
}

/// Ordering fields for replicated agent run rows.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct AgentRunCompactionRankValue {
    phase_version: u64,
    status: AgentRunStatusRank,
    updated_at: Option<DateTime<Utc>>,
    id: Uuid,
    tie_breaker: Box<Reverse<AgentRunSpecValue>>,
}

/// Agent compaction ranker used by the generic MVReg adapter.
pub struct AgentCompactionRank;

impl MvRegCompactionRanker<AgentRecordValue, Uuid> for AgentCompactionRank {
    type Rank = AgentRecordCompactionRank;

    /// Ranks one agent record using the same lifecycle fields as the registry selector.
    fn rank(entry: &MvRegEntry<AgentRecordValue, Uuid>) -> Self::Rank {
        match entry.value() {
            AgentRecordValue::Session(value) => {
                AgentRecordCompactionRank::Session(AgentSessionCompactionRankValue {
                    event_sequence: value.event_sequence,
                    phase_version: value.phase_version,
                    status: value.status.precedence_rank(),
                    updated_at: parse_timestamp(&value.updated_at),
                    id: value.id,
                    tie_breaker: Box::new(Reverse(value.as_ref().clone())),
                })
            }
            AgentRecordValue::Run(value) => {
                AgentRecordCompactionRank::Run(AgentRunCompactionRankValue {
                    phase_version: value.phase_version,
                    status: value.status.precedence_rank(),
                    updated_at: parse_timestamp(&value.updated_at),
                    id: value.id,
                    tie_breaker: Box::new(Reverse(value.as_ref().clone())),
                })
            }
        }
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
