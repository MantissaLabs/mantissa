use crate::agents::types::{
    AgentRecordValue, AgentRunSpecValue, AgentRunStatus, AgentSessionSpecValue, AgentSessionStatus,
    parse_timestamp,
};
use crate::store::agent_store::AgentStore;
use anyhow::{Result, anyhow};
use crdt_store::uuid_key::UuidKey;
use std::cmp::Ordering;
use std::collections::HashSet;
use uuid::Uuid;

/// Thin accessor over the replicated agent store.
#[derive(Clone)]
pub struct AgentRegistry {
    store: AgentStore,
}

impl AgentRegistry {
    /// Builds one registry backed by the durable CRDT agent store.
    pub fn new(store: AgentStore) -> Self {
        Self { store }
    }

    /// Returns the underlying store change clock so callers can invalidate cached projections.
    pub fn change_clock(&self) -> u64 {
        self.store.change_clock()
    }

    /// Upserts one durable agent session into the shared store.
    pub async fn upsert_session(&self, value: AgentSessionSpecValue) -> Result<()> {
        self.store
            .upsert(
                &UuidKey::from(value.id),
                AgentRecordValue::Session(Box::new(value)),
            )
            .await
            .map_err(|error| anyhow!("agent session upsert failed: {error}"))?;
        Ok(())
    }

    /// Upserts one durable agent run into the shared store.
    pub async fn upsert_run(&self, value: AgentRunSpecValue) -> Result<()> {
        self.store
            .upsert(
                &UuidKey::from(value.id),
                AgentRecordValue::Run(Box::new(value)),
            )
            .await
            .map_err(|error| anyhow!("agent run upsert failed: {error}"))?;
        Ok(())
    }

    /// Removes one agent record, whether it is a session or a run.
    pub async fn remove_by_id(&self, id: Uuid) -> Result<()> {
        self.store
            .remove(&UuidKey::from(id))
            .await
            .map_err(|error| anyhow!("agent record remove failed: {error}"))?;
        Ok(())
    }

    /// Returns the canonical current session record for one identifier.
    pub fn get_session(&self, id: Uuid) -> Result<Option<AgentSessionSpecValue>> {
        let Some(value) = self.get_record(id)? else {
            return Ok(None);
        };
        Ok(match value {
            AgentRecordValue::Session(value) => Some(*value),
            AgentRecordValue::Run(_) => None,
        })
    }

    /// Returns the canonical current run record for one identifier.
    pub fn get_run(&self, id: Uuid) -> Result<Option<AgentRunSpecValue>> {
        let Some(value) = self.get_record(id)? else {
            return Ok(None);
        };
        Ok(match value {
            AgentRecordValue::Run(value) => Some(*value),
            AgentRecordValue::Session(_) => None,
        })
    }

    /// Lists every canonical agent session sorted by operator-facing name and identifier.
    pub fn list_sessions(&self) -> Result<Vec<AgentSessionSpecValue>> {
        let values = self.list_records()?;
        let mut sessions = Vec::new();
        for value in values {
            if let AgentRecordValue::Session(value) = value {
                sessions.push(*value);
            }
        }
        sessions.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
        Ok(sessions)
    }

    /// Lists every canonical run, optionally restricted to one owning session.
    pub fn list_runs(&self, session_id: Option<Uuid>) -> Result<Vec<AgentRunSpecValue>> {
        let values = self.list_records()?;
        let mut runs = Vec::new();
        for value in values {
            if let AgentRecordValue::Run(value) = value
                && session_id.is_none_or(|expected| value.session_id == expected)
            {
                runs.push(*value);
            }
        }
        runs.sort_by(|left, right| {
            left.session_name
                .cmp(&right.session_name)
                .then(left.created_at.cmp(&right.created_at))
                .then(left.id.cmp(&right.id))
        });
        Ok(runs)
    }

    /// Returns the canonical current record for one identifier regardless of variant.
    fn get_record(&self, id: Uuid) -> Result<Option<AgentRecordValue>> {
        let key = UuidKey::from(id);
        let snapshot = self
            .store
            .get_snapshot(&key)
            .map_err(|error| anyhow!("agent lookup failed: {error}"))?;
        Ok(snapshot.and_then(|snap| select_best_agent_record(snap.as_slice())))
    }

    /// Lists the canonical current value currently selected for every stored identifier.
    fn list_records(&self) -> Result<Vec<AgentRecordValue>> {
        let (entries, _) = self
            .store
            .load_all()
            .map_err(|error| anyhow!("agent store load_all failed: {error}"))?;

        let mut seen = HashSet::new();
        let mut values = Vec::with_capacity(entries.len());
        for (key, snapshot) in entries {
            let id = key.to_uuid();
            if let Some(value) = select_best_agent_record(snapshot.as_slice())
                && seen.insert(id)
            {
                values.push(value);
            }
        }
        Ok(values)
    }
}

/// Picks the canonical replicated agent record from one concurrent MVReg snapshot.
pub fn select_best_agent_record(values: &[AgentRecordValue]) -> Option<AgentRecordValue> {
    let mut best: Option<&AgentRecordValue> = None;
    for value in values {
        match best {
            None => best = Some(value),
            Some(current) => {
                if compare_agent_records(value, current).is_gt() {
                    best = Some(value);
                }
            }
        }
    }
    best.cloned()
}

/// Orders concurrent agent records so later phases dominate stale values deterministically.
pub fn compare_agent_records(left: &AgentRecordValue, right: &AgentRecordValue) -> Ordering {
    match (left, right) {
        (AgentRecordValue::Session(left), AgentRecordValue::Session(right)) => {
            compare_agent_sessions(left, right)
        }
        (AgentRecordValue::Run(left), AgentRecordValue::Run(right)) => {
            compare_agent_runs(left, right)
        }
        (AgentRecordValue::Session(left), AgentRecordValue::Run(right)) => {
            left.updated_at.cmp(&right.updated_at)
        }
        (AgentRecordValue::Run(left), AgentRecordValue::Session(right)) => {
            left.updated_at.cmp(&right.updated_at)
        }
    }
}

/// Orders concurrent session records so the most advanced lifecycle state wins.
pub fn compare_agent_sessions(
    left: &AgentSessionSpecValue,
    right: &AgentSessionSpecValue,
) -> Ordering {
    left.event_sequence
        .cmp(&right.event_sequence)
        .then(left.phase_version.cmp(&right.phase_version))
        .then_with(|| session_status_rank(left.status).cmp(&session_status_rank(right.status)))
        .then_with(|| {
            let left_time = parse_timestamp(&left.updated_at);
            let right_time = parse_timestamp(&right.updated_at);
            left_time.cmp(&right_time)
        })
        .then_with(|| left.id.cmp(&right.id))
}

/// Orders concurrent run records so later lifecycle transitions dominate stale values.
pub fn compare_agent_runs(left: &AgentRunSpecValue, right: &AgentRunSpecValue) -> Ordering {
    left.phase_version
        .cmp(&right.phase_version)
        .then_with(|| run_status_rank(left.status).cmp(&run_status_rank(right.status)))
        .then_with(|| {
            let left_time = parse_timestamp(&left.updated_at);
            let right_time = parse_timestamp(&right.updated_at);
            left_time.cmp(&right_time)
        })
        .then_with(|| left.id.cmp(&right.id))
}

fn session_status_rank(status: AgentSessionStatus) -> u8 {
    match status {
        AgentSessionStatus::Closed => 5,
        AgentSessionStatus::Failed => 4,
        AgentSessionStatus::Running => 3,
        AgentSessionStatus::Queued => 2,
        AgentSessionStatus::WaitingInput => 1,
    }
}

fn run_status_rank(status: AgentRunStatus) -> u8 {
    match status {
        AgentRunStatus::Succeeded => 5,
        AgentRunStatus::Failed => 4,
        AgentRunStatus::Cancelled => 3,
        AgentRunStatus::Running => 2,
        AgentRunStatus::Pending => 1,
    }
}
