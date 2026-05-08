use crate::jobs::types::{JobSpecValue, parse_timestamp};
use crate::store::replicated::jobs::JobStore;
use anyhow::{Result, anyhow};
use mantissa_store::uuid_key::UuidKey;
use std::cmp::Ordering;
use std::collections::HashSet;
use uuid::Uuid;

/// Thin accessor over the replicated job store.
#[derive(Clone)]
pub struct JobRegistry {
    store: JobStore,
}

impl JobRegistry {
    /// Builds one registry backed by the durable CRDT job store.
    pub fn new(store: JobStore) -> Self {
        Self { store }
    }

    /// Returns the underlying store change clock so callers can invalidate cached projections.
    pub fn change_clock(&self) -> u64 {
        self.store.change_clock()
    }

    /// Upserts one replicated job value into the durable store.
    pub async fn upsert(&self, value: JobSpecValue) -> Result<()> {
        self.store
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|error| anyhow!("job upsert failed: {error}"))?;
        Ok(())
    }

    /// Removes one job value from the durable store.
    pub async fn remove_by_id(&self, id: Uuid) -> Result<()> {
        self.store
            .remove(&UuidKey::from(id))
            .await
            .map_err(|error| anyhow!("job remove failed: {error}"))?;
        Ok(())
    }

    /// Returns the canonical job value currently selected for one identifier.
    pub fn get(&self, id: Uuid) -> Result<Option<JobSpecValue>> {
        let key = UuidKey::from(id);
        let snapshot = self
            .store
            .get_snapshot(&key)
            .map_err(|error| anyhow!("job lookup failed: {error}"))?;
        Ok(snapshot.and_then(|snap| select_best_job_spec(snap.as_slice())))
    }

    /// Lists the canonical job value currently selected for every stored identifier.
    pub fn list(&self) -> Result<Vec<JobSpecValue>> {
        let (entries, _) = self
            .store
            .load_all()
            .map_err(|error| anyhow!("job store load_all failed: {error}"))?;

        let mut seen = HashSet::new();
        let mut values = Vec::with_capacity(entries.len());
        for (key, snapshot) in entries {
            let id = key.to_uuid();
            if let Some(value) = select_best_job_spec(snapshot.as_slice())
                && seen.insert(id)
            {
                values.push(value);
            }
        }
        values.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
        Ok(values)
    }
}

/// Picks the canonical replicated job value from one concurrent MVReg snapshot.
pub fn select_best_job_spec(values: &[JobSpecValue]) -> Option<JobSpecValue> {
    let mut best: Option<&JobSpecValue> = None;
    for value in values {
        match best {
            None => best = Some(value),
            Some(current) => {
                if compare_job_specs(value, current).is_gt() {
                    best = Some(value);
                }
            }
        }
    }
    best.cloned()
}

/// Orders concurrent job specs so later phases and attempts dominate stale values.
pub fn compare_job_specs(left: &JobSpecValue, right: &JobSpecValue) -> Ordering {
    left.attempts_started
        .cmp(&right.attempts_started)
        .then(left.phase_version.cmp(&right.phase_version))
        .then_with(|| status_rank(left).cmp(&status_rank(right)))
        .then_with(|| {
            let left_time = parse_timestamp(&left.updated_at);
            let right_time = parse_timestamp(&right.updated_at);
            left_time.cmp(&right_time)
        })
        .then_with(|| left.id.cmp(&right.id))
}

/// Returns one stable precedence ranking used to break ties between concurrent job states.
fn status_rank(value: &JobSpecValue) -> u8 {
    match value.status {
        crate::jobs::types::JobStatus::Failed => 7,
        crate::jobs::types::JobStatus::Cancelled => 6,
        crate::jobs::types::JobStatus::Succeeded => 5,
        crate::jobs::types::JobStatus::Cancelling => 4,
        crate::jobs::types::JobStatus::Running => 3,
        crate::jobs::types::JobStatus::Retrying => 2,
        crate::jobs::types::JobStatus::Pending => 1,
    }
}
