use crate::store::workload_store::WorkloadStore;
use crate::workload::model::{WorkloadValue, select_best_workload_value};
use anyhow::{Result, anyhow};
use crdt_store::uuid_key::UuidKey;
use std::collections::HashSet;
use uuid::Uuid;

/// Read/write registry over replicated workload rows used by higher-level orchestration paths.
#[derive(Clone)]
pub struct WorkloadRegistry {
    store: WorkloadStore,
}

impl WorkloadRegistry {
    /// Builds the registry from the underlying replicated workload store.
    pub fn new(store: WorkloadStore) -> Self {
        Self { store }
    }

    /// Lists the canonical workload values currently assigned to one node.
    pub fn list_values_on_node(&self, node_id: Uuid) -> Result<Vec<WorkloadValue>> {
        Ok(self
            .canonical_entries()?
            .into_iter()
            .filter_map(|(_id, value)| (value.node_id == node_id).then_some(value))
            .collect())
    }

    /// Purges the local replica of workload rows assigned to the provided node ids without tombstones.
    ///
    /// Split-time pruning uses this to remove out-of-scope task runtime rows reversibly so later
    /// merge or anti-entropy can restore them from the retained partition.
    pub async fn purge_local_for_nodes(&self, node_ids: &HashSet<Uuid>) -> Result<usize> {
        let entries = self.canonical_entries()?;
        let mut removed = 0usize;
        for (id, value) in entries {
            if !node_ids.contains(&value.node_id) {
                continue;
            }

            self.store
                .purge_local(&UuidKey::from(id))
                .await
                .map_err(|e| anyhow!("workload purge_local failed: {e}"))?;
            removed = removed.saturating_add(1);
        }

        Ok(removed)
    }

    /// Builds the canonical workload projection set keyed by workload identifier.
    fn canonical_entries(&self) -> Result<Vec<(Uuid, WorkloadValue)>> {
        let (entries, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow!("workload store load_all failed: {e}"))?;

        let mut values = Vec::with_capacity(entries.len());
        for (key, snapshot) in entries {
            if let Some(value) = select_best_workload_value(snapshot.as_slice()) {
                values.push((key.to_uuid(), value));
            }
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::workload_store::open_workload_store;
    use crate::workload::model::{ExecutionPlatform, IsolationMode};
    use crate::workload::model::{WorkloadPhase, WorkloadValueDraft};
    use redb::Database;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Builds one temporary registry so registry behavior can be tested against a real store.
    fn temp_registry() -> WorkloadRegistry {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("workload-registry.redb");
        let db = Arc::new(Database::create(path).expect("create db"));
        let store = open_workload_store(db, Uuid::new_v4()).expect("open workload store");
        WorkloadRegistry::new(store)
    }

    /// Builds one minimal workload row for the provided node and phase.
    fn workload_value(node_id: Uuid, name: &str, state: WorkloadPhase) -> WorkloadValue {
        WorkloadValue::new(WorkloadValueDraft {
            id: Uuid::new_v4(),
            name: name.to_string(),
            image: "ghcr.io/demo/workload:latest".to_string(),
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: IsolationMode::Standard,
            isolation_profile: None,
            state,
            phase_reason: None,
            phase_progress: None,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
            command: Vec::new(),
            tty: false,
            node_id,
            node_name: "node".to_string(),
            slot_ids: Vec::new(),
            networks: Vec::new(),
            cpu_millis: 0,
            memory_bytes: 0,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            owner: None,
            lease_id: None,
            lease_coordinator_node_id: None,
            task_epoch: 0,
            phase_version: 0,
            launch_attempt: 0,
            last_terminal_observed_launch: None,
        })
    }

    /// Workload queries should return only the canonical rows assigned to the requested node.
    #[tokio::test]
    async fn list_values_on_node_filters_other_nodes() {
        let registry = temp_registry();
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();

        let task_a = workload_value(node_a, "task-a", WorkloadPhase::Running);
        let task_b = workload_value(node_b, "task-b", WorkloadPhase::Stopped);

        registry
            .store
            .upsert(&UuidKey::from(task_a.id), task_a.clone())
            .await
            .expect("upsert task a");
        registry
            .store
            .upsert(&UuidKey::from(task_b.id), task_b.clone())
            .await
            .expect("upsert task b");

        let values = registry
            .list_values_on_node(node_a)
            .expect("list values on node");
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].id, task_a.id);
    }

    /// Split pruning should remove only rows for evicted nodes while keeping retained workloads.
    #[tokio::test]
    async fn purge_local_for_nodes_keeps_retained_rows() {
        let registry = temp_registry();
        let evicted = Uuid::new_v4();
        let retained = Uuid::new_v4();

        let evicted_task = workload_value(evicted, "task-a", WorkloadPhase::Running);
        let retained_task = workload_value(retained, "task-b", WorkloadPhase::Running);

        registry
            .store
            .upsert(&UuidKey::from(evicted_task.id), evicted_task.clone())
            .await
            .expect("upsert evicted task");
        registry
            .store
            .upsert(&UuidKey::from(retained_task.id), retained_task.clone())
            .await
            .expect("upsert retained task");

        let removed = registry
            .purge_local_for_nodes(&HashSet::from([evicted]))
            .await
            .expect("purge local workloads");

        assert_eq!(removed, 1);
        let remaining = registry
            .list_values_on_node(retained)
            .expect("list retained workloads");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, retained_task.id);
    }
}
