pub(crate) use crate::workload::model::{
    compare_workload_causality as compare_task_causality,
    compare_workload_status_causality as compare_task_status_causality,
    parse_workload_timestamp as parse_task_timestamp,
    should_replace_workload_event as should_replace_task_event, workload_event_id as task_event_id,
};

#[cfg(test)]
pub(crate) use crate::workload::model::compare_workload_spec_causality as compare_task_spec_causality;

#[cfg(test)]
mod tests {
    use super::compare_task_spec_causality;
    use crate::task::container::ContainerState;
    use crate::task::types::TaskSpec;
    use chrono::Utc;
    use std::cmp::Ordering;
    use uuid::Uuid;

    /// Equal task causal tuples should still resolve deterministically by node identifier.
    #[test]
    fn compare_task_spec_causality_breaks_ties_by_node_id() {
        let now = Utc::now().to_rfc3339();
        let current = TaskSpec {
            id: Uuid::new_v4(),
            name: "task".to_string(),
            image: "img".to_string(),
            state: ContainerState::Running,
            phase_reason: None,
            phase_progress: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            command: Vec::new(),
            tty: false,
            node_id: Uuid::from_u128(1),
            node_name: "node-a".to_string(),
            slot_ids: vec![1],
            slot_id: Some(1),
            cpu_millis: 100,
            memory_bytes: 64 * 1_024 * 1_024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            service_metadata: None,
            lease_id: None,
            lease_coordinator_node_id: None,
            task_epoch: 3,
            phase_version: 9,
            launch_attempt: 0,
            last_terminal_observed_launch: None,
        };
        let candidate = TaskSpec {
            node_id: Uuid::from_u128(2),
            node_name: "node-b".to_string(),
            ..current.clone()
        };

        assert_eq!(
            compare_task_spec_causality(&current, &candidate),
            Ordering::Greater
        );
    }
}
