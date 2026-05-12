#[macro_use]
mod common;

use chrono::Utc;
use common::testkit::TestNode;
use mantissa::task::types::{TaskServiceMetadata, TaskValue, TaskValueDraft};
use mantissa::workload::model::{WorkloadOwner, WorkloadPhase};
use mantissa_store::uuid_key::UuidKey;
use uuid::Uuid;

/// Builds one replicated service-owned task value for the public task RPC regression test.
fn replicated_service_task_value(task_id: Uuid, owner_id: Uuid, owner_name: &str) -> TaskValue {
    TaskValue::new(TaskValueDraft {
        id: task_id,
        name: "service-backend".to_string(),
        image: "ghcr.io/mantissa/demo:latest".to_string(),
        execution_platform: mantissa::workload::model::ExecutionPlatform::Oci,
        isolation_mode: mantissa::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec!["/bin/demo".to_string()],
        tty: false,
        node_id: owner_id,
        node_name: owner_name.to_string(),
        slot_ids: vec![7],
        networks: Vec::new(),
        cpu_millis: 250,
        memory_bytes: 128 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        ports: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(TaskServiceMetadata::new(
            "demo-service",
            "backend",
        ))),
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 1,
        last_terminal_observed_launch: None,
    })
}

/// Decodes one required UUID from a 16-byte task protocol field.
fn read_uuid(data: capnp::data::Reader<'_>) -> Result<Uuid, capnp::Error> {
    let bytes = data.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| capnp::Error::failed("invalid uuid".to_string()))?;
    Ok(Uuid::from_bytes(slice))
}

/// Lists task identifiers exposed by the public task capability.
async fn list_task_ids(node: &TestNode) -> Result<Vec<Uuid>, capnp::Error> {
    let response = node.node.task_client.list_request().send().promise.await?;
    let tasks = response.get()?.get_tasks()?;
    let mut ids = Vec::with_capacity(tasks.len() as usize);
    for task in tasks.iter() {
        ids.push(read_uuid(task.get_id()?)?);
    }
    Ok(ids)
}

local_test!(task_list_includes_service_owned_workloads, {
    let node = TestNode::new().await;
    let task_id = Uuid::new_v4();
    let service_task = replicated_service_task_value(task_id, node.id(), "node-a");

    node.node
        .workloads
        .upsert(&UuidKey::from(task_id), service_task.into())
        .await
        .expect("seed service-owned workload");

    let task_ids = list_task_ids(&node)
        .await
        .expect("task list should include service-owned workloads");
    assert_eq!(task_ids, vec![task_id]);
});
