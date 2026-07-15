#[macro_use]
mod common;

use chrono::Utc;
use common::convergence::wait_until;
use common::testkit::{ClusterConfig, TestNode};
use mantissa::task::types::{TaskServiceMetadata, TaskStateFilter, TaskValue, TaskValueDraft};
use mantissa::workload::model::{WorkloadOwner, WorkloadPhase};
use mantissa_client::{
    config::ClientConfig,
    tasks::{self as client_tasks, TaskStartOptions},
};
use mantissa_store::uuid_key::UuidKey;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
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
            1,
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

/// Starts one standalone task through the client API used by `mantissa tasks start`.
async fn start_task_via_cli_path(client_config: &ClientConfig, name: &str) {
    let command = vec!["sh".to_string(), "-lc".to_string(), "sleep 60".to_string()];
    let volumes = Vec::new();
    client_tasks::start(
        client_config,
        &TaskStartOptions {
            name,
            image: "alpine:3.20",
            command: &command,
            cpu_millis: 250,
            memory_bytes: 128 * 1_024 * 1_024,
            gpu_count: 0,
            volumes: &volumes,
        },
    )
    .await
    .expect("start standalone task through CLI client path");
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

local_test!(task_start_rejects_missing_resource_request, {
    let node = TestNode::new().await;
    let socket_dir = common::temp_db_dir();
    let socket_path = socket_dir.path().join("mantissa.sock");
    node.node
        .start_local_admin_socket_at(socket_path.clone())
        .await
        .expect("start local admin socket for CLI client path");
    let client_config = ClientConfig {
        socket: Some(socket_path),
        ..ClientConfig::default()
    };
    let command = vec!["sh".to_string(), "-lc".to_string(), "sleep 60".to_string()];
    let volumes = Vec::new();

    let error = client_tasks::start(
        &client_config,
        &TaskStartOptions {
            name: "missing-resource-task",
            image: "alpine:3.20",
            command: &command,
            cpu_millis: 0,
            memory_bytes: 0,
            gpu_count: 0,
            volumes: &volumes,
        },
    )
    .await
    .expect_err("zero resource task must fail through CLI client path");

    assert!(
        error.to_string().contains("cpu_millis and memory_bytes"),
        "unexpected error: {error:#}"
    );
});

local_test!(
    task_start_spreads_independent_cli_submissions_across_cluster,
    {
        let cfg = ClusterConfig {
            sync_tick_ms: Some(100),
            gossip_tick_ms: Some(100),
            gossip_fanout: Some(2),
            ..ClusterConfig::default()
        };
        let cluster = TestNode::new_cluster_inproc_with_config(5, cfg)
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 5, "cluster should stabilise to five nodes")
            .await;
        TestNode::wait_cluster_ready_all(&cluster, 5, Duration::from_secs(10))
            .await
            .expect("cluster readiness converges before task starts");

        let node_ids = cluster.iter().map(TestNode::id).collect::<Vec<_>>();
        let remote_node_ids: HashSet<Uuid> = node_ids
            .iter()
            .copied()
            .filter(|node_id| *node_id != cluster[0].id())
            .collect();
        let digests_ready = wait_until(Duration::from_secs(10), Duration::from_millis(100), || {
            let expected = remote_node_ids.clone();
            let anchor = &cluster[0];
            async move {
                let observed = anchor
                    .node
                    .scheduler
                    .observed_scheduler_digests()
                    .expect("load observed scheduler digests")
                    .into_iter()
                    .map(|digest| digest.digest.node_id)
                    .collect::<HashSet<_>>();
                expected.iter().all(|node_id| observed.contains(node_id))
            }
        })
        .await;
        assert!(
            digests_ready,
            "anchor should observe every peer scheduler digest before task starts"
        );

        let socket_dir = common::temp_db_dir();
        let socket_path = socket_dir.path().join("mantissa.sock");
        cluster[0]
            .node
            .start_local_admin_socket_at(socket_path.clone())
            .await
            .expect("start local admin socket for CLI client path");
        let client_config = ClientConfig {
            socket: Some(socket_path),
            ..ClientConfig::default()
        };

        let task_prefix = "cli-task-spread";
        for index in 0..10 {
            let name = format!("{task_prefix}-{index}");
            start_task_via_cli_path(&client_config, &name).await;
        }

        let balanced = wait_until(Duration::from_secs(20), Duration::from_millis(100), || {
            let expected_nodes = node_ids.clone();
            let anchor = &cluster[0];
            async move {
                let tasks = anchor
                    .node
                    .workload_manager
                    .list_workloads(&TaskStateFilter::active_only())
                    .await
                    .expect("list active workloads");
                let mut counts = HashMap::new();
                for task in tasks.iter().filter(|task| {
                    task.name.starts_with(task_prefix)
                        && matches!(task.state, WorkloadPhase::Running)
                }) {
                    *counts.entry(task.node_id).or_insert(0usize) += 1;
                }

                expected_nodes
                    .iter()
                    .all(|node_id| counts.get(node_id).copied().unwrap_or(0) == 2)
            }
        })
        .await;

        if !balanced {
            let tasks = cluster[0]
                .node
                .workload_manager
                .list_workloads(&TaskStateFilter::active_only())
                .await
                .expect("list active workloads after spread timeout");
            let mut counts = HashMap::new();
            let mut states = Vec::new();
            for task in tasks
                .into_iter()
                .filter(|task| task.name.starts_with(task_prefix))
            {
                *counts.entry(task.node_id).or_insert(0usize) += 1;
                states.push((task.name, task.node_id, task.state));
            }
            panic!(
                "CLI-path task placement did not reach two running tasks per node; counts={counts:?}; states={states:?}"
            );
        }
    }
);
