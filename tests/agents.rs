#[macro_use]
mod common;

use chrono::Utc;
use common::convergence::wait_until;
use common::testkit::{RuntimeBackendOverrideGuard, TestNode};
use crdt_store::uuid_key::UuidKey;
use mantissa::task::types::TaskValue;
use mantissa::workload::model::WorkloadPhase;
use mantissa::workload::model::WorkloadSpec;
use mantissa::workload::model::{ExecutionSubstrate, IsolationMode};
use protocol::agents::{
    AgentRunStatus as ProtoAgentRunStatus, AgentSessionStatus as ProtoAgentSessionStatus, agents,
};
use std::time::Duration;
use uuid::Uuid;

local_test!(
    agents_submit_launches_sandbox_run_and_returns_to_waiting_input,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;

        let session_id = submit_agent_session(
            &node.node.agents_client,
            "demo-agent",
            Some("solve the task"),
            Some("oci-default"),
        )
        .await
        .expect("submit agent session");

        let (run_id, task_id) = wait_for_active_run(&node.node.agents_client, session_id).await;

        let task = node
            .node
            .workload_manager
            .inspect_workload(task_id)
            .await
            .expect("inspect agent task");
        assert_eq!(task.execution_substrate, ExecutionSubstrate::Oci);
        assert_eq!(task.isolation_mode, IsolationMode::Sandboxed);
        assert_eq!(task.isolation_profile.as_deref(), Some("oci-default"));

        let mut exited_task = task.clone();
        exited_task.state = WorkloadPhase::Exited(0);
        exited_task.updated_at = Utc::now().to_rfc3339();
        node.node
            .workloads
            .upsert(&UuidKey::from(task_id), task_spec_to_value(&exited_task))
            .await
            .expect("persist successful agent task state");

        assert!(
            wait_until(Duration::from_secs(10), Duration::from_millis(100), || {
                let client = node.node.agents_client.clone();
                async move {
                    let sessions = list_sessions(&client).await.expect("list agent sessions");
                    let runs = list_runs(&client, Some(session_id))
                        .await
                        .expect("list agent runs");
                    sessions.iter().any(|session| {
                        session.id == session_id
                            && session.status == ProtoAgentSessionStatus::WaitingInput
                            && session.active_run_id.is_none()
                            && session.last_run_id == Some(run_id)
                    }) && runs.iter().any(|run| {
                        run.id == run_id
                            && run.status == ProtoAgentRunStatus::Succeeded
                            && run.task_id == Some(task_id)
                            && run.exit_code == Some(0)
                    })
                }
            })
            .await,
            "agent session should return to waiting_input after a successful sandbox exit"
        );
    }
);

local_test!(agents_submit_input_reuses_session_and_starts_new_run, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let session_id = submit_agent_session(&node.node.agents_client, "idle-agent", None, None)
        .await
        .expect("submit idle agent session");

    assert!(
        wait_until(Duration::from_secs(5), Duration::from_millis(100), || {
            let client = node.node.agents_client.clone();
            async move {
                list_sessions(&client)
                    .await
                    .expect("list agent sessions")
                    .into_iter()
                    .any(|session| {
                        session.id == session_id
                            && session.status == ProtoAgentSessionStatus::WaitingInput
                            && session.active_run_id.is_none()
                    })
            }
        })
        .await,
        "new agent session without input should remain idle"
    );

    submit_agent_input(&node.node.agents_client, session_id, "first turn")
        .await
        .expect("submit first input");

    let (first_run_id, first_task_id) =
        wait_for_active_run(&node.node.agents_client, session_id).await;
    mark_task_exited(&node, first_task_id, 0).await;

    assert!(
        wait_until(Duration::from_secs(10), Duration::from_millis(100), || {
            let client = node.node.agents_client.clone();
            async move {
                list_sessions(&client)
                    .await
                    .expect("list agent sessions")
                    .into_iter()
                    .any(|session| {
                        session.id == session_id
                            && session.status == ProtoAgentSessionStatus::WaitingInput
                            && session.active_run_id.is_none()
                            && session.last_run_id == Some(first_run_id)
                    })
            }
        })
        .await,
        "agent session should become idle again after the first run succeeds"
    );

    submit_agent_input(&node.node.agents_client, session_id, "second turn")
        .await
        .expect("submit second input");

    let (second_run_id, second_task_id) =
        wait_for_active_run(&node.node.agents_client, session_id).await;
    assert_ne!(second_run_id, first_run_id);
    assert_ne!(second_task_id, first_task_id);
});

#[derive(Clone, Copy)]
struct AgentSessionSnapshot {
    id: Uuid,
    status: ProtoAgentSessionStatus,
    active_run_id: Option<Uuid>,
    last_run_id: Option<Uuid>,
}

#[derive(Clone, Copy)]
struct AgentRunSnapshot {
    id: Uuid,
    status: ProtoAgentRunStatus,
    task_id: Option<Uuid>,
    exit_code: Option<i32>,
}

/// Submits one durable agent session with an optional initial input and sandbox profile.
async fn submit_agent_session(
    client: &agents::Client,
    name: &str,
    initial_input: Option<&str>,
    isolation_profile: Option<&str>,
) -> Result<Uuid, capnp::Error> {
    let mut request = client.submit_request();
    {
        let mut builder = request.get().init_session();
        builder.set_name(name);
        builder.set_image("ghcr.io/mantissa/demo-agent:latest");
        builder.set_tty(false);
        builder.set_cpu_millis(250);
        builder.set_memory_bytes(128 * 1024 * 1024);
        builder.set_gpu_count(0);
        builder.set_isolation_profile(isolation_profile.unwrap_or_default());
        builder.set_pending_input(initial_input.unwrap_or_default());
        builder.reborrow().init_command(0);
        builder.reborrow().init_env(0);
        builder.reborrow().init_secret_files(0);
        builder.reborrow().init_volumes(0);
        builder.reborrow().init_networks(0);
        builder.reborrow().init_events(0);
        builder.reborrow().init_pre_stop_command(0);

        let mut workspace = builder.reborrow().init_workspace();
        workspace.reborrow().init_mount();
        workspace.set_working_directory("");
        workspace.set_persistent(false);

        let mut tools = builder.reborrow().init_tools();
        tools.reborrow().init_allowed_tools(0);
        tools.set_allow_network(false);
        tools.set_allow_pty(false);
        tools.set_allow_write(false);

        let mut checkpoint = builder.reborrow().init_checkpoint();
        checkpoint.set_enabled(false);
        checkpoint.set_interval_secs(0);
        checkpoint.reborrow().init_mount();

        let mut interaction = builder.reborrow().init_interaction();
        interaction.set_require_user_input_between_runs(true);
        interaction.set_max_turns_per_run(1);
        interaction.set_idle_timeout_secs(0);
    }

    let response = request.send().promise.await?;
    read_uuid(response.get()?.get_session_id()?)
}

/// Queues one operator input on an existing agent session.
async fn submit_agent_input(
    client: &agents::Client,
    session_id: Uuid,
    input: &str,
) -> Result<(), capnp::Error> {
    let mut request = client.submit_input_request();
    {
        let mut builder = request.get();
        builder.set_session_id(session_id.as_bytes());
        builder.set_input(input);
    }
    request.send().promise.await?;
    Ok(())
}

/// Lists the current replicated agent sessions exposed by the agents capability.
async fn list_sessions(client: &agents::Client) -> Result<Vec<AgentSessionSnapshot>, capnp::Error> {
    let response = client.list_sessions_request().send().promise.await?;
    let sessions = response.get()?.get_sessions()?;
    let mut snapshots = Vec::with_capacity(sessions.len() as usize);
    for reader in sessions.iter() {
        snapshots.push(AgentSessionSnapshot {
            id: read_uuid(reader.get_id()?)?,
            status: reader.get_status()?,
            active_run_id: read_optional_uuid(reader.get_active_run_id()?),
            last_run_id: read_optional_uuid(reader.get_last_run_id()?),
        });
    }
    Ok(snapshots)
}

/// Lists the current replicated agent runs, optionally filtered by one owning session.
async fn list_runs(
    client: &agents::Client,
    session_id: Option<Uuid>,
) -> Result<Vec<AgentRunSnapshot>, capnp::Error> {
    let mut request = client.list_runs_request();
    match session_id {
        Some(session_id) => request.get().set_session_id(session_id.as_bytes()),
        None => request.get().set_session_id(&[]),
    }

    let response = request.send().promise.await?;
    let runs = response.get()?.get_runs()?;
    let mut snapshots = Vec::with_capacity(runs.len() as usize);
    for reader in runs.iter() {
        snapshots.push(AgentRunSnapshot {
            id: read_uuid(reader.get_id()?)?,
            status: reader.get_status()?,
            task_id: read_optional_uuid(reader.get_task_id()?),
            exit_code: reader.get_has_exit_code().then_some(reader.get_exit_code()),
        });
    }
    Ok(snapshots)
}

/// Waits until the selected session exposes one active run with a bound workload task.
async fn wait_for_active_run(client: &agents::Client, session_id: Uuid) -> (Uuid, Uuid) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let sessions = list_sessions(client).await.expect("list agent sessions");
        let runs = list_runs(client, Some(session_id))
            .await
            .expect("list agent runs");

        if let Some(session) = sessions
            .into_iter()
            .find(|session| session.id == session_id)
            && let Some(run_id) = session.active_run_id
            && let Some(task_id) = runs
                .into_iter()
                .find(|run| run.id == run_id)
                .and_then(|run| run.task_id)
        {
            return (run_id, task_id);
        }

        assert!(
            tokio::time::Instant::now() <= deadline,
            "agent session {session_id} did not start a sandbox run in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Marks one persisted task as exited so the agent controller can observe completion.
async fn mark_task_exited(node: &TestNode, task_id: Uuid, exit_code: i32) {
    let mut task = node
        .node
        .workload_manager
        .inspect_workload(task_id)
        .await
        .expect("inspect agent task");
    task.state = WorkloadPhase::Exited(exit_code);
    task.updated_at = Utc::now().to_rfc3339();
    node.node
        .workloads
        .upsert(&UuidKey::from(task_id), task_spec_to_value(&task))
        .await
        .expect("persist exited agent task state");
}

/// Rebuilds one workload-store value from the current task spec so tests can inject state transitions.
fn task_spec_to_value(spec: &WorkloadSpec) -> TaskValue {
    TaskValue {
        id: spec.id,
        name: spec.name.clone(),
        image: spec.image.clone(),
        execution_substrate: spec.execution_substrate,
        isolation_mode: spec.isolation_mode,
        isolation_profile: spec.isolation_profile.clone(),
        state: spec.state.clone(),
        phase_reason: spec.phase_reason.clone(),
        phase_progress: spec.phase_progress.clone(),
        created_at: spec.created_at.clone(),
        updated_at: spec.updated_at.clone(),
        command: spec.command.clone(),
        tty: spec.tty,
        node_id: spec.node_id,
        node_name: spec.node_name.clone(),
        slot_ids: spec.slot_ids.clone(),
        slot_id: spec.slot_id,
        cpu_millis: spec.cpu_millis,
        memory_bytes: spec.memory_bytes,
        gpu_count: spec.gpu_count,
        gpu_device_ids: spec.gpu_device_ids.clone(),
        restart_policy: spec.restart_policy.clone(),
        termination_grace_period_secs: spec.termination_grace_period_secs,
        pre_stop_command: spec.pre_stop_command.clone(),
        liveness: spec.liveness.clone(),
        env: spec.env.clone(),
        secret_files: spec.secret_files.clone(),
        volumes: spec.volumes.clone(),
        networks: spec.networks.clone(),
        service_metadata: spec.service_metadata.clone(),
        job_metadata: spec.job_metadata.clone(),
        agent_run_metadata: spec.agent_run_metadata.clone(),
        lease_id: spec.lease_id,
        lease_coordinator_node_id: spec.lease_coordinator_node_id,
        task_epoch: spec.task_epoch,
        phase_version: spec.phase_version,
        launch_attempt: spec.launch_attempt,
        last_terminal_observed_launch: spec.last_terminal_observed_launch,
        definition_complete: true,
    }
}

/// Decodes one required UUID from a 16-byte protocol field.
fn read_uuid(data: capnp::data::Reader<'_>) -> Result<Uuid, capnp::Error> {
    let bytes = data.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| capnp::Error::failed("invalid uuid".to_string()))?;
    Ok(Uuid::from_bytes(slice))
}

/// Decodes one optional UUID from a protocol field that may be empty.
fn read_optional_uuid(data: capnp::data::Reader<'_>) -> Option<Uuid> {
    (data.len() == 16).then(|| {
        let bytes_owned = data.to_owned();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(bytes_owned.as_slice());
        Uuid::from_bytes(bytes)
    })
}
