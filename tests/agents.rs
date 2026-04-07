#[macro_use]
mod common;

use async_trait::async_trait;
use chrono::Utc;
use common::convergence::wait_until;
use common::testkit::{RuntimeBackendOverrideGuard, TestNode};
use crdt_store::uuid_key::UuidKey;
use mantissa::runtime::testing::InMemoryRuntimeBackend;
use mantissa::runtime::types::{
    RuntimeAttachOptions, RuntimeBackend, RuntimeCreateRequest, RuntimeEvent, RuntimeExecOptions,
    RuntimeExecResult, RuntimeInfo, RuntimeLogFrame, RuntimeLogsOptions, RuntimeResult,
    RuntimeSupportContract, RuntimeSupportProfile,
};
use mantissa::task::types::TaskValue;
use mantissa::workload::model::WorkloadPhase;
use mantissa::workload::model::WorkloadSpec;
use mantissa::workload::model::{ExecutionPlatform, IsolationMode};
use protocol::agents::{
    AgentRunStatus as ProtoAgentRunStatus, AgentSessionStatus as ProtoAgentSessionStatus, agents,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender, UnboundedSender};
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

        let (run_id, workload_id) = wait_for_active_run(&node.node.agents_client, session_id).await;

        let task = node
            .node
            .workload_manager
            .inspect_workload(workload_id)
            .await
            .expect("inspect agent workload");
        assert_eq!(task.execution_platform, ExecutionPlatform::Oci);
        assert_eq!(task.isolation_mode, IsolationMode::Sandboxed);
        assert_eq!(task.isolation_profile.as_deref(), Some("oci-default"));

        let mut exited_task = task.clone();
        exited_task.state = WorkloadPhase::Exited(0);
        exited_task.updated_at = Utc::now().to_rfc3339();
        node.node
            .workloads
            .upsert(
                &UuidKey::from(workload_id),
                task_spec_to_value(&exited_task),
            )
            .await
            .expect("persist successful agent workload state");

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
                            && run.workload_id == Some(workload_id)
                            && run.exit_code == Some(0)
                    })
                }
            })
            .await,
            "agent session should return to waiting_input after a successful sandbox exit"
        );
    }
);

local_test!(agents_inspect_returns_session_and_run_history, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let session_id = submit_agent_session(
        &node.node.agents_client,
        "inspect-agent",
        Some("summarize the repo"),
        Some("oci-default"),
    )
    .await
    .expect("submit agent session");

    let (run_id, workload_id) = wait_for_active_run(&node.node.agents_client, session_id).await;

    let mut request = node.node.agents_client.inspect_request();
    request.get().set_session_id(session_id.as_bytes());
    let response = request.send().promise.await.expect("inspect agent session");
    let reader = response.get().expect("inspect agent response");
    let session = reader.get_session().expect("inspect session payload");
    let runs = reader.get_runs().expect("inspect run payload");

    assert_eq!(
        read_uuid(session.get_id().expect("session id")).expect("parse session id"),
        session_id
    );
    assert_eq!(
        read_optional_uuid(session.get_active_run_id().expect("active run id")),
        Some(run_id)
    );
    assert_eq!(
        session
            .get_isolation_profile()
            .expect("session isolation profile")
            .to_str()
            .expect("profile utf8"),
        "oci-default"
    );
    assert!(runs.iter().any(|run| {
        read_uuid(run.get_id().expect("run id")).expect("parse run id") == run_id
            && read_optional_uuid(run.get_workload_id().expect("workload id")) == Some(workload_id)
    }));
});

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

    let (first_run_id, first_workload_id) =
        wait_for_active_run(&node.node.agents_client, session_id).await;
    mark_workload_exited(&node, first_workload_id, 0).await;

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

    let (second_run_id, second_workload_id) =
        wait_for_active_run(&node.node.agents_client, session_id).await;
    assert_ne!(second_run_id, first_run_id);
    assert_ne!(second_workload_id, first_workload_id);
});

local_test!(agents_submit_nono_run_projects_runtime_sandbox_policy, {
    let runtime_backend = Arc::new(RecordingRuntimeBackend::new());
    let _guard = RuntimeBackendOverrideGuard::install(runtime_backend.clone());
    let node = TestNode::new().await;

    let session_id = submit_agent_session_with_options(
        &node.node.agents_client,
        "nono-agent",
        AgentSessionSubmitOptions {
            initial_input: Some("inspect the workspace"),
            isolation_profile: Some("nono-default"),
            workspace_directory: Some("/workspace"),
            allow_network: false,
            allow_write: true,
        },
    )
    .await
    .expect("submit nono agent session");

    let (_run_id, workload_id) = wait_for_active_run(&node.node.agents_client, session_id).await;
    let create_request = wait_for_runtime_create_request(runtime_backend.as_ref()).await;

    assert_eq!(create_request.execution_platform, ExecutionPlatform::Oci);
    assert_eq!(create_request.isolation_mode, IsolationMode::Sandboxed);
    assert_eq!(
        create_request.isolation_profile.as_deref(),
        Some("nono-default")
    );

    let policy = create_request
        .sandbox_policy
        .expect("nono-backed agent runs should carry a runtime sandbox policy");
    assert_eq!(
        policy.working_directory.as_deref(),
        Some(Path::new("/workspace"))
    );
    assert_eq!(
        policy.network,
        mantissa::runtime::types::RuntimeSandboxNetworkMode::Blocked
    );
    assert!(policy.filesystem.iter().any(|rule| {
        rule.path == Path::new("/workspace")
            && rule.access == mantissa::runtime::types::RuntimeSandboxAccessMode::ReadWrite
    }));
    assert!(policy.filesystem.iter().any(|rule| {
        rule.path == Path::new("/tmp")
            && rule.access == mantissa::runtime::types::RuntimeSandboxAccessMode::ReadWrite
    }));
    assert!(policy.filesystem.iter().any(|rule| {
        rule.path == Path::new("/var/tmp")
            && rule.access == mantissa::runtime::types::RuntimeSandboxAccessMode::ReadWrite
    }));

    mark_workload_exited(&node, workload_id, 0).await;
});

local_test!(agents_cancel_active_run_returns_session_to_waiting_input, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let session_id = submit_agent_session(
        &node.node.agents_client,
        "cancel-agent",
        Some("cancel this run"),
        None,
    )
    .await
    .expect("submit cancellable agent session");

    let (run_id, workload_id) = wait_for_active_run(&node.node.agents_client, session_id).await;
    cancel_agent_session(&node.node.agents_client, session_id)
        .await
        .expect("cancel active agent session");
    mark_workload_phase(&node, workload_id, WorkloadPhase::Stopped).await;

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
                        && run.status == ProtoAgentRunStatus::Cancelled
                        && run.workload_id == Some(workload_id)
                })
            }
        })
        .await,
        "cancelled agent run should return the session to waiting_input"
    );
});

local_test!(agents_close_active_run_transitions_to_closed, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let session_id = submit_agent_session(
        &node.node.agents_client,
        "close-agent",
        Some("close this session"),
        None,
    )
    .await
    .expect("submit closable agent session");

    let (run_id, workload_id) = wait_for_active_run(&node.node.agents_client, session_id).await;
    close_agent_session(&node.node.agents_client, session_id)
        .await
        .expect("close active agent session");

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
                            && session.status == ProtoAgentSessionStatus::Closing
                            && session.active_run_id == Some(run_id)
                    })
            }
        })
        .await,
        "closing an active session should expose the closing intermediary state"
    );

    mark_workload_phase(&node, workload_id, WorkloadPhase::Stopped).await;

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
                        && session.status == ProtoAgentSessionStatus::Closed
                        && session.active_run_id.is_none()
                        && session.last_run_id == Some(run_id)
                }) && runs.iter().any(|run| {
                    run.id == run_id
                        && run.status == ProtoAgentRunStatus::Cancelled
                        && run.workload_id == Some(workload_id)
                })
            }
        })
        .await,
        "closed agent session should retain the cancelled last run"
    );
});

local_test!(agents_delete_closed_session_removes_session_and_runs, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let session_id = submit_agent_session(&node.node.agents_client, "delete-agent", None, None)
        .await
        .expect("submit deletable agent session");

    close_agent_session(&node.node.agents_client, session_id)
        .await
        .expect("close idle agent session");
    delete_agent_session(&node.node.agents_client, session_id)
        .await
        .expect("delete closed agent session");

    assert!(
        wait_until(Duration::from_secs(10), Duration::from_millis(100), || {
            let client = node.node.agents_client.clone();
            async move {
                let sessions = list_sessions(&client).await.expect("list agent sessions");
                let runs = list_runs(&client, Some(session_id))
                    .await
                    .expect("list agent runs");
                sessions.into_iter().all(|session| session.id != session_id) && runs.is_empty()
            }
        })
        .await,
        "deleted agent sessions should disappear together with their run history"
    );
});

#[derive(Clone, Copy, Default)]
struct AgentSessionSubmitOptions<'a> {
    initial_input: Option<&'a str>,
    isolation_profile: Option<&'a str>,
    workspace_directory: Option<&'a str>,
    allow_network: bool,
    allow_write: bool,
}

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
    workload_id: Option<Uuid>,
    exit_code: Option<i32>,
}

#[derive(Default)]
struct RecordingRuntimeBackend {
    inner: Arc<InMemoryRuntimeBackend>,
    create_requests: Arc<AsyncMutex<Vec<RuntimeCreateRequest>>>,
}

impl RecordingRuntimeBackend {
    /// Builds one recording wrapper around the shared in-memory runtime backend.
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryRuntimeBackend::default()),
            create_requests: Arc::new(AsyncMutex::new(Vec::new())),
        }
    }

    /// Returns the most recent runtime create request captured by the backend.
    async fn last_create_request(&self) -> Option<RuntimeCreateRequest> {
        self.create_requests.lock().await.last().cloned()
    }
}

#[async_trait]
impl RuntimeBackend for RecordingRuntimeBackend {
    /// Captures one runtime create request before forwarding it to the in-memory backend.
    async fn create_instance(&self, request: RuntimeCreateRequest) -> RuntimeResult<String> {
        self.create_requests.lock().await.push(request.clone());
        self.inner.create_instance(request).await
    }

    /// Starts one in-memory runtime instance through the wrapped backend.
    async fn start_instance(&self, runtime_id: &str) -> RuntimeResult<()> {
        self.inner.start_instance(runtime_id).await
    }

    /// Stops one in-memory runtime instance through the wrapped backend.
    async fn stop_instance(
        &self,
        runtime_id: &str,
        timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        self.inner.stop_instance(runtime_id, timeout).await
    }

    /// Executes one non-interactive command through the wrapped backend.
    async fn exec_instance(
        &self,
        runtime_id: &str,
        command: &[String],
        timeout: Option<Duration>,
    ) -> RuntimeResult<RuntimeExecResult> {
        self.inner.exec_instance(runtime_id, command, timeout).await
    }

    /// Executes one streamed command through the wrapped backend.
    async fn exec_instance_stream(
        &self,
        runtime_id: &str,
        options: &RuntimeExecOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<RuntimeExecResult> {
        self.inner
            .exec_instance_stream(runtime_id, options, output_tx, input_rx)
            .await
    }

    /// Streams runtime logs through the wrapped backend.
    async fn stream_instance_logs(
        &self,
        runtime_id: &str,
        options: &RuntimeLogsOptions,
        logs_tx: MpscSender<RuntimeLogFrame>,
    ) -> RuntimeResult<()> {
        self.inner
            .stream_instance_logs(runtime_id, options, logs_tx)
            .await
    }

    /// Attaches to one running runtime instance through the wrapped backend.
    async fn attach_instance(
        &self,
        runtime_id: &str,
        options: &RuntimeAttachOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<()> {
        self.inner
            .attach_instance(runtime_id, options, output_tx, input_rx)
            .await
    }

    /// Restarts one in-memory runtime instance through the wrapped backend.
    async fn restart_instance(
        &self,
        runtime_id: &str,
        timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        self.inner.restart_instance(runtime_id, timeout).await
    }

    /// Removes one in-memory runtime instance through the wrapped backend.
    async fn remove_instance(
        &self,
        runtime_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> RuntimeResult<()> {
        self.inner
            .remove_instance(runtime_id, force, remove_volumes)
            .await
    }

    /// Lists in-memory runtime instances through the wrapped backend.
    async fn list_instances(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> RuntimeResult<Vec<RuntimeInfo>> {
        self.inner.list_instances(filters).await
    }

    /// Returns inspect data for one runtime instance through the wrapped backend.
    async fn inspect_instance(&self, runtime_id: &str) -> RuntimeResult<RuntimeInfo> {
        self.inner.inspect_instance(runtime_id).await
    }

    /// Reports image presence using the wrapped in-memory backend.
    async fn image_present(&self, image: &str) -> RuntimeResult<bool> {
        self.inner.image_present(image).await
    }

    /// Treats image pulls as no-ops through the wrapped in-memory backend.
    async fn pull_image(&self, image: &str) -> RuntimeResult<()> {
        self.inner.pull_image(image).await
    }

    /// Reports the wrapped backend capabilities unchanged.
    fn capabilities(&self) -> mantissa::runtime::types::RuntimeCapabilities {
        self.inner.capabilities()
    }

    /// Advertises exact OCI contracts including the `nono-default` sandbox profile.
    fn advertised_support(&self) -> RuntimeSupportProfile {
        RuntimeSupportProfile::from_exact_contracts(
            [
                RuntimeSupportContract::new(ExecutionPlatform::Oci, IsolationMode::Standard, None),
                RuntimeSupportContract::new(
                    ExecutionPlatform::Oci,
                    IsolationMode::Standard,
                    Some("default"),
                ),
                RuntimeSupportContract::new(ExecutionPlatform::Oci, IsolationMode::Sandboxed, None),
                RuntimeSupportContract::new(
                    ExecutionPlatform::Oci,
                    IsolationMode::Sandboxed,
                    Some("oci-default"),
                ),
                RuntimeSupportContract::new(
                    ExecutionPlatform::Oci,
                    IsolationMode::Sandboxed,
                    Some("nono-default"),
                ),
            ],
            self.capabilities().feature_flags(),
        )
    }

    /// Forwards lifecycle watch requests to the wrapped backend.
    async fn watch_runtime_events(
        &self,
        events_tx: UnboundedSender<RuntimeEvent>,
    ) -> RuntimeResult<()> {
        self.inner.watch_runtime_events(events_tx).await
    }
}

/// Submits one durable agent session with an optional initial input and sandbox profile.
async fn submit_agent_session(
    client: &agents::Client,
    name: &str,
    initial_input: Option<&str>,
    isolation_profile: Option<&str>,
) -> Result<Uuid, capnp::Error> {
    submit_agent_session_with_options(
        client,
        name,
        AgentSessionSubmitOptions {
            initial_input,
            isolation_profile,
            workspace_directory: None,
            allow_network: false,
            allow_write: false,
        },
    )
    .await
}

/// Submits one durable agent session using explicit sandbox policy options.
async fn submit_agent_session_with_options(
    client: &agents::Client,
    name: &str,
    options: AgentSessionSubmitOptions<'_>,
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
        builder.set_isolation_profile(options.isolation_profile.unwrap_or_default());
        builder.set_pending_input(options.initial_input.unwrap_or_default());
        builder.reborrow().init_command(0);
        builder.reborrow().init_env(0);
        builder.reborrow().init_secret_files(0);
        builder.reborrow().init_volumes(0);
        builder.reborrow().init_networks(0);
        builder.reborrow().init_events(0);
        builder.reborrow().init_pre_stop_command(0);

        let mut workspace = builder.reborrow().init_workspace();
        workspace.reborrow().init_mount();
        workspace.set_working_directory(options.workspace_directory.unwrap_or_default());
        workspace.set_persistent(false);

        let mut tools = builder.reborrow().init_tools();
        tools.reborrow().init_allowed_tools(0);
        tools.set_allow_network(options.allow_network);
        tools.set_allow_pty(false);
        tools.set_allow_write(options.allow_write);

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

/// Waits until one runtime create request has been recorded and returns it.
async fn wait_for_runtime_create_request(
    backend: &RecordingRuntimeBackend,
) -> RuntimeCreateRequest {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(request) = backend.last_create_request().await {
            return request;
        }

        assert!(
            tokio::time::Instant::now() <= deadline,
            "agent run did not reach runtime create in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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

/// Requests cancellation for one active or queued agent session.
async fn cancel_agent_session(
    client: &agents::Client,
    session_id: Uuid,
) -> Result<(), capnp::Error> {
    let mut request = client.cancel_request();
    request.get().set_session_id(session_id.as_bytes());
    request.send().promise.await?;
    Ok(())
}

/// Requests closure for one agent session.
async fn close_agent_session(
    client: &agents::Client,
    session_id: Uuid,
) -> Result<(), capnp::Error> {
    let mut request = client.close_request();
    request.get().set_session_id(session_id.as_bytes());
    request.send().promise.await?;
    Ok(())
}

/// Deletes one previously closed agent session.
async fn delete_agent_session(
    client: &agents::Client,
    session_id: Uuid,
) -> Result<(), capnp::Error> {
    let mut request = client.delete_request();
    request.get().set_session_id(session_id.as_bytes());
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
            workload_id: read_optional_uuid(reader.get_workload_id()?),
            exit_code: reader.get_has_exit_code().then_some(reader.get_exit_code()),
        });
    }
    Ok(snapshots)
}

/// Waits until the selected session exposes one active run with a bound workload.
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
            && let Some(workload_id) = runs
                .into_iter()
                .find(|run| run.id == run_id)
                .and_then(|run| run.workload_id)
        {
            return (run_id, workload_id);
        }

        assert!(
            tokio::time::Instant::now() <= deadline,
            "agent session {session_id} did not start a sandbox run in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Marks one persisted workload as exited so the agent controller can observe completion.
async fn mark_workload_exited(node: &TestNode, workload_id: Uuid, exit_code: i32) {
    mark_workload_phase(node, workload_id, WorkloadPhase::Exited(exit_code)).await;
}

/// Marks one persisted workload with an arbitrary phase so the agent controller can observe it.
async fn mark_workload_phase(node: &TestNode, workload_id: Uuid, phase: WorkloadPhase) {
    let mut task = node
        .node
        .workload_manager
        .inspect_workload(workload_id)
        .await
        .expect("inspect agent workload");
    task.state = phase;
    task.updated_at = Utc::now().to_rfc3339();
    node.node
        .workloads
        .upsert(&UuidKey::from(workload_id), task_spec_to_value(&task))
        .await
        .expect("persist exited agent workload state");
}

/// Rebuilds one workload-store value from the current task spec so tests can inject state transitions.
fn task_spec_to_value(spec: &WorkloadSpec) -> TaskValue {
    TaskValue {
        id: spec.id,
        name: spec.name.clone(),
        image: spec.image.clone(),
        execution_platform: spec.execution_platform,
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
        owner: spec.owner.clone(),
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
