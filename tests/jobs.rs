#[macro_use]
mod common;

use async_trait::async_trait;
use common::convergence::wait_until;
use common::testkit::{
    ClusterConfig, InMemoryRuntimeBackend, RuntimeBackendOverrideGuard, TestNode,
};
use mantissa::runtime::set::RuntimeSet;
use mantissa::runtime::testing::IN_MEMORY_RUNTIME_BACKEND_KIND;
use mantissa::runtime::types::{
    RuntimeBackend, RuntimeCapabilities, RuntimeCreateRequest, RuntimeError, RuntimeEvent,
    RuntimeInfo,
};
use mantissa::server::headless::{HeadlessConfig, HeadlessKeys, HeadlessNode, HeadlessTransport};
use mantissa::task::types::TaskStateFilter;
use mantissa::workload::model::{ExecutionPlatform, IsolationMode, WorkloadAdmissionState};
use mantissa::workload::types::WorkloadAdmissionMode;
use mantissa_net::noise::NoiseKeys;
use mantissa_protocol::jobs::{JobStatus as ProtoJobStatus, jobs};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::sleep;
use uuid::Uuid;

const OVERCOMMITTED_CPU_MILLIS: u64 = 500_000;
const OVERCOMMITTED_MEMORY_BYTES: u64 = 8 * 1024 * 1024 * 1024;

local_test!(jobs_submit_and_reach_succeeded_after_task_exit, {
    let backend = Arc::new(ControllableExitRuntimeBackend::default());
    let _guard = RuntimeBackendOverrideGuard::install(backend.clone());
    let node = TestNode::new().await;

    let job_id = submit_job(&node.node.jobs_client, "demo-job", 0, 0)
        .await
        .expect("submit job");
    let inspected = inspect_job(&node.node.jobs_client, job_id)
        .await
        .expect("inspect submitted job");
    assert_eq!(inspected.snapshot.id, job_id);

    let active_workload_id =
        wait_for_active_workload(&node.node.jobs_client, job_id, Duration::from_secs(5))
            .await
            .expect("job should launch one workload");
    let inspected = inspect_job(&node.node.jobs_client, job_id)
        .await
        .expect("inspect running job");
    assert!(
        inspected.attempts.iter().any(|attempt| {
            attempt.workload_id == active_workload_id && attempt.is_active && attempt.is_last
        }),
        "inspect should expose the active workload attempt through the jobs surface"
    );

    backend
        .signal_workload_exit(active_workload_id, 0)
        .await
        .expect("emit successful runtime exit");

    assert!(
        wait_for_job_status(
            &node.node.jobs_client,
            job_id,
            ProtoJobStatus::Succeeded,
            Duration::from_secs(10)
        )
        .await,
        "job should converge to succeeded after task exit"
    );
});

local_test!(jobs_retry_after_failed_task, {
    let backend = Arc::new(ControllableExitRuntimeBackend::default());
    let _guard = RuntimeBackendOverrideGuard::install(backend.clone());
    let node = TestNode::new().await;

    let job_id = submit_job(&node.node.jobs_client, "retry-job", 1, 0)
        .await
        .expect("submit retrying job");

    let first_workload_id =
        wait_for_active_workload(&node.node.jobs_client, job_id, Duration::from_secs(5))
            .await
            .expect("job should launch first workload");

    backend
        .signal_workload_exit(first_workload_id, 1)
        .await
        .expect("emit failed runtime exit");

    assert!(
        wait_until(Duration::from_secs(10), Duration::from_millis(100), || {
            let client = node.node.jobs_client.clone();
            async move {
                let jobs = list_jobs(&client).await.expect("list jobs");
                jobs.iter().any(|job| {
                    job.id == job_id
                        && job.status == ProtoJobStatus::Running
                        && job.attempts_started >= 2
                        && job
                            .active_workload_id
                            .is_some_and(|workload_id| workload_id != first_workload_id)
                })
            }
        })
        .await,
        "job should launch a second attempt after the first workload exits unsuccessfully"
    );

    let tasks = node
        .node
        .workload_manager
        .list_workloads(&TaskStateFilter::all())
        .await
        .expect("list tasks");
    assert!(
        tasks.iter().any(|task| task.id == first_workload_id),
        "first failed workload should remain visible in the replicated workload store"
    );
});

local_test!(jobs_gang_admission_records_grouped_attempt, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let job_id = submit_job_with_admission(
        &node.node.jobs_client,
        "gang-job",
        WorkloadAdmissionMode::Gang,
    )
    .await
    .expect("submit gang-admitted job");

    let workload_id =
        wait_for_active_workload(&node.node.jobs_client, job_id, Duration::from_secs(5))
            .await
            .expect("gang job should launch one workload");

    let workload = node
        .node
        .workload_manager
        .inspect_workload(workload_id)
        .await
        .expect("inspect gang job workload");
    assert!(
        workload.admission_group_id.is_some(),
        "gang job attempt should record an admission group id"
    );
    assert_eq!(
        workload.admission_state,
        WorkloadAdmissionState::GroupCommitted,
        "gang job attempt should become runnable only after group commit"
    );
});

local_test!(jobs_gang_capacity_failure_leaves_no_attempt_workloads, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let job_id = submit_job_with_admission_resources(
        &node.node.jobs_client,
        "gang-job-overcommit",
        WorkloadAdmissionMode::Gang,
        OVERCOMMITTED_CPU_MILLIS,
        OVERCOMMITTED_MEMORY_BYTES,
    )
    .await
    .expect("submit overcommitted gang-admitted job");

    assert!(
        wait_for_job_status(
            &node.node.jobs_client,
            job_id,
            ProtoJobStatus::Failed,
            Duration::from_secs(10),
        )
        .await,
        "overcommitted gang job should fail the launch attempt"
    );

    let inspected = inspect_job(&node.node.jobs_client, job_id)
        .await
        .expect("inspect failed gang job");
    assert_eq!(inspected.snapshot.attempts_started, 1);
    let detail = inspected
        .snapshot
        .status_detail
        .as_deref()
        .expect("failed gang job should report a launch detail");
    assert!(
        detail.contains("not enough schedulable slots or resources"),
        "failed gang job should explain the reservation failure: {detail}"
    );
    assert!(
        inspected.snapshot.active_workload_id.is_none(),
        "failed gang job should not keep an active workload id"
    );
    assert!(
        inspected.attempts.is_empty(),
        "failed gang admission should not leave job attempt workload rows"
    );

    let workloads = node
        .node
        .workload_manager
        .list_workloads(&TaskStateFilter::all())
        .await
        .expect("list workloads after failed gang job");
    assert!(
        workloads
            .iter()
            .filter_map(|workload| workload.job_owner())
            .all(|owner| owner.job_id != job_id),
        "failed gang admission should not leak job-owned workload rows"
    );
});

local_test!(jobs_cancel_running_job_reaches_cancelled, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let job_id = submit_job(&node.node.jobs_client, "cancel-job", 0, 0)
        .await
        .expect("submit cancellable job");

    let active_workload_id =
        wait_for_active_workload(&node.node.jobs_client, job_id, Duration::from_secs(5))
            .await
            .expect("job should launch one workload before cancellation");

    let snapshot = cancel_job(&node.node.jobs_client, job_id)
        .await
        .expect("cancel job");
    assert_eq!(snapshot.id, job_id);
    assert!(
        matches!(
            snapshot.status,
            ProtoJobStatus::Cancelling | ProtoJobStatus::Cancelled
        ),
        "cancel should move the job into cancelling or cancelled state"
    );
    assert_eq!(snapshot.active_workload_id, Some(active_workload_id));

    assert!(
        wait_for_job_status(
            &node.node.jobs_client,
            job_id,
            ProtoJobStatus::Cancelled,
            Duration::from_secs(10)
        )
        .await,
        "job should converge to cancelled after the active workload is stopped"
    );
});

local_test!(jobs_delete_requires_terminal_state_and_removes_job, {
    let backend = Arc::new(ControllableExitRuntimeBackend::default());
    let _guard = RuntimeBackendOverrideGuard::install(backend.clone());
    let node = TestNode::new().await;

    let job_id = submit_job(&node.node.jobs_client, "delete-job", 0, 0)
        .await
        .expect("submit deletable job");

    let delete_error = delete_job(&node.node.jobs_client, job_id)
        .await
        .expect_err("non-terminal delete should fail");
    assert!(
        delete_error.to_string().contains("not terminal"),
        "delete error should explain that the job must be terminal first"
    );

    let active_workload_id =
        wait_for_active_workload(&node.node.jobs_client, job_id, Duration::from_secs(5))
            .await
            .expect("job should launch one workload");

    backend
        .signal_workload_exit(active_workload_id, 0)
        .await
        .expect("emit successful runtime exit");

    assert!(
        wait_for_job_status(
            &node.node.jobs_client,
            job_id,
            ProtoJobStatus::Succeeded,
            Duration::from_secs(10)
        )
        .await,
        "job should converge to succeeded before delete"
    );

    let removed = delete_job(&node.node.jobs_client, job_id)
        .await
        .expect("delete terminal job");
    assert_eq!(removed.id, job_id);
    assert_eq!(removed.status, ProtoJobStatus::Succeeded);

    assert!(
        wait_until(Duration::from_secs(10), Duration::from_millis(100), || {
            let client = node.node.jobs_client.clone();
            async move {
                let jobs = list_jobs(&client).await.expect("list jobs");
                jobs.into_iter().all(|job| job.id != job_id)
            }
        })
        .await,
        "deleted job should disappear from the replicated jobs surface"
    );
});

local_test!(jobs_runtime_selection_reaches_workload_attempts, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let job_id = submit_job_with_runtime(
        &node.node.jobs_client,
        "sandboxed-job",
        0,
        0,
        "oci",
        "sandboxed",
        Some("oci-default"),
    )
    .await
    .expect("submit sandboxed job");

    let inspected = inspect_job(&node.node.jobs_client, job_id)
        .await
        .expect("inspect submitted sandboxed job");
    assert_eq!(inspected.snapshot.execution_platform, "oci");
    assert_eq!(inspected.snapshot.isolation_mode, "sandboxed");
    assert_eq!(
        inspected.snapshot.isolation_profile.as_deref(),
        Some("oci-default")
    );

    let active_workload_id =
        wait_for_active_workload(&node.node.jobs_client, job_id, Duration::from_secs(5))
            .await
            .expect("job should launch one sandboxed workload");
    let workload = node
        .node
        .workload_manager
        .inspect_workload(active_workload_id)
        .await
        .expect("inspect sandboxed job workload");
    assert_eq!(workload.execution_platform, ExecutionPlatform::Oci);
    assert_eq!(workload.isolation_mode, IsolationMode::Sandboxed);
    assert_eq!(workload.isolation_profile.as_deref(), Some("oci-default"));

    let inspected = inspect_job(&node.node.jobs_client, job_id)
        .await
        .expect("inspect launched sandboxed job");
    assert!(
        inspected.attempts.iter().any(|attempt| {
            attempt.workload_id == active_workload_id
                && attempt.execution_platform == "oci"
                && attempt.isolation_mode == "sandboxed"
                && attempt.isolation_profile.as_deref() == Some("oci-default")
        }),
        "derived attempt summaries should expose the requested runtime selection"
    );
});

local_test!(jobs_retry_backoff_survives_controller_restart, {
    let state_dir = tempdir().expect("state dir");
    let db_path = state_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let self_id = Uuid::new_v4();
    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([0x21; 32]));
    let signing = ed25519_dalek::SigningKey::from_bytes(&[0x31; 32]);
    let local_volume_root = state_dir.path().join("volumes");
    let runtime = Arc::new(ControllableExitRuntimeBackend::default());

    let node = create_restartable_job_node(
        db.clone(),
        self_id,
        HeadlessKeys::new(noise_keys.clone(), signing.clone()),
        runtime.clone(),
        local_volume_root.clone(),
    )
    .await;

    let job_id = submit_job(&node.jobs_client, "restartable-job", 1, 3)
        .await
        .expect("submit restartable job");

    let first_workload_id =
        wait_for_active_workload(&node.jobs_client, job_id, Duration::from_secs(5))
            .await
            .expect("job should launch first workload before restart");
    runtime
        .signal_workload_exit(first_workload_id, 1)
        .await
        .expect("emit failed runtime exit");

    assert!(
        wait_for_job_status(
            &node.jobs_client,
            job_id,
            ProtoJobStatus::Retrying,
            Duration::from_secs(10)
        )
        .await,
        "job should enter retrying before restart"
    );

    node.shutdown().await.expect("shut down restartable node");

    let restarted = create_restartable_job_node(
        db,
        self_id,
        HeadlessKeys::new(noise_keys, signing),
        runtime.clone(),
        local_volume_root,
    )
    .await;

    assert!(
        !wait_until(Duration::from_secs(1), Duration::from_millis(100), || {
            let client = restarted.jobs_client.clone();
            async move {
                let jobs = list_jobs(&client).await.expect("list jobs after restart");
                jobs.iter().any(|job| {
                    job.id == job_id
                        && job.attempts_started >= 2
                        && job
                            .active_workload_id
                            .is_some_and(|workload_id| workload_id != first_workload_id)
                })
            }
        })
        .await,
        "job should not launch the retry immediately after restart while backoff is still active"
    );

    let second_workload_id = wait_for_active_workload_transition(
        &restarted.jobs_client,
        job_id,
        Some(first_workload_id),
        Duration::from_secs(10),
    )
    .await
    .expect("job should launch retry attempt after persisted backoff");

    runtime
        .signal_workload_exit(second_workload_id, 0)
        .await
        .expect("emit successful runtime exit after restart");

    assert!(
        wait_for_job_status(
            &restarted.jobs_client,
            job_id,
            ProtoJobStatus::Succeeded,
            Duration::from_secs(10)
        )
        .await,
        "restarted controller should complete the retried job successfully"
    );
});

local_test!(jobs_retrying_owner_failover_launches_next_attempt, {
    let backends: Vec<Arc<ControllableExitRuntimeBackend>> = (0..3)
        .map(|_| Arc::new(ControllableExitRuntimeBackend::default()))
        .collect();
    let next_backend = Arc::new(AtomicUsize::new(0));
    let backend_sequence = backends.clone();
    let _guard = RuntimeBackendOverrideGuard::install_factory(Arc::new(move || {
        let index = next_backend.fetch_add(1, Ordering::Relaxed);
        let backend: Arc<dyn RuntimeBackend + Send + Sync> = backend_sequence
            .get(index)
            .expect("runtime backend for cluster node")
            .clone();
        backend
    }));
    let mut cluster = TestNode::new_cluster_inproc_with_config(
        3,
        ClusterConfig {
            sync_tick_ms: Some(100),
            gossip_tick_ms: Some(100),
            ..ClusterConfig::default()
        },
    )
    .await
    .expect("create jobs failover cluster");
    TestNode::wait_cluster_ready_all(&cluster, 3, Duration::from_secs(10))
        .await
        .expect("jobs failover cluster should converge to three ready nodes");
    let backend_by_node = cluster
        .iter()
        .zip(backends.iter())
        .map(|(node, backend)| (node.id(), backend.clone()))
        .collect::<HashMap<_, _>>();

    let job_id = submit_job(&cluster[0].node.jobs_client, "failover-job", 1, 2)
        .await
        .expect("submit failover job");
    let owner_id = select_job_owner_for_test(job_id, &cluster_ids(&cluster)).expect("job owner");
    let owner_index = cluster
        .iter()
        .position(|node| node.id() == owner_id)
        .expect("owner index");
    let owner = &cluster[owner_index];

    let first_workload_id =
        wait_for_active_workload(&owner.node.jobs_client, job_id, Duration::from_secs(10))
            .await
            .expect("job should launch first workload");
    let first_attempt = owner
        .node
        .workload_manager
        .inspect_workload(first_workload_id)
        .await
        .expect("inspect first workload");
    let first_backend = backend_for_node(&backend_by_node, first_attempt.node_id);
    first_backend
        .signal_workload_exit(first_workload_id, 1)
        .await
        .expect("emit failed first workload exit");

    assert!(
        wait_for_job_status(
            &owner.node.jobs_client,
            job_id,
            ProtoJobStatus::Retrying,
            Duration::from_secs(10)
        )
        .await,
        "job should enter retrying before owner failover"
    );

    cluster[owner_index].leave().await.expect("owner leave");
    let departed_owner = cluster.remove(owner_index);
    let departed_owner_node = *departed_owner.node;
    departed_owner_node
        .shutdown()
        .await
        .expect("shut down departed job owner");

    for node in &cluster {
        node.assert_cluster_size(2, "remaining nodes should converge after owner leave")
            .await;
    }

    let remaining_ids = cluster_ids(&cluster);
    let remaining_owner_id =
        select_job_owner_for_test(job_id, &remaining_ids).expect("post-failover job owner");
    let remaining_owner = cluster
        .iter()
        .find(|node| node.id() == remaining_owner_id)
        .expect("post-failover owner node");

    let second_workload_id = wait_for_active_workload_transition(
        &remaining_owner.node.jobs_client,
        job_id,
        Some(first_workload_id),
        Duration::from_secs(12),
    )
    .await
    .expect("remaining owner should launch retry attempt after failover");

    let second_attempt = remaining_owner
        .node
        .workload_manager
        .inspect_workload(second_workload_id)
        .await
        .expect("inspect second workload");
    assert_ne!(
        second_attempt.node_id, owner_id,
        "retry attempt should not be placed on the departed owner"
    );
    let second_backend = backend_for_node(&backend_by_node, second_attempt.node_id);
    second_backend
        .signal_workload_exit(second_workload_id, 0)
        .await
        .expect("emit successful retry exit after failover");

    if !wait_for_job_status(
        &remaining_owner.node.jobs_client,
        job_id,
        ProtoJobStatus::Succeeded,
        Duration::from_secs(20),
    )
    .await
    {
        panic!(
            "post-failover owner should complete the job after the retry attempt exits: {}",
            describe_remaining_job_state(&cluster, job_id, second_workload_id).await
        );
    }

    for node in &cluster {
        assert!(
            wait_for_job_status(
                &node.node.jobs_client,
                job_id,
                ProtoJobStatus::Succeeded,
                Duration::from_secs(10)
            )
            .await,
            "remaining node should observe the completed job after owner failover"
        );
    }
});

#[derive(Clone, Debug)]
struct JobSnapshot {
    id: Uuid,
    status: ProtoJobStatus,
    status_detail: Option<String>,
    attempts_started: u32,
    active_workload_id: Option<Uuid>,
    execution_platform: String,
    isolation_mode: String,
    isolation_profile: Option<String>,
}

#[derive(Clone, Debug)]
struct JobAttemptSnapshot {
    workload_id: Uuid,
    is_active: bool,
    is_last: bool,
    execution_platform: String,
    isolation_mode: String,
    isolation_profile: Option<String>,
}

#[derive(Clone, Debug)]
struct JobDetail {
    snapshot: JobSnapshot,
    attempts: Vec<JobAttemptSnapshot>,
}

/// Submits one first-class job through the jobs capability and returns the generated id.
async fn submit_job(
    client: &jobs::Client,
    name: &str,
    max_retries: u32,
    retry_backoff_secs: u32,
) -> Result<Uuid, capnp::Error> {
    submit_job_with_runtime(
        client,
        name,
        max_retries,
        retry_backoff_secs,
        "oci",
        "standard",
        None,
    )
    .await
}

/// Submits one first-class job with an explicit workload admission mode.
async fn submit_job_with_admission(
    client: &jobs::Client,
    name: &str,
    admission_mode: WorkloadAdmissionMode,
) -> Result<Uuid, capnp::Error> {
    submit_job_with_admission_resources(client, name, admission_mode, 250, 128 * 1024 * 1024).await
}

/// Submits one first-class job with explicit admission and resource requirements.
async fn submit_job_with_admission_resources(
    client: &jobs::Client,
    name: &str,
    admission_mode: WorkloadAdmissionMode,
    cpu_millis: u64,
    memory_bytes: u64,
) -> Result<Uuid, capnp::Error> {
    let mut request = client.submit_request();
    {
        let mut builder = request.get().init_spec();
        builder.set_name(name);
        builder.set_execution_platform("oci");
        builder.set_isolation_mode("standard");
        builder.set_isolation_profile("");
        let mut execution = builder.reborrow().init_execution();
        execution.set_image("ghcr.io/mantissa/demo-job:latest");
        execution.set_tty(false);
        execution.set_cpu_millis(cpu_millis);
        execution.set_memory_bytes(memory_bytes);
        execution.set_gpu_count(0);
        execution.reborrow().init_command(0);
        execution.reborrow().init_env(0);
        execution.reborrow().init_secret_files(0);
        execution.reborrow().init_volumes(0);
        execution.reborrow().init_networks(0);
        let mut retry_policy = builder.reborrow().init_retry_policy();
        retry_policy.set_max_retries(0);
        retry_policy.set_backoff_secs(0);
        let proto_mode = match admission_mode {
            WorkloadAdmissionMode::Incremental => {
                mantissa_protocol::workload::AdmissionMode::Incremental
            }
            WorkloadAdmissionMode::Gang => mantissa_protocol::workload::AdmissionMode::Gang,
        };
        builder
            .reborrow()
            .init_admission_policy()
            .set_mode(proto_mode);
    }
    let response = request.send().promise.await?;
    read_uuid(response.get()?.get_job_id()?)
}

/// Submits one first-class job with explicit runtime selection and returns the generated id.
async fn submit_job_with_runtime(
    client: &jobs::Client,
    name: &str,
    max_retries: u32,
    retry_backoff_secs: u32,
    execution_platform: &str,
    isolation_mode: &str,
    isolation_profile: Option<&str>,
) -> Result<Uuid, capnp::Error> {
    let mut request = client.submit_request();
    {
        let mut builder = request.get().init_spec();
        builder.set_name(name);
        builder.set_execution_platform(execution_platform);
        builder.set_isolation_mode(isolation_mode);
        builder.set_isolation_profile(isolation_profile.unwrap_or_default());
        let mut execution = builder.reborrow().init_execution();
        execution.set_image("ghcr.io/mantissa/demo-job:latest");
        execution.set_tty(false);
        execution.set_cpu_millis(250);
        execution.set_memory_bytes(128 * 1024 * 1024);
        execution.set_gpu_count(0);
        execution.reborrow().init_command(0);
        execution.reborrow().init_env(0);
        execution.reborrow().init_secret_files(0);
        execution.reborrow().init_volumes(0);
        execution.reborrow().init_networks(0);
        let mut retry_policy = builder.reborrow().init_retry_policy();
        retry_policy.set_max_retries(max_retries);
        retry_policy.set_backoff_secs(retry_backoff_secs);
    }
    let response = request.send().promise.await?;
    read_uuid(response.get()?.get_job_id()?)
}

/// Lists the current replicated jobs exposed by the jobs capability.
async fn list_jobs(client: &jobs::Client) -> Result<Vec<JobSnapshot>, capnp::Error> {
    let response = client.list_request().send().promise.await?;
    let jobs = response.get()?.get_jobs()?;
    let mut snapshots = Vec::with_capacity(jobs.len() as usize);
    for reader in jobs.iter() {
        snapshots.push(JobSnapshot {
            id: read_uuid(reader.get_id()?)?,
            status: reader.get_status()?,
            status_detail: read_optional_text(reader.get_status_detail()?.to_str()?),
            attempts_started: reader.get_attempts_started(),
            active_workload_id: read_optional_uuid(reader.get_active_workload_id()?),
            execution_platform: reader.get_execution_platform()?.to_str()?.to_string(),
            isolation_mode: reader.get_isolation_mode()?.to_str()?.to_string(),
            isolation_profile: read_optional_text(reader.get_isolation_profile()?.to_str()?),
        });
    }
    Ok(snapshots)
}

/// Loads one replicated job snapshot by its durable identifier.
async fn inspect_job(client: &jobs::Client, job_id: Uuid) -> Result<JobDetail, capnp::Error> {
    let mut request = client.inspect_request();
    request.get().set_id(job_id.as_bytes());
    let response = request.send().promise.await?;
    let reader = response.get()?.get_job()?;
    let snapshot = reader.get_snapshot()?;
    let mut attempts = Vec::new();
    for attempt in reader.get_attempts()?.iter() {
        attempts.push(JobAttemptSnapshot {
            workload_id: read_uuid(attempt.get_workload_id()?)?,
            is_active: attempt.get_is_active(),
            is_last: attempt.get_is_last(),
            execution_platform: attempt.get_execution_platform()?.to_str()?.to_string(),
            isolation_mode: attempt.get_isolation_mode()?.to_str()?.to_string(),
            isolation_profile: read_optional_text(attempt.get_isolation_profile()?.to_str()?),
        });
    }
    Ok(JobDetail {
        snapshot: JobSnapshot {
            id: read_uuid(snapshot.get_id()?)?,
            status: snapshot.get_status()?,
            status_detail: read_optional_text(snapshot.get_status_detail()?.to_str()?),
            attempts_started: snapshot.get_attempts_started(),
            active_workload_id: read_optional_uuid(snapshot.get_active_workload_id()?),
            execution_platform: snapshot.get_execution_platform()?.to_str()?.to_string(),
            isolation_mode: snapshot.get_isolation_mode()?.to_str()?.to_string(),
            isolation_profile: read_optional_text(snapshot.get_isolation_profile()?.to_str()?),
        },
        attempts,
    })
}

/// Requests cancellation for one job through the jobs capability.
async fn cancel_job(client: &jobs::Client, job_id: Uuid) -> Result<JobSnapshot, capnp::Error> {
    let mut request = client.cancel_request();
    request.get().set_id(job_id.as_bytes());
    let response = request.send().promise.await?;
    let reader = response.get()?.get_job()?;
    Ok(JobSnapshot {
        id: read_uuid(reader.get_id()?)?,
        status: reader.get_status()?,
        status_detail: read_optional_text(reader.get_status_detail()?.to_str()?),
        attempts_started: reader.get_attempts_started(),
        active_workload_id: read_optional_uuid(reader.get_active_workload_id()?),
        execution_platform: reader.get_execution_platform()?.to_str()?.to_string(),
        isolation_mode: reader.get_isolation_mode()?.to_str()?.to_string(),
        isolation_profile: read_optional_text(reader.get_isolation_profile()?.to_str()?),
    })
}

/// Deletes one terminal job through the jobs capability.
async fn delete_job(client: &jobs::Client, job_id: Uuid) -> Result<JobSnapshot, capnp::Error> {
    let mut request = client.delete_request();
    request.get().set_id(job_id.as_bytes());
    let response = request.send().promise.await?;
    let reader = response.get()?.get_job()?;
    Ok(JobSnapshot {
        id: read_uuid(reader.get_id()?)?,
        status: reader.get_status()?,
        status_detail: read_optional_text(reader.get_status_detail()?.to_str()?),
        attempts_started: reader.get_attempts_started(),
        active_workload_id: read_optional_uuid(reader.get_active_workload_id()?),
        execution_platform: reader.get_execution_platform()?.to_str()?.to_string(),
        isolation_mode: reader.get_isolation_mode()?.to_str()?.to_string(),
        isolation_profile: read_optional_text(reader.get_isolation_profile()?.to_str()?),
    })
}

/// Waits until the selected job exposes one active workload identifier.
async fn wait_for_active_workload(
    client: &jobs::Client,
    job_id: Uuid,
    timeout: Duration,
) -> Option<Uuid> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let jobs = list_jobs(client).await.expect("list jobs");
        if let Some(workload_id) = jobs
            .into_iter()
            .find(|job| job.id == job_id)
            .and_then(|job| job.active_workload_id)
        {
            return Some(workload_id);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Waits until the selected job reaches the expected coarse lifecycle status.
async fn wait_for_job_status(
    client: &jobs::Client,
    job_id: Uuid,
    expected: ProtoJobStatus,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || {
        let client = client.clone();
        async move {
            let jobs = list_jobs(&client).await.expect("list jobs");
            jobs.into_iter()
                .any(|job| job.id == job_id && job.status == expected)
        }
    })
    .await
}

/// Builds a compact diagnostic string for one job and workload across remaining nodes.
async fn describe_remaining_job_state(
    nodes: &[TestNode],
    job_id: Uuid,
    workload_id: Uuid,
) -> String {
    let mut entries = Vec::with_capacity(nodes.len());
    for node in nodes {
        let job = match list_jobs(&node.node.jobs_client).await {
            Ok(jobs) => jobs
                .into_iter()
                .find(|job| job.id == job_id)
                .map(|job| {
                    format!(
                        "job={:?}/attempts={}/active={:?}",
                        job.status, job.attempts_started, job.active_workload_id
                    )
                })
                .unwrap_or_else(|| "job=<missing>".to_string()),
            Err(err) => format!("job=<list error: {err}>"),
        };
        let workload = match node
            .node
            .workload_manager
            .inspect_workload(workload_id)
            .await
        {
            Ok(workload) => format!("workload={:?}/host={}", workload.state, workload.node_id),
            Err(err) => format!("workload=<inspect error: {err}>"),
        };
        entries.push(format!("{}: {job}; {workload}", node.id()));
    }
    entries.join(" | ")
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

/// Decodes one optional text field from the public jobs schema.
fn read_optional_text(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Waits until a job exposes a different active workload than one optional previous attempt.
async fn wait_for_active_workload_transition(
    client: &jobs::Client,
    job_id: Uuid,
    previous: Option<Uuid>,
    timeout: Duration,
) -> Option<Uuid> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let jobs = list_jobs(client).await.expect("list jobs");
        if let Some(workload_id) = jobs
            .into_iter()
            .find(|job| job.id == job_id)
            .and_then(|job| job.active_workload_id)
            .filter(|workload_id| Some(*workload_id) != previous)
        {
            return Some(workload_id);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Returns the deterministic rendezvous owner for one job from the provided cluster node ids.
fn select_job_owner_for_test(job_id: Uuid, candidates: &[Uuid]) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = job_owner_score_for_test(job_id, *node_id);
        match best {
            None => best = Some((*node_id, score)),
            Some((_, best_score)) if score > best_score => best = Some((*node_id, score)),
            _ => {}
        }
    }
    best.map(|(node_id, _)| node_id)
}

/// Computes the same rendezvous score used by the job controller to select one owner.
fn job_owner_score_for_test(job_id: Uuid, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"job-owner");
    hasher.update(job_id.as_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

/// Returns the cluster node ids in a deterministic slice order.
fn cluster_ids(cluster: &[TestNode]) -> Vec<Uuid> {
    cluster.iter().map(TestNode::id).collect()
}

/// Resolves the controllable runtime backend hosting one workload by node id.
fn backend_for_node(
    backends_by_node: &HashMap<Uuid, Arc<ControllableExitRuntimeBackend>>,
    node_id: Uuid,
) -> Arc<ControllableExitRuntimeBackend> {
    backends_by_node
        .get(&node_id)
        .cloned()
        .expect("backend node")
}

/// Creates one restartable headless node backed by the provided durable state and runtime.
async fn create_restartable_job_node(
    db: Arc<redb::Database>,
    self_id: Uuid,
    keys: HeadlessKeys,
    runtime_backend: Arc<dyn RuntimeBackend + Send + Sync>,
    local_volume_root: PathBuf,
) -> HeadlessNode {
    HeadlessNode::new_with(
        db,
        self_id,
        keys,
        HeadlessConfig {
            listen_addr: "127.0.0.1:0".to_string(),
            transport: HeadlessTransport::Inproc,
            root_schema_override: None,
            sync_tick: Some(Duration::from_millis(100)),
            sync_fanout: None,
            global_metadata_sync_tick: Some(Duration::from_millis(100)),
            global_metadata_sync_fanout: None,
            gossip_tick: Some(Duration::from_millis(100)),
            gossip_fanout: None,
            network_reconcile_tick: None,
            network_attachment_refresh_tick: None,
            gossip_channel_capacity: None,
            task_runtime: None,
            service_ready_stability: None,
            runtime_set: Some(RuntimeSet::singleton(
                IN_MEMORY_RUNTIME_BACKEND_KIND,
                runtime_backend,
            )),
            local_volume_root: Some(local_volume_root),
            master_key_kdf_params: None,
        },
    )
    .await
    .expect("start restartable job node")
}

#[derive(Default)]
/// Emits controllable runtime exit events for started workloads.
///
/// Jobs need real runtime-backed terminal transitions for success, retry, and
/// restart coverage. This backend keeps the shared in-memory runtime behavior
/// but lets tests inject one explicit exit signal for a chosen workload id.
struct ControllableExitRuntimeBackend {
    inner: InMemoryRuntimeBackend,
    runtime_ids_by_workload: AsyncMutex<HashMap<Uuid, String>>,
    runtime_events_tx: AsyncMutex<Option<tokio::sync::mpsc::UnboundedSender<RuntimeEvent>>>,
    pending_runtime_events: AsyncMutex<Vec<RuntimeEvent>>,
}

impl ControllableExitRuntimeBackend {
    /// Waits until the backend has created a runtime instance for one workload.
    async fn wait_for_runtime_instance(&self, workload_id: Uuid, timeout: Duration) -> bool {
        wait_until(timeout, Duration::from_millis(25), || async {
            self.runtime_ids_by_workload
                .lock()
                .await
                .contains_key(&workload_id)
        })
        .await
    }

    /// Emits one explicit task-exit event for the selected workload id.
    async fn signal_workload_exit(
        &self,
        workload_id: Uuid,
        exit_code: i32,
    ) -> Result<(), RuntimeError> {
        if !self
            .wait_for_runtime_instance(workload_id, Duration::from_secs(5))
            .await
        {
            return Err(RuntimeError::NotFound(format!(
                "runtime instance for workload {workload_id}"
            )));
        }

        let runtime_id = self
            .runtime_ids_by_workload
            .lock()
            .await
            .get(&workload_id)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("runtime instance for workload {workload_id}"))
            })?;
        self.inner.stop_instance(&runtime_id, None).await?;

        let event = RuntimeEvent::TaskExited {
            task_id: workload_id,
            exit_code,
        };
        let sender = self.runtime_events_tx.lock().await.clone();
        if let Some(sender) = sender {
            let _ = sender.send(event);
        } else {
            self.pending_runtime_events.lock().await.push(event);
        }
        Ok(())
    }
}

#[async_trait]
impl RuntimeBackend for ControllableExitRuntimeBackend {
    async fn create_instance(&self, request: RuntimeCreateRequest) -> Result<String, RuntimeError> {
        let workload_id = request
            .labels
            .as_ref()
            .and_then(|labels| labels.get("mantissa.workload_id"))
            .and_then(|raw| Uuid::parse_str(raw).ok());
        let runtime_id = self.inner.create_instance(request).await?;
        if let Some(workload_id) = workload_id {
            self.runtime_ids_by_workload
                .lock()
                .await
                .insert(workload_id, runtime_id.clone());
        }
        Ok(runtime_id)
    }

    async fn start_instance(&self, runtime_id: &str) -> Result<(), RuntimeError> {
        self.inner.start_instance(runtime_id).await
    }

    async fn stop_instance(
        &self,
        runtime_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        self.inner.stop_instance(runtime_id, timeout).await
    }

    async fn restart_instance(
        &self,
        runtime_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        self.inner.restart_instance(runtime_id, timeout).await
    }

    async fn remove_instance(
        &self,
        runtime_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> Result<(), RuntimeError> {
        if let Some(workload_id) = self.runtime_ids_by_workload.lock().await.iter().find_map(
            |(workload_id, current_runtime_id)| {
                (current_runtime_id == runtime_id).then_some(*workload_id)
            },
        ) {
            self.runtime_ids_by_workload
                .lock()
                .await
                .remove(&workload_id);
        }
        self.inner
            .remove_instance(runtime_id, force, remove_volumes)
            .await
    }

    async fn list_instances(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<Vec<RuntimeInfo>, RuntimeError> {
        self.inner.list_instances(filters).await
    }

    async fn inspect_instance(&self, runtime_id: &str) -> Result<RuntimeInfo, RuntimeError> {
        self.inner.inspect_instance(runtime_id).await
    }

    async fn pull_image(&self, image: &str) -> Result<(), RuntimeError> {
        self.inner.pull_image(image).await
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        let mut capabilities = self.inner.capabilities();
        capabilities.lifecycle_events = true;
        capabilities
    }

    async fn watch_runtime_events(
        &self,
        events_tx: tokio::sync::mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<(), RuntimeError> {
        let pending = {
            let mut pending = self.pending_runtime_events.lock().await;
            std::mem::take(&mut *pending)
        };
        *self.runtime_events_tx.lock().await = Some(events_tx.clone());
        for event in pending {
            let _ = events_tx.send(event);
        }
        while !events_tx.is_closed() {
            sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
}
