#[macro_use]
mod common;

use chrono::Utc;
use common::convergence::wait_until;
use common::testkit::{RuntimeBackendOverrideGuard, TestNode};
use crdt_store::uuid_key::UuidKey;
use mantissa::task::types::{TaskStateFilter, TaskValue};
use mantissa::workload::model::{ExecutionSubstrate, IsolationMode, WorkloadPhase, WorkloadSpec};
use protocol::jobs::{JobStatus as ProtoJobStatus, jobs};
use std::time::Duration;
use uuid::Uuid;

local_test!(jobs_submit_and_reach_succeeded_after_task_exit, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
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

    let mut task = node
        .node
        .workload_manager
        .inspect_workload(active_workload_id)
        .await
        .expect("inspect job workload");
    task.state = WorkloadPhase::Exited(0);
    task.updated_at = Utc::now().to_rfc3339();
    node.node
        .workloads
        .upsert(
            &UuidKey::from(active_workload_id),
            task_spec_to_value(&task),
        )
        .await
        .expect("persist successful workload state");

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
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let job_id = submit_job(&node.node.jobs_client, "retry-job", 1, 0)
        .await
        .expect("submit retrying job");

    let first_workload_id =
        wait_for_active_workload(&node.node.jobs_client, job_id, Duration::from_secs(5))
            .await
            .expect("job should launch first workload");

    let mut task = node
        .node
        .workload_manager
        .inspect_workload(first_workload_id)
        .await
        .expect("inspect first job workload");
    task.state = WorkloadPhase::Exited(1);
    task.updated_at = Utc::now().to_rfc3339();
    node.node
        .workloads
        .upsert(&UuidKey::from(first_workload_id), task_spec_to_value(&task))
        .await
        .expect("persist failed workload state");

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
    let _guard = RuntimeBackendOverrideGuard::install_default();
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

    let mut task = node
        .node
        .workload_manager
        .inspect_workload(active_workload_id)
        .await
        .expect("inspect job workload");
    task.state = WorkloadPhase::Exited(0);
    task.updated_at = Utc::now().to_rfc3339();
    node.node
        .workloads
        .upsert(
            &UuidKey::from(active_workload_id),
            task_spec_to_value(&task),
        )
        .await
        .expect("persist successful workload state");

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
    assert_eq!(inspected.snapshot.execution_substrate, "oci");
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
    assert_eq!(workload.execution_substrate, ExecutionSubstrate::Oci);
    assert_eq!(workload.isolation_mode, IsolationMode::Sandboxed);
    assert_eq!(workload.isolation_profile.as_deref(), Some("oci-default"));

    let inspected = inspect_job(&node.node.jobs_client, job_id)
        .await
        .expect("inspect launched sandboxed job");
    assert!(
        inspected.attempts.iter().any(|attempt| {
            attempt.workload_id == active_workload_id
                && attempt.execution_substrate == "oci"
                && attempt.isolation_mode == "sandboxed"
                && attempt.isolation_profile.as_deref() == Some("oci-default")
        }),
        "derived attempt summaries should expose the requested runtime selection"
    );
});

#[derive(Clone, Debug)]
struct JobSnapshot {
    id: Uuid,
    status: ProtoJobStatus,
    attempts_started: u32,
    active_workload_id: Option<Uuid>,
    execution_substrate: String,
    isolation_mode: String,
    isolation_profile: Option<String>,
}

#[derive(Clone, Debug)]
struct JobAttemptSnapshot {
    workload_id: Uuid,
    is_active: bool,
    is_last: bool,
    execution_substrate: String,
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

/// Submits one first-class job with explicit runtime selection and returns the generated id.
async fn submit_job_with_runtime(
    client: &jobs::Client,
    name: &str,
    max_retries: u32,
    retry_backoff_secs: u32,
    execution_substrate: &str,
    isolation_mode: &str,
    isolation_profile: Option<&str>,
) -> Result<Uuid, capnp::Error> {
    let mut request = client.submit_request();
    {
        let mut builder = request.get().init_spec();
        builder.set_name(name);
        builder.set_execution_substrate(execution_substrate);
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
            attempts_started: reader.get_attempts_started(),
            active_workload_id: read_optional_uuid(reader.get_active_workload_id()?),
            execution_substrate: reader.get_execution_substrate()?.to_str()?.to_string(),
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
            execution_substrate: attempt.get_execution_substrate()?.to_str()?.to_string(),
            isolation_mode: attempt.get_isolation_mode()?.to_str()?.to_string(),
            isolation_profile: read_optional_text(attempt.get_isolation_profile()?.to_str()?),
        });
    }
    Ok(JobDetail {
        snapshot: JobSnapshot {
            id: read_uuid(snapshot.get_id()?)?,
            status: snapshot.get_status()?,
            attempts_started: snapshot.get_attempts_started(),
            active_workload_id: read_optional_uuid(snapshot.get_active_workload_id()?),
            execution_substrate: snapshot.get_execution_substrate()?.to_str()?.to_string(),
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
        attempts_started: reader.get_attempts_started(),
        active_workload_id: read_optional_uuid(reader.get_active_workload_id()?),
        execution_substrate: reader.get_execution_substrate()?.to_str()?.to_string(),
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
        attempts_started: reader.get_attempts_started(),
        active_workload_id: read_optional_uuid(reader.get_active_workload_id()?),
        execution_substrate: reader.get_execution_substrate()?.to_str()?.to_string(),
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

/// Decodes one optional text field from the public jobs schema.
fn read_optional_text(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}
