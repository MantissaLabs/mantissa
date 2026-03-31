#[macro_use]
mod common;

use chrono::Utc;
use common::convergence::wait_until;
use common::testkit::{RuntimeBackendOverrideGuard, TestNode};
use crdt_store::uuid_key::UuidKey;
use mantissa::task::types::{TaskStateFilter, TaskValue};
use mantissa::workload::model::WorkloadPhase;
use mantissa::workload::model::WorkloadSpec;
use protocol::jobs::{JobStatus as ProtoJobStatus, jobs};
use std::time::Duration;
use uuid::Uuid;

local_test!(jobs_submit_and_reach_succeeded_after_task_exit, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let job_id = submit_job(&node.node.jobs_client, "demo-job", 0, 0)
        .await
        .expect("submit job");

    let active_task_id =
        wait_for_active_task(&node.node.jobs_client, job_id, Duration::from_secs(5))
            .await
            .expect("job should launch one task");

    let mut task = node
        .node
        .task_manager
        .inspect_task(active_task_id)
        .await
        .expect("inspect job task");
    task.state = WorkloadPhase::Exited(0);
    task.updated_at = Utc::now().to_rfc3339();
    node.node
        .workloads
        .upsert(&UuidKey::from(active_task_id), task_spec_to_value(&task))
        .await
        .expect("persist successful task state");

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

    let first_task_id =
        wait_for_active_task(&node.node.jobs_client, job_id, Duration::from_secs(5))
            .await
            .expect("job should launch first task");

    let mut task = node
        .node
        .task_manager
        .inspect_task(first_task_id)
        .await
        .expect("inspect first job task");
    task.state = WorkloadPhase::Exited(1);
    task.updated_at = Utc::now().to_rfc3339();
    node.node
        .workloads
        .upsert(&UuidKey::from(first_task_id), task_spec_to_value(&task))
        .await
        .expect("persist failed task state");

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
                            .active_task_id
                            .is_some_and(|task_id| task_id != first_task_id)
                })
            }
        })
        .await,
        "job should launch a second attempt after the first task exits unsuccessfully"
    );

    let tasks = node
        .node
        .task_manager
        .list_tasks(&TaskStateFilter::all())
        .await
        .expect("list tasks");
    assert!(
        tasks.iter().any(|task| task.id == first_task_id),
        "first failed task should remain visible in the replicated workload store"
    );
});

#[derive(Clone, Copy)]
struct JobSnapshot {
    id: Uuid,
    status: ProtoJobStatus,
    attempts_started: u32,
    active_task_id: Option<Uuid>,
}

/// Submits one first-class job through the jobs capability and returns the generated id.
async fn submit_job(
    client: &jobs::Client,
    name: &str,
    max_retries: u32,
    retry_backoff_secs: u32,
) -> Result<Uuid, capnp::Error> {
    let mut request = client.submit_request();
    {
        let mut builder = request.get().init_spec();
        builder.set_name(name);
        builder.set_image("ghcr.io/mantissa/demo-job:latest");
        builder.set_tty(false);
        builder.set_cpu_millis(250);
        builder.set_memory_bytes(128 * 1024 * 1024);
        builder.set_gpu_count(0);
        builder.set_max_retries(max_retries);
        builder.set_retry_backoff_secs(retry_backoff_secs);
        builder.reborrow().init_command(0);
        builder.reborrow().init_env(0);
        builder.reborrow().init_secret_files(0);
        builder.reborrow().init_volumes(0);
        builder.reborrow().init_networks(0);
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
            active_task_id: read_optional_uuid(reader.get_active_task_id()?),
        });
    }
    Ok(snapshots)
}

/// Waits until the selected job exposes one active task identifier.
async fn wait_for_active_task(
    client: &jobs::Client,
    job_id: Uuid,
    timeout: Duration,
) -> Option<Uuid> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let jobs = list_jobs(client).await.expect("list jobs");
        if let Some(task_id) = jobs
            .into_iter()
            .find(|job| job.id == job_id)
            .and_then(|job| job.active_task_id)
        {
            return Some(task_id);
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
        runtime_class: spec.runtime_class,
        sandbox_profile: spec.sandbox_profile.clone(),
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
