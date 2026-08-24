use super::*;
use crate::services::ownership::{
    DeploymentCoordinatorSelector, ServiceDeploymentGroup, build_service_deployment_groups,
};
use crate::workload::manager::{ServiceShardAssignmentFailure, ServiceShardAssignmentRequest};
use crate::workload::model::WorkloadOwner;
use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use thiserror::Error;

/// A task batch could not complete through its selected coordinator.
///
/// This error is separate from a task rejection. The deployment can retry it
/// without changing the task IDs.
#[derive(Debug, Error)]
#[error(
    "service batch {batch_index} coordination with {coordinator_node_id} did not complete: {reason}"
)]
struct CoordinatorRequestError {
    batch_index: usize,
    coordinator_node_id: Uuid,
    reason: String,
}

/// Data needed to divide one large service launch among several coordinators.
#[derive(Clone, Debug)]
pub(super) struct DeploymentCoordinatorPlan {
    service_id: Uuid,
    service_epoch: u64,
    eligible_nodes: Vec<Uuid>,
    target_node_count: usize,
    node_groups: Vec<ServiceDeploymentGroup>,
}

/// One coordinator request and the positions of its tasks in the original list.
#[derive(Clone)]
struct CoordinatorBatch {
    group: ServiceDeploymentGroup,
    indexed_requests: Vec<(usize, WorkloadStartRequest)>,
}

/// Returns true when deployment should stay in `Deploying` and retry later.
///
/// A failed coordinator RPC is retryable because the owner cannot tell whether
/// the coordinator handled it. Retrying is safe because task IDs do not change.
pub(super) fn deployment_launch_error_requires_service_requeue(err: &anyhow::Error) -> bool {
    workload_start_error_requires_service_requeue(err)
        || err
            .chain()
            .any(|cause| cause.downcast_ref::<CoordinatorRequestError>().is_some())
}

/// Returns how many coordinator requests may run at the same time.
pub(super) fn service_coordinator_parallelism() -> usize {
    crate::config::replication_runtime_config()
        .service_shard_parallelism
        .max(1)
}

/// Counts unique target nodes in a pinned service launch request batch.
pub(super) fn service_launch_target_node_count(requests: &[WorkloadStartRequest]) -> usize {
    requests
        .iter()
        .filter_map(|request| request.target_node)
        .collect::<HashSet<_>>()
        .len()
}

/// Builds coordinator requests that stay within the configured task limit.
///
/// The existing node groups limit how many nodes one coordinator contacts. A
/// group can still contain many tasks, so this function also limits the number
/// of tasks sent in each request.
fn build_coordinator_batches(
    service_id: Uuid,
    service_epoch: u64,
    eligible_nodes: &[Uuid],
    node_groups: &[ServiceDeploymentGroup],
    requests: Vec<WorkloadStartRequest>,
    max_tasks_per_request: usize,
    context: &str,
) -> anyhow::Result<Vec<CoordinatorBatch>> {
    let max_tasks_per_request = max_tasks_per_request.max(1);
    // The nodes allowed to coordinate are the same for every request.
    let coordinator_selector = DeploymentCoordinatorSelector::new(eligible_nodes);
    let mut target_to_group = HashMap::new();
    for group in node_groups {
        for target_node_id in &group.target_node_ids {
            if target_to_group
                .insert(*target_node_id, group.clone())
                .is_some()
            {
                return Err(anyhow!(
                    "service launch for {context} assigned target node {target_node_id} to multiple groups"
                ));
            }
        }
    }

    let mut requests_by_group: HashMap<
        usize,
        (ServiceDeploymentGroup, Vec<(usize, WorkloadStartRequest)>),
    > = HashMap::new();
    for (index, request) in requests.into_iter().enumerate() {
        let target_node = request.target_node.ok_or_else(|| {
            anyhow!("service launch for {context} received a task without a target node")
        })?;
        let group = target_to_group.get(&target_node).ok_or_else(|| {
            anyhow!("service launch for {context} has no group for target node {target_node}")
        })?;
        requests_by_group
            .entry(group.group_index)
            .or_insert_with(|| (group.clone(), Vec::new()))
            .1
            .push((index, request));
    }

    let mut grouped_requests = requests_by_group.into_values().collect::<Vec<_>>();
    grouped_requests.sort_by_key(|(group, _)| group.group_index);

    let mut batches = Vec::new();
    for (_, indexed_requests) in grouped_requests {
        for chunk in indexed_requests.chunks(max_tasks_per_request) {
            let batch_index = batches.len();
            let mut target_node_ids = chunk
                .iter()
                .filter_map(|(_, request)| request.target_node)
                .collect::<Vec<_>>();
            target_node_ids.sort_unstable();
            target_node_ids.dedup();

            let coordinator_node_id = coordinator_selector
                .select(service_id, service_epoch, batch_index, &target_node_ids)
                .ok_or_else(|| {
                anyhow!(
                    "service launch for {context} could not select coordinator for batch {batch_index}"
                )
            })?;

            batches.push(CoordinatorBatch {
                group: ServiceDeploymentGroup {
                    group_index: batch_index,
                    coordinator_node_id,
                    target_node_ids,
                },
                indexed_requests: chunk.to_vec(),
            });
        }
    }

    Ok(batches)
}

/// Extracts the service generation identity shared by one service-owned start batch.
fn service_generation_from_requests(requests: &[WorkloadStartRequest]) -> Option<(Uuid, u64)> {
    let mut generation = None;
    for request in requests {
        let Some(WorkloadOwner::ServiceReplica(metadata)) = request.owner.as_ref() else {
            return None;
        };
        let current = (
            compute_service_id(&metadata.service_name),
            metadata.service_epoch,
        );
        match generation {
            None => generation = Some(current),
            Some(expected) if expected == current => {}
            Some(_) => return None,
        }
    }
    generation
}

impl ServiceController {
    /// Returns a coordinator plan for a large launch, or `None` for a direct launch.
    pub(super) fn coordinator_plan(
        &self,
        requests: &[WorkloadStartRequest],
    ) -> Option<DeploymentCoordinatorPlan> {
        let request_count = requests.len();
        let mut target_nodes = requests
            .iter()
            .filter_map(|request| request.target_node)
            .collect::<Vec<_>>();
        if target_nodes.len() != requests.len() {
            tracing::info!(
                target: "services",
                request_count,
                pinned_request_count = target_nodes.len(),
                "using direct service deployment launch because not every request has a pinned target"
            );
            return None;
        }
        if requests.iter().any(|request| request.id.is_none()) {
            tracing::info!(
                target: "services",
                request_count,
                "using direct service deployment launch because at least one request is missing a deterministic task id"
            );
            return None;
        }
        target_nodes.sort_unstable();
        target_nodes.dedup();

        let runtime = crate::config::replication_runtime_config();
        if target_nodes.len() < runtime.service_shard_target_threshold {
            tracing::info!(
                target: "services",
                request_count,
                target_node_count = target_nodes.len(),
                target_threshold = runtime.service_shard_target_threshold,
                "using direct service deployment launch because target node count is below the sharding threshold"
            );
            return None;
        }

        let Some((service_id, service_epoch)) = service_generation_from_requests(requests) else {
            tracing::info!(
                target: "services",
                request_count,
                target_node_count = target_nodes.len(),
                "using direct service deployment launch because requests do not describe one service generation"
            );
            return None;
        };
        let mut eligible_nodes = self.collect_eligible_nodes();
        eligible_nodes.sort_unstable();
        eligible_nodes.dedup();
        let node_groups = build_service_deployment_groups(
            service_id,
            service_epoch,
            &eligible_nodes,
            &target_nodes,
            runtime.service_shard_target_size,
        );
        if node_groups.is_empty() {
            tracing::info!(
                target: "services",
                service_id = %service_id,
                service_epoch,
                request_count,
                target_node_count = target_nodes.len(),
                eligible_node_count = eligible_nodes.len(),
                target_size = runtime.service_shard_target_size,
                "using direct service deployment launch because no target-node groups could be built"
            );
            return None;
        }

        Some(DeploymentCoordinatorPlan {
            service_id,
            service_epoch,
            eligible_nodes,
            target_node_count: target_nodes.len(),
            node_groups,
        })
    }

    /// Sends each task batch in a large deployment to its selected coordinator.
    ///
    /// This prevents the service owner from opening a connection to every target
    /// node. Coordinators use the normal task-start path after receiving a batch.
    pub(super) async fn start_tasks_with_coordinators(
        &self,
        plan: DeploymentCoordinatorPlan,
        requests: Vec<WorkloadStartRequest>,
        context: &str,
    ) -> anyhow::Result<Vec<WorkloadSpec>> {
        let DeploymentCoordinatorPlan {
            service_id,
            service_epoch,
            eligible_nodes,
            target_node_count,
            node_groups,
        } = plan;
        let request_count = requests.len();
        let target_group_count = node_groups.len();
        let max_nodes_per_group = node_groups
            .iter()
            .map(|group| group.target_node_ids.len())
            .max()
            .unwrap_or(0);
        let task_target_size =
            crate::config::replication_runtime_config().service_shard_task_target_size;
        let coordinator_batches = build_coordinator_batches(
            service_id,
            service_epoch,
            &eligible_nodes,
            &node_groups,
            requests,
            task_target_size,
            context,
        )?;
        let coordinator_count = coordinator_batches
            .iter()
            .map(|batch| batch.group.coordinator_node_id)
            .collect::<HashSet<_>>()
            .len();
        let max_tasks_per_batch = coordinator_batches
            .iter()
            .map(|batch| batch.indexed_requests.len())
            .max()
            .unwrap_or(0);
        let max_targets_per_batch = coordinator_batches
            .iter()
            .map(|batch| batch.group.target_node_ids.len())
            .max()
            .unwrap_or(0);
        let last_batch_index = coordinator_batches
            .last()
            .map(|batch| batch.group.group_index)
            .unwrap_or(0);

        tracing::info!(
            target: "services",
            service_id = %service_id,
            service_epoch,
            target_node_count,
            target_group_count,
            batch_count = coordinator_batches.len(),
            coordinator_count,
            max_nodes_per_group,
            max_targets_per_batch,
            max_tasks_per_batch,
            task_target_size,
            last_batch_index,
            "computed deterministic service deployment coordinator plan"
        );

        tracing::info!(
            target: "services",
            service_id = %service_id,
            service_epoch,
            batch_count = coordinator_batches.len(),
            task_count = request_count,
            "delegating service deployment through coordinator batches for {context}"
        );
        crate::observability::metrics::record_service_deployment_launch_shape(
            "sharded",
            target_node_count,
            coordinator_batches.len(),
            coordinator_count,
            request_count,
        );

        let mut ordered: Vec<Option<WorkloadSpec>> = vec![None; request_count];
        let mut ordered_batches = coordinator_batches;
        ordered_batches.sort_by_key(|batch| batch.group.group_index);

        let parallelism = service_coordinator_parallelism();
        let mut pending_batches = ordered_batches.into_iter();
        let mut inflight = FuturesUnordered::new();

        loop {
            while inflight.len() < parallelism {
                let Some(batch) = pending_batches.next() else {
                    break;
                };
                inflight.push(self.coordinate_deployment_batch(
                    service_id,
                    service_epoch,
                    batch.group,
                    batch.indexed_requests,
                    context,
                ));
            }

            let Some((batch_index, original_indices, specs)) = inflight.next().await else {
                break;
            };
            let specs = specs?;
            if specs.len() != original_indices.len() {
                return Err(anyhow!(
                    "service batch {} for {context} returned {} specs for {} requests",
                    batch_index,
                    specs.len(),
                    original_indices.len()
                ));
            }

            for (original_index, spec) in original_indices.into_iter().zip(specs) {
                ordered[original_index] = Some(spec);
            }
        }

        ordered
            .into_iter()
            .enumerate()
            .map(|(index, spec)| {
                spec.ok_or_else(|| anyhow!("service launch for {context} missed result {index}"))
            })
            .collect()
    }

    /// Sends one task batch to its coordinator, either locally or over RPC.
    ///
    /// Local coordinator errors keep their original type. Remote errors are
    /// split into two cases: coordinator application failures keep their typed
    /// response classification, while transport/session failures become
    /// retryable handoff failures because the owner cannot know whether the
    /// selected coordinator processed the request.
    async fn coordinate_deployment_batch(
        &self,
        service_id: Uuid,
        service_epoch: u64,
        group: ServiceDeploymentGroup,
        indexed_requests: Vec<(usize, WorkloadStartRequest)>,
        context: &str,
    ) -> (usize, Vec<usize>, anyhow::Result<Vec<WorkloadSpec>>) {
        let original_indices = indexed_requests
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        let batch_requests = indexed_requests
            .into_iter()
            .map(|(_, request)| request)
            .collect::<Vec<_>>();
        let request = ServiceShardAssignmentRequest {
            owner_node_id: self.local_node_id,
            coordinator_node_id: group.coordinator_node_id,
            service_id,
            service_epoch,
            shard_index: group.group_index,
            requests: batch_requests,
        };

        let result = if group.coordinator_node_id == self.local_node_id {
            self.workload_manager
                .coordinate_service_shard_assignments(request)
                .await
        } else {
            self.workload_manager
                .coordinate_remote_service_shard_assignments(group.coordinator_node_id, request)
                .await
                .map_err(|err| {
                    if err.chain().any(|cause| {
                        cause
                            .downcast_ref::<ServiceShardAssignmentFailure>()
                            .is_some()
                    }) {
                        return err;
                    }

                    anyhow::Error::new(CoordinatorRequestError {
                        batch_index: group.group_index,
                        coordinator_node_id: group.coordinator_node_id,
                        reason: err.to_string(),
                    })
                })
        }
        .with_context(|| {
            format!(
                "service batch {} coordinator request failed for {context}",
                group.group_index
            )
        });

        (group.group_index, original_indices, result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::model::{ExecutionPlatform, IsolationMode, WorkloadServiceMetadata};
    use crate::workload::types::ResolvedExecutionSpec;

    /// Builds one task request assigned to a specific node for batching tests.
    fn pinned_service_request(
        service_name: &str,
        service_epoch: u64,
        replica_index: usize,
        target_node: Uuid,
    ) -> WorkloadStartRequest {
        WorkloadStartRequest {
            name: format!("replica-{replica_index}"),
            execution: ResolvedExecutionSpec {
                image: "busybox:latest".to_string(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 100,
                memory_bytes: 32 * 1_024 * 1_024,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                ports: Vec::new(),
                placement: Default::default(),
            },
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: Some(Uuid::from_u128(10_000 + replica_index as u128)),
            slot_ids: Vec::new(),
            owner: Some(WorkloadOwner::ServiceReplica(
                WorkloadServiceMetadata::new(service_name, "web", 1)
                    .with_service_epoch(service_epoch),
            )),
            dependency_requirements: Vec::new(),
            service_placement_preferences: Vec::new(),
            target_node: Some(target_node),
        }
    }

    /// Ensures no coordinator request exceeds the configured task limit.
    #[test]
    fn coordinator_batches_limit_tasks_per_request() {
        let service_name = "large-service";
        let service_id = compute_service_id(service_name);
        let service_epoch = 3;
        let eligible_nodes = (1u128..=4).map(Uuid::from_u128).collect::<Vec<_>>();
        let node_groups = build_service_deployment_groups(
            service_id,
            service_epoch,
            &eligible_nodes,
            &eligible_nodes,
            4,
        );
        let requests = (0..10)
            .map(|index| {
                pinned_service_request(
                    service_name,
                    service_epoch,
                    index,
                    eligible_nodes[index % eligible_nodes.len()],
                )
            })
            .collect::<Vec<_>>();

        let batches = build_coordinator_batches(
            service_id,
            service_epoch,
            &eligible_nodes,
            &node_groups,
            requests,
            3,
            "test deployment",
        )
        .expect("coordinator batches");

        assert_eq!(batches.len(), 4);
        assert!(
            batches
                .iter()
                .all(|batch| batch.indexed_requests.len() <= 3)
        );
        assert!(batches.iter().all(|batch| {
            eligible_nodes.contains(&batch.group.coordinator_node_id)
                && !batch.group.target_node_ids.is_empty()
        }));

        let mut original_indices = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .indexed_requests
                    .iter()
                    .map(|(original_index, _)| *original_index)
            })
            .collect::<Vec<_>>();
        original_indices.sort_unstable();
        assert_eq!(original_indices, (0..10).collect::<Vec<_>>());
    }
}
