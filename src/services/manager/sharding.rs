use super::*;
use crate::services::ownership::{
    ServiceDeploymentShard, build_service_deployment_shards, select_shard_coordinator,
};
use crate::workload::manager::{ServiceShardAssignmentFailure, ServiceShardAssignmentRequest};
use crate::workload::model::WorkloadOwner;
use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use thiserror::Error;

/// Retryable failure while a generation owner delegates work to a shard coordinator.
///
/// The coordinator path uses ordinary workload start semantics after the
/// delegation arrives. A remote-session or coordinator availability failure is
/// different from a local scheduler rejection: the owner has not proven that the
/// shard cannot be launched, only that this attempt did not reach the selected
/// coordinator. Keeping this as a typed error lets deployment stay in
/// `Deploying` and retry the deterministic shard later.
#[derive(Debug, Error)]
#[error(
    "service shard {shard_index} coordination with {coordinator_node_id} did not complete: {reason}"
)]
struct ServiceShardCoordinationError {
    shard_index: usize,
    coordinator_node_id: Uuid,
    reason: String,
}

/// Deterministic target-peer shard plan for one service generation launch.
#[derive(Clone, Debug)]
pub(super) struct DeploymentShardPlan {
    service_id: Uuid,
    service_epoch: u64,
    eligible_nodes: Vec<Uuid>,
    target_peer_count: usize,
    target_shards: Vec<ServiceDeploymentShard>,
}

/// Concrete work unit sent to one deployment shard coordinator.
#[derive(Clone)]
struct DeploymentShardWork {
    shard: ServiceDeploymentShard,
    indexed_requests: Vec<(usize, WorkloadStartRequest)>,
}

/// Returns true when deployment should stay in `Deploying` and retry later.
///
/// Direct workload starts already classify missing scheduler prerequisites as
/// retryable. Sharded launches add one more retryable class: a failed handoff to
/// the selected shard coordinator. The owner should not mark the service failed
/// for that case because a later loop can reuse the same deterministic shard
/// plan and the coordinator path is idempotent by workload id.
pub(super) fn deployment_launch_error_requires_service_requeue(err: &anyhow::Error) -> bool {
    workload_start_error_requires_service_requeue(err)
        || err.chain().any(|cause| {
            cause
                .downcast_ref::<ServiceShardCoordinationError>()
                .is_some()
        })
}

/// Returns the configured owner-to-shard coordinator RPC parallelism.
pub(super) fn service_shard_parallelism() -> usize {
    crate::config::replication_runtime_config()
        .service_shard_parallelism
        .max(1)
}

/// Counts unique target nodes in a pinned service launch request batch.
pub(super) fn service_launch_target_peer_count(requests: &[WorkloadStartRequest]) -> usize {
    requests
        .iter()
        .filter_map(|request| request.target_node)
        .collect::<HashSet<_>>()
        .len()
}

/// Splits target-peer shards into task-bounded coordinator work units.
///
/// Target-peer sharding caps how many nodes one coordinator contacts, but it
/// does not cap how many replicas that coordinator starts. This second split
/// keeps each coordinator request bounded by replica count while preserving the
/// same target-peer partition as the outer shard plan.
fn build_deployment_shard_work(
    service_id: Uuid,
    service_epoch: u64,
    eligible_nodes: &[Uuid],
    target_shards: &[ServiceDeploymentShard],
    requests: Vec<WorkloadStartRequest>,
    max_tasks_per_shard: usize,
    context: &str,
) -> anyhow::Result<Vec<DeploymentShardWork>> {
    let max_tasks_per_shard = max_tasks_per_shard.max(1);
    let mut target_to_shard = HashMap::new();
    for shard in target_shards {
        for target_node_id in &shard.target_node_ids {
            if target_to_shard
                .insert(*target_node_id, shard.clone())
                .is_some()
            {
                return Err(anyhow!(
                    "service shard launch for {context} assigned target node {target_node_id} to multiple target shards"
                ));
            }
        }
    }

    let mut grouped: HashMap<usize, (ServiceDeploymentShard, Vec<(usize, WorkloadStartRequest)>)> =
        HashMap::new();
    for (index, request) in requests.into_iter().enumerate() {
        let target_node = request.target_node.ok_or_else(|| {
            anyhow!("service shard launch for {context} received an unpinned workload request")
        })?;
        let shard = target_to_shard.get(&target_node).ok_or_else(|| {
            anyhow!("service shard launch for {context} has no shard for target node {target_node}")
        })?;
        grouped
            .entry(shard.shard_index)
            .or_insert_with(|| (shard.clone(), Vec::new()))
            .1
            .push((index, request));
    }

    let mut target_groups = grouped.into_values().collect::<Vec<_>>();
    target_groups.sort_by_key(|(shard, _)| shard.shard_index);

    let mut work = Vec::new();
    for (_, indexed_requests) in target_groups {
        for chunk in indexed_requests.chunks(max_tasks_per_shard) {
            let shard_index = work.len();
            let mut target_node_ids = chunk
                .iter()
                .filter_map(|(_, request)| request.target_node)
                .collect::<Vec<_>>();
            target_node_ids.sort_unstable();
            target_node_ids.dedup();

            let coordinator_node_id = select_shard_coordinator(
                service_id,
                service_epoch,
                shard_index,
                &target_node_ids,
                eligible_nodes,
            )
            .ok_or_else(|| {
                anyhow!(
                    "service shard launch for {context} could not select coordinator for shard {shard_index}"
                )
            })?;

            work.push(DeploymentShardWork {
                shard: ServiceDeploymentShard {
                    shard_index,
                    coordinator_node_id,
                    target_node_ids,
                },
                indexed_requests: chunk.to_vec(),
            });
        }
    }

    Ok(work)
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
    /// Computes the deterministic shard shape for large targeted service launches.
    pub(super) fn deployment_shard_plan(
        &self,
        requests: &[WorkloadStartRequest],
    ) -> Option<DeploymentShardPlan> {
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
                target_peer_count = target_nodes.len(),
                target_threshold = runtime.service_shard_target_threshold,
                "using direct service deployment launch because target peer count is below the sharding threshold"
            );
            return None;
        }

        let Some((service_id, service_epoch)) = service_generation_from_requests(requests) else {
            tracing::info!(
                target: "services",
                request_count,
                target_peer_count = target_nodes.len(),
                "using direct service deployment launch because requests do not describe one service generation"
            );
            return None;
        };
        let mut eligible_nodes = self.collect_eligible_nodes();
        eligible_nodes.sort_unstable();
        eligible_nodes.dedup();
        let shards = build_service_deployment_shards(
            service_id,
            service_epoch,
            &eligible_nodes,
            &target_nodes,
            runtime.service_shard_target_size,
        );
        if shards.is_empty() {
            tracing::info!(
                target: "services",
                service_id = %service_id,
                service_epoch,
                request_count,
                target_peer_count = target_nodes.len(),
                eligible_peer_count = eligible_nodes.len(),
                target_size = runtime.service_shard_target_size,
                "using direct service deployment launch because no deployment shards could be built"
            );
            return None;
        }

        Some(DeploymentShardPlan {
            service_id,
            service_epoch,
            eligible_nodes,
            target_peer_count: target_nodes.len(),
            target_shards: shards,
        })
    }

    /// Starts a large pinned deployment through deterministic shard coordinators.
    ///
    /// The generation owner sends one service-specific shard request to each
    /// coordinator instead of opening scheduler and assignment sessions to
    /// every target node itself. Coordinators still use the normal workload
    /// manager path, so reservation, target assignment delivery, and sync repair
    /// remain the same mechanisms used by direct owner launches.
    pub(super) async fn start_tasks_with_deployment_shards(
        &self,
        plan: DeploymentShardPlan,
        requests: Vec<WorkloadStartRequest>,
        context: &str,
    ) -> anyhow::Result<Vec<WorkloadSpec>> {
        let DeploymentShardPlan {
            service_id,
            service_epoch,
            eligible_nodes,
            target_peer_count,
            target_shards,
        } = plan;
        let request_count = requests.len();
        let target_shard_count = target_shards.len();
        let max_target_peers_per_shard = target_shards
            .iter()
            .map(|shard| shard.target_node_ids.len())
            .max()
            .unwrap_or(0);
        let task_target_size =
            crate::config::replication_runtime_config().service_shard_task_target_size;
        let work_shards = build_deployment_shard_work(
            service_id,
            service_epoch,
            &eligible_nodes,
            &target_shards,
            requests,
            task_target_size,
            context,
        )?;
        let coordinator_count = work_shards
            .iter()
            .map(|work| work.shard.coordinator_node_id)
            .collect::<HashSet<_>>()
            .len();
        let max_tasks_per_shard = work_shards
            .iter()
            .map(|work| work.indexed_requests.len())
            .max()
            .unwrap_or(0);
        let max_targets_per_shard = work_shards
            .iter()
            .map(|work| work.shard.target_node_ids.len())
            .max()
            .unwrap_or(0);
        let last_shard_index = work_shards
            .last()
            .map(|work| work.shard.shard_index)
            .unwrap_or(0);

        tracing::info!(
            target: "services",
            service_id = %service_id,
            service_epoch,
            target_peer_count,
            target_shard_count,
            shard_count = work_shards.len(),
            coordinator_count,
            max_target_peers_per_shard,
            max_targets_per_shard,
            max_tasks_per_shard,
            task_target_size,
            last_shard_index,
            "computed deterministic service deployment shard plan"
        );

        tracing::info!(
            target: "services",
            service_id = %service_id,
            service_epoch,
            shard_count = work_shards.len(),
            task_count = request_count,
            "delegating service deployment through deterministic shard coordinators for {context}"
        );
        crate::observability::metrics::record_service_deployment_launch_shape(
            "sharded",
            target_peer_count,
            work_shards.len(),
            coordinator_count,
            request_count,
        );

        let mut ordered: Vec<Option<WorkloadSpec>> = vec![None; request_count];
        let mut shard_groups = work_shards;
        shard_groups.sort_by_key(|work| work.shard.shard_index);

        let parallelism = service_shard_parallelism();
        let mut pending_shards = shard_groups.into_iter();
        let mut inflight = FuturesUnordered::new();

        loop {
            while inflight.len() < parallelism {
                let Some(work) = pending_shards.next() else {
                    break;
                };
                inflight.push(self.coordinate_deployment_shard(
                    service_id,
                    service_epoch,
                    work.shard,
                    work.indexed_requests,
                    context,
                ));
            }

            let Some((shard_index, original_indices, specs)) = inflight.next().await else {
                break;
            };
            let specs = specs?;
            if specs.len() != original_indices.len() {
                return Err(anyhow!(
                    "service shard {} for {context} returned {} specs for {} requests",
                    shard_index,
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
                spec.ok_or_else(|| anyhow!("service shard launch for {context} missed row {index}"))
            })
            .collect()
    }

    /// Coordinates one deployment shard locally or through the selected remote coordinator.
    ///
    /// Local coordinator errors keep their original type. Remote errors are
    /// split into two cases: coordinator application failures keep their typed
    /// response classification, while transport/session failures become
    /// retryable handoff failures because the owner cannot know whether the
    /// selected coordinator processed the request.
    async fn coordinate_deployment_shard(
        &self,
        service_id: Uuid,
        service_epoch: u64,
        shard: ServiceDeploymentShard,
        indexed_requests: Vec<(usize, WorkloadStartRequest)>,
        context: &str,
    ) -> (usize, Vec<usize>, anyhow::Result<Vec<WorkloadSpec>>) {
        let original_indices = indexed_requests
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        let shard_requests = indexed_requests
            .into_iter()
            .map(|(_, request)| request)
            .collect::<Vec<_>>();
        let request = ServiceShardAssignmentRequest {
            owner_node_id: self.local_node_id,
            coordinator_node_id: shard.coordinator_node_id,
            service_id,
            service_epoch,
            shard_index: shard.shard_index,
            requests: shard_requests,
        };

        let result = if shard.coordinator_node_id == self.local_node_id {
            self.workload_manager
                .coordinate_service_shard_assignments(request)
                .await
        } else {
            self.workload_manager
                .coordinate_remote_service_shard_assignments(shard.coordinator_node_id, request)
                .await
                .map_err(|err| {
                    if err.chain().any(|cause| {
                        cause
                            .downcast_ref::<ServiceShardAssignmentFailure>()
                            .is_some()
                    }) {
                        return err;
                    }

                    anyhow::Error::new(ServiceShardCoordinationError {
                        shard_index: shard.shard_index,
                        coordinator_node_id: shard.coordinator_node_id,
                        reason: err.to_string(),
                    })
                })
        }
        .with_context(|| {
            format!(
                "service shard {} coordination failed for {context}",
                shard.shard_index
            )
        });

        (shard.shard_index, original_indices, result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::model::{ExecutionPlatform, IsolationMode, WorkloadServiceMetadata};
    use crate::workload::types::ResolvedExecutionSpec;

    /// Builds one minimal pinned service-replica request for shard splitting tests.
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

    /// Ensures shard work is bounded by replica count, not only target-node count.
    #[test]
    fn deployment_shard_work_splits_by_task_count() {
        let service_name = "large-service";
        let service_id = compute_service_id(service_name);
        let service_epoch = 3;
        let eligible_nodes = (1u128..=4).map(Uuid::from_u128).collect::<Vec<_>>();
        let target_shards = build_service_deployment_shards(
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

        let work = build_deployment_shard_work(
            service_id,
            service_epoch,
            &eligible_nodes,
            &target_shards,
            requests,
            3,
            "test deployment",
        )
        .expect("deployment shard work");

        assert_eq!(work.len(), 4);
        assert!(work.iter().all(|work| work.indexed_requests.len() <= 3));
        assert!(work.iter().all(|work| {
            eligible_nodes.contains(&work.shard.coordinator_node_id)
                && !work.shard.target_node_ids.is_empty()
        }));

        let mut original_indices = work
            .iter()
            .flat_map(|work| {
                work.indexed_requests
                    .iter()
                    .map(|(original_index, _)| *original_index)
            })
            .collect::<Vec<_>>();
        original_indices.sort_unstable();
        assert_eq!(original_indices, (0..10).collect::<Vec<_>>());
    }
}
