use anyhow::{Context, Result};
use futures::{StreamExt, TryStreamExt, stream};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Weak};
use std::time::Duration;
use uuid::Uuid;

use super::runtime::{LeaderVolumeGroupState, ReplicatedVolumeRuntime};
use crate::cluster::ClusterViewId;
use crate::cluster::operations::{
    ClusterOperationKind, ClusterOperationRecord, ClusterOperationStage, SplitNodeAssignment,
};
use crate::store::replicated::cluster_operations::ClusterOperationStore;
use crate::volumes::registry::VolumeRegistry;
use crate::volumes::types::{
    ReplicatedVolumeGroupStatusValue, ReplicatedVolumePlan, VolumeDriver, VolumeNodeState,
    VolumeNodeStateValue, VolumeSpecValue, compute_replicated_volume_group_id,
};

/// Validation bound used only when this node has no replicated-volume runtime.
const SPLIT_VALIDATION_WITHOUT_STORAGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Bounds concurrent group reads without making split time linear in volume count.
const SPLIT_GROUP_READ_CONCURRENCY: usize = 16;

/// One stable group placement proven from a linearizable Raft read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplicatedVolumeSplitPlacement {
    pub(crate) volume_id: Uuid,
    pub(crate) volume_name: String,
    pub(crate) generation: u64,
    pub(crate) group_id: Uuid,
    pub(crate) target_view: ClusterViewId,
    pub(crate) voter_node_ids: BTreeSet<Uuid>,
}

/// Converged public metadata reused across the two Raft membership reads.
struct SplitVolumeGroup {
    spec: VolumeSpecValue,
    plan: ReplicatedVolumePlan,
    status: Option<ReplicatedVolumeGroupStatusValue>,
    node_states: Vec<VolumeNodeStateValue>,
}

/// Pauses replica and Raft membership changes while a split is being checked.
#[derive(Clone)]
pub(crate) struct VolumeMembershipChangeBlocker {
    cluster_operations: ClusterOperationStore,
    cluster_view: crate::cluster::ClusterViewState,
}

impl VolumeMembershipChangeBlocker {
    /// Builds the shared membership gate from durable split state and the local view.
    pub(crate) fn new(
        cluster_operations: ClusterOperationStore,
        cluster_view: crate::cluster::ClusterViewState,
    ) -> Self {
        Self {
            cluster_operations,
            cluster_view,
        }
    }

    /// Returns whether a ready split currently pauses membership changes.
    pub(crate) fn changes_are_blocked(&self) -> Result<bool> {
        let operations = self
            .cluster_operations
            .list_records()
            .context("load cluster operations for replicated-volume membership")?;
        Ok(split_blocks_membership_changes(
            &operations,
            self.cluster_view.active_view(),
        ))
    }

    /// Rejects one replica or Raft membership change while a split is active.
    pub(crate) fn ensure_changes_allowed(&self) -> Result<()> {
        if self.changes_are_blocked()? {
            anyhow::bail!(
                "replicated-volume membership changes are paused while a cluster split is in progress"
            );
        }
        Ok(())
    }
}

/// Proves that each replicated-volume group stays inside one split target.
#[derive(Clone)]
pub(crate) struct ReplicatedVolumeSplitValidator {
    volumes: VolumeRegistry,
    runtime: Option<Weak<ReplicatedVolumeRuntime>>,
}

impl ReplicatedVolumeSplitValidator {
    /// Builds validation around the recovered storage runtime, when available.
    pub(crate) fn new(
        volumes: VolumeRegistry,
        runtime: Option<Arc<ReplicatedVolumeRuntime>>,
    ) -> Self {
        Self {
            volumes,
            runtime: runtime.as_ref().map(Arc::downgrade),
        }
    }

    /// Returns the storage operation bound used by preflight and authoritative validation.
    pub(crate) fn validation_timeout(&self) -> Duration {
        self.runtime
            .as_ref()
            .and_then(Weak::upgrade)
            .map(|runtime| runtime.operation_timeout())
            .unwrap_or(SPLIT_VALIDATION_WITHOUT_STORAGE_TIMEOUT)
    }

    /// Returns whether this node must prove source-wide volume metadata convergence.
    pub(crate) fn requires_source_metadata_convergence(&self) -> Result<bool> {
        if self.runtime.as_ref().and_then(Weak::upgrade).is_some() {
            return Ok(true);
        }
        if !self
            .volumes
            .replicated_volume_generations_with_records()?
            .is_empty()
        {
            return Ok(true);
        }
        Ok(self
            .volumes
            .list_specs_including_deleting()?
            .into_iter()
            .any(|spec| matches!(spec.driver, VolumeDriver::Replicated(_))))
    }

    /// Runs one best-effort request-time placement check before the split row is saved.
    pub(crate) async fn check_before_submit(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<Vec<ReplicatedVolumeSplitPlacement>> {
        let assignments = split_assignment_map(operation)?;
        let groups = self.load_groups()?;
        tokio::time::timeout(
            self.validation_timeout(),
            self.inspect_groups(operation, &assignments, &groups),
        )
        .await
        .context("replicated-volume split preflight timed out")?
    }

    /// Reads every group twice and rejects membership that changed between the reads.
    pub(crate) async fn validate_unchanged_membership(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<Vec<ReplicatedVolumeSplitPlacement>> {
        let assignments = split_assignment_map(operation)?;
        let groups = self.load_groups()?;
        let first = self
            .inspect_groups(operation, &assignments, &groups)
            .await?;
        let second = self
            .inspect_groups(operation, &assignments, &groups)
            .await?;
        ensure_same_group_membership(&first, &second)?;
        Ok(second)
    }

    /// Loads public metadata in fixed scans rather than one full scan per volume.
    fn load_groups(&self) -> Result<Vec<SplitVolumeGroup>> {
        let generations_with_records = self.volumes.replicated_volume_generations_with_records()?;
        let mut node_states_by_volume = HashMap::<Uuid, Vec<VolumeNodeStateValue>>::new();
        for node_state in self.volumes.list_node_states_including_deleting()? {
            node_states_by_volume
                .entry(node_state.volume_id)
                .or_default()
                .push(node_state);
        }

        let mut groups = Vec::new();
        for spec in self.volumes.list_specs_including_deleting()? {
            if !matches!(spec.driver, VolumeDriver::Replicated(_)) {
                continue;
            }
            let plan = self.volumes.get_plan(spec.id)?;
            if spec.is_delete_marker() && plan.is_none() {
                if generations_with_records.contains(&(spec.id, spec.volume_epoch)) {
                    anyhow::bail!(
                        "cannot split cluster: deleted replicated volume {} still has metadata to remove",
                        spec.name
                    );
                }
                continue;
            }
            let plan = plan.with_context(|| {
                format!(
                    "cannot split cluster: replicated volume {} has no completed replica plan",
                    spec.name
                )
            })?;
            let status = self.volumes.get_group_status(spec.id)?;
            let node_states = node_states_by_volume.remove(&spec.id).unwrap_or_default();
            groups.push(SplitVolumeGroup {
                spec,
                plan,
                status,
                node_states,
            });
        }
        Ok(groups)
    }

    /// Reads and validates every replicated-volume group known in converged metadata.
    async fn inspect_groups(
        &self,
        operation: &ClusterOperationRecord,
        assignments: &HashMap<Uuid, usize>,
        groups: &[SplitVolumeGroup],
    ) -> Result<Vec<ReplicatedVolumeSplitPlacement>> {
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        let runtime = self.runtime()?;
        let mut placements = stream::iter(groups.iter().map(|group| {
            let runtime = Arc::clone(&runtime);
            async move {
                self.inspect_group(&runtime, group, operation, assignments)
                    .await
            }
        }))
        .buffer_unordered(SPLIT_GROUP_READ_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
        placements.sort_by_key(|placement| (placement.volume_name.clone(), placement.volume_id));
        Ok(placements)
    }

    /// Finds the leader and validates one complete group observation.
    async fn inspect_group(
        &self,
        runtime: &ReplicatedVolumeRuntime,
        group: &SplitVolumeGroup,
        operation: &ClusterOperationRecord,
        assignments: &HashMap<Uuid, usize>,
    ) -> Result<ReplicatedVolumeSplitPlacement> {
        let descriptor = group
            .plan
            .descriptor
            .to_storage()
            .context("decode replicated-volume descriptor for split")?;
        let observation = self
            .read_group_from_current_leader(runtime, group, descriptor.clone())
            .await?;
        validate_stable_group(&group.spec, &group.plan, &observation)?;
        validate_public_replica_rows(
            &group.spec,
            &group.node_states,
            &observation.membership.members,
        )?;
        let target_index = one_group_target(
            &group.spec.name,
            &observation.membership.members,
            assignments,
        )?;
        let target_view = operation
            .target_views
            .get(target_index)
            .copied()
            .with_context(|| {
                format!(
                    "cannot split cluster: replicated volume {} references missing split target {}",
                    group.spec.name, target_index
                )
            })?;
        Ok(ReplicatedVolumeSplitPlacement {
            volume_id: group.spec.id,
            volume_name: group.spec.name.clone(),
            generation: descriptor.generation().get(),
            group_id: compute_replicated_volume_group_id(
                group.plan.descriptor.volume_id,
                group.plan.descriptor.generation,
            ),
            target_view,
            voter_node_ids: observation.membership.voters,
        })
    }

    /// Tries current leader and member hints until one leader confirms a quorum read.
    async fn read_group_from_current_leader(
        &self,
        runtime: &ReplicatedVolumeRuntime,
        group: &SplitVolumeGroup,
        descriptor: mantissa_volume::VolumeDescriptor,
    ) -> Result<LeaderVolumeGroupState> {
        let mut candidates = Vec::new();
        if let Some(status) = &group.status {
            push_candidate(&mut candidates, status.leader_node_id);
            push_candidate(&mut candidates, Some(status.reporter_node_id));
            for node_id in &status.voter_node_ids {
                push_candidate(&mut candidates, Some(*node_id));
            }
        }
        for node_id in group.plan.replica_node_ids {
            push_candidate(&mut candidates, Some(node_id));
        }
        for row in &group.node_states {
            push_candidate(&mut candidates, Some(row.node_id));
        }

        let mut failures = Vec::new();
        let mut index = 0;
        while let Some(candidate) = candidates.get(index).copied() {
            index += 1;
            match runtime
                .inspect_quorum_state_on(candidate, descriptor.clone())
                .await
            {
                Ok(observation) => return Ok(observation),
                Err(error) => failures.push(format!("{candidate}: {error:#}")),
            }
            if let Ok(status) = runtime
                .inspect_replica_on(candidate, descriptor.clone())
                .await
            {
                push_candidate(&mut candidates, status.leader_node_id);
                for voter in status.voter_node_ids {
                    push_candidate(&mut candidates, Some(voter));
                }
            }
        }
        anyhow::bail!(
            "cannot split cluster: replicated volume {} has no reachable Raft leader{}",
            group.spec.name,
            if failures.is_empty() {
                String::new()
            } else {
                format!("; tried {}", failures.join(", "))
            }
        )
    }

    /// Clones the recovered runtime only for the bounded validation call.
    fn runtime(&self) -> Result<Arc<ReplicatedVolumeRuntime>> {
        self.runtime
            .as_ref()
            .and_then(Weak::upgrade)
            .context("replicated-volume storage is unavailable for cluster split validation")
    }
}

/// Rejects old public replica rows that name a node outside current membership.
fn validate_public_replica_rows(
    spec: &VolumeSpecValue,
    node_states: &[VolumeNodeStateValue],
    current_members: &BTreeSet<Uuid>,
) -> Result<()> {
    for row in node_states {
        if current_members.contains(&row.node_id) || row.state == VolumeNodeState::Pending {
            continue;
        }
        anyhow::bail!(
            "cannot split cluster: replicated volume {} still has replica state {:?} on node {} outside current Raft membership",
            spec.name,
            row.state,
            row.node_id
        );
    }
    Ok(())
}

/// Adds one non-zero candidate once while preserving preferred order.
fn push_candidate(candidates: &mut Vec<Uuid>, candidate: Option<Uuid>) {
    let Some(candidate) = candidate.filter(|candidate| !candidate.is_nil()) else {
        return;
    };
    if !candidates.contains(&candidate) {
        candidates.push(candidate);
    }
}

/// Returns exact node-to-target assignments after validating their shape.
fn split_assignment_map(operation: &ClusterOperationRecord) -> Result<HashMap<Uuid, usize>> {
    if operation.kind != ClusterOperationKind::Split {
        anyhow::bail!("replicated-volume placement can only validate a cluster split");
    }
    let mut assignments = HashMap::with_capacity(operation.split_assignments.len());
    for SplitNodeAssignment {
        node_id,
        target_index,
    } in &operation.split_assignments
    {
        if *target_index >= operation.target_views.len() {
            anyhow::bail!(
                "cluster split assignment for node {} references missing target {}",
                node_id,
                target_index
            );
        }
        if assignments.insert(*node_id, *target_index).is_some() {
            anyhow::bail!("cluster split assigns node {} more than once", node_id);
        }
    }
    Ok(assignments)
}

/// Requires every group member to have one assignment to the same target.
fn one_group_target(
    volume_name: &str,
    member_node_ids: &BTreeSet<Uuid>,
    assignments: &HashMap<Uuid, usize>,
) -> Result<usize> {
    let mut target = None;
    for node_id in member_node_ids {
        let node_target = assignments.get(node_id).copied().with_context(|| {
            format!(
                "cannot split cluster: replicated volume {volume_name} member {node_id} has no split assignment"
            )
        })?;
        match target {
            None => target = Some(node_target),
            Some(current) if current == node_target => {}
            Some(_) => {
                anyhow::bail!(
                    "cannot split cluster: replicated volume {volume_name} has members in more than one target"
                );
            }
        }
    }
    target.with_context(|| {
        format!("cannot split cluster: replicated volume {volume_name} has no Raft members")
    })
}

/// Checks the steady-state conditions required before sibling nodes are excluded.
fn validate_stable_group(
    spec: &VolumeSpecValue,
    plan: &ReplicatedVolumePlan,
    observation: &LeaderVolumeGroupState,
) -> Result<()> {
    let descriptor = observation.control_state.descriptor().with_context(|| {
        format!(
            "cannot split cluster: replicated volume {} is not initialized",
            spec.name
        )
    })?;
    let planned = plan.descriptor.to_storage()?;
    if !descriptor.has_same_storage_identity(&planned) {
        anyhow::bail!(
            "cannot split cluster: replicated volume {} returned a different Raft group",
            spec.name
        );
    }
    if observation.membership.is_joint {
        anyhow::bail!(
            "cannot split cluster: replicated volume {} is changing Raft voters",
            spec.name
        );
    }
    if observation.membership.voters.len() != 3 {
        anyhow::bail!(
            "cannot split cluster: replicated volume {} has {} Raft voters instead of 3",
            spec.name,
            observation.membership.voters.len()
        );
    }
    if observation.membership.members != observation.membership.voters {
        anyhow::bail!(
            "cannot split cluster: replicated volume {} has a Raft learner or extra member",
            spec.name
        );
    }
    let data = observation.control_state.data().with_context(|| {
        format!(
            "cannot split cluster: replicated volume {} has no committed data-copy state",
            spec.name
        )
    })?;
    let copy_node_ids = data
        .copies
        .iter()
        .map(|node_id| *node_id.as_uuid())
        .collect::<BTreeSet<_>>();
    if let Some(writer) = data.writer
        && !observation
            .membership
            .voters
            .contains(writer.node_id.as_uuid())
    {
        anyhow::bail!(
            "cannot split cluster: replicated volume {} writer {} is not a Raft voter",
            spec.name,
            writer.node_id.as_uuid()
        );
    }
    if copy_node_ids != observation.membership.voters {
        anyhow::bail!(
            "cannot split cluster: replicated volume {} has not finished reconciling data copies and Raft voters",
            spec.name
        );
    }
    if observation.control_state.replacement().is_some() {
        anyhow::bail!(
            "cannot split cluster: replicated volume {} is replacing a replica",
            spec.name
        );
    }
    if data.recovery.is_some() {
        anyhow::bail!(
            "cannot split cluster: replicated volume {} is recovering replica data",
            spec.name
        );
    }
    Ok(())
}

/// Rejects a voter change between the two post-freeze observations.
fn ensure_same_group_membership(
    first: &[ReplicatedVolumeSplitPlacement],
    second: &[ReplicatedVolumeSplitPlacement],
) -> Result<()> {
    let first = first
        .iter()
        .map(|placement| (placement.group_id, &placement.voter_node_ids))
        .collect::<BTreeMap<_, _>>();
    let second = second
        .iter()
        .map(|placement| (placement.group_id, &placement.voter_node_ids))
        .collect::<BTreeMap<_, _>>();
    if first != second {
        anyhow::bail!(
            "cannot split cluster: replicated-volume Raft membership changed during validation"
        );
    }
    Ok(())
}

/// Returns whether one ready split still affects the local source view.
fn split_blocks_membership_changes(
    operations: &[ClusterOperationRecord],
    active_view: ClusterViewId,
) -> bool {
    let by_id = operations
        .iter()
        .map(|operation| (operation.id, operation))
        .collect::<HashMap<_, _>>();
    operations.iter().any(|operation| {
        split_can_still_change_view(operation, active_view)
            && operation
                .dependency_operation_ids
                .iter()
                .all(|dependency_id| {
                    by_id.get(dependency_id).is_some_and(|dependency| {
                        dependency.stage == ClusterOperationStage::Finalized
                    })
                })
    })
}

/// Returns whether one split has not yet completed or aborted from this source view.
fn split_can_still_change_view(
    operation: &ClusterOperationRecord,
    active_view: ClusterViewId,
) -> bool {
    !operation.dry_run
        && operation.kind == ClusterOperationKind::Split
        && operation.stage != ClusterOperationStage::Aborted
        && operation.source_views.contains(&active_view)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ClusterId, ClusterViewId};
    use crate::volumes::types::{
        FilesystemOwnership, ReplicatedVolumeFilesystem, ReplicatedVolumeSpec,
        SavedVolumeDescriptor, VolumeAccessMode, VolumeBindingMode, VolumeReclaimPolicy,
        VolumeSpecDraft,
    };
    use mantissa_volume::control_state::{
        BeginReplicaReplacement, BeginVolumeRecovery, ExpectedVolumeRevision, GrantVolumeWriter,
        InitializeVolume, RecoveryGrant, ReplacementGrant, VolumeCommand, VolumeControlState,
        WriterGrant,
    };
    use mantissa_volume::{
        DriverSessionId, RecoveryId, ReplacementId, VolumeGeneration, VolumeNodeId,
    };

    /// Builds deterministic assignments for two three-node targets.
    fn assignments(groups: [[u128; 3]; 2]) -> (ClusterOperationRecord, HashMap<Uuid, usize>) {
        let target_views = vec![
            ClusterViewId::new(ClusterId::from_uuid(Uuid::from_u128(101)), 1),
            ClusterViewId::new(ClusterId::from_uuid(Uuid::from_u128(102)), 1),
        ];
        let split_assignments = groups
            .into_iter()
            .enumerate()
            .flat_map(|(target_index, nodes)| {
                nodes.into_iter().map(move |node_id| SplitNodeAssignment {
                    node_id: Uuid::from_u128(node_id),
                    target_index,
                })
            })
            .collect::<Vec<_>>();
        let operation = ClusterOperationRecord {
            id: Uuid::from_u128(200),
            submitted_by_node_id: Uuid::from_u128(1),
            kind: ClusterOperationKind::Split,
            stage: ClusterOperationStage::Proposed,
            dry_run: false,
            created_at_unix_ms: 1,
            dependency_operation_ids: Vec::new(),
            source_views: vec![ClusterViewId::legacy_default()],
            target_views,
            target_cluster_names: vec!["left".to_string(), "right".to_string()],
            split_assignments,
            split_service_policy: Default::default(),
            split_network_policy: Default::default(),
            merge_service_policy: Default::default(),
            updated_at_unix_ms: 1,
            details: String::new(),
        };
        let map = split_assignment_map(&operation).expect("valid assignments");
        (operation, map)
    }

    /// Builds a volume, immutable plan, and initialized group observation.
    fn stable_group(
        copies: [u128; 3],
    ) -> (
        VolumeSpecValue,
        ReplicatedVolumePlan,
        LeaderVolumeGroupState,
    ) {
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: "database".to_string(),
            driver: VolumeDriver::Replicated(ReplicatedVolumeSpec {
                ownership: FilesystemOwnership::Daemon,
                filesystem: ReplicatedVolumeFilesystem::Ext4,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Delete,
            initial_capacity_bytes: Some(64 << 20),
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        spec.plan_coordinator_node_id = Some(Uuid::from_u128(1));
        let descriptor = SavedVolumeDescriptor::for_volume(
            spec.id,
            spec.volume_epoch,
            spec.initial_capacity_bytes.expect("test capacity"),
        )
        .expect("valid descriptor");
        let plan = ReplicatedVolumePlan::new(
            spec.id,
            spec.volume_epoch,
            Uuid::from_u128(50),
            Uuid::from_u128(1),
            [Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)],
            descriptor,
        );
        let copy_node_ids = copies
            .map(|node_id| VolumeNodeId::new(Uuid::from_u128(node_id)).expect("non-zero copy node"))
            .into_iter()
            .collect();
        let control_state = VolumeControlState::default()
            .evaluate(&VolumeCommand::Initialize(InitializeVolume {
                descriptor: descriptor.to_storage().expect("storage descriptor"),
                initial_copies: copy_node_ids,
            }))
            .state;
        let voters = copies
            .into_iter()
            .map(Uuid::from_u128)
            .collect::<BTreeSet<_>>();
        let observation = LeaderVolumeGroupState {
            control_state,
            membership: super::super::runtime::RaftMembershipSnapshot {
                voters: voters.clone(),
                members: voters,
                is_joint: false,
            },
        };
        (spec, plan, observation)
    }

    /// A six-node split accepts a group kept wholly in the first target.
    #[test]
    fn six_node_split_accepts_group_inside_one_target() {
        let (_operation, assignments) = assignments([[1, 2, 3], [4, 5, 6]]);
        let members = [1, 2, 3]
            .into_iter()
            .map(Uuid::from_u128)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            one_group_target("database", &members, &assignments).expect("safe placement"),
            0
        );
    }

    /// A six-node split rejects a group divided between its targets.
    #[test]
    fn six_node_split_rejects_divided_group() {
        let (_operation, assignments) = assignments([[1, 2, 3], [4, 5, 6]]);
        let members = [1, 2, 6]
            .into_iter()
            .map(Uuid::from_u128)
            .collect::<BTreeSet<_>>();
        let error = one_group_target("database", &members, &assignments)
            .expect_err("divided placement must fail");
        assert!(error.to_string().contains("more than one target"));
    }

    /// Regrouping the same six nodes around the current voters makes the split valid.
    #[test]
    fn six_node_split_accepts_replacement_voters_kept_together() {
        let (_operation, assignments) = assignments([[1, 2, 6], [3, 4, 5]]);
        let members = [1, 2, 6]
            .into_iter()
            .map(Uuid::from_u128)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            one_group_target("database", &members, &assignments).expect("safe placement"),
            0
        );
    }

    /// Missing assignments fail rather than guessing a target for one voter.
    #[test]
    fn six_node_split_rejects_missing_voter_assignment() {
        let (_operation, mut assignments) = assignments([[1, 2, 3], [4, 5, 6]]);
        assignments.remove(&Uuid::from_u128(3));
        let members = [1, 2, 3]
            .into_iter()
            .map(Uuid::from_u128)
            .collect::<BTreeSet<_>>();
        let error = one_group_target("database", &members, &assignments)
            .expect_err("missing voter must fail");
        assert!(error.to_string().contains("has no split assignment"));
    }

    /// Each volume independently constrains the same immutable assignments.
    #[test]
    fn six_node_split_checks_multiple_groups_independently() {
        let (_operation, assignments) = assignments([[1, 2, 3], [4, 5, 6]]);
        for members in [[1, 2, 3], [4, 5, 6]] {
            let members = members
                .into_iter()
                .map(Uuid::from_u128)
                .collect::<BTreeSet<_>>();
            one_group_target("volume", &members, &assignments)
                .expect("each complete group should fit one target");
        }
        let overlapping = [1, 3, 4]
            .into_iter()
            .map(Uuid::from_u128)
            .collect::<BTreeSet<_>>();
        assert!(one_group_target("overlap", &overlapping, &assignments).is_err());
    }

    /// Stable current voters override the original bootstrap-node placement.
    #[test]
    fn stable_post_replacement_voters_override_plan_nodes() {
        let (spec, plan, observation) = stable_group([1, 2, 6]);

        validate_stable_group(&spec, &plan, &observation)
            .expect("the immutable plan should only identify the group");
    }

    /// Joint membership, learners, and copy mismatch are never safe split points.
    #[test]
    fn unstable_membership_blocks_split() {
        let (spec, plan, observation) = stable_group([1, 2, 3]);

        let mut joint = observation.clone();
        joint.membership.is_joint = true;
        assert!(validate_stable_group(&spec, &plan, &joint).is_err());

        let mut learner = observation.clone();
        learner.membership.members.insert(Uuid::from_u128(4));
        assert!(validate_stable_group(&spec, &plan, &learner).is_err());

        let mut copy_mismatch = observation;
        copy_mismatch.membership.voters.remove(&Uuid::from_u128(3));
        copy_mismatch.membership.voters.insert(Uuid::from_u128(4));
        copy_mismatch.membership.members = copy_mismatch.membership.voters.clone();
        assert!(validate_stable_group(&spec, &plan, &copy_mismatch).is_err());
    }

    /// A writer outside the stable voter set cannot survive either split child safely.
    #[test]
    fn writer_outside_voters_blocks_split() {
        let (spec, plan, mut observation) = stable_group([1, 2, 3]);
        let state = observation
            .control_state
            .evaluate(&VolumeCommand::GrantWriter(GrantVolumeWriter {
                expected: ExpectedVolumeRevision {
                    generation: VolumeGeneration::new(1).expect("generation"),
                    revision: observation.control_state.revision(),
                },
                writer: WriterGrant {
                    node_id: VolumeNodeId::new(Uuid::from_u128(3)).expect("writer node"),
                    session_id: DriverSessionId::new(Uuid::from_u128(70)).expect("writer session"),
                },
            }))
            .state;
        observation.control_state = state;
        observation.membership.voters.remove(&Uuid::from_u128(3));
        observation.membership.voters.insert(Uuid::from_u128(4));
        observation.membership.members = observation.membership.voters.clone();

        let error = validate_stable_group(&spec, &plan, &observation)
            .expect_err("a writer outside the voters must block the split");
        assert!(error.to_string().contains("is not a Raft voter"));
    }

    /// Active replacement and recovery work must finish before a split.
    #[test]
    fn active_copy_change_blocks_split() {
        let (spec, plan, observation) = stable_group([1, 2, 3]);
        let initial = observation.control_state.clone();
        let replacing = initial
            .evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
                expected: ExpectedVolumeRevision {
                    generation: VolumeGeneration::new(1).expect("generation"),
                    revision: initial.revision(),
                },
                replacement: ReplacementGrant {
                    id: ReplacementId::new(Uuid::from_u128(60)).expect("replacement ID"),
                    coordinator_node_id: VolumeNodeId::new(Uuid::from_u128(1))
                        .expect("coordinator"),
                    old_node_id: Some(VolumeNodeId::new(Uuid::from_u128(3)).expect("old copy")),
                    new_node_id: VolumeNodeId::new(Uuid::from_u128(4)).expect("new copy"),
                    source_node_id: VolumeNodeId::new(Uuid::from_u128(1)).expect("source copy"),
                },
            }))
            .state;
        let mut replacing_observation = observation.clone();
        replacing_observation.control_state = replacing;
        assert!(validate_stable_group(&spec, &plan, &replacing_observation).is_err());

        let recovery = RecoveryGrant {
            id: RecoveryId::new(Uuid::from_u128(70)).expect("recovery ID"),
            coordinator_node_id: VolumeNodeId::new(Uuid::from_u128(1)).expect("coordinator"),
            source_node_id: VolumeNodeId::new(Uuid::from_u128(1)).expect("source copy"),
            target_node_ids: [1, 2, 3]
                .map(|node_id| {
                    VolumeNodeId::new(Uuid::from_u128(node_id)).expect("recovery target")
                })
                .into_iter()
                .collect(),
        };
        let recovering = initial
            .evaluate(&VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                expected: ExpectedVolumeRevision {
                    generation: VolumeGeneration::new(1).expect("generation"),
                    revision: initial.revision(),
                },
                expected_writer: None,
                replaced_recovery_id: None,
                recovery,
            }))
            .state;
        let mut recovering_observation = observation;
        recovering_observation.control_state = recovering;
        assert!(validate_stable_group(&spec, &plan, &recovering_observation).is_err());
    }

    /// The second read must contain exactly the voter sets from the first read.
    #[test]
    fn changed_voters_between_reads_block_split() {
        let (operation, assignments) = assignments([[1, 2, 3], [4, 5, 6]]);
        let first_voters = [1, 2, 3]
            .into_iter()
            .map(Uuid::from_u128)
            .collect::<BTreeSet<_>>();
        let second_voters = [1, 2, 6]
            .into_iter()
            .map(Uuid::from_u128)
            .collect::<BTreeSet<_>>();
        let placement = |voter_node_ids| ReplicatedVolumeSplitPlacement {
            volume_id: Uuid::from_u128(80),
            volume_name: "database".to_string(),
            generation: 1,
            group_id: Uuid::from_u128(81),
            target_view: operation.target_views[*assignments.get(&Uuid::from_u128(1)).unwrap()],
            voter_node_ids,
        };

        assert!(
            ensure_same_group_membership(&[placement(first_voters)], &[placement(second_voters)])
                .is_err()
        );
    }

    /// Proposed through finalized stages block only while the source view remains active.
    #[test]
    fn split_membership_block_ends_after_local_view_changes() {
        let (mut operation, _assignments) = assignments([[1, 2, 3], [4, 5, 6]]);
        let source = operation.source_views[0];
        assert!(split_blocks_membership_changes(
            &[operation.clone()],
            source
        ));

        operation.stage = ClusterOperationStage::Prepared;
        assert!(split_blocks_membership_changes(
            &[operation.clone()],
            source
        ));

        operation.stage = ClusterOperationStage::Committed;
        assert!(split_blocks_membership_changes(
            &[operation.clone()],
            source
        ));

        operation.stage = ClusterOperationStage::Finalized;
        assert!(split_blocks_membership_changes(
            &[operation.clone()],
            source
        ));
        assert!(!split_blocks_membership_changes(
            &[operation.clone()],
            operation.target_views[0]
        ));

        operation.stage = ClusterOperationStage::Aborted;
        assert!(!split_blocks_membership_changes(&[operation], source));
    }
}
