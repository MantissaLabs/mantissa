use crate::cluster::operations::{
    ClusterOperationKind, ClusterOperationRecord, ClusterOperationStage, MergeServicePolicy,
    SplitNetworkPolicy, SplitNodeAssignment, SplitSelectorClauseSpec, SplitServicePolicy,
    SplitTargetSpec,
};
use crate::cluster::{ClusterId, ClusterViewId};
use crate::node::id::read_node_id;
use crate::topology::Topology;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use tracing::{debug, warn};
use uuid::Uuid;

type ParsedSplitTargets = (Vec<SplitTargetSpec>, Vec<ClusterViewId>, Vec<String>);

pub(in crate::topology) struct SplitOperationBuildInput<'a> {
    pub source_view: ClusterViewId,
    pub dry_run: bool,
    pub split_service_policy: SplitServicePolicy,
    pub split_network_policy: SplitNetworkPolicy,
    pub target_specs: &'a [SplitTargetSpec],
    pub target_views: Vec<ClusterViewId>,
    pub detail_targets: Vec<String>,
    pub split_assignments: Vec<SplitNodeAssignment>,
}

impl Topology {
    /// Converts the merge request policy from the Cap'n Proto enum into local durable policy state.
    pub(in crate::topology) fn merge_service_policy_from_capnp(
        policy: mantissa_protocol::topology::MergeServicePolicy,
    ) -> MergeServicePolicy {
        match policy {
            mantissa_protocol::topology::MergeServicePolicy::Rebalance => {
                MergeServicePolicy::Rebalance
            }
            mantissa_protocol::topology::MergeServicePolicy::Preserve => {
                MergeServicePolicy::Preserve
            }
        }
    }

    /// Converts the split request service policy into local durable policy state.
    pub(in crate::topology) fn split_service_policy_from_capnp(
        policy: mantissa_protocol::topology::SplitServicePolicy,
    ) -> SplitServicePolicy {
        match policy {
            mantissa_protocol::topology::SplitServicePolicy::Partitioned => {
                SplitServicePolicy::Partitioned
            }
            mantissa_protocol::topology::SplitServicePolicy::Preserve => {
                SplitServicePolicy::Preserve
            }
        }
    }

    /// Converts the split request network policy into local durable policy state.
    pub(in crate::topology) fn split_network_policy_from_capnp(
        policy: mantissa_protocol::topology::SplitNetworkPolicy,
    ) -> SplitNetworkPolicy {
        match policy {
            mantissa_protocol::topology::SplitNetworkPolicy::Isolate => SplitNetworkPolicy::Isolate,
            mantissa_protocol::topology::SplitNetworkPolicy::Preserve => {
                SplitNetworkPolicy::Preserve
            }
        }
    }

    /// Parses split target selectors and derives deterministic target view ids from target names.
    pub(in crate::topology) fn parse_split_target_specs(
        &self,
        source_view: ClusterViewId,
        targets: capnp::struct_list::Reader<'_, mantissa_protocol::topology::split_target::Owned>,
    ) -> Result<ParsedSplitTargets, capnp::Error> {
        if targets.is_empty() {
            return Err(capnp::Error::failed(
                "split request requires at least one target".into(),
            ));
        }

        let mut seen_names = HashSet::<String>::new();
        let mut target_specs = Vec::with_capacity(targets.len() as usize);
        let mut target_views = Vec::with_capacity(targets.len() as usize);
        let mut detail_targets = Vec::with_capacity(targets.len() as usize);

        for idx in 0..targets.len() {
            let target = targets.get(idx);
            let name = target.get_name()?.to_string()?;
            if name.trim().is_empty() {
                return Err(capnp::Error::failed(
                    "split target name must not be empty".into(),
                ));
            }
            if !seen_names.insert(name.clone()) {
                return Err(capnp::Error::failed(format!(
                    "duplicate split target name: {name}"
                )));
            }

            let selector = target.get_selector()?;
            let clauses_reader = selector.get_clauses()?;
            let explicit_nodes_reader = selector.get_explicit_nodes()?;
            let clause_count = clauses_reader.len();
            let explicit_count = explicit_nodes_reader.len();
            let mut clauses = Vec::with_capacity(clause_count as usize);
            for clause_index in 0..clauses_reader.len() {
                let clause = clauses_reader.get(clause_index);
                let key = clause.get_key()?.to_string()?;
                if key.trim().is_empty() {
                    return Err(capnp::Error::failed(
                        "split selector clause key must not be empty".into(),
                    ));
                }

                clauses.push(SplitSelectorClauseSpec {
                    key,
                    op: clause.get_op()?,
                    value: clause.get_value()?.to_string()?,
                });
            }

            let mut explicit_nodes = HashSet::with_capacity(explicit_count as usize);
            for node_index in 0..explicit_nodes_reader.len() {
                let node_id = read_node_id(explicit_nodes_reader.get(node_index))?;
                if !explicit_nodes.insert(node_id) {
                    return Err(capnp::Error::failed(format!(
                        "split target '{name}' contains duplicate explicit node {node_id}"
                    )));
                }
            }

            let mut hasher = Sha256::new();
            hasher.update(source_view.cluster_id.as_bytes());
            hasher.update(source_view.epoch.to_le_bytes());
            hasher.update(name.as_bytes());
            let digest = hasher.finalize();
            let mut cluster_bytes = [0u8; 16];
            cluster_bytes.copy_from_slice(&digest[..16]);
            let target_cluster = ClusterId::from_bytes(cluster_bytes);
            let view = ClusterViewId::new(target_cluster, source_view.epoch.saturating_add(1));
            target_views.push(view);
            target_specs.push(SplitTargetSpec {
                name: name.clone(),
                clauses,
                explicit_nodes,
            });
            detail_targets.push(format!(
                "{name}(clauses={clause_count}, explicit_nodes={explicit_count}, view={view})"
            ));
        }

        Ok((target_specs, target_views, detail_targets))
    }

    /// Builds the durable merge operation record after request validation and policy parsing.
    pub(in crate::topology) fn build_merge_operation_record(
        &self,
        source_view: ClusterViewId,
        destination_view: ClusterViewId,
        dry_run: bool,
        merge_service_policy: MergeServicePolicy,
    ) -> ClusterOperationRecord {
        let now = Self::now_unix_ms();
        ClusterOperationRecord {
            id: Uuid::new_v4(),
            kind: ClusterOperationKind::Merge,
            stage: ClusterOperationStage::Proposed,
            dry_run,
            created_at_unix_ms: now,
            depends_on_operation_id: None,
            source_views: vec![source_view],
            target_views: vec![destination_view],
            target_cluster_names: Vec::new(),
            split_assignments: Vec::new(),
            split_service_policy: SplitServicePolicy::default(),
            split_network_policy: SplitNetworkPolicy::default(),
            merge_service_policy,
            updated_at_unix_ms: now,
            details: format!(
                "merge proposed: source={source_view}, destination={destination_view}, dry_run={dry_run}, service_policy={merge_service_policy:?}"
            ),
        }
    }

    /// Builds the durable split operation record including assignment coverage diagnostics.
    pub(in crate::topology) fn build_split_operation_record(
        &self,
        input: SplitOperationBuildInput<'_>,
    ) -> ClusterOperationRecord {
        let SplitOperationBuildInput {
            source_view,
            dry_run,
            split_service_policy,
            split_network_policy,
            target_specs,
            target_views,
            detail_targets,
            split_assignments,
        } = input;
        let mut assignments_per_target = vec![0usize; target_views.len()];
        for assignment in &split_assignments {
            if let Some(slot) = assignments_per_target.get_mut(assignment.target_index) {
                *slot = slot.saturating_add(1);
            }
        }
        let assignment_detail = target_specs
            .iter()
            .enumerate()
            .map(|(idx, target)| format!("{}={}", target.name, assignments_per_target[idx]))
            .collect::<Vec<_>>()
            .join(", ");

        let now = Self::now_unix_ms();
        ClusterOperationRecord {
            id: Uuid::new_v4(),
            kind: ClusterOperationKind::Split,
            stage: ClusterOperationStage::Proposed,
            dry_run,
            created_at_unix_ms: now,
            depends_on_operation_id: None,
            source_views: vec![source_view],
            target_views,
            target_cluster_names: target_specs
                .iter()
                .map(|target| target.name.clone())
                .collect(),
            split_assignments,
            split_service_policy,
            split_network_policy,
            merge_service_policy: MergeServicePolicy::default(),
            updated_at_unix_ms: now,
            details: format!(
                "split proposed: source={source_view}, dry_run={dry_run}, service_policy={split_service_policy:?}, network_policy={split_network_policy:?}, targets=[{}], assignments=[{}]",
                detail_targets.join(", "),
                assignment_detail
            ),
        }
    }

    /// Persists one operation and triggers broadcast/progression side effects for non-dry-run requests.
    pub(in crate::topology) async fn persist_and_dispatch_operation(
        &self,
        operation: &mut ClusterOperationRecord,
    ) -> Result<(), capnp::Error> {
        if !operation.dry_run {
            let _ = self.normalize_cluster_operation_dependency(operation)?;
        }
        self.persist_cluster_operation(operation).await?;
        if !operation.dry_run {
            let _ = self.broadcast_cluster_operation(operation).await?;
        }
        if self.operation_ready_to_progress(operation)? {
            self.trigger_operation_progress(operation.id, operation.dry_run);
        } else if let Some(dependency_id) = operation.depends_on_operation_id {
            self.trigger_operation_progress(dependency_id, false);
        }
        Ok(())
    }

    /// Applies one relayed operation payload and triggers local progression when stage requires it.
    pub(in crate::topology) async fn accept_submitted_cluster_operation(
        &self,
        operation_id: Uuid,
        payload: &[u8],
    ) -> Result<(), capnp::Error> {
        let mut incoming: ClusterOperationRecord = ClusterOperationRecord::decode_capnp(payload)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        if incoming.updated_at_unix_ms == 0 {
            incoming.updated_at_unix_ms = Self::now_unix_ms();
        }
        if incoming.created_at_unix_ms == 0 {
            incoming.created_at_unix_ms = incoming.updated_at_unix_ms;
        }
        if incoming.id != operation_id {
            return Err(capnp::Error::failed(format!(
                "relayed operation id mismatch: envelope={operation_id}, payload={}",
                incoming.id
            )));
        }

        let _ = self.normalize_cluster_operation_dependency(&mut incoming)?;

        let merged = match self.load_cluster_operation(operation_id)? {
            Some(current) if !incoming.supersedes(&current) => current,
            _ => {
                self.persist_cluster_operation(&incoming).await?;
                incoming
            }
        };

        if merged.dry_run {
            return Ok(());
        }

        if !self.operation_ready_to_progress(&merged)? {
            if let Some(dependency_id) = merged.depends_on_operation_id {
                self.trigger_operation_progress(dependency_id, false);
            }
            let _ = self.garbage_collect_cluster_operations().await?;
            return Ok(());
        }

        if let Some(active) = self.active_cluster_operation()?
            && active.id != operation_id
        {
            warn!(
                target: "cluster_view",
                operation_id = %merged.id,
                incoming_kind = ?merged.kind,
                incoming_stage = ?merged.stage,
                active_operation = %active.id,
                active_kind = ?active.kind,
                active_stage = ?active.stage,
                "deferring relayed cluster operation until active operation finalizes"
            );
            self.trigger_operation_progress(active.id, false);
            return Ok(());
        }

        match merged.stage {
            ClusterOperationStage::Proposed
            | ClusterOperationStage::Prepared
            | ClusterOperationStage::Committed => {
                self.trigger_operation_progress(merged.id, false);
            }
            ClusterOperationStage::Finalized => {
                if let Some(target) =
                    self.target_view_if_finalized_operation_affects_active_view(&merged)?
                {
                    if (merged.kind == ClusterOperationKind::Merge
                        || self.active_cluster_view() != target)
                        && let Err(err) = self.apply_committed_operation_side_effects(&merged).await
                    {
                        if Self::is_commit_precondition_failure(&err) {
                            warn!(
                                target: "cluster_view",
                                operation_id = %merged.id,
                                "skipped finalized operation side effects due to commit precondition mismatch: {err}"
                            );
                        } else {
                            return Err(err);
                        }
                    }
                } else {
                    debug!(
                        target: "cluster_view",
                        operation_id = %merged.id,
                        kind = ?merged.kind,
                        active_view = %self.active_cluster_view(),
                        "accepted finalized operation outside local cluster lineage"
                    );
                }
            }
            ClusterOperationStage::Aborted => {}
        }

        let _ = self.garbage_collect_cluster_operations().await?;
        if let Some(next) = self.active_cluster_operation_excluding(operation_id)? {
            self.trigger_operation_progress(next.id, false);
        }

        Ok(())
    }
}
