use crate::cluster::operations::{
    ClusterOperationKind, ClusterOperationRecord, ClusterOperationStage, ClusterOperationStageRank,
    MergeServicePolicy,
};
use crate::cluster::transition::ClusterTransition;
use crate::cluster::{ClusterId, ClusterViewId};
use crate::secrets::master_key::reconciler::SecretMasterKeyReconciler;
use crate::services::ServiceReconcileTrigger;
use crate::store::replicated::cluster_views::{ClusterNameRecord, ClusterNodeCountRecord};
use crate::topology::Topology;
use crate::topology::cluster_operations::{
    CLUSTER_OPERATION_FINALIZED_RETENTION_COUNT, COMMIT_PRECONDITION_FAILURE_PREFIX,
};
use crate::topology::peer_cache::PeerSnapshot;
use mantissa_protocol::server::cluster_session;
use mantissa_protocol::sync::Domain;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Requests local service reconciliation after a completed rebalance merge.
fn request_post_merge_service_reconcile(
    transition: &ClusterTransition,
    trigger: &ServiceReconcileTrigger,
) -> bool {
    if !transition.is_merge() || transition.merge_service_policy != MergeServicePolicy::Rebalance {
        return false;
    }

    trigger.request_reconcile();
    true
}

impl Topology {
    /// Applies one conflict-resolved cluster lineage name update into durable cluster-view storage.
    pub(in crate::topology) async fn upsert_cluster_name_record(
        &self,
        cluster_id: ClusterId,
        record: &ClusterNameRecord,
    ) -> Result<bool, capnp::Error> {
        self.stores
            .cluster_view_store
            .upsert_cluster_name(cluster_id, record)
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))
    }

    /// Applies one conflict-resolved cluster lineage node-count update into durable cluster-view storage.
    pub(in crate::topology) async fn upsert_cluster_node_count_record(
        &self,
        cluster_id: ClusterId,
        record: &ClusterNodeCountRecord,
    ) -> Result<bool, capnp::Error> {
        self.stores
            .cluster_view_store
            .upsert_cluster_node_count(cluster_id, record)
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))
    }

    /// Returns the immutable timestamp used by cluster-name hints from one operation.
    ///
    /// Operation stage updates must not make an unchanged target name newer than a user rename.
    fn operation_cluster_name_timestamp(operation: &ClusterOperationRecord) -> u64 {
        operation.created_at_unix_ms
    }

    /// Publishes the local active cluster's current member count into the replicated metadata domain.
    pub(crate) async fn publish_local_cluster_node_count(&self) -> Result<bool, capnp::Error> {
        if !self.local_allows_outbound_cluster_traffic() {
            return Ok(false);
        }

        let local_view = self.active_cluster_view();
        let snapshot = self.peer_snapshot_or_error().await?;
        let excluded_peers = self.excluded_peers_snapshot().await;
        let node_count = self
            .local_cluster_view_member_count_from_snapshot(&snapshot, &excluded_peers)
            .await;
        let current = self
            .stores
            .cluster_view_store
            .winning_cluster_node_count_for(local_view.cluster_id)
            .map_err(|err| capnp::Error::failed(err.to_string()))?;
        debug!(
            target: "cluster_view",
            local_view = %local_view,
            node_count,
            peer_generation = snapshot.generation,
            current = ?current,
            "publishing local cluster node count"
        );
        if let Some(current) = current.as_ref()
            && current.source_view == local_view
            && current.node_count == node_count
        {
            debug!(
                target: "cluster_view",
                local_view = %local_view,
                node_count,
                peer_generation = snapshot.generation,
                "local cluster node count already current"
            );
            return Ok(false);
        }

        let updated_at_unix_ms = ClusterNodeCountRecord::next_publish_timestamp_after(
            current.as_ref(),
            local_view,
            Self::now_unix_ms(),
        );
        let record = ClusterNodeCountRecord {
            node_count,
            source_view: local_view,
            updated_at_unix_ms,
            actor_node_id: self.local.node.id,
            membership_generation: snapshot.generation,
        };
        let changed = self
            .upsert_cluster_node_count_record(local_view.cluster_id, &record)
            .await?;
        debug!(
            target: "cluster_view",
            local_view = %local_view,
            node_count,
            changed,
            record = ?record,
            "published local cluster node count"
        );
        Ok(changed)
    }

    /// Loads the current peer snapshot or converts the storage failure into an RPC error.
    async fn peer_snapshot_or_error(&self) -> Result<PeerSnapshot, capnp::Error> {
        self.peer_snapshot()
            .await
            .ok_or_else(|| capnp::Error::failed("failed to load peer snapshot".to_string()))
    }

    /// Persists split target names carried by one operation record so cluster lineage labels survive restarts.
    async fn persist_operation_cluster_name_hints(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<(), capnp::Error> {
        if operation.target_cluster_names.len() != operation.target_views.len() {
            return Ok(());
        }

        let updated_at_unix_ms = Self::operation_cluster_name_timestamp(operation);

        for (target_view, target_name) in operation
            .target_views
            .iter()
            .zip(operation.target_cluster_names.iter())
        {
            let name = target_name.trim();
            if name.is_empty() {
                continue;
            }

            let record = ClusterNameRecord {
                name: name.to_string(),
                updated_at_unix_ms,
                actor_node_id: operation.id,
            };
            let _ = self
                .upsert_cluster_name_record(target_view.cluster_id, &record)
                .await?;
        }

        Ok(())
    }

    /// Restores missing cluster lineage names from durable operation history during startup and upgrades.
    pub(crate) async fn restore_cluster_names_from_operations(
        &self,
    ) -> Result<usize, capnp::Error> {
        let operations = self.load_cluster_operations()?;
        let mut restored = 0usize;
        for operation in operations {
            if operation.dry_run {
                continue;
            }
            if operation.target_cluster_names.len() != operation.target_views.len() {
                continue;
            }

            let updated_at_unix_ms = Self::operation_cluster_name_timestamp(&operation);
            for (target_view, target_name) in operation
                .target_views
                .iter()
                .zip(operation.target_cluster_names.iter())
            {
                let name = target_name.trim();
                if name.is_empty() {
                    continue;
                }

                let record = ClusterNameRecord {
                    name: name.to_string(),
                    updated_at_unix_ms,
                    actor_node_id: operation.id,
                };
                if self
                    .upsert_cluster_name_record(target_view.cluster_id, &record)
                    .await?
                {
                    restored = restored.saturating_add(1);
                }
            }
        }
        Ok(restored)
    }

    /// Maps operation stage values into a monotonic ordering used for conflict resolution.
    pub(in crate::topology) fn stage_rank(
        stage: ClusterOperationStage,
    ) -> ClusterOperationStageRank {
        stage.rank()
    }

    /// Returns the current UNIX timestamp in milliseconds for durable operation metadata updates.
    pub(in crate::topology) fn now_unix_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or_default()
    }

    /// Reads the cluster view currently bound to a session for assignment introspection.
    pub(in crate::topology) async fn session_cluster_view(
        session: &cluster_session::Client,
    ) -> Result<ClusterViewId, capnp::Error> {
        let request = session.get_cluster_view_request();
        let response = request.send().promise.await?;
        ClusterViewId::from_capnp(response.get()?.get_view()?).map_err(capnp::Error::failed)
    }

    /// Resolves the best-known cluster view for one peer session, if available.
    pub(in crate::topology) async fn best_known_peer_view(
        &self,
        peer_id: Uuid,
    ) -> Option<ClusterViewId> {
        if peer_id == self.local.node.id {
            return Some(self.active_cluster_view());
        }

        // Keep list/split introspection side-effect free: do not force session bootstrap
        // from read-only view probes.
        let session = self.deps.registry.cached_session_for(peer_id).await?;
        Self::session_cluster_view(&session).await.ok()
    }

    /// Counts active peers currently believed to belong to the local active cluster view.
    ///
    /// This is the authoritative local membership count used for replicated cluster metadata.
    pub(in crate::topology) async fn local_cluster_view_member_count(
        &self,
    ) -> Result<u32, capnp::Error> {
        let snapshot = self.peer_snapshot_or_error().await?;
        let excluded_peers = self.excluded_peers_snapshot().await;
        Ok(self
            .local_cluster_view_member_count_from_snapshot(&snapshot, &excluded_peers)
            .await)
    }

    /// Counts active peers from a cached snapshot that belong to the local cluster view.
    async fn local_cluster_view_member_count_from_snapshot(
        &self,
        snapshot: &PeerSnapshot,
        excluded_peers: &HashSet<Uuid>,
    ) -> u32 {
        let local_view = self.active_cluster_view();

        let mut count = 1u32;
        for entry in snapshot.entries.iter() {
            let peer_id = entry.peer_id;
            if peer_id == self.local.node.id {
                continue;
            }
            if excluded_peers.contains(&peer_id) {
                continue;
            }

            let view = self
                .best_known_peer_view(peer_id)
                .await
                .unwrap_or(local_view);
            if view != local_view {
                continue;
            }
            count = count.saturating_add(1);
        }

        count
    }

    /// Counts locally active peer rows without consulting cached peer sessions.
    async fn local_active_peer_row_member_count(&self) -> Result<u32, capnp::Error> {
        let snapshot = self.peer_snapshot_or_error().await?;
        let excluded_peers = self.excluded_peers_snapshot().await;
        Ok(self.local_active_peer_row_member_count_from_snapshot(&snapshot, &excluded_peers))
    }

    /// Counts active peer snapshot rows without opening peer sessions.
    fn local_active_peer_row_member_count_from_snapshot(
        &self,
        snapshot: &PeerSnapshot,
        excluded_peers: &HashSet<Uuid>,
    ) -> u32 {
        let mut count = 1u32;
        for entry in snapshot.entries.iter() {
            let peer_id = entry.peer_id;
            if peer_id == self.local.node.id {
                continue;
            }
            if excluded_peers.contains(&peer_id) {
                continue;
            }
            count = count.saturating_add(1);
        }

        count
    }

    /// Decodes one raw Cap'n Proto cluster lineage identifier into internal `ClusterId` bytes.
    pub(in crate::topology) fn cluster_id_from_capnp(
        reader: mantissa_protocol::topology::cluster_id::Reader<'_>,
    ) -> Result<ClusterId, capnp::Error> {
        let value = reader.get_value()?;
        if value.len() != 16 {
            return Err(capnp::Error::failed(format!(
                "cluster id must be exactly 16 bytes, got {}",
                value.len()
            )));
        }

        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(value);
        Ok(ClusterId::from_bytes(bytes))
    }

    /// Applies one local cluster lineage name update with deterministic last-writer conflict resolution.
    pub(in crate::topology) async fn apply_cluster_name_update(
        &self,
        cluster_id: ClusterId,
        name: &str,
        updated_at_unix_ms: u64,
        actor_node_id: Uuid,
    ) -> Result<bool, capnp::Error> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(capnp::Error::failed(
                "cluster name must not be empty".to_string(),
            ));
        }

        let record = ClusterNameRecord {
            name: trimmed.to_string(),
            updated_at_unix_ms,
            actor_node_id,
        };
        self.upsert_cluster_name_record(cluster_id, &record).await
    }

    /// Returns the local active views from which this split/merge transition may be applied.
    fn allowed_active_views_for_cluster_transition(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<Vec<ClusterViewId>, capnp::Error> {
        let source_view = operation.source_views.first().copied().ok_or_else(|| {
            capnp::Error::failed(format!("operation {} missing source view", operation.id))
        })?;
        let mut allowed_views = vec![source_view];
        if operation.kind == ClusterOperationKind::Merge {
            for target in operation.target_views.iter().copied() {
                if !allowed_views.contains(&target) {
                    allowed_views.push(target);
                }
            }
        } else if let Some(target) = self.recoverable_split_target(operation)?
            && !allowed_views.contains(&target)
        {
            // This target is valid only for recovery after the local active-view transaction
            // committed but the operation stage update had not yet reached durable storage.
            allowed_views.push(target);
        }
        Ok(allowed_views)
    }

    /// Returns the local split target only when this operation installed the active view.
    ///
    /// Different split operations can assign the same deterministic target. The durable operation
    /// marker distinguishes a real interrupted commit from an unrelated operation on that target.
    pub(in crate::topology) fn recoverable_split_target(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<Option<ClusterViewId>, capnp::Error> {
        let Some(target) = self.target_view_if_local_participant(operation)? else {
            return Ok(None);
        };
        let installed = self
            .stores
            .cluster_view_store
            .active_view_was_installed_by(operation.id, target)
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        Ok(installed.then_some(target))
    }

    /// Ensures the current node may apply this split/merge transition from its active view.
    fn ensure_cluster_transition_can_apply(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<(), capnp::Error> {
        let active_view = self.active_cluster_view();
        let allowed_views = self.allowed_active_views_for_cluster_transition(operation)?;
        if allowed_views.contains(&active_view) {
            return self.ensure_operation_views_not_retired(operation);
        }

        let allowed_render = allowed_views
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        Err(capnp::Error::failed(format!(
            "{COMMIT_PRECONDITION_FAILURE_PREFIX}: operation={} kind={:?} active_view={} allowed_views=[{}]",
            operation.id, operation.kind, active_view, allowed_render
        )))
    }

    /// Rejects operations whose source or destination views were already retired by another merge.
    fn ensure_operation_views_not_retired(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<(), capnp::Error> {
        if let Some(view) = self.operation_view_retired_by_prior_merge(operation)? {
            return Err(capnp::Error::failed(format!(
                "{COMMIT_PRECONDITION_FAILURE_PREFIX}: operation={} kind={:?} view={} already retired",
                operation.id, operation.kind, view
            )));
        }
        Ok(())
    }

    /// Returns the operation view already retired by an earlier merge, if any.
    fn operation_view_retired_by_prior_merge(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<Option<ClusterViewId>, capnp::Error> {
        let retired = self.retired_views_before(operation)?;
        let mut checked_views = operation.source_views.clone();
        if operation.kind == ClusterOperationKind::Merge {
            checked_views.extend(operation.target_views.iter().copied());
        }
        Ok(checked_views
            .into_iter()
            .find(|view| retired.contains(view)))
    }

    /// Returns views retired by merge operations that precede the supplied operation.
    fn retired_views_before(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<HashSet<ClusterViewId>, capnp::Error> {
        let mut retired = HashSet::new();
        let operation_key = operation.lineage_order_key();
        for candidate in self.load_cluster_operations()? {
            if candidate.id == operation.id
                || candidate.dry_run
                || candidate.kind != ClusterOperationKind::Merge
                || candidate.lineage_order_key() >= operation_key
            {
                continue;
            }
            if matches!(
                candidate.stage,
                ClusterOperationStage::Committed | ClusterOperationStage::Finalized
            ) {
                retired.extend(candidate.source_views.iter().copied());
            }
        }
        Ok(retired)
    }

    /// Returns whether an operation dependency is absent or already terminal locally.
    pub(in crate::topology) fn operation_ready_to_progress(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<bool, capnp::Error> {
        let Some(dependency_id) = operation.depends_on_operation_id else {
            return Ok(true);
        };
        let Some(dependency) = self.load_cluster_operation(dependency_id)? else {
            return Ok(false);
        };
        Ok(matches!(
            dependency.stage,
            ClusterOperationStage::Finalized | ClusterOperationStage::Aborted
        ))
    }

    /// Returns whether an operation stage is terminal and no longer participates in the fence.
    fn operation_stage_is_terminal(stage: ClusterOperationStage) -> bool {
        matches!(
            stage,
            ClusterOperationStage::Finalized | ClusterOperationStage::Aborted
        )
    }

    /// Returns every lineage view touched by this operation for overlap fencing.
    fn operation_lineage_views(operation: &ClusterOperationRecord) -> HashSet<ClusterViewId> {
        let mut views = operation
            .source_views
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        views.extend(operation.target_views.iter().copied());
        views
    }

    /// Returns true when two operations touch at least one common cluster lineage view.
    fn operations_overlap(left: &ClusterOperationRecord, right: &ClusterOperationRecord) -> bool {
        let left_views = Self::operation_lineage_views(left);
        right
            .source_views
            .iter()
            .chain(right.target_views.iter())
            .any(|view| left_views.contains(view))
    }

    /// Returns whether a pending operation is allowed to advance from this node's active view.
    ///
    /// Split operations normally advance from their source view. An assigned target is accepted
    /// only when this operation durably installed it before a crash. Merge operations can be
    /// driven by either side because both partitions must converge.
    fn cluster_operation_can_progress_from_active_view(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<bool, capnp::Error> {
        let active_view = self.active_cluster_view();
        match operation.kind {
            ClusterOperationKind::Merge => {
                if operation.target_views.is_empty() {
                    return Err(capnp::Error::failed(format!(
                        "merge operation {} missing target view",
                        operation.id
                    )));
                }
                Ok(operation.source_views.contains(&active_view)
                    || operation.target_views.contains(&active_view))
            }
            ClusterOperationKind::Split => {
                if operation.source_views.contains(&active_view) {
                    return Ok(true);
                }
                Ok(self.recoverable_split_target(operation)? == Some(active_view))
            }
        }
    }

    /// Selects the latest pending overlapping predecessor for a new or learned operation.
    pub(in crate::topology) fn pending_cluster_operation_tail_for(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<Option<ClusterOperationRecord>, capnp::Error> {
        if operation.dry_run || Self::operation_stage_is_terminal(operation.stage) {
            return Ok(None);
        }

        let operation_key = operation.lineage_order_key();
        let mut predecessors = Vec::new();
        for candidate in self.load_cluster_operations()? {
            if candidate.id == operation.id
                || candidate.dry_run
                || Self::operation_stage_is_terminal(candidate.stage)
                || candidate.lineage_order_key() >= operation_key
                || !Self::operations_overlap(&candidate, operation)
            {
                continue;
            }
            predecessors.push(candidate);
        }

        predecessors.sort_by_key(ClusterOperationRecord::lineage_order_key);
        Ok(predecessors.into_iter().next_back())
    }

    /// Normalizes an operation dependency to the latest pending overlapping predecessor.
    pub(in crate::topology) fn normalize_cluster_operation_dependency(
        &self,
        operation: &mut ClusterOperationRecord,
    ) -> Result<bool, capnp::Error> {
        let Some(predecessor) = self.pending_cluster_operation_tail_for(operation)? else {
            return Ok(false);
        };
        if operation.depends_on_operation_id == Some(predecessor.id) {
            return Ok(false);
        }

        operation.depends_on_operation_id = Some(predecessor.id);
        operation.updated_at_unix_ms = Self::now_unix_ms();
        operation.details = format!(
            "{} | queued_after predecessor_operation={}",
            operation.details, predecessor.id
        );
        Ok(true)
    }

    /// Returns the next ready non-finalized cluster operation, if any.
    pub(in crate::topology) fn active_cluster_operation(
        &self,
    ) -> Result<Option<ClusterOperationRecord>, capnp::Error> {
        let mut active = Vec::new();
        for operation in self.load_cluster_operations()? {
            if self.operation_is_ready_non_terminal(&operation)? {
                active.push(operation);
            }
        }

        active.sort_by_key(ClusterOperationRecord::lineage_order_key);

        Ok(active.into_iter().next())
    }

    /// Returns the next ready non-finalized operation excluding one specific id.
    pub(in crate::topology) fn active_cluster_operation_excluding(
        &self,
        excluded_operation_id: Uuid,
    ) -> Result<Option<ClusterOperationRecord>, capnp::Error> {
        let mut active = Vec::new();
        for operation in self.load_cluster_operations()? {
            if operation.id != excluded_operation_id
                && self.operation_is_ready_non_terminal(&operation)?
            {
                active.push(operation);
            }
        }

        active.sort_by_key(ClusterOperationRecord::lineage_order_key);

        Ok(active.into_iter().next())
    }

    /// Returns whether an operation can currently take the local progress gate.
    fn operation_is_ready_non_terminal(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<bool, capnp::Error> {
        Ok(!operation.dry_run
            && matches!(
                operation.stage,
                ClusterOperationStage::Proposed
                    | ClusterOperationStage::Prepared
                    | ClusterOperationStage::Committed
            )
            && self.cluster_operation_can_progress_from_active_view(operation)?
            && self.operation_ready_to_progress(operation)?)
    }

    /// Rejects one mutating action while a split/merge operation is still in progress.
    pub(crate) fn ensure_no_active_cluster_operation(
        &self,
        action: &str,
    ) -> Result<(), capnp::Error> {
        let Some(operation) = self.active_cluster_operation()? else {
            return Ok(());
        };

        Err(capnp::Error::failed(format!(
            "cannot {action} while cluster operation {} ({:?}/{:?}) is in progress",
            operation.id, operation.kind, operation.stage
        )))
    }

    /// Rejects peer joins during active split operations to avoid assignment ambiguity.
    pub(crate) fn ensure_join_allowed(&self) -> Result<(), capnp::Error> {
        let Some(operation) = self.active_cluster_operation()? else {
            return Ok(());
        };

        if operation.kind == ClusterOperationKind::Split {
            return Err(capnp::Error::failed(format!(
                "cannot register peer while split operation {} ({:?}) is in progress",
                operation.id, operation.stage
            )));
        }

        Ok(())
    }

    /// Persists a target view and its source-view retirement work in one local transaction.
    fn persist_cluster_transition_view(
        &self,
        operation: &ClusterOperationRecord,
        view: ClusterViewId,
    ) -> Result<(), capnp::Error> {
        self.stores
            .cluster_view_store
            .install_cluster_transition(operation.id, view, &operation.source_views)
            .map_err(|err| capnp::Error::failed(err.to_string()))
    }

    /// Publishes every locally pending view retirement and clears completed local work.
    ///
    /// The active-view transaction records this work before the process changes views. Repeating
    /// the publication after a crash is safe because retirement only moves to a higher epoch.
    pub(crate) async fn publish_pending_view_retirements(&self) -> Result<usize, capnp::Error> {
        let pending = self
            .stores
            .cluster_view_store
            .pending_view_retirements()
            .map_err(|err| capnp::Error::failed(err.to_string()))?;
        let mut completed = 0usize;

        for retired_view in pending {
            self.stores
                .cluster_view_store
                .retire_view(retired_view)
                .await
                .map_err(|err| {
                    capnp::Error::failed(format!(
                        "publish cluster view retirement for {retired_view}: {err}"
                    ))
                })?;
            self.stores
                .cluster_view_store
                .complete_view_retirement(retired_view)
                .map_err(|err| {
                    capnp::Error::failed(format!(
                        "complete cluster view retirement for {retired_view}: {err}"
                    ))
                })?;
            completed = completed.saturating_add(1);
        }

        Ok(completed)
    }

    /// Returns true when an error represents a stale commit precondition mismatch.
    pub(in crate::topology) fn is_commit_precondition_failure(err: &capnp::Error) -> bool {
        err.to_string().contains(COMMIT_PRECONDITION_FAILURE_PREFIX)
    }

    /// Persists a cluster operation record in the replicated durable operation store.
    pub(in crate::topology) async fn persist_cluster_operation(
        &self,
        op: &ClusterOperationRecord,
    ) -> Result<(), capnp::Error> {
        self.stores
            .cluster_operations
            .put_record(op)
            .await
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        if !op.dry_run {
            self.persist_operation_cluster_name_hints(op).await?;
        }
        Ok(())
    }

    /// Loads a cluster operation record by id from the replicated durable operation store.
    pub(in crate::topology) fn load_cluster_operation(
        &self,
        id: Uuid,
    ) -> Result<Option<ClusterOperationRecord>, capnp::Error> {
        self.stores
            .cluster_operations
            .get_record(id)
            .map_err(|e| capnp::Error::failed(e.to_string()))
    }

    /// Loads all operation records from the replicated durable operation store.
    pub(in crate::topology) fn load_cluster_operations(
        &self,
    ) -> Result<Vec<ClusterOperationRecord>, capnp::Error> {
        self.stores
            .cluster_operations
            .list_records()
            .map_err(|e| capnp::Error::failed(e.to_string()))
    }

    /// Parses a cluster operation id from raw RPC bytes, enforcing UUID byte length.
    pub(in crate::topology) fn operation_id_from_data(
        data: capnp::data::Reader<'_>,
    ) -> Result<Uuid, capnp::Error> {
        let id_bytes: [u8; 16] = data
            .try_into()
            .map_err(|_| capnp::Error::failed("cluster operation id must be 16 bytes".into()))?;
        Ok(Uuid::from_bytes(id_bytes))
    }

    /// Updates an operation stage, appends stage details, and persists the updated record.
    ///
    /// Learned terminal rows can race with an already-running local progress task. Before writing
    /// the requested stage, re-read the durable row and refuse to overwrite a strictly newer stage.
    /// This keeps a late stale-precondition abort from replacing a committed/finalized operation.
    async fn update_cluster_operation_stage(
        &self,
        operation: &mut ClusterOperationRecord,
        stage: ClusterOperationStage,
        detail: &str,
    ) -> Result<bool, capnp::Error> {
        if let Some(current) = self.load_cluster_operation(operation.id)?
            && Self::stage_rank(current.stage) > Self::stage_rank(stage)
        {
            debug!(
                target: "cluster_view",
                operation_id = %operation.id,
                current_stage = ?current.stage,
                requested_stage = ?stage,
                "skipping stale cluster operation stage update"
            );
            *operation = current;
            return Ok(false);
        }

        operation.stage = stage;
        operation.updated_at_unix_ms = Self::now_unix_ms();
        if !detail.is_empty() {
            operation.details = format!("{} | {}", operation.details, detail);
        }
        self.persist_cluster_operation(operation).await?;
        Ok(true)
    }

    /// Tombstones old terminal operations so replicated operation history stays bounded.
    pub(in crate::topology) async fn garbage_collect_cluster_operations(
        &self,
    ) -> Result<usize, capnp::Error> {
        let mut terminal = self
            .load_cluster_operations()?
            .into_iter()
            .filter(|operation| {
                matches!(
                    operation.stage,
                    ClusterOperationStage::Finalized | ClusterOperationStage::Aborted
                )
            })
            .collect::<Vec<_>>();

        if terminal.len() <= CLUSTER_OPERATION_FINALIZED_RETENTION_COUNT {
            return Ok(0);
        }

        terminal.sort_by(|left, right| {
            right
                .updated_at_unix_ms
                .cmp(&left.updated_at_unix_ms)
                .then_with(|| right.id.cmp(&left.id))
        });

        // A terminal row is still an immutable repair intent for a node that was offline during
        // the transition. Keep it for longer than the key cleanup window before count-based
        // compaction can tombstone it.
        let intent_retention_ms = crate::config::store_gc_runtime_config()
            .policy
            .tombstone_min_retention_ms
            .saturating_mul(2);
        let eligible_before_unix_ms = Self::now_unix_ms().saturating_sub(intent_retention_ms);
        let to_delete = terminal
            .into_iter()
            .skip(CLUSTER_OPERATION_FINALIZED_RETENTION_COUNT)
            .filter(|operation| operation.updated_at_unix_ms <= eligible_before_unix_ms)
            .map(|operation| operation.id)
            .collect::<Vec<_>>();
        if to_delete.is_empty()
            || !self
                .cluster_operation_gc_has_global_sync_frontier(Self::now_unix_ms())
                .await?
        {
            return Ok(0);
        }
        let removed = self
            .stores
            .cluster_operations
            .delete_many(&to_delete)
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))?;
        if removed > 0 {
            info!(
                target: "cluster_view",
                removed,
                retained = CLUSTER_OPERATION_FINALIZED_RETENTION_COUNT,
                "garbage-collected terminal cluster operations"
            );
        }

        Ok(removed)
    }

    /// Proves every known cluster-wide peer has the metadata needed before intent deletion.
    ///
    /// This frontier is deliberately independent of operation completion. Nodes apply transitions
    /// without acknowledgements and cleanup waits for ordinary anti-entropy root equality so an
    /// offline participant cannot lose the only durable copy of its assignment or target key.
    async fn cluster_operation_gc_has_global_sync_frontier(
        &self,
        now_unix_ms: u64,
    ) -> Result<bool, capnp::Error> {
        let remote_peers = self
            .deps
            .registry
            .known_peers_unscoped()
            .map_err(|err| capnp::Error::failed(format!("load operation GC peers: {err}")))?;
        let cluster_view = self.active_cluster_view();
        let root_schema_version = self.supported_root_schema_version();
        let roots = [
            (
                Domain::Peers,
                self.stores
                    .peers
                    .root_digest_at_version(root_schema_version)
                    .await
                    .map_err(|err| capnp::Error::failed(err.to_string()))?,
            ),
            (
                Domain::ClusterViews,
                self.stores
                    .cluster_view_store
                    .root_digest_at_version(root_schema_version)
                    .await
                    .map_err(|err| capnp::Error::failed(err.to_string()))?,
            ),
            (
                Domain::SecretMasterKeys,
                self.stores
                    .secret_master_keys
                    .root_digest_at_version(root_schema_version)
                    .await
                    .map_err(|err| capnp::Error::failed(err.to_string()))?,
            ),
            (
                Domain::ClusterOperations,
                self.stores
                    .cluster_operations
                    .domain_store()
                    .root_digest_at_version(root_schema_version)
                    .await
                    .map_err(|err| capnp::Error::failed(err.to_string()))?,
            ),
        ];
        let progress = self.deps.sync.gc_progress();
        Ok(roots.into_iter().all(|(domain, root_digest)| {
            progress
                .barrier_for_domain(
                    remote_peers.iter().copied(),
                    domain,
                    cluster_view,
                    root_schema_version,
                    root_digest,
                    now_unix_ms,
                )
                .is_some()
        }))
    }

    /// Applies a split/merge cluster transition and installs its target active view.
    pub(in crate::topology) async fn apply_cluster_transition(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<(), capnp::Error> {
        self.ensure_cluster_transition_can_apply(operation)?;

        let transition = self.transition_for_operation(operation)?;
        let reports = self
            .run_cluster_transition_participants(&transition)
            .await?;
        for report in reports {
            info!(
                target: "cluster_view",
                operation_id = %transition.operation_id,
                participant = report.name,
                details = %report.render(),
                "applied cluster transition participant"
            );
        }

        self.persist_cluster_transition_view(operation, transition.local_target_view)?;
        let previous = self.set_active_cluster_view(transition.local_target_view);
        match self.publish_pending_view_retirements().await {
            Ok(completed) if completed > 0 => {
                info!(
                    target: "cluster_view",
                    operation_id = %transition.operation_id,
                    completed,
                    "published source cluster view retirements"
                );
            }
            Ok(_) => {}
            Err(error) => {
                // The active-view transaction keeps this work pending for startup and metadata
                // reconciliation. Failing closed here retains old keys until publication succeeds.
                warn!(
                    target: "cluster_view",
                    operation_id = %transition.operation_id,
                    "deferred source cluster view retirement publication: {error}"
                );
            }
        }
        self.reconcile_secret_master_keys_for_view(transition.local_target_view)
            .await;
        self.deps.registry.clear().await;
        match self.publish_local_cluster_node_count().await {
            Ok(true) => self.sync_once_now(),
            Ok(false) => {}
            Err(err) => {
                warn!(
                    target: "cluster_view",
                    operation_id = %transition.operation_id,
                    target_view = %transition.local_target_view,
                    "failed to publish cluster node count after cluster transition: {err}"
                );
            }
        }
        // Transition participants run against the source view. Wake service reconciliation only
        // after the target view, its master key, and the new peer-session scope are installed.
        if request_post_merge_service_reconcile(&transition, &self.deps.service_reconcile_trigger) {
            info!(
                target: "cluster_view",
                operation_id = %transition.operation_id,
                target_view = %transition.local_target_view,
                "requested service reconciliation after merge transition"
            );
        }
        info!(
            target: "cluster_view",
            operation_id = %transition.operation_id,
            previous_view = %previous,
            target_view = %transition.local_target_view,
            "applied cluster transition"
        );

        Ok(())
    }

    /// Returns whether a finalized merge still has local split scope to clear.
    ///
    /// A destination-side participant can already report the merge target view when it learns the
    /// finalized merge row. If split peer exclusions are still installed, this node has not run the
    /// merge transition locally and must replay it once to rejoin peer scope.
    pub(in crate::topology) async fn finalized_merge_requires_cluster_transition_replay(
        &self,
        operation: &ClusterOperationRecord,
    ) -> bool {
        operation.kind == ClusterOperationKind::Merge
            && !self.excluded_peers_snapshot().await.is_empty()
    }

    /// Refreshes merge node-count metadata when the active view is correct but counters are stale.
    ///
    /// This path intentionally avoids replaying transition participants or rewriting the active
    /// view. If a later known merge will retire this view, the newer operation owns convergence.
    pub(in crate::topology) async fn refresh_finalized_merge_membership_metadata_if_stale(
        &self,
        operation: &ClusterOperationRecord,
        target_view: ClusterViewId,
    ) -> Result<(), capnp::Error> {
        if operation.kind != ClusterOperationKind::Merge {
            return Ok(());
        }

        let known_operations = self.load_cluster_operations()?;
        if let Some(invalidating_operation_id) =
            operation.invalidating_later_merge_id_for_view(known_operations.iter(), target_view)
        {
            debug!(
                target: "cluster_view",
                operation_id = %operation.id,
                invalidating_operation_id = %invalidating_operation_id,
                target_view = %target_view,
                "skipping finalized merge metadata refresh for view invalidated by later merge"
            );
            return Ok(());
        }

        let observed = self.local_cluster_view_member_count().await?;
        let expected = self.local_active_peer_row_member_count().await?;
        if observed == expected {
            return Ok(());
        }

        debug!(
            target: "cluster_view",
            operation_id = %operation.id,
            target_view = %target_view,
            observed_members = observed,
            expected_members = expected,
            "refreshing finalized merge metadata"
        );
        match self.publish_local_cluster_node_count().await {
            Ok(true) => {
                self.sync_once_now();
                debug!(
                    target: "cluster_view",
                    operation_id = %operation.id,
                    target_view = %target_view,
                    "refreshed finalized merge node-count metadata"
                );
            }
            Ok(false) => {
                debug!(
                    target: "cluster_view",
                    operation_id = %operation.id,
                    target_view = %target_view,
                    "finalized merge node-count metadata already current"
                );
            }
            Err(err) => {
                warn!(
                    target: "cluster_view",
                    operation_id = %operation.id,
                    target_view = %target_view,
                    "failed to publish cluster node count while refreshing finalized merge metadata: {err}"
                );
            }
        }

        Ok(())
    }

    /// Re-runs master-key adoption after a committed transition changes the active view.
    async fn reconcile_secret_master_keys_for_view(&self, view: ClusterViewId) {
        let reconciler = SecretMasterKeyReconciler::new(
            self.local.node.id,
            self.deps.registry.noise_keys(),
            self.deps.registry.clone(),
            self.stores.secret_master_keys.clone(),
            self.stores.secret_master_store.clone(),
            self.stores.secret_keyring.clone(),
            self.local.cluster_view.clone(),
        );

        match reconciler.reconcile_view(view).await {
            Ok(report)
                if report.current_waiting_for_descriptor || report.current_waiting_for_key =>
            {
                debug!(
                    target: "secrets",
                    cluster_view = %view,
                    waiting_for_descriptor = report.current_waiting_for_descriptor,
                    waiting_for_key = report.current_waiting_for_key,
                    "secret master-key current not yet adoptable after cluster transition"
                );
            }
            Ok(_) => {}
            Err(error) => {
                warn!(
                    target: "secrets",
                    cluster_view = %view,
                    "failed to reconcile secret master keys after cluster transition: {error:#}"
                );
            }
        }
    }

    /// Starts asynchronous local progression for a cluster operation if it is not a dry run.
    pub(in crate::topology) fn trigger_operation_progress(
        &self,
        operation_id: Uuid,
        dry_run: bool,
    ) {
        if dry_run {
            return;
        }

        let topology = self.clone();
        tokio::task::spawn_local(async move {
            // Let the submitting RPC flush its durable Proposed result before key wrapping can
            // occupy this local executor thread for a large participant set.
            tokio::task::yield_now().await;
            if let Err(err) = topology.progress_cluster_operation(operation_id).await {
                warn!(
                    target: "cluster_view",
                    operation_id = %operation_id,
                    "failed to progress cluster operation: {err}"
                );
            }
        });
    }

    /// Starts local progression for the next ready queued operation, if one exists.
    pub(crate) fn trigger_ready_cluster_operation_progress(&self) {
        match self.active_cluster_operation() {
            Ok(Some(operation)) => self.trigger_operation_progress(operation.id, operation.dry_run),
            Ok(None) => {}
            Err(err) => {
                warn!(
                    target: "cluster_view",
                    "failed to select ready cluster operation after metadata sync: {err}"
                );
            }
        }
    }

    /// Replays one finalized split/merge row when it still changes this node's cluster view.
    ///
    /// The replicated operation ledger can expose a terminal row before every participant has
    /// applied the local cluster transition. This method reconciles that gap for the current node:
    /// it ignores finalized rows outside the local lineage, applies the transition when the node is
    /// still on a source view, and refreshes already-target merge metadata without rerunning
    /// unrelated transition participants.
    pub(in crate::topology) async fn replay_finalized_cluster_transition_for_active_view(
        &self,
        operation: &ClusterOperationRecord,
        replay_context: &'static str,
    ) -> Result<bool, capnp::Error> {
        if operation.dry_run || operation.stage != ClusterOperationStage::Finalized {
            return Ok(false);
        }

        let Some(target) = self.finalized_cluster_transition_target(operation)? else {
            debug!(
                target: "cluster_view",
                operation_id = %operation.id,
                kind = ?operation.kind,
                active_view = %self.active_cluster_view(),
                replay_context,
                "skipping finalized operation outside local cluster lineage"
            );
            return Ok(false);
        };

        if self.active_cluster_view() == target {
            if self
                .finalized_merge_requires_cluster_transition_replay(operation)
                .await
            {
                if let Err(err) = self.apply_cluster_transition(operation).await {
                    if Self::is_commit_precondition_failure(&err) {
                        debug!(
                            target: "cluster_view",
                            operation_id = %operation.id,
                            replay_context,
                            "skipped finalized merge target-view commit replay due to commit precondition mismatch: {err}"
                        );
                        return Ok(false);
                    }
                    return Err(err);
                }
                return Ok(true);
            }

            debug!(
                target: "cluster_view",
                operation_id = %operation.id,
                kind = ?operation.kind,
                active_view = %self.active_cluster_view(),
                replay_context,
                "finalized operation already matches local active view"
            );
            self.refresh_finalized_merge_membership_metadata_if_stale(operation, target)
                .await?;
            return Ok(false);
        }

        if let Err(err) = self.apply_cluster_transition(operation).await {
            if Self::is_commit_precondition_failure(&err) {
                debug!(
                    target: "cluster_view",
                    operation_id = %operation.id,
                    replay_context,
                    "skipped finalized operation replay due to commit precondition mismatch: {err}"
                );
                return Ok(false);
            }
            return Err(err);
        }

        Ok(true)
    }

    /// Reconciles all finalized operation rows with this node's current active view.
    ///
    /// Call this at boundaries where local code is about to trust `active_cluster_view`.
    /// It removes the ambiguity between "the operation row is finalized" and "this node has
    /// applied the finalized transition locally."
    pub(crate) async fn reconcile_finalized_cluster_transitions_for_active_view(
        &self,
        replay_context: &'static str,
    ) -> Result<(), capnp::Error> {
        let completed = self.publish_pending_view_retirements().await?;
        if completed > 0 {
            info!(
                target: "cluster_view",
                completed,
                replay_context,
                "completed pending source cluster view retirements"
            );
        }

        let mut operations = self.load_cluster_operations()?;
        operations.sort_by_key(ClusterOperationRecord::lineage_order_key);

        for operation in operations {
            let _ = self
                .replay_finalized_cluster_transition_for_active_view(&operation, replay_context)
                .await?;
        }

        Ok(())
    }

    /// Reconciles operation rows learned through anti-entropy and wakes queued work.
    pub(crate) async fn reconcile_cluster_operations_after_sync(&self) -> Result<(), capnp::Error> {
        self.reconcile_finalized_cluster_transitions_for_active_view("sync reconciliation")
            .await?;
        self.trigger_ready_cluster_operation_progress();
        let _ = self.garbage_collect_cluster_operations().await?;
        Ok(())
    }

    /// Progresses one operation forward based on its current persisted stage.
    async fn progress_cluster_operation(&self, operation_id: Uuid) -> Result<(), capnp::Error> {
        let _guard = self.runtime.cluster_operation_gate.gate.lock().await;

        let mut operation = self.load_cluster_operation(operation_id)?.ok_or_else(|| {
            capnp::Error::failed(format!("cluster operation not found: {operation_id}"))
        })?;
        if !Self::operation_stage_is_terminal(operation.stage)
            && let Some(retired_view) = self.operation_view_retired_by_prior_merge(&operation)?
        {
            let detail = format!(
                "aborted stale_precondition: {COMMIT_PRECONDITION_FAILURE_PREFIX}: operation={} kind={:?} view={} already retired",
                operation.id, operation.kind, retired_view
            );
            let updated = self
                .update_cluster_operation_stage(
                    &mut operation,
                    ClusterOperationStage::Aborted,
                    &detail,
                )
                .await?;
            if updated && operation.stage == ClusterOperationStage::Aborted {
                debug!(
                    target: "cluster_view",
                    operation_id = %operation.id,
                    stage = ?operation.stage,
                    retired_view = %retired_view,
                    "aborted stale cluster operation after prior lineage retirement"
                );
            }
            return Ok(());
        }
        if !self.cluster_operation_can_progress_from_active_view(&operation)? {
            debug!(
                target: "cluster_view",
                operation_id = %operation.id,
                kind = ?operation.kind,
                active_view = %self.active_cluster_view(),
                "cluster operation is outside local cluster lineage"
            );
            return Ok(());
        }
        if self.normalize_cluster_operation_dependency(&mut operation)? {
            self.persist_cluster_operation(&operation).await?;
        }
        if !self.operation_ready_to_progress(&operation)? {
            debug!(
                target: "cluster_view",
                operation_id = %operation.id,
                dependency = ?operation.depends_on_operation_id,
                "cluster operation is waiting for dependency"
            );
            return Ok(());
        }
        if let Some(active) = self.active_cluster_operation()?
            && active.id != operation.id
        {
            debug!(
                target: "cluster_view",
                operation_id = %operation.id,
                active_operation = %active.id,
                "cluster operation is waiting behind earlier ready operation"
            );
            self.trigger_operation_progress(active.id, false);
            return Ok(());
        }

        match operation.stage {
            ClusterOperationStage::Proposed => {
                // This is the only actionability frontier: a replicated Proposed row is harmless,
                // while Prepared certifies that this node durably installed every target-key row
                // it is responsible for publishing. Missing remote rows remain a retryable Sync
                // condition and never turn into a participant acknowledgement barrier.
                if !self.publish_transition_key_material(&operation).await? {
                    debug!(
                        target: "cluster_view",
                        operation_id = %operation.id,
                        submitted_by_node_id = %operation.submitted_by_node_id,
                        "cluster operation is waiting for its deterministic key publisher"
                    );
                    return Ok(());
                }
                if !self
                    .update_cluster_operation_stage(
                        &mut operation,
                        ClusterOperationStage::Prepared,
                        "prepared",
                    )
                    .await?
                    || operation.stage != ClusterOperationStage::Prepared
                {
                    return Ok(());
                }
                if let Err(err) = self.apply_cluster_transition(&operation).await {
                    if Self::is_commit_precondition_failure(&err) {
                        let updated = self
                            .update_cluster_operation_stage(
                                &mut operation,
                                ClusterOperationStage::Aborted,
                                &format!("aborted stale_precondition: {err}"),
                            )
                            .await?;
                        if updated && operation.stage == ClusterOperationStage::Aborted {
                            warn!(
                                target: "cluster_view",
                                operation_id = %operation.id,
                                stage = ?operation.stage,
                                "aborted cluster operation due to commit precondition mismatch: {err}"
                            );
                        }
                    } else {
                        return Err(err);
                    }
                } else if self
                    .update_cluster_operation_stage(
                        &mut operation,
                        ClusterOperationStage::Committed,
                        &format!("committed active_view={}", self.active_cluster_view()),
                    )
                    .await?
                    && operation.stage == ClusterOperationStage::Committed
                {
                    self.update_cluster_operation_stage(
                        &mut operation,
                        ClusterOperationStage::Finalized,
                        "finalized",
                    )
                    .await?;
                }
            }
            ClusterOperationStage::Prepared => {
                if let Err(err) = self.apply_cluster_transition(&operation).await {
                    if Self::is_commit_precondition_failure(&err) {
                        let updated = self
                            .update_cluster_operation_stage(
                                &mut operation,
                                ClusterOperationStage::Aborted,
                                &format!("aborted stale_precondition: {err}"),
                            )
                            .await?;
                        if updated && operation.stage == ClusterOperationStage::Aborted {
                            warn!(
                                target: "cluster_view",
                                operation_id = %operation.id,
                                stage = ?operation.stage,
                                "aborted cluster operation due to commit precondition mismatch: {err}"
                            );
                        }
                    } else {
                        return Err(err);
                    }
                } else if self
                    .update_cluster_operation_stage(
                        &mut operation,
                        ClusterOperationStage::Committed,
                        &format!("committed active_view={}", self.active_cluster_view()),
                    )
                    .await?
                    && operation.stage == ClusterOperationStage::Committed
                {
                    self.update_cluster_operation_stage(
                        &mut operation,
                        ClusterOperationStage::Finalized,
                        "finalized",
                    )
                    .await?;
                }
            }
            ClusterOperationStage::Committed => {
                self.update_cluster_operation_stage(
                    &mut operation,
                    ClusterOperationStage::Finalized,
                    "finalized",
                )
                .await?;
            }
            ClusterOperationStage::Finalized => {
                let _ = self
                    .replay_finalized_cluster_transition_for_active_view(
                        &operation,
                        "operation progress",
                    )
                    .await?;
            }
            ClusterOperationStage::Aborted => {}
        }

        let _ = self.garbage_collect_cluster_operations().await?;
        drop(_guard);

        if let Some(next) = self.active_cluster_operation_excluding(operation_id)? {
            self.trigger_operation_progress(next.id, false);
        }

        Ok(())
    }

    /// Replays any non-finalized durable operation records so crashes do not strand topology changes.
    pub(crate) async fn replay_cluster_operations_on_startup(&self) -> Result<usize, capnp::Error> {
        let mut operations = self.load_cluster_operations()?;
        operations.sort_by_key(ClusterOperationRecord::lineage_order_key);

        let mut replayed = 0usize;
        for operation in operations {
            if operation.dry_run {
                continue;
            }
            if matches!(
                operation.stage,
                ClusterOperationStage::Finalized | ClusterOperationStage::Aborted
            ) {
                continue;
            }

            info!(
                target: "cluster_view",
                operation_id = %operation.id,
                stage = ?operation.stage,
                kind = ?operation.kind,
                "replaying pending cluster operation from durable store"
            );

            self.progress_cluster_operation(operation.id).await?;
            replayed = replayed.saturating_add(1);
        }

        info!(
            target: "cluster_view",
            replayed,
            "pending cluster operation startup replay complete"
        );

        let _ = self.garbage_collect_cluster_operations().await?;

        Ok(replayed)
    }

    /// Restores split/merge peer scope from durable operation history after process startup.
    ///
    /// This rebuilds the in-memory excluded-peer set used by list/sync/health loops so
    /// restart does not temporarily fall back to cross-view peer assumptions.
    pub(crate) async fn restore_peer_scope_from_operation_history(
        &self,
    ) -> Result<usize, capnp::Error> {
        let active_view = self.active_cluster_view();
        let mut operations = self.load_cluster_operations()?;
        operations.sort_by_key(ClusterOperationRecord::lineage_order_key);

        let mut excluded = HashSet::<Uuid>::new();
        let mut source_operation = None::<Uuid>;

        for operation in operations {
            if operation.dry_run {
                continue;
            }
            if !matches!(
                operation.stage,
                ClusterOperationStage::Committed | ClusterOperationStage::Finalized
            ) {
                continue;
            }

            let local_target_view = match self.target_view_for_local_participant(&operation) {
                Ok(view) => view,
                Err(err) => {
                    warn!(
                        target: "cluster_view",
                        operation_id = %operation.id,
                        kind = ?operation.kind,
                        stage = ?operation.stage,
                        "skipping operation while restoring peer scope: {err}"
                    );
                    continue;
                }
            };
            if local_target_view != active_view {
                continue;
            }

            match operation.kind {
                ClusterOperationKind::Merge => {
                    excluded.clear();
                    source_operation = Some(operation.id);
                }
                ClusterOperationKind::Split => {
                    let transition = match self.transition_for_operation(&operation) {
                        Ok(value) => value,
                        Err(err) => {
                            warn!(
                                target: "cluster_view",
                                operation_id = %operation.id,
                                kind = ?operation.kind,
                                stage = ?operation.stage,
                                "skipping split scope restore because transition derivation failed: {err}"
                            );
                            continue;
                        }
                    };
                    excluded = transition.evicted_node_ids;
                    source_operation = Some(operation.id);
                }
            }
        }

        self.set_excluded_peers(excluded.clone()).await;
        self.deps.registry.set_excluded_peers(excluded.clone());

        let excluded_count = excluded.len();
        info!(
            target: "cluster_view",
            active_view = %active_view,
            excluded_count,
            source_operation = ?source_operation,
            "restored peer scope from durable operation history"
        );

        Ok(excluded_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::operations::{SplitNetworkPolicy, SplitServicePolicy};
    use tokio::time::{Duration, timeout};

    /// Builds a transition with the requested kind and merge service policy.
    fn transition(
        kind: ClusterOperationKind,
        merge_service_policy: MergeServicePolicy,
    ) -> ClusterTransition {
        ClusterTransition {
            operation_id: Uuid::new_v4(),
            kind,
            local_target_view: ClusterViewId::legacy_default(),
            local_split_target_index: None,
            retained_node_ids: HashSet::new(),
            evicted_node_ids: HashSet::new(),
            split_service_policy: SplitServicePolicy::default(),
            split_network_policy: SplitNetworkPolicy::default(),
            merge_service_policy,
        }
    }

    /// A completed rebalance merge should wake local service reconciliation.
    #[tokio::test]
    async fn post_merge_rebalance_requests_local_service_reconcile() {
        let trigger = ServiceReconcileTrigger::new();

        assert!(request_post_merge_service_reconcile(
            &transition(ClusterOperationKind::Merge, MergeServicePolicy::Rebalance),
            &trigger,
        ));
        timeout(Duration::from_millis(100), trigger.wait_for_reconcile())
            .await
            .expect("local service reconcile request");
    }

    /// Preserve merges should leave local service reconciliation on its normal tick.
    #[tokio::test]
    async fn post_merge_preserve_does_not_request_service_reconcile() {
        let trigger = ServiceReconcileTrigger::new();

        assert!(!request_post_merge_service_reconcile(
            &transition(ClusterOperationKind::Merge, MergeServicePolicy::Preserve),
            &trigger,
        ));
        assert!(
            timeout(Duration::from_millis(10), trigger.wait_for_reconcile())
                .await
                .is_err()
        );
    }

    /// Split transitions must not use the merge-specific service wakeup.
    #[tokio::test]
    async fn split_does_not_request_post_merge_service_reconcile() {
        let trigger = ServiceReconcileTrigger::new();

        assert!(!request_post_merge_service_reconcile(
            &transition(ClusterOperationKind::Split, MergeServicePolicy::Rebalance),
            &trigger,
        ));
        assert!(
            timeout(Duration::from_millis(10), trigger.wait_for_reconcile())
                .await
                .is_err()
        );
    }

    /// Operation-derived name timestamps must stay fixed while operation stages advance.
    #[test]
    fn cluster_name_hint_timestamp_uses_operation_creation_time() {
        let operation = ClusterOperationRecord {
            id: Uuid::new_v4(),
            submitted_by_node_id: Uuid::new_v4(),
            kind: ClusterOperationKind::Split,
            stage: ClusterOperationStage::Finalized,
            dry_run: false,
            created_at_unix_ms: 10,
            depends_on_operation_id: None,
            source_views: Vec::new(),
            target_views: Vec::new(),
            target_cluster_names: Vec::new(),
            split_assignments: Vec::new(),
            split_service_policy: Default::default(),
            split_network_policy: Default::default(),
            merge_service_policy: Default::default(),
            updated_at_unix_ms: 20,
            details: String::new(),
        };

        assert_eq!(Topology::operation_cluster_name_timestamp(&operation), 10);
    }

    /// Ensures committed operations cannot be overwritten by late stale-precondition aborts.
    #[test]
    fn cluster_operation_stage_rank_keeps_commit_terminal_over_abort() {
        assert!(
            Topology::stage_rank(ClusterOperationStage::Committed)
                > Topology::stage_rank(ClusterOperationStage::Aborted)
        );
        assert!(
            Topology::stage_rank(ClusterOperationStage::Finalized)
                > Topology::stage_rank(ClusterOperationStage::Aborted)
        );
        assert!(
            Topology::stage_rank(ClusterOperationStage::Aborted)
                > Topology::stage_rank(ClusterOperationStage::Prepared)
        );
    }
}
