use super::{
    CLUSTER_OPERATION_FINALIZED_RETENTION_COUNT, COMMIT_PRECONDITION_FAILURE_PREFIX, Topology,
};
use crate::cluster::ClusterViewId;
use crate::topology::operation::{
    ClusterOperationKind, ClusterOperationRecord, ClusterOperationStage,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};
use uuid::Uuid;

impl Topology {
    /// Maps operation stage values into a monotonic ordering used for conflict resolution.
    pub(super) fn stage_rank(stage: ClusterOperationStage) -> u8 {
        match stage {
            ClusterOperationStage::Proposed => 0,
            ClusterOperationStage::Prepared => 1,
            ClusterOperationStage::Committed => 2,
            ClusterOperationStage::Finalized => 3,
            ClusterOperationStage::Aborted => 4,
        }
    }

    /// Returns the current UNIX timestamp in milliseconds for durable operation metadata updates.
    pub(super) fn now_unix_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or_default()
    }

    /// Resolves the active-view set accepted for commit-time side effects on this operation.
    fn commit_precondition_views(
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
        }
        Ok(allowed_views)
    }

    /// Validates the operation still matches the current local active view before commit effects.
    fn ensure_commit_precondition(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<(), capnp::Error> {
        let active_view = self.active_cluster_view();
        let allowed_views = Self::commit_precondition_views(operation)?;
        if allowed_views.iter().any(|view| *view == active_view) {
            return Ok(());
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

    /// Returns the most recently updated non-finalized cluster operation, if any.
    pub(crate) fn active_cluster_operation(
        &self,
    ) -> Result<Option<ClusterOperationRecord>, capnp::Error> {
        let mut active = self
            .load_cluster_operations()?
            .into_iter()
            .filter(|operation| {
                !operation.dry_run
                    && matches!(
                        operation.stage,
                        ClusterOperationStage::Proposed
                            | ClusterOperationStage::Prepared
                            | ClusterOperationStage::Committed
                    )
            })
            .collect::<Vec<_>>();

        active.sort_by(|left, right| {
            right
                .updated_at_unix_ms
                .cmp(&left.updated_at_unix_ms)
                .then_with(|| right.id.cmp(&left.id))
        });

        Ok(active.into_iter().next())
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

    /// Persists the active cluster view durably so finalized operations survive process restarts.
    fn persist_active_cluster_view(&self, view: ClusterViewId) -> Result<(), capnp::Error> {
        self.cluster_view_store
            .write_active_view(view)
            .map_err(|err| capnp::Error::failed(err.to_string()))
    }

    /// Returns true when an error represents a stale commit precondition mismatch.
    pub(super) fn is_commit_precondition_failure(err: &capnp::Error) -> bool {
        err.to_string().contains(COMMIT_PRECONDITION_FAILURE_PREFIX)
    }

    /// Persists a cluster operation record in the local durable operation store.
    pub(super) fn persist_cluster_operation(
        &self,
        op: &ClusterOperationRecord,
    ) -> Result<(), capnp::Error> {
        let encoded = bincode::serialize(op).map_err(|e| capnp::Error::failed(e.to_string()))?;
        self.cluster_operations
            .put(op.id, &encoded)
            .map_err(|e| capnp::Error::failed(e.to_string()))
    }

    /// Loads a cluster operation record by id from the local durable operation store.
    pub(super) fn load_cluster_operation(
        &self,
        id: Uuid,
    ) -> Result<Option<ClusterOperationRecord>, capnp::Error> {
        let bytes = self
            .cluster_operations
            .get(id)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let decoded: ClusterOperationRecord =
            bincode::deserialize(&bytes).map_err(|e| capnp::Error::failed(e.to_string()))?;
        Ok(Some(decoded))
    }

    /// Loads all operation records from the local durable store, skipping malformed rows.
    pub(super) fn load_cluster_operations(
        &self,
    ) -> Result<Vec<ClusterOperationRecord>, capnp::Error> {
        let encoded_rows = self
            .cluster_operations
            .list()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        let mut operations = Vec::with_capacity(encoded_rows.len());

        for (operation_id, payload) in encoded_rows {
            match bincode::deserialize::<ClusterOperationRecord>(&payload) {
                Ok(operation) => {
                    if operation.id != operation_id {
                        warn!(
                            target: "cluster_view",
                            key_id = %operation_id,
                            payload_id = %operation.id,
                            "skipping cluster operation with mismatched durable key and payload id"
                        );
                        continue;
                    }
                    operations.push(operation);
                }
                Err(err) => {
                    warn!(
                        target: "cluster_view",
                        operation_id = %operation_id,
                        "skipping malformed cluster operation payload: {err}"
                    );
                }
            }
        }

        Ok(operations)
    }

    /// Parses a cluster operation id from raw RPC bytes, enforcing UUID byte length.
    pub(super) fn operation_id_from_data(
        data: capnp::data::Reader<'_>,
    ) -> Result<Uuid, capnp::Error> {
        let id_bytes: [u8; 16] = data
            .try_into()
            .map_err(|_| capnp::Error::failed("cluster operation id must be 16 bytes".into()))?;
        Ok(Uuid::from_bytes(id_bytes))
    }

    /// Updates an operation stage, appends stage details, and persists the updated record.
    fn update_cluster_operation_stage(
        &self,
        operation: &mut ClusterOperationRecord,
        stage: ClusterOperationStage,
        detail: &str,
    ) -> Result<(), capnp::Error> {
        operation.stage = stage;
        operation.updated_at_unix_ms = Self::now_unix_ms();
        if !detail.is_empty() {
            operation.details = format!("{} | {}", operation.details, detail);
        }
        self.persist_cluster_operation(operation)
    }

    /// Removes old terminal operations so the durable operation table stays bounded over long runtimes.
    pub(super) fn garbage_collect_cluster_operations(&self) -> Result<usize, capnp::Error> {
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

        let to_delete = terminal
            .into_iter()
            .skip(CLUSTER_OPERATION_FINALIZED_RETENTION_COUNT)
            .map(|operation| operation.id)
            .collect::<Vec<_>>();
        let removed = self
            .cluster_operations
            .delete_many(&to_delete)
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

    /// Applies local side effects for a committed operation, including active view switch.
    pub(super) async fn apply_committed_operation_side_effects(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<(), capnp::Error> {
        self.ensure_commit_precondition(operation)?;

        let transition = self.transition_for_operation(operation)?;
        let reports = self.run_transition_commit_hooks(&transition).await?;
        for report in reports {
            info!(
                target: "cluster_view",
                operation_id = %transition.operation_id,
                participant = report.name,
                details = %report.render(),
                "applied cluster transition participant"
            );
        }

        self.persist_active_cluster_view(transition.local_target_view)?;
        let previous = self.set_active_cluster_view(transition.local_target_view);
        self.registry.clear().await;
        info!(
            target: "cluster_view",
            operation_id = %transition.operation_id,
            previous_view = %previous,
            target_view = %transition.local_target_view,
            "applied operation commit side effects"
        );

        Ok(())
    }

    /// Starts asynchronous local progression for a cluster operation if it is not a dry run.
    pub(super) fn trigger_operation_progress(&self, operation_id: Uuid, dry_run: bool) {
        if dry_run {
            return;
        }

        let topology = self.clone();
        tokio::task::spawn_local(async move {
            if let Err(err) = topology.progress_cluster_operation(operation_id).await {
                warn!(
                    target: "cluster_view",
                    operation_id = %operation_id,
                    "failed to progress cluster operation: {err}"
                );
            }
        });
    }

    /// Progresses one operation forward based on its current persisted stage.
    async fn progress_cluster_operation(&self, operation_id: Uuid) -> Result<(), capnp::Error> {
        let _guard = self.operations.gate.lock().await;

        let mut operation = self.load_cluster_operation(operation_id)?.ok_or_else(|| {
            capnp::Error::failed(format!("cluster operation not found: {operation_id}"))
        })?;

        match operation.stage {
            ClusterOperationStage::Proposed => {
                self.update_cluster_operation_stage(
                    &mut operation,
                    ClusterOperationStage::Prepared,
                    "prepared",
                )?;
                if let Err(err) = self
                    .apply_committed_operation_side_effects(&operation)
                    .await
                {
                    if Self::is_commit_precondition_failure(&err) {
                        self.update_cluster_operation_stage(
                            &mut operation,
                            ClusterOperationStage::Aborted,
                            &format!("aborted stale_precondition: {err}"),
                        )?;
                        warn!(
                            target: "cluster_view",
                            operation_id = %operation.id,
                            stage = ?operation.stage,
                            "aborted cluster operation due to commit precondition mismatch: {err}"
                        );
                    } else {
                        return Err(err);
                    }
                } else {
                    self.update_cluster_operation_stage(
                        &mut operation,
                        ClusterOperationStage::Committed,
                        &format!("committed active_view={}", self.active_cluster_view()),
                    )?;
                    self.update_cluster_operation_stage(
                        &mut operation,
                        ClusterOperationStage::Finalized,
                        "finalized",
                    )?;
                }
            }
            ClusterOperationStage::Prepared => {
                if let Err(err) = self
                    .apply_committed_operation_side_effects(&operation)
                    .await
                {
                    if Self::is_commit_precondition_failure(&err) {
                        self.update_cluster_operation_stage(
                            &mut operation,
                            ClusterOperationStage::Aborted,
                            &format!("aborted stale_precondition: {err}"),
                        )?;
                        warn!(
                            target: "cluster_view",
                            operation_id = %operation.id,
                            stage = ?operation.stage,
                            "aborted cluster operation due to commit precondition mismatch: {err}"
                        );
                    } else {
                        return Err(err);
                    }
                } else {
                    self.update_cluster_operation_stage(
                        &mut operation,
                        ClusterOperationStage::Committed,
                        &format!("committed active_view={}", self.active_cluster_view()),
                    )?;
                    self.update_cluster_operation_stage(
                        &mut operation,
                        ClusterOperationStage::Finalized,
                        "finalized",
                    )?;
                }
            }
            ClusterOperationStage::Committed => {
                self.update_cluster_operation_stage(
                    &mut operation,
                    ClusterOperationStage::Finalized,
                    "finalized",
                )?;
            }
            ClusterOperationStage::Finalized | ClusterOperationStage::Aborted => {}
        }

        let _ = self.garbage_collect_cluster_operations()?;
        drop(_guard);

        if !operation.dry_run {
            let _ = self.broadcast_cluster_operation(&operation).await?;
        }

        Ok(())
    }

    /// Replays any non-finalized durable operation records so crashes do not strand topology changes.
    pub(crate) async fn replay_cluster_operations_on_startup(&self) -> Result<usize, capnp::Error> {
        let mut operations = self.load_cluster_operations()?;
        operations.sort_by_key(|operation| operation.id);

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
            "cluster operation startup replay complete"
        );

        let _ = self.garbage_collect_cluster_operations()?;

        Ok(replayed)
    }
}
