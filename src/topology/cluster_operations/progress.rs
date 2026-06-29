use crate::cluster::operations::{
    ClusterOperationKind, ClusterOperationRecord, ClusterOperationStage,
};
use crate::cluster::{ClusterId, ClusterViewId};
use crate::secrets::master_key::reconciler::SecretMasterKeyReconciler;
use crate::store::replicated::cluster_views::{ClusterNameRecord, ClusterNodeCountRecord};
use crate::topology::Topology;
use crate::topology::cluster_operations::{
    CLUSTER_OPERATION_FINALIZED_RETENTION_COUNT, COMMIT_PRECONDITION_FAILURE_PREFIX,
};
use crate::topology::peers::PeerValue;
use mantissa_protocol::server::cluster_session;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};
use uuid::Uuid;

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

    /// Publishes the local active cluster's current member count into the replicated metadata domain.
    pub(crate) async fn publish_local_cluster_node_count(&self) -> Result<bool, capnp::Error> {
        if !self.local_allows_outbound_cluster_traffic() {
            return Ok(false);
        }

        let local_view = self.active_cluster_view();
        let node_count = self.local_cluster_view_member_count().await?;
        let record = ClusterNodeCountRecord {
            node_count,
            source_view: local_view,
            updated_at_unix_ms: Self::now_unix_ms(),
            actor_node_id: self.local.node.id,
            membership_generation: self.stores.peers.change_clock(),
        };
        self.upsert_cluster_node_count_record(local_view.cluster_id, &record)
            .await
    }

    /// Persists split target names carried by one operation record so cluster lineage labels survive restarts.
    async fn persist_operation_cluster_name_hints(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<(), capnp::Error> {
        if operation.target_cluster_names.len() != operation.target_views.len() {
            return Ok(());
        }

        let updated_at_unix_ms = if operation.updated_at_unix_ms == 0 {
            Self::now_unix_ms()
        } else {
            operation.updated_at_unix_ms
        };

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

            let updated_at_unix_ms = if operation.updated_at_unix_ms == 0 {
                Self::now_unix_ms()
            } else {
                operation.updated_at_unix_ms
            };
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
    pub(in crate::topology) fn stage_rank(stage: ClusterOperationStage) -> u8 {
        stage.rank()
    }

    /// Returns the current UNIX timestamp in milliseconds for durable operation metadata updates.
    pub(in crate::topology) fn now_unix_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or_default()
    }

    /// Reads the cluster view currently bound to a session for operation relay validation.
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
        let local_view = self.active_cluster_view();
        let excluded_peers = self.excluded_peers_snapshot().await;
        let (actives, _) = self
            .stores
            .peers
            .load_all_regs()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let mut count = 1u32;
        for (key, reg) in actives {
            let peer_id = key.to_uuid();
            if peer_id == self.local.node.id {
                continue;
            }
            if excluded_peers.contains(&peer_id) {
                continue;
            }
            let Some(_selected) = PeerValue::select_reg(&reg).filter(|value| value.is_active())
            else {
                continue;
            };

            let view = self
                .best_known_peer_view(peer_id)
                .await
                .unwrap_or(local_view);
            if view != local_view {
                continue;
            }
            count = count.saturating_add(1);
        }

        Ok(count)
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

    /// Best-effort relay of one operation record to peers in the operation's relay scope.
    pub(in crate::topology) async fn broadcast_cluster_operation(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<usize, capnp::Error> {
        let relay_views = match operation.kind {
            ClusterOperationKind::Split => {
                let source_view = operation.source_views.first().copied().ok_or_else(|| {
                    capnp::Error::failed("split operation missing source view".to_string())
                })?;
                HashSet::from([source_view])
            }
            ClusterOperationKind::Merge => {
                let source_view = operation.source_views.first().copied().ok_or_else(|| {
                    capnp::Error::failed("merge operation missing source view".to_string())
                })?;
                let mut views = HashSet::from([source_view]);
                for target in operation.target_views.iter().copied() {
                    views.insert(target);
                }
                views
            }
        };
        let snapshot = match self.peer_snapshot().await {
            Some(snapshot) => snapshot,
            None => return Ok(0),
        };
        let payload = operation
            .encode_capnp()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        let mut relayed = 0usize;

        for entry in snapshot.entries.iter() {
            let peer_id = entry.peer_id;
            if peer_id == self.local.node.id {
                continue;
            }

            let session = if operation.kind == ClusterOperationKind::Merge {
                self.deps.registry.session_for_peer_unscoped(peer_id).await
            } else {
                self.deps.registry.session_for_peer(peer_id).await
            };
            let Some(session) = session else {
                continue;
            };
            let peer_view = match Self::session_cluster_view(&session).await {
                Ok(view) => view,
                Err(err) => {
                    warn!(
                        target: "cluster_view",
                        operation_id = %operation.id,
                        peer_id = %peer_id,
                        "failed to read peer session view for operation relay: {err}"
                    );
                    continue;
                }
            };
            if !relay_views.contains(&peer_view) {
                continue;
            }

            let topology = session
                .get_topology_request()
                .send()
                .pipeline
                .get_topology();
            let mut relay = topology.submit_cluster_operation_request();
            relay.get().set_id(operation.id.as_bytes());
            relay.get().set_payload(&payload);
            match relay.send().promise.await {
                Ok(_) => {
                    relayed = relayed.saturating_add(1);
                }
                Err(err) => {
                    warn!(
                        target: "cluster_view",
                        operation_id = %operation.id,
                        peer_id = %peer_id,
                        "failed to relay cluster operation: {err}"
                    );
                }
            }
        }

        if relayed > 0 {
            info!(
                target: "cluster_view",
                operation_id = %operation.id,
                relayed,
                relay_view_count = relay_views.len(),
                "relayed cluster operation to peers"
            );
        }

        Ok(relayed)
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
        let operation_key = Self::operation_execution_key(operation);
        for candidate in self.load_cluster_operations()? {
            if candidate.id == operation.id
                || candidate.dry_run
                || candidate.kind != ClusterOperationKind::Merge
                || Self::operation_execution_key(&candidate) >= operation_key
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

    /// Returns the stable creation time used for operation ordering.
    fn operation_created_at_unix_ms(operation: &ClusterOperationRecord) -> u64 {
        if operation.created_at_unix_ms == 0 {
            operation.updated_at_unix_ms
        } else {
            operation.created_at_unix_ms
        }
    }

    /// Returns the stable operation order used when several ready operations overlap.
    fn operation_execution_key(operation: &ClusterOperationRecord) -> (u64, Uuid) {
        (Self::operation_created_at_unix_ms(operation), operation.id)
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

    /// Returns whether a non-terminal operation can advance from this node's active view.
    fn non_terminal_operation_affects_active_view(
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
            ClusterOperationKind::Split => Ok(operation.source_views.contains(&active_view)),
        }
    }

    /// Selects the latest pending overlapping predecessor for a new or relayed operation.
    pub(in crate::topology) fn pending_cluster_operation_tail_for(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<Option<ClusterOperationRecord>, capnp::Error> {
        if operation.dry_run || Self::operation_stage_is_terminal(operation.stage) {
            return Ok(None);
        }

        let operation_key = Self::operation_execution_key(operation);
        let mut predecessors = Vec::new();
        for candidate in self.load_cluster_operations()? {
            if candidate.id == operation.id
                || candidate.dry_run
                || Self::operation_stage_is_terminal(candidate.stage)
                || Self::operation_execution_key(&candidate) >= operation_key
                || !Self::operations_overlap(&candidate, operation)
            {
                continue;
            }
            predecessors.push(candidate);
        }

        predecessors.sort_by_key(Self::operation_execution_key);
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

        active.sort_by_key(Self::operation_execution_key);

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

        active.sort_by_key(Self::operation_execution_key);

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
            && self.non_terminal_operation_affects_active_view(operation)?
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

    /// Persists the active cluster view durably so finalized operations survive process restarts.
    fn persist_active_cluster_view(&self, view: ClusterViewId) -> Result<(), capnp::Error> {
        self.stores
            .cluster_view_store
            .write_active_view(view)
            .map_err(|err| capnp::Error::failed(err.to_string()))
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
    /// Relayed terminal rows can race with an already-running local progress task. Before writing
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

        let to_delete = terminal
            .into_iter()
            .skip(CLUSTER_OPERATION_FINALIZED_RETENTION_COUNT)
            .map(|operation| operation.id)
            .collect::<Vec<_>>();
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

    /// Applies local side effects for a committed operation, including active view switch.
    pub(in crate::topology) async fn apply_committed_operation_side_effects(
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
                    "failed to publish cluster node count after committed transition: {err}"
                );
            }
        }
        info!(
            target: "cluster_view",
            operation_id = %transition.operation_id,
            previous_view = %previous,
            target_view = %transition.local_target_view,
            "applied operation commit side effects"
        );

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

    /// Applies finalized operations learned through metadata anti-entropy and wakes queued work.
    pub(crate) async fn reconcile_cluster_operations_after_metadata_sync(
        &self,
    ) -> Result<(), capnp::Error> {
        let mut operations = self.load_cluster_operations()?;
        operations.sort_by_key(Self::operation_execution_key);

        for operation in operations {
            if operation.dry_run || operation.stage != ClusterOperationStage::Finalized {
                continue;
            }

            let Some(target) =
                self.target_view_if_finalized_operation_affects_active_view(&operation)?
            else {
                debug!(
                    target: "cluster_view",
                    operation_id = %operation.id,
                    kind = ?operation.kind,
                    active_view = %self.active_cluster_view(),
                    "skipping synced finalized operation outside local cluster lineage"
                );
                continue;
            };
            if operation.kind != ClusterOperationKind::Merge && self.active_cluster_view() == target
            {
                continue;
            }

            if let Err(err) = self
                .apply_committed_operation_side_effects(&operation)
                .await
            {
                if Self::is_commit_precondition_failure(&err) {
                    debug!(
                        target: "cluster_view",
                        operation_id = %operation.id,
                        "skipped synced finalized operation side effects due to commit precondition mismatch: {err}"
                    );
                    continue;
                }
                return Err(err);
            }
        }

        self.trigger_ready_cluster_operation_progress();
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
        if !self.non_terminal_operation_affects_active_view(&operation)? {
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
                if let Err(err) = self
                    .apply_committed_operation_side_effects(&operation)
                    .await
                {
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
                if let Err(err) = self
                    .apply_committed_operation_side_effects(&operation)
                    .await
                {
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
            ClusterOperationStage::Finalized | ClusterOperationStage::Aborted => {}
        }

        let _ = self.garbage_collect_cluster_operations().await?;
        drop(_guard);

        if !operation.dry_run {
            let _ = self.broadcast_cluster_operation(&operation).await?;
        }

        if let Some(next) = self.active_cluster_operation_excluding(operation_id)? {
            self.trigger_operation_progress(next.id, false);
        }

        Ok(())
    }

    /// Replays any non-finalized durable operation records so crashes do not strand topology changes.
    pub(crate) async fn replay_cluster_operations_on_startup(&self) -> Result<usize, capnp::Error> {
        let mut operations = self.load_cluster_operations()?;
        operations.sort_by_key(Self::operation_execution_key);

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
        operations.sort_by_key(Self::operation_execution_key);

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
