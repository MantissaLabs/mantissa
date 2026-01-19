use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_channel::Sender;
use chrono::Utc;
use crdt_store::uuid_key::UuidKey;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::gossip::Message;
use crate::scheduler::{SchedulerError, SlotId, SlotState};
use crate::task::container::ContainerState;
use crate::task::docker::{
    ContainerCreateRequest, ContainerError, ResourceLimits, RestartPolicyConfig, RestartPolicyType,
};
use crate::task::types::{
    TaskEvent, TaskRestartPolicy, TaskRestartPolicyKind, TaskSpec, TaskValue, TaskValueDraft,
};

use super::{
    TaskManager, container_already_running, is_name_conflict, value_to_spec, wrap_create_error,
    wrap_existing_inspect_error, wrap_start_error,
};

impl TaskManager {
    /// Persists a task snapshot in the backing store.
    pub(super) async fn persist_spec(&self, spec: &TaskSpec) -> Result<(), anyhow::Error> {
        let mut value = TaskValue::new(TaskValueDraft {
            id: spec.id,
            name: spec.name.clone(),
            image: spec.image.clone(),
            state: spec.state.clone(),
            created_at: spec.created_at.clone(),
            command: spec.command.clone(),
            node_id: spec.node_id,
            node_name: spec.node_name.clone(),
            slot_ids: spec.slot_ids.clone(),
            networks: spec.networks.clone(),
            cpu_millis: spec.cpu_millis,
            memory_bytes: spec.memory_bytes,
            env: spec.env.clone(),
            secret_files: spec.secret_files.clone(),
            service_metadata: spec.service_metadata.clone(),
        });

        value.restart_policy = spec.restart_policy.clone();

        self.store
            .upsert(&UuidKey::from(spec.id), value)
            .await
            .map_err(|e| anyhow::anyhow!("task upsert failed: {e}"))
    }

    /// Removes a task snapshot from the store.
    pub(super) async fn remove_spec(&self, id: Uuid) -> Result<(), anyhow::Error> {
        self.store
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow::anyhow!("task remove failed: {e}"))?;
        Ok(())
    }

    fn tx(&self) -> Sender<Message> {
        self.tx.clone()
    }

    /// Broadcasts specs originating from remote peers to the local gossip loop.
    pub(super) async fn broadcast_remote_specs(&self, specs: &[TaskSpec]) {
        for spec in specs {
            if spec.node_id == self.local_node_id {
                continue;
            }

            if let Err(err) = self
                .enqueue_gossip(TaskEvent::Upsert(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to relay task {} from node {}: {err}",
                    spec.name,
                    spec.node_id
                );
            }
        }
    }

    /// Ensures that slots that no longer correspond to running containers are released.
    pub(super) async fn cleanup_orphaned_slots(&self) {
        const MAX_ATTEMPTS: usize = 5;

        for _ in 0..MAX_ATTEMPTS {
            let snapshot = match self.scheduler.snapshot().await {
                Some(snapshot) => snapshot,
                None => return,
            };

            let reserved: Vec<SlotId> = snapshot
                .slots
                .iter()
                .filter_map(|slot| match &slot.state {
                    SlotState::Reserved(reservation) if reservation.owner == self.local_node_id => {
                        Some(slot.slot_id)
                    }
                    _ => None,
                })
                .collect();

            if reserved.is_empty() {
                return;
            }

            let active = match self.collect_local_slot_ids().await {
                Ok(ids) => ids,
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to collect active slots while cleaning orphans: {err}"
                    );
                    return;
                }
            };

            let to_free: Vec<SlotId> = reserved
                .into_iter()
                .filter(|slot_id| !active.contains(slot_id))
                .collect();

            if to_free.is_empty() {
                return;
            }

            match self
                .scheduler
                .free_slots(snapshot.version, to_free.clone())
                .await
            {
                Ok(_) => return,
                Err(SchedulerError::SnapshotMismatch { .. })
                | Err(SchedulerError::SlotsNotReserved { .. }) => continue,
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to free orphaned slots {:?}: {err}",
                        to_free
                    );
                    return;
                }
            }
        }
    }

    /// Collects the set of slot IDs that belong to tasks owned by this node.
    pub(super) async fn collect_local_slot_ids(&self) -> Result<HashSet<SlotId>, anyhow::Error> {
        let (actives, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

        let mut slots = HashSet::new();
        for (key, snapshot) in actives {
            let id = key.to_uuid();
            if let Some(value) = snapshot.as_slice().last() {
                if value.node_id == self.local_node_id {
                    if value.slot_ids.is_empty() {
                        if let Some(slot_id) = value.slot_id {
                            slots.insert(slot_id);
                        }
                    } else {
                        for slot_id in &value.slot_ids {
                            slots.insert(*slot_id);
                        }
                    }
                }
            } else {
                let _ = self.remove_spec(id).await;
            }
        }

        Ok(slots)
    }

    /// Pushes a gossip event into the dispatcher queue.
    pub(super) async fn enqueue_gossip(&self, event: TaskEvent) -> Result<(), anyhow::Error> {
        let id = Uuid::new_v4();
        let message = Message::Task { id, event };
        self.tx()
            .send(message)
            .await
            .map_err(|e| anyhow::anyhow!("failed to enqueue task gossip: {e}"))
    }

    /// Performs a graceful stop of a locally owned task and tears down its container.
    pub(super) async fn perform_local_stop(
        &self,
        spec: TaskSpec,
    ) -> Result<TaskSpec, anyhow::Error> {
        if matches!(spec.state, ContainerState::Stopped) {
            return Ok(spec);
        }

        let id = spec.id;
        let identifier_entry = {
            let mut guard = self.local_containers.lock().await;
            guard.remove(&id)
        };

        let (container_identifier, from_cache) = match identifier_entry {
            Some(value) => (value, true),
            None => (format!("mantissa-{id}"), false),
        };

        let mut updated = spec.clone();
        if !matches!(spec.state, ContainerState::Stopping) {
            updated.state = ContainerState::Stopping;
            self.persist_spec(&updated).await?;
            self.enqueue_gossip(TaskEvent::Upsert(Box::new(updated.clone())))
                .await?;
        }

        match self
            .container_manager
            .stop_container(&container_identifier, Some(Duration::from_secs(10)))
            .await
        {
            Ok(_) => {}
            Err(ContainerError::NotFound(_)) => {
                debug!(
                    target: "task",
                    "container {container_identifier} not found while stopping task {id}; cache_hit={from_cache}"
                );
            }
            Err(e) => {
                updated.state = spec.state;
                if updated.state != ContainerState::Stopping {
                    self.persist_spec(&updated).await?;
                    self.enqueue_gossip(TaskEvent::Upsert(Box::new(updated.clone())))
                        .await?;
                }
                return Err(anyhow::anyhow!("docker stop failed: {e}"));
            }
        }

        if let Err(e) = self
            .container_manager
            .remove_container(&container_identifier, false, true)
            .await
        {
            match e {
                ContainerError::NotFound(_) => debug!(
                    target: "task",
                    "container {container_identifier} already absent while removing task {id}"
                ),
                other => warn!(
                    target: "task",
                    "failed to remove container {container_identifier}: {other}"
                ),
            }
        }

        self.cleanup_secret_artifacts(id).await;

        if let Err(err) = self.teardown_runtime_attachments(id, HashSet::new()).await {
            warn!(
                target: "task",
                "failed to teardown network attachments for task {}: {err}",
                id
            );
        }

        updated.state = ContainerState::Stopped;
        if !spec.slot_ids.is_empty() {
            for slot_id in &spec.slot_ids {
                self.release_slot(*slot_id)
                    .await
                    .with_context(|| "scheduler release failed during stop".to_string())?;
            }
            updated.slot_ids.clear();
            updated.slot_id = None;
            updated.cpu_millis = 0;
            updated.memory_bytes = 0;
        }

        self.persist_spec(&updated).await?;
        self.enqueue_gossip(TaskEvent::Upsert(Box::new(updated.clone())))
            .await?;
        self.cleanup_orphaned_slots().await;
        self.remove_spec(id).await?;
        self.enqueue_gossip(TaskEvent::Remove { id }).await?;
        if let Err(err) = self.cleanup_orphaned_local_attachments().await {
            warn!(
                target: "task",
                task = %id,
                "failed to run orphaned attachment cleanup after stop: {err}"
            );
        }
        Ok(updated)
    }

    /// Marks a task as failed and frees any resources it owned.
    pub(super) async fn mark_task_failed(
        &self,
        mut spec: TaskSpec,
        error: anyhow::Error,
    ) -> anyhow::Error {
        let task_id = spec.id;
        warn!(
            target: "task",
            error = %error,
            error_chain = %format!("{error:#}"),
            task = %spec.name,
            task_id = %task_id,
            "marking task as failed"
        );

        {
            let mut guard = self.local_containers.lock().await;
            guard.remove(&task_id);
        }

        self.cleanup_secret_artifacts(task_id).await;

        if let Err(err) = self
            .teardown_runtime_attachments(task_id, HashSet::new())
            .await
        {
            warn!(
                target: "task",
                "failed to teardown attachments after failure of {}: {err}",
                task_id
            );
        }

        if !spec.slot_ids.is_empty() {
            for slot_id in &spec.slot_ids {
                if let Err(err) = self.release_slot(*slot_id).await {
                    warn!(
                        target: "task",
                        "failed to release slot {} after failure of {}: {err}",
                        slot_id,
                        task_id
                    );
                }
            }
            spec.slot_ids.clear();
            spec.slot_id = None;
        }

        spec.state = ContainerState::Failed;

        if let Err(err) = self.persist_spec(&spec).await {
            warn!(
                target: "task",
                "failed to persist failed state for task {}: {err}",
                task_id
            );
        } else if let Err(err) = self
            .enqueue_gossip(TaskEvent::Upsert(Box::new(spec.clone())))
            .await
        {
            warn!(
                target: "task",
                "failed to broadcast failed state for task {}: {err}",
                task_id
            );
        }

        self.cleanup_orphaned_slots().await;
        error
    }

    pub(super) async fn resolve_dns_servers(
        &self,
        network_ids: &[Uuid],
    ) -> anyhow::Result<Vec<String>> {
        if network_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut servers = Vec::new();
        let mut seen = HashSet::new();

        for network_id in network_ids {
            match self.network_registry.get_spec(*network_id) {
                Ok(Some(spec)) => {
                    match crate::network::allocator::resolver_ipv4_address(
                        &spec,
                        self.local_node_id,
                    ) {
                        Ok(addr) => {
                            if seen.insert(addr) {
                                servers.push(addr.to_string());
                            }
                        }
                        Err(err) => {
                            warn!(
                                target: "task",
                                network = %network_id,
                                "failed to compute resolver address: {err}"
                            );
                        }
                    }
                }
                Ok(None) => {
                    warn!(
                        target: "task",
                        network = %network_id,
                        "missing network spec while computing resolver"
                    );
                }
                Err(err) => {
                    warn!(
                        target: "task",
                        network = %network_id,
                        "failed to load network spec while computing resolver: {err:#}"
                    );
                }
            }
        }

        if servers.is_empty() && !network_ids.is_empty() {
            anyhow::bail!("no DNS resolvers available for task networks: {network_ids:?}");
        }

        Ok(servers)
    }

    /// Maps a task restart policy into the Docker restart policy configuration.
    pub(super) fn restart_policy_to_config(policy: &TaskRestartPolicy) -> RestartPolicyConfig {
        RestartPolicyConfig {
            name: match policy.name {
                TaskRestartPolicyKind::No => RestartPolicyType::No,
                TaskRestartPolicyKind::Always => RestartPolicyType::Always,
                TaskRestartPolicyKind::OnFailure => RestartPolicyType::OnFailure,
                TaskRestartPolicyKind::UnlessStopped => RestartPolicyType::UnlessStopped,
            },
            max_retry_count: policy.max_retry_count,
        }
    }

    /// Ensures the in-memory container tracking reflects the persisted spec.
    pub(super) async fn ensure_local_tracking(&self, spec: &TaskSpec) {
        let mut guard = self.local_containers.lock().await;
        guard
            .entry(spec.id)
            .or_insert_with(|| format!("mantissa-{}", spec.id));
    }

    /// Starts or reuses a container so the task transitions into running state locally.
    pub(super) async fn ensure_task_running(&self, spec: TaskSpec) -> Result<(), anyhow::Error> {
        let mut working = spec.clone();
        let task_name = working.name.clone();

        if matches!(working.state, ContainerState::Running) {
            self.ensure_local_tracking(&working).await;
            return Ok(());
        }

        if matches!(
            spec.state,
            ContainerState::Stopping | ContainerState::Stopped | ContainerState::Failed
        ) {
            return Ok(());
        }

        // Never attempt to launch a container unless the scheduler assignment is visible:
        // otherwise we would leak reservations and confuse remote capability negotiations.
        if spec.slot_ids.is_empty() {
            return Err(anyhow!(
                "task {} ({}) missing scheduler slot assignments",
                spec.name,
                spec.id
            ));
        }

        // Drive a single state transition to `Creating` so peers observe progress exactly once.
        if !matches!(working.state, ContainerState::Creating) {
            working.state = ContainerState::Creating;
            self.persist_spec(&working).await?;
            if let Err(err) = self
                .enqueue_gossip(TaskEvent::Upsert(Box::new(working.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to broadcast creating state for task {}: {err}",
                    working.id
                );
            }
        }

        if let Err(err) = self
            .container_manager
            .pull_image(&working.image)
            .await
            .with_context(|| format!("docker pull failed for image {}", working.image))
        {
            let err = self.mark_task_failed(working, err).await;
            return Err(err);
        }

        let restart_policy = working
            .restart_policy
            .as_ref()
            .map(Self::restart_policy_to_config);

        let resource_limits =
            ResourceLimits::from_requests(working.cpu_millis, working.memory_bytes);

        let mut resolved = self
            .resolve_runtime_secrets(working.id, &working.env, &working.secret_files)
            .await?;
        let env_vars = if resolved.env.is_empty() {
            None
        } else {
            Some(resolved.env.clone())
        };
        let volumes = if resolved.mounts.is_empty() {
            None
        } else {
            Some(resolved.mounts.clone())
        };

        let container_name = format!("mantissa-{}", working.id);

        debug!(
            target: "task",
            task = %working.id,
            container = %container_name,
            networks = ?working.networks,
            "resolving dns servers for task"
        );
        let dns_servers = self.resolve_dns_servers(&working.networks).await?;
        let dns_servers = if dns_servers.is_empty() {
            None
        } else {
            Some(dns_servers)
        };

        let create_request = ContainerCreateRequest {
            name: container_name.clone(),
            image: working.image.clone(),
            command: if working.command.is_empty() {
                None
            } else {
                Some(working.command.clone())
            },
            env_vars,
            ports: None,
            volumes,
            restart_policy,
            resource_limits,
            dns_servers,
        };

        let create_outcome = self
            .container_manager
            .create_container(create_request)
            .await;

        let (container_id, created_fresh): (String, bool) = match create_outcome {
            Ok(id) => (id, true),
            Err(err) => {
                if is_name_conflict(&err) {
                    match self.resolve_existing_container_id(&container_name).await {
                        Ok(Some(existing_id)) => (existing_id, false),
                        Ok(None) => {
                            if let Some(artifacts) = resolved.artifacts.take() {
                                if let Err(clean_err) = artifacts.cleanup().await {
                                    warn!(
                                        target: "task",
                                        "failed to cleanup staged secrets after missing container {}: {clean_err}",
                                        working.id
                                    );
                                }
                            }
                            let err = self
                                .mark_task_failed(working, wrap_create_error(&task_name, err))
                                .await;
                            return Err(err);
                        }
                        Err(inspect_err) => {
                            if let Some(artifacts) = resolved.artifacts.take() {
                                if let Err(clean_err) = artifacts.cleanup().await {
                                    warn!(
                                        target: "task",
                                        "failed to cleanup staged secrets after inspect error for {}: {clean_err}",
                                        working.id
                                    );
                                }
                            }
                            let err = self
                                .mark_task_failed(
                                    working,
                                    wrap_existing_inspect_error(&task_name, inspect_err),
                                )
                                .await;
                            return Err(err);
                        }
                    }
                } else {
                    if let Some(artifacts) = resolved.artifacts.take() {
                        if let Err(clean_err) = artifacts.cleanup().await {
                            warn!(
                                target: "task",
                                "failed to cleanup staged secrets after create error for {}: {clean_err}",
                                working.id
                            );
                        }
                    }
                    let err = self
                        .mark_task_failed(working, wrap_create_error(&task_name, err))
                        .await;
                    return Err(err);
                }
            }
        };

        if let Some(artifacts) = resolved.artifacts.take() {
            let mut guard = self.secret_artifacts.lock().await;
            guard.insert(working.id, artifacts);
        }

        match self.container_manager.start_container(&container_id).await {
            Ok(_) => {}
            Err(err) => {
                if container_already_running(&err) {
                    debug!(
                        target: "task",
                        "container {} already running while starting task {}",
                        container_id,
                        working.id
                    );
                } else {
                    if created_fresh {
                        if let Err(remove_err) = self
                            .container_manager
                            .remove_container(&container_id, true, true)
                            .await
                        {
                            warn!(
                                target: "task",
                                "failed to remove container {} after start failure: {remove_err}",
                                container_id
                            );
                        }
                    }
                    let err = self
                        .mark_task_failed(working, wrap_start_error(&task_name, err))
                        .await;
                    return Err(err);
                }
            }
        }

        {
            let mut guard = self.local_containers.lock().await;
            guard.insert(working.id, container_id.clone());
        }

        if let Err(err) = self
            .ensure_runtime_attachments(
                working.id,
                &container_id,
                &working.networks,
                working.service_metadata.as_ref(),
            )
            .await
        {
            let err = err.context(format!(
                "failed to configure runtime network attachments for task {}",
                working.name
            ));
            if let Err(teardown_err) = self
                .teardown_runtime_attachments(working.id, HashSet::new())
                .await
            {
                warn!(
                    target: "task",
                    "failed to cleanup partial attachments for task {}: {teardown_err}",
                    working.id
                );
            }
            if let Err(stop_err) = self
                .container_manager
                .stop_container(&container_id, Some(Duration::from_secs(10)))
                .await
            {
                warn!(
                    target: "task",
                    "failed to stop container {} after attachment setup failure: {stop_err}",
                    container_id
                );
            }
            if let Err(remove_err) = self
                .container_manager
                .remove_container(&container_id, true, true)
                .await
            {
                warn!(
                    target: "task",
                    "failed to remove container {} after attachment setup failure: {remove_err}",
                    container_id
                );
            }
            let err = self.mark_task_failed(working, err).await;
            return Err(err);
        }

        working.state = ContainerState::Running;
        working.created_at = Utc::now().to_rfc3339();
        working.node_id = self.local_node_id;
        working.node_name = self.local_node_name.clone();

        if let Err(err) = self.persist_spec(&working).await {
            warn!(
                target: "task",
                "failed to persist running state for task {}: {err}",
                working.id
            );
            if let Err(stop_err) = self
                .container_manager
                .stop_container(&container_id, Some(Duration::from_secs(10)))
                .await
            {
                warn!(
                    target: "task",
                    "failed to stop container {} during rollback: {stop_err}",
                    container_id
                );
            }
            if let Err(remove_err) = self
                .container_manager
                .remove_container(&container_id, true, true)
                .await
            {
                warn!(
                    target: "task",
                    "failed to remove container {} during rollback: {remove_err}",
                    container_id
                );
            }
            let err = err.context("task state commit failed after container launch");
            let err = self.mark_task_failed(working, err).await;
            return Err(err);
        }

        if let Err(err) = self
            .enqueue_gossip(TaskEvent::Upsert(Box::new(working.clone())))
            .await
        {
            warn!(
                target: "task",
                "failed to enqueue task gossip for {}: {err}",
                working.name
            );
        }

        Ok(())
    }

    /// Resolves an existing container identifier when a create call hit a name conflict.
    pub(super) async fn resolve_existing_container_id(
        &self,
        container_name: &str,
    ) -> Result<Option<String>, ContainerError> {
        match self
            .container_manager
            .inspect_container(container_name)
            .await
        {
            Ok(info) => {
                let raw = info.id.unwrap_or_else(|| container_name.to_string());
                Ok(Some(raw.trim_start_matches('/').to_string()))
            }
            Err(ContainerError::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Ensures that a locally tracked task has completely stopped and released resources.
    pub(super) async fn ensure_task_stopped(&self, spec: TaskSpec) -> Result<(), anyhow::Error> {
        let mut has_container = {
            let guard = self.local_containers.lock().await;
            guard.contains_key(&spec.id)
        };

        if !has_container {
            // After a daemon restart the in-memory cache is empty, so inspect by name
            // before declaring the task containerless.
            let container_name = format!("mantissa-{}", spec.id);
            match self.container_manager.inspect_container(&container_name).await {
                Ok(info) => {
                    let resolved = info.id.unwrap_or(container_name);
                    let mut guard = self.local_containers.lock().await;
                    guard.insert(spec.id, resolved);
                    has_container = true;
                }
                Err(ContainerError::NotFound(_)) => {}
                Err(err) => {
                    warn!(
                        target: "task",
                        task = %spec.id,
                        "failed to inspect container while stopping task: {err}"
                    );
                }
            }
        }

        if !has_container {
            self.cleanup_secret_artifacts(spec.id).await;
            if let Err(err) = self
                .teardown_runtime_attachments(spec.id, HashSet::new())
                .await
            {
                warn!(
                    target: "task",
                    "failed to cleanup attachments for containerless task {}: {err}",
                    spec.id
                );
            }
            self.remove_spec(spec.id).await?;
            self.enqueue_gossip(TaskEvent::Remove { id: spec.id })
                .await?;
            if let Err(err) = self.cleanup_orphaned_local_attachments().await {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "failed to run orphaned attachment cleanup for containerless task: {err}"
                );
            }
            return Ok(());
        }

        let mut working = spec.clone();
        if matches!(working.state, ContainerState::Stopped) {
            // Force a stop pass even if the persisted state already says "stopped".
            working.state = ContainerState::Stopping;
        }
        let _ = self.perform_local_stop(working).await?;
        Ok(())
    }

    /// Reconciles the desired state of a locally owned task with the actual container state.
    pub(super) async fn reconcile_local_task(&self, spec: TaskSpec) -> Result<(), anyhow::Error> {
        match spec.state {
            ContainerState::Pending | ContainerState::Creating => {
                self.ensure_task_running(spec).await
            }
            ContainerState::Running => {
                self.ensure_local_tracking(&spec).await;
                Ok(())
            }
            ContainerState::Stopping | ContainerState::Stopped => {
                self.ensure_task_stopped(spec).await
            }
            ContainerState::Paused
            | ContainerState::Failed
            | ContainerState::Exited(_)
            | ContainerState::Unknown => {
                self.local_containers.lock().await.remove(&spec.id);
                Ok(())
            }
        }
    }

    /// Loads the current persisted spec for a task by identifier.
    pub(super) async fn load_spec(&self, id: Uuid) -> Result<TaskSpec, anyhow::Error> {
        let key = UuidKey::from(id);
        let snapshot = self
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow::anyhow!("task lookup failed: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("unknown task {id}"))?;

        let value = snapshot
            .as_slice()
            .last()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("task {id} has no value"))?;

        Ok(value_to_spec(id, value))
    }
}
