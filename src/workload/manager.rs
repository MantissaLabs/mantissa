use crate::gossip::Message;
use crate::scheduler::{
    Scheduler, SchedulerError, SlotCapacity, SlotId, SlotReservationRequest, SlotState,
};
use crate::store::workload_store::WorkloadStore;
use crate::workload::container::ContainerState;
use crate::workload::docker::ContainerManager;
use crate::workload::types::{WorkloadEvent, WorkloadSpec, WorkloadValue};
use anyhow::Context;
use async_channel::{Receiver, Sender};
use chrono::{DateTime, Utc};
use crdt_store::uuid_key::UuidKey;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;
use uuid::Uuid;

#[derive(Clone)]
pub struct WorkloadManager {
    store: WorkloadStore,
    tx: Sender<Message>,
    rx: Receiver<Message>,
    seen_ids: Arc<AsyncMutex<HashSet<Uuid>>>,
    local_node_id: Uuid,
    local_node_name: String,
    scheduler: Rc<Scheduler>,
    container_manager: Arc<dyn ContainerManager + Send + Sync>,
    local_containers: Arc<AsyncMutex<HashMap<Uuid, String>>>,
}

#[derive(Clone)]
pub struct ContainerStartRequest {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
}

struct BatchStartPlan {
    id: Uuid,
    name: String,
    image: String,
    command: Vec<String>,
    cpu_millis: u64,
    memory_bytes: u64,
    slot_id: SlotId,
    slot_capacity: SlotCapacity,
    container_name: String,
    container_id: Option<String>,
    created_at: DateTime<Utc>,
}

impl WorkloadManager {
    pub fn new(
        store: WorkloadStore,
        tx: Sender<Message>,
        rx: Receiver<Message>,
        local_node_id: Uuid,
        local_node_name: impl Into<String>,
        scheduler: Rc<Scheduler>,
        container_manager: Arc<dyn ContainerManager + Send + Sync>,
    ) -> Self {
        Self {
            store,
            tx,
            rx,
            seen_ids: Arc::new(AsyncMutex::new(HashSet::new())),
            local_node_id,
            local_node_name: local_node_name.into(),
            scheduler,
            container_manager,
            local_containers: Arc::new(AsyncMutex::new(HashMap::new())),
        }
    }

    async fn reserve_slot(
        &self,
        workload_id: Uuid,
        cpu_millis: u64,
        memory_bytes: u64,
    ) -> Result<(SlotId, SlotCapacity), anyhow::Error> {
        const MAX_ATTEMPTS: usize = 10;

        for _ in 0..MAX_ATTEMPTS {
            let snapshot = self
                .scheduler
                .snapshot()
                .await
                .ok_or_else(|| anyhow::anyhow!("scheduler snapshot unavailable"))?;

            let Some(slot) = snapshot.slots.iter().find(|slot| {
                matches!(slot.state, SlotState::Free)
                    && slot.capacity.cpu_millis >= cpu_millis
                    && slot.capacity.memory_bytes >= memory_bytes
            }) else {
                return Err(anyhow::anyhow!(
                    "no scheduler slot satisfies cpu={cpu_millis} memory={memory_bytes}"
                ));
            };

            let request = SlotReservationRequest {
                slot_id: slot.slot_id,
                owner: self.local_node_id,
                workload_id: Some(workload_id),
            };

            match self
                .scheduler
                .reserve_slots(snapshot.version, vec![request])
                .await
            {
                Ok(_) => return Ok((slot.slot_id, slot.capacity)),
                Err(SchedulerError::SnapshotMismatch { .. })
                | Err(SchedulerError::SlotsUnavailable { .. }) => continue,
                Err(err) => return Err(anyhow::anyhow!(err)),
            }
        }

        Err(anyhow::anyhow!(
            "failed to reserve scheduler slot after retries"
        ))
    }

    async fn release_slot(&self, slot_id: SlotId) -> Result<(), anyhow::Error> {
        const MAX_ATTEMPTS: usize = 10;

        for _ in 0..MAX_ATTEMPTS {
            let snapshot = match self.scheduler.snapshot().await {
                Some(s) => s,
                None => return Err(anyhow::anyhow!("scheduler snapshot unavailable")),
            };

            match self.scheduler.free_slots(snapshot.version, [slot_id]).await {
                Ok(_) => return Ok(()),
                Err(SchedulerError::SnapshotMismatch { .. }) => continue,
                Err(SchedulerError::UnknownSlots { .. })
                | Err(SchedulerError::SlotsNotReserved { .. }) => return Ok(()),
                Err(err) => return Err(anyhow::anyhow!(err)),
            }
        }

        Err(anyhow::anyhow!(
            "failed to free scheduler slot after retries"
        ))
    }

    pub async fn start_container(
        &self,
        name: impl Into<String>,
        image: impl Into<String>,
        command: Vec<String>,
        cpu_millis: u64,
        memory_bytes: u64,
    ) -> Result<WorkloadSpec, anyhow::Error> {
        let request = ContainerStartRequest {
            name: name.into(),
            image: image.into(),
            command,
            cpu_millis,
            memory_bytes,
        };

        let mut specs = self.start_containers_batch(vec![request]).await?;
        Ok(specs
            .pop()
            .expect("batch start with single request should yield one spec"))
    }

    pub async fn start_containers_batch(
        &self,
        requests: Vec<ContainerStartRequest>,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut plans: Vec<BatchStartPlan> = requests
            .into_iter()
            .map(|request| BatchStartPlan {
                id: Uuid::new_v4(),
                name: request.name,
                image: request.image,
                command: request.command,
                cpu_millis: request.cpu_millis,
                memory_bytes: request.memory_bytes,
                slot_id: 0,
                slot_capacity: SlotCapacity::new(0, 0),
                container_name: String::new(),
                container_id: None,
                created_at: Utc::now(),
            })
            .collect();

        let intents: Vec<(Uuid, u64, u64)> = plans
            .iter()
            .map(|plan| (plan.id, plan.cpu_millis, plan.memory_bytes))
            .collect();

        let allocations = match self.reserve_slots_for_workloads(&intents).await {
            Ok(slots) => slots,
            Err(err) => return Err(err.context("scheduler reservation failed")),
        };

        for (plan, (slot_id, slot_capacity)) in plans.iter_mut().zip(allocations.into_iter()) {
            plan.slot_id = slot_id;
            plan.slot_capacity = slot_capacity;
            plan.container_name = format!("mantissa-{}", plan.id);
        }

        if let Err(err) = self.launch_batch_containers(&mut plans).await {
            self.cleanup_batch(&plans).await;
            return Err(err);
        }

        match self.commit_batch(&plans).await {
            Ok(specs) => Ok(specs),
            Err(err) => {
                self.cleanup_batch(&plans).await;
                Err(err)
            }
        }
    }

    async fn reserve_slots_for_workloads(
        &self,
        intents: &[(Uuid, u64, u64)],
    ) -> Result<Vec<(SlotId, SlotCapacity)>, anyhow::Error> {
        if intents.is_empty() {
            return Ok(Vec::new());
        }

        const MAX_ATTEMPTS: usize = 10;
        for _ in 0..MAX_ATTEMPTS {
            let snapshot = self
                .scheduler
                .snapshot()
                .await
                .ok_or_else(|| anyhow::anyhow!("scheduler snapshot unavailable"))?;

            let mut chosen = Vec::with_capacity(intents.len());
            let mut used = HashSet::with_capacity(intents.len());

            let mut insufficient = false;
            for (_, cpu_millis, memory_bytes) in intents.iter() {
                let mut candidate = None;
                for slot in snapshot.slots.iter() {
                    if matches!(slot.state, SlotState::Free)
                        && slot.capacity.cpu_millis >= *cpu_millis
                        && slot.capacity.memory_bytes >= *memory_bytes
                        && !used.contains(&slot.slot_id)
                    {
                        candidate = Some((slot.slot_id, slot.capacity));
                        break;
                    }
                }

                if let Some((slot_id, capacity)) = candidate {
                    used.insert(slot_id);
                    chosen.push((slot_id, capacity));
                } else {
                    insufficient = true;
                    break;
                }
            }

            if insufficient {
                return Err(anyhow::anyhow!(
                    "scheduler reservation failed: insufficient capacity for batch"
                ));
            }

            let requests: Vec<_> = intents
                .iter()
                .zip(chosen.iter())
                .map(
                    |((workload_id, _, _), (slot_id, _))| SlotReservationRequest {
                        slot_id: *slot_id,
                        owner: self.local_node_id,
                        workload_id: Some(*workload_id),
                    },
                )
                .collect();

            match self
                .scheduler
                .reserve_slots(snapshot.version, requests)
                .await
            {
                Ok(_) => return Ok(chosen),
                Err(SchedulerError::SnapshotMismatch { .. })
                | Err(SchedulerError::SlotsUnavailable { .. })
                | Err(SchedulerError::UnknownSlots { .. }) => continue,
                Err(err) => return Err(anyhow::anyhow!(err)),
            }
        }

        Err(anyhow::anyhow!(
            "failed to reserve scheduler slots for batch after retries"
        ))
    }

    async fn launch_batch_containers(
        &self,
        plans: &mut [BatchStartPlan],
    ) -> Result<(), anyhow::Error> {
        for plan in plans.iter_mut() {
            self.container_manager
                .pull_image(&plan.image)
                .await
                .with_context(|| format!("docker pull failed for image {}", plan.image))?;

            let container_id = self
                .container_manager
                .create_container(
                    &plan.container_name,
                    &plan.image,
                    if plan.command.is_empty() {
                        None
                    } else {
                        Some(plan.command.clone())
                    },
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .with_context(|| format!("docker create failed for workload {}", plan.name))?;

            plan.container_id = Some(container_id.clone());

            self.container_manager
                .start_container(&container_id)
                .await
                .with_context(|| format!("docker start failed for workload {}", plan.name))?;

            plan.created_at = Utc::now();
        }

        Ok(())
    }

    async fn commit_batch(
        &self,
        plans: &[BatchStartPlan],
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        let mut specs = Vec::with_capacity(plans.len());
        let mut persisted: Vec<WorkloadSpec> = Vec::new();

        for plan in plans {
            let spec = WorkloadSpec {
                id: plan.id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                state: ContainerState::Running,
                created_at: plan.created_at.to_rfc3339(),
                command: plan.command.clone(),
                node_id: self.local_node_id,
                node_name: self.local_node_name.clone(),
                slot_id: Some(plan.slot_id),
                cpu_millis: plan.slot_capacity.cpu_millis,
                memory_bytes: plan.slot_capacity.memory_bytes,
            };

            if let Err(err) = self.persist_spec(&spec).await {
                for rollback in &persisted {
                    let _ = self.remove_spec(rollback.id).await;
                }
                return Err(err.context(format!("failed to persist workload spec {}", spec.name)));
            }

            persisted.push(spec.clone());
            specs.push(spec);
        }

        let mut gossiped: Vec<WorkloadSpec> = Vec::new();
        for spec in &specs {
            if let Err(err) = self
                .enqueue_gossip(WorkloadEvent::Upsert(spec.clone()))
                .await
            {
                for rollback in &gossiped {
                    let _ = self
                        .enqueue_gossip(WorkloadEvent::Remove { id: rollback.id })
                        .await;
                }
                for rollback in &persisted {
                    let _ = self.remove_spec(rollback.id).await;
                }
                return Err(err.context(format!("failed to broadcast workload spec {}", spec.name)));
            }

            gossiped.push(spec.clone());
        }

        {
            let mut guard = self.local_containers.lock().await;
            for plan in plans {
                if let Some(container_id) = plan.container_id.as_ref() {
                    guard.insert(plan.id, container_id.clone());
                }
            }
        }

        Ok(specs)
    }

    async fn cleanup_batch(&self, plans: &[BatchStartPlan]) {
        for plan in plans {
            if let Some(container_id) = plan.container_id.as_ref() {
                if let Err(err) = self
                    .container_manager
                    .stop_container(container_id, Some(Duration::from_secs(10)))
                    .await
                {
                    warn!(
                        target: "workload",
                        "failed to stop container {container_id} for workload {}: {err}",
                        plan.id
                    );
                }

                if let Err(err) = self
                    .container_manager
                    .remove_container(container_id, true, true)
                    .await
                {
                    warn!(
                        target: "workload",
                        "failed to remove container {container_id} for workload {}: {err}",
                        plan.id
                    );
                }

                let mut guard = self.local_containers.lock().await;
                guard.remove(&plan.id);
            }

            if plan.slot_id != 0 {
                if let Err(err) = self.release_slot(plan.slot_id).await {
                    warn!(
                        target: "workload",
                        "failed to release slot {} during rollback: {err}",
                        plan.slot_id
                    );
                }
            }
        }
    }

    pub async fn list_containers(&self) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        let (actives, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("workload store load_all failed: {e}"))?;

        let mut specs = Vec::with_capacity(actives.len());
        for (k, snap) in actives {
            let id = k.to_uuid();
            if let Some(value) = snap.as_slice().last() {
                specs.push(value_to_spec(id, value.clone()));
            }
        }
        Ok(specs)
    }

    async fn persist_spec(&self, spec: &WorkloadSpec) -> Result<(), anyhow::Error> {
        let value = WorkloadValue::new(
            spec.id,
            spec.name.clone(),
            spec.image.clone(),
            spec.state.clone(),
            spec.created_at.clone(),
            spec.command.clone(),
            spec.node_id,
            spec.node_name.clone(),
            spec.slot_id,
            spec.cpu_millis,
            spec.memory_bytes,
        );

        self.store
            .upsert(&UuidKey::from(spec.id), value)
            .await
            .map_err(|e| anyhow::anyhow!("workload upsert failed: {e}"))
    }

    async fn remove_spec(&self, id: Uuid) -> Result<(), anyhow::Error> {
        self.store
            .remove(&UuidKey::from(id))
            .await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("workload remove failed: {e}"))
    }

    fn tx(&self) -> Sender<Message> {
        self.tx.clone()
    }

    async fn enqueue_gossip(&self, event: WorkloadEvent) -> Result<(), anyhow::Error> {
        let id = Uuid::new_v4();
        let message = Message::Workload { id, event };
        self.tx()
            .send(message)
            .await
            .map_err(|e| anyhow::anyhow!("failed to enqueue workload gossip: {e}"))
    }

    async fn load_spec(&self, id: Uuid) -> Result<WorkloadSpec, anyhow::Error> {
        let key = UuidKey::from(id);
        let snapshot = self
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow::anyhow!("workload lookup failed: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("unknown workload {id}"))?;

        let value = snapshot
            .as_slice()
            .last()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("workload {id} has no value"))?;

        Ok(value_to_spec(id, value))
    }

    pub async fn stop_workload(&self, id: Uuid) -> Result<WorkloadSpec, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        let node_name = spec.node_name.clone();

        if spec.node_id != self.local_node_id {
            return Err(anyhow::anyhow!(
                "workload {id} is assigned to node {node_name}",
            ));
        }

        if let Some(container_id) = self.local_containers.lock().await.remove(&id) {
            self.container_manager
                .stop_container(&container_id, Some(Duration::from_secs(10)))
                .await
                .map_err(|e| anyhow::anyhow!("docker stop failed: {e}"))?;

            if let Err(e) = self
                .container_manager
                .remove_container(&container_id, false, true)
                .await
            {
                tracing::warn!(
                    target: "workload",
                    "failed to remove container {container_id}: {e}"
                );
            }
        }

        let mut updated = spec.clone();
        updated.state = ContainerState::Stopped;
        if let Some(slot_id) = spec.slot_id {
            self.release_slot(slot_id)
                .await
                .with_context(|| "scheduler release failed during stop".to_string())?;
            updated.slot_id = None;
            updated.cpu_millis = 0;
            updated.memory_bytes = 0;
        }

        self.persist_spec(&updated).await?;
        self.enqueue_gossip(WorkloadEvent::Upsert(updated.clone()))
            .await?;
        Ok(updated)
    }

    async fn record_gossip_id(&self, id: Uuid) -> bool {
        let mut guard = self.seen_ids.lock().await;
        guard.insert(id)
    }

    pub async fn run(&mut self) {
        while let Ok(message) = self.rx.recv().await {
            match message {
                Message::Workload { id, event } => {
                    if !self.record_gossip_id(id).await {
                        continue;
                    }
                    if let Err(e) = self.handle_event(event).await {
                        tracing::error!(target: "workload", "failed to handle workload event: {e}");
                    }
                }
                Message::Void { .. } => {}
                _ => {}
            }
        }
    }

    async fn handle_event(&self, event: WorkloadEvent) -> Result<(), anyhow::Error> {
        match event {
            WorkloadEvent::Upsert(spec) => {
                if spec.node_id == self.local_node_id && spec.state != ContainerState::Running {
                    self.local_containers.lock().await.remove(&spec.id);
                }
                self.persist_spec(&spec).await
            }
            WorkloadEvent::Remove { id } => self.remove_spec(id).await,
        }
    }
}

fn value_to_spec(id: Uuid, value: WorkloadValue) -> WorkloadSpec {
    WorkloadSpec {
        id,
        name: value.name,
        image: value.image,
        state: value.state,
        created_at: value.created_at,
        command: value.command,
        node_id: value.node_id,
        node_name: value.node_name,
        slot_id: value.slot_id,
        cpu_millis: value.cpu_millis,
        memory_bytes: value.memory_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::registry::Registry;
    use crate::scheduler::SlotSpec;
    use crate::store::local_session_store::LocalSessionStore;
    use crate::store::peer_store::open_peers_store;
    use crate::store::scheduler_store::open_scheduler_store;
    use crate::store::workload_store::open_workload_store;
    use ::health::{Config as HealthConfig, HealthMonitor};
    use async_channel::bounded;
    use async_trait::async_trait;
    use ed25519_dalek::SigningKey;
    use net::noise::NoiseKeys;
    use std::collections::HashMap;
    use std::rc::Rc;
    use tempfile::tempdir;

    #[derive(Clone, Default)]
    struct MockContainerManager {
        created: Arc<AsyncMutex<Vec<String>>>,
        stopped: Arc<AsyncMutex<Vec<String>>>,
    }

    #[async_trait]
    impl ContainerManager for MockContainerManager {
        async fn create_container(
            &self,
            _name: &str,
            _image: &str,
            _command: Option<Vec<String>>,
            _env_vars: Option<Vec<String>>,
            _ports: Option<HashMap<String, Vec<HashMap<String, String>>>>,
            _volumes: Option<Vec<String>>,
            _restart_policy: Option<crate::workload::docker::RestartPolicyConfig>,
        ) -> crate::workload::docker::ContainerResult<String> {
            let mut guard = self.created.lock().await;
            let id = format!("container-{}", guard.len());
            guard.push(id.clone());
            Ok(id)
        }

        async fn start_container(
            &self,
            _container_id: &str,
        ) -> crate::workload::docker::ContainerResult<()> {
            Ok(())
        }

        async fn stop_container(
            &self,
            container_id: &str,
            _timeout: Option<std::time::Duration>,
        ) -> crate::workload::docker::ContainerResult<()> {
            self.stopped.lock().await.push(container_id.to_string());
            Ok(())
        }

        async fn restart_container(
            &self,
            _container_id: &str,
            _timeout: Option<std::time::Duration>,
        ) -> crate::workload::docker::ContainerResult<()> {
            Ok(())
        }

        async fn remove_container(
            &self,
            _container_id: &str,
            _force: bool,
            _remove_volumes: bool,
        ) -> crate::workload::docker::ContainerResult<()> {
            Ok(())
        }

        async fn list_containers(
            &self,
            _filters: Option<HashMap<String, Vec<String>>>,
        ) -> crate::workload::docker::ContainerResult<Vec<crate::workload::docker::ContainerInfo>>
        {
            Ok(Vec::new())
        }

        async fn inspect_container(
            &self,
            _container_id: &str,
        ) -> crate::workload::docker::ContainerResult<bollard::service::ContainerInspectResponse>
        {
            Err(crate::workload::docker::ContainerError::OperationFailed(
                "inspect unsupported in mock".into(),
            ))
        }

        async fn pull_image(&self, _image: &str) -> crate::workload::docker::ContainerResult<()> {
            Ok(())
        }
    }

    fn temp_db(prefix: &str) -> (Arc<redb::Database>, tempfile::TempDir) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join(format!("{prefix}-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(path).expect("create db"));
        (db, dir)
    }

    async fn setup_manager() -> (WorkloadManager, Rc<Scheduler>, Arc<MockContainerManager>) {
        let actor = Uuid::new_v4();
        let (scheduler_db, _dir) = temp_db("scheduler");
        let scheduler_store =
            open_scheduler_store(scheduler_db.clone(), actor).expect("open scheduler store");
        scheduler_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild scheduler store");

        let (registry_db, _reg_dir) = temp_db("registry");
        let peers_store = open_peers_store(registry_db.clone(), actor).expect("open peers store");
        peers_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild peers store");

        let noise_keys = NoiseKeys::from_private_bytes([0x11; 32]);
        let session_store =
            LocalSessionStore::open(registry_db.clone(), &noise_keys).expect("open sessions");

        let health_monitor = HealthMonitor::new(HealthConfig::default());

        let registry = Registry::new(
            peers_store,
            session_store,
            SigningKey::from_bytes(&[0xA5; 32]),
            actor,
            health_monitor,
        );

        let scheduler =
            Rc::new(Scheduler::new(scheduler_store, registry, actor).expect("scheduler init"));
        scheduler
            .init_slots([
                SlotSpec::new(0, SlotCapacity::new(1_000, 1_024 * 1_024 * 1_024)),
                SlotSpec::new(1, SlotCapacity::new(500, 512 * 1_024 * 1_024)),
            ])
            .await
            .expect("init slots");

        let (workload_db, _wd) = temp_db("workload");
        let workload_store = open_workload_store(workload_db, actor).expect("open workload store");

        let mock_manager = Arc::new(MockContainerManager::default());
        let (tx, rx) = bounded(4);
        let container_manager: Arc<dyn ContainerManager + Send + Sync> = mock_manager.clone();
        let manager = WorkloadManager::new(
            workload_store,
            tx,
            rx,
            actor,
            "local-node",
            scheduler.clone(),
            container_manager,
        );

        (manager, scheduler, mock_manager)
    }

    #[tokio::test]
    async fn start_container_reserves_slot_and_records_resources() {
        let (manager, scheduler, _cm) = setup_manager().await;

        let spec = manager
            .start_container(
                "svc",
                "image",
                vec!["--arg".into()],
                500,
                256 * 1_024 * 1_024,
            )
            .await
            .expect("start container");

        assert_eq!(spec.cpu_millis, 1_000);
        assert_eq!(spec.memory_bytes, 1_024 * 1_024 * 1_024);
        let slot_id = spec.slot_id.expect("slot assigned");

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        let slot = snapshot
            .slots
            .iter()
            .find(|s| s.slot_id == slot_id)
            .expect("slot exists");
        assert!(matches!(slot.state, SlotState::Reserved(_)));
    }

    #[tokio::test]
    async fn stop_workload_releases_slot_and_clears_resources() {
        let (manager, scheduler, _cm) = setup_manager().await;

        let spec = manager
            .start_container("svc", "image", vec![], 500, 256 * 1_024 * 1_024)
            .await
            .expect("start container");

        let slot_id = spec.slot_id.expect("slot assigned");
        let stopped = manager.stop_workload(spec.id).await.expect("stop workload");

        assert!(stopped.slot_id.is_none());
        assert_eq!(stopped.cpu_millis, 0);
        assert_eq!(stopped.memory_bytes, 0);

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        let slot = snapshot
            .slots
            .iter()
            .find(|s| s.slot_id == slot_id)
            .expect("slot exists");
        assert!(matches!(slot.state, SlotState::Free));
    }

    #[tokio::test]
    async fn start_container_fails_when_no_matching_slot() {
        let (manager, _scheduler, _cm) = setup_manager().await;

        let err = manager
            .start_container("svc", "image", vec![], 2_000, 512 * 1_024 * 1_024)
            .await
            .expect_err("reservation should fail");
        assert!(err.to_string().contains("scheduler reservation failed"));
    }

    #[tokio::test]
    async fn start_containers_batch_reserves_every_slot() {
        let (manager, scheduler, mock_cm) = setup_manager().await;

        let specs = manager
            .start_containers_batch(vec![
                ContainerStartRequest {
                    name: "svc-a".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 400,
                    memory_bytes: 128 * 1_024 * 1_024,
                },
                ContainerStartRequest {
                    name: "svc-b".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 200,
                    memory_bytes: 64 * 1_024 * 1_024,
                },
            ])
            .await
            .expect("batch start");

        assert_eq!(specs.len(), 2);

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        let reserved = snapshot
            .slots
            .iter()
            .filter(|slot| matches!(slot.state, SlotState::Reserved(_)))
            .count();
        assert_eq!(reserved, 2);

        let created = mock_cm.created.lock().await.clone();
        assert_eq!(created.len(), 2);
    }

    #[tokio::test]
    async fn start_containers_batch_is_atomic_on_capacity_failure() {
        let (manager, scheduler, mock_cm) = setup_manager().await;

        manager
            .start_container("baseline", "img", vec![], 400, 128 * 1_024 * 1_024)
            .await
            .expect("pre-existing container");

        let created_before = mock_cm.created.lock().await.len();

        let err = manager
            .start_containers_batch(vec![
                ContainerStartRequest {
                    name: "svc-c".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 200,
                    memory_bytes: 64 * 1_024 * 1_024,
                },
                ContainerStartRequest {
                    name: "svc-d".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 200,
                    memory_bytes: 64 * 1_024 * 1_024,
                },
            ])
            .await
            .expect_err("batch should fail when capacity is insufficient");

        assert!(err.to_string().contains("scheduler reservation failed"));

        let created_after = mock_cm.created.lock().await.len();
        assert_eq!(created_before, created_after);

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        let reserved = snapshot
            .slots
            .iter()
            .filter(|slot| matches!(slot.state, SlotState::Reserved(_)))
            .count();
        assert_eq!(reserved, 1);
    }
}
