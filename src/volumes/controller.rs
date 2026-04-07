use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use tokio::time::interval;
use tracing::warn;
use uuid::Uuid;

use crate::gossip::Message;

use super::local::ensure_local_volume_path;
use super::registry::VolumeRegistry;
use super::types::{
    VolumeDriver, VolumeEvent, VolumeNodeState, VolumeNodeStateValue, VolumeSpecValue, VolumeStatus,
};
use async_channel::Sender;

const VOLUME_RECONCILE_TICK_SECS: u64 = 2;

/// Reconciles local-driver volume realization on the node that owns the bound path.
#[derive(Clone)]
pub struct VolumeController {
    registry: VolumeRegistry,
    gossip_tx: Sender<Message>,
    local_node_id: Uuid,
    local_node_name: String,
    local_volume_root: PathBuf,
    enforce_capacity_limits: bool,
}

impl VolumeController {
    /// Builds one local volume controller bound to the node-local filesystem root.
    pub fn new(
        registry: VolumeRegistry,
        gossip_tx: Sender<Message>,
        local_node_id: Uuid,
        local_node_name: impl Into<String>,
        local_volume_root: PathBuf,
        enforce_capacity_limits: bool,
    ) -> Self {
        Self {
            registry,
            gossip_tx,
            local_node_id,
            local_node_name: local_node_name.into(),
            local_volume_root,
            enforce_capacity_limits,
        }
    }

    /// Runs the local volume reconciliation loop so bound local paths stay realized across restarts.
    pub async fn run(&self) {
        let mut tick = interval(Duration::from_secs(VOLUME_RECONCILE_TICK_SECS));
        loop {
            tick.tick().await;
            if let Err(err) = self.reconcile_local_volumes().await {
                warn!(target: "volumes", "failed to reconcile local volumes: {err:#}");
            }
        }
    }

    /// Ensures every local-driver volume bound to this node has a realized node-state row.
    pub async fn reconcile_local_volumes(&self) -> Result<()> {
        let specs = self.registry.list_specs()?;
        for spec in specs {
            if spec.bound_node_id != Some(self.local_node_id) {
                continue;
            }
            if !matches!(spec.driver, VolumeDriver::Local(_)) {
                continue;
            }
            self.reconcile_one_local_volume(spec).await?;
        }
        Ok(())
    }

    /// Materializes one local-driver volume and reports readiness or error through the node-state row.
    async fn reconcile_one_local_volume(&self, mut spec: VolumeSpecValue) -> Result<()> {
        let current = self
            .registry
            .get_node_state(spec.id, self.local_node_id)?
            .unwrap_or_else(|| {
                VolumeNodeStateValue::new(
                    spec.id,
                    self.local_node_id,
                    self.local_node_name.clone(),
                    None,
                    VolumeNodeState::Pending,
                    spec.requested_bytes,
                )
            });

        match ensure_local_volume_path(&self.local_volume_root, &spec)
            .and_then(|path| measure_local_volume_usage(&path).map(|used_bytes| (path, used_bytes)))
        {
            Ok((path, used_bytes)) => {
                let local_path = path.to_string_lossy().to_string();
                let mut desired = current.clone();
                desired.local_path = Some(local_path);
                desired.capacity_bytes = spec.requested_bytes;
                desired.used_bytes = Some(used_bytes);

                let over_capacity = self
                    .enforce_capacity_limits
                    .then_some((used_bytes, spec.requested_bytes))
                    .and_then(|(used_bytes, capacity)| {
                        capacity.map(|capacity| (used_bytes, capacity))
                    })
                    .filter(|(used_bytes, capacity)| used_bytes > capacity);

                if let Some((used_bytes, capacity)) = over_capacity {
                    let message = capacity_exceeded_message(&spec.name, used_bytes, capacity);
                    desired.state = VolumeNodeState::Error;
                    desired.last_error = Some(message.clone());
                    desired.updated_at = Utc::now().to_rfc3339();
                    self.upsert_node_state_if_changed(&desired, &current)
                        .await?;

                    if spec.status != VolumeStatus::Failed
                        || spec.reason.as_deref() != Some("capacity_exceeded")
                        || spec.message.as_deref() != Some(message.as_str())
                    {
                        spec.status = VolumeStatus::Failed;
                        spec.reason = Some("capacity_exceeded".to_string());
                        spec.message = Some(message);
                        spec.updated_at = Utc::now().to_rfc3339();
                        self.upsert_spec(spec).await?;
                    }
                    return Ok(());
                }

                desired.state = if desired.published_task_ids.is_empty() {
                    VolumeNodeState::Ready
                } else {
                    VolumeNodeState::Published
                };
                desired.last_error = None;
                desired.updated_at = Utc::now().to_rfc3339();
                self.upsert_node_state_if_changed(&desired, &current)
                    .await?;

                if matches!(
                    spec.status,
                    VolumeStatus::Pending | VolumeStatus::Bound | VolumeStatus::Failed
                ) {
                    spec.status = VolumeStatus::Ready;
                    spec.reason = None;
                    spec.message = Some("local volume realized".to_string());
                    spec.updated_at = Utc::now().to_rfc3339();
                    self.upsert_spec(spec).await?;
                }
            }
            Err(err) => {
                let mut desired = current;
                desired.state = VolumeNodeState::Error;
                desired.last_error = Some(err.to_string());
                desired.updated_at = Utc::now().to_rfc3339();
                self.upsert_node_state(desired).await?;

                if spec.status != VolumeStatus::Failed
                    || spec.reason.as_deref() != Some("local_realization_failed")
                {
                    spec.status = VolumeStatus::Failed;
                    spec.reason = Some("local_realization_failed".to_string());
                    spec.message = Some(err.to_string());
                    spec.updated_at = Utc::now().to_rfc3339();
                    self.upsert_spec(spec).await?;
                }
            }
        }

        Ok(())
    }

    /// Stores and broadcasts one canonical volume spec update.
    async fn upsert_spec(&self, spec: VolumeSpecValue) -> Result<()> {
        self.registry.upsert_spec(spec.clone()).await?;
        self.gossip_tx
            .send(Message::Volume {
                id: Uuid::new_v4(),
                event: VolumeEvent::Upsert(Box::new(spec)),
            })
            .await
            .map_err(|err| anyhow::anyhow!("failed to enqueue volume spec gossip: {err}"))?;
        Ok(())
    }

    /// Stores and broadcasts one node-state update for a local volume realization.
    async fn upsert_node_state(&self, state: VolumeNodeStateValue) -> Result<()> {
        self.registry.upsert_node_state(state.clone()).await?;
        self.gossip_tx
            .send(Message::Volume {
                id: Uuid::new_v4(),
                event: VolumeEvent::NodeUpsert(Box::new(state)),
            })
            .await
            .map_err(|err| anyhow::anyhow!("failed to enqueue volume node-state gossip: {err}"))?;
        Ok(())
    }

    /// Avoids unnecessary gossip churn when the canonical node-state row is already current.
    async fn upsert_node_state_if_changed(
        &self,
        desired: &VolumeNodeStateValue,
        current: &VolumeNodeStateValue,
    ) -> Result<()> {
        if desired.local_path == current.local_path
            && desired.state == current.state
            && desired.capacity_bytes == current.capacity_bytes
            && desired.used_bytes == current.used_bytes
            && desired.published_task_ids == current.published_task_ids
            && desired.last_error == current.last_error
        {
            return Ok(());
        }

        self.upsert_node_state(desired.clone()).await
    }
}

/// Recursively measures the bytes currently stored below one realized local volume path.
fn measure_local_volume_usage(path: &Path) -> Result<u64> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        total = total.saturating_add(measure_local_volume_usage(&entry.path())?);
    }
    Ok(total)
}

/// Formats the operator-facing capacity exceeded message stored on the node-state row.
fn capacity_exceeded_message(volume_name: &str, used_bytes: u64, capacity_bytes: u64) -> String {
    format!(
        "volume '{}' exceeded requested capacity: used {} bytes, limit {} bytes",
        volume_name, used_bytes, capacity_bytes
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::volume_store::{open_volume_node_store, open_volume_spec_store};
    use crate::volumes::types::{
        LocalVolumeOwnership, LocalVolumeSource, LocalVolumeSpec, VolumeAccessMode,
        VolumeBindingMode, VolumeDriver, VolumeReclaimPolicy, VolumeSpecDraft, VolumeStatus,
    };
    use async_channel::bounded;
    use std::sync::Arc;

    struct TestRegistry {
        registry: VolumeRegistry,
        _dir: tempfile::TempDir,
    }

    /// Builds one temporary registry backing store for controller tests.
    async fn make_test_registry() -> TestRegistry {
        let dir = tempfile::tempdir().expect("create tempdir");
        let db_path = dir.path().join("volumes.redb");
        let db = Arc::new(redb::Database::create(db_path).expect("create volume db"));
        let actor = Uuid::new_v4();
        let spec_store = open_volume_spec_store(db.clone(), actor).expect("open volume spec store");
        spec_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume spec store");
        let node_store = open_volume_node_store(db, actor).expect("open volume node store");
        node_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume node store");
        TestRegistry {
            registry: VolumeRegistry::new(spec_store, node_store),
            _dir: dir,
        }
    }

    /// Persists one managed local volume bound to the controller node.
    async fn persist_bound_volume(
        registry: &VolumeRegistry,
        node_id: Uuid,
        name: &str,
        requested_bytes: Option<u64>,
    ) -> VolumeSpecValue {
        let spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: name.to_string(),
            driver: VolumeDriver::Local(LocalVolumeSpec {
                source: LocalVolumeSource::Managed,
                ownership: LocalVolumeOwnership::Daemon,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::Immediate,
            reclaim_policy: VolumeReclaimPolicy::Retain,
            requested_bytes,
            labels: Vec::new(),
            bound_node_id: Some(node_id),
            bound_node_name: Some("node-a".to_string()),
        });
        registry
            .upsert_spec(spec.clone())
            .await
            .expect("persist volume spec");
        spec
    }

    /// Reconcile must report the bytes currently stored under the realized volume path.
    #[tokio::test(flavor = "current_thread")]
    async fn reconcile_local_volumes_reports_used_bytes() {
        let test_registry = make_test_registry().await;
        let node_id = Uuid::new_v4();
        let local_volume_root = tempfile::tempdir().expect("create local volume root");
        let spec = persist_bound_volume(&test_registry.registry, node_id, "pgdata", None).await;
        let data_path =
            super::super::local::managed_volume_data_path(local_volume_root.path(), spec.id);
        fs::create_dir_all(&data_path).expect("create data path");
        fs::write(data_path.join("pg.bin"), vec![1u8; 5]).expect("write volume data");

        let (tx, _rx) = bounded(8);
        let controller = VolumeController::new(
            test_registry.registry.clone(),
            tx,
            node_id,
            "node-a",
            local_volume_root.path().to_path_buf(),
            false,
        );
        controller
            .reconcile_local_volumes()
            .await
            .expect("reconcile local volumes");

        let state = test_registry
            .registry
            .get_node_state(spec.id, node_id)
            .expect("load node state")
            .expect("node state present");
        assert_eq!(state.used_bytes, Some(5));
        assert!(matches!(state.state, VolumeNodeState::Ready));
    }

    /// Capacity enforcement must flip the node-state into `Error` once usage exceeds the limit.
    #[tokio::test(flavor = "current_thread")]
    async fn reconcile_local_volumes_marks_capacity_exceeded_when_enforced() {
        let test_registry = make_test_registry().await;
        let node_id = Uuid::new_v4();
        let local_volume_root = tempfile::tempdir().expect("create local volume root");
        let spec = persist_bound_volume(&test_registry.registry, node_id, "quota", Some(4)).await;
        let data_path =
            super::super::local::managed_volume_data_path(local_volume_root.path(), spec.id);
        fs::create_dir_all(&data_path).expect("create data path");
        fs::write(data_path.join("pg.bin"), vec![1u8; 5]).expect("write volume data");

        let (tx, _rx) = bounded(8);
        let controller = VolumeController::new(
            test_registry.registry.clone(),
            tx,
            node_id,
            "node-a",
            local_volume_root.path().to_path_buf(),
            true,
        );
        controller
            .reconcile_local_volumes()
            .await
            .expect("reconcile local volumes");

        let state = test_registry
            .registry
            .get_node_state(spec.id, node_id)
            .expect("load node state")
            .expect("node state present");
        assert_eq!(state.used_bytes, Some(5));
        assert!(matches!(state.state, VolumeNodeState::Error));
        assert!(
            state
                .last_error
                .as_deref()
                .is_some_and(|value| value.contains("exceeded requested capacity"))
        );

        let refreshed = test_registry
            .registry
            .get_spec(spec.id)
            .expect("load volume spec")
            .expect("volume spec present");
        assert_eq!(refreshed.status, VolumeStatus::Failed);
        assert_eq!(refreshed.reason.as_deref(), Some("capacity_exceeded"));
    }
}
