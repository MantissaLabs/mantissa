use std::path::PathBuf;
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
}

impl VolumeController {
    /// Builds one local volume controller bound to the node-local filesystem root.
    pub fn new(
        registry: VolumeRegistry,
        gossip_tx: Sender<Message>,
        local_node_id: Uuid,
        local_node_name: impl Into<String>,
        local_volume_root: PathBuf,
    ) -> Self {
        Self {
            registry,
            gossip_tx,
            local_node_id,
            local_node_name: local_node_name.into(),
            local_volume_root,
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

        match ensure_local_volume_path(&self.local_volume_root, &spec) {
            Ok(path) => {
                let local_path = path.to_string_lossy().to_string();
                let mut desired = current.clone();
                desired.local_path = Some(local_path);
                desired.capacity_bytes = spec.requested_bytes;
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
