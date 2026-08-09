use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use futures::stream::{self, StreamExt};

use crate::workload::model::{WorkloadSpec, WorkloadStateFilter};

use super::super::WorkloadManager;
use super::{ReplicatedVolumeTaskQuiescence, unique_volume_ids};

/// Limits pressure on the container runtime while quiescing independent tasks together.
const MAX_CONCURRENT_QUIESCES: usize = 8;

/// Reports whether one task quiesced and preserves its error for logs.
struct TaskQuiesceResult {
    quiesced: bool,
    quiescence_error: Option<String>,
}

/// Records anything that prevents a safe replicated-storage shutdown.
#[derive(Default)]
struct RuntimeUseCheck {
    volume_users: Vec<String>,
    inspection_errors: Vec<String>,
}

impl WorkloadManager {
    /// Quiesces every local task that may use replicated storage before daemon shutdown.
    pub(crate) async fn quiesce_replicated_volume_tasks_for_shutdown(
        &self,
    ) -> Result<ReplicatedVolumeTaskQuiescence> {
        let active_tasks = self
            .list_workloads(&WorkloadStateFilter::active_only())
            .await?;
        let (mount_paths, mut safety_check_errors) =
            self.replicated_mount_paths_for_shutdown().await;
        let mut tasks_to_quiesce = Vec::new();
        let mut quiescence_errors = Vec::new();

        for task in active_tasks {
            if task.node_id != self.local_node_id {
                continue;
            }

            match self.task_uses_replicated_volume(&task) {
                Ok(true) => tasks_to_quiesce.push(task),
                Ok(false) => {}
                Err(error) => {
                    // A missing volume row must not let a possibly affected container survive.
                    // Quiesce the task conservatively, then verify the runtime before storage exits.
                    quiescence_errors.push(format!(
                        "could not check volumes for task {}: {error:#}",
                        task.id
                    ));
                    tasks_to_quiesce.push(task);
                }
            }
        }

        let attempted = tasks_to_quiesce.len();
        let task_runtime_names = tasks_to_quiesce
            .iter()
            .map(|task| format!("mantissa-{}", task.id))
            .collect::<HashSet<_>>();
        let quiesce_results = stream::iter(tasks_to_quiesce.into_iter().map(|task| {
            let manager = self.clone();
            async move { manager.quiesce_replicated_volume_task(task).await }
        }))
        .buffer_unordered(MAX_CONCURRENT_QUIESCES)
        .collect::<Vec<_>>()
        .await;

        let mut quiesced = 0;
        for quiesce_result in quiesce_results {
            quiesced += usize::from(quiesce_result.quiesced);
            if let Some(quiescence_error) = quiesce_result.quiescence_error {
                quiescence_errors.push(quiescence_error);
            }
        }

        // Failed launches and removals can leave a durable consumer id after its runtime is gone.
        // Reconcile those ids from actual runtime inventory before deciding that storage is unused.
        // Each failed detach remains published and is retried by the outer shutdown loop.
        safety_check_errors.extend(self.reconcile_stale_replicated_volume_publications().await);

        let runtime_check = self
            .find_replicated_mount_users(&task_runtime_names, &mount_paths)
            .await;
        safety_check_errors.extend(runtime_check.inspection_errors);
        // Task quiescence can fail after its container has stopped. The runtime
        // inventory is the final safety check before storage is removed.
        if !runtime_check.volume_users.is_empty() || !safety_check_errors.is_empty() {
            let mut reasons = Vec::new();
            if !runtime_check.volume_users.is_empty() {
                reasons.push(format!(
                    "running containers still use replicated storage: {}",
                    runtime_check.volume_users.join(", ")
                ));
            }
            reasons.extend(safety_check_errors);
            if !quiescence_errors.is_empty() {
                reasons.push(format!(
                    "task quiescence errors: {}",
                    quiescence_errors.join("; ")
                ));
            }
            return Err(anyhow!(
                "cannot stop replicated storage safely: {}",
                reasons.join("; ")
            ));
        }

        Ok(ReplicatedVolumeTaskQuiescence {
            attempted,
            quiesced,
            quiescence_errors,
        })
    }

    /// Returns whether one task references at least one replicated volume.
    fn task_uses_replicated_volume(&self, task: &WorkloadSpec) -> Result<bool> {
        for volume_id in unique_volume_ids(&task.volumes) {
            let volume = self
                .volumes
                .volume_registry
                .get_spec(volume_id)?
                .ok_or_else(|| anyhow!("unknown volume {volume_id}"))?;
            if volume.driver.is_replicated() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Reads authoritative local mounts before task quiescence changes public status.
    async fn replicated_mount_paths_for_shutdown(&self) -> (HashSet<PathBuf>, Vec<String>) {
        let runtime = match self.replicated_volume_runtime() {
            Ok(runtime) => runtime,
            Err(error) => {
                return (
                    HashSet::new(),
                    vec![format!(
                        "could not access replicated-volume mount inventory: {error:#}"
                    )],
                );
            }
        };
        match runtime.local_mount_paths().await {
            Ok(paths) => (paths.into_iter().collect(), Vec::new()),
            Err(error) => (
                HashSet::new(),
                vec![format!(
                    "could not read local replicated-volume mounts: {error:#}"
                )],
            ),
        }
    }

    /// Quiesces one task locally while preserving its durable demand for restart reconciliation.
    async fn quiesce_replicated_volume_task(&self, task: WorkloadSpec) -> TaskQuiesceResult {
        let task_id = task.id;
        if let Err(error) = self.quiesce_local_task_for_daemon_shutdown(&task).await {
            return TaskQuiesceResult {
                quiesced: false,
                quiescence_error: Some(format!("failed to quiesce task {task_id}: {error:#}")),
            };
        }

        TaskQuiesceResult {
            quiesced: true,
            quiescence_error: None,
        }
    }

    /// Inspects active runtime instances and reports every remaining replicated mount user.
    async fn find_replicated_mount_users(
        &self,
        task_runtime_names: &HashSet<String>,
        replicated_mount_paths: &HashSet<PathBuf>,
    ) -> RuntimeUseCheck {
        let instances = match self.runtime.runtime_set.list_instances(None).await {
            Ok(instances) => instances,
            Err(error) => {
                return RuntimeUseCheck {
                    inspection_errors: vec![format!(
                        "could not list runtime instances after quiescing tasks: {error}"
                    )],
                    ..Default::default()
                };
            }
        };

        let mut check = RuntimeUseCheck::default();
        for instance in instances {
            let info = instance.info;
            if info.state.running == Some(false) {
                continue;
            }

            let instance_name = runtime_name(&info.name, &instance.runtime.handle);
            let belongs_to_task = task_runtime_names.contains(instance_name);
            let replicated_mount = info
                .mounts
                .iter()
                .find(|mount| uses_mount(&mount.source, replicated_mount_paths));
            if belongs_to_task {
                check
                    .volume_users
                    .push(if info.state.running == Some(true) {
                        instance_name.to_string()
                    } else {
                        format!("{instance_name} (running state unknown)")
                    });
            } else if let Some(mount) = replicated_mount {
                let unknown_state_note = if info.state.running == Some(true) {
                    ""
                } else {
                    ", running state unknown"
                };
                check.volume_users.push(format!(
                    "{instance_name} (mounts {}{unknown_state_note})",
                    mount.source
                ));
            }
        }

        check
    }
}

/// Chooses the inspect name when present and falls back to the list response name.
fn runtime_name<'a>(inspected: &'a str, listed: &'a str) -> &'a str {
    if inspected.is_empty() {
        listed
    } else {
        inspected
    }
}

/// Returns whether a runtime source path is the saved mount or one of its children.
fn uses_mount(source: &str, mounts: &HashSet<PathBuf>) -> bool {
    let source = Path::new(source);
    mounts
        .iter()
        .any(|mount| source == mount || source.starts_with(mount))
}
