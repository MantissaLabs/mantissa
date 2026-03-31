use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender};

use crate::runtime::types::{
    RuntimeAttachmentTarget, RuntimeBackend, RuntimeCapabilities, RuntimeCreateRequest,
    RuntimeError, RuntimeExecOptions, RuntimeExecResult, RuntimeInfo, RuntimeLogFrame,
    RuntimeLogsOptions, RuntimeResult, RuntimeStateInfo, RuntimeSupportProfile,
};
use crate::workload::model::ExecutionSubstrate;

/// Returns whether tests requested the shared in-memory runtime backend through one env override.
pub fn use_in_memory_runtime_backend_from_env() -> bool {
    std::env::var_os("MANTISSA_TEST_INMEMORY_CONTAINER_MANAGER").is_some()
}

#[derive(Default)]
pub struct InMemoryRuntimeBackend {
    instances: AsyncMutex<HashMap<String, InMemoryRuntimeEntry>>,
    names: AsyncMutex<HashMap<String, String>>,
}

#[derive(Clone)]
struct InMemoryRuntimeEntry {
    id: String,
    name: String,
    image: String,
    labels: HashMap<String, String>,
    running: bool,
}

impl InMemoryRuntimeBackend {
    /// Builds one not-found error for the named runtime handle.
    fn not_found(runtime_id: &str) -> RuntimeError {
        RuntimeError::NotFound(runtime_id.to_string())
    }

    /// Builds one deterministic name-conflict error for repeated runtime names.
    fn name_conflict(name: &str) -> RuntimeError {
        RuntimeError::backend(Some(409), format!("runtime name '{name}' already in use"))
    }

    /// Resolves one runtime id from either the backend id or the deterministic runtime name.
    async fn resolve_runtime_id(&self, key: &str) -> Option<String> {
        {
            let instances = self.instances.lock().await;
            if instances.contains_key(key) {
                return Some(key.to_string());
            }
        }

        let names = self.names.lock().await;
        names.get(key).cloned()
    }
}

/// Creates the shared in-memory runtime backend used by tests and env-driven local runs.
pub fn new_in_memory_runtime_backend() -> Arc<dyn RuntimeBackend + Send + Sync> {
    Arc::new(InMemoryRuntimeBackend::default())
}

#[async_trait]
impl RuntimeBackend for InMemoryRuntimeBackend {
    /// Creates one synthetic in-memory runtime entry.
    async fn create_instance(&self, request: RuntimeCreateRequest) -> RuntimeResult<String> {
        {
            let names = self.names.lock().await;
            if names.contains_key(&request.name) {
                return Err(Self::name_conflict(&request.name));
            }
        }

        let id = uuid::Uuid::new_v4().to_string();
        let entry = InMemoryRuntimeEntry {
            id: id.clone(),
            name: request.name.clone(),
            image: request.image,
            labels: request.labels.unwrap_or_default(),
            running: false,
        };

        self.instances.lock().await.insert(id.clone(), entry);
        self.names.lock().await.insert(request.name, id.clone());
        Ok(id)
    }

    /// Marks one in-memory runtime as started.
    async fn start_instance(&self, runtime_id: &str) -> RuntimeResult<()> {
        let Some(id) = self.resolve_runtime_id(runtime_id).await else {
            return Err(Self::not_found(runtime_id));
        };

        let mut instances = self.instances.lock().await;
        let Some(instance) = instances.get_mut(&id) else {
            return Err(Self::not_found(runtime_id));
        };
        instance.running = true;
        Ok(())
    }

    /// Marks one in-memory runtime as stopped.
    async fn stop_instance(
        &self,
        runtime_id: &str,
        _timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        let Some(id) = self.resolve_runtime_id(runtime_id).await else {
            return Err(Self::not_found(runtime_id));
        };

        let mut instances = self.instances.lock().await;
        let Some(instance) = instances.get_mut(&id) else {
            return Err(Self::not_found(runtime_id));
        };
        instance.running = false;
        Ok(())
    }

    /// Executes one successful synthetic command when the runtime is running.
    async fn exec_instance(
        &self,
        runtime_id: &str,
        _command: &[String],
        _timeout: Option<Duration>,
    ) -> RuntimeResult<RuntimeExecResult> {
        let Some(id) = self.resolve_runtime_id(runtime_id).await else {
            return Err(Self::not_found(runtime_id));
        };

        let instances = self.instances.lock().await;
        let Some(instance) = instances.get(&id) else {
            return Err(Self::not_found(runtime_id));
        };
        if !instance.running {
            return Err(RuntimeError::OperationFailed(format!(
                "runtime {runtime_id} is not running"
            )));
        }

        Ok(RuntimeExecResult { exit_code: Some(0) })
    }

    /// Drains the interactive input stream and reports one successful synthetic exec.
    async fn exec_instance_stream(
        &self,
        runtime_id: &str,
        options: &RuntimeExecOptions,
        _output_tx: MpscSender<RuntimeLogFrame>,
        mut input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<RuntimeExecResult> {
        if options.command.is_empty() {
            return Err(RuntimeError::OperationFailed(
                "exec command must contain at least one argument".to_string(),
            ));
        }

        let Some(id) = self.resolve_runtime_id(runtime_id).await else {
            return Err(Self::not_found(runtime_id));
        };

        let instances = self.instances.lock().await;
        let Some(instance) = instances.get(&id) else {
            return Err(Self::not_found(runtime_id));
        };
        if !instance.running {
            return Err(RuntimeError::OperationFailed(format!(
                "runtime {runtime_id} is not running"
            )));
        }
        drop(instances);

        while input_rx.recv().await.is_some() {}
        Ok(RuntimeExecResult { exit_code: Some(0) })
    }

    /// Marks one in-memory runtime as restarted.
    async fn restart_instance(
        &self,
        runtime_id: &str,
        _timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        let Some(id) = self.resolve_runtime_id(runtime_id).await else {
            return Err(Self::not_found(runtime_id));
        };

        let mut instances = self.instances.lock().await;
        let Some(instance) = instances.get_mut(&id) else {
            return Err(Self::not_found(runtime_id));
        };
        instance.running = true;
        Ok(())
    }

    /// Removes one in-memory runtime and any name mapping pointing at it.
    async fn remove_instance(
        &self,
        runtime_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> RuntimeResult<()> {
        let Some(id) = self.resolve_runtime_id(runtime_id).await else {
            return Ok(());
        };

        let removed = self.instances.lock().await.remove(&id);
        if let Some(entry) = removed {
            self.names.lock().await.remove(&entry.name);
        }
        Ok(())
    }

    /// Lists the synthetic runtime inventory tracked by the in-memory backend.
    async fn list_instances(
        &self,
        _filters: Option<HashMap<String, Vec<String>>>,
    ) -> RuntimeResult<Vec<RuntimeInfo>> {
        let instances = self.instances.lock().await;
        let mut out = Vec::with_capacity(instances.len());
        for entry in instances.values() {
            out.push(RuntimeInfo {
                id: entry.id.clone(),
                name: entry.name.clone(),
                image: entry.image.clone(),
                labels: entry.labels.clone(),
                status: if entry.running {
                    "running".to_string()
                } else {
                    "stopped".to_string()
                },
                state: RuntimeStateInfo {
                    raw_status: Some(if entry.running {
                        "running".to_string()
                    } else {
                        "exited".to_string()
                    }),
                    running: Some(entry.running),
                    pid: Some(if entry.running { 1000 } else { 0 }),
                    ..Default::default()
                },
                created: 0,
                attachment_target: entry
                    .running
                    .then_some(RuntimeAttachmentTarget::NetworkNamespacePid(1000)),
                ..Default::default()
            });
        }
        Ok(out)
    }

    /// Returns one inspect-level runtime snapshot for the requested in-memory entry.
    async fn inspect_instance(&self, runtime_id: &str) -> RuntimeResult<RuntimeInfo> {
        let Some(id) = self.resolve_runtime_id(runtime_id).await else {
            return Err(Self::not_found(runtime_id));
        };

        let instances = self.instances.lock().await;
        let Some(entry) = instances.get(&id) else {
            return Err(Self::not_found(runtime_id));
        };

        Ok(RuntimeInfo {
            id: entry.id.clone(),
            name: entry.name.clone(),
            image: entry.image.clone(),
            labels: entry.labels.clone(),
            status: if entry.running {
                "running".to_string()
            } else {
                "stopped".to_string()
            },
            state: RuntimeStateInfo {
                raw_status: Some(if entry.running {
                    "running".to_string()
                } else {
                    "exited".to_string()
                }),
                running: Some(entry.running),
                pid: Some(if entry.running { 1000 } else { 0 }),
                ..Default::default()
            },
            created: 0,
            attachment_target: entry
                .running
                .then_some(RuntimeAttachmentTarget::NetworkNamespacePid(1000)),
            ..Default::default()
        })
    }

    /// Reports that every image is locally available in the in-memory backend.
    async fn image_present(&self, _image: &str) -> RuntimeResult<bool> {
        Ok(true)
    }

    /// Treats image pulls as no-ops because the in-memory backend has no image store.
    async fn pull_image(&self, _image: &str) -> RuntimeResult<()> {
        Ok(())
    }

    /// Advertises the capabilities implemented by the shared in-memory backend.
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            exec: true,
            interactive_exec: true,
            logs: true,
            attach: false,
            lifecycle_events: false,
        }
    }

    /// Advertises that the shared in-memory backend can host OCI workloads in standard or
    /// sandboxed isolation modes.
    fn advertised_support(&self) -> RuntimeSupportProfile {
        RuntimeSupportProfile::new(
            [ExecutionSubstrate::Oci],
            [
                crate::workload::model::IsolationMode::Standard,
                crate::workload::model::IsolationMode::Sandboxed,
            ],
            ["default", "oci-default"],
            self.capabilities().feature_flags(),
        )
    }

    /// Accepts log-stream requests without emitting any synthetic frames.
    async fn stream_instance_logs(
        &self,
        runtime_id: &str,
        _options: &RuntimeLogsOptions,
        _logs_tx: MpscSender<RuntimeLogFrame>,
    ) -> RuntimeResult<()> {
        let Some(_id) = self.resolve_runtime_id(runtime_id).await else {
            return Err(Self::not_found(runtime_id));
        };
        Ok(())
    }
}
