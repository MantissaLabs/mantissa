use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender, UnboundedSender};
use tracing::warn;

use crate::runtime::types::{
    RuntimeAttachOptions, RuntimeBackend, RuntimeCapabilities, RuntimeCreateRequest, RuntimeError,
    RuntimeEvent, RuntimeExecOptions, RuntimeExecResult, RuntimeInfo, RuntimeInstanceRef,
    RuntimeLogFrame, RuntimeLogsOptions, RuntimeResult, RuntimeSupportContract,
    RuntimeSupportProfile,
};
use crate::workload::model::{ExecutionPlatform, IsolationMode};

type SharedRuntimeBackend = Arc<dyn RuntimeBackend + Send + Sync>;

/// Runtime instance row discovered while scanning one or more local runtime backends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeDiscoveredInstance {
    pub runtime: RuntimeInstanceRef,
    pub info: RuntimeInfo,
}

#[derive(Clone)]
struct RuntimeSetEntry {
    kind: String,
    backend: SharedRuntimeBackend,
    capabilities: RuntimeCapabilities,
    support: RuntimeSupportProfile,
}

/// Node-local runtime registry used to expose and route multiple backend implementations.
#[derive(Clone)]
pub struct RuntimeSet {
    entries: Arc<Vec<RuntimeSetEntry>>,
    capabilities: RuntimeCapabilities,
    advertised_support: RuntimeSupportProfile,
}

impl RuntimeSet {
    /// Builds one runtime set from the provided backend registrations.
    pub fn new<I, S>(registrations: I) -> Result<Self, RuntimeError>
    where
        I: IntoIterator<Item = (S, SharedRuntimeBackend)>,
        S: Into<String>,
    {
        let mut entries = Vec::new();
        for (kind, backend) in registrations {
            let kind = kind.into().trim().to_string();
            if kind.is_empty() {
                return Err(RuntimeError::OperationFailed(
                    "runtime backend kind cannot be empty".to_string(),
                ));
            }
            if entries
                .iter()
                .any(|entry: &RuntimeSetEntry| entry.kind == kind)
            {
                return Err(RuntimeError::OperationFailed(format!(
                    "runtime backend kind '{kind}' is registered more than once"
                )));
            }

            entries.push(RuntimeSetEntry {
                support: backend.advertised_support(),
                capabilities: backend.capabilities(),
                backend,
                kind,
            });
        }

        if entries.is_empty() {
            return Err(RuntimeError::OperationFailed(
                "runtime set must contain at least one backend".to_string(),
            ));
        }

        let mut capabilities = RuntimeCapabilities::default();
        let mut exact_contracts = Vec::new();
        let mut feature_flags = Vec::new();
        for entry in &entries {
            capabilities = capabilities.merged(entry.capabilities);
            exact_contracts.extend(entry.support.supported_contracts());
            feature_flags.extend(entry.support.non_contract_feature_flags());
        }

        Ok(Self {
            entries: Arc::new(entries),
            capabilities,
            advertised_support: RuntimeSupportProfile::from_exact_contracts(
                exact_contracts,
                feature_flags,
            ),
        })
    }

    /// Builds one runtime set containing exactly one backend registration.
    pub fn singleton(kind: impl Into<String>, backend: SharedRuntimeBackend) -> Self {
        Self::new([(kind.into(), backend)]).expect("runtime set singleton should be valid")
    }

    /// Returns the aggregated runtime support advertised by this node.
    pub fn advertised_support(&self) -> RuntimeSupportProfile {
        self.advertised_support.clone()
    }

    /// Returns the union of runtime capabilities exposed by the registered backend set.
    pub fn capabilities(&self) -> RuntimeCapabilities {
        self.capabilities
    }

    /// Returns the backend capabilities used for the provided execution requirements.
    pub fn capabilities_for_requirements(
        &self,
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<&str>,
        feature_flags: &[String],
    ) -> Option<RuntimeCapabilities> {
        self.select_backend_for_requirements(
            execution_platform,
            isolation_mode,
            isolation_profile,
            feature_flags,
        )
        .ok()
        .map(|entry| entry.capabilities)
    }

    /// Returns the backend capabilities for one runtime reference, if the backend is registered.
    pub fn capabilities_for_runtime(
        &self,
        runtime: &RuntimeInstanceRef,
    ) -> Option<RuntimeCapabilities> {
        self.entry_by_kind(&runtime.backend_kind)
            .map(|entry| entry.capabilities)
    }

    /// Creates one runtime instance on the backend selected by the requested execution contract.
    pub async fn create_instance(
        &self,
        request: RuntimeCreateRequest,
    ) -> RuntimeResult<RuntimeInstanceRef> {
        let entry = self.select_backend_for_request(&request)?;
        let handle = entry.backend.create_instance(request).await?;
        Ok(RuntimeInstanceRef::new(entry.kind.clone(), handle))
    }

    /// Starts one known runtime instance.
    pub async fn start_instance(&self, runtime: &RuntimeInstanceRef) -> RuntimeResult<()> {
        self.backend_for_runtime(runtime)?
            .start_instance(&runtime.handle)
            .await
    }

    /// Stops one known runtime instance.
    pub async fn stop_instance(
        &self,
        runtime: &RuntimeInstanceRef,
        timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        self.backend_for_runtime(runtime)?
            .stop_instance(&runtime.handle, timeout)
            .await
    }

    /// Restarts one known runtime instance.
    pub async fn restart_instance(
        &self,
        runtime: &RuntimeInstanceRef,
        timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        self.backend_for_runtime(runtime)?
            .restart_instance(&runtime.handle, timeout)
            .await
    }

    /// Removes one known runtime instance.
    pub async fn remove_instance(
        &self,
        runtime: &RuntimeInstanceRef,
        force: bool,
        remove_volumes: bool,
    ) -> RuntimeResult<()> {
        self.backend_for_runtime(runtime)?
            .remove_instance(&runtime.handle, force, remove_volumes)
            .await
    }

    /// Executes one non-interactive command inside one known runtime instance.
    pub async fn exec_instance(
        &self,
        runtime: &RuntimeInstanceRef,
        command: &[String],
        timeout: Option<Duration>,
    ) -> RuntimeResult<RuntimeExecResult> {
        self.backend_for_runtime(runtime)?
            .exec_instance(&runtime.handle, command, timeout)
            .await
    }

    /// Starts one interactive exec session inside one known runtime instance.
    pub async fn exec_instance_stream(
        &self,
        runtime: &RuntimeInstanceRef,
        options: &RuntimeExecOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<RuntimeExecResult> {
        self.backend_for_runtime(runtime)?
            .exec_instance_stream(&runtime.handle, options, output_tx, input_rx)
            .await
    }

    /// Streams logs from one known runtime instance.
    pub async fn stream_instance_logs(
        &self,
        runtime: &RuntimeInstanceRef,
        options: &RuntimeLogsOptions,
        logs_tx: MpscSender<RuntimeLogFrame>,
    ) -> RuntimeResult<()> {
        self.backend_for_runtime(runtime)?
            .stream_instance_logs(&runtime.handle, options, logs_tx)
            .await
    }

    /// Attaches to one known runtime instance.
    pub async fn attach_instance(
        &self,
        runtime: &RuntimeInstanceRef,
        options: &RuntimeAttachOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<()> {
        self.backend_for_runtime(runtime)?
            .attach_instance(&runtime.handle, options, output_tx, input_rx)
            .await
    }

    /// Returns inspect metadata for one known runtime instance.
    pub async fn inspect_instance(
        &self,
        runtime: &RuntimeInstanceRef,
    ) -> RuntimeResult<RuntimeInfo> {
        self.backend_for_runtime(runtime)?
            .inspect_instance(&runtime.handle)
            .await
    }

    /// Returns inspect metadata for one named runtime instance across matching backends.
    pub async fn inspect_named_instance(
        &self,
        name: &str,
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<&str>,
    ) -> RuntimeResult<Option<RuntimeDiscoveredInstance>> {
        let entries =
            self.matching_backends(execution_platform, isolation_mode, isolation_profile, &[]);
        if entries.is_empty() {
            return Ok(None);
        }

        let mut last_error = None;
        for entry in entries {
            match entry.backend.inspect_instance(name).await {
                Ok(info) => {
                    let handle = if info.id.is_empty() {
                        name.to_string()
                    } else {
                        info.id.clone()
                    };
                    return Ok(Some(RuntimeDiscoveredInstance {
                        runtime: RuntimeInstanceRef::new(entry.kind.clone(), handle),
                        info,
                    }));
                }
                Err(RuntimeError::NotFound(_)) => {}
                Err(error) => {
                    last_error = Some(error);
                    break;
                }
            }
        }

        if last_error.is_none() {
            for entry in
                self.matching_backends(execution_platform, isolation_mode, isolation_profile, &[])
            {
                let listed = entry.backend.list_instances(None).await?;
                if let Some(info) = listed
                    .into_iter()
                    .find(|info| info.name == name || info.id == name)
                {
                    let handle = if info.id.is_empty() {
                        name.to_string()
                    } else {
                        info.id.clone()
                    };
                    return Ok(Some(RuntimeDiscoveredInstance {
                        runtime: RuntimeInstanceRef::new(entry.kind.clone(), handle),
                        info,
                    }));
                }
            }
        }

        match last_error {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }

    /// Lists runtime inventory across every registered backend.
    pub async fn list_instances(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> RuntimeResult<Vec<RuntimeDiscoveredInstance>> {
        let mut instances = Vec::new();
        for entry in self.entries.iter() {
            let listed = entry.backend.list_instances(filters.clone()).await?;
            for info in listed {
                let handle = if info.id.is_empty() {
                    info.name.clone()
                } else {
                    info.id.clone()
                };
                instances.push(RuntimeDiscoveredInstance {
                    runtime: RuntimeInstanceRef::new(entry.kind.clone(), handle),
                    info,
                });
            }
        }
        Ok(instances)
    }

    /// Returns whether the named image already exists in the selected backend image store.
    pub async fn image_present(
        &self,
        image: &str,
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<&str>,
    ) -> RuntimeResult<bool> {
        self.select_backend_for_requirements(
            execution_platform,
            isolation_mode,
            isolation_profile,
            &[],
        )?
        .backend
        .image_present(image)
        .await
    }

    /// Pulls one image into the backend selected by the requested execution contract.
    pub async fn pull_image(
        &self,
        image: &str,
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<&str>,
    ) -> RuntimeResult<()> {
        self.select_backend_for_requirements(
            execution_platform,
            isolation_mode,
            isolation_profile,
            &[],
        )?
        .backend
        .pull_image(image)
        .await
    }

    /// Starts lifecycle event watchers across the registered backend set.
    pub async fn watch_runtime_events(
        &self,
        events_tx: UnboundedSender<RuntimeEvent>,
    ) -> RuntimeResult<()> {
        let mut entries = self.entries.iter();
        let Some(primary) = entries.next() else {
            return Ok(());
        };

        for entry in entries {
            let backend = entry.backend.clone();
            let events_tx = events_tx.clone();
            tokio::spawn(async move {
                if let Err(error) = backend.watch_runtime_events(events_tx).await {
                    warn!(target: "task", "secondary runtime event watcher exited: {error}");
                }
            });
        }

        primary.backend.watch_runtime_events(events_tx).await
    }

    /// Returns the runtime contracts supported by the currently registered backend set.
    pub fn supported_contracts(&self) -> Vec<RuntimeSupportContract> {
        self.advertised_support.supported_contracts()
    }

    fn select_backend_for_request(
        &self,
        request: &RuntimeCreateRequest,
    ) -> RuntimeResult<&RuntimeSetEntry> {
        self.select_backend_for_requirements(
            request.execution_platform,
            request.isolation_mode,
            request.isolation_profile.as_deref(),
            &[],
        )
    }

    fn select_backend_for_requirements(
        &self,
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<&str>,
        feature_flags: &[String],
    ) -> RuntimeResult<&RuntimeSetEntry> {
        self.matching_backends(
            execution_platform,
            isolation_mode,
            isolation_profile,
            feature_flags,
        )
        .into_iter()
        .next()
        .ok_or_else(|| {
            RuntimeError::OperationFailed(format!(
                "no local runtime backend supports platform={} isolation={} profile={:?} features={feature_flags:?}",
                execution_platform.as_str(),
                isolation_mode.as_str(),
                isolation_profile,
            ))
        })
    }

    fn matching_backends(
        &self,
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<&str>,
        feature_flags: &[String],
    ) -> Vec<&RuntimeSetEntry> {
        self.entries
            .iter()
            .filter(|entry| {
                entry.support.supports_requirements(
                    execution_platform,
                    isolation_mode,
                    isolation_profile,
                    feature_flags,
                )
            })
            .collect()
    }

    fn backend_for_runtime(
        &self,
        runtime: &RuntimeInstanceRef,
    ) -> RuntimeResult<&SharedRuntimeBackend> {
        self.entry_by_kind(&runtime.backend_kind)
            .map(|entry| &entry.backend)
            .ok_or_else(|| {
                RuntimeError::OperationFailed(format!(
                    "runtime backend '{}' is not registered on this node",
                    runtime.backend_kind
                ))
            })
    }

    fn entry_by_kind(&self, kind: &str) -> Option<&RuntimeSetEntry> {
        self.entries.iter().find(|entry| entry.kind == kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::types::RuntimeStateInfo;
    use async_trait::async_trait;

    const SANDBOXED_OCI_PROFILE: &str = "sandboxed-oci";

    #[derive(Default)]
    struct StubRuntimeBackend {
        support: Option<RuntimeSupportProfile>,
        created: tokio::sync::Mutex<Vec<RuntimeCreateRequest>>,
    }

    impl StubRuntimeBackend {
        fn with_support(support: RuntimeSupportProfile) -> Self {
            Self {
                support: Some(support),
                created: tokio::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl RuntimeBackend for StubRuntimeBackend {
        async fn create_instance(&self, request: RuntimeCreateRequest) -> RuntimeResult<String> {
            self.created.lock().await.push(request.clone());
            Ok(format!("{}-instance", request.name))
        }

        async fn start_instance(&self, _runtime_id: &str) -> RuntimeResult<()> {
            Ok(())
        }

        async fn stop_instance(
            &self,
            _runtime_id: &str,
            _timeout: Option<Duration>,
        ) -> RuntimeResult<()> {
            Ok(())
        }

        async fn restart_instance(
            &self,
            _runtime_id: &str,
            _timeout: Option<Duration>,
        ) -> RuntimeResult<()> {
            Ok(())
        }

        async fn remove_instance(
            &self,
            _runtime_id: &str,
            _force: bool,
            _remove_volumes: bool,
        ) -> RuntimeResult<()> {
            Ok(())
        }

        async fn list_instances(
            &self,
            _filters: Option<HashMap<String, Vec<String>>>,
        ) -> RuntimeResult<Vec<RuntimeInfo>> {
            Ok(Vec::new())
        }

        async fn inspect_instance(&self, runtime_id: &str) -> RuntimeResult<RuntimeInfo> {
            Ok(RuntimeInfo {
                id: runtime_id.to_string(),
                name: runtime_id.to_string(),
                image: "stub".to_string(),
                status: "running".to_string(),
                state: RuntimeStateInfo {
                    running: Some(true),
                    ..Default::default()
                },
                ..Default::default()
            })
        }

        async fn pull_image(&self, _image: &str) -> RuntimeResult<()> {
            Ok(())
        }

        fn advertised_support(&self) -> RuntimeSupportProfile {
            self.support.clone().unwrap_or_default()
        }
    }

    #[test]
    fn runtime_set_preserves_exact_backend_contracts() {
        let standard_oci_support = RuntimeSupportProfile::from_exact_contracts(
            [RuntimeSupportContract::new(
                ExecutionPlatform::Oci,
                IsolationMode::Standard,
                None,
            )],
            ["logs"],
        );
        let sandboxed_oci_support = RuntimeSupportProfile::from_exact_contracts(
            [RuntimeSupportContract::new(
                ExecutionPlatform::Oci,
                IsolationMode::Sandboxed,
                Some(SANDBOXED_OCI_PROFILE),
            )],
            ["exec"],
        );

        let runtime_set = RuntimeSet::new([
            (
                "oci-standard",
                Arc::new(StubRuntimeBackend::with_support(standard_oci_support))
                    as SharedRuntimeBackend,
            ),
            (
                "oci-sandboxed",
                Arc::new(StubRuntimeBackend::with_support(sandboxed_oci_support))
                    as SharedRuntimeBackend,
            ),
        ])
        .expect("runtime set");
        let support = runtime_set.advertised_support();

        assert!(support.supports_requirements(
            ExecutionPlatform::Oci,
            IsolationMode::Standard,
            None,
            &[],
        ));
        assert!(support.supports_requirements(
            ExecutionPlatform::Oci,
            IsolationMode::Sandboxed,
            Some(SANDBOXED_OCI_PROFILE),
            &[],
        ));
        assert!(!support.supports_requirements(
            ExecutionPlatform::MicroVm,
            IsolationMode::Sandboxed,
            None,
            &[],
        ));
    }

    #[test]
    fn runtime_set_preserves_legacy_profile_defaults_for_unqualified_requests() {
        let runtime_set = RuntimeSet::new([(
            "in-memory",
            Arc::new(StubRuntimeBackend::with_support(
                RuntimeSupportProfile::new(
                    [ExecutionPlatform::Oci],
                    [IsolationMode::Sandboxed],
                    ["default", "oci-default"],
                    Vec::<String>::new(),
                ),
            )) as SharedRuntimeBackend,
        )])
        .expect("runtime set");
        let support = runtime_set.advertised_support();

        assert!(support.supports_requirements(
            ExecutionPlatform::Oci,
            IsolationMode::Sandboxed,
            None,
            &[],
        ));
        assert!(support.supports_requirements(
            ExecutionPlatform::Oci,
            IsolationMode::Sandboxed,
            Some("oci-default"),
            &[],
        ));
    }

    #[tokio::test]
    async fn runtime_set_selects_backend_by_execution_contract() {
        let runtime_set = RuntimeSet::new([
            (
                "oci-standard",
                Arc::new(StubRuntimeBackend::with_support(
                    RuntimeSupportProfile::from_exact_contracts(
                        [RuntimeSupportContract::new(
                            ExecutionPlatform::Oci,
                            IsolationMode::Standard,
                            None,
                        )],
                        Vec::<String>::new(),
                    ),
                )) as SharedRuntimeBackend,
            ),
            (
                "oci-sandboxed",
                Arc::new(StubRuntimeBackend::with_support(
                    RuntimeSupportProfile::from_exact_contracts(
                        [RuntimeSupportContract::new(
                            ExecutionPlatform::Oci,
                            IsolationMode::Sandboxed,
                            Some(SANDBOXED_OCI_PROFILE),
                        )],
                        Vec::<String>::new(),
                    ),
                )) as SharedRuntimeBackend,
            ),
        ])
        .expect("runtime set");

        let docker_runtime = runtime_set
            .create_instance(RuntimeCreateRequest {
                name: "oci".to_string(),
                image: "img".to_string(),
                execution_platform: ExecutionPlatform::Oci,
                isolation_mode: IsolationMode::Standard,
                ..Default::default()
            })
            .await
            .expect("oci runtime");
        let sandboxed_runtime = runtime_set
            .create_instance(RuntimeCreateRequest {
                name: "sandboxed".to_string(),
                image: "img".to_string(),
                execution_platform: ExecutionPlatform::Oci,
                isolation_mode: IsolationMode::Sandboxed,
                isolation_profile: Some(SANDBOXED_OCI_PROFILE.to_string()),
                ..Default::default()
            })
            .await
            .expect("sandboxed runtime");

        assert_eq!(docker_runtime.backend_kind, "oci-standard");
        assert_eq!(docker_runtime.handle, "oci-instance");
        assert_eq!(sandboxed_runtime.backend_kind, "oci-sandboxed");
        assert_eq!(sandboxed_runtime.handle, "sandboxed-instance");
    }
}
