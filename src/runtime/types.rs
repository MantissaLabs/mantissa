use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender, UnboundedSender};

use crate::workload::model::RuntimeClass;

/// Errors returned by runtime backends.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RuntimeError {
    #[error("Runtime backend error (status={status_code:?}): {message}")]
    Backend {
        status_code: Option<u16>,
        message: String,
    },

    #[error("Runtime instance not found: {0}")]
    NotFound(String),

    #[error("Runtime operation timeout")]
    Timeout,

    #[error("Runtime operation failed: {0}")]
    OperationFailed(String),
}

impl RuntimeError {
    /// Builds one backend error that preserves an engine-specific status code when available.
    pub fn backend(status_code: Option<u16>, message: impl Into<String>) -> Self {
        Self::Backend {
            status_code,
            message: message.into(),
        }
    }

    /// Returns the optional backend status code carried by one runtime error.
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Backend { status_code, .. } => *status_code,
            _ => None,
        }
    }
}

/// Result type shared by all runtime backend operations.
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Stable handle used to address one runtime instance.
pub type RuntimeHandle = String;

/// Exit status returned by a command executed inside one running runtime instance.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeExecResult {
    pub exit_code: Option<i64>,
}

/// Stream selector used by runtime log frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeLogStream {
    StdOut,
    StdErr,
    Console,
}

/// One ordered chunk returned by one runtime log or attach stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeLogFrame {
    pub stream: RuntimeLogStream,
    pub message: Vec<u8>,
}

/// Request options supported by runtime log streaming.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeLogsOptions {
    pub follow: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub timestamps: bool,
    pub tail: String,
}

impl Default for RuntimeLogsOptions {
    /// Builds sane defaults for task log streaming across supported backends.
    fn default() -> Self {
        Self {
            follow: false,
            stdout: true,
            stderr: true,
            timestamps: false,
            tail: "all".to_string(),
        }
    }
}

impl RuntimeLogsOptions {
    /// Normalizes operator input so runtimes always receive explicit stream selection.
    pub fn normalized(&self) -> Self {
        let mut normalized = self.clone();
        if !normalized.stdout && !normalized.stderr {
            normalized.stdout = true;
            normalized.stderr = true;
        }

        let tail = normalized.tail.trim();
        normalized.tail = if tail.is_empty() {
            "all".to_string()
        } else {
            tail.to_string()
        };
        normalized
    }
}

/// Request options supported by interactive runtime attach sessions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeAttachOptions {
    pub logs: bool,
    pub stream: bool,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub detach_keys: Option<String>,
    pub tty: bool,
    pub tty_width: Option<u16>,
    pub tty_height: Option<u16>,
}

impl Default for RuntimeAttachOptions {
    /// Builds sane defaults for interactive task attach sessions.
    fn default() -> Self {
        Self {
            logs: false,
            stream: true,
            stdin: true,
            stdout: true,
            stderr: true,
            detach_keys: None,
            tty: false,
            tty_width: None,
            tty_height: None,
        }
    }
}

/// Request options supported by interactive runtime exec sessions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeExecOptions {
    pub command: Vec<String>,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
    pub detach_keys: Option<String>,
    pub tty_width: Option<u16>,
    pub tty_height: Option<u16>,
}

impl Default for RuntimeExecOptions {
    /// Builds sane defaults for interactive task exec sessions.
    fn default() -> Self {
        Self {
            command: Vec::new(),
            stdin: true,
            stdout: true,
            stderr: true,
            tty: false,
            detach_keys: None,
            tty_width: None,
            tty_height: None,
        }
    }
}

/// Parameters describing how one backend should create a runtime instance.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeCreateRequest {
    pub name: String,
    pub image: String,
    pub runtime_class: RuntimeClass,
    pub sandbox_profile: Option<String>,
    pub labels: Option<HashMap<String, String>>,
    pub command: Option<Vec<String>>,
    pub tty: bool,
    pub open_stdin: bool,
    pub env_vars: Option<Vec<String>>,
    pub ports: Option<HashMap<String, Vec<HashMap<String, String>>>>,
    pub volumes: Option<Vec<String>>,
    pub restart_policy: Option<RestartPolicyConfig>,
    pub resource_limits: ResourceLimits,
    pub dns_servers: Option<Vec<String>>,
    pub gpu_device_ids: Option<Vec<String>>,
}

/// Configuration for runtime restart policy.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestartPolicyConfig {
    pub name: RestartPolicyType,
    pub max_retry_count: Option<i32>,
}

/// Types of restart policies supported by OCI-like runtimes.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RestartPolicyType {
    No,
    Always,
    OnFailure,
    UnlessStopped,
}

/// Resource limits that should be enforced by the runtime backend.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceLimits {
    pub memory_bytes: Option<i64>,
    pub nano_cpus: Option<i64>,
    pub cpu_shares: Option<i64>,
}

impl ResourceLimits {
    const MIN_CPU_SHARES: i64 = 2;
    const MAX_CPU_SHARES: i64 = 262_144;

    /// Builds runtime limits from scheduler requests expressed in milli-CPU and bytes.
    pub fn from_requests(cpu_millis: u64, memory_bytes: u64) -> Self {
        let memory_bytes = if memory_bytes == 0 {
            None
        } else {
            Some(Self::saturating_i64(memory_bytes as u128))
        };

        let nano_cpus = if cpu_millis == 0 {
            None
        } else {
            let nanos = (cpu_millis as u128).saturating_mul(1_000_000u128);
            Some(Self::saturating_i64(nanos))
        };

        let cpu_shares = if cpu_millis == 0 {
            None
        } else {
            let shares = (cpu_millis as u128).saturating_mul(1024u128) / 1_000u128;
            let shares = shares
                .max(Self::MIN_CPU_SHARES as u128)
                .min(Self::MAX_CPU_SHARES as u128);
            Some(Self::saturating_i64(shares))
        };

        Self {
            memory_bytes,
            nano_cpus,
            cpu_shares,
        }
    }

    fn saturating_i64(value: u128) -> i64 {
        if value > i64::MAX as u128 {
            i64::MAX
        } else {
            value as i64
        }
    }
}

/// Runtime configuration bits surfaced through inspect responses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeConfigInfo {
    pub tty: Option<bool>,
}

/// Runtime process state surfaced through inspect responses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeStateInfo {
    pub raw_status: Option<String>,
    pub running: Option<bool>,
    pub pid: Option<i64>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

/// Runtime-specific network attachment target used by the networking layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeAttachmentTarget {
    NetworkNamespacePid(i32),
    NetworkNamespacePath(String),
    TapDevice(String),
}

/// One runtime network endpoint surfaced by inspect responses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeNetworkEndpoint {
    pub name: String,
    pub ip_address: Option<String>,
}

/// Generic metadata returned by runtime list and inspect operations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeInfo {
    pub id: String,
    pub name: String,
    pub image: String,
    pub labels: HashMap<String, String>,
    pub status: String,
    pub state: RuntimeStateInfo,
    pub created: i64,
    pub config: RuntimeConfigInfo,
    pub attachment_target: Option<RuntimeAttachmentTarget>,
    pub network_endpoints: Vec<RuntimeNetworkEndpoint>,
}

/// Capability flags exposed by one runtime backend.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeCapabilities {
    pub exec: bool,
    pub interactive_exec: bool,
    pub logs: bool,
    pub attach: bool,
    pub lifecycle_events: bool,
}

impl RuntimeCapabilities {
    /// Converts one backend capability bitset into canonical cluster-visible feature flags.
    pub fn feature_flags(&self) -> Vec<String> {
        let mut flags = Vec::new();
        if self.exec {
            flags.push("exec".to_string());
        }
        if self.interactive_exec {
            flags.push("interactive_exec".to_string());
        }
        if self.logs {
            flags.push("logs".to_string());
        }
        if self.attach {
            flags.push("attach".to_string());
        }
        if self.lifecycle_events {
            flags.push("lifecycle_events".to_string());
        }
        flags
    }
}

/// Cluster-visible runtime support metadata advertised by one node.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct RuntimeSupportProfile {
    #[serde(default = "default_runtime_classes")]
    pub runtime_classes: Vec<RuntimeClass>,
    #[serde(default)]
    pub sandbox_profiles: Vec<String>,
    #[serde(default)]
    pub feature_flags: Vec<String>,
}

impl Default for RuntimeSupportProfile {
    /// Builds the current task-era default node profile for legacy or test rows.
    fn default() -> Self {
        Self::new(
            [RuntimeClass::Oci],
            Vec::<String>::new(),
            RuntimeCapabilities {
                exec: true,
                interactive_exec: true,
                logs: true,
                attach: true,
                lifecycle_events: true,
            }
            .feature_flags(),
        )
    }
}

impl RuntimeSupportProfile {
    /// Normalizes one runtime support profile into a deterministic, deduplicated form.
    pub fn new<I, J, K>(runtime_classes: I, sandbox_profiles: J, feature_flags: K) -> Self
    where
        I: IntoIterator<Item = RuntimeClass>,
        J: IntoIterator,
        J::Item: Into<String>,
        K: IntoIterator,
        K::Item: Into<String>,
    {
        let mut runtime_classes: Vec<RuntimeClass> = runtime_classes.into_iter().collect();
        runtime_classes.sort_unstable();
        runtime_classes.dedup();
        if runtime_classes.is_empty() {
            runtime_classes.push(RuntimeClass::Oci);
        }

        let mut sandbox_profiles: Vec<String> = sandbox_profiles
            .into_iter()
            .map(Into::into)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();
        sandbox_profiles.sort_unstable();
        sandbox_profiles.dedup();

        let mut feature_flags: Vec<String> = feature_flags
            .into_iter()
            .map(Into::into)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();
        feature_flags.sort_unstable();
        feature_flags.dedup();

        Self {
            runtime_classes,
            sandbox_profiles,
            feature_flags,
        }
    }

    /// Builds the default profile for one OCI runtime backend from its feature flags.
    pub fn from_oci_capabilities(capabilities: RuntimeCapabilities) -> Self {
        Self::new(
            [RuntimeClass::Oci],
            Vec::<String>::new(),
            capabilities.feature_flags(),
        )
    }

    /// Returns true when this node advertises support for the requested runtime family.
    pub fn supports_runtime_class(&self, runtime_class: RuntimeClass) -> bool {
        self.runtime_classes.contains(&runtime_class)
    }

    /// Returns true when this node advertises the requested sandbox profile, if any.
    pub fn supports_sandbox_profile(&self, sandbox_profile: Option<&str>) -> bool {
        let Some(sandbox_profile) = sandbox_profile
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return true;
        };
        self.sandbox_profiles
            .iter()
            .any(|value| value == sandbox_profile)
    }

    /// Returns true when this node advertises every required runtime feature flag.
    pub fn supports_feature_flags(&self, required_flags: &[String]) -> bool {
        required_flags
            .iter()
            .all(|required| self.feature_flags.iter().any(|value| value == required))
    }

    /// Returns true when this profile satisfies the requested runtime requirements.
    pub fn supports_requirements(
        &self,
        runtime_class: RuntimeClass,
        sandbox_profile: Option<&str>,
        feature_flags: &[String],
    ) -> bool {
        self.supports_runtime_class(runtime_class)
            && self.supports_sandbox_profile(sandbox_profile)
            && self.supports_feature_flags(feature_flags)
    }

    /// Selects the more complete runtime profile between two concurrent peer rows.
    pub fn preferred(left: Option<&Self>, right: Option<&Self>) -> Option<Self> {
        fn precedence_key(
            value: &RuntimeSupportProfile,
        ) -> (
            usize,
            usize,
            usize,
            &Vec<RuntimeClass>,
            &Vec<String>,
            &Vec<String>,
        ) {
            (
                value.runtime_classes.len(),
                value.sandbox_profiles.len(),
                value.feature_flags.len(),
                &value.runtime_classes,
                &value.sandbox_profiles,
                &value.feature_flags,
            )
        }

        match (left, right) {
            (Some(left), Some(right)) => {
                if precedence_key(left) >= precedence_key(right) {
                    Some(left.clone())
                } else {
                    Some(right.clone())
                }
            }
            (Some(left), None) => Some(left.clone()),
            (None, Some(right)) => Some(right.clone()),
            (None, None) => None,
        }
    }
}

fn default_runtime_classes() -> Vec<RuntimeClass> {
    vec![RuntimeClass::Oci]
}

/// Runtime lifecycle events used by task reconciliation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeEvent {
    InstanceStateChanged,
    TaskExited { task_id: uuid::Uuid, exit_code: i32 },
}

/// Interface for backend-specific runtime management operations.
#[async_trait]
pub trait RuntimeBackend {
    /// Creates one new runtime instance and returns its backend handle.
    async fn create_instance(&self, request: RuntimeCreateRequest) -> RuntimeResult<RuntimeHandle>;

    /// Starts one existing runtime instance.
    async fn start_instance(&self, runtime_id: &str) -> RuntimeResult<()>;

    /// Stops one existing runtime instance.
    async fn stop_instance(&self, runtime_id: &str, timeout: Option<Duration>)
    -> RuntimeResult<()>;

    /// Executes one non-interactive command inside one running instance.
    async fn exec_instance(
        &self,
        _runtime_id: &str,
        _command: &[String],
        _timeout: Option<Duration>,
    ) -> RuntimeResult<RuntimeExecResult> {
        Err(RuntimeError::OperationFailed(
            "runtime exec is not supported by this backend".to_string(),
        ))
    }

    /// Starts one streamed exec session inside one running instance.
    async fn exec_instance_stream(
        &self,
        _runtime_id: &str,
        _options: &RuntimeExecOptions,
        _output_tx: MpscSender<RuntimeLogFrame>,
        _input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<RuntimeExecResult> {
        Err(RuntimeError::OperationFailed(
            "interactive runtime exec is not supported by this backend".to_string(),
        ))
    }

    /// Streams ordered runtime log frames into the provided bounded channel.
    async fn stream_instance_logs(
        &self,
        _runtime_id: &str,
        _options: &RuntimeLogsOptions,
        _logs_tx: MpscSender<RuntimeLogFrame>,
    ) -> RuntimeResult<()> {
        Err(RuntimeError::OperationFailed(
            "runtime log streaming is not supported by this backend".to_string(),
        ))
    }

    /// Attaches to one runtime instance stdio stream using bounded channels.
    async fn attach_instance(
        &self,
        _runtime_id: &str,
        _options: &RuntimeAttachOptions,
        _output_tx: MpscSender<RuntimeLogFrame>,
        _input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<()> {
        Err(RuntimeError::OperationFailed(
            "runtime attach is not supported by this backend".to_string(),
        ))
    }

    /// Restarts one existing runtime instance.
    async fn restart_instance(
        &self,
        runtime_id: &str,
        timeout: Option<Duration>,
    ) -> RuntimeResult<()>;

    /// Removes one runtime instance from the backend.
    async fn remove_instance(
        &self,
        runtime_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> RuntimeResult<()>;

    /// Lists runtime instances, optionally filtered by backend-specific key/value selectors.
    async fn list_instances(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> RuntimeResult<Vec<RuntimeInfo>>;

    /// Returns inspect-level metadata for one runtime instance.
    async fn inspect_instance(&self, runtime_id: &str) -> RuntimeResult<RuntimeInfo>;

    /// Returns whether the named image is already present in the local image store.
    async fn image_present(&self, _image: &str) -> RuntimeResult<bool> {
        Ok(false)
    }

    /// Pulls one image into the local backend image store.
    async fn pull_image(&self, image: &str) -> RuntimeResult<()>;

    /// Reports the optional capabilities implemented by this runtime backend.
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities::default()
    }

    /// Reports the cluster-visible runtime support metadata advertised by this backend.
    fn advertised_support(&self) -> RuntimeSupportProfile {
        RuntimeSupportProfile::from_oci_capabilities(self.capabilities())
    }

    /// Streams runtime lifecycle events into the provided queue until the stream ends.
    async fn watch_runtime_events(
        &self,
        _events_tx: UnboundedSender<RuntimeEvent>,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}
