use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender, UnboundedSender};

use crate::workload::model::{ExecutionPlatform, IsolationMode};

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

/// Stable backend-qualified reference used to route follow-up operations for one runtime instance.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuntimeInstanceRef {
    pub backend_kind: String,
    pub handle: RuntimeHandle,
}

impl RuntimeInstanceRef {
    /// Builds one backend-qualified runtime reference.
    pub fn new(backend_kind: impl Into<String>, handle: impl Into<RuntimeHandle>) -> Self {
        Self {
            backend_kind: backend_kind.into(),
            handle: handle.into(),
        }
    }
}

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

/// Filesystem access mode carried by one runtime-enforced sandbox policy.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSandboxAccessMode {
    Read,
    Write,
    ReadWrite,
}

/// Path target kind carried by one runtime-enforced sandbox policy.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSandboxPathKind {
    #[default]
    Directory,
    File,
}

/// One filesystem grant carried by one runtime-enforced sandbox policy.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSandboxPathRule {
    pub path: PathBuf,
    #[serde(default)]
    pub kind: RuntimeSandboxPathKind,
    pub access: RuntimeSandboxAccessMode,
}

impl RuntimeSandboxPathRule {
    /// Builds one directory grant for a runtime-enforced sandbox policy.
    pub fn directory(path: impl Into<PathBuf>, access: RuntimeSandboxAccessMode) -> Self {
        Self {
            path: path.into(),
            kind: RuntimeSandboxPathKind::Directory,
            access,
        }
    }

    /// Builds one file grant for a runtime-enforced sandbox policy.
    pub fn file(path: impl Into<PathBuf>, access: RuntimeSandboxAccessMode) -> Self {
        Self {
            path: path.into(),
            kind: RuntimeSandboxPathKind::File,
            access,
        }
    }
}

/// Network mode carried by one runtime-enforced sandbox policy.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSandboxNetworkMode {
    #[default]
    AllowAll,
    Blocked,
}

/// Structured sandbox policy that one runtime backend can translate into real enforcement.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSandboxPolicy {
    #[serde(default)]
    pub working_directory: Option<PathBuf>,
    #[serde(default)]
    pub filesystem: Vec<RuntimeSandboxPathRule>,
    #[serde(default)]
    pub network: RuntimeSandboxNetworkMode,
}

impl RuntimeSandboxPolicy {
    /// Encodes one sandbox policy into the env-safe transport string used by helper binaries.
    pub fn encode_env_value(&self) -> Result<String, RuntimeSandboxPolicyCodecError> {
        ron::ser::to_string(self)
            .map_err(|err| RuntimeSandboxPolicyCodecError::Encode(err.to_string()))
    }

    /// Decodes one helper-provided transport string back into a structured sandbox policy.
    pub fn decode_env_value(value: &str) -> Result<Self, RuntimeSandboxPolicyCodecError> {
        ron::from_str(value).map_err(|err| RuntimeSandboxPolicyCodecError::Decode(err.to_string()))
    }
}

/// Errors returned while transporting one runtime sandbox policy through helper env vars.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RuntimeSandboxPolicyCodecError {
    #[error("sandbox policy encode failed: {0}")]
    Encode(String),

    #[error("sandbox policy decode failed: {0}")]
    Decode(String),
}

/// Parameters describing how one backend should create a runtime instance.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeCreateRequest {
    pub name: String,
    pub image: String,
    pub execution_platform: ExecutionPlatform,
    pub isolation_mode: IsolationMode,
    pub isolation_profile: Option<String>,
    pub sandbox_policy: Option<RuntimeSandboxPolicy>,
    pub labels: Option<HashMap<String, String>>,
    pub command: Option<Vec<String>>,
    pub tty: bool,
    pub open_stdin: bool,
    pub env_vars: Option<Vec<String>>,
    pub ports: Vec<RuntimePortBinding>,
    pub volumes: Option<Vec<String>>,
    pub restart_policy: Option<RestartPolicyConfig>,
    pub resource_limits: ResourceLimits,
    pub dns_servers: Option<Vec<String>>,
    pub gpu_device_ids: Option<Vec<String>>,
}

/// Runtime-neutral host port binding passed to OCI-like backends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimePortBinding {
    pub target_port: u16,
    pub host_port: u16,
    pub host_ip: String,
    pub protocol: RuntimePortProtocol,
}

/// Runtime-neutral transport protocol for a port binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimePortProtocol {
    Tcp,
    Udp,
}

impl RuntimePortProtocol {
    /// Returns the suffix expected by OCI/Docker port maps.
    pub const fn as_port_key_suffix(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
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

/// Point-in-time resource usage sample for one runtime instance.
///
/// CPU usage is cumulative nanoseconds consumed since runtime creation. Callers
/// compute rates from deltas so the runtime backend stays a thin accounting
/// adapter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeUsageSample {
    pub runtime_id: String,
    pub sampled_at_unix_ms: u64,
    pub cpu_usage_nanos: u64,
    pub memory_current_bytes: u64,
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

    /// Returns the union of two backend capability sets.
    pub fn merged(self, other: Self) -> Self {
        Self {
            exec: self.exec || other.exec,
            interactive_exec: self.interactive_exec || other.interactive_exec,
            logs: self.logs || other.logs,
            attach: self.attach || other.attach,
            lifecycle_events: self.lifecycle_events || other.lifecycle_events,
        }
    }
}

/// Reserved feature-flag prefix used to encode exact runtime support contracts.
const RUNTIME_SUPPORT_CONTRACT_FLAG_PREFIX: &str = "runtime_contract:";

/// One exact runtime contract advertised by one backend implementation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuntimeSupportContract {
    pub execution_platform: ExecutionPlatform,
    pub isolation_mode: IsolationMode,
    pub isolation_profile: Option<String>,
}

impl RuntimeSupportContract {
    /// Builds one exact runtime contract from the scheduler-visible runtime selectors.
    pub fn new(
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<&str>,
    ) -> Self {
        Self {
            execution_platform,
            isolation_mode,
            isolation_profile: isolation_profile
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        }
    }

    /// Encodes this contract into the reserved runtime-support feature-flag namespace.
    pub fn feature_flag(&self) -> String {
        let isolation_profile = self.isolation_profile.as_deref().unwrap_or("_");
        format!(
            "{RUNTIME_SUPPORT_CONTRACT_FLAG_PREFIX}{}:{}:{isolation_profile}",
            self.execution_platform.as_str(),
            self.isolation_mode.as_str(),
        )
    }

    /// Decodes one reserved runtime-support feature flag into an exact contract.
    pub fn from_feature_flag(value: &str) -> Option<Self> {
        let encoded = value.strip_prefix(RUNTIME_SUPPORT_CONTRACT_FLAG_PREFIX)?;
        let mut parts = encoded.splitn(3, ':');
        let execution_platform = parts.next()?.parse().ok()?;
        let isolation_mode = parts.next()?.parse().ok()?;
        let isolation_profile = match parts.next()? {
            "_" => None,
            value => Some(value.to_string()),
        };

        Some(Self {
            execution_platform,
            isolation_mode,
            isolation_profile,
        })
    }
}

/// Cluster-visible runtime support metadata advertised by one node.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct RuntimeSupportProfile {
    #[serde(default = "default_execution_platforms")]
    pub execution_platforms: Vec<ExecutionPlatform>,
    #[serde(default = "default_isolation_modes")]
    pub isolation_modes: Vec<IsolationMode>,
    #[serde(default)]
    pub isolation_profiles: Vec<String>,
    #[serde(default)]
    pub feature_flags: Vec<String>,
}

impl Default for RuntimeSupportProfile {
    /// Builds the current task-era default node profile for legacy or test rows.
    fn default() -> Self {
        Self::new(
            [ExecutionPlatform::Oci],
            [IsolationMode::Standard],
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
    pub fn new<I, J, K, L>(
        execution_platforms: I,
        isolation_modes: J,
        isolation_profiles: K,
        feature_flags: L,
    ) -> Self
    where
        I: IntoIterator<Item = ExecutionPlatform>,
        J: IntoIterator<Item = IsolationMode>,
        K: IntoIterator,
        K::Item: Into<String>,
        L: IntoIterator,
        L::Item: Into<String>,
    {
        let mut execution_platforms: Vec<ExecutionPlatform> =
            execution_platforms.into_iter().collect();
        execution_platforms.sort_unstable();
        execution_platforms.dedup();
        if execution_platforms.is_empty() {
            execution_platforms.push(ExecutionPlatform::Oci);
        }

        let mut isolation_modes: Vec<IsolationMode> = isolation_modes.into_iter().collect();
        isolation_modes.sort_unstable();
        isolation_modes.dedup();
        if isolation_modes.is_empty() {
            isolation_modes.push(IsolationMode::Standard);
        }

        let mut isolation_profiles: Vec<String> = isolation_profiles
            .into_iter()
            .map(Into::into)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();
        isolation_profiles.sort_unstable();
        isolation_profiles.dedup();

        let mut feature_flags: Vec<String> = feature_flags
            .into_iter()
            .map(Into::into)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();
        feature_flags.sort_unstable();
        feature_flags.dedup();

        Self {
            execution_platforms,
            isolation_modes,
            isolation_profiles,
            feature_flags,
        }
    }

    /// Builds the default profile for one OCI runtime backend from its feature flags.
    pub fn from_oci_capabilities(capabilities: RuntimeCapabilities) -> Self {
        Self::new(
            [ExecutionPlatform::Oci],
            [IsolationMode::Standard],
            Vec::<String>::new(),
            capabilities.feature_flags(),
        )
    }

    /// Builds one profile from exact backend contracts while preserving summarized legacy fields.
    pub fn from_exact_contracts<I, J>(contracts: I, feature_flags: J) -> Self
    where
        I: IntoIterator<Item = RuntimeSupportContract>,
        J: IntoIterator,
        J::Item: Into<String>,
    {
        let mut contracts: Vec<RuntimeSupportContract> = contracts.into_iter().collect();
        contracts.sort_unstable();
        contracts.dedup();

        let execution_platforms = contracts.iter().map(|value| value.execution_platform);
        let isolation_modes = contracts.iter().map(|value| value.isolation_mode);
        let isolation_profiles = contracts
            .iter()
            .filter_map(|value| value.isolation_profile.clone());

        let mut profile = Self::new(
            execution_platforms,
            isolation_modes,
            isolation_profiles,
            feature_flags,
        );
        profile
            .feature_flags
            .extend(contracts.into_iter().map(|value| value.feature_flag()));
        profile.feature_flags.sort_unstable();
        profile.feature_flags.dedup();
        profile
    }

    /// Returns whether this profile carries exact backend contracts.
    pub fn advertises_exact_contracts(&self) -> bool {
        self.feature_flags
            .iter()
            .any(|value| RuntimeSupportContract::from_feature_flag(value).is_some())
    }

    /// Returns the feature flags that are not part of the reserved contract namespace.
    pub fn non_contract_feature_flags(&self) -> Vec<String> {
        self.feature_flags
            .iter()
            .filter(|value| RuntimeSupportContract::from_feature_flag(value).is_none())
            .cloned()
            .collect()
    }

    /// Returns the exact contracts this profile advertises.
    ///
    /// Legacy profiles without reserved contract flags are expanded using their
    /// summarized runtime fields so existing single-backend behavior stays intact.
    pub fn supported_contracts(&self) -> Vec<RuntimeSupportContract> {
        let mut contracts = self
            .feature_flags
            .iter()
            .filter_map(|value| RuntimeSupportContract::from_feature_flag(value))
            .collect::<Vec<_>>();

        if contracts.is_empty() {
            for execution_platform in &self.execution_platforms {
                for isolation_mode in &self.isolation_modes {
                    contracts.push(RuntimeSupportContract::new(
                        *execution_platform,
                        *isolation_mode,
                        None,
                    ));
                    for isolation_profile in &self.isolation_profiles {
                        contracts.push(RuntimeSupportContract::new(
                            *execution_platform,
                            *isolation_mode,
                            Some(isolation_profile),
                        ));
                    }
                }
            }
        }

        contracts.sort_unstable();
        contracts.dedup();
        contracts
    }

    /// Returns true when this node advertises support for the requested runtime family.
    pub fn supports_execution_platform(&self, execution_platform: ExecutionPlatform) -> bool {
        self.execution_platforms.contains(&execution_platform)
    }

    /// Returns true when this node advertises the requested isolation mode.
    pub fn supports_isolation_mode(&self, isolation_mode: IsolationMode) -> bool {
        self.isolation_modes.contains(&isolation_mode)
    }

    /// Returns true when this node advertises the requested isolation profile, if any.
    pub fn supports_isolation_profile(&self, isolation_profile: Option<&str>) -> bool {
        let Some(isolation_profile) = isolation_profile
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return true;
        };
        self.isolation_profiles
            .iter()
            .any(|value| value == isolation_profile)
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
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<&str>,
        feature_flags: &[String],
    ) -> bool {
        if self.advertises_exact_contracts() {
            let requested =
                RuntimeSupportContract::new(execution_platform, isolation_mode, isolation_profile);
            self.supported_contracts().contains(&requested)
                && self.supports_feature_flags(feature_flags)
        } else {
            self.supports_execution_platform(execution_platform)
                && self.supports_isolation_mode(isolation_mode)
                && self.supports_isolation_profile(isolation_profile)
                && self.supports_feature_flags(feature_flags)
        }
    }

    /// Selects the more complete runtime profile between two concurrent peer rows.
    pub fn preferred(left: Option<&Self>, right: Option<&Self>) -> Option<Self> {
        type PrecedenceKey<'a> = (
            usize,
            usize,
            usize,
            usize,
            &'a Vec<ExecutionPlatform>,
            &'a Vec<IsolationMode>,
            &'a Vec<String>,
            &'a Vec<String>,
        );

        fn precedence_key(value: &RuntimeSupportProfile) -> PrecedenceKey<'_> {
            (
                value.execution_platforms.len(),
                value.isolation_modes.len(),
                value.isolation_profiles.len(),
                value.feature_flags.len(),
                &value.execution_platforms,
                &value.isolation_modes,
                &value.isolation_profiles,
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

fn default_execution_platforms() -> Vec<ExecutionPlatform> {
    vec![ExecutionPlatform::Oci]
}

fn default_isolation_modes() -> Vec<IsolationMode> {
    vec![IsolationMode::Standard]
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

    /// Returns a point-in-time usage sample for one runtime instance.
    async fn sample_instance_usage(&self, _runtime_id: &str) -> RuntimeResult<RuntimeUsageSample> {
        Err(RuntimeError::OperationFailed(
            "runtime usage sampling is not supported by this backend".to_string(),
        ))
    }

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
