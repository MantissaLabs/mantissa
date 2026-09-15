use crate::types::common::{
    self, HostPort, TaskSecretFile, TaskVolumeMount, text_list, uuid_to_string,
};
use mantissa_client::tasks::TaskRow;
use mantissa_client::{
    host_ports::decode_host_ports, services::manifest as config,
    tasks::inspect::state_and_exit_code,
};
use mantissa_protocol::{task::task_spec, workload};
use serde::{Deserialize, Deserializer, Serialize, de};
use utoipa::{IntoParams, ToSchema};

/// REST-facing task summary returned by task routes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct TaskSummary {
    pub id: String,
    pub name: String,
    pub image: String,
    pub slot: String,
    pub cpu_millis: u64,
    pub memory_mib: u64,
    pub gpu_count: u32,
    pub command: String,
    pub node: String,
    pub ports: Vec<HostPort>,
    pub state: String,
    pub created_at: String,
}

impl From<TaskRow> for TaskSummary {
    /// Converts the client task row into the REST JSON shape.
    fn from(value: TaskRow) -> Self {
        Self {
            id: value.id,
            name: value.name,
            image: value.image,
            slot: value.slot,
            cpu_millis: value.cpu_millis,
            memory_mib: value.memory_bytes / (1024 * 1024),
            gpu_count: value.gpu_count,
            command: value.command,
            node: value.node,
            ports: value.ports.into_iter().map(HostPort::from).collect(),
            state: value.state,
            created_at: value.created_at,
        }
    }
}

/// Complete task configuration and lifecycle diagnostics from one replicated snapshot.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct TaskDetail {
    pub id: String,
    pub name: String,
    pub state: String,
    pub exit_code: Option<i32>,
    pub phase_reason: Option<String>,
    pub phase_progress: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub image: String,
    pub command: Vec<String>,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
    pub tty: bool,
    pub node_id: String,
    pub node_name: String,
    pub slot_ids: Vec<u64>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub gpu_device_ids: Vec<String>,
    pub restart_policy: Option<config::TaskTemplateRestartPolicy>,
    pub termination_grace_period_secs: Option<u32>,
    pub pre_stop_command: Vec<String>,
    pub liveness: Option<config::LivenessProbe>,
    pub networks: Vec<String>,
    pub ports: Vec<HostPort>,
    pub volumes: Vec<TaskVolumeMount>,
    pub env: Vec<config::EnvironmentVariable>,
    pub secret_files: Vec<TaskSecretFile>,
    pub task_epoch: u64,
    pub phase_version: u64,
    pub launch_attempt: u64,
    pub last_terminal_observed_launch: Option<u64>,
    pub lease_id: Option<String>,
    pub lease_coordinator_node_id: Option<String>,
}

impl TaskDetail {
    /// Keeps exact resource units and structured references instead of reusing formatted list fields.
    pub fn from_spec(spec: task_spec::Reader<'_>) -> capnp::Result<Self> {
        let (state, exit_code) = state_and_exit_code(spec.get_state()?.to_str()?);
        let restart_policy = if spec.has_restart_policy() {
            let policy = spec.get_restart_policy()?;
            Some(config::TaskTemplateRestartPolicy {
                name: match policy.get_name()? {
                    workload::RestartPolicyName::No => config::RestartPolicyName::No,
                    workload::RestartPolicyName::Always => config::RestartPolicyName::Always,
                    workload::RestartPolicyName::OnFailure => config::RestartPolicyName::OnFailure,
                    workload::RestartPolicyName::UnlessStopped => {
                        config::RestartPolicyName::UnlessStopped
                    }
                },
                max_retry_count: policy.get_max_retry_count().try_into().ok(),
            })
        } else {
            None
        };

        let liveness = if spec.has_liveness() {
            let probe = spec.get_liveness()?;
            Some(config::LivenessProbe {
                kind: match probe.get_kind()? {
                    workload::LivenessProbeKind::Exec => config::LivenessKind::Exec,
                    workload::LivenessProbeKind::Http => config::LivenessKind::Http,
                    workload::LivenessProbeKind::Tcp => config::LivenessKind::Tcp,
                },
                command: text_list(probe.get_command()?)?,
                port: probe.get_port(),
                path: optional_text(probe.get_path()?)?,
                interval_ms: probe.get_interval_ms(),
                timeout_ms: probe.get_timeout_ms(),
                failure_threshold: probe.get_failure_threshold(),
                start_period_ms: probe.get_start_period_ms(),
            })
        } else {
            None
        };

        let grace = spec.get_termination_grace_period_secs();
        let terminal = spec.get_last_terminal_observed_launch();
        Ok(Self {
            id: uuid_to_string(spec.get_id()?)?,
            name: spec.get_name()?.to_str()?.to_string(),
            state: state.to_string(),
            exit_code,
            phase_reason: optional_text(spec.get_phase_reason()?)?,
            phase_progress: optional_text(spec.get_phase_progress()?)?,
            created_at: spec.get_created_at()?.to_str()?.to_string(),
            updated_at: spec.get_updated_at()?.to_str()?.to_string(),
            image: spec.get_image()?.to_str()?.to_string(),
            command: text_list(spec.get_command()?)?,
            execution_platform: spec.get_execution_platform()?.to_str()?.to_string(),
            isolation_mode: spec.get_isolation_mode()?.to_str()?.to_string(),
            isolation_profile: optional_text(spec.get_isolation_profile()?)?,
            tty: spec.get_tty(),
            node_id: uuid_to_string(spec.get_node_id()?)?,
            node_name: spec.get_node_name()?.to_str()?.to_string(),
            slot_ids: spec.get_slot_ids()?.iter().collect(),
            cpu_millis: spec.get_cpu_millis(),
            memory_bytes: spec.get_memory_bytes(),
            gpu_count: spec.get_gpu_count(),
            gpu_device_ids: text_list(spec.get_gpu_device_ids()?)?,
            restart_policy,
            termination_grace_period_secs: (grace != 0).then_some(grace),
            pre_stop_command: text_list(spec.get_pre_stop_command()?)?,
            liveness,
            networks: spec
                .get_networks()?
                .iter()
                .map(|id| uuid_to_string(id?))
                .collect::<capnp::Result<_>>()?,
            ports: decode_host_ports(spec.get_ports()?)?
                .into_iter()
                .map(HostPort::from)
                .collect(),
            volumes: common::volumes(spec.get_volumes()?)?,
            env: common::env(spec.get_env()?)?,
            secret_files: common::secret_files(spec.get_secret_files()?)?,
            task_epoch: spec.get_task_epoch(),
            phase_version: spec.get_phase_version(),
            launch_attempt: spec.get_launch_attempt(),
            last_terminal_observed_launch: (terminal != 0).then_some(terminal),
            lease_id: optional_uuid(spec.get_lease_id()?)?,
            lease_coordinator_node_id: optional_uuid(spec.get_lease_coordinator_node_id()?)?,
        })
    }
}

/// Represents absent diagnostics as null while retaining nonempty text exactly.
fn optional_text(value: capnp::text::Reader<'_>) -> capnp::Result<Option<String>> {
    let value = value.to_str()?;
    Ok((!value.is_empty()).then(|| value.to_string()))
}

/// Preserves optional lease identifiers without introducing a nil UUID sentinel.
fn optional_uuid(bytes: &[u8]) -> capnp::Result<Option<String>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        uuid_to_string(bytes).map(Some)
    }
}

/// REST request body for starting one standalone task.
#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskStartRequest {
    pub name: String,
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[schema(minimum = 1)]
    pub cpu_millis: u64,
    #[schema(minimum = 1)]
    pub memory_bytes: u64,
    #[serde(default)]
    pub gpu_count: u32,
    #[serde(default)]
    pub volumes: Vec<String>,
}

/// REST query parameters for streaming standalone task logs.
#[derive(Clone, Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub struct TaskLogsQuery {
    #[serde(default)]
    pub follow: bool,
    #[serde(default = "default_log_tail")]
    pub tail: String,
    #[serde(default)]
    pub stdout: bool,
    #[serde(default)]
    pub stderr: bool,
    #[serde(default)]
    pub timestamps: bool,
}

/// REST WebSocket query parameters for attaching to one task.
#[derive(Clone, Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub struct TaskAttachQuery {
    #[serde(default = "default_true")]
    pub logs: bool,
    #[serde(default = "default_true")]
    pub stream: bool,
    #[serde(default = "default_true")]
    pub stdin: bool,
    #[serde(default = "default_true")]
    pub stdout: bool,
    #[serde(default = "default_true")]
    pub stderr: bool,
    #[serde(default)]
    pub detach_keys: Option<String>,
    #[serde(default)]
    pub tty_width: Option<u16>,
    #[serde(default)]
    pub tty_height: Option<u16>,
}

/// REST WebSocket query parameters for starting one task exec session.
#[derive(Clone, Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub struct TaskExecQuery {
    #[serde(default, deserialize_with = "deserialize_command_query")]
    pub command: Vec<String>,
    #[serde(default = "default_true")]
    pub stdin: bool,
    #[serde(default = "default_true")]
    pub stdout: bool,
    #[serde(default = "default_true")]
    pub stderr: bool,
    #[serde(default)]
    pub tty: bool,
    #[serde(default)]
    pub detach_keys: Option<String>,
    #[serde(default)]
    pub tty_width: Option<u16>,
    #[serde(default)]
    pub tty_height: Option<u16>,
}

/// Raw query representation accepted for the exec command vector.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CommandQueryField {
    Args(Vec<String>),
    Text(String),
}

impl TaskLogsQuery {
    /// Validates query options before the worker starts a Cap'n Proto log stream.
    pub fn validate(&self) -> Result<(), String> {
        let tail = self.tail.trim();
        if tail.is_empty() {
            return Err("tail must not be empty".to_string());
        }
        if tail.eq_ignore_ascii_case("all") || tail.parse::<u64>().is_ok() {
            return Ok(());
        }
        Err(format!(
            "invalid tail '{tail}': expected a non-negative integer or 'all'"
        ))
    }
}

impl TaskExecQuery {
    /// Validates query options before the worker starts a Cap'n Proto exec session.
    pub fn validate(&self) -> Result<(), String> {
        if self.command.is_empty() {
            return Err("command must contain at least one argument".to_string());
        }
        if self.command.iter().any(|arg| arg.trim().is_empty()) {
            return Err("command arguments must not be empty".to_string());
        }
        Ok(())
    }
}

/// Returns true for enabled-by-default stream options.
fn default_true() -> bool {
    true
}

/// Returns the default task log tail request.
fn default_log_tail() -> String {
    "all".to_string()
}

/// Decodes the exec command from a URL query field.
fn deserialize_command_query<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    match CommandQueryField::deserialize(deserializer)? {
        CommandQueryField::Args(args) => Ok(args),
        CommandQueryField::Text(text) => {
            let trimmed = text.trim();
            if trimmed.starts_with('[') {
                serde_json::from_str::<Vec<String>>(trimmed).map_err(de::Error::custom)
            } else if trimmed.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(vec![text])
            }
        }
    }
}
