use crate::types::common::HostPort;
use mantissa_client::tasks::TaskRow;
use serde::{Deserialize, Serialize};

/// REST-facing task summary returned by task routes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
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
            memory_mib: value.memory_mib,
            gpu_count: value.gpu_count,
            command: value.command,
            node: value.node,
            ports: value.ports.into_iter().map(HostPort::from).collect(),
            state: value.state,
            created_at: value.created_at,
        }
    }
}

/// REST request body for starting one standalone task.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskStartRequest {
    pub name: String,
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default = "default_cpu_millis")]
    pub cpu_millis: u64,
    #[serde(default = "default_memory_bytes")]
    pub memory_bytes: u64,
    #[serde(default)]
    pub gpu_count: u32,
    #[serde(default)]
    pub volumes: Vec<String>,
}

/// REST query parameters for streaming standalone task logs.
#[derive(Clone, Debug, Deserialize)]
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
#[derive(Clone, Debug, Deserialize)]
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
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskExecQuery {
    #[serde(default)]
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

/// Returns the default CPU request for REST task submissions.
fn default_cpu_millis() -> u64 {
    1_000
}

/// Returns the default memory request for REST task submissions.
fn default_memory_bytes() -> u64 {
    536_870_912
}

/// Returns the default task log tail request.
fn default_log_tail() -> String {
    "all".to_string()
}
