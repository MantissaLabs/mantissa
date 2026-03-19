// src/cli.rs
use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use client::tasks::{TasksListOutput, TasksListState};
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

/// Parses CLI durations used by maintenance commands, defaulting bare integers to seconds.
fn parse_cli_duration(raw: &str) -> Result<Duration, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("duration must not be empty".to_string());
    }

    let (digits, unit) = if let Some(value) = trimmed.strip_suffix("ms") {
        (value, "ms")
    } else if let Some(value) = trimmed.strip_suffix('s') {
        (value, "s")
    } else if let Some(value) = trimmed.strip_suffix('m') {
        (value, "m")
    } else if let Some(value) = trimmed.strip_suffix('h') {
        (value, "h")
    } else {
        (trimmed, "s")
    };

    let value = digits
        .trim()
        .parse::<u64>()
        .map_err(|err| format!("invalid duration '{raw}': {err}"))?;

    match unit {
        "ms" => Ok(Duration::from_millis(value)),
        "s" => Ok(Duration::from_secs(value)),
        "m" => value
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("duration '{raw}' is too large")),
        "h" => value
            .checked_mul(60)
            .and_then(|minutes| minutes.checked_mul(60))
            .map(Duration::from_secs)
            .ok_or_else(|| format!("duration '{raw}' is too large")),
        _ => Err(format!("unsupported duration unit in '{raw}'")),
    }
}

/// Parses `docker logs`-style tail values (`all` or a non-negative integer).
fn parse_log_tail(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("tail must not be empty".to_string());
    }

    if trimmed.eq_ignore_ascii_case("all") {
        return Ok("all".to_string());
    }

    trimmed
        .parse::<u64>()
        .map(|_| trimmed.to_string())
        .map_err(|err| format!("invalid tail '{raw}': {err}"))
}

#[derive(Parser, Debug)]
#[command(
    name = "mantissa",
    version = "0.0.1",
    author = "Mantissa Labs",
    about = "Decentralized job orchestration and cluster management",
    subcommand_required = true,
    arg_required_else_help = true
)]
pub struct MantissaCli {
    /// Sets a custom config file
    #[arg(short = 'c', long = "config", value_name = "CONFIG")]
    pub config: Option<String>,

    /// Sets the listen address
    #[arg(
        short = 'l',
        long = "listen",
        value_name = "LISTEN-ADDRESS",
        default_value = "0.0.0.0:6578"
    )]
    pub listen: String,

    /// Sets the name of the machine
    #[arg(short = 'n', long = "name", value_name = "MACHINE-NAME")]
    pub name: Option<String>,

    /// Sets the level of verbosity (-v, -vv, -vvv)
    #[arg(short = 'v', action = ArgAction::Count)]
    pub verbosity: u8,

    #[command(subcommand)]
    pub cmd: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Initialize a single machine cluster
    Init(InitArgs),

    /// Get system information on a machine
    Info(InfoArgs),

    /// Link a node to an existing cluster
    Link(LinkArgs),

    /// Leave a node to an existing cluster
    Leave(LeaveArgs),

    /// Nodes subcommands
    #[command(alias = "n", subcommand_required = true, arg_required_else_help = true)]
    Nodes {
        #[command(subcommand)]
        cmd: NodesCommand,
    },

    /// Clusters subcommands
    #[command(subcommand_required = true, arg_required_else_help = true)]
    Clusters {
        #[command(subcommand)]
        cmd: ClustersCommand,
    },

    /// Token subcommands
    #[command(subcommand_required = true, arg_required_else_help = true)]
    Token {
        #[command(subcommand)]
        cmd: TokenCommand,
    },

    /// Tasks subcommands
    #[command(alias = "t")]
    Tasks {
        #[command(subcommand)]
        cmd: TasksCommand,
    },

    /// Scheduler inspection subcommands
    #[command(subcommand_required = true, arg_required_else_help = true)]
    Scheduler {
        #[command(subcommand)]
        cmd: SchedulerCommand,
    },

    /// Configuration inspection subcommands
    #[command(subcommand_required = true, arg_required_else_help = true)]
    Config {
        #[command(subcommand)]
        cmd: ConfigCommand,
    },

    /// Service deployment subcommands
    #[command(
        alias = "svc",
        subcommand_required = true,
        arg_required_else_help = true
    )]
    Services {
        #[command(subcommand)]
        cmd: ServicesCommand,
    },

    /// Secrets management subcommands
    #[command(subcommand_required = true, arg_required_else_help = true)]
    Secrets {
        #[command(subcommand)]
        cmd: SecretsCommand,
    },

    /// Network management subcommands
    #[command(
        alias = "net",
        subcommand_required = true,
        arg_required_else_help = true
    )]
    Networks {
        #[command(subcommand)]
        cmd: NetworksCommand,
    },

    /// Volume management subcommands
    #[command(
        alias = "vol",
        subcommand_required = true,
        arg_required_else_help = true
    )]
    Volumes {
        #[command(subcommand)]
        cmd: VolumesCommand,
    },

    /// Submit a job to the cluster
    Submit(SubmitArgs),
}

#[derive(Args, Debug)]
pub struct InitArgs {
    /// Print debug information verbosely
    #[arg(short = 'd', action = ArgAction::SetTrue)]
    pub debug: bool,
}

#[derive(Args, Debug)]
pub struct InfoArgs {
    /// Print debug information verbosely
    #[arg(short = 'd', action = ArgAction::SetTrue)]
    pub debug: bool,
}

#[derive(Args, Debug)]
pub struct LinkArgs {
    /// Sets the anchor address to join the network of nodes
    #[arg(
        short = 'a',
        long = "anchor",
        value_name = "ANCHOR",
        default_value = "0.0.0.0:6578"
    )]
    pub anchor: String,

    /// Join token to authenticate with the remote anchor
    #[arg(long = "join-token", value_name = "TOKEN")]
    pub join_token: Option<String>,

    /// Print debug information verbosely
    #[arg(short = 'd', action = ArgAction::SetTrue)]
    pub debug: bool,
}

#[derive(Args, Debug)]
pub struct LeaveArgs {
    /// Print debug information verbosely
    #[arg(short = 'd', action = ArgAction::SetTrue)]
    pub debug: bool,
}

#[derive(Subcommand, Debug)]
pub enum NodesCommand {
    /// List nodes in a cluster
    #[command(alias = "ls")]
    List(NodesListArgs),

    /// Show detailed drain state for one node
    Status(NodesStatusArgs),

    /// Mark one node unschedulable for maintenance
    Drain(NodesDrainArgs),

    /// Clear maintenance fencing for one node
    Resume(NodesResumeArgs),
}

#[derive(Subcommand, Debug)]
pub enum ClustersCommand {
    /// List known clusters and their node counts
    #[command(alias = "ls")]
    List,

    /// Set or update a friendly name for one cluster lineage
    Name(ClusterNameArgs),

    /// Merge one or more existing clusters together
    Merge(MergeArgs),

    /// Split an existing cluster into multiple sub-clusters
    Split(SplitArgs),
}

#[derive(Args, Debug)]
pub struct NodesListArgs {
    /// The cluster to list nodes from
    #[arg(index = 1)]
    pub cluster: Option<String>,
}

#[derive(Args, Debug)]
pub struct NodesStatusArgs {
    /// Identifier of the node to inspect
    #[arg(index = 1, value_name = "NODE-ID")]
    pub node_id: Uuid,
}

#[derive(Args, Debug)]
pub struct NodesDrainArgs {
    /// Identifier of the node to drain
    #[arg(index = 1, value_name = "NODE-ID")]
    pub node_id: Uuid,

    /// Optional operator-supplied maintenance reason
    #[arg(long = "reason", value_name = "TEXT")]
    pub reason: Option<String>,

    /// Override task terminationGracePeriod while this node is draining
    #[arg(
        long = "task-stop-timeout",
        value_name = "DURATION",
        value_parser = parse_cli_duration
    )]
    pub task_stop_timeout: Option<Duration>,

    /// Maximum time to wait for the node to finish draining
    #[arg(
        long = "timeout",
        value_name = "DURATION",
        default_value = "10m",
        value_parser = parse_cli_duration
    )]
    pub timeout: Duration,

    /// Return after fencing the node instead of waiting for full drain completion
    #[arg(long = "no-wait", action = ArgAction::SetTrue)]
    pub no_wait: bool,
}

#[derive(Args, Debug)]
pub struct NodesResumeArgs {
    /// Identifier of the node to resume
    #[arg(index = 1, value_name = "NODE-ID")]
    pub node_id: Uuid,
}

#[derive(Subcommand, Debug)]
pub enum TokenCommand {
    /// Shows the join token on this node
    Show,
    /// Rotates the token on the node
    Rotate,
}

#[derive(Subcommand, Debug)]
pub enum TasksCommand {
    /// List tasks in a cluster
    #[command(alias = "ls")]
    List(TasksListArgs),

    /// Stream logs for a task
    Logs(TasksLogsArgs),

    /// Start a container task
    #[command(alias = "run")]
    Start(TasksStartArgs),

    /// Stop a container task
    Stop(TasksStopArgs),
}

#[derive(Args, Debug)]
pub struct TasksListArgs {
    /// The cluster to list tasks for
    #[arg(index = 1)]
    pub cluster: Option<String>,

    /// Output format (`table` is compact, `wide` includes command and created timestamp)
    #[arg(
        short = 'o',
        long = "output",
        value_enum,
        default_value_t = TasksListOutputOpt::Table,
        value_name = "FORMAT"
    )]
    pub output: TasksListOutputOpt,

    /// Disable compact truncation for long columns
    #[arg(long = "no-trunc", action = ArgAction::SetTrue)]
    pub no_trunc: bool,

    /// Filter tasks by lifecycle state (repeat flag to combine)
    #[arg(
        long = "state",
        value_enum,
        action = ArgAction::Append,
        value_name = "STATE"
    )]
    pub states: Vec<TasksListStateOpt>,
}

#[derive(Args, Debug)]
pub struct TasksStartArgs {
    /// Friendly name for the task
    #[arg(index = 1, value_name = "NAME")]
    pub name: String,

    /// Container image to run
    #[arg(short = 'i', long = "image", value_name = "IMAGE")]
    pub image: String,

    /// Command arguments for the container (repeat flag to add arguments)
    #[arg(short = 'c', long = "command", value_name = "ARG", action = ArgAction::Append)]
    pub command: Vec<String>,

    /// CPU requested in milli-CPUs (e.g. 500 = 0.5 vCPU)
    #[arg(long = "cpu-millis", value_name = "MCPU", default_value = "1000")]
    pub cpu_millis: u64,

    /// Memory requested in bytes
    #[arg(
        long = "memory-bytes",
        value_name = "BYTES",
        default_value = "536870912"
    )]
    pub memory_bytes: u64,

    /// GPU count requested
    #[arg(long = "gpu-count", value_name = "COUNT", default_value = "0")]
    pub gpu_count: u32,

    /// Named volume mount in SOURCE:TARGET[:ro|rw] form (repeat flag to add multiple mounts)
    #[arg(long = "volume", value_name = "MOUNT", action = ArgAction::Append)]
    pub volumes: Vec<String>,
}

#[derive(Args, Debug)]
pub struct TasksLogsArgs {
    /// Task ID or unique prefix to stream logs for
    #[arg(index = 1, value_name = "ID")]
    pub id: String,

    /// Follow the log stream until the task/container stops
    #[arg(short = 'f', long = "follow", action = ArgAction::SetTrue)]
    pub follow: bool,

    /// Number of lines to show from the end of the log, or `all`
    #[arg(
        short = 'n',
        long = "tail",
        value_name = "LINES",
        default_value = "all",
        value_parser = parse_log_tail
    )]
    pub tail: String,

    /// Include stdout log frames
    #[arg(long = "stdout", action = ArgAction::SetTrue)]
    pub stdout: bool,

    /// Include stderr log frames
    #[arg(long = "stderr", action = ArgAction::SetTrue)]
    pub stderr: bool,

    /// Prefix each log line with its timestamp when supported by the runtime
    #[arg(long = "timestamps", action = ArgAction::SetTrue)]
    pub timestamps: bool,
}

#[derive(Args, Debug)]
pub struct TasksStopArgs {
    /// Task ID or unique prefix to stop
    #[arg(index = 1, value_name = "ID")]
    pub id: String,
}

/// CLI representation of task lifecycle states.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum TasksListStateOpt {
    Pending,
    Creating,
    Running,
    Paused,
    Stopping,
    Stopped,
    Failed,
    Exited,
    Unknown,
}

/// CLI representation of the `tasks list` output presets.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum TasksListOutputOpt {
    Table,
    Wide,
}

impl From<TasksListOutputOpt> for TasksListOutput {
    fn from(value: TasksListOutputOpt) -> Self {
        match value {
            TasksListOutputOpt::Table => TasksListOutput::Table,
            TasksListOutputOpt::Wide => TasksListOutput::Wide,
        }
    }
}

impl From<TasksListStateOpt> for TasksListState {
    fn from(value: TasksListStateOpt) -> Self {
        match value {
            TasksListStateOpt::Pending => TasksListState::Pending,
            TasksListStateOpt::Creating => TasksListState::Creating,
            TasksListStateOpt::Running => TasksListState::Running,
            TasksListStateOpt::Paused => TasksListState::Paused,
            TasksListStateOpt::Stopping => TasksListState::Stopping,
            TasksListStateOpt::Stopped => TasksListState::Stopped,
            TasksListStateOpt::Failed => TasksListState::Failed,
            TasksListStateOpt::Exited => TasksListState::Exited,
            TasksListStateOpt::Unknown => TasksListState::Unknown,
        }
    }
}

#[derive(Subcommand, Debug)]
pub enum SchedulerCommand {
    /// Show slot usage for a node
    #[command(alias = "ls")]
    Slots(SchedulerSlotsArgs),
}

#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Show the currently loaded configuration.
    Show,
    /// Validate the resolved configuration and exit.
    Validate,
    /// Print the config file path being used (if any).
    Path,
}

#[derive(Args, Debug)]
pub struct SchedulerSlotsArgs {
    /// Optional peer ID (UUID). Defaults to the local node when omitted.
    #[arg(index = 1, value_name = "PEER-ID")]
    pub peer_id: Option<String>,

    /// Include per-slot details
    #[arg(long = "details", action = ArgAction::SetTrue)]
    pub details: bool,
}

#[derive(Subcommand, Debug)]
pub enum ServicesCommand {
    /// Deploy a service manifest from a RON description
    #[command(alias = "apply")]
    Run(ServicesRunArgs),

    /// List desired services and their configuration
    #[command(alias = "ls")]
    List(ServicesListArgs),

    /// Inspect rollout progress and failure details for one service
    Rollout {
        #[command(subcommand)]
        cmd: ServicesRolloutCommand,
    },

    /// Stop a service and all associated tasks
    Stop(ServicesStopArgs),
}

#[derive(Args, Debug)]
pub struct ServicesRunArgs {
    /// Path to the RON manifest describing the services to deploy
    #[arg(index = 1, value_name = "MANIFEST")]
    pub manifest: PathBuf,
}

#[derive(Args, Debug, Default)]
pub struct ServicesListArgs {}

#[derive(Args, Debug)]
pub struct ServicesStopArgs {
    /// Service ID (UUID)
    #[arg(index = 1, value_name = "ID")]
    pub id: String,
}

#[derive(Subcommand, Debug)]
pub enum ServicesRolloutCommand {
    /// Show current rollout progress and diagnostics for one service
    Status(ServicesRolloutStatusArgs),
}

#[derive(Args, Debug)]
pub struct ServicesRolloutStatusArgs {
    /// Service ID (UUID) or service name
    #[arg(index = 1, value_name = "SERVICE")]
    pub service: String,
}

#[derive(Subcommand, Debug)]
pub enum SecretsCommand {
    /// Create a new secret or replace existing metadata
    Create(SecretsCreateArgs),

    /// Update an existing secret value
    Update(SecretsCreateArgs),

    /// List available secrets
    #[command(alias = "ls")]
    List,

    /// Delete secrets by name
    Delete(SecretsDeleteArgs),

    /// Show the latest secret value
    Show(SecretsShowArgs),

    /// Rotate the cluster-wide secret master key
    RotateMasterKey,
}

#[derive(Args, Debug)]
pub struct SecretsCreateArgs {
    /// Secret name
    #[arg(index = 1, value_name = "NAME")]
    pub name: String,

    /// Plaintext value (if omitted, read from stdin)
    #[arg(long = "value", short = 'v')]
    pub value: Option<String>,

    /// Description attached to the secret
    #[arg(long = "description")]
    pub description: Option<String>,

    /// Optional labels in KEY=VALUE form (repeat flag to add multiple labels)
    #[arg(long = "label", value_name = "KEY=VALUE", action = ArgAction::Append)]
    pub labels: Vec<String>,
}

#[derive(Args, Debug)]
pub struct SecretsDeleteArgs {
    /// Secret names to delete
    #[arg(required = true, value_name = "NAME")]
    pub names: Vec<String>,
}

#[derive(Args, Debug)]
pub struct SecretsShowArgs {
    /// Secret name to display
    #[arg(index = 1, value_name = "NAME")]
    pub name: String,

    /// Optional secret version (UUID) to display
    #[arg(long = "version")]
    pub version: Option<Uuid>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum NetworkDriverOpt {
    Vxlan,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum VolumeBindingOpt {
    Immediate,
    WaitForFirstConsumer,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum VolumeReclaimOpt {
    Retain,
    Delete,
}

#[derive(Subcommand, Debug)]
pub enum NetworksCommand {
    /// Create a new overlay network in the cluster
    Create(NetworksCreateArgs),

    /// Delete one or more overlay networks
    Delete(NetworksDeleteArgs),

    /// List configured overlay networks
    #[command(alias = "ls")]
    List(NetworksListArgs),

    /// Inspect a specific network and its attached peers
    Inspect(NetworksInspectArgs),

    /// Show per-peer readiness for a network
    Status(NetworksStatusArgs),

    /// List network attachments and their assigned addresses
    Attachments(NetworksAttachmentsArgs),
}

#[derive(Subcommand, Debug)]
pub enum VolumesCommand {
    /// Create a managed local volume
    Create(VolumesCreateArgs),

    /// Import an existing host path as a local volume
    Import(VolumesImportArgs),

    /// List known volumes
    #[command(alias = "ls")]
    List,

    /// Inspect the canonical volume object
    Inspect(VolumesInspectArgs),

    /// Show node-local realization state for one volume
    Status(VolumesStatusArgs),

    /// Delete a volume object
    Delete(VolumesDeleteArgs),
}

#[derive(Args, Debug)]
pub struct NetworksCreateArgs {
    /// Human-friendly network name
    #[arg(long = "name", value_name = "NAME")]
    pub name: String,

    /// Optional description for operators
    #[arg(long = "description", value_name = "TEXT")]
    pub description: Option<String>,

    /// Overlay driver to use
    #[arg(long = "driver", value_enum, default_value = "vxlan")]
    pub driver: NetworkDriverOpt,

    /// CIDR range allocated to the overlay (e.g. 10.24.0.0/16).
    /// Defaults to 10.42.0.0/16 when omitted.
    #[arg(long = "subnet", value_name = "CIDR")]
    pub subnet: Option<String>,

    /// Explicit VXLAN identifier (auto-assigned when omitted)
    #[arg(long = "vni", value_name = "VNI")]
    pub vni: Option<u32>,

    /// MTU for the overlay (0 uses driver default)
    #[arg(long = "mtu", value_name = "BYTES")]
    pub mtu: Option<u32>,

    /// Optional BPF program identifiers to attach (repeat flag).
    /// Defaults to the standard VXLAN and bridge programs; provided entries are appended.
    #[arg(long = "bpf-program", value_name = "PROGRAM", action = ArgAction::Append)]
    pub bpf_programs: Vec<String>,

    /// Mark the network spec read-only after creation
    #[arg(long = "sealed", action = ArgAction::SetTrue)]
    pub sealed: bool,
}

impl NetworksCreateArgs {
    /// Resolve the subnet CIDR for network creation, falling back to the shared VXLAN range when
    /// the caller does not provide an explicit value.
    pub fn resolved_subnet(&self) -> String {
        self.subnet
            .clone()
            .unwrap_or_else(|| client::networks::DEFAULT_NETWORK_SUBNET.to_string())
    }

    /// Merge user-provided programs with the defaults so dataplane maps and load-balancing remain
    /// available even when no BPF flags are specified on the CLI.
    pub fn resolved_bpf_programs(&self) -> Vec<String> {
        let mut programs = client::networks::default_network_bpf_programs();
        programs.extend(self.bpf_programs.iter().cloned());
        programs.sort();
        programs.dedup();
        programs
    }
}

#[derive(Args, Debug)]
pub struct NetworksDeleteArgs {
    /// Network UUIDs to delete
    #[arg(index = 1, value_name = "ID", required = true, num_args = 1..)]
    pub ids: Vec<String>,
}

#[derive(Args, Debug, Default)]
pub struct NetworksListArgs {}

#[derive(Args, Debug)]
pub struct NetworksInspectArgs {
    /// Network UUID to inspect
    #[arg(index = 1, value_name = "ID")]
    pub id: String,
}

#[derive(Args, Debug)]
pub struct NetworksStatusArgs {
    /// Network UUID to query
    #[arg(index = 1, value_name = "ID")]
    pub id: String,
}

#[derive(Args, Debug)]
pub struct NetworksAttachmentsArgs {
    /// Network UUID whose attachments should be listed
    #[arg(index = 1, value_name = "ID")]
    pub id: String,
}

#[derive(Args, Debug)]
pub struct VolumesCreateArgs {
    /// Human-friendly volume name
    #[arg(long = "name", value_name = "NAME")]
    pub name: String,

    /// Volume binding policy
    #[arg(
        long = "binding",
        value_enum,
        default_value = "wait-for-first-consumer"
    )]
    pub binding: VolumeBindingOpt,

    /// Volume reclaim policy
    #[arg(long = "reclaim", value_enum, default_value = "retain")]
    pub reclaim: VolumeReclaimOpt,

    /// Optional capacity hint in MiB
    #[arg(long = "capacity-mb", value_name = "MIB")]
    pub capacity_mb: Option<u64>,

    /// Optional labels in KEY=VALUE form (repeat flag to add multiple labels)
    #[arg(long = "label", value_name = "KEY=VALUE", action = ArgAction::Append)]
    pub labels: Vec<String>,

    /// Bound node selector when using immediate binding (UUID or hostname)
    #[arg(long = "node", value_name = "NODE")]
    pub node: Option<String>,
}

#[derive(Args, Debug)]
pub struct VolumesImportArgs {
    /// Human-friendly volume name
    #[arg(long = "name", value_name = "NAME")]
    pub name: String,

    /// Node selector hosting the imported path (UUID or hostname)
    #[arg(long = "node", value_name = "NODE")]
    pub node: String,

    /// Absolute host path to import
    #[arg(long = "path", value_name = "PATH")]
    pub path: PathBuf,

    /// Optional capacity hint in MiB
    #[arg(long = "capacity-mb", value_name = "MIB")]
    pub capacity_mb: Option<u64>,

    /// Optional labels in KEY=VALUE form (repeat flag to add multiple labels)
    #[arg(long = "label", value_name = "KEY=VALUE", action = ArgAction::Append)]
    pub labels: Vec<String>,
}

#[derive(Args, Debug)]
pub struct VolumesInspectArgs {
    /// Volume UUID or name to inspect
    #[arg(index = 1, value_name = "ID-OR-NAME")]
    pub selector: String,
}

#[derive(Args, Debug)]
pub struct VolumesStatusArgs {
    /// Volume UUID or name to query
    #[arg(index = 1, value_name = "ID-OR-NAME")]
    pub selector: String,
}

#[derive(Args, Debug)]
pub struct VolumesDeleteArgs {
    /// Volume UUID or name to delete
    #[arg(index = 1, value_name = "ID-OR-NAME")]
    pub selector: String,
}

#[derive(Args, Debug)]
pub struct SubmitArgs {
    /// The description of the task to deploy in .yml format
    #[arg(index = 1)]
    pub input: String,

    /// Print debug information verbosely
    #[arg(short = 'd', action = ArgAction::SetTrue)]
    pub debug: bool,
}

#[derive(Args, Debug)]
pub struct MergeArgs {
    /// Source cluster lineage identifier (`CLUSTER_UUID`)
    #[arg(index = 1, value_name = "SOURCE_CLUSTER_ID")]
    pub source_cluster_id: String,

    /// Destination cluster lineage identifier (`CLUSTER_UUID`)
    #[arg(index = 2, value_name = "DESTINATION_CLUSTER_ID")]
    pub destination_cluster_id: String,

    /// Validate and record the operation without applying control-plane changes.
    #[arg(long = "dry-run", action = ArgAction::SetTrue)]
    pub dry_run: bool,

    /// Service behavior policy applied when the merge commits.
    #[arg(long = "services", value_enum, default_value = "rebalance")]
    pub services: MergeServicePolicyOpt,

    /// Print debug information verbosely
    #[arg(short = 'd', action = ArgAction::SetTrue)]
    pub debug: bool,
}

#[derive(Args, Debug)]
pub struct ClusterNameArgs {
    /// Cluster lineage identifier (`CLUSTER_UUID`)
    #[arg(index = 1, value_name = "CLUSTER_ID")]
    pub cluster_id: String,

    /// Friendly name to apply to this cluster lineage
    #[arg(index = 2, value_name = "NAME")]
    pub name: String,
}

#[derive(Args, Debug)]
pub struct SplitArgs {
    /// Optional cluster lineage identifier (`CLUSTER_UUID`).
    /// When omitted, the local active cluster is used.
    #[arg(long = "cluster", value_name = "CLUSTER_ID")]
    pub cluster: Option<String>,

    /// Start an interactive split planner with left/right node assignment.
    #[arg(
        long = "interactive",
        action = ArgAction::SetTrue,
        conflicts_with_all = ["filter_per_gpu", "by", "values", "remainder_name"]
    )]
    pub interactive: bool,

    /// Built-in simple split by GPU vendor list (example: --filter-per-gpu NVIDIA,AMD).
    #[arg(
        long = "filter-per-gpu",
        value_name = "VENDORS",
        value_delimiter = ',',
        num_args = 1..,
        conflicts_with_all = ["by", "values", "interactive"]
    )]
    pub filter_per_gpu: Vec<String>,

    /// Generic split selector kind.
    #[arg(
        long = "by",
        value_enum,
        value_name = "FILTER",
        required_unless_present_any = ["filter_per_gpu", "interactive"]
    )]
    pub by: Option<SplitFilterOpt>,

    /// Comma-separated selector values matched by `--by` (example: --values Intel,AMD).
    #[arg(
        long = "values",
        value_name = "VALUES",
        value_delimiter = ',',
        num_args = 1..,
        required_unless_present_any = ["filter_per_gpu", "interactive"]
    )]
    pub values: Vec<String>,

    /// Name for the automatic fallback split target when nodes do not match any listed value.
    #[arg(
        long = "remainder-name",
        value_name = "NAME",
        default_value = "other",
        conflicts_with = "interactive"
    )]
    pub remainder_name: String,

    /// Left partition name used by `--interactive`.
    #[arg(long = "left-name", value_name = "NAME", default_value = "left")]
    pub left_name: String,

    /// Right partition name used by `--interactive`.
    #[arg(long = "right-name", value_name = "NAME", default_value = "right")]
    pub right_name: String,

    /// Validate and record the operation without applying control-plane changes.
    #[arg(long = "dry-run", action = ArgAction::SetTrue)]
    pub dry_run: bool,

    /// Service behavior policy applied when the split commits.
    #[arg(long = "services", value_enum, default_value = "partitioned")]
    pub services: SplitServicePolicyOpt,

    /// Overlay/network behavior policy applied when the split commits.
    #[arg(long = "networks", value_enum, default_value = "isolate")]
    pub networks: SplitNetworkPolicyOpt,

    /// Print debug information verbosely
    #[arg(short = 'd', action = ArgAction::SetTrue)]
    pub debug: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SplitFilterOpt {
    GpuVendor,
    GpuModel,
    CpuVendor,
    CpuBrand,
    GpuCount,
    CpuCores,
    CpuLogical,
    MemoryTotalKb,
    MemoryTotalBytes,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SplitServicePolicyOpt {
    Partitioned,
    Preserve,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SplitNetworkPolicyOpt {
    Isolate,
    Preserve,
}

impl From<SplitFilterOpt> for client::clusters::SplitFilterKind {
    /// Convert CLI split-filter selectors to client split-filter selectors.
    fn from(value: SplitFilterOpt) -> Self {
        match value {
            SplitFilterOpt::GpuVendor => client::clusters::SplitFilterKind::GpuVendor,
            SplitFilterOpt::GpuModel => client::clusters::SplitFilterKind::GpuModel,
            SplitFilterOpt::CpuVendor => client::clusters::SplitFilterKind::CpuVendor,
            SplitFilterOpt::CpuBrand => client::clusters::SplitFilterKind::CpuBrand,
            SplitFilterOpt::GpuCount => client::clusters::SplitFilterKind::GpuCount,
            SplitFilterOpt::CpuCores => client::clusters::SplitFilterKind::CpuCores,
            SplitFilterOpt::CpuLogical => client::clusters::SplitFilterKind::CpuLogical,
            SplitFilterOpt::MemoryTotalKb => client::clusters::SplitFilterKind::MemoryTotalKb,
            SplitFilterOpt::MemoryTotalBytes => client::clusters::SplitFilterKind::MemoryTotalBytes,
        }
    }
}

impl From<SplitServicePolicyOpt> for client::clusters::SplitServicePolicy {
    /// Convert CLI split service-policy options to client split service-policy values.
    fn from(value: SplitServicePolicyOpt) -> Self {
        match value {
            SplitServicePolicyOpt::Partitioned => client::clusters::SplitServicePolicy::Partitioned,
            SplitServicePolicyOpt::Preserve => client::clusters::SplitServicePolicy::Preserve,
        }
    }
}

impl From<SplitNetworkPolicyOpt> for client::clusters::SplitNetworkPolicy {
    /// Convert CLI split network-policy options to client split network-policy values.
    fn from(value: SplitNetworkPolicyOpt) -> Self {
        match value {
            SplitNetworkPolicyOpt::Isolate => client::clusters::SplitNetworkPolicy::Isolate,
            SplitNetworkPolicyOpt::Preserve => client::clusters::SplitNetworkPolicy::Preserve,
        }
    }
}

impl From<SplitArgs> for client::clusters::SplitCommandRequest {
    /// Convert split CLI arguments into the client request consumed by split orchestration.
    fn from(value: SplitArgs) -> Self {
        Self {
            source_cluster_id: value.cluster,
            interactive: value.interactive,
            filter_per_gpu: value.filter_per_gpu,
            filter: value.by.map(Into::into),
            values: value.values,
            remainder_name: value.remainder_name,
            left_name: value.left_name,
            right_name: value.right_name,
            dry_run: value.dry_run,
            service_policy: value.services.into(),
            network_policy: value.networks.into(),
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum MergeServicePolicyOpt {
    Rebalance,
    Preserve,
}
