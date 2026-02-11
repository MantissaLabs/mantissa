// src/cli.rs
use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use client::tasks::TasksListState;
use std::path::PathBuf;
use uuid::Uuid;

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

    /// Submit a job to the cluster
    Submit(SubmitArgs),

    /// Merge one or more existing clusters together
    Merge(MergeArgs),

    /// Split an existing cluster into multiple sub-clusters
    Split(SplitArgs),
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
}

#[derive(Subcommand, Debug)]
pub enum ClustersCommand {
    /// List known clusters and their node counts
    #[command(alias = "ls")]
    List,
}

#[derive(Args, Debug)]
pub struct NodesListArgs {
    /// The cluster to list nodes from
    #[arg(index = 1)]
    pub cluster: Option<String>,
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
}

#[derive(Args, Debug)]
pub struct TasksStopArgs {
    /// Task ID to stop (UUID)
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

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum MergeServicePolicyOpt {
    Rebalance,
    Preserve,
}
