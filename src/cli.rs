// src/cli.rs
use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use client::tasks::TasksListState;
use std::path::PathBuf;

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

    /// Start a container workload
    #[command(alias = "run")]
    Start(TasksStartArgs),

    /// Stop a container workload
    Stop(TasksStopArgs),
}

#[derive(Args, Debug)]
pub struct TasksListArgs {
    /// The cluster to list tasks for
    #[arg(index = 1)]
    pub cluster: Option<String>,

    /// Filter workloads by lifecycle state (repeat flag to combine)
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
    /// Friendly name for the workload
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
}

#[derive(Args, Debug)]
pub struct TasksStopArgs {
    /// Workload ID to stop (UUID)
    #[arg(index = 1, value_name = "ID")]
    pub id: String,
}

/// CLI representation of workload lifecycle states.
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
}

#[derive(Args, Debug)]
pub struct ServicesRunArgs {
    /// Path to the RON manifest describing the services to deploy
    #[arg(index = 1, value_name = "MANIFEST")]
    pub manifest: PathBuf,
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
    /// The first cluster
    #[arg(index = 1)]
    pub origin: String,

    /// The second cluster
    #[arg(index = 2)]
    pub destination: String,

    /// Print debug information verbosely
    #[arg(short = 'd', action = ArgAction::SetTrue)]
    pub debug: bool,
}

#[derive(Args, Debug)]
pub struct SplitArgs {
    /// The cluster to split into multiple sub-clusters
    #[arg(index = 1)]
    pub cluster: String,

    /// Print debug information verbosely
    #[arg(short = 'd', action = ArgAction::SetTrue)]
    pub debug: bool,
}
