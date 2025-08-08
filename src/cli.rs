use clap::{Arg, ArgAction, Command, Parser};

#[derive(Parser)]
struct Opts {
    /// Sets a custom config file
    #[clap(short, long, default_value = "default.conf")]
    config: String,
}

pub fn init() -> Command {
    Command::new("mantissa")
        .version("0.0.1")
        .about("Decentralized job orchestration and cluster management")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .author("Mantissa Labs")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("CONFIG")
                .help("Sets a custom config file"),
        )
        .arg(
            Arg::new("listen")
                .short('l')
                .long("listen")
                .value_name("LISTEN-ADDRESS")
                .default_value("127.0.0.1:6578")
                .help("Sets the listen address"),
        )
        .arg(
            Arg::new("anchor")
                .short('a')
                .long("anchor")
                .value_name("ANCHOR")
                .default_value("127.0.0.1:6578")
                .help("Sets the anchor address to join the network of nodes"),
        )
        .arg(
            Arg::new("name")
                .short('n')
                .long("name")
                .value_name("MACHINE-NAME")
                .help("Sets the name of the machine"),
        )
        .arg(
            Arg::new("v")
                .short('v')
                .action(ArgAction::Count)
                .help("Sets the level of verbosity"),
        )
        .subcommand(
            Command::new("init")
                .about("Initialize a single machine cluster")
                .arg(
                    Arg::new("debug")
                        .short('d')
                        .help("print debug information verbosely")
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("info")
                .about("Get system information on a machine")
                .arg(
                    Arg::new("debug")
                        .short('d')
                        .help("print debug information verbosely")
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("link")
                .about("Link a node to an existing cluster")
                .arg(
                    Arg::new("debug")
                        .short('d')
                        .help("print debug information verbosely")
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("join-token")
                        .long("join-token")
                        .value_name("TOKEN")
                        .help("Join token to authenticate with the remote anchor")
                        .num_args(1),
                ),
        )
        .subcommand(
            Command::new("nodes")
                .about("Nodes subcommands")
                .arg_required_else_help(true)
                .alias("n")
                .subcommand(
                    Command::new("list")
                        .about("List nodes in a cluster")
                        .alias("ls")
                        .arg(
                            Arg::new("cluster")
                                .help("the cluster to list nodes from")
                                .default_missing_value("")
                                .index(1),
                        ),
                ),
        )
        .subcommand(
            Command::new("token")
                .about("Token subcommands")
                .arg_required_else_help(true)
                .subcommand(Command::new("show").about("Shows the join token on this node"))
                .subcommand(Command::new("rotate").about("Rotates the token on the node")),
        )
        .subcommand(
            Command::new("tasks")
                .about("Tasks subcommands")
                .alias("t")
                .subcommand(
                    Command::new("list")
                        .about("List tasks in a cluster")
                        .alias("ls")
                        .arg(
                            Arg::new("cluster")
                                .help("the cluster to list tasks for")
                                .index(1),
                        ),
                ),
        )
        .subcommand(
            Command::new("submit")
                .about("Submit a job to the cluster")
                .arg(
                    Arg::new("input")
                        .help("the description of the task to deploy in .yml format")
                        .index(1)
                        .required(true),
                )
                .arg(
                    Arg::new("debug")
                        .short('d')
                        .help("print debug information verbosely")
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("merge")
                .about("Merge one or more existing clusters together")
                .arg(
                    Arg::new("origin")
                        .help("the first cluster")
                        .index(1)
                        .required(true),
                )
                .arg(
                    Arg::new("destination")
                        .help("the second cluster")
                        .index(2)
                        .required(true),
                )
                .arg(
                    Arg::new("debug")
                        .short('d')
                        .help("print debug information verbosely")
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("split")
                .about("Split an existing cluster into multiple sub-clusters")
                .arg(
                    Arg::new("cluster")
                        .help("the cluster to split into multiple sub-clusters")
                        .index(1)
                        .required(true),
                )
                .arg(
                    Arg::new("debug")
                        .short('d')
                        .help("print debug information verbosely")
                        .action(ArgAction::SetTrue),
                ),
        )
}
