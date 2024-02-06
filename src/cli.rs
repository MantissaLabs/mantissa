use clap::{Arg, ArgAction, Command};

pub fn init() -> Command {
    Command::new("Mantissa")
        .about("decentralized cluster management")
        .version("0.0.1")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .author("Mantissa Labs")
        .arg(
            Arg::new("config")
                .global(true)
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
                .help("Sets the listen address"),
        )
        .arg(
            Arg::new("anchor")
                .short('a')
                .long("anchor")
                .value_name("ANCHOR")
                .help("Sets the anchor address to join the network of nodes"),
        )
        .arg(
            Arg::new("name")
                .short('n')
                .long("name")
                .value_name("MACHINE-NAME")
                .help("Sets the name of the machine"),
        )
        .arg(Arg::new("v").short('v').help("Sets the level of verbosity"))
        .subcommand(
            Command::new("bootstrap")
                .about("Bootstrap a single machine cluster")
                .arg(
                    Arg::new("debug")
                        .short('d')
                        .long("debug")
                        .help("print debug information verbosely")
                        .action(ArgAction::Set)
                        .num_args(1..),
                )
                .arg(
                    Arg::new("info")
                        .long("info")
                        .short('i')
                        .help("view package information")
                        .action(ArgAction::Set)
                        .num_args(1..),
                ),
        )
}
