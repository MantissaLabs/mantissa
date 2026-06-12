use mantissa_rest::{
    config::{RestConfig, RestConfigError},
    server::{self, RestServerError},
};
use std::{env, ffi::OsString};

const USAGE: &str = "\
Usage:
  mantissa-rest serve
  mantissa-rest --help

Environment:
  MANTISSA_REST_ADDR              bind address, default 127.0.0.1:6579
  MANTISSA_REST_SOCKET            optional Mantissa daemon Unix socket path
  MANTISSA_REST_TOKEN             bearer token required by REST handlers
  MANTISSA_REST_INSECURE_NO_AUTH  set to true only for loopback dev use
";

/// Standalone REST binary commands.
#[derive(Debug, PartialEq, Eq)]
enum RestCliCommand {
    Serve,
    Help,
}

/// Starts the standalone local REST gateway.
#[tokio::main]
async fn main() -> Result<(), RestCliError> {
    match parse_args(env::args_os().skip(1))? {
        RestCliCommand::Serve => {
            let config = RestConfig::from_env()?;
            server::serve(config).await?;
            Ok(())
        }
        RestCliCommand::Help => {
            print!("{USAGE}");
            Ok(())
        }
    }
}

/// Parses standalone REST binary arguments.
fn parse_args<I>(args: I) -> Result<RestCliCommand, RestCliError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter();
    let Some(command) = args.next() else {
        return Err(RestCliError::Usage(format!("missing command\n\n{USAGE}")));
    };
    let command = arg_to_string(command)?;

    if let Some(extra) = args.next() {
        let extra = arg_to_string(extra)?;
        return Err(RestCliError::Usage(format!(
            "unexpected argument '{extra}'\n\n{USAGE}"
        )));
    }

    match command.as_str() {
        "serve" => Ok(RestCliCommand::Serve),
        "--help" | "-h" | "help" => Ok(RestCliCommand::Help),
        _ => Err(RestCliError::Usage(format!(
            "unknown command '{command}'\n\n{USAGE}"
        ))),
    }
}

/// Converts one OS argument into UTF-8 text for command matching.
fn arg_to_string(arg: OsString) -> Result<String, RestCliError> {
    arg.into_string()
        .map_err(|_| RestCliError::Usage(format!("arguments must be valid UTF-8\n\n{USAGE}")))
}

/// Errors returned before or during standalone REST binary execution.
#[derive(Debug)]
enum RestCliError {
    Usage(String),
    Config(RestConfigError),
    Server(RestServerError),
}

impl std::fmt::Display for RestCliError {
    /// Formats CLI startup and runtime errors for stderr.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(message) => write!(formatter, "{message}"),
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Server(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for RestCliError {}

impl From<RestConfigError> for RestCliError {
    /// Converts REST configuration failures into CLI errors.
    fn from(error: RestConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<RestServerError> for RestCliError {
    /// Converts REST server failures into CLI errors.
    fn from(error: RestServerError) -> Self {
        Self::Server(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_accepts_serve_command() {
        assert!(matches!(
            parse_args([OsString::from("serve")]),
            Ok(RestCliCommand::Serve)
        ));
    }

    #[test]
    fn parse_args_accepts_help_command() {
        assert!(matches!(
            parse_args([OsString::from("--help")]),
            Ok(RestCliCommand::Help)
        ));
    }

    #[test]
    fn parse_args_rejects_missing_command() {
        assert!(matches!(
            parse_args([]),
            Err(RestCliError::Usage(message)) if message.contains("missing command")
        ));
    }

    #[test]
    fn parse_args_rejects_extra_arguments() {
        assert!(matches!(
            parse_args([OsString::from("serve"), OsString::from("extra")]),
            Err(RestCliError::Usage(message)) if message.contains("unexpected argument")
        ));
    }
}
