use std::process::ExitCode;

use mantissa_sandbox::run_sandbox_init;

/// Enters the Mantissa sandbox boundary and then replaces itself with the target workload command.
fn main() -> ExitCode {
    match run_sandbox_init(std::env::args_os().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("mantissa-sandbox-init: {err}");
            ExitCode::FAILURE
        }
    }
}
