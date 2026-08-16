#![recursion_limit = "256"]

use anyhow::Result;

/// Starts the CLI process by delegating to the library entrypoint.
#[tokio::main]
async fn main() -> Result<()> {
    mantissa_cli::run_cli().await
}
