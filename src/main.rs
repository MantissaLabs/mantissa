use anyhow::Result;

/// Starts the CLI process by delegating to the library entrypoint.
#[tokio::main]
async fn main() -> Result<()> {
    mantissa::run_cli().await
}
