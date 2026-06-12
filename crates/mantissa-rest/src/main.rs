use mantissa_rest::{config::RestConfig, server};

/// Starts the standalone local REST gateway.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RestConfig::from_env()?;
    server::serve(config).await?;
    Ok(())
}
