use super::list::{ServiceRow, inspect_service_row};
use crate::config::ClientConfig;
use anyhow::Result;

/// Resolves one service by id or name and returns its rollout status snapshot.
pub async fn status(cfg: &ClientConfig, selector: &str) -> Result<ServiceRow> {
    inspect_service_row(cfg, selector).await
}
