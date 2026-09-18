use crate::tasks::{self, TaskExecOptions};
use anyhow::{Context, Result};
use mantissa_client::{config::ClientConfig, services::select_exec_task};
use std::io::{self, Write};

/// Resolves one replica, reports its identity, and reuses task exec for the terminal session.
pub async fn exec(
    cfg: &ClientConfig,
    selector: &str,
    template: Option<&str>,
    replica: Option<u16>,
    options: &TaskExecOptions<'_>,
) -> Result<()> {
    let task = select_exec_task(cfg, selector, template, replica).await?;
    writeln!(
        io::stderr().lock(),
        "executing in {}/{} [{}]",
        task.template,
        task.replica,
        task.id
    )?;

    // Retrying on another replica could run a command twice after a transport failure.
    tasks::exec(cfg, &task.id, options)
        .await
        .with_context(|| format!("could not execute command in task '{}'", task.id))
}
