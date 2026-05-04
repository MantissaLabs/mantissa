use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
pub use mantissa_client::jobs::{JobRunOptions, JobRunResult};
use std::io::Write;

/// Submits one first-class job and renders the submitted job summary.
pub async fn run(cfg: &ClientConfig, options: &JobRunOptions<'_>) -> Result<()> {
    let result = mantissa_client::jobs::run(cfg, options).await?;
    output::emit_block(format!("submitted job:\n{}", render_run_result(&result)?));
    Ok(())
}

/// Renders the job submission result table.
fn render_run_result(result: &JobRunResult) -> Result<String> {
    let mut tw = tabwriter::TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tIMAGE\tCPU(m)\tMEM(MiB)\tGPU\tPLATFORM\tMODE\tPROFILE\tRETRIES"
    )?;
    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        result.id,
        result.name,
        result.image,
        result.cpu_millis,
        result.memory_mib,
        result.gpu_count,
        result.execution_platform,
        result.isolation_mode,
        result.isolation_profile.as_deref().unwrap_or("default"),
        result.max_retries,
    )?;
    tw.flush()?;
    Ok(String::from_utf8(tw.into_inner()?)?)
}
