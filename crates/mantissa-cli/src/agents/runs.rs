use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

/// Lists first-class agent runs through the agents control-plane capability.
pub async fn list_runs(cfg: &ClientConfig, session_id: Option<Uuid>) -> Result<()> {
    let rows = mantissa_client::agents::list_runs(cfg, session_id).await?;
    if rows.is_empty() {
        println!("no agent runs registered");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "RUN ID\tSESSION\tSTATUS\tWORKLOAD\tEXIT\tPLATFORM\tMODE\tPROFILE\tUPDATED"
    )?;
    for row in rows {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.id,
            row.session_name,
            row.status,
            row.workload_id.unwrap_or_else(|| "-".to_string()),
            row.exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.execution_platform,
            row.isolation_mode,
            row.isolation_profile
                .unwrap_or_else(|| "default".to_string()),
            row.updated_at,
        )?;
    }
    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);
    Ok(())
}
