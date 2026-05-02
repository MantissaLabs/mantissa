use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result};
use std::io::Write;
use tabwriter::TabWriter;

use super::parse_secret_spec;

/// List secrets registered in the cluster and render them in tabular form.
pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();
    let response = secrets_client
        .list_request()
        .send()
        .promise
        .await
        .context("secrets list request failed")?;
    let reader = response.get()?.get_secrets()?;

    let mut summaries = Vec::with_capacity(reader.len() as usize);
    for spec in reader.iter() {
        summaries.push(parse_secret_spec(spec)?);
    }

    if summaries.is_empty() {
        output::emit_line("no secrets found");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "NAME\tVERSION\tUPDATED\tDESCRIPTION")?;
    for summary in summaries {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}",
            summary.name,
            summary.version_id,
            summary.updated_at,
            summary.description.unwrap_or_default()
        )?;
    }
    tw.flush()?;
    let rendered = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(rendered);
    Ok(())
}
