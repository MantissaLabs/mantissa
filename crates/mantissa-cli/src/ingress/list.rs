use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::io::Write;
use tabwriter::TabWriter;

/// Lists replicated ingress pools in a compact operator table.
pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let mut pools = mantissa_client::ingress::list(cfg).await?;
    if pools.is_empty() {
        output::emit_line("no ingress pools registered");
        return Ok(());
    }

    pools.sort_by(|left, right| left.name.cmp(&right.name));
    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "ID\tNAME\tMIN\tMAX\tGENERATION\tUPDATED")?;
    for pool in pools {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}",
            pool.id,
            pool.name,
            pool.min_nodes,
            pool.max_nodes_label(),
            pool.generation,
            pool.updated_at,
        )?;
    }
    tw.flush()?;
    output::emit_block(String::from_utf8(tw.into_inner()?)?);
    Ok(())
}
