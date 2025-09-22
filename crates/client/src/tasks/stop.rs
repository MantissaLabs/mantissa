use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::uuid_from_data;
use anyhow::{Result, anyhow};
use std::io::Write;
use uuid::Uuid;

pub async fn stop(cfg: &ClientConfig, id: &str) -> Result<()> {
    let id = Uuid::parse_str(id).map_err(|e| anyhow!("invalid workload id: {e}"))?;

    let client = connection::get_local_session(cfg).await?;

    let request = client.get_workload_request();
    let workload = request.send().pipeline.get_workload();
    let mut request = workload.stop_request();
    let mut builder = request.get().init_request();
    builder.set_id(id.as_bytes());

    let response = request.send().promise.await?;
    let spec = response.get()?.get_spec()?;

    let spec_id = uuid_from_data(spec.get_id()?)?;
    let mut command_display = Vec::new();
    for arg in spec.get_command()?.iter() {
        command_display.push(arg?.to_str()?.to_string());
    }

    let mut tw = tabwriter::TabWriter::new(Vec::new());
    writeln!(&mut tw, "ID\tNAME\tIMAGE\tCOMMAND\tNODE\tSTATUS")?;
    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}",
        spec_id,
        spec.get_name()?.to_str()?,
        spec.get_image()?.to_str()?,
        if command_display.is_empty() {
            "-".to_string()
        } else {
            command_display.join(" ")
        },
        spec.get_node_name()?.to_str()?,
        spec.get_state()?.to_str()?,
    )?;

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    println!("stopped workload:\n{output}");

    Ok(())
}
