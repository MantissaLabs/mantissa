use crate::config::ClientConfig;
use crate::connection;
use crate::workload::uuid_to_string;
use anyhow::Result;
use std::io::Write;

pub async fn start(cfg: &ClientConfig, name: &str, image: &str, command: &[String]) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_workload_request();
    let workload = request.send().pipeline.get_workload();
    let mut request = workload.start_request();

    {
        let mut builder = request.get().init_request();
        builder.set_name(name);
        builder.set_image(image);
        let mut cmd_builder = builder.reborrow().init_command(command.len() as u32);
        for (idx, arg) in command.iter().enumerate() {
            cmd_builder.set(idx as u32, arg);
        }
    }

    let response = request.send().promise.await?;
    let spec = response.get()?.get_spec()?;

    let id = uuid_to_string(spec.get_id()?)?;
    let state = spec.get_state()?.to_str()?.to_string();
    let node = spec.get_node_name()?.to_str()?.to_string();

    let mut command_display = Vec::new();
    for arg in spec.get_command()?.iter() {
        command_display.push(arg?.to_str()?.to_string());
    }

    let mut tw = tabwriter::TabWriter::new(Vec::new());
    writeln!(&mut tw, "ID\tNAME\tIMAGE\tCOMMAND\tNODE\tSTATUS")?;
    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}",
        id,
        spec.get_name()?.to_str()?,
        spec.get_image()?.to_str()?,
        if command_display.is_empty() {
            "-".to_string()
        } else {
            command_display.join(" ")
        },
        if node.is_empty() {
            "local".to_string()
        } else {
            node
        },
        state,
    )?;

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    println!("started workload:\n{output}");

    Ok(())
}
