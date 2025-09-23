use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::uuid_to_string;
use anyhow::Result;
use std::io::Write;
use tabwriter::TabWriter;

#[derive(Debug, Clone)]
pub struct StartedWorkload {
    pub id: String,
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub node: String,
    pub state: String,
}

pub async fn start_with_details(
    cfg: &ClientConfig,
    name: &str,
    image: &str,
    command: &[String],
) -> Result<StartedWorkload> {
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
    let node_name = spec.get_node_name()?.to_str()?.to_string();

    let mut command_display = Vec::new();
    for arg in spec.get_command()?.iter() {
        command_display.push(arg?.to_str()?.to_string());
    }

    Ok(StartedWorkload {
        id,
        name: spec.get_name()?.to_str()?.to_string(),
        image: spec.get_image()?.to_str()?.to_string(),
        command: command_display,
        node: if node_name.is_empty() {
            "local".to_string()
        } else {
            node_name
        },
        state,
    })
}

pub async fn start(cfg: &ClientConfig, name: &str, image: &str, command: &[String]) -> Result<()> {
    let spec = start_with_details(cfg, name, image, command).await?;

    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "ID\tNAME\tIMAGE\tCOMMAND\tNODE\tSTATUS")?;
    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}",
        spec.id,
        spec.name,
        spec.image,
        if spec.command.is_empty() {
            "-".to_string()
        } else {
            spec.command.join(" ")
        },
        spec.node,
        spec.state,
    )?;

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    println!("started workload:\n{output}");

    Ok(())
}
