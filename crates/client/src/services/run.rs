use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::uuid_to_string;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct StartedWorkload {
    pub id: String,
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub node: String,
    pub state: String,
}

/// Run a workload via the workload service and return its runtime details.
pub async fn run(
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
