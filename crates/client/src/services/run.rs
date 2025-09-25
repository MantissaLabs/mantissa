use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::uuid_to_string;
use anyhow::{Result, anyhow};
use protocol::workload::workload_spec;

#[derive(Debug, Clone)]
pub struct StartedWorkload {
    pub id: String,
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub node: String,
    pub state: String,
}

#[derive(Debug, Clone)]
pub struct WorkloadStartParams {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
}

/// Run a workload via the workload service and return its runtime details.
pub async fn run(
    cfg: &ClientConfig,
    name: &str,
    image: &str,
    command: &[String],
) -> Result<StartedWorkload> {
    let params = WorkloadStartParams {
        name: name.to_string(),
        image: image.to_string(),
        command: command.to_vec(),
        cpu_millis: 0,
        memory_bytes: 0,
    };

    let mut workloads = run_many(cfg, vec![params]).await?;
    workloads
        .pop()
        .ok_or_else(|| anyhow!("workload batch returned no results"))
}

pub async fn run_many(
    cfg: &ClientConfig,
    workloads: Vec<WorkloadStartParams>,
) -> Result<Vec<StartedWorkload>> {
    if workloads.is_empty() {
        return Ok(Vec::new());
    }

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_workload_request();
    let workload = request.send().pipeline.get_workload();
    let mut batch = workload.start_many_request();

    {
        let mut builder = batch.get().init_requests(workloads.len() as u32);
        for (idx, workload) in workloads.iter().enumerate() {
            let mut entry = builder.reborrow().get(idx as u32);
            entry.set_name(&workload.name);
            entry.set_image(&workload.image);
            entry.set_cpu_millis(workload.cpu_millis);
            entry.set_memory_bytes(workload.memory_bytes);

            let mut cmd_builder = entry.reborrow().init_command(workload.command.len() as u32);
            for (cmd_idx, arg) in workload.command.iter().enumerate() {
                cmd_builder.set(cmd_idx as u32, arg);
            }
        }
    }

    let response = batch.send().promise.await?;
    let reader = response.get()?;
    let specs = reader.get_specs()?;

    let mut out = Vec::with_capacity(specs.len() as usize);
    for spec in specs.iter() {
        out.push(spec_to_started_workload(spec)?);
    }

    Ok(out)
}

fn spec_to_started_workload(spec: workload_spec::Reader<'_>) -> Result<StartedWorkload> {
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
