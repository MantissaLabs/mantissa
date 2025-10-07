use crate::config::ClientConfig;
use crate::connection;
use crate::services::manifest::{
    RestartPolicyName as ManifestRestartPolicyName, TaskRestartPolicy,
};
use crate::tasks::uuid_to_string;
use anyhow::{Result, anyhow};
use protocol::task::task_spec;

#[derive(Debug, Clone)]
pub struct StartedTask {
    pub id: String,
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub node: String,
    pub state: String,
}

#[derive(Debug, Clone)]
pub struct TaskStartParams {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub restart_policy: Option<TaskRestartPolicy>,
}

/// Run a task via the task service and return its runtime details.
pub async fn run(
    cfg: &ClientConfig,
    name: &str,
    image: &str,
    command: &[String],
) -> Result<StartedTask> {
    let params = TaskStartParams {
        name: name.to_string(),
        image: image.to_string(),
        command: command.to_vec(),
        cpu_millis: 0,
        memory_bytes: 0,
        restart_policy: None,
    };

    let mut tasks = run_many(cfg, vec![params]).await?;
    tasks
        .pop()
        .ok_or_else(|| anyhow!("task batch returned no results"))
}

pub async fn run_many(cfg: &ClientConfig, tasks: Vec<TaskStartParams>) -> Result<Vec<StartedTask>> {
    if tasks.is_empty() {
        return Ok(Vec::new());
    }

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
    let mut batch = task.start_many_request();

    {
        let mut builder = batch.get().init_requests(tasks.len() as u32);
        for (idx, task) in tasks.iter().enumerate() {
            let mut entry = builder.reborrow().get(idx as u32);
            entry.set_name(&task.name);
            entry.set_image(&task.image);
            entry.set_cpu_millis(task.cpu_millis);
            entry.set_memory_bytes(task.memory_bytes);
            entry.reborrow().init_slot_ids(0);

            let mut cmd_builder = entry.reborrow().init_command(task.command.len() as u32);
            for (cmd_idx, arg) in task.command.iter().enumerate() {
                cmd_builder.set(cmd_idx as u32, arg);
            }

            if let Some(policy) = &task.restart_policy {
                let mut policy_builder = entry.reborrow().init_restart_policy();
                let name = match policy.name {
                    ManifestRestartPolicyName::No => protocol::task::RestartPolicyName::No,
                    ManifestRestartPolicyName::Always => protocol::task::RestartPolicyName::Always,
                    ManifestRestartPolicyName::OnFailure => {
                        protocol::task::RestartPolicyName::OnFailure
                    }
                    ManifestRestartPolicyName::UnlessStopped => {
                        protocol::task::RestartPolicyName::UnlessStopped
                    }
                };
                policy_builder.set_name(name);
                policy_builder.set_max_retry_count(policy.max_retry_count.map_or(-1, |value| {
                    i32::try_from(value).expect("validated restart policy bound")
                }));
            }
        }
    }

    let response = batch.send().promise.await?;
    let reader = response.get()?;
    let specs = reader.get_specs()?;

    let mut out = Vec::with_capacity(specs.len() as usize);
    for spec in specs.iter() {
        out.push(spec_to_started_task(spec)?);
    }

    Ok(out)
}

fn spec_to_started_task(spec: task_spec::Reader<'_>) -> Result<StartedTask> {
    let id = uuid_to_string(spec.get_id()?)?;
    let state = spec.get_state()?.to_str()?.to_string();
    let node_name = spec.get_node_name()?.to_str()?.to_string();

    let mut command_display = Vec::new();
    for arg in spec.get_command()?.iter() {
        command_display.push(arg?.to_str()?.to_string());
    }

    Ok(StartedTask {
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
