use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::{uuid_short, uuid_to_string};
use anyhow::Result;
use capnp::Error as CapnpError;
use protocol::task::{TaskStateFilter, task_spec};
use std::io::Write;
use tabwriter::TabWriter;

pub async fn list(cfg: &ClientConfig, states: &[TasksListState]) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
    let mut request = task.list_request();
    {
        let mut builder = request.get().init_request();
        if !states.is_empty() {
            let mut state_builder = builder.reborrow().init_states(states.len() as u32);
            for (idx, state) in states.iter().enumerate() {
                state_builder.set(idx as u32, (*state).into());
            }
        }
    }

    let response = request.send().promise.await?;
    let tasks = response.get()?.get_tasks()?;

    let mut specs: Vec<TaskRow> = Vec::new();
    for spec in tasks.iter() {
        specs.push(TaskRow::from_reader(spec)?);
    }

    specs.sort_by(|a, b| a.name.cmp(&b.name));

    if specs.is_empty() {
        println!("no tasks found");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tIMAGE\tSLOT\tCPU(m)\tMEM(MiB)\tCOMMAND\tNODE\tSTATUS\tCREATED"
    )?;

    for spec in specs {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            spec.id,
            spec.name,
            spec.image,
            spec.slot,
            spec.cpu_millis,
            spec.memory_mib,
            spec.command,
            spec.node,
            spec.state,
            spec.created_at,
        )?;
    }

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    println!("{output}");

    Ok(())
}

struct TaskRow {
    id: String,
    name: String,
    image: String,
    slot: String,
    cpu_millis: u64,
    memory_mib: u64,
    command: String,
    node: String,
    state: String,
    created_at: String,
}

impl TaskRow {
    fn from_reader(spec: task_spec::Reader) -> Result<Self, CapnpError> {
        let id = uuid_to_string(spec.get_id()?)?;
        let name = spec.get_name()?.to_str()?.to_string();
        let image = spec.get_image()?.to_str()?.to_string();
        let state = spec.get_state()?.to_str()?.to_string();
        let created_at = spec.get_created_at()?.to_str()?.to_string();
        let node_name = spec.get_node_name()?.to_str()?.to_string();
        let node_id = uuid_short(spec.get_node_id()?)?;
        let slots_reader = spec.get_slot_ids()?;

        let slot = if slots_reader.is_empty() {
            "-".to_string()
        } else {
            let mut rendered = Vec::with_capacity(slots_reader.len() as usize);
            for slot_id in slots_reader.iter() {
                rendered.push(slot_id.to_string());
            }
            rendered.join(",")
        };

        let cpu_millis = spec.get_cpu_millis();
        let memory_mib = spec.get_memory_bytes() / (1024 * 1024);

        let mut command = Vec::new();
        for arg in spec.get_command()?.iter() {
            command.push(arg?.to_str()?.to_string());
        }

        let node = if node_name.is_empty() {
            node_id
        } else {
            format!("{node_name} ({node_id})")
        };

        Ok(Self {
            id,
            name,
            image,
            slot,
            cpu_millis,
            memory_mib,
            command: if command.is_empty() {
                "-".to_string()
            } else {
                command.join(" ")
            },
            node,
            state,
            created_at,
        })
    }
}

/// Client-side representation of the selectable task lifecycle states.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TasksListState {
    Pending,
    Creating,
    Running,
    Paused,
    Stopping,
    Stopped,
    Failed,
    Exited,
    Unknown,
}

impl From<TasksListState> for TaskStateFilter {
    fn from(value: TasksListState) -> Self {
        match value {
            TasksListState::Pending => TaskStateFilter::Pending,
            TasksListState::Creating => TaskStateFilter::Creating,
            TasksListState::Running => TaskStateFilter::Running,
            TasksListState::Paused => TaskStateFilter::Paused,
            TasksListState::Stopping => TaskStateFilter::Stopping,
            TasksListState::Stopped => TaskStateFilter::Stopped,
            TasksListState::Failed => TaskStateFilter::Failed,
            TasksListState::Exited => TaskStateFilter::Exited,
            TasksListState::Unknown => TaskStateFilter::Unknown,
        }
    }
}
