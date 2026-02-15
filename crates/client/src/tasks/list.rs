use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use crate::tasks::{uuid_short, uuid_to_string};
use anyhow::Result;
use capnp::Error as CapnpError;
use protocol::task::{TaskStateFilter, task_spec};
use std::io::Write;
use tabwriter::TabWriter;

const TASK_NAME_MAX_CHARS: usize = 36;
const IMAGE_MAX_CHARS: usize = 32;
const SLOT_MAX_CHARS: usize = 20;
const NODE_MAX_CHARS: usize = 28;
const COMMAND_MAX_CHARS: usize = 64;
const CREATED_MAX_CHARS: usize = 30;

/// Output presets for `mantissa tasks list`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TasksListOutput {
    /// Compact table designed to stay readable in narrow terminals.
    Table,
    /// Extended table that includes created timestamp and container command.
    Wide,
}

/// Rendering options for `mantissa tasks list`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TasksListOptions {
    pub output: TasksListOutput,
    pub no_trunc: bool,
}

impl Default for TasksListOptions {
    fn default() -> Self {
        Self {
            output: TasksListOutput::Table,
            no_trunc: false,
        }
    }
}

pub async fn list(
    cfg: &ClientConfig,
    states: &[TasksListState],
    options: TasksListOptions,
) -> Result<()> {
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
    match options.output {
        TasksListOutput::Table => {
            writeln!(
                &mut tw,
                "ID\tNAME\tIMAGE\tSLOT\tCPU(m)\tMEM(MiB)\tGPU\tSTATUS\tNODE"
            )?;
        }
        TasksListOutput::Wide => {
            writeln!(
                &mut tw,
                "ID\tNAME\tIMAGE\tSLOT\tCPU(m)\tMEM(MiB)\tGPU\tSTATUS\tNODE\tCREATED\tCOMMAND"
            )?;
        }
    }

    for spec in specs {
        let rendered = spec.render(options.no_trunc);
        match options.output {
            TasksListOutput::Table => {
                writeln!(
                    &mut tw,
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    rendered.id,
                    rendered.name,
                    rendered.image,
                    rendered.slot,
                    rendered.cpu_millis,
                    rendered.memory_mib,
                    rendered.gpu_count,
                    rendered.state,
                    rendered.node,
                )?;
            }
            TasksListOutput::Wide => {
                writeln!(
                    &mut tw,
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    rendered.id,
                    rendered.name,
                    rendered.image,
                    rendered.slot,
                    rendered.cpu_millis,
                    rendered.memory_mib,
                    rendered.gpu_count,
                    rendered.state,
                    rendered.node,
                    rendered.created_at,
                    rendered.command,
                )?;
            }
        }
    }

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);

    Ok(())
}

struct TaskRow {
    id: String,
    name: String,
    image: String,
    slot: String,
    cpu_millis: u64,
    memory_mib: u64,
    gpu_count: u32,
    command: String,
    node: String,
    state: String,
    created_at: String,
}

struct RenderedTaskRow {
    id: String,
    name: String,
    image: String,
    slot: String,
    cpu_millis: u64,
    memory_mib: u64,
    gpu_count: u32,
    command: String,
    node: String,
    state: String,
    created_at: String,
}

impl TaskRow {
    /// Build a render-safe row where noisy fields are shortened unless `no_trunc` is requested.
    fn render(&self, no_trunc: bool) -> RenderedTaskRow {
        RenderedTaskRow {
            id: render_task_id(&self.id, no_trunc),
            name: truncate_field(&self.name, TASK_NAME_MAX_CHARS, no_trunc),
            image: truncate_field(&self.image, IMAGE_MAX_CHARS, no_trunc),
            slot: truncate_field(&self.slot, SLOT_MAX_CHARS, no_trunc),
            cpu_millis: self.cpu_millis,
            memory_mib: self.memory_mib,
            gpu_count: self.gpu_count,
            command: truncate_field(&self.command, COMMAND_MAX_CHARS, no_trunc),
            node: truncate_field(&self.node, NODE_MAX_CHARS, no_trunc),
            state: self.state.clone(),
            created_at: truncate_field(&self.created_at, CREATED_MAX_CHARS, no_trunc),
        }
    }

    /// Decode a task spec from Cap'n Proto into a printable row model.
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
        let gpu_count = spec.get_gpu_count();

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
            gpu_count,
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

/// Render task IDs in short form by default, matching common CLI table conventions.
fn render_task_id(id: &str, no_trunc: bool) -> String {
    if no_trunc {
        id.to_string()
    } else {
        id.split('-').next().unwrap_or(id).to_string()
    }
}

/// Truncate long values to keep tables readable while preserving deterministic prefixes.
fn truncate_field(value: &str, max_chars: usize, no_trunc: bool) -> String {
    if no_trunc {
        return value.to_string();
    }

    let value_len = value.chars().count();
    if value_len <= max_chars {
        return value.to_string();
    }

    if max_chars <= 3 {
        return ".".repeat(max_chars);
    }

    let keep = max_chars.saturating_sub(3);
    let prefix: String = value.chars().take(keep).collect();
    format!("{prefix}...")
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

#[cfg(test)]
mod tests {
    use super::{render_task_id, truncate_field};

    #[test]
    fn render_task_id_compacts_uuid_prefix_by_default() {
        let rendered = render_task_id("a911bc95-fe38-4509-8c6c-5edde24dd5e4", false);
        assert_eq!(rendered, "a911bc95");
    }

    #[test]
    fn render_task_id_keeps_full_value_when_no_trunc_enabled() {
        let rendered = render_task_id("a911bc95-fe38-4509-8c6c-5edde24dd5e4", true);
        assert_eq!(rendered, "a911bc95-fe38-4509-8c6c-5edde24dd5e4");
    }

    #[test]
    fn truncate_field_adds_ascii_ellipsis() {
        let rendered = truncate_field("0123456789abcdef", 10, false);
        assert_eq!(rendered, "0123456...");
    }

    #[test]
    fn truncate_field_returns_input_when_no_trunc_enabled() {
        let rendered = truncate_field("0123456789abcdef", 10, true);
        assert_eq!(rendered, "0123456789abcdef");
    }
}
