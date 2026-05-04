use crate::host_ports::render_host_ports;
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::tasks::TaskRow;
pub use mantissa_client::tasks::TasksListState;
use std::io::Write;
use tabwriter::TabWriter;

const TASK_NAME_MAX_CHARS: usize = 36;
const IMAGE_MAX_CHARS: usize = 32;
const SLOT_MAX_CHARS: usize = 20;
const NODE_MAX_CHARS: usize = 28;
const COMMAND_MAX_CHARS: usize = 64;
const CREATED_MAX_CHARS: usize = 30;
const HOST_PORTS_MAX_CHARS: usize = 48;

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

/// Lists tasks and renders them according to the selected output preset.
pub async fn list(
    cfg: &ClientConfig,
    states: &[TasksListState],
    options: TasksListOptions,
) -> Result<()> {
    let mut rows = mantissa_client::tasks::list(cfg, states).await?;
    rows.sort_by(|a, b| a.name.cmp(&b.name));

    if rows.is_empty() {
        println!("no tasks found");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    match options.output {
        TasksListOutput::Table => {
            writeln!(
                &mut tw,
                "ID\tNAME\tIMAGE\tSLOT\tCPU(m)\tMEM(MiB)\tGPU\tSTATUS\tNODE\tHOST PORTS"
            )?;
        }
        TasksListOutput::Wide => {
            writeln!(
                &mut tw,
                "ID\tNAME\tIMAGE\tSLOT\tCPU(m)\tMEM(MiB)\tGPU\tSTATUS\tNODE\tHOST PORTS\tCREATED\tCOMMAND"
            )?;
        }
    }

    for row in rows {
        let rendered = render_task_row(&row, options.no_trunc);
        match options.output {
            TasksListOutput::Table => {
                writeln!(
                    &mut tw,
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    rendered.id,
                    rendered.name,
                    rendered.image,
                    rendered.slot,
                    rendered.cpu_millis,
                    rendered.memory_mib,
                    rendered.gpu_count,
                    rendered.state,
                    rendered.node,
                    rendered.host_ports,
                )?;
            }
            TasksListOutput::Wide => {
                writeln!(
                    &mut tw,
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    rendered.id,
                    rendered.name,
                    rendered.image,
                    rendered.slot,
                    rendered.cpu_millis,
                    rendered.memory_mib,
                    rendered.gpu_count,
                    rendered.state,
                    rendered.node,
                    rendered.host_ports,
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
    host_ports: String,
    state: String,
    created_at: String,
}

/// Builds a render-safe row where noisy fields are shortened unless `no_trunc` is requested.
fn render_task_row(row: &TaskRow, no_trunc: bool) -> RenderedTaskRow {
    RenderedTaskRow {
        id: render_task_id(&row.id, no_trunc),
        name: truncate_field(&row.name, TASK_NAME_MAX_CHARS, no_trunc),
        image: truncate_field(&row.image, IMAGE_MAX_CHARS, no_trunc),
        slot: truncate_field(&row.slot, SLOT_MAX_CHARS, no_trunc),
        cpu_millis: row.cpu_millis,
        memory_mib: row.memory_mib,
        gpu_count: row.gpu_count,
        command: truncate_field(&row.command, COMMAND_MAX_CHARS, no_trunc),
        node: truncate_field(&row.node, NODE_MAX_CHARS, no_trunc),
        host_ports: truncate_field(
            &render_host_ports(&row.ports),
            HOST_PORTS_MAX_CHARS,
            no_trunc,
        ),
        state: row.state.clone(),
        created_at: truncate_field(&row.created_at, CREATED_MAX_CHARS, no_trunc),
    }
}

/// Renders task IDs in short form by default, matching common CLI table conventions.
fn render_task_id(id: &str, no_trunc: bool) -> String {
    if no_trunc {
        id.to_string()
    } else {
        id.split('-').next().unwrap_or(id).to_string()
    }
}

/// Truncates long values to keep tables readable while preserving deterministic prefixes.
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
