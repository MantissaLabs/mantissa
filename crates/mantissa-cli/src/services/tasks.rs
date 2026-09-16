use crate::{
    output,
    tasks::list::{TasksListOptions, TasksListOutput, render_task_id, truncate_field},
};
use anyhow::Result;
use mantissa_client::{config::ClientConfig, services::ServiceTaskRow};
use std::io::Write;
use tabwriter::TabWriter;

/// Lists actual service replicas, including retained failures that aggregate service status can hide.
pub async fn tasks(cfg: &ClientConfig, selector: &str, options: TasksListOptions) -> Result<()> {
    let rows = mantissa_client::services::tasks(cfg, selector).await?;
    if rows.is_empty() {
        output::emit_line("no tasks found for service");
        return Ok(());
    }

    let rendered = render_tasks(&rows, options)?;
    output::emit_block(rendered);

    Ok(())
}

/// Keeps replica identity and diagnostics visible while reserving longer fields for wide output.
fn render_tasks(rows: &[ServiceTaskRow], options: TasksListOptions) -> Result<String> {
    let mut table = TabWriter::new(Vec::new());
    let wide = options.output == TasksListOutput::Wide;
    let extra_headers = if wide {
        "\tNAME\tIMAGE\tCREATED\tUPDATED"
    } else {
        ""
    };
    writeln!(
        table,
        "ID\tTEMPLATE\tREPLICA\tGENERATION\tSTATUS\tNODE\tDETAIL{extra_headers}"
    )?;

    for row in rows {
        let id = render_task_id(&row.id, options.no_trunc);
        let template = truncate_field(&row.template, 24, options.no_trunc);
        let state = match row.exit_code {
            Some(code) => format!("{} ({code})", row.state),
            None => row.state.replace('_', " "),
        };
        let node = if row.node_name.is_empty() {
            render_task_id(&row.node_id, options.no_trunc)
        } else {
            truncate_field(&row.node_name, 28, options.no_trunc)
        };
        let detail = match (&row.phase_reason, &row.phase_progress) {
            (Some(reason), Some(progress)) if reason != progress => format!("{reason}; {progress}"),
            (Some(detail), _) | (_, Some(detail)) => detail.clone(),
            (None, None) => "-".into(),
        };
        let detail = truncate_field(&detail, 64, options.no_trunc);

        write!(
            table,
            "{id}\t{template}\t{}\t{}\t{state}\t{node}\t{detail}",
            row.replica, row.generation
        )?;
        if wide {
            let name = truncate_field(&row.name, 36, options.no_trunc);
            let image = truncate_field(&row.image, 32, options.no_trunc);
            let created = &row.created_at;
            let updated = &row.updated_at;
            write!(table, "\t{name}\t{image}\t{created}\t{updated}")?;
        }
        writeln!(table)?;
    }

    table.flush()?;
    Ok(String::from_utf8(table.into_inner()?)?)
}
