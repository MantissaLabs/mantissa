use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use crate::tasks::uuid_to_string;
use anyhow::Result;
use capnp::Error as CapnpError;
use protocol::jobs::{JobStatus as ProtoJobStatus, job_spec};
use std::io::Write;
use tabwriter::TabWriter;

/// Lists first-class jobs through the jobs control-plane capability.
pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_jobs_request();
    let jobs = request.send().pipeline.get_jobs();
    let response = jobs.list_request().send().promise.await?;
    let specs = response.get()?.get_jobs()?;

    let mut rows = Vec::with_capacity(specs.len() as usize);
    for spec in specs.iter() {
        rows.push(JobRow::from_reader(spec)?);
    }
    rows.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    if rows.is_empty() {
        println!("no jobs registered");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tIMAGE\tSTATUS\tATTEMPTS\tACTIVE TASK\tUPDATED"
    )?;
    for row in rows {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.id,
            row.name,
            row.image,
            row.status,
            row.attempts_started,
            row.active_task_id.unwrap_or_else(|| "-".to_string()),
            row.updated_at,
        )?;
    }
    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);
    Ok(())
}

struct JobRow {
    id: String,
    name: String,
    image: String,
    status: &'static str,
    attempts_started: u32,
    active_task_id: Option<String>,
    updated_at: String,
}

impl JobRow {
    /// Decodes one protocol job spec into a printable list row.
    fn from_reader(reader: job_spec::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(Self {
            id: uuid_to_string(reader.get_id()?)?,
            name: reader.get_name()?.to_str()?.to_string(),
            image: reader.get_image()?.to_str()?.to_string(),
            status: match reader.get_status()? {
                ProtoJobStatus::Pending => "pending",
                ProtoJobStatus::Running => "running",
                ProtoJobStatus::Retrying => "retrying",
                ProtoJobStatus::Succeeded => "succeeded",
                ProtoJobStatus::Failed => "failed",
            },
            attempts_started: reader.get_attempts_started(),
            active_task_id: {
                let data = reader.get_active_task_id()?;
                (!data.is_empty())
                    .then(|| uuid_to_string(data))
                    .transpose()?
            },
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        })
    }
}
