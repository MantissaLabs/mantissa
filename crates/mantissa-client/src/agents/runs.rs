use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use crate::tasks::{uuid_short, uuid_to_string};
use anyhow::Result;
use capnp::Error as CapnpError;
use mantissa_protocol::agents::{AgentRunStatus as ProtoAgentRunStatus, agent_run_spec};
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

/// Lists first-class agent runs through the agents control-plane capability.
pub async fn list_runs(cfg: &ClientConfig, session_id: Option<Uuid>) -> Result<()> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_agents_request();
    let agents = request.send().pipeline.get_agents();
    let mut request = agents.list_runs_request();
    if let Some(session_id) = session_id {
        request.get().set_session_id(session_id.as_bytes());
    } else {
        request.get().set_session_id(&[]);
    }

    let response = request.send().promise.await?;
    let runs = response.get()?.get_runs()?;

    let mut rows = Vec::with_capacity(runs.len() as usize);
    for reader in runs.iter() {
        rows.push(AgentRunRow::from_reader(reader)?);
    }
    rows.sort_by(|left, right| {
        left.session_name
            .cmp(&right.session_name)
            .then(left.id.cmp(&right.id))
    });

    if rows.is_empty() {
        println!("no agent runs registered");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "RUN ID\tSESSION\tSTATUS\tWORKLOAD\tEXIT\tPLATFORM\tMODE\tPROFILE\tUPDATED"
    )?;
    for row in rows {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.id,
            row.session_name,
            row.status,
            row.workload_id.unwrap_or_else(|| "-".to_string()),
            row.exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.execution_platform,
            row.isolation_mode,
            row.isolation_profile
                .unwrap_or_else(|| "default".to_string()),
            row.updated_at,
        )?;
    }
    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);
    Ok(())
}

struct AgentRunRow {
    id: String,
    session_name: String,
    status: &'static str,
    workload_id: Option<String>,
    exit_code: Option<i32>,
    execution_platform: String,
    isolation_mode: String,
    isolation_profile: Option<String>,
    updated_at: String,
}

impl AgentRunRow {
    /// Decodes one protocol agent run into a printable list row.
    fn from_reader(reader: agent_run_spec::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(Self {
            id: uuid_to_string(reader.get_id()?)?,
            session_name: reader.get_session_name()?.to_str()?.to_string(),
            status: match reader.get_status()? {
                ProtoAgentRunStatus::Pending => "pending",
                ProtoAgentRunStatus::Running => "running",
                ProtoAgentRunStatus::Succeeded => "succeeded",
                ProtoAgentRunStatus::Failed => "failed",
                ProtoAgentRunStatus::Cancelled => "cancelled",
            },
            workload_id: {
                let data = reader.get_workload_id()?;
                (!data.is_empty()).then(|| uuid_short(data)).transpose()?
            },
            exit_code: reader.get_has_exit_code().then_some(reader.get_exit_code()),
            execution_platform: reader.get_execution_platform()?.to_str()?.to_string(),
            isolation_mode: reader.get_isolation_mode()?.to_str()?.to_string(),
            isolation_profile: {
                let profile = reader.get_isolation_profile()?.to_str()?.trim().to_string();
                (!profile.is_empty()).then_some(profile)
            },
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        })
    }
}
