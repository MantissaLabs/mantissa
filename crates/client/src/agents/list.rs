use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use crate::tasks::{uuid_short, uuid_to_string};
use anyhow::Result;
use capnp::Error as CapnpError;
use protocol::agents::{AgentSessionStatus as ProtoAgentSessionStatus, agent_session_spec};
use std::io::Write;
use tabwriter::TabWriter;

/// Lists first-class agent sessions through the agents control-plane capability.
pub async fn list_sessions(cfg: &ClientConfig) -> Result<()> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_agents_request();
    let agents = request.send().pipeline.get_agents();
    let response = agents.list_sessions_request().send().promise.await?;
    let sessions = response.get()?.get_sessions()?;

    let mut rows = Vec::with_capacity(sessions.len() as usize);
    for reader in sessions.iter() {
        rows.push(AgentSessionRow::from_reader(reader)?);
    }
    rows.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    if rows.is_empty() {
        println!("no agent sessions registered");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tSTATUS\tACTIVE RUN\tLAST RUN\tSANDBOX\tUPDATED"
    )?;
    for row in rows {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.id,
            row.name,
            row.status,
            row.active_run_id.unwrap_or_else(|| "-".to_string()),
            row.last_run_id.unwrap_or_else(|| "-".to_string()),
            row.sandbox_profile.unwrap_or_else(|| "default".to_string()),
            row.updated_at,
        )?;
    }
    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);
    Ok(())
}

struct AgentSessionRow {
    id: String,
    name: String,
    status: &'static str,
    active_run_id: Option<String>,
    last_run_id: Option<String>,
    sandbox_profile: Option<String>,
    updated_at: String,
}

impl AgentSessionRow {
    /// Decodes one protocol agent session into a printable list row.
    fn from_reader(reader: agent_session_spec::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(Self {
            id: uuid_to_string(reader.get_id()?)?,
            name: reader.get_name()?.to_str()?.to_string(),
            status: match reader.get_status()? {
                ProtoAgentSessionStatus::WaitingInput => "waiting_input",
                ProtoAgentSessionStatus::Queued => "queued",
                ProtoAgentSessionStatus::Running => "running",
                ProtoAgentSessionStatus::Failed => "failed",
                ProtoAgentSessionStatus::Closed => "closed",
            },
            active_run_id: {
                let data = reader.get_active_run_id()?;
                (!data.is_empty()).then(|| uuid_short(data)).transpose()?
            },
            last_run_id: {
                let data = reader.get_last_run_id()?;
                (!data.is_empty()).then(|| uuid_short(data)).transpose()?
            },
            sandbox_profile: {
                let profile = reader.get_sandbox_profile()?.to_str()?.trim().to_string();
                (!profile.is_empty()).then_some(profile)
            },
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        })
    }
}
