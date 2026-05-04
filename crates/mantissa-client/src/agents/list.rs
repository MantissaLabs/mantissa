use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::{uuid_short, uuid_to_string};
use anyhow::Result;
use capnp::Error as CapnpError;
use mantissa_protocol::agents::{
    AgentSessionStatus as ProtoAgentSessionStatus, agent_session_spec,
};

/// Lists first-class agent sessions through the agents control-plane capability.
pub async fn list_sessions(cfg: &ClientConfig) -> Result<Vec<AgentSessionRow>> {
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

    Ok(rows)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentSessionRow {
    pub id: String,
    pub name: String,
    pub status: &'static str,
    pub active_run_id: Option<String>,
    pub last_run_id: Option<String>,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
    pub updated_at: String,
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
                ProtoAgentSessionStatus::Closing => "closing",
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
