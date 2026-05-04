use crate::config::ClientConfig;
use crate::connection;
use anyhow::Result;
use uuid::Uuid;

/// Queues one operator input on an existing agent session.
pub async fn submit_input(cfg: &ClientConfig, session_id: Uuid, input: &str) -> Result<()> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_agents_request();
    let agents = request.send().pipeline.get_agents();
    let mut request = agents.submit_input_request();
    {
        let mut builder = request.get();
        builder.set_session_id(session_id.as_bytes());
        builder.set_input(input);
    }
    request.send().promise.await?;
    Ok(())
}
