use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Result, anyhow};
use capnp::message::Builder;
use mantissa_protocol::topology::join_request as JoinRequest;

pub async fn join(cfg: &ClientConfig) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let mut request = topology.join_request();

    let mut builder = Builder::new_default();

    let anchor = cfg
        .anchor
        .as_deref()
        .ok_or_else(|| anyhow!("anchor is required to join"))?;
    let join_token = cfg
        .join_token
        .as_deref()
        .ok_or_else(|| anyhow!("join token is required to join"))?;

    // Build join request payload.
    let mut join_request = builder.init_root::<JoinRequest::Builder>();
    join_request.set_anchor(anchor);
    join_request.set_join_token(join_token);

    let _ = request
        .get()
        .set_request(builder.get_root::<JoinRequest::Builder>()?.into_reader());

    let response = request.send().promise.await.map_err(|e| {
        let mut msg = e.to_string();
        if let Some(stripped) = msg.strip_prefix("Failed: ") {
            msg = stripped.to_string();
        }
        if let Some(stripped) = msg.strip_prefix("remote exception: ") {
            msg = stripped.to_string();
        }
        anyhow!(msg)
    })?;
    let join_resp = response.get()?.get_resp()?;
    let err = join_resp.get_error()?.to_string()?;

    if !err.is_empty() {
        return Err(anyhow!(err.to_string()));
    }

    println!("join succeeded via {}", anchor);

    Ok(())
}
