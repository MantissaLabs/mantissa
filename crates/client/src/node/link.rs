use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Result, anyhow};
use capnp::message::Builder;
use protocol::topology::join_request as JoinRequest;

pub async fn link(cfg: &ClientConfig) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let mut request = topology.join_request();

    let mut builder = Builder::new_default();

    let anchor = cfg
        .anchor
        .as_deref()
        .ok_or_else(|| anyhow!("anchor is required to link"))?;
    let join_token = cfg
        .join_token
        .as_deref()
        .ok_or_else(|| anyhow!("join token is required to link"))?;

    // Build link message.
    let mut link = builder.init_root::<JoinRequest::Builder>();
    link.set_anchor(anchor);
    link.set_join_token(join_token);

    let _ = request
        .get()
        .set_link(builder.get_root::<JoinRequest::Builder>()?.into_reader());

    let response = request.send().promise.await?;
    let join_resp = response.get()?.get_resp()?;
    let err = join_resp.get_error()?.to_string()?;

    if !err.is_empty() {
        return Err(anyhow!(err.to_string()));
    }

    println!(
        "join succeeded via {}",
        anchor
    );

    Ok(())
}
