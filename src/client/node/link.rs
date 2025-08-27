use crate::client::common;
use crate::client::config::ClientConfig;
use crate::topology_capnp::join_request as JoinRequest;
use anyhow::{anyhow, Result};
use capnp::message::Builder;

pub async fn link(cfg: &ClientConfig) -> Result<()> {
    let client = common::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let mut request = topology.join_request();

    let mut builder = Builder::new_default();

    // Build link message.
    let mut link = builder.init_root::<JoinRequest::Builder>();
    link.set_anchor(cfg.anchor.as_ref().unwrap());
    link.set_join_token(cfg.join_token.as_ref().unwrap());

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
        cfg.anchor.as_deref().unwrap_or("<unknown anchor>")
    );

    Ok(())
}
