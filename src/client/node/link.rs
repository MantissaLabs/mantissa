use crate::client::common;
use crate::client::config::ClientConfig;
use crate::topology_capnp::join_request as JoinRequest;
use capnp::message::Builder;
use std::error::Error;

pub async fn link(cfg: &ClientConfig) -> Result<(), Box<dyn Error>> {
    let client = common::get_client_auto(cfg).await?;

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

    // TODO: Do something with the response.
    let response = request.send().promise.await?;

    // TODO: Synchronize with ClusterSync interface?

    Ok(())
}
