use crate::client::common;
use crate::topology_capnp::join_request as JoinRequest;
use capnp::message::Builder;
use std::error::Error;

pub async fn link(
    server_address: &str,
    join_address: &str,
    join_token: &str,
) -> Result<(), Box<dyn Error>> {
    let client = common::get_client_secure(server_address, join_token).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let mut request = topology.join_request();

    let mut builder = Builder::new_default();

    // Build link message.
    let mut link = builder.init_root::<JoinRequest::Builder>();
    link.set_anchor(join_address);
    link.set_join_token(join_token);

    let _ = request
        .get()
        .set_link(builder.get_root::<JoinRequest::Builder>()?.into_reader());

    // TODO: Do something with the response.
    let response = request.send().promise.await?;

    // TODO: Synchronize with ClusterSync interface?

    Ok(())
}
