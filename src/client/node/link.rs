use crate::client::common;
use crate::topology_capnp::join_request as JoinRequest;
use capnp::message::Builder;
use std::error::Error;

pub async fn link(server_address: &str, join_address: &str) -> Result<(), Box<dyn Error>> {
    let client = common::get_client(server_address).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let mut request = topology.join_request();

    let mut builder = Builder::new_default();

    {
        let builder = &mut builder;
        let mut link = builder.init_root::<JoinRequest::Builder>();

        link.set_anchor(join_address);
    }

    let _ = request
        .get()
        .set_link(builder.get_root::<JoinRequest::Builder>()?.into_reader());

    let response = request.send().promise.await?;

    // TODO: Synchronize with ClusterSync interface?

    Ok(())
}
