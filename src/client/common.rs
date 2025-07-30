use crate::server_capnp::server::Client;
use anyhow::Error;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;

pub async fn get_client(server: &str) -> Result<Client, Error> {
    use std::net::ToSocketAddrs;

    let addr = server
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("could not parse address");

    let stream = tokio::net::TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;

    let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();

    let rpc_network = Box::new(twoparty::VatNetwork::new(
        futures::io::BufReader::new(reader),
        futures::io::BufWriter::new(writer),
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));

    let mut rpc_system = RpcSystem::new(rpc_network, None);
    let client: Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

    tokio::task::spawn_local(rpc_system);

    Ok(client)
}
