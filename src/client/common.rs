use crate::{noise::client_handshake, server_capnp::server::Client};
use anyhow::Error;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};

// Used to get a client connection with Capn'proto.
// At the moment, any method using `get_client` *needs* to be run in a tokio task,
// otherwise this will panic.
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

pub async fn get_client_secure(addr: &str, token: &str) -> Result<Client, capnp::Error> {
    use std::net::ToSocketAddrs;
    let sock = addr
        .to_socket_addrs()
        .map_err(|e| capnp::Error::failed(format!("bad addr: {e}")))?
        .next()
        .ok_or_else(|| capnp::Error::failed("no addr".into()))?;

    let tcp = tokio::net::TcpStream::connect(sock)
        .await
        .map_err(|e| capnp::Error::failed(format!("tcp connect: {e}")))?;
    tcp.set_nodelay(true).ok();

    let keys = crate::noise::generate_noise_keys(); // or load client's static if you want it stable
    let noise_stream = client_handshake(tcp, token, &keys)
        .await
        .map_err(|e| capnp::Error::failed(format!("noise: {e}")))?;

    let (r, w) = tokio_util::compat::TokioAsyncReadCompatExt::compat(noise_stream).split();

    let network = Box::new(twoparty::VatNetwork::new(
        futures::io::BufReader::new(r),
        futures::io::BufWriter::new(w),
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));

    let mut rpc = RpcSystem::new(network, None);
    let client: Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
    tokio::task::spawn_local(rpc.map(|_| ()));
    Ok(client)
}
