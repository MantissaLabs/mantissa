use crate::{
    net::unix_socket::candidate_unix_socket_paths,
    noise::{client_handshake, load_or_generate_noise_keys},
    server_capnp::server::Client,
};
use capnp::Error as CapnpError;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};
use std::sync::Arc;
use tokio::net::UnixStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

// Used to get a client connection with Capn'proto.
// At the moment, any method using `get_client` *needs* to be run in a tokio task,
// otherwise this will panic.
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

    let keys_path = crate::noise::resolve_noise_key_path()?;
    let keys = Arc::new(load_or_generate_noise_keys(keys_path)?);
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

// Explicit socket for local communication.
pub async fn get_client_unix_path(path: std::path::PathBuf) -> Result<Client, CapnpError> {
    let stream = UnixStream::connect(path)
        .await
        .map_err(|e| CapnpError::failed(e.to_string()))?;
    let (reader, writer) = stream.compat().split();
    let network = twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    );
    let mut rpc = RpcSystem::new(Box::new(network), None);
    let client: Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
    tokio::task::spawn_local(rpc.map(|_| ()));
    Ok(client)
}

// Auto socket: try /var/run, /run, $XDG_RUNTIME_DIR, /tmp
pub async fn get_client_unix_auto() -> Result<Client, CapnpError> {
    let mut last = None;
    for p in candidate_unix_socket_paths() {
        match UnixStream::connect(&p).await {
            Ok(stream) => {
                let (reader, writer) = stream.compat().split();
                let network = twoparty::VatNetwork::new(
                    reader,
                    writer,
                    rpc_twoparty_capnp::Side::Client,
                    Default::default(),
                );
                let mut rpc = RpcSystem::new(Box::new(network), None);
                let client: Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
                tokio::task::spawn_local(rpc.map(|_| ()));
                return Ok(client);
            }
            Err(e) => last = Some(e),
        }
    }
    Err(CapnpError::failed(format!(
        "no local mantissa.sock found: last error: {last:?}"
    )))
}

// Used for local client command -> mantissa process communication.
pub async fn get_client_auto(
    cfg: &crate::client::config::ClientConfig,
) -> Result<Client, CapnpError> {
    if let Some(ref p) = cfg.socket {
        return get_client_unix_path(p.clone()).await;
    }
    get_client_unix_auto().await
}
