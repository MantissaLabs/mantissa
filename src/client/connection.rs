use crate::{
    client::errors::ClientConnectError,
    net::unix_socket::candidate_unix_socket_paths,
    noise::{client_handshake, load_or_generate_noise_keys},
};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};
use protocol::server::{cluster_session, server};
use std::{
    fs, io,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::net::UnixStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

/// Used to get a client connection with Capn'proto.
/// At the moment, any method using `get_client` *needs* to be run in a tokio task,
/// otherwise this will panic.
pub async fn get_client_secure(addr: &str) -> Result<server::Client, capnp::Error> {
    // Only useful for tests, catch the capnp capability in-process to
    // avoid any networking call.
    #[cfg(any(test, feature = "testkit"))]
    {
        if let Some(rest) = addr.strip_prefix("inproc://") {
            use crate::net;

            if let Some(c) = net::inproc::get(rest) {
                return Ok(c);
            }
            return Err(capnp::Error::failed(format!(
                "inproc target not found: {}",
                rest
            )));
        }
    }

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
    let noise_stream = client_handshake(tcp, &keys)
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
    let client: server::Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
    tokio::task::spawn_local(rpc.map(|_| ()));
    Ok(client)
}

/// Shared helper to build a client from a connected UnixStream
async fn client_from_unix_stream(
    stream: UnixStream,
) -> Result<cluster_session::Client, ClientConnectError> {
    let (reader, writer) = stream.compat().split();
    let network = twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    );
    let mut rpc = RpcSystem::new(Box::new(network), None);
    let client: cluster_session::Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
    tokio::task::spawn_local(rpc.map(|_| ()));
    Ok(client)
}

fn classify_path_not_socket(path: &Path) -> Option<ClientConnectError> {
    if let Ok(meta) = fs::symlink_metadata(path) {
        if !meta.file_type().is_socket() {
            return Some(ClientConnectError::LocalSocketNotASocket {
                path: path.to_path_buf(),
            });
        }
    }
    None
}

/// Explicit socket for local communication.
pub async fn get_client_unix_path(
    path: PathBuf,
) -> Result<cluster_session::Client, ClientConnectError> {
    if let Some(e) = classify_path_not_socket(&path) {
        return Err(e);
    }

    match UnixStream::connect(&path).await {
        Ok(stream) => client_from_unix_stream(stream).await,
        Err(e) => {
            use io::ErrorKind::*;
            Err(match e.kind() {
                NotFound => ClientConnectError::LocalSocketNotFound { tried: vec![path] },
                PermissionDenied => ClientConnectError::LocalSocketPermissionDenied { path },
                ConnectionRefused => ClientConnectError::LocalSocketRefused { path },
                _ => ClientConnectError::LocalSocketOther { path, source: e },
            })
        }
    }
}

/// Get local socket client, either use explicitly provided socket path
/// or auto-discover.
pub async fn get_local_session(
    cfg: &crate::client::config::ClientConfig,
) -> Result<cluster_session::Client, ClientConnectError> {
    if let Some(ref p) = cfg.socket {
        return get_client_unix_path(p.clone()).await;
    }

    // Auto discover local socket.
    let mut tried: Vec<PathBuf> = Vec::new();
    for p in candidate_unix_socket_paths() {
        tried.push(p.clone());
        if let Some(e) = classify_path_not_socket(&p) {
            return Err(e);
        }
        match UnixStream::connect(&p).await {
            Ok(stream) => return client_from_unix_stream(stream).await,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                return Err(ClientConnectError::LocalSocketPermissionDenied { path: p })
            }
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                return Err(ClientConnectError::LocalSocketRefused { path: p })
            }
            Err(e) => return Err(ClientConnectError::LocalSocketOther { path: p, source: e }),
        }
    }
    Err(ClientConnectError::LocalSocketNotFound { tried })
}
