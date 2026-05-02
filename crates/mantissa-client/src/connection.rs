use crate::errors::ClientSocketError;
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use mantissa_net::{
    noise::{
        NoiseKeys, NoiseStream, client_handshake_join_with_probe, client_handshake_peer,
        join_probe_client, load_or_generate_noise_keys,
    },
    unix_socket::candidate_unix_socket_paths,
};
use mantissa_protocol::server::{cluster_session, server};
use std::{
    fs, io,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
};
use tokio::net::UnixStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

/// Resolve an in-process client handle when using the inproc transport.
fn inproc_client(addr: &str) -> Result<Option<server::Client>, capnp::Error> {
    if let Some(rest) = addr.strip_prefix("inproc://") {
        if let Some(c) = mantissa_net::inproc::get(rest) {
            return Ok(Some(c));
        }
        return Err(capnp::Error::failed(format!(
            "inproc target not found: {rest}"
        )));
    }
    Ok(None)
}

fn to_socket_addr(addr: &str) -> Result<std::net::SocketAddr, capnp::Error> {
    use std::net::ToSocketAddrs;
    addr.to_socket_addrs()
        .map_err(|e| capnp::Error::failed(format!("bad addr: {e}")))?
        .next()
        .ok_or_else(|| capnp::Error::failed("no addr".into()))
}

async fn rpc_client_from_stream(noise_stream: NoiseStream) -> Result<server::Client, capnp::Error> {
    let (reader, writer) = noise_stream.into_split();

    let network = Box::new(twoparty::VatNetwork::new(
        reader.compat(),
        writer.compat_write(),
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));

    let mut rpc = RpcSystem::new(network, None);
    let client: server::Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
    tokio::task::spawn_local(async move {
        if let Err(e) = rpc.await {
            eprintln!("capnp rpc system shutdown: {e}");
        }
    });
    Ok(client)
}

/// Join an anchor over TCP+Noise using the join token PSK (Noise_XXpsk3).
pub async fn get_client_secure_join(
    addr: &str,
    join_token: &str,
) -> Result<server::Client, capnp::Error> {
    let keys_path = mantissa_net::noise::resolve_noise_key_path()?;
    let keys = load_or_generate_noise_keys(keys_path)?;
    get_client_secure_join_with_keys(addr, join_token, &keys).await
}

/// Join an anchor over TCP+Noise using the join token PSK (Noise_XXpsk3),
/// supplying explicit Noise keys for the initiator.
pub async fn get_client_secure_join_with_keys(
    addr: &str,
    join_token: &str,
    keys: &NoiseKeys,
) -> Result<server::Client, capnp::Error> {
    if let Some(c) = inproc_client(addr)? {
        return Ok(c);
    }

    let sock = to_socket_addr(addr)?;
    let tcp = tokio::net::TcpStream::connect(sock)
        .await
        .map_err(|e| capnp::Error::failed(format!("tcp connect: {e}")))?;
    tcp.set_nodelay(true).ok();

    let psk = mantissa_net::noise::derive_psk_from_token(join_token)
        .map_err(|e| capnp::Error::failed(format!("psk derivation: {e}")))?;

    let mut handshake = client_handshake_join_with_probe(tcp, keys, &psk)
        .await
        .map_err(|e| capnp::Error::failed(format!("noise: {e}")))?;

    if handshake.probe_enabled && join_probe_client(&mut handshake.stream).await.is_err() {
        return Err(capnp::Error::failed("invalid join token".to_string()));
    }

    rpc_client_from_stream(handshake.stream).await
}

/// Connect to a known peer over TCP+Noise using static key authentication (Noise IK).
pub async fn get_client_secure_peer(
    addr: &str,
    peer_static: &[u8; 32],
) -> Result<server::Client, capnp::Error> {
    let keys_path = mantissa_net::noise::resolve_noise_key_path()?;
    let keys = load_or_generate_noise_keys(keys_path)?;
    get_client_secure_peer_with_keys(addr, peer_static, &keys).await
}

/// Connect to a known peer over TCP+Noise using static key authentication (Noise IK),
/// supplying explicit Noise keys for the initiator.
pub async fn get_client_secure_peer_with_keys(
    addr: &str,
    peer_static: &[u8; 32],
    keys: &NoiseKeys,
) -> Result<server::Client, capnp::Error> {
    if let Some(c) = inproc_client(addr)? {
        return Ok(c);
    }

    let sock = to_socket_addr(addr)?;
    let tcp = tokio::net::TcpStream::connect(sock)
        .await
        .map_err(|e| capnp::Error::failed(format!("tcp connect: {e}")))?;
    tcp.set_nodelay(true).ok();

    let noise_stream = client_handshake_peer(tcp, keys, peer_static)
        .await
        .map_err(|e| capnp::Error::failed(format!("noise: {e}")))?;

    rpc_client_from_stream(noise_stream).await
}

/// Shared helper to build a client from a connected UnixStream
async fn client_from_unix_stream(
    stream: UnixStream,
) -> Result<cluster_session::Client, ClientSocketError> {
    let (reader, writer) = stream.compat().split();
    let network = twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    );
    let mut rpc = RpcSystem::new(Box::new(network), None);
    let client: cluster_session::Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
    tokio::task::spawn_local(async move {
        if let Err(e) = rpc.await {
            eprintln!("capnp rpc system shutdown: {e}");
        }
    });
    Ok(client)
}

fn classify_path_not_socket(path: &Path) -> Option<ClientSocketError> {
    if let Ok(meta) = fs::symlink_metadata(path)
        && !meta.file_type().is_socket()
    {
        return Some(ClientSocketError::NotASocket {
            path: path.to_path_buf(),
        });
    }
    None
}

/// Explicit socket for local communication.
pub async fn get_client_unix_path(
    path: PathBuf,
) -> Result<cluster_session::Client, ClientSocketError> {
    if let Some(e) = classify_path_not_socket(&path) {
        return Err(e);
    }

    match UnixStream::connect(&path).await {
        Ok(stream) => client_from_unix_stream(stream).await,
        Err(e) => {
            use io::ErrorKind::*;
            Err(match e.kind() {
                NotFound => ClientSocketError::NotFound { tried: vec![path] },
                PermissionDenied => ClientSocketError::PermissionDenied { path },
                ConnectionRefused => ClientSocketError::Refused { path },
                _ => ClientSocketError::Other { path, source: e },
            })
        }
    }
}

/// Get local socket client, either use explicitly provided socket path
/// or auto-discover.
pub async fn get_local_session(
    cfg: &crate::config::ClientConfig,
) -> Result<cluster_session::Client, ClientSocketError> {
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
                return Err(ClientSocketError::PermissionDenied { path: p });
            }
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                return Err(ClientSocketError::Refused { path: p });
            }
            Err(e) => return Err(ClientSocketError::Other { path: p, source: e }),
        }
    }
    Err(ClientSocketError::NotFound { tried })
}
