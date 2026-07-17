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
    net::SocketAddr,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
};
use tokio::net::{TcpStream, UnixStream};
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

/// Resolve one TCP endpoint without blocking the async runtime thread on DNS.
async fn resolve_socket_addr(addr: &str) -> Result<SocketAddr, capnp::Error> {
    tokio::net::lookup_host(addr)
        .await
        .map_err(|e| capnp::Error::failed(format!("bad addr: {e}")))?
        .next()
        .ok_or_else(|| capnp::Error::failed("no addr".into()))
}

/// Connect one TCP stream and enable low-latency writes before the Noise handshake starts.
async fn connect_tcp_low_latency(addr: &str) -> Result<TcpStream, capnp::Error> {
    let sock = resolve_socket_addr(addr).await?;
    let tcp = TcpStream::connect(sock)
        .await
        .map_err(|e| capnp::Error::failed(format!("tcp connect: {e}")))?;
    tcp.set_nodelay(true)
        .map_err(|e| capnp::Error::failed(format!("tcp nodelay: {e}")))?;
    Ok(tcp)
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
        let _ = rpc.await;
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

    let tcp = connect_tcp_low_latency(addr).await?;

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

    let tcp = connect_tcp_low_latency(addr).await?;

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
        let _ = rpc.await;
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

    let (_path, stream) = connect_discovered_unix_stream(candidate_unix_socket_paths()).await?;
    client_from_unix_stream(stream).await
}

/// Opens the first usable Unix socket from an auto-discovery candidate list.
async fn connect_discovered_unix_stream(
    paths: Vec<PathBuf>,
) -> Result<(PathBuf, UnixStream), ClientSocketError> {
    let mut tried = Vec::new();
    let mut first_permission_denied = None;
    let mut first_refused = None;
    let mut first_not_socket = None;
    let mut first_other = None;

    for p in paths {
        tried.push(p.clone());
        if let Some(e) = classify_path_not_socket(&p) {
            if first_not_socket.is_none() {
                first_not_socket = Some(e);
            }
            continue;
        }
        match UnixStream::connect(&p).await {
            Ok(stream) => return Ok((p, stream)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                if first_permission_denied.is_none() {
                    first_permission_denied = Some(ClientSocketError::PermissionDenied { path: p });
                }
            }
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                if first_refused.is_none() {
                    first_refused = Some(ClientSocketError::Refused { path: p });
                }
            }
            Err(e) => {
                if first_other.is_none() {
                    first_other = Some(ClientSocketError::Other { path: p, source: e });
                }
            }
        }
    }

    if let Some(e) = first_permission_denied {
        return Err(e);
    }
    if let Some(e) = first_refused {
        return Err(e);
    }
    if let Some(e) = first_not_socket {
        return Err(e);
    }
    if let Some(e) = first_other {
        return Err(e);
    }
    Err(ClientSocketError::NotFound { tried })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::net::{TcpListener, UnixListener};

    /// Creates a unique test directory below the system temp directory.
    fn test_socket_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock before epoch")
            .as_nanos();
        // Keep the directory name short because macOS limits the complete Unix
        // socket path, including the system temp-directory prefix, to SUN_LEN.
        let dir = std::env::temp_dir().join(format!("mc-{}-{unique:x}", std::process::id()));
        fs::create_dir_all(&dir).expect("create test socket dir");
        dir
    }

    #[tokio::test]
    async fn auto_discovery_skips_refused_stale_socket() {
        let dir = test_socket_dir();
        let stale = dir.join("stale.sock");
        let live = dir.join("live.sock");
        let stale_listener = UnixListener::bind(&stale).expect("bind stale socket");
        drop(stale_listener);
        let live_listener = UnixListener::bind(&live).expect("bind live socket");
        let accept = tokio::spawn(async move {
            let _accepted = live_listener.accept().await;
        });

        let (path, stream) = connect_discovered_unix_stream(vec![stale, live.clone()])
            .await
            .expect("connect live socket after stale refusal");

        assert_eq!(path, live);
        drop(stream);
        accept.await.expect("accept task");
        fs::remove_dir_all(dir).expect("remove test socket dir");
    }

    #[tokio::test]
    async fn tcp_connect_uses_nodelay() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind tcp listener");
        let addr = listener.local_addr().expect("listener address").to_string();
        let accept = tokio::spawn(async move {
            let _accepted = listener.accept().await;
        });

        let stream = connect_tcp_low_latency(&addr)
            .await
            .expect("connect tcp low latency");

        assert!(stream.nodelay().expect("read nodelay"));
        drop(stream);
        accept.await.expect("accept task");
    }
}
