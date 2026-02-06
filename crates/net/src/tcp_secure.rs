use crate::noise::{HandshakeKind, NoisePeerVerifier, NoisePskProvider};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

/// Accept-loop used by both blocking and non-blocking variants.
async fn accept_loop(
    listener: TcpListener,
    server_handle: protocol::server::server::Client,
    noise_keys: Arc<crate::noise::NoiseKeys>,
    psk_provider: Arc<dyn NoisePskProvider>,
    peer_verifier: Arc<dyn NoisePeerVerifier>,
) {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                error!(target: "server", "TCP accept error: {e}");
                continue;
            }
        };

        if let Err(e) = stream.set_nodelay(true) {
            warn!(target: "server", "set_nodelay failed: {e}");
        }

        let server_handle_clone = server_handle.clone();
        let keys = noise_keys.clone();
        let psk_provider = psk_provider.clone();
        let peer_verifier = peer_verifier.clone();

        tokio::task::spawn_local(async move {
            let psk = match psk_provider.psk().await {
                Ok(psk) => psk,
                Err(e) => {
                    error!(target: "server", "Noise PSK derivation failed: {e}");
                    return;
                }
            };

            let (mut rd, wr) = stream.into_split();
            let mut first = vec![0u8; 65535];
            let nread = match crate::noise::read_framed_len(&mut rd, &mut first).await {
                Ok(n) => n,
                Err(e) => {
                    error!(target: "server", "Noise handshake read failed: {e}");
                    return;
                }
            };

            match crate::noise::server_handshake_select(
                rd,
                wr,
                &keys,
                &psk,
                &first[..nread],
                peer_verifier,
            )
            .await
            {
                Ok(mut handshake) => {
                    if matches!(handshake.kind, HandshakeKind::Join) {
                        if let Err(e) = crate::noise::join_probe_server(&mut handshake.stream).await
                        {
                            warn!(target: "server", "Noise join probe failed: {e}");
                            return;
                        }
                    }

                    let (reader, writer) =
                        tokio_util::compat::TokioAsyncReadCompatExt::compat(handshake.stream)
                            .split();

                    let network = twoparty::VatNetwork::new(
                        futures::io::BufReader::new(reader),
                        futures::io::BufWriter::new(writer),
                        rpc_twoparty_capnp::Side::Server,
                        Default::default(),
                    );

                    let rpc_system =
                        RpcSystem::new(Box::new(network), Some(server_handle_clone.client));

                    if let Err(e) = rpc_system.await {
                        error!(target: "server", "TCP secure RPC error: {e}");
                    }
                }
                Err(crate::noise::ServerHandshakeError::UnknownPeer) => {
                    warn!(target: "server", "Noise peer rejected: unknown static key");
                }
                Err(crate::noise::ServerHandshakeError::Io(e)) => {
                    if e.to_string() == "invalid join token" {
                        warn!(target: "server", "Noise join handshake failed: invalid join token");
                    } else {
                        error!(target: "server", "Noise handshake failed: {e}");
                    }
                }
            }
        });
    }
}

/// **Blocking**: runs the accept loop on the current task until error/abort.
/// (compat: unchanged signature/behavior)
pub async fn start_tcp_secure_listener(
    listen_addr: String,
    server_handle: protocol::server::server::Client,
    noise_keys: Arc<crate::noise::NoiseKeys>,
    psk_provider: Arc<dyn NoisePskProvider>,
    peer_verifier: Arc<dyn NoisePeerVerifier>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&listen_addr).await?;
    let bound = listener.local_addr()?;
    info!(target: "server", "Server listening (secure) on {}", bound);
    accept_loop(
        listener,
        server_handle,
        noise_keys,
        psk_provider,
        peer_verifier,
    )
    .await;
    Ok(())
}

/// **Non-Blocking**: spawns the accept loop and returns:
///  - JoinHandle<()> for the loop
///  - oneshot::Receiver<()> that fires once the socket is bound (readiness)
///  - the actual bound SocketAddr (helpful if you passed "127.0.0.1:0")
pub async fn start_tcp_secure_listener_nonblocking_with_ready(
    listen_addr: String,
    server_handle: protocol::server::server::Client,
    noise_keys: Arc<crate::noise::NoiseKeys>,
    psk_provider: Arc<dyn NoisePskProvider>,
    peer_verifier: Arc<dyn NoisePeerVerifier>,
) -> Result<
    (
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Receiver<()>,
        std::net::SocketAddr,
    ),
    Box<dyn std::error::Error>,
> {
    let listener = TcpListener::bind(&listen_addr).await?;
    let bound = listener.local_addr()?;
    info!(target: "server", "Server listening (secure) on {}", bound);

    let (tx, rx) = tokio::sync::oneshot::channel();

    // Move everything into the local task (Cap’n Proto requires !Send)
    let handle = tokio::task::spawn_local(async move {
        // Signal readiness immediately after successful bind.
        let _ = tx.send(());
        accept_loop(
            listener,
            server_handle,
            noise_keys,
            psk_provider,
            peer_verifier,
        )
        .await;
    });

    Ok((handle, rx, bound))
}
