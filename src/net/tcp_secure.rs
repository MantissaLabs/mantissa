use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

/// Accept-loop used by both blocking and non-blocking variants.
async fn accept_loop(
    listener: TcpListener,
    server_handle: protocol::server::server::Client,
    noise_keys: Arc<crate::noise::NoiseKeys>,
) {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("TCP accept error: {e}");
                continue;
            }
        };

        if let Err(e) = stream.set_nodelay(true) {
            eprintln!("set_nodelay failed: {e}");
        }

        let server_handle_clone = server_handle.clone();
        let keys = noise_keys.clone();

        tokio::task::spawn_local(async move {
            match crate::noise::server_handshake(stream, &keys).await {
                Ok(noise_stream) => {
                    let (reader, writer) =
                        tokio_util::compat::TokioAsyncReadCompatExt::compat(noise_stream).split();

                    let network = twoparty::VatNetwork::new(
                        futures::io::BufReader::new(reader),
                        futures::io::BufWriter::new(writer),
                        rpc_twoparty_capnp::Side::Server,
                        Default::default(),
                    );

                    let rpc_system =
                        RpcSystem::new(Box::new(network), Some(server_handle_clone.client));

                    if let Err(e) = rpc_system.await {
                        eprintln!("TCP secure RPC error: {e}");
                    }
                }
                Err(e) => eprintln!("Noise handshake/token failed: {e}"),
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
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&listen_addr).await?;
    let bound = listener.local_addr()?;
    info!(target: "server", "Server listening (secure) on {}", bound);
    accept_loop(listener, server_handle, noise_keys).await;
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
        accept_loop(listener, server_handle, noise_keys).await;
    });

    Ok((handle, rx, bound))
}
