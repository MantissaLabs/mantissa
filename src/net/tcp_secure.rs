use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use std::sync::Arc;
use tokio::net::TcpListener;

pub async fn start_tcp_secure_listener(
    listen_addr: String,
    server_handle: crate::server_capnp::server::Client,
    noise_keys: Arc<crate::noise::NoiseKeys>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&listen_addr).await?;
    println!("Server listening (secure) on {}", listen_addr);

    loop {
        let (stream, _peer) = listener.accept().await?;
        stream.set_nodelay(true)?;

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
