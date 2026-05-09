mod context;
mod runtime;
mod stores;
mod transport;

use crate::secrets::master_key::envelope::SecretPassphrase;
use crate::server;
use tracing::{info, warn};

pub(crate) use context::BootstrapContext;
pub(crate) use runtime::{BootedRuntime, BootstrapOptions, RuntimeTaskHandles, boot};

pub(crate) type BootstrapResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Starts the daemon and its subsystems, picking a run mode and whether to
/// enable the Unix socket transport or not.
///
/// This stays as the public daemon entrypoint while the internal boot flow is
/// split into explicit phases shared by both production and headless startup.
pub async fn start(
    listen_addr: String,
    advertise_addr: Option<String>,
    mode: server::RunMode,
    enable_unix_socket: bool,
    master_key_passphrase: SecretPassphrase,
) -> BootstrapResult<Option<server::RunHandles>> {
    let ctx = BootstrapContext::init_base(listen_addr).await?;
    let runtime = boot(
        ctx,
        transport::daemon_bootstrap_options(advertise_addr, master_key_passphrase),
    )
    .await?;
    match mode {
        server::RunMode::Blocking => {
            let mut handles = runtime.server.start_nonblocking(enable_unix_socket).await?;
            handles.wait_ready().await;
            tokio::select! {
                _ = handles.wait() => {
                    warn!(target: "server", "daemon transport exited");
                }
                _ = wait_for_shutdown_signal() => {
                    info!(target: "server", "shutdown signal received");
                }
            }
            runtime.server.set_online(false);
            handles.abort();
            let network_shutdown = runtime.components.network_controller.shutdown().await;
            runtime.runtime_tasks.abort_and_wait().await;
            network_shutdown.map_err(|error| -> Box<dyn std::error::Error> {
                Box::new(std::io::Error::other(error.to_string()))
            })?;
            Ok(None)
        }
        server::RunMode::NonBlocking => runtime
            .server
            .start_nonblocking(enable_unix_socket)
            .await
            .map(Some)
            .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) }),
    }
}

/// Waits for the process-level signal that should stop a foreground daemon.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        match (
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()),
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()),
        ) {
            (Ok(mut interrupt), Ok(mut terminate)) => {
                tokio::select! {
                    _ = interrupt.recv() => {}
                    _ = terminate.recv() => {}
                }
            }
            _ => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
