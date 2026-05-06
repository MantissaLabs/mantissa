mod context;
mod runtime;
mod stores;
mod transport;

use crate::secrets::master_key::envelope::SecretPassphrase;
use crate::server;

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
            runtime.server.run_blocking(enable_unix_socket).await?;
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
