mod context;
mod runtime;
mod stores;
mod transport;

use crate::secrets::master_key::envelope::SecretPassphrase;
use crate::server;
use tracing::{info, warn};

pub(crate) use context::BootstrapContext;
pub(crate) use runtime::{
    BootedRuntime, BootstrapOptions, ReplicatedVolumeStartup, RuntimeTaskHandles, boot,
    boot_with_replicated_volume_listener,
};

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
    rest_token_enabled: bool,
) -> BootstrapResult<Option<server::RunHandles>> {
    let ctx = BootstrapContext::init_base(listen_addr).await?;
    let runtime = boot(
        ctx,
        transport::daemon_bootstrap_options(
            advertise_addr,
            master_key_passphrase,
            rest_token_enabled,
        ),
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
            // Close externally initiated workload admission before draining local storage users.
            // Durable workload demand remains intact so restart reconciliation recreates tasks.
            runtime.server.set_online(false);
            let replicated_shutdown_deadline = if let Some(storage) =
                runtime.components.replicated_volumes.as_ref()
            {
                let deadline = tokio::time::Instant::now() + storage.shutdown_attempt_timeout();
                tokio::time::timeout_at(deadline, async {
                    runtime
                        .components
                        .workload_manager
                        .begin_shutdown()
                        .await;
                    loop {
                        match runtime
                            .components
                            .workload_manager
                            .quiesce_replicated_volume_tasks_for_shutdown()
                            .await
                        {
                            Ok(report) => {
                                if report.attempted > 0 {
                                    info!(
                                        target: "server",
                                        attempted = report.attempted,
                                        quiesced = report.quiesced,
                                        "finished replicated-volume task quiescence before storage shutdown"
                                    );
                                }
                                if !report.quiescence_errors.is_empty() {
                                    warn!(
                                        target: "server",
                                        errors = report.quiescence_errors.len(),
                                        details = %report.quiescence_errors.join("; "),
                                        "replicated-volume task quiescence finished with errors"
                                    );
                                }
                                break;
                            }
                            Err(error) => {
                                // Storage stays alive while a container can still reach one of
                                // its block devices. A supervisor may terminate the process after
                                // the complete shutdown attempt reports its bounded failure.
                                warn!(
                                    target: "server",
                                    "replicated storage is still in use; retrying task quiescence: \
                                     {error:#}"
                                );
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            }
                        }
                    }
                })
                .await
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "replicated-volume tasks did not reach a safe quiescent state",
                    )
                })?;
                // No accepted workload launch can create another mount after begin_shutdown
                // drains its launch barrier, and every existing consumer is now detached.
                storage.begin_shutdown();
                Some(deadline)
            } else {
                None
            };
            if let Err(error) = runtime
                .components
                .workload_manager
                .flush_workload_updates_for_shutdown()
                .await
            {
                warn!(
                    target: "server",
                    "could not queue final workload updates before shutdown: {error:#}"
                );
            } else if let Err(error) = runtime
                .runtime_tasks
                .flush_gossip(std::time::Duration::from_secs(5))
                .await
            {
                warn!(
                    target: "server",
                    "could not attempt final workload updates before shutdown: {error:#}"
                );
            }
            handles.abort();
            runtime.server.release_replicated_volumes();
            let network_shutdown = runtime.components.network_controller.shutdown().await;
            runtime.runtime_tasks.abort_and_wait().await;
            let storage_shutdown = match (
                runtime.components.replicated_volumes.as_ref(),
                replicated_shutdown_deadline,
            ) {
                (Some(storage), Some(deadline)) => {
                    storage
                        .shutdown_with_timeout(
                            deadline.saturating_duration_since(tokio::time::Instant::now()),
                        )
                        .await
                }
                (None, None) => Ok(()),
                _ => Err(anyhow::anyhow!(
                    "replicated-volume shutdown deadline does not match runtime state"
                )),
            };
            storage_shutdown?;
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
