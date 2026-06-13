use crate::cli::InitArgs;
use crate::output;
use anyhow::{Context, Result};
use mantissa_client::config::ClientConfig;
use mantissa_rest::{
    config::RestConfig,
    server::{self, RestServerError},
};
use std::{env, net::SocketAddr};
use tokio::{sync::oneshot, task::JoinHandle};

const ENV_REST_ENABLED: &str = "MANTISSA_REST_ENABLED";

/// Handle for an embedded REST listener owned by the daemon lifecycle.
pub(crate) struct EmbeddedRestServer {
    local_addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), RestServerError>>,
}

impl EmbeddedRestServer {
    /// Returns the bound address where the embedded REST API is listening.
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Requests graceful REST shutdown and waits for the listener task to exit.
    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown.send(());
        match self.task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("embedded REST listener exited with error: {error}"),
            Err(error) => eprintln!("embedded REST listener task failed: {error}"),
        }
    }
}

/// Starts the embedded REST listener when requested by CLI flags or environment.
pub(crate) async fn start_embedded(init: &InitArgs) -> Result<Option<EmbeddedRestServer>> {
    let Some(config) = config_from_init(init)? else {
        return Ok(None);
    };
    let server = server::bind(config)
        .await
        .context("bind embedded REST API")?;
    let local_addr = server.local_addr();
    let (shutdown, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        server
            .serve_until(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    Ok(Some(EmbeddedRestServer {
        local_addr,
        shutdown,
        task,
    }))
}

/// Builds embedded REST config from `mantissa init` flags and REST env vars.
pub(crate) fn config_from_init(init: &InitArgs) -> Result<Option<RestConfig>> {
    if !rest_requested(init) {
        return Ok(None);
    }

    let mut config = RestConfig::from_env_unvalidated().context("load REST environment")?;
    if let Some(bind_addr) = init.rest_addr {
        config.bind_addr = bind_addr;
    }
    config.validate().context("validate embedded REST config")?;
    Ok(Some(config))
}

/// Prints the daemon-owned local REST bearer token.
pub async fn show_token(cfg: &ClientConfig) -> Result<()> {
    let token = mantissa_client::rest::show_token(cfg).await?;
    output::emit_line(token);
    Ok(())
}

/// Rotates the daemon-owned local REST bearer token and prints the new value.
pub async fn rotate_token(cfg: &ClientConfig) -> Result<()> {
    let token = mantissa_client::rest::rotate_token(cfg).await?;
    output::emit_line(token);
    Ok(())
}

/// Returns true when embedded REST was requested by flag or environment.
fn rest_requested(init: &InitArgs) -> bool {
    init.rest || env_flag_enabled(ENV_REST_ENABLED)
}

/// Parses one boolean environment flag using the existing config conventions.
fn env_flag_enabled(name: &str) -> bool {
    matches!(
        env::var(name).as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Builds init arguments with REST disabled unless a test overrides fields.
    fn init_args() -> InitArgs {
        InitArgs {
            debug: false,
            detach: false,
            daemon_child: false,
            advertise: None,
            reset_identity: false,
            state_dir: None,
            log_file: None,
            detach_timeout: Duration::from_secs(10),
            master_key_passphrase_file: None,
            master_key_passphrase_fd: None,
            rest: false,
            rest_addr: None,
        }
    }

    #[test]
    fn config_from_init_applies_cli_addr_override() {
        let init = InitArgs {
            rest: true,
            rest_addr: Some("0.0.0.0:6580".parse().unwrap()),
            ..init_args()
        };

        let config = config_from_init(&init)
            .unwrap()
            .expect("REST config requested");

        assert_eq!(config.bind_addr, "0.0.0.0:6580".parse().unwrap());
    }

    #[test]
    fn config_from_init_returns_none_when_rest_is_not_requested() {
        assert!(config_from_init(&init_args()).unwrap().is_none());
    }
}
