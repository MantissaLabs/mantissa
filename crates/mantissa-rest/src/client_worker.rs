use mantissa_client::{config::ClientConfig, connection};
use tokio::sync::{mpsc, oneshot};

const CLIENT_COMMAND_BUFFER: usize = 32;

/// Handle used by HTTP handlers to request local Mantissa client work.
#[derive(Clone)]
pub struct ClientWorkerHandle {
    sender: mpsc::Sender<ClientCommand>,
}

impl ClientWorkerHandle {
    /// Starts a dedicated local-task worker for Cap'n Proto client calls.
    pub fn spawn(config: ClientConfig) -> Result<Self, ClientWorkerError> {
        let (sender, receiver) = mpsc::channel(CLIENT_COMMAND_BUFFER);
        std::thread::Builder::new()
            .name("mantissa-rest-client".to_string())
            .spawn(move || run_client_thread(config, receiver))
            .map_err(|error| ClientWorkerError::Runtime(error.to_string()))?;
        Ok(Self { sender })
    }

    /// Sends one health command to the worker and awaits the daemon result.
    pub async fn health(&self) -> Result<ClientHealth, ClientWorkerError> {
        let (respond_to, response) = oneshot::channel();
        self.sender
            .send(ClientCommand::Health { respond_to })
            .await
            .map_err(|_| ClientWorkerError::RequestChannelClosed)?;
        response
            .await
            .map_err(|_| ClientWorkerError::ResponseChannelClosed)?
    }

    /// Builds a deterministic worker handle for route tests.
    #[cfg(test)]
    pub fn fixed_health_for_tests(result: Result<ClientHealth, ClientWorkerError>) -> Self {
        let (sender, mut receiver) = mpsc::channel(CLIENT_COMMAND_BUFFER);
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    ClientCommand::Health { respond_to } => {
                        let _ignored = respond_to.send(result.clone());
                    }
                }
            }
        });
        Self { sender }
    }
}

/// Minimal daemon health result returned by the local client worker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientHealth {
    pub daemon_reachable: bool,
}

/// Worker failures before a route can produce a domain response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientWorkerError {
    DaemonUnavailable(String),
    RequestChannelClosed,
    ResponseChannelClosed,
    Runtime(String),
}

impl std::fmt::Display for ClientWorkerError {
    /// Formats worker errors for REST error responses and logs.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DaemonUnavailable(message) => write!(formatter, "{message}"),
            Self::RequestChannelClosed => write!(formatter, "REST client worker is closed"),
            Self::ResponseChannelClosed => write!(formatter, "REST client worker stopped"),
            Self::Runtime(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for ClientWorkerError {}

/// Commands accepted by the local Cap'n Proto client worker.
enum ClientCommand {
    Health {
        respond_to: oneshot::Sender<Result<ClientHealth, ClientWorkerError>>,
    },
}

/// Runs the client worker on a current-thread runtime with a local task set.
fn run_client_thread(config: ClientConfig, receiver: mpsc::Receiver<ClientCommand>) {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(client_worker_loop(config, receiver)));
}

/// Processes worker commands until all HTTP handles have been dropped.
async fn client_worker_loop(config: ClientConfig, mut receiver: mpsc::Receiver<ClientCommand>) {
    while let Some(command) = receiver.recv().await {
        match command {
            ClientCommand::Health { respond_to } => {
                let result = check_daemon_health(&config).await;
                let _ignored = respond_to.send(result);
            }
        }
    }
}

/// Opens the local Cap'n Proto session and sends a lightweight ping.
async fn check_daemon_health(config: &ClientConfig) -> Result<ClientHealth, ClientWorkerError> {
    let session = connection::get_local_session(config)
        .await
        .map_err(|error| ClientWorkerError::DaemonUnavailable(error.to_string()))?;
    session
        .ping_request()
        .send()
        .promise
        .await
        .map_err(|error| ClientWorkerError::DaemonUnavailable(error.to_string()))?;
    Ok(ClientHealth {
        daemon_reachable: true,
    })
}
