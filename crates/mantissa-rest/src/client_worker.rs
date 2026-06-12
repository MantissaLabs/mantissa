use crate::types::{
    clusters::{ClusterSummary, ClusterView, ClusterViewSummary},
    jobs::{JobDetail, JobSummary},
    networks::{NetworkInspect, NetworkSummary},
    nodes::NodeSummary,
    scheduler::SchedulerSummary,
    services::ServiceSummary,
    volumes::{VolumeInspect, VolumeSummary},
};
use mantissa_client::{
    clusters, config::ClientConfig, connection, jobs, networks, nodes, scheduler, services, volumes,
};
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
        self.send(ClientCommand::Health).await
    }

    /// Lists cluster nodes visible through the local topology capability.
    pub async fn list_nodes(&self) -> Result<Vec<NodeSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListNodes).await
    }

    /// Fetches one node summary by node UUID string.
    pub async fn get_node(&self, node_id: String) -> Result<NodeSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetNode {
            node_id,
            respond_to,
        })
        .await
    }

    /// Lists first-class jobs visible through the local jobs capability.
    pub async fn list_jobs(&self) -> Result<Vec<JobSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListJobs).await
    }

    /// Fetches one first-class job detail by UUID or accepted job selector.
    pub async fn get_job(&self, job_id: String) -> Result<JobDetail, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetJob { job_id, respond_to })
            .await
    }

    /// Lists services visible through the local services capability.
    pub async fn list_services(&self) -> Result<Vec<ServiceSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListServices).await
    }

    /// Fetches one service by UUID text or exact service name.
    pub async fn get_service(&self, selector: String) -> Result<ServiceSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetService {
            selector,
            respond_to,
        })
        .await
    }

    /// Fetches one service status snapshot by UUID text or exact service name.
    pub async fn get_service_status(
        &self,
        selector: String,
    ) -> Result<ServiceSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetServiceStatus {
            selector,
            respond_to,
        })
        .await
    }

    /// Lists overlay networks visible through the local networks capability.
    pub async fn list_networks(&self) -> Result<Vec<NetworkSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListNetworks).await
    }

    /// Fetches one overlay network inspection by UUID string.
    pub async fn get_network(
        &self,
        network_id: String,
    ) -> Result<NetworkInspect, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetNetwork {
            network_id,
            respond_to,
        })
        .await
    }

    /// Lists volumes visible through the local volumes capability.
    pub async fn list_volumes(&self) -> Result<Vec<VolumeSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListVolumes).await
    }

    /// Fetches one volume inspection by UUID text or exact volume name.
    pub async fn get_volume(&self, selector: String) -> Result<VolumeInspect, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetVolume {
            selector,
            respond_to,
        })
        .await
    }

    /// Fetches one volume status snapshot by UUID text or exact volume name.
    pub async fn get_volume_status(
        &self,
        selector: String,
    ) -> Result<VolumeInspect, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetVolumeStatus {
            selector,
            respond_to,
        })
        .await
    }

    /// Fetches a scheduler capacity summary.
    pub async fn scheduler_summary(
        &self,
        peer_id: Option<String>,
        details: bool,
    ) -> Result<SchedulerSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::SchedulerSummary {
            peer_id,
            details,
            respond_to,
        })
        .await
    }

    /// Lists cluster lineage summaries known to the local node.
    pub async fn list_clusters(&self) -> Result<Vec<ClusterSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListClusters).await
    }

    /// Lists raw cluster view summaries known to the local node.
    pub async fn list_cluster_views(&self) -> Result<Vec<ClusterViewSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListClusterViews).await
    }

    /// Fetches the active cluster view associated with the local session.
    pub async fn active_cluster_view(&self) -> Result<ClusterView, ClientWorkerError> {
        self.send(ClientCommand::ActiveClusterView).await
    }

    /// Sends one typed command to the worker and awaits its typed response.
    async fn send<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, ClientWorkerError>>) -> ClientCommand,
    ) -> Result<T, ClientWorkerError>
    where
        T: Send + 'static,
    {
        let (respond_to, response) = oneshot::channel();
        self.sender
            .send(command(respond_to))
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
                if let ClientCommand::Health(respond_to) = command {
                    let _ignored = respond_to.send(result.clone());
                }
            }
        });
        Self { sender }
    }

    /// Builds a deterministic node-list worker handle for route tests.
    #[cfg(test)]
    pub fn fixed_nodes_for_tests(result: Result<Vec<NodeSummary>, ClientWorkerError>) -> Self {
        let (sender, mut receiver) = mpsc::channel(CLIENT_COMMAND_BUFFER);
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                if let ClientCommand::ListNodes(respond_to) = command {
                    let _ignored = respond_to.send(result.clone());
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
    InvalidRequest(String),
    NotFound(String),
    OperationFailed(String),
    RequestChannelClosed,
    ResponseChannelClosed,
    Runtime(String),
}

impl std::fmt::Display for ClientWorkerError {
    /// Formats worker errors for REST error responses and logs.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DaemonUnavailable(message) => write!(formatter, "{message}"),
            Self::InvalidRequest(message) => write!(formatter, "{message}"),
            Self::NotFound(message) => write!(formatter, "{message}"),
            Self::OperationFailed(message) => write!(formatter, "{message}"),
            Self::RequestChannelClosed => write!(formatter, "REST client worker is closed"),
            Self::ResponseChannelClosed => write!(formatter, "REST client worker stopped"),
            Self::Runtime(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for ClientWorkerError {}

/// Commands accepted by the local Cap'n Proto client worker.
enum ClientCommand {
    Health(oneshot::Sender<Result<ClientHealth, ClientWorkerError>>),
    ListNodes(oneshot::Sender<Result<Vec<NodeSummary>, ClientWorkerError>>),
    GetNode {
        node_id: String,
        respond_to: oneshot::Sender<Result<NodeSummary, ClientWorkerError>>,
    },
    ListJobs(oneshot::Sender<Result<Vec<JobSummary>, ClientWorkerError>>),
    GetJob {
        job_id: String,
        respond_to: oneshot::Sender<Result<JobDetail, ClientWorkerError>>,
    },
    ListServices(oneshot::Sender<Result<Vec<ServiceSummary>, ClientWorkerError>>),
    GetService {
        selector: String,
        respond_to: oneshot::Sender<Result<ServiceSummary, ClientWorkerError>>,
    },
    GetServiceStatus {
        selector: String,
        respond_to: oneshot::Sender<Result<ServiceSummary, ClientWorkerError>>,
    },
    ListNetworks(oneshot::Sender<Result<Vec<NetworkSummary>, ClientWorkerError>>),
    GetNetwork {
        network_id: String,
        respond_to: oneshot::Sender<Result<NetworkInspect, ClientWorkerError>>,
    },
    ListVolumes(oneshot::Sender<Result<Vec<VolumeSummary>, ClientWorkerError>>),
    GetVolume {
        selector: String,
        respond_to: oneshot::Sender<Result<VolumeInspect, ClientWorkerError>>,
    },
    GetVolumeStatus {
        selector: String,
        respond_to: oneshot::Sender<Result<VolumeInspect, ClientWorkerError>>,
    },
    SchedulerSummary {
        peer_id: Option<String>,
        details: bool,
        respond_to: oneshot::Sender<Result<SchedulerSummary, ClientWorkerError>>,
    },
    ListClusters(oneshot::Sender<Result<Vec<ClusterSummary>, ClientWorkerError>>),
    ListClusterViews(oneshot::Sender<Result<Vec<ClusterViewSummary>, ClientWorkerError>>),
    ActiveClusterView(oneshot::Sender<Result<ClusterView, ClientWorkerError>>),
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
            ClientCommand::Health(respond_to) => {
                let result = check_daemon_health(&config).await;
                let _ignored = respond_to.send(result);
            }
            ClientCommand::ListNodes(respond_to) => {
                let _ignored = respond_to.send(list_nodes(&config).await);
            }
            ClientCommand::GetNode {
                node_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(get_node(&config, &node_id).await);
            }
            ClientCommand::ListJobs(respond_to) => {
                let _ignored = respond_to.send(list_jobs(&config).await);
            }
            ClientCommand::GetJob { job_id, respond_to } => {
                let _ignored = respond_to.send(get_job(&config, &job_id).await);
            }
            ClientCommand::ListServices(respond_to) => {
                let _ignored = respond_to.send(list_services(&config).await);
            }
            ClientCommand::GetService {
                selector,
                respond_to,
            } => {
                let _ignored = respond_to.send(get_service(&config, &selector).await);
            }
            ClientCommand::GetServiceStatus {
                selector,
                respond_to,
            } => {
                let _ignored = respond_to.send(get_service_status(&config, &selector).await);
            }
            ClientCommand::ListNetworks(respond_to) => {
                let _ignored = respond_to.send(list_networks(&config).await);
            }
            ClientCommand::GetNetwork {
                network_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(get_network(&config, &network_id).await);
            }
            ClientCommand::ListVolumes(respond_to) => {
                let _ignored = respond_to.send(list_volumes(&config).await);
            }
            ClientCommand::GetVolume {
                selector,
                respond_to,
            } => {
                let _ignored = respond_to.send(get_volume(&config, &selector).await);
            }
            ClientCommand::GetVolumeStatus {
                selector,
                respond_to,
            } => {
                let _ignored = respond_to.send(get_volume_status(&config, &selector).await);
            }
            ClientCommand::SchedulerSummary {
                peer_id,
                details,
                respond_to,
            } => {
                let _ignored = respond_to.send(scheduler_summary(&config, peer_id, details).await);
            }
            ClientCommand::ListClusters(respond_to) => {
                let _ignored = respond_to.send(list_clusters(&config).await);
            }
            ClientCommand::ListClusterViews(respond_to) => {
                let _ignored = respond_to.send(list_cluster_views(&config).await);
            }
            ClientCommand::ActiveClusterView(respond_to) => {
                let _ignored = respond_to.send(active_cluster_view(&config).await);
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

/// Lists nodes through the reusable Mantissa client API.
async fn list_nodes(config: &ClientConfig) -> Result<Vec<NodeSummary>, ClientWorkerError> {
    nodes::list(config)
        .await
        .map(|nodes| nodes.into_iter().map(NodeSummary::from).collect())
        .map_err(operation_error)
}

/// Fetches one node summary from the topology list response.
async fn get_node(config: &ClientConfig, node_id: &str) -> Result<NodeSummary, ClientWorkerError> {
    let nodes = list_nodes(config).await?;
    nodes
        .into_iter()
        .find(|node| node.id == node_id)
        .ok_or_else(|| ClientWorkerError::NotFound(format!("node '{node_id}' not found")))
}

/// Lists jobs through the reusable Mantissa client API.
async fn list_jobs(config: &ClientConfig) -> Result<Vec<JobSummary>, ClientWorkerError> {
    jobs::list(config)
        .await
        .map(|jobs| jobs.into_iter().map(JobSummary::from).collect())
        .map_err(operation_error)
}

/// Fetches one job detail through the reusable Mantissa client API.
async fn get_job(config: &ClientConfig, job_id: &str) -> Result<JobDetail, ClientWorkerError> {
    jobs::inspect(config, job_id)
        .await
        .map(JobDetail::from)
        .map_err(operation_error)
}

/// Lists services through the reusable Mantissa client API.
async fn list_services(config: &ClientConfig) -> Result<Vec<ServiceSummary>, ClientWorkerError> {
    services::list(config)
        .await
        .map(|services| services.into_iter().map(ServiceSummary::from).collect())
        .map_err(operation_error)
}

/// Fetches one service through the reusable Mantissa client API.
async fn get_service(
    config: &ClientConfig,
    selector: &str,
) -> Result<ServiceSummary, ClientWorkerError> {
    services::list::inspect_service_row(config, selector)
        .await
        .map(ServiceSummary::from)
        .map_err(operation_error)
}

/// Fetches one service status through the reusable Mantissa client API.
async fn get_service_status(
    config: &ClientConfig,
    selector: &str,
) -> Result<ServiceSummary, ClientWorkerError> {
    services::rollout_status(config, selector)
        .await
        .map(ServiceSummary::from)
        .map_err(operation_error)
}

/// Lists networks through the reusable Mantissa client API.
async fn list_networks(config: &ClientConfig) -> Result<Vec<NetworkSummary>, ClientWorkerError> {
    networks::list(config)
        .await
        .map(|networks| networks.into_iter().map(NetworkSummary::from).collect())
        .map_err(operation_error)
}

/// Fetches one network inspection through the reusable Mantissa client API.
async fn get_network(
    config: &ClientConfig,
    network_id: &str,
) -> Result<NetworkInspect, ClientWorkerError> {
    networks::inspect(config, network_id)
        .await
        .map(NetworkInspect::from)
        .map_err(operation_error)
}

/// Lists volumes through the reusable Mantissa client API.
async fn list_volumes(config: &ClientConfig) -> Result<Vec<VolumeSummary>, ClientWorkerError> {
    volumes::list(config)
        .await
        .map(|volumes| volumes.into_iter().map(VolumeSummary::from).collect())
        .map_err(operation_error)
}

/// Fetches one volume inspection through the reusable Mantissa client API.
async fn get_volume(
    config: &ClientConfig,
    selector: &str,
) -> Result<VolumeInspect, ClientWorkerError> {
    volumes::inspect(config, selector)
        .await
        .map(VolumeInspect::from)
        .map_err(operation_error)
}

/// Fetches one volume status through the reusable Mantissa client API.
async fn get_volume_status(
    config: &ClientConfig,
    selector: &str,
) -> Result<VolumeInspect, ClientWorkerError> {
    volumes::status(config, selector)
        .await
        .map(VolumeInspect::from)
        .map_err(operation_error)
}

/// Fetches scheduler summary through the reusable Mantissa client API.
async fn scheduler_summary(
    config: &ClientConfig,
    peer_id: Option<String>,
    details: bool,
) -> Result<SchedulerSummary, ClientWorkerError> {
    scheduler::slots(config, peer_id.as_deref(), details)
        .await
        .map(SchedulerSummary::from)
        .map_err(operation_error)
}

/// Lists cluster lineage summaries through the reusable Mantissa client API.
async fn list_clusters(config: &ClientConfig) -> Result<Vec<ClusterSummary>, ClientWorkerError> {
    clusters::list_clusters(config)
        .await
        .map(|clusters| clusters.into_iter().map(ClusterSummary::from).collect())
        .map_err(operation_error)
}

/// Lists cluster view summaries through the reusable Mantissa client API.
async fn list_cluster_views(
    config: &ClientConfig,
) -> Result<Vec<ClusterViewSummary>, ClientWorkerError> {
    clusters::list_cluster_views(config)
        .await
        .map(|views| views.into_iter().map(ClusterViewSummary::from).collect())
        .map_err(operation_error)
}

/// Fetches the active cluster view through the reusable Mantissa client API.
async fn active_cluster_view(config: &ClientConfig) -> Result<ClusterView, ClientWorkerError> {
    clusters::active_cluster_view(config)
        .await
        .map(ClusterView::from)
        .map_err(operation_error)
}

/// Converts a client operation failure into a worker error.
fn operation_error(error: impl std::fmt::Display) -> ClientWorkerError {
    ClientWorkerError::OperationFailed(error.to_string())
}
