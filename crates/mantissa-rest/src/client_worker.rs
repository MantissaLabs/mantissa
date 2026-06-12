use crate::types::{
    clusters::{ClusterSummary, ClusterView, ClusterViewSummary},
    jobs::{JobDetail, JobSubmitRequest, JobSubmitResponse, JobSummary},
    networks::{
        NetworkCreateRequest, NetworkCreateResponse, NetworkDeleteResponse, NetworkInspect,
        NetworkSummary,
    },
    nodes::{NodeActionResponse, NodeDrainRequest, NodeSummary},
    scheduler::SchedulerSummary,
    secrets::{SecretDeleteResponse, SecretDetail, SecretSummary, SecretUpsertRequest},
    services::{ServiceDeployRequest, ServiceDeployResponse, ServiceSummary},
    tasks::{TaskStartRequest, TaskSummary},
    volumes::{
        VolumeCreateRequest, VolumeDeleteResponse, VolumeImportRequest, VolumeInspect, VolumeSpec,
        VolumeSummary,
    },
};
use mantissa_client::{
    clusters, config::ClientConfig, connection, jobs, networks, nodes, scheduler, secrets,
    services, tasks, volumes,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

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

    /// Submits one first-class job from a REST manifest request.
    pub async fn submit_job(
        &self,
        request: JobSubmitRequest,
    ) -> Result<JobSubmitResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::SubmitJob {
            request: Box::new(request),
            respond_to,
        })
        .await
    }

    /// Fetches one first-class job detail by UUID or accepted job selector.
    pub async fn get_job(&self, job_id: String) -> Result<JobDetail, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetJob { job_id, respond_to })
            .await
    }

    /// Cancels one first-class job by UUID or accepted job selector.
    pub async fn cancel_job(&self, job_id: String) -> Result<JobSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::CancelJob { job_id, respond_to })
            .await
    }

    /// Deletes one terminal first-class job by UUID or accepted job selector.
    pub async fn delete_job(&self, job_id: String) -> Result<JobSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::DeleteJob { job_id, respond_to })
            .await
    }

    /// Lists services visible through the local services capability.
    pub async fn list_services(&self) -> Result<Vec<ServiceSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListServices).await
    }

    /// Deploys or updates one service from a REST manifest request.
    pub async fn deploy_service(
        &self,
        request: ServiceDeployRequest,
    ) -> Result<ServiceDeployResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::DeployService {
            request: Box::new(request),
            respond_to,
        })
        .await
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

    /// Deletes one service by UUID text or exact service name.
    pub async fn delete_service(
        &self,
        selector: String,
    ) -> Result<ServiceSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::DeleteService {
            selector,
            respond_to,
        })
        .await
    }

    /// Lists overlay networks visible through the local networks capability.
    pub async fn list_networks(&self) -> Result<Vec<NetworkSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListNetworks).await
    }

    /// Creates one overlay network.
    pub async fn create_network(
        &self,
        request: NetworkCreateRequest,
    ) -> Result<NetworkCreateResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::CreateNetwork {
            request: Box::new(request),
            respond_to,
        })
        .await
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

    /// Deletes one overlay network by UUID string.
    pub async fn delete_network(
        &self,
        network_id: String,
    ) -> Result<NetworkDeleteResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::DeleteNetwork {
            network_id,
            respond_to,
        })
        .await
    }

    /// Lists volumes visible through the local volumes capability.
    pub async fn list_volumes(&self) -> Result<Vec<VolumeSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListVolumes).await
    }

    /// Creates one managed local volume.
    pub async fn create_volume(
        &self,
        request: VolumeCreateRequest,
    ) -> Result<VolumeSpec, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::CreateVolume {
            request: Box::new(request),
            respond_to,
        })
        .await
    }

    /// Imports one existing local path as a volume.
    pub async fn import_volume(
        &self,
        request: VolumeImportRequest,
    ) -> Result<VolumeSpec, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::ImportVolume {
            request: Box::new(request),
            respond_to,
        })
        .await
    }

    /// Fetches one volume inspection by UUID text or exact volume name.
    pub async fn get_volume(&self, selector: String) -> Result<VolumeInspect, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetVolume {
            selector,
            respond_to,
        })
        .await
    }

    /// Deletes one volume by UUID text or exact volume name.
    pub async fn delete_volume(
        &self,
        selector: String,
    ) -> Result<VolumeDeleteResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::DeleteVolume {
            selector,
            respond_to,
        })
        .await
    }

    /// Lists standalone tasks visible through the task capability.
    pub async fn list_tasks(&self) -> Result<Vec<TaskSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListTasks).await
    }

    /// Fetches one standalone task by UUID text or exact task name.
    pub async fn get_task(&self, selector: String) -> Result<TaskSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetTask {
            selector,
            respond_to,
        })
        .await
    }

    /// Starts one standalone task.
    pub async fn start_task(
        &self,
        request: TaskStartRequest,
    ) -> Result<TaskSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::StartTask {
            request: Box::new(request),
            respond_to,
        })
        .await
    }

    /// Stops one standalone task by selector.
    pub async fn stop_task(&self, selector: String) -> Result<TaskSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::StopTask {
            selector,
            respond_to,
        })
        .await
    }

    /// Lists secret summaries visible through the secrets capability.
    pub async fn list_secrets(&self) -> Result<Vec<SecretSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListSecrets).await
    }

    /// Creates one secret by name.
    pub async fn create_secret(
        &self,
        name: String,
        request: SecretUpsertRequest,
    ) -> Result<SecretSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::CreateSecret {
            name,
            request: Box::new(request),
            respond_to,
        })
        .await
    }

    /// Updates one secret by name.
    pub async fn update_secret(
        &self,
        name: String,
        request: SecretUpsertRequest,
    ) -> Result<SecretSummary, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::UpdateSecret {
            name,
            request: Box::new(request),
            respond_to,
        })
        .await
    }

    /// Fetches one secret detail by name and optional version UUID.
    pub async fn get_secret(
        &self,
        name: String,
        version_id: Option<String>,
    ) -> Result<SecretDetail, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetSecret {
            name,
            version_id,
            respond_to,
        })
        .await
    }

    /// Deletes one secret by name.
    pub async fn delete_secret(
        &self,
        name: String,
    ) -> Result<SecretDeleteResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::DeleteSecret { name, respond_to })
            .await
    }

    /// Requests node drain by UUID string.
    pub async fn drain_node(
        &self,
        node_id: String,
        request: NodeDrainRequest,
    ) -> Result<NodeActionResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::DrainNode {
            node_id,
            request: Box::new(request),
            respond_to,
        })
        .await
    }

    /// Resumes one drained node by UUID string.
    pub async fn resume_node(
        &self,
        node_id: String,
    ) -> Result<NodeActionResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::ResumeNode {
            node_id,
            respond_to,
        })
        .await
    }

    /// Evicts one stale node by UUID string.
    pub async fn evict_node(
        &self,
        node_id: String,
    ) -> Result<NodeActionResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::EvictNode {
            node_id,
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
    SubmitJob {
        request: Box<JobSubmitRequest>,
        respond_to: oneshot::Sender<Result<JobSubmitResponse, ClientWorkerError>>,
    },
    GetJob {
        job_id: String,
        respond_to: oneshot::Sender<Result<JobDetail, ClientWorkerError>>,
    },
    CancelJob {
        job_id: String,
        respond_to: oneshot::Sender<Result<JobSummary, ClientWorkerError>>,
    },
    DeleteJob {
        job_id: String,
        respond_to: oneshot::Sender<Result<JobSummary, ClientWorkerError>>,
    },
    ListServices(oneshot::Sender<Result<Vec<ServiceSummary>, ClientWorkerError>>),
    DeployService {
        request: Box<ServiceDeployRequest>,
        respond_to: oneshot::Sender<Result<ServiceDeployResponse, ClientWorkerError>>,
    },
    GetService {
        selector: String,
        respond_to: oneshot::Sender<Result<ServiceSummary, ClientWorkerError>>,
    },
    GetServiceStatus {
        selector: String,
        respond_to: oneshot::Sender<Result<ServiceSummary, ClientWorkerError>>,
    },
    DeleteService {
        selector: String,
        respond_to: oneshot::Sender<Result<ServiceSummary, ClientWorkerError>>,
    },
    ListNetworks(oneshot::Sender<Result<Vec<NetworkSummary>, ClientWorkerError>>),
    CreateNetwork {
        request: Box<NetworkCreateRequest>,
        respond_to: oneshot::Sender<Result<NetworkCreateResponse, ClientWorkerError>>,
    },
    GetNetwork {
        network_id: String,
        respond_to: oneshot::Sender<Result<NetworkInspect, ClientWorkerError>>,
    },
    DeleteNetwork {
        network_id: String,
        respond_to: oneshot::Sender<Result<NetworkDeleteResponse, ClientWorkerError>>,
    },
    ListVolumes(oneshot::Sender<Result<Vec<VolumeSummary>, ClientWorkerError>>),
    CreateVolume {
        request: Box<VolumeCreateRequest>,
        respond_to: oneshot::Sender<Result<VolumeSpec, ClientWorkerError>>,
    },
    ImportVolume {
        request: Box<VolumeImportRequest>,
        respond_to: oneshot::Sender<Result<VolumeSpec, ClientWorkerError>>,
    },
    GetVolume {
        selector: String,
        respond_to: oneshot::Sender<Result<VolumeInspect, ClientWorkerError>>,
    },
    GetVolumeStatus {
        selector: String,
        respond_to: oneshot::Sender<Result<VolumeInspect, ClientWorkerError>>,
    },
    DeleteVolume {
        selector: String,
        respond_to: oneshot::Sender<Result<VolumeDeleteResponse, ClientWorkerError>>,
    },
    ListTasks(oneshot::Sender<Result<Vec<TaskSummary>, ClientWorkerError>>),
    GetTask {
        selector: String,
        respond_to: oneshot::Sender<Result<TaskSummary, ClientWorkerError>>,
    },
    StartTask {
        request: Box<TaskStartRequest>,
        respond_to: oneshot::Sender<Result<TaskSummary, ClientWorkerError>>,
    },
    StopTask {
        selector: String,
        respond_to: oneshot::Sender<Result<TaskSummary, ClientWorkerError>>,
    },
    ListSecrets(oneshot::Sender<Result<Vec<SecretSummary>, ClientWorkerError>>),
    CreateSecret {
        name: String,
        request: Box<SecretUpsertRequest>,
        respond_to: oneshot::Sender<Result<SecretSummary, ClientWorkerError>>,
    },
    UpdateSecret {
        name: String,
        request: Box<SecretUpsertRequest>,
        respond_to: oneshot::Sender<Result<SecretSummary, ClientWorkerError>>,
    },
    GetSecret {
        name: String,
        version_id: Option<String>,
        respond_to: oneshot::Sender<Result<SecretDetail, ClientWorkerError>>,
    },
    DeleteSecret {
        name: String,
        respond_to: oneshot::Sender<Result<SecretDeleteResponse, ClientWorkerError>>,
    },
    DrainNode {
        node_id: String,
        request: Box<NodeDrainRequest>,
        respond_to: oneshot::Sender<Result<NodeActionResponse, ClientWorkerError>>,
    },
    ResumeNode {
        node_id: String,
        respond_to: oneshot::Sender<Result<NodeActionResponse, ClientWorkerError>>,
    },
    EvictNode {
        node_id: String,
        respond_to: oneshot::Sender<Result<NodeActionResponse, ClientWorkerError>>,
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
            ClientCommand::SubmitJob {
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(submit_job(&config, *request).await);
            }
            ClientCommand::GetJob { job_id, respond_to } => {
                let _ignored = respond_to.send(get_job(&config, &job_id).await);
            }
            ClientCommand::CancelJob { job_id, respond_to } => {
                let _ignored = respond_to.send(cancel_job(&config, &job_id).await);
            }
            ClientCommand::DeleteJob { job_id, respond_to } => {
                let _ignored = respond_to.send(delete_job(&config, &job_id).await);
            }
            ClientCommand::ListServices(respond_to) => {
                let _ignored = respond_to.send(list_services(&config).await);
            }
            ClientCommand::DeployService {
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(deploy_service(&config, *request).await);
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
            ClientCommand::DeleteService {
                selector,
                respond_to,
            } => {
                let _ignored = respond_to.send(delete_service(&config, &selector).await);
            }
            ClientCommand::ListNetworks(respond_to) => {
                let _ignored = respond_to.send(list_networks(&config).await);
            }
            ClientCommand::CreateNetwork {
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(create_network(&config, *request).await);
            }
            ClientCommand::GetNetwork {
                network_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(get_network(&config, &network_id).await);
            }
            ClientCommand::DeleteNetwork {
                network_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(delete_network(&config, network_id).await);
            }
            ClientCommand::ListVolumes(respond_to) => {
                let _ignored = respond_to.send(list_volumes(&config).await);
            }
            ClientCommand::CreateVolume {
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(create_volume(&config, *request).await);
            }
            ClientCommand::ImportVolume {
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(import_volume(&config, *request).await);
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
            ClientCommand::DeleteVolume {
                selector,
                respond_to,
            } => {
                let _ignored = respond_to.send(delete_volume(&config, &selector).await);
            }
            ClientCommand::ListTasks(respond_to) => {
                let _ignored = respond_to.send(list_tasks(&config).await);
            }
            ClientCommand::GetTask {
                selector,
                respond_to,
            } => {
                let _ignored = respond_to.send(get_task(&config, &selector).await);
            }
            ClientCommand::StartTask {
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(start_task(&config, *request).await);
            }
            ClientCommand::StopTask {
                selector,
                respond_to,
            } => {
                let _ignored = respond_to.send(stop_task(&config, &selector).await);
            }
            ClientCommand::ListSecrets(respond_to) => {
                let _ignored = respond_to.send(list_secrets(&config).await);
            }
            ClientCommand::CreateSecret {
                name,
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(create_secret(&config, &name, *request).await);
            }
            ClientCommand::UpdateSecret {
                name,
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(update_secret(&config, &name, *request).await);
            }
            ClientCommand::GetSecret {
                name,
                version_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(get_secret(&config, &name, version_id).await);
            }
            ClientCommand::DeleteSecret { name, respond_to } => {
                let _ignored = respond_to.send(delete_secret(&config, &name).await);
            }
            ClientCommand::DrainNode {
                node_id,
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(drain_node(&config, &node_id, *request).await);
            }
            ClientCommand::ResumeNode {
                node_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(resume_node(&config, &node_id).await);
            }
            ClientCommand::EvictNode {
                node_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(evict_node(&config, &node_id).await);
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
    parse_uuid("job id", job_id)?;
    jobs::inspect(config, job_id)
        .await
        .map(JobDetail::from)
        .map_err(operation_error)
}

/// Submits one job manifest through the reusable Mantissa client API.
async fn submit_job(
    config: &ClientConfig,
    request: JobSubmitRequest,
) -> Result<JobSubmitResponse, ClientWorkerError> {
    jobs::run_manifest(config, &request.manifest)
        .await
        .map(JobSubmitResponse::from)
        .map_err(operation_error)
}

/// Cancels one job through the reusable Mantissa client API.
async fn cancel_job(config: &ClientConfig, job_id: &str) -> Result<JobSummary, ClientWorkerError> {
    parse_uuid("job id", job_id)?;
    jobs::cancel(config, job_id)
        .await
        .map(JobSummary::from)
        .map_err(operation_error)
}

/// Deletes one terminal job through the reusable Mantissa client API.
async fn delete_job(config: &ClientConfig, job_id: &str) -> Result<JobSummary, ClientWorkerError> {
    parse_uuid("job id", job_id)?;
    jobs::delete(config, job_id)
        .await
        .map(JobSummary::from)
        .map_err(operation_error)
}

/// Lists services through the reusable Mantissa client API.
async fn list_services(config: &ClientConfig) -> Result<Vec<ServiceSummary>, ClientWorkerError> {
    services::list(config)
        .await
        .map(|services| services.into_iter().map(ServiceSummary::from).collect())
        .map_err(operation_error)
}

/// Deploys one service manifest through the reusable Mantissa client API.
async fn deploy_service(
    config: &ClientConfig,
    request: ServiceDeployRequest,
) -> Result<ServiceDeployResponse, ClientWorkerError> {
    services::deploy_manifest(config, &request.manifest)
        .await
        .map(ServiceDeployResponse::from)
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

/// Deletes one service through the reusable Mantissa client API.
async fn delete_service(
    config: &ClientConfig,
    selector: &str,
) -> Result<ServiceSummary, ClientWorkerError> {
    let service = services::list::inspect_service_row(config, selector)
        .await
        .map_err(operation_error)?;
    services::stop(config, &service.service_id.to_string())
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

/// Creates one network through the reusable Mantissa client API.
async fn create_network(
    config: &ClientConfig,
    request: NetworkCreateRequest,
) -> Result<NetworkCreateResponse, ClientWorkerError> {
    let request = request.into();
    networks::create(config, &request)
        .await
        .map(|network_id| NetworkCreateResponse {
            network_id: network_id.to_string(),
        })
        .map_err(operation_error)
}

/// Fetches one network inspection through the reusable Mantissa client API.
async fn get_network(
    config: &ClientConfig,
    network_id: &str,
) -> Result<NetworkInspect, ClientWorkerError> {
    parse_uuid("network id", network_id)?;
    networks::inspect(config, network_id)
        .await
        .map(NetworkInspect::from)
        .map_err(operation_error)
}

/// Deletes one network through the reusable Mantissa client API.
async fn delete_network(
    config: &ClientConfig,
    network_id: String,
) -> Result<NetworkDeleteResponse, ClientWorkerError> {
    parse_uuid("network id", &network_id)?;
    networks::delete(config, &[network_id])
        .await
        .map(|deleted| NetworkDeleteResponse { deleted })
        .map_err(operation_error)
}

/// Lists volumes through the reusable Mantissa client API.
async fn list_volumes(config: &ClientConfig) -> Result<Vec<VolumeSummary>, ClientWorkerError> {
    volumes::list(config)
        .await
        .map(|volumes| volumes.into_iter().map(VolumeSummary::from).collect())
        .map_err(operation_error)
}

/// Creates one volume through the reusable Mantissa client API.
async fn create_volume(
    config: &ClientConfig,
    request: VolumeCreateRequest,
) -> Result<VolumeSpec, ClientWorkerError> {
    let request = request
        .into_client()
        .map_err(ClientWorkerError::InvalidRequest)?;
    volumes::create_with_request(config, &request)
        .await
        .map(VolumeSpec::from)
        .map_err(operation_error)
}

/// Imports one volume through the reusable Mantissa client API.
async fn import_volume(
    config: &ClientConfig,
    request: VolumeImportRequest,
) -> Result<VolumeSpec, ClientWorkerError> {
    let request = request.into();
    volumes::import_with_request(config, &request)
        .await
        .map(VolumeSpec::from)
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

/// Deletes one volume through the reusable Mantissa client API.
async fn delete_volume(
    config: &ClientConfig,
    selector: &str,
) -> Result<VolumeDeleteResponse, ClientWorkerError> {
    volumes::delete(config, selector)
        .await
        .map(VolumeDeleteResponse::from)
        .map_err(operation_error)
}

/// Lists standalone tasks through the reusable Mantissa client API.
async fn list_tasks(config: &ClientConfig) -> Result<Vec<TaskSummary>, ClientWorkerError> {
    tasks::list(config, &[])
        .await
        .map(|tasks| tasks.into_iter().map(TaskSummary::from).collect())
        .map_err(operation_error)
}

/// Fetches one standalone task from the task list response.
async fn get_task(config: &ClientConfig, selector: &str) -> Result<TaskSummary, ClientWorkerError> {
    let selector = clean_required_name("task selector", selector)?;
    let tasks = list_tasks(config).await?;
    tasks
        .into_iter()
        .find(|task| task.id == selector || task.name == selector)
        .ok_or_else(|| ClientWorkerError::NotFound(format!("task '{selector}' not found")))
}

/// Starts one standalone task through the reusable Mantissa client API.
async fn start_task(
    config: &ClientConfig,
    request: TaskStartRequest,
) -> Result<TaskSummary, ClientWorkerError> {
    let options = tasks::TaskStartOptions {
        name: &request.name,
        image: &request.image,
        command: &request.command,
        cpu_millis: request.cpu_millis,
        memory_bytes: request.memory_bytes,
        gpu_count: request.gpu_count,
        volumes: &request.volumes,
    };
    tasks::start(config, &options)
        .await
        .map(TaskSummary::from)
        .map_err(operation_error)
}

/// Stops one standalone task through the reusable Mantissa client API.
async fn stop_task(
    config: &ClientConfig,
    selector: &str,
) -> Result<TaskSummary, ClientWorkerError> {
    tasks::stop(config, selector)
        .await
        .map(TaskSummary::from)
        .map_err(operation_error)
}

/// Lists secrets through the reusable Mantissa client API.
async fn list_secrets(config: &ClientConfig) -> Result<Vec<SecretSummary>, ClientWorkerError> {
    secrets::list(config)
        .await
        .map(|secrets| secrets.into_iter().map(SecretSummary::from).collect())
        .map_err(operation_error)
}

/// Creates one secret through the reusable Mantissa client API.
async fn create_secret(
    config: &ClientConfig,
    name: &str,
    request: SecretUpsertRequest,
) -> Result<SecretSummary, ClientWorkerError> {
    let plaintext = request
        .plaintext()
        .map_err(ClientWorkerError::InvalidRequest)?;
    secrets::create(
        config,
        clean_required_name("secret name", name)?,
        &plaintext,
        request.description.as_deref(),
        &request.labels(),
    )
    .await
    .map(SecretSummary::from)
    .map_err(operation_error)
}

/// Updates one secret through the reusable Mantissa client API.
async fn update_secret(
    config: &ClientConfig,
    name: &str,
    request: SecretUpsertRequest,
) -> Result<SecretSummary, ClientWorkerError> {
    let plaintext = request
        .plaintext()
        .map_err(ClientWorkerError::InvalidRequest)?;
    secrets::update(
        config,
        clean_required_name("secret name", name)?,
        &plaintext,
        request.description.as_deref(),
        &request.labels(),
    )
    .await
    .map(SecretSummary::from)
    .map_err(operation_error)
}

/// Fetches one secret detail through the reusable Mantissa client API.
async fn get_secret(
    config: &ClientConfig,
    name: &str,
    version_id: Option<String>,
) -> Result<SecretDetail, ClientWorkerError> {
    let version_id = version_id
        .as_deref()
        .map(|value| parse_uuid("secret version id", value))
        .transpose()?;
    secrets::show(
        config,
        clean_required_name("secret name", name)?,
        version_id,
    )
    .await
    .map(SecretDetail::from)
    .map_err(operation_error)
}

/// Deletes one secret through the reusable Mantissa client API.
async fn delete_secret(
    config: &ClientConfig,
    name: &str,
) -> Result<SecretDeleteResponse, ClientWorkerError> {
    let name = clean_required_name("secret name", name)?.to_string();
    secrets::delete(config, &[name])
        .await
        .map(|deleted| SecretDeleteResponse { deleted })
        .map_err(operation_error)
}

/// Requests one node drain through the reusable Mantissa client API.
async fn drain_node(
    config: &ClientConfig,
    node_id: &str,
    request: NodeDrainRequest,
) -> Result<NodeActionResponse, ClientWorkerError> {
    let node_id = parse_uuid("node id", node_id)?;
    let timeout = request.task_stop_timeout_secs.map(Duration::from_secs);
    nodes::request_drain(config, node_id, request.reason.as_deref(), timeout)
        .await
        .map(|operation| NodeActionResponse {
            node_id: operation.node_id.to_string(),
            accepted: true,
        })
        .map_err(operation_error)
}

/// Resumes one node through the reusable Mantissa client API.
async fn resume_node(
    config: &ClientConfig,
    node_id: &str,
) -> Result<NodeActionResponse, ClientWorkerError> {
    let node_id = parse_uuid("node id", node_id)?;
    nodes::resume(config, node_id)
        .await
        .map(|()| NodeActionResponse {
            node_id: node_id.to_string(),
            accepted: true,
        })
        .map_err(operation_error)
}

/// Evicts one node through the reusable Mantissa client API.
async fn evict_node(
    config: &ClientConfig,
    node_id: &str,
) -> Result<NodeActionResponse, ClientWorkerError> {
    let node_id = parse_uuid("node id", node_id)?;
    nodes::evict(config, node_id)
        .await
        .map(|()| NodeActionResponse {
            node_id: node_id.to_string(),
            accepted: true,
        })
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

/// Parses a REST UUID path segment before issuing a client request.
fn parse_uuid(field: &str, value: &str) -> Result<Uuid, ClientWorkerError> {
    Uuid::parse_str(value.trim()).map_err(|error| {
        ClientWorkerError::InvalidRequest(format!("invalid {field} '{value}': {error}"))
    })
}

/// Validates one required path name and returns the trimmed value.
fn clean_required_name<'a>(field: &str, value: &'a str) -> Result<&'a str, ClientWorkerError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ClientWorkerError::InvalidRequest(format!(
            "{field} cannot be empty"
        )));
    }
    Ok(value)
}
