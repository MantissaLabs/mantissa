use crate::stream::{
    task_exec::{
        TASK_INTERACTIVE_EVENT_BUFFER, TaskInteractiveEvent, TaskInteractiveInput,
        TaskInteractiveSession,
    },
    task_logs::{TASK_LOG_EVENT_BUFFER, TaskLogEvent, TaskLogHttpStream},
};
use crate::types::{
    agents::{
        AgentInputRequest, AgentInputResponse, AgentRunSummary, AgentSession, AgentSessionDetail,
        AgentSessionSummary, AgentSubmitRequest, AgentSubmitResponse,
    },
    clusters::{
        ClusterOperation, ClusterSummary, ClusterView, ClusterViewSummary, SplitCandidateList,
    },
    jobs::{JobDetail, JobSubmitRequest, JobSubmitResponse, JobSummary},
    networks::{
        NetworkAttachment, NetworkCreateRequest, NetworkCreateResponse, NetworkDeleteResponse,
        NetworkInspect, NetworkPeerStatus, NetworkSummary,
    },
    nodes::{
        NodeActionResponse, NodeDrainRequest, NodeDrainStatus, NodeLabelsRequest,
        NodeLabelsResponse, NodeSummary,
    },
    scheduler::SchedulerSummary,
    secrets::{SecretDeleteResponse, SecretDetail, SecretSummary, SecretUpsertRequest},
    services::{ServiceDeployRequest, ServiceDeployResponse, ServiceSummary},
    tasks::{TaskAttachQuery, TaskExecQuery, TaskLogsQuery, TaskStartRequest, TaskSummary},
    volumes::{
        VolumeCreateRequest, VolumeDeleteResponse, VolumeImportRequest, VolumeInspect, VolumeSpec,
        VolumeSummary,
    },
};
use mantissa_client::{
    agents, clusters,
    config::ClientConfig,
    connection,
    error::{ClientError, ClientErrorKind},
    jobs, networks, nodes, scheduler, secrets, services, tasks, volumes,
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

    /// Validates one REST bearer token through the daemon-owned token store.
    pub async fn validate_rest_token(&self, token: String) -> Result<bool, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::ValidateRestToken { token, respond_to })
            .await
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

    /// Lists durable agent sessions visible through the agents capability.
    pub async fn list_agent_sessions(&self) -> Result<Vec<AgentSessionSummary>, ClientWorkerError> {
        self.send(ClientCommand::ListAgentSessions).await
    }

    /// Submits one durable agent session from a REST manifest request.
    pub async fn submit_agent_session(
        &self,
        request: AgentSubmitRequest,
    ) -> Result<AgentSubmitResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::SubmitAgentSession {
            request: Box::new(request),
            respond_to,
        })
        .await
    }

    /// Fetches one durable agent session and its run history by UUID string.
    pub async fn get_agent_session(
        &self,
        session_id: String,
    ) -> Result<AgentSessionDetail, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::GetAgentSession {
            session_id,
            respond_to,
        })
        .await
    }

    /// Lists durable runs for one agent session by UUID string.
    pub async fn list_agent_runs(
        &self,
        session_id: String,
    ) -> Result<Vec<AgentRunSummary>, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::ListAgentRuns {
            session_id,
            respond_to,
        })
        .await
    }

    /// Queues structured input for one idle agent session.
    pub async fn submit_agent_input(
        &self,
        session_id: String,
        request: AgentInputRequest,
    ) -> Result<AgentInputResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::SubmitAgentInput {
            session_id,
            request: Box::new(request),
            respond_to,
        })
        .await
    }

    /// Requests cancellation for one active or queued agent session run.
    pub async fn cancel_agent_session(
        &self,
        session_id: String,
    ) -> Result<AgentSession, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::CancelAgentSession {
            session_id,
            respond_to,
        })
        .await
    }

    /// Closes one durable agent session and cancels any active run.
    pub async fn close_agent_session(
        &self,
        session_id: String,
    ) -> Result<AgentSession, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::CloseAgentSession {
            session_id,
            respond_to,
        })
        .await
    }

    /// Deletes one closed agent session and its retained run history.
    pub async fn delete_agent_session(
        &self,
        session_id: String,
    ) -> Result<AgentSession, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::DeleteAgentSession {
            session_id,
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

    /// Lists peer reconciliation status rows for one overlay network.
    pub async fn list_network_peers(
        &self,
        network_id: String,
    ) -> Result<Vec<NetworkPeerStatus>, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::ListNetworkPeers {
            network_id,
            respond_to,
        })
        .await
    }

    /// Lists workload attachment rows for one overlay network.
    pub async fn list_network_attachments(
        &self,
        network_id: String,
    ) -> Result<Vec<NetworkAttachment>, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::ListNetworkAttachments {
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

    /// Streams standalone task logs as newline-delimited JSON events.
    pub async fn task_logs(
        &self,
        selector: String,
        request: TaskLogsQuery,
    ) -> Result<TaskLogHttpStream, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::TaskLogs {
            selector,
            request: Box::new(request),
            respond_to,
        })
        .await
    }

    /// Opens one bidirectional task attach WebSocket bridge.
    pub async fn task_attach(
        &self,
        selector: String,
        request: TaskAttachQuery,
    ) -> Result<TaskInteractiveSession, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::TaskAttach {
            selector,
            request: Box::new(request),
            respond_to,
        })
        .await
    }

    /// Opens one bidirectional task exec WebSocket bridge.
    pub async fn task_exec(
        &self,
        selector: String,
        request: TaskExecQuery,
    ) -> Result<TaskInteractiveSession, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::TaskExec {
            selector,
            request: Box::new(request),
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

    /// Fetches one node drain-status snapshot by UUID string.
    pub async fn node_drain_status(
        &self,
        node_id: String,
    ) -> Result<NodeDrainStatus, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::NodeDrainStatus {
            node_id,
            respond_to,
        })
        .await
    }

    /// Applies one node label mutation by UUID string.
    pub async fn update_node_labels(
        &self,
        node_id: String,
        request: NodeLabelsRequest,
    ) -> Result<NodeLabelsResponse, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::UpdateNodeLabels {
            node_id,
            request: Box::new(request),
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

    /// Fetches the latest locally known state for one cluster operation.
    pub async fn cluster_operation(
        &self,
        operation_id: String,
    ) -> Result<ClusterOperation, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::ClusterOperation {
            operation_id,
            respond_to,
        })
        .await
    }

    /// Lists split candidates for the active or selected cluster lineage.
    pub async fn list_split_candidates(
        &self,
        cluster_id: Option<String>,
    ) -> Result<SplitCandidateList, ClientWorkerError> {
        self.send(|respond_to| ClientCommand::ListSplitCandidates {
            cluster_id,
            respond_to,
        })
        .await
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
                match command {
                    ClientCommand::Health(respond_to) => {
                        let _ignored = respond_to.send(result.clone());
                    }
                    ClientCommand::ValidateRestToken { token, respond_to } => {
                        let _ignored = respond_to.send(Ok(token == "secret"));
                    }
                    _ => {}
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
                match command {
                    ClientCommand::ListNodes(respond_to) => {
                        let _ignored = respond_to.send(result.clone());
                    }
                    ClientCommand::ValidateRestToken { token, respond_to } => {
                        let _ignored = respond_to.send(Ok(token == "secret"));
                    }
                    _ => {}
                }
            }
        });
        Self { sender }
    }

    /// Builds a deterministic agent-session-list worker handle for route tests.
    #[cfg(test)]
    pub fn fixed_agent_sessions_for_tests(
        result: Result<Vec<AgentSessionSummary>, ClientWorkerError>,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel(CLIENT_COMMAND_BUFFER);
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    ClientCommand::ListAgentSessions(respond_to) => {
                        let _ignored = respond_to.send(result.clone());
                    }
                    ClientCommand::ValidateRestToken { token, respond_to } => {
                        let _ignored = respond_to.send(Ok(token == "secret"));
                    }
                    _ => {}
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
    Conflict(String),
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
            Self::Conflict(message) => write!(formatter, "{message}"),
            Self::OperationFailed(message) => write!(formatter, "{message}"),
            Self::RequestChannelClosed => write!(formatter, "REST client worker is closed"),
            Self::ResponseChannelClosed => write!(formatter, "REST client worker stopped"),
            Self::Runtime(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for ClientWorkerError {}

impl From<ClientError> for ClientWorkerError {
    /// Maps reusable client error classes into REST worker failures.
    fn from(error: ClientError) -> Self {
        let message = error.to_string();
        match error.kind() {
            ClientErrorKind::InvalidRequest => Self::InvalidRequest(message),
            ClientErrorKind::NotFound => Self::NotFound(message),
            ClientErrorKind::Conflict => Self::Conflict(message),
            ClientErrorKind::OperationFailed => Self::OperationFailed(message),
        }
    }
}

/// Commands accepted by the local Cap'n Proto client worker.
enum ClientCommand {
    Health(oneshot::Sender<Result<ClientHealth, ClientWorkerError>>),
    ValidateRestToken {
        token: String,
        respond_to: oneshot::Sender<Result<bool, ClientWorkerError>>,
    },
    ListNodes(oneshot::Sender<Result<Vec<NodeSummary>, ClientWorkerError>>),
    GetNode {
        node_id: String,
        respond_to: oneshot::Sender<Result<NodeSummary, ClientWorkerError>>,
    },
    ListAgentSessions(oneshot::Sender<Result<Vec<AgentSessionSummary>, ClientWorkerError>>),
    SubmitAgentSession {
        request: Box<AgentSubmitRequest>,
        respond_to: oneshot::Sender<Result<AgentSubmitResponse, ClientWorkerError>>,
    },
    GetAgentSession {
        session_id: String,
        respond_to: oneshot::Sender<Result<AgentSessionDetail, ClientWorkerError>>,
    },
    ListAgentRuns {
        session_id: String,
        respond_to: oneshot::Sender<Result<Vec<AgentRunSummary>, ClientWorkerError>>,
    },
    SubmitAgentInput {
        session_id: String,
        request: Box<AgentInputRequest>,
        respond_to: oneshot::Sender<Result<AgentInputResponse, ClientWorkerError>>,
    },
    CancelAgentSession {
        session_id: String,
        respond_to: oneshot::Sender<Result<AgentSession, ClientWorkerError>>,
    },
    CloseAgentSession {
        session_id: String,
        respond_to: oneshot::Sender<Result<AgentSession, ClientWorkerError>>,
    },
    DeleteAgentSession {
        session_id: String,
        respond_to: oneshot::Sender<Result<AgentSession, ClientWorkerError>>,
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
    ListNetworkPeers {
        network_id: String,
        respond_to: oneshot::Sender<Result<Vec<NetworkPeerStatus>, ClientWorkerError>>,
    },
    ListNetworkAttachments {
        network_id: String,
        respond_to: oneshot::Sender<Result<Vec<NetworkAttachment>, ClientWorkerError>>,
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
    TaskLogs {
        selector: String,
        request: Box<TaskLogsQuery>,
        respond_to: oneshot::Sender<Result<TaskLogHttpStream, ClientWorkerError>>,
    },
    TaskAttach {
        selector: String,
        request: Box<TaskAttachQuery>,
        respond_to: oneshot::Sender<Result<TaskInteractiveSession, ClientWorkerError>>,
    },
    TaskExec {
        selector: String,
        request: Box<TaskExecQuery>,
        respond_to: oneshot::Sender<Result<TaskInteractiveSession, ClientWorkerError>>,
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
    NodeDrainStatus {
        node_id: String,
        respond_to: oneshot::Sender<Result<NodeDrainStatus, ClientWorkerError>>,
    },
    UpdateNodeLabels {
        node_id: String,
        request: Box<NodeLabelsRequest>,
        respond_to: oneshot::Sender<Result<NodeLabelsResponse, ClientWorkerError>>,
    },
    SchedulerSummary {
        peer_id: Option<String>,
        details: bool,
        respond_to: oneshot::Sender<Result<SchedulerSummary, ClientWorkerError>>,
    },
    ListClusters(oneshot::Sender<Result<Vec<ClusterSummary>, ClientWorkerError>>),
    ListClusterViews(oneshot::Sender<Result<Vec<ClusterViewSummary>, ClientWorkerError>>),
    ActiveClusterView(oneshot::Sender<Result<ClusterView, ClientWorkerError>>),
    ClusterOperation {
        operation_id: String,
        respond_to: oneshot::Sender<Result<ClusterOperation, ClientWorkerError>>,
    },
    ListSplitCandidates {
        cluster_id: Option<String>,
        respond_to: oneshot::Sender<Result<SplitCandidateList, ClientWorkerError>>,
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
            ClientCommand::Health(respond_to) => {
                let result = check_daemon_health(&config).await;
                let _ignored = respond_to.send(result);
            }
            ClientCommand::ValidateRestToken { token, respond_to } => {
                let _ignored = respond_to.send(validate_rest_token(&config, &token).await);
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
            ClientCommand::ListAgentSessions(respond_to) => {
                let _ignored = respond_to.send(list_agent_sessions(&config).await);
            }
            ClientCommand::SubmitAgentSession {
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(submit_agent_session(&config, *request).await);
            }
            ClientCommand::GetAgentSession {
                session_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(get_agent_session(&config, &session_id).await);
            }
            ClientCommand::ListAgentRuns {
                session_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(list_agent_runs(&config, &session_id).await);
            }
            ClientCommand::SubmitAgentInput {
                session_id,
                request,
                respond_to,
            } => {
                let _ignored =
                    respond_to.send(submit_agent_input(&config, &session_id, *request).await);
            }
            ClientCommand::CancelAgentSession {
                session_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(cancel_agent_session(&config, &session_id).await);
            }
            ClientCommand::CloseAgentSession {
                session_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(close_agent_session(&config, &session_id).await);
            }
            ClientCommand::DeleteAgentSession {
                session_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(delete_agent_session(&config, &session_id).await);
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
            ClientCommand::ListNetworkPeers {
                network_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(list_network_peers(&config, &network_id).await);
            }
            ClientCommand::ListNetworkAttachments {
                network_id,
                respond_to,
            } => {
                let _ignored =
                    respond_to.send(list_network_attachments(&config, &network_id).await);
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
            ClientCommand::TaskLogs {
                selector,
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(start_task_logs(&config, selector, *request));
            }
            ClientCommand::TaskAttach {
                selector,
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(start_task_attach(&config, selector, *request));
            }
            ClientCommand::TaskExec {
                selector,
                request,
                respond_to,
            } => {
                let _ignored = respond_to.send(start_task_exec(&config, selector, *request));
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
            ClientCommand::NodeDrainStatus {
                node_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(node_drain_status(&config, &node_id).await);
            }
            ClientCommand::UpdateNodeLabels {
                node_id,
                request,
                respond_to,
            } => {
                let _ignored =
                    respond_to.send(update_node_labels(&config, &node_id, *request).await);
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
            ClientCommand::ClusterOperation {
                operation_id,
                respond_to,
            } => {
                let _ignored = respond_to.send(cluster_operation(&config, &operation_id).await);
            }
            ClientCommand::ListSplitCandidates {
                cluster_id,
                respond_to,
            } => {
                let _ignored =
                    respond_to.send(list_split_candidates(&config, cluster_id.as_deref()).await);
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

/// Validates one REST bearer token through the local daemon session.
async fn validate_rest_token(
    config: &ClientConfig,
    token: &str,
) -> Result<bool, ClientWorkerError> {
    mantissa_client::rest::validate_token(config, token)
        .await
        .map_err(|error| ClientWorkerError::DaemonUnavailable(error.to_string()))
}

/// Lists nodes through the reusable Mantissa client API.
async fn list_nodes(config: &ClientConfig) -> Result<Vec<NodeSummary>, ClientWorkerError> {
    nodes::list(config)
        .await
        .map(|nodes| nodes.into_iter().map(NodeSummary::from).collect())
        .map_err(operation_failed_error)
}

/// Fetches one node summary from the topology list response.
async fn get_node(config: &ClientConfig, node_id: &str) -> Result<NodeSummary, ClientWorkerError> {
    let nodes = list_nodes(config).await?;
    nodes
        .into_iter()
        .find(|node| node.id == node_id)
        .ok_or_else(|| ClientWorkerError::NotFound(format!("node '{node_id}' not found")))
}

/// Lists agent sessions through the reusable Mantissa client API.
async fn list_agent_sessions(
    config: &ClientConfig,
) -> Result<Vec<AgentSessionSummary>, ClientWorkerError> {
    agents::list_sessions(config)
        .await
        .map(|sessions| {
            sessions
                .into_iter()
                .map(AgentSessionSummary::from)
                .collect()
        })
        .map_err(operation_failed_error)
}

/// Submits one agent manifest through the reusable Mantissa client API.
async fn submit_agent_session(
    config: &ClientConfig,
    request: AgentSubmitRequest,
) -> Result<AgentSubmitResponse, ClientWorkerError> {
    agents::run_manifest(config, &request.manifest)
        .await
        .map(AgentSubmitResponse::from)
        .map_err(invalid_request_error)
}

/// Fetches one agent session detail through the reusable Mantissa client API.
async fn get_agent_session(
    config: &ClientConfig,
    session_id: &str,
) -> Result<AgentSessionDetail, ClientWorkerError> {
    parse_uuid("agent session id", session_id)?;
    agents::inspect(config, session_id)
        .await
        .map(AgentSessionDetail::from)
        .map_err(not_found_error)
}

/// Lists one agent session's durable runs through the reusable client API.
async fn list_agent_runs(
    config: &ClientConfig,
    session_id: &str,
) -> Result<Vec<AgentRunSummary>, ClientWorkerError> {
    let session_id = parse_uuid("agent session id", session_id)?;
    get_agent_session(config, &session_id.to_string()).await?;
    agents::list_runs(config, Some(session_id))
        .await
        .map(|runs| runs.into_iter().map(AgentRunSummary::from).collect())
        .map_err(operation_failed_error)
}

/// Queues one structured input message through the reusable client API.
async fn submit_agent_input(
    config: &ClientConfig,
    session_id: &str,
    request: AgentInputRequest,
) -> Result<AgentInputResponse, ClientWorkerError> {
    let parsed_session_id = parse_uuid("agent session id", session_id)?;
    let input = clean_required_name("agent input", &request.input)?;
    agents::inspect(config, session_id)
        .await
        .map_err(not_found_error)?;
    agents::submit_input(config, parsed_session_id, input)
        .await
        .map(|()| AgentInputResponse::accepted())
        .map_err(conflict_error)
}

/// Cancels one agent session through the reusable client API.
async fn cancel_agent_session(
    config: &ClientConfig,
    session_id: &str,
) -> Result<AgentSession, ClientWorkerError> {
    parse_uuid("agent session id", session_id)?;
    agents::inspect(config, session_id)
        .await
        .map_err(not_found_error)?;
    agents::cancel(config, session_id)
        .await
        .map(AgentSession::from)
        .map_err(conflict_error)
}

/// Closes one agent session through the reusable client API.
async fn close_agent_session(
    config: &ClientConfig,
    session_id: &str,
) -> Result<AgentSession, ClientWorkerError> {
    parse_uuid("agent session id", session_id)?;
    agents::inspect(config, session_id)
        .await
        .map_err(not_found_error)?;
    agents::close(config, session_id)
        .await
        .map(AgentSession::from)
        .map_err(conflict_error)
}

/// Deletes one closed agent session through the reusable client API.
async fn delete_agent_session(
    config: &ClientConfig,
    session_id: &str,
) -> Result<AgentSession, ClientWorkerError> {
    parse_uuid("agent session id", session_id)?;
    agents::inspect(config, session_id)
        .await
        .map_err(not_found_error)?;
    agents::delete(config, session_id)
        .await
        .map(AgentSession::from)
        .map_err(conflict_error)
}

/// Lists jobs through the reusable Mantissa client API.
async fn list_jobs(config: &ClientConfig) -> Result<Vec<JobSummary>, ClientWorkerError> {
    jobs::list(config)
        .await
        .map(|jobs| jobs.into_iter().map(JobSummary::from).collect())
        .map_err(operation_failed_error)
}

/// Fetches one job detail through the reusable Mantissa client API.
async fn get_job(config: &ClientConfig, job_id: &str) -> Result<JobDetail, ClientWorkerError> {
    parse_uuid("job id", job_id)?;
    jobs::inspect(config, job_id)
        .await
        .map(JobDetail::from)
        .map_err(not_found_error)
}

/// Submits one job manifest through the reusable Mantissa client API.
async fn submit_job(
    config: &ClientConfig,
    request: JobSubmitRequest,
) -> Result<JobSubmitResponse, ClientWorkerError> {
    jobs::run_manifest(config, &request.manifest)
        .await
        .map(JobSubmitResponse::from)
        .map_err(invalid_request_error)
}

/// Cancels one job through the reusable Mantissa client API.
async fn cancel_job(config: &ClientConfig, job_id: &str) -> Result<JobSummary, ClientWorkerError> {
    parse_uuid("job id", job_id)?;
    jobs::inspect(config, job_id)
        .await
        .map_err(not_found_error)?;
    jobs::cancel(config, job_id)
        .await
        .map(JobSummary::from)
        .map_err(conflict_error)
}

/// Deletes one terminal job through the reusable Mantissa client API.
async fn delete_job(config: &ClientConfig, job_id: &str) -> Result<JobSummary, ClientWorkerError> {
    parse_uuid("job id", job_id)?;
    jobs::inspect(config, job_id)
        .await
        .map_err(not_found_error)?;
    jobs::delete(config, job_id)
        .await
        .map(JobSummary::from)
        .map_err(conflict_error)
}

/// Lists services through the reusable Mantissa client API.
async fn list_services(config: &ClientConfig) -> Result<Vec<ServiceSummary>, ClientWorkerError> {
    services::list(config)
        .await
        .map(|services| services.into_iter().map(ServiceSummary::from).collect())
        .map_err(operation_failed_error)
}

/// Deploys one service manifest through the reusable Mantissa client API.
async fn deploy_service(
    config: &ClientConfig,
    request: ServiceDeployRequest,
) -> Result<ServiceDeployResponse, ClientWorkerError> {
    services::deploy_manifest(config, &request.manifest)
        .await
        .map(ServiceDeployResponse::from)
        .map_err(invalid_request_error)
}

/// Fetches one service through the reusable Mantissa client API.
async fn get_service(
    config: &ClientConfig,
    selector: &str,
) -> Result<ServiceSummary, ClientWorkerError> {
    services::list::inspect_service_row(config, selector)
        .await
        .map(ServiceSummary::from)
        .map_err(not_found_error)
}

/// Fetches one service status through the reusable Mantissa client API.
async fn get_service_status(
    config: &ClientConfig,
    selector: &str,
) -> Result<ServiceSummary, ClientWorkerError> {
    get_service(config, selector).await?;
    services::rollout_status(config, selector)
        .await
        .map(ServiceSummary::from)
        .map_err(operation_failed_error)
}

/// Deletes one service through the reusable Mantissa client API.
async fn delete_service(
    config: &ClientConfig,
    selector: &str,
) -> Result<ServiceSummary, ClientWorkerError> {
    let service = services::list::inspect_service_row(config, selector)
        .await
        .map_err(not_found_error)?;
    services::stop(config, &service.service_id.to_string())
        .await
        .map(ServiceSummary::from)
        .map_err(operation_failed_error)
}

/// Lists networks through the reusable Mantissa client API.
async fn list_networks(config: &ClientConfig) -> Result<Vec<NetworkSummary>, ClientWorkerError> {
    networks::list(config)
        .await
        .map(|networks| networks.into_iter().map(NetworkSummary::from).collect())
        .map_err(operation_failed_error)
}

/// Creates one network through the reusable Mantissa client API.
async fn create_network(
    config: &ClientConfig,
    mut request: NetworkCreateRequest,
) -> Result<NetworkCreateResponse, ClientWorkerError> {
    let name = clean_required_name("network name", &request.name)?.to_string();
    ensure_unique_network_name(config, &name).await?;
    request.name = name;
    let request = request.into();
    networks::create(config, &request)
        .await
        .map(|network_id| NetworkCreateResponse {
            network_id: network_id.to_string(),
        })
        .map_err(invalid_request_error)
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
        .map_err(not_found_error)
}

/// Lists network peer status rows through the reusable Mantissa client API.
async fn list_network_peers(
    config: &ClientConfig,
    network_id: &str,
) -> Result<Vec<NetworkPeerStatus>, ClientWorkerError> {
    parse_uuid("network id", network_id)?;
    ensure_network_exists(config, network_id).await?;
    networks::peer_status(config, network_id)
        .await
        .map(|peers| peers.into_iter().map(NetworkPeerStatus::from).collect())
        .map_err(operation_failed_error)
}

/// Lists network attachment rows through the reusable Mantissa client API.
async fn list_network_attachments(
    config: &ClientConfig,
    network_id: &str,
) -> Result<Vec<NetworkAttachment>, ClientWorkerError> {
    parse_uuid("network id", network_id)?;
    ensure_network_exists(config, network_id).await?;
    networks::attachments(config, network_id)
        .await
        .map(|attachments| {
            attachments
                .into_iter()
                .map(NetworkAttachment::from)
                .collect()
        })
        .map_err(operation_failed_error)
}

/// Deletes one network through the reusable Mantissa client API.
async fn delete_network(
    config: &ClientConfig,
    network_id: String,
) -> Result<NetworkDeleteResponse, ClientWorkerError> {
    parse_uuid("network id", &network_id)?;
    ensure_network_exists(config, &network_id).await?;
    networks::delete_typed(config, &[network_id])
        .await
        .map(|deleted| NetworkDeleteResponse { deleted })
        .map_err(ClientWorkerError::from)
}

/// Lists volumes through the reusable Mantissa client API.
async fn list_volumes(config: &ClientConfig) -> Result<Vec<VolumeSummary>, ClientWorkerError> {
    volumes::list(config)
        .await
        .map(|volumes| volumes.into_iter().map(VolumeSummary::from).collect())
        .map_err(operation_failed_error)
}

/// Creates one volume through the reusable Mantissa client API.
async fn create_volume(
    config: &ClientConfig,
    request: VolumeCreateRequest,
) -> Result<VolumeSpec, ClientWorkerError> {
    let mut request = request
        .into_client()
        .map_err(ClientWorkerError::InvalidRequest)?;
    let name = clean_required_name("volume name", &request.name)?.to_string();
    ensure_unique_volume_name(config, &name).await?;
    request.name = name;
    volumes::create_with_request(config, &request)
        .await
        .map(VolumeSpec::from)
        .map_err(invalid_request_error)
}

/// Imports one volume through the reusable Mantissa client API.
async fn import_volume(
    config: &ClientConfig,
    request: VolumeImportRequest,
) -> Result<VolumeSpec, ClientWorkerError> {
    let mut request: mantissa_client::volumes::VolumeImportRequest = request.into();
    let name = clean_required_name("volume name", &request.name)?.to_string();
    ensure_unique_volume_name(config, &name).await?;
    request.name = name;
    volumes::import_with_request(config, &request)
        .await
        .map(VolumeSpec::from)
        .map_err(invalid_request_error)
}

/// Fetches one volume inspection through the reusable Mantissa client API.
async fn get_volume(
    config: &ClientConfig,
    selector: &str,
) -> Result<VolumeInspect, ClientWorkerError> {
    volumes::inspect(config, selector)
        .await
        .map(VolumeInspect::from)
        .map_err(not_found_error)
}

/// Fetches one volume status through the reusable Mantissa client API.
async fn get_volume_status(
    config: &ClientConfig,
    selector: &str,
) -> Result<VolumeInspect, ClientWorkerError> {
    get_volume(config, selector).await?;
    volumes::status(config, selector)
        .await
        .map(VolumeInspect::from)
        .map_err(operation_failed_error)
}

/// Deletes one volume through the reusable Mantissa client API.
async fn delete_volume(
    config: &ClientConfig,
    selector: &str,
) -> Result<VolumeDeleteResponse, ClientWorkerError> {
    volumes::inspect(config, selector)
        .await
        .map_err(not_found_error)?;
    volumes::delete(config, selector)
        .await
        .map(VolumeDeleteResponse::from)
        .map_err(conflict_error)
}

/// Lists standalone tasks through the reusable Mantissa client API.
async fn list_tasks(config: &ClientConfig) -> Result<Vec<TaskSummary>, ClientWorkerError> {
    tasks::list(config, &[])
        .await
        .map(|tasks| tasks.into_iter().map(TaskSummary::from).collect())
        .map_err(operation_failed_error)
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

/// Starts one worker-local task log stream and returns its HTTP receiver.
fn start_task_logs(
    config: &ClientConfig,
    selector: String,
    request: TaskLogsQuery,
) -> Result<TaskLogHttpStream, ClientWorkerError> {
    request
        .validate()
        .map_err(ClientWorkerError::InvalidRequest)?;
    let selector = clean_required_name("task selector", &selector)?.to_string();
    let config = config.clone();
    let (events_tx, events_rx) = mpsc::channel(TASK_LOG_EVENT_BUFFER);
    let (cancel_tx, cancel_rx) = oneshot::channel();

    tokio::task::spawn_local(async move {
        let sink = crate::stream::task_logs::new_task_log_sink(events_tx.clone());
        let options = tasks::TaskLogsOptions {
            follow: request.follow,
            tail: &request.tail,
            stdout: request.stdout,
            stderr: request.stderr,
            timestamps: request.timestamps,
        };

        let result = tokio::select! {
            result = tasks::logs_with_sink(&config, &selector, &options, sink) => Some(result),
            _ = cancel_rx => None,
        };

        if let Some(Err(error)) = result {
            let _ignored = events_tx.send(TaskLogEvent::error(error.to_string())).await;
        }
    });

    Ok(TaskLogHttpStream::new(events_rx, cancel_tx))
}

/// Starts one worker-local task attach bridge and returns its WebSocket channels.
fn start_task_attach(
    config: &ClientConfig,
    selector: String,
    request: TaskAttachQuery,
) -> Result<TaskInteractiveSession, ClientWorkerError> {
    let selector = clean_required_name("task selector", &selector)?.to_string();
    let config = config.clone();
    let (events_tx, events_rx) = mpsc::channel(TASK_INTERACTIVE_EVENT_BUFFER);
    let (input_tx, input_rx) = mpsc::channel(TASK_INTERACTIVE_EVENT_BUFFER);
    let (cancel_tx, cancel_rx) = oneshot::channel();

    tokio::task::spawn_local(async move {
        let sink = crate::stream::task_exec::new_task_interactive_sink(events_tx.clone());
        let options = tasks::TaskAttachOptions {
            logs: request.logs,
            stream: request.stream,
            stdin: request.stdin,
            stdout: request.stdout,
            stderr: request.stderr,
            detach_keys: clean_optional_text(request.detach_keys),
            tty_width: request.tty_width,
            tty_height: request.tty_height,
        };

        match tasks::attach_with_sink(&config, &selector, &options, sink).await {
            Ok(session) => drive_attach_session(session, input_rx, cancel_rx, events_tx).await,
            Err(error) => {
                let _ignored = events_tx
                    .send(TaskInteractiveEvent::error(error.to_string()))
                    .await;
            }
        }
    });

    Ok(TaskInteractiveSession::new(
        input_tx, events_rx, cancel_tx, false,
    ))
}

/// Starts one worker-local task exec bridge and returns its WebSocket channels.
fn start_task_exec(
    config: &ClientConfig,
    selector: String,
    request: TaskExecQuery,
) -> Result<TaskInteractiveSession, ClientWorkerError> {
    request
        .validate()
        .map_err(ClientWorkerError::InvalidRequest)?;
    let selector = clean_required_name("task selector", &selector)?.to_string();
    let config = config.clone();
    let (events_tx, events_rx) = mpsc::channel(TASK_INTERACTIVE_EVENT_BUFFER);
    let (input_tx, input_rx) = mpsc::channel(TASK_INTERACTIVE_EVENT_BUFFER);
    let (cancel_tx, cancel_rx) = oneshot::channel();

    tokio::task::spawn_local(async move {
        let sink = crate::stream::task_exec::new_task_interactive_sink(events_tx.clone());
        let options = tasks::TaskExecOptions {
            command: request.command,
            stdin: request.stdin,
            stdout: request.stdout,
            stderr: request.stderr,
            tty: request.tty,
            detach_keys: clean_optional_text(request.detach_keys),
            tty_width: request.tty_width,
            tty_height: request.tty_height,
        };

        match tasks::exec_with_sink(&config, &selector, &options, sink).await {
            Ok(session) => {
                let wait_session = session.clone();
                let wait_events = events_tx.clone();
                tokio::task::spawn_local(async move {
                    match tasks::wait_exec_result(&wait_session).await {
                        Ok(result) => {
                            let _ignored = wait_events
                                .send(TaskInteractiveEvent::result(result.exit_code))
                                .await;
                        }
                        Err(error) => {
                            let _ignored = wait_events
                                .send(TaskInteractiveEvent::error(error.to_string()))
                                .await;
                        }
                    }
                });
                drive_exec_session(session, input_rx, cancel_rx, events_tx).await;
            }
            Err(error) => {
                let _ignored = events_tx
                    .send(TaskInteractiveEvent::error(error.to_string()))
                    .await;
            }
        }
    });

    Ok(TaskInteractiveSession::new(
        input_tx, events_rx, cancel_tx, true,
    ))
}

/// Forwards WebSocket input events into one Cap'n Proto attach session.
async fn drive_attach_session(
    session: mantissa_protocol::task::task_attach_session::Client,
    mut input_rx: mpsc::Receiver<TaskInteractiveInput>,
    mut cancel_rx: oneshot::Receiver<()>,
    events_tx: mpsc::Sender<TaskInteractiveEvent>,
) {
    loop {
        tokio::select! {
            _ = &mut cancel_rx => {
                close_attach_input(&session, &events_tx).await;
                return;
            }
            input = input_rx.recv() => {
                match input {
                    Some(TaskInteractiveInput::Data(bytes)) => {
                        if !push_attach_input(&session, bytes, &events_tx).await {
                            return;
                        }
                    }
                    Some(TaskInteractiveInput::CloseInput) | None => {
                        close_attach_input(&session, &events_tx).await;
                        return;
                    }
                }
            }
        }
    }
}

/// Forwards WebSocket input events into one Cap'n Proto exec session.
async fn drive_exec_session(
    session: mantissa_protocol::task::task_exec_session::Client,
    mut input_rx: mpsc::Receiver<TaskInteractiveInput>,
    mut cancel_rx: oneshot::Receiver<()>,
    events_tx: mpsc::Sender<TaskInteractiveEvent>,
) {
    loop {
        tokio::select! {
            _ = &mut cancel_rx => {
                close_exec_input(&session, &events_tx).await;
                return;
            }
            input = input_rx.recv() => {
                match input {
                    Some(TaskInteractiveInput::Data(bytes)) => {
                        if !push_exec_input(&session, bytes, &events_tx).await {
                            return;
                        }
                    }
                    Some(TaskInteractiveInput::CloseInput) | None => {
                        close_exec_input(&session, &events_tx).await;
                        return;
                    }
                }
            }
        }
    }
}

/// Pushes one stdin chunk into an attach session and reports failures in-band.
async fn push_attach_input(
    session: &mantissa_protocol::task::task_attach_session::Client,
    bytes: Vec<u8>,
    events_tx: &mpsc::Sender<TaskInteractiveEvent>,
) -> bool {
    let mut request = session.push_input_request();
    request.get().set_data(&bytes);
    match request.send().await {
        Ok(()) => true,
        Err(error) => {
            let _ignored = events_tx
                .send(TaskInteractiveEvent::error(error.to_string()))
                .await;
            false
        }
    }
}

/// Pushes one stdin chunk into an exec session and reports failures in-band.
async fn push_exec_input(
    session: &mantissa_protocol::task::task_exec_session::Client,
    bytes: Vec<u8>,
    events_tx: &mpsc::Sender<TaskInteractiveEvent>,
) -> bool {
    let mut request = session.push_input_request();
    request.get().set_data(&bytes);
    match request.send().await {
        Ok(()) => true,
        Err(error) => {
            let _ignored = events_tx
                .send(TaskInteractiveEvent::error(error.to_string()))
                .await;
            false
        }
    }
}

/// Closes stdin on an attach session and reports failures in-band.
async fn close_attach_input(
    session: &mantissa_protocol::task::task_attach_session::Client,
    events_tx: &mpsc::Sender<TaskInteractiveEvent>,
) {
    if let Err(error) = session.close_input_request().send().promise.await {
        let _ignored = events_tx
            .send(TaskInteractiveEvent::error(error.to_string()))
            .await;
    }
}

/// Closes stdin on an exec session and reports failures in-band.
async fn close_exec_input(
    session: &mantissa_protocol::task::task_exec_session::Client,
    events_tx: &mpsc::Sender<TaskInteractiveEvent>,
) {
    if let Err(error) = session.close_input_request().send().promise.await {
        let _ignored = events_tx
            .send(TaskInteractiveEvent::error(error.to_string()))
            .await;
    }
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
        .map_err(invalid_request_error)
}

/// Stops one standalone task through the reusable Mantissa client API.
async fn stop_task(
    config: &ClientConfig,
    selector: &str,
) -> Result<TaskSummary, ClientWorkerError> {
    get_task(config, selector).await?;
    tasks::stop(config, selector)
        .await
        .map(TaskSummary::from)
        .map_err(conflict_error)
}

/// Lists secrets through the reusable Mantissa client API.
async fn list_secrets(config: &ClientConfig) -> Result<Vec<SecretSummary>, ClientWorkerError> {
    secrets::list(config)
        .await
        .map(|secrets| secrets.into_iter().map(SecretSummary::from).collect())
        .map_err(operation_failed_error)
}

/// Creates one secret through the reusable Mantissa client API.
async fn create_secret(
    config: &ClientConfig,
    name: &str,
    request: SecretUpsertRequest,
) -> Result<SecretSummary, ClientWorkerError> {
    let name = clean_required_name("secret name", name)?;
    ensure_unique_secret_name(config, name).await?;
    let plaintext = request
        .plaintext()
        .map_err(ClientWorkerError::InvalidRequest)?;
    secrets::create(
        config,
        name,
        &plaintext,
        request.description.as_deref(),
        &request.labels(),
    )
    .await
    .map(SecretSummary::from)
    .map_err(invalid_request_error)
}

/// Updates one secret through the reusable Mantissa client API.
async fn update_secret(
    config: &ClientConfig,
    name: &str,
    request: SecretUpsertRequest,
) -> Result<SecretSummary, ClientWorkerError> {
    let name = clean_required_name("secret name", name)?;
    ensure_secret_exists(config, name).await?;
    let plaintext = request
        .plaintext()
        .map_err(ClientWorkerError::InvalidRequest)?;
    secrets::update(
        config,
        name,
        &plaintext,
        request.description.as_deref(),
        &request.labels(),
    )
    .await
    .map(SecretSummary::from)
    .map_err(operation_failed_error)
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
    .map_err(not_found_error)
}

/// Deletes one secret through the reusable Mantissa client API.
async fn delete_secret(
    config: &ClientConfig,
    name: &str,
) -> Result<SecretDeleteResponse, ClientWorkerError> {
    let name = clean_required_name("secret name", name)?.to_string();
    ensure_secret_exists(config, &name).await?;
    secrets::delete(config, &[name])
        .await
        .map(|deleted| SecretDeleteResponse { deleted })
        .map_err(operation_failed_error)
}

/// Requests one node drain through the reusable Mantissa client API.
async fn drain_node(
    config: &ClientConfig,
    node_id: &str,
    request: NodeDrainRequest,
) -> Result<NodeActionResponse, ClientWorkerError> {
    let node_id = parse_uuid("node id", node_id)?;
    ensure_node_exists(config, node_id).await?;
    let timeout = drain_timeout(request.task_stop_timeout_secs)?;
    nodes::request_drain_typed(config, node_id, request.reason.as_deref(), timeout)
        .await
        .map(|operation| NodeActionResponse {
            node_id: operation.node_id.to_string(),
            accepted: true,
        })
        .map_err(ClientWorkerError::from)
}

/// Resumes one node through the reusable Mantissa client API.
async fn resume_node(
    config: &ClientConfig,
    node_id: &str,
) -> Result<NodeActionResponse, ClientWorkerError> {
    let node_id = parse_uuid("node id", node_id)?;
    ensure_node_exists(config, node_id).await?;
    nodes::resume(config, node_id)
        .await
        .map(|()| NodeActionResponse {
            node_id: node_id.to_string(),
            accepted: true,
        })
        .map_err(operation_failed_error)
}

/// Evicts one node through the reusable Mantissa client API.
async fn evict_node(
    config: &ClientConfig,
    node_id: &str,
) -> Result<NodeActionResponse, ClientWorkerError> {
    let node_id = parse_uuid("node id", node_id)?;
    ensure_node_exists(config, node_id).await?;
    nodes::evict(config, node_id)
        .await
        .map(|()| NodeActionResponse {
            node_id: node_id.to_string(),
            accepted: true,
        })
        .map_err(conflict_error)
}

/// Fetches one node drain-status snapshot through the reusable Mantissa client API.
async fn node_drain_status(
    config: &ClientConfig,
    node_id: &str,
) -> Result<NodeDrainStatus, ClientWorkerError> {
    let node_id = parse_uuid("node id", node_id)?;
    ensure_node_exists(config, node_id).await?;
    nodes::status(config, node_id)
        .await
        .map(NodeDrainStatus::from)
        .map_err(operation_failed_error)
}

/// Applies one node label mutation through the reusable Mantissa client API.
async fn update_node_labels(
    config: &ClientConfig,
    node_id: &str,
    request: NodeLabelsRequest,
) -> Result<NodeLabelsResponse, ClientWorkerError> {
    let node_id = parse_uuid("node id", node_id)?;
    ensure_node_exists(config, node_id).await?;
    if !request.replace && request.labels.is_empty() && request.remove.is_empty() {
        return Err(ClientWorkerError::InvalidRequest(
            "label update requires labels, remove, or replace".to_string(),
        ));
    }
    nodes::labels(
        config,
        node_id,
        &request.labels,
        &request.remove,
        request.replace,
    )
    .await
    .map(NodeLabelsResponse::from)
    .map_err(invalid_request_error)
}

/// Fetches scheduler summary through the reusable Mantissa client API.
async fn scheduler_summary(
    config: &ClientConfig,
    peer_id: Option<String>,
    details: bool,
) -> Result<SchedulerSummary, ClientWorkerError> {
    if let Some(peer_id) = peer_id.as_deref() {
        let peer_id = parse_uuid("peer id", peer_id)?;
        ensure_node_exists(config, peer_id).await?;
    }
    scheduler::slots(config, peer_id.as_deref(), details)
        .await
        .map(SchedulerSummary::from)
        .map_err(operation_failed_error)
}

/// Lists cluster lineage summaries through the reusable Mantissa client API.
async fn list_clusters(config: &ClientConfig) -> Result<Vec<ClusterSummary>, ClientWorkerError> {
    clusters::list_clusters(config)
        .await
        .map(|clusters| clusters.into_iter().map(ClusterSummary::from).collect())
        .map_err(operation_failed_error)
}

/// Lists cluster view summaries through the reusable Mantissa client API.
async fn list_cluster_views(
    config: &ClientConfig,
) -> Result<Vec<ClusterViewSummary>, ClientWorkerError> {
    clusters::list_cluster_views(config)
        .await
        .map(|views| views.into_iter().map(ClusterViewSummary::from).collect())
        .map_err(operation_failed_error)
}

/// Fetches the active cluster view through the reusable Mantissa client API.
async fn active_cluster_view(config: &ClientConfig) -> Result<ClusterView, ClientWorkerError> {
    clusters::active_cluster_view(config)
        .await
        .map(ClusterView::from)
        .map_err(operation_failed_error)
}

/// Fetches one cluster operation through the reusable Mantissa client API.
async fn cluster_operation(
    config: &ClientConfig,
    operation_id: &str,
) -> Result<ClusterOperation, ClientWorkerError> {
    parse_uuid("cluster operation id", operation_id)?;
    clusters::get_cluster_operation(config, operation_id)
        .await
        .map(ClusterOperation::from)
        .map_err(not_found_error)
}

/// Lists split candidates through the reusable Mantissa client API.
async fn list_split_candidates(
    config: &ClientConfig,
    cluster_id: Option<&str>,
) -> Result<SplitCandidateList, ClientWorkerError> {
    if let Some(cluster_id) = cluster_id {
        parse_uuid("cluster id", cluster_id)?;
    }
    clusters::list_split_candidates(config, cluster_id)
        .await
        .map(SplitCandidateList::from)
        .map_err(not_found_error)
}

/// Converts a known client error class into a worker error.
fn client_error(kind: ClientErrorKind, error: impl std::fmt::Display) -> ClientWorkerError {
    ClientError::from_display(kind, error).into()
}

/// Marks reusable client failures caused by invalid REST input.
fn invalid_request_error(error: impl std::fmt::Display) -> ClientWorkerError {
    client_error(ClientErrorKind::InvalidRequest, error)
}

/// Marks reusable client failures caused by a missing selected resource.
fn not_found_error(error: impl std::fmt::Display) -> ClientWorkerError {
    client_error(ClientErrorKind::NotFound, error)
}

/// Marks reusable client failures caused by a conflicting resource state.
fn conflict_error(error: impl std::fmt::Display) -> ClientWorkerError {
    client_error(ClientErrorKind::Conflict, error)
}

/// Marks reusable client failures that do not map to a domain status.
fn operation_failed_error(error: impl std::fmt::Display) -> ClientWorkerError {
    client_error(ClientErrorKind::OperationFailed, error)
}

/// Builds a conflict error for a unique resource name that is already visible.
fn already_exists(kind: &str, name: &str) -> ClientWorkerError {
    ClientWorkerError::Conflict(format!("{kind} '{name}' already exists"))
}

/// Ensures one node id is visible before running a node mutation.
async fn ensure_node_exists(config: &ClientConfig, node_id: Uuid) -> Result<(), ClientWorkerError> {
    get_node(config, &node_id.to_string()).await.map(|_| ())
}

/// Ensures one network id is visible before running a network subresource operation.
async fn ensure_network_exists(
    config: &ClientConfig,
    network_id: &str,
) -> Result<(), ClientWorkerError> {
    get_network(config, network_id).await.map(|_| ())
}

/// Ensures one secret name is visible before running a secret mutation.
async fn ensure_secret_exists(config: &ClientConfig, name: &str) -> Result<(), ClientWorkerError> {
    secrets::show(config, name, None)
        .await
        .map(|_| ())
        .map_err(not_found_error)
}

/// Rejects a network create request if another network already owns the name.
async fn ensure_unique_network_name(
    config: &ClientConfig,
    name: &str,
) -> Result<(), ClientWorkerError> {
    if list_networks(config)
        .await?
        .iter()
        .any(|network| network.name == name)
    {
        return Err(already_exists("network", name));
    }
    Ok(())
}

/// Rejects a volume create request if another volume already owns the name.
async fn ensure_unique_volume_name(
    config: &ClientConfig,
    name: &str,
) -> Result<(), ClientWorkerError> {
    if list_volumes(config)
        .await?
        .iter()
        .any(|volume| volume.name == name)
    {
        return Err(already_exists("volume", name));
    }
    Ok(())
}

/// Rejects a secret create request if another secret already owns the name.
async fn ensure_unique_secret_name(
    config: &ClientConfig,
    name: &str,
) -> Result<(), ClientWorkerError> {
    if list_secrets(config)
        .await?
        .iter()
        .any(|secret| secret.name == name)
    {
        return Err(already_exists("secret", name));
    }
    Ok(())
}

/// Converts the REST drain timeout into the protocol-supported duration range.
fn drain_timeout(value: Option<u64>) -> Result<Option<Duration>, ClientWorkerError> {
    let Some(secs) = value else {
        return Ok(None);
    };
    let _wire_secs = u32::try_from(secs).map_err(|_| {
        ClientWorkerError::InvalidRequest(format!(
            "task_stop_timeout_secs {secs} exceeds protocol limit"
        ))
    })?;
    Ok(Some(Duration::from_secs(secs)))
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

/// Normalizes optional text fields and drops empty values.
fn clean_optional_text(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_string();
        (!value.is_empty()).then_some(value)
    })
}
