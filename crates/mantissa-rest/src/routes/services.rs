use crate::{
    auth::RestAuth,
    error::RestError,
    extract::{RestJson, RestQuery},
    routes::{tasks::drive_task_websocket, worker_error_to_rest},
    state::AppState,
    stream::{service_logs::ServiceLogEvent, task_exec::TaskInteractiveEvent},
    types::services::{
        ServiceDeployRequest, ServiceDeployResponse, ServiceDetail, ServiceExecQuery,
        ServiceLogsQuery, ServiceSummary, ServiceTaskSummary,
    },
};
use axum::{
    Json,
    body::Body,
    extract::{Path, State, ws::WebSocketUpgrade},
    http::header::CONTENT_TYPE,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use utoipa::IntoParams;

/// Controls whether the service list includes stopped records retained for inspection.
#[derive(Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct ServiceListQuery {
    /// Include stopped services. Defaults to false.
    #[serde(default)]
    #[param(default = false)]
    pub include_stopped: bool,
}

/// Lists services visible to the local daemon.
#[utoipa::path(
    get,
    path = "/v1/services",
    tag = "services",
    params(ServiceListQuery),
    responses((status = 200, description = "Services visible to the local daemon.", body = [ServiceSummary]))
)]
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestQuery(query): RestQuery<ServiceListQuery>,
) -> Result<Json<Vec<ServiceSummary>>, RestError> {
    state
        .client()
        .list_services(query.include_stopped)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Deploys or updates one service manifest through the local daemon.
#[utoipa::path(
    post,
    path = "/v1/services",
    tag = "services",
    request_body = ServiceDeployRequest,
    responses((status = 200, description = "Service deployment result.", body = ServiceDeployResponse))
)]
pub async fn deploy(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestJson(request): RestJson<ServiceDeployRequest>,
) -> Result<Json<ServiceDeployResponse>, RestError> {
    state
        .client()
        .deploy_service(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one service by UUID text or exact service name.
#[utoipa::path(
    get,
    path = "/v1/services/{selector}",
    tag = "services",
    params(("selector" = String, Path, description = "Service UUID string or exact service name.")),
    responses((status = 200, description = "Complete service configuration and replica progress.", body = ServiceDetail))
)]
pub async fn get(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<ServiceDetail>, RestError> {
    state
        .client()
        .get_service(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Lists a service's observed replicas, including retained stopped and failed tasks.
#[utoipa::path(
    get,
    path = "/v1/services/{selector}/tasks",
    tag = "services",
    params(("selector" = String, Path, description = "Service UUID string or exact service name.")),
    responses((status = 200, description = "Observed service tasks, including retained task attempts.", body = [ServiceTaskSummary]))
)]
pub async fn tasks(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<Vec<ServiceTaskSummary>>, RestError> {
    state
        .client()
        .list_service_tasks(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Combines logs for a fixed set of service tasks, with task identity on every event.
#[utoipa::path(
    get,
    path = "/v1/services/{selector}/logs",
    operation_id = "service_logs",
    tag = "services",
    params(
        ("selector" = String, Path, description = "Service UUID string or exact service name."),
        ServiceLogsQuery
    ),
    responses((status = 200, description = "Newline-delimited log events from tasks present at request start. Following does not include newly created tasks.", body = ServiceLogEvent, content_type = "application/x-ndjson"))
)]
pub async fn logs(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
    RestQuery(query): RestQuery<ServiceLogsQuery>,
) -> Result<Response, RestError> {
    let stream = state
        .client()
        .service_logs(selector, query)
        .await
        .map_err(worker_error_to_rest)?;

    Ok((
        [(CONTENT_TYPE, "application/x-ndjson")],
        Body::from_stream(stream),
    )
        .into_response())
}

/// Selects one running service task before opening the shared command stream.
#[utoipa::path(
    get,
    path = "/v1/services/{selector}/exec",
    operation_id = "service_exec",
    tag = "services",
    params(
        ("selector" = String, Path, description = "Service UUID string or exact service name."),
        ServiceExecQuery
    ),
    responses((status = 101, description = "WebSocket upgrade. The first event is selected_task, followed by task exec output and exit-result events. Clients send TaskInteractiveClientMessage JSON frames.", body = TaskInteractiveEvent))
)]
pub async fn exec(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
    RestQuery(query): RestQuery<ServiceExecQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, RestError> {
    let (task, session) = state
        .client()
        .service_exec(selector, query)
        .await
        .map_err(worker_error_to_rest)?;
    let selected_task = TaskInteractiveEvent::SelectedTask {
        id: task.id,
        template: task.template,
        replica: task.replica,
    };

    Ok(ws
        .on_upgrade(move |socket| drive_task_websocket(socket, session, Some(selected_task)))
        .into_response())
}

/// Deletes one service by UUID text or exact service name.
#[utoipa::path(
    delete,
    path = "/v1/services/{selector}",
    tag = "services",
    params(("selector" = String, Path, description = "Service UUID string or exact service name.")),
    responses((status = 200, description = "Deleted service summary.", body = ServiceSummary))
)]
pub async fn delete(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<ServiceSummary>, RestError> {
    state
        .client()
        .delete_service(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one service status snapshot by UUID text or exact service name.
#[utoipa::path(
    get,
    path = "/v1/services/{selector}/status",
    tag = "services",
    params(("selector" = String, Path, description = "Service UUID string or exact service name.")),
    responses((status = 200, description = "Complete service configuration and replica progress.", body = ServiceDetail))
)]
pub async fn status(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<ServiceDetail>, RestError> {
    state
        .client()
        .get_service(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
