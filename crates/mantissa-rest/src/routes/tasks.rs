//! Task route handlers.

use crate::{
    auth::RestAuth,
    error::RestError,
    extract::{RestJson, RestQuery},
    routes::worker_error_to_rest,
    state::AppState,
    stream::{
        task_exec::{
            TaskInteractiveEvent, TaskInteractiveInput, TaskInteractiveSession,
            decode_client_message,
        },
        task_logs::TaskLogEvent,
    },
    types::tasks::{TaskAttachQuery, TaskExecQuery, TaskLogsQuery, TaskStartRequest, TaskSummary},
};
use axum::{
    Json,
    body::Body,
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::header::CONTENT_TYPE,
    response::{IntoResponse, Response},
};

/// Lists standalone tasks visible to the local daemon.
#[utoipa::path(
    get,
    path = "/v1/tasks",
    tag = "tasks",
    responses((status = 200, description = "Standalone tasks visible to the local daemon.", body = [TaskSummary]))
)]
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<TaskSummary>>, RestError> {
    state
        .client()
        .list_tasks()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Starts one standalone task through the local daemon.
#[utoipa::path(
    post,
    path = "/v1/tasks",
    tag = "tasks",
    request_body = TaskStartRequest,
    responses((status = 200, description = "Started standalone task summary.", body = TaskSummary))
)]
pub async fn start(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestJson(request): RestJson<TaskStartRequest>,
) -> Result<Json<TaskSummary>, RestError> {
    state
        .client()
        .start_task(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one standalone task by UUID text or exact task name.
#[utoipa::path(
    get,
    path = "/v1/tasks/{selector}",
    tag = "tasks",
    params(("selector" = String, Path, description = "Task UUID string or exact task name.")),
    responses((status = 200, description = "Standalone task summary.", body = TaskSummary))
)]
pub async fn get(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<TaskSummary>, RestError> {
    state
        .client()
        .get_task(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Streams standalone task logs as newline-delimited JSON frames.
#[utoipa::path(
    get,
    path = "/v1/tasks/{selector}/logs",
    tag = "tasks",
    params(
        ("selector" = String, Path, description = "Task UUID string or exact task name."),
        TaskLogsQuery
    ),
    responses((status = 200, description = "Newline-delimited task log events.", body = TaskLogEvent, content_type = "application/x-ndjson"))
)]
pub async fn logs(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
    RestQuery(query): RestQuery<TaskLogsQuery>,
) -> Result<Response, RestError> {
    let stream = state
        .client()
        .task_logs(selector, query)
        .await
        .map_err(worker_error_to_rest)?;
    Ok((
        [(CONTENT_TYPE, "application/x-ndjson")],
        Body::from_stream(stream),
    )
        .into_response())
}

/// Opens a WebSocket bridge to one running task's stdio streams.
#[utoipa::path(
    get,
    path = "/v1/tasks/{selector}/attach",
    tag = "tasks",
    params(
        ("selector" = String, Path, description = "Task UUID string or exact task name."),
        TaskAttachQuery
    ),
    responses((status = 101, description = "WebSocket upgrade. Clients may send TaskInteractiveClientMessage JSON frames and receive TaskInteractiveEvent JSON frames.", body = TaskInteractiveEvent))
)]
pub async fn attach(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
    RestQuery(query): RestQuery<TaskAttachQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, RestError> {
    let session = state
        .client()
        .task_attach(selector, query)
        .await
        .map_err(worker_error_to_rest)?;
    Ok(ws
        .on_upgrade(move |socket| drive_task_websocket(socket, session))
        .into_response())
}

/// Opens a WebSocket bridge to one command exec session inside a running task.
#[utoipa::path(
    get,
    path = "/v1/tasks/{selector}/exec",
    tag = "tasks",
    params(
        ("selector" = String, Path, description = "Task UUID string or exact task name."),
        TaskExecQuery
    ),
    responses((status = 101, description = "WebSocket upgrade. Clients may send TaskInteractiveClientMessage JSON frames and receive TaskInteractiveEvent JSON frames.", body = TaskInteractiveEvent))
)]
pub async fn exec(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
    RestQuery(query): RestQuery<TaskExecQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, RestError> {
    let session = state
        .client()
        .task_exec(selector, query)
        .await
        .map_err(worker_error_to_rest)?;
    Ok(ws
        .on_upgrade(move |socket| drive_task_websocket(socket, session))
        .into_response())
}

/// Stops one standalone task by UUID text or accepted selector.
#[utoipa::path(
    post,
    path = "/v1/tasks/{selector}/stop",
    tag = "tasks",
    params(("selector" = String, Path, description = "Task UUID string or exact task name.")),
    responses((status = 200, description = "Stopped task summary.", body = TaskSummary))
)]
pub async fn stop(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<TaskSummary>, RestError> {
    state
        .client()
        .stop_task(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Drives one bidirectional task WebSocket until either side closes.
async fn drive_task_websocket(mut socket: WebSocket, mut session: TaskInteractiveSession) {
    let requires_result = session.requires_result();
    let mut end_seen = false;
    let mut result_seen = !requires_result;
    loop {
        tokio::select! {
            event = session.recv_event() => {
                let Some(event) = event else {
                    let _ignored = socket.send(Message::Close(None)).await;
                    return;
                };
                match event {
                    TaskInteractiveEvent::End => end_seen = true,
                    TaskInteractiveEvent::Result { .. } | TaskInteractiveEvent::Error { .. } => {
                        if requires_result {
                            result_seen = true;
                        }
                    }
                    TaskInteractiveEvent::Frame { .. } => {}
                }
                if socket.send(Message::text(event.into_json_text())).await.is_err() {
                    return;
                }
                if end_seen && result_seen {
                    let _ignored = socket.send(Message::Close(None)).await;
                    return;
                }
            }
            message = socket.recv() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        match decode_client_message(text.as_str()) {
                            Ok(input) => {
                                if session.send_input(input).await.is_err() {
                                    let event = TaskInteractiveEvent::error("task stream session is closed");
                                    let _ignored = socket.send(Message::text(event.into_json_text())).await;
                                    return;
                                }
                            }
                            Err(message) => {
                                let event = TaskInteractiveEvent::error(message);
                                if socket.send(Message::text(event.into_json_text())).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        if session
                            .send_input(TaskInteractiveInput::Data(bytes.to_vec()))
                            .await
                            .is_err()
                        {
                            let event = TaskInteractiveEvent::error("task stream session is closed");
                            let _ignored = socket.send(Message::text(event.into_json_text())).await;
                            return;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => return,
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                    Some(Err(_)) => return,
                }
            }
        }
    }
}
