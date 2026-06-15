//! Task attach and exec WebSocket bridge from Cap'n Proto sinks to JSON frames.

use base64::{Engine, engine::general_purpose::STANDARD};
use capnp_rpc::new_client;
use mantissa_protocol::task::{TaskLogStream, task_log_sink};
use serde::{Deserialize, Serialize};
use std::rc::Rc;
use tokio::sync::{mpsc, oneshot};
use utoipa::ToSchema;

/// Bounded event buffer between Cap'n Proto streams and WebSocket writers.
pub const TASK_INTERACTIVE_EVENT_BUFFER: usize = 16;

/// One stdin/control message sent from the WebSocket handler to the worker.
#[derive(Debug, PartialEq, Eq)]
pub enum TaskInteractiveInput {
    Data(Vec<u8>),
    CloseInput,
}

/// One JSON message accepted from REST WebSocket clients.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskInteractiveClientMessage {
    Input { data_base64: String },
    CloseInput,
}

/// One JSON event emitted to REST WebSocket clients.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskInteractiveEvent {
    Frame {
        stream: String,
        data_base64: String,
    },
    Result {
        has_exit_code: bool,
        exit_code: Option<i32>,
    },
    End,
    Error {
        message: String,
    },
}

impl TaskInteractiveEvent {
    /// Converts one protocol log frame into the stable WebSocket event shape.
    pub fn frame(stream: TaskLogStream, data: Vec<u8>) -> Self {
        Self::Frame {
            stream: stream_label(stream).to_string(),
            data_base64: STANDARD.encode(data),
        }
    }

    /// Converts one exec result into the stable WebSocket event shape.
    pub fn result(exit_code: Option<i32>) -> Self {
        Self::Result {
            has_exit_code: exit_code.is_some(),
            exit_code,
        }
    }

    /// Converts one streaming failure into an in-band WebSocket event.
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
        }
    }

    /// Serializes this event as one JSON text message.
    pub fn into_json_text(self) -> String {
        serde_json::to_string(&self).unwrap_or_else(|_| {
            "{\"type\":\"error\",\"message\":\"failed to encode task stream event\"}".to_string()
        })
    }
}

/// Worker-owned task stream session exposed to WebSocket route handlers.
pub struct TaskInteractiveSession {
    input: mpsc::Sender<TaskInteractiveInput>,
    events: mpsc::Receiver<TaskInteractiveEvent>,
    cancel: Option<oneshot::Sender<()>>,
    requires_result: bool,
}

impl TaskInteractiveSession {
    /// Builds one bidirectional stream session around bounded channels.
    pub fn new(
        input: mpsc::Sender<TaskInteractiveInput>,
        events: mpsc::Receiver<TaskInteractiveEvent>,
        cancel: oneshot::Sender<()>,
        requires_result: bool,
    ) -> Self {
        Self {
            input,
            events,
            cancel: Some(cancel),
            requires_result,
        }
    }

    /// Forwards one client input/control message to the worker task.
    pub async fn send_input(&self, input: TaskInteractiveInput) -> Result<(), String> {
        self.input
            .send(input)
            .await
            .map_err(|_| "task stream session is closed".to_string())
    }

    /// Receives the next output/result event from the worker task.
    pub async fn recv_event(&mut self) -> Option<TaskInteractiveEvent> {
        self.events.recv().await
    }

    /// Returns whether this session must emit an exec result before close.
    pub fn requires_result(&self) -> bool {
        self.requires_result
    }
}

impl Drop for TaskInteractiveSession {
    /// Cancels the worker-side stream task when the WebSocket handler drops.
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ignored = cancel.send(());
        }
    }
}

/// Parses one JSON client text message into a worker input event.
pub fn decode_client_message(message: &str) -> Result<TaskInteractiveInput, String> {
    match serde_json::from_str::<TaskInteractiveClientMessage>(message)
        .map_err(|error| format!("invalid task stream message: {error}"))?
    {
        TaskInteractiveClientMessage::Input { data_base64 } => STANDARD
            .decode(data_base64)
            .map(TaskInteractiveInput::Data)
            .map_err(|error| format!("invalid data_base64: {error}")),
        TaskInteractiveClientMessage::CloseInput => Ok(TaskInteractiveInput::CloseInput),
    }
}

/// Builds a Cap'n Proto sink client that forwards frames into a WebSocket event channel.
pub fn new_task_interactive_sink(
    events: mpsc::Sender<TaskInteractiveEvent>,
) -> task_log_sink::Client {
    new_client(RestTaskInteractiveSink { events })
}

/// Cap'n Proto task sink backed by a bounded WebSocket event channel.
struct RestTaskInteractiveSink {
    events: mpsc::Sender<TaskInteractiveEvent>,
}

impl task_log_sink::Server for RestTaskInteractiveSink {
    /// Pushes one Cap'n Proto frame into the WebSocket event channel.
    async fn push_frame(
        self: Rc<Self>,
        params: task_log_sink::PushFrameParams,
    ) -> Result<(), capnp::Error> {
        let frame = params.get()?.get_frame()?;
        let stream = frame
            .get_stream()
            .map_err(|_| capnp::Error::failed("unknown task log stream".to_string()))?;
        let data = frame.get_data()?.to_owned();
        self.events
            .send(TaskInteractiveEvent::frame(stream, data))
            .await
            .map_err(|_| capnp::Error::failed("REST task stream closed".to_string()))
    }

    /// Forwards the Cap'n Proto end marker to the WebSocket event channel.
    async fn end(
        self: Rc<Self>,
        _params: task_log_sink::EndParams,
        _results: task_log_sink::EndResults,
    ) -> Result<(), capnp::Error> {
        self.events
            .send(TaskInteractiveEvent::End)
            .await
            .map_err(|_| capnp::Error::failed("REST task stream closed".to_string()))
    }
}

/// Returns the stable REST label for a protocol task log stream.
fn stream_label(stream: TaskLogStream) -> &'static str {
    match stream {
        TaskLogStream::Stdout => "stdout",
        TaskLogStream::Stderr => "stderr",
        TaskLogStream::Console => "console",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_client_message_decodes_base64_input() {
        let input = decode_client_message(r#"{"type":"input","data_base64":"aGVsbG8="}"#).unwrap();
        assert_eq!(input, TaskInteractiveInput::Data(b"hello".to_vec()));
    }

    #[test]
    fn websocket_event_encodes_result() {
        let text = TaskInteractiveEvent::result(Some(7)).into_json_text();
        assert_eq!(
            text,
            "{\"type\":\"result\",\"has_exit_code\":true,\"exit_code\":7}"
        );
    }
}
