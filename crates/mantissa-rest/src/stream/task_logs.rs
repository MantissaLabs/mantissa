//! Task log streaming bridge from Cap'n Proto sinks to HTTP bodies.

use axum::body::Bytes;
use base64::{Engine, engine::general_purpose::STANDARD};
use capnp_rpc::new_client;
use futures_core::Stream;
use mantissa_protocol::task::{TaskLogStream, task_log_sink};
use serde::Serialize;
use std::{
    convert::Infallible,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};
use tokio::sync::{mpsc, oneshot};
use utoipa::ToSchema;

/// Bounded frame buffer between the Cap'n Proto sink and HTTP response body.
pub const TASK_LOG_EVENT_BUFFER: usize = 16;

/// One task log event emitted by the REST streaming endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskLogEvent {
    Frame { stream: String, data_base64: String },
    Error { message: String },
}

impl TaskLogEvent {
    /// Converts one protocol log frame into the stable REST stream event.
    pub fn frame(stream: TaskLogStream, data: Vec<u8>) -> Self {
        Self::Frame {
            stream: stream_label(stream).to_string(),
            data_base64: STANDARD.encode(data),
        }
    }

    /// Converts one streaming failure into an in-band REST stream event.
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
        }
    }

    /// Serializes this event as one newline-delimited JSON frame.
    fn into_ndjson_bytes(self) -> Bytes {
        let mut bytes = serde_json::to_vec(&self).unwrap_or_else(|_| {
            b"{\"type\":\"error\",\"message\":\"failed to encode task log event\"}".to_vec()
        });
        bytes.push(b'\n');
        Bytes::from(bytes)
    }
}

/// HTTP body stream for task log events.
pub struct TaskLogHttpStream {
    receiver: mpsc::Receiver<TaskLogEvent>,
    cancel: Option<oneshot::Sender<()>>,
}

impl TaskLogHttpStream {
    /// Builds an HTTP stream from a bounded event receiver and cancellation signal.
    pub fn new(receiver: mpsc::Receiver<TaskLogEvent>, cancel: oneshot::Sender<()>) -> Self {
        Self {
            receiver,
            cancel: Some(cancel),
        }
    }
}

impl Drop for TaskLogHttpStream {
    /// Cancels the worker-side log task when the HTTP body is dropped.
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ignored = cancel.send(());
        }
    }
}

impl Stream for TaskLogHttpStream {
    type Item = Result<Bytes, Infallible>;

    /// Polls the next NDJSON event from the bounded log channel.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        Pin::new(&mut stream.receiver)
            .poll_recv(cx)
            .map(|event| event.map(|event| Ok(event.into_ndjson_bytes())))
    }
}

/// Builds a Cap'n Proto sink client that forwards frames into the REST stream.
pub fn new_task_log_sink(events: mpsc::Sender<TaskLogEvent>) -> task_log_sink::Client {
    new_client(RestTaskLogSink { events })
}

/// Cap'n Proto task log sink backed by a bounded REST event channel.
struct RestTaskLogSink {
    events: mpsc::Sender<TaskLogEvent>,
}

impl task_log_sink::Server for RestTaskLogSink {
    /// Pushes one Cap'n Proto frame into the REST event channel.
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
            .send(TaskLogEvent::frame(stream, data))
            .await
            .map_err(|_| capnp::Error::failed("REST task log stream closed".to_string()))
    }

    /// Acknowledges the producer's end-of-stream marker.
    async fn end(
        self: Rc<Self>,
        _params: task_log_sink::EndParams,
        _results: task_log_sink::EndResults,
    ) -> Result<(), capnp::Error> {
        Ok(())
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
    use futures_util::StreamExt;

    #[tokio::test]
    async fn http_stream_encodes_one_ndjson_frame() {
        let (tx, rx) = mpsc::channel(1);
        let (cancel_tx, _cancel_rx) = oneshot::channel();
        let mut stream = TaskLogHttpStream::new(rx, cancel_tx);

        tx.send(TaskLogEvent::frame(
            TaskLogStream::Stdout,
            b"hello".to_vec(),
        ))
        .await
        .unwrap();
        drop(tx);

        let bytes = stream.next().await.unwrap().unwrap();
        assert_eq!(
            bytes,
            Bytes::from_static(
                b"{\"type\":\"frame\",\"stream\":\"stdout\",\"data_base64\":\"aGVsbG8=\"}\n"
            )
        );
    }

    #[tokio::test]
    async fn dropping_http_stream_sends_cancellation() {
        let (_tx, rx) = mpsc::channel(1);
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let stream = TaskLogHttpStream::new(rx, cancel_tx);

        drop(stream);

        cancel_rx.await.unwrap();
    }
}
