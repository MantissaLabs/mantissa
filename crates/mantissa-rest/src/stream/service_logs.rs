use axum::body::Bytes;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_core::Stream;
use mantissa_client::services::{
    ServiceLogEvent as ClientEvent, ServiceLogSource, ServiceLogStream,
};
use serde::Serialize;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use utoipa::ToSchema;

/// Each event identifies its task; errors and completion apply only to that task's stream.
#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServiceLogEvent {
    /// Raw output from one task, encoded without assuming UTF-8 or complete lines.
    Frame {
        source: ServiceLogSource,
        stream: String,
        data_base64: String,
    },
    /// The task's log request completed and will emit no further frames.
    End { source: ServiceLogSource },
    /// The task's log request failed; other task streams can continue.
    Error {
        source: ServiceLogSource,
        message: String,
    },
}

impl From<ClientEvent> for ServiceLogEvent {
    /// Keeps source fields structured instead of embedding terminal prefixes in log bytes.
    fn from(event: ClientEvent) -> Self {
        match event {
            ClientEvent::Frame {
                source,
                stream,
                data,
            } => Self::Frame {
                source: (*source).clone(),
                stream: super::task_logs::stream_label(stream).to_string(),
                data_base64: STANDARD.encode(data),
            },
            ClientEvent::End { source } => Self::End {
                source: (*source).clone(),
            },
            ClientEvent::Error { source, message } => Self::Error {
                source: (*source).clone(),
                message,
            },
        }
    }
}

/// Consumes the client's bounded stream directly; dropping it cancels all selected log requests.
pub struct ServiceLogHttpStream(pub ServiceLogStream);

impl Stream for ServiceLogHttpStream {
    type Item = Result<Bytes, serde_json::Error>;

    /// Encodes each event independently so HTTP clients can read output as it arrives.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().0).poll_next(cx).map(|event| {
            event.map(|event| {
                let event = ServiceLogEvent::from(event);
                serde_json::to_vec(&event).map(|mut bytes| {
                    bytes.push(b'\n');
                    Bytes::from(bytes)
                })
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantissa_protocol::task::TaskLogStream;
    use serde_json::json;
    use std::sync::Arc;

    /// JSON keeps the task identity separate and preserves arbitrary bytes through base64.
    #[test]
    fn service_log_events_preserve_source_and_bytes() {
        let source = Arc::new(ServiceLogSource {
            task_id: "11111111-0000-4000-8000-000000000001".into(),
            template: "backend".into(),
            replica: 2,
            generation: 7,
            node_id: "22222222-0000-4000-8000-000000000002".into(),
        });
        let frame = ServiceLogEvent::from(ClientEvent::Frame {
            source: source.clone(),
            stream: TaskLogStream::Stderr,
            data: vec![0xff, b'\n'],
        });

        let value = serde_json::to_value(frame).unwrap();

        assert_eq!(
            value,
            json!({
                "type": "frame",
                "source": {
                    "task_id": "11111111-0000-4000-8000-000000000001",
                    "template": "backend",
                    "replica": 2,
                    "generation": 7,
                    "node_id": "22222222-0000-4000-8000-000000000002"
                },
                "stream": "stderr",
                "data_base64": "/wo="
            })
        );

        let error = ServiceLogEvent::from(ClientEvent::Error {
            source: source.clone(),
            message: "runtime unavailable".into(),
        });
        let end = ServiceLogEvent::from(ClientEvent::End { source });
        let error = serde_json::to_value(error).unwrap();
        let end = serde_json::to_value(end).unwrap();

        assert_eq!(error["type"], "error");
        assert_eq!(error["message"], "runtime unavailable");
        assert_eq!(error["source"], value["source"]);
        assert_eq!(end["type"], "end");
        assert_eq!(end["source"], value["source"]);
    }
}
