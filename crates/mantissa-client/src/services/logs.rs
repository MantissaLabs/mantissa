use crate::{
    config::ClientConfig,
    connection,
    error::{ClientError, ClientErrorKind},
    tasks::{TaskLogsOptions, logs::logs_with_client},
};
use anyhow::{Result, ensure};
use capnp_rpc::new_client;
use futures::{Stream, StreamExt, stream::FuturesUnordered};
use mantissa_protocol::task::{TaskLogStream, task_log_sink};
use serde::Serialize;
use std::{
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{sync::mpsc, task::JoinHandle};

/// Identifies a log source even when several generations of a replica are visible.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ServiceLogSource {
    pub task_id: String,
    pub template: String,
    pub replica: u16,
    pub generation: u64,
    pub node_id: String,
}

/// Preserves raw log bytes and reports each task's completion or failure independently.
#[derive(Debug)]
pub enum ServiceLogEvent {
    /// Runtime output with the identity of the task that emitted it.
    Frame {
        source: Arc<ServiceLogSource>,
        stream: TaskLogStream,
        data: Vec<u8>,
    },
    /// The task's log request completed; no more frames will follow for it.
    End { source: Arc<ServiceLogSource> },
    /// This task's log request failed while other requests may continue.
    Error {
        source: Arc<ServiceLogSource>,
        message: String,
    },
}

/// Owns a fixed set of log requests and cancels them when the consumer goes away.
pub struct ServiceLogStream {
    receiver: mpsc::Receiver<ServiceLogEvent>,
    producer: JoinHandle<()>,
}

impl ServiceLogStream {
    /// Receives the next task event without requiring callers to manage the producer task.
    pub async fn recv(&mut self) -> Option<ServiceLogEvent> {
        self.receiver.recv().await
    }
}

impl Stream for ServiceLogStream {
    type Item = ServiceLogEvent;

    /// Exposes the bounded receiver to HTTP bodies and other stream consumers.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().receiver).poll_recv(cx)
    }
}

impl Drop for ServiceLogStream {
    /// Stops outstanding RPCs when the CLI or HTTP reader closes the stream.
    fn drop(&mut self) {
        self.producer.abort();
    }
}

/// Selects tasks once, then follows only those task IDs until their streams finish.
pub async fn logs(
    cfg: &ClientConfig,
    selector: &str,
    template: Option<&str>,
    options: &TaskLogsOptions<'_>,
) -> Result<ServiceLogStream> {
    let options = options
        .normalized()
        .map_err(|error| ClientError::from_display(ClientErrorKind::InvalidRequest, error))?;
    let template = template.map(str::trim);
    ensure!(
        template.is_none_or(|value| !value.is_empty()),
        ClientError::new(
            ClientErrorKind::InvalidRequest,
            "template must not be empty"
        )
    );

    let sources: Vec<_> = super::tasks(cfg, selector)
        .await?
        .into_iter()
        .filter(|task| template.is_none_or(|template| task.template == template))
        .map(|task| {
            Arc::new(ServiceLogSource {
                task_id: task.id,
                template: task.template,
                replica: task.replica,
                generation: task.generation,
                node_id: task.node_id,
            })
        })
        .collect();

    let session = connection::get_local_session(cfg).await?;
    let task_client = session.get_task_request().send().pipeline.get_task();

    // A slow reader must apply backpressure instead of accumulating logs from every replica.
    let (events, receiver) = mpsc::channel(16);
    let producer = tokio::task::spawn_local(async move {
        let mut requests: FuturesUnordered<_> = sources
            .into_iter()
            .map(|source| {
                let task_client = &task_client;
                let options = &options;
                let sink = new_client(ServiceTaskLogSink {
                    source: source.clone(),
                    events: events.clone(),
                });

                async move {
                    let result =
                        logs_with_client(task_client, &source.task_id, options, sink).await;
                    match result {
                        Ok(()) => ServiceLogEvent::End { source },
                        Err(error) => ServiceLogEvent::Error {
                            source,
                            message: format!("{error:#}"),
                        },
                    }
                }
            })
            .collect();

        loop {
            let event = tokio::select! {
                event = requests.next() => event,
                _ = events.closed() => break,
            };
            let Some(event) = event else { break };
            if events.send(event).await.is_err() {
                break;
            }
        }
    });

    Ok(ServiceLogStream { receiver, producer })
}

/// Attaches source identity to frames while retaining the task RPC's backpressure.
struct ServiceTaskLogSink {
    source: Arc<ServiceLogSource>,
    events: mpsc::Sender<ServiceLogEvent>,
}

impl task_log_sink::Server for ServiceTaskLogSink {
    /// Forwards bytes unchanged so consumers can handle split lines and non-UTF-8 output.
    async fn push_frame(
        self: Rc<Self>,
        params: task_log_sink::PushFrameParams,
    ) -> capnp::Result<()> {
        let frame = params.get()?.get_frame()?;
        let event = ServiceLogEvent::Frame {
            source: self.source.clone(),
            stream: frame.get_stream()?,
            data: frame.get_data()?.to_vec(),
        };

        self.events
            .send(event)
            .await
            .map_err(|_| capnp::Error::disconnected("service log output closed".into()))
    }

    /// Completion is emitted after the log RPC settles, including any final RPC error.
    async fn end(
        self: Rc<Self>,
        _params: task_log_sink::EndParams,
        _results: task_log_sink::EndResults,
    ) -> capnp::Result<()> {
        Ok(())
    }
}
