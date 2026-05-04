use crate::tasks::util::write_frame;
use anyhow::Result;
use capnp_rpc::new_client;
use mantissa_client::config::ClientConfig;
pub use mantissa_client::tasks::TaskLogsOptions;
use mantissa_protocol::task::task_log_sink;
use std::rc::Rc;

/// Sink used by the CLI to render streamed task log frames as they arrive.
struct CliTaskLogSink;

impl task_log_sink::Server for CliTaskLogSink {
    async fn push_frame(
        self: Rc<Self>,
        params: task_log_sink::PushFrameParams,
    ) -> Result<(), capnp::Error> {
        let frame = params.get()?.get_frame()?;
        let stream = frame
            .get_stream()
            .map_err(|_| capnp::Error::failed("unknown task log stream".into()))?;
        let bytes = frame.get_data()?.to_owned();
        write_frame(stream, bytes.as_slice())
    }

    async fn end(
        self: Rc<Self>,
        _params: task_log_sink::EndParams,
        _results: task_log_sink::EndResults,
    ) -> Result<(), capnp::Error> {
        Ok(())
    }
}

/// Streams task logs from the local node or the current remote owner.
pub async fn logs(cfg: &ClientConfig, id: &str, options: &TaskLogsOptions<'_>) -> Result<()> {
    mantissa_client::tasks::logs_with_sink(cfg, id, options, new_client(CliTaskLogSink)).await
}
