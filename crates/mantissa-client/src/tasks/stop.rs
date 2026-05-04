use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::TaskRow;
use anyhow::Result;

/// Stops one task by selector and returns the stopped task snapshot.
pub async fn stop(cfg: &ClientConfig, id: &str) -> Result<TaskRow> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
    let mut request = task.stop_request();
    let mut builder = request.get().init_request();
    builder.set_selector(id);

    let response = request.send().promise.await?;
    let spec = response.get()?.get_spec()?;
    Ok(TaskRow::from_reader(spec)?)
}
