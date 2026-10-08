use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::TaskRow;
use crate::volumes;
use anyhow::Result;

pub struct TaskStartOptions<'a> {
    pub name: &'a str,
    pub image: &'a str,
    pub registry_auth: Option<crate::jobs::manifest::SecretReference>,
    pub command: &'a [String],
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub volumes: &'a [String],
}

/// Starts one standalone task and returns its created task snapshot.
pub async fn start(cfg: &ClientConfig, options: &TaskStartOptions<'_>) -> Result<TaskRow> {
    if let Some(auth) = &options.registry_auth
        && auth.name.trim().is_empty()
    {
        anyhow::bail!("registry_auth secret name must not be empty");
    }

    let resolved_volumes = volumes::resolve_cli_volume_mounts(cfg, options.volumes).await?;
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
    let mut request = task.start_request();

    {
        let mut builder = request.get().init_request();
        builder.set_name(options.name);
        builder.set_image(options.image);
        if let Some(auth) = &options.registry_auth {
            crate::workload_wire::write_secret_reference(
                builder.reborrow().init_registry_auth(),
                auth,
            );
        }

        let mut cmd_builder = builder
            .reborrow()
            .init_command(options.command.len() as u32);
        for (idx, arg) in options.command.iter().enumerate() {
            cmd_builder.set(idx as u32, arg);
        }
        builder.set_cpu_millis(options.cpu_millis);
        builder.set_memory_bytes(options.memory_bytes);
        builder.set_gpu_count(options.gpu_count);
        builder.reborrow().init_slot_ids(0);
        builder.reborrow().init_gpu_device_ids(0);
        let mut volume_builder = builder
            .reborrow()
            .init_volumes(resolved_volumes.len() as u32);
        for (idx, mount) in resolved_volumes.iter().enumerate() {
            let mut entry = volume_builder.reborrow().get(idx as u32);
            entry.set_volume_id(mount.volume_id.as_bytes());
            entry.set_volume_name(&mount.volume_name);
            entry.set_target(&mount.target);
            entry.set_read_only(mount.read_only);
        }
    }

    let response = request.send().promise.await?;
    let spec = response.get()?.get_spec()?;
    Ok(TaskRow::from_reader(spec)?)
}
