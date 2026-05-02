use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use crate::tasks::uuid_to_string;
use crate::volumes;
use anyhow::Result;
use std::io::Write;

pub struct TaskStartOptions<'a> {
    pub name: &'a str,
    pub image: &'a str,
    pub command: &'a [String],
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub volumes: &'a [String],
}

pub async fn start(cfg: &ClientConfig, options: &TaskStartOptions<'_>) -> Result<()> {
    let resolved_volumes = volumes::resolve_cli_volume_mounts(cfg, options.volumes).await?;
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
    let mut request = task.start_request();

    {
        let mut builder = request.get().init_request();
        builder.set_name(options.name);
        builder.set_image(options.image);
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

    let id = uuid_to_string(spec.get_id()?)?;
    let state = spec.get_state()?.to_str()?.to_string();
    let node = spec.get_node_name()?.to_str()?.to_string();

    let mut command_display = Vec::new();
    for arg in spec.get_command()?.iter() {
        command_display.push(arg?.to_str()?.to_string());
    }

    let mut tw = tabwriter::TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tIMAGE\tCPU(m)\tMEM(MiB)\tGPU\tCOMMAND\tNODE\tSTATUS"
    )?;
    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        id,
        spec.get_name()?.to_str()?,
        spec.get_image()?.to_str()?,
        spec.get_cpu_millis(),
        spec.get_memory_bytes() / (1024 * 1024),
        spec.get_gpu_count(),
        if command_display.is_empty() {
            "-".to_string()
        } else {
            command_display.join(" ")
        },
        if node.is_empty() {
            "local".to_string()
        } else {
            node
        },
        state,
    )?;

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(format!("started task:\n{output}"));

    Ok(())
}
