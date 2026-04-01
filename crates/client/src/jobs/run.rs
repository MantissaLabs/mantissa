use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use crate::tasks::uuid_to_string;
use crate::volumes;
use anyhow::Result;
use std::io::Write;

/// Options accepted by `mantissa jobs run`.
pub struct JobRunOptions<'a> {
    pub name: &'a str,
    pub image: &'a str,
    pub command: &'a [String],
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub max_retries: u32,
    pub retry_backoff_secs: u32,
    pub volumes: &'a [String],
}

/// Submits one first-class job through the jobs control-plane capability.
pub async fn run(cfg: &ClientConfig, options: &JobRunOptions<'_>) -> Result<()> {
    let resolved_volumes = volumes::resolve_cli_volume_mounts(cfg, options.volumes).await?;
    let session = connection::get_local_session(cfg).await?;

    let request = session.get_jobs_request();
    let jobs = request.send().pipeline.get_jobs();
    let mut request = jobs.submit_request();

    {
        let mut builder = request.get().init_spec();
        builder.set_name(options.name);
        let mut execution = builder.reborrow().init_execution();
        execution.set_image(options.image);
        execution.set_tty(false);
        execution.set_cpu_millis(options.cpu_millis);
        execution.set_memory_bytes(options.memory_bytes);
        execution.set_gpu_count(options.gpu_count);

        let mut command = execution
            .reborrow()
            .init_command(options.command.len() as u32);
        for (index, arg) in options.command.iter().enumerate() {
            command.set(index as u32, arg);
        }

        let mut volumes = execution
            .reborrow()
            .init_volumes(resolved_volumes.len() as u32);
        for (index, mount) in resolved_volumes.iter().enumerate() {
            let mut entry = volumes.reborrow().get(index as u32);
            entry.set_volume_id(mount.volume_id.as_bytes());
            entry.set_volume_name(&mount.volume_name);
            entry.set_target(&mount.target);
            entry.set_read_only(mount.read_only);
        }

        execution.reborrow().init_env(0);
        execution.reborrow().init_secret_files(0);
        execution.reborrow().init_networks(0);

        let mut retry_policy = builder.reborrow().init_retry_policy();
        retry_policy.set_max_retries(options.max_retries);
        retry_policy.set_backoff_secs(options.retry_backoff_secs);
    }

    let response = request.send().promise.await?;
    let reader = response.get()?;
    let job_id = uuid_to_string(reader.get_job_id()?)?;

    let mut tw = tabwriter::TabWriter::new(Vec::new());
    writeln!(&mut tw, "ID\tNAME\tIMAGE\tCPU(m)\tMEM(MiB)\tGPU\tRETRIES")?;
    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}",
        job_id,
        options.name,
        options.image,
        options.cpu_millis,
        options.memory_bytes / (1024 * 1024),
        options.gpu_count,
        options.max_retries,
    )?;
    tw.flush()?;

    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(format!("submitted job:\n{output}"));
    Ok(())
}
