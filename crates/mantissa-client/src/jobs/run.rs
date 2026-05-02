use crate::config::ClientConfig;
use crate::connection;
use crate::jobs::manifest::{
    EnvironmentVariable, JobManifest, LivenessProbe, SecretFileProjection, load_manifest_from_path,
};
use crate::output;
use crate::runtime_contract::{
    normalize_execution_platform, normalize_isolation_mode, normalize_isolation_profile,
};
use crate::tasks::uuid_to_string;
use crate::volumes;
use crate::workload_submit::{
    ManifestPortBinding, ResolvedDeclaredVolume, compute_network_id, ensure_declared_volumes,
    ensure_named_networks,
};
use crate::workload_wire::{
    PreparedVolumeMount, prepared_volume_mount_from_resolved, write_env_vars, write_liveness_probe,
    write_port_bindings, write_secret_files, write_volume_mounts,
};
use anyhow::{Result, anyhow};
use mantissa_protocol::jobs::{job_execution, job_retry_policy, job_submit_spec};
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use uuid::Uuid;

/// Default CPU request used by the raw `jobs run` CLI flow.
const DEFAULT_CPU_MILLIS: u64 = 1_000;

/// Default memory request used by the raw `jobs run` CLI flow.
const DEFAULT_MEMORY_BYTES: u64 = 536_870_912;

/// Default GPU request used by the raw `jobs run` CLI flow.
const DEFAULT_GPU_COUNT: u32 = 0;

/// Default controller retry count used by the raw `jobs run` CLI flow.
const DEFAULT_MAX_RETRIES: u32 = 0;

/// Default controller retry backoff used by the raw `jobs run` CLI flow.
const DEFAULT_RETRY_BACKOFF_SECS: u32 = 2;

/// Options accepted by `mantissa jobs run`.
pub struct JobRunOptions<'a> {
    pub manifest_path: Option<&'a Path>,
    pub name: Option<&'a str>,
    pub image: Option<&'a str>,
    pub command: &'a [String],
    pub tty: bool,
    pub cpu_millis: Option<u64>,
    pub memory_bytes: Option<u64>,
    pub gpu_count: Option<u32>,
    pub max_retries: Option<u32>,
    pub retry_backoff_secs: Option<u32>,
    pub execution_platform: &'a str,
    pub isolation_mode: &'a str,
    pub isolation_profile: Option<&'a str>,
    pub volumes: &'a [String],
}

/// One prepared jobs submission after CLI and manifest normalization.
struct PreparedJobSubmitSpec {
    name: String,
    execution: PreparedJobExecution,
    retry_policy: PreparedJobRetryPolicy,
    execution_platform: String,
    isolation_mode: String,
    isolation_profile: Option<String>,
}

/// One prepared execution template ready for jobs wire encoding.
struct PreparedJobExecution {
    image: String,
    command: Vec<String>,
    tty: bool,
    cpu_millis: u64,
    memory_bytes: u64,
    gpu_count: u32,
    termination_grace_period_secs: Option<u32>,
    pre_stop_command: Option<Vec<String>>,
    env: Vec<EnvironmentVariable>,
    secret_files: Vec<SecretFileProjection>,
    volumes: Vec<PreparedVolumeMount>,
    networks: Vec<Uuid>,
    ports: Vec<ManifestPortBinding>,
    liveness: Option<LivenessProbe>,
}

/// One prepared controller retry policy ready for jobs wire encoding.
struct PreparedJobRetryPolicy {
    max_retries: u32,
    backoff_secs: u32,
}

/// Submits one first-class job through the jobs control-plane capability.
pub async fn run(cfg: &ClientConfig, options: &JobRunOptions<'_>) -> Result<()> {
    let prepared = prepare_submit_spec(cfg, options).await?;
    let session = connection::get_local_session(cfg).await?;

    let request = session.get_jobs_request();
    let jobs = request.send().pipeline.get_jobs();
    let mut request = jobs.submit_request();
    write_job_submit_spec(request.get().init_spec(), &prepared)?;

    let response = request.send().promise.await?;
    let reader = response.get()?;
    let job_id = uuid_to_string(reader.get_job_id()?)?;

    let mut tw = tabwriter::TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tIMAGE\tCPU(m)\tMEM(MiB)\tGPU\tPLATFORM\tMODE\tPROFILE\tRETRIES"
    )?;
    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        job_id,
        prepared.name,
        prepared.execution.image,
        prepared.execution.cpu_millis,
        prepared.execution.memory_bytes / (1024 * 1024),
        prepared.execution.gpu_count,
        prepared.execution_platform,
        prepared.isolation_mode,
        prepared.isolation_profile.as_deref().unwrap_or("default"),
        prepared.retry_policy.max_retries,
    )?;
    tw.flush()?;

    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(format!("submitted job:\n{output}"));
    Ok(())
}

/// Normalizes one job CLI invocation into the public submit contract.
async fn prepare_submit_spec(
    cfg: &ClientConfig,
    options: &JobRunOptions<'_>,
) -> Result<PreparedJobSubmitSpec> {
    if let Some(path) = options.manifest_path {
        return prepare_manifest_submit_spec(cfg, path).await;
    }

    prepare_raw_submit_spec(cfg, options).await
}

/// Normalizes one raw-flag job submission into the public jobs submit contract.
async fn prepare_raw_submit_spec(
    cfg: &ClientConfig,
    options: &JobRunOptions<'_>,
) -> Result<PreparedJobSubmitSpec> {
    let name = options
        .name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("jobs run requires NAME unless --file is used"))?;
    let image = options
        .image
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("jobs run requires --image unless --file is used"))?;

    let resolved_volumes = volumes::resolve_cli_volume_mounts(cfg, options.volumes).await?;
    let execution_platform = normalize_execution_platform(options.execution_platform)?;
    let isolation_mode = normalize_isolation_mode(options.isolation_mode)?;
    let isolation_profile = normalize_isolation_profile(options.isolation_profile);

    Ok(PreparedJobSubmitSpec {
        name: name.to_string(),
        execution: PreparedJobExecution {
            image: image.to_string(),
            command: options.command.to_vec(),
            tty: options.tty,
            cpu_millis: options.cpu_millis.unwrap_or(DEFAULT_CPU_MILLIS),
            memory_bytes: options.memory_bytes.unwrap_or(DEFAULT_MEMORY_BYTES),
            gpu_count: options.gpu_count.unwrap_or(DEFAULT_GPU_COUNT),
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: resolved_volumes
                .iter()
                .map(prepared_volume_mount_from_resolved)
                .collect(),
            networks: Vec::new(),
            ports: Vec::new(),
            liveness: None,
        },
        retry_policy: PreparedJobRetryPolicy {
            max_retries: options.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            backoff_secs: options
                .retry_backoff_secs
                .unwrap_or(DEFAULT_RETRY_BACKOFF_SECS),
        },
        execution_platform,
        isolation_mode,
        isolation_profile,
    })
}

/// Normalizes one manifest-backed job submission into the public jobs submit contract.
async fn prepare_manifest_submit_spec(
    cfg: &ClientConfig,
    path: &Path,
) -> Result<PreparedJobSubmitSpec> {
    let manifest = load_manifest_from_path(path)?;
    ensure_named_networks(cfg, manifest.requested_networks()?).await?;
    let resolved_volumes = ensure_declared_volumes(cfg, &manifest.declared_volume_specs()).await?;

    Ok(PreparedJobSubmitSpec {
        name: manifest.name.clone(),
        execution: prepared_execution_from_manifest(&manifest, &resolved_volumes)?,
        retry_policy: PreparedJobRetryPolicy {
            max_retries: manifest.retry_policy.max_retries,
            backoff_secs: manifest.retry_policy.backoff_secs,
        },
        execution_platform: manifest.execution_platform.clone(),
        isolation_mode: manifest.isolation_mode.clone(),
        isolation_profile: manifest.isolation_profile.clone(),
    })
}

/// Resolves one job manifest execution template into a submit-ready execution payload.
fn prepared_execution_from_manifest(
    manifest: &JobManifest,
    resolved_volumes: &HashMap<String, ResolvedDeclaredVolume>,
) -> Result<PreparedJobExecution> {
    let execution = &manifest.execution;
    let mut prepared_volumes = Vec::with_capacity(execution.volumes.len());
    for mount in &execution.volumes {
        let source = mount.source.trim();
        let resolved = resolved_volumes.get(source).ok_or_else(|| {
            anyhow!(
                "job manifest references unresolved volume '{}'",
                mount.source
            )
        })?;
        prepared_volumes.push(PreparedVolumeMount {
            volume_id: resolved.volume_id,
            volume_name: resolved.volume_name.clone(),
            target: mount.target.clone(),
            read_only: mount.read_only,
        });
    }

    Ok(PreparedJobExecution {
        image: execution.image.clone(),
        command: execution.command.clone(),
        tty: execution.tty,
        cpu_millis: execution.resources.cpu_millis,
        memory_bytes: execution.resources.memory_bytes(),
        gpu_count: execution.resources.gpu_count,
        termination_grace_period_secs: execution.termination_grace_period_secs,
        pre_stop_command: execution.pre_stop_command.clone(),
        env: execution.env.clone(),
        secret_files: execution.secret_files.clone(),
        volumes: prepared_volumes,
        networks: execution
            .networks
            .iter()
            .map(|network| compute_network_id(network.trim()))
            .collect(),
        ports: execution.ports.clone(),
        liveness: execution.liveness.clone(),
    })
}

/// Encodes one prepared jobs submit payload into the jobs wire builder.
fn write_job_submit_spec(
    mut builder: job_submit_spec::Builder<'_>,
    spec: &PreparedJobSubmitSpec,
) -> Result<()> {
    builder.set_name(&spec.name);
    write_job_execution(builder.reborrow().init_execution(), &spec.execution)?;
    write_job_retry_policy(builder.reborrow().init_retry_policy(), &spec.retry_policy);
    builder.set_execution_platform(&spec.execution_platform);
    builder.set_isolation_mode(&spec.isolation_mode);
    builder.set_isolation_profile(spec.isolation_profile.as_deref().unwrap_or(""));
    Ok(())
}

/// Encodes one prepared jobs execution template into the jobs wire builder.
fn write_job_execution(
    mut builder: job_execution::Builder<'_>,
    execution: &PreparedJobExecution,
) -> Result<()> {
    builder.set_image(&execution.image);
    builder.set_tty(execution.tty);
    builder.set_cpu_millis(execution.cpu_millis);
    builder.set_memory_bytes(execution.memory_bytes);
    builder.set_gpu_count(execution.gpu_count);
    builder.set_termination_grace_period_secs(execution.termination_grace_period_secs.unwrap_or(0));

    let mut command = builder
        .reborrow()
        .init_command(execution.command.len() as u32);
    for (index, arg) in execution.command.iter().enumerate() {
        command.set(index as u32, arg);
    }

    let pre_stop = execution.pre_stop_command.as_deref().unwrap_or(&[]);
    let mut pre_stop_builder = builder
        .reborrow()
        .init_pre_stop_command(pre_stop.len() as u32);
    for (index, arg) in pre_stop.iter().enumerate() {
        pre_stop_builder.set(index as u32, arg);
    }

    let mut env = builder.reborrow().init_env(execution.env.len() as u32);
    write_env_vars(&mut env, &execution.env);

    let mut secret_files = builder
        .reborrow()
        .init_secret_files(execution.secret_files.len() as u32);
    write_secret_files(&mut secret_files, &execution.secret_files);

    let mut volumes = builder
        .reborrow()
        .init_volumes(execution.volumes.len() as u32);
    write_volume_mounts(&mut volumes, &execution.volumes);

    let mut networks = builder
        .reborrow()
        .init_networks(execution.networks.len() as u32);
    for (index, network_id) in execution.networks.iter().enumerate() {
        networks.set(index as u32, network_id.as_bytes());
    }

    let mut ports = builder.reborrow().init_ports(execution.ports.len() as u32);
    write_port_bindings(&mut ports, &execution.ports);

    if let Some(liveness) = execution.liveness.as_ref() {
        write_liveness_probe(builder.reborrow().init_liveness(), liveness);
    }

    Ok(())
}

/// Encodes one prepared jobs retry policy into the jobs wire builder.
fn write_job_retry_policy(
    mut builder: job_retry_policy::Builder<'_>,
    retry_policy: &PreparedJobRetryPolicy,
) {
    builder.set_max_retries(retry_policy.max_retries);
    builder.set_backoff_secs(retry_policy.backoff_secs);
}
