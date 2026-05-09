use crate::cli::{DaemonLogsArgs, DaemonShutdownArgs, DaemonStatusArgs, InitArgs};
use anyhow::{Context, Result, anyhow};
use mantissa_protocol::server::cluster_session;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use zeroize::Zeroizing;

#[cfg(unix)]
use std::os::fd::{FromRawFd, RawFd};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(not(unix))]
type RawFd = i32;

const PID_FILE_NAME: &str = "mantissa.pid";
const SOCKET_FILE_NAME: &str = "mantissa.sock";
const DEFAULT_LOG_DIR: &str = "logs";
const DEFAULT_LOG_FILE: &str = "mantissa.log";
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const FOLLOW_POLL_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(unix)]
const ROOT_DAEMON_DIR_MODE: u32 = 0o750;
#[cfg(unix)]
const USER_DAEMON_DIR_MODE: u32 = 0o700;
#[cfg(unix)]
const ROOT_DAEMON_FILE_MODE: u32 = 0o640;
#[cfg(unix)]
const USER_DAEMON_FILE_MODE: u32 = 0o600;

/// Options needed to spawn a detached daemon child with the same CLI context.
pub(crate) struct DetachedInitOptions<'a> {
    pub config: Option<&'a str>,
    pub listen: &'a str,
    pub name: Option<&'a str>,
    pub verbosity: u8,
    pub init: &'a InitArgs,
    pub prompted_passphrase: Option<Zeroizing<Vec<u8>>>,
}

/// Guard that removes foreground daemon metadata when this process exits cleanly.
pub(crate) struct ForegroundMetadataGuard {
    path: PathBuf,
    pid: u32,
}

impl Drop for ForegroundMetadataGuard {
    fn drop(&mut self) {
        if metadata_pid(&self.path) == Some(self.pid) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Starts `mantissa init` as a detached daemon child and waits for local readiness.
pub(crate) async fn start_detached(options: DetachedInitOptions<'_>) -> Result<()> {
    let state_dir = mantissa_net::paths::ensure_state_dir().context("prepare state directory")?;
    let log_path = options
        .init
        .log_file
        .clone()
        .unwrap_or_else(|| default_log_path(&state_dir));
    let preferred_socket = preferred_detached_socket(&state_dir, options.init);
    prepare_log_file(&log_path, &state_dir)?;
    ensure_no_recorded_daemon(&state_dir)?;
    ensure_no_reachable_daemon(preferred_socket.clone()).await?;

    let stdout = open_log_append(&log_path)?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("clone daemon log handle {}", log_path.display()))?;
    let passphrase_pipe = PassphrasePipe::maybe_new(
        options
            .prompted_passphrase
            .as_ref()
            .map(|passphrase| passphrase.as_slice()),
    )?;
    let mut command = detached_child_command(
        &options,
        stdout,
        stderr,
        passphrase_pipe.as_ref().map(PassphrasePipe::read_fd),
    )?;
    let mut child = command.spawn().context("spawn detached mantissa daemon")?;
    drop(passphrase_pipe);
    let pid = child.id();
    let mut metadata =
        DaemonMetadata::new(pid, state_dir.clone(), log_path.clone(), options.listen);
    write_metadata(&state_dir, &metadata)?;

    match wait_for_daemon_ready(&mut child, options.init.detach_timeout, preferred_socket).await {
        Ok(socket_path) => {
            metadata.socket_path = Some(socket_path.clone());
            write_metadata(&state_dir, &metadata)?;
            println!("Mantissa daemon started");
            println!("pid: {pid}");
            println!("socket: {}", socket_path.display());
            println!("logs: {}", log_path.display());
            Ok(())
        }
        Err(error) => {
            if process_state(metadata.pid) != ProcessState::Alive {
                remove_metadata_if_matches(&state_dir, metadata.pid);
            }
            Err(error.context(format!(
                "daemon did not become reachable; pid file: {}, logs: {}",
                metadata_path(&state_dir).display(),
                log_path.display()
            )))
        }
    }
}

/// Writes pid metadata for a foreground daemon and removes stale metadata first.
pub(crate) fn record_foreground_start(
    state_dir: &Path,
    listen: &str,
) -> Result<ForegroundMetadataGuard> {
    ensure_no_recorded_daemon(state_dir)?;
    let pid = std::process::id();
    let metadata = DaemonMetadata::new(
        pid,
        state_dir.to_path_buf(),
        default_log_path(state_dir),
        listen,
    );
    let path = write_metadata(state_dir, &metadata)?;
    Ok(ForegroundMetadataGuard { path, pid })
}

/// Prints status for the local daemon targeted by the provided arguments.
pub(crate) async fn status(args: &DaemonStatusArgs) -> Result<()> {
    let (state_dir, metadata) = lifecycle_metadata_target(args.state_dir.as_deref())?;
    let explicit_state_dir = args.state_dir.is_some();
    let preferred_socket = metadata
        .as_ref()
        .and_then(|value| value.socket_path.clone())
        .or_else(|| explicit_state_dir.then(|| default_socket_path(&state_dir)));
    let reachable = connect_reachable_socket(preferred_socket).await;
    let pid_state = metadata
        .as_ref()
        .map(|value| process_state(value.pid))
        .unwrap_or(ProcessState::Unknown);

    if let Some((socket_path, session)) = reachable {
        let health = health_snapshot(&session).await.ok();
        println!("Mantissa daemon: running");
        print_status_target(metadata.as_ref(), &state_dir);
        if metadata.is_some() {
            println!("process: {}", pid_state.as_str());
        } else {
            println!("process: unknown");
        }
        println!("socket: {}", socket_path.display());
        if let Some(health) = health {
            println!("health: {}", if health.ok { "ok" } else { "unhealthy" });
            println!("peers root: {}", health.root_digest);
            println!("daemon time: {}", health.now_unix_secs);
        }
        return Ok(());
    }

    match (metadata.as_ref(), pid_state) {
        (Some(metadata), ProcessState::Alive) => {
            println!("Mantissa daemon: starting or unhealthy");
            print_metadata_status(Some(metadata), &state_dir);
            println!("process: alive");
            println!("socket: unreachable");
        }
        (Some(metadata), _) => {
            println!("Mantissa daemon: stopped");
            print_metadata_status(Some(metadata), &state_dir);
            println!("process: not running");
            println!("socket: unreachable");
        }
        (None, _) => {
            println!("Mantissa daemon: stopped");
            println!("state dir: {}", state_dir.display());
            println!("pid file: {}", metadata_path(&state_dir).display());
        }
    }

    Ok(())
}

/// Sends a graceful shutdown signal to the local daemon and optionally forces exit.
pub(crate) async fn shutdown(args: &DaemonShutdownArgs) -> Result<()> {
    let state_dir = lifecycle_state_dir(args.state_dir.as_deref())?;
    let metadata = read_metadata(&state_dir).with_context(|| {
        format!(
            "no daemon pid file found at {}; start with `mantissa init` or `mantissa init --detach`",
            metadata_path(&state_dir).display()
        )
    })?;

    if process_state(metadata.pid) != ProcessState::Alive {
        remove_metadata_if_matches(&state_dir, metadata.pid);
        println!("Mantissa daemon is already stopped");
        return Ok(());
    }

    send_signal(metadata.pid, ShutdownSignal::Terminate)?;
    if wait_for_process_exit(metadata.pid, args.timeout).await {
        remove_metadata_if_matches(&state_dir, metadata.pid);
        println!("Mantissa daemon stopped");
        return Ok(());
    }

    if !args.force {
        return Err(anyhow!(
            "daemon pid {} did not stop within {:?}; retry with --force to send SIGKILL",
            metadata.pid,
            args.timeout
        ));
    }

    send_signal(metadata.pid, ShutdownSignal::Kill)?;
    if wait_for_process_exit(metadata.pid, args.timeout).await {
        remove_metadata_if_matches(&state_dir, metadata.pid);
        println!("Mantissa daemon killed");
        return Ok(());
    }

    Err(anyhow!(
        "daemon pid {} is still running after SIGKILL wait",
        metadata.pid
    ))
}

/// Prints daemon logs from the configured log file and optionally follows appends.
pub(crate) async fn logs(args: &DaemonLogsArgs) -> Result<()> {
    let (state_dir, metadata) = lifecycle_metadata_target(args.state_dir.as_deref())?;
    let log_path = args
        .file
        .clone()
        .or_else(|| metadata.map(|metadata| metadata.log_path))
        .or_else(|| readable_system_log_path(args.state_dir.as_deref(), &state_dir))
        .unwrap_or_else(|| default_log_path(&state_dir));
    let tail = LogTail::parse(&args.tail)?;
    let mut offset = print_log_tail(&log_path, tail)?;
    if args.follow {
        follow_log(&log_path, &mut offset).await?;
    }
    Ok(())
}

/// Returns the state directory used by lifecycle commands without creating it for read paths.
fn lifecycle_state_dir(override_dir: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = override_dir {
        return Ok(path.to_path_buf());
    }
    mantissa_net::paths::resolve_state_dir_path().context("resolve state directory")
}

/// Returns the best daemon metadata target for read-only lifecycle commands.
fn lifecycle_metadata_target(
    override_dir: Option<&Path>,
) -> Result<(PathBuf, Option<DaemonMetadata>)> {
    let state_dir = lifecycle_state_dir(override_dir)?;
    let metadata = read_metadata(&state_dir).ok();
    if override_dir.is_some() || metadata.is_some() {
        return Ok((state_dir, metadata));
    }

    let system_state_dir = system_state_dir();
    if system_state_dir != state_dir
        && let Ok(metadata) = read_metadata(&system_state_dir)
    {
        return Ok((system_state_dir, Some(metadata)));
    }

    Ok((state_dir, None))
}

/// Returns the system daemon log path when it is readable and no override was supplied.
fn readable_system_log_path(override_dir: Option<&Path>, state_dir: &Path) -> Option<PathBuf> {
    if override_dir.is_some() {
        return None;
    }

    let system_state_dir = system_state_dir();
    if system_state_dir == state_dir {
        return None;
    }

    let log_path = default_log_path(&system_state_dir);
    log_path.exists().then_some(log_path)
}

/// Builds the child command line for the foreground daemon process behind `--detach`.
fn detached_child_command(
    options: &DetachedInitOptions<'_>,
    stdout: File,
    stderr: File,
    prompted_passphrase_fd: Option<RawFd>,
) -> Result<Command> {
    let exe = std::env::current_exe().context("resolve current executable")?;
    let mut command = Command::new(exe);
    push_global_args(&mut command, options);
    push_init_args(&mut command, options.init, prompted_passphrase_fd);
    command.stdin(Stdio::null());
    command.stdout(Stdio::from(stdout));
    command.stderr(Stdio::from(stderr));

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // `pre_exec` runs in the child after fork and before exec; `setsid`
        // only touches process/session state and avoids allocations here.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    Ok(command)
}

/// Appends global CLI flags to the detached child command.
fn push_global_args(command: &mut Command, options: &DetachedInitOptions<'_>) {
    if let Some(config) = options.config {
        command.arg("--config").arg(config);
    }
    command.arg("--listen").arg(options.listen);
    if let Some(name) = options.name {
        command.arg("--name").arg(name);
    }
    if options.verbosity > 0 {
        command.arg(format!("-{}", "v".repeat(options.verbosity as usize)));
    }
}

/// Appends `init` flags to the detached child command without reusing `--detach`.
fn push_init_args(command: &mut Command, init: &InitArgs, prompted_passphrase_fd: Option<RawFd>) {
    command.arg("init");
    if init.debug {
        command.arg("-d");
    }
    command.arg("--daemon-child");
    if let Some(advertise) = &init.advertise {
        command.arg("--advertise").arg(advertise);
    }
    if init.reset_identity {
        command.arg("--reset-identity");
    }
    if let Some(state_dir) = &init.state_dir {
        command.arg("--state-dir").arg(state_dir);
    }
    if let Some(path) = &init.master_key_passphrase_file {
        command.arg("--master-key-passphrase-file").arg(path);
    }
    if let Some(fd) = prompted_passphrase_fd.or(init.master_key_passphrase_fd) {
        command
            .arg("--master-key-passphrase-fd")
            .arg(fd.to_string());
    }
}

/// Waits until the spawned daemon either answers the local socket or exits.
async fn wait_for_daemon_ready(
    child: &mut Child,
    timeout: Duration,
    preferred_socket: Option<PathBuf>,
) -> Result<PathBuf> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| anyhow!("detach timeout is too large"))?;

    loop {
        if let Some(status) = child.try_wait().context("check detached daemon status")? {
            return Err(anyhow!("detached daemon exited early with status {status}"));
        }
        if let Some((socket_path, _session)) =
            connect_reachable_socket(preferred_socket.clone()).await
        {
            return Ok(socket_path);
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out after {:?} waiting for daemon readiness",
                timeout
            ));
        }
        sleep(READY_POLL_INTERVAL).await;
    }
}

/// Returns a reachable local session and the socket path that accepted it.
async fn connect_reachable_socket(
    preferred_socket: Option<PathBuf>,
) -> Option<(PathBuf, cluster_session::Client)> {
    for path in candidate_socket_paths(preferred_socket) {
        let Ok(session) = mantissa_client::connection::get_client_unix_path(path.clone()).await
        else {
            continue;
        };
        if ping_session(&session).await.is_ok() {
            return Some((path, session));
        }
    }
    None
}

/// Fails when any local daemon socket is already reachable before startup.
async fn ensure_no_reachable_daemon(preferred_socket: Option<PathBuf>) -> Result<()> {
    if let Some((socket_path, _session)) = connect_reachable_socket(preferred_socket).await {
        return Err(anyhow!(
            "Mantissa daemon is already reachable at {}",
            socket_path.display()
        ));
    }
    Ok(())
}

/// Pings one local cluster session to confirm it is attached to a live daemon.
async fn ping_session(session: &cluster_session::Client) -> Result<()> {
    session.ping_request().send().promise.await?;
    Ok(())
}

/// Reads health details through the local session capabilities.
async fn health_snapshot(session: &cluster_session::Client) -> Result<HealthSnapshot> {
    let capabilities_response = session.get_capabilities_request().send().promise.await?;
    let capabilities_reader = capabilities_response.get()?;
    let capabilities = capabilities_reader.get_caps()?;
    let health = capabilities.get_health()?;
    let response = health.ping_request().send().promise.await?;
    let reader = response.get()?;
    Ok(HealthSnapshot {
        ok: reader.get_ok(),
        now_unix_secs: reader.get_now(),
        root_digest: bytes_to_hex(reader.get_root_digest()?),
    })
}

/// Builds a deduplicated socket candidate list with an optional first choice.
fn candidate_socket_paths(preferred_socket: Option<PathBuf>) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(path) = preferred_socket {
        paths.push(path);
    }
    for path in mantissa_net::unix_socket::candidate_unix_socket_paths() {
        if !paths.iter().any(|existing| existing == &path) {
            paths.push(path);
        }
    }
    paths
}

/// Returns the expected detached daemon socket when startup targets a custom state directory.
fn preferred_detached_socket(state_dir: &Path, init: &InitArgs) -> Option<PathBuf> {
    if init.state_dir.is_some()
        || mantissa_net::paths::state_dir_override().is_some()
        || std::env::var_os(mantissa_net::paths::STATE_DIR_ENV).is_some()
    {
        return Some(default_socket_path(state_dir));
    }
    None
}

/// Ensures no live process is recorded in the pid file for this state directory.
fn ensure_no_recorded_daemon(state_dir: &Path) -> Result<()> {
    let Ok(metadata) = read_metadata(state_dir) else {
        return Ok(());
    };
    if process_state(metadata.pid) == ProcessState::Alive {
        return Err(anyhow!(
            "Mantissa daemon is already running with pid {}",
            metadata.pid
        ));
    }
    remove_metadata_if_matches(state_dir, metadata.pid);
    Ok(())
}

/// Prints common metadata fields for daemon status output.
fn print_metadata_status(metadata: Option<&DaemonMetadata>, state_dir: &Path) {
    println!("state dir: {}", state_dir.display());
    println!("pid file: {}", metadata_path(state_dir).display());
    if let Some(metadata) = metadata {
        println!("pid: {}", metadata.pid);
        println!("listen: {}", metadata.listen_addr);
        println!("logs: {}", metadata.log_path.display());
        println!("started: {}", metadata.started_at_unix_secs);
    }
}

/// Prints daemon target fields even when no pid metadata is readable.
fn print_status_target(metadata: Option<&DaemonMetadata>, state_dir: &Path) {
    if metadata.is_some() {
        print_metadata_status(metadata, state_dir);
    } else {
        println!("state dir: {}", state_dir.display());
        println!("pid file: {}", metadata_path(state_dir).display());
    }
}

/// Returns the pid currently stored in one metadata file.
fn metadata_pid(path: &Path) -> Option<u32> {
    let raw = fs::read_to_string(path).ok()?;
    DaemonMetadata::decode(&raw)
        .ok()
        .map(|metadata| metadata.pid)
}

/// Reads daemon metadata from the state directory pid file.
fn read_metadata(state_dir: &Path) -> Result<DaemonMetadata> {
    let path = metadata_path(state_dir);
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("read daemon metadata {}", path.display()))?;
    DaemonMetadata::decode(&raw)
}

/// Writes daemon metadata atomically enough for local lifecycle commands.
fn write_metadata(state_dir: &Path, metadata: &DaemonMetadata) -> Result<PathBuf> {
    fs::create_dir_all(state_dir)
        .with_context(|| format!("create state directory {}", state_dir.display()))?;
    secure_daemon_dir(state_dir)?;
    let path = metadata_path(state_dir);
    let tmp = path.with_extension("pid.tmp");
    fs::write(&tmp, metadata.encode())
        .with_context(|| format!("write daemon metadata {}", tmp.display()))?;
    secure_daemon_file(&tmp)?;
    fs::rename(&tmp, &path).with_context(|| {
        format!(
            "replace daemon metadata {} with {}",
            path.display(),
            tmp.display()
        )
    })?;
    secure_daemon_file(&path)?;
    Ok(path)
}

/// Removes daemon metadata only when it still refers to the expected pid.
fn remove_metadata_if_matches(state_dir: &Path, pid: u32) {
    let path = metadata_path(state_dir);
    if metadata_pid(&path) == Some(pid) {
        let _ = fs::remove_file(path);
    }
}

/// Returns the path to the daemon metadata file for one state directory.
fn metadata_path(state_dir: &Path) -> PathBuf {
    state_dir.join(PID_FILE_NAME)
}

/// Returns the local admin socket path inside one state directory.
fn default_socket_path(state_dir: &Path) -> PathBuf {
    state_dir.join(SOCKET_FILE_NAME)
}

/// Returns the root daemon's persistent state directory.
fn system_state_dir() -> PathBuf {
    PathBuf::from(mantissa_net::paths::SYSTEM_STATE_DIR)
}

/// Returns the default daemon log path for one state directory.
fn default_log_path(state_dir: &Path) -> PathBuf {
    state_dir.join(DEFAULT_LOG_DIR).join(DEFAULT_LOG_FILE)
}

/// Creates the log parent directory and initial log file with daemon permissions.
fn prepare_log_file(path: &Path, state_dir: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create daemon log directory {}", parent.display()))?;
        secure_managed_log_dirs(parent, state_dir)?;
    }
    let _file = open_log_append(path)?;
    Ok(())
}

/// Opens a daemon log file for append.
fn open_log_append(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open daemon log {}", path.display()))?;
    secure_daemon_file(path)?;
    Ok(file)
}

/// Tightens one Mantissa-managed directory for the daemon owner model.
fn secure_daemon_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let mode = if mantissa_net::paths::running_as_root() {
            ROOT_DAEMON_DIR_MODE
        } else {
            USER_DAEMON_DIR_MODE
        };
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
            .with_context(|| format!("set daemon directory permissions on {}", path.display()))?;
        if mantissa_net::paths::running_as_root() {
            mantissa_net::paths::ensure_mantissa_group(path);
        }
    }
    Ok(())
}

/// Tightens one Mantissa-managed file for the daemon owner model.
fn secure_daemon_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let mode = if mantissa_net::paths::running_as_root() {
            ROOT_DAEMON_FILE_MODE
        } else {
            USER_DAEMON_FILE_MODE
        };
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
            .with_context(|| format!("set daemon file permissions on {}", path.display()))?;
        if mantissa_net::paths::running_as_root() {
            mantissa_net::paths::ensure_mantissa_group(path);
        }
    }
    Ok(())
}

/// Tightens log directories that live below the managed state directory.
fn secure_managed_log_dirs(parent: &Path, state_dir: &Path) -> Result<()> {
    if !parent.starts_with(state_dir) {
        return Ok(());
    }

    let mut dirs = Vec::new();
    let mut current = Some(parent);
    while let Some(dir) = current {
        if dir == state_dir {
            break;
        }
        dirs.push(dir.to_path_buf());
        current = dir.parent();
    }

    for dir in dirs.iter().rev() {
        secure_daemon_dir(dir)?;
    }
    Ok(())
}

/// Prints the selected tail of a daemon log and returns the byte offset at EOF.
fn print_log_tail(path: &Path, tail: LogTail) -> Result<u64> {
    let file = File::open(path).with_context(|| format!("open daemon log {}", path.display()))?;
    let mut reader = BufReader::new(file);
    match tail {
        LogTail::All => {
            let mut output = String::new();
            reader
                .read_to_string(&mut output)
                .with_context(|| format!("read daemon log {}", path.display()))?;
            print!("{output}");
        }
        LogTail::Lines(limit) => {
            let mut lines = VecDeque::with_capacity(limit.min(1024));
            for line in reader.by_ref().lines() {
                let line = line.with_context(|| format!("read daemon log {}", path.display()))?;
                if limit > 0 && lines.len() == limit {
                    lines.pop_front();
                }
                if limit > 0 {
                    lines.push_back(line);
                }
            }
            for line in lines {
                println!("{line}");
            }
        }
    }
    std::io::stdout().flush().ok();
    Ok(fs::metadata(path)
        .with_context(|| format!("inspect daemon log {}", path.display()))?
        .len())
}

/// Streams appended daemon log bytes until the user interrupts the command.
async fn follow_log(path: &Path, offset: &mut u64) -> Result<()> {
    loop {
        let metadata =
            fs::metadata(path).with_context(|| format!("inspect daemon log {}", path.display()))?;
        if metadata.len() < *offset {
            *offset = 0;
        }
        if metadata.len() > *offset {
            let mut file =
                File::open(path).with_context(|| format!("open daemon log {}", path.display()))?;
            file.seek(SeekFrom::Start(*offset))
                .with_context(|| format!("seek daemon log {}", path.display()))?;
            let mut output = String::new();
            file.read_to_string(&mut output)
                .with_context(|| format!("read daemon log {}", path.display()))?;
            print!("{output}");
            std::io::stdout().flush().ok();
            *offset = metadata.len();
        }
        sleep(FOLLOW_POLL_INTERVAL).await;
    }
}

/// Waits for a process id to disappear before the timeout elapses.
async fn wait_for_process_exit(pid: u32, timeout: Duration) -> bool {
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return false;
    };
    loop {
        if process_state(pid) != ProcessState::Alive {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(READY_POLL_INTERVAL).await;
    }
}

/// Returns whether a pid appears to belong to a live process.
fn process_state(pid: u32) -> ProcessState {
    if pid == 0 {
        return ProcessState::Dead;
    }

    #[cfg(unix)]
    {
        let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
        if result == 0 {
            return ProcessState::Alive;
        }
        match std::io::Error::last_os_error().raw_os_error() {
            Some(libc::EPERM) => ProcessState::Alive,
            Some(libc::ESRCH) => ProcessState::Dead,
            _ => ProcessState::Unknown,
        }
    }

    #[cfg(not(unix))]
    {
        ProcessState::Unknown
    }
}

/// Sends one shutdown signal to a daemon pid.
fn send_signal(pid: u32, signal: ShutdownSignal) -> Result<()> {
    #[cfg(unix)]
    {
        let raw_signal = match signal {
            ShutdownSignal::Terminate => libc::SIGTERM,
            ShutdownSignal::Kill => libc::SIGKILL,
        };
        let result = unsafe { libc::kill(pid as libc::pid_t, raw_signal) };
        if result == 0 {
            Ok(())
        } else {
            Err(anyhow!(
                "failed to signal daemon pid {}: {}",
                pid,
                std::io::Error::last_os_error()
            ))
        }
    }

    #[cfg(not(unix))]
    {
        let _ = (pid, signal);
        Err(anyhow!("daemon shutdown is only supported on Unix hosts"))
    }
}

/// Converts bytes to lower-case hexadecimal for operator-facing status output.
fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Captures persisted daemon metadata used by status, logs, and shutdown.
struct DaemonMetadata {
    pid: u32,
    state_dir: PathBuf,
    log_path: PathBuf,
    listen_addr: String,
    started_at_unix_secs: u64,
    socket_path: Option<PathBuf>,
}

impl DaemonMetadata {
    /// Builds daemon metadata for a newly started process.
    fn new(pid: u32, state_dir: PathBuf, log_path: PathBuf, listen_addr: &str) -> Self {
        Self {
            pid,
            state_dir,
            log_path,
            listen_addr: listen_addr.to_string(),
            started_at_unix_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or(0),
            socket_path: None,
        }
    }

    /// Encodes metadata as simple key-value lines for shell-friendly inspection.
    fn encode(&self) -> String {
        let socket_path = self
            .socket_path
            .as_ref()
            .map(|path| path_to_string(path))
            .unwrap_or_default();
        format!(
            "pid={}\nstate_dir={}\nlog_path={}\nlisten={}\nstarted_at_unix_secs={}\nsocket_path={}\n",
            self.pid,
            path_to_string(&self.state_dir),
            path_to_string(&self.log_path),
            self.listen_addr,
            self.started_at_unix_secs,
            socket_path
        )
    }

    /// Decodes daemon metadata written by `encode`.
    fn decode(raw: &str) -> Result<Self> {
        let mut fields = HashMap::new();
        for line in raw.lines() {
            if let Some((key, value)) = line.split_once('=') {
                fields.insert(key, value);
            }
        }

        let pid = required_field(&fields, "pid")?
            .parse::<u32>()
            .context("parse daemon pid")?;
        let state_dir = PathBuf::from(required_field(&fields, "state_dir")?);
        let log_path = PathBuf::from(required_field(&fields, "log_path")?);
        let listen_addr = required_field(&fields, "listen")?.to_string();
        let started_at_unix_secs = required_field(&fields, "started_at_unix_secs")?
            .parse::<u64>()
            .context("parse daemon start time")?;
        let socket_path = fields
            .get("socket_path")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);

        Ok(Self {
            pid,
            state_dir,
            log_path,
            listen_addr,
            started_at_unix_secs,
            socket_path,
        })
    }
}

/// Returns a required daemon metadata field or a descriptive error.
fn required_field<'a>(fields: &'a HashMap<&str, &str>, key: &str) -> Result<&'a str> {
    fields
        .get(key)
        .copied()
        .ok_or_else(|| anyhow!("daemon metadata is missing `{key}`"))
}

/// Converts a path into the string form stored in daemon metadata.
fn path_to_string(path: &Path) -> String {
    path.as_os_str().to_string_lossy().into_owned()
}

/// Snapshot returned by the daemon health capability.
struct HealthSnapshot {
    ok: bool,
    now_unix_secs: u64,
    root_digest: String,
}

/// Parsed log tail mode for daemon log output.
#[derive(Clone, Copy)]
enum LogTail {
    All,
    Lines(usize),
}

impl LogTail {
    /// Parses `all` or a non-negative line count for local daemon logs.
    fn parse(raw: &str) -> Result<Self> {
        if raw.eq_ignore_ascii_case("all") {
            return Ok(Self::All);
        }
        let value = raw
            .parse::<usize>()
            .with_context(|| format!("parse log tail value `{raw}`"))?;
        Ok(Self::Lines(value))
    }
}

/// Process state derived from a pid probe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProcessState {
    Alive,
    Dead,
    Unknown,
}

impl ProcessState {
    /// Renders the state for concise daemon status output.
    fn as_str(self) -> &'static str {
        match self {
            Self::Alive => "alive",
            Self::Dead => "not running",
            Self::Unknown => "unknown",
        }
    }
}

/// Signals used by the root `shutdown` command.
#[derive(Clone, Copy)]
enum ShutdownSignal {
    Terminate,
    Kill,
}

/// Read side of a one-shot pipe used to hand prompted passphrase bytes to the child.
#[cfg(unix)]
struct PassphrasePipe {
    read_fd: RawFd,
}

#[cfg(unix)]
impl PassphrasePipe {
    /// Builds a pipe containing prompted passphrase bytes for the detached child.
    fn maybe_new(passphrase: Option<&[u8]>) -> Result<Option<Self>> {
        passphrase.map(Self::new).transpose()
    }

    /// Writes the passphrase into a pipe whose read fd is inherited across exec.
    fn new(passphrase: &[u8]) -> Result<Self> {
        let mut fds = [0; 2];
        let result = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if result != 0 {
            return Err(anyhow!(
                "failed to create passphrase pipe: {}",
                std::io::Error::last_os_error()
            ));
        }

        let read_fd = fds[0];
        let write_fd = fds[1];
        let mut writer = unsafe { File::from_raw_fd(write_fd) };
        if let Err(error) = writer.write_all(passphrase) {
            drop(writer);
            let _ = unsafe { libc::close(read_fd) };
            return Err(anyhow!("failed to write passphrase pipe: {error}"));
        }
        drop(writer);

        Ok(Self { read_fd })
    }

    /// Returns the inherited read fd passed to `--master-key-passphrase-fd`.
    fn read_fd(&self) -> RawFd {
        self.read_fd
    }
}

#[cfg(unix)]
impl Drop for PassphrasePipe {
    fn drop(&mut self) {
        let _ = unsafe { libc::close(self.read_fd) };
    }
}

/// Stub pipe helper for platforms where fd inheritance is unavailable.
#[cfg(not(unix))]
struct PassphrasePipe;

#[cfg(not(unix))]
impl PassphrasePipe {
    /// Rejects prompted detached startup on unsupported platforms.
    fn maybe_new(passphrase: Option<&[u8]>) -> Result<Option<Self>> {
        if passphrase.is_some() {
            return Err(anyhow!(
                "interactive `mantissa init --detach` passphrase handoff is only supported on Unix hosts"
            ));
        }
        Ok(None)
    }

    /// Stub read fd accessor for cross-platform type checking.
    fn read_fd(&self) -> i32 {
        -1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    /// Creates one unique daemon test directory below the system temp directory.
    fn test_state_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock before epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "mantissa-daemon-perms-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create daemon test directory");
        dir
    }

    #[cfg(unix)]
    /// Returns the permission bits currently set on a filesystem path.
    fn mode(path: &Path) -> u32 {
        fs::metadata(path)
            .expect("read metadata")
            .permissions()
            .mode()
            & 0o777
    }

    #[cfg(unix)]
    /// Returns the expected daemon directory mode for the current test uid.
    fn expected_dir_mode() -> u32 {
        if mantissa_net::paths::running_as_root() {
            ROOT_DAEMON_DIR_MODE
        } else {
            USER_DAEMON_DIR_MODE
        }
    }

    #[cfg(unix)]
    /// Returns the expected daemon file mode for the current test uid.
    fn expected_file_mode() -> u32 {
        if mantissa_net::paths::running_as_root() {
            ROOT_DAEMON_FILE_MODE
        } else {
            USER_DAEMON_FILE_MODE
        }
    }

    #[cfg(unix)]
    #[test]
    fn prepare_log_file_tightens_managed_log_permissions() {
        let state_dir = test_state_dir();
        let log_dir = state_dir.join("logs");
        let log_path = log_dir.join("mantissa.log");
        fs::create_dir_all(&log_dir).expect("create log directory");
        fs::set_permissions(&log_dir, fs::Permissions::from_mode(0o777))
            .expect("loosen log directory");
        fs::write(&log_path, "existing log\n").expect("write log file");
        fs::set_permissions(&log_path, fs::Permissions::from_mode(0o666)).expect("loosen log file");

        prepare_log_file(&log_path, &state_dir).expect("prepare log file");

        assert_eq!(mode(&log_dir), expected_dir_mode());
        assert_eq!(mode(&log_path), expected_file_mode());
        fs::remove_dir_all(state_dir).expect("remove daemon test directory");
    }

    #[cfg(unix)]
    #[test]
    fn write_metadata_tightens_state_and_pid_permissions() {
        let state_dir = test_state_dir();
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o777))
            .expect("loosen state directory");
        let metadata = DaemonMetadata::new(
            42,
            state_dir.clone(),
            state_dir.join("logs").join("mantissa.log"),
            "127.0.0.1:0",
        );

        let pid_path = write_metadata(&state_dir, &metadata).expect("write daemon metadata");

        assert_eq!(mode(&state_dir), expected_dir_mode());
        assert_eq!(mode(&pid_path), expected_file_mode());
        fs::remove_dir_all(state_dir).expect("remove daemon test directory");
    }

    #[cfg(unix)]
    #[test]
    fn passphrase_pipe_round_trips_bytes() {
        let pipe = PassphrasePipe::new(b"correct horse battery staple").unwrap();
        let duplicated = unsafe { libc::dup(pipe.read_fd()) };
        assert!(duplicated >= 0);

        let mut reader = unsafe { File::from_raw_fd(duplicated) };
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();

        assert_eq!(bytes, b"correct horse battery staple");
    }
}
