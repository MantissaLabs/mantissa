//! Live fixed-file data-path proof through ublk, dm-linear, and ext4.

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mantissa_net::noise::{
    NoiseKeys, NoisePeerVerifier, client_handshake_peer, read_framed_len,
    server_handshake_peer_identified_with_first_frame,
};
use mantissa_raft::ApplyContext;
use mantissa_volume::control_state::{
    ExpectedVolumeRevision, GrantVolumeWriter, InitializeVolume, VolumeCommand, VolumeControlState,
    WriterGrant,
};
use mantissa_volume::driver::{
    MappedVolumeLayout, MappedVolumeSystem, UblkDevice, UblkOwnerId, UblkQueueSettings,
    UblkSettings, UblkSystem,
};
use mantissa_volume::storage::replica_file::connection::{
    ReplicaDataConnection, ReplicaDataServer, ReplicaDataServerSettings,
};
use mantissa_volume::storage::replica_file::data_path::{
    FixedReplicaCopy, FixedReplicaPath, FixedReplicaPathSettings,
};
use mantissa_volume::storage::replica_file::io_admission::{
    AppliedVolumeStateRegistry, FenceAdmission,
};
use mantissa_volume::storage::replica_file::wire::{
    ReplicaDataConnectionOpen, ReplicaDataConnectionPurpose, ReplicaDataLimits,
};
use mantissa_volume::storage::replica_file::{
    ReplicaFile, ReplicaFileSettings, ReplicaFileWorkerPool,
};
use mantissa_volume::{
    DriverSessionId, FenceEpoch, VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId,
    VolumeNodeId,
};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

const MIB: u64 = 1024 * 1024;
const CAPACITY_BYTES: u64 = 1024 * MIB;
const REQUEST_BYTES: u32 = 128 * 1024;
const TEST_OWNER: UblkOwnerId = UblkOwnerId::new(u64::from_be_bytes(*b"MNTS-FIX"));
const CLIENT_KEY_BYTE: u8 = 71;
const SECOND_KEY_BYTE: u8 = 72;
const THIRD_KEY_BYTE: u8 = 73;

/// Returns one stable node identity for the live data-path permission.
fn node(value: u128) -> VolumeNodeId {
    VolumeNodeId::new(Uuid::from_u128(value)).expect("test volume node ID must be valid")
}

/// Returns one stable session for the standalone fixed-file writer.
fn driver_session() -> DriverSessionId {
    DriverSessionId::new(Uuid::from_u128(501)).expect("fixed driver session must be valid")
}

/// Publishes the writer grant used by one standalone receiving copy.
fn data_authorization() -> (ReplicaDataConnectionOpen, Arc<FenceAdmission>) {
    let initialized = VolumeControlState::default()
        .evaluate(&VolumeCommand::Initialize(InitializeVolume {
            descriptor: descriptor(),
            initial_copies: [node(1), node(2), node(3)].into_iter().collect(),
        }))
        .state;
    let writer = WriterGrant {
        node_id: node(1),
        session_id: driver_session(),
    };
    let attached = initialized
        .evaluate(&VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: ExpectedVolumeRevision {
                generation: descriptor().generation(),
                revision: initialized.revision(),
            },
            writer,
        }))
        .state;
    let registry = AppliedVolumeStateRegistry::new();
    let cell = registry
        .publish(ApplyContext::new(1, attached.revision()), &attached)
        .expect("fixed writer grant must publish")
        .expect("fixed writer grant must create a cell");
    let admission = FenceAdmission::new(cell, node(2));
    admission
        .enable_for(attached.revision())
        .expect("fixed receiving copy must enable");
    let open = ReplicaDataConnectionOpen::new(
        descriptor(),
        attached
            .data()
            .expect("fixed writer grant must have data")
            .fence,
        writer.session_id,
        ReplicaDataConnectionPurpose::Data,
    );
    (open, admission)
}

/// Static key accepted by one standalone three-VM benchmark server.
struct ExpectedPeer {
    public_key: [u8; 32],
}

#[async_trait(?Send)]
impl NoisePeerVerifier for ExpectedPeer {
    /// Accepts only the fixed client key used by this isolated benchmark.
    async fn is_allowed(&self, remote_static: &[u8]) -> io::Result<bool> {
        Ok(remote_static == self.public_key)
    }
}

/// Removes one live mount even when an assertion stops the test early.
struct MountCleanup {
    path: PathBuf,
    mounted: bool,
}

/// Removes the published mapping before the private ublk device on test failure.
struct MappedCleanup {
    mapped_volumes: MappedVolumeSystem,
    node_id: VolumeNodeId,
    key: mantissa_volume::catalog::ReplicaKey,
    mapped: bool,
}

/// Removes one temporary PostgreSQL container after success or failure.
struct DockerCleanup {
    name: String,
}

/// Process and replica-file counters captured around one measured workload.
struct ResourceSnapshot {
    cpu: Duration,
    write_bytes: u64,
    write_calls: u64,
    allocated_bytes: u64,
}

/// Resource use added by one measured workload.
struct ResourceUse {
    cpu: Duration,
    write_bytes: u64,
    write_calls: u64,
    allocated_bytes: u64,
    maximum_rss_kib: i64,
}

/// Sends small requests through Tokio while the storage workload is active.
struct ControlProbe {
    stop: Arc<AtomicBool>,
    client: Option<std::thread::JoinHandle<Result<Duration, String>>>,
    server: tokio::task::JoinHandle<()>,
}

impl ControlProbe {
    /// Starts one bounded request loop outside the measured Tokio executor.
    fn start() -> Self {
        let (requests, mut receiver) =
            tokio::sync::mpsc::channel::<std::sync::mpsc::SyncSender<()>>(1);
        let server = tokio::spawn(async move {
            while let Some(reply) = receiver.recv().await {
                let _ = reply.send(());
            }
        });
        let stop = Arc::new(AtomicBool::new(false));
        let client_stop = Arc::clone(&stop);
        let client = std::thread::spawn(move || {
            let mut maximum = Duration::ZERO;
            while !client_stop.load(Ordering::Acquire) {
                let started = Instant::now();
                let (reply, result) = std::sync::mpsc::sync_channel(1);
                requests
                    .blocking_send(reply)
                    .map_err(|_| "control probe server stopped".to_string())?;
                result
                    .recv_timeout(Duration::from_secs(1))
                    .map_err(|_| "control probe timed out after 1s".to_string())?;
                maximum = maximum.max(started.elapsed());
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(maximum)
        });
        Self {
            stop,
            client: Some(client),
            server,
        }
    }

    /// Stops the request loop and returns its longest measured response time.
    async fn stop(mut self) -> Result<Duration, Box<dyn Error>> {
        self.stop.store(true, Ordering::Release);
        let client = self
            .client
            .take()
            .ok_or("control probe client is missing")?;
        let maximum =
            tokio::time::timeout(Duration::from_secs(2), smol::unblock(move || client.join()))
                .await
                .map_err(|_| "control probe client did not stop within 2s")?
                .map_err(|_| "control probe client panicked")?
                .map_err(|error| format!("control probe failed: {error}"))?;
        tokio::time::timeout(Duration::from_secs(2), self.server)
            .await
            .map_err(|_| "control probe server did not stop within 2s")?
            .map_err(|error| format!("control probe server failed: {error}"))?;
        Ok(maximum)
    }
}

impl ResourceSnapshot {
    /// Captures counters after setup work and before the measured operation.
    fn start(files: &[Arc<ReplicaFile>]) -> Self {
        let (write_bytes, write_calls) = process_io();
        Self {
            cpu: process_cpu_time(),
            write_bytes,
            write_calls,
            allocated_bytes: allocated_bytes(files),
        }
    }

    /// Returns counters added since this snapshot was captured.
    fn finish(self, files: &[Arc<ReplicaFile>]) -> ResourceUse {
        let (write_bytes, write_calls) = process_io();
        ResourceUse {
            cpu: process_cpu_time().saturating_sub(self.cpu),
            write_bytes: write_bytes.saturating_sub(self.write_bytes),
            write_calls: write_calls.saturating_sub(self.write_calls),
            allocated_bytes: allocated_bytes(files).saturating_sub(self.allocated_bytes),
            maximum_rss_kib: maximum_rss_kib(),
        }
    }
}

/// Returns user plus kernel CPU time consumed by this test process.
fn process_cpu_time() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided value when it succeeds.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(status, 0, "getrusage must work in the live benchmark");
    // SAFETY: the successful getrusage call above initialized every field.
    let usage = unsafe { usage.assume_init() };
    timeval_duration(usage.ru_utime) + timeval_duration(usage.ru_stime)
}

/// Converts one non-negative process CPU counter into a duration.
fn timeval_duration(time: libc::timeval) -> Duration {
    Duration::from_secs(u64::try_from(time.tv_sec).expect("CPU seconds must be positive"))
        + Duration::from_micros(
            u64::try_from(time.tv_usec).expect("CPU microseconds must be positive"),
        )
}

/// Returns physical write bytes and write syscall count for this process.
fn process_io() -> (u64, u64) {
    let contents = std::fs::read_to_string("/proc/self/io")
        .expect("Linux process I/O counters must be readable");
    let counter = |name: &str| {
        contents
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .expect("required Linux process I/O counter must exist")
            .trim()
            .parse::<u64>()
            .expect("Linux process I/O counter must be an integer")
    };
    (counter("write_bytes:"), counter("syscw:"))
}

/// Returns the largest resident memory size observed by this process.
fn maximum_rss_kib() -> i64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided value when it succeeds.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(status, 0, "getrusage must work in the live benchmark");
    // SAFETY: the successful getrusage call above initialized every field.
    unsafe { usage.assume_init() }.ru_maxrss
}

/// Returns disk blocks allocated by the data and changed-region files.
fn allocated_bytes(files: &[Arc<ReplicaFile>]) -> u64 {
    files
        .iter()
        .flat_map(|file| {
            std::fs::read_dir(file.directory()).expect("replica directory must be readable")
        })
        .map(|entry| {
            entry
                .expect("replica file entry must be readable")
                .metadata()
                .expect("replica file metadata must be readable")
                .blocks()
                .checked_mul(512)
                .expect("allocated byte count must fit")
        })
        .sum()
}

impl DockerCleanup {
    /// Records the exact temporary container name selected by this test.
    fn new(name: String) -> Self {
        Self { name }
    }
}

impl Drop for DockerCleanup {
    /// Forces removal because a failed benchmark must not leak a container.
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

impl MountCleanup {
    /// Starts with an unmounted directory owned by the caller.
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            mounted: false,
        }
    }

    /// Records that the kernel mount must be removed during cleanup.
    fn mounted(&mut self) {
        self.mounted = true;
    }

    /// Unmounts now and disables the fallback drop cleanup.
    fn unmount(&mut self) -> Result<(), Box<dyn Error>> {
        if self.mounted {
            run(
                Command::new("umount").arg(&self.path),
                "unmount fixed replica test",
            )?;
            self.mounted = false;
        }
        Ok(())
    }
}

impl Drop for MountCleanup {
    /// Makes a best effort to release the filesystem after a failed test.
    fn drop(&mut self) {
        if self.mounted {
            let _ = Command::new("umount").arg(&self.path).status();
        }
    }
}

impl MappedCleanup {
    /// Takes cleanup ownership of one newly created mapping.
    fn new(
        mapped_volumes: MappedVolumeSystem,
        node_id: VolumeNodeId,
        key: mantissa_volume::catalog::ReplicaKey,
    ) -> Self {
        Self {
            mapped_volumes,
            node_id,
            key,
            mapped: true,
        }
    }

    /// Records that the test removed the mapping in the required order.
    fn removed(&mut self) {
        self.mapped = false;
    }

    /// Takes cleanup ownership again after restoring or replacing its ublk device.
    fn mapped(&mut self) {
        self.mapped = true;
    }
}

impl Drop for MappedCleanup {
    /// Makes a best effort to remove a mapping left by a failed test.
    fn drop(&mut self) {
        if self.mapped {
            let _ = self.mapped_volumes.remove(self.node_id, self.key);
        }
    }
}

/// Creates one checked 1 GiB descriptor for the live proof.
fn descriptor() -> VolumeDescriptor {
    VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(0x018f_89ad_6bc8_7b3d_a8ef_50b1_3cda_14f1))
            .expect("fixed test volume ID must be valid"),
        VolumeGeneration::new(1).expect("fixed test generation must be valid"),
        CAPACITY_BYTES,
        VolumeBlockSizes::supported(),
    )
    .expect("fixed live descriptor must be valid")
}

/// Returns the fixed-file limits used by this one live path.
fn file_settings() -> ReplicaFileSettings {
    ReplicaFileSettings::new(changed_region_bytes(), 32, REQUEST_BYTES as usize, 1024)
        .expect("fixed live file settings must be valid")
}

/// Returns the repair-region size selected for this benchmark run.
fn changed_region_bytes() -> u64 {
    std::env::var("MANTISSA_FIXED_CHANGED_REGION_MIB")
        .map(|value| {
            value
                .parse::<u64>()
                .expect("MANTISSA_FIXED_CHANGED_REGION_MIB must be an integer")
                .checked_mul(MIB)
                .expect("changed-region byte count must fit")
        })
        .unwrap_or(64 * MIB)
}

/// Returns one local copy or the normal local-plus-two-remote layout.
fn copy_count() -> usize {
    let count = std::env::var("MANTISSA_FIXED_COPY_COUNT")
        .map(|value| {
            value
                .parse::<usize>()
                .expect("MANTISSA_FIXED_COPY_COUNT must be an integer")
        })
        .unwrap_or(3);
    assert!(
        count == 1 || count == 3,
        "MANTISSA_FIXED_COPY_COUNT must be 1 or 3"
    );
    count
}

/// Returns two explicitly configured physical benchmark peers, when present.
fn remote_addresses() -> Option<[String; 2]> {
    let value = std::env::var("MANTISSA_FIXED_REMOTE_ADDRESSES").ok()?;
    let addresses = value
        .split(',')
        .map(str::trim)
        .filter(|address| !address.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        addresses.len(),
        2,
        "MANTISSA_FIXED_REMOTE_ADDRESSES must contain two comma-separated addresses"
    );
    Some([addresses[0].clone(), addresses[1].clone()])
}

/// Opens authenticated streams to the two standalone physical copy servers.
async fn connect_remote_copies(
    addresses: [String; 2],
    limits: ReplicaDataLimits,
) -> Result<Vec<Arc<ReplicaDataConnection>>, Box<dyn Error>> {
    let client_keys = NoiseKeys::from_private_bytes([CLIENT_KEY_BYTE; 32]);
    let mut connections = Vec::with_capacity(2);
    for (address, key_byte) in addresses.into_iter().zip([SECOND_KEY_BYTE, THIRD_KEY_BYTE]) {
        let tcp = TcpStream::connect(&address).await?;
        let server_keys = NoiseKeys::from_private_bytes([key_byte; 32]);
        let stream = client_handshake_peer(tcp, &client_keys, &server_keys.public_bytes()).await?;
        connections.push(Arc::new(ReplicaDataConnection::start(stream, limits, 64)?));
    }
    Ok(connections)
}

/// Returns bounded cache, file-worker, and deadline values for the live path.
fn path_settings() -> FixedReplicaPathSettings {
    FixedReplicaPathSettings::new(
        file_settings(),
        256,
        64 * MIB as usize,
        64,
        Duration::from_secs(30),
    )
    .expect("fixed live path settings must be valid")
    .with_combine_delay(Duration::from_micros(250))
}

/// Returns the explicitly selected node-wide file-worker count.
fn file_worker_count() -> usize {
    std::env::var("MANTISSA_FIXED_FILE_WORKERS")
        .map(|value| {
            value
                .parse()
                .expect("MANTISSA_FIXED_FILE_WORKERS must be an integer")
        })
        .unwrap_or(2)
}

/// Returns the same ublk geometry used by the current replicated volume path.
fn ublk_settings() -> UblkSettings {
    UblkSettings::new(
        &descriptor(),
        UblkQueueSettings {
            queue_count: 2,
            queue_depth: 32,
            max_request_bytes: REQUEST_BYTES,
            memory_limit_bytes: 2 * 32 * u64::from(REQUEST_BYTES),
        },
    )
    .expect("fixed live ublk settings must be valid")
}

/// Creates a temporary root on the selected real benchmark filesystem.
fn test_root() -> TempDir {
    match std::env::var_os("MANTISSA_FIXED_UBLK_TEST_DIR") {
        Some(root) => {
            let root = PathBuf::from(root);
            std::fs::create_dir_all(&root).expect("fixed live benchmark root must exist");
            tempfile::Builder::new()
                .prefix("mantissa-fixed-ublk-")
                .tempdir_in(root)
                .expect("fixed live test directory must be created")
        }
        None => TempDir::new().expect("fixed live temporary directory must be created"),
    }
}

/// Runs one local tool and includes its exit code in a simple failure.
fn run(command: &mut Command, operation: &str) -> Result<(), Box<dyn Error>> {
    let status = command.status()?;
    if !status.success() {
        return Err(format!("{operation} failed with {status}").into());
    }
    Ok(())
}

/// Writes and reads one file so ext4 exercises normal writes and a real flush.
fn check_mounted_file(path: &Path) -> Result<Duration, Box<dyn Error>> {
    let file_path = path.join("fixed-path-check");
    let payload = vec![0x5a; 64 * MIB as usize];
    let started = Instant::now();
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&file_path)?;
    file.write_all(&payload)?;
    file.sync_all()?;
    let elapsed = started.elapsed();
    drop(file);

    let mut saved = Vec::new();
    File::open(file_path)?.read_to_end(&mut saved)?;
    if saved != payload {
        return Err("fixed replica ext4 file did not read back exactly".into());
    }
    Ok(elapsed)
}

/// Verifies the recognizable file after every replica and ublk process reopens.
fn verify_reopened_file(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut saved = Vec::new();
    File::open(path.join("fixed-path-check"))?.read_to_end(&mut saved)?;
    if saved.len() != 64 * MIB as usize || saved.iter().any(|byte| *byte != 0x5a) {
        return Err("reopened fixed replica ext4 file did not read back exactly".into());
    }
    Ok(())
}

/// Waits until the final PostgreSQL process accepts local connections.
fn wait_for_postgres(name: &str) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        if Command::new("docker")
            .args([
                "exec",
                name,
                "sh",
                "-c",
                "test \"$(cat /proc/1/comm)\" = postgres && exec psql -U mantissa -d app -c 'SELECT 1'",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()?
            .success()
        {
            return Ok(());
        }
        let running = Command::new("docker")
            .args(["inspect", "-f", "{{.State.Running}}", name])
            .stderr(std::process::Stdio::null())
            .output()?;
        if !running.status.success() || running.stdout != b"true\n" {
            return Err(format!("temporary PostgreSQL container {name} stopped").into());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("temporary PostgreSQL container {name} did not become ready").into(),
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Runs the exact scale-10 initialization against one host data directory.
fn benchmark_postgres(path: &Path, label: &str) -> Result<Duration, Box<dyn Error>> {
    let name = format!("mantissa-fixed-{label}-{}", std::process::id());
    let cleanup = DockerCleanup::new(name.clone());
    run(
        Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &name,
                "-e",
                "POSTGRES_USER=mantissa",
                "-e",
                "POSTGRES_DB=app",
                "-e",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                "-e",
                "PGDATA=/var/lib/postgresql/data/pgdata",
                "-v",
            ])
            .arg(format!("{}:/var/lib/postgresql/data", path.display()))
            .arg("postgres:16-alpine"),
        "start temporary PostgreSQL",
    )?;
    wait_for_postgres(&name)?;

    let started = Instant::now();
    let output = Command::new("docker")
        .args([
            "exec", &name, "pgbench", "-i", "-s", "10", "-U", "mantissa", "app",
        ])
        .output()?;
    let elapsed = started.elapsed();
    if !output.status.success() {
        return Err(format!(
            "{label} pgbench initialization failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    println!(
        "{label} pgbench initialization: {elapsed:?}\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
    drop(cleanup);
    Ok(elapsed)
}

/// Formats, mounts, writes, flushes, and reads through the complete proof path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires root, Linux ublk, dm-linear, mkfs.ext4, mount, and umount"]
async fn fixed_replica_path_formats_and_mounts_ext4() -> Result<(), Box<dyn Error>> {
    UblkSystem::system(TEST_OWNER).require_features()?;
    let mapped_volumes = MappedVolumeSystem::new()?;
    mapped_volumes.require_features()?;
    let file_workers = ReplicaFileWorkerPool::start(file_worker_count(), 256)?;
    let root = test_root();
    let epoch = FenceEpoch::new(2).expect("fixed live data fence must be valid");
    let physical_remotes = remote_addresses();
    let reopen_locally = physical_remotes.is_none();
    if physical_remotes.is_some() {
        assert_eq!(
            copy_count(),
            3,
            "the physical benchmark requires one local and two remote copies"
        );
    }
    let local_file_count = if physical_remotes.is_some() {
        1
    } else {
        copy_count()
    };
    let mut files = Vec::new();
    for index in 0..local_file_count {
        let directory = root.path().join(format!("copy-{index}"));
        files.push(Arc::new(ReplicaFile::create(
            directory,
            descriptor(),
            epoch,
            file_settings(),
        )?));
    }
    let data_limits = ReplicaDataLimits::new(1 << 20, 256, file_settings())?;
    let mut connections = match physical_remotes {
        Some(addresses) => connect_remote_copies(addresses, data_limits).await?,
        None => Vec::new(),
    };
    let mut servers = Vec::new();
    for file in files.iter().skip(1) {
        let (client_stream, server_stream) = tokio::io::duplex(8 << 20);
        let server_file = Arc::clone(file);
        let server_file_workers = file_workers.clone();
        let (open, admission) = data_authorization();
        servers.push(tokio::spawn(async move {
            ReplicaDataServer::new(
                server_file_workers,
                data_limits,
                ReplicaDataServerSettings::new(64, Duration::from_secs(30))?,
            )
            .serve_authorized(server_stream, server_file, open, node(1), admission)
            .await
        }));
        connections.push(Arc::new(ReplicaDataConnection::start(
            client_stream,
            data_limits,
            64,
        )?));
    }
    let mut copies = vec![FixedReplicaCopy::local(Arc::clone(&files[0]))];
    for connection in &connections {
        copies.push(FixedReplicaCopy::remote(descriptor(), epoch, Arc::clone(connection)).await?);
    }
    let mut path = FixedReplicaPath::start_copies(
        descriptor(),
        epoch,
        path_settings(),
        &file_workers,
        copies,
    )?;
    let mut device = UblkDevice::start(TEST_OWNER, ublk_settings(), path.handler())?;
    let mapped_layout = MappedVolumeLayout::new(node(1), &descriptor(), device.block_path())?;
    let mapped_path = mapped_volumes.create_or_verify(&mapped_layout)?;
    let mut mapped_cleanup = MappedCleanup::new(
        mapped_volumes.clone(),
        mapped_layout.node_id(),
        mapped_layout.key(),
    );

    let format_started = Instant::now();
    run(
        Command::new("mkfs.ext4")
            .args(["-q", "-F", "-b", "4096", "-E", "nodiscard"])
            .arg(mapped_path.as_path()),
        "format fixed replica ext4",
    )?;
    let format_elapsed = format_started.elapsed();

    let mount_path = root.path().join("mount");
    std::fs::create_dir(&mount_path)?;
    let mut mount = MountCleanup::new(mount_path.clone());
    run(
        Command::new("mount")
            .args(["-t", "ext4", "-o", "noatime"])
            .arg(mapped_path.as_path())
            .arg(&mount_path),
        "mount fixed replica ext4",
    )?;
    mount.mounted();
    let write_elapsed = check_mounted_file(&mount_path)?;
    let local_data = root.path().join("local-postgres");
    std::fs::create_dir(&local_data)?;
    let local_pgbench = benchmark_postgres(&local_data, "local")?;
    let replicated_data = mount_path.join("replicated-postgres");
    std::fs::create_dir(&replicated_data)?;
    let resources = ResourceSnapshot::start(&files);
    let control_probe = ControlProbe::start();
    let replicated_pgbench = benchmark_postgres(&replicated_data, "replicated")?;
    let control_maximum = control_probe.stop().await?;
    let resources = resources.finish(&files);
    mount.unmount()?;

    mapped_volumes.remove(mapped_layout.node_id(), mapped_layout.key())?;
    mapped_cleanup.removed();
    device.stop()?;
    path.stop().await?;
    for connection in connections {
        Arc::try_unwrap(connection)
            .unwrap_or_else(|_| panic!("live data connection must have one owner"))
            .stop()
            .await;
    }
    for server in servers {
        server.await??;
    }
    for file in &files {
        assert!(file.progress().flush_number() > 0);
    }
    let saved_progress = files[0].progress();
    if reopen_locally {
        drop(path);
        drop(files);
        let reopened_files = (0..local_file_count)
            .map(|index| {
                ReplicaFile::open(
                    root.path().join(format!("copy-{index}")),
                    descriptor(),
                    file_settings(),
                )
                .map(Arc::new)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for file in &reopened_files {
            assert_eq!(file.progress(), saved_progress);
        }
        let mut reopened_path = FixedReplicaPath::start(
            descriptor(),
            epoch,
            path_settings(),
            &file_workers,
            reopened_files,
        )?;
        let mut reopened_device =
            UblkDevice::start(TEST_OWNER, ublk_settings(), reopened_path.handler())?;
        let reopened_layout =
            MappedVolumeLayout::new(node(1), &descriptor(), reopened_device.block_path())?;
        let reopened_mapped_path = mapped_volumes.create_or_verify(&reopened_layout)?;
        mapped_cleanup.mapped();
        run(
            Command::new("mount")
                .args(["-t", "ext4", "-o", "noatime"])
                .arg(reopened_mapped_path.as_path())
                .arg(&mount_path),
            "mount reopened fixed replica ext4",
        )?;
        mount.mounted();
        verify_reopened_file(&mount_path)?;
        mount.unmount()?;
        mapped_volumes.remove(reopened_layout.node_id(), reopened_layout.key())?;
        mapped_cleanup.removed();
        reopened_device.stop()?;
        reopened_path.stop().await?;
    }
    file_workers.stop(Duration::from_secs(30)).await?;
    println!(
        "fixed replica mapped ext4: copies={} changed_region_mib={} format={format_elapsed:?} \
         write_and_sync_64m={write_elapsed:?} local_pgbench={local_pgbench:?} \
         replicated_pgbench={replicated_pgbench:?} cpu_ms={:.3} process_write_mib={:.3} \
         write_calls={} allocated_mib={:.3} max_rss_mib={:.3} control_max_ms={:.3}",
        copy_count(),
        changed_region_bytes() / MIB,
        resources.cpu.as_secs_f64() * 1_000.0,
        resources.write_bytes as f64 / MIB as f64,
        resources.write_calls,
        resources.allocated_bytes as f64 / MIB as f64,
        resources.maximum_rss_kib as f64 / 1024.0,
        control_maximum.as_secs_f64() * 1_000.0,
    );
    Ok(())
}

/// Serves one authenticated fixed file for the physical three-VM proof.
#[tokio::test(flavor = "current_thread")]
#[ignore = "manual server for the physical three-VM fixed-file benchmark"]
async fn serve_fixed_replica_copy() -> Result<(), Box<dyn Error>> {
    let bind = std::env::var("MANTISSA_FIXED_SERVER_BIND")
        .map_err(|_| "MANTISSA_FIXED_SERVER_BIND is required")?;
    let key_byte = std::env::var("MANTISSA_FIXED_SERVER_KEY_BYTE")
        .map_err(|_| "MANTISSA_FIXED_SERVER_KEY_BYTE is required")?
        .parse::<u8>()?;
    if key_byte != SECOND_KEY_BYTE && key_byte != THIRD_KEY_BYTE {
        return Err(format!(
            "MANTISSA_FIXED_SERVER_KEY_BYTE must be {SECOND_KEY_BYTE} or {THIRD_KEY_BYTE}"
        )
        .into());
    }
    let root = test_root();
    let file_workers = ReplicaFileWorkerPool::start(file_worker_count(), 256)?;
    let epoch = FenceEpoch::new(2).expect("fixed server data fence must be valid");
    let file = Arc::new(ReplicaFile::create(
        root.path().join("copy"),
        descriptor(),
        epoch,
        file_settings(),
    )?);
    let listener = TcpListener::bind(&bind).await?;
    println!("fixed replica server listening on {bind}");
    let (tcp, remote) = listener.accept().await?;
    let (mut reader, writer) = tcp.into_split();
    let mut first_frame = Vec::new();
    let first_bytes = read_framed_len(&mut reader, &mut first_frame).await?;
    let server_keys = NoiseKeys::from_private_bytes([key_byte; 32]);
    let client_keys = NoiseKeys::from_private_bytes([CLIENT_KEY_BYTE; 32]);
    let authenticated = server_handshake_peer_identified_with_first_frame(
        reader,
        writer,
        &server_keys,
        &first_frame[..first_bytes],
        Rc::new(ExpectedPeer {
            public_key: client_keys.public_bytes(),
        }),
    )
    .await
    .map_err(|error| format!("authenticate fixed replica client: {error:?}"))?;
    println!("fixed replica server accepted {remote}");
    let (open, admission) = data_authorization();
    ReplicaDataServer::new(
        file_workers.clone(),
        ReplicaDataLimits::new(1 << 20, 256, file_settings())?,
        ReplicaDataServerSettings::new(64, Duration::from_secs(30))?,
    )
    .serve_authorized(
        authenticated.stream,
        Arc::clone(&file),
        open,
        node(1),
        admission,
    )
    .await?;
    file_workers.stop(Duration::from_secs(30)).await?;
    let progress = file.progress();
    if progress.flush_number() == 0 {
        return Err("physical replica server did not complete a flush".into());
    }
    println!(
        "fixed replica server stopped at flush {} through write {}",
        progress.flush_number(),
        progress.durable_write_number()
    );
    Ok(())
}
