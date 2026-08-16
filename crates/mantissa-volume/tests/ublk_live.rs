#![cfg(target_os = "linux")]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use mantissa_volume::driver::{
    BlockHandler, BlockIoError, UblkDevice, UblkDeviceId, UblkDeviceState, UblkError, UblkOwnerId,
    UblkQueueSettings, UblkSettings, UblkSystem,
};
use mantissa_volume::{VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId};
use parking_lot::Mutex;
use tempfile::TempDir;
use uuid::Uuid;

const BLOCK_BYTES: usize = 4 * 1024;
const REQUEST_BYTES: u32 = 128 * 1024;
const WAIT: Duration = Duration::from_secs(10);
const EXT4_WAIT: Duration = Duration::from_secs(10);
const CHILD_MARKER: &str = "MANTISSA_UBLK_TEST_MARKER";
const BLOCKED_WRITE_MARKER: &str = "MANTISSA_UBLK_BLOCKED_WRITE_MARKER";
const MIB: u64 = 1024 * 1024;
const TIB: u64 = 1024 * 1024 * MIB;
const PIB: u64 = 1024 * TIB;
const EIB: u64 = 1024 * PIB;
const TEST_OWNER: UblkOwnerId = UblkOwnerId::new(u64::from_be_bytes(*b"MNTS-TST"));

#[derive(Default)]
struct SparseMemoryHandler {
    blocks: Mutex<BTreeMap<u64, Box<[u8; BLOCK_BYTES]>>>,
    blocked_write_marker: Option<PathBuf>,
    slow_writes: bool,
    active_writes: AtomicUsize,
    largest_active_writes: AtomicUsize,
    writes: AtomicUsize,
    forced_writes: AtomicUsize,
    flushes: AtomicUsize,
    discards: AtomicUsize,
    zeroes: AtomicUsize,
}

impl SparseMemoryHandler {
    /// Creates a child handler that blocks after receiving its first write.
    fn with_blocked_writes(marker: PathBuf) -> Self {
        Self {
            blocked_write_marker: Some(marker),
            ..Self::default()
        }
    }

    /// Creates a handler that leaves each write active long enough to overlap.
    fn with_slow_writes() -> Self {
        Self {
            slow_writes: true,
            ..Self::default()
        }
    }

    /// Copies one complete block from the sparse test map.
    fn read_block(&self, offset: u64, output: &mut [u8]) {
        let blocks = self.blocks.lock();
        match blocks.get(&offset) {
            Some(block) => output.copy_from_slice(block.as_slice()),
            None => output.fill(0),
        }
    }

    /// Saves one complete block in the sparse test map.
    fn save_block(&self, offset: u64, input: &[u8]) {
        let mut block = Box::new([0_u8; BLOCK_BYTES]);
        block.copy_from_slice(input);
        self.blocks.lock().insert(offset, block);
    }

    /// Removes all complete blocks in one checked byte range.
    fn remove_blocks(&self, offset: u64, length: u64) {
        let mut blocks = self.blocks.lock();
        for block_offset in (offset..offset + length).step_by(BLOCK_BYTES) {
            blocks.remove(&block_offset);
        }
    }
}

#[async_trait::async_trait]
impl BlockHandler for SparseMemoryHandler {
    /// Reads complete 4 KiB blocks without allocating the logical capacity.
    async fn read(&self, offset: u64, output: &mut [u8]) -> Result<(), BlockIoError> {
        for (index, part) in output.chunks_exact_mut(BLOCK_BYTES).enumerate() {
            let part_offset = offset + (index * BLOCK_BYTES) as u64;
            self.read_block(part_offset, part);
        }
        Ok(())
    }

    /// Saves complete 4 KiB blocks and records whether the kernel sent FUA.
    async fn write(
        &self,
        offset: u64,
        input: Bytes,
        force_unit_access: bool,
    ) -> Result<(), BlockIoError> {
        if let Some(marker) = &self.blocked_write_marker {
            std::fs::write(marker, b"received").map_err(|error| {
                BlockIoError::failed(format!("could not publish blocked write: {error}"))
            })?;
            loop {
                std::thread::park();
            }
        }
        let active = self.active_writes.fetch_add(1, Ordering::AcqRel) + 1;
        self.largest_active_writes
            .fetch_max(active, Ordering::AcqRel);
        if self.slow_writes {
            smol::Timer::after(Duration::from_millis(100)).await;
        }
        self.active_writes.fetch_sub(1, Ordering::AcqRel);
        self.writes.fetch_add(1, Ordering::SeqCst);
        if force_unit_access {
            self.forced_writes.fetch_add(1, Ordering::SeqCst);
        }
        for (index, part) in input.chunks_exact(BLOCK_BYTES).enumerate() {
            let part_offset = offset + (index * BLOCK_BYTES) as u64;
            self.save_block(part_offset, part);
        }
        Ok(())
    }

    /// Records a kernel flush after all prior in-memory writes.
    async fn flush(&self) -> Result<(), BlockIoError> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Removes complete blocks and records a kernel discard.
    async fn discard(&self, offset: u64, length: u64) -> Result<(), BlockIoError> {
        self.discards.fetch_add(1, Ordering::SeqCst);
        self.remove_blocks(offset, length);
        Ok(())
    }

    /// Removes complete blocks and records a kernel write-zeroes request.
    async fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
        _force_unit_access: bool,
        allow_discard: bool,
    ) -> Result<(), BlockIoError> {
        self.zeroes.fetch_add(1, Ordering::SeqCst);
        if allow_discard {
            self.remove_blocks(offset, length);
        } else {
            for block_offset in (offset..offset + length).step_by(BLOCK_BYTES) {
                self.save_block(block_offset, &[0; BLOCK_BYTES]);
            }
        }
        Ok(())
    }
}

struct ChildCleanup {
    child: Option<Child>,
    device_id: Option<UblkDeviceId>,
}

impl ChildCleanup {
    /// Starts with one child and no published kernel device.
    fn new(child: Child) -> Self {
        Self {
            child: Some(child),
            device_id: None,
        }
    }

    /// Records the device that must be removed if the test stops early.
    fn set_device(&mut self, id: UblkDeviceId) {
        self.device_id = Some(id);
    }

    /// Kills and reaps the child after the test has captured its device.
    fn kill_child(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Stops automatic removal after the recovered device is deleted.
    fn clear_device(&mut self) {
        self.device_id = None;
    }
}

impl Drop for ChildCleanup {
    /// Prevents a failed recovery assertion from leaving a child or device.
    fn drop(&mut self) {
        self.kill_child();
        if let Some(id) = self.device_id {
            let _ = UblkSystem::system(TEST_OWNER).remove(id);
        }
    }
}

/// Creates a descriptor without adding a product capacity ceiling.
fn descriptor(capacity_bytes: u64) -> VolumeDescriptor {
    VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(0x018f_89ad_6bc8_7b3d_a8ef_50b1_3cda_14c2))
            .expect("fixed volume ID must be valid"),
        VolumeGeneration::new(1).expect("fixed generation must be valid"),
        capacity_bytes,
        VolumeBlockSizes::supported(),
    )
    .expect("capacity candidate must form a valid descriptor")
}

/// Creates small explicit queue limits for live qualification.
fn settings(capacity_bytes: u64, queue_count: u16) -> UblkSettings {
    let queue_depth = 32;
    UblkSettings::new(
        &descriptor(capacity_bytes),
        UblkQueueSettings {
            queue_count,
            queue_depth,
            max_request_bytes: REQUEST_BYTES,
            memory_limit_bytes: u64::from(queue_count)
                * u64::from(queue_depth)
                * u64::from(REQUEST_BYTES),
        },
    )
    .expect("live test settings must be valid")
}

/// Returns the current process thread or file-descriptor count.
fn process_entry_count(path: &str) -> usize {
    std::fs::read_dir(path)
        .expect("read process resource directory")
        .count()
}

/// Returns the current set of Mantissa ublk device IDs.
fn device_ids() -> BTreeSet<UblkDeviceId> {
    UblkSystem::system(TEST_OWNER)
        .devices()
        .expect("read current ublk devices")
        .into_iter()
        .map(|device| device.id())
        .collect()
}

/// Waits for an external kernel or child-process state with a fixed deadline.
fn wait_for(mut ready: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + WAIT;
    while !ready() {
        assert!(Instant::now() < deadline, "{message}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Tries to write and flush one complete block.
fn try_write_block(path: &Path, offset: u64, byte: u8) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    let mut file = options.open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&vec![byte; BLOCK_BYTES])?;
    file.sync_all()
}

/// Writes one complete block in tests that require success.
fn write_block(path: &Path, offset: u64, byte: u8) {
    try_write_block(path, offset, byte).expect("write live ublk block");
}

/// Tries to read one complete block from the live device.
fn try_read_block(path: &Path, offset: u64) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut output = vec![0_u8; BLOCK_BYTES];
    file.read_exact(&mut output)?;
    Ok(output)
}

/// Reads one complete block in tests that require success.
fn read_block(path: &Path, offset: u64) -> Vec<u8> {
    try_read_block(path, offset).expect("read live ublk block")
}

/// Runs blkdiscard for one aligned range.
fn run_blkdiscard(path: &Path, offset: u64, write_zeroes: bool) {
    let mut command = Command::new("blkdiscard");
    command
        .arg("--force")
        .arg("--quiet")
        .arg("--offset")
        .arg(offset.to_string())
        .arg("--length")
        .arg(BLOCK_BYTES.to_string());
    if write_zeroes {
        command.arg("--zeroout");
    }
    let status = command
        .arg(path)
        .status()
        .expect("run blkdiscard against live ublk device");
    assert!(status.success(), "blkdiscard operation failed");
}

/// Issues one direct `RWF_DSYNC` write so the kernel sends FUA.
fn run_fua_write(path: &Path, offset: u64) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
        .expect("open live ublk device for direct FUA write");
    let mut buffer = libublk::helpers::IoBuf::<u8>::new(BLOCK_BYTES);
    buffer.as_mut_slice().fill(0x33);
    let vector = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    let offset = libc::off_t::try_from(offset).expect("FUA test offset must fit off_t");
    // SAFETY: `vector` points to one live, page-aligned libublk buffer for the
    // duration of this call. The file descriptor and checked byte offset are
    // valid, and the kernel reads but does not retain the iovec.
    let written = unsafe { libc::pwritev2(file.as_raw_fd(), &vector, 1, offset, libc::RWF_DSYNC) };
    assert_eq!(BLOCK_BYTES as isize, written, "direct FUA write failed");
}

/// Issues one aligned direct write without adding a flush or FUA request.
fn try_direct_write(path: &Path, offset: u64, byte: u8) -> io::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;
    let mut buffer = libublk::helpers::IoBuf::<u8>::new(BLOCK_BYTES);
    buffer.as_mut_slice().fill(byte);
    let offset = libc::off_t::try_from(offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "write offset is too large"))?;
    // SAFETY: `buffer` remains live and page-aligned for the complete call.
    // The file descriptor and byte offset are valid and the kernel does not
    // retain the buffer after `pwrite` returns.
    let written = unsafe {
        libc::pwrite(
            file.as_raw_fd(),
            buffer.as_ptr().cast(),
            buffer.len(),
            offset,
        )
    };
    if written == BLOCK_BYTES as isize {
        Ok(())
    } else if written < 0 {
        Err(io::Error::last_os_error())
    } else {
        Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "direct write completed only part of the block",
        ))
    }
}

/// Reads one integer block setting from sysfs.
fn read_sysfs_u64(id: UblkDeviceId, name: &str) -> u64 {
    read_sysfs_text(id, name)
        .parse()
        .expect("parse live ublk sysfs setting")
}

/// Reads one block setting from sysfs without its trailing newline.
fn read_sysfs_text(id: UblkDeviceId, name: &str) -> String {
    std::fs::read_to_string(format!("/sys/block/ublkb{}/queue/{name}", id.get()))
        .expect("read live ublk sysfs setting")
        .trim()
        .to_string()
}

/// Returns one command's combined first output line for the qualification log.
fn command_version(command: &str, arguments: &[&str]) -> String {
    let output = Command::new(command)
        .args(arguments)
        .output()
        .expect("run qualification version command");
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    text.lines()
        .find(|line| !line.trim().is_empty())
        .map_or_else(|| "unknown".to_string(), |line| line.trim().to_string())
}

/// Reads the exact capacity reported through the block-device ioctl.
fn blockdev_capacity(path: &Path) -> u64 {
    let output = Command::new("blockdev")
        .arg("--getsize64")
        .arg(path)
        .output()
        .expect("run blockdev capacity query");
    assert!(output.status.success(), "blockdev capacity query failed");
    String::from_utf8(output.stdout)
        .expect("blockdev capacity must be UTF-8")
        .trim()
        .parse()
        .expect("blockdev capacity must be an integer")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Ext4Check {
    Accepted,
    Rejected,
    TimedOut,
    Skipped,
}

/// Runs the non-writing ext4 geometry check with a fixed tool deadline.
fn check_ext4(path: &Path) -> Ext4Check {
    let mut child = Command::new("mkfs.ext4")
        .args(["-n", "-F", "-b", "4096"])
        .arg(path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("start non-writing ext4 check");
    let deadline = Instant::now() + EXT4_WAIT;
    loop {
        match child.try_wait().expect("wait for non-writing ext4 check") {
            Some(status) if status.success() => return Ext4Check::Accepted,
            Some(_) => return Ext4Check::Rejected,
            None if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            None => {
                let _ = child.kill();
                let _ = child.wait();
                return Ext4Check::TimedOut;
            }
        }
    }
}

#[test]
#[ignore = "requires root, Linux ublk, blockdev, and blkdiscard"]
fn live_driver_operations_and_cleanup() -> Result<(), Box<dyn Error>> {
    let system = UblkSystem::system(TEST_OWNER);
    let features = system.require_features()?;
    assert!(features.supports_user_recovery());
    assert!(features.supports_request_reissue());
    // Start smol's one process-wide reactor before measuring resources owned
    // by this device. The reactor thread intentionally outlives one device.
    smol::block_on(smol::Timer::after(Duration::ZERO));
    let before_devices = device_ids();
    let before_threads = process_entry_count("/proc/self/task");
    let before_files = process_entry_count("/proc/self/fd");

    let missing_id = UblkDeviceId::new(i32::MAX as u32);
    let after_reboot = system.check_devices([missing_id])?;
    assert_eq!(&[missing_id], after_reboot.missing());

    let handler = Arc::new(SparseMemoryHandler::default());
    let mut device = UblkDevice::start(TEST_OWNER, settings(64 * MIB, 2), handler.clone())?;
    let id = device.id();
    let info = system
        .device(id)?
        .expect("live device must be in inventory");
    assert!(
        UblkSystem::system(UblkOwnerId::new(TEST_OWNER.get() ^ 1))
            .device(id)?
            .is_none(),
        "another node must not claim this ublk device"
    );
    assert_eq!(UblkDeviceState::Running, info.state());
    assert_eq!(2, info.queue_count());
    assert_eq!(32, info.queue_depth());
    assert_eq!(REQUEST_BYTES, info.max_request_bytes());
    assert_eq!(64 * MIB, info.capacity_bytes());
    assert_eq!(4096, info.logical_sector_bytes());
    assert_eq!(4096, info.physical_block_bytes());
    assert_eq!(4096, info.minimum_io_bytes());
    assert!(info.reissues_requests_after_recovery());
    assert_eq!(Some(std::process::id()), info.server_pid());
    assert_eq!(4096, read_sysfs_u64(id, "logical_block_size"));
    assert_eq!(4096, read_sysfs_u64(id, "physical_block_size"));
    assert_eq!(4096, read_sysfs_u64(id, "minimum_io_size"));
    assert_eq!("write back", read_sysfs_text(id, "write_cache"));

    write_block(device.block_path(), 0, 0x11);
    assert_eq!(vec![0x11; BLOCK_BYTES], read_block(device.block_path(), 0));
    let last_offset = 64 * MIB - BLOCK_BYTES as u64;
    write_block(device.block_path(), last_offset, 0x22);
    assert_eq!(
        vec![0x22; BLOCK_BYTES],
        read_block(device.block_path(), last_offset)
    );
    run_fua_write(device.block_path(), BLOCK_BYTES as u64);
    assert!(
        handler.forced_writes.load(Ordering::SeqCst) >= 1,
        "RWF_DSYNC write did not reach ublk with FUA"
    );

    let discard_offset = (2 * BLOCK_BYTES) as u64;
    write_block(device.block_path(), discard_offset, 0x44);
    run_blkdiscard(device.block_path(), discard_offset, false);
    assert_eq!(
        vec![0; BLOCK_BYTES],
        read_block(device.block_path(), discard_offset)
    );
    assert!(handler.discards.load(Ordering::SeqCst) >= 1);

    let zero_offset = (3 * BLOCK_BYTES) as u64;
    write_block(device.block_path(), zero_offset, 0x55);
    run_blkdiscard(device.block_path(), zero_offset, true);
    assert_eq!(
        vec![0; BLOCK_BYTES],
        read_block(device.block_path(), zero_offset)
    );
    assert!(handler.zeroes.load(Ordering::SeqCst) >= 1);

    device.stop()?;
    wait_for(
        || system.device(id).ok().flatten().is_none(),
        "ublk device was not removed",
    );
    assert_eq!(before_devices, device_ids());
    assert_eq!(before_threads, process_entry_count("/proc/self/task"));
    assert_eq!(before_files, process_entry_count("/proc/self/fd"));
    Ok(())
}

#[test]
#[ignore = "requires root and Linux ublk"]
fn live_queue_depth_runs_requests_together() -> Result<(), Box<dyn Error>> {
    let system = UblkSystem::system(TEST_OWNER);
    system.require_features()?;
    let handler = Arc::new(SparseMemoryHandler::with_slow_writes());
    let mut device = UblkDevice::start(TEST_OWNER, settings(64 * MIB, 1), handler.clone())?;
    let block_path = device.block_path().to_path_buf();
    let start = Arc::new(std::sync::Barrier::new(9));
    let mut writers = Vec::new();
    for index in 0..8_u64 {
        let block_path = block_path.clone();
        let start = Arc::clone(&start);
        writers.push(std::thread::spawn(move || {
            start.wait();
            try_direct_write(&block_path, index * BLOCK_BYTES as u64, index as u8)
        }));
    }
    let writes_started = Instant::now();
    start.wait();
    for writer in writers {
        writer
            .join()
            .expect("direct writer must not panic")
            .expect("direct write must pass");
    }
    assert!(
        writes_started.elapsed() < Duration::from_millis(750),
        "completed async writes waited for the idle queue timeout"
    );
    assert!(
        handler.largest_active_writes.load(Ordering::Acquire) >= 4,
        "ublk queue depth did not allow four requests to run together"
    );
    let stop_started = Instant::now();
    device.stop()?;
    assert!(
        stop_started.elapsed() < Duration::from_secs(3),
        "idle ublk queue took too long to stop"
    );
    Ok(())
}

#[test]
#[ignore = "helper process for live_user_recovery"]
fn recovery_server_child() -> Result<(), Box<dyn Error>> {
    let Ok(marker) = std::env::var(CHILD_MARKER) else {
        return Ok(());
    };
    let blocked_write_marker = std::env::var(BLOCKED_WRITE_MARKER)?;
    let handler = Arc::new(SparseMemoryHandler::with_blocked_writes(
        blocked_write_marker.into(),
    ));
    let device = UblkDevice::start(TEST_OWNER, settings(64 * MIB, 1), handler)?;
    std::fs::write(marker, device.id().get().to_string())?;
    loop {
        std::thread::park();
    }
}

#[test]
#[ignore = "requires root and Linux ublk user recovery"]
fn live_user_recovery() -> Result<(), Box<dyn Error>> {
    let system = UblkSystem::system(TEST_OWNER);
    system.require_features()?;
    let marker_directory = TempDir::new()?;
    let marker = marker_directory.path().join("device-id");
    let blocked_write_marker = marker_directory.path().join("blocked-write");
    let child = Command::new(std::env::current_exe()?)
        .args([
            "--ignored",
            "--exact",
            "recovery_server_child",
            "--nocapture",
        ])
        .env(CHILD_MARKER, &marker)
        .env(BLOCKED_WRITE_MARKER, &blocked_write_marker)
        .spawn()?;
    let mut cleanup = ChildCleanup::new(child);
    wait_for(
        || {
            if marker.exists() {
                return true;
            }
            cleanup
                .child
                .as_mut()
                .and_then(|child| child.try_wait().ok())
                .flatten()
                .is_some()
        },
        "recovery child did not publish its device",
    );
    assert!(
        marker.exists(),
        "recovery child exited before device creation"
    );
    let id = UblkDeviceId::new(std::fs::read_to_string(&marker)?.trim().parse()?);
    cleanup.set_device(id);
    assert_eq!(
        UblkDeviceState::Running,
        system.device(id)?.expect("child device must exist").state()
    );

    let block_path = system
        .device(id)?
        .expect("child device must exist before the write")
        .block_path()
        .to_path_buf();
    let (write_sender, write_receiver) = mpsc::sync_channel(1);
    let writer = std::thread::spawn(move || {
        let _ = write_sender.send(try_write_block(&block_path, 0, 0x66));
    });
    wait_for(
        || blocked_write_marker.exists(),
        "write did not reach the child ublk server",
    );

    cleanup.kill_child();
    wait_for(
        || {
            system
                .device(id)
                .ok()
                .flatten()
                .is_some_and(|device| device.state() == UblkDeviceState::NeedsRecovery)
        },
        "device did not enter user recovery after server death",
    );
    let check = system.check_devices([id])?;
    assert_eq!(1, check.found().len());
    assert!(check.missing().is_empty());

    let wrong_recovery = UblkDevice::recover(
        TEST_OWNER,
        id,
        settings(64 * MIB, 2),
        Arc::new(SparseMemoryHandler::default()),
    );
    assert!(matches!(
        wrong_recovery,
        Err(UblkError::RecoverySettingMismatch {
            field: "queue count",
            saved: 2,
            kernel: 1,
        })
    ));

    let handler = Arc::new(SparseMemoryHandler::default());
    let mut recovered = UblkDevice::recover(TEST_OWNER, id, settings(64 * MIB, 1), handler)?;
    assert_eq!(id, recovered.id());
    assert_eq!(
        UblkDeviceState::Running,
        system
            .device(id)?
            .expect("recovered device must exist")
            .state()
    );
    write_receiver
        .recv_timeout(WAIT)
        .expect("interrupted write was not reissued after recovery")?;
    writer.join().expect("writer thread must not panic");
    assert_eq!(
        vec![0x66; BLOCK_BYTES],
        read_block(recovered.block_path(), 0)
    );
    recovered.stop()?;
    cleanup.clear_device();
    wait_for(
        || system.device(id).ok().flatten().is_none(),
        "recovered device was not removed",
    );
    Ok(())
}

#[test]
#[ignore = "requires root, Linux ublk, blockdev, and mkfs.ext4"]
fn live_capacity_report() -> Result<(), Box<dyn Error>> {
    let system = UblkSystem::system(TEST_OWNER);
    let features = system.require_features()?;
    let kernel = command_version("uname", &["-r"]);
    let ext4 = command_version("mkfs.ext4", &["-V"]);
    println!(
        "ublk qualification: kernel={kernel}; libublk=0.4.6; \
         features=0x{:x}; ext4_tool={ext4}",
        features.raw()
    );

    let candidates = [
        64 * MIB,
        TIB,
        16 * TIB,
        64 * TIB,
        128 * TIB,
        PIB,
        16 * PIB,
        EIB,
        8 * EIB,
        u64::MAX - (BLOCK_BYTES as u64 - 1),
    ];
    let mut largest_ublk = 0;
    let mut largest_edge_io = 0;
    let mut check_larger_ext4 = true;
    let mut ext4_at_one_tib = None;
    for capacity in candidates {
        let handler = Arc::new(SparseMemoryHandler::default());
        let mut device = match UblkDevice::start(TEST_OWNER, settings(capacity, 1), handler) {
            Ok(device) => device,
            Err(error) => {
                println!("capacity bytes={capacity}: ublk=failed; error={error}");
                break;
            }
        };
        largest_ublk = capacity;
        assert_eq!(capacity, blockdev_capacity(device.block_path()));
        let edge_io = try_write_block(device.block_path(), 0, 0x31)
            .and_then(|()| {
                try_write_block(device.block_path(), capacity - BLOCK_BYTES as u64, 0x32)
            })
            .and_then(|()| try_read_block(device.block_path(), 0))
            .and_then(|first| {
                if first == vec![0x31; BLOCK_BYTES] {
                    Ok(())
                } else {
                    Err(io::Error::other("first block did not match"))
                }
            })
            .and_then(|()| try_read_block(device.block_path(), capacity - BLOCK_BYTES as u64))
            .and_then(|last| {
                if last == vec![0x32; BLOCK_BYTES] {
                    Ok(())
                } else {
                    Err(io::Error::other("last block did not match"))
                }
            });
        if edge_io.is_ok() {
            largest_edge_io = capacity;
        }
        let ext4_result = if check_larger_ext4 {
            check_ext4(device.block_path())
        } else {
            Ext4Check::Skipped
        };
        check_larger_ext4 = ext4_result == Ext4Check::Accepted;
        if capacity == TIB {
            ext4_at_one_tib = Some(ext4_result);
        }
        println!(
            "capacity bytes={capacity}: ublk=ok; first_last_io={}; \
             ext4_dry_run={ext4_result:?}",
            edge_io.is_ok()
        );
        let id = device.id();
        device.stop()?;
        wait_for(
            || system.device(id).ok().flatten().is_none(),
            "capacity test device was not removed",
        );
        if let Err(error) = edge_io {
            println!("capacity bytes={capacity}: first_last_error={error}");
            break;
        }
    }

    assert!(
        largest_ublk >= 128 * TIB,
        "ublk did not reach the required 128 TiB candidate"
    );
    assert!(largest_edge_io >= 128 * TIB);
    assert_eq!(Some(Ext4Check::Accepted), ext4_at_one_tib);
    Ok(())
}
