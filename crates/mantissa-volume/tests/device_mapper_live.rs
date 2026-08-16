#![cfg(target_os = "linux")]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use devicemapper::{DM, DevId, DmFlags, DmName, DmOptions, DmUuid};
use mantissa_volume::driver::{
    BlockHandler, BlockIoError, MappedVolumeError, MappedVolumeLayout, MappedVolumeSystem,
    UblkDevice, UblkDeviceId, UblkDeviceState, UblkOwnerId, UblkQueueSettings, UblkSettings,
    UblkSystem,
};
use mantissa_volume::{
    VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId, VolumeNodeId,
};
use parking_lot::Mutex;
use tempfile::TempDir;
use uuid::Uuid;

const BLOCK_BYTES: usize = 4 * 1024;
const CAPACITY_BYTES: u64 = 64 * 1024 * 1024;
const EXPANDED_CAPACITY_BYTES: u64 = 128 * 1024 * 1024;
const RECOVERY_MARKER: &str = "MANTISSA_MAPPED_RECOVERY_MARKER";
const TEST_OWNER: UblkOwnerId = UblkOwnerId::new(u64::from_be_bytes(*b"MNTS-DM-"));
const WAIT: Duration = Duration::from_secs(10);

/// Returns the local node encoded in every mapping owned by this test binary.
fn mapped_node_id() -> VolumeNodeId {
    VolumeNodeId::new(Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0))
        .expect("non-zero mapped-volume test node")
}

#[derive(Default)]
struct MemoryBlocks {
    blocks: Mutex<BTreeMap<u64, Box<[u8; BLOCK_BYTES]>>>,
    forced_writes: AtomicUsize,
    flushes: AtomicUsize,
    discards: AtomicUsize,
    zeroes: AtomicUsize,
}

#[derive(Clone, Copy)]
struct WriteMeasurement {
    mebibytes_per_second: f64,
    p95: Duration,
}

impl MemoryBlocks {
    /// Copies one complete block, treating an unallocated block as zeroed.
    fn read_block(&self, offset: u64, output: &mut [u8]) {
        match self.blocks.lock().get(&offset) {
            Some(block) => output.copy_from_slice(block.as_slice()),
            None => output.fill(0),
        }
    }

    /// Saves one complete block without allocating the full logical device.
    fn save_block(&self, offset: u64, input: &[u8]) {
        let mut block = Box::new([0_u8; BLOCK_BYTES]);
        block.copy_from_slice(input);
        self.blocks.lock().insert(offset, block);
    }

    /// Removes every complete block in one aligned byte range.
    fn remove_blocks(&self, offset: u64, length: u64) {
        let mut blocks = self.blocks.lock();
        for block_offset in (offset..offset + length).step_by(BLOCK_BYTES) {
            blocks.remove(&block_offset);
        }
    }
}

#[async_trait::async_trait]
impl BlockHandler for MemoryBlocks {
    /// Reads complete 4 KiB blocks from the sparse test store.
    async fn read(&self, offset: u64, output: &mut [u8]) -> Result<(), BlockIoError> {
        for (index, block) in output.chunks_exact_mut(BLOCK_BYTES).enumerate() {
            self.read_block(offset + (index * BLOCK_BYTES) as u64, block);
        }
        Ok(())
    }

    /// Saves complete blocks and records force-unit-access requests.
    async fn write(
        &self,
        offset: u64,
        input: Bytes,
        force_unit_access: bool,
    ) -> Result<(), BlockIoError> {
        if force_unit_access {
            self.forced_writes.fetch_add(1, Ordering::SeqCst);
        }
        for (index, block) in input.chunks_exact(BLOCK_BYTES).enumerate() {
            self.save_block(offset + (index * BLOCK_BYTES) as u64, block);
        }
        Ok(())
    }

    /// Records one flush after all earlier writes.
    async fn flush(&self) -> Result<(), BlockIoError> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Releases every block in one discarded byte range.
    async fn discard(&self, offset: u64, length: u64) -> Result<(), BlockIoError> {
        self.discards.fetch_add(1, Ordering::SeqCst);
        self.remove_blocks(offset, length);
        Ok(())
    }

    /// Zeroes or releases every block in one aligned byte range.
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

struct MappingCleanup {
    mapped_volumes: MappedVolumeSystem,
    mapping: Option<(VolumeNodeId, mantissa_volume::catalog::ReplicaKey)>,
    mount_path: Option<PathBuf>,
}

impl MappingCleanup {
    /// Tracks kernel state that must be removed even after a failed assertion.
    fn new(mapped_volumes: MappedVolumeSystem, layout: &MappedVolumeLayout) -> Self {
        Self {
            mapped_volumes,
            mapping: Some((layout.node_id(), layout.key())),
            mount_path: None,
        }
    }

    /// Records the mount that must be detached before mapping removal.
    fn mounted(&mut self, path: PathBuf) {
        self.mount_path = Some(path);
    }

    /// Unmounts the test filesystem and clears its cleanup record.
    fn unmount(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(path) = self.mount_path.take() {
            run(
                Command::new("umount").arg(&path),
                "unmount mapped test volume",
            )?;
        }
        Ok(())
    }

    /// Stops removing a mapping that the test removed successfully.
    fn mapping_removed(&mut self) {
        self.mapping = None;
    }
}

impl Drop for MappingCleanup {
    /// Best-effort cleanup keeps failed privileged tests from poisoning retries.
    fn drop(&mut self) {
        if let Some(path) = self.mount_path.take() {
            let _ = Command::new("umount").arg(path).status();
        }
        if let Some((node_id, key)) = self.mapping.take() {
            let _ = self.mapped_volumes.remove(node_id, key);
        }
    }
}

struct RawMappingCleanup {
    name: String,
}

impl RawMappingCleanup {
    /// Tracks a deliberately malformed raw mapping used by a rejection test.
    fn new(name: String) -> Self {
        Self { name }
    }
}

impl Drop for RawMappingCleanup {
    /// Removes the raw mapping without relying on Mantissa ownership checks.
    fn drop(&mut self) {
        if let (Ok(dm), Ok(name)) = (DM::new(), DmName::new(&self.name)) {
            let _ = dm.device_remove(&DevId::Name(name), DmOptions::default());
        }
    }
}

struct RecoveryCleanup {
    child: Option<Child>,
    device_id: Option<UblkDeviceId>,
    mapping: Option<(
        MappedVolumeSystem,
        VolumeNodeId,
        mantissa_volume::catalog::ReplicaKey,
    )>,
}

impl RecoveryCleanup {
    /// Starts cleanup ownership with the child that serves the ublk device.
    fn new(child: Child) -> Self {
        Self {
            child: Some(child),
            device_id: None,
            mapping: None,
        }
    }

    /// Records the ublk device left in user-recovery state if the child dies.
    fn device_started(&mut self, device_id: UblkDeviceId) {
        self.device_id = Some(device_id);
    }

    /// Records the mapping that must be removed before the ublk device.
    fn mapping_created(
        &mut self,
        mapped_volumes: MappedVolumeSystem,
        node_id: VolumeNodeId,
        key: mantissa_volume::catalog::ReplicaKey,
    ) {
        self.mapping = Some((mapped_volumes, node_id, key));
    }

    /// Kills and reaps the child so the ublk device enters user recovery.
    fn kill_child(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Stops cleanup after the mapping was removed in the required order.
    fn mapping_removed(&mut self) {
        self.mapping = None;
    }

    /// Stops cleanup after the recovered ublk device was removed.
    fn ublk_device_removed(&mut self) {
        self.device_id = None;
    }
}

impl Drop for RecoveryCleanup {
    /// Removes the mapped device before any ublk device left by a failed test.
    fn drop(&mut self) {
        self.kill_child();
        if let Some((mapped_volumes, node_id, key)) = self.mapping.take() {
            let _ = mapped_volumes.remove(node_id, key);
        }
        if let Some(device_id) = self.device_id.take() {
            let _ = UblkSystem::system(TEST_OWNER).remove(device_id);
        }
    }
}

/// Creates a descriptor with one exact capacity for a live kernel case.
fn descriptor_with_capacity(generation: u64, capacity_bytes: u64) -> VolumeDescriptor {
    VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef))
            .expect("non-zero test volume ID"),
        VolumeGeneration::new(generation).expect("non-zero test generation"),
        capacity_bytes,
        VolumeBlockSizes::supported(),
    )
    .expect("valid mapped-volume descriptor")
}

/// Creates a descriptor with a unique generation for each live case.
fn descriptor(generation: u64) -> VolumeDescriptor {
    descriptor_with_capacity(generation, CAPACITY_BYTES)
}

/// Creates one small ublk configuration for mapper qualification.
fn ublk_settings(descriptor: &VolumeDescriptor) -> UblkSettings {
    UblkSettings::new(
        descriptor,
        UblkQueueSettings {
            queue_count: 1,
            queue_depth: 32,
            max_request_bytes: 128 * 1024,
            memory_limit_bytes: 4 * 1024 * 1024,
        },
    )
    .expect("valid mapped-volume ublk settings")
}

/// Runs one required system command and includes stderr on failure.
fn run(command: &mut Command, label: &str) -> Result<(), Box<dyn Error>> {
    let output = command.output()?;
    if !output.status.success() {
        return Err(format!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

/// Reads one block device's exact byte capacity through the kernel.
fn block_device_capacity(path: &Path) -> Result<u64, Box<dyn Error>> {
    let output = Command::new("blockdev")
        .arg("--getsize64")
        .arg(path)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "read block device capacity failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().parse()?)
}

/// Reads the mounted filesystem size reported in bytes by the host tools.
fn filesystem_capacity(path: &Path) -> Result<u64, Box<dyn Error>> {
    let output = Command::new("df")
        .args(["--block-size=1", "--output=size"])
        .arg(path)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "read filesystem capacity failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    String::from_utf8(output.stdout)?
        .lines()
        .nth(1)
        .ok_or_else(|| "filesystem capacity output has no value".into())
        .and_then(|value| value.trim().parse::<u64>().map_err(Into::into))
}

/// Returns the current Mantissa-owned ublk device IDs.
fn ublk_ids() -> BTreeSet<u32> {
    UblkSystem::system(TEST_OWNER)
        .devices()
        .expect("list live ublk devices")
        .into_iter()
        .map(|device| device.id().get())
        .collect()
}

/// Returns the current Mantissa-owned mapped volume names.
fn mapped_names(mapped_volumes: &MappedVolumeSystem) -> BTreeSet<String> {
    mapped_volumes
        .owned_devices()
        .expect("list live mapped volumes")
        .into_iter()
        .map(|device| device.name().to_string())
        .collect()
}

/// Writes and flushes one complete block through the mapped path.
fn write_block(path: &Path, offset: u64, byte: u8) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&vec![byte; BLOCK_BYTES])?;
    file.sync_all()?;
    Ok(())
}

/// Reads one complete block through the mapped path.
fn read_block(path: &Path, offset: u64) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut output = vec![0_u8; BLOCK_BYTES];
    file.read_exact(&mut output)?;
    Ok(output)
}

/// Issues one direct data-sync write through device-mapper.
fn write_with_fua(path: &Path, offset: u64) -> Result<(), Box<dyn Error>> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;
    let mut buffer = libublk::helpers::IoBuf::<u8>::new(BLOCK_BYTES);
    buffer.as_mut_slice().fill(0x33);
    let vector = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    let offset = libc::off_t::try_from(offset)?;
    // SAFETY: the page-aligned buffer and iovec remain live for the complete
    // call. The kernel reads them synchronously and does not retain pointers.
    let written = unsafe { libc::pwritev2(file.as_raw_fd(), &vector, 1, offset, libc::RWF_DSYNC) };
    if written != BLOCK_BYTES as isize {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

/// Measures synchronous direct writes without filesystem or cache effects.
fn measure_direct_writes(
    path: &Path,
    request_count: usize,
) -> Result<WriteMeasurement, Box<dyn Error>> {
    const REQUEST_BYTES: usize = 128 * 1024;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;
    let mut buffer = libublk::helpers::IoBuf::<u8>::new(REQUEST_BYTES);
    buffer.as_mut_slice().fill(0xa5);
    let mut latencies = Vec::with_capacity(request_count);
    let started = Instant::now();
    for request in 0..request_count {
        let offset = (request * REQUEST_BYTES) as u64 % CAPACITY_BYTES;
        let request_started = Instant::now();
        // SAFETY: the page-aligned buffer remains live for the synchronous
        // call, and every selected offset and request length is block-aligned.
        let written = unsafe {
            libc::pwrite(
                file.as_raw_fd(),
                buffer.as_ptr().cast(),
                buffer.len(),
                libc::off_t::try_from(offset)?,
            )
        };
        if written != REQUEST_BYTES as isize {
            return Err(std::io::Error::last_os_error().into());
        }
        latencies.push(request_started.elapsed());
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    let p95_index = (latencies.len() * 95 / 100).min(latencies.len() - 1);
    Ok(WriteMeasurement {
        mebibytes_per_second: request_count as f64 * REQUEST_BYTES as f64
            / (1024.0 * 1024.0)
            / elapsed.as_secs_f64(),
        p95: latencies[p95_index],
    })
}

/// Issues one discard or explicit zero operation through device-mapper.
fn discard(path: &Path, offset: u64, zero: bool) -> Result<(), Box<dyn Error>> {
    let mut command = Command::new("blkdiscard");
    command
        .arg("--force")
        .arg("--quiet")
        .arg("--offset")
        .arg(offset.to_string())
        .arg("--length")
        .arg(BLOCK_BYTES.to_string());
    if zero {
        command.arg("--zeroout");
    }
    run(command.arg(path), "issue mapped discard")
}

/// Waits for one external kernel or child-process state with a deadline.
fn wait_for(mut ready: impl FnMut() -> bool, failure: &str) {
    let deadline = Instant::now() + WAIT;
    while !ready() {
        assert!(Instant::now() < deadline, "{failure}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "requires root, Linux ublk, dm-linear, blkdiscard, ext4, and mount"]
fn live_mapped_volume_io_mount_and_cleanup() -> Result<(), Box<dyn Error>> {
    let ublk_system = UblkSystem::system(TEST_OWNER);
    ublk_system.require_features()?;
    let mapped_volumes = MappedVolumeSystem::new()?;
    mapped_volumes.require_features()?;
    let before_ublk = ublk_ids();
    let before_mapped = mapped_names(&mapped_volumes);

    let descriptor = descriptor(1);
    let blocks = Arc::new(MemoryBlocks::default());
    let mut ublk_device =
        UblkDevice::start(TEST_OWNER, ublk_settings(&descriptor), blocks.clone())?;
    let layout = MappedVolumeLayout::new(mapped_node_id(), &descriptor, ublk_device.block_path())?;
    let mut cleanup = MappingCleanup::new(mapped_volumes.clone(), &layout);
    let first = mapped_volumes.create_or_verify(&layout)?;
    let second = mapped_volumes.create_or_verify(&layout)?;
    assert_eq!(
        first, second,
        "repeated creation must reuse the existing mapping"
    );
    assert_eq!(
        Some(first.clone()),
        mapped_volumes.inspect(&layout)?,
        "inspect must verify the active table"
    );
    assert_eq!(CAPACITY_BYTES.to_string(), {
        let output = Command::new("blockdev")
            .arg("--getsize64")
            .arg(first.as_path())
            .output()?;
        assert!(output.status.success(), "read mapped capacity");
        String::from_utf8(output.stdout)?.trim().to_string()
    });
    assert!(mapped_volumes.underlying_device_is_referenced(ublk_device.block_path())?);

    measure_direct_writes(ublk_device.block_path(), 256)?;
    measure_direct_writes(first.as_path(), 256)?;
    let raw_first = measure_direct_writes(ublk_device.block_path(), 4096)?;
    let mapped = measure_direct_writes(first.as_path(), 4096)?;
    let raw_second = measure_direct_writes(ublk_device.block_path(), 4096)?;
    let raw_throughput = (raw_first.mebibytes_per_second + raw_second.mebibytes_per_second) / 2.0;
    let raw_p95 = raw_first.p95.max(raw_second.p95);
    println!(
        "direct 128 KiB writes: raw={raw_throughput:.1} MiB/s p95={raw_p95:?}; \
         dm-linear={:.1} MiB/s p95={:?}",
        mapped.mebibytes_per_second, mapped.p95
    );
    assert!(
        mapped.mebibytes_per_second >= raw_throughput * 0.80,
        "dm-linear reduced direct-write throughput by more than 20%"
    );
    assert!(
        mapped.p95 <= raw_p95.saturating_mul(2),
        "dm-linear more than doubled direct-write p95 latency"
    );

    write_block(first.as_path(), 0, 0x11)?;
    assert_eq!(vec![0x11; BLOCK_BYTES], read_block(first.as_path(), 0)?);
    write_with_fua(first.as_path(), BLOCK_BYTES as u64)?;
    assert!(blocks.forced_writes.load(Ordering::SeqCst) > 0);
    assert!(blocks.flushes.load(Ordering::SeqCst) > 0);
    let discard_offset = (2 * BLOCK_BYTES) as u64;
    write_block(first.as_path(), discard_offset, 0x44)?;
    discard(first.as_path(), discard_offset, false)?;
    assert_eq!(
        vec![0; BLOCK_BYTES],
        read_block(first.as_path(), discard_offset)?
    );
    assert!(blocks.discards.load(Ordering::SeqCst) > 0);
    let zero_offset = (3 * BLOCK_BYTES) as u64;
    write_block(first.as_path(), zero_offset, 0x55)?;
    discard(first.as_path(), zero_offset, true)?;
    assert_eq!(
        vec![0; BLOCK_BYTES],
        read_block(first.as_path(), zero_offset)?
    );
    assert!(blocks.zeroes.load(Ordering::SeqCst) > 0);

    run(
        Command::new("mkfs.ext4")
            .args(["-q", "-F", "-b", "4096", "-E", "nodiscard"])
            .arg(first.as_path()),
        "format mapped volume",
    )?;
    let root = TempDir::new()?;
    let mount_path = root.path().join("mount");
    std::fs::create_dir(&mount_path)?;
    run(
        Command::new("mount")
            .args(["-t", "ext4", "-o", "noatime"])
            .arg(first.as_path())
            .arg(&mount_path),
        "mount mapped volume",
    )?;
    cleanup.mounted(mount_path.clone());
    std::fs::write(mount_path.join("mapped-path-check"), b"mapped")?;
    assert!(
        matches!(
            mapped_volumes.remove(layout.node_id(), layout.key()),
            Err(MappedVolumeError::Kernel { .. })
        ),
        "an open filesystem must keep the mapping busy"
    );
    assert!(mapped_volumes.inspect(&layout)?.is_some());
    cleanup.unmount()?;

    mapped_volumes.remove(layout.node_id(), layout.key())?;
    cleanup.mapping_removed();
    assert!(mapped_volumes.inspect(&layout)?.is_none());
    assert!(!mapped_volumes.underlying_device_is_referenced(ublk_device.block_path())?);
    ublk_device.stop()?;
    assert_eq!(before_mapped, mapped_names(&mapped_volumes));
    assert_eq!(before_ublk, ublk_ids());
    Ok(())
}

#[test]
#[ignore = "requires root, Linux ublk, dm-linear, ext4, and mount"]
fn live_failed_io_mapping_allows_ext4_detach() -> Result<(), Box<dyn Error>> {
    let ublk_system = UblkSystem::system(TEST_OWNER);
    ublk_system.require_features()?;
    let mapped_volumes = MappedVolumeSystem::new()?;
    mapped_volumes.require_features()?;
    let before_ublk = ublk_ids();
    let before_mapped = mapped_names(&mapped_volumes);

    let descriptor = descriptor(5);
    let mut ublk_device = UblkDevice::start(
        TEST_OWNER,
        ublk_settings(&descriptor),
        Arc::new(MemoryBlocks::default()),
    )?;
    let layout = MappedVolumeLayout::new(mapped_node_id(), &descriptor, ublk_device.block_path())?;
    let mut cleanup = MappingCleanup::new(mapped_volumes.clone(), &layout);
    let mapped_path = mapped_volumes.create_or_verify(&layout)?;
    run(
        Command::new("mkfs.ext4")
            .args(["-q", "-F", "-b", "4096", "-E", "nodiscard"])
            .arg(mapped_path.as_path()),
        "format cleanup-test mapped volume",
    )?;
    let root = TempDir::new()?;
    let mount_path = root.path().join("mount");
    std::fs::create_dir(&mount_path)?;
    run(
        Command::new("mount")
            .args(["-t", "ext4", "-o", "noatime"])
            .arg(mapped_path.as_path())
            .arg(&mount_path),
        "mount cleanup-test mapped volume",
    )?;
    cleanup.mounted(mount_path.clone());
    let mut sentinel = File::create(mount_path.join("cleanup-check"))?;
    sentinel.write_all(b"durable before fencing")?;
    sentinel.sync_all()?;
    drop(sentinel);

    assert!(
        mapped_volumes
            .fail_io_for_cleanup(layout.node_id(), layout.key(), &[])
            .is_err(),
        "an existing mapping requires one exact saved layout"
    );
    mapped_volumes.fail_io_for_cleanup(
        layout.node_id(),
        layout.key(),
        std::slice::from_ref(&layout),
    )?;
    mapped_volumes.fail_io_for_cleanup(
        layout.node_id(),
        layout.key(),
        std::slice::from_ref(&layout),
    )?;
    run(
        Command::new("umount").arg("--lazy").arg(&mount_path),
        "detach filesystem from failed-I/O mapping",
    )?;
    cleanup.mount_path = None;
    mapped_volumes.remove(layout.node_id(), layout.key())?;
    cleanup.mapping_removed();
    mapped_volumes.fail_io_for_cleanup(layout.node_id(), layout.key(), &[])?;
    ublk_device.stop()?;
    assert_eq!(before_mapped, mapped_names(&mapped_volumes));
    assert_eq!(before_ublk, ublk_ids());
    Ok(())
}

#[test]
#[ignore = "requires root, Linux ublk, dm-linear, ext4, resize2fs, and mount"]
fn live_mapped_volume_expands_without_unmounting() -> Result<(), Box<dyn Error>> {
    let ublk_system = UblkSystem::system(TEST_OWNER);
    ublk_system.require_features()?;
    let mapped_volumes = MappedVolumeSystem::new()?;
    mapped_volumes.require_features()?;
    let before_ublk = ublk_ids();
    let before_mapped = mapped_names(&mapped_volumes);

    let initial = descriptor_with_capacity(4, CAPACITY_BYTES);
    let expanded = descriptor_with_capacity(4, EXPANDED_CAPACITY_BYTES);
    let blocks = Arc::new(MemoryBlocks::default());
    let mut initial_ublk_device =
        UblkDevice::start(TEST_OWNER, ublk_settings(&initial), blocks.clone())?;
    let mut expanded_ublk_device =
        UblkDevice::start(TEST_OWNER, ublk_settings(&expanded), blocks.clone())?;
    let initial_layout =
        MappedVolumeLayout::new(mapped_node_id(), &initial, initial_ublk_device.block_path())?;
    let expanded_layout = MappedVolumeLayout::new(
        mapped_node_id(),
        &expanded,
        expanded_ublk_device.block_path(),
    )?;
    let mut cleanup = MappingCleanup::new(mapped_volumes.clone(), &initial_layout);
    let mapped_path = mapped_volumes.create_or_verify(&initial_layout)?;

    run(
        Command::new("mkfs.ext4")
            .args(["-q", "-F", "-b", "4096", "-E", "nodiscard"])
            .arg(mapped_path.as_path()),
        "format expandable mapped volume",
    )?;
    let root = TempDir::new()?;
    let mount_path = root.path().join("mount");
    std::fs::create_dir(&mount_path)?;
    run(
        Command::new("mount")
            .args(["-t", "ext4", "-o", "noatime"])
            .arg(mapped_path.as_path())
            .arg(&mount_path),
        "mount expandable mapped volume",
    )?;
    cleanup.mounted(mount_path.clone());
    let sentinel_path = mount_path.join("before-expansion");
    let mut sentinel = File::create(&sentinel_path)?;
    sentinel.write_all(b"survives online expansion")?;
    sentinel.sync_all()?;
    let initial_filesystem_capacity = filesystem_capacity(&mount_path)?;

    assert_eq!(
        mapped_path,
        mapped_volumes.suspend_for_expansion(&initial_layout, &expanded_layout)?,
        "suspending the old ublk device must preserve the public mapped path"
    );
    assert_eq!(
        mapped_path,
        mapped_volumes.suspend_for_expansion(&initial_layout, &expanded_layout)?,
        "retrying the suspended boundary must be idempotent"
    );
    assert_eq!(
        mapped_path,
        mapped_volumes.activate_expanded_layout(&initial_layout, &expanded_layout)?,
        "the public mapped path must not change during expansion"
    );
    assert_eq!(
        mapped_path,
        mapped_volumes.activate_expanded_layout(&initial_layout, &expanded_layout)?,
        "retrying completed mapped-volume expansion must be idempotent"
    );
    assert_eq!(
        EXPANDED_CAPACITY_BYTES,
        block_device_capacity(mapped_path.as_path())?
    );
    assert_eq!(
        Some(1),
        mapped_volumes.active_layout(&[initial_layout.clone(), expanded_layout.clone()])?
    );
    let mapping_name = mapped_path
        .as_path()
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("mapped path has no UTF-8 device name")?;
    let dm = DM::new()?;
    let dm_name = DmName::new(mapping_name)?;
    dm.device_suspend(
        &DevId::Name(dm_name),
        DmOptions::default().set_flags(DmFlags::DM_SUSPEND),
    )?;
    assert_eq!(
        mapped_path,
        mapped_volumes.resume_exact(&expanded_layout)?,
        "startup recovery must resume the exact expanded active table"
    );
    assert_eq!(
        mapped_path,
        mapped_volumes.resume_exact(&expanded_layout)?,
        "retrying saved-mapping recovery must be idempotent"
    );
    let during_path = mount_path.join("during-expansion");
    let mut during = File::create(&during_path)?;
    during.write_all(b"written after the mapper switch")?;
    during.sync_all()?;

    run(
        Command::new("resize2fs").arg(mapped_path.as_path()),
        "expand mounted ext4 filesystem",
    )?;
    run(
        Command::new("resize2fs").arg(mapped_path.as_path()),
        "retry completed ext4 expansion before saving its receipt",
    )?;
    assert!(
        filesystem_capacity(&mount_path)? > initial_filesystem_capacity,
        "online ext4 expansion must expose more filesystem capacity"
    );
    assert_eq!(
        b"survives online expansion",
        std::fs::read(&sentinel_path)?.as_slice()
    );
    assert_eq!(
        b"written after the mapper switch",
        std::fs::read(&during_path)?.as_slice()
    );
    let after_path = mount_path.join("after-expansion");
    let mut after = File::create(after_path)?;
    after.write_all(&vec![0x5a; 1024 * 1024])?;
    after.sync_all()?;

    drop(after);
    drop(during);
    drop(sentinel);
    cleanup.unmount()?;
    mapped_volumes.remove(initial_layout.node_id(), initial_layout.key())?;
    cleanup.mapping_removed();
    expanded_ublk_device.stop()?;
    initial_ublk_device.stop()?;
    assert_eq!(before_mapped, mapped_names(&mapped_volumes));
    assert_eq!(before_ublk, ublk_ids());
    Ok(())
}

#[test]
#[ignore = "requires root, Linux ublk, and dm-linear"]
fn live_foreign_name_and_wrong_table_are_rejected() -> Result<(), Box<dyn Error>> {
    let mapped_volumes = MappedVolumeSystem::new()?;
    mapped_volumes.require_features()?;
    let descriptor = descriptor(2);
    let mut ublk_device = UblkDevice::start(
        TEST_OWNER,
        ublk_settings(&descriptor),
        Arc::new(MemoryBlocks::default()),
    )?;
    let layout = MappedVolumeLayout::new(mapped_node_id(), &descriptor, ublk_device.block_path())?;
    let name = layout
        .expected_path()
        .file_name()
        .expect("mapping path has a name")
        .to_str()
        .expect("mapping name is UTF-8")
        .to_string();
    let dm = DM::new()?;
    let dm_name = DmName::new(&name)?;
    let foreign_uuid = DmUuid::new("MANTISSA-FOREIGN-TEST")?;
    dm.device_create(dm_name, Some(foreign_uuid), DmOptions::default())?;
    let foreign_cleanup = RawMappingCleanup::new(name.clone());
    assert!(matches!(
        mapped_volumes.create_or_verify(&layout),
        Err(MappedVolumeError::ForeignName { .. })
    ));
    assert!(!mapped_volumes.underlying_device_is_referenced(ublk_device.block_path())?);
    let ublk_device_number = std::fs::read_to_string(format!(
        "/sys/class/block/ublkb{}/dev",
        ublk_device.id().get()
    ))?;
    dm.table_load(
        &DevId::Name(dm_name),
        &[(
            0,
            CAPACITY_BYTES / 512,
            "linear".to_string(),
            format!("{} 0", ublk_device_number.trim()),
        )],
        DmOptions::default(),
    )?;
    dm.device_suspend(&DevId::Name(dm_name), DmOptions::default())?;
    assert!(mapped_volumes.underlying_device_is_referenced(ublk_device.block_path())?);
    drop(foreign_cleanup);

    let uuid = format!(
        "MANTISSA-RV-{}-V{}-G{}",
        layout.node_id().as_uuid().simple(),
        descriptor.volume_id().as_uuid().simple(),
        descriptor.generation().get()
    );
    let dm_name = DmName::new(&name)?;
    let dm_uuid = DmUuid::new(&uuid)?;
    dm.device_create(dm_name, Some(dm_uuid), DmOptions::default())?;
    let empty_cleanup = RawMappingCleanup::new(name.clone());
    let empty = mapped_volumes
        .owned_devices()?
        .into_iter()
        .find(|mapped| mapped.node_id() == layout.node_id() && mapped.key() == layout.key())
        .expect("empty owned mapping is visible");
    assert!(empty.underlying_device_numbers().is_empty());
    mapped_volumes.create_or_verify(&layout)?;
    assert!(mapped_volumes.underlying_device_is_referenced(ublk_device.block_path())?);
    mapped_volumes.remove(layout.node_id(), layout.key())?;
    drop(empty_cleanup);

    let dm_name = DmName::new(&name)?;
    let dm_uuid = DmUuid::new(&uuid)?;
    dm.device_create(dm_name, Some(dm_uuid), DmOptions::default())?;
    let wrong_cleanup = RawMappingCleanup::new(name.clone());
    dm.table_load(
        &DevId::Name(dm_name),
        &[(
            0,
            CAPACITY_BYTES / 512 - BLOCK_BYTES as u64 / 512,
            "linear".to_string(),
            format!("{} 0", ublk_device_number.trim()),
        )],
        DmOptions::default(),
    )?;
    dm.device_suspend(&DevId::Name(dm_name), DmOptions::default())?;
    assert!(matches!(
        mapped_volumes.create_or_verify(&layout),
        Err(MappedVolumeError::WrongLayout { .. })
    ));
    let cleanup_result = mapped_volumes.fail_io_for_cleanup(
        layout.node_id(),
        layout.key(),
        std::slice::from_ref(&layout),
    );
    assert!(
        matches!(cleanup_result, Err(MappedVolumeError::WrongLayout { .. })),
        "unexpected failed-I/O cleanup result: {cleanup_result:?}"
    );
    drop(wrong_cleanup);

    let wrong_name = format!("{name}-alias");
    let wrong_dm_name = DmName::new(&wrong_name)?;
    let dm_uuid = DmUuid::new(&uuid)?;
    dm.device_create(wrong_dm_name, Some(dm_uuid), DmOptions::default())?;
    let wrong_name_cleanup = RawMappingCleanup::new(wrong_name.clone());
    assert!(matches!(
        mapped_volumes.create_or_verify(&layout),
        Err(MappedVolumeError::WrongName { .. })
    ));
    drop(wrong_name_cleanup);

    let dm_name = DmName::new(&name)?;
    let dm_uuid = DmUuid::new(&uuid)?;
    dm.device_create(dm_name, Some(dm_uuid), DmOptions::default())?;
    let expected_cleanup = RawMappingCleanup::new(name);
    let wrong_dm_name = DmName::new(&wrong_name)?;
    let dm_uuid = DmUuid::new(&uuid)?;
    assert!(
        dm.device_create(wrong_dm_name, Some(dm_uuid), DmOptions::default())
            .is_err(),
        "the kernel must reject a duplicate device-mapper UUID"
    );
    drop(expected_cleanup);
    ublk_device.stop()?;
    Ok(())
}

#[test]
#[ignore = "helper process for live_mapped_volume_user_recovery"]
fn mapped_recovery_server_child() -> Result<(), Box<dyn Error>> {
    let Ok(marker) = std::env::var(RECOVERY_MARKER) else {
        return Ok(());
    };
    let descriptor = descriptor(3);
    let ublk_device = UblkDevice::start(
        TEST_OWNER,
        ublk_settings(&descriptor),
        Arc::new(MemoryBlocks::default()),
    )?;
    std::fs::write(marker, ublk_device.id().get().to_string())?;
    loop {
        std::thread::park();
    }
}

#[test]
#[ignore = "requires root, Linux ublk user recovery, and dm-linear"]
fn live_mapped_volume_user_recovery() -> Result<(), Box<dyn Error>> {
    let ublk_system = UblkSystem::system(TEST_OWNER);
    ublk_system.require_features()?;
    let mapped_volumes = MappedVolumeSystem::new()?;
    mapped_volumes.require_features()?;
    let marker_directory = TempDir::new()?;
    let marker = marker_directory.path().join("ublk-device-id");
    let child = Command::new(std::env::current_exe()?)
        .args([
            "--ignored",
            "--exact",
            "mapped_recovery_server_child",
            "--nocapture",
        ])
        .env(RECOVERY_MARKER, &marker)
        .spawn()?;
    let mut cleanup = RecoveryCleanup::new(child);
    wait_for(
        || {
            marker.exists()
                || cleanup
                    .child
                    .as_mut()
                    .and_then(|child| child.try_wait().ok())
                    .flatten()
                    .is_some()
        },
        "recovery child did not publish its ublk device",
    );
    assert!(marker.exists(), "recovery child exited before startup");
    let device_id = UblkDeviceId::new(std::fs::read_to_string(&marker)?.trim().parse()?);
    cleanup.device_started(device_id);
    let ublk_device_path = ublk_system
        .device(device_id)?
        .expect("child ublk device exists")
        .block_path()
        .to_path_buf();
    let descriptor = descriptor(3);
    let layout = MappedVolumeLayout::new(mapped_node_id(), &descriptor, &ublk_device_path)?;
    let mapped_path = mapped_volumes.create_or_verify(&layout)?;
    cleanup.mapping_created(mapped_volumes.clone(), layout.node_id(), layout.key());

    cleanup.kill_child();
    wait_for(
        || {
            ublk_system
                .device(device_id)
                .ok()
                .flatten()
                .is_some_and(|device| device.state() == UblkDeviceState::NeedsRecovery)
        },
        "ublk device did not enter user recovery",
    );
    let mut recovered = UblkDevice::recover(
        TEST_OWNER,
        device_id,
        ublk_settings(&descriptor),
        Arc::new(MemoryBlocks::default()),
    )?;
    assert_eq!(
        mapped_path,
        mapped_volumes.create_or_verify(&layout)?,
        "ublk device recovery must preserve the mapped device"
    );
    write_block(mapped_path.as_path(), 0, 0x77)?;
    assert_eq!(
        vec![0x77; BLOCK_BYTES],
        read_block(mapped_path.as_path(), 0)?
    );

    mapped_volumes.remove(layout.node_id(), layout.key())?;
    cleanup.mapping_removed();
    recovered.stop()?;
    cleanup.ublk_device_removed();
    Ok(())
}
