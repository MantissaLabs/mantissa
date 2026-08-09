use std::fs::File;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;
use libublk::ctrl::UblkCtrlBuilder;
use libublk::helpers::IoBuf;
use libublk::io::{UblkDev, UblkQueue, with_task_io_ring, with_task_io_ring_mut};
use libublk::uring_async::{ublk_reap_io_events_with_update_queue, ublk_wake_task};
use libublk::{BufDesc, UblkError as NativeUblkError, UblkFlags};

use super::{REQUIRED_KERNEL_FEATURES, UblkDeviceId, UblkError, UblkOwnerId, native_error};
use crate::driver::{BlockHandler, BlockIoError, UblkSettings};

const SECTOR_BYTES: u64 = 512;
const STOP_CHECK_SECONDS: u64 = 1;

#[derive(Clone, Copy)]
pub(super) enum DeviceMode {
    Create,
    Recover(UblkDeviceId),
}

pub(super) struct ServerReady {
    pub(super) id: UblkDeviceId,
    pub(super) block_path: PathBuf,
}

#[derive(Clone, Copy)]
struct Request {
    operation: u32,
    offset: u64,
    length: usize,
    force_unit_access: bool,
    allow_discard: bool,
}

#[derive(Clone, Copy)]
enum QueueWake {
    Kernel,
    Task,
    Idle,
}

/// Runs one libublk control loop and all of its queue threads.
pub(super) fn serve(
    owner_id: UblkOwnerId,
    mode: DeviceMode,
    settings: UblkSettings,
    handler: Arc<dyn BlockHandler>,
    ready: mpsc::SyncSender<ServerReady>,
) -> Result<(), UblkError> {
    let device_id = match mode {
        DeviceMode::Create => -1,
        DeviceMode::Recover(id) => id.as_i32()?,
    };
    let device_flag = match mode {
        DeviceMode::Create => UblkFlags::UBLK_DEV_F_ADD_DEV,
        DeviceMode::Recover(_) => UblkFlags::UBLK_DEV_F_RECOVER_DEV,
    };
    let control = UblkCtrlBuilder::default()
        .name("mantissa-volume")
        .id(device_id)
        .nr_queues(settings.queue_count())
        .depth(settings.queue_depth())
        .io_buf_bytes(settings.max_request_bytes())
        .ctrl_flags(REQUIRED_KERNEL_FEATURES)
        .ctrl_target_flags(owner_id.get())
        .dev_flags(device_flag)
        .build()
        .map_err(|source| native_error("create ublk control device", source))?;

    let queue_handler = move |queue_id, device: &UblkDev| {
        serve_queue(queue_id, device, Arc::clone(&handler), settings);
    };
    control
        .run_target(
            move |device| {
                configure_device(device, settings);
                Ok(())
            },
            queue_handler,
            move |running| {
                let id = UblkDeviceId::new(running.dev_info().dev_id);
                let _ = ready.send(ServerReady {
                    id,
                    block_path: PathBuf::from(running.get_bdev_path()),
                });
            },
        )
        .map_err(|source| native_error("run ublk server", source))?;
    Ok(())
}

/// Reports capacity, block sizes, flush, FUA, discard, and zero support.
fn configure_device(device: &mut UblkDev, settings: UblkSettings) {
    device.set_default_params(settings.capacity_bytes());
    let basic = &mut device.tgt.params.basic;
    // A block handler may hold ordinary writes in a bounded memory cache.
    // Linux must therefore send flush and FUA requests at filesystem
    // durability boundaries.
    basic.attrs = libublk::sys::UBLK_ATTR_VOLATILE_CACHE | libublk::sys::UBLK_ATTR_FUA;
    basic.logical_bs_shift = settings.logical_sector_bytes().trailing_zeros() as u8;
    basic.physical_bs_shift = settings.physical_block_bytes().trailing_zeros() as u8;
    basic.io_min_shift = settings.minimum_io_bytes().trailing_zeros() as u8;
    basic.io_opt_shift = 0;
    basic.max_sectors = settings.max_request_bytes() >> 9;
    basic.dev_sectors = settings.capacity_bytes() >> 9;

    device.tgt.params.types |= libublk::sys::UBLK_PARAM_TYPE_DISCARD;
    device.tgt.params.discard = libublk::sys::ublk_param_discard {
        discard_alignment: 0,
        discard_granularity: settings.logical_sector_bytes(),
        max_discard_sectors: settings.max_request_bytes() >> 9,
        max_write_zeroes_sectors: settings.max_request_bytes() >> 9,
        max_discard_segments: 1,
        ..Default::default()
    };
}

/// Owns one queue's registered buffers until the kernel stops the queue.
fn serve_queue(
    queue_id: u16,
    device: &UblkDev,
    handler: Arc<dyn BlockHandler>,
    settings: UblkSettings,
) {
    let queue = Rc::new(match UblkQueue::new(queue_id, device) {
        Ok(queue) => queue,
        Err(_) => {
            // Device startup waits for every queue to register its buffers.
            device.notify_buffer_registration_complete(true);
            return;
        }
    });
    let executor = Rc::new(smol::LocalExecutor::new());
    let mut tasks = Vec::with_capacity(usize::from(settings.queue_depth()));
    for tag in 0..settings.queue_depth() {
        let queue = Rc::clone(&queue);
        let handler = Arc::clone(&handler);
        tasks.push(executor.spawn(async move { serve_tag(&queue, tag, settings, handler).await }));
    }

    smol::block_on(run_queue_events(&queue, &executor, &tasks));
}

/// Wakes the queue for either kernel events or completed async block work.
async fn run_queue_events(
    queue: &UblkQueue<'_>,
    executor: &smol::LocalExecutor<'_>,
    tasks: &[smol::Task<Result<(), NativeUblkError>>],
) {
    let ring_fd = with_task_io_ring(|ring| ring.as_raw_fd());
    // Register a duplicate because the io_uring thread-local owns the original.
    let ring_file = match unsafe { BorrowedFd::borrow_raw(ring_fd) }.try_clone_to_owned() {
        Ok(ring_file) => File::from(ring_file),
        Err(_) => return,
    };
    let async_ring = match smol::Async::new(ring_file) {
        Ok(ring) => ring,
        Err(_) => return,
    };

    while executor.try_tick() {}
    loop {
        if with_task_io_ring_mut(|ring| ring.submit_and_wait(0)).is_err() {
            break;
        }

        let kernel = async { async_ring.readable().await.map(|_| QueueWake::Kernel) };
        let task = async {
            executor.tick().await;
            Ok(QueueWake::Task)
        };
        let idle = async {
            smol::Timer::after(Duration::from_secs(STOP_CHECK_SECONDS)).await;
            Ok(QueueWake::Idle)
        };
        let wake = smol::future::race(smol::future::race(kernel, task), idle).await;
        let wake = match wake {
            Ok(wake) => wake,
            Err(_) => break,
        };

        let aborted = match wake {
            QueueWake::Task => false,
            QueueWake::Kernel | QueueWake::Idle => {
                let idle = matches!(wake, QueueWake::Idle);
                match ublk_reap_io_events_with_update_queue(queue, idle, None, |event| {
                    ublk_wake_task(event.user_data(), event);
                }) {
                    Ok(aborted) => aborted,
                    Err(_) => break,
                }
            }
        };
        while executor.try_tick() {}

        if aborted && tasks.iter().all(smol::Task::is_finished) {
            break;
        }
    }
}

/// Keeps one queue tag active so every configured queue slot can run at once.
async fn serve_tag(
    queue: &UblkQueue<'_>,
    tag: u16,
    settings: UblkSettings,
    handler: Arc<dyn BlockHandler>,
) -> Result<(), NativeUblkError> {
    let mut buffer = IoBuf::<u8>::new(settings.max_request_bytes() as usize);
    queue
        .submit_io_prep_cmd(tag, BufDesc::Slice(buffer.as_slice()), 0, Some(&buffer))
        .await?;
    loop {
        let descriptor = queue.get_iod(tag);
        let request = parse_request(
            descriptor.op_flags,
            descriptor.start_sector,
            descriptor.nr_sectors,
        );
        let result = match request {
            Ok(request) => handle_request(handler.as_ref(), settings, request, &mut buffer)
                .await
                .unwrap_or_else(block_error),
            Err(error) => block_error(error),
        };
        queue
            .submit_io_commit_cmd(tag, BufDesc::Slice(buffer.as_slice()), result)
            .await?;
    }
}

/// Converts the kernel descriptor without allowing integer overflow.
fn parse_request(
    operation_flags: u32,
    start_sector: u64,
    sector_count: u32,
) -> Result<Request, BlockIoError> {
    let operation = operation_flags & 0xff;
    if operation == libublk::sys::UBLK_IO_OP_FLUSH {
        return Ok(Request {
            operation,
            offset: 0,
            length: 0,
            force_unit_access: false,
            allow_discard: false,
        });
    }
    let offset = start_sector
        .checked_mul(SECTOR_BYTES)
        .ok_or(BlockIoError::InvalidRequest)?;
    let length = u64::from(sector_count)
        .checked_mul(SECTOR_BYTES)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or(BlockIoError::InvalidRequest)?;
    Ok(Request {
        operation,
        offset,
        length,
        force_unit_access: operation_flags & libublk::sys::UBLK_IO_F_FUA != 0,
        allow_discard: operation_flags & libublk::sys::UBLK_IO_F_NOUNMAP == 0,
    })
}

/// Validates one request before exposing a queue buffer to the block handler.
async fn handle_request(
    handler: &dyn BlockHandler,
    settings: UblkSettings,
    request: Request,
    buffer: &mut libublk::helpers::IoBuf<u8>,
) -> Result<i32, BlockIoError> {
    if request.operation == libublk::sys::UBLK_IO_OP_FLUSH {
        if request.length != 0 {
            return Err(BlockIoError::InvalidRequest);
        }
        handler.flush().await?;
        return Ok(0);
    }

    let length = u64::try_from(request.length).map_err(|_| BlockIoError::InvalidRequest)?;
    if request.length == 0
        || request.length > settings.max_request_bytes() as usize
        || !request
            .offset
            .is_multiple_of(u64::from(settings.logical_sector_bytes()))
        || !length.is_multiple_of(u64::from(settings.logical_sector_bytes()))
        || request
            .offset
            .checked_add(length)
            .is_none_or(|end| end > settings.capacity_bytes())
        || request.length > buffer.as_slice().len()
    {
        return Err(BlockIoError::InvalidRequest);
    }

    let result = match request.operation {
        libublk::sys::UBLK_IO_OP_READ => {
            handler
                .read(request.offset, &mut buffer.as_mut_slice()[..request.length])
                .await?;
            Ok(())
        }
        libublk::sys::UBLK_IO_OP_WRITE => {
            let input = Bytes::copy_from_slice(&buffer.as_slice()[..request.length]);
            handler
                .write(request.offset, input, request.force_unit_access)
                .await
        }
        libublk::sys::UBLK_IO_OP_DISCARD => handler.discard(request.offset, length).await,
        libublk::sys::UBLK_IO_OP_WRITE_ZEROES => {
            handler
                .write_zeroes(
                    request.offset,
                    length,
                    request.force_unit_access,
                    request.allow_discard,
                )
                .await
        }
        _ => return Err(BlockIoError::InvalidRequest),
    };
    result?;
    i32::try_from(request.length).map_err(|_| BlockIoError::InvalidRequest)
}

/// Maps one deliberate handler failure to a Linux block error.
fn block_error(error: BlockIoError) -> i32 {
    match error {
        BlockIoError::InvalidRequest => -libc::EINVAL,
        BlockIoError::OutOfSpace => -libc::ENOSPC,
        BlockIoError::NotServing | BlockIoError::Retry | BlockIoError::Failed { .. } => -libc::EIO,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use parking_lot::Mutex;
    use uuid::Uuid;

    use super::*;
    use crate::driver::UblkQueueSettings;
    use crate::{VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId};

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum SeenRequest {
        Read(u64, usize),
        Write(u64, Vec<u8>, bool),
        Flush,
        Discard(u64, u64),
        WriteZeroes(u64, u64, bool, bool),
    }

    #[derive(Default)]
    struct RecordingHandler {
        requests: Mutex<Vec<SeenRequest>>,
        read_buffer_address: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl BlockHandler for RecordingHandler {
        /// Fills reads with a fixed byte and records their checked range.
        async fn read(&self, offset: u64, output: &mut [u8]) -> Result<(), BlockIoError> {
            self.read_buffer_address
                .store(output.as_ptr() as usize, Ordering::Release);
            output.fill(0x5a);
            self.requests
                .lock()
                .push(SeenRequest::Read(offset, output.len()));
            Ok(())
        }

        /// Records one checked write and its FUA flag.
        async fn write(
            &self,
            offset: u64,
            input: Bytes,
            force_unit_access: bool,
        ) -> Result<(), BlockIoError> {
            self.requests.lock().push(SeenRequest::Write(
                offset,
                input.to_vec(),
                force_unit_access,
            ));
            Ok(())
        }

        /// Records one flush.
        async fn flush(&self) -> Result<(), BlockIoError> {
            self.requests.lock().push(SeenRequest::Flush);
            Ok(())
        }

        /// Records one checked discard range.
        async fn discard(&self, offset: u64, length: u64) -> Result<(), BlockIoError> {
            self.requests
                .lock()
                .push(SeenRequest::Discard(offset, length));
            Ok(())
        }

        /// Records one checked zero range and its kernel flags.
        async fn write_zeroes(
            &self,
            offset: u64,
            length: u64,
            force_unit_access: bool,
            allow_discard: bool,
        ) -> Result<(), BlockIoError> {
            self.requests.lock().push(SeenRequest::WriteZeroes(
                offset,
                length,
                force_unit_access,
                allow_discard,
            ));
            Ok(())
        }
    }

    /// Creates fixed settings with one 128 KiB request buffer.
    fn settings() -> UblkSettings {
        let descriptor = VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(1)).expect("fixed volume ID must be valid"),
            VolumeGeneration::new(1).expect("fixed generation must be valid"),
            64 * 1024 * 1024,
            VolumeBlockSizes::supported(),
        )
        .expect("fixed descriptor must be valid");
        UblkSettings::new(
            &descriptor,
            UblkQueueSettings {
                queue_count: 1,
                queue_depth: 1,
                max_request_bytes: 128 * 1024,
                memory_limit_bytes: 128 * 1024,
            },
        )
        .expect("fixed ublk settings must be valid")
    }

    /// Creates one checked request without constructing a kernel descriptor.
    fn request(operation: u32, offset: u64, length: usize, fua: bool) -> Request {
        Request {
            operation,
            offset,
            length,
            force_unit_access: fua,
            allow_discard: true,
        }
    }

    #[test]
    fn every_supported_request_reaches_the_small_handler_interface() {
        smol::block_on(async {
            let handler = RecordingHandler::default();
            let mut buffer = libublk::helpers::IoBuf::<u8>::new(128 * 1024);
            buffer.as_mut_slice()[..4096].fill(0xa5);

            assert_eq!(
                4096,
                handle_request(
                    &handler,
                    settings(),
                    request(libublk::sys::UBLK_IO_OP_WRITE, 4096, 4096, true),
                    &mut buffer,
                )
                .await
                .expect("checked write must pass")
            );
            assert_eq!(
                4096,
                handle_request(
                    &handler,
                    settings(),
                    request(libublk::sys::UBLK_IO_OP_READ, 8192, 4096, false),
                    &mut buffer,
                )
                .await
                .expect("checked read must pass")
            );
            assert!(buffer.as_slice()[..4096].iter().all(|byte| *byte == 0x5a));
            assert_eq!(
                buffer.as_slice().as_ptr() as usize,
                handler.read_buffer_address.load(Ordering::Acquire),
                "the handler must fill the ublk queue buffer directly"
            );
            assert_eq!(
                0,
                handle_request(
                    &handler,
                    settings(),
                    request(libublk::sys::UBLK_IO_OP_FLUSH, 0, 0, false),
                    &mut buffer,
                )
                .await
                .expect("flush must pass")
            );
            assert_eq!(
                4096,
                handle_request(
                    &handler,
                    settings(),
                    request(libublk::sys::UBLK_IO_OP_DISCARD, 12_288, 4096, false),
                    &mut buffer,
                )
                .await
                .expect("discard must pass")
            );
            assert_eq!(
                4096,
                handle_request(
                    &handler,
                    settings(),
                    request(libublk::sys::UBLK_IO_OP_WRITE_ZEROES, 16_384, 4096, true,),
                    &mut buffer,
                )
                .await
                .expect("write zeroes must pass")
            );

            assert_eq!(
                vec![
                    SeenRequest::Write(4096, vec![0xa5; 4096], true),
                    SeenRequest::Read(8192, 4096),
                    SeenRequest::Flush,
                    SeenRequest::Discard(12_288, 4096),
                    SeenRequest::WriteZeroes(16_384, 4096, true, true),
                ],
                *handler.requests.lock()
            );
        });
    }

    #[test]
    fn invalid_requests_fail_before_the_handler() {
        smol::block_on(async {
            let handler = RecordingHandler::default();
            let mut buffer = libublk::helpers::IoBuf::<u8>::new(128 * 1024);
            let cases = [
                request(libublk::sys::UBLK_IO_OP_READ, 1, 4096, false),
                request(libublk::sys::UBLK_IO_OP_WRITE, 0, 1, false),
                request(
                    libublk::sys::UBLK_IO_OP_READ,
                    settings().capacity_bytes(),
                    4096,
                    false,
                ),
                request(u32::MAX, 0, 4096, false),
            ];
            for request in cases {
                assert_eq!(
                    BlockIoError::InvalidRequest,
                    handle_request(&handler, settings(), request, &mut buffer)
                        .await
                        .expect_err("invalid request must fail")
                );
            }
            assert!(handler.requests.lock().is_empty());
        });
    }

    #[test]
    fn flush_ignores_the_kernel_unused_sector_value() {
        let request = parse_request(libublk::sys::UBLK_IO_OP_FLUSH, u64::MAX, 0)
            .expect("flush sentinel must parse");
        assert_eq!(libublk::sys::UBLK_IO_OP_FLUSH, request.operation);
        assert_eq!(0, request.offset);
        assert_eq!(0, request.length);
    }

    #[test]
    fn write_zeroes_preserves_the_kernel_discard_choice() {
        let request = parse_request(
            libublk::sys::UBLK_IO_OP_WRITE_ZEROES | libublk::sys::UBLK_IO_F_NOUNMAP,
            0,
            8,
        )
        .expect("write zeroes request must parse");
        assert!(!request.allow_discard);
    }

    #[test]
    fn handler_errors_map_to_deliberate_linux_errors() {
        assert_eq!(-libc::EINVAL, block_error(BlockIoError::InvalidRequest));
        assert_eq!(-libc::ENOSPC, block_error(BlockIoError::OutOfSpace));
        assert_eq!(-libc::EIO, block_error(BlockIoError::NotServing));
        assert_eq!(-libc::EIO, block_error(BlockIoError::failed("test")));
    }
}
