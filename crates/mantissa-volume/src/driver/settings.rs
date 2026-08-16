use thiserror::Error;

use crate::{VolumeBlockSizes, VolumeDescriptor};

const LIBUBLK_MAX_REQUEST_BYTES: u32 = 32 * 1024 * 1024;
const UBLK_MAX_QUEUE_COUNT: u32 = 4096;
const UBLK_MAX_QUEUE_DEPTH: u32 = 4096;

/// Caller-selected queue and memory limits for one ublk device.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UblkQueueSettings {
    /// Number of independent kernel queues.
    pub queue_count: u16,

    /// Largest number of requests held by each queue.
    pub queue_depth: u16,

    /// Largest request buffer allocated for one queue entry.
    pub max_request_bytes: u32,

    /// Largest total allocation allowed for all queue request buffers.
    pub memory_limit_bytes: u64,
}

/// Caller-selected limits for one running block driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverLimitSettings {
    /// ublk queue count, depth, request size, and queue-buffer limit.
    pub queues: UblkQueueSettings,

    /// Largest number of requests waiting for or running on the shared worker.
    pub max_pending_requests: usize,

    /// Largest combined size of request data copied by the block handler.
    pub max_pending_buffer_bytes: usize,
}

/// Checked queue and pending-request limits for one driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverLimits {
    queues: UblkQueueSettings,
    max_pending_requests: usize,
    max_pending_buffer_bytes: usize,
}

impl DriverLimits {
    /// Checks every driver limit before a device or request worker starts.
    pub fn new(settings: DriverLimitSettings) -> Result<Self, InvalidDriverLimits> {
        check_queue_settings(
            settings.queues,
            VolumeBlockSizes::supported().logical_sector().bytes(),
        )?;
        if settings.max_pending_requests == 0 {
            return Err(InvalidDriverLimits::ZeroPendingRequests);
        }
        if settings.max_pending_buffer_bytes < settings.queues.max_request_bytes as usize {
            return Err(InvalidDriverLimits::PendingBufferLimit {
                maximum_request_bytes: settings.queues.max_request_bytes,
                buffer_limit_bytes: settings.max_pending_buffer_bytes,
            });
        }
        Ok(Self {
            queues: settings.queues,
            max_pending_requests: settings.max_pending_requests,
            max_pending_buffer_bytes: settings.max_pending_buffer_bytes,
        })
    }

    /// Builds the exact ublk settings for one checked volume descriptor.
    pub fn ublk_settings(
        self,
        descriptor: &VolumeDescriptor,
    ) -> Result<UblkSettings, InvalidUblkSettings> {
        UblkSettings::new(descriptor, self.queues)
    }

    /// Returns the selected ublk queue settings.
    #[must_use]
    pub const fn queues(self) -> UblkQueueSettings {
        self.queues
    }

    /// Returns the number of requests allowed to wait for or run on the worker.
    #[must_use]
    pub const fn max_pending_requests(self) -> usize {
        self.max_pending_requests
    }

    /// Returns the combined copied-buffer limit.
    #[must_use]
    pub const fn max_pending_buffer_bytes(self) -> usize {
        self.max_pending_buffer_bytes
    }
}

/// Checked capacity, block sizes, and queue limits for one ublk device.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UblkSettings {
    capacity_bytes: u64,
    logical_sector_bytes: u32,
    physical_block_bytes: u32,
    minimum_io_bytes: u32,
    queue_count: u16,
    queue_depth: u16,
    max_request_bytes: u32,
    queue_buffer_bytes: u64,
}

impl UblkSettings {
    /// Checks caller-selected settings against the descriptor and libublk.
    pub fn new(
        descriptor: &VolumeDescriptor,
        queues: UblkQueueSettings,
    ) -> Result<Self, InvalidUblkSettings> {
        let logical_sector_bytes = descriptor.block_sizes().logical_sector().bytes();
        let queue_buffer_bytes = check_queue_settings(queues, logical_sector_bytes)?;

        Ok(Self {
            capacity_bytes: descriptor.capacity().bytes(),
            logical_sector_bytes,
            physical_block_bytes: descriptor.block_sizes().physical_block().bytes(),
            minimum_io_bytes: descriptor.block_sizes().minimum_io().bytes(),
            queue_count: queues.queue_count,
            queue_depth: queues.queue_depth,
            max_request_bytes: queues.max_request_bytes,
            queue_buffer_bytes,
        })
    }

    /// Returns the reported device capacity.
    #[must_use]
    pub const fn capacity_bytes(self) -> u64 {
        self.capacity_bytes
    }

    /// Returns the reported logical-sector size.
    #[must_use]
    pub const fn logical_sector_bytes(self) -> u32 {
        self.logical_sector_bytes
    }

    /// Returns the reported physical-block size.
    #[must_use]
    pub const fn physical_block_bytes(self) -> u32 {
        self.physical_block_bytes
    }

    /// Returns the reported minimum-I/O size.
    #[must_use]
    pub const fn minimum_io_bytes(self) -> u32 {
        self.minimum_io_bytes
    }

    /// Returns the number of kernel queues.
    #[must_use]
    pub const fn queue_count(self) -> u16 {
        self.queue_count
    }

    /// Returns the request depth of each queue.
    #[must_use]
    pub const fn queue_depth(self) -> u16 {
        self.queue_depth
    }

    /// Returns the largest accepted kernel request.
    #[must_use]
    pub const fn max_request_bytes(self) -> u32 {
        self.max_request_bytes
    }

    /// Returns the exact allocation required by all queue buffers.
    #[must_use]
    pub const fn queue_buffer_bytes(self) -> u64 {
        self.queue_buffer_bytes
    }
}

/// Checks queue shape, request alignment, and exact allocated memory.
fn check_queue_settings(
    queues: UblkQueueSettings,
    logical_sector_bytes: u32,
) -> Result<u64, InvalidUblkSettings> {
    if queues.queue_count == 0 {
        return Err(InvalidUblkSettings::ZeroQueueCount);
    }
    if u32::from(queues.queue_count) > UBLK_MAX_QUEUE_COUNT {
        return Err(InvalidUblkSettings::TooManyQueues {
            requested: queues.queue_count,
            maximum: UBLK_MAX_QUEUE_COUNT,
        });
    }
    if queues.queue_depth == 0 {
        return Err(InvalidUblkSettings::ZeroQueueDepth);
    }
    if u32::from(queues.queue_depth) > UBLK_MAX_QUEUE_DEPTH {
        return Err(InvalidUblkSettings::QueueTooDeep {
            requested: queues.queue_depth,
            maximum: UBLK_MAX_QUEUE_DEPTH,
        });
    }

    let host_page_bytes = host_page_bytes()?;
    if queues.max_request_bytes == 0
        || !queues
            .max_request_bytes
            .is_multiple_of(logical_sector_bytes)
    {
        return Err(InvalidUblkSettings::InvalidRequestSize {
            requested_bytes: queues.max_request_bytes,
            alignment_bytes: logical_sector_bytes,
        });
    }
    if !queues.max_request_bytes.is_multiple_of(host_page_bytes) {
        return Err(InvalidUblkSettings::InvalidRequestSize {
            requested_bytes: queues.max_request_bytes,
            alignment_bytes: host_page_bytes,
        });
    }
    if queues.max_request_bytes > LIBUBLK_MAX_REQUEST_BYTES {
        return Err(InvalidUblkSettings::RequestTooLarge {
            requested_bytes: queues.max_request_bytes,
            maximum_bytes: LIBUBLK_MAX_REQUEST_BYTES,
        });
    }

    let queue_buffer_bytes = u64::from(queues.queue_count)
        .checked_mul(u64::from(queues.queue_depth))
        .and_then(|value| value.checked_mul(u64::from(queues.max_request_bytes)))
        .ok_or(InvalidUblkSettings::QueueMemoryOverflow)?;
    if queue_buffer_bytes > queues.memory_limit_bytes {
        return Err(InvalidUblkSettings::QueueMemoryLimit {
            required_bytes: queue_buffer_bytes,
            limit_bytes: queues.memory_limit_bytes,
        });
    }
    Ok(queue_buffer_bytes)
}

/// Rejects unsafe request-worker and queue limits.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InvalidDriverLimits {
    /// The underlying ublk queue settings are invalid.
    #[error(transparent)]
    Ublk(#[from] InvalidUblkSettings),

    /// A bounded worker must accept at least one waiting request.
    #[error("driver pending request count must be greater than zero")]
    ZeroPendingRequests,

    /// One maximum-size request must fit in the copied-buffer limit.
    #[error(
        "driver request size {maximum_request_bytes} exceeds the copied-buffer \
         limit of {buffer_limit_bytes} bytes"
    )]
    PendingBufferLimit {
        /// Largest request accepted from ublk.
        maximum_request_bytes: u32,

        /// Combined copied-buffer limit selected by the caller.
        buffer_limit_bytes: usize,
    },
}

/// Rejects ublk queue settings that are invalid or exceed caller limits.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InvalidUblkSettings {
    /// The saved capacity does not fit the volume's fixed block layout.
    #[error("ublk capacity does not fit the volume block layout")]
    InvalidCapacity,

    /// A device needs at least one queue.
    #[error("ublk queue count must be greater than zero")]
    ZeroQueueCount,

    /// The queue count exceeds the kernel API.
    #[error("ublk queue count {requested} exceeds the kernel maximum of {maximum}")]
    TooManyQueues {
        /// Queue count supplied by the caller.
        requested: u16,

        /// Largest queue count accepted by the ublk API.
        maximum: u32,
    },

    /// Each queue needs at least one request entry.
    #[error("ublk queue depth must be greater than zero")]
    ZeroQueueDepth,

    /// The requested queue depth exceeds the kernel API.
    #[error("ublk queue depth {requested} exceeds the kernel maximum of {maximum}")]
    QueueTooDeep {
        /// Queue depth supplied by the caller.
        requested: u16,

        /// Largest queue depth accepted by the ublk API.
        maximum: u32,
    },

    /// Request buffers must be non-zero whole logical sectors.
    #[error(
        "ublk request size {requested_bytes} is not a non-zero multiple of \
         {alignment_bytes} bytes"
    )]
    InvalidRequestSize {
        /// Request size supplied by the caller.
        requested_bytes: u32,

        /// Logical-sector alignment required by this volume.
        alignment_bytes: u32,
    },

    /// libublk rejects request buffers larger than 32 MiB.
    #[error(
        "ublk request size {requested_bytes} exceeds the libublk maximum of \
         {maximum_bytes} bytes"
    )]
    RequestTooLarge {
        /// Request size supplied by the caller.
        requested_bytes: u32,

        /// Largest request buffer accepted by this libublk version.
        maximum_bytes: u32,
    },

    /// Queue count, depth, and request size overflowed their memory calculation.
    #[error("ublk queue buffer memory calculation overflowed")]
    QueueMemoryOverflow,

    /// Queue buffers exceed the memory limit selected by the caller.
    #[error(
        "ublk queues require {required_bytes} buffer bytes, exceeding the \
         caller limit of {limit_bytes} bytes"
    )]
    QueueMemoryLimit {
        /// Exact queue buffer allocation.
        required_bytes: u64,

        /// Maximum allocation allowed by the caller.
        limit_bytes: u64,
    },

    /// The operating-system page size could not be represented safely.
    #[error("could not read a valid host page size")]
    HostPageSizeUnavailable,
}

/// Returns the page-size alignment required by libublk request buffers.
fn host_page_bytes() -> Result<u32, InvalidUblkSettings> {
    // SAFETY: sysconf reads one process setting and does not use pointers.
    let bytes = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u32::try_from(bytes)
        .ok()
        .filter(|bytes| *bytes != 0)
        .ok_or(InvalidUblkSettings::HostPageSizeUnavailable)
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::{VolumeBlockSizes, VolumeGeneration, VolumeId};

    /// Creates one small valid descriptor for queue-setting tests.
    fn descriptor() -> VolumeDescriptor {
        VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(1)).expect("fixed volume ID must be valid"),
            VolumeGeneration::new(1).expect("fixed generation must be valid"),
            64 * 1024 * 1024,
            VolumeBlockSizes::supported(),
        )
        .expect("fixed descriptor must be valid")
    }

    #[test]
    fn queue_memory_is_exact_and_caller_limited() {
        let queues = UblkQueueSettings {
            queue_count: 2,
            queue_depth: 32,
            max_request_bytes: 128 * 1024,
            memory_limit_bytes: 8 * 1024 * 1024,
        };
        let settings = UblkSettings::new(&descriptor(), queues).expect("settings must fit exactly");
        assert_eq!(8 * 1024 * 1024, settings.queue_buffer_bytes());

        let error = UblkSettings::new(
            &descriptor(),
            UblkQueueSettings {
                memory_limit_bytes: settings.queue_buffer_bytes() - 1,
                ..queues
            },
        )
        .expect_err("one byte below the exact allocation must fail");
        assert_eq!(
            InvalidUblkSettings::QueueMemoryLimit {
                required_bytes: 8 * 1024 * 1024,
                limit_bytes: 8 * 1024 * 1024 - 1,
            },
            error
        );
    }

    #[test]
    fn pending_requests_and_copied_buffers_are_bounded() {
        let queues = UblkQueueSettings {
            queue_count: 2,
            queue_depth: 32,
            max_request_bytes: 128 * 1024,
            memory_limit_bytes: 8 * 1024 * 1024,
        };
        let valid = DriverLimitSettings {
            queues,
            max_pending_requests: 8,
            max_pending_buffer_bytes: 1024 * 1024,
        };
        let limits = DriverLimits::new(valid).expect("driver limits must be valid");
        assert_eq!(8, limits.max_pending_requests());
        assert_eq!(1024 * 1024, limits.max_pending_buffer_bytes());

        assert_eq!(
            InvalidDriverLimits::ZeroPendingRequests,
            DriverLimits::new(DriverLimitSettings {
                max_pending_requests: 0,
                ..valid
            })
            .expect_err("zero waiting requests must fail")
        );
        assert_eq!(
            InvalidDriverLimits::PendingBufferLimit {
                maximum_request_bytes: 128 * 1024,
                buffer_limit_bytes: 128 * 1024 - 1,
            },
            DriverLimits::new(DriverLimitSettings {
                max_pending_buffer_bytes: 128 * 1024 - 1,
                ..valid
            })
            .expect_err("one maximum request must fit")
        );
    }

    #[test]
    fn zero_and_misaligned_queue_values_fail_before_ublk() {
        let valid = UblkQueueSettings {
            queue_count: 1,
            queue_depth: 1,
            max_request_bytes: 4 * 1024,
            memory_limit_bytes: 4 * 1024,
        };
        let cases = [
            (
                UblkQueueSettings {
                    queue_count: 0,
                    ..valid
                },
                InvalidUblkSettings::ZeroQueueCount,
            ),
            (
                UblkQueueSettings {
                    queue_depth: 0,
                    ..valid
                },
                InvalidUblkSettings::ZeroQueueDepth,
            ),
            (
                UblkQueueSettings {
                    max_request_bytes: 4097,
                    memory_limit_bytes: 4097,
                    ..valid
                },
                InvalidUblkSettings::InvalidRequestSize {
                    requested_bytes: 4097,
                    alignment_bytes: 4096,
                },
            ),
        ];

        for (settings, expected) in cases {
            assert_eq!(
                expected,
                UblkSettings::new(&descriptor(), settings)
                    .expect_err("invalid queue settings must fail")
            );
        }
    }
}
