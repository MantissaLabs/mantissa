use thiserror::Error;

/// Caller-selected limits for encrypted Raft log segments.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogLimitSettings {
    /// Largest complete frame, including framing and authentication data.
    pub max_frame_bytes: usize,

    /// Largest segment file before a new segment is created.
    pub max_segment_bytes: u64,
}

/// Validated size limits with no built-in storage defaults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogLimits {
    max_frame_bytes: usize,
    max_segment_bytes: u64,
}

impl LogLimits {
    /// Validates caller-selected frame and segment limits.
    pub fn new(settings: LogLimitSettings) -> Result<Self, InvalidLogLimits> {
        if settings.max_frame_bytes == 0 {
            return Err(InvalidLogLimits::ZeroFrameSize);
        }
        if settings.max_frame_bytes > u32::MAX as usize {
            return Err(InvalidLogLimits::FrameSizeTooLarge {
                actual: settings.max_frame_bytes,
            });
        }
        if settings.max_segment_bytes == 0 {
            return Err(InvalidLogLimits::ZeroSegmentSize);
        }
        if settings.max_segment_bytes < settings.max_frame_bytes as u64 {
            return Err(InvalidLogLimits::SegmentBelowFrame {
                frame_bytes: settings.max_frame_bytes,
                segment_bytes: settings.max_segment_bytes,
            });
        }

        Ok(Self {
            max_frame_bytes: settings.max_frame_bytes,
            max_segment_bytes: settings.max_segment_bytes,
        })
    }

    /// Returns the largest complete frame.
    #[must_use]
    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }

    /// Returns the largest complete segment.
    #[must_use]
    pub const fn max_segment_bytes(self) -> u64 {
        self.max_segment_bytes
    }
}

/// Explains why encrypted log limits cannot be used.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InvalidLogLimits {
    /// A frame limit must permit at least one byte.
    #[error("maximum Raft log frame size must be greater than zero")]
    ZeroFrameSize,

    /// Frame lengths are stored as unsigned 32-bit values.
    #[error("maximum Raft log frame size {actual} exceeds u32::MAX")]
    FrameSizeTooLarge {
        /// Caller-selected frame limit.
        actual: usize,
    },

    /// A segment limit must permit at least one byte.
    #[error("maximum Raft log segment size must be greater than zero")]
    ZeroSegmentSize,

    /// A segment cannot be smaller than one frame before header bytes are added.
    #[error(
        "maximum Raft log segment size {segment_bytes} is below frame size \
         {frame_bytes}"
    )]
    SegmentBelowFrame {
        /// Caller-selected frame limit.
        frame_bytes: usize,

        /// Caller-selected segment limit.
        segment_bytes: u64,
    },
}
