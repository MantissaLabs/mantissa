use std::time::Duration;

use thiserror::Error;

/// Caller-selected limits for one shared Raft TCP transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportLimitSettings {
    /// Largest number of live inbound and outbound connections.
    pub max_connections: usize,

    /// Largest number of queued calls in each priority queue.
    pub max_queued_calls: usize,

    /// Largest combined byte size of calls waiting to be sent.
    pub max_queued_bytes: usize,

    /// Queue bytes kept available for votes and heartbeats.
    pub reserved_vote_and_heartbeat_queue_bytes: usize,

    /// Largest number of queued or running calls for one peer.
    pub max_calls_per_peer: usize,

    /// Active call slots kept available for votes and heartbeats.
    pub reserved_vote_and_heartbeat_calls_per_peer: usize,

    /// Largest data part sent by one internal snapshot call.
    pub max_snapshot_chunk_bytes: usize,

    /// Time allowed to open a TCP connection.
    pub connect_timeout: Duration,

    /// Time allowed to finish a Noise handshake.
    pub handshake_timeout: Duration,

    /// Time allowed for one Raft RPC call.
    pub call_timeout: Duration,

    /// Time allowed to wait for queue space or a per-peer work slot.
    pub queue_timeout: Duration,

    /// Minimum delay between failed connection attempts to one peer.
    pub reconnect_delay: Duration,
}

/// Checked limits used by the transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportLimits {
    settings: TransportLimitSettings,
}

impl TransportLimits {
    /// Checks every limit before the listener or worker thread starts.
    pub fn new(settings: TransportLimitSettings) -> Result<Self, InvalidTransportLimits> {
        let non_zero_counts = [
            ("maximum connections", settings.max_connections),
            ("maximum queued calls", settings.max_queued_calls),
            ("maximum queued bytes", settings.max_queued_bytes),
            (
                "reserved vote and heartbeat queue bytes",
                settings.reserved_vote_and_heartbeat_queue_bytes,
            ),
            ("maximum calls per peer", settings.max_calls_per_peer),
            (
                "reserved vote and heartbeat calls per peer",
                settings.reserved_vote_and_heartbeat_calls_per_peer,
            ),
            (
                "maximum snapshot part bytes",
                settings.max_snapshot_chunk_bytes,
            ),
        ];
        if let Some((name, _)) = non_zero_counts.into_iter().find(|(_, value)| *value == 0) {
            return Err(InvalidTransportLimits::Zero { name });
        }
        if settings.max_queued_bytes > u32::MAX as usize {
            return Err(InvalidTransportLimits::QueuedBytesTooLarge {
                actual: settings.max_queued_bytes,
                maximum: u32::MAX as usize,
            });
        }
        if settings.reserved_vote_and_heartbeat_queue_bytes >= settings.max_queued_bytes {
            return Err(InvalidTransportLimits::NoDataQueueBytes {
                maximum: settings.max_queued_bytes,
                reserved: settings.reserved_vote_and_heartbeat_queue_bytes,
            });
        }
        if settings.reserved_vote_and_heartbeat_calls_per_peer >= settings.max_calls_per_peer {
            return Err(InvalidTransportLimits::NoDataCallSlot {
                maximum: settings.max_calls_per_peer,
                reserved: settings.reserved_vote_and_heartbeat_calls_per_peer,
            });
        }
        let non_zero_times = [
            ("connect timeout", settings.connect_timeout),
            ("handshake timeout", settings.handshake_timeout),
            ("call timeout", settings.call_timeout),
            ("queue timeout", settings.queue_timeout),
            ("reconnect delay", settings.reconnect_delay),
        ];
        if let Some((name, _)) = non_zero_times
            .into_iter()
            .find(|(_, value)| value.is_zero())
        {
            return Err(InvalidTransportLimits::ZeroDuration { name });
        }
        Ok(Self { settings })
    }

    /// Returns the complete checked settings.
    #[must_use]
    pub const fn settings(self) -> TransportLimitSettings {
        self.settings
    }

    /// Returns the live connection limit.
    #[must_use]
    pub const fn max_connections(self) -> usize {
        self.settings.max_connections
    }

    /// Returns the capacity of each call queue.
    #[must_use]
    pub const fn max_queued_calls(self) -> usize {
        self.settings.max_queued_calls
    }

    /// Returns the combined queued-byte limit.
    #[must_use]
    pub const fn max_queued_bytes(self) -> usize {
        self.settings.max_queued_bytes
    }

    /// Returns the queue bytes kept free for votes and heartbeats.
    #[must_use]
    pub const fn reserved_vote_and_heartbeat_queue_bytes(self) -> usize {
        self.settings.reserved_vote_and_heartbeat_queue_bytes
    }

    /// Returns the queued or running call limit for one peer.
    #[must_use]
    pub const fn max_calls_per_peer(self) -> usize {
        self.settings.max_calls_per_peer
    }

    /// Returns the call slots kept free for votes and heartbeats.
    #[must_use]
    pub const fn reserved_vote_and_heartbeat_calls_per_peer(self) -> usize {
        self.settings.reserved_vote_and_heartbeat_calls_per_peer
    }

    /// Returns the largest snapshot data part.
    #[must_use]
    pub const fn max_snapshot_chunk_bytes(self) -> usize {
        self.settings.max_snapshot_chunk_bytes
    }

    /// Returns the TCP connection time limit.
    #[must_use]
    pub const fn connect_timeout(self) -> Duration {
        self.settings.connect_timeout
    }

    /// Returns the Noise handshake time limit.
    #[must_use]
    pub const fn handshake_timeout(self) -> Duration {
        self.settings.handshake_timeout
    }

    /// Returns the time limit for one RPC.
    #[must_use]
    pub const fn call_timeout(self) -> Duration {
        self.settings.call_timeout
    }

    /// Returns the time limit for waiting on bounded local work.
    #[must_use]
    pub const fn queue_timeout(self) -> Duration {
        self.settings.queue_timeout
    }

    /// Returns the minimum delay between failed connection attempts.
    #[must_use]
    pub const fn reconnect_delay(self) -> Duration {
        self.settings.reconnect_delay
    }
}

/// Explains why transport limits cannot be used.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InvalidTransportLimits {
    /// A count or byte limit must be greater than zero.
    #[error("{name} must be greater than zero")]
    Zero {
        /// Setting that was zero.
        name: &'static str,
    },

    /// Tokio byte permits use a 32-bit count.
    #[error("maximum queued bytes {actual} exceeds {maximum}")]
    QueuedBytesTooLarge {
        /// Requested byte limit.
        actual: usize,

        /// Largest supported byte limit.
        maximum: usize,
    },

    /// Reserving every call slot would prevent log and snapshot transfer.
    #[error(
        "{reserved} of {maximum} calls are reserved for votes and \
         heartbeats; at least one data call slot is required"
    )]
    NoDataCallSlot {
        /// Largest number of queued or running calls to one peer.
        maximum: usize,

        /// Calls reserved for votes and heartbeats.
        reserved: usize,
    },

    /// Reserving every queue byte would prevent log and snapshot transfer.
    #[error(
        "{reserved} of {maximum} queue bytes are reserved for votes and \
         heartbeats; at least one data byte is required"
    )]
    NoDataQueueBytes {
        /// Largest queued byte count.
        maximum: usize,

        /// Queue bytes reserved for votes and heartbeats.
        reserved: usize,
    },

    /// A zero time limit would reject every operation.
    #[error("{name} must be greater than zero")]
    ZeroDuration {
        /// Setting that was zero.
        name: &'static str,
    },
}
