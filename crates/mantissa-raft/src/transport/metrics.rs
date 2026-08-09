use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Current connection, call, and queue counts for one TCP transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportMetrics {
    /// Live inbound and outbound authenticated connections.
    pub active_connections: usize,

    /// Largest live connection count observed since startup.
    pub peak_connections: usize,

    /// TCP connection attempts started by the dialer.
    pub connection_attempts: u64,

    /// Calls currently running on the transport thread.
    pub active_calls: usize,

    /// Largest active call count observed since startup.
    pub peak_calls: usize,

    /// Bytes currently waiting in an outbound priority queue.
    pub queued_bytes: usize,
}

pub(super) struct MetricState {
    active_connections: AtomicUsize,
    peak_connections: AtomicUsize,
    connection_attempts: AtomicU64,
    active_calls: AtomicUsize,
    peak_calls: AtomicUsize,
    queued_bytes: AtomicUsize,
}

impl MetricState {
    /// Creates zeroed counters shared by the caller and transport thread.
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            active_connections: AtomicUsize::new(0),
            peak_connections: AtomicUsize::new(0),
            connection_attempts: AtomicU64::new(0),
            active_calls: AtomicUsize::new(0),
            peak_calls: AtomicUsize::new(0),
            queued_bytes: AtomicUsize::new(0),
        })
    }

    /// Returns one stable point-in-time copy of all counters.
    pub(super) fn snapshot(&self) -> TransportMetrics {
        TransportMetrics {
            active_connections: self.active_connections.load(Ordering::Acquire),
            peak_connections: self.peak_connections.load(Ordering::Acquire),
            connection_attempts: self.connection_attempts.load(Ordering::Acquire),
            active_calls: self.active_calls.load(Ordering::Acquire),
            peak_calls: self.peak_calls.load(Ordering::Acquire),
            queued_bytes: self.queued_bytes.load(Ordering::Acquire),
        }
    }

    /// Records one new authenticated connection.
    pub(super) fn connection_started(&self) {
        let active = self.active_connections.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak_connections.fetch_max(active, Ordering::AcqRel);
    }

    /// Records one closed authenticated connection.
    pub(super) fn connection_stopped(&self) {
        self.active_connections.fetch_sub(1, Ordering::AcqRel);
    }

    /// Counts one outbound TCP connection attempt.
    pub(super) fn connection_attempted(&self) {
        self.connection_attempts.fetch_add(1, Ordering::AcqRel);
    }

    /// Records one RPC call started by the transport thread.
    pub(super) fn call_started(&self) {
        let active = self.active_calls.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak_calls.fetch_max(active, Ordering::AcqRel);
    }

    /// Records one completed or cancelled RPC call.
    pub(super) fn call_stopped(&self) {
        self.active_calls.fetch_sub(1, Ordering::AcqRel);
    }

    /// Adds one request to the queued-byte count.
    pub(super) fn request_queued(&self, bytes: usize) {
        self.queued_bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    /// Removes one started or cancelled request from the queued-byte count.
    pub(super) fn request_dequeued(&self, bytes: usize) {
        self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }
}
