use std::error::Error;
use std::future::Future;

/// One running group that can be stopped without taking ownership from users.
pub trait RunningGroup: Send + Sync + 'static {
    /// Failure returned while stopping this group.
    type Error: Error + Send + Sync + 'static;

    /// Stops the group and joins every task that belongs to it.
    ///
    /// The call must be safe to repeat. After it returns, including on error,
    /// the group must reject new work.
    fn shutdown(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Starts one application group while owning shared transport and storage setup.
///
/// One starter instance is reused by every group in the runtime. A later
/// network starter can therefore keep one connection pool for all groups.
pub trait GroupStarter<GID>: Send + Sync + 'static {
    /// Live group returned after a successful start.
    type Group: RunningGroup<Error = Self::Error>;

    /// Failure returned while starting or stopping a group.
    type Error: Error + Send + Sync + 'static;

    /// Opens all application and Raft state for one durable group.
    ///
    /// The runtime owns this future after accepting a start. Cancelling an
    /// individual `activate` waiter therefore never cancels this operation or
    /// hides its partially opened resources from shutdown.
    fn start(&self, group_id: GID)
    -> impl Future<Output = Result<Self::Group, Self::Error>> + Send;
}

/// Counts durable and live resources owned by one group runtime.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeMetrics {
    /// Durable catalog rows, including groups that use no runtime resources.
    pub saved_groups: u64,

    /// Groups with a live handle.
    pub active_groups: usize,

    /// Groups currently being opened.
    pub starting_groups: usize,

    /// Groups currently being stopped.
    pub stopping_groups: usize,

    /// Small state entries created only for groups used since process start.
    pub group_entries: usize,

    /// Shared background jobs currently running.
    pub background_jobs: usize,
}
