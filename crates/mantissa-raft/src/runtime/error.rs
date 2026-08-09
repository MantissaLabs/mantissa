use std::error::Error;
use std::time::Duration;

use thiserror::Error;

use crate::catalog::CatalogError;

/// Reports a failure while starting, stopping, or finding a local Raft group.
#[derive(Debug, Error)]
pub enum RuntimeError<E>
where
    E: Error + Send + Sync + 'static,
{
    /// The durable group catalog could not be read or changed.
    #[error(transparent)]
    Catalog(#[from] CatalogError),

    /// The runtime has started shutting down and accepts no new work.
    #[error("the Raft group runtime is shutting down")]
    ShuttingDown,

    /// The requested group is not running on this node.
    #[error("the requested Raft group is not running")]
    GroupNotRunning,

    /// The caller-selected active group limit has been reached.
    #[error("the active Raft group limit of {maximum} has been reached")]
    ActiveGroupLimit {
        /// Largest number of groups allowed to run at once.
        maximum: usize,
    },

    /// The application could not start one group.
    #[error("could not start Raft group")]
    Start(#[source] E),

    /// One running group could not stop cleanly.
    #[error("could not stop Raft group")]
    Stop(#[source] E),

    /// Current work and groups did not stop before the supplied deadline.
    #[error(
        "Raft runtime did not stop within {timeout:?}; {active_groups} groups \
         and {background_jobs} background jobs remain"
    )]
    ShutdownTimeout {
        /// Complete deadline supplied by the caller.
        timeout: Duration,

        /// Groups still starting, running, or stopping.
        active_groups: usize,

        /// Background jobs that have not finished.
        background_jobs: usize,
    },
}
