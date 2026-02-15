use serde::{Deserialize, Serialize};

/// Lifecycle state for a task container or micro VM.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ContainerState {
    Pending,
    Pulling,
    Creating,
    Running,
    Paused,
    Stopping,
    Stopped,
    Failed,
    Exited(i32),
    Unknown,
}
