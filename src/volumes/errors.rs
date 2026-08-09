use thiserror::Error;

/// Recoverable volume access failure that blocks task launch or recovery.
#[derive(Debug, Error)]
pub enum VolumeAccessError {
    #[error("{message}")]
    Unavailable { message: String },
}

impl VolumeAccessError {
    /// Builds one recoverable volume-unavailable error from an operator-facing message.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }
}
