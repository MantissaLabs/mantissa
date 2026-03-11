use thiserror::Error;

/// Structured local-volume access failures that block task launch or recovery.
#[derive(Debug, Error)]
pub enum LocalVolumeAccessError {
    #[error("{message}")]
    Unavailable { message: String },
}

impl LocalVolumeAccessError {
    /// Builds one recoverable volume-unavailable error from an operator-facing message.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }
}
