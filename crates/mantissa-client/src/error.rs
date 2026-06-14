use std::fmt;

/// Stable classification for failures returned by reusable client operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientErrorKind {
    InvalidRequest,
    NotFound,
    Conflict,
    OperationFailed,
}

/// Typed client failure with a stable classification and human-readable detail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientError {
    kind: ClientErrorKind,
    message: String,
}

impl ClientError {
    /// Builds a typed client error from a known classification.
    pub fn new(kind: ClientErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Preserves display detail from an underlying client failure.
    pub fn from_display(kind: ClientErrorKind, error: impl fmt::Display) -> Self {
        Self::new(kind, format!("{error:#}"))
    }

    /// Returns the stable classification for callers that map to other APIs.
    pub fn kind(&self) -> ClientErrorKind {
        self.kind
    }
}

impl fmt::Display for ClientError {
    /// Formats the original client-facing failure detail.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for ClientError {}
