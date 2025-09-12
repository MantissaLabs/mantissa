use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("redb error: {0}")]
    Redb(#[from] redb::Error),
    #[error("serialize error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("invalid key bytes: {0}")]
    InvalidKey(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("other: {0}")]
    Other(String),
}

impl From<crate::uuid_key::UuidKeyParseError> for Error {
    fn from(_: crate::uuid_key::UuidKeyParseError) -> Self {
        Error::InvalidKey("UuidKey: expected 16 bytes".to_string())
    }
}

// redb granular error conversions (map to stringy Other for now)
impl From<redb::StorageError> for Error {
    fn from(e: redb::StorageError) -> Self {
        Error::Other(e.to_string())
    }
}

impl From<redb::TransactionError> for Error {
    fn from(e: redb::TransactionError) -> Self {
        Error::Other(e.to_string())
    }
}

impl From<redb::TableError> for Error {
    fn from(e: redb::TableError) -> Self {
        Error::Other(e.to_string())
    }
}

impl From<redb::CommitError> for Error {
    fn from(e: redb::CommitError) -> Self {
        Error::Other(e.to_string())
    }
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
    }
}
