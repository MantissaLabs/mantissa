use redb::{Database, ReadTransaction, ReadableDatabase, WriteTransaction};
use std::io;

/// Maps Redb and serialization errors into `io::Error` for store APIs.
pub(crate) fn into_io<E: std::error::Error>(err: E) -> io::Error {
    io::Error::other(err.to_string())
}

/// Opens a read transaction and executes `op` within that transaction scope.
pub(crate) fn with_read_tx<T>(
    db: &Database,
    op: impl FnOnce(&ReadTransaction) -> io::Result<T>,
) -> io::Result<T> {
    let tx = db.begin_read().map_err(into_io)?;
    op(&tx)
}

/// Opens a write transaction, executes `op`, and commits if `op` succeeded.
pub(crate) fn with_write_tx<T>(
    db: &Database,
    op: impl FnOnce(&WriteTransaction) -> io::Result<T>,
) -> io::Result<T> {
    let tx = db.begin_write().map_err(into_io)?;
    let out = op(&tx)?;
    tx.commit().map_err(into_io)?;
    Ok(out)
}
