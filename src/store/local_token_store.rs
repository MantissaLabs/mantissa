use redb::{Database, TableDefinition};
use std::{io, sync::Arc};

/// A dedicated table for the persisted join token.
const T_TOKEN: TableDefinition<&'static str, &'static str> =
    TableDefinition::new("join_token_local");

#[inline]
fn ioerr<E: std::error::Error>(err: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err.to_string())
}

/// Durable store for the cluster join token. Very small, single-row.
#[derive(Clone)]
pub struct LocalTokenStore {
    db: Arc<Database>,
}

impl LocalTokenStore {
    /// Create the table if missing.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        let write_transaction = db.begin_write().map_err(ioerr)?;
        {
            // `open_table` will create-if-missing (consistent with the rest of your stores)
            let _ = write_transaction.open_table(T_TOKEN).map_err(ioerr)?;
        }
        write_transaction.commit().map_err(ioerr)?;
        Ok(Self { db })
    }

    /// Read the token if present.
    pub fn read(&self) -> io::Result<Option<String>> {
        let read_transaction = self.db.begin_read().map_err(ioerr)?;
        let table = read_transaction.open_table(T_TOKEN).map_err(ioerr)?;
        let got = table.get("join_token").map_err(ioerr)?;
        Ok(got.map(|x| x.value().to_string()))
    }

    /// Write or overwrite the token atomically.
    pub fn write(&self, token: &str) -> io::Result<()> {
        let write_transaction = self.db.begin_write().map_err(ioerr)?;
        {
            let mut table = write_transaction.open_table(T_TOKEN).map_err(ioerr)?;
            table.insert("join_token", token).map_err(ioerr)?;
        }
        write_transaction.commit().map_err(ioerr)
    }
}
