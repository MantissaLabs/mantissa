use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use redb::{Database, TableDefinition};
use std::{io, sync::Arc};

/// A dedicated table for the persisted join token.
const T_TOKEN: TableDefinition<&'static str, &'static str> =
    TableDefinition::new("join_token_local");

/// Durable store for the cluster join token. Very small, single-row.
#[derive(Clone)]
pub struct LocalTokenStore {
    db: Arc<Database>,
}

impl LocalTokenStore {
    /// Create the table if missing.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        with_write_tx(&db, |tx| {
            // `open_table` will create-if-missing (consistent with the rest of your stores)
            let _ = tx.open_table(T_TOKEN).map_err(into_io)?;
            Ok(())
        })?;
        Ok(Self { db })
    }

    /// Read the token if present.
    pub fn read(&self) -> io::Result<Option<String>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_TOKEN).map_err(into_io)?;
            let got = table.get("join_token").map_err(into_io)?;
            Ok(got.map(|value| value.value().to_string()))
        })
    }

    /// Write or overwrite the token atomically.
    pub fn write(&self, token: &str) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_TOKEN).map_err(into_io)?;
            table.insert("join_token", token).map_err(into_io)?;
            Ok(())
        })
    }
}
