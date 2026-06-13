use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use redb::{Database, TableDefinition};
use std::{io, sync::Arc};

/// Dedicated table for the persisted local REST bearer token.
const T_REST_TOKEN: TableDefinition<&'static str, &'static str> =
    TableDefinition::new("rest_token_local");

/// Durable store for the node-local REST bearer token.
#[derive(Clone)]
pub struct LocalRestTokenStore {
    db: Arc<Database>,
}

impl LocalRestTokenStore {
    /// Creates the REST token table if it does not already exist.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        with_write_tx(&db, |tx| {
            let _ = tx.open_table(T_REST_TOKEN).map_err(into_io)?;
            Ok(())
        })?;
        Ok(Self { db })
    }

    /// Reads the current REST token if one has been persisted.
    pub fn read(&self) -> io::Result<Option<String>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_REST_TOKEN).map_err(into_io)?;
            let token = table.get("rest_token").map_err(into_io)?;
            Ok(token.map(|value| value.value().to_string()))
        })
    }

    /// Writes or replaces the current REST token atomically.
    pub fn write(&self, token: &str) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_REST_TOKEN).map_err(into_io)?;
            table.insert("rest_token", token).map_err(into_io)?;
            Ok(())
        })
    }
}
