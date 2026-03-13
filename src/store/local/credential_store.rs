use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use redb::{Database, TableDefinition};
use std::{io, sync::Arc};
use uuid::Uuid;

const T_CRED: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("session_credentials_local");

/// Client-side store of short-lived cluster credentials, keyed by remote peer id.
#[derive(Clone)]
pub struct LocalCredentialStore {
    db: Arc<Database>,
}

impl LocalCredentialStore {
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        with_write_tx(&db, |tx| {
            let _ = tx.open_table(T_CRED).map_err(into_io)?;
            Ok(())
        })?;
        Ok(Self { db })
    }

    /// Put/replace credential for `peer`.
    pub fn put(&self, peer: Uuid, cred: &[u8]) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_CRED).map_err(into_io)?;
            table.insert(*peer.as_bytes(), cred).map_err(into_io)?;
            Ok(())
        })
    }

    /// Get credential for `peer` (if any).
    #[allow(dead_code)]
    pub fn get(&self, peer: Uuid) -> io::Result<Option<Vec<u8>>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_CRED).map_err(into_io)?;
            let out = table
                .get(*peer.as_bytes())
                .map_err(into_io)?
                .map(|guard| guard.value().to_vec());
            Ok(out)
        })
    }

    #[allow(dead_code)]
    pub fn remove(&self, peer: Uuid) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_CRED).map_err(into_io)?;
            let _ = table.remove(*peer.as_bytes()).map_err(into_io)?;
            Ok(())
        })
    }
}
