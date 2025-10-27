use redb::{Database, TableDefinition};
use std::{io, sync::Arc};
use uuid::Uuid;

const T_CRED: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("session_credentials_local");

#[inline]
fn ioerr<E: std::error::Error>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

/// Client-side store of short-lived cluster credentials, keyed by remote peer id.
#[derive(Clone)]
pub struct LocalCredentialStore {
    db: Arc<Database>,
}

impl LocalCredentialStore {
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        let w = db.begin_write().map_err(ioerr)?;
        {
            let _ = w.open_table(T_CRED).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;
        Ok(Self { db })
    }

    /// Put/replace credential for `peer`.
    pub fn put(&self, peer: Uuid, cred: &[u8]) -> io::Result<()> {
        let w = self.db.begin_write().map_err(ioerr)?;
        {
            let mut t = w.open_table(T_CRED).map_err(ioerr)?;
            t.insert(*peer.as_bytes(), cred).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;
        Ok(())
    }

    /// Get credential for `peer` (if any).
    #[allow(dead_code)]
    pub fn get(&self, peer: Uuid) -> io::Result<Option<Vec<u8>>> {
        let r = self.db.begin_read().map_err(ioerr)?;
        let t = r.open_table(T_CRED).map_err(ioerr)?;
        let out = t
            .get(*peer.as_bytes())
            .map_err(ioerr)?
            .map(|g| g.value().to_vec());
        Ok(out)
    }

    #[allow(dead_code)]
    pub fn remove(&self, peer: Uuid) -> io::Result<()> {
        let w = self.db.begin_write().map_err(ioerr)?;
        {
            let mut t = w.open_table(T_CRED).map_err(ioerr)?;
            let _ = t.remove(*peer.as_bytes()).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;
        Ok(())
    }
}
