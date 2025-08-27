use redb::{Database, ReadableTable, TableDefinition};
use std::{io, sync::Arc};
use uuid::Uuid;

/// KV table: key = remote peer UUID (16 bytes), value = opaque ticket bytes.
/// We store variable-length bytes using redb's `&'static [u8]` value type.
const T_SESS: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("session_tickets_local");

#[inline]
fn ioerr<E: std::error::Error>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

/// Client-side store of resume tickets, keyed by remote peer id.
/// Use this on the **joining node** to remember tickets per anchor/peer.
#[derive(Clone)]
pub struct LocalSessionStore {
    db: Arc<Database>,
}

impl LocalSessionStore {
    /// Ensure the table exists.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        let w = db.begin_write().map_err(ioerr)?;
        {
            let _ = w.open_table(T_SESS).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;
        Ok(Self { db })
    }

    /// Put/replace ticket for `peer`.
    pub fn put(&self, peer: Uuid, ticket: &[u8]) -> io::Result<()> {
        let w = self.db.begin_write().map_err(ioerr)?;
        {
            let mut t = w.open_table(T_SESS).map_err(ioerr)?;
            t.insert(*peer.as_bytes(), ticket).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;
        Ok(())
    }

    /// Get ticket for `peer` (if any).
    pub fn get(&self, peer: Uuid) -> io::Result<Option<Vec<u8>>> {
        let r = self.db.begin_read().map_err(ioerr)?;
        let t = r.open_table(T_SESS).map_err(ioerr)?;
        let out = match t.get(*peer.as_bytes()).map_err(ioerr)? {
            Some(g) => Some(g.value().to_vec()),
            None => None,
        };
        Ok(out)
    }

    /// Remove ticket for `peer`.
    pub fn remove(&self, peer: Uuid) -> io::Result<()> {
        let w = self.db.begin_write().map_err(ioerr)?;
        {
            let mut t = w.open_table(T_SESS).map_err(ioerr)?;
            let _ = t.remove(*peer.as_bytes()).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;
        Ok(())
    }

    /// (Optional) List all (peer, ticket) pairs — handy for debugging/resume-all.
    pub fn list(&self) -> io::Result<Vec<(Uuid, Vec<u8>)>> {
        let r = self.db.begin_read().map_err(ioerr)?;
        let t = r.open_table(T_SESS).map_err(ioerr)?;
        let mut out = Vec::new();
        let mut it = t.iter().map_err(ioerr)?;
        while let Some(Ok((k, v))) = it.next() {
            let peer = Uuid::from_bytes(k.value());
            out.push((peer, v.value().to_vec()));
        }
        Ok(out)
    }
}
