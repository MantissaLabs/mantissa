use redb::{Database, TableDefinition};
use std::{io, sync::Arc};
use uuid::Uuid;

const T_TICKETS: TableDefinition<&'static [u8], [u8; 16]> = TableDefinition::new("session_tickets"); // ticket -> peer uuid bytes (fixed 16)
const T_REVERSE: TableDefinition<[u8; 16], &'static [u8]> = TableDefinition::new("peer_to_ticket"); // peer uuid bytes -> ticket (bytes)

#[inline]
fn ioerr<E: std::error::Error>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

pub struct AuthStore {
    db: Arc<Database>,
}

impl AuthStore {
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        let w = db.begin_write().map_err(ioerr)?;
        {
            let _ = w.open_table(T_TICKETS).map_err(ioerr)?;
            let _ = w.open_table(T_REVERSE).map_err(ioerr)?;
        } // drop tables before commit
        w.commit().map_err(ioerr)?;
        Ok(Self { db })
    }

    /// Issue a random 32-byte session ticket for `peer` and persist both maps.
    pub fn issue_ticket(&self, peer: Uuid) -> io::Result<Vec<u8>> {
        use getrandom::getrandom;

        let mut ticket = vec![0u8; 32];
        getrandom(&mut ticket)?;

        let w = self.db.begin_write().map_err(ioerr)?;
        {
            // ticket -> peer
            let mut t = w.open_table(T_TICKETS).map_err(ioerr)?;
            t.insert(ticket.as_slice(), *peer.as_bytes())
                .map_err(ioerr)?;
        }
        {
            // peer -> ticket
            let mut r = w.open_table(T_REVERSE).map_err(ioerr)?;
            r.insert(*peer.as_bytes(), ticket.as_slice())
                .map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;

        Ok(ticket)
    }

    /// Resolve a ticket to its peer UUID, if it exists.
    pub fn lookup(&self, ticket: &[u8]) -> io::Result<Option<Uuid>> {
        let r = self.db.begin_read().map_err(ioerr)?;
        let t = r.open_table(T_TICKETS).map_err(ioerr)?;
        let out = match t.get(ticket).map_err(ioerr)? {
            // g.value() returns [u8;16] by value for fixed-size types.
            Some(g) => Some(Uuid::from_bytes(g.value())),
            None => None,
        };
        Ok(out)
    }

    /// Revoke by peer UUID: remove reverse mapping and then forward mapping.
    pub fn revoke_by_peer(&self, peer: Uuid) -> io::Result<()> {
        let w = self.db.begin_write().map_err(ioerr)?;

        // 1) peer -> ticket: remove, copy ticket bytes, drop guard & table
        let ticket_opt: Option<Vec<u8>> = {
            let mut rev = w.open_table(T_REVERSE).map_err(ioerr)?;
            let key: [u8; 16] = *peer.as_bytes();
            // inner scope ensures AccessGuard is dropped before `rev`
            let out = {
                let removed = rev.remove(&key).map_err(ioerr)?;
                removed.map(|g| g.value().to_vec()) // copy &[u8] -> Vec<u8>
            };
            out
        };

        // 2) ticket -> peer: remove using the copied ticket bytes
        if let Some(ticket) = ticket_opt {
            let mut fwd = w.open_table(T_TICKETS).map_err(ioerr)?;
            let _ = fwd.remove(ticket.as_slice()).map_err(ioerr)?;
        }

        w.commit().map_err(ioerr)?;
        Ok(())
    }

    /// Revoke by ticket: remove forward mapping and then reverse mapping.
    pub fn revoke_by_ticket(&self, ticket: &[u8]) -> io::Result<()> {
        let w = self.db.begin_write().map_err(ioerr)?;

        // 1) ticket -> peer: remove, copy peer bytes, drop guard & table
        let peer_opt: Option<[u8; 16]> = {
            let mut fwd = w.open_table(T_TICKETS).map_err(ioerr)?;
            let out = {
                let removed = fwd.remove(ticket).map_err(ioerr)?;
                removed.map(|g| g.value()) // returns [u8;16] by value
            };
            out
        };

        // 2) peer -> ticket: remove using the copied peer bytes
        if let Some(peer_arr) = peer_opt {
            let mut rev = w.open_table(T_REVERSE).map_err(ioerr)?;
            let _ = rev.remove(&peer_arr).map_err(ioerr)?;
        }

        w.commit().map_err(ioerr)?;
        Ok(())
    }
}
