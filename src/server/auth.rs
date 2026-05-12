use crate::{config::DEFAULT_SESSION_TICKET_TTL_SECS, crypto::rand};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::{
    io,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

const T_TICKETS: TableDefinition<&'static [u8], &'static [u8]> =
    TableDefinition::new("session_ticket_records");
const T_REVERSE: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("peer_to_session_ticket");
const SERVER_TICKET_RECORD_LEN: usize = 32;

/// Convert one Redb error into the store's I/O error surface.
#[inline]
fn ioerr<E: std::error::Error>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

/// Return the current Unix time used for ticket expiry comparisons.
fn now_secs() -> io::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_secs())
        .map_err(|err| io::Error::other(format!("system clock before unix epoch: {err}")))
}

/// Durable server-side metadata bound to one opaque bearer ticket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TicketRecord {
    peer: Uuid,
    issued_at: u64,
    expires_at: u64,
}

impl TicketRecord {
    /// Encode the ticket record as a fixed-width durable table value.
    fn encode(self) -> [u8; SERVER_TICKET_RECORD_LEN] {
        let mut out = [0u8; SERVER_TICKET_RECORD_LEN];
        out[..16].copy_from_slice(self.peer.as_bytes());
        out[16..24].copy_from_slice(&self.issued_at.to_be_bytes());
        out[24..32].copy_from_slice(&self.expires_at.to_be_bytes());
        out
    }

    /// Decode the ticket record from the fixed-width durable table value.
    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != SERVER_TICKET_RECORD_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "session ticket record length invalid",
            ));
        }

        let mut peer = [0u8; 16];
        peer.copy_from_slice(&bytes[..16]);

        let mut issued_at = [0u8; 8];
        issued_at.copy_from_slice(&bytes[16..24]);

        let mut expires_at = [0u8; 8];
        expires_at.copy_from_slice(&bytes[24..32]);

        Ok(Self {
            peer: Uuid::from_bytes(peer),
            issued_at: u64::from_be_bytes(issued_at),
            expires_at: u64::from_be_bytes(expires_at),
        })
    }

    /// Return true once the record has reached its absolute expiry timestamp.
    fn is_expired(self, now: u64) -> bool {
        self.expires_at <= now
    }
}

/// Session ticket issued by the server authority with its absolute expiry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IssuedSessionTicket {
    pub ticket: Vec<u8>,
    pub expires_at_unix_secs: u64,
}

/// Durable server-side authority for peer session bearer tickets.
#[derive(Clone)]
pub struct AuthStore {
    db: Arc<Database>,
    ticket_ttl_secs: u64,
}

impl AuthStore {
    /// Opens the auth tables with the default durable ticket lifetime.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        Self::with_ticket_ttl(db, DEFAULT_SESSION_TICKET_TTL_SECS)
    }

    /// Opens the auth tables with an explicit durable ticket lifetime.
    pub fn with_ticket_ttl(db: Arc<Database>, ticket_ttl_secs: u64) -> io::Result<Self> {
        if ticket_ttl_secs == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session ticket ttl must be greater than zero",
            ));
        }

        let w = db.begin_write().map_err(ioerr)?;
        {
            let _ = w.open_table(T_TICKETS).map_err(ioerr)?;
            let _ = w.open_table(T_REVERSE).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;

        let store = Self {
            db,
            ticket_ttl_secs,
        };
        store.reap_expired()?;
        Ok(store)
    }

    /// Issue a random 32-byte session ticket for `peer` and persist both maps.
    pub fn issue_ticket(&self, peer: Uuid) -> io::Result<IssuedSessionTicket> {
        let ticket = rand::random_vec(32)?;
        let issued_at = now_secs()?;
        let expires_at = issued_at
            .checked_add(self.ticket_ttl_secs)
            .ok_or_else(|| io::Error::other("session ticket expiry overflow"))?;
        let record = TicketRecord {
            peer,
            issued_at,
            expires_at,
        };
        let record_bytes = record.encode();

        let w = self.db.begin_write().map_err(ioerr)?;
        let previous_ticket = {
            let rev = w.open_table(T_REVERSE).map_err(ioerr)?;
            rev.get(*peer.as_bytes())
                .map_err(ioerr)?
                .map(|guard| guard.value().to_vec())
        };

        {
            let mut fwd = w.open_table(T_TICKETS).map_err(ioerr)?;
            if let Some(previous) = previous_ticket {
                let _ = fwd.remove(previous.as_slice()).map_err(ioerr)?;
            }
            fwd.insert(ticket.as_slice(), record_bytes.as_slice())
                .map_err(ioerr)?;
        }

        {
            let mut rev = w.open_table(T_REVERSE).map_err(ioerr)?;
            rev.insert(*peer.as_bytes(), ticket.as_slice())
                .map_err(ioerr)?;
        }

        w.commit().map_err(ioerr)?;

        crate::observability::metrics::record_session_ticket_event("issued");
        Ok(IssuedSessionTicket {
            ticket,
            expires_at_unix_secs: expires_at,
        })
    }

    /// Return the current non-expired ticket for `peer`, issuing one only when needed.
    ///
    /// Peer session bootstrap can be requested concurrently by sync, gossip, and join catch-up.
    /// Reusing the current ticket keeps session creation idempotent while preserving the
    /// one-active-ticket-per-peer authority model.
    pub fn get_or_issue_ticket(&self, peer: Uuid) -> io::Result<IssuedSessionTicket> {
        let now = now_secs()?;
        let w = self.db.begin_write().map_err(ioerr)?;
        let mut replaced_expired = false;
        let previous_ticket = {
            let rev = w.open_table(T_REVERSE).map_err(ioerr)?;
            rev.get(*peer.as_bytes())
                .map_err(ioerr)?
                .map(|guard| guard.value().to_vec())
        };

        if let Some(ticket) = previous_ticket.as_ref() {
            let current = {
                let fwd = w.open_table(T_TICKETS).map_err(ioerr)?;
                fwd.get(ticket.as_slice())
                    .map_err(ioerr)?
                    .map(|guard| TicketRecord::decode(guard.value()))
                    .transpose()?
            };

            if let Some(record) = current {
                if record.peer == peer && !record.is_expired(now) {
                    w.commit().map_err(ioerr)?;
                    crate::observability::metrics::record_session_ticket_event("reused");
                    return Ok(IssuedSessionTicket {
                        ticket: ticket.clone(),
                        expires_at_unix_secs: record.expires_at,
                    });
                }

                if record.peer == peer || record.is_expired(now) {
                    let mut fwd = w.open_table(T_TICKETS).map_err(ioerr)?;
                    let _ = fwd.remove(ticket.as_slice()).map_err(ioerr)?;
                    if record.peer == peer && record.is_expired(now) {
                        replaced_expired = true;
                    }
                }
            }

            let mut rev = w.open_table(T_REVERSE).map_err(ioerr)?;
            let _ = rev.remove(*peer.as_bytes()).map_err(ioerr)?;
        }

        let ticket = rand::random_vec(32)?;
        let expires_at = now
            .checked_add(self.ticket_ttl_secs)
            .ok_or_else(|| io::Error::other("session ticket expiry overflow"))?;
        let record = TicketRecord {
            peer,
            issued_at: now,
            expires_at,
        };
        let record_bytes = record.encode();

        {
            let mut fwd = w.open_table(T_TICKETS).map_err(ioerr)?;
            fwd.insert(ticket.as_slice(), record_bytes.as_slice())
                .map_err(ioerr)?;
        }
        {
            let mut rev = w.open_table(T_REVERSE).map_err(ioerr)?;
            rev.insert(*peer.as_bytes(), ticket.as_slice())
                .map_err(ioerr)?;
        }

        w.commit().map_err(ioerr)?;

        if replaced_expired {
            crate::observability::metrics::record_session_ticket_event("expired");
        }
        crate::observability::metrics::record_session_ticket_event("issued");
        Ok(IssuedSessionTicket {
            ticket,
            expires_at_unix_secs: expires_at,
        })
    }

    /// Resolve a non-expired ticket to its peer UUID.
    pub fn lookup(&self, ticket: &[u8]) -> io::Result<Option<Uuid>> {
        let Some(record) = self.load_ticket_record(ticket)? else {
            return Ok(None);
        };

        if record.is_expired(now_secs()?) {
            if self.remove_ticket(ticket)? {
                crate::observability::metrics::record_session_ticket_event("expired");
            }
            return Ok(None);
        }

        Ok(Some(record.peer))
    }

    /// Remove expired tickets from both forward and reverse indexes.
    pub fn reap_expired(&self) -> io::Result<usize> {
        let now = now_secs()?;
        let expired = {
            let r = self.db.begin_read().map_err(ioerr)?;
            let table = r.open_table(T_TICKETS).map_err(ioerr)?;
            let mut expired = Vec::new();
            for entry in table.iter().map_err(ioerr)? {
                let (ticket, raw_record) = entry.map_err(ioerr)?;
                let record = TicketRecord::decode(raw_record.value())?;
                if record.is_expired(now) {
                    expired.push((ticket.value().to_vec(), record.peer));
                }
            }
            expired
        };

        if expired.is_empty() {
            return Ok(0);
        }

        let w = self.db.begin_write().map_err(ioerr)?;
        let mut removed = 0usize;
        {
            let mut fwd = w.open_table(T_TICKETS).map_err(ioerr)?;
            for (ticket, _) in &expired {
                if fwd.remove(ticket.as_slice()).map_err(ioerr)?.is_some() {
                    removed = removed.saturating_add(1);
                }
            }
        }
        {
            let mut rev = w.open_table(T_REVERSE).map_err(ioerr)?;
            for (ticket, peer) in &expired {
                let reverse_matches = rev
                    .get(*peer.as_bytes())
                    .map_err(ioerr)?
                    .map(|guard| guard.value() == ticket.as_slice())
                    .unwrap_or(false);
                if reverse_matches {
                    let _ = rev.remove(*peer.as_bytes()).map_err(ioerr)?;
                }
            }
        }
        w.commit().map_err(ioerr)?;

        crate::observability::metrics::record_session_ticket_events("expired", removed);
        Ok(removed)
    }

    /// Revoke by peer UUID: remove reverse mapping and then forward mapping.
    pub fn revoke_by_peer(&self, peer: Uuid) -> io::Result<()> {
        let w = self.db.begin_write().map_err(ioerr)?;

        let ticket_opt = {
            let mut rev = w.open_table(T_REVERSE).map_err(ioerr)?;
            rev.remove(*peer.as_bytes())
                .map_err(ioerr)?
                .map(|guard| guard.value().to_vec())
        };

        let removed = ticket_opt.is_some();
        if let Some(ticket) = ticket_opt {
            let mut fwd = w.open_table(T_TICKETS).map_err(ioerr)?;
            let _ = fwd.remove(ticket.as_slice()).map_err(ioerr)?;
        }

        w.commit().map_err(ioerr)?;
        if removed {
            crate::observability::metrics::record_session_ticket_event("revoked");
        }
        Ok(())
    }

    /// Revoke by ticket: remove forward mapping and then reverse mapping.
    pub fn revoke_by_ticket(&self, ticket: &[u8]) -> io::Result<()> {
        if self.remove_ticket(ticket)? {
            crate::observability::metrics::record_session_ticket_event("revoked");
        }
        Ok(())
    }

    /// Remove a ticket from both durable indexes and return whether it existed.
    fn remove_ticket(&self, ticket: &[u8]) -> io::Result<bool> {
        let w = self.db.begin_write().map_err(ioerr)?;

        let peer_opt = {
            let mut fwd = w.open_table(T_TICKETS).map_err(ioerr)?;
            let removed = fwd.remove(ticket).map_err(ioerr)?;
            removed
                .map(|guard| TicketRecord::decode(guard.value()).map(|record| record.peer))
                .transpose()?
        };

        let removed = peer_opt.is_some();
        if let Some(peer) = peer_opt {
            let mut rev = w.open_table(T_REVERSE).map_err(ioerr)?;
            let _ = rev.remove(*peer.as_bytes()).map_err(ioerr)?;
        }

        w.commit().map_err(ioerr)?;
        Ok(removed)
    }

    /// Load one ticket record directly from the forward index.
    fn load_ticket_record(&self, ticket: &[u8]) -> io::Result<Option<TicketRecord>> {
        let r = self.db.begin_read().map_err(ioerr)?;
        let table = r.open_table(T_TICKETS).map_err(ioerr)?;
        table
            .get(ticket)
            .map_err(ioerr)?
            .map(|guard| TicketRecord::decode(guard.value()))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthStore, T_REVERSE, T_TICKETS, TicketRecord, ioerr, now_secs};
    use redb::Database;
    use std::sync::Arc;
    use tempfile::tempdir;
    use uuid::Uuid;

    /// Build an auth store backed by a fresh temporary Redb database.
    fn temp_store(ttl_secs: u64) -> (AuthStore, tempfile::TempDir) {
        let dir = tempdir().expect("temp dir");
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).expect("create db"));
        (
            AuthStore::with_ticket_ttl(db, ttl_secs).expect("open auth store"),
            dir,
        )
    }

    /// Insert a server ticket record directly so expiry behavior does not rely on sleeps.
    fn insert_ticket_record(store: &AuthStore, ticket: &[u8], record: TicketRecord) {
        let record_bytes = record.encode();
        let w = store.db.begin_write().map_err(ioerr).expect("begin write");
        {
            let mut tickets = w.open_table(T_TICKETS).map_err(ioerr).expect("tickets");
            tickets
                .insert(ticket, record_bytes.as_slice())
                .map_err(ioerr)
                .expect("insert ticket");
        }
        {
            let mut reverse = w.open_table(T_REVERSE).map_err(ioerr).expect("reverse");
            reverse
                .insert(*record.peer.as_bytes(), ticket)
                .map_err(ioerr)
                .expect("insert reverse");
        }
        w.commit().map_err(ioerr).expect("commit");
    }

    /// Insert only a reverse index row so stale-index repair can be tested directly.
    fn insert_reverse_ticket(store: &AuthStore, peer: Uuid, ticket: &[u8]) {
        let w = store.db.begin_write().map_err(ioerr).expect("begin write");
        {
            let mut reverse = w.open_table(T_REVERSE).map_err(ioerr).expect("reverse");
            reverse
                .insert(*peer.as_bytes(), ticket)
                .map_err(ioerr)
                .expect("insert reverse");
        }
        w.commit().map_err(ioerr).expect("commit");
    }

    #[test]
    fn lookup_rejects_and_purges_expired_ticket() {
        let (store, _dir) = temp_store(60);
        let peer = Uuid::new_v4();
        let ticket = b"expired-ticket";
        let now = now_secs().expect("now");
        let record = TicketRecord {
            peer,
            issued_at: now.saturating_sub(120),
            expires_at: now.saturating_sub(1),
        };

        insert_ticket_record(&store, ticket, record);

        assert!(store.lookup(ticket).expect("lookup").is_none());
        assert!(store.load_ticket_record(ticket).expect("load").is_none());
    }

    #[test]
    fn issuing_new_ticket_invalidates_previous_ticket_for_peer() {
        let (store, _dir) = temp_store(60);
        let peer = Uuid::new_v4();

        let first = store.issue_ticket(peer).expect("first ticket");
        let second = store.issue_ticket(peer).expect("second ticket");

        assert_ne!(first.ticket, second.ticket);
        assert!(store.lookup(&first.ticket).expect("old lookup").is_none());
        assert_eq!(
            store.lookup(&second.ticket).expect("new lookup"),
            Some(peer)
        );
    }

    #[test]
    fn get_or_issue_ticket_reuses_current_ticket_for_peer() {
        let (store, _dir) = temp_store(60);
        let peer = Uuid::new_v4();

        let first = store.get_or_issue_ticket(peer).expect("first ticket");
        let second = store.get_or_issue_ticket(peer).expect("second ticket");

        assert_eq!(first, second);
        assert_eq!(
            store.lookup(&first.ticket).expect("lookup reused ticket"),
            Some(peer)
        );
    }

    #[test]
    fn get_or_issue_ticket_replaces_expired_current_ticket_for_peer() {
        let (store, _dir) = temp_store(60);
        let peer = Uuid::new_v4();
        let ticket = b"expired-ticket";
        let now = now_secs().expect("now");
        insert_ticket_record(
            &store,
            ticket,
            TicketRecord {
                peer,
                issued_at: now.saturating_sub(120),
                expires_at: now.saturating_sub(1),
            },
        );

        let issued = store.get_or_issue_ticket(peer).expect("replacement ticket");

        assert_ne!(issued.ticket, ticket);
        assert!(
            store
                .load_ticket_record(ticket)
                .expect("old load")
                .is_none()
        );
        assert_eq!(
            store.lookup(&issued.ticket).expect("new lookup"),
            Some(peer)
        );
    }

    #[test]
    fn get_or_issue_ticket_repairs_stale_reverse_without_removing_other_peer_ticket() {
        let (store, _dir) = temp_store(60);
        let owner = Uuid::new_v4();
        let stale_peer = Uuid::new_v4();
        let ticket = b"owned-ticket";
        let now = now_secs().expect("now");
        insert_ticket_record(
            &store,
            ticket,
            TicketRecord {
                peer: owner,
                issued_at: now,
                expires_at: now.saturating_add(60),
            },
        );
        insert_reverse_ticket(&store, stale_peer, ticket);

        let issued = store
            .get_or_issue_ticket(stale_peer)
            .expect("replacement ticket");

        assert_ne!(issued.ticket, ticket);
        assert_eq!(
            store.lookup(ticket).expect("owner lookup"),
            Some(owner),
            "repairing a stale reverse row must not delete another peer's live ticket"
        );
        assert_eq!(
            store.lookup(&issued.ticket).expect("stale peer lookup"),
            Some(stale_peer)
        );
    }
}
