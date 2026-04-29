use crate::{
    config::DEFAULT_SESSION_TICKET_TTL_SECS,
    store::tx::{into_io, with_read_tx, with_write_tx},
};
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, KeyInit},
};
use hkdf::Hkdf;
use net::noise::NoiseKeys;
use redb::{Database, ReadableTable, TableDefinition};
use sha2::Sha256;
use std::{
    io,
    io::Cursor,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

/// KV table: key = remote peer UUID (16 bytes), value = sealed blob (nonce||ciphertext).
const T_SESS: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("session_tickets_local");

fn now_secs() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => dur.as_secs(),
        Err(err) => {
            tracing::warn!("system clock error for session store: {err}");
            0
        }
    }
}

/// What we store (plaintext) before sealing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TicketRecord {
    ticket: Vec<u8>,
    issued_at: u64,
    /// Optional absolute expiry; legacy `None` records fall back to the store default lifetime.
    expires_at: Option<u64>,
    /// Optional human hint (e.g., “anchor 192.168.1.10:6578”).
    note: Option<String>,
}

impl TicketRecord {
    /// Encodes this local ticket record into its stable Cap'n Proto plaintext payload.
    fn encode_capnp(&self) -> Vec<u8> {
        let mut message = capnp::message::Builder::new_default();
        let mut builder =
            message.init_root::<protocol::server::session_ticket_record::Builder<'_>>();
        builder.set_ticket(&self.ticket);
        builder.set_issued_at_unix_secs(self.issued_at);

        if let Some(expires_at) = self.expires_at {
            builder.set_has_expires_at(true);
            builder.set_expires_at_unix_secs(expires_at);
        }

        if let Some(note) = self.note.as_deref() {
            builder.set_has_note(true);
            builder.set_note(note);
        }

        capnp::serialize::write_message_to_words(&message)
    }

    /// Decodes one local ticket record from its stable Cap'n Proto plaintext payload.
    fn decode_capnp(bytes: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(into_io)?;
        let record = reader
            .get_root::<protocol::server::session_ticket_record::Reader<'_>>()
            .map_err(into_io)?;
        let note = if record.get_has_note() {
            Some(
                record
                    .get_note()
                    .map_err(into_io)?
                    .to_str()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
                    .to_string(),
            )
        } else {
            None
        };

        Ok(Self {
            ticket: record.get_ticket().map_err(into_io)?.to_vec(),
            issued_at: record.get_issued_at_unix_secs(),
            expires_at: record
                .get_has_expires_at()
                .then_some(record.get_expires_at_unix_secs()),
            note,
        })
    }
}

/// Derive a per-node KEK from the Noise static private key.
/// Rotating the Noise key invalidates old blobs (acceptable).
fn derive_local_key(noise_priv: &[u8; 32]) -> [u8; 32] {
    const INFO: &[u8] = b"mantissa/local-session-store/v1";
    const SALT: &[u8] = b"mantissa.salt.local.session";
    let hk = Hkdf::<Sha256>::new(Some(SALT), noise_priv);
    let mut out = [0u8; 32];
    hk.expand(INFO, &mut out).expect("hkdf expand");
    out
}

fn seal(kek: &[u8; 32], plaintext: &[u8]) -> io::Result<Vec<u8>> {
    let key = Key::from_slice(kek);
    let aead = ChaCha20Poly1305::new(key);

    let mut nonce = [0u8; 12];
    getrandom::getrandom(&mut nonce)?;

    let ct = aead
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|e| io::Error::other(e.to_string()))?;

    let mut out = Vec::with_capacity(12 + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

fn open(kek: &[u8; 32], blob: &[u8]) -> io::Result<Vec<u8>> {
    if blob.len() < 12 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "blob too small"));
    }
    let (nonce, ct) = blob.split_at(12);

    let key = Key::from_slice(kek);
    let aead = ChaCha20Poly1305::new(key);

    aead.decrypt(Nonce::from_slice(nonce), ct)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "decrypt fail"))
}

/// Encrypted local store of resume tickets, keyed by remote peer id.
#[derive(Clone)]
pub struct LocalSessionStore {
    db: Arc<Database>,
    kek: [u8; 32],
    ticket_ttl_secs: u64,
}

impl LocalSessionStore {
    /// Open or create the table and derive the local KEK from Noise keys.
    pub fn open(db: Arc<Database>, noise_keys: &NoiseKeys) -> io::Result<Self> {
        Self::open_with_ticket_ttl(db, noise_keys, DEFAULT_SESSION_TICKET_TTL_SECS)
    }

    /// Open or create the table with an explicit default ticket lifetime.
    pub fn open_with_ticket_ttl(
        db: Arc<Database>,
        noise_keys: &NoiseKeys,
        ticket_ttl_secs: u64,
    ) -> io::Result<Self> {
        if ticket_ttl_secs == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "local session ticket ttl must be greater than zero",
            ));
        }

        let kek = derive_local_key(&noise_keys.to_private_bytes());
        with_write_tx(&db, |tx| {
            let _ = tx.open_table(T_SESS).map_err(into_io)?;
            Ok(())
        })?;
        Ok(Self {
            db,
            kek,
            ticket_ttl_secs,
        })
    }

    /// Put/replace ticket for `peer` using the store's default lifetime.
    pub fn put(&self, peer: Uuid, ticket: &[u8]) -> io::Result<()> {
        self.put_with_meta(peer, ticket, self.default_expires_at()?, None)
    }

    /// Get valid ticket bytes for `peer`, purging the cached value when expired.
    pub fn get(&self, peer: Uuid) -> io::Result<Option<Vec<u8>>> {
        Ok(self.get_valid_record(peer, true)?.map(|m| m.ticket))
    }

    /// Remove ticket for `peer`.
    pub fn remove(&self, peer: Uuid) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_SESS).map_err(into_io)?;
            let _ = table.remove(*peer.as_bytes()).map_err(into_io)?;
            Ok(())
        })
    }

    /// Remove every stored ticket so the node no longer attempts to resume any peer sessions.
    pub fn clear(&self) -> io::Result<()> {
        let peers = self
            .list_records()?
            .into_iter()
            .map(|(peer, _)| peer)
            .collect::<Vec<_>>();

        if peers.is_empty() {
            return Ok(());
        }

        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_SESS).map_err(into_io)?;
            for peer in peers {
                let _ = table.remove(*peer.as_bytes()).map_err(into_io)?;
            }
            Ok(())
        })
    }

    /// List all peers with valid ticket bytes, purging expired cached values.
    pub fn list(&self) -> io::Result<Vec<(Uuid, Vec<u8>)>> {
        Ok(self
            .list_valid_records(true)?
            .into_iter()
            .map(|(p, m)| (p, m.ticket))
            .collect())
    }

    /// Put/replace with metadata.
    /// - `expires_at`: absolute unix seconds; `None` falls back to the default lifetime
    /// - `note`: optional human hint
    pub fn put_with_meta(
        &self,
        peer: Uuid,
        ticket: &[u8],
        expires_at: Option<u64>,
        note: Option<String>,
    ) -> io::Result<()> {
        let rec = TicketRecord {
            ticket: ticket.to_vec(),
            issued_at: now_secs(),
            expires_at,
            note,
        };
        let plain = rec.encode_capnp();
        let blob = seal(&self.kek, &plain)?;

        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_SESS).map_err(into_io)?;
            table
                .insert(*peer.as_bytes(), blob.as_slice())
                .map_err(into_io)?;
            Ok(())
        })
    }

    /// Return full record without applying expiry checks.
    pub fn get_record(&self, peer: Uuid) -> io::Result<Option<TicketRecord>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_SESS).map_err(into_io)?;
            let out = match table.get(*peer.as_bytes()).map_err(into_io)? {
                Some(guard) => {
                    let blob = guard.value();
                    let pt = open(&self.kek, blob)?;
                    let rec = TicketRecord::decode_capnp(&pt)?;
                    Some(rec)
                }
                None => None,
            };
            Ok(out)
        })
    }

    /// Return full record only if not expired, optionally purging an expired value.
    pub fn get_valid_record(
        &self,
        peer: Uuid,
        auto_purge: bool,
    ) -> io::Result<Option<TicketRecord>> {
        let maybe = self.get_record(peer)?;
        let now = now_secs();
        Ok(match maybe {
            Some(rec) => {
                if self.record_is_expired(&rec, now) {
                    if auto_purge {
                        let _ = self.remove(peer);
                    }
                    None
                } else {
                    Some(rec)
                }
            }
            None => None,
        })
    }

    /// List all records without applying expiry checks.
    pub fn list_records(&self) -> io::Result<Vec<(Uuid, TicketRecord)>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_SESS).map_err(into_io)?;
            let mut out = Vec::new();
            for entry in table.iter().map_err(into_io)? {
                let (key, value) = entry.map_err(into_io)?;
                let peer = Uuid::from_bytes(key.value());
                let blob = value.value();
                let pt = open(&self.kek, blob)?;
                let rec = TicketRecord::decode_capnp(&pt)?;
                out.push((peer, rec));
            }
            Ok(out)
        })
    }

    /// List valid records, optionally purging expired cached values.
    pub fn list_valid_records(&self, auto_purge: bool) -> io::Result<Vec<(Uuid, TicketRecord)>> {
        let now = now_secs();
        let mut expired = Vec::new();
        let mut valid = Vec::new();

        for (peer, record) in self.list_records()? {
            if self.record_is_expired(&record, now) {
                expired.push(peer);
            } else {
                valid.push((peer, record));
            }
        }

        if auto_purge && !expired.is_empty() {
            with_write_tx(&self.db, |tx| {
                let mut table = tx.open_table(T_SESS).map_err(into_io)?;
                for peer in &expired {
                    let _ = table.remove(*peer.as_bytes()).map_err(into_io)?;
                }
                Ok(())
            })?;
        }

        Ok(valid)
    }

    /// Purge expired entries; returns the number removed.
    pub fn purge_expired(&self) -> io::Result<usize> {
        let now = now_secs();
        let peers = self
            .list_records()?
            .into_iter()
            .filter_map(|(p, m)| self.record_is_expired(&m, now).then_some(p))
            .collect::<Vec<_>>();

        if peers.is_empty() {
            return Ok(0);
        }

        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_SESS).map_err(into_io)?;
            for peer in &peers {
                let _ = table.remove(*peer.as_bytes()).map_err(into_io)?;
            }
            Ok(peers.len())
        })
    }

    /// Return the default absolute expiry timestamp used for newly cached tickets.
    fn default_expires_at(&self) -> io::Result<Option<u64>> {
        now_secs()
            .checked_add(self.ticket_ttl_secs)
            .map(Some)
            .ok_or_else(|| io::Error::other("local session ticket expiry overflow"))
    }

    /// Return the effective expiry for a record, including legacy records without explicit TTL.
    fn effective_expires_at(&self, record: &TicketRecord) -> Option<u64> {
        record
            .expires_at
            .or_else(|| record.issued_at.checked_add(self.ticket_ttl_secs))
    }

    /// Return true when a record has expired or cannot derive a bounded expiry.
    fn record_is_expired(&self, record: &TicketRecord, now: u64) -> bool {
        self.effective_expires_at(record)
            .map(|expires_at| expires_at <= now)
            .unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ensures local ticket plaintext payloads preserve optional metadata.
    #[test]
    fn ticket_record_capnp_round_trip_preserves_metadata() {
        let record = TicketRecord {
            ticket: b"ticket".to_vec(),
            issued_at: 10,
            expires_at: Some(20),
            note: Some("anchor".to_string()),
        };

        let encoded = record.encode_capnp();
        let decoded = TicketRecord::decode_capnp(&encoded).expect("decode ticket record");

        assert_eq!(decoded, record);
    }
}
