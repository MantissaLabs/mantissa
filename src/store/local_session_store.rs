use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, KeyInit},
};
use hkdf::Hkdf;
use net::noise::NoiseKeys;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    io,
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
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TicketRecord {
    ticket: Vec<u8>,
    issued_at: u64,
    /// Optional absolute expiry (unix seconds). `None` = no expiry.
    expires_at: Option<u64>,
    /// Optional human hint (e.g., “anchor 192.168.1.10:6578”).
    note: Option<String>,
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
}

impl LocalSessionStore {
    /// Open or create the table and derive the local KEK from Noise keys.
    pub fn open(db: Arc<Database>, noise_keys: &NoiseKeys) -> io::Result<Self> {
        let kek = derive_local_key(&noise_keys.to_private_bytes());
        with_write_tx(&db, |tx| {
            let _ = tx.open_table(T_SESS).map_err(into_io)?;
            Ok(())
        })?;
        Ok(Self { db, kek })
    }

    /// Put/replace ticket for `peer`. (no expiry, no note)
    pub fn put(&self, peer: Uuid, ticket: &[u8]) -> io::Result<()> {
        self.put_with_meta(peer, ticket, None, None)
    }

    /// Get ticket bytes for `peer` (returns even if expired).
    pub fn get(&self, peer: Uuid) -> io::Result<Option<Vec<u8>>> {
        Ok(self.get_record(peer)?.map(|m| m.ticket))
    }

    /// Remove ticket for `peer`.
    #[allow(dead_code)]
    pub fn remove(&self, peer: Uuid) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_SESS).map_err(into_io)?;
            let _ = table.remove(*peer.as_bytes()).map_err(into_io)?;
            Ok(())
        })
    }

    /// List all peers with their ticket bytes (returns even if expired).
    #[allow(dead_code)]
    pub fn list(&self) -> io::Result<Vec<(Uuid, Vec<u8>)>> {
        Ok(self
            .list_records()?
            .into_iter()
            .map(|(p, m)| (p, m.ticket))
            .collect())
    }

    /// Put/replace with metadata.
    /// - `expires_at`: absolute unix seconds; `None` = no expiry
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
        let plain = bincode::serialize(&rec).map_err(into_io)?;
        let blob = seal(&self.kek, &plain)?;

        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_SESS).map_err(into_io)?;
            table
                .insert(*peer.as_bytes(), blob.as_slice())
                .map_err(into_io)?;
            Ok(())
        })
    }

    // Return full record (even if expired).
    pub fn get_record(&self, peer: Uuid) -> io::Result<Option<TicketRecord>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_SESS).map_err(into_io)?;
            let out = match table.get(*peer.as_bytes()).map_err(into_io)? {
                Some(guard) => {
                    let blob = guard.value();
                    let pt = open(&self.kek, blob)?;
                    let rec: TicketRecord = bincode::deserialize(&pt).map_err(into_io)?;
                    Some(rec)
                }
                None => None,
            };
            Ok(out)
        })
    }

    // Return full record only if not expired (and optionally auto-purge).
    #[allow(dead_code)]
    pub fn get_valid_record(
        &self,
        peer: Uuid,
        auto_purge: bool,
    ) -> io::Result<Option<TicketRecord>> {
        let maybe = self.get_record(peer)?;
        let now = now_secs();
        Ok(match maybe {
            Some(rec) => {
                if let Some(exp) = rec.expires_at {
                    if exp <= now {
                        if auto_purge {
                            let _ = self.remove(peer);
                        }
                        None
                    } else {
                        Some(rec)
                    }
                } else {
                    Some(rec)
                }
            }
            None => None,
        })
    }

    // List all records (even if expired).
    pub fn list_records(&self) -> io::Result<Vec<(Uuid, TicketRecord)>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_SESS).map_err(into_io)?;
            let mut it = table.iter().map_err(into_io)?;
            let mut out = Vec::new();
            while let Some(Ok((key, value))) = it.next() {
                let peer = Uuid::from_bytes(key.value());
                let blob = value.value();
                let pt = open(&self.kek, blob)?;
                let rec: TicketRecord = bincode::deserialize(&pt).map_err(into_io)?;
                out.push((peer, rec));
            }
            Ok(out)
        })
    }

    /// Purge expired entries; returns the number removed.
    #[allow(dead_code)]
    pub fn purge_expired(&self) -> io::Result<usize> {
        let peers = self
            .list_records()?
            .into_iter()
            .filter_map(|(p, m)| match m.expires_at {
                Some(exp) if exp <= now_secs() => Some(p),
                _ => None,
            })
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
}
