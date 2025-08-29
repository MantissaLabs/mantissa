use redb::{Database, ReadableTable, TableDefinition};
use std::{
    io,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

use chacha20poly1305::{
    aead::{Aead, KeyInit},
    ChaCha20Poly1305, Key, Nonce,
};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::noise::NoiseKeys;

/// KV table: key = remote peer UUID (16 bytes), value = sealed blob (nonce||ciphertext).
/// We store variable-length bytes using redb's `&'static [u8]` value type.
const T_SESS: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("session_tickets_local");

#[inline]
fn ioerr<E: std::error::Error>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// What we actually store (plaintext) before sealing.
#[derive(Serialize, Deserialize)]
struct TicketRecord {
    ticket: Vec<u8>,
    issued_at: u64,
    // TODO: Keep room for future use; not surfaced in the API:
    // expires_at: Option<u64>,
    // note: Option<String>,
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

    // 96-bit random nonce
    let mut nonce = [0u8; 12];
    getrandom::getrandom(&mut nonce)?;

    let ct = aead
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    // Store nonce || ciphertext
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

/// Encrypted local store of **resume tickets**, keyed by remote peer id.
/// Public API stays the same as your previous `LocalSessionStore`.
#[derive(Clone)]
pub struct LocalSessionStore {
    db: Arc<Database>,
    kek: [u8; 32],
}

impl LocalSessionStore {
    /// Open (or create) the table and derive the local KEK from Noise keys.
    /// NOTE: signature changed from the previous `new(...)` to require `&NoiseKeys`.
    pub fn open(db: Arc<Database>, noise_keys: &NoiseKeys) -> io::Result<Self> {
        let kek = derive_local_key(&noise_keys.to_private_bytes());
        let w = db.begin_write().map_err(ioerr)?;
        {
            let _ = w.open_table(T_SESS).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;
        Ok(Self { db, kek })
    }

    /// Put/replace ticket for `peer`. (Encrypted at rest.)
    pub fn put(&self, peer: Uuid, ticket: &[u8]) -> io::Result<()> {
        let rec = TicketRecord {
            ticket: ticket.to_vec(),
            issued_at: now_secs(),
        };
        let plain = bincode::serialize(&rec).map_err(ioerr)?;
        let blob = seal(&self.kek, &plain)?;

        let w = self.db.begin_write().map_err(ioerr)?;
        {
            let mut t = w.open_table(T_SESS).map_err(ioerr)?;
            t.insert(*peer.as_bytes(), blob.as_slice()).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;
        Ok(())
    }

    /// Get ticket for `peer` (if any). Decrypts and returns raw ticket bytes.
    pub fn get(&self, peer: Uuid) -> io::Result<Option<Vec<u8>>> {
        let r = self.db.begin_read().map_err(ioerr)?;
        let t = r.open_table(T_SESS).map_err(ioerr)?;
        let out = match t.get(*peer.as_bytes()).map_err(ioerr)? {
            Some(g) => {
                let blob = g.value();
                let pt = open(&self.kek, blob)?;
                let rec: TicketRecord = bincode::deserialize(&pt).map_err(ioerr)?;
                Some(rec.ticket)
            }
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

    /// List all (peer, ticket) — useful for resume-all on boot.
    pub fn list(&self) -> io::Result<Vec<(Uuid, Vec<u8>)>> {
        let r = self.db.begin_read().map_err(ioerr)?;
        let t = r.open_table(T_SESS).map_err(ioerr)?;
        let mut out = Vec::new();
        let mut it = t.iter().map_err(ioerr)?;
        while let Some(Ok((k, v))) = it.next() {
            let peer = Uuid::from_bytes(k.value());
            let blob = v.value();
            let pt = open(&self.kek, blob)?;
            let rec: TicketRecord = bincode::deserialize(&pt).map_err(ioerr)?;
            out.push((peer, rec.ticket));
        }
        Ok(out)
    }
}
