use redb::Database;
use redb::ReadableTable;
use redb::TableDefinition;
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
use getrandom::getrandom;
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::noise::NoiseKeys;

const T_SESS: TableDefinition<[u8; 16], &'static [u8]> = TableDefinition::new("session_tickets");

#[inline]
fn ioerr<E: std::error::Error>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

#[derive(Serialize, Deserialize)]
struct TicketRecord {
    ticket: Vec<u8>,
    issued_at: u64,          // unix secs
    expires_at: Option<u64>, // optional
    note: String,            // e.g. "anchor 192.168.1.10:6578"
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Derive a local AEAD key from the Noise static private key. No extra secret file needed.
/// If you rotate the Noise key, old tickets become undecryptable (acceptable).
fn derive_local_key(noise_priv: &[u8; 32]) -> [u8; 32] {
    const INFO: &[u8] = b"mantissa/session-store/v1";
    const SALT: &[u8] = b"mantissa.salt.session";
    let hk = Hkdf::<Sha256>::new(Some(SALT), noise_priv);
    let mut out = [0u8; 32];
    hk.expand(INFO, &mut out).expect("hkdf expand");
    out
}

fn seal(kek: &[u8; 32], plaintext: &[u8]) -> io::Result<Vec<u8>> {
    let key = Key::from_slice(kek);
    let aead = ChaCha20Poly1305::new(key);
    let mut nonce = [0u8; 12];
    getrandom(&mut nonce)?;
    let ct = aead
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    // store nonce || ciphertext
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
    let pt = aead
        .decrypt(Nonce::from_slice(nonce), ct)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "decrypt fail"))?;
    Ok(pt)
}

pub struct SessionStore {
    db: Arc<Database>,
    kek: [u8; 32],
}

impl SessionStore {
    pub fn open(db: Arc<Database>, noise_keys: &NoiseKeys) -> io::Result<Self> {
        let kek = derive_local_key(&noise_keys.to_private_bytes());
        let w = db.begin_write().map_err(ioerr)?;
        let _ = w.open_table(T_SESS).map_err(ioerr)?;
        w.commit().map_err(ioerr)?;
        Ok(Self { db, kek })
    }

    /// Store/overwrite a ticket for `peer`. Add a small note (e.g., address) if you like.
    pub fn put(
        &self,
        peer: Uuid,
        ticket: Vec<u8>,
        expires_at: Option<u64>,
        note: impl Into<String>,
    ) -> io::Result<()> {
        let rec = TicketRecord {
            ticket,
            issued_at: now_secs(),
            expires_at,
            note: note.into(),
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

    /// Load latest ticket for peer (if present).
    pub fn get(&self, peer: Uuid) -> io::Result<Option<Vec<u8>>> {
        let r = self.db.begin_read().map_err(ioerr)?;
        let t = r.open_table(T_SESS).map_err(ioerr)?;
        if let Some(g) = t.get(*peer.as_bytes()).map_err(ioerr)? {
            let blob = g.value();
            let pt = open(&self.kek, blob)?;
            let rec: TicketRecord = bincode::deserialize(&pt).map_err(ioerr)?;
            Ok(Some(rec.ticket))
        } else {
            Ok(None)
        }
    }

    /// Remove ticket for peer.
    pub fn remove(&self, peer: Uuid) -> io::Result<()> {
        let w = self.db.begin_write().map_err(ioerr)?;
        {
            let mut t = w.open_table(T_SESS).map_err(ioerr)?;
            let _ = t.remove(*peer.as_bytes()).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;
        Ok(())
    }

    /// List peers we have tickets for (optional helper).
    pub fn list_peers(&self) -> io::Result<Vec<Uuid>> {
        let r = self.db.begin_read().map_err(ioerr)?;
        let t = r.open_table(T_SESS).map_err(ioerr)?;
        let mut it = t.iter().map_err(ioerr)?;
        let mut out = Vec::new();
        while let Some(Ok((k, _))) = it.next() {
            out.push(Uuid::from_bytes(k.value()));
        }
        Ok(out)
    }
}
