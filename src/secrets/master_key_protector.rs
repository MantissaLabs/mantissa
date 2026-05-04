use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::{
    Key, XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, Payload},
};
use hkdf::Hkdf;
use mantissa_net::noise::NoiseKeys;
use mantissa_protocol::secrets::{passphrase_master_key_metadata, wrapped_secret_master_key};
use sha2::Sha256;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::{Zeroize, Zeroizing};

pub const MASTER_KEY_SIZE: usize = 32;

const WRAPPED_SCHEMA_VERSION: u16 = 1;
const PASSPHRASE_PROVIDER: &str = "passphrase";
const PASSPHRASE_PROVIDER_KEY_ID: &str = "local-passphrase";
const XCHACHA20_POLY1305: &str = "xchacha20poly1305";
const ENVELOPE_AAD_PREFIX: &[u8] = b"mantissa.secret-master.v1";
const PASSPHRASE_HKDF_SALT: &[u8] = b"mantissa.secret-master.passphrase.v1";
const PASSPHRASE_HKDF_INFO: &[u8] = b"mantissa.secret-master.passphrase.wrap-key.v1";
const TRANSFER_AAD_PREFIX: &[u8] = b"mantissa.secret-master.transfer.v1";
const TRANSFER_HKDF_SALT: &[u8] = b"mantissa.secret-master.transfer.hkdf.v1";
const TRANSFER_HKDF_INFO: &[u8] = b"mantissa.secret-master.transfer.aead-key.v1";
const PASSPHRASE_SALT_SIZE: usize = 16;
const XCHACHA_NONCE_SIZE: usize = 24;

/// Plaintext cluster master key material kept out of durable storage.
pub struct MasterKeyPlaintext {
    bytes: Zeroizing<[u8; MASTER_KEY_SIZE]>,
}

impl MasterKeyPlaintext {
    /// Builds a plaintext key wrapper from exact master key bytes.
    pub fn new(bytes: [u8; MASTER_KEY_SIZE]) -> Self {
        Self {
            bytes: Zeroizing::new(bytes),
        }
    }

    /// Generates a fresh cryptographically random cluster master key.
    pub fn generate() -> io::Result<Self> {
        let mut bytes = [0u8; MASTER_KEY_SIZE];
        getrandom::getrandom(&mut bytes)?;
        Ok(Self::new(bytes))
    }

    /// Copies one validated byte slice into a plaintext master key wrapper.
    pub fn from_slice(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != MASTER_KEY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "secret master key must be 32 bytes",
            ));
        }
        let mut key = [0u8; MASTER_KEY_SIZE];
        key.copy_from_slice(bytes);
        Ok(Self::new(key))
    }

    /// Borrows the raw key bytes for immediate cryptographic use.
    pub fn as_bytes(&self) -> &[u8; MASTER_KEY_SIZE] {
        &self.bytes
    }
}

impl Clone for MasterKeyPlaintext {
    /// Clones plaintext key material for bounded in-memory caches.
    fn clone(&self) -> Self {
        Self::new(*self.as_bytes())
    }
}

impl PartialEq for MasterKeyPlaintext {
    /// Compares plaintext keys by byte value for tests and rotation checks.
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for MasterKeyPlaintext {}

impl fmt::Debug for MasterKeyPlaintext {
    /// Hides plaintext key bytes from debug output.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MasterKeyPlaintext(<redacted>)")
    }
}

/// Durable encrypted envelope for one cluster master key version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrappedMasterKeyRecord {
    pub schema_version: u16,
    pub master_key_version: u64,
    pub provider: String,
    pub provider_key_id: String,
    pub cipher_suite: String,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
    pub created_at_unix_secs: u64,
    pub provider_metadata: Vec<u8>,
}

impl WrappedMasterKeyRecord {
    /// Encodes this durable envelope into the local Redb value format.
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut builder = message.init_root::<wrapped_secret_master_key::Builder<'_>>();
            builder.set_schema_version(self.schema_version);
            builder.set_master_key_version(self.master_key_version);
            builder.set_provider(&self.provider);
            builder.set_provider_key_id(&self.provider_key_id);
            builder.set_cipher_suite(&self.cipher_suite);
            builder.set_nonce(&self.nonce);
            builder.set_ciphertext(&self.ciphertext);
            builder.set_created_at_unix_secs(self.created_at_unix_secs);
            builder.set_provider_metadata(&self.provider_metadata);
        }
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one durable envelope from the local Redb value format.
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(capnp_to_io)?;
        let record = reader
            .get_root::<wrapped_secret_master_key::Reader<'_>>()
            .map_err(capnp_to_io)?;
        Ok(Self {
            schema_version: record.get_schema_version(),
            master_key_version: record.get_master_key_version(),
            provider: record
                .get_provider()
                .map_err(capnp_to_io)?
                .to_str()
                .map_err(capnp_to_io)?
                .to_string(),
            provider_key_id: record
                .get_provider_key_id()
                .map_err(capnp_to_io)?
                .to_str()
                .map_err(capnp_to_io)?
                .to_string(),
            cipher_suite: record
                .get_cipher_suite()
                .map_err(capnp_to_io)?
                .to_str()
                .map_err(capnp_to_io)?
                .to_string(),
            nonce: record.get_nonce().map_err(capnp_to_io)?.to_vec(),
            ciphertext: record.get_ciphertext().map_err(capnp_to_io)?.to_vec(),
            created_at_unix_secs: record.get_created_at_unix_secs(),
            provider_metadata: record
                .get_provider_metadata()
                .map_err(capnp_to_io)?
                .to_vec(),
        })
    }
}

/// Interface for local providers that wrap cluster master key material.
pub trait MasterKeyProtector: Send + Sync {
    /// Returns the stable provider id stored in envelopes this protector creates.
    fn provider(&self) -> &'static str;

    /// Wraps one plaintext cluster master key for local durable storage.
    fn wrap(
        &self,
        version: u64,
        plaintext: &MasterKeyPlaintext,
    ) -> io::Result<WrappedMasterKeyRecord>;

    /// Unwraps one locally stored envelope into plaintext key material.
    fn unwrap(&self, record: &WrappedMasterKeyRecord) -> io::Result<MasterKeyPlaintext>;
}

pub type MasterKeyProtectorHandle = Arc<dyn MasterKeyProtector>;

/// Passphrase bytes provided by a local operator or protected daemon source.
#[derive(Clone)]
pub struct SecretPassphrase {
    inner: Arc<Zeroizing<Vec<u8>>>,
}

impl SecretPassphrase {
    /// Stores one passphrase in zeroizing memory for provider construction.
    pub fn new(bytes: Vec<u8>) -> io::Result<Self> {
        if bytes.len() < 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "master key passphrase must be at least 12 bytes",
            ));
        }
        Ok(Self {
            inner: Arc::new(Zeroizing::new(bytes)),
        })
    }

    /// Borrows the passphrase bytes for KDF input.
    fn as_bytes(&self) -> &[u8] {
        self.inner.as_slice()
    }
}

/// Argon2id parameter set persisted in every passphrase-backed envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PassphraseKdfParams {
    pub memory_cost_kib: u32,
    pub time_cost: u32,
    pub parallelism: u32,
}

impl PassphraseKdfParams {
    /// Returns the default production KDF cost for passphrase-backed envelopes.
    pub fn production() -> Self {
        Self {
            memory_cost_kib: 64 * 1024,
            time_cost: 3,
            parallelism: 1,
        }
    }

    /// Returns a low-cost KDF profile for deterministic local tests.
    pub fn test() -> Self {
        Self {
            memory_cost_kib: 1024,
            time_cost: 1,
            parallelism: 1,
        }
    }
}

/// Local passphrase-backed master key protector.
pub struct PassphraseMasterKeyProtector {
    passphrase: SecretPassphrase,
    params: PassphraseKdfParams,
}

impl PassphraseMasterKeyProtector {
    /// Creates a production passphrase protector for one local node.
    pub fn new(passphrase: SecretPassphrase, local_node_id: Uuid) -> Self {
        Self::with_params(passphrase, local_node_id, PassphraseKdfParams::production())
    }

    /// Creates a passphrase protector with explicit KDF parameters.
    pub fn with_params(
        passphrase: SecretPassphrase,
        _local_node_id: Uuid,
        params: PassphraseKdfParams,
    ) -> Self {
        Self { passphrase, params }
    }

    /// Creates a deterministic low-cost protector used by tests and headless harnesses.
    pub fn for_test(local_node_id: Uuid) -> io::Result<Self> {
        let passphrase = SecretPassphrase::new(b"mantissa-test-master-key-passphrase".to_vec())?;
        Ok(Self::with_params(
            passphrase,
            local_node_id,
            PassphraseKdfParams::test(),
        ))
    }
}

impl MasterKeyProtector for PassphraseMasterKeyProtector {
    /// Returns the stable passphrase provider id.
    fn provider(&self) -> &'static str {
        PASSPHRASE_PROVIDER
    }

    /// Wraps a master key with a key derived from the local passphrase.
    fn wrap(
        &self,
        version: u64,
        plaintext: &MasterKeyPlaintext,
    ) -> io::Result<WrappedMasterKeyRecord> {
        let mut salt = [0u8; PASSPHRASE_SALT_SIZE];
        getrandom::getrandom(&mut salt)?;
        let provider_metadata = encode_passphrase_metadata(&salt, self.params)?;

        let mut nonce = [0u8; XCHACHA_NONCE_SIZE];
        getrandom::getrandom(&mut nonce)?;
        let created_at_unix_secs = unix_now_secs()?;
        let mut record = WrappedMasterKeyRecord {
            schema_version: WRAPPED_SCHEMA_VERSION,
            master_key_version: version,
            provider: PASSPHRASE_PROVIDER.to_string(),
            provider_key_id: PASSPHRASE_PROVIDER_KEY_ID.to_string(),
            cipher_suite: XCHACHA20_POLY1305.to_string(),
            nonce: nonce.to_vec(),
            ciphertext: Vec::new(),
            created_at_unix_secs,
            provider_metadata,
        };

        let mut wrap_key = self.derive_wrap_key(&record.provider_metadata)?;
        let aead = XChaCha20Poly1305::new(Key::from_slice(wrap_key.as_slice()));
        let aad = envelope_aad(&record);
        record.ciphertext = aead
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: plaintext.as_bytes().as_slice(),
                    aad: &aad,
                },
            )
            .map_err(|_| {
                io::Error::new(io::ErrorKind::PermissionDenied, "master key wrap failed")
            })?;
        wrap_key.zeroize();
        nonce.zeroize();
        salt.zeroize();
        Ok(record)
    }

    /// Unwraps a passphrase-backed master key envelope.
    fn unwrap(&self, record: &WrappedMasterKeyRecord) -> io::Result<MasterKeyPlaintext> {
        if record.schema_version != WRAPPED_SCHEMA_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported wrapped master key schema version",
            ));
        }
        if record.provider != PASSPHRASE_PROVIDER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wrapped master key provider mismatch",
            ));
        }
        if record.cipher_suite != XCHACHA20_POLY1305 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wrapped master key cipher suite mismatch",
            ));
        }
        if record.nonce.len() != XCHACHA_NONCE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wrapped master key nonce length invalid",
            ));
        }

        let mut wrap_key = self.derive_wrap_key(&record.provider_metadata)?;
        let aead = XChaCha20Poly1305::new(Key::from_slice(wrap_key.as_slice()));
        let aad = envelope_aad(record);
        let plaintext = aead
            .decrypt(
                XNonce::from_slice(&record.nonce),
                Payload {
                    msg: record.ciphertext.as_slice(),
                    aad: &aad,
                },
            )
            .map_err(|_| {
                io::Error::new(io::ErrorKind::PermissionDenied, "master key unwrap failed")
            })?;
        wrap_key.zeroize();
        MasterKeyPlaintext::from_slice(&plaintext)
    }
}

impl PassphraseMasterKeyProtector {
    /// Derives the AEAD wrapping key from the passphrase and stored provider metadata.
    fn derive_wrap_key(&self, metadata: &[u8]) -> io::Result<Zeroizing<[u8; MASTER_KEY_SIZE]>> {
        let parsed = decode_passphrase_metadata(metadata)?;
        let params = Params::new(
            parsed.params.memory_cost_kib,
            parsed.params.time_cost,
            parsed.params.parallelism,
            Some(MASTER_KEY_SIZE),
        )
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
        let mut intermediate = Zeroizing::new([0u8; MASTER_KEY_SIZE]);
        argon2
            .hash_password_into(
                self.passphrase.as_bytes(),
                &parsed.salt,
                intermediate.as_mut(),
            )
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;

        let hkdf = Hkdf::<Sha256>::new(Some(PASSPHRASE_HKDF_SALT), intermediate.as_slice());
        let mut out = Zeroizing::new([0u8; MASTER_KEY_SIZE]);
        hkdf.expand(PASSPHRASE_HKDF_INFO, out.as_mut())
            .map_err(|_| io::Error::other("master key passphrase HKDF failed"))?;
        Ok(out)
    }
}

/// Encrypted master key transfer between trusted nodes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MasterKeyTransfer {
    pub version: u64,
    pub sender_node_id: Uuid,
    pub recipient_node_id: Uuid,
    pub transfer_public_key: [u8; MASTER_KEY_SIZE],
    pub recipient_noise_static_pub: [u8; MASTER_KEY_SIZE],
    pub nonce: [u8; XCHACHA_NONCE_SIZE],
    pub ciphertext: Vec<u8>,
}

impl MasterKeyTransfer {
    /// Encrypts one plaintext master key to a recipient node's static X25519 key.
    pub fn encrypt(
        version: u64,
        plaintext: &MasterKeyPlaintext,
        sender_node_id: Uuid,
        recipient_node_id: Uuid,
        recipient_noise_static_pub: [u8; MASTER_KEY_SIZE],
    ) -> io::Result<Self> {
        let mut transfer_secret_bytes = Zeroizing::new([0u8; MASTER_KEY_SIZE]);
        getrandom::getrandom(transfer_secret_bytes.as_mut())?;
        let transfer_secret = StaticSecret::from(*transfer_secret_bytes);
        let transfer_public_key = PublicKey::from(&transfer_secret).to_bytes();
        let recipient_public = PublicKey::from(recipient_noise_static_pub);
        let shared = transfer_secret.diffie_hellman(&recipient_public);

        let mut nonce = [0u8; XCHACHA_NONCE_SIZE];
        getrandom::getrandom(&mut nonce)?;
        let mut transfer = Self {
            version,
            sender_node_id,
            recipient_node_id,
            transfer_public_key,
            recipient_noise_static_pub,
            nonce,
            ciphertext: Vec::new(),
        };
        let mut key = transfer_aead_key(shared.as_bytes(), &transfer)?;
        let aead = XChaCha20Poly1305::new(Key::from_slice(key.as_slice()));
        let aad = transfer_aad(&transfer);
        transfer.ciphertext = aead
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: plaintext.as_bytes().as_slice(),
                    aad: &aad,
                },
            )
            .map_err(|_| io::Error::other("master key transfer encryption failed"))?;
        key.zeroize();
        nonce.zeroize();
        Ok(transfer)
    }

    /// Decrypts a transfer addressed to the local node into plaintext master key material.
    pub fn decrypt(
        &self,
        local_node_id: Uuid,
        noise_keys: &NoiseKeys,
    ) -> io::Result<MasterKeyPlaintext> {
        if self.recipient_node_id != local_node_id {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "master key transfer recipient node mismatch",
            ));
        }
        if self.recipient_noise_static_pub != noise_keys.public_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "master key transfer recipient key mismatch",
            ));
        }
        let transfer_public = PublicKey::from(self.transfer_public_key);
        let shared = noise_keys.private.diffie_hellman(&transfer_public);
        let mut key = transfer_aead_key(shared.as_bytes(), self)?;
        let aead = XChaCha20Poly1305::new(Key::from_slice(key.as_slice()));
        let aad = transfer_aad(self);
        let plaintext = aead
            .decrypt(
                XNonce::from_slice(&self.nonce),
                Payload {
                    msg: self.ciphertext.as_slice(),
                    aad: &aad,
                },
            )
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "master key transfer decrypt failed",
                )
            })?;
        key.zeroize();
        MasterKeyPlaintext::from_slice(&plaintext)
    }
}

struct DecodedPassphraseMetadata {
    salt: [u8; PASSPHRASE_SALT_SIZE],
    params: PassphraseKdfParams,
}

/// Encodes passphrase KDF metadata into the provider metadata field.
fn encode_passphrase_metadata(
    salt: &[u8; PASSPHRASE_SALT_SIZE],
    params: PassphraseKdfParams,
) -> io::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut builder = message.init_root::<passphrase_master_key_metadata::Builder<'_>>();
        builder.set_salt(salt);
        builder.set_argon2_memory_cost_kib(params.memory_cost_kib);
        builder.set_argon2_time_cost(params.time_cost);
        builder.set_argon2_parallelism(params.parallelism);
    }
    Ok(capnp::serialize::write_message_to_words(&message))
}

/// Decodes passphrase KDF metadata from a durable envelope.
fn decode_passphrase_metadata(bytes: &[u8]) -> io::Result<DecodedPassphraseMetadata> {
    let mut cursor = std::io::Cursor::new(bytes);
    let reader = capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
        .map_err(capnp_to_io)?;
    let metadata = reader
        .get_root::<passphrase_master_key_metadata::Reader<'_>>()
        .map_err(capnp_to_io)?;
    let salt_reader = metadata.get_salt().map_err(capnp_to_io)?;
    if salt_reader.len() != PASSPHRASE_SALT_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "passphrase master key salt length invalid",
        ));
    }
    let mut salt = [0u8; PASSPHRASE_SALT_SIZE];
    salt.copy_from_slice(salt_reader);
    let params = PassphraseKdfParams {
        memory_cost_kib: metadata.get_argon2_memory_cost_kib(),
        time_cost: metadata.get_argon2_time_cost(),
        parallelism: metadata.get_argon2_parallelism(),
    };
    if params.memory_cost_kib == 0 || params.time_cost == 0 || params.parallelism == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "passphrase master key Argon2id parameters invalid",
        ));
    }
    Ok(DecodedPassphraseMetadata { salt, params })
}

/// Builds authenticated data for a durable master key envelope.
fn envelope_aad(record: &WrappedMasterKeyRecord) -> Vec<u8> {
    let mut aad = Vec::new();
    aad.extend_from_slice(ENVELOPE_AAD_PREFIX);
    aad.extend_from_slice(&record.schema_version.to_be_bytes());
    aad.extend_from_slice(&record.master_key_version.to_be_bytes());
    extend_bytes(&mut aad, record.provider.as_bytes());
    extend_bytes(&mut aad, record.provider_key_id.as_bytes());
    extend_bytes(&mut aad, record.cipher_suite.as_bytes());
    aad.extend_from_slice(&record.created_at_unix_secs.to_be_bytes());
    extend_bytes(&mut aad, &record.provider_metadata);
    aad
}

/// Builds authenticated data for a node-to-node master key transfer.
fn transfer_aad(transfer: &MasterKeyTransfer) -> Vec<u8> {
    let mut aad = Vec::new();
    aad.extend_from_slice(TRANSFER_AAD_PREFIX);
    aad.extend_from_slice(&transfer.version.to_be_bytes());
    aad.extend_from_slice(transfer.sender_node_id.as_bytes());
    aad.extend_from_slice(transfer.recipient_node_id.as_bytes());
    aad.extend_from_slice(&transfer.transfer_public_key);
    aad.extend_from_slice(&transfer.recipient_noise_static_pub);
    aad
}

/// Derives the AEAD key used for one node-to-node transfer.
fn transfer_aead_key(
    shared_secret: &[u8; MASTER_KEY_SIZE],
    transfer: &MasterKeyTransfer,
) -> io::Result<Zeroizing<[u8; MASTER_KEY_SIZE]>> {
    let hkdf = Hkdf::<Sha256>::new(Some(TRANSFER_HKDF_SALT), shared_secret);
    let mut info = Vec::from(TRANSFER_HKDF_INFO);
    info.extend_from_slice(&transfer_aad(transfer));
    let mut out = Zeroizing::new([0u8; MASTER_KEY_SIZE]);
    hkdf.expand(&info, out.as_mut())
        .map_err(|_| io::Error::other("master key transfer HKDF failed"))?;
    Ok(out)
}

/// Appends a length-delimited byte string to an AAD buffer.
fn extend_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Returns the current Unix timestamp for envelope metadata.
fn unix_now_secs() -> io::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| io::Error::other(error.to_string()))
}

/// Converts a Cap'n Proto codec error into the local I/O error surface.
fn capnp_to_io(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        MasterKeyPlaintext, MasterKeyProtector, MasterKeyTransfer, PassphraseMasterKeyProtector,
        SecretPassphrase,
    };
    use mantissa_net::noise::NoiseKeys;
    use uuid::Uuid;

    #[test]
    fn passphrase_provider_reopens_wrapped_key() {
        let node_id = Uuid::new_v4();
        let passphrase =
            SecretPassphrase::new(b"correct horse battery staple".to_vec()).expect("passphrase");
        let provider = PassphraseMasterKeyProtector::new(passphrase.clone(), node_id);
        let key = MasterKeyPlaintext::generate().expect("key");

        let wrapped = provider.wrap(1, &key).expect("wrap");
        let reopened = PassphraseMasterKeyProtector::new(passphrase, node_id);
        let unwrapped = reopened.unwrap(&wrapped).expect("unwrap");

        assert_eq!(key.as_bytes(), unwrapped.as_bytes());
    }

    #[test]
    fn passphrase_provider_rejects_wrong_passphrase() {
        let node_id = Uuid::new_v4();
        let passphrase =
            SecretPassphrase::new(b"correct horse battery staple".to_vec()).expect("passphrase");
        let provider = PassphraseMasterKeyProtector::new(passphrase, node_id);
        let key = MasterKeyPlaintext::generate().expect("key");
        let wrapped = provider.wrap(1, &key).expect("wrap");

        let wrong = SecretPassphrase::new(b"incorrect horse battery staple".to_vec())
            .expect("wrong passphrase");
        let reopened = PassphraseMasterKeyProtector::new(wrong, node_id);
        assert!(reopened.unwrap(&wrapped).is_err());
    }

    #[test]
    fn master_key_transfer_roundtrips_to_recipient_noise_key() {
        let sender_id = Uuid::new_v4();
        let recipient_id = Uuid::new_v4();
        let recipient = NoiseKeys::from_private_bytes([7u8; 32]);
        let key = MasterKeyPlaintext::generate().expect("key");
        let transfer =
            MasterKeyTransfer::encrypt(3, &key, sender_id, recipient_id, recipient.public_bytes())
                .expect("encrypt transfer");

        let decrypted = transfer
            .decrypt(recipient_id, &recipient)
            .expect("decrypt transfer");

        assert_eq!(key.as_bytes(), decrypted.as_bytes());
    }
}
