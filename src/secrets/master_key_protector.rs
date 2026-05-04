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

/// Size in bytes for all symmetric master-key material.
///
/// This is a 256-bit cryptographic key size, not a passphrase length. It also
/// matches the X25519 key size used by Noise identities and transfer wrapping.
pub const MASTER_KEY_SIZE: usize = 32;

/// Current schema version for locally persisted wrapped master-key records.
const WRAPPED_SCHEMA_VERSION: u16 = 1;
/// Open-ended local provider id for passphrase-backed envelopes.
const PASSPHRASE_PROVIDER: &str = "passphrase";
/// Stable key id for the single local passphrase provider in v1.
const PASSPHRASE_PROVIDER_KEY_ID: &str = "local-passphrase";
/// Stable on-disk cipher-suite identifier for wrapped master-key envelopes.
const XCHACHA20_POLY1305: &str = "xchacha20poly1305";
/// Domain separator for AEAD authenticated data on durable local envelopes.
const ENVELOPE_AAD_PREFIX: &[u8] = b"mantissa.secret-master.v1";
/// Domain separator for deriving the local wrapping key from Argon2id output.
const PASSPHRASE_HKDF_SALT: &[u8] = b"mantissa.secret-master.passphrase.v1";
/// HKDF info string for the local passphrase-derived wrapping key.
const PASSPHRASE_HKDF_INFO: &[u8] = b"mantissa.secret-master.passphrase.wrap-key.v1";
/// Domain separator for AEAD authenticated data on node-to-node transfers.
const TRANSFER_AAD_PREFIX: &[u8] = b"mantissa.secret-master.transfer.v1";
/// Domain separator for deriving transfer AEAD keys from X25519 shared secrets.
const TRANSFER_HKDF_SALT: &[u8] = b"mantissa.secret-master.transfer.hkdf.v1";
/// HKDF info string for node-to-node master-key transfer AEAD keys.
const TRANSFER_HKDF_INFO: &[u8] = b"mantissa.secret-master.transfer.aead-key.v1";
/// Random Argon2id salt size persisted in passphrase provider metadata.
const PASSPHRASE_SALT_SIZE: usize = 16;
/// Nonce size required by XChaCha20-Poly1305.
const XCHACHA_NONCE_SIZE: usize = 24;
/// Maximum decoded provider metadata accepted before any KDF work begins.
const MAX_PASSPHRASE_METADATA_SIZE: usize = 256;
/// Upper bound for stored Argon2id memory cost to prevent local DB DoS.
const MAX_ARGON2_MEMORY_COST_KIB: u32 = 256 * 1024;
/// Upper bound for stored Argon2id iterations to prevent local DB DoS.
const MAX_ARGON2_TIME_COST: u32 = 10;
/// Upper bound for stored Argon2id lanes to prevent local DB DoS.
const MAX_ARGON2_PARALLELISM: u32 = 8;

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

/// Supported AEAD suite for wrapped durable master-key envelopes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MasterKeyCipherSuite {
    XChaCha20Poly1305,
}

impl MasterKeyCipherSuite {
    /// Returns the stable on-disk identifier for this cipher suite.
    fn as_str(self) -> &'static str {
        match self {
            Self::XChaCha20Poly1305 => XCHACHA20_POLY1305,
        }
    }

    /// Parses a stable on-disk cipher suite identifier.
    fn from_str(value: &str) -> io::Result<Self> {
        match value {
            XCHACHA20_POLY1305 => Ok(Self::XChaCha20Poly1305),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wrapped master key cipher suite mismatch",
            )),
        }
    }
}

/// Durable encrypted envelope for one cluster master key version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrappedMasterKeyRecord {
    pub schema_version: u16,
    pub master_key_version: u64,
    pub provider: String,
    pub provider_key_id: String,
    pub cipher_suite: MasterKeyCipherSuite,
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
            builder.set_cipher_suite(self.cipher_suite.as_str());
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
            cipher_suite: MasterKeyCipherSuite::from_str(
                record
                    .get_cipher_suite()
                    .map_err(capnp_to_io)?
                    .to_str()
                    .map_err(capnp_to_io)?,
            )?,
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
    /// Creates a production passphrase protector for locally persisted envelopes.
    pub fn new(passphrase: SecretPassphrase) -> Self {
        Self::with_params(passphrase, PassphraseKdfParams::production())
    }

    /// Creates a passphrase protector with explicit KDF parameters.
    pub fn with_params(passphrase: SecretPassphrase, params: PassphraseKdfParams) -> Self {
        Self { passphrase, params }
    }

    /// Creates a deterministic low-cost protector used by tests and headless harnesses.
    pub fn for_test() -> io::Result<Self> {
        let passphrase = SecretPassphrase::new(b"mantissa-test-master-key-passphrase".to_vec())?;
        Ok(Self::with_params(passphrase, PassphraseKdfParams::test()))
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
            cipher_suite: MasterKeyCipherSuite::XChaCha20Poly1305,
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
    pub sender_noise_static_pub: [u8; MASTER_KEY_SIZE],
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
        sender_noise_keys: &NoiseKeys,
        recipient_node_id: Uuid,
        recipient_noise_static_pub: [u8; MASTER_KEY_SIZE],
    ) -> io::Result<Self> {
        let mut transfer_secret_bytes = Zeroizing::new([0u8; MASTER_KEY_SIZE]);
        getrandom::getrandom(transfer_secret_bytes.as_mut())?;
        let transfer_secret = StaticSecret::from(*transfer_secret_bytes);
        let transfer_public_key = PublicKey::from(&transfer_secret).to_bytes();
        let recipient_public = PublicKey::from(recipient_noise_static_pub);
        let ephemeral_shared = transfer_secret.diffie_hellman(&recipient_public);
        if !ephemeral_shared.was_contributory() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "recipient noise key is not contributory",
            ));
        }
        let sender_static_shared = sender_noise_keys.private.diffie_hellman(&recipient_public);
        if !sender_static_shared.was_contributory() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sender static master key transfer exchange is not contributory",
            ));
        }

        let mut nonce = [0u8; XCHACHA_NONCE_SIZE];
        getrandom::getrandom(&mut nonce)?;
        let mut transfer = Self {
            version,
            sender_node_id,
            recipient_node_id,
            sender_noise_static_pub: sender_noise_keys.public_bytes(),
            transfer_public_key,
            recipient_noise_static_pub,
            nonce,
            ciphertext: Vec::new(),
        };
        let mut key = transfer_aead_key(
            ephemeral_shared.as_bytes(),
            sender_static_shared.as_bytes(),
            &transfer,
        )?;
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
        expected_sender_node_id: Uuid,
        expected_sender_noise_static_pub: [u8; MASTER_KEY_SIZE],
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
        if self.sender_node_id != expected_sender_node_id {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "master key transfer sender node mismatch",
            ));
        }
        if self.sender_noise_static_pub != expected_sender_noise_static_pub {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "master key transfer sender key mismatch",
            ));
        }
        let transfer_public = PublicKey::from(self.transfer_public_key);
        let ephemeral_shared = noise_keys.private.diffie_hellman(&transfer_public);
        if !ephemeral_shared.was_contributory() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "master key transfer public key is not contributory",
            ));
        }
        let sender_public = PublicKey::from(self.sender_noise_static_pub);
        let sender_static_shared = noise_keys.private.diffie_hellman(&sender_public);
        if !sender_static_shared.was_contributory() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "master key transfer sender key is not contributory",
            ));
        }
        let mut key = transfer_aead_key(
            ephemeral_shared.as_bytes(),
            sender_static_shared.as_bytes(),
            self,
        )?;
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
    if bytes.len() > MAX_PASSPHRASE_METADATA_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "passphrase master key metadata too large",
        ));
    }
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
    if params.memory_cost_kib > MAX_ARGON2_MEMORY_COST_KIB
        || params.time_cost > MAX_ARGON2_TIME_COST
        || params.parallelism > MAX_ARGON2_PARALLELISM
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "passphrase master key Argon2id parameters exceed supported limits",
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
    extend_bytes(&mut aad, record.cipher_suite.as_str().as_bytes());
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
    aad.extend_from_slice(&transfer.sender_noise_static_pub);
    aad.extend_from_slice(&transfer.transfer_public_key);
    aad.extend_from_slice(&transfer.recipient_noise_static_pub);
    aad
}

/// Derives the AEAD key used for one node-to-node transfer.
fn transfer_aead_key(
    ephemeral_shared_secret: &[u8; MASTER_KEY_SIZE],
    sender_static_shared_secret: &[u8; MASTER_KEY_SIZE],
    transfer: &MasterKeyTransfer,
) -> io::Result<Zeroizing<[u8; MASTER_KEY_SIZE]>> {
    let mut input = Zeroizing::new([0u8; MASTER_KEY_SIZE * 2]);
    input[..MASTER_KEY_SIZE].copy_from_slice(ephemeral_shared_secret);
    input[MASTER_KEY_SIZE..].copy_from_slice(sender_static_shared_secret);

    let hkdf = Hkdf::<Sha256>::new(Some(TRANSFER_HKDF_SALT), input.as_slice());
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
        MAX_ARGON2_MEMORY_COST_KIB, MasterKeyPlaintext, MasterKeyProtector, MasterKeyTransfer,
        PASSPHRASE_SALT_SIZE, PassphraseKdfParams, PassphraseMasterKeyProtector, SecretPassphrase,
        encode_passphrase_metadata,
    };
    use mantissa_net::noise::NoiseKeys;
    use uuid::Uuid;

    #[test]
    fn passphrase_provider_reopens_wrapped_key() {
        let passphrase =
            SecretPassphrase::new(b"correct horse battery staple".to_vec()).expect("passphrase");
        let provider = PassphraseMasterKeyProtector::new(passphrase.clone());
        let key = MasterKeyPlaintext::generate().expect("key");

        let wrapped = provider.wrap(1, &key).expect("wrap");
        let reopened = PassphraseMasterKeyProtector::new(passphrase);
        let unwrapped = reopened.unwrap(&wrapped).expect("unwrap");

        assert_eq!(key.as_bytes(), unwrapped.as_bytes());
    }

    #[test]
    fn passphrase_provider_rejects_wrong_passphrase() {
        let passphrase =
            SecretPassphrase::new(b"correct horse battery staple".to_vec()).expect("passphrase");
        let provider = PassphraseMasterKeyProtector::new(passphrase);
        let key = MasterKeyPlaintext::generate().expect("key");
        let wrapped = provider.wrap(1, &key).expect("wrap");

        let wrong = SecretPassphrase::new(b"incorrect horse battery staple".to_vec())
            .expect("wrong passphrase");
        let reopened = PassphraseMasterKeyProtector::new(wrong);
        assert!(reopened.unwrap(&wrapped).is_err());
    }

    #[test]
    fn master_key_transfer_roundtrips_to_recipient_noise_key() {
        let sender_id = Uuid::new_v4();
        let recipient_id = Uuid::new_v4();
        let sender = NoiseKeys::from_private_bytes([3u8; 32]);
        let recipient = NoiseKeys::from_private_bytes([7u8; 32]);
        let key = MasterKeyPlaintext::generate().expect("key");
        let transfer = MasterKeyTransfer::encrypt(
            3,
            &key,
            sender_id,
            &sender,
            recipient_id,
            recipient.public_bytes(),
        )
        .expect("encrypt transfer");

        let decrypted = transfer
            .decrypt(recipient_id, &recipient, sender_id, sender.public_bytes())
            .expect("decrypt transfer");

        assert_eq!(key.as_bytes(), decrypted.as_bytes());
    }

    #[test]
    fn master_key_transfer_rejects_wrong_sender_noise_key() {
        let sender_id = Uuid::new_v4();
        let recipient_id = Uuid::new_v4();
        let sender = NoiseKeys::from_private_bytes([3u8; 32]);
        let wrong_sender = NoiseKeys::from_private_bytes([4u8; 32]);
        let recipient = NoiseKeys::from_private_bytes([7u8; 32]);
        let key = MasterKeyPlaintext::generate().expect("key");
        let transfer = MasterKeyTransfer::encrypt(
            3,
            &key,
            sender_id,
            &sender,
            recipient_id,
            recipient.public_bytes(),
        )
        .expect("encrypt transfer");

        assert!(
            transfer
                .decrypt(
                    recipient_id,
                    &recipient,
                    sender_id,
                    wrong_sender.public_bytes()
                )
                .is_err()
        );
    }

    #[test]
    fn passphrase_provider_rejects_excessive_kdf_params() {
        let passphrase =
            SecretPassphrase::new(b"correct horse battery staple".to_vec()).expect("passphrase");
        let provider = PassphraseMasterKeyProtector::new(passphrase);
        let key = MasterKeyPlaintext::generate().expect("key");
        let mut wrapped = provider.wrap(1, &key).expect("wrap");
        let salt = [1u8; PASSPHRASE_SALT_SIZE];
        wrapped.provider_metadata = encode_passphrase_metadata(
            &salt,
            PassphraseKdfParams {
                memory_cost_kib: MAX_ARGON2_MEMORY_COST_KIB + 1,
                time_cost: 1,
                parallelism: 1,
            },
        )
        .expect("metadata");

        assert!(provider.unwrap(&wrapped).is_err());
    }
}
