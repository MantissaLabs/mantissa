use crate::secrets::crypto::SecretKeyring;
use crate::secrets::gossip::SecretReplicator;
use crate::secrets::master_key_protector::MasterKeyTransfer;
use crate::secrets::registry::SecretRegistry;
use crate::secrets::types::{
    SecretCiphertext, SecretEvent, SecretMetadata, SecretValue, SecretVersion, compute_secret_id,
};
use crate::store::local::{MasterKeyRecord, SecretMasterStore};
use crate::topology::Topology;
use capnp::Error;
use capnp::struct_list;
use chrono::Utc;
use mantissa_net::noise::NoiseKeys;
use mantissa_protocol::secrets::{
    secret_ciphertext, secret_event, secret_master_key_transfer, secret_metadata_entry,
    secret_record, secret_spec, secrets,
};
use mantissa_store::codec::StoreValueCodec;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;
use uuid::Uuid;

pub struct SecretsService {
    registry: SecretRegistry,
    keyring: Arc<RwLock<SecretKeyring>>,
    master_store: SecretMasterStore,
    topology: Option<Topology>,
    replicator: SecretReplicator,
    local_node_id: Uuid,
    noise_keys: Arc<NoiseKeys>,
}

impl SecretsService {
    /// Constructs the secrets RPC surface with access to registry, keyring, and master store.
    pub fn new(
        registry: SecretRegistry,
        keyring: Arc<RwLock<SecretKeyring>>,
        master_store: SecretMasterStore,
        topology: Option<Topology>,
        replicator: SecretReplicator,
        local_node_id: Uuid,
        noise_keys: Arc<NoiseKeys>,
    ) -> Self {
        Self {
            registry,
            keyring,
            master_store,
            topology,
            replicator,
            local_node_id,
            noise_keys,
        }
    }

    fn keyring(&self) -> Arc<RwLock<SecretKeyring>> {
        self.keyring.clone()
    }

    fn registry(&self) -> SecretRegistry {
        self.registry.clone()
    }

    fn master_store(&self) -> SecretMasterStore {
        self.master_store.clone()
    }

    fn topology(&self) -> Option<Topology> {
        self.topology.clone()
    }

    fn replicator(&self) -> SecretReplicator {
        self.replicator.clone()
    }

    fn local_node_id(&self) -> Uuid {
        self.local_node_id
    }

    fn noise_keys(&self) -> Arc<NoiseKeys> {
        self.noise_keys.clone()
    }

    /// Rejects secret mutations while split/merge topology operations are in progress.
    fn ensure_mutation_allowed(&self, action: &str) -> Result<(), Error> {
        if let Some(topology) = self.topology() {
            topology.ensure_no_active_cluster_operation(action)?;
        }
        Ok(())
    }
}

fn metadata_from_entries(
    entries: struct_list::Reader<secret_metadata_entry::Owned>,
    description: Option<String>,
) -> SecretMetadata {
    let mut labels = BTreeMap::new();
    for entry in entries.iter() {
        let key = entry.get_key().ok().and_then(|k| k.to_str().ok());
        let value = entry.get_value().ok().and_then(|v| v.to_str().ok());
        if let (Some(key), Some(value)) = (key, value) {
            let trimmed = key.trim();
            if !trimmed.is_empty() {
                labels.insert(trimmed.to_string(), value.to_string());
            }
        }
    }

    SecretMetadata {
        description,
        labels,
    }
}

fn write_metadata_entries(
    builder: &mut struct_list::Builder<secret_metadata_entry::Owned>,
    metadata: &SecretMetadata,
) {
    for (idx, (key, value)) in metadata.labels.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_key(key);
        entry.set_value(value);
    }
}

fn write_secret_spec(mut builder: secret_spec::Builder<'_>, value: &SecretValue) {
    builder.set_id(value.id.as_bytes());
    builder.set_name(&value.name);
    builder.set_created_at(&value.created_at);
    builder.set_updated_at(&value.updated_at);
    builder.set_description(value.metadata.description.as_deref().unwrap_or(""));

    let mut metadata_builder = builder
        .reborrow()
        .init_metadata(value.metadata.labels.len() as u32);
    write_metadata_entries(&mut metadata_builder, &value.metadata);

    let mut version_builder = builder.reborrow().init_current_version();
    version_builder.set_version_id(value.current_version.version_id.as_bytes());
    version_builder.set_created_at(&value.current_version.created_at);
    if let Some(created_by) = value.current_version.created_by {
        version_builder.set_created_by(created_by.as_bytes());
    } else {
        version_builder.set_created_by(&[]);
    }
    version_builder.set_master_key_version(value.current_version.master_key_version);
}

fn write_secret_ciphertext(
    mut builder: secret_ciphertext::Builder<'_>,
    ciphertext: &SecretCiphertext,
) {
    builder.set_master_key_version(ciphertext.master_key_version);
    builder.set_nonce(&ciphertext.nonce);
    builder.set_ciphertext(&ciphertext.ciphertext);
    builder.set_digest(&ciphertext.digest);
}

/// Serializes a secret registry event into the Cap’n Proto gossip envelope.
pub(crate) fn write_secret_event(
    mut builder: secret_event::Builder<'_>,
    event: &SecretEvent,
) -> Result<(), Error> {
    match event {
        SecretEvent::Upsert(value) => {
            let secret = value.as_ref();
            let mut record_builder = builder.reborrow().init_upsert();
            let spec_builder = record_builder.reborrow().init_spec();
            write_secret_spec(spec_builder, secret);
            let ciphertext_builder = record_builder.reborrow().init_ciphertext();
            write_secret_ciphertext(ciphertext_builder, &secret.current_version.ciphertext);
        }
        SecretEvent::Remove(id) => {
            builder.set_remove(id.as_bytes());
        }
    }
    Ok(())
}

/// Deserializes a secret gossip event into a domain object.
pub(crate) fn read_secret_event(reader: secret_event::Reader<'_>) -> Result<SecretEvent, Error> {
    match reader.which()? {
        secret_event::Which::Upsert(Ok(record_reader)) => {
            let value = read_secret_record(record_reader)?;
            Ok(SecretEvent::Upsert(Box::new(value)))
        }
        secret_event::Which::Upsert(Err(e)) => Err(e),
        secret_event::Which::Remove(Ok(bytes)) => Ok(SecretEvent::Remove(read_uuid(bytes)?)),
        secret_event::Which::Remove(Err(e)) => Err(e),
    }
}

fn read_secret_record(reader: secret_record::Reader<'_>) -> Result<SecretValue, Error> {
    let ciphertext = read_secret_ciphertext(reader.get_ciphertext()?)?;
    let spec_reader = reader.get_spec()?;
    read_secret_spec_value(spec_reader, ciphertext)
}

impl StoreValueCodec for SecretValue {
    /// Encodes one secret value as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut record = message.init_root::<secret_record::Builder<'_>>();
            write_secret_spec(record.reborrow().init_spec(), self);
            write_secret_ciphertext(
                record.reborrow().init_ciphertext(),
                &self.current_version.ciphertext,
            );
        }
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one secret value from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(secret_store_codec_error)?;
        let record = reader
            .get_root::<secret_record::Reader<'_>>()
            .map_err(secret_store_codec_error)?;
        read_secret_record(record).map_err(secret_store_codec_error)
    }
}

/// Converts secret store-codec errors into the CRDT store error type.
fn secret_store_codec_error<E: std::fmt::Display>(error: E) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "secret store codec error: {error}"
    )))
}

fn read_secret_spec_value(
    reader: secret_spec::Reader<'_>,
    ciphertext: SecretCiphertext,
) -> Result<SecretValue, Error> {
    let id = read_uuid(reader.get_id()?)?;
    let name = reader.get_name()?.to_str()?.to_string();
    let created_at = reader.get_created_at()?.to_str()?.to_string();
    let updated_at = reader.get_updated_at()?.to_str()?.to_string();

    let description_raw = reader.get_description()?.to_str()?.trim().to_string();
    let description = if description_raw.is_empty() {
        None
    } else {
        Some(description_raw)
    };
    let metadata = metadata_from_entries(reader.get_metadata()?, description);

    let version_reader = reader.get_current_version()?;
    let version_id = read_uuid(version_reader.get_version_id()?)?;
    let version_created_at = version_reader.get_created_at()?.to_str()?.to_string();
    let created_by = {
        let data = version_reader.get_created_by()?;
        if data.len() == 16 {
            Some(read_uuid(data)?)
        } else {
            None
        }
    };
    let master_key_version = version_reader.get_master_key_version();

    let version = SecretVersion::new(
        version_id,
        ciphertext,
        version_created_at,
        created_by,
        master_key_version,
    );

    Ok(SecretValue {
        id,
        name,
        metadata,
        created_at,
        updated_at,
        current_version: version,
    })
}

fn read_secret_ciphertext(
    reader: secret_ciphertext::Reader<'_>,
) -> Result<SecretCiphertext, Error> {
    let nonce_reader = reader.get_nonce()?;
    if nonce_reader.len() != 12 {
        return Err(Error::failed("secret nonce must be 12 bytes".into()));
    }
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(nonce_reader);

    let digest_reader = reader.get_digest()?;
    if digest_reader.len() != 32 {
        return Err(Error::failed("secret digest must be 32 bytes".into()));
    }
    let mut digest = [0u8; 32];
    digest.copy_from_slice(digest_reader);

    Ok(SecretCiphertext {
        master_key_version: reader.get_master_key_version(),
        nonce,
        ciphertext: reader.get_ciphertext()?.to_vec(),
        digest,
    })
}

/// Writes one encrypted master-key transfer into the RPC response envelope.
pub(crate) fn write_master_key_transfer(
    mut builder: secret_master_key_transfer::Builder<'_>,
    transfer: &MasterKeyTransfer,
) {
    builder.set_version(transfer.version);
    builder.set_sender_node_id(transfer.sender_node_id.as_bytes());
    builder.set_recipient_node_id(transfer.recipient_node_id.as_bytes());
    builder.set_sender_noise_static_pub(&transfer.sender_noise_static_pub);
    builder.set_transfer_public_key(&transfer.transfer_public_key);
    builder.set_recipient_noise_static_pub(&transfer.recipient_noise_static_pub);
    builder.set_nonce(&transfer.nonce);
    builder.set_ciphertext(&transfer.ciphertext);
}

/// Reads and validates one encrypted master-key transfer from an RPC request.
pub(crate) fn read_master_key_transfer(
    reader: secret_master_key_transfer::Reader<'_>,
) -> Result<MasterKeyTransfer, Error> {
    let transfer_public_key =
        read_fixed_32(reader.get_transfer_public_key()?, "transfer public key")?;
    let sender_noise_static_pub = read_fixed_32(
        reader.get_sender_noise_static_pub()?,
        "sender noise static key",
    )?;
    let recipient_noise_static_pub = read_fixed_32(
        reader.get_recipient_noise_static_pub()?,
        "recipient noise static key",
    )?;
    let nonce_reader = reader.get_nonce()?;
    if nonce_reader.len() != 24 {
        return Err(Error::failed(
            "master key transfer nonce must be 24 bytes".into(),
        ));
    }
    let mut nonce = [0u8; 24];
    nonce.copy_from_slice(nonce_reader);

    Ok(MasterKeyTransfer {
        version: reader.get_version(),
        sender_node_id: read_uuid(reader.get_sender_node_id()?)?,
        recipient_node_id: read_uuid(reader.get_recipient_node_id()?)?,
        sender_noise_static_pub,
        transfer_public_key,
        recipient_noise_static_pub,
        nonce,
        ciphertext: reader.get_ciphertext()?.to_vec(),
    })
}

fn read_uuid(data: capnp::data::Reader<'_>) -> Result<Uuid, Error> {
    if data.len() != 16 {
        return Err(Error::failed("uuid must be 16 bytes".into()));
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(data);
    Ok(Uuid::from_bytes(bytes))
}

fn read_fixed_32(data: capnp::data::Reader<'_>, label: &str) -> Result<[u8; 32], Error> {
    if data.len() != 32 {
        return Err(Error::failed(format!("{label} must be 32 bytes")));
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(data);
    Ok(bytes)
}

fn secret_ciphertext_from_encryption(result: SecretCiphertext) -> SecretCiphertext {
    result
}

fn plaintext_from_reader(reader: capnp::data::Reader<'_>) -> Vec<u8> {
    reader.to_owned()
}

impl secrets::Server for SecretsService {
    async fn list(
        self: Rc<Self>,
        _params: secrets::ListParams,
        mut results: secrets::ListResults,
    ) -> Result<(), Error> {
        let registry = self.registry();
        let secrets = registry.list().map_err(|e| Error::failed(e.to_string()))?;

        let mut list_builder = results.get().init_secrets(secrets.len() as u32);
        for (idx, value) in secrets.iter().enumerate() {
            let spec_builder = list_builder.reborrow().get(idx as u32);
            write_secret_spec(spec_builder, value);
        }

        Ok(())
    }

    async fn create(
        self: Rc<Self>,
        params: secrets::CreateParams,
        mut results: secrets::CreateResults,
    ) -> Result<(), Error> {
        self.ensure_mutation_allowed("create secrets")?;

        let registry = self.registry();
        let keyring = self.keyring();

        let request = params.get()?.get_request()?;
        let name = request.get_name()?.to_str()?.trim().to_string();
        if name.is_empty() {
            return Err(Error::failed("secret name cannot be empty".into()));
        }

        if registry
            .get_by_name(&name)
            .map_err(|e| Error::failed(e.to_string()))?
            .is_some()
        {
            return Err(Error::failed(format!("secret '{name}' already exists")));
        }

        let plaintext = plaintext_from_reader(request.get_plaintext()?);
        let description_raw = request.get_description()?.to_str()?.trim().to_string();
        let description = if description_raw.is_empty() {
            None
        } else {
            Some(description_raw)
        };
        let metadata = metadata_from_entries(request.get_metadata()?, description);

        let secret_id = compute_secret_id(&name);
        let version_id = Uuid::new_v4();
        let ciphertext = {
            let guard = keyring.read().await;
            guard
                .encrypt(secret_id, version_id, &plaintext)
                .map_err(|e| Error::failed(e.to_string()))?
        };
        let ciphertext = secret_ciphertext_from_encryption(ciphertext);
        let master_key_version = ciphertext.master_key_version;

        let now = Utc::now().to_rfc3339();
        let version = SecretVersion::new(
            version_id,
            ciphertext,
            now.clone(),
            None,
            master_key_version,
        );
        let value = SecretValue::new(name.clone(), metadata, now, version);

        registry
            .upsert(value.clone())
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        self.replicator()
            .broadcast(SecretEvent::Upsert(Box::new(value.clone())))
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        let spec_builder = results.get().init_secret();
        write_secret_spec(spec_builder, &value);
        Ok(())
    }

    async fn update(
        self: Rc<Self>,
        params: secrets::UpdateParams,
        mut results: secrets::UpdateResults,
    ) -> Result<(), Error> {
        self.ensure_mutation_allowed("update secrets")?;

        let registry = self.registry();
        let keyring = self.keyring();

        let request = params.get()?.get_request()?;
        let name = request.get_name()?.to_str()?.trim().to_string();
        if name.is_empty() {
            return Err(Error::failed("secret name cannot be empty".into()));
        }

        let existing = registry
            .get_by_name(&name)
            .map_err(|e| Error::failed(e.to_string()))?
            .ok_or_else(|| Error::failed(format!("secret '{name}' not found")))?;

        let plaintext = plaintext_from_reader(request.get_plaintext()?);
        let description_raw = request.get_description()?.to_str()?.trim().to_string();
        let description = if description_raw.is_empty() {
            None
        } else {
            Some(description_raw)
        };
        let metadata = metadata_from_entries(request.get_metadata()?, description);

        let version_id = Uuid::new_v4();
        let ciphertext = {
            let guard = keyring.read().await;
            guard
                .encrypt(existing.id, version_id, &plaintext)
                .map_err(|e| Error::failed(e.to_string()))?
        };
        let ciphertext = secret_ciphertext_from_encryption(ciphertext);
        let master_key_version = ciphertext.master_key_version;

        let now = Utc::now().to_rfc3339();
        let version = SecretVersion::new(
            version_id,
            ciphertext,
            now.clone(),
            None,
            master_key_version,
        );
        let mut updated = existing.clone();
        updated.metadata = metadata;
        updated.set_version(version, now);

        registry
            .upsert(updated.clone())
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        self.replicator()
            .broadcast(SecretEvent::Upsert(Box::new(updated.clone())))
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        let spec_builder = results.get().init_secret();
        write_secret_spec(spec_builder, &updated);
        Ok(())
    }

    async fn delete(
        self: Rc<Self>,
        params: secrets::DeleteParams,
        _results: secrets::DeleteResults,
    ) -> Result<(), Error> {
        self.ensure_mutation_allowed("delete secrets")?;

        let registry = self.registry();

        let names = params.get()?.get_names()?;
        for name_reader in names.iter() {
            let name = name_reader?.to_str()?.trim().to_string();
            if name.is_empty() {
                continue;
            }
            let id = compute_secret_id(&name);
            registry
                .remove(id)
                .await
                .map_err(|e| Error::failed(e.to_string()))?;
            self.replicator()
                .broadcast(SecretEvent::Remove(id))
                .await
                .map_err(|e| Error::failed(e.to_string()))?;
        }
        Ok(())
    }

    async fn get(
        self: Rc<Self>,
        params: secrets::GetParams,
        mut results: secrets::GetResults,
    ) -> Result<(), Error> {
        let registry = self.registry();
        let keyring = self.keyring();

        let request = params.get()?;
        let name = request.get_name()?.to_str()?.trim().to_string();
        if name.is_empty() {
            return Err(Error::failed("secret name cannot be empty".into()));
        }

        let requested_version = {
            let data = request.get_version_id()?;
            if data.len() == 16 {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(data);
                Some(Uuid::from_bytes(bytes))
            } else {
                None
            }
        };

        let value = registry
            .get_by_name(&name)
            .map_err(|e| Error::failed(e.to_string()))?
            .ok_or_else(|| Error::failed(format!("secret '{name}' not found")))?;

        let version_id = value.current_version.version_id;
        if let Some(requested) = requested_version
            && requested != version_id
        {
            return Err(Error::failed(format!(
                "secret '{name}' version {requested} not found"
            )));
        }

        let plaintext = {
            let guard = keyring.read().await;
            guard
                .decrypt(value.id, version_id, &value.current_version.ciphertext)
                .map_err(|e| Error::failed(e.to_string()))?
        };

        let mut data_builder = results.get().init_version();
        let spec_builder = data_builder.reborrow().init_spec();
        write_secret_spec(spec_builder, &value);
        data_builder.set_plaintext(&plaintext);
        Ok(())
    }

    /// Exposes the current master key encrypted to a registered recipient node.
    async fn get_master_key_transfer(
        self: Rc<Self>,
        params: secrets::GetMasterKeyTransferParams,
        mut results: secrets::GetMasterKeyTransferResults,
    ) -> Result<(), Error> {
        let store = self.master_store();
        let request = params.get()?.get_request()?;
        let recipient_node_id = read_uuid(request.get_recipient_node_id()?)?;
        let recipient_noise_static_pub = read_fixed_32(
            request.get_recipient_noise_static_pub()?,
            "recipient noise static key",
        )?;

        if let Some(topology) = self.topology() {
            let registry = topology.registry();
            let peer = registry
                .peer_value_unscoped(recipient_node_id)
                .ok_or_else(|| {
                    Error::failed(format!(
                        "recipient node {recipient_node_id} is not registered"
                    ))
                })?;
            if !peer.membership.is_active() {
                return Err(Error::failed(format!(
                    "recipient node {recipient_node_id} is not active"
                )));
            }
            if peer.noise_static_pub != recipient_noise_static_pub {
                return Err(Error::failed(format!(
                    "recipient node {recipient_node_id} noise key mismatch"
                )));
            }
        }

        // Exporting a master key is a cluster-forming decision. Hold the
        // keyring read lock while committing the store policy so an import or
        // rotation cannot swap the cached plaintext between "which key will I
        // export?" and "is this bootstrap key now final?". This avoids an
        // extra envelope unwrap on the common join path without reopening the
        // race where a node could export key A and then adopt key B.
        let keyring = self.keyring();
        let record = {
            let guard = keyring.read().await;
            let record = guard
                .current_record()
                .map_err(|e| Error::failed(format!("failed to load master key: {e}")))?;
            store
                .commit_current_for_transfer(record.version)
                .map_err(|e| Error::failed(format!("failed to commit master key export: {e}")))?;
            record
        };
        let transfer = MasterKeyTransfer::encrypt(
            record.version,
            &record.key,
            self.local_node_id(),
            self.noise_keys().as_ref(),
            recipient_node_id,
            recipient_noise_static_pub,
        )
        .map_err(|e| Error::failed(format!("failed to encrypt master key transfer: {e}")))?;
        write_master_key_transfer(results.get().init_envelope(), &transfer);
        Ok(())
    }

    /// Rotates the cluster master key, re-encrypting all stored secrets with the new version.
    async fn rotate_master_key(
        self: Rc<Self>,
        _params: secrets::RotateMasterKeyParams,
        mut results: secrets::RotateMasterKeyResults,
    ) -> Result<(), Error> {
        self.ensure_mutation_allowed("rotate master key")?;

        let registry = self.registry();
        let keyring_handle = self.keyring();
        let master_store = self.master_store();
        let topology = self.topology();

        // Note: We keep previous master-key material around after rotation so peers still
        // decrypt pre-rotation ciphertext while convergence happens. We push the new version
        // to every known peer below. Once the cluster settles, the old key can be GC’d later.
        let (new_record, keyring_clone) = {
            let guard = keyring_handle.write().await;
            let new_record = master_store
                .rotate()
                .map_err(|e| Error::failed(format!("failed to rotate master key: {e}")))?;
            guard.install_current(&new_record);
            (new_record, guard.clone())
        };

        let secrets = registry.list().map_err(|e| Error::failed(e.to_string()))?;

        for mut value in secrets {
            let plaintext = keyring_clone
                .decrypt(
                    value.id,
                    value.current_version.version_id,
                    &value.current_version.ciphertext,
                )
                .map_err(|e| Error::failed(e.to_string()))?;

            let ciphertext = keyring_clone
                .encrypt(value.id, value.current_version.version_id, &plaintext)
                .map_err(|e| Error::failed(e.to_string()))?;
            let ciphertext = secret_ciphertext_from_encryption(ciphertext);

            value.current_version.master_key_version = ciphertext.master_key_version;
            value.current_version.ciphertext = ciphertext;
            value.touch(Utc::now().to_rfc3339());

            registry
                .upsert(value)
                .await
                .map_err(|e| Error::failed(e.to_string()))?;
        }

        if let Some(topology) = topology
            && let Err(e) = distribute_master_key(
                topology,
                self.local_node_id(),
                self.noise_keys(),
                &new_record,
            )
            .await
        {
            warn!(target: "secrets", "failed to distribute master key v{}: {e}", new_record.version);
        }

        results.get().set_version(new_record.version);
        Ok(())
    }

    async fn install_master_key_transfer(
        self: Rc<Self>,
        params: secrets::InstallMasterKeyTransferParams,
        _results: secrets::InstallMasterKeyTransferResults,
    ) -> Result<(), Error> {
        let store = self.master_store();
        let keyring = self.keyring();
        let noise_keys = self.noise_keys();
        let topology = self.topology().ok_or_else(|| {
            Error::failed("topology is required to authenticate master key transfer sender".into())
        })?;

        let transfer = read_master_key_transfer(params.get()?.get_envelope()?)?;
        let sender = topology
            .registry()
            .peer_value_unscoped(transfer.sender_node_id)
            .ok_or_else(|| {
                Error::failed(format!(
                    "master key transfer sender {} is not registered",
                    transfer.sender_node_id
                ))
            })?;
        if !sender.membership.is_active() {
            return Err(Error::failed(format!(
                "master key transfer sender {} is not active",
                transfer.sender_node_id
            )));
        }
        let plaintext = transfer
            .decrypt(
                self.local_node_id(),
                noise_keys.as_ref(),
                transfer.sender_node_id,
                sender.noise_static_pub,
            )
            .map_err(|e| Error::failed(format!("failed to decrypt master key transfer: {e}")))?;
        let record = MasterKeyRecord::new(transfer.version, plaintext)
            .map_err(|e| Error::failed(e.to_string()))?;

        {
            let guard = keyring.write().await;
            store
                .import_current(&record)
                .map_err(|e| Error::failed(format!("failed to persist master key: {e}")))?;
            guard.install_current(&record);
        }

        Ok(())
    }
}

async fn distribute_master_key(
    topology: Topology,
    sender_node_id: Uuid,
    sender_noise_keys: Arc<NoiseKeys>,
    record: &MasterKeyRecord,
) -> Result<(), Error> {
    let registry = topology.registry();
    let peers = registry
        .known_peers()
        .map_err(|e| Error::failed(format!("failed to load peer list: {e}")))?;

    for peer in peers {
        let Some(peer_value) = registry.peer_value_unscoped(peer) else {
            continue;
        };
        let Some(session) = registry.session_for_peer(peer).await else {
            continue;
        };
        let transfer = MasterKeyTransfer::encrypt(
            record.version,
            &record.key,
            sender_node_id,
            sender_noise_keys.as_ref(),
            peer,
            peer_value.noise_static_pub,
        )
        .map_err(|e| Error::failed(format!("failed to encrypt master key transfer: {e}")))?;
        let request = session.get_secrets_request();
        let secrets_client = request.send().pipeline.get_secrets();
        let mut install = secrets_client.install_master_key_transfer_request();
        write_master_key_transfer(install.get().init_envelope(), &transfer);

        if let Err(e) = install.send().promise.await {
            warn!(
                target: "secrets",
                peer = %peer,
                "install master key v{} failed: {e}",
                record.version
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::secret_store::open_secret_store;
    use mantissa_store::uuid_key::UuidKey;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Builds one deterministic secret value used by store codec tests.
    fn sample_secret() -> SecretValue {
        let mut labels = BTreeMap::new();
        labels.insert("env".to_string(), "dev".to_string());
        let metadata = SecretMetadata {
            description: Some("database password".to_string()),
            labels,
        };
        let ciphertext = SecretCiphertext {
            master_key_version: 7,
            nonce: [1u8; 12],
            ciphertext: vec![2, 3, 4, 5],
            digest: [6u8; 32],
        };
        let version = SecretVersion::new(
            Uuid::new_v4(),
            ciphertext,
            "2026-03-25T12:00:00Z",
            Some(Uuid::new_v4()),
            7,
        );
        let mut secret = SecretValue::new("db-password", metadata, "2026-03-25T12:00:00Z", version);
        secret.touch("2026-03-25T12:01:00Z");
        secret
    }

    /// Secret values should round-trip through the Cap'n Proto store-value codec.
    #[test]
    fn store_value_codec_roundtrips_secret_value() {
        let secret = sample_secret();

        let encoded = secret
            .encode_store_value()
            .expect("encode secret store value");
        let decoded = SecretValue::decode_store_value(&encoded).expect("decode secret store value");

        assert_eq!(decoded, secret);
    }

    /// Reopening the secret store should decode Cap'n Proto MVReg rows from Redb.
    #[tokio::test]
    async fn secret_store_reopens_capnp_rows() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("secret-reopen-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let secret = sample_secret();
        let key = UuidKey::from(secret.id);

        {
            let store = open_secret_store(db.clone(), actor).expect("open secret store");
            store
                .upsert(&key, secret.clone())
                .await
                .expect("upsert secret");
        }

        let reopened = open_secret_store(db, actor).expect("reopen secret store");
        reopened
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild secret MST");
        let snapshot = reopened
            .get_snapshot(&key)
            .expect("lookup reopened secret")
            .expect("secret present");

        assert_eq!(snapshot.as_slice(), &[secret]);
    }
}
