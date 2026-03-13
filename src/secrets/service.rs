use crate::secrets::crypto::SecretKeyring;
use crate::secrets::gossip::SecretReplicator;
use crate::secrets::registry::SecretRegistry;
use crate::secrets::types::{
    SecretCiphertext, SecretEvent, SecretMetadata, SecretValue, SecretVersion, compute_secret_id,
};
use crate::store::local::{MasterKeyRecord, SecretMasterStore};
use crate::topology::Topology;
use capnp::Error;
use capnp::struct_list;
use chrono::Utc;
use protocol::secrets::{
    secret_ciphertext, secret_event, secret_metadata_entry, secret_record, secret_spec, secrets,
};
use std::collections::BTreeMap;
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
}

impl SecretsService {
    /// Constructs the secrets RPC surface with access to registry, keyring, and master store.
    pub fn new(
        registry: SecretRegistry,
        keyring: Arc<RwLock<SecretKeyring>>,
        master_store: SecretMasterStore,
        topology: Option<Topology>,
        replicator: SecretReplicator,
    ) -> Self {
        Self {
            registry,
            keyring,
            master_store,
            topology,
            replicator,
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

fn read_uuid(data: capnp::data::Reader<'_>) -> Result<Uuid, Error> {
    if data.len() != 16 {
        return Err(Error::failed("uuid must be 16 bytes".into()));
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(data);
    Ok(Uuid::from_bytes(bytes))
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

    /// Exposes the currently active master key so authenticated peers can bootstrap.
    async fn get_master_key(
        self: Rc<Self>,
        _params: secrets::GetMasterKeyParams,
        mut results: secrets::GetMasterKeyResults,
    ) -> Result<(), Error> {
        let store = self.master_store();

        let record = store
            .current()
            .map_err(|e| Error::failed(format!("failed to load master key: {e}")))?;
        let mut envelope = results.get().init_envelope();
        envelope.set_version(record.version);
        envelope.set_key(&record.key);
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
        let new_record = master_store
            .rotate()
            .map_err(|e| Error::failed(format!("failed to rotate master key: {e}")))?;

        let keyring_clone = {
            let guard = keyring_handle.write().await;
            guard.install_current(new_record);
            guard.clone()
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
            && let Err(e) = distribute_master_key(topology, new_record).await
        {
            warn!(target: "secrets", "failed to distribute master key v{}: {e}", new_record.version);
        }

        results.get().set_version(new_record.version);
        Ok(())
    }

    async fn install_master_key(
        self: Rc<Self>,
        params: secrets::InstallMasterKeyParams,
        _results: secrets::InstallMasterKeyResults,
    ) -> Result<(), Error> {
        let store = self.master_store();
        let keyring = self.keyring();

        let envelope = params.get()?.get_envelope()?;
        let key_bytes = envelope.get_key()?;
        if key_bytes.len() != 32 {
            return Err(Error::failed(
                "master key payload must be exactly 32 bytes".into(),
            ));
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(key_bytes);
        let record = MasterKeyRecord::new(envelope.get_version(), key)
            .map_err(|e| Error::failed(e.to_string()))?;

        store
            .import_current(&record)
            .map_err(|e| Error::failed(format!("failed to persist master key: {e}")))?;

        {
            let guard = keyring.write().await;
            guard.install_current(record);
        }

        Ok(())
    }
}

async fn distribute_master_key(topology: Topology, record: MasterKeyRecord) -> Result<(), Error> {
    let registry = topology.registry();
    let peers = registry
        .known_peers()
        .map_err(|e| Error::failed(format!("failed to load peer list: {e}")))?;

    for peer in peers {
        let Some(session) = registry.session_for_peer(peer).await else {
            continue;
        };
        let request = session.get_secrets_request();
        let secrets_client = request.send().pipeline.get_secrets();
        let mut install = secrets_client.install_master_key_request();
        let mut envelope = install.get().init_envelope();
        envelope.set_version(record.version);
        envelope.set_key(&record.key);

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
