use crate::gossip::Message;
use crate::secrets::master_key::envelope::{MasterKeyDescriptor, MasterKeyTransfer};
use crate::store::local::MasterKeyRecord;
use crate::store::replicated::secret_key_sync::{
    SecretMasterKeyCurrent, SecretMasterKeyStore, SecretMasterKeySyncRecord,
    current_from_descriptor, current_row_id, descriptor_row_id, grant_row_id, upsert_record,
};
use anyhow::{Result, anyhow};
use async_channel::{Receiver, Sender};
use mantissa_net::noise::NoiseKeys;
use mantissa_store::uuid_key::UuidKey;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::warn;
use uuid::Uuid;

/// Node identity used to encrypt one replicated master-key grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecretMasterKeyGrantRecipient {
    pub node_id: Uuid,
    pub noise_static_pub: [u8; 32],
}

/// Writes locally created master-key rows and gossips them for low-latency convergence.
#[derive(Clone)]
pub struct SecretMasterKeyPublisher {
    sync_store: SecretMasterKeyStore,
    gossip_tx: Sender<Message>,
    sync_notify: Arc<Notify>,
    local_node_id: Uuid,
    noise_keys: Arc<NoiseKeys>,
}

/// Applies inbound master-key gossip rows into the replicated store.
#[derive(Clone)]
pub struct SecretMasterKeyReplicator {
    sync_store: SecretMasterKeyStore,
    gossip_rx: Receiver<Message>,
    sync_notify: Arc<Notify>,
}

impl SecretMasterKeyPublisher {
    /// Builds the local producer for replicated master-key rows.
    pub fn new(
        sync_store: SecretMasterKeyStore,
        gossip_tx: Sender<Message>,
        sync_notify: Arc<Notify>,
        local_node_id: Uuid,
        noise_keys: Arc<NoiseKeys>,
    ) -> Self {
        Self {
            sync_store,
            gossip_tx,
            sync_notify,
            local_node_id,
            noise_keys,
        }
    }

    /// Publishes the current key descriptor, recipient grants, and current pointer.
    pub async fn publish_current_key(
        &self,
        record: &MasterKeyRecord,
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<()> {
        let mut rows = Vec::with_capacity(recipients.len().saturating_add(2));
        self.append_missing_key_grants(&mut rows, record, recipients)?;
        self.append_current_if_missing(&mut rows, &current_from_descriptor(&record.descriptor))?;
        self.publish_records(rows).await
    }

    /// Publishes the current key rows and returns them for latency-sensitive join seeding.
    pub async fn publish_current_key_returning_records(
        &self,
        record: &MasterKeyRecord,
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<Vec<SecretMasterKeySyncRecord>> {
        self.publish_current_with_key_grants_returning_records(
            record,
            std::slice::from_ref(record),
            recipients,
        )
        .await
    }

    /// Publishes all known key grants for recipients and advances the current pointer.
    ///
    /// Callers that must make historical ciphertext readable for a recipient
    /// can pass every locally known key. The join fast path intentionally uses
    /// `publish_current_key_returning_records` instead so registerNode does
    /// not unwrap every local envelope before it can return.
    pub async fn publish_current_with_key_grants(
        &self,
        current: &MasterKeyRecord,
        records: &[MasterKeyRecord],
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<()> {
        self.publish_current_with_key_grants_returning_records(current, records, recipients)
            .await
            .map(|_| ())
    }

    /// Publishes current rows and returns the exact records written for immediate seeding.
    pub async fn publish_current_with_key_grants_returning_records(
        &self,
        current: &MasterKeyRecord,
        records: &[MasterKeyRecord],
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<Vec<SecretMasterKeySyncRecord>> {
        let mut rows = Vec::with_capacity(
            records
                .len()
                .saturating_add(1)
                .saturating_mul(recipients.len().saturating_add(1))
                .saturating_add(1),
        );
        let mut included_current = false;

        for record in records {
            included_current |= record.key_id() == current.key_id();
            self.append_key_grants(&mut rows, record, recipients)?;
        }
        if !included_current {
            self.append_key_grants(&mut rows, current, recipients)?;
        }

        rows.push(SecretMasterKeySyncRecord::Current(current_from_descriptor(
            &current.descriptor,
        )));
        self.publish_records(rows.clone()).await?;
        Ok(rows)
    }

    /// Publishes descriptor and grant rows for existing keys without changing current metadata.
    pub async fn publish_key_grants(
        &self,
        records: &[MasterKeyRecord],
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let mut rows = Vec::with_capacity(
            records
                .len()
                .saturating_mul(recipients.len().saturating_add(1)),
        );
        for record in records {
            self.append_missing_key_grants(&mut rows, record, recipients)?;
        }
        self.publish_records(rows).await
    }

    /// Returns true when any descriptor or grant row for this key still needs publication.
    pub fn key_grants_need_publication(
        &self,
        descriptor: &MasterKeyDescriptor,
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<bool> {
        let descriptor_record = SecretMasterKeySyncRecord::Descriptor(descriptor.clone());
        if !self.record_is_visible(descriptor_row_id(descriptor.key_id), &descriptor_record)? {
            return Ok(true);
        }

        for recipient in recipients {
            if !self.grant_is_visible(descriptor, recipient)? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Persists rows first, then queues gossip as an acceleration hint.
    async fn publish_records(
        &self,
        records: impl IntoIterator<Item = SecretMasterKeySyncRecord>,
    ) -> Result<()> {
        let records = records.into_iter().collect::<Vec<_>>();
        if records.is_empty() {
            return Ok(());
        }

        for record in &records {
            upsert_record(&self.sync_store, record.clone())
                .await
                .map_err(|error| anyhow!("upsert replicated master-key row: {error}"))?;
        }
        self.sync_notify.notify_waiters();

        for record in records {
            self.gossip_tx
                .send(Message::SecretMasterKey {
                    id: Uuid::new_v4(),
                    record,
                })
                .await
                .map_err(|error| anyhow!("enqueue master-key gossip: {error}"))?;
        }

        Ok(())
    }

    /// Appends descriptor and recipient grants for one plaintext key record.
    fn append_key_grants(
        &self,
        rows: &mut Vec<SecretMasterKeySyncRecord>,
        record: &MasterKeyRecord,
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<()> {
        rows.push(SecretMasterKeySyncRecord::Descriptor(
            record.descriptor.clone(),
        ));
        for recipient in recipients {
            rows.push(self.grant_record(record, *recipient)?);
        }
        Ok(())
    }

    /// Appends only missing descriptor and recipient grants for one plaintext key record.
    fn append_missing_key_grants(
        &self,
        rows: &mut Vec<SecretMasterKeySyncRecord>,
        record: &MasterKeyRecord,
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<()> {
        let descriptor_record = SecretMasterKeySyncRecord::Descriptor(record.descriptor.clone());
        if !self.record_is_visible(descriptor_row_id(record.key_id()), &descriptor_record)? {
            rows.push(descriptor_record);
        }

        for recipient in recipients {
            if !self.grant_is_visible(&record.descriptor, recipient)? {
                rows.push(self.grant_record(record, *recipient)?);
            }
        }

        Ok(())
    }

    /// Appends one current pointer row only when the replicated store does not already expose it.
    fn append_current_if_missing(
        &self,
        rows: &mut Vec<SecretMasterKeySyncRecord>,
        current: &SecretMasterKeyCurrent,
    ) -> Result<()> {
        let current_record = SecretMasterKeySyncRecord::Current(current.clone());
        if !self.record_is_visible(current_row_id(current.scope_view), &current_record)? {
            rows.push(current_record);
        }
        Ok(())
    }

    /// Returns true when the exact record is already visible at its deterministic row id.
    fn record_is_visible(&self, row_id: Uuid, record: &SecretMasterKeySyncRecord) -> Result<bool> {
        let Some(snapshot) = self
            .sync_store
            .get_snapshot(&UuidKey::from(row_id))
            .map_err(|error| anyhow!("read replicated master-key row: {error}"))?
        else {
            return Ok(false);
        };
        Ok(snapshot.as_slice().iter().any(|visible| visible == record))
    }

    /// Returns true when a compatible grant already exists for this key and recipient.
    fn grant_is_visible(
        &self,
        descriptor: &MasterKeyDescriptor,
        recipient: &SecretMasterKeyGrantRecipient,
    ) -> Result<bool> {
        let Some(snapshot) = self
            .sync_store
            .get_snapshot(&UuidKey::from(grant_row_id(
                descriptor.key_id,
                recipient.node_id,
            )))
            .map_err(|error| anyhow!("read replicated master-key grant row: {error}"))?
        else {
            return Ok(false);
        };

        Ok(snapshot.as_slice().iter().any(|visible| {
            let SecretMasterKeySyncRecord::Grant(grant) = visible else {
                return false;
            };
            grant.descriptor == *descriptor
                && grant.sender_node_id == self.local_node_id
                && grant.sender_noise_static_pub == self.noise_keys.public_bytes()
                && grant.recipient_node_id == recipient.node_id
                && grant.recipient_noise_static_pub == recipient.noise_static_pub
        }))
    }

    /// Encrypts one local master key into a replicated recipient grant row.
    fn grant_record(
        &self,
        record: &MasterKeyRecord,
        recipient: SecretMasterKeyGrantRecipient,
    ) -> Result<SecretMasterKeySyncRecord> {
        let grant = MasterKeyTransfer::encrypt(
            record.descriptor.clone(),
            &record.key,
            self.local_node_id,
            self.noise_keys.as_ref(),
            recipient.node_id,
            recipient.noise_static_pub,
        )
        .map_err(|error| anyhow!("encrypt replicated master-key grant: {error}"))?;
        Ok(SecretMasterKeySyncRecord::Grant(grant))
    }
}

impl SecretMasterKeyReplicator {
    /// Creates the inbound gossip applier for replicated master-key rows.
    pub fn new(
        sync_store: SecretMasterKeyStore,
        gossip_rx: Receiver<Message>,
        sync_notify: Arc<Notify>,
    ) -> Self {
        Self {
            sync_store,
            gossip_rx,
            sync_notify,
        }
    }

    /// Runs the inbound gossip loop and wakes the reconciler after each applied row.
    pub async fn run(&self) {
        while let Ok(message) = self.gossip_rx.recv().await {
            let Message::SecretMasterKey { record, .. } = message else {
                continue;
            };

            if let Err(error) = upsert_record(&self.sync_store, record).await {
                warn!(
                    target: "secrets",
                    "failed to apply secret master-key gossip row: {error}"
                );
                continue;
            }
            self.sync_notify.notify_waiters();
        }
    }
}
