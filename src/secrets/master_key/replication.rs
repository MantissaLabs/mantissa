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
use tokio::sync::{Mutex, Notify};
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
    publish_gate: Arc<Mutex<()>>,
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
            publish_gate: Arc::new(Mutex::new(())),
        }
    }

    /// Publishes the current key descriptor, recipient grants, and current pointer.
    pub async fn publish_current_key(
        &self,
        record: &MasterKeyRecord,
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<()> {
        let rows = {
            let _guard = self.publish_gate.lock().await;
            let mut rows = Vec::with_capacity(recipients.len().saturating_add(2));
            self.append_missing_key_grants(&mut rows, record, recipients)?;
            self.append_current_if_missing(
                &mut rows,
                &current_from_descriptor(&record.descriptor),
            )?;
            self.persist_and_notify_locked(&rows).await?;
            rows
        };
        self.gossip_records(rows);
        Ok(())
    }

    /// Ensures current key rows exist and returns rows for latency-sensitive join seeding.
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

    /// Ensures all known key grants exist for recipients and advances the current pointer.
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

    /// Ensures current rows exist and returns usable records for immediate seeding.
    pub async fn publish_current_with_key_grants_returning_records(
        &self,
        current: &MasterKeyRecord,
        records: &[MasterKeyRecord],
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<Vec<SecretMasterKeySyncRecord>> {
        let (seed_rows, published_rows) = {
            let _guard = self.publish_gate.lock().await;
            let capacity = records
                .len()
                .saturating_add(1)
                .saturating_mul(recipients.len().saturating_add(1))
                .saturating_add(1);
            let mut seed_rows = Vec::with_capacity(capacity);
            let mut published_rows = Vec::with_capacity(capacity);
            let mut included_current = false;

            for record in records {
                included_current |= record.key_id() == current.key_id();
                self.append_seed_key_grants(
                    &mut seed_rows,
                    &mut published_rows,
                    record,
                    recipients,
                )?;
            }
            if !included_current {
                self.append_seed_key_grants(
                    &mut seed_rows,
                    &mut published_rows,
                    current,
                    recipients,
                )?;
            }
            self.append_seed_current(
                &mut seed_rows,
                &mut published_rows,
                &current_from_descriptor(&current.descriptor),
            )?;

            self.persist_and_notify_locked(&published_rows).await?;
            (seed_rows, published_rows)
        };
        self.gossip_records(published_rows);
        Ok(seed_rows)
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

        let rows = {
            let _guard = self.publish_gate.lock().await;
            let mut rows = Vec::with_capacity(
                records
                    .len()
                    .saturating_mul(recipients.len().saturating_add(1)),
            );
            for record in records {
                self.append_missing_key_grants(&mut rows, record, recipients)?;
            }
            self.persist_and_notify_locked(&rows).await?;
            rows
        };
        self.gossip_records(rows);
        Ok(())
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

    /// Persists rows and wakes local reconcilers while the caller holds `publish_gate`.
    async fn persist_and_notify_locked(&self, records: &[SecretMasterKeySyncRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        for record in records {
            upsert_record(&self.sync_store, record.clone())
                .await
                .map_err(|error| anyhow!("upsert replicated master-key row: {error}"))?;
        }
        // Wake the reconciler even if it is already busy with a previous row. Master-key rows often
        // arrive descriptor/current first and grants later; dropping the later wake can leave the
        // local current stuck until an unrelated sync delta happens.
        self.sync_notify.notify_one();
        Ok(())
    }

    /// Best-effort queues already-persisted rows as convergence acceleration hints.
    fn gossip_records(&self, records: Vec<SecretMasterKeySyncRecord>) {
        if records.is_empty() {
            return;
        }

        for record in records {
            if let Err(error) = self.gossip_tx.try_send(Message::SecretMasterKey {
                id: Uuid::new_v4(),
                record,
            }) {
                // The durable SecretMasterKeys MST is authoritative. Dropping the remaining hints
                // avoids holding transition progress behind a saturated or closed gossip queue.
                warn!(
                    target: "secrets",
                    "failed to enqueue master-key gossip hint; global Sync will repair it: {error}"
                );
                break;
            }
        }
    }

    /// Appends seed rows and separately tracks rows that are not yet visible locally.
    fn append_seed_key_grants(
        &self,
        seed_rows: &mut Vec<SecretMasterKeySyncRecord>,
        published_rows: &mut Vec<SecretMasterKeySyncRecord>,
        record: &MasterKeyRecord,
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<()> {
        let descriptor_record = SecretMasterKeySyncRecord::Descriptor(record.descriptor.clone());
        seed_rows.push(descriptor_record.clone());
        if !self.record_is_visible(descriptor_row_id(record.key_id()), &descriptor_record)? {
            published_rows.push(descriptor_record);
        }

        for recipient in recipients {
            if let Some(grant) = self.visible_grant_record(&record.descriptor, recipient)? {
                seed_rows.push(grant);
            } else {
                let grant = self.grant_record(record, *recipient)?;
                seed_rows.push(grant.clone());
                published_rows.push(grant);
            }
        }
        Ok(())
    }

    /// Appends the current row for seeding and tracks it for publication when missing.
    fn append_seed_current(
        &self,
        seed_rows: &mut Vec<SecretMasterKeySyncRecord>,
        published_rows: &mut Vec<SecretMasterKeySyncRecord>,
        current: &SecretMasterKeyCurrent,
    ) -> Result<()> {
        let current_record = SecretMasterKeySyncRecord::Current(current.clone());
        seed_rows.push(current_record.clone());
        if !self.record_is_visible(current_row_id(current.scope_view), &current_record)? {
            published_rows.push(current_record);
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
            if self
                .visible_grant_record(&record.descriptor, recipient)?
                .is_none()
            {
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
        Ok(self.visible_grant_record(descriptor, recipient)?.is_some())
    }

    /// Returns the first compatible visible grant for this key and recipient.
    ///
    /// Any sender holding the same verified key can produce a usable envelope. Treating only this
    /// node's envelopes as visible makes transition replay rewrite an already satisfied recipient
    /// row with a concurrent sender value, so idempotence is defined by descriptor and recipient.
    fn visible_grant_record(
        &self,
        descriptor: &MasterKeyDescriptor,
        recipient: &SecretMasterKeyGrantRecipient,
    ) -> Result<Option<SecretMasterKeySyncRecord>> {
        let Some(snapshot) = self
            .sync_store
            .get_snapshot(&UuidKey::from(grant_row_id(
                descriptor.key_id,
                recipient.node_id,
            )))
            .map_err(|error| anyhow!("read replicated master-key grant row: {error}"))?
        else {
            return Ok(None);
        };

        Ok(snapshot.as_slice().iter().find_map(|visible| {
            let SecretMasterKeySyncRecord::Grant(grant) = visible else {
                return None;
            };
            (grant.descriptor == *descriptor
                && grant.recipient_node_id == recipient.node_id
                && grant.recipient_noise_static_pub == recipient.noise_static_pub)
                .then(|| SecretMasterKeySyncRecord::Grant(grant.clone()))
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
            // Wake the reconciler even if it is already busy. Gossip can make anti-entropy a no-op,
            // so this wake may be the only retry after a prior descriptor/key wait.
            self.sync_notify.notify_one();
        }
    }
}
