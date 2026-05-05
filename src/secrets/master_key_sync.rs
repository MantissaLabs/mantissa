use crate::gossip::Message;
use crate::secrets::master_key_protector::MasterKeyTransfer;
use crate::store::local::MasterKeyRecord;
use crate::store::secret_master_key_store::{
    SecretMasterKeyStore, SecretMasterKeySyncRecord, current_from_descriptor, upsert_record,
};
use anyhow::{Result, anyhow};
use async_channel::{Receiver, Sender};
use mantissa_net::noise::NoiseKeys;
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
        let mut records = Vec::with_capacity(recipients.len().saturating_add(2));
        records.push(SecretMasterKeySyncRecord::Descriptor(
            record.descriptor.clone(),
        ));
        for recipient in recipients {
            records.push(self.grant_record(record, *recipient)?);
        }
        records.push(SecretMasterKeySyncRecord::Current(current_from_descriptor(
            &record.descriptor,
        )));
        self.publish_records(records).await
    }

    /// Publishes the replicated rows represented by one join bootstrap transfer.
    pub async fn publish_transfer(&self, transfer: MasterKeyTransfer) -> Result<()> {
        let descriptor = transfer.descriptor.clone();
        self.publish_records([
            SecretMasterKeySyncRecord::Descriptor(descriptor.clone()),
            SecretMasterKeySyncRecord::Grant(transfer),
            SecretMasterKeySyncRecord::Current(current_from_descriptor(&descriptor)),
        ])
        .await
    }

    /// Publishes join bootstrap rows plus historical grants for referenced old keys.
    pub async fn publish_join_grants(
        &self,
        current_transfer: MasterKeyTransfer,
        historical_records: &[MasterKeyRecord],
        recipient: SecretMasterKeyGrantRecipient,
    ) -> Result<()> {
        let current_key_id = current_transfer.descriptor.key_id;
        let mut records = Vec::with_capacity(historical_records.len().saturating_mul(2) + 3);
        records.push(SecretMasterKeySyncRecord::Descriptor(
            current_transfer.descriptor.clone(),
        ));
        records.push(SecretMasterKeySyncRecord::Grant(current_transfer.clone()));
        records.push(SecretMasterKeySyncRecord::Current(current_from_descriptor(
            &current_transfer.descriptor,
        )));

        for record in historical_records {
            if record.key_id() == current_key_id {
                continue;
            }
            records.push(SecretMasterKeySyncRecord::Descriptor(
                record.descriptor.clone(),
            ));
            records.push(self.grant_record(record, recipient)?);
        }

        self.publish_records(records).await
    }

    /// Persists rows first, then queues gossip as an acceleration hint.
    async fn publish_records(
        &self,
        records: impl IntoIterator<Item = SecretMasterKeySyncRecord>,
    ) -> Result<()> {
        let records = records.into_iter().collect::<Vec<_>>();
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
