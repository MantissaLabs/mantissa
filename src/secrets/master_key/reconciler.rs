use crate::cluster::{ClusterViewId, ClusterViewState};
use crate::registry::Registry;
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::master_key::envelope::{MasterKeyDescriptor, MasterKeyTransfer};
use crate::store::local::{MasterKeyRecord, SecretMasterStore};
use crate::store::replicated::secret_master_key_store::{
    SecretMasterKeyCurrent, SecretMasterKeyGrant, SecretMasterKeyStore, SecretMasterKeySyncRecord,
    current_for_scope,
};
use anyhow::{Context, Result};
use mantissa_net::noise::NoiseKeys;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tracing::warn;
use uuid::Uuid;

/// Outcome counters for one master-key grant reconciliation pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SecretMasterKeyReconcileReport {
    pub grants_seen: usize,
    pub grants_imported: usize,
    pub grants_skipped: usize,
    pub current_adopted: bool,
    pub current_waiting_for_descriptor: bool,
    pub current_waiting_for_key: bool,
}

/// Imports replicated encrypted master-key grants into the local wrapped store.
#[derive(Clone)]
pub struct SecretMasterKeyReconciler {
    local_node_id: Uuid,
    noise_keys: Arc<NoiseKeys>,
    peer_registry: Registry,
    sync_store: SecretMasterKeyStore,
    master_store: SecretMasterStore,
    keyring: Arc<RwLock<SecretKeyring>>,
    cluster_view: ClusterViewState,
}

struct MasterKeySyncSnapshot {
    descriptors: HashMap<Uuid, MasterKeyDescriptor>,
    local_grants: Vec<SecretMasterKeyGrant>,
}

impl SecretMasterKeyReconciler {
    /// Builds a reconciler over replicated grants and local wrapped-key state.
    pub fn new(
        local_node_id: Uuid,
        noise_keys: Arc<NoiseKeys>,
        peer_registry: Registry,
        sync_store: SecretMasterKeyStore,
        master_store: SecretMasterStore,
        keyring: Arc<RwLock<SecretKeyring>>,
        cluster_view: ClusterViewState,
    ) -> Self {
        Self {
            local_node_id,
            noise_keys,
            peer_registry,
            sync_store,
            master_store,
            keyring,
            cluster_view,
        }
    }

    /// Runs forever, reconciling whenever anti-entropy applies master-key rows.
    pub async fn run_on_notify(self, notify: Arc<Notify>) {
        loop {
            notify.notified().await;
            if let Err(error) = self.reconcile_active_view().await {
                warn!(
                    target: "secrets",
                    "failed to reconcile replicated secret master keys: {error:#}"
                );
            }
        }
    }

    /// Reconciles grants and current metadata for the active cluster view.
    pub async fn reconcile_active_view(&self) -> Result<SecretMasterKeyReconcileReport> {
        let active_view = self.cluster_view.active_view();
        let snapshot = self.load_sync_snapshot()?;
        let mut report = self.import_local_grants(&snapshot).await?;
        self.adopt_current(active_view, &snapshot, &mut report)
            .await?;
        Ok(report)
    }

    /// Loads replicated descriptors and grants addressed to the local node.
    fn load_sync_snapshot(&self) -> Result<MasterKeySyncSnapshot> {
        let (rows, _) = self
            .sync_store
            .load_all()
            .context("load replicated secret master-key rows")?;
        let mut descriptors = HashMap::new();
        let mut local_grants = Vec::new();

        for (_, row_snapshot) in rows {
            for row in row_snapshot.as_slice() {
                match row {
                    SecretMasterKeySyncRecord::Descriptor(descriptor) => {
                        descriptors
                            .entry(descriptor.key_id)
                            .and_modify(|current| {
                                if descriptor > current {
                                    *current = descriptor.clone();
                                }
                            })
                            .or_insert_with(|| descriptor.clone());
                    }
                    SecretMasterKeySyncRecord::Grant(grant)
                        if grant.recipient_node_id == self.local_node_id =>
                    {
                        local_grants.push(grant.clone());
                    }
                    SecretMasterKeySyncRecord::Grant(_) | SecretMasterKeySyncRecord::Current(_) => {
                    }
                }
            }
        }

        Ok(MasterKeySyncSnapshot {
            descriptors,
            local_grants,
        })
    }

    /// Decrypts and locally wraps missing grants addressed to this node.
    async fn import_local_grants(
        &self,
        snapshot: &MasterKeySyncSnapshot,
    ) -> Result<SecretMasterKeyReconcileReport> {
        let mut report = SecretMasterKeyReconcileReport::default();

        for grant in &snapshot.local_grants {
            report.grants_seen = report.grants_seen.saturating_add(1);
            if self
                .master_store
                .contains_key(grant.descriptor.key_id)
                .context("check local master-key envelope")?
            {
                continue;
            }

            let Some(descriptor) = snapshot.descriptors.get(&grant.descriptor.key_id) else {
                report.grants_skipped = report.grants_skipped.saturating_add(1);
                continue;
            };
            if descriptor != &grant.descriptor {
                report.grants_skipped = report.grants_skipped.saturating_add(1);
                warn!(
                    target: "secrets",
                    key_id = %grant.descriptor.key_id,
                    "skipping master-key grant with mismatched descriptor row"
                );
                continue;
            }

            let Some(expected_sender_noise_static_pub) = self.expected_sender_noise_key(grant)
            else {
                report.grants_skipped = report.grants_skipped.saturating_add(1);
                continue;
            };

            let plaintext = match grant.decrypt(
                self.local_node_id,
                self.noise_keys.as_ref(),
                grant.sender_node_id,
                expected_sender_noise_static_pub,
            ) {
                Ok(plaintext) => plaintext,
                Err(error) => {
                    report.grants_skipped = report.grants_skipped.saturating_add(1);
                    warn!(
                        target: "secrets",
                        key_id = %grant.descriptor.key_id,
                        sender = %grant.sender_node_id,
                        "skipping undecryptable master-key grant: {error}"
                    );
                    continue;
                }
            };

            let record = MasterKeyRecord::new(grant.descriptor.clone(), plaintext)
                .context("build imported master-key record")?;
            self.master_store
                .import_key(&record)
                .context("persist imported master-key grant")?;
            {
                let keyring = self.keyring.read().await;
                keyring.cache_key(&record);
            }
            report.grants_imported = report.grants_imported.saturating_add(1);
        }

        Ok(report)
    }

    /// Resolves and verifies the expected sender static key for one grant.
    fn expected_sender_noise_key(&self, grant: &MasterKeyTransfer) -> Option<[u8; 32]> {
        if grant.recipient_noise_static_pub != self.noise_keys.public_bytes() {
            warn!(
                target: "secrets",
                key_id = %grant.descriptor.key_id,
                "skipping master-key grant encrypted to a stale local noise key"
            );
            return None;
        }

        if grant.sender_node_id == self.local_node_id {
            return (grant.sender_noise_static_pub == self.noise_keys.public_bytes())
                .then(|| self.noise_keys.public_bytes());
        }

        let sender = self
            .peer_registry
            .peer_value_unscoped(grant.sender_node_id)?;
        if !sender.is_active() || sender.noise_static_pub != grant.sender_noise_static_pub {
            return None;
        }
        Some(sender.noise_static_pub)
    }

    /// Installs the replicated current key after its descriptor and local key exist.
    async fn adopt_current(
        &self,
        active_view: ClusterViewId,
        snapshot: &MasterKeySyncSnapshot,
        report: &mut SecretMasterKeyReconcileReport,
    ) -> Result<()> {
        let Some(current) =
            current_for_scope(&self.sync_store, active_view).context("load current key row")?
        else {
            return Ok(());
        };
        let Some(descriptor) = snapshot.descriptors.get(&current.key_id) else {
            report.current_waiting_for_descriptor = true;
            return Ok(());
        };
        if !current_matches_descriptor(&current, descriptor) {
            report.current_waiting_for_descriptor = true;
            warn!(
                target: "secrets",
                key_id = %current.key_id,
                "skipping master-key current row with mismatched descriptor"
            );
            return Ok(());
        }

        // The grant import pass above has already wrapped newly received
        // plaintext and cached it in the keyring. Prefer that cache so join
        // adoption performs one production KDF wrap, not a wrap followed by an
        // immediate unwrap of the same local envelope. The store fallback still
        // covers startup or reconciliation after the process lost its cache.
        let cached_record = {
            let keyring = self.keyring.read().await;
            keyring
                .cached_record(descriptor)
                .context("read cached current master key")?
        };
        let record = match cached_record {
            Some(record) => record,
            None => {
                let Some(record) = self
                    .master_store
                    .load_key(current.key_id)
                    .context("load local current master key")?
                else {
                    report.current_waiting_for_key = true;
                    return Ok(());
                };
                record
            }
        };
        if &record.descriptor != descriptor {
            anyhow::bail!(
                "local master key {} descriptor does not match replicated descriptor",
                current.key_id
            );
        }

        let keyring = self.keyring.write().await;
        let previous_key_id = keyring.current_key_id();
        self.master_store
            .activate_current(&record)
            .context("activate replicated current master key")?;
        keyring.install_current(&record);
        report.current_adopted = previous_key_id != record.key_id();
        Ok(())
    }
}

/// Returns true when a current pointer is backed by its descriptor row.
fn current_matches_descriptor(
    current: &SecretMasterKeyCurrent,
    descriptor: &MasterKeyDescriptor,
) -> bool {
    current.scope_view == descriptor.scope_view
        && current.key_id == descriptor.key_id
        && current.generation == descriptor.generation
        && current.created_by_operation_id == descriptor.created_by_operation_id
        && current.parent_key_ids == descriptor.parent_key_ids
}

#[cfg(test)]
mod tests {
    use super::SecretMasterKeyReconciler;
    use crate::cluster::{ClusterViewId, ClusterViewState};
    use crate::registry::Registry;
    use crate::secrets::crypto::SecretKeyring;
    use crate::secrets::master_key::envelope::{
        MasterKeyDescriptor, MasterKeyPlaintext, MasterKeyTransfer, PassphraseProvider,
    };
    use crate::store::local::{LocalSessionStore, MasterKeyRecord, SecretMasterStore};
    use crate::store::replicated::peer_store::open_peers_store;
    use crate::store::replicated::secret_master_key_store::{
        SecretMasterKeyCurrent, SecretMasterKeyStore, open_secret_master_key_store, upsert_current,
        upsert_descriptor, upsert_grant,
    };
    use ed25519_dalek::SigningKey;
    use mantissa_net::noise::NoiseKeys;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::RwLock;
    use uuid::Uuid;

    struct ReconcilerHarness {
        _dir: TempDir,
        local_node_id: Uuid,
        noise_keys: Arc<NoiseKeys>,
        sync_store: SecretMasterKeyStore,
        master_store: SecretMasterStore,
        keyring: Arc<RwLock<SecretKeyring>>,
        reconciler: SecretMasterKeyReconciler,
    }

    /// Builds one descriptor used by reconciler tests.
    fn descriptor(key_id: Uuid, generation: u64, node_id: Uuid) -> MasterKeyDescriptor {
        MasterKeyDescriptor {
            key_id,
            generation,
            scope_view: ClusterViewId::legacy_default(),
            origin_view: ClusterViewId::legacy_default(),
            created_by_node_id: node_id,
            created_by_operation_id: None,
            parent_key_ids: Vec::new(),
            created_at_unix_secs: 42,
        }
    }

    /// Builds the current pointer that corresponds to `descriptor`.
    fn current(descriptor: &MasterKeyDescriptor) -> SecretMasterKeyCurrent {
        SecretMasterKeyCurrent {
            scope_view: descriptor.scope_view,
            key_id: descriptor.key_id,
            generation: descriptor.generation,
            created_by_operation_id: descriptor.created_by_operation_id,
            parent_key_ids: descriptor.parent_key_ids.clone(),
        }
    }

    /// Creates an isolated reconciler with empty replicated grant state.
    async fn harness() -> ReconcilerHarness {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
        let local_node_id = Uuid::from_u128(1);
        let noise_keys = Arc::new(NoiseKeys::from_private_bytes([7u8; 32]));
        let envelope_provider =
            Arc::new(PassphraseProvider::for_test().expect("envelope provider"));
        let master_store =
            SecretMasterStore::new(db.clone(), envelope_provider).expect("open master store");
        let active = master_store
            .ensure_current_for_node(ClusterViewId::legacy_default(), local_node_id)
            .expect("ensure current");
        let keyring = Arc::new(RwLock::new(SecretKeyring::new(
            master_store.clone(),
            active,
        )));
        let sync_store =
            open_secret_master_key_store(db.clone(), local_node_id).expect("open sync store");
        sync_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild sync");
        let peers = open_peers_store(db.clone(), local_node_id).expect("open peers");
        peers.rebuild_mst_from_disk().await.expect("rebuild peers");
        let sessions =
            LocalSessionStore::open(db, noise_keys.as_ref()).expect("open local sessions");
        let registry = Registry::new(
            peers,
            sessions,
            SigningKey::from_bytes(&[9u8; 32]),
            noise_keys.clone(),
            local_node_id,
            mantissa_health::HealthMonitor::new(local_node_id),
        );
        let cluster_view = ClusterViewState::new(ClusterViewId::legacy_default());
        let reconciler = SecretMasterKeyReconciler::new(
            local_node_id,
            noise_keys.clone(),
            registry,
            sync_store.clone(),
            master_store.clone(),
            keyring.clone(),
            cluster_view,
        );

        ReconcilerHarness {
            _dir: dir,
            local_node_id,
            noise_keys,
            sync_store,
            master_store,
            keyring,
            reconciler,
        }
    }

    /// Writes descriptor, grant, and current rows for one key into replicated state.
    async fn write_self_grant(
        harness: &ReconcilerHarness,
        descriptor: MasterKeyDescriptor,
        key: &MasterKeyPlaintext,
    ) {
        let grant = MasterKeyTransfer::encrypt(
            descriptor.clone(),
            key,
            harness.local_node_id,
            harness.noise_keys.as_ref(),
            harness.local_node_id,
            harness.noise_keys.public_bytes(),
        )
        .expect("encrypt self grant");
        upsert_descriptor(&harness.sync_store, descriptor.clone())
            .await
            .expect("upsert descriptor");
        upsert_grant(&harness.sync_store, grant)
            .await
            .expect("upsert grant");
        upsert_current(&harness.sync_store, current(&descriptor))
            .await
            .expect("upsert current");
    }

    /// A local grant should be decrypted, wrapped locally, cached, and activated as current.
    #[tokio::test]
    async fn reconcile_imports_local_grant_and_adopts_current() {
        let harness = harness().await;
        let key = MasterKeyPlaintext::generate().expect("generate key");
        let descriptor = descriptor(Uuid::from_u128(10), 2, harness.local_node_id);
        write_self_grant(&harness, descriptor.clone(), &key).await;

        let report = harness
            .reconciler
            .reconcile_active_view()
            .await
            .expect("reconcile");

        assert_eq!(report.grants_seen, 1);
        assert_eq!(report.grants_imported, 1);
        assert!(report.current_adopted);
        assert_eq!(
            harness
                .master_store
                .current()
                .expect("current master")
                .key_id(),
            descriptor.key_id
        );
        assert_eq!(
            harness.keyring.read().await.current_key_id(),
            descriptor.key_id
        );
    }

    /// Current adoption should wait until the local recipient grant has arrived.
    #[tokio::test]
    async fn reconcile_waits_for_missing_current_key_grant() {
        let harness = harness().await;
        let key = MasterKeyPlaintext::generate().expect("generate key");
        let descriptor = descriptor(Uuid::from_u128(20), 2, harness.local_node_id);
        upsert_descriptor(&harness.sync_store, descriptor.clone())
            .await
            .expect("upsert descriptor");
        upsert_current(&harness.sync_store, current(&descriptor))
            .await
            .expect("upsert current");

        let waiting = harness
            .reconciler
            .reconcile_active_view()
            .await
            .expect("reconcile missing grant");
        assert!(waiting.current_waiting_for_key);
        assert_ne!(
            harness
                .master_store
                .current()
                .expect("current master")
                .key_id(),
            descriptor.key_id
        );

        write_self_grant(&harness, descriptor.clone(), &key).await;
        let imported = harness
            .reconciler
            .reconcile_active_view()
            .await
            .expect("reconcile after grant");
        assert_eq!(imported.grants_imported, 1);
        assert!(imported.current_adopted);
    }

    /// A grant for another recipient must not unlock the current key locally.
    #[tokio::test]
    async fn reconcile_ignores_grants_for_other_recipients() {
        let harness = harness().await;
        let key = MasterKeyPlaintext::generate().expect("generate key");
        let descriptor = descriptor(Uuid::from_u128(30), 2, harness.local_node_id);
        let other_noise = NoiseKeys::from_private_bytes([8u8; 32]);
        let grant = MasterKeyTransfer::encrypt(
            descriptor.clone(),
            &key,
            harness.local_node_id,
            harness.noise_keys.as_ref(),
            Uuid::from_u128(31),
            other_noise.public_bytes(),
        )
        .expect("encrypt other grant");

        upsert_descriptor(&harness.sync_store, descriptor.clone())
            .await
            .expect("upsert descriptor");
        upsert_grant(&harness.sync_store, grant)
            .await
            .expect("upsert grant");
        upsert_current(&harness.sync_store, current(&descriptor))
            .await
            .expect("upsert current");

        let report = harness
            .reconciler
            .reconcile_active_view()
            .await
            .expect("reconcile");

        assert_eq!(report.grants_seen, 0);
        assert!(report.current_waiting_for_key);
        assert!(
            harness
                .master_store
                .load_key(descriptor.key_id)
                .expect("load key")
                .is_none()
        );
    }

    /// Imported historical grants should not become current without a current row.
    #[tokio::test]
    async fn reconcile_imports_historical_grant_without_activation() {
        let harness = harness().await;
        let original = harness.master_store.current().expect("original current");
        let key = MasterKeyPlaintext::generate().expect("generate key");
        let descriptor = descriptor(Uuid::from_u128(40), 2, harness.local_node_id);
        let record = MasterKeyRecord::new(descriptor.clone(), key.clone()).expect("record");
        let grant = MasterKeyTransfer::encrypt(
            descriptor.clone(),
            &record.key,
            harness.local_node_id,
            harness.noise_keys.as_ref(),
            harness.local_node_id,
            harness.noise_keys.public_bytes(),
        )
        .expect("encrypt historical grant");

        upsert_descriptor(&harness.sync_store, descriptor.clone())
            .await
            .expect("upsert descriptor");
        upsert_grant(&harness.sync_store, grant)
            .await
            .expect("upsert grant");

        let report = harness
            .reconciler
            .reconcile_active_view()
            .await
            .expect("reconcile");

        assert_eq!(report.grants_imported, 1);
        assert!(!report.current_adopted);
        assert_eq!(
            harness
                .master_store
                .current()
                .expect("current master")
                .key_id(),
            original.key_id()
        );
        assert!(
            harness
                .master_store
                .contains_key(descriptor.key_id)
                .expect("contains imported key")
        );
    }
}
