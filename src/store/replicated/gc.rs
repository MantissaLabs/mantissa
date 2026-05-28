//! Runtime garbage collection loop for replicated CRDT stores.
//!
//! The generic store can prune durable tombstone rows once a caller supplies a
//! safety barrier. This module is the daemon-side glue that turns sync progress
//! and the current active peer set into those barriers, then applies bounded
//! store-local GC one replicated domain at a time.

use crate::cluster::{ClusterViewState, RootSchemaState};
use crate::config::RuntimeStoreGcConfig;
use crate::registry::Registry;
use crate::store::replicated::registry::ReplicatedStoreEntry;
use crate::store::replicated::secret_key_sync::{
    SecretMasterKeyStore, SecretMasterKeySyncRecord, current_for_scope,
};
use crate::store::replicated::secrets::SecretStore;
use crate::sync::{SyncGcProgress, SyncStores};
use mantissa_protocol::sync::Domain;
use mantissa_store::gc::{GcBarrier, StoreGcReport};
use std::collections::{BTreeSet, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tracing::{debug, error, trace};

/// Daemon task that periodically prunes safe tombstones from replicated stores.
#[derive(Clone)]
pub struct StoreGcRunner {
    stores: SyncStores,
    registry: Registry,
    progress: SyncGcProgress,
    cluster_view: ClusterViewState,
    root_schema: RootSchemaState,
    local_node_id: uuid::Uuid,
    secrets: SecretStore,
    secret_master_keys: SecretMasterKeyStore,
    config: RuntimeStoreGcConfig,
}

/// Dependencies needed to build the replicated store GC runner.
pub struct StoreGcRunnerInputs {
    pub stores: SyncStores,
    pub registry: Registry,
    pub progress: SyncGcProgress,
    pub cluster_view: ClusterViewState,
    pub root_schema: RootSchemaState,
    pub local_node_id: uuid::Uuid,
    pub secrets: SecretStore,
    pub secret_master_keys: SecretMasterKeyStore,
    pub config: RuntimeStoreGcConfig,
}

impl StoreGcRunner {
    /// Builds the store GC runner from the already-wired runtime dependencies.
    pub fn new(inputs: StoreGcRunnerInputs) -> Self {
        Self {
            stores: inputs.stores,
            registry: inputs.registry,
            progress: inputs.progress,
            cluster_view: inputs.cluster_view,
            root_schema: inputs.root_schema,
            local_node_id: inputs.local_node_id,
            secrets: inputs.secrets,
            secret_master_keys: inputs.secret_master_keys,
            config: inputs.config,
        }
    }

    /// Spawns the periodic GC loop when storage GC is enabled.
    ///
    /// Bootstrap calls this after all stores, topology, and sync actors have
    /// been assembled. Returning `None` keeps disabled GC out of the task set
    /// entirely, which avoids idle timers in tests and minimal deployments.
    pub fn spawn(self) -> Option<JoinHandle<()>> {
        if !self.config.enabled {
            return None;
        }

        Some(tokio::task::spawn_local(async move {
            self.run().await;
        }))
    }

    /// Runs the periodic sweep loop until the task is aborted by shutdown.
    async fn run(self) {
        let jitter = initial_jitter(self.config.interval);
        if !jitter.is_zero() {
            time::sleep(jitter).await;
        }

        let mut interval = time::interval(self.config.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            interval.tick().await;
            self.run_once().await;
        }
    }

    /// Executes one bounded GC pass across all replicated domains.
    ///
    /// The active peer snapshot is captured once per pass so every domain uses
    /// the same membership view for tombstone barriers. Register compaction does
    /// not need that barrier because it is propagated as a normal register merge.
    pub async fn run_once(&self) {
        let started_at = std::time::Instant::now();
        let mut pass_failed = false;
        let now_unix_ms = now_unix_ms();
        let active_remote_peers = match self.registry.known_peers() {
            Ok(peers) => Some(peers),
            Err(error) => {
                pass_failed = true;
                error!(target: "store.gc", "failed to load active peer snapshot: {error}");
                None
            }
        };
        let cluster_view = self.cluster_view.active_view();
        let root_schema_version = self.root_schema.supported_version();
        let pruned_progress = self
            .progress
            .retain_view_schema(cluster_view, root_schema_version);
        if pruned_progress > 0 {
            trace!(
                target: "store.gc",
                pruned_progress,
                %cluster_view,
                root_schema_version,
                "dropped stale sync GC progress entries"
            );
        }

        for entry in self.stores.entries() {
            let domain = entry.domain;
            if let Some(active_remote_peers) = &active_remote_peers {
                let local_root_digest = match entry
                    .store
                    .root_digest_at_version(root_schema_version)
                    .await
                {
                    Ok(root_digest) => root_digest,
                    Err(error) => {
                        pass_failed = true;
                        error!(
                            target: "store.gc",
                            ?domain,
                            "failed to load local root for GC barrier: {error}"
                        );
                        continue;
                    }
                };
                let Some(barrier) = self.progress.barrier_for_domain(
                    active_remote_peers.iter().copied(),
                    domain,
                    cluster_view,
                    root_schema_version,
                    local_root_digest,
                    now_unix_ms,
                ) else {
                    trace!(
                        target: "store.gc",
                        ?domain,
                        %cluster_view,
                        root_schema_version,
                        "skipping tombstone GC without complete sync barrier"
                    );
                    crate::observability::metrics::record_store_gc_skipped_domain(
                        domain,
                        "no_barrier",
                    );
                    continue;
                };

                let tombstones_pruned = match self
                    .garbage_collect_domain_tombstones(entry, barrier, now_unix_ms)
                    .await
                {
                    Ok(report) => {
                        let tombstones_pruned = report.tombstones_pruned;
                        self.trace_domain_report(domain, &report);
                        tombstones_pruned
                    }
                    Err(error) => {
                        pass_failed = true;
                        error!(
                            target: "store.gc",
                            ?domain,
                            "tombstone GC failed: {error}"
                        );
                        continue;
                    }
                };
                if tombstones_pruned > 0 {
                    continue;
                }
            } else {
                crate::observability::metrics::record_store_gc_skipped_domain(domain, "no_barrier");
                continue;
            }

            pass_failed |= self.compact_domain_registers_with_trace(entry).await;
        }
        if let Some(active_remote_peers) = &active_remote_peers {
            let owner = select_secret_master_key_gc_owner(
                cluster_view,
                self.local_node_id,
                active_remote_peers,
            );
            if owner == Some(self.local_node_id) {
                match prune_unreferenced_secret_master_key_rows(
                    &self.secrets,
                    &self.secret_master_keys,
                    &self.progress,
                    active_remote_peers,
                    cluster_view,
                    root_schema_version,
                    now_unix_ms,
                )
                .await
                {
                    Ok(pruned) if pruned > 0 => {
                        debug!(
                            target: "store.gc",
                            pruned,
                            "pruned unreferenced secret master-key rows"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        pass_failed = true;
                        error!(
                            target: "store.gc",
                            "secret master-key semantic GC failed: {error}"
                        );
                    }
                }
            } else if let Some(owner) = owner {
                trace!(
                    target: "store.gc",
                    owner = %owner,
                    local_node = %self.local_node_id,
                    %cluster_view,
                    "skipping secret master-key semantic GC on non-owner"
                );
            }
        }
        crate::observability::metrics::set_store_gc_last_duration(started_at.elapsed());
        crate::observability::metrics::record_store_gc_run(if pass_failed {
            "failure"
        } else {
            "success"
        });
    }

    /// Applies store-local tombstone GC to the backing store for one sync domain.
    async fn garbage_collect_domain_tombstones(
        &self,
        entry: &ReplicatedStoreEntry,
        barrier: GcBarrier,
        now_unix_ms: u64,
    ) -> mantissa_store::Result<StoreGcReport> {
        entry
            .store
            .garbage_collect_tombstones(&self.config.policy, barrier, now_unix_ms)
            .await
    }

    /// Applies register compaction to one sync domain and traces any work done.
    async fn compact_domain_registers_with_trace(&self, entry: &ReplicatedStoreEntry) -> bool {
        match entry.store.compact_registers(&self.config.policy).await {
            Ok(report) => {
                self.trace_domain_report(entry.domain, &report);
                false
            }
            Err(error) => {
                error!(
                    target: "store.gc",
                    domain = ?entry.domain,
                    "register compaction failed: {error}"
                );
                true
            }
        }
    }

    /// Emits one compact trace line for domains where a GC pass did work.
    fn trace_domain_report(&self, domain: Domain, report: &StoreGcReport) {
        crate::observability::metrics::record_store_gc_tombstones_pruned(
            domain,
            report.tombstones_pruned,
        );
        crate::observability::metrics::record_store_gc_registers_compacted(
            domain,
            report.registers_compacted,
        );
        if report.tombstones_scanned == 0
            && report.tombstones_pruned == 0
            && report.registers_scanned == 0
            && report.registers_compacted == 0
        {
            return;
        }

        debug!(
            target: "store.gc",
            ?domain,
            tombstones_scanned = report.tombstones_scanned,
            tombstones_pruned = report.tombstones_pruned,
            registers_scanned = report.registers_scanned,
            registers_compacted = report.registers_compacted,
            "store GC pass completed"
        );
    }
}

/// Selects the single node allowed to author semantic master-key GC for a view.
fn select_secret_master_key_gc_owner(
    cluster_view: crate::cluster::ClusterViewId,
    local_node_id: uuid::Uuid,
    active_remote_peers: &[uuid::Uuid],
) -> Option<uuid::Uuid> {
    let mut candidates = BTreeSet::new();
    candidates.insert(local_node_id);
    candidates.extend(active_remote_peers.iter().copied());

    let mut best: Option<(uuid::Uuid, u128)> = None;
    for node_id in candidates {
        let score = secret_master_key_gc_owner_score(cluster_view, node_id);
        match best {
            None => best = Some((node_id, score)),
            Some((_, best_score)) if score > best_score => best = Some((node_id, score)),
            _ => {}
        }
    }
    best.map(|(node_id, _)| node_id)
}

/// Computes the rendezvous score for semantic master-key GC ownership.
fn secret_master_key_gc_owner_score(
    cluster_view: crate::cluster::ClusterViewId,
    node_id: uuid::Uuid,
) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"secret-master-key-gc");
    hasher.update(cluster_view.cluster_id.as_bytes());
    hasher.update(&cluster_view.epoch.to_le_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

/// Tombstones secret-master-key rows that no converged secret or active current uses.
async fn prune_unreferenced_secret_master_key_rows(
    secrets: &SecretStore,
    secret_master_keys: &SecretMasterKeyStore,
    progress: &SyncGcProgress,
    active_remote_peers: &[uuid::Uuid],
    cluster_view: crate::cluster::ClusterViewId,
    root_schema_version: u32,
    now_unix_ms: u64,
) -> mantissa_store::Result<usize> {
    let secrets_root_digest = secrets.root_digest_at_version(root_schema_version).await?;
    let Some(_secrets_barrier) = progress.barrier_for_domain(
        active_remote_peers.iter().copied(),
        Domain::Secrets,
        cluster_view,
        root_schema_version,
        secrets_root_digest,
        now_unix_ms,
    ) else {
        return Ok(0);
    };
    let master_keys_root_digest = secret_master_keys
        .root_digest_at_version(root_schema_version)
        .await?;
    let Some(_master_keys_barrier) = progress.barrier_for_domain(
        active_remote_peers.iter().copied(),
        Domain::SecretMasterKeys,
        cluster_view,
        root_schema_version,
        master_keys_root_digest,
        now_unix_ms,
    ) else {
        return Ok(0);
    };

    let Some(active_current) = current_for_scope(secret_master_keys, cluster_view)? else {
        return Ok(0);
    };

    let mut referenced_key_ids = referenced_secret_master_key_ids(secrets)?;
    referenced_key_ids.insert(active_current.key_id);

    let (rows, _) = secret_master_keys.load_all()?;
    let mut prune_rows = Vec::new();
    for (row_id, snapshot) in rows {
        let mut has_prunable_record = false;
        let mut has_retained_record = false;
        for record in snapshot.as_slice() {
            match record {
                SecretMasterKeySyncRecord::Descriptor(descriptor) => {
                    if referenced_key_ids.contains(&descriptor.key_id) {
                        has_retained_record = true;
                    } else {
                        has_prunable_record = true;
                    }
                }
                SecretMasterKeySyncRecord::Grant(grant) => {
                    if referenced_key_ids.contains(&grant.descriptor.key_id) {
                        has_retained_record = true;
                    } else {
                        has_prunable_record = true;
                    }
                }
                SecretMasterKeySyncRecord::Current(current) => {
                    if current.scope_view == cluster_view && current.key_id == active_current.key_id
                    {
                        has_retained_record = true;
                    } else {
                        has_prunable_record = true;
                    }
                }
            }
        }

        if has_prunable_record && !has_retained_record {
            prune_rows.push(row_id);
        }
    }

    let mut pruned = 0usize;
    for row_id in prune_rows {
        secret_master_keys.remove(&row_id).await?;
        pruned = pruned.saturating_add(1);
    }
    Ok(pruned)
}

/// Returns master-key ids referenced by the converged visible secret set.
fn referenced_secret_master_key_ids(
    secrets: &SecretStore,
) -> mantissa_store::Result<HashSet<uuid::Uuid>> {
    let (entries, _) = secrets.load_all()?;
    let mut key_ids = HashSet::new();
    for (_, snapshot) in entries {
        for secret in snapshot.as_slice() {
            key_ids.insert(secret.current_version.master_key_id);
            key_ids.insert(secret.current_version.ciphertext.master_key_id);
        }
    }
    Ok(key_ids)
}

/// Computes a small startup jitter so nodes do not sweep at exactly the same instant.
fn initial_jitter(interval: Duration) -> Duration {
    let interval_ms = interval.as_millis().min(u128::from(u64::MAX)) as u64;
    if interval_ms <= 1 {
        return Duration::ZERO;
    }
    Duration::from_millis(now_unix_ms() % interval_ms)
}

/// Returns the current local wall-clock time as Unix milliseconds.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ClusterId, ClusterViewId};
    use crate::secrets::master_key::envelope::{
        MasterKeyDescriptor, MasterKeyPlaintext, MasterKeyTransfer,
    };
    use crate::secrets::types::{SecretCiphertext, SecretMetadata, SecretValue, SecretVersion};
    use crate::store::replicated::secret_key_sync::{
        current_from_descriptor, current_row_id, descriptor_row_id, grant_row_id,
        open_secret_master_key_store, upsert_record,
    };
    use crate::store::replicated::secrets::open_secret_store;
    use mantissa_net::noise::NoiseKeys;
    use mantissa_store::uuid_key::UuidKey;
    use std::sync::Arc;

    /// Builds deterministic descriptor metadata for semantic master-key GC tests.
    fn descriptor(
        key_id: uuid::Uuid,
        generation: u64,
        scope_view: ClusterViewId,
    ) -> MasterKeyDescriptor {
        MasterKeyDescriptor {
            key_id,
            generation,
            scope_view,
            origin_view: scope_view,
            created_by_node_id: uuid::Uuid::from_u128(10),
            created_by_operation_id: None,
            parent_key_ids: Vec::new(),
            created_at_unix_secs: generation,
        }
    }

    /// Builds one replicated secret value referencing the provided master key.
    fn secret_value(name: &str, key_id: uuid::Uuid, generation: u64) -> SecretValue {
        let ciphertext = SecretCiphertext {
            master_key_id: key_id,
            master_key_generation: generation,
            nonce: [1; 12],
            ciphertext: vec![2, 3, 4],
            digest: [5; 32],
        };
        let version = SecretVersion::new(
            uuid::Uuid::from_u128(50),
            ciphertext,
            "now",
            None,
            key_id,
            generation,
        );
        SecretValue::new(name, SecretMetadata::default(), "now", version)
    }

    #[test]
    fn semantic_master_key_gc_owner_is_order_independent() {
        let active_view = ClusterViewId::new(ClusterId::from_uuid(uuid::Uuid::from_u128(100)), 3);
        let local = uuid::Uuid::from_u128(1);
        let peer_a = uuid::Uuid::from_u128(2);
        let peer_b = uuid::Uuid::from_u128(3);

        let first = select_secret_master_key_gc_owner(active_view, local, &[peer_a, peer_b]);
        let second = select_secret_master_key_gc_owner(active_view, local, &[peer_b, peer_a]);

        assert_eq!(first, second);
        assert!(matches!(first, Some(owner) if [local, peer_a, peer_b].contains(&owner)));
    }

    #[test]
    fn semantic_master_key_gc_owner_moves_when_owner_is_not_active() {
        let active_view = ClusterViewId::new(ClusterId::from_uuid(uuid::Uuid::from_u128(100)), 3);
        let nodes = [
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(2),
            uuid::Uuid::from_u128(3),
        ];
        let owner = select_secret_master_key_gc_owner(active_view, nodes[0], &nodes[1..])
            .expect("owner selected");
        let remaining = nodes
            .into_iter()
            .filter(|node_id| *node_id != owner)
            .collect::<Vec<_>>();

        let next = select_secret_master_key_gc_owner(active_view, remaining[0], &remaining[1..])
            .expect("replacement owner selected");

        assert_ne!(next, owner);
        assert!(remaining.contains(&next));
    }

    /// Builds one encrypted grant row for the provided master-key descriptor.
    fn grant_record(descriptor: &MasterKeyDescriptor) -> SecretMasterKeySyncRecord {
        let sender_id = uuid::Uuid::from_u128(20);
        let recipient_id = uuid::Uuid::from_u128(21);
        let sender = NoiseKeys::from_private_bytes([7; 32]);
        let recipient = NoiseKeys::from_private_bytes([8; 32]);
        let plaintext = MasterKeyPlaintext::new([9; 32]);
        SecretMasterKeySyncRecord::Grant(
            MasterKeyTransfer::encrypt(
                descriptor.clone(),
                &plaintext,
                sender_id,
                &sender,
                recipient_id,
                recipient.public_bytes(),
            )
            .expect("encrypt test master-key grant"),
        )
    }

    #[tokio::test]
    async fn semantic_master_key_gc_prunes_only_converged_unreferenced_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Arc::new(redb::Database::create(dir.path().join("gc.redb")).expect("db"));
        let actor = uuid::Uuid::from_u128(1);
        let peer = uuid::Uuid::from_u128(2);
        let secrets = open_secret_store(db.clone(), actor).expect("secret store");
        let master_keys = open_secret_master_key_store(db, actor).expect("secret master-key store");
        let progress = SyncGcProgress::new();
        let active_view = ClusterViewId::new(ClusterId::from_uuid(uuid::Uuid::from_u128(100)), 3);
        let referenced_view =
            ClusterViewId::new(ClusterId::from_uuid(uuid::Uuid::from_u128(101)), 2);
        let unreferenced_view =
            ClusterViewId::new(ClusterId::from_uuid(uuid::Uuid::from_u128(102)), 2);

        let active_key = uuid::Uuid::from_u128(1_000);
        let referenced_key = uuid::Uuid::from_u128(1_001);
        let unreferenced_key = uuid::Uuid::from_u128(1_002);
        let active = descriptor(active_key, 3, active_view);
        let referenced = descriptor(referenced_key, 2, referenced_view);
        let unreferenced = descriptor(unreferenced_key, 2, unreferenced_view);

        for record in [
            SecretMasterKeySyncRecord::Descriptor(active.clone()),
            SecretMasterKeySyncRecord::Current(current_from_descriptor(&active)),
            SecretMasterKeySyncRecord::Descriptor(referenced.clone()),
            SecretMasterKeySyncRecord::Current(current_from_descriptor(&referenced)),
            SecretMasterKeySyncRecord::Descriptor(unreferenced.clone()),
            SecretMasterKeySyncRecord::Current(current_from_descriptor(&unreferenced)),
            grant_record(&unreferenced),
        ] {
            upsert_record(&master_keys, record)
                .await
                .expect("upsert master-key row");
        }

        let secret = secret_value("referenced-key-secret", referenced_key, 2);
        secrets
            .upsert(&UuidKey::from(secret.id), secret)
            .await
            .expect("upsert secret");

        let secrets_root_digest = secrets
            .root_digest_at_version(1)
            .await
            .expect("secret root digest");
        progress.record_equal_root(
            peer,
            Domain::Secrets,
            active_view,
            1,
            secrets_root_digest,
            10,
        );
        let skipped = prune_unreferenced_secret_master_key_rows(
            &secrets,
            &master_keys,
            &progress,
            &[peer],
            active_view,
            1,
            20,
        )
        .await
        .expect("skip without master-key barrier");
        assert_eq!(skipped, 0);

        let master_keys_root_digest = master_keys
            .root_digest_at_version(1)
            .await
            .expect("master-key root digest");
        progress.record_equal_root(
            peer,
            Domain::SecretMasterKeys,
            active_view,
            1,
            master_keys_root_digest,
            10,
        );
        let pruned = prune_unreferenced_secret_master_key_rows(
            &secrets,
            &master_keys,
            &progress,
            &[peer],
            active_view,
            1,
            20,
        )
        .await
        .expect("semantic master-key prune");
        assert_eq!(pruned, 4);

        assert!(
            master_keys
                .has_tombstone(&UuidKey::from(descriptor_row_id(unreferenced_key)))
                .expect("unreferenced descriptor tombstone")
        );
        assert!(
            master_keys
                .has_tombstone(&UuidKey::from(current_row_id(referenced_view)))
                .expect("referenced stale current tombstone")
        );
        assert!(
            master_keys
                .has_tombstone(&UuidKey::from(current_row_id(unreferenced_view)))
                .expect("unreferenced current tombstone")
        );
        assert!(
            master_keys
                .has_tombstone(&UuidKey::from(grant_row_id(
                    unreferenced_key,
                    uuid::Uuid::from_u128(21),
                )))
                .expect("unreferenced grant tombstone")
        );
        assert!(
            master_keys
                .get_snapshot(&UuidKey::from(descriptor_row_id(active_key)))
                .expect("active descriptor snapshot")
                .is_some()
        );
        assert!(
            master_keys
                .get_snapshot(&UuidKey::from(descriptor_row_id(referenced_key)))
                .expect("referenced descriptor snapshot")
                .is_some()
        );
        assert!(
            master_keys
                .get_snapshot(&UuidKey::from(current_row_id(active_view)))
                .expect("active current snapshot")
                .is_some()
        );
    }

    #[tokio::test]
    async fn semantic_master_key_gc_rejects_stale_master_key_root_barrier() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Arc::new(redb::Database::create(dir.path().join("gc.redb")).expect("db"));
        let actor = uuid::Uuid::from_u128(1);
        let peer = uuid::Uuid::from_u128(2);
        let secrets = open_secret_store(db.clone(), actor).expect("secret store");
        let master_keys = open_secret_master_key_store(db, actor).expect("secret master-key store");
        let progress = SyncGcProgress::new();
        let active_view = ClusterViewId::new(ClusterId::from_uuid(uuid::Uuid::from_u128(100)), 3);

        let active_key = uuid::Uuid::from_u128(2_000);
        let stale_key = uuid::Uuid::from_u128(2_001);
        let active = descriptor(active_key, 3, active_view);
        for record in [
            SecretMasterKeySyncRecord::Descriptor(active.clone()),
            SecretMasterKeySyncRecord::Current(current_from_descriptor(&active)),
        ] {
            upsert_record(&master_keys, record)
                .await
                .expect("upsert active master-key row");
        }

        let secrets_root_digest = secrets
            .root_digest_at_version(1)
            .await
            .expect("secret root digest");
        let stale_master_keys_root_digest = master_keys
            .root_digest_at_version(1)
            .await
            .expect("stale master-key root digest");
        progress.record_equal_root(
            peer,
            Domain::Secrets,
            active_view,
            1,
            secrets_root_digest,
            10,
        );
        progress.record_equal_root(
            peer,
            Domain::SecretMasterKeys,
            active_view,
            1,
            stale_master_keys_root_digest,
            10,
        );

        let stale = descriptor(stale_key, 2, active_view);
        upsert_record(
            &master_keys,
            SecretMasterKeySyncRecord::Descriptor(stale.clone()),
        )
        .await
        .expect("upsert stale unreferenced descriptor after barrier");

        let pruned = prune_unreferenced_secret_master_key_rows(
            &secrets,
            &master_keys,
            &progress,
            &[peer],
            active_view,
            1,
            20,
        )
        .await
        .expect("semantic master-key prune with stale barrier");

        assert_eq!(pruned, 0);
        assert!(
            master_keys
                .get_snapshot(&UuidKey::from(descriptor_row_id(stale_key)))
                .expect("stale descriptor snapshot")
                .is_some(),
            "stale root equality must not authorize pruning rows added after that equality"
        );
    }
}
