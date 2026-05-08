//! Registry for replicated CRDT stores keyed by sync domain.
//!
//! Sync, GC, and root-schema maintenance all need the same small set of
//! store operations across every replicated domain. This registry centralizes
//! the domain-to-store mapping so those callers can iterate or look up a store
//! without repeating large `Domain` match blocks.

use crate::store::replicated::agents::AgentStore;
use crate::store::replicated::cluster_views::ClusterViewDomainStore;
use crate::store::replicated::jobs::JobStore;
use crate::store::replicated::networks::{
    NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore,
};
use crate::store::replicated::peers::PeersStore;
use crate::store::replicated::scheduler_digests::SchedulerDigestStore;
use crate::store::replicated::secret_key_sync::SecretMasterKeyStore;
use crate::store::replicated::secrets::SecretStore;
use crate::store::replicated::services::ServiceStore;
use crate::store::replicated::volumes::{VolumeNodeStore, VolumeSpecStore};
use crate::store::replicated::workloads::WorkloadStore;
use mantissa_protocol::sync::Domain;
use mantissa_store::adapter::RegAdapter;
use mantissa_store::codec::TombstoneRecord;
use mantissa_store::error::Error as StoreError;
use mantissa_store::gc::{GcBarrier, StoreGcPolicy, StoreGcReport};
use mantissa_store::mst_store::{
    CrdtMstStore, Entry, Registers, TombstonePruneFrontiers, Tombstones,
};
use mantissa_store::uuid_key::UuidKey;
use mantissa_store::{PageDigestRange, TableSet};
use merkle_search_tree::digest::Hasher as MstHasher;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Opaque encoded register row used by the sync wire protocol.
pub type EncodedRegister = (Vec<u8>, Vec<u8>);

/// Batch of opaque encoded register rows used by the sync wire protocol.
pub type EncodedRegisters = Vec<EncodedRegister>;

/// Opaque encoded tombstone row used by the sync wire protocol.
pub type EncodedTombstone = (Vec<u8>, u64, Vec<u8>);

/// Batch of opaque encoded tombstone rows used by the sync wire protocol.
pub type EncodedTombstones = Vec<EncodedTombstone>;

/// Boxed future returned by object-safe replicated store operations.
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Decoded register batch for one concrete CRDT adapter.
type DecodedRegisters<C> = Registers<UuidKey, <C as RegAdapter>::Reg>;

/// Decoded register and tombstone batch for one concrete CRDT adapter.
type DecodedDelta<C> = (DecodedRegisters<C>, Tombstones<UuidKey>);

/// Canonical full-sync domain set shared by all replicated-store callers.
pub const REPLICATED_DOMAINS: [Domain; 14] = [
    Domain::Peers,
    Domain::Workloads,
    Domain::Services,
    Domain::Jobs,
    Domain::Agents,
    Domain::Secrets,
    Domain::Networks,
    Domain::NetworkPeers,
    Domain::NetworkAttachments,
    Domain::ClusterViews,
    Domain::Volumes,
    Domain::VolumeNodes,
    Domain::SchedulerDigests,
    Domain::SecretMasterKeys,
];

/// Returns the debug label associated with one replicated sync domain.
pub fn domain_label(domain: Domain) -> &'static str {
    match domain {
        Domain::Peers => "peers",
        Domain::Workloads => "workloads",
        Domain::Services => "services",
        Domain::Jobs => "jobs",
        Domain::Agents => "agents",
        Domain::Secrets => "secrets",
        Domain::SecretMasterKeys => "secret master keys",
        Domain::Networks => "networks",
        Domain::NetworkPeers => "network peers",
        Domain::NetworkAttachments => "network attachments",
        Domain::ClusterViews => "cluster views",
        Domain::Volumes => "volumes",
        Domain::VolumeNodes => "volume nodes",
        Domain::SchedulerDigests => "scheduler digests",
    }
}

/// Maps a replicated sync domain to a stable compact key for in-memory indexes.
pub fn domain_key(domain: Domain) -> u16 {
    match domain {
        Domain::Peers => 0,
        Domain::Workloads => 1,
        Domain::Services => 2,
        Domain::Jobs => 3,
        Domain::Agents => 4,
        Domain::Secrets => 5,
        Domain::Networks => 6,
        Domain::NetworkPeers => 7,
        Domain::NetworkAttachments => 8,
        Domain::ClusterViews => 9,
        Domain::Volumes => 10,
        Domain::VolumeNodes => 11,
        Domain::SchedulerDigests => 12,
        Domain::SecretMasterKeys => 13,
    }
}

/// Object-safe surface shared by every replicated MST-backed domain store.
pub trait ReplicatedDomainStore: Send + Sync {
    /// Reads this store's current MST root at the requested semantic root schema.
    fn root_digest_at_version<'a>(
        &'a self,
        root_schema_version: u32,
    ) -> StoreFuture<'a, mantissa_store::Result<[u8; 16]>>;

    /// Reads this store's page-range summary at the requested semantic root schema.
    fn page_range_summary_at_version<'a>(
        &'a self,
        root_schema_version: u32,
    ) -> StoreFuture<'a, mantissa_store::Result<Vec<PageDigestRange>>>;

    /// Rebuilds this store's in-memory MST for the requested semantic root schema.
    fn rebuild_mst_from_disk_at_version<'a>(
        &'a self,
        root_schema_version: u32,
    ) -> StoreFuture<'a, mantissa_store::Result<()>>;

    /// Loads this store's durable tombstone prune frontiers.
    fn load_tombstone_prune_frontiers(&self) -> mantissa_store::Result<TombstonePruneFrontiers>;

    /// Applies peer tombstone prune frontiers to this store.
    fn apply_tombstone_prune_frontiers<'a>(
        &'a self,
        frontiers: TombstonePruneFrontiers,
    ) -> StoreFuture<'a, mantissa_store::Result<usize>>;

    /// Exports and encodes deltas for the requested MST page ranges.
    fn export_delta_encoded(
        &self,
        want_ranges: &[PageDigestRange],
    ) -> mantissa_store::Result<(EncodedRegisters, EncodedTombstones)>;

    /// Decodes and applies one incoming sync delta to this store.
    fn apply_delta_encoded<'a>(
        &'a self,
        registers: EncodedRegisters,
        tombstones: EncodedTombstones,
    ) -> StoreFuture<'a, mantissa_store::Result<()>>;

    /// Runs tombstone GC on this store using a caller-provided safety barrier.
    fn garbage_collect_tombstones<'a>(
        &'a self,
        policy: &'a StoreGcPolicy,
        barrier: GcBarrier,
        now_unix_ms: u64,
    ) -> StoreFuture<'a, mantissa_store::Result<StoreGcReport>>;

    /// Runs MVReg compaction on this store using the domain's compaction ranker.
    fn compact_registers<'a>(
        &'a self,
        policy: &'a StoreGcPolicy,
    ) -> StoreFuture<'a, mantissa_store::Result<StoreGcReport>>;

    /// Emits a root debug dump when store debug dumping is enabled.
    fn debug_dump_root<'a>(&'a self, label: &'a str) -> StoreFuture<'a, ()>;

    /// Emits page-range debug dumps when store debug dumping is enabled.
    fn debug_dump_ranges<'a>(&'a self, label: &'a str, limit: usize) -> StoreFuture<'a, ()>;
}

impl<C, H, T> ReplicatedDomainStore for CrdtMstStore<C, H, T>
where
    C: RegAdapter<Key = UuidKey> + Send + Sync + 'static,
    H: MstHasher<16, C::Key>
        + MstHasher<16, Entry<C::Snapshot>>
        + Default
        + Clone
        + Send
        + Sync
        + 'static,
    C::Actor: Send + Sync,
    C::Snapshot: Send + Sync,
    T: TableSet + Send + Sync + 'static,
{
    /// Reads this store's current MST root at the requested semantic root schema.
    fn root_digest_at_version<'a>(
        &'a self,
        root_schema_version: u32,
    ) -> StoreFuture<'a, mantissa_store::Result<[u8; 16]>> {
        Box::pin(async move {
            CrdtMstStore::<C, H, T>::root_digest_at_version(self, root_schema_version).await
        })
    }

    /// Reads this store's page-range summary at the requested semantic root schema.
    fn page_range_summary_at_version<'a>(
        &'a self,
        root_schema_version: u32,
    ) -> StoreFuture<'a, mantissa_store::Result<Vec<PageDigestRange>>> {
        Box::pin(async move {
            CrdtMstStore::<C, H, T>::page_range_summary_at_version(self, root_schema_version).await
        })
    }

    /// Rebuilds this store's in-memory MST for the requested semantic root schema.
    fn rebuild_mst_from_disk_at_version<'a>(
        &'a self,
        root_schema_version: u32,
    ) -> StoreFuture<'a, mantissa_store::Result<()>> {
        Box::pin(async move {
            CrdtMstStore::<C, H, T>::rebuild_mst_from_disk_at_version(self, root_schema_version)
                .await
        })
    }

    /// Loads this store's durable tombstone prune frontiers.
    fn load_tombstone_prune_frontiers(&self) -> mantissa_store::Result<TombstonePruneFrontiers> {
        CrdtMstStore::<C, H, T>::load_tombstone_prune_frontiers(self)
    }

    /// Applies peer tombstone prune frontiers to this store.
    fn apply_tombstone_prune_frontiers<'a>(
        &'a self,
        frontiers: TombstonePruneFrontiers,
    ) -> StoreFuture<'a, mantissa_store::Result<usize>> {
        Box::pin(async move {
            CrdtMstStore::<C, H, T>::apply_tombstone_prune_frontiers(self, frontiers).await
        })
    }

    /// Exports and encodes deltas for the requested MST page ranges.
    fn export_delta_encoded(
        &self,
        want_ranges: &[PageDigestRange],
    ) -> mantissa_store::Result<(EncodedRegisters, EncodedTombstones)> {
        let (registers, tombstones) =
            CrdtMstStore::<C, H, T>::export_page_ranges_delta(self, want_ranges)?;
        let registers = CrdtMstStore::<C, H, T>::encode_register_delta(self, registers)?;
        Ok((registers, encode_tombstones(tombstones)))
    }

    /// Decodes and applies one incoming sync delta to this store.
    fn apply_delta_encoded<'a>(
        &'a self,
        registers: EncodedRegisters,
        tombstones: EncodedTombstones,
    ) -> StoreFuture<'a, mantissa_store::Result<()>> {
        let decoded = decode_delta::<C>(registers, tombstones);
        Box::pin(async move {
            let (registers, tombstones) = decoded?;
            CrdtMstStore::<C, H, T>::apply_delta_chunk_update_mst(self, registers, tombstones)
                .await
                .map_err(|error| Box::new(StoreError::from(error)))
        })
    }

    /// Runs tombstone GC on this store using a caller-provided safety barrier.
    fn garbage_collect_tombstones<'a>(
        &'a self,
        policy: &'a StoreGcPolicy,
        barrier: GcBarrier,
        now_unix_ms: u64,
    ) -> StoreFuture<'a, mantissa_store::Result<StoreGcReport>> {
        Box::pin(async move {
            CrdtMstStore::<C, H, T>::garbage_collect_tombstones(self, policy, barrier, now_unix_ms)
                .await
        })
    }

    /// Runs MVReg compaction on this store using the domain's compaction ranker.
    fn compact_registers<'a>(
        &'a self,
        policy: &'a StoreGcPolicy,
    ) -> StoreFuture<'a, mantissa_store::Result<StoreGcReport>> {
        Box::pin(async move { CrdtMstStore::<C, H, T>::compact_registers(self, policy).await })
    }

    /// Emits a root debug dump when store debug dumping is enabled.
    fn debug_dump_root<'a>(&'a self, label: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(async move { CrdtMstStore::<C, H, T>::debug_dump_root(self, label).await })
    }

    /// Emits page-range debug dumps when store debug dumping is enabled.
    fn debug_dump_ranges<'a>(&'a self, label: &'a str, limit: usize) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            CrdtMstStore::<C, H, T>::debug_dump_ranges(self, label, limit).await;
        })
    }
}

/// Registry entry tying one sync domain to its replicated store handle.
#[derive(Clone)]
pub struct ReplicatedStoreEntry {
    pub domain: Domain,
    pub label: &'static str,
    pub store: Arc<dyn ReplicatedDomainStore>,
}

impl ReplicatedStoreEntry {
    /// Builds one registry entry for a concrete replicated domain store.
    pub fn new<S>(domain: Domain, store: Arc<S>) -> Self
    where
        S: ReplicatedDomainStore + 'static,
    {
        Self {
            domain,
            label: domain_label(domain),
            store,
        }
    }

    /// Builds a stable debug label for this domain and sync phase.
    pub fn dump_label(&self, prefix: &str) -> String {
        format!("{prefix}.{}", self.label)
    }
}

/// Shared registry of all replicated MST-backed domain stores.
#[derive(Clone)]
pub struct ReplicatedStoreRegistry {
    entries: Arc<[ReplicatedStoreEntry]>,
}

impl ReplicatedStoreRegistry {
    /// Builds a replicated-store registry in canonical domain order.
    pub fn new(entries: impl IntoIterator<Item = ReplicatedStoreEntry>) -> Self {
        let entries = entries.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(
            entries
                .iter()
                .map(|entry| entry.domain)
                .collect::<Vec<_>>()
                .as_slice(),
            REPLICATED_DOMAINS.as_slice(),
            "replicated store registry should be built in canonical domain order"
        );
        Self {
            entries: entries.into(),
        }
    }

    /// Returns every replicated store entry in canonical sync order.
    pub fn entries(&self) -> &[ReplicatedStoreEntry] {
        &self.entries
    }

    /// Returns the replicated store entry for one sync domain.
    pub fn get(&self, domain: Domain) -> Option<&ReplicatedStoreEntry> {
        self.entries.iter().find(|entry| entry.domain == domain)
    }

    /// Returns the replicated store entry or a storage error if the registry is incomplete.
    pub fn require(&self, domain: Domain) -> mantissa_store::Result<&ReplicatedStoreEntry> {
        self.get(domain).ok_or_else(|| {
            Box::new(StoreError::Other(format!(
                "no replicated store registered for domain {domain:?}"
            )))
        })
    }

    /// Reads one domain's current MST root at the requested semantic root schema.
    pub async fn root_digest_at_version(
        &self,
        domain: Domain,
        root_schema_version: u32,
    ) -> mantissa_store::Result<[u8; 16]> {
        self.require(domain)?
            .store
            .root_digest_at_version(root_schema_version)
            .await
    }

    /// Reads one domain's page-range summary at the requested semantic root schema.
    pub async fn page_range_summary_at_version(
        &self,
        domain: Domain,
        root_schema_version: u32,
    ) -> mantissa_store::Result<Vec<PageDigestRange>> {
        self.require(domain)?
            .store
            .page_range_summary_at_version(root_schema_version)
            .await
    }

    /// Applies remote tombstone prune frontiers to one replicated domain store.
    pub async fn apply_tombstone_prune_frontiers(
        &self,
        domain: Domain,
        frontiers: TombstonePruneFrontiers,
    ) -> mantissa_store::Result<usize> {
        self.require(domain)?
            .store
            .apply_tombstone_prune_frontiers(frontiers)
            .await
    }

    /// Rebuilds every replicated in-memory MST for one semantic root schema.
    pub async fn rebuild_msts_for_root_schema_version(
        &self,
        root_schema_version: u32,
    ) -> mantissa_store::Result<()> {
        for entry in self.entries() {
            entry
                .store
                .rebuild_mst_from_disk_at_version(root_schema_version)
                .await?;
        }
        Ok(())
    }
}

/// Builds the replicated store registry from the concrete stores opened at bootstrap.
pub fn replicated_store_registry(stores: ReplicatedStoreHandles) -> ReplicatedStoreRegistry {
    ReplicatedStoreRegistry::new([
        ReplicatedStoreEntry::new(Domain::Peers, stores.peers),
        ReplicatedStoreEntry::new(Domain::Workloads, stores.workloads),
        ReplicatedStoreEntry::new(Domain::Services, stores.services),
        ReplicatedStoreEntry::new(Domain::Jobs, stores.jobs),
        ReplicatedStoreEntry::new(Domain::Agents, stores.agents),
        ReplicatedStoreEntry::new(Domain::Secrets, stores.secrets),
        ReplicatedStoreEntry::new(Domain::Networks, stores.networks),
        ReplicatedStoreEntry::new(Domain::NetworkPeers, stores.network_peers),
        ReplicatedStoreEntry::new(Domain::NetworkAttachments, stores.network_attachments),
        ReplicatedStoreEntry::new(Domain::ClusterViews, stores.cluster_views),
        ReplicatedStoreEntry::new(Domain::Volumes, stores.volumes),
        ReplicatedStoreEntry::new(Domain::VolumeNodes, stores.volume_nodes),
        ReplicatedStoreEntry::new(Domain::SchedulerDigests, stores.scheduler_digests),
        ReplicatedStoreEntry::new(Domain::SecretMasterKeys, stores.secret_master_keys),
    ])
}

/// Concrete replicated store handles needed to build a registry.
pub struct ReplicatedStoreHandles {
    pub peers: PeersStore,
    pub workloads: WorkloadStore,
    pub services: ServiceStore,
    pub jobs: JobStore,
    pub agents: AgentStore,
    pub secrets: SecretStore,
    pub secret_master_keys: SecretMasterKeyStore,
    pub networks: NetworkSpecStore,
    pub network_peers: NetworkPeerStore,
    pub network_attachments: NetworkAttachmentStore,
    pub cluster_views: ClusterViewDomainStore,
    pub volumes: VolumeSpecStore,
    pub volume_nodes: VolumeNodeStore,
    pub scheduler_digests: SchedulerDigestStore,
}

/// Encodes tombstone rows into the compact sync wire representation.
fn encode_tombstones(tombstones: Tombstones<UuidKey>) -> EncodedTombstones {
    tombstones
        .into_iter()
        .map(|(key, tombstone)| {
            (
                key.as_ref().to_vec(),
                tombstone.sequence,
                tombstone.origin_actor,
            )
        })
        .collect()
}

/// Decodes one sync wire delta into typed register and tombstone rows.
fn decode_delta<C>(
    registers: EncodedRegisters,
    tombstones: EncodedTombstones,
) -> mantissa_store::Result<DecodedDelta<C>>
where
    C: RegAdapter<Key = UuidKey>,
{
    let registers = decode_registers::<C>(registers)?;
    let tombstones = decode_tombstones::<C>(tombstones)?;
    Ok((registers, tombstones))
}

/// Decodes opaque register payloads with the destination store adapter.
fn decode_registers<C>(registers: EncodedRegisters) -> mantissa_store::Result<DecodedRegisters<C>>
where
    C: RegAdapter<Key = UuidKey>,
{
    let mut decoded = Vec::with_capacity(registers.len());
    for (key_bytes, register_bytes) in registers {
        let key = UuidKey::try_from(key_bytes.as_slice())
            .map_err(|error| Box::new(StoreError::from(error)))?;
        let register = C::decode_reg(register_bytes.as_slice())?;
        decoded.push((key, register));
    }
    Ok(decoded)
}

/// Decodes opaque tombstone payloads with the destination store actor codec.
fn decode_tombstones<C>(
    tombstones: EncodedTombstones,
) -> mantissa_store::Result<Tombstones<UuidKey>>
where
    C: RegAdapter<Key = UuidKey>,
{
    let mut decoded = Vec::with_capacity(tombstones.len());
    for (key_bytes, sequence, origin_actor) in tombstones {
        let key = UuidKey::try_from(key_bytes.as_slice())
            .map_err(|error| Box::new(StoreError::from(error)))?;
        let actor = C::actor_from_bytes(origin_actor.as_slice())
            .map_err(|error| Box::new(StoreError::from(error)))?;
        decoded.push((
            key,
            TombstoneRecord::new(sequence, C::actor_to_bytes(&actor), 0),
        ));
    }
    Ok(decoded)
}
