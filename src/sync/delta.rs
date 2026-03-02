use crate::cluster::ClusterViewId;
use crate::network::types::{NetworkAttachmentValue, NetworkPeerStateValue, NetworkSpecValue};
use crate::secrets::types::SecretValue;
use crate::services::types::ServiceSpecValue;
use crate::store::cluster_view_store::{ClusterNameRecord, ClusterViewDomainStore};
use crate::store::network_store::{NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore};
use crate::store::peer_store::PeersStore;
use crate::store::secret_store::SecretStore;
use crate::store::service_store::ServiceStore;
use crate::store::task_store::TaskStore;
use crate::sync::ranges::{capnp_fill_ranges, page_ranges_from_capnp};
use crate::task::types::TaskValue;
use crate::topology::peers::PeerValue;
use async_trait::async_trait;
use bincode;
use capnp_rpc::new_client;
use crdt_store::{PageDigestRange, compute_want_from_have, uuid_key::UuidKey};
use crdts::MVReg;
use protocol::sync::{self, Domain, delta_chunk, delta_sink};
use std::io;
use std::rc::Rc;
use tracing::{debug, warn};

type RegisterDelta<V> = Vec<(UuidKey, MVReg<V, uuid::Uuid>)>;
type TombstoneDelta = Vec<(UuidKey, u64)>;

#[derive(Clone)]
pub struct SyncStores {
    pub peers: PeersStore,
    pub tasks: TaskStore,
    pub services: ServiceStore,
    pub secrets: SecretStore,
    pub networks: NetworkSpecStore,
    pub network_peers: NetworkPeerStore,
    pub network_attachments: NetworkAttachmentStore,
    pub cluster_views: ClusterViewDomainStore,
}

/// Carries one peer-scoped context for anti-entropy diagnostics.
#[derive(Clone, Debug)]
pub struct SyncTraceContext {
    pub peer_id: uuid::Uuid,
    pub peer_addr: String,
    pub reason: &'static str,
}

impl SyncTraceContext {
    /// # Description:
    ///
    /// Builds one peer-scoped trace context used by sync diagnostics.
    pub fn peer(peer_id: uuid::Uuid, peer_addr: impl Into<String>, reason: &'static str) -> Self {
        Self {
            peer_id,
            peer_addr: peer_addr.into(),
            reason,
        }
    }
}

impl SyncStores {
    async fn root_hex(&self, domain: Domain) -> String {
        match domain {
            Domain::Peers => self.peers.root_hex().await,
            Domain::Tasks => self.tasks.root_hex().await,
            Domain::Services => self.services.root_hex().await,
            Domain::Secrets => self.secrets.root_hex().await,
            Domain::Networks => self.networks.root_hex().await,
            Domain::NetworkPeers => self.network_peers.root_hex().await,
            Domain::NetworkAttachments => self.network_attachments.root_hex().await,
            Domain::ClusterViews => self.cluster_views.root_hex().await,
        }
    }

    async fn page_range_summary(&self, domain: Domain) -> crdt_store::Result<Vec<PageDigestRange>> {
        match domain {
            Domain::Peers => self.peers.page_range_summary().await,
            Domain::Tasks => self.tasks.page_range_summary().await,
            Domain::Services => self.services.page_range_summary().await,
            Domain::Secrets => self.secrets.page_range_summary().await,
            Domain::Networks => self.networks.page_range_summary().await,
            Domain::NetworkPeers => self.network_peers.page_range_summary().await,
            Domain::NetworkAttachments => self.network_attachments.page_range_summary().await,
            Domain::ClusterViews => self.cluster_views.page_range_summary().await,
        }
    }
}

pub struct DeltaSinkImpl {
    stores: SyncStores,
    expected_view: ClusterViewId,
}

impl DeltaSinkImpl {
    pub fn new(stores: SyncStores, expected_view: ClusterViewId) -> Self {
        Self {
            stores,
            expected_view,
        }
    }
}

impl delta_sink::Server for DeltaSinkImpl {
    async fn push_chunk(
        self: Rc<Self>,
        params: delta_sink::PushChunkParams,
    ) -> Result<(), capnp::Error> {
        let chunk = params.get()?.get_chunk()?;
        let domain = chunk
            .get_domain()
            .map_err(|_| capnp::Error::failed("unknown sync domain".into()))?;
        let chunk_view =
            ClusterViewId::from_capnp(chunk.get_view()?).map_err(capnp::Error::failed)?;
        if chunk_view != self.expected_view {
            return Err(capnp::Error::failed(format!(
                "delta chunk view mismatch: expected {}, got {}",
                self.expected_view, chunk_view
            )));
        }
        debug!(
            target: "delta",
            cluster_view = %chunk_view,
            ?domain,
            "received delta chunk"
        );

        match domain {
            Domain::Peers => {
                apply_chunk(
                    self.stores.peers.clone(),
                    &chunk,
                    decode_register::<PeerValue>,
                )
                .await?
            }
            Domain::Tasks => {
                apply_chunk(
                    self.stores.tasks.clone(),
                    &chunk,
                    decode_register::<TaskValue>,
                )
                .await?
            }
            Domain::Services => {
                apply_chunk(
                    self.stores.services.clone(),
                    &chunk,
                    decode_register::<ServiceSpecValue>,
                )
                .await?
            }
            Domain::Secrets => {
                apply_chunk(
                    self.stores.secrets.clone(),
                    &chunk,
                    decode_register::<SecretValue>,
                )
                .await?
            }
            Domain::Networks => {
                apply_chunk(
                    self.stores.networks.clone(),
                    &chunk,
                    decode_register::<NetworkSpecValue>,
                )
                .await?
            }
            Domain::NetworkPeers => {
                apply_chunk(
                    self.stores.network_peers.clone(),
                    &chunk,
                    decode_register::<NetworkPeerStateValue>,
                )
                .await?
            }
            Domain::NetworkAttachments => {
                apply_chunk(
                    self.stores.network_attachments.clone(),
                    &chunk,
                    decode_register::<NetworkAttachmentValue>,
                )
                .await?
            }
            Domain::ClusterViews => {
                apply_chunk(
                    self.stores.cluster_views.clone(),
                    &chunk,
                    decode_register::<ClusterNameRecord>,
                )
                .await?
            }
        }

        Ok(())
    }

    async fn end(
        self: Rc<Self>,
        _params: delta_sink::EndParams,
        _results: delta_sink::EndResults,
    ) -> Result<(), capnp::Error> {
        debug!(target: "delta", "delta stream end");
        Ok(())
    }
}

async fn apply_chunk<V, F>(
    store: impl DeltaStore<V>,
    chunk: &delta_chunk::Reader<'_>,
    decode: F,
) -> Result<(), capnp::Error>
where
    V: Clone + Send + Sync + 'static,
    F: Fn(&delta_chunk::Reader<'_>) -> Result<RegisterDelta<V>, capnp::Error>,
{
    let regs = decode(chunk)?;
    let tombs = collect_tombstones(chunk)?;

    store.apply_delta(regs, tombs).await.map_err(to_capnp)
}

fn collect_tombstones(chunk: &delta_chunk::Reader<'_>) -> Result<TombstoneDelta, capnp::Error> {
    let mut tombs = Vec::new();
    for entry in chunk.get_tombs()?.iter() {
        let key =
            UuidKey::try_from(entry.get_key()?).map_err(|e| capnp::Error::failed(e.to_string()))?;
        tombs.push((key, entry.get_ts()));
    }
    Ok(tombs)
}

fn decode_register<V>(chunk: &delta_chunk::Reader<'_>) -> Result<RegisterDelta<V>, capnp::Error>
where
    V: for<'de> serde::Deserialize<'de>,
{
    let mut regs = Vec::new();
    for entry in chunk.get_regs()?.iter() {
        let key =
            UuidKey::try_from(entry.get_key()?).map_err(|e| capnp::Error::failed(e.to_string()))?;
        let register: MVReg<V, uuid::Uuid> = bincode::deserialize(entry.get_reg()?)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        regs.push((key, register));
    }
    Ok(regs)
}

fn to_capnp<E: std::fmt::Display>(e: E) -> capnp::Error {
    capnp::Error::failed(e.to_string())
}

#[async_trait]
trait DeltaStore<V>: Clone + Send + Sync + 'static {
    async fn apply_delta(self, regs: RegisterDelta<V>, tombs: TombstoneDelta) -> io::Result<()>;
}

#[async_trait]
impl DeltaStore<PeerValue> for PeersStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<PeerValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<TaskValue> for TaskStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<TaskValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<ServiceSpecValue> for ServiceStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<ServiceSpecValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<SecretValue> for SecretStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<SecretValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<NetworkSpecValue> for NetworkSpecStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<NetworkSpecValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<NetworkPeerStateValue> for NetworkPeerStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<NetworkPeerStateValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<NetworkAttachmentValue> for NetworkAttachmentStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<NetworkAttachmentValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<ClusterNameRecord> for ClusterViewDomainStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<ClusterNameRecord, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

pub async fn sync_all_domains(
    stores: SyncStores,
    sync_cap: sync::Client,
    cluster_view: ClusterViewId,
    trace: Option<SyncTraceContext>,
) {
    let res: Result<(), capnp::Error> = async {
        let domains = [
            Domain::Peers,
            Domain::Tasks,
            Domain::Services,
            Domain::Secrets,
            Domain::Networks,
            Domain::NetworkPeers,
            Domain::NetworkAttachments,
            Domain::ClusterViews,
        ];

        let mut roots_req = sync_cap.get_roots_for_view_request();
        {
            let mut req = roots_req.get().init_req();
            cluster_view.write_capnp(req.reborrow().init_view());
        }
        let roots_resp = roots_req.send().promise.await?;
        let roots_reader = roots_resp.get()?.get_roots()?;

        let mut remote_roots = Vec::with_capacity(roots_reader.len() as usize);
        for idx in 0..roots_reader.len() {
            let entry = roots_reader.get(idx);
            let root_view =
                ClusterViewId::from_capnp(entry.get_view()?).map_err(capnp::Error::failed)?;
            if root_view != cluster_view {
                return Err(capnp::Error::failed(format!(
                    "sync roots view mismatch: expected {cluster_view}, got {root_view}"
                )));
            }
            let domain = entry
                .get_domain()
                .map_err(|_| capnp::Error::failed("unknown domain".into()))?;
            let hex = entry.get_root_hex()?.to_string()?;
            remote_roots.push((domain, hex));
        }

        let mut domains_to_sync = Vec::new();
        for domain in domains.iter() {
            let local_root = stores.root_hex(*domain).await;
            let remote_root = remote_roots
                .iter()
                .find(|(d, _)| *d == *domain)
                .map(|(_, hex)| hex.clone())
                .unwrap_or_default();
            if remote_root != local_root {
                domains_to_sync.push(*domain);
            }
        }

        if domains_to_sync.is_empty() {
            return Ok(());
        }

        let mut ranges_req = sync_cap.get_ranges_for_view_request();
        {
            let mut req = ranges_req.get().init_req();
            cluster_view.write_capnp(req.reborrow().init_view());
            let mut list = req.reborrow().init_domains(domains_to_sync.len() as u32);
            for (idx, domain) in domains_to_sync.iter().enumerate() {
                list.set(idx as u32, *domain);
            }
        }
        let ranges_resp = ranges_req.send().promise.await?;
        let ranges_reader = ranges_resp.get()?.get_ranges()?;

        let mut domains_wants = Vec::new();
        for idx in 0..ranges_reader.len() {
            let summary = ranges_reader.get(idx);
            let summary_view =
                ClusterViewId::from_capnp(summary.get_view()?).map_err(capnp::Error::failed)?;
            if summary_view != cluster_view {
                return Err(capnp::Error::failed(format!(
                    "sync ranges view mismatch: expected {cluster_view}, got {summary_view}"
                )));
            }
            let domain = summary
                .get_domain()
                .map_err(|_| capnp::Error::failed("unknown domain".into()))?;
            let remote_summary = summary.get_summary()?;
            let remote_ranges = page_ranges_from_capnp(remote_summary)?;
            let local_ranges = stores.page_range_summary(domain).await.map_err(to_capnp)?;
            let want = compute_want_from_have(&remote_ranges, &local_ranges);
            if !want.is_empty() {
                domains_wants.push((domain, want));
            }
        }

        if domains_wants.is_empty() {
            return Ok(());
        }

        let sink_client = new_client(DeltaSinkImpl::new(stores.clone(), cluster_view));

        let mut od = sync_cap.open_delta_for_view_request();
        {
            let mut req = od.get().init_req();
            cluster_view.write_capnp(req.reborrow().init_view());
            let mut wants_builder = req.reborrow().init_wants(domains_wants.len() as u32);
            for (idx, (domain, want_ranges)) in domains_wants.iter().enumerate() {
                let mut entry = wants_builder.reborrow().get(idx as u32);
                entry.set_domain(*domain);
                let summary_builder = entry.reborrow().init_want();
                capnp_fill_ranges(want_ranges, summary_builder)?;
                cluster_view.write_capnp(entry.reborrow().init_view());
            }
            req.set_sink(sink_client);
        }

        debug!(
            target: "sync",
            cluster_view = %cluster_view,
            domains = ?domains_wants.len(),
            "opening multi-domain delta stream"
        );
        od.send().promise.await?;
        Ok(())
    }
    .await;

    if let Err(e) = res {
        warn!(
            target: "sync",
            cluster_view = %cluster_view,
            "sync_all_domains error: {e}"
        );
        if let Some(ctx) = trace.as_ref() {
            warn!(
                target: "diag.sync.peer",
                cluster_view = %cluster_view,
                peer = %ctx.peer_id,
                addr = %ctx.peer_addr,
                reason = %ctx.reason,
                disconnected = is_disconnected_capnp(&e),
                error = %e,
                "peer-scoped sync_all_domains failure"
            );
        }
    }
}

/// # Description:
///
/// Returns true when one Cap'n Proto error corresponds to a disconnected transport path.
fn is_disconnected_capnp(error: &capnp::Error) -> bool {
    let text = error.to_string();
    text.contains("Disconnected") || text.contains("disconnected")
}
