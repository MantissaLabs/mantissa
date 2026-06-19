use crate::ingress::codec::{read_spread_key, write_ingress_pool_spec};
use crate::ingress::registry::IngressPoolRegistry;
use crate::ingress::types::{IngressPoolSpecDraft, IngressPoolSpecValue, current_timestamp};
use crate::network::controller::NetworkController;
use crate::network::discovery::{
    PublicEndpointIngressMode, PublicEndpointKey, PublicEndpointSnapshot,
};
use crate::network::nodeport::NodePortProtocol;
use crate::network::registry::NetworkRegistry;
use crate::network::types::NetworkAttachmentState;
use crate::registry::Registry;
use crate::scheduler::placement::PlacementNode;
use crate::services::registry::ServiceRegistry;
use crate::services::types::{
    PublicIngressPolicy, ServicePortProtocol, ServiceSpecValue, ServiceStatus,
};
use crate::topology::Topology;
use crate::workload::capnp_codec::decode_placement_policy;
use capnp::Error;
use futures::{StreamExt, stream};
use mantissa_health::Status as HealthStatus;
use mantissa_protocol::info_capnp::public_endpoint_info;
use mantissa_protocol::ingress::{
    ingress, ingress_endpoint, ingress_endpoint_filter, ingress_pool_apply_spec,
};
use mantissa_protocol::server::cluster_session;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::rc::Rc;
use tracing::debug;
use uuid::Uuid;

const ENDPOINT_PEER_FANOUT: usize = 64;

/// Cap'n Proto RPC surface for replicated ingress pools and endpoint diagnostics.
pub struct IngressRpc {
    pools: IngressPoolRegistry,
    services: ServiceRegistry,
    networks: NetworkRegistry,
    registry: Registry,
    network_controller: NetworkController,
    topology: Topology,
}

impl IngressRpc {
    /// Constructs the ingress RPC service over the existing replicated pool registry.
    pub fn new(
        pools: IngressPoolRegistry,
        services: ServiceRegistry,
        networks: NetworkRegistry,
        registry: Registry,
        network_controller: NetworkController,
        topology: Topology,
    ) -> Self {
        Self {
            pools,
            services,
            networks,
            registry,
            network_controller,
            topology,
        }
    }

    /// Read a required text field from a request and reject blank values at the edge.
    fn read_non_empty_text(text: capnp::text::Reader<'_>, field: &str) -> Result<String, Error> {
        let value = text
            .to_str()
            .map_err(|error| Error::failed(format!("{field}: {error}")))?
            .trim()
            .to_string();
        if value.is_empty() {
            return Err(Error::failed(format!("{field} cannot be empty")));
        }
        Ok(value)
    }

    /// Decodes one ingress-pool apply request into a validated replicated spec.
    fn read_pool_apply_spec(
        &self,
        reader: ingress_pool_apply_spec::Reader<'_>,
    ) -> Result<IngressPoolSpecValue, Error> {
        let name = Self::read_non_empty_text(reader.get_name()?, "ingress pool name")?;
        let draft = IngressPoolSpecDraft {
            name: name.clone(),
            min_nodes: reader.get_min_nodes(),
            max_nodes: match reader.get_max_nodes() {
                0 => None,
                value => Some(value),
            },
            placement: decode_placement_policy(reader.get_placement()?)?,
            spread_by: read_spread_key(reader.get_spread_by()?)?,
        };
        let mut value = IngressPoolSpecValue::from_draft(draft).map_err(Error::failed)?;
        if let Some(current) = self.pools.get_by_name(&name).map_err(to_capnp)? {
            value.id = current.id;
            value.created_at = current.created_at;
            value.generation = current.generation.saturating_add(1);
            value.updated_at = current_timestamp();
        }
        Ok(value)
    }

    /// Resolves an ingress-pool selector as an exact UUID first, then as an exact pool name.
    fn get_pool_by_selector(&self, selector: &str) -> Result<Option<IngressPoolSpecValue>, Error> {
        let selector = selector.trim();
        if let Ok(id) = Uuid::parse_str(selector)
            && let Some(pool) = self.pools.get(id).map_err(to_capnp)?
        {
            return Ok(Some(pool));
        }
        self.pools.get_by_name(selector).map_err(to_capnp)
    }
}

#[derive(Clone, Debug)]
struct EndpointFilter {
    service: String,
    template: String,
    pool: String,
    port: u16,
    ready_only: bool,
}

impl EndpointFilter {
    /// Decodes the wire endpoint filter once so endpoint matching can run after awaits.
    fn from_reader(reader: ingress_endpoint_filter::Reader<'_>) -> Result<Self, Error> {
        Ok(Self {
            service: reader.get_service()?.to_str()?.trim().to_string(),
            template: reader.get_template()?.to_str()?.trim().to_string(),
            pool: reader.get_pool()?.to_str()?.trim().to_string(),
            port: reader.get_port(),
            ready_only: reader.get_ready_only(),
        })
    }

    /// Returns whether this filter admits one service row.
    fn matches_service(&self, service: &ServiceSpecValue) -> bool {
        self.service.is_empty()
            || self.service == service.id.to_string()
            || self.service == service.service_name
    }

    /// Returns whether this filter admits one task template name.
    fn matches_template(&self, template_name: &str) -> bool {
        self.template.is_empty() || self.template == template_name
    }

    /// Returns whether this filter admits one public port.
    fn matches_port(&self, port: u16) -> bool {
        self.port == 0 || self.port == port
    }

    /// Returns whether this filter admits one public ingress policy.
    fn matches_public_ingress(&self, ingress: &PublicIngressPolicy) -> bool {
        if self.pool.is_empty() {
            return true;
        }
        matches!(
            ingress,
            PublicIngressPolicy::IngressPool { pool } if pool.trim() == self.pool
        )
    }
}

#[derive(Clone, Debug)]
struct EndpointSourcePeer {
    id: Uuid,
    name: String,
}

#[derive(Clone, Debug)]
struct EndpointRow {
    snapshot: PublicEndpointSnapshot,
    service_name: Option<String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct EndpointRowKey {
    service_id: Uuid,
    template_name: String,
    network_id: Uuid,
    node_id: Uuid,
    public_port: u16,
    protocol: NodePortProtocol,
}

/// Converts local errors into Cap'n Proto RPC errors at the ingress boundary.
fn to_capnp<E: std::fmt::Display>(error: E) -> Error {
    Error::failed(error.to_string())
}

/// Returns the stable protocol label used in public endpoint rows.
fn nodeport_protocol_label(protocol: NodePortProtocol) -> &'static str {
    match protocol {
        NodePortProtocol::Tcp => "tcp",
        NodePortProtocol::Udp => "udp",
    }
}

/// Converts service manifest protocol intent into a NodePort dataplane protocol.
fn nodeport_protocol(protocol: ServicePortProtocol) -> NodePortProtocol {
    match protocol {
        ServicePortProtocol::Tcp => NodePortProtocol::Tcp,
        ServicePortProtocol::Udp => NodePortProtocol::Udp,
        ServicePortProtocol::TcpUdp => NodePortProtocol::Tcp,
    }
}

/// Returns stable display labels for the public endpoint ingress policy.
fn public_endpoint_ingress_labels(
    ingress: &PublicEndpointIngressMode,
) -> (&'static str, Option<&str>) {
    match ingress {
        PublicEndpointIngressMode::AllNodes => ("all_nodes", None),
        PublicEndpointIngressMode::TaskNodes => ("task_nodes", None),
        PublicEndpointIngressMode::IngressPool { pool } => ("ingress_pool", Some(pool.as_str())),
    }
}

/// Returns whether a service status still needs public endpoint source discovery.
fn service_reserves_public_endpoint(status: ServiceStatus) -> bool {
    !matches!(status, ServiceStatus::Stopping | ServiceStatus::Stopped)
}

/// Builds a template-to-task-id index using the service's flattened replica order.
fn service_template_task_ids(service: &ServiceSpecValue) -> HashMap<String, HashSet<Uuid>> {
    let replica_ids = service.assigned_replica_ids();
    let mut next_id = replica_ids.iter();
    let mut ids_by_template = HashMap::new();
    for template in &service.task_templates {
        let mut ids = HashSet::with_capacity(template.replicas as usize);
        for _ in 0..template.replicas {
            let Some(task_id) = next_id.next() else {
                break;
            };
            ids.insert(*task_id);
        }
        ids_by_template.insert(template.name.clone(), ids);
    }
    ids_by_template
}

/// Returns whether one local endpoint snapshot matches an endpoint-list filter.
fn endpoint_matches_filter(
    snapshot: &PublicEndpointSnapshot,
    service_name: Option<&str>,
    filter: &EndpointFilter,
) -> Result<bool, Error> {
    if !filter.service.is_empty()
        && filter.service != snapshot.key.service_id.to_string()
        && service_name != Some(filter.service.as_str())
    {
        return Ok(false);
    }

    if !filter.template.is_empty() && filter.template != snapshot.key.template_name {
        return Ok(false);
    }

    if !filter.pool.is_empty() {
        match &snapshot.ingress {
            PublicEndpointIngressMode::IngressPool {
                pool: snapshot_pool,
            } if snapshot_pool == &filter.pool => {}
            _ => return Ok(false),
        }
    }

    if filter.port != 0 && filter.port != snapshot.key.public_port {
        return Ok(false);
    }

    if filter.ready_only && !snapshot.ready {
        return Ok(false);
    }

    Ok(true)
}

/// Parses one public endpoint row from node-info into the ingress endpoint model.
fn read_public_endpoint_info(
    endpoint: public_endpoint_info::Reader<'_>,
) -> Result<PublicEndpointSnapshot, Error> {
    let service_id = Uuid::parse_str(endpoint.get_service_id()?.to_str()?)
        .map_err(|error| Error::failed(format!("invalid endpoint service id: {error}")))?;
    let network_id = Uuid::parse_str(endpoint.get_network_id()?.to_str()?)
        .map_err(|error| Error::failed(format!("invalid endpoint network id: {error}")))?;
    let node_id = Uuid::parse_str(endpoint.get_node_id()?.to_str()?)
        .map_err(|error| Error::failed(format!("invalid endpoint node id: {error}")))?;
    let protocol = match endpoint.get_protocol()?.to_str()?.trim() {
        "tcp" => NodePortProtocol::Tcp,
        "udp" => NodePortProtocol::Udp,
        other => {
            return Err(Error::failed(format!(
                "invalid endpoint protocol '{other}'"
            )));
        }
    };
    let ingress = match endpoint.get_ingress_mode()?.to_str()?.trim() {
        "all_nodes" => PublicEndpointIngressMode::AllNodes,
        "task_nodes" => PublicEndpointIngressMode::TaskNodes,
        "ingress_pool" => PublicEndpointIngressMode::IngressPool {
            pool: endpoint.get_ingress_pool()?.to_str()?.trim().to_string(),
        },
        other => {
            return Err(Error::failed(format!(
                "invalid endpoint ingress mode '{other}'"
            )));
        }
    };
    let node_ip = endpoint
        .get_node_ip()?
        .to_str()?
        .trim()
        .parse::<IpAddr>()
        .ok();
    let detail = endpoint
        .get_detail()?
        .to_str()
        .ok()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    Ok(PublicEndpointSnapshot {
        key: PublicEndpointKey {
            service_id,
            template_name: endpoint.get_template_name()?.to_str()?.to_string(),
            public_port: endpoint.get_public_port(),
            protocol,
            node_id,
        },
        network_id,
        node_ip,
        ingress,
        ready: endpoint.get_ready(),
        generation: endpoint.get_generation(),
        detail,
    })
}

/// Compare endpoint snapshots by the same stable fields exposed to operators.
fn compare_endpoint_snapshots(
    left: &PublicEndpointSnapshot,
    right: &PublicEndpointSnapshot,
) -> std::cmp::Ordering {
    left.key
        .service_id
        .cmp(&right.key.service_id)
        .then_with(|| left.key.template_name.cmp(&right.key.template_name))
        .then_with(|| left.key.public_port.cmp(&right.key.public_port))
        .then_with(|| {
            nodeport_protocol_label(left.key.protocol)
                .cmp(nodeport_protocol_label(right.key.protocol))
        })
        .then_with(|| left.key.node_id.cmp(&right.key.node_id))
        .then_with(|| left.network_id.cmp(&right.network_id))
}

/// Compare fully decorated endpoint rows for deterministic RPC output.
fn compare_endpoint_rows(left: &EndpointRow, right: &EndpointRow) -> std::cmp::Ordering {
    compare_endpoint_snapshots(&left.snapshot, &right.snapshot)
}

/// Returns the identity key used to reconcile expected and reported endpoint rows.
fn endpoint_row_key(snapshot: &PublicEndpointSnapshot) -> EndpointRowKey {
    EndpointRowKey {
        service_id: snapshot.key.service_id,
        template_name: snapshot.key.template_name.clone(),
        network_id: snapshot.network_id,
        node_id: snapshot.key.node_id,
        public_port: snapshot.key.public_port,
        protocol: snapshot.key.protocol,
    }
}

/// Serializes one local endpoint snapshot into the ingress endpoint response shape.
fn write_endpoint(
    mut builder: ingress_endpoint::Builder<'_>,
    snapshot: &PublicEndpointSnapshot,
    service_name: Option<&str>,
) {
    builder.set_service_id(snapshot.key.service_id.as_bytes());
    builder.set_service_name(service_name.unwrap_or(""));
    builder.set_template_name(&snapshot.key.template_name);
    builder.set_network_id(snapshot.network_id.as_bytes());
    builder.set_node_id(snapshot.key.node_id.as_bytes());
    let node_ip = snapshot
        .node_ip
        .map(|ip| ip.to_string())
        .unwrap_or_default();
    builder.set_node_ip(&node_ip);
    builder.set_public_port(snapshot.key.public_port);
    builder.set_protocol(nodeport_protocol_label(snapshot.key.protocol));
    let (ingress_mode, ingress_pool) = public_endpoint_ingress_labels(&snapshot.ingress);
    builder.set_ingress_mode(ingress_mode);
    builder.set_ingress_pool(ingress_pool.unwrap_or(""));
    builder.set_ready(snapshot.ready);
    builder.set_generation(snapshot.generation);
    builder.set_detail(snapshot.detail.as_deref().unwrap_or(""));
}

impl IngressRpc {
    /// Returns active peer metadata keyed by node id for endpoint source selection.
    fn active_peer_sources(&self) -> Result<HashMap<Uuid, EndpointSourcePeer>, Error> {
        let peers = self.registry.peer_values_snapshot().map_err(to_capnp)?;
        Ok(peers
            .into_iter()
            .map(|(id, value)| {
                let name = if value.hostname.trim().is_empty() {
                    id.to_string()
                } else {
                    value.hostname
                };
                (id, EndpointSourcePeer { id, name })
            })
            .collect())
    }

    /// Builds scheduler-visible ingress-pool candidates from converged peer metadata.
    fn ingress_pool_candidates(&self) -> Result<Vec<PlacementNode>, Error> {
        let health_snapshot = self.registry.health_monitor().snapshot();
        let peers = self.registry.peer_values_snapshot().map_err(to_capnp)?;
        let mut candidates = Vec::with_capacity(peers.len());
        for (node_id, value) in peers {
            if !value.scheduling.schedulable || !value.readiness.is_ready() {
                continue;
            }
            if matches!(health_snapshot.get(&node_id), Some(HealthStatus::Down)) {
                continue;
            }
            candidates.push(PlacementNode::new(
                node_id,
                value.hostname,
                value.address,
                value.platform_os,
                value.platform_arch,
                value.labels.labels,
            ));
        }
        Ok(candidates)
    }

    /// Returns task-host endpoint sources that currently publish traffic for one template.
    fn task_node_endpoint_sources(
        &self,
        service: &ServiceSpecValue,
        template_name: &str,
        network_ids: &[Uuid],
        task_ids: &HashSet<Uuid>,
        active_node_ids: &HashSet<Uuid>,
    ) -> Result<Vec<(Uuid, Uuid)>, Error> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut sources = HashSet::new();
        for network_id in network_ids {
            let attachments = self
                .networks
                .list_attachments(Some(*network_id))
                .map_err(to_capnp)?;
            for attachment in attachments {
                if !task_ids.contains(&attachment.task_id)
                    || attachment.state != NetworkAttachmentState::Ready
                    || !attachment.traffic_published
                    || !active_node_ids.contains(&attachment.node_id)
                {
                    continue;
                }
                if attachment.service_name.as_deref() != Some(service.service_name.as_str())
                    || attachment.template_name.as_deref() != Some(template_name)
                {
                    continue;
                }
                sources.insert((*network_id, attachment.node_id));
            }
        }
        Ok(sources.into_iter().collect())
    }

    /// Adds expected not-ready rows for node/network combinations selected by ingress intent.
    fn add_expected_endpoint_rows(
        service: &ServiceSpecValue,
        template_name: &str,
        network_ids: &[Uuid],
        public_port: u16,
        protocols: &[NodePortProtocol],
        ingress: PublicEndpointIngressMode,
        node_ids: impl IntoIterator<Item = Uuid>,
        rows: &mut Vec<EndpointRow>,
    ) {
        let mut seen = HashSet::new();
        for node_id in node_ids {
            for network_id in network_ids {
                for protocol in protocols.iter().copied() {
                    let key = EndpointRowKey {
                        service_id: service.id,
                        template_name: template_name.to_string(),
                        network_id: *network_id,
                        node_id,
                        public_port,
                        protocol,
                    };
                    if !seen.insert(key) {
                        continue;
                    }
                    rows.push(EndpointRow {
                        snapshot: PublicEndpointSnapshot {
                            key: PublicEndpointKey {
                                service_id: service.id,
                                template_name: template_name.to_string(),
                                public_port,
                                protocol,
                                node_id,
                            },
                            network_id: *network_id,
                            node_ip: None,
                            ingress: ingress.clone(),
                            ready: false,
                            generation: service.service_epoch,
                            detail: Some(
                                "endpoint has not been reported by the source node".to_string(),
                            ),
                        },
                        service_name: Some(service.service_name.clone()),
                    });
                }
            }
        }
    }

    /// Derives expected endpoint rows matching the provided filter from replicated intent.
    fn expected_endpoint_rows(
        &self,
        services: &[ServiceSpecValue],
        filter: &EndpointFilter,
    ) -> Result<Vec<EndpointRow>, Error> {
        let active_sources = self.active_peer_sources()?;
        let active_node_ids = active_sources.keys().copied().collect::<HashSet<_>>();
        let ingress_candidates = self.ingress_pool_candidates()?;
        let mut rows = Vec::new();

        for service in services {
            if !service_reserves_public_endpoint(service.status())
                || !filter.matches_service(service)
            {
                continue;
            }
            let task_ids_by_template = service_template_task_ids(service);
            for template in &service.task_templates {
                let Some(public_port) = template.public_port() else {
                    continue;
                };
                let network_ids = template.required_network_ids();
                if network_ids.is_empty() {
                    continue;
                }
                if !filter.matches_template(&template.name)
                    || !filter.matches_port(public_port)
                    || !filter.matches_public_ingress(&template.public_ingress)
                {
                    continue;
                }
                let protocols = template
                    .public_protocols()
                    .map(nodeport_protocol)
                    .collect::<Vec<_>>();

                match &template.public_ingress {
                    PublicIngressPolicy::AllNodes => {
                        Self::add_expected_endpoint_rows(
                            service,
                            &template.name,
                            &network_ids,
                            public_port,
                            &protocols,
                            PublicEndpointIngressMode::AllNodes,
                            active_node_ids.iter().copied(),
                            &mut rows,
                        );
                    }
                    PublicIngressPolicy::TaskNodes => {
                        let task_ids = task_ids_by_template
                            .get(&template.name)
                            .cloned()
                            .unwrap_or_default();
                        let sources = self.task_node_endpoint_sources(
                            service,
                            &template.name,
                            &network_ids,
                            &task_ids,
                            &active_node_ids,
                        )?;
                        for (network_id, node_id) in sources {
                            Self::add_expected_endpoint_rows(
                                service,
                                &template.name,
                                &[network_id],
                                public_port,
                                &protocols,
                                PublicEndpointIngressMode::TaskNodes,
                                [node_id],
                                &mut rows,
                            );
                        }
                    }
                    PublicIngressPolicy::IngressPool { pool } => {
                        let pool_name = pool.trim();
                        let Some(pool_spec) =
                            self.pools.get_by_name(pool_name).map_err(to_capnp)?
                        else {
                            continue;
                        };
                        let selection = self.pools.select_nodes(&pool_spec, &ingress_candidates);
                        if selection.is_ready() {
                            Self::add_expected_endpoint_rows(
                                service,
                                &template.name,
                                &network_ids,
                                public_port,
                                &protocols,
                                PublicEndpointIngressMode::IngressPool {
                                    pool: pool_name.to_string(),
                                },
                                selection
                                    .selected_nodes
                                    .iter()
                                    .map(|node| node.node_id)
                                    .filter(|node_id| active_node_ids.contains(node_id)),
                                &mut rows,
                            );
                        }
                    }
                }
            }
        }

        Ok(rows)
    }

    /// Reads public endpoint rows from one remote node-info capability.
    async fn remote_public_endpoint_snapshots(
        &self,
        source: EndpointSourcePeer,
    ) -> Result<Vec<PublicEndpointSnapshot>, String> {
        let session = self
            .registry
            .session_for_peer(source.id)
            .await
            .ok_or_else(|| format!("no session for endpoint source {}", source.name))?;
        read_remote_public_endpoint_snapshots(session)
            .await
            .map_err(|error| format!("{}: {error}", source.name))
    }

    /// Reads matching endpoint rows from all selected source nodes.
    async fn collect_endpoint_rows(
        self: &Rc<Self>,
        services: &[ServiceSpecValue],
        filter: &EndpointFilter,
    ) -> Result<Vec<EndpointRow>, Error> {
        let services_by_id = services
            .iter()
            .map(|service| (service.id, service.service_name.clone()))
            .collect::<HashMap<_, _>>();
        let expected_rows = self.expected_endpoint_rows(services, filter)?;
        let source_node_ids = expected_rows
            .iter()
            .map(|row| row.snapshot.key.node_id)
            .collect::<HashSet<_>>();
        let active_sources = self.active_peer_sources()?;
        let local_node_id = self.topology.self_id();
        let mut rows = Vec::new();

        if source_node_ids.contains(&local_node_id) {
            for snapshot in self.network_controller.public_endpoint_snapshots().await {
                let service_name = services_by_id
                    .get(&snapshot.key.service_id)
                    .map(String::as_str);
                if endpoint_matches_filter(&snapshot, service_name, filter)? {
                    rows.push(EndpointRow {
                        snapshot,
                        service_name: service_name.map(str::to_string),
                    });
                }
            }
        }

        let remote_sources = source_node_ids
            .into_iter()
            .filter(|node_id| *node_id != local_node_id)
            .filter_map(|node_id| active_sources.get(&node_id).cloned())
            .collect::<Vec<_>>();
        let remote_results = stream::iter(remote_sources)
            .map(|source| {
                let this = Rc::clone(self);
                async move { this.remote_public_endpoint_snapshots(source).await }
            })
            .buffer_unordered(ENDPOINT_PEER_FANOUT)
            .collect::<Vec<_>>()
            .await;

        for result in remote_results {
            match result {
                Ok(snapshots) => {
                    for snapshot in snapshots {
                        let service_name = services_by_id
                            .get(&snapshot.key.service_id)
                            .map(String::as_str);
                        if endpoint_matches_filter(&snapshot, service_name, filter)? {
                            rows.push(EndpointRow {
                                snapshot,
                                service_name: service_name.map(str::to_string),
                            });
                        }
                    }
                }
                Err(error) => {
                    debug!(target: "ingress", "failed to read remote ingress endpoints: {error}");
                }
            }
        }

        let mut covered_keys = rows
            .iter()
            .map(|row| endpoint_row_key(&row.snapshot))
            .collect::<HashSet<_>>();
        for row in expected_rows {
            if !covered_keys.insert(endpoint_row_key(&row.snapshot)) {
                continue;
            }
            if endpoint_matches_filter(&row.snapshot, row.service_name.as_deref(), filter)? {
                rows.push(row);
            }
        }

        rows.sort_by(compare_endpoint_rows);
        Ok(rows)
    }
}

/// Reads public endpoint rows from one established cluster session.
async fn read_remote_public_endpoint_snapshots(
    session: cluster_session::Client,
) -> Result<Vec<PublicEndpointSnapshot>, Error> {
    let node = session.get_node_request().send().pipeline.get_node();
    let response = node.info_request().send().promise.await?;
    let info = response.get()?.get_info()?;
    let endpoints = info.get_public_endpoints()?;
    let mut snapshots = Vec::with_capacity(endpoints.len() as usize);
    for endpoint in endpoints.iter() {
        snapshots.push(read_public_endpoint_info(endpoint)?);
    }
    Ok(snapshots)
}

impl ingress::Server for IngressRpc {
    /// Creates or replaces one replicated ingress-pool spec.
    async fn apply(
        self: Rc<Self>,
        params: ingress::ApplyParams,
        mut results: ingress::ApplyResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("apply ingress pool")?;
        let spec = self.read_pool_apply_spec(params.get()?.get_spec()?)?;
        self.pools.upsert(spec.clone()).await.map_err(to_capnp)?;
        write_ingress_pool_spec(results.get().init_pool(), &spec)?;
        Ok(())
    }

    /// Deletes one ingress pool by exact UUID or exact name.
    async fn delete(
        self: Rc<Self>,
        params: ingress::DeleteParams,
        _results: ingress::DeleteResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("delete ingress pool")?;
        let selector =
            Self::read_non_empty_text(params.get()?.get_name()?, "ingress pool selector")?;
        let pool = self
            .get_pool_by_selector(&selector)?
            .ok_or_else(|| Error::failed(format!("ingress pool '{selector}' not found")))?;
        self.pools.remove(pool.id).await.map_err(to_capnp)?;
        Ok(())
    }

    /// Lists replicated ingress pools visible to this node.
    async fn list(
        self: Rc<Self>,
        _params: ingress::ListParams,
        mut results: ingress::ListResults,
    ) -> Result<(), Error> {
        let pools = self.pools.list().map_err(to_capnp)?;
        let mut builder = results.get().init_pools(pools.len() as u32);
        for (idx, pool) in pools.iter().enumerate() {
            write_ingress_pool_spec(builder.reborrow().get(idx as u32), pool)?;
        }
        Ok(())
    }

    /// Fetches one ingress pool by exact UUID or exact name.
    async fn inspect(
        self: Rc<Self>,
        params: ingress::InspectParams,
        mut results: ingress::InspectResults,
    ) -> Result<(), Error> {
        let selector =
            Self::read_non_empty_text(params.get()?.get_name()?, "ingress pool selector")?;
        let pool = self
            .get_pool_by_selector(&selector)?
            .ok_or_else(|| Error::failed(format!("ingress pool '{selector}' not found")))?;
        write_ingress_pool_spec(results.get().init_pool(), &pool)?;
        Ok(())
    }

    /// Lists cluster endpoint rows using source nodes derived from replicated intent.
    async fn endpoints(
        self: Rc<Self>,
        params: ingress::EndpointsParams,
        mut results: ingress::EndpointsResults,
    ) -> Result<(), Error> {
        let params_reader = params.get()?;
        let filter = EndpointFilter::from_reader(params_reader.get_filter()?)?;
        let services = self.services.list().map_err(to_capnp)?;
        let matched = self.collect_endpoint_rows(&services, &filter).await?;

        let mut builder = results.get().init_endpoints(matched.len() as u32);
        for (idx, row) in matched.iter().enumerate() {
            write_endpoint(
                builder.reborrow().get(idx as u32),
                &row.snapshot,
                row.service_name.as_deref(),
            );
        }
        Ok(())
    }
}
