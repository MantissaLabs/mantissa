use crate::ingress::codec::{read_spread_key, write_ingress_pool_spec};
use crate::ingress::registry::IngressPoolRegistry;
use crate::ingress::types::{IngressPoolSpecDraft, IngressPoolSpecValue, current_timestamp};
use crate::network::controller::NetworkController;
use crate::network::discovery::{PublicEndpointIngressMode, PublicEndpointSnapshot};
use crate::network::nodeport::NodePortProtocol;
use crate::services::registry::ServiceRegistry;
use crate::topology::Topology;
use crate::workload::capnp_codec::decode_placement_policy;
use capnp::Error;
use mantissa_protocol::ingress::{
    ingress, ingress_endpoint, ingress_endpoint_filter, ingress_pool_apply_spec,
};
use std::collections::HashMap;
use std::rc::Rc;
use uuid::Uuid;

/// Cap'n Proto RPC surface for replicated ingress pools and endpoint diagnostics.
pub struct IngressRpc {
    pools: IngressPoolRegistry,
    services: ServiceRegistry,
    network_controller: NetworkController,
    topology: Topology,
}

impl IngressRpc {
    /// Constructs the ingress RPC service over the existing replicated pool registry.
    pub fn new(
        pools: IngressPoolRegistry,
        services: ServiceRegistry,
        network_controller: NetworkController,
        topology: Topology,
    ) -> Self {
        Self {
            pools,
            services,
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

/// Builds a service-id to service-name map for endpoint rows.
fn service_name_index(registry: &ServiceRegistry) -> Result<HashMap<Uuid, String>, Error> {
    let services = registry.list().map_err(to_capnp)?;
    Ok(services
        .into_iter()
        .map(|service| (service.id, service.service_name))
        .collect())
}

/// Returns whether one local endpoint snapshot matches an endpoint-list filter.
fn endpoint_matches_filter(
    snapshot: &PublicEndpointSnapshot,
    service_name: Option<&str>,
    filter: &ingress_endpoint_filter::Reader<'_>,
) -> Result<bool, Error> {
    let service = filter.get_service()?.to_str()?.trim();
    if !service.is_empty()
        && service != snapshot.key.service_id.to_string()
        && service_name != Some(service)
    {
        return Ok(false);
    }

    let template = filter.get_template()?.to_str()?.trim();
    if !template.is_empty() && template != snapshot.key.template_name {
        return Ok(false);
    }

    let pool = filter.get_pool()?.to_str()?.trim();
    if !pool.is_empty() {
        match &snapshot.ingress {
            PublicEndpointIngressMode::IngressPool {
                pool: snapshot_pool,
            } if snapshot_pool == pool => {}
            _ => return Ok(false),
        }
    }

    let port = filter.get_port();
    if port != 0 && port != snapshot.key.public_port {
        return Ok(false);
    }

    if filter.get_ready_only() && !snapshot.ready {
        return Ok(false);
    }

    Ok(true)
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

    /// Deletes one ingress pool by exact name.
    async fn delete(
        self: Rc<Self>,
        params: ingress::DeleteParams,
        _results: ingress::DeleteResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("delete ingress pool")?;
        let name = Self::read_non_empty_text(params.get()?.get_name()?, "ingress pool name")?;
        let pool = self
            .pools
            .get_by_name(&name)
            .map_err(to_capnp)?
            .ok_or_else(|| Error::failed(format!("ingress pool '{name}' not found")))?;
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

    /// Fetches one ingress pool by exact name.
    async fn inspect(
        self: Rc<Self>,
        params: ingress::InspectParams,
        mut results: ingress::InspectResults,
    ) -> Result<(), Error> {
        let name = Self::read_non_empty_text(params.get()?.get_name()?, "ingress pool name")?;
        let pool = self
            .pools
            .get_by_name(&name)
            .map_err(to_capnp)?
            .ok_or_else(|| Error::failed(format!("ingress pool '{name}' not found")))?;
        write_ingress_pool_spec(results.get().init_pool(), &pool)?;
        Ok(())
    }

    /// Lists node-local public endpoint rows using the stable ingress command surface.
    async fn endpoints(
        self: Rc<Self>,
        params: ingress::EndpointsParams,
        mut results: ingress::EndpointsResults,
    ) -> Result<(), Error> {
        let params_reader = params.get()?;
        let filter = params_reader.get_filter()?;
        let services_by_id = service_name_index(&self.services)?;
        let snapshots = self.network_controller.public_endpoint_snapshots().await;
        let mut matched = Vec::new();
        for snapshot in snapshots {
            let service_name = services_by_id
                .get(&snapshot.key.service_id)
                .map(String::as_str);
            if endpoint_matches_filter(&snapshot, service_name, &filter)? {
                matched.push((snapshot, service_name.map(str::to_string)));
            }
        }

        let mut builder = results.get().init_endpoints(matched.len() as u32);
        for (idx, (snapshot, service_name)) in matched.iter().enumerate() {
            write_endpoint(
                builder.reborrow().get(idx as u32),
                snapshot,
                service_name.as_deref(),
            );
        }
        Ok(())
    }
}
