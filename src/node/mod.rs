use crate::info_capnp::info as SystemInfo;
use crate::network::controller::NetworkController;
use crate::network::discovery::{PublicEndpointIngressMode, PublicEndpointSnapshot};
use crate::network::lb::BpfLoadBalancer;
use crate::network::nodeport::{NodePortManager, NodePortProtocol};
use crate::node::id::new_node_id_v7;
use crate::node::info::NodeInfo;
use capnp::Error;
use capnp::message::Builder;
use mantissa_protocol::node;
use std::cell::RefCell;
use std::rc::Rc;
use tracing::info;

pub mod address;
pub mod id;
pub mod identity;
pub mod info;

pub type NodeId = uuid::Uuid;

/// This structure defines the delegate in charge of booking slots
/// running tasks on the machine.
#[derive(Clone)]
pub struct Node {
    pub id: NodeId,
    pub system_info: NodeInfo,
    nodeport: Rc<RefCell<Option<NodePortManager>>>,
    network_controller: Rc<RefCell<Option<NetworkController>>>,
    // engine: Rc<Engine>,
}

impl std::fmt::Debug for Node {
    /// Render the node without expanding shared runtime handles that are only used for diagnostics.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("id", &self.id)
            .field("system_info", &self.system_info)
            .finish()
    }
}

impl Default for Node {
    fn default() -> Self {
        Node {
            id: new_node_id_v7(),
            system_info: NodeInfo::new(),
            nodeport: Rc::new(RefCell::new(None)),
            network_controller: Rc::new(RefCell::new(None)),
        }
    }
}

impl Node {
    /// Creates a new node instance.
    pub fn new() -> Node {
        Default::default()
    }

    /// Collects system wide informations.
    pub fn collect_system_info(&mut self) -> &mut Node {
        self.system_info.collect();
        self
    }

    /// Store the live NodePort manager so node-local diagnostics can report public ingress state.
    pub fn set_nodeport_manager(&self, nodeport: NodePortManager) {
        *self.nodeport.borrow_mut() = Some(nodeport);
    }

    /// Store the network controller so node info can expose local public endpoint rows.
    pub fn set_network_controller(&self, controller: NetworkController) {
        *self.network_controller.borrow_mut() = Some(controller);
    }

    /// Returns the live network controller handle when runtime bootstrap has wired it.
    pub fn network_controller(&self) -> Option<NetworkController> {
        self.network_controller.borrow().clone()
    }
}

impl node::Server for Node {
    /// Returns the information on a host.
    ///
    /// This method returns general informations such as load average,
    /// CPU specs, amount of Memory, Disk capacity, etc. to print on cli.
    async fn info(
        self: std::rc::Rc<Self>,
        _params: node::InfoParams,
        mut results: node::InfoResults,
    ) -> Result<(), Error> {
        info!(target: "node", "Collecting system information...");

        let mut builder = Builder::new_default();

        let info = self.system_info.info.clone();
        let nodeport = self.nodeport.borrow().clone();
        let nodeport_status = match nodeport {
            Some(manager) => Some(manager.status().await),
            None => None,
        };
        let network_controller = self.network_controller.borrow().clone();
        let public_endpoints = match network_controller {
            Some(controller) => controller.public_endpoint_snapshots().await,
            None => Vec::new(),
        };
        let load_balancer_status = BpfLoadBalancer::new().status();

        {
            let builder = &mut builder;
            let mut system = builder.init_root::<SystemInfo::Builder>();

            if let Some(hostname) = info.hostname {
                system.set_hostname(hostname);
            }

            // Operating system
            {
                let mut os = system.reborrow().init_os();

                if let Some(os_info) = info.os_info {
                    os.set_name(&os_info.os_name);
                    os.set_version(&os_info.os_version);
                    os.set_kernel_version(&os_info.kernel_version)
                }
            }

            // Load average
            {
                let mut load = system.reborrow().init_load();

                if let Some(load_info) = info.load_info {
                    load.set_one(load_info.one);
                    load.set_five(load_info.five);
                    load.set_fifteen(load_info.fifteen);
                }
            }

            // CPU
            {
                let mut cpu = system.reborrow().init_cpu();

                if let Some(cpu_info) = info.cpu_info {
                    cpu.set_vendor(cpu_info.vendor.unwrap_or(String::from("Unknown")));
                    cpu.set_brand(cpu_info.brand.unwrap_or(String::from("Unknown")));
                    cpu.set_codename(cpu_info.codename.unwrap_or(String::from("Unknown")));
                    cpu.set_frequency(cpu_info.frequency.unwrap_or(0));
                    cpu.set_num_cores(cpu_info.num_cores);
                    cpu.set_logical_cpus(cpu_info.num_logical_cpus);
                    cpu.set_total_logical_cpus(cpu_info.total_logical_cpus.unwrap_or(0));
                    cpu.set_l1_data_cache(cpu_info.l1_data_cache.unwrap_or(0));
                    cpu.set_l1_instruction_cache(cpu_info.l1_instruction_cache.unwrap_or(0));
                    cpu.set_l2_cache(cpu_info.l2_cache.unwrap_or(0));
                    cpu.set_l3_cache(cpu_info.l3_cache.unwrap_or(0));
                }
            }

            // Memory
            {
                let mut mem = system.reborrow().init_memory();

                if let Some(mem_info) = info.mem_info {
                    mem.set_total(mem_info.total);
                    mem.set_free(mem_info.free);
                    mem.set_avail(mem_info.available);
                    mem.set_swap_total(mem_info.swap_total);
                    mem.set_swap_free(mem_info.swap_free);
                }
            }

            // Disk
            {
                let mut disk = system.reborrow().init_disk();

                if let Some(disk_info) = info.disk_info {
                    disk.set_total(disk_info.total);
                    disk.set_free(disk_info.free);
                }
            }

            // GPU
            {
                let mut gpu = system.reborrow().init_gpu();
                if let Some(gpu_info) = info.gpu_info {
                    gpu.set_vendor(&gpu_info.vendor);
                    let mut devices = gpu.reborrow().init_devices(gpu_info.devices.len() as u32);
                    for (idx, device) in gpu_info.devices.iter().enumerate() {
                        let mut entry = devices.reborrow().get(idx as u32);
                        entry.set_index(device.index);
                        entry.set_uuid(device.uuid.as_deref().unwrap_or(""));
                        entry.set_name(&device.name);
                        entry.set_memory_total_bytes(device.memory_total_bytes);
                        entry.set_memory_free_bytes(device.memory_free_bytes);
                        entry.set_compute_capability(
                            device.compute_capability.as_deref().unwrap_or(""),
                        );
                        entry.set_pci_bus_id(device.pci_bus_id.as_deref().unwrap_or(""));
                    }
                } else {
                    gpu.set_vendor("");
                    gpu.reborrow().init_devices(0);
                }
            }

            // NodePort
            {
                let mut nodeport = system.reborrow().init_nodeport();
                if let Some(status) = nodeport_status {
                    nodeport.set_desired_enabled(status.desired_enabled);
                    let state = status.state.to_string();
                    nodeport.set_state(state);
                    let source_mode = status.source_mode.to_string();
                    nodeport.set_source_mode(source_mode);
                    let identity_source = status
                        .identity_source
                        .map(|source| source.to_string())
                        .unwrap_or_default();
                    nodeport.set_identity_source(&identity_source);
                    nodeport.set_resolved_iface(status.resolved_iface.as_deref().unwrap_or(""));
                    let resolved_node_ip = status
                        .resolved_node_ip
                        .map(|ip| ip.to_string())
                        .unwrap_or_default();
                    nodeport.set_resolved_node_ip(&resolved_node_ip);
                    nodeport.set_active_networks(usize_to_u32(status.active_networks));
                    nodeport.set_active_ports(usize_to_u32(status.active_ports));
                    nodeport.set_active_host_networks(usize_to_u32(status.active_host_networks));
                    nodeport.set_vip_capacity(usize_to_u32(status.vip_capacity));
                    nodeport.set_host_capacity(usize_to_u32(status.host_capacity));
                    nodeport.set_flow_capacity(usize_to_u32(status.flow_capacity));
                    nodeport.set_last_error(status.last_error.as_deref().unwrap_or(""));
                    nodeport.set_stats_error(status.stats_error.as_deref().unwrap_or(""));

                    let mut ingress = nodeport.reborrow().init_ingress();
                    if let Some(stats) = status.ingress_stats {
                        ingress.set_packets(stats.packets);
                        ingress.set_bytes(stats.bytes);
                        ingress.set_drops(stats.drops);
                    }
                    let mut ingress_drop_reasons = nodeport.reborrow().init_ingress_drop_reasons();
                    if let Some(reasons) = status.ingress_drop_reasons {
                        ingress_drop_reasons.set_invalid_ipv4_headers(reasons.invalid_ipv4_headers);
                        ingress_drop_reasons.set_invalid_l4_headers(reasons.invalid_l4_headers);
                        ingress_drop_reasons.set_missing_host_entries(reasons.missing_host_entries);
                        ingress_drop_reasons.set_nat_insert_failures(reasons.nat_insert_failures);
                        ingress_drop_reasons.set_rewrite_failures(reasons.rewrite_failures);
                        ingress_drop_reasons
                            .set_fragmented_ipv4_packets(reasons.fragmented_ipv4_packets);
                    }

                    let mut egress = nodeport.reborrow().init_egress();
                    if let Some(stats) = status.egress_stats {
                        egress.set_packets(stats.packets);
                        egress.set_bytes(stats.bytes);
                        egress.set_drops(stats.drops);
                    }

                    let mut flow_diagnostics = nodeport.reborrow().init_flow_diagnostics();
                    if let Some(diagnostics) = status.flow_diagnostics {
                        flow_diagnostics
                            .set_ipv4_flow_pairs(usize_to_u32(diagnostics.ipv4_flow_pairs));
                        flow_diagnostics
                            .set_ipv6_flow_pairs(usize_to_u32(diagnostics.ipv6_flow_pairs));
                        flow_diagnostics.set_flow_creates(diagnostics.flow_creates);
                        flow_diagnostics.set_flow_clears(diagnostics.flow_clears);
                        flow_diagnostics
                            .set_estimated_flow_evictions(diagnostics.estimated_flow_evictions);
                        flow_diagnostics.set_reverse_misses(diagnostics.reverse_misses);
                        flow_diagnostics.set_invalid_conntrack_transitions(
                            diagnostics.invalid_conntrack_transitions,
                        );
                        flow_diagnostics
                            .set_return_path_bypass_packets(diagnostics.return_path_bypass_packets);
                    }
                } else {
                    nodeport.set_state("unavailable");
                    nodeport.set_source_mode("");
                    nodeport.set_identity_source("");
                    nodeport.set_last_error("nodeport manager not wired");
                    nodeport.reborrow().init_ingress();
                    nodeport.reborrow().init_ingress_drop_reasons();
                    nodeport.reborrow().init_egress();
                    nodeport.reborrow().init_flow_diagnostics();
                }
            }

            // Overlay Load Balancer
            {
                let status = load_balancer_status;
                let mut load_balancer = system.reborrow().init_load_balancer();
                load_balancer.set_desired_enabled(status.desired_enabled);
                load_balancer.set_programmed_networks(usize_to_u32(status.programmed_networks));
                load_balancer.set_ipv4_vips(usize_to_u32(status.ipv4_vips));
                load_balancer.set_ipv6_vips(usize_to_u32(status.ipv6_vips));
                load_balancer.set_flow_capacity(usize_to_u32(status.flow_capacity));
                load_balancer.set_stats_error(status.stats_error.as_deref().unwrap_or(""));

                let mut flow_diagnostics = load_balancer.reborrow().init_flow_diagnostics();
                if let Some(diagnostics) = status.flow_diagnostics {
                    flow_diagnostics.set_ipv4_flow_pairs(usize_to_u32(diagnostics.ipv4_flow_pairs));
                    flow_diagnostics.set_ipv6_flow_pairs(usize_to_u32(diagnostics.ipv6_flow_pairs));
                }
            }

            // Public endpoints
            {
                let mut endpoints = system
                    .reborrow()
                    .init_public_endpoints(usize_to_u32(public_endpoints.len()));
                for (idx, snapshot) in public_endpoints.iter().enumerate() {
                    let mut endpoint = endpoints.reborrow().get(usize_to_u32(idx));
                    write_public_endpoint(endpoint.reborrow(), snapshot);
                }
            }
        }

        match builder.get_root::<SystemInfo::Builder>() {
            Ok(system_reader) => {
                results.get().set_info(system_reader.into_reader())?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

/// Encodes one local public endpoint snapshot into node-info diagnostics.
fn write_public_endpoint(
    mut endpoint: mantissa_protocol::info_capnp::public_endpoint_info::Builder<'_>,
    snapshot: &PublicEndpointSnapshot,
) {
    endpoint.set_service_id(&snapshot.key.service_id.to_string());
    endpoint.set_template_name(&snapshot.key.template_name);
    endpoint.set_network_id(&snapshot.network_id.to_string());
    endpoint.set_node_id(&snapshot.key.node_id.to_string());
    let node_ip = snapshot
        .node_ip
        .map(|ip| ip.to_string())
        .unwrap_or_default();
    endpoint.set_node_ip(&node_ip);
    endpoint.set_public_port(snapshot.key.public_port);
    endpoint.set_protocol(nodeport_protocol_label(snapshot.key.protocol));
    let (ingress_mode, ingress_pool) = public_endpoint_ingress_labels(&snapshot.ingress);
    endpoint.set_ingress_mode(ingress_mode);
    endpoint.set_ingress_pool(ingress_pool.unwrap_or(""));
    endpoint.set_ready(snapshot.ready);
    endpoint.set_generation(snapshot.generation);
    endpoint.set_detail(snapshot.detail.as_deref().unwrap_or(""));
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

/// Returns the stable protocol label used in node-info public endpoint rows.
fn nodeport_protocol_label(protocol: NodePortProtocol) -> &'static str {
    match protocol {
        NodePortProtocol::Tcp => "tcp",
        NodePortProtocol::Udp => "udp",
    }
}

/// Convert one local count into the wire format used by node diagnostics without panicking on large values.
fn usize_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}
