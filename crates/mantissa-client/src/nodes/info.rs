use crate::{config::ClientConfig, connection};
use anyhow::Result;

/// Full local node information returned by the node info RPC.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeInfoView {
    pub hostname: String,
    pub os: OsInfoView,
    pub cpu: CpuInfoView,
    pub load: LoadAverageView,
    pub memory: MemoryInfoView,
    pub disk: DiskInfoView,
    pub gpu: GpuInfoView,
    pub nodeport: NodePortInfoView,
    pub load_balancer: LoadBalancerInfoView,
    pub public_endpoints: Vec<PublicEndpointInfoView>,
}

/// Operating-system details reported by the local node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OsInfoView {
    pub name: String,
    pub version: String,
    pub kernel_version: String,
}

/// CPU inventory reported by the local node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpuInfoView {
    pub vendor: String,
    pub brand: String,
    pub codename: String,
    pub frequency_mhz: u64,
    pub cores: u32,
    pub logical_cpus: u32,
    pub total_logical_cpus: u32,
    pub l1_data_cache: u64,
    pub l1_instruction_cache: u64,
    pub l2_cache: u64,
    pub l3_cache: u64,
}

/// Load averages reported by the local node.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadAverageView {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

/// Memory inventory reported by the local node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryInfoView {
    pub total: u64,
    pub free: u64,
    pub available: u64,
    pub buffers: u64,
    pub cached: u64,
    pub swap_total: u64,
    pub swap_free: u64,
}

/// Disk inventory reported by the local node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiskInfoView {
    pub total: u64,
    pub free: u64,
}

/// GPU inventory reported by the local node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GpuInfoView {
    pub vendor: Option<String>,
    pub devices: Vec<GpuDeviceView>,
}

/// One GPU device reported by the local node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GpuDeviceView {
    pub index: u32,
    pub name: Option<String>,
    pub uuid: Option<String>,
    pub pci_bus_id: Option<String>,
    pub compute_capability: Option<String>,
    pub memory_total_bytes: u64,
    pub memory_free_bytes: u64,
}

/// Packet counters returned by networking diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PacketCountersView {
    pub packets: u64,
    pub bytes: u64,
    pub drops: u64,
}

/// NodePort flow diagnostics returned by the local dataplane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodePortFlowDiagnosticsView {
    pub flow_pairs: u64,
    pub ipv4_flow_pairs: u64,
    pub ipv6_flow_pairs: u64,
    pub flow_creates: u64,
    pub flow_clears: u64,
    pub estimated_flow_evictions: u64,
    pub reverse_misses: u64,
    pub invalid_conntrack_transitions: u64,
    pub return_path_bypass_packets: u64,
}

/// NodePort ingress drop counters returned by the local dataplane.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodePortIngressDropReasonsView {
    pub invalid_ipv4_headers: u64,
    pub invalid_l4_headers: u64,
    pub missing_host_entries: u64,
    pub nat_insert_failures: u64,
    pub rewrite_failures: u64,
    pub fragmented_ipv4_packets: u64,
}

/// NodePort diagnostics reported by the local node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodePortInfoView {
    pub desired_enabled: bool,
    pub state: Option<String>,
    pub source_mode: Option<String>,
    pub identity_source: Option<String>,
    pub resolved_iface: Option<String>,
    pub resolved_node_ip: Option<String>,
    pub active_networks: u64,
    pub active_ports: u64,
    pub active_host_networks: u64,
    pub vip_capacity: u64,
    pub host_capacity: u64,
    pub flow_capacity: u64,
    pub ingress: PacketCountersView,
    pub ingress_drop_reasons: NodePortIngressDropReasonsView,
    pub egress: PacketCountersView,
    pub flow_diagnostics: NodePortFlowDiagnosticsView,
    pub last_error: Option<String>,
    pub stats_error: Option<String>,
}

/// Load-balancer flow diagnostics returned by the local dataplane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadBalancerFlowDiagnosticsView {
    pub flow_pairs: u64,
    pub ipv4_flow_pairs: u64,
    pub ipv6_flow_pairs: u64,
}

/// Load-balancer diagnostics reported by the local node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadBalancerInfoView {
    pub desired_enabled: bool,
    pub programmed_networks: u64,
    pub active_vips: u64,
    pub ipv4_vips: u64,
    pub ipv6_vips: u64,
    pub flow_capacity: u64,
    pub flow_diagnostics: LoadBalancerFlowDiagnosticsView,
    pub stats_error: Option<String>,
}

/// One node-local public endpoint row reported by service discovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicEndpointInfoView {
    pub service_id: String,
    pub template_name: String,
    pub network_id: String,
    pub node_id: String,
    pub node_ip: Option<String>,
    pub public_port: u16,
    pub protocol: String,
    pub ingress_mode: String,
    pub ingress_pool: Option<String>,
    pub ready: bool,
    pub generation: u64,
    pub detail: Option<String>,
}

/// Fetches local node information from the node RPC capability.
pub async fn info(cfg: &ClientConfig) -> Result<NodeInfoView> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_node_request();
    let node = request.send().pipeline.get_node();
    let request = node.info_request();

    let response = request.send().promise.await?;
    let info = response.get()?.get_info()?;

    let os = info.get_os()?;
    let cpu = info.get_cpu()?;
    let load = info.get_load()?;
    let memory = info.get_memory()?;
    let disk = info.get_disk()?;
    let gpu = info.get_gpu()?;
    let nodeport = info.get_nodeport()?;
    let load_balancer = info.get_load_balancer()?;
    let public_endpoints = info.get_public_endpoints()?;

    Ok(NodeInfoView {
        hostname: info.get_hostname()?.to_str()?.to_string(),
        os: OsInfoView {
            name: os.get_name()?.to_str()?.to_string(),
            version: os.get_version()?.to_str()?.to_string(),
            kernel_version: os.get_kernel_version()?.to_str()?.to_string(),
        },
        cpu: CpuInfoView {
            vendor: cpu.get_vendor()?.to_str()?.to_string(),
            brand: cpu.get_brand()?.to_str()?.to_string(),
            codename: cpu.get_codename()?.to_str()?.to_string(),
            frequency_mhz: cpu.get_frequency(),
            cores: non_negative_u32(cpu.get_num_cores()),
            logical_cpus: non_negative_u32(cpu.get_logical_cpus()),
            total_logical_cpus: non_negative_u32(cpu.get_total_logical_cpus()),
            l1_data_cache: non_negative_u64(cpu.get_l1_data_cache()),
            l1_instruction_cache: non_negative_u64(cpu.get_l1_instruction_cache()),
            l2_cache: non_negative_u64(cpu.get_l2_cache()),
            l3_cache: non_negative_u64(cpu.get_l3_cache()),
        },
        load: LoadAverageView {
            one: load.get_one(),
            five: load.get_five(),
            fifteen: load.get_fifteen(),
        },
        memory: MemoryInfoView {
            total: memory.get_total(),
            free: memory.get_free(),
            available: memory.get_avail(),
            buffers: memory.get_buffers(),
            cached: memory.get_cached(),
            swap_total: memory.get_swap_total(),
            swap_free: memory.get_swap_free(),
        },
        disk: DiskInfoView {
            total: disk.get_total(),
            free: disk.get_free(),
        },
        gpu: decode_gpu(gpu)?,
        nodeport: decode_nodeport(nodeport)?,
        load_balancer: decode_load_balancer(load_balancer)?,
        public_endpoints: decode_public_endpoints(public_endpoints)?,
    })
}

/// Decodes the GPU inventory section into owned client data.
fn decode_gpu(gpu: mantissa_protocol::info_capnp::gpu_info::Reader<'_>) -> Result<GpuInfoView> {
    let vendor = optional_text(gpu.get_vendor()?);
    let devices = gpu.get_devices()?;
    let mut decoded = Vec::with_capacity(devices.len() as usize);
    for device in devices.iter() {
        decoded.push(GpuDeviceView {
            index: device.get_index(),
            name: optional_text(device.get_name()?),
            uuid: optional_text(device.get_uuid()?),
            pci_bus_id: optional_text(device.get_pci_bus_id()?),
            compute_capability: optional_text(device.get_compute_capability()?),
            memory_total_bytes: device.get_memory_total_bytes(),
            memory_free_bytes: device.get_memory_free_bytes(),
        });
    }

    Ok(GpuInfoView {
        vendor,
        devices: decoded,
    })
}

/// Decodes the NodePort diagnostics section into owned client data.
fn decode_nodeport(
    nodeport: mantissa_protocol::info_capnp::node_port_info::Reader<'_>,
) -> Result<NodePortInfoView> {
    let ingress = nodeport.get_ingress()?;
    let egress = nodeport.get_egress()?;
    let ingress_drop_reasons = nodeport.get_ingress_drop_reasons()?;
    let flow_diagnostics = nodeport.get_flow_diagnostics()?;

    Ok(NodePortInfoView {
        desired_enabled: nodeport.get_desired_enabled(),
        state: optional_text(nodeport.get_state()?),
        source_mode: optional_text(nodeport.get_source_mode()?),
        identity_source: optional_text(nodeport.get_identity_source()?),
        resolved_iface: optional_text(nodeport.get_resolved_iface()?),
        resolved_node_ip: optional_text(nodeport.get_resolved_node_ip()?),
        active_networks: u64::from(nodeport.get_active_networks()),
        active_ports: u64::from(nodeport.get_active_ports()),
        active_host_networks: u64::from(nodeport.get_active_host_networks()),
        vip_capacity: u64::from(nodeport.get_vip_capacity()),
        host_capacity: u64::from(nodeport.get_host_capacity()),
        flow_capacity: u64::from(nodeport.get_flow_capacity()),
        ingress: PacketCountersView {
            packets: ingress.get_packets(),
            bytes: ingress.get_bytes(),
            drops: ingress.get_drops(),
        },
        ingress_drop_reasons: NodePortIngressDropReasonsView {
            invalid_ipv4_headers: ingress_drop_reasons.get_invalid_ipv4_headers(),
            invalid_l4_headers: ingress_drop_reasons.get_invalid_l4_headers(),
            missing_host_entries: ingress_drop_reasons.get_missing_host_entries(),
            nat_insert_failures: ingress_drop_reasons.get_nat_insert_failures(),
            rewrite_failures: ingress_drop_reasons.get_rewrite_failures(),
            fragmented_ipv4_packets: ingress_drop_reasons.get_fragmented_ipv4_packets(),
        },
        egress: PacketCountersView {
            packets: egress.get_packets(),
            bytes: egress.get_bytes(),
            drops: egress.get_drops(),
        },
        flow_diagnostics: NodePortFlowDiagnosticsView {
            flow_pairs: u64::from(
                flow_diagnostics
                    .get_ipv4_flow_pairs()
                    .saturating_add(flow_diagnostics.get_ipv6_flow_pairs()),
            ),
            ipv4_flow_pairs: u64::from(flow_diagnostics.get_ipv4_flow_pairs()),
            ipv6_flow_pairs: u64::from(flow_diagnostics.get_ipv6_flow_pairs()),
            flow_creates: flow_diagnostics.get_flow_creates(),
            flow_clears: flow_diagnostics.get_flow_clears(),
            estimated_flow_evictions: flow_diagnostics.get_estimated_flow_evictions(),
            reverse_misses: flow_diagnostics.get_reverse_misses(),
            invalid_conntrack_transitions: flow_diagnostics.get_invalid_conntrack_transitions(),
            return_path_bypass_packets: flow_diagnostics.get_return_path_bypass_packets(),
        },
        last_error: optional_text(nodeport.get_last_error()?),
        stats_error: optional_text(nodeport.get_stats_error()?),
    })
}

/// Decodes the load-balancer diagnostics section into owned client data.
fn decode_load_balancer(
    load_balancer: mantissa_protocol::info_capnp::load_balancer_info::Reader<'_>,
) -> Result<LoadBalancerInfoView> {
    let flow_diagnostics = load_balancer.get_flow_diagnostics()?;
    let ipv4_vips = u64::from(load_balancer.get_ipv4_vips());
    let ipv6_vips = u64::from(load_balancer.get_ipv6_vips());

    Ok(LoadBalancerInfoView {
        desired_enabled: load_balancer.get_desired_enabled(),
        programmed_networks: u64::from(load_balancer.get_programmed_networks()),
        active_vips: ipv4_vips.saturating_add(ipv6_vips),
        ipv4_vips,
        ipv6_vips,
        flow_capacity: u64::from(load_balancer.get_flow_capacity()),
        flow_diagnostics: LoadBalancerFlowDiagnosticsView {
            flow_pairs: u64::from(
                flow_diagnostics
                    .get_ipv4_flow_pairs()
                    .saturating_add(flow_diagnostics.get_ipv6_flow_pairs()),
            ),
            ipv4_flow_pairs: u64::from(flow_diagnostics.get_ipv4_flow_pairs()),
            ipv6_flow_pairs: u64::from(flow_diagnostics.get_ipv6_flow_pairs()),
        },
        stats_error: optional_text(load_balancer.get_stats_error()?),
    })
}

/// Decodes node-local public endpoint rows into owned client data.
fn decode_public_endpoints(
    endpoints: capnp::struct_list::Reader<
        mantissa_protocol::info_capnp::public_endpoint_info::Owned,
    >,
) -> Result<Vec<PublicEndpointInfoView>> {
    let mut decoded = Vec::with_capacity(endpoints.len() as usize);
    for endpoint in endpoints.iter() {
        decoded.push(PublicEndpointInfoView {
            service_id: endpoint.get_service_id()?.to_str()?.to_string(),
            template_name: endpoint.get_template_name()?.to_str()?.to_string(),
            network_id: endpoint.get_network_id()?.to_str()?.to_string(),
            node_id: endpoint.get_node_id()?.to_str()?.to_string(),
            node_ip: optional_text(endpoint.get_node_ip()?),
            public_port: endpoint.get_public_port(),
            protocol: endpoint.get_protocol()?.to_str()?.to_string(),
            ingress_mode: endpoint.get_ingress_mode()?.to_str()?.to_string(),
            ingress_pool: optional_text(endpoint.get_ingress_pool()?),
            ready: endpoint.get_ready(),
            generation: endpoint.get_generation(),
            detail: optional_text(endpoint.get_detail()?),
        });
    }
    Ok(decoded)
}

/// Converts one protocol text field into an optional owned string after trimming.
fn optional_text(text: capnp::text::Reader<'_>) -> Option<String> {
    let trimmed = text.to_str().ok()?.trim().to_string();
    (!trimmed.is_empty()).then_some(trimmed)
}

/// Converts signed schema counters into a non-negative public unsigned value.
fn non_negative_u32(value: i32) -> u32 {
    value.max(0) as u32
}

/// Converts signed schema counters into a non-negative public unsigned value.
fn non_negative_u64(value: i32) -> u64 {
    value.max(0) as u64
}
