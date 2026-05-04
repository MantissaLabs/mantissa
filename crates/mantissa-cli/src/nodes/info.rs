use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::nodes::{
    LoadBalancerFlowDiagnosticsView, NodeInfoView, NodePortFlowDiagnosticsView,
    NodePortIngressDropReasonsView,
};

/// Fetches and prints local node information.
pub async fn info(cfg: &ClientConfig) -> Result<()> {
    let info = mantissa_client::nodes::info(cfg).await?;
    render_info(&info);
    Ok(())
}

/// Renders the local node information using the existing plain-text layout.
fn render_info(info: &NodeInfoView) {
    println!("Hostname: {:?}", info.hostname);

    println!("Operating System:");
    println!("  name: {:?}", info.os.name);
    println!("  version: {:?}", info.os.version);
    println!("  kernel_version: {:?}", info.os.kernel_version);

    println!("CPU:");
    println!("  vendor: {:?}", info.cpu.vendor);
    println!("  brand: {:?}", info.cpu.brand);
    println!("  codename: {:?}", info.cpu.codename);
    println!("  frequency (MHz): {}", info.cpu.frequency_mhz);
    println!("  cores: {}", info.cpu.cores);
    println!("  logical cpus: {}", info.cpu.logical_cpus);
    println!("  total logical cpus: {}", info.cpu.total_logical_cpus);
    println!("  L1 data cache: {}", info.cpu.l1_data_cache);
    println!("  L1 instruction cache: {}", info.cpu.l1_instruction_cache);
    println!("  L2 cache: {}", info.cpu.l2_cache);
    println!("  L3 cache: {}", info.cpu.l3_cache);

    println!("Load Average:");
    println!(
        "  {} / {} / {}",
        info.load.one, info.load.five, info.load.fifteen,
    );

    println!("Memory (Kb):");
    println!("  total: {}", info.memory.total);
    println!("  free: {}", info.memory.free);
    println!("  available: {}", info.memory.available);
    println!("  buffers: {}", info.memory.buffers);
    println!("  cached: {}", info.memory.cached);
    println!("  swap total: {}", info.memory.swap_total);
    println!("  swap free: {}", info.memory.swap_free);

    println!("Disk (Kb):");
    println!("  total: {}", info.disk.total);
    println!("  free: {}", info.disk.free);

    println!("GPU:");
    if info.gpu.devices.is_empty() {
        println!("  no GPU device detected");
    } else {
        if let Some(vendor) = &info.gpu.vendor {
            println!("  vendor: {vendor}");
        }
        for device in &info.gpu.devices {
            println!("  - index: {}", device.index);
            if let Some(name) = &device.name {
                println!("    name: {name}");
            }
            if let Some(uuid) = &device.uuid {
                println!("    uuid: {uuid}");
            }
            if let Some(pci_bus_id) = &device.pci_bus_id {
                println!("    pci_bus_id: {pci_bus_id}");
            }
            if let Some(cc) = &device.compute_capability {
                println!("    compute_capability: {cc}");
            }
            println!("    memory_total_bytes: {}", device.memory_total_bytes);
            println!("    memory_free_bytes: {}", device.memory_free_bytes);
        }
    }

    println!("NodePort:");
    println!("  desired_enabled: {}", info.nodeport.desired_enabled);
    print_optional("state", info.nodeport.state.as_deref());
    print_optional("source_mode", info.nodeport.source_mode.as_deref());
    print_optional("identity_source", info.nodeport.identity_source.as_deref());
    print_optional("resolved_iface", info.nodeport.resolved_iface.as_deref());
    print_optional(
        "resolved_node_ip",
        info.nodeport.resolved_node_ip.as_deref(),
    );
    println!("  active_networks: {}", info.nodeport.active_networks);
    println!("  active_ports: {}", info.nodeport.active_ports);
    println!(
        "  active_host_networks: {}",
        info.nodeport.active_host_networks
    );
    println!("  vip_capacity: {}", info.nodeport.vip_capacity);
    println!("  host_capacity: {}", info.nodeport.host_capacity);
    println!("  flow_capacity: {}", info.nodeport.flow_capacity);
    println!(
        "  ingress: packets={} bytes={} drops={}",
        info.nodeport.ingress.packets, info.nodeport.ingress.bytes, info.nodeport.ingress.drops,
    );
    let ingress_drop_reason_fields =
        nodeport_drop_reason_fields(&info.nodeport.ingress_drop_reasons);
    if !ingress_drop_reason_fields.is_empty() {
        println!(
            "  ingress_drop_reasons: {}",
            ingress_drop_reason_fields.join(" ")
        );
    }
    println!(
        "  egress: packets={} bytes={} drops={}",
        info.nodeport.egress.packets, info.nodeport.egress.bytes, info.nodeport.egress.drops,
    );
    println!(
        "  flow_diagnostics: {}",
        nodeport_flow_diagnostics_fields(&info.nodeport.flow_diagnostics).join(" ")
    );
    print_optional("last_error", info.nodeport.last_error.as_deref());
    print_optional("stats_error", info.nodeport.stats_error.as_deref());

    println!("Load Balancer:");
    println!("  desired_enabled: {}", info.load_balancer.desired_enabled);
    println!(
        "  programmed_networks: {}",
        info.load_balancer.programmed_networks
    );
    println!("  active_vips: {}", info.load_balancer.active_vips);
    println!("  ipv4_vips: {}", info.load_balancer.ipv4_vips);
    println!("  ipv6_vips: {}", info.load_balancer.ipv6_vips);
    println!("  flow_capacity: {}", info.load_balancer.flow_capacity);
    println!(
        "  flow_diagnostics: {}",
        load_balancer_flow_diagnostics_fields(&info.load_balancer.flow_diagnostics).join(" ")
    );
    print_optional("stats_error", info.load_balancer.stats_error.as_deref());
}

/// Prints one optional indented field when present.
fn print_optional(label: &str, value: Option<&str>) {
    if let Some(value) = value {
        println!("  {label}: {value}");
    }
}

/// Renders non-zero NodePort ingress drop reasons for operator diagnostics.
fn nodeport_drop_reason_fields(reasons: &NodePortIngressDropReasonsView) -> Vec<String> {
    let mut fields = Vec::new();
    if reasons.invalid_ipv4_headers > 0 {
        fields.push(format!(
            "invalid_ipv4_headers={}",
            reasons.invalid_ipv4_headers
        ));
    }
    if reasons.invalid_l4_headers > 0 {
        fields.push(format!("invalid_l4_headers={}", reasons.invalid_l4_headers));
    }
    if reasons.missing_host_entries > 0 {
        fields.push(format!(
            "missing_host_entries={}",
            reasons.missing_host_entries
        ));
    }
    if reasons.nat_insert_failures > 0 {
        fields.push(format!(
            "nat_insert_failures={}",
            reasons.nat_insert_failures
        ));
    }
    if reasons.rewrite_failures > 0 {
        fields.push(format!("rewrite_failures={}", reasons.rewrite_failures));
    }
    if reasons.fragmented_ipv4_packets > 0 {
        fields.push(format!(
            "fragmented_ipv4_packets={}",
            reasons.fragmented_ipv4_packets
        ));
    }
    fields
}

/// Renders NodePort flow diagnostics in one compact operator-facing line.
fn nodeport_flow_diagnostics_fields(diagnostics: &NodePortFlowDiagnosticsView) -> Vec<String> {
    vec![
        format!("flow_pairs={}", diagnostics.flow_pairs),
        format!("ipv4_flow_pairs={}", diagnostics.ipv4_flow_pairs),
        format!("ipv6_flow_pairs={}", diagnostics.ipv6_flow_pairs),
        format!("flow_creates={}", diagnostics.flow_creates),
        format!("flow_clears={}", diagnostics.flow_clears),
        format!(
            "estimated_flow_evictions={}",
            diagnostics.estimated_flow_evictions
        ),
        format!("reverse_misses={}", diagnostics.reverse_misses),
        format!(
            "invalid_conntrack_transitions={}",
            diagnostics.invalid_conntrack_transitions
        ),
        format!(
            "return_path_bypass_packets={}",
            diagnostics.return_path_bypass_packets
        ),
    ]
}

/// Renders load-balancer flow diagnostics in one compact operator-facing line.
fn load_balancer_flow_diagnostics_fields(
    diagnostics: &LoadBalancerFlowDiagnosticsView,
) -> Vec<String> {
    vec![
        format!("flow_pairs={}", diagnostics.flow_pairs),
        format!("ipv4_flow_pairs={}", diagnostics.ipv4_flow_pairs),
        format!("ipv6_flow_pairs={}", diagnostics.ipv6_flow_pairs),
    ]
}
