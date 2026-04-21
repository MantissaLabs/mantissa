use crate::{config::ClientConfig, connection};
use anyhow::Result;

/// Render the non-zero NodePort ingress drop reasons so operators can distinguish malformed traffic from dataplane bugs.
fn nodeport_drop_reason_fields(
    reasons: protocol::info_capnp::node_port_ingress_drop_reasons::Reader<'_>,
) -> Result<Vec<String>> {
    let mut fields = Vec::new();
    let invalid_ipv4_headers = reasons.get_invalid_ipv4_headers();
    if invalid_ipv4_headers > 0 {
        fields.push(format!("invalid_ipv4_headers={invalid_ipv4_headers}"));
    }
    let invalid_l4_headers = reasons.get_invalid_l4_headers();
    if invalid_l4_headers > 0 {
        fields.push(format!("invalid_l4_headers={invalid_l4_headers}"));
    }
    let missing_host_entries = reasons.get_missing_host_entries();
    if missing_host_entries > 0 {
        fields.push(format!("missing_host_entries={missing_host_entries}"));
    }
    let nat_insert_failures = reasons.get_nat_insert_failures();
    if nat_insert_failures > 0 {
        fields.push(format!("nat_insert_failures={nat_insert_failures}"));
    }
    let rewrite_failures = reasons.get_rewrite_failures();
    if rewrite_failures > 0 {
        fields.push(format!("rewrite_failures={rewrite_failures}"));
    }
    let fragmented_ipv4_packets = reasons.get_fragmented_ipv4_packets();
    if fragmented_ipv4_packets > 0 {
        fields.push(format!("fragmented_ipv4_packets={fragmented_ipv4_packets}"));
    }
    Ok(fields)
}

/// Render the NodePort flow diagnostics in one compact line so operators can spot pressure and
/// reverse-path failures from `mantissa info`.
fn nodeport_flow_diagnostics_fields(
    diagnostics: protocol::info_capnp::node_port_flow_diagnostics::Reader<'_>,
) -> Vec<String> {
    let ipv4_pairs = diagnostics.get_ipv4_flow_pairs();
    let ipv6_pairs = diagnostics.get_ipv6_flow_pairs();
    let total_pairs = ipv4_pairs.saturating_add(ipv6_pairs);

    vec![
        format!("flow_pairs={total_pairs}"),
        format!("ipv4_flow_pairs={ipv4_pairs}"),
        format!("ipv6_flow_pairs={ipv6_pairs}"),
        format!("flow_creates={}", diagnostics.get_flow_creates()),
        format!("flow_clears={}", diagnostics.get_flow_clears()),
        format!(
            "estimated_flow_evictions={}",
            diagnostics.get_estimated_flow_evictions()
        ),
        format!("reverse_misses={}", diagnostics.get_reverse_misses()),
        format!(
            "invalid_conntrack_transitions={}",
            diagnostics.get_invalid_conntrack_transitions()
        ),
        format!(
            "return_path_bypass_packets={}",
            diagnostics.get_return_path_bypass_packets()
        ),
    ]
}

pub async fn info(cfg: &ClientConfig) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_node_request();
    let node = request.send().pipeline.get_node();
    let request = node.info_request();

    let response = request.send().promise.await?;

    let info = response.get()?.get_info()?;

    println!("Hostname: {:?}", info.get_hostname()?);

    let os = info.get_os()?;
    println!("Operating System:");
    println!("  name: {:?}", os.get_name()?);
    println!("  version: {:?}", os.get_version()?);
    println!("  kernel_version: {:?}", os.get_kernel_version()?);

    let cpu = info.get_cpu()?;
    println!("CPU:");
    println!("  vendor: {:?}", cpu.get_vendor()?);
    println!("  brand: {:?}", cpu.get_brand()?);
    println!("  codename: {:?}", cpu.get_codename()?);
    println!("  frequency (MHz): {}", cpu.get_frequency());
    println!("  cores: {}", cpu.get_num_cores());
    println!("  logical cpus: {}", cpu.get_logical_cpus());
    println!("  total logical cpus: {}", cpu.get_total_logical_cpus());
    println!("  L1 data cache: {}", cpu.get_l1_data_cache());
    println!("  L1 instruction cache: {}", cpu.get_l1_instruction_cache());
    println!("  L2 cache: {}", cpu.get_l2_cache());
    println!("  L3 cache: {}", cpu.get_l3_cache());

    let load = info.get_load()?;
    println!("Load Average:");
    println!(
        "  {} / {} / {}",
        load.get_one(),
        load.get_five(),
        load.get_fifteen(),
    );

    let mem = info.get_memory()?;
    println!("Memory (Kb):");
    println!("  total: {}", mem.get_total());
    println!("  free: {}", mem.get_free());
    println!("  available: {}", mem.get_avail());
    println!("  buffers: {}", mem.get_buffers());
    println!("  cached: {}", mem.get_cached());
    println!("  swap total: {}", mem.get_swap_total());
    println!("  swap free: {}", mem.get_swap_free());

    let disk = info.get_disk()?;
    println!("Disk (Kb):");
    println!("  total: {}", disk.get_total());
    println!("  free: {}", disk.get_free());

    let gpu = info.get_gpu()?;
    let devices = gpu.get_devices()?;
    if devices.is_empty() {
        println!("GPU:");
        println!("  no GPU device detected");
    } else {
        println!("GPU:");
        let vendor = gpu.get_vendor()?.to_str()?.to_string();
        if !vendor.is_empty() {
            println!("  vendor: {vendor}");
        }
        for device in devices.iter() {
            println!("  - index: {}", device.get_index(),);
            let name = device.get_name()?.to_str()?.to_string();
            if !name.is_empty() {
                println!("    name: {name}");
            }
            let uuid = device.get_uuid()?.to_str()?.to_string();
            if !uuid.is_empty() {
                println!("    uuid: {uuid}");
            }
            let pci_bus_id = device.get_pci_bus_id()?.to_str()?.to_string();
            if !pci_bus_id.is_empty() {
                println!("    pci_bus_id: {pci_bus_id}");
            }
            let cc = device.get_compute_capability()?.to_str()?.to_string();
            if !cc.is_empty() {
                println!("    compute_capability: {cc}");
            }
            println!(
                "    memory_total_bytes: {}",
                device.get_memory_total_bytes()
            );
            println!("    memory_free_bytes: {}", device.get_memory_free_bytes());
        }
    }

    let nodeport = info.get_nodeport()?;
    let nodeport_state = nodeport.get_state()?.to_str()?.to_string();
    let nodeport_source_mode = nodeport.get_source_mode()?.to_str()?.to_string();
    let nodeport_identity_source = nodeport.get_identity_source()?.to_str()?.to_string();
    let resolved_iface = nodeport.get_resolved_iface()?.to_str()?.to_string();
    let resolved_node_ip = nodeport.get_resolved_node_ip()?.to_str()?.to_string();
    let last_error = nodeport.get_last_error()?.to_str()?.to_string();
    let stats_error = nodeport.get_stats_error()?.to_str()?.to_string();
    let ingress = nodeport.get_ingress()?;
    let ingress_drop_reasons = nodeport.get_ingress_drop_reasons()?;
    let egress = nodeport.get_egress()?;
    let flow_diagnostics = nodeport.get_flow_diagnostics()?;

    println!("NodePort:");
    println!("  desired_enabled: {}", nodeport.get_desired_enabled());
    if !nodeport_state.is_empty() {
        println!("  state: {nodeport_state}");
    }
    if !nodeport_source_mode.is_empty() {
        println!("  source_mode: {nodeport_source_mode}");
    }
    if !nodeport_identity_source.is_empty() {
        println!("  identity_source: {nodeport_identity_source}");
    }
    if !resolved_iface.is_empty() {
        println!("  resolved_iface: {resolved_iface}");
    }
    if !resolved_node_ip.is_empty() {
        println!("  resolved_node_ip: {resolved_node_ip}");
    }
    println!("  active_networks: {}", nodeport.get_active_networks());
    println!("  active_ports: {}", nodeport.get_active_ports());
    println!(
        "  active_host_networks: {}",
        nodeport.get_active_host_networks()
    );
    println!("  vip_capacity: {}", nodeport.get_vip_capacity());
    println!("  host_capacity: {}", nodeport.get_host_capacity());
    println!("  flow_capacity: {}", nodeport.get_flow_capacity());
    println!(
        "  ingress: packets={} bytes={} drops={}",
        ingress.get_packets(),
        ingress.get_bytes(),
        ingress.get_drops(),
    );
    let ingress_drop_reason_fields = nodeport_drop_reason_fields(ingress_drop_reasons)?;
    if !ingress_drop_reason_fields.is_empty() {
        println!(
            "  ingress_drop_reasons: {}",
            ingress_drop_reason_fields.join(" ")
        );
    }
    println!(
        "  egress: packets={} bytes={} drops={}",
        egress.get_packets(),
        egress.get_bytes(),
        egress.get_drops(),
    );
    println!(
        "  flow_diagnostics: {}",
        nodeport_flow_diagnostics_fields(flow_diagnostics).join(" ")
    );
    if !last_error.is_empty() {
        println!("  last_error: {last_error}");
    }
    if !stats_error.is_empty() {
        println!("  stats_error: {stats_error}");
    }

    Ok(())
}
