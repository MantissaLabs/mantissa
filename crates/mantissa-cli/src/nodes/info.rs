use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::nodes::info::PacketCountersView;
use mantissa_client::nodes::{
    LoadBalancerFlowDiagnosticsView, NodeInfoView, NodePortFlowDiagnosticsView,
    NodePortIngressDropReasonsView,
};
use std::fmt::{Display, Write as _};

use crate::output;

/// Fetches and prints local node information.
pub async fn info(cfg: &ClientConfig) -> Result<()> {
    let info = mantissa_client::nodes::info(cfg).await?;
    output::emit_block(render_info(&info));
    Ok(())
}

/// Renders the local node information as a Docker-info-style plain-text report.
fn render_info(info: &NodeInfoView) -> String {
    let mut rendered = String::new();

    push_section(&mut rendered, "Mantissa");
    push_field(&mut rendered, "  ", "version", env!("CARGO_PKG_VERSION"));

    push_section(&mut rendered, "Host");
    push_field(
        &mut rendered,
        "  ",
        "hostname",
        display_text(&info.hostname),
    );
    push_field(
        &mut rendered,
        "  ",
        "operating_system",
        display_text(&info.os.name),
    );
    push_field(
        &mut rendered,
        "  ",
        "os_version",
        display_text(&info.os.version),
    );
    push_field(
        &mut rendered,
        "  ",
        "kernel_version",
        display_text(&info.os.kernel_version),
    );

    push_section(&mut rendered, "CPU");
    push_field(
        &mut rendered,
        "  ",
        "vendor",
        display_text(&info.cpu.vendor),
    );
    push_field(&mut rendered, "  ", "brand", display_text(&info.cpu.brand));
    push_field(
        &mut rendered,
        "  ",
        "codename",
        display_text(&info.cpu.codename),
    );
    push_field(
        &mut rendered,
        "  ",
        "frequency",
        format_cpu_frequency(info.cpu.frequency_mhz),
    );
    push_field(&mut rendered, "  ", "physical_cores", info.cpu.cores);
    push_field(&mut rendered, "  ", "logical_cpus", info.cpu.logical_cpus);
    push_field(
        &mut rendered,
        "  ",
        "total_logical_cpus",
        info.cpu.total_logical_cpus,
    );
    push_field(
        &mut rendered,
        "  ",
        "l1_data_cache",
        format_cache_kib(info.cpu.l1_data_cache),
    );
    push_field(
        &mut rendered,
        "  ",
        "l1_instruction_cache",
        format_cache_kib(info.cpu.l1_instruction_cache),
    );
    push_field(
        &mut rendered,
        "  ",
        "l2_cache",
        format_cache_kib(info.cpu.l2_cache),
    );
    push_field(
        &mut rendered,
        "  ",
        "l3_cache",
        format_cache_kib(info.cpu.l3_cache),
    );

    push_section(&mut rendered, "GPU");
    if info.gpu.devices.is_empty() {
        let _ = writeln!(rendered, "  no GPU device detected");
    } else {
        if let Some(vendor) = &info.gpu.vendor {
            push_field(&mut rendered, "  ", "vendor", vendor);
        }
        for device in &info.gpu.devices {
            let _ = writeln!(rendered, "  - index: {}", device.index);
            if let Some(name) = &device.name {
                push_field(&mut rendered, "    ", "name", name);
            }
            if let Some(uuid) = &device.uuid {
                push_field(&mut rendered, "    ", "uuid", uuid);
            }
            if let Some(pci_bus_id) = &device.pci_bus_id {
                push_field(&mut rendered, "    ", "pci_bus_id", pci_bus_id);
            }
            if let Some(cc) = &device.compute_capability {
                push_field(&mut rendered, "    ", "compute_capability", cc);
            }
            push_field(
                &mut rendered,
                "    ",
                "memory_total",
                format_bytes(device.memory_total_bytes),
            );
            push_field(
                &mut rendered,
                "    ",
                "memory_free",
                format_bytes(device.memory_free_bytes),
            );
        }
    }

    push_section(&mut rendered, "Load Average");
    push_field(&mut rendered, "  ", "1m", format_load(info.load.one));
    push_field(&mut rendered, "  ", "5m", format_load(info.load.five));
    push_field(&mut rendered, "  ", "15m", format_load(info.load.fifteen));

    push_section(&mut rendered, "Memory");
    push_field(
        &mut rendered,
        "  ",
        "total",
        format_bytes(info.memory.total),
    );
    push_field(&mut rendered, "  ", "free", format_bytes(info.memory.free));
    push_field(
        &mut rendered,
        "  ",
        "available",
        format_bytes(info.memory.available),
    );
    push_field(
        &mut rendered,
        "  ",
        "buffers",
        format_bytes(info.memory.buffers),
    );
    push_field(
        &mut rendered,
        "  ",
        "cached",
        format_bytes(info.memory.cached),
    );
    push_field(
        &mut rendered,
        "  ",
        "swap_total",
        format_bytes(info.memory.swap_total),
    );
    push_field(
        &mut rendered,
        "  ",
        "swap_free",
        format_bytes(info.memory.swap_free),
    );

    push_section(&mut rendered, "Disk");
    push_field(&mut rendered, "  ", "total", format_bytes(info.disk.total));
    push_field(&mut rendered, "  ", "free", format_bytes(info.disk.free));

    push_section(&mut rendered, "Network");
    let _ = writeln!(rendered, "  NodePort:");
    push_field(
        &mut rendered,
        "    ",
        "desired_enabled",
        info.nodeport.desired_enabled,
    );
    push_optional_field(
        &mut rendered,
        "    ",
        "state",
        info.nodeport.state.as_deref(),
    );
    push_optional_field(
        &mut rendered,
        "    ",
        "source_mode",
        info.nodeport.source_mode.as_deref(),
    );
    push_optional_field(
        &mut rendered,
        "    ",
        "identity_source",
        info.nodeport.identity_source.as_deref(),
    );
    push_optional_field(
        &mut rendered,
        "    ",
        "resolved_iface",
        info.nodeport.resolved_iface.as_deref(),
    );
    push_optional_field(
        &mut rendered,
        "    ",
        "resolved_node_ip",
        info.nodeport.resolved_node_ip.as_deref(),
    );
    push_field(
        &mut rendered,
        "    ",
        "active_networks",
        info.nodeport.active_networks,
    );
    push_field(
        &mut rendered,
        "    ",
        "active_ports",
        info.nodeport.active_ports,
    );
    push_field(
        &mut rendered,
        "    ",
        "active_host_networks",
        info.nodeport.active_host_networks,
    );
    push_field(
        &mut rendered,
        "    ",
        "vip_capacity",
        info.nodeport.vip_capacity,
    );
    push_field(
        &mut rendered,
        "    ",
        "host_capacity",
        info.nodeport.host_capacity,
    );
    push_field(
        &mut rendered,
        "    ",
        "flow_capacity",
        info.nodeport.flow_capacity,
    );
    push_packet_counters(&mut rendered, "    ", "ingress", &info.nodeport.ingress);
    let ingress_drop_reason_fields =
        nodeport_drop_reason_fields(&info.nodeport.ingress_drop_reasons);
    if !ingress_drop_reason_fields.is_empty() {
        let _ = writeln!(rendered, "    ingress_drop_reasons:");
        for (label, value) in ingress_drop_reason_fields {
            push_field(&mut rendered, "      ", label, value);
        }
    }
    push_packet_counters(&mut rendered, "    ", "egress", &info.nodeport.egress);
    let _ = writeln!(rendered, "    flow_diagnostics:");
    for (label, value) in nodeport_flow_diagnostics_fields(&info.nodeport.flow_diagnostics) {
        push_field(&mut rendered, "      ", label, value);
    }
    push_optional_field(
        &mut rendered,
        "    ",
        "last_error",
        info.nodeport.last_error.as_deref(),
    );
    push_optional_field(
        &mut rendered,
        "    ",
        "stats_error",
        info.nodeport.stats_error.as_deref(),
    );

    let _ = writeln!(rendered, "  Load Balancer:");
    push_field(
        &mut rendered,
        "    ",
        "desired_enabled",
        info.load_balancer.desired_enabled,
    );
    push_field(
        &mut rendered,
        "    ",
        "programmed_networks",
        info.load_balancer.programmed_networks,
    );
    push_field(
        &mut rendered,
        "    ",
        "active_vips",
        info.load_balancer.active_vips,
    );
    push_field(
        &mut rendered,
        "    ",
        "ipv4_vips",
        info.load_balancer.ipv4_vips,
    );
    push_field(
        &mut rendered,
        "    ",
        "ipv6_vips",
        info.load_balancer.ipv6_vips,
    );
    push_field(
        &mut rendered,
        "    ",
        "flow_capacity",
        info.load_balancer.flow_capacity,
    );
    let _ = writeln!(rendered, "    flow_diagnostics:");
    for (label, value) in
        load_balancer_flow_diagnostics_fields(&info.load_balancer.flow_diagnostics)
    {
        push_field(&mut rendered, "      ", label, value);
    }
    push_optional_field(
        &mut rendered,
        "    ",
        "stats_error",
        info.load_balancer.stats_error.as_deref(),
    );

    rendered
}

/// Appends one top-level report section, separating sections with one blank line.
fn push_section(rendered: &mut String, title: &str) {
    if !rendered.is_empty() {
        rendered.push('\n');
    }
    let _ = writeln!(rendered, "{title}:");
}

/// Appends one indented label/value row to the report.
fn push_field(rendered: &mut String, indent: &str, label: &str, value: impl Display) {
    let _ = writeln!(rendered, "{indent}{label}: {value}");
}

/// Appends one optional indented label/value row when the value is present.
fn push_optional_field(rendered: &mut String, indent: &str, label: &str, value: Option<&str>) {
    if let Some(value) = value {
        push_field(rendered, indent, label, value);
    }
}

/// Appends one packet-counter block using readable byte units for data volume.
fn push_packet_counters(
    rendered: &mut String,
    indent: &str,
    label: &str,
    counters: &PacketCountersView,
) {
    let _ = writeln!(rendered, "{indent}{label}:");
    let child_indent = format!("{indent}  ");
    push_field(rendered, &child_indent, "packets", counters.packets);
    push_field(
        rendered,
        &child_indent,
        "bytes",
        format_bytes(counters.bytes),
    );
    push_field(rendered, &child_indent, "drops", counters.drops);
}

/// Returns a displayable text value, replacing empty protocol fields with a clear placeholder.
fn display_text(value: &str) -> &str {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "Unknown"
    } else {
        trimmed
    }
}

/// Renders one load-average value with stable precision.
fn format_load(value: f64) -> String {
    format!("{value:.2}")
}

/// Renders a CPU frequency, showing unavailable data explicitly instead of `0MHz`.
fn format_cpu_frequency(mhz: u64) -> String {
    if mhz == 0 {
        "unavailable".to_string()
    } else {
        format!("{mhz}MHz")
    }
}

/// Renders a CPU cache size reported in KiB, preserving unknown cache values.
fn format_cache_kib(kib: u64) -> String {
    if kib == 0 {
        "unavailable".to_string()
    } else {
        format_bytes(kib.saturating_mul(1024))
    }
}

/// Renders a byte count using IEC units with compact Docker-info-style suffixes.
fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];

    if bytes < 1024 {
        return format!("{bytes}B");
    }

    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    let formatted = format!("{value:.1}");
    format!("{}{}", formatted.trim_end_matches(".0"), UNITS[unit])
}

/// Renders non-zero NodePort ingress drop reasons for operator diagnostics.
fn nodeport_drop_reason_fields(reasons: &NodePortIngressDropReasonsView) -> Vec<(&str, u64)> {
    let mut fields = Vec::new();
    if reasons.invalid_ipv4_headers > 0 {
        fields.push(("invalid_ipv4_headers", reasons.invalid_ipv4_headers));
    }
    if reasons.invalid_l4_headers > 0 {
        fields.push(("invalid_l4_headers", reasons.invalid_l4_headers));
    }
    if reasons.missing_host_entries > 0 {
        fields.push(("missing_host_entries", reasons.missing_host_entries));
    }
    if reasons.nat_insert_failures > 0 {
        fields.push(("nat_insert_failures", reasons.nat_insert_failures));
    }
    if reasons.rewrite_failures > 0 {
        fields.push(("rewrite_failures", reasons.rewrite_failures));
    }
    if reasons.fragmented_ipv4_packets > 0 {
        fields.push(("fragmented_ipv4_packets", reasons.fragmented_ipv4_packets));
    }
    fields
}

/// Renders NodePort flow diagnostics as stable label/value rows.
fn nodeport_flow_diagnostics_fields(diagnostics: &NodePortFlowDiagnosticsView) -> Vec<(&str, u64)> {
    vec![
        ("flow_pairs", diagnostics.flow_pairs),
        ("ipv4_flow_pairs", diagnostics.ipv4_flow_pairs),
        ("ipv6_flow_pairs", diagnostics.ipv6_flow_pairs),
        ("flow_creates", diagnostics.flow_creates),
        ("flow_clears", diagnostics.flow_clears),
        (
            "estimated_flow_evictions",
            diagnostics.estimated_flow_evictions,
        ),
        ("reverse_misses", diagnostics.reverse_misses),
        (
            "invalid_conntrack_transitions",
            diagnostics.invalid_conntrack_transitions,
        ),
        (
            "return_path_bypass_packets",
            diagnostics.return_path_bypass_packets,
        ),
    ]
}

/// Renders load-balancer flow diagnostics as stable label/value rows.
fn load_balancer_flow_diagnostics_fields(
    diagnostics: &LoadBalancerFlowDiagnosticsView,
) -> Vec<(&str, u64)> {
    vec![
        ("flow_pairs", diagnostics.flow_pairs),
        ("ipv4_flow_pairs", diagnostics.ipv4_flow_pairs),
        ("ipv6_flow_pairs", diagnostics.ipv6_flow_pairs),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantissa_client::nodes::info::{
        CpuInfoView, DiskInfoView, GpuInfoView, LoadAverageView, LoadBalancerInfoView,
        MemoryInfoView, NodePortInfoView, OsInfoView, PacketCountersView,
    };

    /// Builds a representative node-info view for CLI rendering tests.
    fn sample_info() -> NodeInfoView {
        NodeInfoView {
            hostname: "mantissa-1".to_string(),
            os: OsInfoView {
                name: "Debian GNU/Linux".to_string(),
                version: "12".to_string(),
                kernel_version: "6.1.0-45-arm64".to_string(),
            },
            cpu: CpuInfoView {
                vendor: "Unknown".to_string(),
                brand: String::new(),
                codename: "Unknown".to_string(),
                frequency_mhz: 0,
                cores: 10,
                logical_cpus: 10,
                total_logical_cpus: 10,
                l1_data_cache: 0,
                l1_instruction_cache: 0,
                l2_cache: 256,
                l3_cache: 0,
            },
            load: LoadAverageView {
                one: 0.56,
                five: 0.24,
                fifteen: 0.09,
            },
            memory: MemoryInfoView {
                total: 33_606_361_088,
                free: 22_315_606_016,
                available: 32_620_093_440,
                buffers: 0,
                cached: 0,
                swap_total: 0,
                swap_free: 0,
            },
            disk: DiskInfoView {
                total: 253_283_461_191_680,
                free: 85_354_989_463_552,
            },
            gpu: GpuInfoView {
                vendor: None,
                devices: Vec::new(),
            },
            nodeport: NodePortInfoView {
                desired_enabled: true,
                state: Some("pending".to_string()),
                source_mode: Some("snat_host_access".to_string()),
                identity_source: None,
                resolved_iface: None,
                resolved_node_ip: None,
                active_networks: 0,
                active_ports: 0,
                active_host_networks: 0,
                vip_capacity: 1024,
                host_capacity: 256,
                flow_capacity: 2048,
                ingress: PacketCountersView {
                    packets: 0,
                    bytes: 1024,
                    drops: 0,
                },
                ingress_drop_reasons: NodePortIngressDropReasonsView {
                    invalid_ipv4_headers: 2,
                    ..NodePortIngressDropReasonsView::default()
                },
                egress: PacketCountersView {
                    packets: 0,
                    bytes: 0,
                    drops: 0,
                },
                flow_diagnostics: NodePortFlowDiagnosticsView {
                    flow_pairs: 3,
                    ipv4_flow_pairs: 1,
                    ipv6_flow_pairs: 2,
                    flow_creates: 4,
                    flow_clears: 5,
                    estimated_flow_evictions: 6,
                    reverse_misses: 7,
                    invalid_conntrack_transitions: 8,
                    return_path_bypass_packets: 9,
                },
                last_error: None,
                stats_error: None,
            },
            load_balancer: LoadBalancerInfoView {
                desired_enabled: true,
                programmed_networks: 0,
                active_vips: 0,
                ipv4_vips: 0,
                ipv6_vips: 0,
                flow_capacity: 1024,
                flow_diagnostics: LoadBalancerFlowDiagnosticsView {
                    flow_pairs: 0,
                    ipv4_flow_pairs: 0,
                    ipv6_flow_pairs: 0,
                },
                stats_error: None,
            },
        }
    }

    /// Verifies the info report uses plain display strings and the requested section order.
    #[test]
    fn render_info_omits_quotes_and_orders_sections() {
        let rendered = render_info(&sample_info());

        assert!(rendered.starts_with("Mantissa:\n  version: "));
        assert!(rendered.contains("  hostname: mantissa-1\n"));
        assert!(rendered.contains("  operating_system: Debian GNU/Linux\n"));
        assert!(!rendered.contains('"'));

        let cpu_pos = rendered.find("CPU:\n").unwrap_or(usize::MAX);
        let gpu_pos = rendered.find("GPU:\n").unwrap_or(usize::MAX);
        let load_pos = rendered.find("Load Average:\n").unwrap_or(usize::MAX);
        assert!(cpu_pos < gpu_pos);
        assert!(gpu_pos < load_pos);
    }

    /// Verifies byte-backed resource counters are rendered as readable IEC units.
    #[test]
    fn render_info_formats_capacity_units() {
        let rendered = render_info(&sample_info());

        assert!(rendered.contains("Memory:\n  total: 31.3GiB\n"));
        assert!(rendered.contains("  available: 30.4GiB\n"));
        assert!(rendered.contains("Disk:\n  total: 230.4TiB\n"));
        assert!(rendered.contains("  l2_cache: 256KiB\n"));
        assert!(!rendered.contains("Memory (Kb)"));
        assert!(!rendered.contains("Disk (Kb)"));
    }

    /// Verifies networking diagnostics are grouped and expanded into scan-friendly rows.
    #[test]
    fn render_info_groups_network_and_expands_diagnostics() {
        let rendered = render_info(&sample_info());

        assert!(rendered.contains("Network:\n  NodePort:\n"));
        assert!(rendered.contains("  Load Balancer:\n"));
        assert!(!rendered.contains("\nNodePort:\n"));
        assert!(!rendered.contains("\nLoad Balancer:\n"));
        assert!(rendered.contains("    ingress:\n      packets: 0\n      bytes: 1KiB\n"));
        assert!(rendered.contains("    ingress_drop_reasons:\n      invalid_ipv4_headers: 2\n"));
        assert!(rendered.contains("    flow_diagnostics:\n      flow_pairs: 3\n"));
        assert!(rendered.contains("      return_path_bypass_packets: 9\n"));
        assert!(!rendered.contains("flow_diagnostics: flow_pairs="));
    }
}
