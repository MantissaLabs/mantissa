use mantissa_client::host_ports::{HostPortProtocolView, HostPortView};
use std::net::IpAddr;

/// Renders host-port bindings in the same compact form across CLI surfaces.
pub(crate) fn render_host_ports(ports: &[HostPortView]) -> String {
    if ports.is_empty() {
        return "-".to_string();
    }

    let mut rendered: Vec<String> = ports.iter().map(render_host_port).collect();
    rendered.sort();
    rendered.join(", ")
}

/// Renders one host-port binding as `name host_ip:host_port->target/protocol`.
fn render_host_port(port: &HostPortView) -> String {
    let endpoint = render_host_endpoint(&port.host_ip, port.host_port);
    let binding = format!(
        "{endpoint}->{}/{}",
        port.target_port,
        host_port_protocol_label(port.protocol)
    );
    let name = port.name.trim();
    if name.is_empty() {
        binding
    } else {
        format!("{name} {binding}")
    }
}

/// Returns the lowercase transport suffix used in CLI output.
fn host_port_protocol_label(protocol: HostPortProtocolView) -> &'static str {
    match protocol {
        HostPortProtocolView::Tcp => "tcp",
        HostPortProtocolView::Udp => "udp",
    }
}

/// Renders a host socket endpoint, including IPv6 bracket syntax.
fn render_host_endpoint(host_ip: &str, host_port: u16) -> String {
    let host_ip = host_ip.trim();
    let host_ip = if host_ip.is_empty() {
        "0.0.0.0"
    } else {
        host_ip
    };
    match host_ip.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => format!("{ip}:{host_port}"),
        Ok(IpAddr::V6(ip)) => format!("[{ip}]:{host_port}"),
        Err(_) => format!("{host_ip}:{host_port}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds one rendered host-port row without constructing a Cap'n Proto payload.
    fn host_port(name: &str, host_ip: &str, host_port: u16) -> HostPortView {
        HostPortView {
            name: name.to_string(),
            target_port: 8080,
            host_port,
            host_ip: host_ip.to_string(),
            protocol: HostPortProtocolView::Tcp,
        }
    }

    #[test]
    fn render_host_ports_includes_name_host_and_target() {
        let rendered = render_host_ports(&[host_port("http", "0.0.0.0", 18080)]);

        assert_eq!(rendered, "http 0.0.0.0:18080->8080/tcp");
    }

    #[test]
    fn render_host_ports_brackets_ipv6_hosts() {
        let rendered = render_host_ports(&[host_port("http", "::1", 18080)]);

        assert_eq!(rendered, "http [::1]:18080->8080/tcp");
    }
}
