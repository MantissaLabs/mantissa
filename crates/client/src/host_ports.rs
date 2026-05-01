use capnp::{Error as CapnpError, struct_list};
use protocol::workload::{PortProtocol as ProtoPortProtocol, port_binding};
use std::net::IpAddr;

/// Client-side view of one node-local host port binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HostPortView {
    pub(crate) name: String,
    pub(crate) target_port: u16,
    pub(crate) host_port: u16,
    pub(crate) host_ip: String,
    pub(crate) protocol: HostPortProtocolView,
}

impl HostPortView {
    /// Decodes one protocol host-port binding into a stable client view.
    fn from_reader(reader: port_binding::Reader<'_>) -> Result<Self, CapnpError> {
        let protocol = match reader.get_protocol()? {
            ProtoPortProtocol::Tcp => HostPortProtocolView::Tcp,
            ProtoPortProtocol::Udp => HostPortProtocolView::Udp,
        };
        Ok(Self {
            name: reader.get_name()?.to_str()?.to_string(),
            target_port: reader.get_target_port(),
            host_port: reader.get_host_port(),
            host_ip: reader.get_host_ip()?.to_str()?.to_string(),
            protocol,
        })
    }
}

/// Transport protocol used by a rendered host-port binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HostPortProtocolView {
    Tcp,
    Udp,
}

impl HostPortProtocolView {
    /// Returns the lowercase transport suffix used in CLI output.
    fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

/// Decodes host-port bindings from one Cap'n Proto list.
pub(crate) fn decode_host_ports(
    list: struct_list::Reader<port_binding::Owned>,
) -> Result<Vec<HostPortView>, CapnpError> {
    let mut ports = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        ports.push(HostPortView::from_reader(entry)?);
    }
    Ok(ports)
}

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
        port.protocol.as_str()
    );
    let name = port.name.trim();
    if name.is_empty() {
        binding
    } else {
        format!("{name} {binding}")
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
