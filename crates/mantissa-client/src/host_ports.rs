use capnp::{Error as CapnpError, struct_list};
use mantissa_protocol::workload::{PortProtocol as ProtoPortProtocol, port_binding};

/// Client-side view of one node-local host port binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostPortView {
    pub name: String,
    pub target_port: u16,
    pub host_port: u16,
    pub host_ip: String,
    pub protocol: HostPortProtocolView,
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
pub enum HostPortProtocolView {
    Tcp,
    Udp,
}

impl HostPortProtocolView {
    /// Returns the lowercase transport suffix used by host-port renderers.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

/// Decodes host-port bindings from one Cap'n Proto list.
pub fn decode_host_ports(
    list: struct_list::Reader<port_binding::Owned>,
) -> Result<Vec<HostPortView>, CapnpError> {
    let mut ports = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        ports.push(HostPortView::from_reader(entry)?);
    }
    Ok(ports)
}
