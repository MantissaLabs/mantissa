use std::io;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs, UdpSocket};

// TODO: This is a really hacky way of getting the local IP address to send
// on join request alongside the NodeInfo struct. We should probably get the
// address from the stream on connection accept within Server, and find a way
// to pass that down to Topology/Membership. For the time being, it is more
// convenient to use this approach to keep Server and Topology decoupled.

/// Returns the local IP the kernel would pick to reach `dest` (no packets sent).
pub fn outbound_ip_for<A: ToSocketAddrs>(dest: A) -> io::Result<IpAddr> {
    // Resolve once; prefer IPv4 if present (tweak if you want IPv6-first).
    let mut addrs = dest.to_socket_addrs()?;
    let dest_sa = addrs
        .find(|sa| sa.is_ipv4())
        .or_else(|| addrs.next())
        .ok_or_else(|| io::Error::other("no destination addrs"))?;

    // Bind wildcard on the same family as dest
    let bind_sa: SocketAddr = match dest_sa {
        SocketAddr::V4(_) => "0.0.0.0:0".parse::<SocketAddr>().unwrap(),
        SocketAddr::V6(_) => "[::]:0".parse::<SocketAddr>().unwrap(),
    };
    let sock = UdpSocket::bind(bind_sa)?;
    sock.connect(dest_sa)?;
    Ok(sock.local_addr()?.ip())
}

/// Best-effort local advertise address:
/// 1) If `cfg_advertise` provided, use it.
/// 2) Else, if `anchor` provided, derive via outbound_ip_for(anchor).
/// 3) Else, derive via outbound_ip_for("8.8.8.8:53") as a default route probe.
pub fn compute_advertise_ip(
    cfg_advertise: Option<IpAddr>,
    anchor: Option<&str>,
) -> io::Result<IpAddr> {
    if let Some(ip) = cfg_advertise {
        return Ok(ip);
    }
    if let Some(a) = anchor {
        if let Ok(ip) = outbound_ip_for(a) {
            return Ok(ip);
        }
    }
    outbound_ip_for("8.8.8.8:53")
}

/// Extract the port from an advertise address string.
///
/// Accepts:
/// - "192.168.104.3:6578"
/// - "hostname.local:8080"
/// - "[fe80::1%eth0]:9000"
/// - "fe80::1:9000"          // fallback: last ':' is treated as the port separator
pub fn extract_port(addr: &str) -> Result<u16, io::Error> {
    if let Ok(sa) = addr.parse::<SocketAddr>() {
        return Ok(sa.port());
    }

    // Fallback: split on the last ':' and parse the tail as a u16 port.
    if let Some((_head, tail)) = addr.rsplit_once(':') {
        let port = tail
            .parse::<u16>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid port"))?;
        Ok(port)
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidInput, "no port found"))
    }
}
