use std::net::{IpAddr, ToSocketAddrs};

/// Resolve one configured socket address into its current IP address.
///
/// Mantissa accepts advertise addresses in `host:port` form. Several networking subsystems only
/// need the resolved IP portion so they can select local interfaces or publication identities
/// without duplicating the same socket-parsing logic.
pub(crate) fn resolve_advertise_ip(addr: &str) -> Option<IpAddr> {
    addr.to_socket_addrs()
        .ok()?
        .next()
        .map(|socket| socket.ip())
}
