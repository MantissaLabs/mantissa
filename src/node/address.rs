use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs, UdpSocket};
#[cfg(unix)]
use std::ptr;

use crate::ip_family::IpFamily;

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
        SocketAddr::V4(_) => {
            // Stick to the same address family by binding to 0.0.0.0:0 for IPv4 probes.
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
        }
        SocketAddr::V6(_) => {
            // Stick to the same address family by binding to [::]:0 for IPv6 probes.
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
        }
    };
    let sock = UdpSocket::bind(bind_sa)?;
    sock.connect(dest_sa)?;
    Ok(sock.local_addr()?.ip())
}

/// Detects whether the host currently has usable local interface addresses in each family.
pub fn detect_local_ip_families() -> (bool, bool) {
    let Ok(addresses) = usable_local_interface_ips() else {
        return (false, false);
    };
    let has_ipv4 = addresses.iter().any(IpAddr::is_ipv4);
    let has_ipv6 = addresses.iter().any(IpAddr::is_ipv6);
    (has_ipv4, has_ipv6)
}

/// Best-effort local advertise address:
/// 1) If `cfg_advertise` provided, use it.
/// 2) Else, if `anchor` provided, derive via outbound_ip_for(anchor).
/// 3) Else, select a usable local interface address in the preferred family order.
pub fn compute_advertise_ip(
    cfg_advertise: Option<IpAddr>,
    anchor: Option<&str>,
    preferred_family: Option<IpFamily>,
) -> io::Result<IpAddr> {
    if let Some(ip) = cfg_advertise {
        return Ok(ip);
    }
    if let Some(a) = anchor
        && let Ok(ip) = outbound_ip_for(a)
    {
        return Ok(ip);
    }

    match preferred_family {
        Some(IpFamily::Ipv6) => local_interface_ip_for_family(IpFamily::Ipv6)
            .or_else(|_| local_interface_ip_for_family(IpFamily::Ipv4)),
        Some(IpFamily::Ipv4) | None => local_interface_ip_for_family(IpFamily::Ipv4)
            .or_else(|_| local_interface_ip_for_family(IpFamily::Ipv6)),
    }
}

/// Returns one local non-loopback interface address for the requested address family.
fn local_interface_ip_for_family(family: IpFamily) -> io::Result<IpAddr> {
    usable_local_interface_ips()?
        .into_iter()
        .find(|ip| {
            matches!(
                (family, ip),
                (IpFamily::Ipv4, IpAddr::V4(_)) | (IpFamily::Ipv6, IpAddr::V6(_))
            )
        })
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                format!("no usable local {family:?} interface address found"),
            )
        })
}

/// Enumerates usable local interface addresses without probing public internet endpoints.
#[cfg(unix)]
fn usable_local_interface_ips() -> io::Result<Vec<IpAddr>> {
    let mut addrs: *mut libc::ifaddrs = ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut addrs) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let _guard = IfAddrs(addrs);

    let mut ips = Vec::new();
    let mut cursor = addrs;
    while !cursor.is_null() {
        let interface = unsafe { &*cursor };
        if interface_is_usable(interface.ifa_flags)
            && let Some(ip) = unsafe { sockaddr_to_ip(interface.ifa_addr) }
            && is_usable_advertise_ip(ip)
        {
            ips.push(ip);
        }
        cursor = interface.ifa_next;
    }

    Ok(ips)
}

/// Reports no local interfaces on unsupported targets while keeping callers portable.
#[cfg(not(unix))]
fn usable_local_interface_ips() -> io::Result<Vec<IpAddr>> {
    Ok(Vec::new())
}

#[cfg(unix)]
/// Owns the linked list allocated by `getifaddrs` until enumeration completes.
struct IfAddrs(*mut libc::ifaddrs);

#[cfg(unix)]
impl Drop for IfAddrs {
    /// Releases the linked list returned by `getifaddrs`.
    fn drop(&mut self) {
        unsafe { libc::freeifaddrs(self.0) };
    }
}

/// Returns whether an interface should be considered for automatic advertisement.
#[cfg(unix)]
fn interface_is_usable(flags: libc::c_uint) -> bool {
    let up = flags & libc::IFF_UP as libc::c_uint != 0;
    let loopback = flags & libc::IFF_LOOPBACK as libc::c_uint != 0;
    up && !loopback
}

/// Converts an OS socket address from `getifaddrs` into a Rust IP address.
#[cfg(unix)]
unsafe fn sockaddr_to_ip(addr: *const libc::sockaddr) -> Option<IpAddr> {
    if addr.is_null() {
        return None;
    }

    match unsafe { (*addr).sa_family as libc::c_int } {
        libc::AF_INET => {
            let addr = unsafe { &*(addr as *const libc::sockaddr_in) };
            Some(IpAddr::V4(Ipv4Addr::from(
                addr.sin_addr.s_addr.to_ne_bytes(),
            )))
        }
        libc::AF_INET6 => {
            let addr = unsafe { &*(addr as *const libc::sockaddr_in6) };
            Some(IpAddr::V6(Ipv6Addr::from(addr.sin6_addr.s6_addr)))
        }
        _ => None,
    }
}

/// Returns whether an interface address is safe to publish automatically.
fn is_usable_advertise_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => !ip.is_unspecified() && !ip.is_loopback() && !ip.is_multicast(),
        IpAddr::V6(ip) => {
            !ip.is_unspecified()
                && !ip.is_loopback()
                && !ip.is_unicast_link_local()
                && !ip.is_multicast()
        }
    }
}

/// Extract the port from an advertise address string.
///
/// Accepts:
/// - "192.168.104.3:6578"
/// - "hostname.local:8080"
/// - "[fe80::1%eth0]:9000"
/// - "fe80::1:9000"          // fallback: last ':' is treated as the port separator
#[allow(dead_code)]
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
