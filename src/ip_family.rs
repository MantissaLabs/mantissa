use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};

/// # Description:
///
/// Configurable policy that selects the default IP family Mantissa should prefer when callers do
/// not explicitly request IPv4 or IPv6.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DefaultIpFamilyPolicy {
    #[default]
    Auto,
    Ipv4,
    Ipv6,
}

/// # Description:
///
/// Concrete single-stack IP family chosen after explicit config and host capability detection are
/// reconciled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IpFamily {
    Ipv4,
    Ipv6,
}

impl IpFamily {
    /// # Description:
    ///
    /// Converts one concrete IP address into its address family.
    pub fn from_ip(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(_) => Self::Ipv4,
            IpAddr::V6(_) => Self::Ipv6,
        }
    }
}

/// # Description:
///
/// Resolve the effective default IP family from explicit node addresses, operator policy, and the
/// discovered host capabilities in a stable precedence order.
pub fn infer_default_ip_family(
    explicit_node_ip: Option<IpAddr>,
    explicit_advertise_addr: Option<&str>,
    policy: DefaultIpFamilyPolicy,
    has_ipv4: bool,
    has_ipv6: bool,
) -> IpFamily {
    if let Some(ip) = explicit_node_ip {
        return IpFamily::from_ip(ip);
    }

    if let Some(addr) = explicit_advertise_addr
        && let Ok(socket_addr) = addr.trim().parse::<SocketAddr>()
    {
        return IpFamily::from_ip(socket_addr.ip());
    }

    match policy {
        DefaultIpFamilyPolicy::Ipv4 => IpFamily::Ipv4,
        DefaultIpFamilyPolicy::Ipv6 => IpFamily::Ipv6,
        DefaultIpFamilyPolicy::Auto => match (has_ipv4, has_ipv6) {
            (false, true) => IpFamily::Ipv6,
            _ => IpFamily::Ipv4,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{DefaultIpFamilyPolicy, IpFamily, infer_default_ip_family};
    use std::net::{IpAddr, Ipv6Addr};

    #[test]
    fn infer_default_ip_family_prefers_explicit_node_ip() {
        let family = infer_default_ip_family(
            Some(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            Some("10.0.0.10:6578"),
            DefaultIpFamilyPolicy::Ipv4,
            true,
            false,
        );
        assert_eq!(family, IpFamily::Ipv6);
    }

    #[test]
    fn infer_default_ip_family_uses_explicit_advertise_addr_before_policy() {
        let family = infer_default_ip_family(
            None,
            Some("[fd42::10]:6578"),
            DefaultIpFamilyPolicy::Ipv4,
            true,
            true,
        );
        assert_eq!(family, IpFamily::Ipv6);
    }

    #[test]
    fn infer_default_ip_family_honors_explicit_policy_without_node_overrides() {
        let family = infer_default_ip_family(None, None, DefaultIpFamilyPolicy::Ipv6, true, true);
        assert_eq!(family, IpFamily::Ipv6);
    }

    #[test]
    fn infer_default_ip_family_auto_prefers_ipv6_only_hosts() {
        let family = infer_default_ip_family(None, None, DefaultIpFamilyPolicy::Auto, false, true);
        assert_eq!(family, IpFamily::Ipv6);
    }

    #[test]
    fn infer_default_ip_family_auto_defaults_to_ipv4_on_dual_stack() {
        let family = infer_default_ip_family(None, None, DefaultIpFamilyPolicy::Auto, true, true);
        assert_eq!(family, IpFamily::Ipv4);
    }
}
