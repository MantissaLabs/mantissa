use std::collections::{BTreeSet, HashSet};
use uuid::Uuid;

/// Return the stable eight-hex suffix Mantissa embeds into every managed interface name.
///
/// The same suffixing scheme is used for per-network bridge, VXLAN, and host-access devices as
/// well as per-attachment veth pairs. Sharing the formatter keeps naming consistent anywhere the
/// control plane reconstructs interface names from replicated identifiers.
pub(crate) fn managed_interface_suffix(id: Uuid) -> String {
    let hex = id.simple().to_string();
    hex.chars().take(8).collect()
}

/// Compute the deterministic host-side veth name for one local task attachment.
pub(crate) fn host_iface_name(attachment_id: Uuid) -> String {
    format!("mnth-{}", managed_interface_suffix(attachment_id))
}

/// Compute the deterministic runtime-side veth name for one local task attachment.
pub(crate) fn instance_iface_name(attachment_id: Uuid) -> String {
    format!("mntc-{}", managed_interface_suffix(attachment_id))
}

/// Compute the deterministic bridge name for one overlay network.
pub(crate) fn bridge_name(network_id: Uuid) -> String {
    format!("mnt-br-{}", managed_interface_suffix(network_id))
}

/// Compute the deterministic host-facing veth name for one overlay network.
pub(crate) fn host_access_host_iface_name(network_id: Uuid) -> String {
    format!("mnhost-{}", managed_interface_suffix(network_id))
}

/// Compute the deterministic bridge-peer veth name for one overlay network.
pub(crate) fn host_access_peer_iface_name(network_id: Uuid) -> String {
    format!("mnhp-{}", managed_interface_suffix(network_id))
}

/// Compute the deterministic VXLAN device name for one overlay network.
pub(crate) fn vxlan_name(network_id: Uuid) -> String {
    format!("mvx-{}", managed_interface_suffix(network_id))
}

/// Return the managed network suffix when a link name belongs to Mantissa overlay plumbing.
///
/// Mantissa-managed network interfaces all embed the same eight-hex network suffix, which lets
/// the controller identify orphaned devices without storing extra local metadata.
pub(crate) fn managed_network_interface_suffix(link_name: &str) -> Option<&str> {
    // Prefixes that encode one network suffix rather than one task attachment suffix.
    const PREFIXES: [&str; 4] = ["mvx-", "mnt-br-", "mnhost-", "mnhp-"];

    let suffix = PREFIXES
        .into_iter()
        .find_map(|prefix| link_name.strip_prefix(prefix))?;
    if suffix.len() == 8 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Some(suffix)
    } else {
        None
    }
}

/// Return whether one host link name belongs to Mantissa-managed overlay plumbing.
///
/// Underlay detection must ignore the controller's own bridge, VXLAN, host-access, and task
/// attachment devices or it can accidentally bootstrap a new network from another overlay's local
/// transient MTU and addressing state.
pub(crate) fn is_managed_overlay_link_name(link_name: &str) -> bool {
    managed_network_interface_suffix(link_name).is_some()
        || link_name.starts_with("mnth-")
        || link_name.starts_with("mntc-")
}

/// Collect the leaked per-network interface suffixes that do not correspond to any live network.
///
/// The controller uses this before reconciling active networks so orphaned host-access links from
/// earlier crashes cannot leave duplicate connected routes that hijack host-originated health
/// probes and embedded DNS traffic.
pub(crate) fn collect_orphaned_network_suffixes<I, S>(
    desired: &HashSet<Uuid>,
    link_names: I,
) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let desired_suffixes: HashSet<String> = desired
        .iter()
        .map(|id| managed_interface_suffix(*id))
        .collect();
    let mut orphaned: BTreeSet<String> = BTreeSet::new();

    for link_name in link_names {
        let Some(suffix) = managed_network_interface_suffix(link_name.as_ref()) else {
            continue;
        };
        if desired_suffixes.contains(suffix) {
            continue;
        }
        orphaned.insert(suffix.to_string());
    }

    orphaned.into_iter().collect()
}
