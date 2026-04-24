use std::net::IpAddr;
use uuid::Uuid;

/// Backend coordinates used when populating dataplane maps.
#[derive(Clone, Debug)]
pub struct BackendAddress {
    pub ip: IpAddr,
    pub mac: [u8; 6],
}

/// Live flow occupancy for the overlay VIP conntrack caches.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LoadBalancerFlowDiagnostics {
    pub ipv4_flow_pairs: usize,
    pub ipv6_flow_pairs: usize,
}

/// Snapshot of the local overlay VIP dataplane state that backs internal service load balancing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LoadBalancerStatus {
    pub desired_enabled: bool,
    pub programmed_networks: usize,
    pub ipv4_vips: usize,
    pub ipv6_vips: usize,
    pub flow_capacity: usize,
    pub flow_diagnostics: Option<LoadBalancerFlowDiagnostics>,
    pub stats_error: Option<String>,
}

/// Platform abstraction that either programs real eBPF maps (Linux) or acts as a no-op shim on
/// unsupported hosts so higher layers can remain platform-agnostic.
#[derive(Clone, Default)]
pub struct BpfLoadBalancer {
    inner: PlatformBpfLoadBalancer,
}

impl BpfLoadBalancer {
    /// Build the platform load balancer wrapper used by service discovery.
    pub fn new() -> Self {
        Self {
            inner: PlatformBpfLoadBalancer::new(),
        }
    }

    /// Synchronize VIP metadata and backend endpoints for the provided network into the pinned eBPF
    /// maps backing the load balancer.
    pub fn sync_vip(
        &self,
        network_id: Uuid,
        vip: IpAddr,
        vip_mac: [u8; 6],
        backends: &[BackendAddress],
    ) -> anyhow::Result<()> {
        self.inner.sync_vip(network_id, vip, vip_mac, backends)
    }

    /// Return the current node-local overlay VIP dataplane status for diagnostics.
    pub fn status(&self) -> LoadBalancerStatus {
        self.inner.status()
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{BackendAddress, LoadBalancerFlowDiagnostics, LoadBalancerStatus};
    use crate::network::attachment::{
        bridge_name, host_access_host_iface_name, host_access_peer_iface_name, vxlan_name,
    };
    use anyhow::{Context, Result};
    use aya::Pod;
    use aya::maps::MapData;
    use nix::mount::{MsFlags, mount};
    use nix::sys::statfs::{BPF_FS_MAGIC, statfs};
    use std::fs;
    use std::mem;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::os::fd::{AsFd, AsRawFd};
    use std::path::Path;
    use tracing::warn;
    use uuid::Uuid;

    #[derive(Clone, Default)]
    pub struct PlatformBpfLoadBalancer;

    /// Names of the pinned BPF maps backing one load-balancer family.
    struct LbMapNames {
        vip_map: &'static str,
        backend_map: &'static str,
        forward_map: &'static str,
        reverse_map: &'static str,
    }

    /// Pinned map names used by the IPv4 overlay VIP dataplane.
    const IPV4_MAPS: LbMapNames = LbMapNames {
        vip_map: "LB_VIPS",
        backend_map: "LB_BACKENDS",
        forward_map: "LB_FWD",
        reverse_map: "LB_REV",
    };

    /// Pinned map names used by the IPv6 overlay VIP dataplane.
    const IPV6_MAPS: LbMapNames = LbMapNames {
        vip_map: "LB_VIPS_V6",
        backend_map: "LB_BACKENDS_V6",
        forward_map: "LB_FWD_V6",
        reverse_map: "LB_REV_V6",
    };

    impl PlatformBpfLoadBalancer {
        /// Build the Linux load balancer map programmer.
        pub fn new() -> Self {
            Self
        }

        /// Return the current overlay VIP dataplane status by aggregating the pinned per-network
        /// maps that back local VIP publication.
        pub fn status(&self) -> LoadBalancerStatus {
            let mut status = LoadBalancerStatus {
                desired_enabled: crate::config::bpf_attach_enabled(),
                flow_capacity: crate::config::bpf_overlay_flow_capacity(),
                ..LoadBalancerStatus::default()
            };

            if !status.desired_enabled {
                status.stats_error = Some("overlay bpf attach is disabled".to_string());
                return status;
            }

            let mut flow_diagnostics = LoadBalancerFlowDiagnostics::default();

            let network_dirs = match load_balancer_network_dirs() {
                Ok(network_dirs) => network_dirs,
                Err(err) => {
                    status.stats_error = Some(format!("read overlay load-balancer maps: {err:#}"));
                    return status;
                }
            };

            for base in network_dirs {
                if !network_has_lb_maps(&base) || !network_has_live_dataplane_interfaces(&base) {
                    continue;
                }

                status.programmed_networks += 1;
                if let Err(err) =
                    accumulate_network_status(&base, &mut status, &mut flow_diagnostics)
                {
                    status.stats_error.get_or_insert_with(|| {
                        format!(
                            "read overlay load-balancer maps from {}: {err:#}",
                            base.display()
                        )
                    });
                }
            }

            status.flow_diagnostics = Some(flow_diagnostics);
            status
        }

        /// Synchronize one VIP into the pinned map family matching its IP address.
        ///
        /// Mantissa programs IPv4 and IPv6 service VIPs into separate BPF map sets so the bridge
        /// classifiers can keep compact keys per family instead of inflating every lookup with a
        /// tagged union layout.
        pub fn sync_vip(
            &self,
            network_id: Uuid,
            vip: IpAddr,
            vip_mac: [u8; 6],
            backends: &[BackendAddress],
        ) -> Result<()> {
            match vip {
                IpAddr::V4(vip) => self.sync_ipv4_vip(network_id, vip, vip_mac, backends),
                IpAddr::V6(vip) => self.sync_ipv6_vip(network_id, vip, vip_mac, backends),
            }
        }

        /// Program one IPv4 VIP plus its backend lookup ring.
        ///
        /// This retains the existing IPv4 map layout while sharing the higher-level `IpAddr`
        /// surface with the new IPv6 path.
        fn sync_ipv4_vip(
            &self,
            network_id: Uuid,
            vip: Ipv4Addr,
            vip_mac: [u8; 6],
            backends: &[BackendAddress],
        ) -> Result<()> {
            let base = map_pin_dir(network_id)?;
            let vip_map = open_map(&base, IPV4_MAPS.vip_map).context("open IPv4 LB_VIPS map")?;
            let backend_map =
                open_map(&base, IPV4_MAPS.backend_map).context("open IPv4 LB_BACKENDS map")?;
            let requested_backends = backends.len();
            let admitted_backends = requested_backends.min(MAX_BACKENDS_PER_VIP);

            if requested_backends > MAX_BACKENDS_PER_VIP {
                warn!(
                    target: "network",
                    network_id = %network_id,
                    vip = %vip,
                    requested_backends,
                    admitted_backends,
                    cap = MAX_BACKENDS_PER_VIP,
                    "service backend fanout exceeds LB dataplane cap; truncating backend set"
                );
            }

            let key = VipKey {
                vip: u32::from_ne_bytes(vip.octets()),
            };
            let desired_ring = build_backend_lookup_ring_v4(backends, MAX_BACKENDS_PER_VIP)
                .context("build backend ring")?;
            let programmed_slots = desired_ring.len();
            let existing_entry =
                lookup_elem::<VipKey, VipEntry>(vip_map.fd().as_fd().as_raw_fd(), &key)
                    .context("lookup existing VIP metadata")?;
            let previous_slots = existing_entry
                .map(|entry| entry.backend_count as usize)
                .unwrap_or(0);

            let mut clear_flows = programmed_slots == 0 && previous_slots > 0;
            if programmed_slots > 0 {
                clear_flows = match backend_state_matches_v4(
                    backend_map.fd().as_fd().as_raw_fd(),
                    &key,
                    existing_entry,
                    vip_mac,
                    &desired_ring,
                ) {
                    Ok(matches) => !matches,
                    Err(_) => true,
                };
            }

            program_backends_v4(backend_map.fd().as_fd().as_raw_fd(), key.vip, &desired_ring)
                .context("program IPv4 backends")?;
            let entry = build_vip_entry(vip_mac, programmed_slots);
            update_elem(vip_map.fd().as_fd().as_raw_fd(), &key, &entry)
                .context("update IPv4 VIP metadata")?;
            trim_vip_backends_v4(
                backend_map.fd().as_fd().as_raw_fd(),
                key.vip,
                programmed_slots,
                previous_slots,
            )
            .context("trim stale IPv4 backend slots")?;

            if clear_flows {
                let _ = clear_vip_flows_v4(&base, key.vip);
            }
            Ok(())
        }

        /// Program one IPv6 VIP plus its backend lookup ring.
        ///
        /// The IPv6 bridge dataplane uses dedicated 16-byte key maps so service discovery can
        /// publish stable AAAA VIP records without expanding the existing IPv4 map ABI.
        fn sync_ipv6_vip(
            &self,
            network_id: Uuid,
            vip: Ipv6Addr,
            vip_mac: [u8; 6],
            backends: &[BackendAddress],
        ) -> Result<()> {
            let base = map_pin_dir(network_id)?;
            let vip_map = open_map(&base, IPV6_MAPS.vip_map).context("open IPv6 LB_VIPS map")?;
            let backend_map =
                open_map(&base, IPV6_MAPS.backend_map).context("open IPv6 LB_BACKENDS map")?;
            let requested_backends = backends.len();
            let admitted_backends = requested_backends.min(MAX_BACKENDS_PER_VIP);

            if requested_backends > MAX_BACKENDS_PER_VIP {
                warn!(
                    target: "network",
                    network_id = %network_id,
                    vip = %vip,
                    requested_backends,
                    admitted_backends,
                    cap = MAX_BACKENDS_PER_VIP,
                    "service backend fanout exceeds LB dataplane cap; truncating backend set"
                );
            }

            let key = VipKey6 { vip: vip.octets() };
            let desired_ring = build_backend_lookup_ring_v6(backends, MAX_BACKENDS_PER_VIP)
                .context("build backend ring")?;
            let programmed_slots = desired_ring.len();
            let existing_entry =
                lookup_elem::<VipKey6, VipEntry>(vip_map.fd().as_fd().as_raw_fd(), &key)
                    .context("lookup existing IPv6 VIP metadata")?;
            let previous_slots = existing_entry
                .map(|entry| entry.backend_count as usize)
                .unwrap_or(0);

            let mut clear_flows = programmed_slots == 0 && previous_slots > 0;
            if programmed_slots > 0 {
                clear_flows = match backend_state_matches_v6(
                    backend_map.fd().as_fd().as_raw_fd(),
                    &key,
                    existing_entry,
                    vip_mac,
                    &desired_ring,
                ) {
                    Ok(matches) => !matches,
                    Err(_) => true,
                };
            }

            program_backends_v6(backend_map.fd().as_fd().as_raw_fd(), key.vip, &desired_ring)
                .context("program IPv6 backends")?;
            let entry = build_vip_entry(vip_mac, programmed_slots);
            update_elem(vip_map.fd().as_fd().as_raw_fd(), &key, &entry)
                .context("update IPv6 VIP metadata")?;
            trim_vip_backends_v6(
                backend_map.fd().as_fd().as_raw_fd(),
                key.vip,
                programmed_slots,
                previous_slots,
            )
            .context("trim stale IPv6 backend slots")?;

            if clear_flows {
                let _ = clear_vip_flows_v6(&base, key.vip);
            }
            Ok(())
        }
    }

    /// Maximum backends programmed per VIP in eBPF maps.
    ///
    /// Keep this in sync with `network_ebpf::lb::MAX_BACKENDS_PER_VIP`.
    const MAX_BACKENDS_PER_VIP: usize = 1024;
    /// bpf(2) command used to upsert VIP and backend map entries.
    const BPF_MAP_UPDATE_ELEM: libc::c_uint = 2;
    /// bpf(2) command used to delete stale VIP, backend, and flow entries.
    const BPF_MAP_DELETE_ELEM: libc::c_uint = 3;
    /// bpf(2) command used to iterate pinned map keys for diagnostics and cleanup.
    const BPF_MAP_GET_NEXT_KEY: libc::c_uint = 4;
    /// bpf(2) command used to read current VIP metadata before deciding whether flows are stale.
    const BPF_MAP_LOOKUP_ELEM: libc::c_uint = 1;

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct VipKey {
        vip: u32,
    }
    unsafe impl Pod for VipKey {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct VipBackendKey {
        vip: u32,
        slot: u32,
    }
    unsafe impl Pod for VipBackendKey {}

    #[repr(C)]
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    struct Backend {
        ip: u32,
        mac: [u8; 6],
        _pad: u16,
    }
    unsafe impl Pod for Backend {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct VipKey6 {
        vip: [u8; 16],
    }
    unsafe impl Pod for VipKey6 {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct VipBackendKey6 {
        vip: [u8; 16],
        slot: u32,
        _pad: [u8; 4],
    }
    unsafe impl Pod for VipBackendKey6 {}

    #[repr(C)]
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    struct Backend6 {
        ip: [u8; 16],
        mac: [u8; 6],
        _pad: [u8; 2],
    }
    unsafe impl Pod for Backend6 {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct Flow4 {
        src: u32,
        dst: u32,
        src_port: u16,
        dst_port: u16,
        proto: u8,
        pad: u8,
        padding: [u8; 2],
    }
    unsafe impl Pod for Flow4 {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct Flow6 {
        src: [u8; 16],
        dst: [u8; 16],
        src_port: u16,
        dst_port: u16,
        proto: u8,
        padding: [u8; 3],
    }
    unsafe impl Pod for Flow6 {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct ConntrackMetadata {
        last_seen_ns: u64,
        protocol: u8,
        state: u8,
        flags: u8,
        _pad: [u8; 5],
    }
    unsafe impl Pod for ConntrackMetadata {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct NatEntry {
        vip: u32,
        vip_mac: [u8; 6],
        _pad0: [u8; 2],
        backend_ip: u32,
        backend_mac: [u8; 6],
        _pad1: [u8; 2],
        conntrack: ConntrackMetadata,
    }
    unsafe impl Pod for NatEntry {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct NatEntry6 {
        vip: [u8; 16],
        vip_mac: [u8; 6],
        _pad0: [u8; 2],
        backend_ip: [u8; 16],
        backend_mac: [u8; 6],
        _pad1: [u8; 2],
        conntrack: ConntrackMetadata,
    }
    unsafe impl Pod for NatEntry6 {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct VipEntry {
        vip_mac: [u8; 6],
        backend_count: u16,
        _pad: [u8; 2],
    }
    unsafe impl Pod for VipEntry {}

    /// Determine whether the currently programmed VIP/backends match the desired inputs.
    ///
    /// This lets us avoid clearing flow state on every refresh, which would otherwise disrupt
    /// stable connections even when the backend set is unchanged.
    fn backend_state_matches_v4(
        backend_fd: std::os::fd::RawFd,
        vip_key: &VipKey,
        existing_entry: Option<VipEntry>,
        vip_mac: [u8; 6],
        backends: &[Backend],
    ) -> Result<bool> {
        let Some(existing) = existing_entry else {
            return Ok(false);
        };

        if existing.vip_mac != vip_mac {
            return Ok(false);
        }

        let expected_count = backends.len();
        if existing.backend_count as usize != expected_count {
            return Ok(false);
        }

        for (idx, backend) in backends.iter().enumerate() {
            let key = VipBackendKey {
                vip: vip_key.vip,
                slot: idx as u32,
            };
            let Some(existing_backend) = lookup_elem::<VipBackendKey, Backend>(backend_fd, &key)?
            else {
                return Ok(false);
            };
            if existing_backend != *backend {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Determine whether the currently programmed IPv6 VIP/backends match the desired inputs.
    ///
    /// This lets refresh loops preserve sticky flow state when the backend membership is already
    /// correct instead of blowing away the v6 flow caches on every pass.
    fn backend_state_matches_v6(
        backend_fd: std::os::fd::RawFd,
        vip_key: &VipKey6,
        existing_entry: Option<VipEntry>,
        vip_mac: [u8; 6],
        backends: &[Backend6],
    ) -> Result<bool> {
        let Some(existing) = existing_entry else {
            return Ok(false);
        };

        if existing.vip_mac != vip_mac {
            return Ok(false);
        }

        let expected_count = backends.len();
        if existing.backend_count as usize != expected_count {
            return Ok(false);
        }

        for (idx, backend) in backends.iter().enumerate() {
            let key = VipBackendKey6 {
                vip: vip_key.vip,
                slot: idx as u32,
                _pad: [0u8; 4],
            };
            let Some(existing_backend) = lookup_elem::<VipBackendKey6, Backend6>(backend_fd, &key)?
            else {
                return Ok(false);
            };
            if existing_backend != *backend {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Build the VIP metadata record written to `LB_VIPS`.
    fn build_vip_entry(vip_mac: [u8; 6], programmed_slots: usize) -> VipEntry {
        VipEntry {
            vip_mac,
            backend_count: programmed_slots as u16,
            ..VipEntry::default()
        }
    }

    /// Precompute a per-VIP backend lookup ring so dataplane selection is O(1) on cache miss.
    fn build_backend_lookup_ring_v4(
        backends: &[BackendAddress],
        max_slots: usize,
    ) -> Result<Vec<Backend>> {
        let candidates: Vec<Backend> = backends
            .iter()
            .take(max_slots)
            .map(backend_to_map_value_v4)
            .collect::<Result<Vec<_>>>()?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let slot_count = desired_lookup_slots(candidates.len(), max_slots);
        let backend_seeds: Vec<u64> = candidates
            .iter()
            .enumerate()
            .map(|(idx, backend)| backend_seed_v4(backend, idx as u64))
            .collect();

        let mut ring = Vec::with_capacity(slot_count);
        for slot in 0..slot_count {
            let slot_seed = mix64(slot as u64);
            let mut best_idx: usize = 0;
            let mut best_score: u64 = 0;
            let mut initialized = false;

            for (idx, backend_seed) in backend_seeds.iter().enumerate() {
                let score = mix64(*backend_seed ^ slot_seed);
                if !initialized || score > best_score {
                    initialized = true;
                    best_score = score;
                    best_idx = idx;
                }
            }

            ring.push(candidates[best_idx]);
        }

        Ok(ring)
    }

    /// Precompute the IPv6 backend ring stored in `LB_BACKENDS_V6`.
    ///
    /// The scoring logic stays the same as IPv4 so flow distribution is deterministic across the
    /// cluster regardless of address family.
    fn build_backend_lookup_ring_v6(
        backends: &[BackendAddress],
        max_slots: usize,
    ) -> Result<Vec<Backend6>> {
        let candidates: Vec<Backend6> = backends
            .iter()
            .take(max_slots)
            .map(backend_to_map_value_v6)
            .collect::<Result<Vec<_>>>()?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let slot_count = desired_lookup_slots(candidates.len(), max_slots);
        let backend_seeds: Vec<u64> = candidates
            .iter()
            .enumerate()
            .map(|(idx, backend)| backend_seed_v6(backend, idx as u64))
            .collect();

        let mut ring = Vec::with_capacity(slot_count);
        for slot in 0..slot_count {
            let slot_seed = mix64(slot as u64);
            let mut best_idx: usize = 0;
            let mut best_score: u64 = 0;
            let mut initialized = false;

            for (idx, backend_seed) in backend_seeds.iter().enumerate() {
                let score = mix64(*backend_seed ^ slot_seed);
                if !initialized || score > best_score {
                    initialized = true;
                    best_score = score;
                    best_idx = idx;
                }
            }

            ring.push(candidates[best_idx]);
        }

        Ok(ring)
    }

    /// Convert high-level IPv4 backend coordinates into the raw map value used by eBPF.
    fn backend_to_map_value_v4(backend: &BackendAddress) -> Result<Backend> {
        let ip = require_ipv4(backend.ip, "service backend")?;
        Ok(Backend {
            ip: u32::from_ne_bytes(ip.octets()),
            mac: backend.mac,
            _pad: 0,
        })
    }

    /// Convert high-level IPv6 backend coordinates into the raw map value used by eBPF.
    fn backend_to_map_value_v6(backend: &BackendAddress) -> Result<Backend6> {
        let ip = require_ipv6(backend.ip, "service backend")?;
        Ok(Backend6 {
            ip: ip.octets(),
            mac: backend.mac,
            _pad: [0u8; 2],
        })
    }

    /// Require IPv4 coordinates before programming the IPv4 dataplane maps.
    fn require_ipv4(ip: IpAddr, context: &str) -> Result<Ipv4Addr> {
        match ip {
            IpAddr::V4(ip) => Ok(ip),
            IpAddr::V6(ip) => anyhow::bail!("{context} {ip} requires IPv6 dataplane support"),
        }
    }

    /// Require IPv6 coordinates before programming the IPv6 dataplane maps.
    fn require_ipv6(ip: IpAddr, context: &str) -> Result<Ipv6Addr> {
        match ip {
            IpAddr::V4(ip) => anyhow::bail!("{context} {ip} requires IPv4 dataplane support"),
            IpAddr::V6(ip) => Ok(ip),
        }
    }

    /// Determine the number of ring slots to precompute for the provided backend cardinality.
    fn desired_lookup_slots(backend_count: usize, max_slots: usize) -> usize {
        if backend_count == 0 || max_slots == 0 {
            return 0;
        }

        let scaled = backend_count.saturating_mul(8);
        let mut slots = scaled.next_power_of_two();
        if slots > max_slots {
            slots = max_slots;
        }
        if slots < backend_count {
            slots = backend_count;
        }
        slots
    }

    /// Build a stable seed per IPv4 backend so ring construction is deterministic across refreshes.
    fn backend_seed_v4(backend: &Backend, ordinal: u64) -> u64 {
        let mut mac_bits = 0u64;
        for byte in backend.mac {
            mac_bits = (mac_bits << 8) | (byte as u64);
        }
        mix64((backend.ip as u64) ^ (mac_bits << 7) ^ (ordinal << 33))
    }

    /// Build a stable seed per IPv6 backend so ring construction is deterministic across refreshes.
    fn backend_seed_v6(backend: &Backend6, ordinal: u64) -> u64 {
        let mut ip_mix = 0u64;
        for chunk in backend.ip.chunks_exact(8) {
            let mut chunk_bytes = [0u8; 8];
            chunk_bytes.copy_from_slice(chunk);
            ip_mix ^= mix64(u64::from_be_bytes(chunk_bytes));
        }
        let mut mac_bits = 0u64;
        for byte in backend.mac {
            mac_bits = (mac_bits << 8) | (byte as u64);
        }
        mix64(ip_mix ^ (mac_bits << 7) ^ (ordinal << 33))
    }

    /// Apply a lightweight 64-bit mix for rendezvous-style slot scoring.
    fn mix64(mut x: u64) -> u64 {
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51afd7ed558ccd);
        x ^= x >> 33;
        x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
        x ^= x >> 33;
        x
    }

    /// Persist the IPv4 backend ring for one VIP.
    fn program_backends_v4(fd: std::os::fd::RawFd, vip: u32, backends: &[Backend]) -> Result<()> {
        for (idx, backend) in backends.iter().enumerate() {
            let key = VipBackendKey {
                vip,
                slot: idx as u32,
            };
            update_elem(fd, &key, backend)
                .with_context(|| format!("update backend slot {} for vip {:08x}", idx, vip))?;
        }

        Ok(())
    }

    /// Persist the IPv6 backend ring for one VIP.
    fn program_backends_v6(
        fd: std::os::fd::RawFd,
        vip: [u8; 16],
        backends: &[Backend6],
    ) -> Result<()> {
        for (idx, backend) in backends.iter().enumerate() {
            let key = VipBackendKey6 {
                vip,
                slot: idx as u32,
                _pad: [0u8; 4],
            };
            update_elem(fd, &key, backend).with_context(|| {
                format!(
                    "update backend slot {} for vip {}",
                    idx,
                    Ipv6Addr::from(vip)
                )
            })?;
        }

        Ok(())
    }

    /// Remove stale backend slots that were part of a previous ring generation.
    fn trim_vip_backends_v4(
        fd: std::os::fd::RawFd,
        vip: u32,
        keep_slots: usize,
        previous_slots: usize,
    ) -> Result<()> {
        if previous_slots <= keep_slots {
            return Ok(());
        }

        for idx in keep_slots..previous_slots {
            let key = VipBackendKey {
                vip,
                slot: idx as u32,
            };
            delete_elem(fd, &key).with_context(|| {
                format!("delete stale backend slot {} for vip {:08x}", idx, vip)
            })?;
        }

        Ok(())
    }

    /// Remove stale IPv6 backend slots that were part of a previous ring generation.
    fn trim_vip_backends_v6(
        fd: std::os::fd::RawFd,
        vip: [u8; 16],
        keep_slots: usize,
        previous_slots: usize,
    ) -> Result<()> {
        if previous_slots <= keep_slots {
            return Ok(());
        }

        for idx in keep_slots..previous_slots {
            let key = VipBackendKey6 {
                vip,
                slot: idx as u32,
                _pad: [0u8; 4],
            };
            delete_elem(fd, &key).with_context(|| {
                format!(
                    "delete stale backend slot {} for vip {}",
                    idx,
                    Ipv6Addr::from(vip)
                )
            })?;
        }

        Ok(())
    }

    /// Clear cached flow mappings that still point to a VIP whose backend set has changed.
    ///
    /// This resets sticky selections so new packets select from the updated backend list.
    fn clear_vip_flows_v4(base: &Path, vip: u32) -> Result<()> {
        if let Ok(fwd_map) = open_map(base, IPV4_MAPS.forward_map) {
            let _ = clear_vip_forward_flows_v4(fwd_map.fd().as_fd().as_raw_fd(), vip);
        }
        if let Ok(rev_map) = open_map(base, IPV4_MAPS.reverse_map) {
            let _ = clear_vip_reverse_flows_v4(rev_map.fd().as_fd().as_raw_fd(), vip);
        }
        Ok(())
    }

    /// Clear cached IPv6 flow mappings that still point to a VIP whose backend set changed.
    fn clear_vip_flows_v6(base: &Path, vip: [u8; 16]) -> Result<()> {
        if let Ok(fwd_map) = open_map(base, IPV6_MAPS.forward_map) {
            let _ = clear_vip_forward_flows_v6(fwd_map.fd().as_fd().as_raw_fd(), vip);
        }
        if let Ok(rev_map) = open_map(base, IPV6_MAPS.reverse_map) {
            let _ = clear_vip_reverse_flows_v6(rev_map.fd().as_fd().as_raw_fd(), vip);
        }
        Ok(())
    }

    /// Aggregate one network's pinned load-balancer state into the node-wide diagnostics snapshot.
    fn accumulate_network_status(
        base: &Path,
        status: &mut LoadBalancerStatus,
        flow_diagnostics: &mut LoadBalancerFlowDiagnostics,
    ) -> Result<()> {
        status.ipv4_vips += count_network_map_entries::<VipKey>(base, IPV4_MAPS.vip_map)?;
        status.ipv6_vips += count_network_map_entries::<VipKey6>(base, IPV6_MAPS.vip_map)?;
        flow_diagnostics.ipv4_flow_pairs +=
            count_network_map_entries::<Flow4>(base, IPV4_MAPS.forward_map)?;
        flow_diagnostics.ipv6_flow_pairs +=
            count_network_map_entries::<Flow6>(base, IPV6_MAPS.forward_map)?;
        Ok(())
    }

    /// Return every per-network bpffs directory that can carry one overlay load-balancer map set.
    fn load_balancer_network_dirs() -> Result<Vec<std::path::PathBuf>> {
        let root = map_root_dir()?;
        let mut dirs = Vec::new();

        for entry in fs::read_dir(&root)
            .with_context(|| format!("read overlay load-balancer root {}", root.display()))?
        {
            let entry = entry.with_context(|| format!("read entry from {}", root.display()))?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if Uuid::parse_str(name).is_ok() {
                dirs.push(path);
            }
        }

        dirs.sort();
        Ok(dirs)
    }

    /// Return whether one network directory currently pins any overlay load-balancer map family.
    fn network_has_lb_maps(base: &Path) -> bool {
        scoped_map_path(base, IPV4_MAPS.vip_map).is_some()
            || scoped_map_path(base, IPV4_MAPS.forward_map).is_some()
            || scoped_map_path(base, IPV6_MAPS.vip_map).is_some()
            || scoped_map_path(base, IPV6_MAPS.forward_map).is_some()
    }

    /// Return whether one pinned network directory still belongs to a live local overlay dataplane.
    ///
    /// The status reader aggregates bpffs directories node-wide, so it must ignore stale UUID
    /// directories left behind after manual cleanup or interrupted teardown. A network counts as
    /// live only when at least one of its deterministic local interfaces is still present.
    fn network_has_live_dataplane_interfaces(base: &Path) -> bool {
        network_has_live_dataplane_interfaces_in(base, Path::new("/sys/class/net"))
    }

    /// Check whether a pinned network directory still has any of its expected local interfaces
    /// under the provided net-class root.
    ///
    /// The helper takes the net-class root explicitly so unit tests can exercise stale-directory
    /// filtering without touching the real host interface namespace.
    fn network_has_live_dataplane_interfaces_in(base: &Path, net_class_root: &Path) -> bool {
        let Some(network_id) = network_id_from_map_dir(base) else {
            return false;
        };

        expected_local_interface_names(network_id)
            .into_iter()
            .any(|iface| net_class_root.join(iface).exists())
    }

    /// Parse the network UUID from one pinned bpffs network directory name.
    ///
    /// The load-balancer status root uses the network UUID string as the stable per-network
    /// directory name, so invalid names can be discarded immediately.
    fn network_id_from_map_dir(base: &Path) -> Option<Uuid> {
        base.file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| Uuid::parse_str(name).ok())
    }

    /// Build every deterministic local interface name that can prove a network is still live.
    ///
    /// Any one of these interfaces is enough to show the overlay dataplane still exists locally,
    /// even if other pieces are mid-reconcile or mid-teardown.
    fn expected_local_interface_names(network_id: Uuid) -> [String; 4] {
        [
            bridge_name(network_id),
            host_access_host_iface_name(network_id),
            host_access_peer_iface_name(network_id),
            vxlan_name(network_id),
        ]
    }

    /// Iterate every key in one BPF map so callers can delete stale entries in place.
    fn visit_map_keys<K, F>(fd: std::os::fd::RawFd, mut visitor: F) -> Result<()>
    where
        K: Pod + Copy + Default,
        F: FnMut(K) -> Result<bool>,
    {
        #[repr(C)]
        struct BpfAttrKeyIter {
            map_fd: u32,
            _pad: u32,
            key: u64,
            next_key: u64,
        }

        let mut cursor: Option<K> = None;
        loop {
            let mut next: K = K::default();
            let mut iter = BpfAttrKeyIter {
                map_fd: fd as u32,
                _pad: 0,
                key: cursor.as_ref().map(|k| k as *const _ as u64).unwrap_or(0),
                next_key: &mut next as *mut _ as u64,
            };

            let ret = unsafe {
                libc::syscall(
                    libc::SYS_bpf,
                    BPF_MAP_GET_NEXT_KEY,
                    &mut iter as *mut _,
                    mem::size_of::<BpfAttrKeyIter>(),
                )
            };

            if ret < 0 {
                break;
            }

            if visitor(next)? {
                let _ = delete_elem(fd, &next);
                cursor = None;
            } else {
                cursor = Some(next);
            }
        }

        Ok(())
    }

    /// Count the number of live entries in one pinned BPF map by walking its keys through the
    /// kernel `get_next_key` interface.
    fn count_map_keys<K>(fd: std::os::fd::RawFd) -> Result<usize>
    where
        K: Pod + Copy + Default,
    {
        let mut count = 0usize;
        visit_map_keys::<K, _>(fd, |_| {
            count += 1;
            Ok(false)
        })?;
        Ok(count)
    }

    /// Remove forward IPv4 flow entries whose destination matches the specified VIP.
    fn clear_vip_forward_flows_v4(fd: std::os::fd::RawFd, vip: u32) -> Result<()> {
        visit_map_keys::<Flow4, _>(fd, |next| Ok(next.dst == vip))
    }

    /// Remove reverse flow entries whose cached VIP matches the specified VIP.
    fn clear_vip_reverse_flows_v4(fd: std::os::fd::RawFd, vip: u32) -> Result<()> {
        visit_map_keys::<Flow4, _>(fd, |next| {
            let matches_vip = lookup_elem::<Flow4, NatEntry>(fd, &next)?
                .map(|entry| entry.vip == vip)
                .unwrap_or(false);
            Ok(matches_vip)
        })
    }

    /// Remove forward IPv6 flow entries whose destination matches the specified VIP.
    fn clear_vip_forward_flows_v6(fd: std::os::fd::RawFd, vip: [u8; 16]) -> Result<()> {
        visit_map_keys::<Flow6, _>(fd, |next| Ok(next.dst == vip))
    }

    /// Remove reverse IPv6 flow entries whose cached VIP matches the specified VIP.
    fn clear_vip_reverse_flows_v6(fd: std::os::fd::RawFd, vip: [u8; 16]) -> Result<()> {
        visit_map_keys::<Flow6, _>(fd, |next| {
            let matches_vip = lookup_elem::<Flow6, NatEntry6>(fd, &next)?
                .map(|entry| entry.vip == vip)
                .unwrap_or(false);
            Ok(matches_vip)
        })
    }

    /// Return the shared bpffs root that stores one subdirectory per overlay network.
    fn map_root_dir() -> Result<std::path::PathBuf> {
        ensure_bpffs().context("prepare bpffs mount")?;
        let path = std::path::PathBuf::from("/sys/fs/bpf/mantissa");
        fs::create_dir_all(&path)
            .with_context(|| format!("create load-balancer map root {}", path.display()))?;
        Ok(path)
    }

    /// Return and create the per-network pinned map directory.
    fn map_pin_dir(network_id: Uuid) -> Result<std::path::PathBuf> {
        let path = map_root_dir()?.join(network_id.to_string());
        fs::create_dir_all(&path)
            .with_context(|| format!("create map pin directory {}", path.display()))?;
        Ok(path)
    }

    /// Upsert one key/value pair into a pinned BPF map through a raw bpf syscall.
    fn update_elem<K: Pod, V: Pod>(fd: std::os::fd::RawFd, key: &K, val: &V) -> Result<()> {
        #[repr(C)]
        struct BpfAttrUpsert {
            map_fd: u32,
            _pad: u32,
            key: u64,
            value: u64,
            flags: u64,
        }

        let mut attr = BpfAttrUpsert {
            map_fd: fd as u32,
            _pad: 0,
            key: key as *const _ as u64,
            value: val as *const _ as u64,
            flags: 0,
        };

        let ret = unsafe {
            libc::syscall(
                libc::SYS_bpf,
                BPF_MAP_UPDATE_ELEM,
                &mut attr as *mut _,
                mem::size_of::<BpfAttrUpsert>(),
            )
        };
        if ret < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }

    /// Look up an element from a pinned BPF map by key, returning None if it is absent.
    fn lookup_elem<K: Pod, V: Pod + Default>(fd: std::os::fd::RawFd, key: &K) -> Result<Option<V>> {
        #[repr(C)]
        struct BpfAttrLookup {
            map_fd: u32,
            _pad: u32,
            key: u64,
            value: u64,
        }

        let mut value = V::default();
        let mut attr = BpfAttrLookup {
            map_fd: fd as u32,
            _pad: 0,
            key: key as *const _ as u64,
            value: &mut value as *mut _ as u64,
        };

        let ret = unsafe {
            libc::syscall(
                libc::SYS_bpf,
                BPF_MAP_LOOKUP_ELEM,
                &mut attr as *mut _,
                mem::size_of::<BpfAttrLookup>(),
            )
        };

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Ok(None);
            }
            return Err(err.into());
        }

        Ok(Some(value))
    }

    /// Delete an element from a pinned BPF map by key.
    fn delete_elem<K: Pod>(fd: std::os::fd::RawFd, key: &K) -> Result<()> {
        #[repr(C)]
        struct BpfAttrDelete {
            map_fd: u32,
            _pad: u32,
            key: u64,
        }

        let mut del = BpfAttrDelete {
            map_fd: fd as u32,
            _pad: 0,
            key: key as *const _ as u64,
        };

        let ret = unsafe {
            libc::syscall(
                libc::SYS_bpf,
                BPF_MAP_DELETE_ELEM,
                &mut del as *mut _,
                mem::size_of::<BpfAttrDelete>(),
            )
        };

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Ok(());
            }
            return Err(err.into());
        }
        Ok(())
    }

    /// Resolve one network-scoped pinned map path without falling back to the global tc namespace.
    ///
    /// Node-wide diagnostics aggregate every network directory independently, so they must avoid
    /// the shared `tc/globals` fallback or the same map could be counted multiple times.
    fn scoped_map_path(base: &Path, name: &str) -> Option<std::path::PathBuf> {
        [base.join(name), base.join("tc").join("globals").join(name)]
            .into_iter()
            .find(|candidate| candidate.exists())
    }

    /// Open one network-scoped pinned map if it exists for the requested network directory.
    fn open_network_map(base: &Path, name: &str) -> Result<Option<MapData>> {
        let Some(path) = scoped_map_path(base, name) else {
            return Ok(None);
        };
        Ok(Some(MapData::from_pin(&path).with_context(|| {
            format!("open pinned load-balancer map {}", path.display())
        })?))
    }

    /// Count the number of live entries in one network-scoped pinned map, returning zero when the
    /// requested family is not present for that network.
    fn count_network_map_entries<K>(base: &Path, name: &str) -> Result<usize>
    where
        K: Pod + Copy + Default,
    {
        let Some(map) = open_network_map(base, name)? else {
            return Ok(0);
        };
        count_map_keys::<K>(map.fd().as_fd().as_raw_fd())
    }

    /// Try to open a pinned map from the expected mantissa directory, falling back to the tc/globals
    /// location Aya may use for TC programs on some kernels.
    fn open_map(base: &Path, name: &str) -> Result<MapData> {
        let candidates = [
            base.join(name),
            base.join("tc").join("globals").join(name),
            Path::new("/sys/fs/bpf/tc/globals").join(name),
        ];

        for candidate in candidates {
            if let Ok(map) = MapData::from_pin(&candidate) {
                return Ok(map);
            }
        }

        Err(anyhow::anyhow!(
            "map {name} not found in expected pin locations"
        ))
    }

    /// Ensure bpffs is mounted at /sys/fs/bpf so TC maps can be pinned predictably.
    fn ensure_bpffs() -> Result<()> {
        let mountpoint = Path::new("/sys/fs/bpf");
        if !mountpoint.exists() {
            fs::create_dir_all(mountpoint).context("create /sys/fs/bpf")?;
        }

        if is_bpffs(mountpoint) {
            return Ok(());
        }

        mount::<Path, Path, str, str>(
            None::<&Path>,
            mountpoint,
            Some("bpf"),
            MsFlags::empty(),
            None::<&str>,
        )
        .context("mount bpffs")?;
        Ok(())
    }

    /// Lightweight check to see if a path is a bpffs mount.
    fn is_bpffs(path: &Path) -> bool {
        matches!(statfs(path), Ok(stat) if stat.filesystem_type() == BPF_FS_MAGIC)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::collections::BTreeSet;
        use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
        use tempfile::tempdir;

        /// Ensure split flow counts still add up to the total operator-visible flow occupancy.
        #[test]
        fn load_balancer_flow_diagnostics_split_by_family() {
            let diagnostics = LoadBalancerFlowDiagnostics {
                ipv4_flow_pairs: 3,
                ipv6_flow_pairs: 5,
            };

            assert_eq!(diagnostics.ipv4_flow_pairs + diagnostics.ipv6_flow_pairs, 8);
        }

        /// Build a synthetic backend value for deterministic ring-construction tests.
        fn synthetic_backend(index: usize) -> BackendAddress {
            BackendAddress {
                ip: IpAddr::V4(Ipv4Addr::from(0x0a00_0001u32 + (index as u32))),
                mac: [
                    0x02,
                    ((index >> 16) & 0xff) as u8,
                    ((index >> 8) & 0xff) as u8,
                    (index & 0xff) as u8,
                    0xaa,
                    0x55,
                ],
            }
        }

        /// Build a synthetic IPv6 backend for deterministic ring-construction tests.
        fn synthetic_backend_v6(index: usize) -> BackendAddress {
            BackendAddress {
                ip: IpAddr::V6(Ipv6Addr::from(
                    0xfd42_0000_0000_0000u128 + index as u128 + 1,
                )),
                mac: [
                    0x02,
                    ((index >> 16) & 0xff) as u8,
                    ((index >> 8) & 0xff) as u8,
                    (index & 0xff) as u8,
                    0xbb,
                    0x66,
                ],
            }
        }

        /// Ensure ring precomputation is stable when inputs are unchanged.
        #[test]
        fn backend_lookup_ring_is_deterministic() {
            let backends: Vec<BackendAddress> = (0..16).map(synthetic_backend).collect();
            let ring_a = build_backend_lookup_ring_v4(&backends, MAX_BACKENDS_PER_VIP)
                .expect("build ring a");
            let ring_b = build_backend_lookup_ring_v4(&backends, MAX_BACKENDS_PER_VIP)
                .expect("build ring b");
            assert_eq!(ring_a, ring_b);
            assert!(!ring_a.is_empty());
        }

        /// Ensure ring precomputation never exceeds the per-VIP programming cap.
        #[test]
        fn backend_lookup_ring_respects_cap() {
            let over_cap = MAX_BACKENDS_PER_VIP + 128;
            let backends: Vec<BackendAddress> = (0..over_cap).map(synthetic_backend).collect();
            let ring =
                build_backend_lookup_ring_v4(&backends, MAX_BACKENDS_PER_VIP).expect("build ring");
            assert!(ring.len() <= MAX_BACKENDS_PER_VIP);
        }

        /// Ensure all ring slots map to one of the admitted backend candidates.
        #[test]
        fn backend_lookup_ring_uses_only_admitted_backends() {
            let backends: Vec<BackendAddress> = (0..12).map(synthetic_backend).collect();
            let ring =
                build_backend_lookup_ring_v4(&backends, MAX_BACKENDS_PER_VIP).expect("build ring");
            let admitted: BTreeSet<(u32, [u8; 6])> = backends
                .iter()
                .map(backend_to_map_value_v4)
                .collect::<Result<Vec<_>>>()
                .expect("convert admitted backends")
                .into_iter()
                .map(|backend| (backend.ip, backend.mac))
                .collect();

            assert!(!ring.is_empty());
            assert!(
                ring.iter()
                    .all(|backend| { admitted.contains(&(backend.ip, backend.mac)) })
            );
        }

        /// Ensure IPv6 ring precomputation is stable when inputs are unchanged.
        #[test]
        fn backend_lookup_ring_v6_is_deterministic() {
            let backends: Vec<BackendAddress> = (0..16).map(synthetic_backend_v6).collect();
            let ring_a = build_backend_lookup_ring_v6(&backends, MAX_BACKENDS_PER_VIP)
                .expect("build IPv6 ring a");
            let ring_b = build_backend_lookup_ring_v6(&backends, MAX_BACKENDS_PER_VIP)
                .expect("build IPv6 ring b");
            assert_eq!(ring_a, ring_b);
            assert!(!ring_a.is_empty());
        }

        /// Ensure stale bpffs directories do not count as live overlay dataplane networks.
        #[test]
        fn stale_network_directory_requires_live_local_interfaces() {
            let root = tempdir().expect("create root tempdir");
            let net_class_root = tempdir().expect("create net-class tempdir");
            let network_id = Uuid::new_v4();
            let network_dir = root.path().join(network_id.to_string());
            fs::create_dir(&network_dir).expect("create network dir");

            assert!(!network_has_live_dataplane_interfaces_in(
                &network_dir,
                net_class_root.path()
            ));
        }

        /// Ensure any deterministic local dataplane interface marks the bpffs directory as live.
        #[test]
        fn live_network_directory_accepts_expected_local_interface() {
            let root = tempdir().expect("create root tempdir");
            let net_class_root = tempdir().expect("create net-class tempdir");
            let network_id = Uuid::new_v4();
            let network_dir = root.path().join(network_id.to_string());
            fs::create_dir(&network_dir).expect("create network dir");
            fs::create_dir(net_class_root.path().join(bridge_name(network_id)))
                .expect("create bridge iface entry");

            assert!(network_has_live_dataplane_interfaces_in(
                &network_dir,
                net_class_root.path()
            ));
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::{BackendAddress, LoadBalancerStatus};
    use anyhow::Result;
    use std::net::IpAddr;
    use uuid::Uuid;

    #[derive(Clone, Default)]
    pub struct PlatformBpfLoadBalancer;

    impl PlatformBpfLoadBalancer {
        /// Build the no-op load balancer used on unsupported platforms.
        pub fn new() -> Self {
            Self
        }

        /// Ignore VIP syncs on unsupported platforms so discovery can fall back to DNS records.
        pub fn sync_vip(
            &self,
            _network_id: Uuid,
            _vip: IpAddr,
            _vip_mac: [u8; 6],
            _backends: &[BackendAddress],
        ) -> Result<()> {
            Ok(())
        }

        /// Return a disabled overlay load-balancer snapshot on unsupported platforms.
        pub fn status(&self) -> LoadBalancerStatus {
            LoadBalancerStatus {
                desired_enabled: false,
                flow_capacity: crate::config::bpf_overlay_flow_capacity(),
                stats_error: Some("overlay load balancer is only available on linux".to_string()),
                ..LoadBalancerStatus::default()
            }
        }
    }
}

#[cfg(target_os = "linux")]
use platform::PlatformBpfLoadBalancer;
#[cfg(not(target_os = "linux"))]
use platform::PlatformBpfLoadBalancer;
