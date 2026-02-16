use std::net::Ipv4Addr;
use uuid::Uuid;

/// Backend coordinates used when populating dataplane maps.
#[derive(Clone, Debug)]
pub struct BackendAddress {
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
}

/// Platform abstraction that either programs real eBPF maps (Linux) or acts as a no-op shim on
/// unsupported hosts so higher layers can remain platform-agnostic.
#[derive(Clone, Default)]
pub struct BpfLoadBalancer {
    inner: PlatformBpfLoadBalancer,
}

impl BpfLoadBalancer {
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
        vip: Ipv4Addr,
        vip_mac: [u8; 6],
        backends: &[BackendAddress],
    ) -> anyhow::Result<()> {
        self.inner.sync_vip(network_id, vip, vip_mac, backends)
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::BackendAddress;
    use anyhow::{Context, Result};
    use aya::Pod;
    use aya::maps::MapData;
    use nix::mount::{MsFlags, mount};
    use nix::sys::statfs::{BPF_FS_MAGIC, statfs};
    use std::fs;
    use std::mem;
    use std::net::Ipv4Addr;
    use std::os::fd::{AsFd, AsRawFd};
    use std::path::Path;
    use tracing::warn;
    use uuid::Uuid;

    #[derive(Clone, Default)]
    pub struct PlatformBpfLoadBalancer;

    impl PlatformBpfLoadBalancer {
        pub fn new() -> Self {
            Self
        }

        pub fn sync_vip(
            &self,
            network_id: Uuid,
            vip: Ipv4Addr,
            vip_mac: [u8; 6],
            backends: &[BackendAddress],
        ) -> Result<()> {
            let base = map_pin_dir(network_id)?;
            let vip_map = open_map(&base, "LB_VIPS").context("open LB_VIPS map")?;
            let backend_map = open_map(&base, "LB_BACKENDS").context("open LB_BACKENDS map")?;
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
                // The eBPF programs read IPv4 fields directly from packet memory without endian
                // conversion, meaning the stored `u32` value must match the host-native
                // interpretation of the network bytes. Using `from_ne_bytes` keeps the in-memory
                // representation consistent for lookups and for rewriting packet headers.
                vip: u32::from_ne_bytes(vip.octets()),
            };
            let desired_ring = build_backend_lookup_ring(backends, MAX_BACKENDS_PER_VIP);
            let programmed_slots = desired_ring.len();
            let existing_entry =
                lookup_elem::<VipKey, VipEntry>(vip_map.fd().as_fd().as_raw_fd(), &key)
                    .context("lookup existing VIP metadata")?;
            let previous_slots = existing_entry
                .map(|entry| entry.backend_count as usize)
                .unwrap_or(0);

            let mut clear_flows = programmed_slots == 0 && previous_slots > 0;
            if programmed_slots > 0 {
                clear_flows = match backend_state_matches(
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

            program_backends(backend_map.fd().as_fd().as_raw_fd(), key.vip, &desired_ring)
                .context("program backends")?;
            let entry = build_vip_entry(vip_mac, programmed_slots);
            update_elem(vip_map.fd().as_fd().as_raw_fd(), &key, &entry)
                .context("update VIP metadata")?;
            trim_vip_backends(
                backend_map.fd().as_fd().as_raw_fd(),
                key.vip,
                programmed_slots,
                previous_slots,
            )
            .context("trim stale backend slots")?;

            if clear_flows {
                let _ = clear_vip_flows(&base, key.vip);
            }
            Ok(())
        }
    }

    /// Maximum backends programmed per VIP in eBPF maps.
    ///
    /// Keep this in sync with `network_ebpf::lb::MAX_BACKENDS_PER_VIP`.
    const MAX_BACKENDS_PER_VIP: usize = 1024;
    const BPF_MAP_UPDATE_ELEM: libc::c_uint = 2;
    const BPF_MAP_DELETE_ELEM: libc::c_uint = 3;
    const BPF_MAP_GET_NEXT_KEY: libc::c_uint = 4;
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

    #[repr(C, packed)]
    #[derive(Clone, Copy, Default)]
    struct NatEntry {
        vip: u32,
        vip_mac: [u8; 6],
        backend_ip: u32,
        backend_mac: [u8; 6],
    }
    unsafe impl Pod for NatEntry {}

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
    fn backend_state_matches(
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

    /// Build the VIP metadata record written to `LB_VIPS`.
    fn build_vip_entry(vip_mac: [u8; 6], programmed_slots: usize) -> VipEntry {
        VipEntry {
            vip_mac,
            backend_count: programmed_slots as u16,
            ..VipEntry::default()
        }
    }

    /// Precompute a per-VIP backend lookup ring so dataplane selection is O(1) on cache miss.
    fn build_backend_lookup_ring(backends: &[BackendAddress], max_slots: usize) -> Vec<Backend> {
        let candidates: Vec<Backend> = backends
            .iter()
            .take(max_slots)
            .map(backend_to_map_value)
            .collect();
        if candidates.is_empty() {
            return Vec::new();
        }

        let slot_count = desired_lookup_slots(candidates.len(), max_slots);
        let backend_seeds: Vec<u64> = candidates
            .iter()
            .enumerate()
            .map(|(idx, backend)| backend_seed(backend, idx as u64))
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

        ring
    }

    /// Convert high-level backend coordinates into the raw value programmed into eBPF maps.
    fn backend_to_map_value(backend: &BackendAddress) -> Backend {
        Backend {
            // Keep the backend IP representation consistent with how the eBPF programs read
            // and write IPv4 header fields (native-endian `u32` matching network bytes).
            ip: u32::from_ne_bytes(backend.ip.octets()),
            mac: backend.mac,
            _pad: 0,
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

    /// Build a stable seed per backend so ring construction is deterministic across refreshes.
    fn backend_seed(backend: &Backend, ordinal: u64) -> u64 {
        let mut mac_bits = 0u64;
        for byte in backend.mac {
            mac_bits = (mac_bits << 8) | (byte as u64);
        }
        mix64((backend.ip as u64) ^ (mac_bits << 7) ^ (ordinal << 33))
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

    fn program_backends(fd: std::os::fd::RawFd, vip: u32, backends: &[Backend]) -> Result<()> {
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

    /// Remove stale backend slots that were part of a previous ring generation.
    fn trim_vip_backends(
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

    /// Clear cached flow mappings that still point to a VIP whose backend set has changed.
    ///
    /// This resets sticky selections so new packets select from the updated backend list.
    fn clear_vip_flows(base: &Path, vip: u32) -> Result<()> {
        if let Ok(fwd_map) = open_map(base, "LB_FWD") {
            let _ = clear_vip_forward_flows(fwd_map.fd().as_fd().as_raw_fd(), vip);
        }
        if let Ok(rev_map) = open_map(base, "LB_REV") {
            let _ = clear_vip_reverse_flows(rev_map.fd().as_fd().as_raw_fd(), vip);
        }
        Ok(())
    }

    /// Remove forward flow entries whose destination matches the specified VIP.
    fn clear_vip_forward_flows(fd: std::os::fd::RawFd, vip: u32) -> Result<()> {
        #[repr(C)]
        struct BpfAttrKeyIter {
            map_fd: u32,
            _pad: u32,
            key: u64,
            next_key: u64,
        }

        let mut cursor: Option<Flow4> = None;
        loop {
            let mut next: Flow4 = Flow4::default();
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

            if next.dst == vip {
                let _ = delete_elem(fd, &next);
                cursor = None;
            } else {
                cursor = Some(next);
            }
        }

        Ok(())
    }

    /// Remove reverse flow entries whose cached VIP matches the specified VIP.
    fn clear_vip_reverse_flows(fd: std::os::fd::RawFd, vip: u32) -> Result<()> {
        #[repr(C)]
        struct BpfAttrKeyIter {
            map_fd: u32,
            _pad: u32,
            key: u64,
            next_key: u64,
        }

        let mut cursor: Option<Flow4> = None;
        loop {
            let mut next: Flow4 = Flow4::default();
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

            let matches_vip = lookup_elem::<Flow4, NatEntry>(fd, &next)?
                .map(|entry| entry.vip == vip)
                .unwrap_or(false);

            if matches_vip {
                let _ = delete_elem(fd, &next);
                cursor = None;
            } else {
                cursor = Some(next);
            }
        }

        Ok(())
    }

    fn map_pin_dir(network_id: Uuid) -> Result<std::path::PathBuf> {
        ensure_bpffs().context("prepare bpffs mount")?;
        let path = std::path::PathBuf::from("/sys/fs/bpf/mantissa").join(network_id.to_string());
        fs::create_dir_all(&path)
            .with_context(|| format!("create map pin directory {}", path.display()))?;
        Ok(path)
    }

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
        use std::net::Ipv4Addr;

        /// Build a synthetic backend value for deterministic ring-construction tests.
        fn synthetic_backend(index: usize) -> BackendAddress {
            BackendAddress {
                ip: Ipv4Addr::from(0x0a00_0001u32 + (index as u32)),
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

        /// Ensure ring precomputation is stable when inputs are unchanged.
        #[test]
        fn backend_lookup_ring_is_deterministic() {
            let backends: Vec<BackendAddress> = (0..16).map(synthetic_backend).collect();
            let ring_a = build_backend_lookup_ring(&backends, MAX_BACKENDS_PER_VIP);
            let ring_b = build_backend_lookup_ring(&backends, MAX_BACKENDS_PER_VIP);
            assert_eq!(ring_a, ring_b);
            assert!(!ring_a.is_empty());
        }

        /// Ensure ring precomputation never exceeds the per-VIP programming cap.
        #[test]
        fn backend_lookup_ring_respects_cap() {
            let over_cap = MAX_BACKENDS_PER_VIP + 128;
            let backends: Vec<BackendAddress> = (0..over_cap).map(synthetic_backend).collect();
            let ring = build_backend_lookup_ring(&backends, MAX_BACKENDS_PER_VIP);
            assert!(ring.len() <= MAX_BACKENDS_PER_VIP);
        }

        /// Ensure all ring slots map to one of the admitted backend candidates.
        #[test]
        fn backend_lookup_ring_uses_only_admitted_backends() {
            let backends: Vec<BackendAddress> = (0..12).map(synthetic_backend).collect();
            let ring = build_backend_lookup_ring(&backends, MAX_BACKENDS_PER_VIP);
            let admitted: BTreeSet<(u32, [u8; 6])> = backends
                .iter()
                .map(backend_to_map_value)
                .map(|backend| (backend.ip, backend.mac))
                .collect();

            assert!(!ring.is_empty());
            assert!(
                ring.iter()
                    .all(|backend| { admitted.contains(&(backend.ip, backend.mac)) })
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::BackendAddress;
    use anyhow::Result;
    use std::net::Ipv4Addr;
    use uuid::Uuid;

    #[derive(Clone, Default)]
    pub struct PlatformBpfLoadBalancer;

    impl PlatformBpfLoadBalancer {
        pub fn new() -> Self {
            Self
        }

        pub fn sync_vip(
            &self,
            _network_id: Uuid,
            _vip: Ipv4Addr,
            _vip_mac: [u8; 6],
            _backends: &[BackendAddress],
        ) -> Result<()> {
            Ok(())
        }
    }
}

#[cfg(target_os = "linux")]
use platform::PlatformBpfLoadBalancer;
#[cfg(not(target_os = "linux"))]
use platform::PlatformBpfLoadBalancer;
