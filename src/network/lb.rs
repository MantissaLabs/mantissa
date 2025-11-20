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
    use std::mem;
    use std::net::Ipv4Addr;
    use std::os::fd::{AsFd, AsRawFd};
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
            vip: Ipv4Addr,
            vip_mac: [u8; 6],
            backends: &[BackendAddress],
        ) -> Result<()> {
            let base = map_pin_dir(_network_id)?;
            let vip_map = MapData::from_pin(base.join("LB_VIPS")).context("open LB_VIPS map")?;

            let entry = build_vip_entry(vip_mac, backends);
            let key = VipKey {
                vip: u32::from_be_bytes(vip.octets()),
            };

            update_elem(vip_map.fd().as_fd().as_raw_fd(), &key, &entry)
                .context("update VIP metadata")?;

            clear_hash_map(base.join("LB_FWD")).context("clear LB_FWD map")?;
            clear_hash_map(base.join("LB_REV")).context("clear LB_REV map")?;
            Ok(())
        }
    }

    const MAX_BACKENDS: usize = 8;
    const BPF_MAP_UPDATE_ELEM: libc::c_uint = 2;
    const BPF_MAP_DELETE_ELEM: libc::c_uint = 3;
    const BPF_MAP_GET_NEXT_KEY: libc::c_uint = 4;

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct VipKey {
        vip: u32,
    }
    unsafe impl Pod for VipKey {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct Backend {
        ip: u32,
        mac: [u8; 6],
        _pad: u16,
    }
    unsafe impl Pod for Backend {}

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct VipEntry {
        vip_mac: [u8; 6],
        backend_count: u8,
        _pad: [u8; 3],
        backends: [Backend; MAX_BACKENDS],
    }
    unsafe impl Pod for VipEntry {}

    impl Default for VipEntry {
        fn default() -> Self {
            Self {
                vip_mac: [0; 6],
                backend_count: 0,
                _pad: [0; 3],
                backends: [Backend::default(); MAX_BACKENDS],
            }
        }
    }

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct Flow4 {
        src: u32,
        dst: u32,
        src_port: u16,
        dst_port: u16,
        proto: u8,
        _pad: u8,
    }
    unsafe impl Pod for Flow4 {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    #[allow(dead_code)]
    struct NatEntry {
        vip: u32,
        vip_mac: [u8; 6],
        backend_ip: u32,
        backend_mac: [u8; 6],
    }
    unsafe impl Pod for NatEntry {}

    fn build_vip_entry(vip_mac: [u8; 6], backends: &[BackendAddress]) -> VipEntry {
        let mut entry = VipEntry::default();
        entry.vip_mac = vip_mac;
        let limit = backends.len().min(MAX_BACKENDS);
        entry.backend_count = limit as u8;
        for (idx, backend) in backends.iter().take(limit).enumerate() {
            entry.backends[idx] = Backend {
                ip: u32::from_be_bytes(backend.ip.octets()),
                mac: backend.mac,
                _pad: 0,
            };
        }
        entry
    }

    fn map_pin_dir(network_id: Uuid) -> Result<std::path::PathBuf> {
        let path = std::path::PathBuf::from("/sys/fs/bpf/mantissa").join(network_id.to_string());
        std::fs::create_dir_all(&path)
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

    fn clear_hash_map(path: std::path::PathBuf) -> Result<()> {
        let map = MapData::from_pin(&path)?;
        let fd = map.fd().as_fd().as_raw_fd();

        #[repr(C)]
        struct BpfAttrKeyIter {
            map_fd: u32,
            _pad: u32,
            key: u64,
            next_key: u64,
        }

        #[repr(C)]
        struct BpfAttrDelete {
            map_fd: u32,
            _pad: u32,
            key: u64,
        }

        let mut prev: Option<Flow4> = None;
        loop {
            let mut next: Flow4 = Flow4::default();
            let mut iter = BpfAttrKeyIter {
                map_fd: fd as u32,
                _pad: 0,
                key: prev.as_ref().map(|k| k as *const _ as u64).unwrap_or(0),
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

            let mut del = BpfAttrDelete {
                map_fd: fd as u32,
                _pad: 0,
                key: &next as *const _ as u64,
            };
            let _ = unsafe {
                libc::syscall(
                    libc::SYS_bpf,
                    BPF_MAP_DELETE_ELEM,
                    &mut del as *mut _,
                    mem::size_of::<BpfAttrDelete>(),
                )
            };
            prev = Some(next);
        }
        Ok(())
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
