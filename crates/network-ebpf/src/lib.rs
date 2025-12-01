#![no_std]

pub mod stats {
    use aya_ebpf::maps::PerCpuArray;

    /// Basic packet counters exposed through per-cpu arrays so user space can aggregate stats.
    #[repr(C)]
    #[derive(Clone, Copy, Debug)]
    pub struct PacketStats {
        pub packets: u64,
        pub bytes: u64,
        pub drops: u64,
    }

    impl PacketStats {
        #[inline(always)]
        pub const fn zero() -> Self {
            Self {
                packets: 0,
                bytes: 0,
                drops: 0,
            }
        }
    }

    #[inline(always)]
    pub unsafe fn record_pass(map: *mut PerCpuArray<PacketStats>, len: usize) {
        update(map, len, false);
    }

    #[inline(always)]
    pub unsafe fn record_drop(map: *mut PerCpuArray<PacketStats>, len: usize) {
        update(map, len, true);
    }

    #[inline(always)]
    unsafe fn update(map: *mut PerCpuArray<PacketStats>, len: usize, dropped: bool) {
        let map_ref = &*map;
        if let Some(ptr) = map_ref.get_ptr_mut(0) {
            let stats = &mut *ptr;
            stats.packets += 1;
            stats.bytes += len as u64;
            if dropped {
                stats.drops += 1;
            }
        }
    }
}

pub mod net {
    use core::{mem, ptr};

    #[repr(C, packed)]
    #[derive(Clone, Copy)]
    pub struct EthernetHeader {
        pub dst: [u8; 6],
        pub src: [u8; 6],
        pub eth_proto: u16,
    }

    impl EthernetHeader {
        #[inline(always)]
        pub fn protocol(&self) -> u16 {
            u16::from_be(self.eth_proto)
        }

        #[inline(always)]
        pub fn source(&self) -> [u8; 6] {
            self.src
        }
    }

    #[repr(C, packed)]
    #[derive(Clone, Copy)]
    pub struct Ipv4Header {
        pub version_ihl: u8,
        pub tos: u8,
        pub tot_len: u16,
        pub id: u16,
        pub frag_off: u16,
        pub ttl: u8,
        pub protocol: u8,
        pub checksum: u16,
        pub src: u32,
        pub dst: u32,
    }

    impl Ipv4Header {
        #[inline(always)]
        pub fn version(&self) -> u8 {
            self.version_ihl >> 4
        }

        #[inline(always)]
        pub fn header_len(&self) -> usize {
            ((self.version_ihl & 0x0f) as usize) * 4
        }

        #[inline(always)]
        pub fn is_fragmented(&self) -> bool {
            (u16::from_be(self.frag_off) & 0x1fff) != 0
        }
    }

    #[repr(C, packed)]
    #[derive(Clone, Copy)]
    pub struct UdpHeader {
        pub source: u16,
        pub dest: u16,
        pub len: u16,
        pub check: u16,
    }

    impl UdpHeader {
        #[inline(always)]
        pub fn dest_port(&self) -> u16 {
            u16::from_be(self.dest)
        }
    }

    pub const ETH_HDR_LEN: usize = mem::size_of::<EthernetHeader>();

    #[inline(always)]
    pub fn frame_len(data: usize, data_end: usize) -> usize {
        data_end.saturating_sub(data)
    }

    #[inline(always)]
    pub unsafe fn read_at<T: Copy>(data: usize, data_end: usize, offset: usize) -> Result<T, ()> {
        let size = mem::size_of::<T>();
        if data + offset + size > data_end {
            return Err(());
        }
        let ptr = (data + offset) as *const T;
        Ok(ptr::read_unaligned(ptr))
    }

    #[inline(always)]
    pub fn is_unicast(mac: &[u8; 6]) -> bool {
        mac[0] & 1 == 0 && !is_zero(mac)
    }

    #[inline(always)]
    pub fn is_zero(mac: &[u8; 6]) -> bool {
        mac.iter().all(|&b| b == 0)
    }

    #[inline(always)]
    pub unsafe fn ptr_at<T>(data: usize, data_end: usize, offset: usize) -> Result<*const T, ()> {
        let size = mem::size_of::<T>();
        if data + offset + size > data_end {
            return Err(());
        }
        Ok((data + offset) as *const T)
    }

    #[inline(always)]
    pub unsafe fn mut_ptr_at<T>(data: usize, data_end: usize, offset: usize) -> Result<*mut T, ()> {
        let size = mem::size_of::<T>();
        if data + offset + size > data_end {
            return Err(());
        }
        Ok((data + offset) as *mut T)
    }
}

pub mod lb {
    /// Maximum number of backend targets tracked per VIP entry.
    pub const MAX_BACKENDS: usize = 255;
    /// Maximum number of VIPs tracked in LB maps.
    pub const MAX_VIPS: usize = 4096;

    /// Key for VIP-backed routing decisions stored in eBPF maps.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct VipKey {
        pub vip: u32,
    }

    /// L2/L3 coordinates for a backend attachment.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct Backend {
        pub ip: u32,
        pub mac: [u8; 6],
        pub _pad: u16,
    }

    /// VIP metadata used when selecting a target; actual backends are stored separately.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct VipEntry {
        pub vip_mac: [u8; 6],
        pub backend_count: u8,
        pub _pad: [u8; 3],
    }

    /// Composite key used to isolate backend slots per VIP inside a flat hash map so the backend
    /// table can grow without a small fixed slot limit.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct VipBackendKey {
        pub vip: u32,
        pub slot: u32,
    }

    /// Normalized 5-tuple used to maintain DNAT/SNAT state.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct Flow4 {
        pub src: u32,
        pub dst: u32,
        pub src_port: u16,
        pub dst_port: u16,
        pub proto: u8,
        pub pad: u8,
    }

    /// Cached per-flow translation data shared between ingress/egress hooks.
    #[repr(C, packed)]
    #[derive(Clone, Copy)]
    pub struct NatEntry {
        pub vip: u32,
        pub vip_mac: [u8; 6],
        pub backend_ip: u32,
        pub backend_mac: [u8; 6],
    }
}
