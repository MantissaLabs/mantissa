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
}
