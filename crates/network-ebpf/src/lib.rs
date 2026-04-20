#![cfg_attr(test, allow(clippy::unwrap_used))]
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
    /// Record a passed packet into the per-CPU stats map.
    ///
    /// # Safety
    /// Caller must pass a valid pointer to a `PerCpuArray<PacketStats>` with at least one slot and
    /// obey eBPF verifier rules for concurrent access.
    pub unsafe fn record_pass(map: *mut PerCpuArray<PacketStats>, len: usize) {
        update(map, len, false);
    }

    #[inline(always)]
    /// Record a dropped packet into the per-CPU stats map.
    ///
    /// # Safety
    /// Caller must pass a valid pointer to a `PerCpuArray<PacketStats>` with at least one slot and
    /// obey eBPF verifier rules for concurrent access.
    pub unsafe fn record_drop(map: *mut PerCpuArray<PacketStats>, len: usize) {
        update(map, len, true);
    }

    #[inline(always)]
    /// Increment one reason counter in a per-CPU `u64` array.
    ///
    /// # Safety
    /// Caller must pass a valid pointer to a `PerCpuArray<u64>` and a valid in-bounds index.
    pub unsafe fn increment_reason(map: *mut PerCpuArray<u64>, index: u32) {
        let map_ref = &*map;
        if let Some(ptr) = map_ref.get_ptr_mut(index) {
            let counter = &mut *ptr;
            *counter += 1;
        }
    }

    #[inline(always)]
    /// Shared counter update helper.
    ///
    /// # Safety
    /// `map` must be a valid pointer to a per-CPU stats array.
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

    pub const TCP_FLAG_FIN: u8 = 0x01;
    pub const TCP_FLAG_SYN: u8 = 0x02;
    pub const TCP_FLAG_RST: u8 = 0x04;
    pub const TCP_FLAG_ACK: u8 = 0x10;

    #[repr(C, packed)]
    #[derive(Clone, Copy)]
    pub struct EthernetHeader {
        pub dst: [u8; 6],
        pub src: [u8; 6],
        pub eth_proto: u16,
    }

    impl EthernetHeader {
        /// Build an IPv4 Ethernet header with the provided source and destination MAC addresses.
        ///
        /// The eBPF dataplane rewrites complete L2 headers when steering packets between bridge,
        /// loopback, and overlay paths. Keeping the constructor here avoids duplicating byte-order
        /// handling at each call site.
        #[inline(always)]
        pub const fn ipv4(dst: [u8; 6], src: [u8; 6]) -> Self {
            Self {
                dst,
                src,
                eth_proto: 0x0800u16.to_be(),
            }
        }

        /// Build an IPv6 Ethernet header with the provided source and destination MAC addresses.
        ///
        /// The bridge load balancer rewrites both IPv4 and IPv6 frames, so callers need the same
        /// ergonomic constructor across both families.
        #[inline(always)]
        pub const fn ipv6(dst: [u8; 6], src: [u8; 6]) -> Self {
            Self {
                dst,
                src,
                eth_proto: 0x86ddu16.to_be(),
            }
        }

        /// Build the synthetic broadcast Ethernet header used for loopback-originated IPv4 traffic.
        ///
        /// NodePort ingress materializes this header before redirecting the packet into the overlay
        /// bridge so a locally generated skb can traverse an Ethernet path.
        #[inline(always)]
        pub const fn broadcast_ipv4(src: [u8; 6]) -> Self {
            Self::ipv4([0xff; 6], src)
        }

        /// Build the synthetic broadcast Ethernet header used for loopback-originated IPv6 traffic.
        ///
        /// IPv6 NodePort curls on loopback still need an L2 envelope before the packet can be
        /// redirected into the bridge dataplane, and the placeholder header is replaced later once
        /// VIP load-balancing selects a concrete backend.
        #[inline(always)]
        pub const fn broadcast_ipv6(src: [u8; 6]) -> Self {
            Self::ipv6([0xff; 6], src)
        }

        #[inline(always)]
        pub fn protocol(&self) -> u16 {
            u16::from_be(self.eth_proto)
        }

        /// Report whether both MAC address fields are still zeroed.
        ///
        /// Some loopback-originated skbs expose an empty L2 slot instead of a populated Ethernet
        /// header. The ingress classifier uses this to decide whether it can fill the slot in place
        /// without overwriting a real Ethernet frame.
        #[inline(always)]
        pub fn has_zero_addresses(&self) -> bool {
            is_zero(&self.dst) && is_zero(&self.src)
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
    pub struct Ipv6Header {
        pub version_tc_flow: u32,
        pub payload_len: u16,
        pub next_header: u8,
        pub hop_limit: u8,
        pub src: [u8; 16],
        pub dst: [u8; 16],
    }

    impl Ipv6Header {
        #[inline(always)]
        pub fn version(&self) -> u8 {
            (u32::from_be(self.version_tc_flow) >> 28) as u8
        }
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct Icmpv6NeighborMessage {
        pub icmp_type: u8,
        pub code: u8,
        pub checksum: u16,
        pub flags_or_reserved: u32,
        pub target: [u8; 16],
        pub option_type: u8,
        pub option_len: u8,
        pub option_mac: [u8; 6],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct Icmpv6NeighborTarget {
        pub icmp_type: u8,
        pub code: u8,
        pub checksum: u16,
        pub flags_or_reserved: u32,
        pub target: [u8; 16],
    }

    /// Fixed TCP header prefix used by the dataplane to extract ports and connection flags.
    ///
    /// Mantissa only needs the stable fields that exist before any TCP options. Keeping the
    /// shared parser here lets the overlay and NodePort classifiers agree on how TCP handshake
    /// packets are identified before conntrack rules become stricter.
    #[repr(C, packed)]
    #[derive(Clone, Copy)]
    pub struct TcpHeader {
        pub source: u16,
        pub dest: u16,
        pub seq: u32,
        pub ack_seq: u32,
        pub data_offset_reserved: u8,
        pub flags: u8,
        pub window: u16,
        pub check: u16,
        pub urg_ptr: u16,
    }

    impl TcpHeader {
        /// Return the full TCP header length in bytes, including any options.
        ///
        /// Later conntrack hardening needs this to reject malformed packets and to distinguish a
        /// plain SYN from packets that already carry payload or option space.
        #[inline(always)]
        pub fn data_offset(&self) -> usize {
            ((self.data_offset_reserved >> 4) as usize) * 4
        }

        /// Return the raw TCP flags byte as it appears on the wire.
        ///
        /// The classifiers store ports in on-wire byte order inside flow keys, so flag inspection
        /// must avoid any other transformation beyond exposing this stable byte.
        #[inline(always)]
        pub fn flags(&self) -> u8 {
            self.flags
        }

        /// Report whether the TCP packet carries the SYN flag.
        ///
        /// SYN detection is the minimal building block for later gating flow creation on a valid
        /// first packet instead of any arbitrary tuple hit.
        #[inline(always)]
        pub fn is_syn(&self) -> bool {
            self.flags() & TCP_FLAG_SYN != 0
        }

        /// Report whether the TCP packet carries the ACK flag.
        ///
        /// Handshake validation needs ACK visibility so stray ACKs do not create new conntrack
        /// state in a later hardening step.
        #[inline(always)]
        pub fn is_ack(&self) -> bool {
            self.flags() & TCP_FLAG_ACK != 0
        }

        /// Report whether the TCP packet carries the FIN flag.
        ///
        /// FIN tracking is the simplest way to bound how long closed flows remain in the reverse
        /// translation cache once teardown handling is added.
        #[inline(always)]
        pub fn is_fin(&self) -> bool {
            self.flags() & TCP_FLAG_FIN != 0
        }

        /// Report whether the TCP packet carries the RST flag.
        ///
        /// Reset detection lets later conntrack rules tear down state immediately when an endpoint
        /// aborts the connection instead of waiting for generic aging.
        #[inline(always)]
        pub fn is_rst(&self) -> bool {
            self.flags() & TCP_FLAG_RST != 0
        }

        /// Report whether the packet is a plain SYN without ACK, FIN, or RST.
        ///
        /// Mantissa's first conntrack hardening pass will use this to restrict TCP flow creation
        /// to valid opening packets without implementing a full kernel-style state machine.
        #[inline(always)]
        pub fn is_syn_only(&self) -> bool {
            self.is_syn() && !self.is_ack() && !self.is_fin() && !self.is_rst()
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

    /// Errors that can occur when reading from packet memory.
    #[derive(Clone, Copy, Debug)]
    pub enum PacketReadError {
        OutOfBounds,
    }

    /// Read a value of type `T` from the packet buffer at the provided offset.
    ///
    /// # Safety
    /// Caller must ensure `data` and `data_end` bound a valid packet region and that `offset`
    /// plus `size_of::<T>()` does not exceed `data_end`.
    pub unsafe fn read_at<T: Copy>(
        data: usize,
        data_end: usize,
        offset: usize,
    ) -> Result<T, PacketReadError> {
        let size = mem::size_of::<T>();
        if data + offset + size > data_end {
            return Err(PacketReadError::OutOfBounds);
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
    /// Return a const pointer to `T` within the packet buffer.
    ///
    /// # Safety
    /// Caller must ensure the pointer stays within the bounds of the packet slice.
    pub unsafe fn ptr_at<T>(
        data: usize,
        data_end: usize,
        offset: usize,
    ) -> Result<*const T, PacketReadError> {
        let size = mem::size_of::<T>();
        if data + offset + size > data_end {
            return Err(PacketReadError::OutOfBounds);
        }
        Ok((data + offset) as *const T)
    }

    #[inline(always)]
    /// Return a mutable pointer to `T` within the packet buffer.
    ///
    /// # Safety
    /// Caller must ensure exclusive access and that the returned pointer is in-bounds.
    pub unsafe fn mut_ptr_at<T>(
        data: usize,
        data_end: usize,
        offset: usize,
    ) -> Result<*mut T, PacketReadError> {
        let size = mem::size_of::<T>();
        if data + offset + size > data_end {
            return Err(PacketReadError::OutOfBounds);
        }
        Ok((data + offset) as *mut T)
    }
}

pub mod lb {
    use super::net::{TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_RST, TCP_FLAG_SYN};

    /// Maximum number of backend targets tracked per VIP entry.
    pub const MAX_BACKENDS_PER_VIP: usize = 1024;
    /// Maximum number of VIPs tracked in LB maps.
    pub const MAX_VIPS: usize = 4096;
    /// Conntrack state value used before a flow has been classified beyond its protocol number.
    pub const CONNTRACK_STATE_UNTRACKED: u8 = 0;
    /// Conntrack state value used for one active UDP flow.
    pub const CONNTRACK_STATE_UDP_ACTIVE: u8 = 1;
    /// Conntrack state value used for a TCP flow after the opening SYN is accepted.
    pub const CONNTRACK_STATE_TCP_SYN_SENT: u8 = 2;
    /// Conntrack state value used once a TCP flow has seen valid bidirectional traffic.
    pub const CONNTRACK_STATE_TCP_ESTABLISHED: u8 = 3;
    /// Conntrack state value used after FIN has started TCP teardown.
    pub const CONNTRACK_STATE_TCP_FIN_WAIT: u8 = 4;
    /// Conntrack state value used once a flow should be considered closed.
    pub const CONNTRACK_STATE_TCP_CLOSED: u8 = 5;
    /// Flag bit that marks one cached NAT flow as ready for aggressive teardown.
    pub const CONNTRACK_FLAG_TERMINATING: u8 = 0x01;
    const IPPROTO_TCP: u8 = 6;
    const IPPROTO_UDP: u8 = 17;

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

    /// VIP metadata used when selecting a target; actual backends are stored in slot maps.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct VipEntry {
        pub vip_mac: [u8; 6],
        /// Number of precomputed backend lookup slots for this VIP.
        pub backend_count: u16,
        pub _pad: [u8; 2],
    }

    /// Composite key used to isolate backend lookup slots per VIP inside a flat hash map.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct VipBackendKey {
        pub vip: u32,
        pub slot: u32,
    }

    /// Key for IPv6 VIP-backed routing decisions stored in eBPF maps.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct VipKey6 {
        pub vip: [u8; 16],
    }

    /// L2/L3 coordinates for an IPv6 backend attachment.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct Backend6 {
        pub ip: [u8; 16],
        pub mac: [u8; 6],
        pub _pad: [u8; 2],
    }

    /// Composite key used to isolate IPv6 backend lookup slots per VIP.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct VipBackendKey6 {
        pub vip: [u8; 16],
        pub slot: u32,
        pub _pad: [u8; 4],
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
        /// Explicit tail padding so the key has deterministic bytes (Rust would otherwise leave
        /// implicit struct padding uninitialized, causing map lookups to miss across programs).
        pub padding: [u8; 2],
    }

    /// Normalized IPv6 5-tuple used to maintain DNAT/SNAT state.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct Flow6 {
        pub src: [u8; 16],
        pub dst: [u8; 16],
        pub src_port: u16,
        pub dst_port: u16,
        pub proto: u8,
        /// Explicit tail padding keeps all bytes deterministic for map lookups.
        pub padding: [u8; 3],
    }

    /// Conntrack verdict returned after evaluating one packet against cached flow state.
    ///
    /// The overlay and NodePort datapaths share the same minimal lifecycle rules: packets are
    /// either rejected without touching the cache, allowed with refreshed metadata, or allowed
    /// while retiring the flow pair after the current rewrite completes.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ConntrackVerdict {
        Reject,
        Remove,
        Allow(ConntrackMetadata),
        AllowAndRemove(ConntrackMetadata),
    }

    #[inline(always)]
    const fn tcp_has_syn(flags: u8) -> bool {
        flags & TCP_FLAG_SYN != 0
    }

    #[inline(always)]
    const fn tcp_has_ack(flags: u8) -> bool {
        flags & TCP_FLAG_ACK != 0
    }

    #[inline(always)]
    const fn tcp_has_fin(flags: u8) -> bool {
        flags & TCP_FLAG_FIN != 0
    }

    #[inline(always)]
    const fn tcp_has_rst(flags: u8) -> bool {
        flags & TCP_FLAG_RST != 0
    }

    #[inline(always)]
    const fn tcp_has_syn_ack(flags: u8) -> bool {
        tcp_has_syn(flags) && tcp_has_ack(flags) && !tcp_has_fin(flags) && !tcp_has_rst(flags)
    }

    #[inline(always)]
    const fn tcp_has_syn_only(flags: u8) -> bool {
        tcp_has_syn(flags) && !tcp_has_ack(flags) && !tcp_has_fin(flags) && !tcp_has_rst(flags)
    }

    /// Per-flow conntrack metadata stored next to each NAT translation entry.
    ///
    /// The dataplane currently only needs a small amount of state: protocol identity, a minimal
    /// TCP/UDP lifecycle marker, one teardown bit, and the last observed activity timestamp. By
    /// reserving that layout now, later hardening can tighten flow validation without another map
    /// value migration.
    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct ConntrackMetadata {
        pub last_seen_ns: u64,
        pub protocol: u8,
        pub state: u8,
        pub flags: u8,
        pub _pad: [u8; 5],
    }

    impl ConntrackMetadata {
        /// Build the default metadata for a freshly selected backend mapping.
        ///
        /// New flow selections initially only know the transport protocol. Later classifiers
        /// refine the state after inspecting the first packet and recording a dataplane timestamp.
        #[inline(always)]
        pub const fn untracked(protocol: u8) -> Self {
            Self {
                last_seen_ns: 0,
                protocol,
                state: CONNTRACK_STATE_UNTRACKED,
                flags: 0,
                _pad: [0u8; 5],
            }
        }

        /// Return a copy of the metadata with an updated protocol-specific conntrack state.
        ///
        /// This keeps later state transitions explicit and side-effect free while the dataplane
        /// still stores the value inline inside map entries.
        #[inline(always)]
        pub const fn with_state(mut self, state: u8) -> Self {
            self.state = state;
            self
        }

        /// Return a copy of the metadata with a refreshed last-seen timestamp.
        ///
        /// Aging logic for UDP and closed TCP flows will use this field instead of relying solely
        /// on opaque LRU eviction behavior.
        #[inline(always)]
        pub const fn with_last_seen_ns(mut self, last_seen_ns: u64) -> Self {
            self.last_seen_ns = last_seen_ns;
            self
        }

        /// Return a copy of the metadata with the teardown marker enabled.
        ///
        /// Later flow cleanup can use this bit to distinguish actively closing flows from normal
        /// steady-state traffic without expanding the state enum further.
        #[inline(always)]
        pub const fn mark_terminating(mut self) -> Self {
            self.flags |= CONNTRACK_FLAG_TERMINATING;
            self
        }

        /// Report whether the flow has already been marked for teardown.
        ///
        /// Egress validation and cleanup paths can use this to avoid rewriting packets that belong
        /// to a connection the dataplane has already decided to retire.
        #[inline(always)]
        pub const fn is_terminating(&self) -> bool {
            self.flags & CONNTRACK_FLAG_TERMINATING != 0
        }

        /// Build the initial conntrack metadata for one newly admitted flow.
        ///
        /// UDP can start on the first packet, while TCP only becomes tracked after a plain SYN.
        /// Rejecting other first TCP packets keeps the dataplane from creating state from stray
        /// ACK, FIN, or RST traffic.
        #[inline(always)]
        pub const fn begin_flow(protocol: u8, tcp_flags: u8, now_ns: u64) -> Option<Self> {
            if protocol == IPPROTO_UDP {
                return Some(
                    Self::untracked(protocol)
                        .with_state(CONNTRACK_STATE_UDP_ACTIVE)
                        .with_last_seen_ns(now_ns),
                );
            }
            if protocol == IPPROTO_TCP && tcp_has_syn_only(tcp_flags) {
                return Some(
                    Self::untracked(protocol)
                        .with_state(CONNTRACK_STATE_TCP_SYN_SENT)
                        .with_last_seen_ns(now_ns),
                );
            }
            None
        }

        /// Evaluate one client-to-backend packet against the cached flow state.
        ///
        /// Forward packets refresh UDP activity, allow TCP SYN retransmits during setup, promote
        /// flows to established once ACK traffic appears, and mark teardown after FIN or RST.
        #[inline(always)]
        pub const fn advance_forward(self, tcp_flags: u8, now_ns: u64) -> ConntrackVerdict {
            if self.protocol == IPPROTO_UDP {
                return ConntrackVerdict::Allow(
                    self.with_state(CONNTRACK_STATE_UDP_ACTIVE)
                        .with_last_seen_ns(now_ns),
                );
            }
            if self.protocol != IPPROTO_TCP {
                return ConntrackVerdict::Remove;
            }

            match self.state {
                CONNTRACK_STATE_TCP_SYN_SENT => {
                    if tcp_has_rst(tcp_flags) {
                        ConntrackVerdict::AllowAndRemove(
                            self.close_terminal_flow(now_ns).with_last_seen_ns(now_ns),
                        )
                    } else if tcp_has_fin(tcp_flags) {
                        ConntrackVerdict::Allow(
                            self.enter_fin_wait(now_ns).with_last_seen_ns(now_ns),
                        )
                    } else if tcp_has_syn_only(tcp_flags) {
                        ConntrackVerdict::Allow(
                            self.with_state(CONNTRACK_STATE_TCP_SYN_SENT)
                                .with_last_seen_ns(now_ns),
                        )
                    } else if tcp_has_ack(tcp_flags) {
                        ConntrackVerdict::Allow(
                            self.with_state(CONNTRACK_STATE_TCP_ESTABLISHED)
                                .with_last_seen_ns(now_ns),
                        )
                    } else {
                        ConntrackVerdict::Reject
                    }
                }
                CONNTRACK_STATE_TCP_ESTABLISHED => {
                    if tcp_has_rst(tcp_flags) {
                        ConntrackVerdict::AllowAndRemove(
                            self.close_terminal_flow(now_ns).with_last_seen_ns(now_ns),
                        )
                    } else if tcp_has_fin(tcp_flags) {
                        ConntrackVerdict::Allow(
                            self.enter_fin_wait(now_ns).with_last_seen_ns(now_ns),
                        )
                    } else {
                        ConntrackVerdict::Allow(
                            self.with_state(CONNTRACK_STATE_TCP_ESTABLISHED)
                                .with_last_seen_ns(now_ns),
                        )
                    }
                }
                CONNTRACK_STATE_TCP_FIN_WAIT => {
                    if tcp_has_rst(tcp_flags) || tcp_has_fin(tcp_flags) {
                        ConntrackVerdict::AllowAndRemove(
                            self.close_terminal_flow(now_ns).with_last_seen_ns(now_ns),
                        )
                    } else {
                        ConntrackVerdict::Allow(
                            self.enter_fin_wait(now_ns).with_last_seen_ns(now_ns),
                        )
                    }
                }
                CONNTRACK_STATE_UNTRACKED
                | CONNTRACK_STATE_UDP_ACTIVE
                | CONNTRACK_STATE_TCP_CLOSED => ConntrackVerdict::Remove,
                _ => ConntrackVerdict::Remove,
            }
        }

        /// Evaluate one backend-to-client packet against the cached flow state.
        ///
        /// Reverse packets only pass during TCP setup when the backend responds with SYN-ACK or
        /// RST. Once established, reverse traffic follows the same FIN/RST retirement rules as the
        /// forward path.
        #[inline(always)]
        pub const fn advance_reverse(self, tcp_flags: u8, now_ns: u64) -> ConntrackVerdict {
            if self.protocol == IPPROTO_UDP {
                return ConntrackVerdict::Allow(
                    self.with_state(CONNTRACK_STATE_UDP_ACTIVE)
                        .with_last_seen_ns(now_ns),
                );
            }
            if self.protocol != IPPROTO_TCP {
                return ConntrackVerdict::Remove;
            }

            match self.state {
                CONNTRACK_STATE_TCP_SYN_SENT => {
                    if tcp_has_rst(tcp_flags) {
                        ConntrackVerdict::AllowAndRemove(
                            self.close_terminal_flow(now_ns).with_last_seen_ns(now_ns),
                        )
                    } else if tcp_has_syn_ack(tcp_flags) {
                        ConntrackVerdict::Allow(
                            self.with_state(CONNTRACK_STATE_TCP_ESTABLISHED)
                                .with_last_seen_ns(now_ns),
                        )
                    } else {
                        ConntrackVerdict::Reject
                    }
                }
                CONNTRACK_STATE_TCP_ESTABLISHED => {
                    if tcp_has_rst(tcp_flags) {
                        ConntrackVerdict::AllowAndRemove(
                            self.close_terminal_flow(now_ns).with_last_seen_ns(now_ns),
                        )
                    } else if tcp_has_fin(tcp_flags) {
                        ConntrackVerdict::Allow(
                            self.enter_fin_wait(now_ns).with_last_seen_ns(now_ns),
                        )
                    } else {
                        ConntrackVerdict::Allow(
                            self.with_state(CONNTRACK_STATE_TCP_ESTABLISHED)
                                .with_last_seen_ns(now_ns),
                        )
                    }
                }
                CONNTRACK_STATE_TCP_FIN_WAIT => {
                    if tcp_has_rst(tcp_flags) || tcp_has_fin(tcp_flags) {
                        ConntrackVerdict::AllowAndRemove(
                            self.close_terminal_flow(now_ns).with_last_seen_ns(now_ns),
                        )
                    } else {
                        ConntrackVerdict::Allow(
                            self.enter_fin_wait(now_ns).with_last_seen_ns(now_ns),
                        )
                    }
                }
                CONNTRACK_STATE_UNTRACKED
                | CONNTRACK_STATE_UDP_ACTIVE
                | CONNTRACK_STATE_TCP_CLOSED => ConntrackVerdict::Remove,
                _ => ConntrackVerdict::Remove,
            }
        }

        /// Return metadata for a flow that has entered the graceful-close phase.
        ///
        /// FIN packets still need one last rewrite on both directions, so the dataplane marks the
        /// flow as terminating but keeps it alive until a later FIN or RST retires the pair.
        #[inline(always)]
        const fn enter_fin_wait(self, now_ns: u64) -> Self {
            self.with_state(CONNTRACK_STATE_TCP_FIN_WAIT)
                .with_last_seen_ns(now_ns)
                .mark_terminating()
        }

        /// Return metadata for a flow that should be removed after the current packet.
        ///
        /// Reset packets and terminal close packets still need one last translation, so the caller
        /// receives a closed state plus a separate verdict that says to delete the cached pair.
        #[inline(always)]
        const fn close_terminal_flow(self, now_ns: u64) -> Self {
            self.with_state(CONNTRACK_STATE_TCP_CLOSED)
                .with_last_seen_ns(now_ns)
                .mark_terminating()
        }
    }

    /// Cached per-flow translation data shared between ingress/egress hooks.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct NatEntry {
        pub vip: u32,
        pub vip_mac: [u8; 6],
        pub _pad0: [u8; 2],
        pub backend_ip: u32,
        pub backend_mac: [u8; 6],
        pub _pad1: [u8; 2],
        pub conntrack: ConntrackMetadata,
    }

    /// Cached per-flow IPv6 translation data shared between ingress and egress hooks.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct NatEntry6 {
        pub vip: [u8; 16],
        pub vip_mac: [u8; 6],
        pub _pad0: [u8; 2],
        pub backend_ip: [u8; 16],
        pub backend_mac: [u8; 6],
        pub _pad1: [u8; 2],
        pub conntrack: ConntrackMetadata,
    }

    /// Cached per-flow NodePort IPv4 translation data shared between ingress and egress hooks.
    ///
    /// Public ingress rewrites traffic from the published node address into the overlay VIP, then
    /// return traffic uses the same cached tuple to restore the external client and node listener
    /// identity on the way back out.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct NodePortNat {
        pub node_ip: u32,
        pub node_port: u16,
        pub _pad: u16,
        pub client_ip: u32,
        pub conntrack: ConntrackMetadata,
    }

    /// Cached per-flow NodePort IPv6 translation data shared between ingress and egress hooks.
    ///
    /// This carries the externally visible node address, the original client address, and the
    /// shared conntrack metadata so both tc hooks can enforce the same flow lifecycle.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct NodePortNat6 {
        pub node_ip: [u8; 16],
        pub node_port: u16,
        pub _pad: [u8; 2],
        pub client_ip: [u8; 16],
        pub conntrack: ConntrackMetadata,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        lb::{
            ConntrackMetadata, ConntrackVerdict, CONNTRACK_FLAG_TERMINATING,
            CONNTRACK_STATE_TCP_ESTABLISHED, CONNTRACK_STATE_TCP_FIN_WAIT,
            CONNTRACK_STATE_TCP_SYN_SENT,
        },
        net::{TcpHeader, TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_RST, TCP_FLAG_SYN},
    };

    #[test]
    fn tcp_header_reports_syn_only_packets() {
        let syn = TcpHeader {
            source: 1234u16.to_be(),
            dest: 80u16.to_be(),
            seq: 0,
            ack_seq: 0,
            data_offset_reserved: 5 << 4,
            flags: TCP_FLAG_SYN,
            window: 0,
            check: 0,
            urg_ptr: 0,
        };

        assert_eq!(syn.data_offset(), 20);
        assert!(syn.is_syn());
        assert!(!syn.is_ack());
        assert!(syn.is_syn_only());
    }

    #[test]
    fn tcp_header_reports_acknowledged_syn_packets() {
        let syn_ack = TcpHeader {
            source: 80u16.to_be(),
            dest: 1234u16.to_be(),
            seq: 0,
            ack_seq: 0,
            data_offset_reserved: 6 << 4,
            flags: TCP_FLAG_SYN | TCP_FLAG_ACK,
            window: 0,
            check: 0,
            urg_ptr: 0,
        };

        assert_eq!(syn_ack.data_offset(), 24);
        assert!(syn_ack.is_syn());
        assert!(syn_ack.is_ack());
        assert!(!syn_ack.is_syn_only());
    }

    #[test]
    fn conntrack_metadata_records_state_and_teardown() {
        let metadata = ConntrackMetadata::untracked(6)
            .with_state(CONNTRACK_STATE_TCP_SYN_SENT)
            .with_last_seen_ns(42)
            .mark_terminating();

        assert_eq!(metadata.protocol, 6);
        assert_eq!(metadata.state, CONNTRACK_STATE_TCP_SYN_SENT);
        assert_eq!(metadata.last_seen_ns, 42);
        assert_eq!(
            metadata.flags & CONNTRACK_FLAG_TERMINATING,
            CONNTRACK_FLAG_TERMINATING
        );
        assert!(metadata.is_terminating());
    }

    #[test]
    fn begin_flow_only_tracks_plain_tcp_syn_packets() {
        assert!(ConntrackMetadata::begin_flow(6, TCP_FLAG_ACK, 10).is_none());

        let metadata = ConntrackMetadata::begin_flow(6, TCP_FLAG_SYN, 10)
            .expect("plain SYN should create tcp state");
        assert_eq!(metadata.protocol, 6);
        assert_eq!(metadata.state, CONNTRACK_STATE_TCP_SYN_SENT);
        assert_eq!(metadata.last_seen_ns, 10);
    }

    #[test]
    fn reverse_syn_ack_promotes_tcp_flow_to_established() {
        let metadata = ConntrackMetadata::begin_flow(6, TCP_FLAG_SYN, 10)
            .expect("plain SYN should create tcp state");

        let verdict = metadata.advance_reverse(TCP_FLAG_SYN | TCP_FLAG_ACK, 20);
        assert_eq!(
            verdict,
            ConntrackVerdict::Allow(
                metadata
                    .with_state(CONNTRACK_STATE_TCP_ESTABLISHED)
                    .with_last_seen_ns(20),
            )
        );
    }

    #[test]
    fn established_fin_marks_flow_terminating_without_immediate_removal() {
        let metadata = ConntrackMetadata::begin_flow(6, TCP_FLAG_SYN, 10)
            .expect("plain SYN should create tcp state")
            .with_state(CONNTRACK_STATE_TCP_ESTABLISHED)
            .with_last_seen_ns(15);

        let verdict = metadata.advance_forward(TCP_FLAG_FIN | TCP_FLAG_ACK, 30);
        assert_eq!(
            verdict,
            ConntrackVerdict::Allow(
                metadata
                    .with_state(CONNTRACK_STATE_TCP_FIN_WAIT)
                    .with_last_seen_ns(30)
                    .mark_terminating(),
            )
        );
    }

    #[test]
    fn reset_closes_flow_and_requests_pair_removal() {
        let metadata = ConntrackMetadata::begin_flow(6, TCP_FLAG_SYN, 10)
            .expect("plain SYN should create tcp state")
            .with_state(CONNTRACK_STATE_TCP_ESTABLISHED)
            .with_last_seen_ns(15);

        let verdict = metadata.advance_reverse(TCP_FLAG_RST, 40);
        assert_eq!(
            verdict,
            ConntrackVerdict::AllowAndRemove(
                metadata
                    .with_state(super::lb::CONNTRACK_STATE_TCP_CLOSED)
                    .with_last_seen_ns(40)
                    .mark_terminating(),
            )
        );
    }
}
