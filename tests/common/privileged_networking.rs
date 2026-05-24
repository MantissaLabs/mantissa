#![allow(dead_code)]

use super::convergence::wait_until;
use anyhow::{Context, Result};
use aya::programs::tc::{TcAttachType, qdisc_detach_program};
use futures::TryStreamExt;
use mantissa::config::{
    Config, ConfigSource, global_config, global_config_source, set_global_config_with_source,
};
use mantissa::network::types::{
    BpfProgramSpec, NetworkDriver, NetworkSpecDraft, NetworkSpecValue, NetworkStatus,
};
use mantissa::server::headless::{HeadlessConfig, HeadlessNode, HeadlessTransport};
use mantissa::workload::manager::WorkloadRuntimeConfig;
use mantissa_net::paths::STATE_DIR_ENV;
use parking_lot::{Mutex, MutexGuard};
use rtnetlink::RouteMessageBuilder;
use rtnetlink::packet_route::address::{AddressAttribute, AddressMessage};
use rtnetlink::packet_route::link::{LinkAttribute, LinkXdp, XdpAttached};
use rtnetlink::packet_route::neighbour::{NeighbourAddress, NeighbourAttribute, NeighbourState};
use rtnetlink::packet_route::route::{RouteAttribute, RouteHeader};
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

pub const NETWORKING_TESTS_ENV: &str = "MANTISSA_RUN_NETWORKING_TESTS";

/// Keeps one process-global environment override paired with its previous value.
struct EnvOverrideGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvOverrideGuard {
    /// Apply one environment override that must be restored when the test exits.
    fn set(key: &'static str, value: impl Into<OsString>) -> Self {
        let previous = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value.into());
        }
        Self { key, previous }
    }
}

impl Drop for EnvOverrideGuard {
    /// Restore the previous environment value once the scoped override is no longer needed.
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => unsafe {
                std::env::set_var(self.key, value);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}

/// Restores global config and environment overrides after one privileged networking test.
pub struct PrivilegedTestGuard {
    previous: Config,
    source: ConfigSource,
    _state_dir: TempDir,
    _env_guards: Vec<EnvOverrideGuard>,
    _lock: MutexGuard<'static, ()>,
}

/// Returns the process-global mutex used to serialize privileged networking overrides.
fn privileged_override_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Returns the process-global registry of privileged test networks that still need host cleanup.
fn privileged_network_cleanup_registry() -> &'static Mutex<BTreeSet<Uuid>> {
    static REGISTRY: OnceLock<Mutex<BTreeSet<Uuid>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(BTreeSet::new()))
}

/// Record one privileged test network so panic paths can still remove its kernel state.
fn register_privileged_network_cleanup(network_id: Uuid) {
    let mut guard = privileged_network_cleanup_registry().lock();
    guard.insert(network_id);
}

/// Stop tracking one privileged test network once explicit teardown has completed.
fn unregister_privileged_network_cleanup(network_id: Uuid) {
    let mut guard = privileged_network_cleanup_registry().lock();
    guard.remove(&network_id);
}

/// Return the currently tracked privileged test networks and clear the registry in one step.
fn take_registered_privileged_networks() -> Vec<Uuid> {
    let mut guard = privileged_network_cleanup_registry().lock();
    std::mem::take(&mut *guard).into_iter().collect()
}

/// Forget all tracked privileged test networks before a fresh test guard starts.
fn clear_registered_privileged_networks() {
    let mut guard = privileged_network_cleanup_registry().lock();
    guard.clear();
}

/// Returns the shared bpffs directory where the NodePort dataplane pins its global maps.
fn privileged_nodeport_bpf_pin_dir() -> PathBuf {
    PathBuf::from("/sys/fs/bpf/mantissa/nodeport")
}

/// Detach one tc program name (and its truncated kernel-visible alias) from one interface hook.
fn detach_tc_program_by_name(
    iface: &str,
    attach_type: TcAttachType,
    program_name: &str,
) -> io::Result<()> {
    let mut candidates = vec![program_name.to_string()];
    let truncated: String = program_name.chars().take(15).collect();
    if truncated != program_name {
        candidates.push(truncated);
    }

    for candidate in candidates {
        match qdisc_detach_program(iface, attach_type, &candidate) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }

    Ok(())
}

/// Force-remove shared NodePort tc attachments and bpffs pins left behind by one failed test.
fn force_cleanup_privileged_nodeport_state_sync() {
    if let Ok(entries) = std::fs::read_dir("/sys/class/net") {
        for entry in entries.flatten() {
            let Some(iface) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let _ = detach_tc_program_by_name(&iface, TcAttachType::Ingress, "nodeport_tc_ingress");
            let _ = detach_tc_program_by_name(&iface, TcAttachType::Egress, "nodeport_tc_egress");
            let _ = detach_tc_program_by_name(&iface, TcAttachType::Ingress, "nodeport_tc_egress");
        }
    }

    let pin_dir = privileged_nodeport_bpf_pin_dir();
    let _ = std::fs::remove_dir_all(&pin_dir);
}

impl PrivilegedTestGuard {
    /// Apply one isolated config override for a privileged networking test.
    ///
    /// The guard serializes access to the process-global config/env state and redirects the
    /// Mantissa state dir into a temporary directory so root-mode tests never touch
    /// `/var/lib/mantissa`.
    pub fn apply<F>(mutator: F) -> Self
    where
        F: FnOnce(&mut Config),
    {
        let lock = privileged_override_lock().lock();
        clear_registered_privileged_networks();
        force_cleanup_privileged_nodeport_state_sync();
        let previous = global_config();
        let source = global_config_source();
        let state_dir = tempfile::tempdir().expect("create privileged test state dir");

        let env_guards = vec![EnvOverrideGuard::set(
            STATE_DIR_ENV,
            state_dir.path().as_os_str(),
        )];

        let mut config = previous.clone();
        mutator(&mut config);

        let mut override_source = source.clone();
        override_source.env_overrides = true;
        set_global_config_with_source(config, override_source);

        Self {
            previous,
            source,
            _state_dir: state_dir,
            _env_guards: env_guards,
            _lock: lock,
        }
    }
}

impl Drop for PrivilegedTestGuard {
    /// Restore the process-global config snapshot after the test-scoped override completes.
    fn drop(&mut self) {
        for network_id in take_registered_privileged_networks() {
            force_cleanup_privileged_network_links_sync(network_id);
        }
        force_cleanup_privileged_nodeport_state_sync();
        set_global_config_with_source(self.previous.clone(), self.source.clone());
    }
}

/// Return whether the shared privileged networking suite is enabled for this process.
pub fn privileged_networking_enabled(label: &str) -> bool {
    if std::env::var_os(NETWORKING_TESTS_ENV).is_none() {
        eprintln!("skipping privileged {label} tests; {NETWORKING_TESTS_ENV} is not set");
        return false;
    }

    assert!(
        unsafe { libc::geteuid() } == 0,
        "{NETWORKING_TESTS_ENV} requires root privileges so kernel networking setup can run"
    );
    true
}

/// Carries the optional external BPF artifact override for privileged networking tests.
pub struct PrivilegedBpfArtifacts {
    artifact_dir: Option<PathBuf>,
}

impl PrivilegedBpfArtifacts {
    /// Apply the artifact override to a test-scoped configuration snapshot.
    pub fn apply_to(&self, config: &mut Config) {
        config.network.bpf.artifact_dir = self
            .artifact_dir
            .as_ref()
            .map(|path| path.display().to_string());
    }
}

/// Resolve BPF artifacts for one privileged networking lane.
pub fn privileged_artifact_dir(
    label: &str,
    required_artifacts: &[&str],
) -> Option<PrivilegedBpfArtifacts> {
    if !privileged_networking_enabled(label) {
        return None;
    }

    let Some(artifact_dir) = std::env::var_os("MANTISSA_BPF_DIR").map(PathBuf::from) else {
        return Some(PrivilegedBpfArtifacts { artifact_dir: None });
    };

    if required_artifacts
        .iter()
        .all(|artifact| bpf_artifact_exists(&artifact_dir, artifact))
    {
        return Some(PrivilegedBpfArtifacts {
            artifact_dir: Some(artifact_dir),
        });
    }

    let missing = required_artifacts
        .iter()
        .filter(|artifact| !bpf_artifact_exists(&artifact_dir, artifact))
        .copied()
        .collect::<Vec<_>>()
        .join(", ");
    panic!(
        "MANTISSA_BPF_DIR={} is missing required BPF artifacts for privileged {label} tests: {missing}",
        artifact_dir.display()
    );
}

/// Return whether `dir` contains an artifact in either packaged or raw cargo-output form.
fn bpf_artifact_exists(dir: &Path, artifact: &str) -> bool {
    if dir.join(artifact).exists() {
        return true;
    }

    let Some(stem) = artifact.strip_suffix(".bpf.o") else {
        return false;
    };

    dir.join(stem).exists() || dir.join(format!("{stem}.o")).exists()
}

/// Return fast workload reconciliation ticks for Docker-backed privileged dataplane tests.
fn privileged_task_runtime_config() -> WorkloadRuntimeConfig {
    WorkloadRuntimeConfig {
        repair_tick: Duration::from_millis(500),
        reconcile_tick: Duration::from_millis(500),
        runtime_event_debounce: Duration::from_millis(100),
    }
}

/// Start one real headless node with fast control-plane ticks for privileged dataplane tests.
pub fn privileged_headless_config() -> HeadlessConfig {
    HeadlessConfig {
        listen_addr: "127.0.0.1:0".to_string(),
        transport: HeadlessTransport::Inproc,
        root_schema_override: None,
        sync_tick: Some(Duration::from_millis(100)),
        sync_fanout: None,
        global_metadata_sync_tick: Some(Duration::from_millis(100)),
        global_metadata_sync_fanout: None,
        gossip_tick: Some(Duration::from_millis(100)),
        gossip_fanout: None,
        network_reconcile_tick: Some(Duration::from_secs(1)),
        network_attachment_refresh_tick: Some(Duration::from_millis(100)),
        gossip_channel_capacity: None,
        task_runtime: Some(privileged_task_runtime_config()),
        runtime_set: None,
        local_volume_root: None,
        master_key_kdf_params: None,
        store_gc_config: None,
    }
}

/// Start one real headless node with fast control-plane ticks for privileged dataplane tests.
pub async fn create_privileged_node() -> HeadlessNode {
    HeadlessNode::new_with_config(privileged_headless_config())
        .await
        .expect("start privileged networking node")
}

/// Build one VXLAN network spec for privileged dataplane validation.
pub fn privileged_test_network(
    name_prefix: &str,
    description: &str,
    subnet_cidr: &str,
    mtu: u32,
    bpf_programs: Vec<BpfProgramSpec>,
) -> NetworkSpecValue {
    NetworkSpecValue::new(NetworkSpecDraft {
        name: format!("{name_prefix}-{}", Uuid::new_v4()),
        description: description.to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: subnet_cidr.to_string(),
        vni: ((Uuid::new_v4().as_u128() % 16_000_000) as u32).max(1),
        mtu,
        sealed: false,
        bpf_programs,
    })
}

/// Generate one randomized private `/24` subnet so failed test cleanup cannot poison later runs.
pub fn privileged_test_subnet() -> String {
    let bytes = Uuid::new_v4().into_bytes();
    let octet_b = (bytes[0] % 254).saturating_add(1);
    let octet_c = (bytes[1] % 254).saturating_add(1);
    format!("10.{octet_b}.{octet_c}.0/24")
}

/// Generate one randomized private `/64` IPv6 subnet for privileged overlay tests.
pub fn privileged_test_subnet_v6() -> String {
    let bytes = Uuid::new_v4().into_bytes();
    let segment_a = u16::from_be_bytes([bytes[0], bytes[1]]);
    let segment_b = u16::from_be_bytes([bytes[2], bytes[3]]);
    format!("fd42:{segment_a:04x}:{segment_b:04x}::/64")
}

/// Persist one overlay network and wait until it reaches the expected local lifecycle state.
pub async fn create_privileged_network(
    node: &HeadlessNode,
    network: NetworkSpecValue,
    expected_status: NetworkStatus,
) -> NetworkSpecValue {
    register_privileged_network_cleanup(network.id);
    node.network_registry
        .upsert_spec(network.clone())
        .await
        .expect("upsert privileged test network");
    node.network_controller
        .schedule_spec_change(network.id)
        .await;

    let reached_status = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(100),
        || async {
            matches!(
                node.network_registry.get_spec(network.id),
                Ok(Some(spec)) if spec.status == expected_status
            )
        },
    )
    .await;
    if !reached_status {
        let observed_status = node
            .network_registry
            .get_spec(network.id)
            .ok()
            .flatten()
            .map(|spec| spec.status);
        let peer_errors: Vec<String> = node
            .network_registry
            .list_peer_states(Some(network.id))
            .map(|states| {
                states
                    .into_iter()
                    .filter_map(|state| {
                        state
                            .error
                            .map(|error| format!("{}:{:?}:{error}", state.peer_name, state.state))
                    })
                    .collect()
            })
            .unwrap_or_default();
        panic!(
            "network {} should reach {expected_status:?}; observed_status={observed_status:?}; peer_errors={peer_errors:?}",
            network.id
        );
    }

    network
}

/// Returns the deterministic kernel link names provisioned for one overlay network.
pub fn privileged_network_interfaces(network_id: Uuid) -> [String; 4] {
    let suffix: String = network_id.simple().to_string().chars().take(8).collect();
    [
        format!("mvx-{suffix}"),
        format!("mnt-br-{suffix}"),
        format!("mnhp-{suffix}"),
        format!("mnhost-{suffix}"),
    ]
}

/// Return whether the kernel still exposes the provided network device name.
pub fn link_exists(iface: &str) -> bool {
    Path::new("/sys/class/net").join(iface).exists()
}

/// Open one rtnetlink connection and spawn its driver on the current Tokio runtime.
fn rtnetlink_handle(context: &str) -> rtnetlink::Handle {
    let (conn, handle, _) =
        rtnetlink::new_connection().unwrap_or_else(|err| panic!("{context}: {err}"));
    tokio::spawn(conn);
    handle
}

/// Resolve one kernel interface index by device name.
async fn link_index(handle: &rtnetlink::Handle, iface: &str) -> Option<u32> {
    handle
        .link()
        .get()
        .match_name(iface.to_string())
        .execute()
        .try_next()
        .await
        .unwrap_or_else(|err| panic!("query interface {iface}: {err}"))
        .map(|link| link.header.index)
}

/// Resolve one kernel interface name by ifindex.
async fn link_name_by_index(handle: &rtnetlink::Handle, index: u32) -> Option<String> {
    handle
        .link()
        .get()
        .match_index(index)
        .execute()
        .try_next()
        .await
        .unwrap_or_else(|err| panic!("query interface index {index}: {err}"))
        .map(|link| {
            link.attributes
                .iter()
                .find_map(|attr| match attr {
                    LinkAttribute::IfName(name) => Some(name.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| format!("ifindex{index}"))
        })
}

/// Return the first main-table IPv4 default-route interface name.
pub async fn default_ipv4_route_iface() -> Option<String> {
    let handle = rtnetlink_handle("open rtnetlink connection for default IPv4 route lookup");
    let mut routes = handle
        .route()
        .get(RouteMessageBuilder::<Ipv4Addr>::new().build())
        .execute();

    while let Some(route) = routes
        .try_next()
        .await
        .expect("list IPv4 routes for default interface lookup")
    {
        let mut table = u32::from(route.header.table);
        let mut destination_present = false;
        let mut output_ifindex = None;
        for attr in route.attributes {
            match attr {
                RouteAttribute::Destination(_) => destination_present = true,
                RouteAttribute::Oif(index) => output_ifindex = Some(index),
                RouteAttribute::Table(route_table) => table = route_table,
                _ => {}
            }
        }
        if table != u32::from(RouteHeader::RT_TABLE_MAIN)
            || route.header.destination_prefix_length != 0
            || destination_present
        {
            continue;
        }
        if let Some(index) = output_ifindex {
            return link_name_by_index(&handle, index).await;
        }
    }

    None
}

/// Return the first IPv4 address assigned to one interface.
pub async fn interface_ipv4(iface: &str) -> Option<Ipv4Addr> {
    let handle = rtnetlink_handle("open rtnetlink connection for interface IPv4 lookup");
    let index = link_index(&handle, iface).await?;
    let mut addresses = handle
        .address()
        .get()
        .set_link_index_filter(index)
        .execute();

    while let Some(msg) = addresses
        .try_next()
        .await
        .unwrap_or_else(|err| panic!("enumerate IPv4 addresses for {iface}: {err}"))
    {
        for attr in &msg.attributes {
            match attr {
                AddressAttribute::Address(IpAddr::V4(addr))
                | AddressAttribute::Local(IpAddr::V4(addr)) => return Some(*addr),
                _ => {}
            }
        }
    }

    None
}

/// Return the first non-link-local IPv6 address assigned to one interface.
pub async fn interface_ipv6(iface: &str) -> Option<Ipv6Addr> {
    let handle = rtnetlink_handle("open rtnetlink connection for interface IPv6 lookup");
    let index = link_index(&handle, iface).await?;
    let mut addresses = handle
        .address()
        .get()
        .set_link_index_filter(index)
        .execute();

    while let Some(msg) = addresses
        .try_next()
        .await
        .unwrap_or_else(|err| panic!("enumerate IPv6 addresses for {iface}: {err}"))
    {
        for attr in &msg.attributes {
            let candidate = match attr {
                AddressAttribute::Address(IpAddr::V6(addr))
                | AddressAttribute::Local(IpAddr::V6(addr)) => *addr,
                _ => continue,
            };
            if !candidate.is_unicast_link_local() {
                return Some(candidate);
            }
        }
    }

    None
}

/// Return whether an interface already has a usable non-link-local IPv6 address.
pub async fn iface_has_usable_ipv6(iface: &str) -> bool {
    interface_ipv6(iface).await.is_some()
}

/// Add one IP address to the requested interface through rtnetlink.
pub async fn add_interface_address(iface: &str, ip: IpAddr, prefix_len: u8) -> Result<()> {
    let handle = rtnetlink_handle("open rtnetlink connection for address add");
    let index = link_index(&handle, iface)
        .await
        .ok_or_else(|| anyhow::anyhow!("interface {iface} not found"))?;
    handle
        .address()
        .add(index, ip, prefix_len)
        .execute()
        .await
        .with_context(|| format!("add address {ip}/{prefix_len} to {iface}"))
}

/// Delete one IP address from the requested interface through rtnetlink.
pub async fn delete_interface_address(iface: &str, ip: IpAddr, prefix_len: u8) -> Result<()> {
    let handle = rtnetlink_handle("open rtnetlink connection for address delete");
    let Some(index) = link_index(&handle, iface).await else {
        return Ok(());
    };
    let mut addresses = handle
        .address()
        .get()
        .set_link_index_filter(index)
        .execute();

    while let Some(msg) = addresses
        .try_next()
        .await
        .with_context(|| format!("enumerate addresses for {iface} delete"))?
    {
        if msg.header.prefix_len != prefix_len || !address_attrs_contain_ip(&msg, ip) {
            continue;
        }
        handle
            .address()
            .del(msg)
            .execute()
            .await
            .with_context(|| format!("delete address {ip}/{prefix_len} from {iface}"))?;
        return Ok(());
    }

    Ok(())
}

/// Return true when one address message carries the provided IP address.
fn address_attrs_contain_ip(message: &AddressMessage, ip: IpAddr) -> bool {
    message.attributes.iter().any(|attr| match attr {
        AddressAttribute::Address(addr) | AddressAttribute::Local(addr) => *addr == ip,
        _ => false,
    })
}

/// Return whether an interface has any XDP program attached according to rtnetlink.
pub async fn link_has_xdp(iface: &str) -> bool {
    let handle = rtnetlink_handle("open rtnetlink connection for XDP link lookup");
    let Some(link) = handle
        .link()
        .get()
        .match_name(iface.to_string())
        .execute()
        .try_next()
        .await
        .unwrap_or_else(|err| panic!("query interface {iface} for XDP: {err}"))
    else {
        return false;
    };
    link.attributes.iter().any(|attr| match attr {
        LinkAttribute::Xdp(entries) => entries.iter().any(link_xdp_attached),
        _ => false,
    })
}

/// Return whether one XDP link attribute reports an attached program.
fn link_xdp_attached(entry: &LinkXdp) -> bool {
    match entry {
        LinkXdp::Attached(attached) => *attached != XdpAttached::None,
        LinkXdp::ProgId(id)
        | LinkXdp::DrvProgId(id)
        | LinkXdp::SkbProgId(id)
        | LinkXdp::HwProgId(id) => *id != 0,
        _ => false,
    }
}

/// Build a compact structured link summary for privileged failure diagnostics.
pub async fn link_summary(iface: &str) -> String {
    let handle = rtnetlink_handle("open rtnetlink connection for link summary");
    let Some(link) = handle
        .link()
        .get()
        .match_name(iface.to_string())
        .execute()
        .try_next()
        .await
        .unwrap_or_else(|err| panic!("query interface {iface} summary: {err}"))
    else {
        return format!("{iface}: <missing>");
    };
    let mtu = link.attributes.iter().find_map(|attr| match attr {
        LinkAttribute::Mtu(mtu) => Some(*mtu),
        _ => None,
    });
    let qdisc = link.attributes.iter().find_map(|attr| match attr {
        LinkAttribute::Qdisc(qdisc) => Some(qdisc.as_str()),
        _ => None,
    });
    let has_xdp = link.attributes.iter().any(|attr| match attr {
        LinkAttribute::Xdp(entries) => entries.iter().any(link_xdp_attached),
        _ => false,
    });
    format!(
        "{iface}: index={} flags={:?} mtu={mtu:?} qdisc={qdisc:?} xdp={has_xdp}",
        link.header.index, link.header.flags
    )
}

/// Build a compact address summary for privileged failure diagnostics.
pub async fn interface_addresses_summary(iface: &str) -> String {
    let handle = rtnetlink_handle("open rtnetlink connection for address summary");
    let Some(index) = link_index(&handle, iface).await else {
        return format!("{iface}: <missing>");
    };
    let mut addresses = handle
        .address()
        .get()
        .set_link_index_filter(index)
        .execute();
    let mut out = Vec::new();

    while let Some(msg) = addresses
        .try_next()
        .await
        .unwrap_or_else(|err| panic!("enumerate addresses for {iface} summary: {err}"))
    {
        for attr in &msg.attributes {
            if let AddressAttribute::Address(addr) | AddressAttribute::Local(addr) = attr {
                out.push(format!("{addr}/{}", msg.header.prefix_len));
            }
        }
    }

    format!("{iface}: [{}]", out.join(", "))
}

/// Return whether one interface has a permanent neighbour entry for the destination IP.
pub async fn permanent_neighbour_exists(iface: &str, ip: IpAddr) -> bool {
    neighbour_exists_with_state(iface, ip, Some(NeighbourState::Permanent)).await
}

/// Return whether one interface has any neighbour entry for the destination IP.
pub async fn neighbour_exists(iface: &str, ip: IpAddr) -> bool {
    neighbour_exists_with_state(iface, ip, None).await
}

/// Return whether one interface has a neighbour entry, optionally matching one state.
async fn neighbour_exists_with_state(
    iface: &str,
    ip: IpAddr,
    state: Option<NeighbourState>,
) -> bool {
    let handle = rtnetlink_handle("open rtnetlink connection for neighbour lookup");
    let Some(index) = link_index(&handle, iface).await else {
        return false;
    };
    let mut neighbours = handle.neighbours().get().execute();

    while let Some(msg) = neighbours
        .try_next()
        .await
        .unwrap_or_else(|err| panic!("enumerate neighbours for {iface}: {err}"))
    {
        if msg.header.ifindex != index {
            continue;
        }
        match &state {
            Some(expected_state) if &msg.header.state != expected_state => continue,
            _ => {}
        }
        if neighbour_destination(&msg.attributes) == Some(ip) {
            return true;
        }
    }

    false
}

/// Build a compact neighbour summary for privileged failure diagnostics.
pub async fn neighbour_summary(iface: &str, ip: IpAddr) -> String {
    let handle = rtnetlink_handle("open rtnetlink connection for neighbour summary");
    let Some(index) = link_index(&handle, iface).await else {
        return format!("{iface}: <missing>");
    };
    let mut neighbours = handle.neighbours().get().execute();
    let mut out = Vec::new();

    while let Some(msg) = neighbours
        .try_next()
        .await
        .unwrap_or_else(|err| panic!("enumerate neighbours for {iface} summary: {err}"))
    {
        if msg.header.ifindex != index {
            continue;
        }
        let Some(destination) = neighbour_destination(&msg.attributes) else {
            continue;
        };
        if destination != ip {
            continue;
        }
        let mac = msg.attributes.iter().find_map(|attr| match attr {
            NeighbourAttribute::LinkLocalAddress(bytes) if bytes.len() == 6 => Some(format!(
                "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
            )),
            _ => None,
        });
        out.push(format!(
            "dst={destination} state={:?} mac={mac:?}",
            msg.header.state
        ));
    }

    format!("{iface}: [{}]", out.join(", "))
}

/// Extract the destination IP from one neighbour attribute set.
fn neighbour_destination(attributes: &[NeighbourAttribute]) -> Option<IpAddr> {
    attributes.iter().find_map(|attr| match attr {
        NeighbourAttribute::Destination(NeighbourAddress::Inet(addr)) => Some(IpAddr::V4(*addr)),
        NeighbourAttribute::Destination(NeighbourAddress::Inet6(addr)) => Some(IpAddr::V6(*addr)),
        _ => None,
    })
}

/// Best-effort delete one kernel link so privileged tests do not leak host interfaces on failure.
fn force_delete_link(iface: &str) {
    let _ = Command::new("ip")
        .args(["link", "delete", "dev", iface])
        .output();
}

/// Force-remove any leftover overlay links after the controller had a chance to clean them up.
fn force_delete_privileged_network_links(interfaces: &[String; 4]) {
    let [vxlan_ifname, bridge_ifname, host_peer_ifname, host_ifname] = interfaces;
    force_delete_link(vxlan_ifname);
    force_delete_link(host_ifname);
    force_delete_link(host_peer_ifname);
    force_delete_link(bridge_ifname);
}

/// Force-remove one leaked privileged test network from the host during panic-path cleanup.
fn force_cleanup_privileged_network_links_sync(network_id: Uuid) {
    let interfaces = privileged_network_interfaces(network_id);
    let pin_dir = privileged_network_bpf_pin_dir(network_id);

    let cleanup_complete =
        || interfaces.iter().all(|iface| !link_exists(iface)) && !pin_dir.exists();

    force_delete_privileged_network_links(&interfaces);
    let _ = std::fs::remove_dir_all(&pin_dir);

    for _ in 0..50 {
        if cleanup_complete() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
        force_delete_privileged_network_links(&interfaces);
        let _ = std::fs::remove_dir_all(&pin_dir);
    }

    eprintln!(
        "warning: forced privileged cleanup left residual network state for {network_id}: interfaces={interfaces:?} pin_dir={}",
        pin_dir.display()
    );
}

/// Force-remove leftover overlay links for one network id and wait until the host is clean.
pub async fn force_cleanup_privileged_network_links(network_id: Uuid) {
    let interfaces = privileged_network_interfaces(network_id);
    let pin_dir = privileged_network_bpf_pin_dir(network_id);
    force_delete_privileged_network_links(&interfaces);
    let _ = std::fs::remove_dir_all(&pin_dir);
    assert!(
        wait_until(Duration::from_secs(5), Duration::from_millis(100), || {
            let interfaces = interfaces.clone();
            let pin_dir = pin_dir.clone();
            async move { interfaces.iter().all(|iface| !link_exists(iface)) && !pin_dir.exists() }
        })
        .await,
        "forced cleanup should remove leftover privileged test network state: interfaces={interfaces:?} pin_dir={}",
        pin_dir.display()
    );
    unregister_privileged_network_cleanup(network_id);
}

/// Return the bpffs directory where one privileged test network pins its eBPF state.
fn privileged_network_bpf_pin_dir(network_id: Uuid) -> PathBuf {
    PathBuf::from("/sys/fs/bpf/mantissa").join(network_id.to_string())
}

/// Delete one privileged test overlay and always finish with explicit host-side cleanup verification.
///
/// The controller should remove the network state on its own, but privileged tests also force the
/// kernel links and bpffs pins away before returning so callers cannot forget the cleanup half of
/// the teardown sequence.
pub async fn delete_privileged_network(node: &HeadlessNode, network_id: Uuid) {
    let Some(mut spec) = node
        .network_registry
        .get_spec(network_id)
        .expect("load privileged test network before delete")
    else {
        return;
    };

    spec.mark_deleted();
    node.network_registry
        .upsert_spec(spec)
        .await
        .expect("mark privileged test network deleted");
    node.network_controller
        .schedule_spec_change(network_id)
        .await;

    let interfaces = privileged_network_interfaces(network_id);
    let pin_dir = privileged_network_bpf_pin_dir(network_id);
    let cleaned_by_controller =
        wait_until(Duration::from_secs(15), Duration::from_millis(100), || {
            let interfaces = interfaces.clone();
            let pin_dir = pin_dir.clone();
            async move {
                let peer_states_gone = node
                    .network_registry
                    .list_peer_states(Some(network_id))
                    .map(|states| states.is_empty())
                    .unwrap_or(false);
                let attachments_gone = node
                    .network_registry
                    .list_attachments(Some(network_id))
                    .map(|attachments| attachments.is_empty())
                    .unwrap_or(false);
                interfaces.iter().all(|iface| !link_exists(iface))
                    && !pin_dir.exists()
                    && peer_states_gone
                    && attachments_gone
            }
        })
        .await;

    force_delete_privileged_network_links(&interfaces);
    let _ = std::fs::remove_dir_all(&pin_dir);
    if !cleaned_by_controller {
        let _ = node
            .network_registry
            .remove_peer_states_for_network(network_id)
            .await;
        let _ = node
            .network_registry
            .remove_attachments_for_network(network_id)
            .await;
    }

    assert!(
        wait_until(Duration::from_secs(5), Duration::from_millis(100), || {
            let interfaces = interfaces.clone();
            let pin_dir = pin_dir.clone();
            async move {
                let peer_states_gone = node
                    .network_registry
                    .list_peer_states(Some(network_id))
                    .map(|states| states.is_empty())
                    .unwrap_or(false);
                let attachments_gone = node
                    .network_registry
                    .list_attachments(Some(network_id))
                    .map(|attachments| attachments.is_empty())
                    .unwrap_or(false);
                interfaces.iter().all(|iface| !link_exists(iface))
                    && !pin_dir.exists()
                    && peer_states_gone
                    && attachments_gone
            }
        })
        .await,
        "privileged test network {network_id} should tear down kernel state: interfaces={interfaces:?} pin_dir={}",
        pin_dir.display()
    );
    assert!(
        cleaned_by_controller,
        "privileged test network {network_id} required forced cleanup; interfaces={interfaces:?} pin_dir={}",
        pin_dir.display()
    );
    unregister_privileged_network_cleanup(network_id);
}

/// Run one host command and fail fast when it exits unsuccessfully.
pub fn command_output(program: &str, args: &[&str]) -> Output {
    let output = Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("run {program} {args:?}: {err}"));
    assert!(
        output.status.success(),
        "{program} {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    output
}

/// Run one host command and return stdout as UTF-8 text for test assertions.
pub fn command_stdout(program: &str, args: &[&str]) -> String {
    String::from_utf8_lossy(&command_output(program, args).stdout).into_owned()
}
