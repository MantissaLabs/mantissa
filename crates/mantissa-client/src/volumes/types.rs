use anyhow::{Result, anyhow};
use mantissa_protocol::volumes::{
    LocalVolumeSourceKind, VolumeAccessMode as ProtoVolumeAccessMode,
    VolumeBindingMode as ProtoVolumeBindingMode, VolumeNodeState as ProtoVolumeNodeState,
    VolumeReclaimPolicy as ProtoVolumeReclaimPolicy, VolumeStatus as ProtoVolumeStatus,
    local_volume_ownership, volume_driver_spec, volume_inspect, volume_node_status, volume_spec,
    volume_summary,
};
use serde::Deserialize;
use std::fmt;
use uuid::Uuid;

/// Client-side representation of one volume label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeLabel {
    pub key: String,
    pub value: String,
}

/// Client-side ownership policy for one Mantissa-managed local volume.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalVolumeOwnership {
    #[default]
    Daemon,
    User {
        uid: u32,
        gid: u32,
    },
    FsGroup {
        gid: u32,
    },
}

impl fmt::Display for LocalVolumeOwnership {
    /// Renders the ownership policy in one compact operator-facing form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Daemon => f.write_str("daemon"),
            Self::User { uid, gid } => write!(f, "user(uid={uid},gid={gid})"),
            Self::FsGroup { gid } => write!(f, "fs_group(gid={gid})"),
        }
    }
}

/// Client-side representation of one volume driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumeDriver {
    LocalManaged,
    LocalImportedPath(String),
    External { driver_name: String, handle: String },
}

impl fmt::Display for VolumeDriver {
    /// Renders the driver in a compact operator-facing form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalManaged => f.write_str("local(managed)"),
            Self::LocalImportedPath(path) => write!(f, "local(imported:{path})"),
            Self::External { driver_name, .. } => write!(f, "external({driver_name})"),
        }
    }
}

/// Client-side representation of one volume access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeAccessMode {
    ReadWriteOnce,
}

impl VolumeAccessMode {
    /// Decodes the protocol enum into the client-side representation.
    pub fn from_proto(mode: ProtoVolumeAccessMode) -> Self {
        match mode {
            ProtoVolumeAccessMode::ReadWriteOnce => Self::ReadWriteOnce,
        }
    }
}

impl fmt::Display for VolumeAccessMode {
    /// Renders the access mode in the CLI-friendly form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadWriteOnce => f.write_str("read_write_once"),
        }
    }
}

/// Client-side representation of one volume binding mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeBindingMode {
    Immediate,
    WaitForFirstConsumer,
}

impl VolumeBindingMode {
    /// Decodes the protocol enum into the client-side representation.
    pub fn from_proto(mode: ProtoVolumeBindingMode) -> Self {
        match mode {
            ProtoVolumeBindingMode::Immediate => Self::Immediate,
            ProtoVolumeBindingMode::WaitForFirstConsumer => Self::WaitForFirstConsumer,
        }
    }
}

impl fmt::Display for VolumeBindingMode {
    /// Renders the binding mode in the CLI-friendly form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Immediate => f.write_str("immediate"),
            Self::WaitForFirstConsumer => f.write_str("wait_for_first_consumer"),
        }
    }
}

/// Client-side representation of one volume reclaim policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeReclaimPolicy {
    Retain,
    Delete,
}

impl VolumeReclaimPolicy {
    /// Decodes the protocol enum into the client-side representation.
    pub fn from_proto(policy: ProtoVolumeReclaimPolicy) -> Self {
        match policy {
            ProtoVolumeReclaimPolicy::Retain => Self::Retain,
            ProtoVolumeReclaimPolicy::Delete => Self::Delete,
        }
    }
}

impl fmt::Display for VolumeReclaimPolicy {
    /// Renders the reclaim policy in the CLI-friendly form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retain => f.write_str("retain"),
            Self::Delete => f.write_str("delete"),
        }
    }
}

/// Client-side representation of one cluster volume status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeStatus {
    Pending,
    Bound,
    Ready,
    InUse,
    Deleting,
    Failed,
}

impl VolumeStatus {
    /// Decodes the protocol enum into the client-side representation.
    pub fn from_proto(status: ProtoVolumeStatus) -> Self {
        match status {
            ProtoVolumeStatus::Pending => Self::Pending,
            ProtoVolumeStatus::Bound => Self::Bound,
            ProtoVolumeStatus::Ready => Self::Ready,
            ProtoVolumeStatus::InUse => Self::InUse,
            ProtoVolumeStatus::Deleting => Self::Deleting,
            ProtoVolumeStatus::Failed => Self::Failed,
        }
    }
}

impl fmt::Display for VolumeStatus {
    /// Renders the volume status in the CLI-friendly form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Bound => f.write_str("bound"),
            Self::Ready => f.write_str("ready"),
            Self::InUse => f.write_str("in_use"),
            Self::Deleting => f.write_str("deleting"),
            Self::Failed => f.write_str("failed"),
        }
    }
}

/// Client-side representation of one node-local volume state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeNodeState {
    Pending,
    Provisioning,
    Ready,
    Published,
    Deleting,
    Error,
}

impl VolumeNodeState {
    /// Decodes the protocol enum into the client-side representation.
    pub fn from_proto(state: ProtoVolumeNodeState) -> Self {
        match state {
            ProtoVolumeNodeState::Pending => Self::Pending,
            ProtoVolumeNodeState::Provisioning => Self::Provisioning,
            ProtoVolumeNodeState::Ready => Self::Ready,
            ProtoVolumeNodeState::Published => Self::Published,
            ProtoVolumeNodeState::Deleting => Self::Deleting,
            ProtoVolumeNodeState::Error => Self::Error,
        }
    }
}

impl fmt::Display for VolumeNodeState {
    /// Renders the node-local state in the CLI-friendly form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Provisioning => f.write_str("provisioning"),
            Self::Ready => f.write_str("ready"),
            Self::Published => f.write_str("published"),
            Self::Deleting => f.write_str("deleting"),
            Self::Error => f.write_str("error"),
        }
    }
}

/// Client-side summary row used by `mantissa volumes list`.
#[derive(Debug, Clone)]
pub struct VolumeSummary {
    pub id: Uuid,
    pub name: String,
    pub driver: VolumeDriver,
    pub local_ownership: Option<LocalVolumeOwnership>,
    pub access_mode: VolumeAccessMode,
    pub binding_mode: VolumeBindingMode,
    pub reclaim_policy: VolumeReclaimPolicy,
    pub status: VolumeStatus,
    pub bound_node_id: Option<Uuid>,
    pub bound_node_name: Option<String>,
    pub requested_bytes: Option<u64>,
    pub in_use: bool,
    pub reason: Option<String>,
    pub updated_at: String,
}

/// Client-side representation of the canonical persisted volume object.
#[derive(Debug, Clone)]
pub struct VolumeSpec {
    pub id: Uuid,
    pub name: String,
    pub driver: VolumeDriver,
    pub local_ownership: Option<LocalVolumeOwnership>,
    pub access_mode: VolumeAccessMode,
    pub binding_mode: VolumeBindingMode,
    pub reclaim_policy: VolumeReclaimPolicy,
    pub requested_bytes: Option<u64>,
    pub labels: Vec<VolumeLabel>,
    pub status: VolumeStatus,
    pub bound_node_id: Option<Uuid>,
    pub bound_node_name: Option<String>,
    pub volume_epoch: u64,
    pub phase_version: u64,
    pub created_at: String,
    pub updated_at: String,
    pub reason: Option<String>,
    pub message: Option<String>,
}

/// Client-side representation of one node-local volume status row.
#[derive(Debug, Clone)]
pub struct VolumeNodeStatus {
    pub id: Uuid,
    pub volume_id: Uuid,
    pub node_id: Uuid,
    pub node_name: String,
    pub local_path: Option<String>,
    pub state: VolumeNodeState,
    pub capacity_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub published_task_ids: Vec<Uuid>,
    pub updated_at: String,
    pub last_error: Option<String>,
}

/// Client-side inspect payload returned by `get` and `getStatus`.
#[derive(Debug, Clone)]
pub struct VolumeInspect {
    pub spec: VolumeSpec,
    pub node_states: Vec<VolumeNodeStatus>,
}

/// Client-side delete result payload.
#[derive(Debug, Clone)]
pub struct VolumeDeleteResult {
    pub preserved_path: Option<String>,
    pub deleted_data: bool,
}

impl VolumeSummary {
    /// Decodes one list summary row from the protocol payload.
    pub fn from_reader(reader: volume_summary::Reader<'_>) -> Result<Self> {
        let (driver, local_ownership) = parse_driver(reader.get_driver()?)?;
        Ok(Self {
            id: read_uuid(reader.get_id()?, "volume id")?,
            name: reader.get_name()?.to_str()?.to_string(),
            driver,
            local_ownership,
            access_mode: VolumeAccessMode::from_proto(reader.get_access_mode()?),
            binding_mode: VolumeBindingMode::from_proto(reader.get_binding_mode()?),
            reclaim_policy: VolumeReclaimPolicy::from_proto(reader.get_reclaim_policy()?),
            status: VolumeStatus::from_proto(reader.get_status()?),
            bound_node_id: read_optional_uuid(reader.get_bound_node_id()?, "bound node id")?,
            bound_node_name: empty_text(reader.get_bound_node_name()?.to_str()?),
            requested_bytes: zero_means_none(reader.get_requested_bytes()),
            in_use: reader.get_in_use(),
            reason: empty_text(reader.get_reason()?.to_str()?),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        })
    }
}

impl VolumeSpec {
    /// Decodes one canonical volume spec from the protocol payload.
    pub fn from_reader(reader: volume_spec::Reader<'_>) -> Result<Self> {
        let (driver, local_ownership) = parse_driver(reader.get_driver()?)?;
        let mut labels = Vec::new();
        for entry in reader.get_labels()?.iter() {
            labels.push(VolumeLabel {
                key: entry.get_key()?.to_str()?.to_string(),
                value: entry.get_value()?.to_str()?.to_string(),
            });
        }
        labels.sort_by(|a, b| a.key.cmp(&b.key).then(a.value.cmp(&b.value)));

        Ok(Self {
            id: read_uuid(reader.get_id()?, "volume id")?,
            name: reader.get_name()?.to_str()?.to_string(),
            driver,
            local_ownership,
            access_mode: VolumeAccessMode::from_proto(reader.get_access_mode()?),
            binding_mode: VolumeBindingMode::from_proto(reader.get_binding_mode()?),
            reclaim_policy: VolumeReclaimPolicy::from_proto(reader.get_reclaim_policy()?),
            requested_bytes: zero_means_none(reader.get_requested_bytes()),
            labels,
            status: VolumeStatus::from_proto(reader.get_status()?),
            bound_node_id: read_optional_uuid(reader.get_bound_node_id()?, "bound node id")?,
            bound_node_name: empty_text(reader.get_bound_node_name()?.to_str()?),
            volume_epoch: reader.get_volume_epoch(),
            phase_version: reader.get_phase_version(),
            created_at: reader.get_created_at()?.to_str()?.to_string(),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
            reason: empty_text(reader.get_reason()?.to_str()?),
            message: empty_text(reader.get_message()?.to_str()?),
        })
    }
}

impl VolumeNodeStatus {
    /// Decodes one node-local status row from the protocol payload.
    pub fn from_reader(reader: volume_node_status::Reader<'_>) -> Result<Self> {
        let mut published_task_ids = Vec::new();
        for task_id in reader.get_published_task_ids()?.iter() {
            published_task_ids.push(read_uuid(task_id?, "published task id")?);
        }

        Ok(Self {
            id: read_uuid(reader.get_id()?, "volume node-state id")?,
            volume_id: read_uuid(reader.get_volume_id()?, "volume id")?,
            node_id: read_uuid(reader.get_node_id()?, "node id")?,
            node_name: reader.get_node_name()?.to_str()?.to_string(),
            local_path: empty_text(reader.get_local_path()?.to_str()?),
            state: VolumeNodeState::from_proto(reader.get_state()?),
            capacity_bytes: zero_means_none(reader.get_capacity_bytes()),
            used_bytes: zero_means_none(reader.get_used_bytes()),
            published_task_ids,
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
            last_error: empty_text(reader.get_last_error()?.to_str()?),
        })
    }
}

impl VolumeInspect {
    /// Decodes one inspect payload from the protocol response.
    pub fn from_reader(reader: volume_inspect::Reader<'_>) -> Result<Self> {
        let spec = VolumeSpec::from_reader(reader.get_spec()?)?;
        let mut node_states = Vec::new();
        for entry in reader.get_node_states()?.iter() {
            node_states.push(VolumeNodeStatus::from_reader(entry)?);
        }
        node_states.sort_by(|a, b| {
            a.node_name
                .cmp(&b.node_name)
                .then(a.node_id.cmp(&b.node_id))
        });
        Ok(Self { spec, node_states })
    }
}

/// Formats one optional byte count for CLI output.
pub fn format_bytes(bytes: Option<u64>) -> String {
    let Some(bytes) = bytes else {
        return "-".to_string();
    };
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit_idx = 0usize;
    while value >= 1024.0 && unit_idx < UNITS.len() - 1 {
        value /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{} {}", bytes, UNITS[unit_idx])
    } else {
        format!("{value:.1} {}", UNITS[unit_idx])
    }
}

/// Formats one list of task identifiers for CLI diagnostics.
pub fn format_task_ids(task_ids: &[Uuid]) -> String {
    if task_ids.is_empty() {
        return "-".to_string();
    }

    task_ids
        .iter()
        .map(Uuid::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Decodes one 16-byte UUID payload from the wire.
fn read_uuid(bytes: capnp::data::Reader<'_>, field: &str) -> Result<Uuid> {
    let data = bytes.to_owned();
    if data.len() != 16 {
        return Err(anyhow!(
            "{field}: invalid uuid length (expected 16, got {})",
            data.len()
        ));
    }
    Uuid::from_slice(&data).map_err(|e| anyhow!(e.to_string()))
}

/// Decodes an optional UUID payload from the wire, returning `None` when empty.
fn read_optional_uuid(bytes: capnp::data::Reader<'_>, field: &str) -> Result<Option<Uuid>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        read_uuid(bytes, field).map(Some)
    }
}

/// Converts zero-valued numeric fields used as wire sentinels into `None`.
fn zero_means_none(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

/// Converts empty wire text into `None` for optional fields.
fn empty_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Decodes one volume driver payload from the protocol response.
fn parse_driver(
    reader: volume_driver_spec::Reader<'_>,
) -> Result<(VolumeDriver, Option<LocalVolumeOwnership>)> {
    match reader.which()? {
        volume_driver_spec::Which::Local(Ok(local_reader)) => {
            match local_reader.get_source_kind()? {
                LocalVolumeSourceKind::Managed => Ok((
                    VolumeDriver::LocalManaged,
                    Some(parse_local_volume_ownership(local_reader.get_ownership()?)?),
                )),
                LocalVolumeSourceKind::ImportedPath => Ok((
                    VolumeDriver::LocalImportedPath(
                        local_reader.get_imported_path()?.to_str()?.to_string(),
                    ),
                    None,
                )),
            }
        }
        volume_driver_spec::Which::Local(Err(err)) => Err(anyhow!(err.to_string())),
        volume_driver_spec::Which::External(Ok(external_reader)) => Ok((
            VolumeDriver::External {
                driver_name: external_reader.get_driver_name()?.to_str()?.to_string(),
                handle: external_reader.get_handle()?.to_str()?.to_string(),
            },
            None,
        )),
        volume_driver_spec::Which::External(Err(err)) => Err(anyhow!(err.to_string())),
    }
}

/// Decodes one managed-volume ownership payload from the protocol response.
fn parse_local_volume_ownership(
    reader: local_volume_ownership::Reader<'_>,
) -> Result<LocalVolumeOwnership> {
    match reader.which()? {
        local_volume_ownership::Which::Daemon(()) => Ok(LocalVolumeOwnership::Daemon),
        local_volume_ownership::Which::User(Ok(user)) => Ok(LocalVolumeOwnership::User {
            uid: user.get_uid(),
            gid: user.get_gid(),
        }),
        local_volume_ownership::Which::User(Err(err)) => Err(anyhow!(err.to_string())),
        local_volume_ownership::Which::FsGroup(Ok(fs_group)) => Ok(LocalVolumeOwnership::FsGroup {
            gid: fs_group.get_gid(),
        }),
        local_volume_ownership::Which::FsGroup(Err(err)) => Err(anyhow!(err.to_string())),
    }
}
