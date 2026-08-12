use anyhow::{Result, anyhow};
use mantissa_protocol::health::NodeStatus as ProtoNodeHealth;
use mantissa_protocol::volumes::{
    VolumeAccessMode as ProtoVolumeAccessMode, VolumeBindingMode as ProtoVolumeBindingMode,
    VolumeDeleteDisposition as ProtoVolumeDeleteDisposition,
    VolumeNodeState as ProtoVolumeNodeState, VolumeReclaimPolicy as ProtoVolumeReclaimPolicy,
    VolumeState as ProtoVolumeState, VolumeStatus as ProtoVolumeStatus, filesystem_ownership,
    local_volume_spec, replicated_volume_group_status, replicated_volume_plan, volume_driver_spec,
    volume_expand_result, volume_filesystem_space, volume_inspect, volume_node_status, volume_spec,
    volume_summary,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Client-side representation of one volume label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeLabel {
    pub key: String,
    pub value: String,
}

/// Client-side ownership policy for one Mantissa-managed filesystem.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum FilesystemOwnership {
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

impl fmt::Display for FilesystemOwnership {
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
    Replicated(ReplicatedVolumeFilesystem),
}

impl fmt::Display for VolumeDriver {
    /// Renders the driver in a compact operator-facing form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalManaged => f.write_str("local(managed)"),
            Self::LocalImportedPath(path) => write!(f, "local(imported:{path})"),
            Self::External { driver_name, .. } => write!(f, "external({driver_name})"),
            Self::Replicated(_) => f.write_str("replicated"),
        }
    }
}

/// Filesystem created inside one replicated block volume.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ReplicatedVolumeFilesystem {
    /// Linux ext4 expanded online with resize2fs.
    #[default]
    Ext4,

    /// Linux XFS expanded online with xfs_growfs.
    Xfs,
}

impl fmt::Display for ReplicatedVolumeFilesystem {
    /// Renders the filesystem name accepted by manifests and the CLI.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ext4 => f.write_str("ext4"),
            Self::Xfs => f.write_str("xfs"),
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
    Retaining,
    Retained,
    Restoring,
    Failed,
    Deleted,
}

/// Clear public state shown by list, inspect, status, and REST responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeState {
    Pending,
    WaitingForConsumer,
    CreatingReplicas,
    Ready,
    Attached,
    Degraded,
    Failed,
    Retaining,
    Retained,
    Restoring,
    Deleted,
    Unavailable,
}

impl VolumeState {
    /// Decodes the public state calculated by the server.
    fn from_proto(state: ProtoVolumeState) -> Self {
        match state {
            ProtoVolumeState::Pending => Self::Pending,
            ProtoVolumeState::WaitingForConsumer => Self::WaitingForConsumer,
            ProtoVolumeState::CreatingReplicas => Self::CreatingReplicas,
            ProtoVolumeState::Ready => Self::Ready,
            ProtoVolumeState::Attached => Self::Attached,
            ProtoVolumeState::Degraded => Self::Degraded,
            ProtoVolumeState::Failed => Self::Failed,
            ProtoVolumeState::Retaining => Self::Retaining,
            ProtoVolumeState::Retained => Self::Retained,
            ProtoVolumeState::Restoring => Self::Restoring,
            ProtoVolumeState::Deleted => Self::Deleted,
            ProtoVolumeState::Unavailable => Self::Unavailable,
        }
    }
}

impl fmt::Display for VolumeState {
    /// Renders the public state in the CLI and REST form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::WaitingForConsumer => f.write_str("waiting_for_consumer"),
            Self::CreatingReplicas => f.write_str("creating_replicas"),
            Self::Ready => f.write_str("ready"),
            Self::Attached => f.write_str("attached"),
            Self::Degraded => f.write_str("degraded"),
            Self::Failed => f.write_str("failed"),
            Self::Retaining => f.write_str("retaining"),
            Self::Retained => f.write_str("retained"),
            Self::Restoring => f.write_str("restoring"),
            Self::Deleted => f.write_str("deleted"),
            Self::Unavailable => f.write_str("unavailable"),
        }
    }
}

impl VolumeStatus {
    /// Decodes the protocol enum into the client-side representation.
    pub fn from_proto(status: ProtoVolumeStatus) -> Self {
        match status {
            ProtoVolumeStatus::Pending => Self::Pending,
            ProtoVolumeStatus::Bound => Self::Bound,
            ProtoVolumeStatus::Ready => Self::Ready,
            ProtoVolumeStatus::InUse => Self::InUse,
            ProtoVolumeStatus::Retaining => Self::Retaining,
            ProtoVolumeStatus::Retained => Self::Retained,
            ProtoVolumeStatus::Restoring => Self::Restoring,
            ProtoVolumeStatus::Failed => Self::Failed,
            ProtoVolumeStatus::Deleted => Self::Deleted,
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
            Self::Retaining => f.write_str("retaining"),
            Self::Retained => f.write_str("retained"),
            Self::Restoring => f.write_str("restoring"),
            Self::Failed => f.write_str("failed"),
            Self::Deleted => f.write_str("deleted"),
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
    Retained,
    Deleting,
    Error,
}

/// Current health of a node that stores one volume copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeHealth {
    Unknown,
    Alive,
    Suspect,
    Down,
    Degraded,
}

impl NodeHealth {
    /// Decodes the node health observed by the daemon serving the response.
    fn from_proto(health: ProtoNodeHealth) -> Self {
        match health {
            ProtoNodeHealth::Unknown => Self::Unknown,
            ProtoNodeHealth::Alive => Self::Alive,
            ProtoNodeHealth::Suspect => Self::Suspect,
            ProtoNodeHealth::Down => Self::Down,
            ProtoNodeHealth::Degraded => Self::Degraded,
        }
    }
}

impl fmt::Display for NodeHealth {
    /// Renders node health in CLI and REST responses.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("unknown"),
            Self::Alive => f.write_str("alive"),
            Self::Suspect => f.write_str("suspect"),
            Self::Down => f.write_str("down"),
            Self::Degraded => f.write_str("degraded"),
        }
    }
}

impl VolumeNodeState {
    /// Decodes the protocol enum into the client-side representation.
    pub fn from_proto(state: ProtoVolumeNodeState) -> Self {
        match state {
            ProtoVolumeNodeState::Pending => Self::Pending,
            ProtoVolumeNodeState::Provisioning => Self::Provisioning,
            ProtoVolumeNodeState::Ready => Self::Ready,
            ProtoVolumeNodeState::Published => Self::Published,
            ProtoVolumeNodeState::Retained => Self::Retained,
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
            Self::Retained => f.write_str("retained"),
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
    pub filesystem_ownership: Option<FilesystemOwnership>,
    pub access_mode: VolumeAccessMode,
    pub binding_mode: VolumeBindingMode,
    pub reclaim_policy: VolumeReclaimPolicy,
    pub status: VolumeStatus,
    pub state: VolumeState,
    pub bound_node_id: Option<Uuid>,
    pub bound_node_name: Option<String>,
    pub initial_capacity_bytes: Option<u64>,
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
    pub filesystem_ownership: Option<FilesystemOwnership>,
    pub access_mode: VolumeAccessMode,
    pub binding_mode: VolumeBindingMode,
    pub reclaim_policy: VolumeReclaimPolicy,
    pub initial_capacity_bytes: Option<u64>,
    pub labels: Vec<VolumeLabel>,
    pub bound_node_id: Option<Uuid>,
    pub bound_node_name: Option<String>,
    pub volume_epoch: u64,
    pub lifecycle_revision: u64,
    pub lifecycle_request_id: Uuid,
    pub desired_disposition: DesiredVolumeDisposition,
    pub remove_data: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// Client representation of the requested lifecycle outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DesiredVolumeDisposition {
    Live,
    Retained,
    Deleted,
}

impl fmt::Display for DesiredVolumeDisposition {
    /// Renders desired state in CLI and REST output.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Live => f.write_str("live"),
            Self::Retained => f.write_str("retained"),
            Self::Deleted => f.write_str("deleted"),
        }
    }
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
    pub health: NodeHealth,
    pub capacity_bytes: Option<u64>,
    pub reserved_capacity_bytes: Option<u64>,
    pub prepared_capacity_bytes: Option<u64>,
    pub served_capacity_bytes: Option<u64>,
    pub device_capacity_bytes: Option<u64>,
    pub filesystem_expansion_pending: bool,
    pub used_bytes: Option<u64>,
    pub published_task_ids: Vec<Uuid>,
    pub updated_at: String,
    pub last_error: Option<String>,
    pub volume_epoch: u64,
    pub group_id: Option<Uuid>,
}

/// Immutable genesis input shared by the three replica nodes.
#[derive(Debug, Clone)]
pub struct ReplicatedVolumePlan {
    pub id: Uuid,
    pub volume_id: Uuid,
    pub volume_epoch: u64,
    pub bootstrap_id: Uuid,
    pub workload_node_id: Uuid,
    pub replica_node_ids: [Uuid; 3],
    pub generation: u64,
    pub initial_capacity_bytes: u64,
    pub logical_sector_bytes: u32,
    pub physical_block_bytes: u32,
    pub minimum_io_bytes: u32,
    pub data_block_bytes: u32,
}

/// Latest public status copied from the replicated volume's Raft state.
#[derive(Debug, Clone)]
pub struct ReplicatedVolumeGroupStatus {
    pub id: Uuid,
    pub volume_id: Uuid,
    pub volume_epoch: u64,
    pub group_id: Uuid,
    pub reporter_node_id: Uuid,
    pub status: VolumeStatus,
    pub committed_index: u64,
    pub leader_node_id: Option<Uuid>,
    pub attached_node_id: Option<Uuid>,
    pub updated_at: String,
    pub message: Option<String>,
    pub control_revision: u64,
    pub replicated_capacity_bytes: u64,
    pub fence: Option<u64>,
    pub copy_node_ids: Vec<Uuid>,
    pub voter_node_ids: Vec<Uuid>,
    pub replacement_id: Option<Uuid>,
    pub replacement_old_node_id: Option<Uuid>,
    pub replacement_new_node_id: Option<Uuid>,
    pub degraded: bool,
}

/// Client-side inspect payload returned by `get` and `getStatus`.
#[derive(Debug, Clone)]
pub struct VolumeInspect {
    pub state: VolumeState,
    pub state_message: Option<String>,
    pub spec: VolumeSpec,
    pub node_states: Vec<VolumeNodeStatus>,
    pub plan: Option<ReplicatedVolumePlan>,
    pub group_status: Option<ReplicatedVolumeGroupStatus>,
    pub filesystem_space: Option<VolumeFilesystemSpace>,
    pub desired_capacity_bytes: Option<u64>,
}

/// Live filesystem space measured on the mounted replicated-volume writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeFilesystemSpace {
    pub writer_node_id: Uuid,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
}

impl VolumeFilesystemSpace {
    /// Decodes one live filesystem-space measurement from its writer node.
    fn from_reader(reader: volume_filesystem_space::Reader<'_>) -> Result<Self> {
        let total_bytes = reader.get_total_bytes();
        let used_bytes = reader.get_used_bytes();
        let available_bytes = reader.get_available_bytes();
        if total_bytes == 0 || used_bytes > total_bytes || available_bytes > total_bytes {
            return Err(anyhow!("filesystem space contains invalid byte counters"));
        }
        Ok(Self {
            writer_node_id: read_uuid(reader.get_writer_node_id()?, "filesystem writer node id")?,
            total_bytes,
            used_bytes,
            available_bytes,
        })
    }
}

/// Client-side delete result payload.
#[derive(Debug, Clone)]
pub struct VolumeDeleteResult {
    pub preserved_path: Option<String>,
    pub disposition: VolumeDeleteDisposition,
}

/// Durable result returned after saving one desired capacity request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeExpandResult {
    pub volume_id: Uuid,
    pub initial_capacity_bytes: u64,
    pub desired_capacity_bytes: u64,
    pub replicated_capacity_bytes: u64,
    pub desired_capacity_changed: bool,
}

impl VolumeExpandResult {
    /// Decodes the accepted desired target and best known committed capacity.
    pub fn from_reader(reader: volume_expand_result::Reader<'_>) -> Result<Self> {
        Ok(Self {
            volume_id: read_uuid(reader.get_volume_id()?, "volume id")?,
            initial_capacity_bytes: reader.get_initial_capacity_bytes(),
            desired_capacity_bytes: reader.get_desired_capacity_bytes(),
            replicated_capacity_bytes: reader.get_replicated_capacity_bytes(),
            desired_capacity_changed: reader.get_desired_capacity_changed(),
        })
    }
}

/// Logical outcome accepted by one volume delete request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeDeleteDisposition {
    Deleted,
    Retained,
}

impl VolumeDeleteDisposition {
    /// Decodes the protocol outcome without inferring physical cleanup progress.
    pub fn from_proto(disposition: ProtoVolumeDeleteDisposition) -> Self {
        match disposition {
            ProtoVolumeDeleteDisposition::Deleted => Self::Deleted,
            ProtoVolumeDeleteDisposition::Retained => Self::Retained,
        }
    }
}

impl VolumeSummary {
    /// Decodes one list summary row from the protocol payload.
    pub fn from_reader(reader: volume_summary::Reader<'_>) -> Result<Self> {
        let (driver, filesystem_ownership) = parse_driver(reader.get_driver()?)?;
        Ok(Self {
            id: read_uuid(reader.get_id()?, "volume id")?,
            name: reader.get_name()?.to_str()?.to_string(),
            driver,
            filesystem_ownership,
            access_mode: VolumeAccessMode::from_proto(reader.get_access_mode()?),
            binding_mode: VolumeBindingMode::from_proto(reader.get_binding_mode()?),
            reclaim_policy: VolumeReclaimPolicy::from_proto(reader.get_reclaim_policy()?),
            status: VolumeStatus::from_proto(reader.get_status()?),
            state: VolumeState::from_proto(reader.get_state()?),
            bound_node_id: read_optional_uuid(reader.get_bound_node_id()?, "bound node id")?,
            bound_node_name: empty_text(reader.get_bound_node_name()?.to_str()?),
            initial_capacity_bytes: zero_means_none(reader.get_initial_capacity_bytes()),
            in_use: reader.get_in_use(),
            reason: empty_text(reader.get_reason()?.to_str()?),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        })
    }
}

impl VolumeSpec {
    /// Decodes one canonical volume spec from the protocol payload.
    pub fn from_reader(reader: volume_spec::Reader<'_>) -> Result<Self> {
        let (driver, filesystem_ownership) = parse_driver(reader.get_driver()?)?;
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
            filesystem_ownership,
            access_mode: VolumeAccessMode::from_proto(reader.get_access_mode()?),
            binding_mode: VolumeBindingMode::from_proto(reader.get_binding_mode()?),
            reclaim_policy: VolumeReclaimPolicy::from_proto(reader.get_reclaim_policy()?),
            initial_capacity_bytes: zero_means_none(reader.get_initial_capacity_bytes()),
            labels,
            bound_node_id: read_optional_uuid(reader.get_bound_node_id()?, "bound node id")?,
            bound_node_name: empty_text(reader.get_bound_node_name()?.to_str()?),
            volume_epoch: reader.get_volume_epoch(),
            lifecycle_revision: reader.get_lifecycle()?.get_revision(),
            lifecycle_request_id: read_uuid(
                reader.get_lifecycle()?.get_request_id()?,
                "lifecycle request id",
            )?,
            desired_disposition: match reader.get_lifecycle()?.get_disposition()? {
                mantissa_protocol::volumes::DesiredVolumeDisposition::Live => {
                    DesiredVolumeDisposition::Live
                }
                mantissa_protocol::volumes::DesiredVolumeDisposition::Retained => {
                    DesiredVolumeDisposition::Retained
                }
                mantissa_protocol::volumes::DesiredVolumeDisposition::Deleted => {
                    DesiredVolumeDisposition::Deleted
                }
            },
            remove_data: reader.get_lifecycle()?.get_remove_data(),
            created_at: reader.get_created_at()?.to_str()?.to_string(),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
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
            health: NodeHealth::from_proto(reader.get_health()?),
            capacity_bytes: zero_means_none(reader.get_capacity_bytes()),
            reserved_capacity_bytes: zero_means_none(reader.get_reserved_capacity_bytes()),
            prepared_capacity_bytes: zero_means_none(reader.get_prepared_capacity_bytes()),
            served_capacity_bytes: zero_means_none(reader.get_served_capacity_bytes()),
            device_capacity_bytes: zero_means_none(reader.get_device_capacity_bytes()),
            filesystem_expansion_pending: reader.get_filesystem_expansion_pending(),
            used_bytes: zero_means_none(reader.get_used_bytes()),
            published_task_ids,
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
            last_error: empty_text(reader.get_last_error()?.to_str()?),
            volume_epoch: reader.get_volume_epoch(),
            group_id: read_optional_uuid(reader.get_group_id()?, "volume group id")?,
        })
    }
}

impl ReplicatedVolumePlan {
    /// Decodes immutable replica genesis input from the protocol response.
    fn from_reader(reader: replicated_volume_plan::Reader<'_>) -> Result<Self> {
        let replicas = reader.get_replica_node_ids()?;
        if replicas.len() != 3 {
            return Err(anyhow!(
                "replicated volume plan requires exactly three replica nodes"
            ));
        }
        let descriptor = reader.get_descriptor()?;
        let block_sizes = descriptor.get_block_sizes()?;
        Ok(Self {
            id: read_uuid(reader.get_id()?, "replicated volume plan id")?,
            volume_id: read_uuid(reader.get_volume_id()?, "volume id")?,
            volume_epoch: reader.get_volume_epoch(),
            bootstrap_id: read_uuid(reader.get_bootstrap_id()?, "bootstrap id")?,
            workload_node_id: read_uuid(reader.get_workload_node_id()?, "workload node id")?,
            replica_node_ids: [
                read_uuid(replicas.get(0)?, "first replica node id")?,
                read_uuid(replicas.get(1)?, "second replica node id")?,
                read_uuid(replicas.get(2)?, "third replica node id")?,
            ],
            generation: descriptor.get_generation(),
            initial_capacity_bytes: descriptor.get_capacity_bytes(),
            logical_sector_bytes: block_sizes.get_logical_sector_bytes(),
            physical_block_bytes: block_sizes.get_physical_block_bytes(),
            minimum_io_bytes: block_sizes.get_minimum_io_bytes(),
            data_block_bytes: block_sizes.get_data_block_bytes(),
        })
    }
}

impl ReplicatedVolumeGroupStatus {
    /// Decodes one status copied from committed Raft state.
    fn from_reader(reader: replicated_volume_group_status::Reader<'_>) -> Result<Self> {
        let mut copy_node_ids = Vec::new();
        for value in reader.get_copy_node_ids()?.iter() {
            copy_node_ids.push(read_uuid(value?, "active copy node id")?);
        }
        let mut voter_node_ids = Vec::new();
        for value in reader.get_voter_node_ids()?.iter() {
            voter_node_ids.push(read_uuid(value?, "volume voter node id")?);
        }
        Ok(Self {
            id: read_uuid(reader.get_id()?, "replicated volume group status id")?,
            volume_id: read_uuid(reader.get_volume_id()?, "volume id")?,
            volume_epoch: reader.get_volume_epoch(),
            group_id: read_uuid(reader.get_group_id()?, "volume group id")?,
            reporter_node_id: read_uuid(reader.get_reporter_node_id()?, "reporter node id")?,
            status: VolumeStatus::from_proto(reader.get_status()?),
            committed_index: reader.get_committed_index(),
            leader_node_id: read_optional_uuid(reader.get_leader_node_id()?, "leader node id")?,
            attached_node_id: read_optional_uuid(
                reader.get_attached_node_id()?,
                "attached node id",
            )?,
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
            message: empty_text(reader.get_message()?.to_str()?),
            control_revision: reader.get_control_revision(),
            replicated_capacity_bytes: reader.get_replicated_capacity_bytes(),
            fence: zero_means_none(reader.get_fence()),
            copy_node_ids,
            voter_node_ids,
            replacement_id: read_optional_uuid(reader.get_replacement_id()?, "replacement id")?,
            replacement_old_node_id: read_optional_uuid(
                reader.get_replacement_old_node_id()?,
                "replacement old node id",
            )?,
            replacement_new_node_id: read_optional_uuid(
                reader.get_replacement_new_node_id()?,
                "replacement new node id",
            )?,
            degraded: reader.get_degraded(),
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
        let plan = reader
            .has_plan()
            .then(|| reader.get_plan())
            .transpose()?
            .map(ReplicatedVolumePlan::from_reader)
            .transpose()?;
        let group_status = reader
            .has_group_status()
            .then(|| reader.get_group_status())
            .transpose()?
            .map(ReplicatedVolumeGroupStatus::from_reader)
            .transpose()?;
        let filesystem_space = reader
            .has_filesystem_space()
            .then(|| reader.get_filesystem_space())
            .transpose()?
            .map(VolumeFilesystemSpace::from_reader)
            .transpose()?;
        Ok(Self {
            state: VolumeState::from_proto(reader.get_state()?),
            state_message: empty_text(reader.get_state_message()?.to_str()?),
            spec,
            node_states,
            plan,
            group_status,
            filesystem_space,
            desired_capacity_bytes: zero_means_none(reader.get_desired_capacity_bytes()),
        })
    }
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
) -> Result<(VolumeDriver, Option<FilesystemOwnership>)> {
    match reader.which()? {
        volume_driver_spec::Which::Local(Ok(local_reader)) => match local_reader.which()? {
            local_volume_spec::Which::Managed(Ok(managed_reader)) => Ok((
                VolumeDriver::LocalManaged,
                Some(parse_filesystem_ownership(managed_reader.get_ownership()?)?),
            )),
            local_volume_spec::Which::Managed(Err(err)) => Err(anyhow!(err.to_string())),
            local_volume_spec::Which::ImportedPath(Ok(path)) => Ok((
                VolumeDriver::LocalImportedPath(path.to_str()?.to_string()),
                None,
            )),
            local_volume_spec::Which::ImportedPath(Err(err)) => Err(anyhow!(err.to_string())),
        },
        volume_driver_spec::Which::Local(Err(err)) => Err(anyhow!(err.to_string())),
        volume_driver_spec::Which::External(Ok(external_reader)) => Ok((
            VolumeDriver::External {
                driver_name: external_reader.get_driver_name()?.to_str()?.to_string(),
                handle: external_reader.get_handle()?.to_str()?.to_string(),
            },
            None,
        )),
        volume_driver_spec::Which::External(Err(err)) => Err(anyhow!(err.to_string())),
        volume_driver_spec::Which::Replicated(Ok(replicated_reader)) => Ok((
            VolumeDriver::Replicated(match replicated_reader.get_filesystem()? {
                mantissa_protocol::volumes::ReplicatedVolumeFilesystem::Ext4 => {
                    ReplicatedVolumeFilesystem::Ext4
                }
                mantissa_protocol::volumes::ReplicatedVolumeFilesystem::Xfs => {
                    ReplicatedVolumeFilesystem::Xfs
                }
            }),
            Some(parse_filesystem_ownership(
                replicated_reader.get_ownership()?,
            )?),
        )),
        volume_driver_spec::Which::Replicated(Err(err)) => Err(anyhow!(err.to_string())),
    }
}

/// Decodes one managed-volume ownership payload from the protocol response.
fn parse_filesystem_ownership(
    reader: filesystem_ownership::Reader<'_>,
) -> Result<FilesystemOwnership> {
    match reader.which()? {
        filesystem_ownership::Which::Daemon(()) => Ok(FilesystemOwnership::Daemon),
        filesystem_ownership::Which::User(Ok(user)) => Ok(FilesystemOwnership::User {
            uid: user.get_uid(),
            gid: user.get_gid(),
        }),
        filesystem_ownership::Which::User(Err(err)) => Err(anyhow!(err.to_string())),
        filesystem_ownership::Which::FsGroup(Ok(fs_group)) => Ok(FilesystemOwnership::FsGroup {
            gid: fs_group.get_gid(),
        }),
        filesystem_ownership::Which::FsGroup(Err(err)) => Err(anyhow!(err.to_string())),
    }
}
