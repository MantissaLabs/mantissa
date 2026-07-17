use crate::cluster::ClusterViewId;
use crate::node::id::set_node_id;
use capnp::Error as CapnpError;
use mantissa_store::codec::StoreValueCodec;
use std::cmp::Ordering;
use std::io::Cursor;
use uuid::Uuid;

/// Supported operation kinds for cluster topology restructuring.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ClusterOperationKind {
    Merge,
    Split,
}

impl ClusterOperationKind {
    /// Converts the internal operation kind to the Cap'n Proto representation for RPC responses.
    fn to_capnp(self) -> mantissa_protocol::topology::ClusterOperationKind {
        match self {
            Self::Merge => mantissa_protocol::topology::ClusterOperationKind::Merge,
            Self::Split => mantissa_protocol::topology::ClusterOperationKind::Split,
        }
    }

    /// Converts the Cap'n Proto operation kind into internal durable state.
    fn from_capnp(value: mantissa_protocol::topology::ClusterOperationKind) -> Self {
        match value {
            mantissa_protocol::topology::ClusterOperationKind::Merge => Self::Merge,
            mantissa_protocol::topology::ClusterOperationKind::Split => Self::Split,
        }
    }
}

/// Lifecycle stages for merge/split orchestration operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ClusterOperationStage {
    Proposed,
    Prepared,
    Committed,
    Finalized,
    Aborted,
}

/// Lifecycle precedence for concurrent split and merge operation rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum ClusterOperationStageRank {
    Proposed,
    Prepared,
    Aborted,
    Committed,
    Finalized,
}

/// Service behavior policy applied when a split operation commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub enum SplitServicePolicy {
    /// Keep services active in each resulting partition and prune out-of-scope runtime tasks.
    #[default]
    Partitioned,
    /// Preserve service/task runtime rows as-is after split.
    Preserve,
}

/// Network behavior policy applied when a split operation commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub enum SplitNetworkPolicy {
    /// Isolate overlays per partition by pruning out-of-scope peer and attachment rows.
    #[default]
    Isolate,
    /// Preserve network peer/attachment rows as-is after split.
    Preserve,
}

/// Service behavior policy applied when a merge operation commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub enum MergeServicePolicy {
    /// Trigger post-merge service reconciliation so replicas can rebalance across all nodes.
    #[default]
    Rebalance,
    /// Preserve current service placement without reconciliation hints.
    Preserve,
}

/// Records the deterministic split target index selected for one node.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SplitNodeAssignment {
    pub node_id: Uuid,
    pub target_index: usize,
}

impl ClusterOperationStage {
    /// Returns the monotonic rank used to merge and replay operation stage updates.
    pub(crate) fn rank(self) -> ClusterOperationStageRank {
        match self {
            Self::Proposed => ClusterOperationStageRank::Proposed,
            Self::Prepared => ClusterOperationStageRank::Prepared,
            Self::Aborted => ClusterOperationStageRank::Aborted,
            Self::Committed => ClusterOperationStageRank::Committed,
            Self::Finalized => ClusterOperationStageRank::Finalized,
        }
    }

    /// Converts the internal stage value to the Cap'n Proto representation for RPC responses.
    fn to_capnp(self) -> mantissa_protocol::topology::ClusterOperationStage {
        match self {
            Self::Proposed => mantissa_protocol::topology::ClusterOperationStage::Proposed,
            Self::Prepared => mantissa_protocol::topology::ClusterOperationStage::Prepared,
            Self::Committed => mantissa_protocol::topology::ClusterOperationStage::Committed,
            Self::Finalized => mantissa_protocol::topology::ClusterOperationStage::Finalized,
            Self::Aborted => mantissa_protocol::topology::ClusterOperationStage::Aborted,
        }
    }

    /// Converts the Cap'n Proto stage value into internal durable state.
    fn from_capnp(value: mantissa_protocol::topology::ClusterOperationStage) -> Self {
        match value {
            mantissa_protocol::topology::ClusterOperationStage::Proposed => Self::Proposed,
            mantissa_protocol::topology::ClusterOperationStage::Prepared => Self::Prepared,
            mantissa_protocol::topology::ClusterOperationStage::Committed => Self::Committed,
            mantissa_protocol::topology::ClusterOperationStage::Finalized => Self::Finalized,
            mantissa_protocol::topology::ClusterOperationStage::Aborted => Self::Aborted,
        }
    }
}

/// Durable operation record used to track merge/split intent and progression.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClusterOperationRecord {
    pub id: Uuid,
    /// Stable submitter used as deterministic transition-key derivation context.
    pub submitted_by_node_id: Uuid,
    pub kind: ClusterOperationKind,
    pub stage: ClusterOperationStage,
    pub dry_run: bool,
    /// Stable creation timestamp used for deterministic execution ordering.
    pub created_at_unix_ms: u64,
    /// Immutable causal predecessors supplied with the operation intent.
    pub dependency_operation_ids: Vec<Uuid>,
    pub source_views: Vec<ClusterViewId>,
    pub target_views: Vec<ClusterViewId>,
    pub target_cluster_names: Vec<String>,
    pub split_assignments: Vec<SplitNodeAssignment>,
    pub split_service_policy: SplitServicePolicy,
    pub split_network_policy: SplitNetworkPolicy,
    pub merge_service_policy: MergeServicePolicy,
    /// Last mutation timestamp used for retention ordering and stale-row eviction.
    pub updated_at_unix_ms: u64,
    pub details: String,
}

impl ClusterOperationRecord {
    /// Returns whether two rows describe the same immutable operation intent.
    pub(crate) fn has_same_intent(&self, other: &Self) -> bool {
        self.has_same_identity(other)
            && self.has_same_transition(other)
            && self.has_same_policies(other)
    }

    /// Returns whether two rows share the immutable operation identity and causal frontier.
    fn has_same_identity(&self, other: &Self) -> bool {
        self.id == other.id
            && self.kind == other.kind
            && self.dry_run == other.dry_run
            && self.dependency_operation_ids == other.dependency_operation_ids
    }

    /// Returns whether two rows describe the same source and target cluster transition.
    fn has_same_transition(&self, other: &Self) -> bool {
        self.source_views == other.source_views
            && self.target_views == other.target_views
            && self.target_cluster_names == other.target_cluster_names
            && self.split_assignments == other.split_assignments
    }

    /// Returns whether two rows carry the same split and merge behavior policies.
    fn has_same_policies(&self, other: &Self) -> bool {
        self.split_service_policy == other.split_service_policy
            && self.split_network_policy == other.split_network_policy
            && self.merge_service_policy == other.merge_service_policy
    }

    /// Returns whether this row should replace `current` for the same operation id.
    pub fn supersedes(&self, current: &Self) -> bool {
        self.precedence_cmp(current).is_gt()
    }

    /// Compares two operation rows with the deterministic replicated winner ordering.
    pub fn precedence_cmp(&self, other: &Self) -> Ordering {
        self.stage
            .rank()
            .cmp(&other.stage.rank())
            .then(self.updated_at_unix_ms.cmp(&other.updated_at_unix_ms))
            .then(self.id.cmp(&other.id))
            .then(self.details.cmp(&other.details))
            .then_with(|| self.cmp(other))
    }

    /// Returns the timestamp used to order this operation within a cluster lineage.
    fn lineage_order_timestamp_unix_ms(&self) -> u64 {
        if self.created_at_unix_ms == 0 {
            self.updated_at_unix_ms
        } else {
            self.created_at_unix_ms
        }
    }

    /// Returns the deterministic ordering key used by lineage operation fences.
    pub(crate) fn lineage_order_key(&self) -> (u64, Uuid, Uuid) {
        (
            self.lineage_order_timestamp_unix_ms(),
            self.submitted_by_node_id,
            self.id,
        )
    }

    /// Encodes this operation record into its stable Cap'n Proto durable payload.
    pub fn encode_capnp(&self) -> Result<Vec<u8>, CapnpError> {
        let mut message = capnp::message::Builder::new_default();
        self.write_capnp(
            message.init_root::<mantissa_protocol::topology::cluster_operation::Builder<'_>>(),
        );
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one operation record from its stable Cap'n Proto durable payload.
    pub fn decode_capnp(bytes: &[u8]) -> Result<Self, CapnpError> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())?;
        let operation =
            reader.get_root::<mantissa_protocol::topology::cluster_operation::Reader<'_>>()?;
        Self::read_capnp(operation)
    }

    /// Encodes this operation record into a Cap'n Proto builder for topology RPC responses.
    pub fn write_capnp(
        &self,
        mut builder: mantissa_protocol::topology::cluster_operation::Builder<'_>,
    ) {
        builder.set_id(self.id.as_bytes());
        builder.set_submitted_by_node_id(self.submitted_by_node_id.as_bytes());
        builder.set_kind(self.kind.to_capnp());
        builder.set_stage(self.stage.to_capnp());
        builder.set_details(&self.details);
        builder.set_dry_run(self.dry_run);
        builder.set_split_service_policy(split_service_policy_to_capnp(self.split_service_policy));
        builder.set_split_network_policy(split_network_policy_to_capnp(self.split_network_policy));
        builder.set_merge_service_policy(merge_service_policy_to_capnp(self.merge_service_policy));
        builder.set_updated_at_unix_ms(self.updated_at_unix_ms);
        builder.set_created_at_unix_ms(self.created_at_unix_ms);
        let mut dependencies = builder
            .reborrow()
            .init_dependency_operation_ids(self.dependency_operation_ids.len() as u32);
        for (index, dependency_id) in self.dependency_operation_ids.iter().enumerate() {
            dependencies.set(index as u32, dependency_id.as_bytes());
        }

        let mut sources = builder
            .reborrow()
            .init_source_views(self.source_views.len() as u32);
        for (idx, source) in self.source_views.iter().enumerate() {
            source.write_capnp(sources.reborrow().get(idx as u32));
        }

        let mut targets = builder
            .reborrow()
            .init_target_views(self.target_views.len() as u32);
        for (idx, target) in self.target_views.iter().enumerate() {
            target.write_capnp(targets.reborrow().get(idx as u32));
        }

        let mut target_names = builder
            .reborrow()
            .init_target_cluster_names(self.target_cluster_names.len() as u32);
        for (idx, name) in self.target_cluster_names.iter().enumerate() {
            target_names.set(idx as u32, name);
        }

        let mut assignments = builder
            .reborrow()
            .init_split_assignments(self.split_assignments.len() as u32);
        for (idx, assignment) in self.split_assignments.iter().enumerate() {
            let mut assignment_builder = assignments.reborrow().get(idx as u32);
            set_node_id(
                assignment_builder.reborrow().init_node_id(),
                &assignment.node_id,
            );
            assignment_builder.set_target_index(assignment.target_index as u64);
        }
    }

    /// Decodes one operation record from a Cap'n Proto topology payload.
    fn read_capnp(
        reader: mantissa_protocol::topology::cluster_operation::Reader<'_>,
    ) -> Result<Self, CapnpError> {
        let id = uuid_from_data(reader.get_id()?, "cluster operation id")?;
        let source_views = read_cluster_views(reader.get_source_views()?)?;
        let target_views = read_cluster_views(reader.get_target_views()?)?;
        let target_cluster_names = read_text_list(reader.get_target_cluster_names()?)?;
        let split_assignments = read_split_assignments(reader.get_split_assignments()?)?;

        Ok(Self {
            id,
            submitted_by_node_id: uuid_from_data(
                reader.get_submitted_by_node_id()?,
                "cluster operation submitter node id",
            )?,
            kind: ClusterOperationKind::from_capnp(reader.get_kind()?),
            stage: ClusterOperationStage::from_capnp(reader.get_stage()?),
            dry_run: reader.get_dry_run(),
            created_at_unix_ms: reader.get_created_at_unix_ms(),
            dependency_operation_ids: read_uuid_list(
                reader.get_dependency_operation_ids()?,
                "cluster operation dependency id",
            )?,
            source_views,
            target_views,
            target_cluster_names,
            split_assignments,
            split_service_policy: split_service_policy_from_capnp(
                reader.get_split_service_policy()?,
            ),
            split_network_policy: split_network_policy_from_capnp(
                reader.get_split_network_policy()?,
            ),
            merge_service_policy: merge_service_policy_from_capnp(
                reader.get_merge_service_policy()?,
            ),
            updated_at_unix_ms: reader.get_updated_at_unix_ms(),
            details: reader.get_details()?.to_str()?.to_string(),
        })
    }
}

impl StoreValueCodec for ClusterOperationRecord {
    /// Encodes one operation record for the replicated operation ledger.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        self.encode_capnp()
            .map_err(|error| Box::new(mantissa_store::error::Error::Other(error.to_string())))
    }

    /// Decodes one operation record from the replicated operation ledger.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        Self::decode_capnp(bytes)
            .map_err(|error| Box::new(mantissa_store::error::Error::Other(error.to_string())))
    }
}

/// Converts one split service policy to its Cap'n Proto representation.
fn split_service_policy_to_capnp(
    value: SplitServicePolicy,
) -> mantissa_protocol::topology::SplitServicePolicy {
    match value {
        SplitServicePolicy::Partitioned => {
            mantissa_protocol::topology::SplitServicePolicy::Partitioned
        }
        SplitServicePolicy::Preserve => mantissa_protocol::topology::SplitServicePolicy::Preserve,
    }
}

/// Converts one Cap'n Proto split service policy into durable state.
fn split_service_policy_from_capnp(
    value: mantissa_protocol::topology::SplitServicePolicy,
) -> SplitServicePolicy {
    match value {
        mantissa_protocol::topology::SplitServicePolicy::Partitioned => {
            SplitServicePolicy::Partitioned
        }
        mantissa_protocol::topology::SplitServicePolicy::Preserve => SplitServicePolicy::Preserve,
    }
}

/// Converts one split network policy to its Cap'n Proto representation.
fn split_network_policy_to_capnp(
    value: SplitNetworkPolicy,
) -> mantissa_protocol::topology::SplitNetworkPolicy {
    match value {
        SplitNetworkPolicy::Isolate => mantissa_protocol::topology::SplitNetworkPolicy::Isolate,
        SplitNetworkPolicy::Preserve => mantissa_protocol::topology::SplitNetworkPolicy::Preserve,
    }
}

/// Converts one Cap'n Proto split network policy into durable state.
fn split_network_policy_from_capnp(
    value: mantissa_protocol::topology::SplitNetworkPolicy,
) -> SplitNetworkPolicy {
    match value {
        mantissa_protocol::topology::SplitNetworkPolicy::Isolate => SplitNetworkPolicy::Isolate,
        mantissa_protocol::topology::SplitNetworkPolicy::Preserve => SplitNetworkPolicy::Preserve,
    }
}

/// Converts one merge service policy to its Cap'n Proto representation.
fn merge_service_policy_to_capnp(
    value: MergeServicePolicy,
) -> mantissa_protocol::topology::MergeServicePolicy {
    match value {
        MergeServicePolicy::Rebalance => mantissa_protocol::topology::MergeServicePolicy::Rebalance,
        MergeServicePolicy::Preserve => mantissa_protocol::topology::MergeServicePolicy::Preserve,
    }
}

/// Converts one Cap'n Proto merge service policy into durable state.
fn merge_service_policy_from_capnp(
    value: mantissa_protocol::topology::MergeServicePolicy,
) -> MergeServicePolicy {
    match value {
        mantissa_protocol::topology::MergeServicePolicy::Rebalance => MergeServicePolicy::Rebalance,
        mantissa_protocol::topology::MergeServicePolicy::Preserve => MergeServicePolicy::Preserve,
    }
}

/// Reads one UUID data field and validates its fixed width.
fn uuid_from_data(data: capnp::data::Reader<'_>, field_name: &str) -> Result<Uuid, CapnpError> {
    if data.len() != 16 {
        return Err(CapnpError::failed(format!(
            "{field_name} must be exactly 16 bytes"
        )));
    }
    Uuid::from_slice(data).map_err(|err| CapnpError::failed(err.to_string()))
}

/// Reads and validates a list of UUID data fields.
fn read_uuid_list(
    reader: capnp::data_list::Reader<'_>,
    field_name: &str,
) -> Result<Vec<Uuid>, CapnpError> {
    let mut values = Vec::with_capacity(reader.len() as usize);
    for data in reader.iter() {
        values.push(uuid_from_data(data?, field_name)?);
    }
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

/// Decodes one cluster-view list from a Cap'n Proto operation payload.
fn read_cluster_views(
    reader: capnp::struct_list::Reader<'_, mantissa_protocol::topology::cluster_view_id::Owned>,
) -> Result<Vec<ClusterViewId>, CapnpError> {
    let mut views = Vec::with_capacity(reader.len() as usize);
    for item in reader.iter() {
        views.push(ClusterViewId::from_capnp(item).map_err(CapnpError::failed)?);
    }
    Ok(views)
}

/// Decodes one text list from a Cap'n Proto operation payload.
fn read_text_list(reader: capnp::text_list::Reader<'_>) -> Result<Vec<String>, CapnpError> {
    let mut values = Vec::with_capacity(reader.len() as usize);
    for item in reader.iter() {
        values.push(item?.to_str()?.to_string());
    }
    Ok(values)
}

/// Decodes split node assignments from a Cap'n Proto operation payload.
fn read_split_assignments(
    reader: capnp::struct_list::Reader<
        '_,
        mantissa_protocol::topology::split_node_assignment::Owned,
    >,
) -> Result<Vec<SplitNodeAssignment>, CapnpError> {
    let mut assignments = Vec::with_capacity(reader.len() as usize);
    for item in reader.iter() {
        let node_id = uuid_from_data(item.get_node_id()?.get_bytes()?, "split assignment node id")?;
        let target_index = usize::try_from(item.get_target_index()).map_err(|_| {
            CapnpError::failed("split assignment target index overflows usize".to_string())
        })?;
        assignments.push(SplitNodeAssignment {
            node_id,
            target_index,
        });
    }
    Ok(assignments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterId;

    /// Ensures durable cluster operation payloads preserve every Cap'n Proto field.
    #[test]
    fn cluster_operation_capnp_round_trip_preserves_durable_fields() {
        let source_cluster = ClusterId::from_uuid(Uuid::from_u128(0x100));
        let target_cluster_a = ClusterId::from_uuid(Uuid::from_u128(0x200));
        let target_cluster_b = ClusterId::from_uuid(Uuid::from_u128(0x300));
        let operation = ClusterOperationRecord {
            id: Uuid::from_u128(0x400),
            submitted_by_node_id: Uuid::from_u128(0x700),
            kind: ClusterOperationKind::Split,
            stage: ClusterOperationStage::Committed,
            dry_run: true,
            created_at_unix_ms: 123_000,
            dependency_operation_ids: vec![Uuid::from_u128(0x401)],
            source_views: vec![ClusterViewId::new(source_cluster, 7)],
            target_views: vec![
                ClusterViewId::new(target_cluster_a, 8),
                ClusterViewId::new(target_cluster_b, 9),
            ],
            target_cluster_names: vec!["blue".to_string(), "green".to_string()],
            split_assignments: vec![
                SplitNodeAssignment {
                    node_id: Uuid::from_u128(0x500),
                    target_index: 0,
                },
                SplitNodeAssignment {
                    node_id: Uuid::from_u128(0x600),
                    target_index: 1,
                },
            ],
            split_service_policy: SplitServicePolicy::Preserve,
            split_network_policy: SplitNetworkPolicy::Preserve,
            merge_service_policy: MergeServicePolicy::Preserve,
            updated_at_unix_ms: 123_456,
            details: "round trip".to_string(),
        };

        let payload = operation.encode_capnp().expect("encode operation");
        let decoded = ClusterOperationRecord::decode_capnp(&payload).expect("decode operation");

        assert_eq!(decoded, operation);
    }
}
