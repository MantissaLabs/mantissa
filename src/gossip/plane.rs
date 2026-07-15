use super::Message;
use crate::topology::TopologyEvent;
use crate::workload::model::WorkloadPropagationClass;
use mantissa_protocol::gossip;
use mantissa_protocol::gossip::gossip_message::Which::{SecretMasterKey, Topology};

/// Gossip transport plane selector.
///
/// `ViewScoped` carries regular control-plane events that must stay inside the active
/// view boundary. `GlobalMetadata` carries low-rate metadata that is allowed to cross
/// split view boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GossipPlane {
    ViewScoped,
    GlobalMetadata,
}

impl GossipPlane {
    /// Returns a stable telemetry label for the plane.
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::ViewScoped => "view_scoped",
            Self::GlobalMetadata => "global_metadata",
        }
    }

    /// Returns true when this plane may cross view boundaries.
    pub(super) fn allows_cross_view(self) -> bool {
        matches!(self, Self::GlobalMetadata)
    }
}

/// Selects the gossip plane for one outbound message.
pub(super) fn gossip_plane_for_message(message: &Message) -> GossipPlane {
    match message {
        Message::Topology {
            event:
                TopologyEvent::ClusterNameUpdated { .. } | TopologyEvent::ClusterMetadataChanged { .. },
            ..
        } => GossipPlane::GlobalMetadata,
        Message::SecretMasterKey { .. } => GossipPlane::GlobalMetadata,
        Message::Workload { event, .. } => workload_gossip_plane(event.propagation_class()),
        _ => GossipPlane::ViewScoped,
    }
}

/// Maps intended workload propagation classes onto today's transport plane.
fn workload_gossip_plane(_class: WorkloadPropagationClass) -> GossipPlane {
    // This implementation step only classifies propagation intent. All workload
    // updates still use the existing active-view gossip path until targeted
    // routes are introduced.
    GossipPlane::ViewScoped
}

/// Selects the gossip plane for one inbound wire message.
pub(super) fn gossip_plane_for_wire_message(
    message: gossip::gossip_message::Reader<'_>,
) -> GossipPlane {
    match message.which() {
        Ok(Topology(Ok(reader))) => match reader.get_event() {
            Ok(mantissa_protocol::topology::topology_event::EventType::ClusterNameUpdated) => {
                GossipPlane::GlobalMetadata
            }
            Ok(mantissa_protocol::topology::topology_event::EventType::ClusterMetadataChanged) => {
                GossipPlane::GlobalMetadata
            }
            _ => GossipPlane::ViewScoped,
        },
        Ok(SecretMasterKey(_)) => GossipPlane::GlobalMetadata,
        _ => GossipPlane::ViewScoped,
    }
}

/// Returns whether one inbound message should be enqueued for relay.
///
/// Global metadata events are always relayed regardless of the generic relay env flag
/// so cluster lineage names converge quickly without enabling high-volume relay for all
/// task/service update traffic.
pub(super) fn should_relay_inbound_message(relay_inbound: bool, message: &Message) -> bool {
    relay_inbound
        || gossip_plane_for_message(message).allows_cross_view()
        || matches!(message, Message::Network { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterViewId;
    use crate::store::replicated::secret_key_sync::{
        SecretMasterKeyCurrent, SecretMasterKeySyncRecord,
    };
    use uuid::Uuid;

    /// Transition hints must cross split boundaries and relay even when generic relay is disabled.
    #[test]
    fn transition_metadata_hints_use_global_plane() {
        let message = Message::Topology {
            id: Uuid::new_v4(),
            event: TopologyEvent::ClusterMetadataChanged {
                operation_id: Uuid::new_v4(),
            },
        };

        assert_eq!(
            gossip_plane_for_message(&message),
            GossipPlane::GlobalMetadata
        );
        assert!(should_relay_inbound_message(false, &message));
    }

    /// Wrapped transition keys use the same cross-view plane as their availability hint.
    #[test]
    fn master_key_rows_use_global_plane() {
        let message = Message::SecretMasterKey {
            id: Uuid::new_v4(),
            record: SecretMasterKeySyncRecord::Current(SecretMasterKeyCurrent {
                scope_view: ClusterViewId::legacy_default(),
                key_id: Uuid::new_v4(),
                generation: 1,
                created_by_operation_id: Some(Uuid::new_v4()),
                parent_key_ids: Vec::new(),
            }),
        };

        assert_eq!(
            gossip_plane_for_message(&message),
            GossipPlane::GlobalMetadata
        );
        assert!(should_relay_inbound_message(false, &message));
    }
}
