use super::Message;
use crate::topology::TopologyEvent;
use protocol::gossip;
use protocol::gossip::gossip_message::Which::Topology;

/// Gossip transport plane selector.
///
/// `ViewScoped` carries regular control-plane events that must stay inside the active
/// view boundary. `GlobalMetadata` carries low-rate metadata that is allowed to cross
/// split view boundaries (currently cluster lineage names).
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
            event: TopologyEvent::ClusterNameUpdated { .. },
            ..
        } => GossipPlane::GlobalMetadata,
        _ => GossipPlane::ViewScoped,
    }
}

/// Selects the gossip plane for one inbound wire message.
pub(super) fn gossip_plane_for_wire_message(
    message: gossip::gossip_message::Reader<'_>,
) -> GossipPlane {
    match message.which() {
        Ok(Topology(Ok(reader))) => match reader.get_event() {
            Ok(protocol::topology::topology_event::EventType::ClusterNameUpdated) => {
                GossipPlane::GlobalMetadata
            }
            _ => GossipPlane::ViewScoped,
        },
        _ => GossipPlane::ViewScoped,
    }
}

/// Returns whether one inbound message should be enqueued for relay.
///
/// Global metadata events are always relayed regardless of the generic relay env flag
/// so cluster lineage names converge quickly without enabling high-volume relay for all
/// task/service update traffic.
pub(super) fn should_relay_inbound_message(relay_inbound: bool, message: &Message) -> bool {
    relay_inbound || gossip_plane_for_message(message).allows_cross_view()
}
