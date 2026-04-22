use uuid::Uuid;

/// Events emitted by attachment provisioning that require the network controller
/// to refresh remote forwarding state.
#[derive(Debug, Clone)]
pub enum ForwardingEvent {
    AttachmentReady { network_id: Uuid },
    TrafficPublicationChanged { network_id: Uuid },
}
