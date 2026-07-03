use uuid::Uuid;

/// Events emitted by attachment provisioning, withdrawal, and service publication that require
/// the network controller to refresh forwarding or discovery-derived dataplane state.
#[derive(Debug, Clone)]
pub enum ForwardingEvent {
    AttachmentReady { network_id: Uuid },
    TrafficPublicationChanged { network_id: Uuid },
}
