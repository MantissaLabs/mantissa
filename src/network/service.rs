use capnp::Error;
use capnp::capability::Promise;
use protocol::network::networks;

/// Placeholder implementation for network RPCs until the full control-plane is wired.
pub struct NetworksRpc;

impl NetworksRpc {
    /// Create a new placeholder networks RPC handler.
    pub fn new() -> Self {
        Self
    }

    fn not_implemented() -> Promise<(), Error> {
        Promise::err(Error::unimplemented(
            "network RPCs are not implemented yet".into(),
        ))
    }
}

#[async_trait::async_trait(?Send)]
impl networks::Server for NetworksRpc {
    fn create(
        &mut self,
        _params: networks::CreateParams,
        _results: networks::CreateResults,
    ) -> Promise<(), Error> {
        Self::not_implemented()
    }

    fn delete(
        &mut self,
        _params: networks::DeleteParams,
        _results: networks::DeleteResults,
    ) -> Promise<(), Error> {
        Self::not_implemented()
    }

    fn list(
        &mut self,
        _params: networks::ListParams,
        _results: networks::ListResults,
    ) -> Promise<(), Error> {
        Self::not_implemented()
    }

    fn inspect(
        &mut self,
        _params: networks::InspectParams,
        _results: networks::InspectResults,
    ) -> Promise<(), Error> {
        Self::not_implemented()
    }

    fn peer_status(
        &mut self,
        _params: networks::PeerStatusParams,
        _results: networks::PeerStatusResults,
    ) -> Promise<(), Error> {
        Self::not_implemented()
    }

    fn attachments(
        &mut self,
        _params: networks::AttachmentsParams,
        _results: networks::AttachmentsResults,
    ) -> Promise<(), Error> {
        Self::not_implemented()
    }
}
