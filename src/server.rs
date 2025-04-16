#[derive(Clone)]
pub struct Server {}

impl Server {
    /// Creates a new server.
    ///
    /// Returns the server and the memberlist actions to execute
    /// in a gossip loop.
    pub fn new() -> Server {
        Server {}
    }

    /// Starts the server, bootstrapping all necessary sub-components
    pub async fn start(self) {}
}
