/// Listener configuration for the exported server transports.
///
/// The server bootstrap path only needs the resolved listen address, so the
/// old builder-style config with unused fields has been collapsed to this
/// minimal transport config.
#[derive(Clone)]
pub struct Config {
    pub listen_addr: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:6578".to_string(),
        }
    }
}

impl Config {
    /// Creates a server transport config for the resolved listen address.
    ///
    /// Bootstrap constructs this once and passes it to `Server`, which then
    /// uses it whenever transport listeners are started or restarted.
    pub fn new(listen_addr: String) -> Self {
        Self { listen_addr }
    }
}
