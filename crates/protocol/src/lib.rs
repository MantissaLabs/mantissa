capnp::generated_code!(pub mod server_capnp);
capnp::generated_code!(pub mod node_capnp);
capnp::generated_code!(pub mod gossip_capnp);
capnp::generated_code!(pub mod topology_capnp);
capnp::generated_code!(pub mod scheduling_capnp);
capnp::generated_code!(pub mod info_capnp);
capnp::generated_code!(pub mod utils_capnp);
capnp::generated_code!(pub mod sync_capnp);
capnp::generated_code!(pub mod health_capnp);

pub use gossip_capnp as gossip;
pub use health_capnp as health;
pub use info_capnp as info;
pub use node_capnp as node;
pub use scheduling_capnp as scheduling;
pub use server_capnp as server;
pub use sync_capnp as sync;
pub use topology_capnp as topology;
pub use utils_capnp as utils;
