pub mod server_capnp {
    include!(concat!(env!("OUT_DIR"), "/server_capnp.rs"));
}
pub mod delegate_capnp {
    include!(concat!(env!("OUT_DIR"), "/delegate_capnp.rs"));
}
pub mod gossip_capnp {
    include!(concat!(env!("OUT_DIR"), "/gossip_capnp.rs"));
}
pub mod topology_capnp {
    include!(concat!(env!("OUT_DIR"), "/topology_capnp.rs"));
}
pub mod ousterhout_capnp {
    include!(concat!(env!("OUT_DIR"), "/ousterhout_capnp.rs"));
}
pub mod stat_capnp {
    include!(concat!(env!("OUT_DIR"), "/stat_capnp.rs"));
}
pub mod utils_capnp {
    include!(concat!(env!("OUT_DIR"), "/utils_capnp.rs"));
}
