/// Compile Cap'n Proto schemas into Rust code for the protocol crate.
fn main() {
    let mut cmd = capnpc::CompilerCommand::new();
    cmd.src_prefix("schema")
        .file("schema/server.capnp")
        .file("schema/node.capnp")
        .file("schema/gossip.capnp")
        .file("schema/topology.capnp")
        .file("schema/scheduling.capnp")
        .file("schema/task.capnp")
        .file("schema/services.capnp")
        .file("schema/info.capnp")
        .file("schema/sync.capnp")
        .file("schema/health.capnp")
        .file("schema/secrets.capnp")
        .file("schema/network.capnp")
        .file("schema/volumes.capnp");
    cmd.run().expect("capnp compile schemas");
}
