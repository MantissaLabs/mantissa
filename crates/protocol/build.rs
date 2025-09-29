fn main() {
    let mut cmd = capnpc::CompilerCommand::new();
    cmd.src_prefix("schema")
        .file("schema/server.capnp")
        .file("schema/node.capnp")
        .file("schema/gossip.capnp")
        .file("schema/topology.capnp")
        .file("schema/scheduling.capnp")
        .file("schema/workload.capnp")
        .file("schema/services.capnp")
        .file("schema/info.capnp")
        .file("schema/utils.capnp")
        .file("schema/sync.capnp")
        .file("schema/health.capnp");
    cmd.run().expect("capnp compile schemas");
}
