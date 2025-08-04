extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src/schema")
        .file("src/schema/server.capnp")
        .run()
        .unwrap();
    capnpc::CompilerCommand::new()
        .src_prefix("src/schema")
        .file("src/schema/node.capnp")
        .run()
        .unwrap();
    capnpc::CompilerCommand::new()
        .src_prefix("src/schema")
        .file("src/schema/gossip.capnp")
        .run()
        .unwrap();
    capnpc::CompilerCommand::new()
        .src_prefix("src/schema")
        .file("src/schema/topology.capnp")
        .run()
        .unwrap();
    capnpc::CompilerCommand::new()
        .src_prefix("src/schema")
        .file("src/schema/scheduling.capnp")
        .run()
        .unwrap();
    capnpc::CompilerCommand::new()
        .src_prefix("src/schema")
        .file("src/schema/info.capnp")
        .run()
        .unwrap();
    capnpc::CompilerCommand::new()
        .src_prefix("src/schema")
        .file("src/schema/utils.capnp")
        .run()
        .unwrap();
    capnpc::CompilerCommand::new()
        .src_prefix("src/schema")
        .file("src/schema/sync.capnp")
        .run()
        .unwrap();

    // TODO: Compile BPF programs
}
