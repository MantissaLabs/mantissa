/// Compile Cap'n Proto schemas used by the generic CRDT store.
fn main() {
    let mut cmd = capnpc::CompilerCommand::new();
    cmd.src_prefix("schema").file("schema/store.capnp");
    cmd.run().expect("capnp compile crdt_store schemas");
}
