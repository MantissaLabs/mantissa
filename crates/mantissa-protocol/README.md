# mantissa-protocol

Generated Cap'n Proto protocol bindings for Mantissa.

This crate is the shared wire contract between Mantissa nodes, clients, and
supporting services. The source schemas live in `schema/*.capnp`; `build.rs`
compiles them and `src/lib.rs` re-exports the generated modules with convenient
paths.

## Included Schemas

The crate currently generates bindings for:

- `server`, `node`, `topology`, `gossip`, `sync`, and `health`
- `workload`, `task`, `scheduling`, `services`, `jobs`, and `agents`
- `secrets`, `network`, `volumes`, and `info`

## Usage

Most consumers use the flattened module exports:

```rust,no_run
use mantissa_protocol::server::ServerClient;
use mantissa_protocol::topology::TopologyClient;

fn accepts_clients(_server: ServerClient, _topology: TopologyClient) {}
```

Generated builders and readers remain available through the schema modules:

```rust,no_run
use mantissa_protocol::topology::topology_event;

fn read_event(reader: topology_event::Reader<'_>) -> capnp::Result<()> {
    match reader.which()? {
        topology_event::Which::Join(_) => {}
        topology_event::Which::Leave(_) => {}
        _ => {}
    }
    Ok(())
}
```

## Stability

This crate follows the repository protocol version, not an independent public
API lifecycle. Schema changes should be made with the corresponding server,
client, and migration updates in the same Mantissa change.
