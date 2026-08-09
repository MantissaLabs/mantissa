//! Independent authenticated connections for fixed-file replica data.

use std::sync::Arc;

use anyhow::Context;
use mantissa_volume::VolumeNodeId;
use mantissa_volume::storage::replica_file::connection::{
    ReplicaDataConnection, write_connection_open,
};
use mantissa_volume::storage::replica_file::wire::{ReplicaDataConnectionOpen, ReplicaDataLimits};

use super::VolumeTransport;

/// Opens one Noise stream that never enters the Raft or control RPC queues.
pub(super) async fn connect(
    transport: &Arc<VolumeTransport>,
    target: VolumeNodeId,
    open: &ReplicaDataConnectionOpen,
    limits: ReplicaDataLimits,
    maximum_in_flight: usize,
) -> anyhow::Result<Arc<ReplicaDataConnection>> {
    let mut stream = transport
        .connect_stream(*target.as_uuid())
        .await
        .with_context(|| format!("connect to replica data copy {target}"))?;
    write_connection_open(&mut stream, open, limits)
        .await
        .with_context(|| format!("open replica data copy {target}"))?;
    let connection = ReplicaDataConnection::start(stream, limits, maximum_in_flight)
        .with_context(|| format!("start replica data connection to {target}"))?;
    Ok(Arc::new(connection))
}
