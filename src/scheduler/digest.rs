use crate::gossip::Message;
use crate::gpu::gpu_runtime_status;
use crate::store::replicated::scheduler_digests::SchedulerDigestStore;
use anyhow::{Result as AnyhowResult, anyhow};
use async_channel::{Receiver, Sender};
use capnp::Error;
use mantissa_protocol::scheduling::{scheduler_digest, scheduler_digest_event};
use mantissa_store::codec::StoreValueCodec;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;
use uuid::Uuid;

use super::{GpuDeviceState, SchedulerSnapshot, SlotState};

/// Compact per-node scheduler state replicated for shortlist selection.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SchedulerDigestValue {
    pub node_id: Uuid,
    pub snapshot_version: u64,
    pub updated_at_unix_ms: u64,
    pub free_slot_count: u32,
    pub free_cpu_millis: u64,
    pub free_memory_bytes: u64,
    pub largest_free_slot_cpu_millis: u64,
    pub largest_free_slot_memory_bytes: u64,
    pub free_gpu_count: u32,
    pub gpu_runtime_ready: bool,
}

impl SchedulerDigestValue {
    /// Builds one digest from the current scheduler snapshot for shortlist selection.
    pub fn from_snapshot(node_id: Uuid, snapshot: &SchedulerSnapshot) -> Self {
        let mut free_slot_count = 0u32;
        let mut free_cpu_millis = 0u64;
        let mut free_memory_bytes = 0u64;
        let mut largest_free_slot_cpu_millis = 0u64;
        let mut largest_free_slot_memory_bytes = 0u64;

        for slot in &snapshot.slots {
            if !matches!(slot.state, SlotState::Free) {
                continue;
            }

            free_slot_count = free_slot_count.saturating_add(1);
            free_cpu_millis = free_cpu_millis.saturating_add(slot.capacity.cpu_millis);
            free_memory_bytes = free_memory_bytes.saturating_add(slot.capacity.memory_bytes);
            largest_free_slot_cpu_millis =
                largest_free_slot_cpu_millis.max(slot.capacity.cpu_millis);
            largest_free_slot_memory_bytes =
                largest_free_slot_memory_bytes.max(slot.capacity.memory_bytes);
        }

        let mut free_gpu_count = 0u32;
        for device in &snapshot.gpu_devices {
            if matches!(device.state, GpuDeviceState::Free) {
                free_gpu_count = free_gpu_count.saturating_add(1);
            }
        }

        Self {
            node_id,
            snapshot_version: snapshot.version,
            updated_at_unix_ms: current_unix_ms(),
            free_slot_count,
            free_cpu_millis,
            free_memory_bytes,
            largest_free_slot_cpu_millis,
            largest_free_slot_memory_bytes,
            free_gpu_count,
            gpu_runtime_ready: snapshot.gpu_devices.is_empty() || gpu_runtime_status().is_ready(),
        }
    }
}

impl StoreValueCodec for SchedulerDigestValue {
    /// Encodes one scheduler digest as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_scheduler_digest(message.init_root::<scheduler_digest::Builder>(), self);
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one scheduler digest from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(store_codec_error)?;
        let digest = reader
            .get_root::<scheduler_digest::Reader>()
            .map_err(store_codec_error)?;
        read_scheduler_digest(digest).map_err(store_codec_error)
    }
}

/// Converts scheduler store-codec errors into the CRDT store error type.
fn store_codec_error<E: std::fmt::Display>(error: E) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "scheduler digest store codec error: {error}"
    )))
}

/// Gossip event carrying one compact scheduler digest mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchedulerDigestEvent {
    Upsert(Box<SchedulerDigestValue>),
    Remove(Uuid),
}

/// Local cache view of one replicated scheduler digest together with its ingest time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedSchedulerDigest {
    pub digest: SchedulerDigestValue,
    pub observed_at_unix_ms: u64,
}

/// Storage-backed access layer for scheduler digest rows.
#[derive(Clone)]
pub struct SchedulerDigestRegistry {
    store: SchedulerDigestStore,
    observed_at_unix_ms: Arc<Mutex<HashMap<Uuid, u64>>>,
}

impl SchedulerDigestRegistry {
    /// Builds the registry from the underlying replicated digest store.
    pub fn new(store: SchedulerDigestStore) -> Self {
        Self {
            store,
            observed_at_unix_ms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Upserts one node-local scheduler digest into the replicated store.
    pub async fn upsert(&self, value: SchedulerDigestValue) -> AnyhowResult<()> {
        let node_id = value.node_id;
        self.store
            .upsert(&mantissa_store::uuid_key::UuidKey::from(node_id), value)
            .await
            .map_err(|e| anyhow!("scheduler digest upsert failed: {e}"))?;
        self.record_observed_now(node_id);
        Ok(())
    }

    /// Removes one scheduler digest row from the replicated store.
    pub async fn remove(&self, node_id: Uuid) -> AnyhowResult<()> {
        self.store
            .remove(&mantissa_store::uuid_key::UuidKey::from(node_id))
            .await
            .map_err(|e| anyhow!("scheduler digest remove failed: {e}"))?;
        self.clear_observed(node_id);
        Ok(())
    }

    /// Reads the canonical digest for one node identifier.
    pub fn get(&self, node_id: Uuid) -> AnyhowResult<Option<SchedulerDigestValue>> {
        let key = mantissa_store::uuid_key::UuidKey::from(node_id);
        let snapshot = self
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow!("scheduler digest lookup failed: {e}"))?;
        Ok(snapshot.and_then(|values| select_best_scheduler_digest(values.as_slice())))
    }

    /// Reads the canonical digest for one node together with the local ingest timestamp.
    pub fn get_observed(&self, node_id: Uuid) -> AnyhowResult<Option<ObservedSchedulerDigest>> {
        let Some(digest) = self.get(node_id)? else {
            return Ok(None);
        };
        Ok(Some(ObservedSchedulerDigest {
            observed_at_unix_ms: self.observed_at(node_id),
            digest,
        }))
    }

    /// Lists every canonical scheduler digest currently known in the replicated store.
    pub fn list(&self) -> AnyhowResult<Vec<SchedulerDigestValue>> {
        let (entries, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow!("scheduler digest load_all failed: {e}"))?;

        let mut values = Vec::with_capacity(entries.len());
        for (_key, snapshot) in entries {
            if let Some(value) = select_best_scheduler_digest(snapshot.as_slice()) {
                values.push(value);
            }
        }

        values.sort_by_key(|value| value.node_id);
        Ok(values)
    }

    /// Lists every canonical scheduler digest currently known together with local ingest times.
    pub fn list_observed(&self) -> AnyhowResult<Vec<ObservedSchedulerDigest>> {
        let mut values = Vec::new();
        for digest in self.list()? {
            let node_id = digest.node_id;
            values.push(ObservedSchedulerDigest {
                observed_at_unix_ms: self.observed_at(node_id),
                digest,
            });
        }
        Ok(values)
    }

    /// Records one local observation timestamp for a digest row.
    fn record_observed_now(&self, node_id: Uuid) {
        let now_unix_ms = current_unix_ms();
        let mut guard = self.observed_at_unix_ms.lock();
        guard.insert(node_id, now_unix_ms);
    }

    /// Returns the local ingest timestamp for one digest row, seeding it lazily if absent.
    fn observed_at(&self, node_id: Uuid) -> u64 {
        let now_unix_ms = current_unix_ms();
        let mut guard = self.observed_at_unix_ms.lock();
        *guard.entry(node_id).or_insert(now_unix_ms)
    }

    /// Clears the local ingest timestamp when the digest row disappears.
    fn clear_observed(&self, node_id: Uuid) {
        let mut guard = self.observed_at_unix_ms.lock();
        guard.remove(&node_id);
    }
}

/// Publishes the local node's digest into storage and onto gossip after scheduler mutations.
#[derive(Clone)]
pub struct SchedulerDigestPublisher {
    registry: SchedulerDigestRegistry,
    gossip_tx: Sender<Message>,
    local_node_id: Uuid,
}

impl SchedulerDigestPublisher {
    /// Creates one publisher bound to the local digest registry and gossip queue.
    pub fn new(
        registry: SchedulerDigestRegistry,
        gossip_tx: Sender<Message>,
        local_node_id: Uuid,
    ) -> Self {
        Self {
            registry,
            gossip_tx,
            local_node_id,
        }
    }

    /// Publishes a fresh digest derived from the provided scheduler snapshot.
    pub async fn publish_from_snapshot(&self, snapshot: &SchedulerSnapshot) -> AnyhowResult<()> {
        let digest = SchedulerDigestValue::from_snapshot(self.local_node_id, snapshot);
        self.registry.upsert(digest.clone()).await?;

        self.gossip_tx
            .send(Message::SchedulerDigest {
                id: Uuid::new_v4(),
                event: SchedulerDigestEvent::Upsert(Box::new(digest)),
            })
            .await
            .map_err(|e| anyhow!("failed to enqueue scheduler digest gossip: {e}"))
    }
}

/// Applies inbound scheduler digest gossip to the local replicated digest store.
#[derive(Clone)]
pub struct SchedulerDigestReplicator {
    registry: SchedulerDigestRegistry,
    gossip_rx: Receiver<Message>,
}

impl SchedulerDigestReplicator {
    /// Creates one inbound replicator bound to the provided registry and gossip channel.
    pub fn new(registry: SchedulerDigestRegistry, gossip_rx: Receiver<Message>) -> Self {
        Self {
            registry,
            gossip_rx,
        }
    }

    /// Runs the inbound gossip loop and applies deduplicated scheduler digest events.
    pub async fn run(&self) {
        while let Ok(message) = self.gossip_rx.recv().await {
            let Message::SchedulerDigest { event, .. } = message else {
                continue;
            };

            if let Err(err) = self.apply_event(event).await {
                warn!(
                    target: "scheduler",
                    "failed to apply scheduler digest gossip event: {err:#}"
                );
            }
        }
    }

    /// Applies one decoded scheduler digest event to the local replicated store.
    async fn apply_event(&self, event: SchedulerDigestEvent) -> AnyhowResult<()> {
        match event {
            SchedulerDigestEvent::Upsert(value) => self.registry.upsert(*value).await?,
            SchedulerDigestEvent::Remove(node_id) => self.registry.remove(node_id).await?,
        }
        Ok(())
    }
}

/// Returns the node identifier affected by one scheduler digest event.
pub fn scheduler_digest_event_node_id(event: &SchedulerDigestEvent) -> Uuid {
    match event {
        SchedulerDigestEvent::Upsert(value) => value.node_id,
        SchedulerDigestEvent::Remove(node_id) => *node_id,
    }
}

/// Returns true when the candidate digest event should replace the retained event.
pub fn should_replace_scheduler_digest_event(
    current: &SchedulerDigestEvent,
    candidate: &SchedulerDigestEvent,
) -> bool {
    match (current, candidate) {
        (SchedulerDigestEvent::Remove(_), SchedulerDigestEvent::Upsert(_)) => false,
        (SchedulerDigestEvent::Upsert(_), SchedulerDigestEvent::Remove(_))
        | (SchedulerDigestEvent::Remove(_), SchedulerDigestEvent::Remove(_)) => true,
        (SchedulerDigestEvent::Upsert(current), SchedulerDigestEvent::Upsert(candidate)) => {
            compare_scheduler_digest_values(candidate, current).is_gt()
        }
    }
}

/// Serializes one scheduler digest value into the Cap'n Proto wire representation.
pub(crate) fn write_scheduler_digest(
    mut builder: scheduler_digest::Builder<'_>,
    value: &SchedulerDigestValue,
) {
    builder.set_node_id(value.node_id.as_bytes());
    builder.set_snapshot_version(value.snapshot_version);
    builder.set_updated_at_unix_ms(value.updated_at_unix_ms);
    builder.set_free_slot_count(value.free_slot_count);
    builder.set_free_cpu_millis(value.free_cpu_millis);
    builder.set_free_memory_bytes(value.free_memory_bytes);
    builder.set_largest_free_slot_cpu_millis(value.largest_free_slot_cpu_millis);
    builder.set_largest_free_slot_memory_bytes(value.largest_free_slot_memory_bytes);
    builder.set_free_gpu_count(value.free_gpu_count);
    builder.set_gpu_runtime_ready(value.gpu_runtime_ready);
}

/// Deserializes one scheduler digest value from the Cap'n Proto wire representation.
pub(crate) fn read_scheduler_digest(
    reader: scheduler_digest::Reader<'_>,
) -> std::result::Result<SchedulerDigestValue, Error> {
    Ok(SchedulerDigestValue {
        node_id: read_uuid(reader.get_node_id()?)?,
        snapshot_version: reader.get_snapshot_version(),
        updated_at_unix_ms: reader.get_updated_at_unix_ms(),
        free_slot_count: reader.get_free_slot_count(),
        free_cpu_millis: reader.get_free_cpu_millis(),
        free_memory_bytes: reader.get_free_memory_bytes(),
        largest_free_slot_cpu_millis: reader.get_largest_free_slot_cpu_millis(),
        largest_free_slot_memory_bytes: reader.get_largest_free_slot_memory_bytes(),
        free_gpu_count: reader.get_free_gpu_count(),
        gpu_runtime_ready: reader.get_gpu_runtime_ready(),
    })
}

/// Serializes one scheduler digest gossip event into the Cap'n Proto envelope.
pub(crate) fn write_scheduler_digest_event(
    mut builder: scheduler_digest_event::Builder<'_>,
    event: &SchedulerDigestEvent,
) -> std::result::Result<(), Error> {
    match event {
        SchedulerDigestEvent::Upsert(value) => {
            write_scheduler_digest(builder.reborrow().init_upsert(), value);
        }
        SchedulerDigestEvent::Remove(node_id) => {
            builder.set_remove(node_id.as_bytes());
        }
    }
    Ok(())
}

/// Deserializes one scheduler digest gossip event from the Cap'n Proto envelope.
pub(crate) fn read_scheduler_digest_event(
    reader: scheduler_digest_event::Reader<'_>,
) -> std::result::Result<SchedulerDigestEvent, Error> {
    match reader.which()? {
        scheduler_digest_event::Which::Upsert(Ok(value)) => Ok(SchedulerDigestEvent::Upsert(
            Box::new(read_scheduler_digest(value)?),
        )),
        scheduler_digest_event::Which::Upsert(Err(err)) => Err(err),
        scheduler_digest_event::Which::Remove(Ok(bytes)) => {
            Ok(SchedulerDigestEvent::Remove(read_uuid(bytes)?))
        }
        scheduler_digest_event::Which::Remove(Err(err)) => Err(err),
    }
}

/// Selects the canonical digest value from one MVReg snapshot.
fn select_best_scheduler_digest(values: &[SchedulerDigestValue]) -> Option<SchedulerDigestValue> {
    let mut best: Option<&SchedulerDigestValue> = None;
    for value in values {
        match best {
            None => best = Some(value),
            Some(current) => {
                if compare_scheduler_digest_values(value, current).is_gt() {
                    best = Some(value);
                }
            }
        }
    }
    best.cloned()
}

/// Compares two digest rows to choose a deterministic canonical value.
fn compare_scheduler_digest_values(
    left: &SchedulerDigestValue,
    right: &SchedulerDigestValue,
) -> Ordering {
    left.snapshot_version
        .cmp(&right.snapshot_version)
        .then(left.updated_at_unix_ms.cmp(&right.updated_at_unix_ms))
        .then(left.free_slot_count.cmp(&right.free_slot_count))
        .then(left.free_cpu_millis.cmp(&right.free_cpu_millis))
        .then(left.free_memory_bytes.cmp(&right.free_memory_bytes))
        .then(
            left.largest_free_slot_cpu_millis
                .cmp(&right.largest_free_slot_cpu_millis),
        )
        .then(
            left.largest_free_slot_memory_bytes
                .cmp(&right.largest_free_slot_memory_bytes),
        )
        .then(left.free_gpu_count.cmp(&right.free_gpu_count))
        .then(left.gpu_runtime_ready.cmp(&right.gpu_runtime_ready))
        .then(left.node_id.cmp(&right.node_id))
}

/// Decodes one required 16-byte UUID payload from the wire.
fn read_uuid(bytes: capnp::data::Reader<'_>) -> std::result::Result<Uuid, Error> {
    if bytes.len() != 16 {
        return Err(Error::failed("uuid must be 16 bytes".into()));
    }

    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    Ok(Uuid::from_bytes(arr))
}

/// Returns the current Unix timestamp in milliseconds for digest freshness ordering.
fn current_unix_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(u64::MAX as u128) as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SchedulerDigestEvent, SchedulerDigestRegistry, SchedulerDigestValue,
        should_replace_scheduler_digest_event,
    };
    use crate::store::replicated::scheduler_digests::open_scheduler_digest_store;
    use mantissa_store::codec::StoreValueCodec;
    use std::sync::Arc;
    use tempfile::tempdir;
    use uuid::Uuid;

    /// Builds one deterministic digest row used by scheduler digest storage tests.
    fn sample_digest(node_id: Uuid, snapshot_version: u64) -> SchedulerDigestValue {
        SchedulerDigestValue {
            node_id,
            snapshot_version,
            updated_at_unix_ms: 42,
            free_slot_count: 2,
            free_cpu_millis: 1_000,
            free_memory_bytes: 2_048,
            largest_free_slot_cpu_millis: 500,
            largest_free_slot_memory_bytes: 1_024,
            free_gpu_count: 1,
            gpu_runtime_ready: true,
        }
    }

    /// Newer snapshot versions should win when coalescing concurrent digest upserts.
    #[test]
    fn newer_snapshot_version_wins() {
        let node_id = Uuid::new_v4();
        let current = SchedulerDigestEvent::Upsert(Box::new(SchedulerDigestValue {
            node_id,
            snapshot_version: 4,
            updated_at_unix_ms: 10,
            free_slot_count: 2,
            free_cpu_millis: 2_000,
            free_memory_bytes: 4_096,
            largest_free_slot_cpu_millis: 1_000,
            largest_free_slot_memory_bytes: 2_048,
            free_gpu_count: 0,
            gpu_runtime_ready: false,
        }));
        let candidate = SchedulerDigestEvent::Upsert(Box::new(SchedulerDigestValue {
            node_id,
            snapshot_version: 5,
            updated_at_unix_ms: 1,
            free_slot_count: 1,
            free_cpu_millis: 1_000,
            free_memory_bytes: 2_048,
            largest_free_slot_cpu_millis: 1_000,
            largest_free_slot_memory_bytes: 2_048,
            free_gpu_count: 0,
            gpu_runtime_ready: false,
        }));

        assert!(should_replace_scheduler_digest_event(&current, &candidate));
    }

    /// Remove events should win over any queued digest upsert for the same node.
    #[test]
    fn remove_wins_over_upsert() {
        let node_id = Uuid::new_v4();
        let current = SchedulerDigestEvent::Upsert(Box::new(SchedulerDigestValue {
            node_id,
            snapshot_version: 4,
            updated_at_unix_ms: 10,
            free_slot_count: 2,
            free_cpu_millis: 2_000,
            free_memory_bytes: 4_096,
            largest_free_slot_cpu_millis: 1_000,
            largest_free_slot_memory_bytes: 2_048,
            free_gpu_count: 0,
            gpu_runtime_ready: false,
        }));
        let candidate = SchedulerDigestEvent::Remove(node_id);

        assert!(should_replace_scheduler_digest_event(&current, &candidate));
    }

    /// Local digest freshness should track when this node ingested the row, not the peer timestamp.
    #[tokio::test]
    async fn observed_digest_tracks_local_ingest_time() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("scheduler-digest-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let store = open_scheduler_digest_store(db, actor).expect("open digest store");
        store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild digest store");
        let registry = SchedulerDigestRegistry::new(store);
        let node_id = Uuid::new_v4();

        registry
            .upsert(SchedulerDigestValue {
                node_id,
                snapshot_version: 9,
                updated_at_unix_ms: 1,
                free_slot_count: 2,
                free_cpu_millis: 1_000,
                free_memory_bytes: 2_048,
                largest_free_slot_cpu_millis: 500,
                largest_free_slot_memory_bytes: 1_024,
                free_gpu_count: 0,
                gpu_runtime_ready: true,
            })
            .await
            .expect("upsert digest");

        let observed = registry
            .get_observed(node_id)
            .expect("lookup observed digest")
            .expect("observed digest");
        assert_eq!(observed.digest.node_id, node_id);
        assert!(
            observed.observed_at_unix_ms > observed.digest.updated_at_unix_ms,
            "local ingest time should not reuse the peer-provided digest timestamp"
        );
    }

    /// Scheduler digest values should round-trip through the Cap'n Proto store-value codec.
    #[test]
    fn store_value_codec_roundtrips_scheduler_digest() {
        let digest = sample_digest(Uuid::new_v4(), 11);

        let encoded = digest
            .encode_store_value()
            .expect("encode scheduler digest store value");
        let decoded = SchedulerDigestValue::decode_store_value(&encoded)
            .expect("decode scheduler digest store value");

        assert_eq!(decoded, digest);
    }

    /// Reopening the scheduler digest store should decode Cap'n Proto MVReg rows from Redb.
    #[tokio::test]
    async fn scheduler_digest_store_reopens_capnp_rows() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("scheduler-digest-reopen-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let digest = sample_digest(node_id, 12);

        {
            let store = open_scheduler_digest_store(db.clone(), actor).expect("open digest store");
            let registry = SchedulerDigestRegistry::new(store);
            registry
                .upsert(digest.clone())
                .await
                .expect("upsert digest");
        }

        let reopened = open_scheduler_digest_store(db, actor).expect("reopen digest store");
        reopened
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild digest MST");
        let registry = SchedulerDigestRegistry::new(reopened);
        let got = registry
            .get(node_id)
            .expect("lookup reopened digest")
            .expect("digest present");

        assert_eq!(got, digest);
    }
}
