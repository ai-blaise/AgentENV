use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::local_store::{LocalKvBatchOp, LocalKvStore, LocalStoreDurability};
use crate::types::{SandboxId, SandboxResources};

use super::{SandboxLifecycleEvent, SandboxLifecycleEventType};

const OUTBOX_VERSION: u32 = 1;
const META_KEY: &[u8] = b"meta";
const EVENT_PREFIX: u8 = b'e';

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OutboxMetadata {
    version: u32,
    stream_id: Uuid,
    last_sequence: u64,
    acknowledged_sequence: u64,
}

impl OutboxMetadata {
    fn new() -> Self {
        Self {
            version: OUTBOX_VERSION,
            stream_id: Uuid::now_v7(),
            last_sequence: 0,
            acknowledged_sequence: 0,
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.version != OUTBOX_VERSION {
            bail!("unsupported lifecycle outbox version {}", self.version);
        }
        if self.acknowledged_sequence > self.last_sequence {
            bail!("lifecycle outbox acknowledgement exceeds its last sequence");
        }
        Ok(())
    }
}

enum OutboxBackend {
    Memory(BTreeMap<u64, SandboxLifecycleEvent>),
    Durable(LocalKvStore),
}

struct OutboxState {
    metadata: OutboxMetadata,
    backend: OutboxBackend,
}

/// Durable, strictly ordered local lifecycle event outbox.
///
/// Appends and acknowledgements are serialized through one mutex and committed
/// with synchronous RocksDB write batches in production. The broadcaster owned
/// by the orchestrator is deliberately only a wake-up hint: delivery always
/// reads from this store, so channel lag and process restarts cannot lose data.
pub struct LifecycleEventOutbox {
    state: Mutex<OutboxState>,
}

impl LifecycleEventOutbox {
    pub fn in_memory() -> Self {
        Self {
            state: Mutex::new(OutboxState {
                metadata: OutboxMetadata::new(),
                backend: OutboxBackend::Memory(BTreeMap::new()),
            }),
        }
    }

    pub async fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = LocalKvStore::open(path.as_ref(), LocalStoreDurability::Sync)
            .await
            .context("open lifecycle event outbox")?;
        let metadata = match db
            .get(META_KEY)
            .await
            .context("read lifecycle outbox metadata")?
        {
            Some(bytes) => {
                let metadata: OutboxMetadata =
                    serde_json::from_slice(&bytes).context("decode lifecycle outbox metadata")?;
                metadata.validate()?;
                metadata
            }
            None => {
                let metadata = OutboxMetadata::new();
                db.put(
                    META_KEY,
                    serde_json::to_vec(&metadata).context("encode lifecycle outbox metadata")?,
                )
                .await
                .context("initialize lifecycle outbox metadata")?;
                metadata
            }
        };

        let outbox = Self {
            state: Mutex::new(OutboxState {
                metadata,
                backend: OutboxBackend::Durable(db),
            }),
        };
        outbox.validate_records().await?;
        Ok(outbox)
    }

    pub async fn append(
        &self,
        event_type: SandboxLifecycleEventType,
        sandbox_id: SandboxId,
        resources: SandboxResources,
    ) -> anyhow::Result<SandboxLifecycleEvent> {
        let mut state = self.state.lock().await;
        let sequence = state
            .metadata
            .last_sequence
            .checked_add(1)
            .context("lifecycle event sequence exhausted")?;
        let occurred_at_unix_ms = unix_millis();
        let event = SandboxLifecycleEvent {
            event_type,
            sandbox_id,
            resources,
            stream_id: state.metadata.stream_id,
            sequence,
            event_id: format!("{}:{sequence}", state.metadata.stream_id),
            occurred_at_unix_ms,
        };
        state.metadata.last_sequence = sequence;
        let metadata_bytes =
            serde_json::to_vec(&state.metadata).context("encode lifecycle outbox metadata")?;

        match &mut state.backend {
            OutboxBackend::Memory(events) => {
                events.insert(sequence, event.clone());
            }
            OutboxBackend::Durable(db) => {
                let event_bytes = serde_json::to_vec(&event).context("encode lifecycle event")?;
                if let Err(error) = db
                    .write_batch([
                        LocalKvBatchOp::put(event_key(sequence), event_bytes),
                        LocalKvBatchOp::put(META_KEY, metadata_bytes),
                    ])
                    .await
                {
                    state.metadata.last_sequence -= 1;
                    return Err(error).context("append lifecycle event");
                }
            }
        }
        Ok(event)
    }

    pub async fn pending(&self, limit: usize) -> anyhow::Result<Vec<SandboxLifecycleEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let state = self.state.lock().await;
        let acknowledged = state.metadata.acknowledged_sequence;
        let mut events = match &state.backend {
            OutboxBackend::Memory(events) => events
                .range(acknowledged.saturating_add(1)..)
                .take(limit)
                .map(|(_, event)| event.clone())
                .collect(),
            OutboxBackend::Durable(db) => {
                let entries = db
                    .scan_prefix([EVENT_PREFIX])
                    .await
                    .context("scan lifecycle outbox")?;
                let mut events = Vec::with_capacity(entries.len().min(limit));
                for (key, value) in entries {
                    let sequence = decode_event_key(&key)?;
                    if sequence <= acknowledged {
                        continue;
                    }
                    let event =
                        serde_json::from_slice(&value).context("decode lifecycle outbox event")?;
                    events.push(event);
                    if events.len() == limit {
                        break;
                    }
                }
                events
            }
        };
        events.sort_unstable_by_key(|event| event.sequence);
        validate_contiguous(&events, acknowledged.saturating_add(1))?;
        Ok(events)
    }

    pub async fn acknowledge(&self, through_sequence: u64) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        if through_sequence <= state.metadata.acknowledged_sequence {
            return Ok(());
        }
        if through_sequence > state.metadata.last_sequence {
            bail!(
                "cannot acknowledge lifecycle sequence {through_sequence}; last sequence is {}",
                state.metadata.last_sequence
            );
        }
        let previous = state.metadata.acknowledged_sequence;
        state.metadata.acknowledged_sequence = through_sequence;
        let metadata_bytes = serde_json::to_vec(&state.metadata)
            .context("encode lifecycle outbox acknowledgement")?;

        match &mut state.backend {
            OutboxBackend::Memory(events) => {
                events.retain(|sequence, _| *sequence > through_sequence);
            }
            OutboxBackend::Durable(db) => {
                let mut operations = (previous.saturating_add(1)..=through_sequence)
                    .map(|sequence| LocalKvBatchOp::delete(event_key(sequence)))
                    .collect::<Vec<_>>();
                operations.push(LocalKvBatchOp::put(META_KEY, metadata_bytes));
                if let Err(error) = db.write_batch(operations).await {
                    state.metadata.acknowledged_sequence = previous;
                    return Err(error).context("acknowledge lifecycle events");
                }
            }
        }
        Ok(())
    }

    pub async fn position(&self) -> (Uuid, u64, u64) {
        let state = self.state.lock().await;
        (
            state.metadata.stream_id,
            state.metadata.last_sequence,
            state.metadata.acknowledged_sequence,
        )
    }

    async fn validate_records(&self) -> anyhow::Result<()> {
        let state = self.state.lock().await;
        let OutboxBackend::Durable(db) = &state.backend else {
            return Ok(());
        };
        let events = db
            .scan_prefix([EVENT_PREFIX])
            .await
            .context("scan lifecycle outbox during recovery")?;
        let expected_count = state
            .metadata
            .last_sequence
            .saturating_sub(state.metadata.acknowledged_sequence);
        if u64::try_from(events.len()).unwrap_or(u64::MAX) != expected_count {
            bail!(
                "lifecycle outbox record count does not match metadata: expected {expected_count}, found {}",
                events.len()
            );
        }
        for (offset, (key, value)) in events.into_iter().enumerate() {
            let sequence = decode_event_key(&key)?;
            let expected = state
                .metadata
                .acknowledged_sequence
                .saturating_add(u64::try_from(offset).unwrap_or(u64::MAX))
                .saturating_add(1);
            if sequence != expected {
                bail!("lifecycle outbox sequence gap: expected {expected}, found {sequence}");
            }
            let event: SandboxLifecycleEvent =
                serde_json::from_slice(&value).context("decode lifecycle event during recovery")?;
            if event.sequence != sequence {
                bail!("lifecycle outbox key and event sequence differ at {sequence}");
            }
            if event.stream_id != state.metadata.stream_id {
                bail!("lifecycle outbox event belongs to a different stream at {sequence}");
            }
        }
        Ok(())
    }
}

impl Default for LifecycleEventOutbox {
    fn default() -> Self {
        Self::in_memory()
    }
}

fn event_key(sequence: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(EVENT_PREFIX);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn decode_event_key(key: &[u8]) -> anyhow::Result<u64> {
    let bytes: [u8; 8] = key
        .get(1..)
        .and_then(|bytes| bytes.try_into().ok())
        .context("invalid lifecycle outbox event key")?;
    Ok(u64::from_be_bytes(bytes))
}

fn validate_contiguous(events: &[SandboxLifecycleEvent], first: u64) -> anyhow::Result<()> {
    for (offset, event) in events.iter().enumerate() {
        let expected = first
            .checked_add(u64::try_from(offset).context("event batch is too large")?)
            .context("lifecycle event sequence exhausted")?;
        if event.sequence != expected {
            bail!(
                "lifecycle outbox sequence gap: expected {expected}, found {}",
                event.sequence
            );
        }
    }
    Ok(())
}

fn unix_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resources() -> SandboxResources {
        SandboxResources {
            cpu_count: 2,
            memory_mib: 512,
            disk_size_mib: 1024,
        }
    }

    #[tokio::test]
    async fn memory_outbox_orders_and_acknowledges_events() {
        let outbox = LifecycleEventOutbox::in_memory();
        let first = outbox
            .append(
                SandboxLifecycleEventType::Create,
                SandboxId::new(),
                resources(),
            )
            .await
            .unwrap();
        let second = outbox
            .append(
                SandboxLifecycleEventType::Pause,
                first.sandbox_id,
                resources(),
            )
            .await
            .unwrap();
        assert_eq!((first.sequence, second.sequence), (1, 2));
        assert_eq!(outbox.pending(1).await.unwrap(), vec![first.clone()]);
        outbox.acknowledge(1).await.unwrap();
        assert_eq!(outbox.pending(10).await.unwrap(), vec![second]);
    }

    #[tokio::test]
    async fn durable_outbox_survives_reopen_with_stream_identity() {
        let directory = tempfile::TempDir::new().unwrap();
        let sandbox_id = SandboxId::new();
        let first_stream;
        {
            let outbox = LifecycleEventOutbox::open(directory.path()).await.unwrap();
            first_stream = outbox.position().await.0;
            outbox
                .append(SandboxLifecycleEventType::Create, sandbox_id, resources())
                .await
                .unwrap();
        }
        let reopened = LifecycleEventOutbox::open(directory.path()).await.unwrap();
        assert_eq!(reopened.position().await, (first_stream, 1, 0));
        assert_eq!(
            reopened.pending(10).await.unwrap()[0].sandbox_id,
            sandbox_id
        );
        reopened.acknowledge(1).await.unwrap();
        drop(reopened);

        let empty = LifecycleEventOutbox::open(directory.path()).await.unwrap();
        assert_eq!(empty.position().await, (first_stream, 1, 1));
        assert!(empty.pending(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn acknowledgement_cannot_skip_beyond_appended_events() {
        let outbox = LifecycleEventOutbox::in_memory();
        assert!(outbox.acknowledge(1).await.is_err());
    }
}
