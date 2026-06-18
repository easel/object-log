//! The buffered, multiplexing log engine.

use crate::sequencer::BatchLocation;
use crate::{BlobStore, CommitBatch, CommitOutcome, ObjectLogError, PartitionKey, Sequencer};
use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::oneshot;

/// The durability point a [`LogEngine::produce`] call resolves at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Resolve as soon as the batch is buffered (fire-and-forget; may be lost on
    /// crash before the flush). No offset is returned.
    Buffered,
    /// Resolve once the containing object is durably PUT (survives crash). No
    /// offset yet — the commit has not run.
    Durable,
    /// Resolve once the batch is durably PUT **and** sequenced (has a stable
    /// offset). This is the strong, no-loss level.
    Sequenced,
}

/// Flush-trigger policy for the engine's group-commit buffer.
#[derive(Clone, Copy, Debug)]
pub struct FlushConfig {
    /// Flush once the buffered object reaches this many bytes (object-size lever;
    /// the primary S3-cost knob).
    pub max_bytes: usize,
    /// Flush once this many batches are buffered.
    pub max_batches: usize,
    /// Maximum time a batch waits in the buffer before a flush is forced. `ZERO`
    /// flushes as soon as a batch arrives (lowest latency).
    pub linger: Duration,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            max_bytes: 8 * 1024 * 1024,
            max_batches: 10_000,
            linger: Duration::ZERO,
        }
    }
}

/// Outcome of a [`LogEngine::produce`] call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendOutcome {
    /// First assigned offset, when resolved at [`Durability::Sequenced`].
    pub base_offset: Option<i64>,
    /// Last assigned offset, when resolved at [`Durability::Sequenced`].
    pub last_offset: Option<i64>,
    /// Whether the batch is durably stored.
    pub durable: bool,
    /// Whether the batch has been sequenced (has an offset).
    pub sequenced: bool,
}

/// A batch read back by [`LogEngine::fetch`], with its assigned base offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedBatch {
    /// First offset of the batch.
    pub base_offset: i64,
    /// Number of records in the batch.
    pub record_count: i32,
    /// The opaque batch payload.
    pub payload: Bytes,
}

type Responder = oneshot::Sender<Result<AppendOutcome, ObjectLogError>>;

struct Pending<M> {
    partition: PartitionKey,
    record_count: i32,
    payload: Bytes,
    meta: M,
    durability: Durability,
    responder: Option<Responder>,
}

struct Queue<M> {
    items: VecDeque<Pending<M>>,
    bytes: usize,
    shutdown: bool,
}

struct Shared<M> {
    queue: Mutex<Queue<M>>,
    cv: Condvar,
}

/// A buffered, multiplexing append-log engine over a [`BlobStore`], with
/// sequencing delegated to a [`Sequencer`].
///
/// `produce` group-commits: many batches across many partitions are multiplexed
/// into one object, PUT durably, then handed to the sequencer in a single call —
/// so PUT count is decoupled from produce count. A single flush worker preserves
/// per-[`PartitionKey`] arrival order and never splits a partition across
/// concurrent commits.
pub struct LogEngine<S: Sequencer> {
    shared: Arc<Shared<S::Meta>>,
    blob: Arc<dyn BlobStore>,
    sequencer: Arc<S>,
    flush_thread: Option<JoinHandle<()>>,
}

impl<S> LogEngine<S>
where
    S: Sequencer + 'static,
    S::Meta: Send + 'static,
{
    /// Create an engine over `blob` and `sequencer` with the given flush policy.
    /// Objects are keyed `<key_prefix><counter>`; pick a prefix unique to this
    /// engine instance if several share a store.
    pub fn new(
        blob: Arc<dyn BlobStore>,
        sequencer: Arc<S>,
        config: FlushConfig,
        key_prefix: impl Into<String>,
    ) -> Self {
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                items: VecDeque::new(),
                bytes: 0,
                shutdown: false,
            }),
            cv: Condvar::new(),
        });
        let flush_thread = {
            let shared = Arc::clone(&shared);
            let blob = Arc::clone(&blob);
            let sequencer = Arc::clone(&sequencer);
            let prefix = key_prefix.into();
            std::thread::Builder::new()
                .name("object-log-flush".into())
                .spawn(move || flush_loop(shared, blob, sequencer, config, prefix))
                .expect("spawn flush thread")
        };
        Self {
            shared,
            blob,
            sequencer,
            flush_thread: Some(flush_thread),
        }
    }

    /// Buffer a batch and resolve at the requested [`Durability`].
    pub async fn produce(
        &self,
        partition: PartitionKey,
        payload: Bytes,
        record_count: i32,
        meta: S::Meta,
        durability: Durability,
    ) -> Result<AppendOutcome, ObjectLogError> {
        if record_count <= 0 {
            return Err(ObjectLogError::InvalidBatch(
                "record_count must be > 0".into(),
            ));
        }
        if matches!(durability, Durability::Buffered) {
            self.enqueue(Pending {
                partition,
                record_count,
                payload,
                meta,
                durability,
                responder: None,
            });
            return Ok(AppendOutcome {
                base_offset: None,
                last_offset: None,
                durable: false,
                sequenced: false,
            });
        }
        let (tx, rx) = oneshot::channel();
        self.enqueue(Pending {
            partition,
            record_count,
            payload,
            meta,
            durability,
            responder: Some(tx),
        });
        rx.await
            .map_err(|_| ObjectLogError::Sequencer("flush worker stopped".into()))?
    }

    fn enqueue(&self, item: Pending<S::Meta>) {
        let mut q = self.shared.queue.lock().expect("poisoned");
        q.bytes += item.payload.len();
        q.items.push_back(item);
        self.shared.cv.notify_one();
    }

    /// Read batches covering offsets at/after `offset`, up to ~`max_bytes`.
    pub async fn fetch(
        &self,
        partition: &PartitionKey,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<FetchedBatch>, ObjectLogError> {
        let entries = self.sequencer.lookup(partition, offset)?;
        let mut out = Vec::new();
        let mut total = 0usize;
        for e in entries {
            if total >= max_bytes && !out.is_empty() {
                break;
            }
            let start = e.location.byte_start as u64;
            let end = start + e.location.byte_len as u64;
            let bytes = self
                .blob
                .get_range(&e.location.object_id, start..end)
                .await?
                .ok_or_else(|| ObjectLogError::MissingObject(e.location.object_id.clone()))?;
            total += bytes.len();
            out.push(FetchedBatch {
                base_offset: e.base_offset,
                record_count: e.record_count,
                payload: bytes,
            });
        }
        Ok(out)
    }

    /// Drop the partition's log below `offset` and delete any object that thereby
    /// becomes fully unreferenced.
    pub async fn truncate_before(
        &self,
        partition: &PartitionKey,
        offset: i64,
    ) -> Result<(), ObjectLogError> {
        let dead = self.sequencer.truncate_before(partition, offset)?;
        for object_id in dead {
            self.blob.delete(&object_id).await?;
        }
        Ok(())
    }
}

impl<S: Sequencer> Drop for LogEngine<S> {
    fn drop(&mut self) {
        {
            let mut q = self.shared.queue.lock().expect("poisoned");
            q.shutdown = true;
        }
        self.shared.cv.notify_all();
        if let Some(t) = self.flush_thread.take() {
            let _ = t.join();
        }
    }
}

fn flush_loop<S>(
    shared: Arc<Shared<S::Meta>>,
    blob: Arc<dyn BlobStore>,
    sequencer: Arc<S>,
    config: FlushConfig,
    prefix: String,
) where
    S: Sequencer + 'static,
    S::Meta: Send + 'static,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("flush runtime");
    let mut counter: u64 = 0;

    loop {
        let mut batch: Vec<Pending<S::Meta>> = {
            let mut q = shared.queue.lock().expect("poisoned");
            loop {
                if q.items.is_empty() {
                    if q.shutdown {
                        return;
                    }
                    q = shared.cv.wait(q).expect("poisoned");
                    continue;
                }
                let triggered = q.shutdown
                    || q.bytes >= config.max_bytes
                    || q.items.len() >= config.max_batches
                    || config.linger.is_zero();
                if triggered {
                    break;
                }
                let (guard, timeout) = shared.cv.wait_timeout(q, config.linger).expect("poisoned");
                q = guard;
                if timeout.timed_out() {
                    break;
                }
            }
            let items: Vec<_> = q.items.drain(..).collect();
            q.bytes = 0;
            items
        };

        // Build one object from all buffered payloads; record each batch's range.
        let mut object = BytesMut::new();
        let mut locations: Vec<BatchLocation> = Vec::with_capacity(batch.len());
        counter += 1;
        let key = format!("{prefix}{counter:020}");
        for p in &batch {
            let start = object.len() as u32;
            object.extend_from_slice(&p.payload);
            locations.push(BatchLocation {
                object_id: key.clone(),
                byte_start: start,
                byte_len: p.payload.len() as u32,
            });
        }

        // Move responders out (so the commit borrow of `batch` doesn't conflict).
        let mut responders: Vec<(Durability, Option<Responder>)> = batch
            .iter_mut()
            .map(|p| (p.durability, p.responder.take()))
            .collect();

        // Durable-then-sequence: PUT first.
        if let Err(e) = rt.block_on(blob.put(&key, object.freeze())) {
            for (_, tx) in responders.iter_mut() {
                if let Some(tx) = tx.take() {
                    let _ = tx.send(Err(e.clone()));
                }
            }
            continue;
        }

        // Signal Durable-level waiters now (after PUT, before commit).
        for (durability, tx) in responders.iter_mut() {
            if *durability == Durability::Durable
                && let Some(tx) = tx.take()
            {
                let _ = tx.send(Ok(AppendOutcome {
                    base_offset: None,
                    last_offset: None,
                    durable: true,
                    sequenced: false,
                }));
            }
        }

        // Sequence the whole object atomically.
        let commit_batches: Vec<CommitBatch<'_, S::Meta>> = batch
            .iter()
            .zip(locations.iter())
            .map(|(p, loc)| CommitBatch {
                partition: p.partition.clone(),
                record_count: p.record_count,
                location: loc.clone(),
                meta: &p.meta,
            })
            .collect();

        match sequencer.commit(&commit_batches) {
            Ok(outcomes) => {
                for (outcome, (_, tx)) in outcomes.into_iter().zip(responders.iter_mut()) {
                    if let Some(tx) = tx.take() {
                        let (base, last) = match outcome {
                            CommitOutcome::Assigned {
                                base_offset,
                                record_count,
                            } => (
                                Some(base_offset),
                                Some(base_offset + record_count as i64 - 1),
                            ),
                            CommitOutcome::Duplicate { base_offset } => (Some(base_offset), None),
                        };
                        let _ = tx.send(Ok(AppendOutcome {
                            base_offset: base,
                            last_offset: last,
                            durable: true,
                            sequenced: true,
                        }));
                    }
                }
            }
            Err(e) => {
                for (_, tx) in responders.iter_mut() {
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(Err(e.clone()));
                    }
                }
            }
        }
    }
}
