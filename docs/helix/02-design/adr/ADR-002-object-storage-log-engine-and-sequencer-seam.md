---
ddx:
  id: adr-object-storage-log-engine-and-sequencer-seam
  depends_on:
    - product-vision
    - prd
    - concerns
  links:
    - adr-kafka-compatible-core-object-storage-backend
---

# ADR-002: Object-Storage Log Engine with a Pluggable Sequencer Seam

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-06-17 (rev. after adversarial review) | Accepted | Erik LaBianca | ADR-001 (superseded) | High |

## Context

| Aspect | Description |
|--------|-------------|
| Problem | ADR-001's backend writes **one object per append** (a sealed segment + a manifest compare-and-set per `append()`). On object storage this is fatal: one PUT per produce wrecks cost and tail latency. Separately, ADR-001 made the core **Kafka-shaped** (topics, `acks`, idempotent-producer triple, epoch fencing), so object-log is a Kafka coordinator wearing a trench coat — the wrong layering. |
| Current State | The per-append model (`ObjectLogBackend`, segment codec, manifest CAS, `EpochGuard`, the rich record model) ships in 0.1.x but is used by no production consumer. fjord rejected it and built its own buffered, multiplexing, coordinator-sequenced write path (`fjord-log`), duplicating storage concerns. |
| Requirements | (1) Viable on object storage: amortize many batches into few PUTs. (2) `acks=all` must not return until durably persisted **and** sequenced. (3) Be the engine fjord runs on **without** re-proving fjord's verified Kafka parity. (4) **Clean three-layer separation** (below). |
| Decision Drivers | Object-storage economics; one write path instead of two; a genuinely generic OSS package; verified-parity preservation. |

## The three layers (separation of concerns)

This ADR's north star is a clean cut across three crates/repos, dependencies pointing **down only**:

- **object-log** (this repo, public) — *"object storage as an append log."* Owns: a `BlobStore` port (Memory/Local/S3), a buffered/multiplexing/ack-aware `LogEngine`, and a pluggable `Sequencer` seam. Deals only in **opaque payload bytes**, partition keys, record counts, byte ranges, and offsets. **Knows nothing of Kafka, records, producers, or brokers.**
- **heimq** (public) — Kafka wire protocol + broker request engine. **Owns the Kafka record-batch byte format**, including stamping `base_offset` into a v2 batch. Knows nothing of object storage.
- **fjord** (private) — broker coordination + binding. The coordinator (offset authority, idempotent-producer dedup, EOS/txn fencing, consumer groups) **implements object-log's `Sequencer`**; the binding layer **implements heimq's `LogBackend`** over object-log's engine. Owns all Kafka semantics that aren't wire-format.

Dependency DAG (no cycles, no up-edges):

```
        object-log  ◄─────────────┐
            ▲                      │
heimq-broker│ (no edge either way) │
   (traits) │                      │
            │              fjord-coordinator  (impl object_log::Sequencer)
            │                      ▲
            └── fjord-heimq-backend ┘  (depends on heimq-broker + object-log + fjord-coordinator — the binding)
                       ▲
                    fjord (bin)
```

## Decision

We will **re-found object-log as a generic, buffered, multiplexing, durability-aware object-storage log engine that is generic over a pluggable `Sequencer`.** This supersedes ADR-001's per-append, Kafka-shaped model.

**1. `BlobStore` port — the only storage abstraction.** Async `put`/`get`/`list`/`delete` over immutable objects keyed by string. Adapters: `Memory`, `Local`, `S3` (feature-gated). Replaces both the CAS `ObjectStore` and fjord's private `BlobStore`/`S3BlobStore`. No conditional writes needed — object keys are unique per flush.

**2. `LogEngine<S: Sequencer>` — buffered group-commit.** Accepts opaque batch payloads with a partition key, a record count, and `S`'s metadata; **group-commits**: many batches across many partitions are multiplexed into **one** object, PUT durably, then handed to the sequencer in one call. Owns the flush policy `FlushConfig { max_bytes, max_batches, linger }`. The engine **authors each batch's `BatchLocation { object_id, byte_start, byte_len }`** (its concern); it never inspects `S`'s metadata.

**3. Durability levels — object-log's own vocabulary, not Kafka `acks`.** A produce future resolves at the requested level:
- `Buffered` — in the flush buffer (may be lost on crash).
- `Durable` — the containing object is PUT (survives crash; **no offset yet**).
- `Sequenced` — `commit` returned (durable **and** has a stable offset).
fjord maps Kafka `acks=0/1/-1` onto these.

**4. `Sequencer` seam — SYNC, generic over metadata, owns the index.**

```rust
pub trait Sequencer: Send + Sync {
    /// Per-batch sequencing metadata the engine forwards UNINTERPRETED.
    /// object-log's default impl uses (); fjord uses its Kafka producer fields.
    type Meta: Send + Sync;

    /// The engine has ALREADY PUT the object durably and computed each batch's
    /// location. Assign offsets ATOMICALLY across the whole object (all-or-nothing)
    /// and persist the offset→location index. One outcome per batch, in order.
    fn commit(&self, batches: &[(Self::Meta, BatchLocation)])
        -> Result<Vec<CommitOutcome>, ObjectLogError>;

    /// Resolve an offset to ordered index entries (the sequencer owns the
    /// offset namespace and the offset→location index).
    fn lookup(&self, partition: &PartitionKey, fetch_offset: i64)
        -> Result<Vec<IndexEntry>, ObjectLogError>;
    fn high_watermark(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError>;
}
```

`BatchLocation`, `CommitOutcome { Assigned { base_offset, record_count } | Duplicate { base_offset } }`, `IndexEntry { location, base_offset, record_count }`, and `PartitionKey` are **generic** and live in object-log. **`Meta` is an associated type — Kafka's `producer_id`/`producer_epoch`/`base_sequence` live only in fjord's `Meta`, never in object-log.**

**5. Sync seam, async engine.** The `Sequencer` is sync because a linearization point is a critical section, not async I/O — and fjord's sequencer does blocking Postgres I/O. The engine owns the flush worker (a dedicated thread / blocking task, per fjord's proven `Flusher`) and resolves async produce futures via waker. No `async_trait`/`spawn_blocking` tax on the lin-point.

**6. Fetch.** `engine.fetch(partition, offset, max_bytes)` calls `sequencer.lookup` → reads the covering objects via `BlobStore` → slices `byte_start..byte_len`. Pure storage IO; the index is the sequencer's.

**7. Offset stamping stays in heimq.** object-log stores and returns **opaque** payload bytes plus the assigned `base_offset`. Writing that offset into a Kafka v2 record batch is heimq's job (it owns the format). fjord must stop poking bytes `[0..8]`.

**Key Points**: amortized PUTs | durable-before-ack | generic over `Sequencer::Meta` (zero Kafka in object-log) | sync seam, async engine | engine authors locations, sequencer owns the index.

### Invariants the seam contract MUST state (from adversarial review)

- **In-order, un-split per-partition presentation.** For any single `PartitionKey`, the engine presents batches to `commit` in arrival (send) order, and never splits one partition's batches across two concurrent in-flight `commit` calls. (Else idempotent sequence-gap detection and epoch fencing misfire — a producer's sequence space is per-partition.) The engine enforces this **without reading `Meta`**, using only the engine-visible `PartitionKey`: satisfied by a single flush worker (FIFO buffer, no sorting), or — if concurrent flush workers are ever added — by sharding concurrency on **`PartitionKey`** while preserving per-key arrival order. Never shard on producer identity (it lives inside the opaque `Meta`, which the engine must not read) and never partition-sort the buffer. The engine **moves** each `Meta` out of its FIFO buffer into `commit` (so `Meta: Send + Sync` suffices — no `Clone` bound).
- **Atomic commit.** `commit` is all-or-nothing across every batch in the object; on `Err` the engine acks nothing (no co-multiplexed survivor is acked).
- **Unique object id, fresh on retry, durable-before-index.** Each flush uses a unique object key; a retry uses a *fresh* key so a crashed PUT is never aliased; a `lookup` entry implies its byte range is durably readable.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Keep ADR-001 per-append model | Simple | 1 PUT/produce — unaffordable; Kafka-shaped; unused | Rejected: violates its own cost model |
| Buffered engine, **concrete** `BatchMeta` with Kafka fields in object-log (the pre-review ADR-002) | Matches fjord's types 1:1 | Kafka producer/idempotency vocab leaks into the generic OSS lib; forces Kafka types to move into object-log | Rejected: layer leak (Reviewer A/B) |
| Opaque `dedup_token: Bytes` in the seam | object-log stays generic | Sequencer must decode it to compare epoch/sequence ordering → serialization tax + lost type safety; risks EOS | Rejected: Reviewer C — opacity can't be ordered |
| **Engine generic over `Sequencer::Meta` (associated type)** | object-log names zero Kafka types; fjord's `Meta` keeps typed fields for EOS; no cycle; no type move | Engine is generic (`LogEngine<S>`); one extra type parameter at fjord's bind site | **Selected: satisfies purity (A) AND correctness (C) AND acyclicity (B)** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | PUT count decoupled from produce/partition count; `acks=all` durably correct; one write path; object-log is genuinely generic (opaque bytes, no Kafka) and standalone-usable; S3 adapter shared. |
| Negative | Breaking → object-log 0.2.0. Removed from object-log: `ObjectLogBackend`, segment codec, `EpochGuard`, the record model (`AppendRecord`/`RecordHeader`/`TimestampPolicy`), `AckMode`, CAS `ObjectStore`. Deleted from fjord: `fjord-object-log` (layer-conflating experiment), `fjord-log`'s `BlobStore`/`WritePath`/`ReadPath`/`MemoryBlobStore`/`S3BlobStore`. fjord-coordinator gains a dep on object-log and `impl Sequencer`. heimq gains an offset-stamping seam; fjord's byte-poking is removed. |
| Neutral | object-log stores opaque multiplexed batch bytes (no framed segment format). Orphan-object reclamation (objects PUT then crashed before `commit`) is **out of scope for 0.2.0**; `BlobStore::list` + the sequencer's referenced-object set make an external reaper buildable. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Rebuilt write path regresses fjord's proven parity (EOS, idempotency, ordering) | M | H | Keep ALL sequencing logic in fjord's coordinator behind the seam; write the in-order/atomic/unique-id invariants into the contract; re-run fjord's full conformance/EOS/group/multi-broker suite as the merge gate. |
| Engine "improvements" (concurrent flush workers, partition-sharded buffers) silently reorder a producer's batches | M | H | Contract invariant above; add a non-Docker engine ordering test (many producers, in-flight=1, assert per-producer contiguity). No current non-Docker test catches this. |
| Default in-memory sequencer's index is not crash-durable → "standalone durable log" is half-true (bytes survive, index doesn't) | M | M | Ship two default sequencers: a trivial in-memory one (tests) AND a `BlobStore`-persisted-index sequencer (writes a manifest object on `commit`) so standalone object-log is crash-durable end-to-end. Amortization: **one manifest PUT per group-commit**, co-amortized with the data object (PUTs stay ∝ flushes, not ∝ produces) — or fold the index into a trailing footer of the data object for exactly 1 PUT/flush. Never a manifest PUT per produce. |
| Crash between PUT and `commit` leaks orphan objects | M | L | Documented out-of-scope for 0.2.0; `BlobStore::list` enables a reaper; fresh-key-on-retry prevents aliasing. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| object-log source contains zero Kafka identifiers (producer_id/epoch/sequence/acks/topic) | Any reappearance |
| PUT count independent of partition count (N flushes → N objects) | Regression |
| `acks=all`→`Sequenced` returns only after durable PUT + commit | Any early-ack path |
| New `Sequencer`-conformance + fault-between-PUT-and-commit + engine-ordering tests pass | — |
| fjord's hermetic parity suite stays green on the rebuilt path | Any conformance/EOS/group/multi-broker failure |

## Supersession

- **Supersedes**: ADR-001 — its one-object-per-append backend, manifest-CAS commit boundary, **and its Kafka-shaped core vocabulary** (topics/`acks`/producer triple now live in fjord/heimq, not object-log).
- **Superseded by**: None.

## Concern Impact

- **Concern selection**: object storage remains the durable tier; adds a pluggable sequencer as the linearization point and an opaque-payload contract.
- **Practice override**: None.

## References

- ADR-001 (this repo) — superseded.
- fjord TD-005 / TD-006 — the multiplexed write/fetch path generalized here.
- fjord ADR-008 — central-coordinator sequencing (fjord's `Sequencer` impl).
- Adversarial review 2026-06-17 — four lenses (layer purity, dependency graph, correctness preservation, seam shape); findings folded into §"Decision" and §"Invariants".

## Review Checklist

- [x] Context names a specific problem (1 PUT/produce; Kafka-shaped core).
- [x] Decision is actionable and resolves the purity-vs-correctness contradiction (generic `Meta`).
- [x] Alternatives include the rejected concrete-`BatchMeta` and opaque-token options with reasons.
- [x] Consequences list exactly what is removed/deleted/added across all three layers.
- [x] Risks carry the adversarial-review ordering/durability findings with mitigations.
- [x] Validation includes a "zero Kafka identifiers in object-log" gate.
- [x] Supersession recorded against ADR-001.
