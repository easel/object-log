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
| 2026-06-17 | Accepted | Erik LaBianca | ADR-001 (superseded) | High |

## Context

| Aspect | Description |
|--------|-------------|
| Problem | ADR-001's object-storage backend writes **one object per append** (one sealed segment + a manifest compare-and-set per `append()`). On real object storage this is economically fatal: at S3 PUT pricing and per-request latency, one PUT per produce makes the cost and tail latency untenable. |
| Current State | The per-append model (`ObjectLogBackend`, segment codec, manifest CAS, `EpochGuard`) shipped in 0.1.x but is **used by no production consumer**. fjord — the intended primary consumer — rejected it during its diskless re-baseline and built its own buffered, multiplexing, coordinator-sequenced write path (`fjord-log`: `WritePath`/`ReadPath` over a `BlobStore`, with a server-side `Flusher`). object-log is consequently near-vestigial: fjord touches only a thin `put`/`get` bridge over the `ObjectStore` port. |
| Requirements | (1) Viable on object storage: amortize many record batches into few PUTs. (2) Honor Kafka acknowledgement semantics: `acks=all` must not return until the write is durably persisted **and** sequenced. (3) Be the engine fjord actually runs on, without re-proving fjord's already-verified Kafka parity. |
| Decision Drivers | Object-storage cost/latency economics; eliminating the duplicate write path across two repos; giving object-log a real reason to exist. |

## Decision

We will **re-found object-log as a buffered, multiplexing, acknowledgement-aware object-storage log engine, with sequencing delegated to a pluggable `Sequencer` seam.** This supersedes ADR-001's per-append model.

The engine owns (generic, storage-shaped concerns):

- An async **`BlobStore`** port — `put` / `get` / `list` / `delete` over immutable, content-addressed objects — with `Memory`, `Local`, and **`S3`** (feature-gated) adapters. This replaces both the CAS-heavy `ObjectStore` and fjord's private `BlobStore`.
- A **`LogEngine`** that buffers record batches in memory and **group-commits** them: many batches spanning many partitions are multiplexed into **one** object, PUT durably, then handed to the sequencer in one call. Flushing is governed by `FlushConfig { max_bytes, max_batches, linger }`.
- **Acknowledgement semantics tied to durability**: an `acks=all` produce future resolves **only after** the containing object is durably PUT **and** sequenced (durable-then-sequence). `acks=none`/`leader` may resolve earlier.
- A **fetch** path: resolve an offset to index entries (via the sequencer), read the covering objects, and slice the byte ranges back out.

Sequencing is **not** an engine responsibility. A **`Sequencer`** trait (`commit_object(object_id, &[BatchMeta]) -> Vec<CommitOutcome>`, `index_lookup(...)`) is the linearization point. Offset assignment, idempotent-producer dedup, transactional/EOS fencing, and epoch fencing live behind it. object-log ships a default in-memory single-node `Sequencer` for standalone use and tests; consumers plug their own.

**Key Points**: amortized PUTs (cost) | durable-before-ack (correctness) | sequencing is a seam, not baked in (lets fjord's proven coordinator plug in unchanged).

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Keep ADR-001 per-append model | Simple; no coordinator needed | 1 PUT/produce — unaffordable on S3; nobody uses it | Rejected: violates the cost model it was meant to serve |
| object-log = storage port only; delete the log engine | Smallest change | Leaves the buffered log + S3 duplicated in fjord; object-log stays a thin adapter | Rejected: doesn't make object-log the engine |
| **Buffered engine + pluggable Sequencer seam** | Amortized PUTs; ack/durability correct; fjord builds on it; coordinator logic stays in fjord behind the seam | Larger rework; a breaking 0.2.0; sequencing contract must be designed carefully | **Selected: only option that satisfies cost + acks + "fjord runs on it" without re-proving parity** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | PUT count is decoupled from produce/partition count; `acks=all` is durably correct by construction; one write path instead of two; object-log gains real consumers; S3 adapter is generic and shared. |
| Negative | Breaking change → object-log 0.2.0; `ObjectLogBackend`/segment codec/`EpochGuard`/manifest-CAS are removed; fjord's `fjord-log` write path, `S3BlobStore`, `Flusher`, and the `fjord-object-log` experiment are deleted and rebuilt on the engine. |
| Neutral | object-log remains usable standalone via its default in-memory sequencer; the on-disk object is now opaque multiplexed batch bytes rather than the framed segment format. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Rebuilding fjord's write path regresses its proven Kafka parity (EOS, idempotency, ordering) | M | H | Keep all sequencing logic in fjord's coordinator behind the `Sequencer` seam — do not reimplement it. Re-run fjord's full conformance/EOS/group/multi-broker suite as the merge gate. |
| Buffering weakens durability vs per-append | L | H | `acks=all` blocks until durable PUT + sequence; durable-then-sequence ordering means a crash mid-flight orphans an unreferenced object with no ack and no offset. |
| Engine's `Sequencer` contract doesn't fit fjord's `CoordinatorStore` | L | M | The contract is modeled directly on fjord's existing `commit_object`/`index_lookup` (`BatchMeta`/`CommitOutcome`); fjord implements the trait via a thin adapter. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| PUT count independent of partition count (test asserts N flushes → N objects) | Regression in object count per flush |
| `acks=all` returns only after the object is durably stored (test) | Any path that acks before durable PUT |
| fjord's hermetic parity suite stays green on the rebuilt path | Any conformance/EOS/group/multi-broker failure |

## Supersession

- **Supersedes**: ADR-001 (Kafka-Compatible Core with Object-Storage Backend) — specifically its one-object-per-append backend and manifest-CAS commit boundary. The Kafka-compatible *record/ack/idempotency vocabulary* ADR-001 established is retained.
- **Superseded by**: None.

## Concern Impact

- **Concern selection**: Reaffirms object storage as the durable tier; adds a pluggable sequencer as the linearization point.
- **Practice override**: None.

## References

- ADR-001 (this repo) — superseded per-append model.
- fjord TD-005 / TD-006 — the multiplexed, coordinator-sequenced write/fetch path this engine generalizes.
- fjord ADR-008 — central-coordinator sequencing (the `Sequencer` impl fjord will plug in).

## Review Checklist

- [x] Context names a specific problem — one PUT per produce is unaffordable.
- [x] Decision statement is actionable.
- [x] At least two alternatives were evaluated with concrete pros/cons.
- [x] Selected option's rationale explains why it wins.
- [x] Consequences include positive and negative impacts with mitigations.
- [x] Risks are specific with probability/impact.
- [x] Validation defines how we'll know the decision was right.
- [x] Supersession recorded against ADR-001.
- [x] Concern impact section complete.
