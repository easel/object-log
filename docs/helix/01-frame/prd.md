---
ddx:
  id: prd
kind: product
depends_on:
  - product-vision
  - concerns
---

# Product Requirements Document

## Summary

object-log is a Rust embeddable commit-log contract with Kafka-compatible core semantics and a low-cost object-storage backend. It provides durable batched append, partitioned offsets, manifest-committed object segments, checksummed replay, and a producer/log model that can also be backed by a real Kafka-compatible broker. The first users are pqueue, which needs a cheap high-commit-latency object-log backend feeding a SQLite projection, and Niflheim, which already has an object-storage WAL that should be extracted into reusable primitives.

The product must not claim full Kafka wire-protocol drop-in behavior by default. The core library must, however, preserve enough Kafka producer and partition-log semantics that pqueue and Niflheim can use the same projection-facing contract with either object-log's object-storage backend or a real Kafka-compatible backend.

## Problem and Goals

### Problem

pqueue and Niflheim both need durable log mechanics: batch buffering, ordered partition replay, sealed segments or broker batches, checksums where the backend owns bytes, and recovery after node loss. Keeping these as product-local implementations duplicates correctness work and makes it harder to audit failure semantics. Kafka and Redpanda solve ordered durable logs with lower commit latency, but they add an external broker cluster and cost profile that pqueue explicitly wants to avoid for its cost-optimized mode.

### Goals

1. Product teams can use one shared Rust library for object-storage-backed durable log append and replay.
2. pqueue can use object-log as the basis for its `object_log_sqlite_projection` backend while retaining the option to use a Kafka backend for lower-latency deployments.
3. Niflheim can migrate hot/cold WAL segment and object-store primitives toward object-log while retaining the option to use a Kafka backend for lower-latency ingestion.
4. Kafka API semantics are an explicit requirements input, so compatibility adapters are straightforward and honest.

### Success Metrics

| Metric | Target | Measurement Method |
|--------|--------|--------------------|
| P0 contract coverage | 100% of P0 FRs covered by unit or integration tests | `cargo test` plus traceability review |
| Segment replay correctness | 100% detection of checksum mismatch, truncated segment, and offset discontinuity fixtures | deterministic corruption tests |
| CAS/fencing correctness | stale epoch and concurrent manifest conflict both fail without acknowledged records | async integration tests |
| Batch cost profile | production config rejects one-record-per-object and records segment records/object ratio | config validation tests and benchmark evidence |
| Backend substitutability | pqueue/Niflheim projection tests pass against object-storage and Kafka-like conformance backends | shared log conformance suite |

### Non-Goals

- Full Kafka broker replacement or Kafka wire-protocol implementation in the core library.
- Kafka consumer groups, transactions, exactly-once stream processing, topic auto-rebalancing, or broker metadata APIs in v1 core.
- Product-specific pqueue command envelopes, queue eligibility rules, or SQLite projection schema.
- Product-specific Niflheim row encoding, Delta materialization, or partition catalog semantics.
- Internal cluster consensus, node discovery, leader election, ZooKeeper, or etcd.

## Users and Scope

### Primary Persona: Embedded Log Integrator

**Role**: Rust infrastructure engineer building a queue engine or ingestion service  
**Goals**: Append batches durably, replay by partition offset, and retain control over ownership and projection semantics  
**Pain Points**: Existing options are either a full broker cluster or custom WAL code with subtle failure modes

### Secondary Persona: Compatibility Adapter Author

**Role**: Engineer building Kafka producer or service compatibility on top of object-log  
**Goals**: Reuse core topic/partition/offset/record semantics and implement client-facing compatibility honestly  
**Pain Points**: Full Kafka compatibility is broad; the core library must make supported subsets explicit

## Requirements

### Must Have (P0)

1. Kafka-compatible partition-log semantics for append, offsets, record metadata, acknowledgement, and per-partition errors.
2. Durable batched append to object-storage-backed segments with manifest-committed acknowledgement.
2. Partitioned ordered replay by offset with checksum validation.
3. Kafka record model: topic, partition, offset, key, value, headers, timestamp, and append timestamp policy.
4. Explicit epoch/fencing hook for owner-assigned writers.
5. Storage traits and conformance tests usable by local, in-memory, and S3-compatible adapters.
6. pqueue and Niflheim requirements captured as first-class compatibility constraints.

### Should Have (P1)

1. Producer-shaped adapter API that can accept Kafka produce semantics without the Kafka wire protocol.
2. Local filesystem object store for deterministic integration tests and development.
3. Benchmarks for segment encoding, append throughput, and replay throughput.
4. Snapshot/retention helper interfaces for systems that compact or project from the log.

### Nice to Have (P2)

1. Kafka wire-protocol service adapter.
2. Native S3 SDK adapter with compatibility tests against LocalStack, Garage, AWS S3, and R2.
3. CLI tools for manifest inspection, segment validation, and replay export.

## Functional Requirements

### Subsystem: Kafka-Compatible Core Log Model

- **FR-1** — The library MUST represent records with topic, partition, offset, optional key, value bytes, headers, timestamp, and attributes sufficient for Kafka producer/log compatibility.
- **FR-2** — The library MUST assign monotonically increasing offsets per topic partition and MUST reject or report offset discontinuity during replay.
- **FR-3** — The library MUST support appending a batch of records and returning base/last offsets only after the selected durable boundary succeeds.
- **FR-4** — The library MUST keep payload bytes opaque and MUST NOT require pqueue or Niflheim schemas in the core record model.
- **FR-5** — The core append API MUST represent Kafka producer `acks` semantics at the compatibility level: `acks=0` may return without committed offsets, while committed/durable acknowledgements return offsets only after the backend durability boundary.
- **FR-6** — The core MUST preserve per-partition ordering under retry when callers use idempotent producer metadata or a single in-flight append per partition.
- **FR-7** — The core MUST carry optional producer id, producer epoch, and base sequence metadata so an adapter can implement Kafka-style idempotent producer deduplication.
- **FR-8** — The core MUST support per-topic append timestamp policy equivalent to create-time or log-append-time behavior.
- **FR-9** — The partitioning boundary MUST support explicit partition selection and key-based partitioner adapters.

### Subsystem: Object Segment Durability

- **FR-10** — The library MUST encode immutable sealed segments containing contiguous record batches, segment metadata, and a checksum.
- **FR-11** — The library MUST write segment objects under deterministic keys that are idempotent for the same topic, partition, base offset, epoch, and checksum.
- **FR-12** — The manifest commit MUST be the default object-storage ack boundary and MUST use compare-and-set or an equivalent conditional append.
- **FR-13** — Production object-storage profiles MUST reject one-record-per-object operation unless an explicit test/development override is enabled.
- **FR-14** — Replay MUST validate segment checksum, topic, partition, base offset, record count, and contiguous offsets before yielding records.

### Subsystem: Ownership and Concurrency

- **FR-15** — Append MUST accept an expected epoch and MUST call an external epoch guard before committing when one is configured.
- **FR-16** — A stale epoch MUST fail before acknowledgement and MUST NOT make records visible in the manifest.
- **FR-17** — Concurrent writers racing to extend the same manifest tail MUST produce at most one successful commit; losers receive a retryable conflict.
- **FR-18** — The library MUST NOT perform node discovery, leader election, or cluster consensus internally.

### Subsystem: Storage Adapter Surface

- **FR-19** — The log backend trait MUST be backend-neutral enough for object-storage and Kafka-compatible implementations.
- **FR-20** — The object-store trait MUST expose put, get, list, delete, and conditional compare-and-set semantics with capability reporting.
- **FR-21** — In-memory and local filesystem adapters MUST pass the same conformance tests for core append/read behavior.
- **FR-22** — S3-compatible adapters MUST document conditional-write support and MUST fail configuration if manifest CAS cannot be provided or delegated.
- **FR-23** — A Kafka backend adapter MUST be able to map core append/read operations to Kafka produce/fetch semantics without changing pqueue or Niflheim projection code.

### Subsystem: Compatibility Use Cases

- **FR-24** — pqueue MUST be able to use object-log for tenant/queue/shard command segments, manifest ack, epoch fencing, and replay into SQLite without object-log knowing queue commands.
- **FR-25** — pqueue MUST be able to substitute a Kafka backend for the object-storage backend when a deployment chooses lower commit latency over lower cost.
- **FR-26** — Niflheim MUST be able to use object-log for sealed segment persistence, object-store retry/deadline practices, and durable replay without object-log knowing row payload schemas.
- **FR-27** — Niflheim MUST be able to substitute a Kafka backend for the object-storage backend when an ingestion deployment chooses lower commit latency over lower cost.
- **FR-28** — Kafka producer compatibility MUST be defined as an adapter layer over core topic/partition/record/ack semantics, not as an implicit promise of full Kafka broker behavior.

## Acceptance Test Sketches

| Requirement | Scenario | Input | Expected Output |
|-------------|----------|-------|-----------------|
| FR-3 | Append batch to empty partition | topic `events`, partition `0`, 3 records | result offsets `0..2`, committed manifest entry |
| FR-5 | Uncommitted send | append with `acks=0` | response has no committed offsets and no durability claim |
| FR-7 | Idempotent retry | same producer id/epoch/base sequence resent after success | duplicate is not appended twice |
| FR-12 | CAS conflict on manifest tail | two writers append from same expected tail | one succeeds, one returns conflict, replay has one segment |
| FR-15/FR-16 | Stale owner attempts append | expected epoch `1`, guard current epoch `2` | append fails fenced, no segment acknowledged |
| FR-14 | Corrupt segment replay | valid manifest points to mutated segment bytes | replay returns checksum/corruption error |
| FR-24 | pqueue command payload append | opaque command bytes keyed by shard | offsets are durable and replay bytes unchanged |
| FR-26 | Niflheim row payload append | opaque row bytes with tenant headers | replay preserves headers and payload bytes unchanged |

## Technical Context

- **Language/Runtime**: Rust 2024 edition
- **Key Libraries**: `tokio`, `async-trait`, `bytes`, `serde`, `sha2`, `thiserror`
- **Data/Storage**: backend-neutral log trait; object-store trait for the low-cost backend; in-memory and local filesystem adapters in v1; S3-compatible and Kafka backends after core contract tests
- **APIs**: Rust library API; Kafka producer semantics as requirements input; optional future Kafka client/wire adapters
- **Platform Targets**: Linux and macOS; object stores that can provide or delegate manifest CAS

### Kafka Requirements Input

The core semantic input is Apache Kafka's producer and partition-log model:

- A producer record has topic, optional partition, optional timestamp, key, value, and headers; explicit partition wins, otherwise key-aware partitioning is adapter-owned.
- Producer acknowledgement levels include no wait (`acks=0`), leader/local acknowledgement (`acks=1`), and strongest durable acknowledgement (`acks=all`/`-1`). object-log's object-storage backend maps durable acknowledgement to manifest-committed segment visibility; Kafka backends map it to broker acknowledgement.
- Producer retries can reorder records when idempotence is disabled and multiple in-flight appends target the same partition; object-log must expose either idempotent metadata or single-in-flight discipline to preserve per-partition ordering.
- Kafka idempotent producers rely on `acks=all`, retries, bounded in-flight requests, and producer sequence metadata. object-log core must carry producer id, producer epoch, and base sequence metadata so an adapter or backend can implement duplicate suppression.
- Kafka protocol compatibility is broader than the core log contract. Produce/fetch compatibility is in scope for core semantics; consumer groups, transactions, broker metadata, ACLs, quotas, and rebalancing are adapter or service concerns.

## Constraints, Assumptions, Dependencies

### Constraints

- **Technical**: Object storage has higher commit latency than broker-local disks; production mode must batch.
- **Business**: pqueue and Niflheim both need a reusable substrate before implementation divergence grows.
- **Legal/Compliance**: Core library handles opaque bytes and must not assume PII handling; callers own data classification.

### Assumptions

- Callers can provide a control-plane epoch/ownership source when stale writers must be fenced.
- Kafka compatibility can start at producer-shaped append semantics before full wire protocol.
- The first implementation can prove the contract with in-memory and local filesystem adapters before adding S3 SDK code.

### Dependencies

- pqueue TD-004 object-log + SQLite projection requirements.
- Niflheim ADR-002 WAL-to-cold-tier durability and ADR-057 WAL format/replay lessons.
- Apache Kafka protocol and producer API semantics, including Produce/Fetch, producer record fields, acknowledgement levels, retries, and idempotent producer constraints.
- Rust async ecosystem for storage adapter implementations.

## Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Kafka compatibility scope expands into a broker clone | Med | High | Make Kafka producer/log semantics P0, but keep consumer groups, transactions, broker metadata, and wire protocol in adapter contracts |
| Object-store CAS differs by provider | High | High | Capability detection plus explicit fallback/delegation contract |
| Library becomes pqueue-specific | Med | High | Keep record payloads opaque and keep queue semantics outside object-log |
| Segment format churn breaks adopters | Med | Med | Version segment format and keep compatibility rules in CONTRACT-001 |

## Open Questions

- [ ] Which S3-compatible providers must be supported first beyond local/CI? — blocks S3 adapter test matrix, ask operators.
- [ ] Should the first Kafka adapter target a Rust trait, `rdkafka`-like wrapper, real Kafka backend adapter, or wire protocol? — blocks adapter scope, ask product owner after core lands.

## Success Criteria

- Core append/read/manifest/replay tests pass for in-memory and local filesystem storage.
- pqueue and Niflheim use cases are traceable to FRs and contracts.
- Kafka API semantics are traceable to FRs and contracts.
- The docs clearly distinguish Kafka-compatible core semantics from full Kafka wire/client drop-in support.
- The code exposes no product-specific queue or row schema.

## Review Checklist

- [x] Summary works as a standalone 1-pager
- [x] Problem statement describes a specific failure mode with concrete cost
- [x] Goals are outcomes, not activities
- [x] Success metrics have numeric targets and named measurement methods
- [x] Non-goals exclude things a reasonable person might assume is in scope
- [x] Personas have specific pain points
- [x] P0 requirements are necessary for launch
- [x] Every P0 requirement has an acceptance test sketch
- [x] Requirements trace upward to the Product Vision and downward to downstream artifacts
- [x] Functional requirements are testable and carry stable `FR-n` IDs
