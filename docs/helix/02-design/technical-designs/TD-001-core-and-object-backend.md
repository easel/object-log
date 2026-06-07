---
ddx:
  id: td-core-and-object-backend
  depends_on:
    - contract-core-log-api
    - contract-object-store-api
    - adr-kafka-compatible-core-object-storage-backend
    - prd
    - concerns
---

# Technical Design: TD-001 Core Log and Object-Storage Backend

**Contracts**: CONTRACT-001, CONTRACT-002 | **ADR**: ADR-001 | **Scope**: initial Rust crate

## Scope

This design makes object-log buildable in small, testable increments. It covers the core log model, segment codec, object-store-backed append/read path, in-memory and local filesystem stores, conformance tests, and the extension point for Kafka-compatible backends.

In scope:

- Rust library crate `object-log`.
- Backend-neutral `LogBackend` trait.
- Object-storage backend implementing `LogBackend`.
- Immutable segment codec with checksums and contiguous offsets.
- Manifest compare-and-set commit.
- Epoch guard hook.
- Idempotent producer metadata and duplicate suppression for object backend.
- In-memory and local filesystem object stores.
- Tests that cover pqueue, Niflheim, and Kafka producer semantics.

Out of scope:

- S3 SDK adapter implementation.
- Kafka wire protocol.
- Consumer groups, transactions, ACLs, broker metadata, and rebalancing.
- pqueue command schema, Niflheim row schema, and downstream projection code.

## Technical Approach

**Strategy**: define the Kafka-compatible log contract in core types and implement a first object-storage backend that stores record batches as sealed segment objects and appends segment references to a manifest by CAS.

**Key Decisions**:

- **Backend-neutral core**: `LogBackend` exposes append/read by topic partition; object storage and Kafka-compatible brokers can implement it.
- **Object manifest as durable boundary**: object backend returns committed offsets for `AckMode::All` only after segment bytes are stored and the manifest CAS succeeds.
- **Opaque payloads**: keys, values, and headers are bytes. Domain schemas stay in pqueue/Niflheim.
- **Idempotent producer metadata is core**: producer id, epoch, and sequence metadata are part of append requests, so the object backend can suppress duplicates and Kafka adapters can map naturally.
- **CAS capability is mandatory for object manifests**: direct store CAS or delegated CAS is required before production object backend use.

**Trade-offs**:

- The core API carries more metadata than a generic byte log, but this avoids incompatibility with Kafka producer semantics.
- The object backend accepts higher acknowledgement latency to reduce object count and external broker cost.
- Local filesystem CAS is good for development and deterministic tests; provider-specific CAS behavior remains a later adapter concern.

## Component Changes

### New: Core Model

- **Purpose**: shared types for topic partitions, records, append/read requests, ack modes, producer metadata, timestamps, offsets, and errors.
- **Files**: `src/model.rs`, `src/error.rs`, `src/lib.rs`

### New: Segment Codec

- **Purpose**: encode and decode immutable object-log segments.
- **Interfaces**: `encode_segment(Segment) -> Bytes`, `decode_segment(Bytes) -> Segment`.
- **Files**: `src/segment.rs`

Segment format v1:

```text
magic              4 bytes = "OLOG"
version            u16 LE = 1
topic_len          u16 LE
topic_bytes        topic_len bytes
partition          u32 LE
base_offset        u64 LE
epoch              u64 LE
record_count       u32 LE
records...
checksum           32 bytes sha256(header_without_checksum + records)

record:
  offset_delta      u32 LE
  timestamp_ms      i64 LE
  key_len           i32 LE (-1 means null)
  value_len         u32 LE
  header_count      u16 LE
  key_bytes         key_len bytes when present
  value_bytes       value_len bytes
  headers...

header:
  name_len          u16 LE
  value_len         u32 LE
  name_bytes        name_len bytes
  value_bytes       value_len bytes
```

### New: Object Store Boundary

- **Purpose**: minimal object store trait with immutable put, get, list, delete, manifest CAS, and capability reporting.
- **Files**: `src/store.rs`

Implementations:

- `MemoryObjectStore`: deterministic tests, supports CAS.
- `LocalObjectStore`: development/integration tests, supports CAS through a process-local lock plus atomic file rename. This is not a distributed-production CAS.

### New: Object Log Backend

- **Purpose**: implement `LogBackend` over object storage.
- **Files**: `src/object_backend.rs`

Append flow:

1. Validate topic, partition, records, ack mode, and production batching constraints.
2. Load current manifest for the topic partition.
3. Check external epoch guard if configured.
4. If producer metadata matches an already committed batch, return prior offsets.
5. Assign contiguous offsets from manifest tail.
6. Encode a sealed segment and write it with `put_if_absent`.
7. Append manifest entry using `compare_and_set`.
8. Return committed offsets for `AckMode::All`; return uncommitted acknowledgement for `AckMode::None`.

Read flow:

1. Load manifest.
2. Find the first segment whose range intersects `start_offset`.
3. Read segment objects in manifest order.
4. Decode and validate checksum, topic, partition, offset range, and contiguous offsets.
5. Return up to `max_records` and the next offset.

## API/Interface Design

```rust
#[async_trait]
pub trait LogBackend: Send + Sync {
    async fn append(&self, batch: AppendBatch) -> Result<AppendResult, ObjectLogError>;
    async fn read(&self, request: ReadRequest) -> Result<ReadBatch, ObjectLogError>;
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get(&self, key: &ObjectKey) -> Result<Option<StoredObject>, ObjectLogError>;
    async fn put_if_absent(&self, key: &ObjectKey, value: Bytes) -> Result<PutOutcome, ObjectLogError>;
    async fn compare_and_set(
        &self,
        key: &ObjectKey,
        expected: Option<ObjectVersion>,
        value: Bytes,
    ) -> Result<StoredObject, ObjectLogError>;
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectKey>, ObjectLogError>;
    async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectLogError>;
    fn capabilities(&self) -> StoreCapabilities;
}
```

## Data Model Changes

No external database schema is introduced. The object backend writes:

| Object | Key shape | Contents |
|--------|-----------|----------|
| Manifest | `topics/{topic}/partitions/{partition}/manifest` | JSON v1 manifest entries and producer dedupe index |
| Segment | `topics/{topic}/partitions/{partition}/segments/{base_offset:020}.olseg` | Binary segment v1 bytes |

## Integration Points

| From | To | Method | Data |
|------|-----|--------|------|
| pqueue | `LogBackend` | Rust trait | queue command bytes as record values |
| Niflheim | `LogBackend` | Rust trait | row/WAL payload bytes as record values |
| Kafka adapter | `LogBackend` | Rust trait | produce/fetch mapped to append/read |
| Object backend | `ObjectStore` | Rust trait | segment and manifest bytes |

### External Dependencies

- **Object storage**: local/memory in v1 implementation; S3-compatible adapter later.
- **Epoch source**: caller-provided guard; object-log fails closed on stale epoch.

## Security

- **Authentication**: not in core; adapters own credentials.
- **Authorization**: not in core; callers enforce tenant/topic access before invoking object-log.
- **Data Protection**: payloads are opaque; callers own encryption/classification. Store adapters may add encryption later.
- **Threats**: object key traversal is rejected; checksum validation detects corrupted segment bytes; stale owners are fenced by caller-owned epoch guard.

## Performance

- **Expected Load**: batch append hot path must support thousands of records per batch and millions of records per hour when segment thresholds are configured appropriately.
- **Response Target**: object backend acknowledgement target is backend/object-store dependent; v1 tests validate correctness, benchmarks record local codec throughput.
- **Optimizations**: contiguous batch encoding, one manifest CAS per segment, no per-record object writes, opaque byte payloads.

## Testing

- [x] **Unit**: topic validation, segment encode/decode, checksum corruption, offset continuity.
- [x] **Integration**: append/read through object backend with memory and local object stores.
- [x] **API**: ack mode behavior, idempotent producer retry, stale epoch, CAS conflict.
- [x] **Security**: invalid object keys and topic path traversal rejection.

## Migration & Rollback

- **Backward Compatibility**: segment format has magic/version; unsupported versions fail explicitly.
- **Data Migration**: none for v1.
- **Feature Toggle**: production tiny-segment rejection can be overridden only for tests/development.
- **Rollback**: callers can stop using the object backend; committed segments/manifests remain readable by v1 tooling.

## Implementation Sequence

1. Core types and errors -- Files: `src/model.rs`, `src/error.rs`, `src/lib.rs` -- Tests: compile/unit tests.
2. Segment codec -- Files: `src/segment.rs` -- Tests: round-trip, corruption, discontinuity.
3. Object store traits and memory/local stores -- Files: `src/store.rs` -- Tests: CAS, put-if-absent, key validation.
4. Object backend append/read -- Files: `src/object_backend.rs` -- Tests: append/read, ack modes, epoch guard, idempotent retry, conflict.
5. Conformance tests -- Files: `tests/object_backend.rs` -- Tests: pqueue/Niflheim opaque payloads and Kafka semantic cases.

**Prerequisites**: PRD, ADR-001, CONTRACT-001, CONTRACT-002.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Local filesystem CAS mistaken for distributed CAS | M | H | Document local store as dev/test only; production S3 adapter must prove CAS/delegation |
| Idempotent producer semantics incomplete | M | H | Carry metadata now; test duplicate suppression; keep full transactions out of scope |
| Manifest grows too large | M | M | Future snapshot/compaction feature; v1 tests small manifests |
| Codec compatibility churn | M | M | Magic/version and explicit unsupported-version errors |

## Review Checklist

- [x] Technical approach inherits from ADR-001 and contracts.
- [x] Key decisions have documented rationale.
- [x] Trade-offs are explicit.
- [x] Component changes clearly describe new surfaces.
- [x] API/interface design includes request and response schemas.
- [x] Data model changes include object key shapes.
- [x] Integration points specify fallback behavior.
- [x] Security section addresses auth boundary, authorization boundary, and data protection.
- [x] Performance targets are numeric where v1 can verify them.
- [x] Testing section covers unit, integration, API, and security scenarios.
- [x] Migration and rollback strategy is documented.
- [x] Implementation sequence is ordered with file paths and test paths.
