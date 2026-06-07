---
ddx:
  id: td-conformance-kafka-backend-extraction
  depends_on:
    - td-core-and-object-backend
    - prd
---

# Technical Design: TD-003 Conformance, Kafka Backend, and Extraction

## Scope

This design defines the shared test contract that keeps object-log usable by
pqueue, Niflheim, Fjord, and a real Kafka backend.

In scope:

- A backend conformance suite for append/read/idempotence/fencing semantics.
- A Kafka-backed `LogBackend` adapter for low-latency deployments.
- Niflheim extraction guidance for object WAL primitives.
- Boundaries between object-log and a reusable Kafka protocol crate.

Out of scope:

- Kafka TCP wire protocol inside object-log.
- Consumer group coordination.
- Niflheim event parsing or schema registry logic.
- Fjord metadata/control-plane implementation.

## Niflheim Kafka Protocol Lessons

Niflheim's Kafka path separates reusable protocol mechanics from product logic:

- `kafka-protocol` crate handles Kafka message structs and record batch codecs.
- Connection code handles frame length prefixes, request headers, API version
  selection, SASL handshake/authenticate, handler dispatch, and Kafka error
  frames.
- The reader task only reads frames into a bounded channel while the writer
  handles requests. This lets network reads continue while the durable WAL write
  is waiting, improving batch coalescing.
- Produce hot path avoids payload copies by cloning `Bytes` and decoding record
  batches only once.
- Domain logic begins after protocol decode: tenant/table routing, RBAC,
  envelope parsing, schema handling, WAL entry encoding, and materialization
  hooks are Niflheim-specific.

Reusable candidate: Kafka wire scaffolding and compatibility harness.
Non-reusable: Niflheim topic semantics, event parsing, row/WAL encoding, and
materialization triggers.

## Shared Kafka Protocol Boundary

A future shared crate should be independent of object-log and Fjord:

```rust
pub trait KafkaRequestHandler: Send + Sync {
    fn api_key(&self) -> i16;
    fn min_version(&self) -> i16;
    fn max_version(&self) -> i16;
    async fn handle(&self, request: KafkaRequestContext, body: bytes::Bytes)
        -> Result<bytes::BytesMut, KafkaProtocolError>;
}
```

It may provide:

- frame read/write with maximum frame limits,
- request/response header version selection,
- API version registry,
- SASL PLAIN/TLS plumbing,
- common error-frame construction,
- Kafka record batch decode/encode helpers,
- compatibility fixtures for Java client, librdkafka/kcat, and kafka-go.

It must not provide:

- object-log storage,
- Fjord group coordination,
- Niflheim tenant/table resolution,
- pqueue queue commands.

## Backend Conformance

Every `LogBackend` must pass the same semantic tests:

| Case | Required Behavior |
|------|-------------------|
| append batch | returns contiguous offsets after selected ack boundary |
| read by offset | returns records in partition order |
| duplicate idempotent append | returns prior offsets without duplicate visibility |
| stale epoch | fails before acknowledgement |
| manifest or backend conflict | returns retryable conflict without false ack |
| corrupt bytes | replay fails closed |
| opaque pqueue payload | bytes and headers survive round trip |
| opaque Niflheim payload | bytes and headers survive round trip |

The conformance suite should run against:

- in-memory object backend,
- local filesystem object backend,
- S3-compatible object backend,
- Kafka backend adapter,
- Fjord embedded object-log integration.

## Kafka Backend Adapter

The Kafka backend maps `LogBackend` to a real Kafka-compatible broker:

- `append` maps to Kafka produce with explicit topic/partition and configured
  `acks`.
- `read` maps to Kafka fetch/consumer assignment by topic partition and offset.
- Producer idempotence is delegated to Kafka when enabled.
- Errors are normalized to `ObjectLogError` while preserving retryability.

The adapter is a deployment choice for pqueue/Niflheim when they prefer lower
commit latency over object-storage cost optimization.

## Implementation Sequence

1. Extract backend conformance tests from `tests/object_backend.rs`.
2. Run conformance against memory and local stores.
3. Add pqueue and Niflheim opaque-payload fixtures.
4. Add Kafka backend trait adapter as a normal runtime-configured backend.
5. Add optional real-Kafka integration tests gated by environment variables.
6. File a separate repo/crate proposal for shared Kafka wire scaffolding once
   Fjord and Niflheim can agree on the minimal API.
