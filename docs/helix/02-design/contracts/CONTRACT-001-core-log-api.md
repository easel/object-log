---
ddx:
  id: contract-core-log-api
  depends_on:
    - adr-kafka-compatible-core-object-storage-backend
    - prd
---

# Contract

**Contract ID**: CONTRACT-001  
**Type**: library  
**Version**: v1  
**Status**: draft  
**Related**: ADR-001, PRD FR-1 through FR-28

## Purpose

This contract defines object-log's normative core append/read surface. Implementations may use object storage, local disk, memory, or Kafka-compatible brokers, but they MUST preserve these semantics for callers.

## Scope and Boundaries

- In scope: topic/partition logs, record batches, offsets, acknowledgement modes, idempotent producer metadata, epoch guard outcomes, replay, and per-partition errors.
- Out of scope: Kafka wire protocol, consumer groups, transactions, broker metadata, authorization, quota management, and product-specific payload schemas.
- Owning system or team: object-log core library.

## Normative Surface

| Element | Type / Shape | Required | Rules | Notes |
|---------|---------------|----------|-------|-------|
| `TopicName` | non-empty string | yes | MUST NOT contain `/`, `..`, NUL, or empty path segments | Safe for object-store prefixes and Kafka-style topic naming adapters |
| `PartitionId` | `u32` | yes | Partition numbers are zero-based | Matches Kafka partition numbering |
| `TopicPartition` | `{ topic, partition }` | yes | Identifies one ordered log | Offset ordering is scoped here |
| `Record` | `{ key?, value, headers, timestamp_ms?, attributes }` | yes | `value` is opaque bytes; headers are ordered key/value byte pairs | Core MUST NOT inspect payload schema |
| `AppendRecord` | `Record` without offset | yes | Caller supplies topic/partition at batch level | Adapter may derive partition before append |
| `AppendedRecord` | `Record + offset + append_timestamp_ms` | yes | Returned only for committed ack modes | Replay yields this shape |
| `AppendBatch` | `{ topic_partition, records, acks, expected_epoch?, producer? }` | yes | Records MUST append contiguously in batch order | Empty batches MUST be rejected |
| `AckMode::None` | enum | yes | MAY return before durable commit; MUST NOT return committed offsets | Kafka `acks=0` compatibility |
| `AckMode::Leader` | enum | yes | Backend-defined local/leader durability; object backend MAY map to `All` or reject if unsupported | Kafka `acks=1` compatibility surface |
| `AckMode::All` | enum | yes | MUST return offsets only after backend durable boundary | Object backend uses manifest CAS; Kafka backend uses broker durable ack |
| `ProducerState` | `{ producer_id, producer_epoch, base_sequence }` | no | When supplied, backend MUST use it for duplicate suppression or return `UnsupportedIdempotence` | Required for Kafka idempotent producer compatibility |
| `AppendResult` | `{ topic_partition, base_offset?, last_offset?, record_count, acked, commit_ref? }` | yes | `base_offset` and `last_offset` MUST be `None` when `acks=None` returns before commit | |
| `ReadRequest` | `{ topic_partition, start_offset, max_records }` | yes | `start_offset` is inclusive | |
| `ReadBatch` | `{ records, next_offset, high_watermark? }` | yes | Records MUST be contiguous unless end of log is reached | |
| `EpochGuard` | async check `{ topic_partition, expected_epoch }` | no | If configured, append MUST call it before durable commit | Provides caller-owned fencing without consensus |
| `LogBackend` | async trait | yes | MUST implement append/read with these semantics | Object store and Kafka adapters implement this |

## Precedence and Compatibility

- Versioning: v1 types may add optional fields. Removing fields or changing offset/ack semantics requires a new contract version.
- Ordering or precedence: per-partition append order is the batch order observed at successful durable commit. `ProducerState` duplicate detection takes precedence over ordinary append when a replayed batch is recognized.
- Backward-compatibility rules: readers MUST reject unsupported segment or contract versions explicitly.
- Deprecation rules: deprecated fields remain readable for at least one major version.

## Error Semantics

| Condition | Error / Outcome | Retry | Recovery Expectation |
|-----------|------------------|-------|----------------------|
| Empty batch | `InvalidBatch` | no | Caller sends at least one record |
| Invalid topic | `InvalidTopic` | no | Caller changes topic |
| Stale epoch | `Fenced` | no under same epoch | Caller obtains current ownership |
| Manifest tail race | `Conflict` | yes | Caller refreshes and retries |
| Idempotent duplicate | success with original offsets | yes | Backend returns prior append result |
| Idempotent sequence conflict | `SequenceConflict` | no without reconciliation | Caller resets producer state |
| Unsupported idempotence | `UnsupportedIdempotence` | no for that backend | Choose backend or mode that supports idempotence |
| Segment checksum mismatch | `CorruptSegment` | no | Operator repairs or restores segment |
| Offset discontinuity | `OffsetDiscontinuity` | no | Operator repairs manifest/log |
| Storage transient failure | `StorageUnavailable` | yes | Retry with same producer metadata |

## Examples

```text
append:
  topic: jobs
  partition: 3
  acks: all
  expected_epoch: 9
  producer:
    producer_id: 42
    producer_epoch: 1
    base_sequence: 100
  records:
    - key: tenant-a
      value: <opaque bytes>
      headers:
        - name: content-type
          value: application/octet-stream

result:
  topic: jobs
  partition: 3
  base_offset: 8100
  last_offset: 8100
  record_count: 1
  acked: true
```

## Non-Normative Notes

For pqueue, `topic` can encode queue/shard and `value` carries command bytes. For Niflheim, `topic` can encode tenant/collection/partition and `value` carries row/WAL payload bytes. Those mappings are caller-owned.

## Validation Checklist

- [x] Normative fields and rules are explicit.
- [x] Compatibility and precedence rules are explicit.
- [x] Error handling is explicit.
- [x] At least one executable test can be derived from this contract.
- [x] Non-normative notes cannot be mistaken for contract requirements.
