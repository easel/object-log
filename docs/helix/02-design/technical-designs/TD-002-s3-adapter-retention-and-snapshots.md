---
ddx:
  id: td-s3-adapter-retention-snapshots
  depends_on:
    - td-core-and-object-backend
    - contract-object-store-api
    - prd
---

# Technical Design: TD-002 S3 Adapter, Retention, and Snapshots

## Scope

This design extends the v1 object backend from local/memory stores to real
S3-compatible object storage and defines the retention/snapshot hooks needed by
pqueue and Niflheim.

In scope:

- S3-compatible `ObjectStore` adapter.
- Provider capability detection for conditional writes.
- Delegated manifest CAS when the object provider cannot provide safe native
  compare-and-set.
- Segment retention after caller-owned snapshots.
- Snapshot marker records for consumers that project log state elsewhere.
- Cost guardrails that prevent one-command-per-object production use.

Out of scope:

- A pqueue SQLite projection implementation.
- Niflheim row format migration.
- Kafka wire protocol.
- Managed hosted control-plane service.

## Design

### S3-Compatible Object Store

The adapter implements `ObjectStore` over an S3-compatible SDK and reports a
capability profile at startup:

| Capability | Required for Production | Notes |
|------------|--------------------------|-------|
| `put_if_absent` | Yes | Native conditional put or delegated CAS required |
| `compare_and_set` | Yes | Manifest commit boundary depends on it |
| `read_after_write` | Yes for committed keys | Adapter must document provider behavior |
| `list_consistency` | No for correctness | Manifests, not LIST order, are authoritative |
| multipart upload | P1 | Needed for large segments and Niflheim-sized batches |

When native conditional writes are unavailable or ambiguous, configuration must
name a delegated CAS provider. The preferred delegated provider for small-scale
deployments is Postgres because pqueue already expects Postgres as a control
plane option. Delegated CAS remains outside the data plane: segment bytes stay
in object storage, while only manifest version decisions are stored elsewhere.

### Manifest Commit Boundary

Append acknowledgement for `AckMode::All` remains:

1. Encode segment bytes.
2. Put the immutable segment object.
3. Compare-and-set the manifest from expected version to new version.
4. Return offsets only after manifest CAS succeeds.

If step 3 fails, the segment object is an unreferenced orphan and must not be
visible to replay. Background cleanup may delete orphaned segments after a safe
age threshold.

### Retention and Snapshots

object-log does not own downstream projection state, but it must let callers
record which offsets are safe to retire.

Core concepts:

- `SnapshotRef`: caller-owned opaque metadata for a projection snapshot.
- `RetainedRange`: topic/partition offset range still required for replay.
- `RetentionPolicy`: delete-by-age, delete-before-offset, and keep-last-N
  segments.
- `RetentionPlan`: dry-run result that lists deletable manifest entries,
  segment objects, and blockers.

Snapshot flow:

1. Caller projects log records into its local state.
2. Caller writes its snapshot through its own storage path.
3. Caller registers a `SnapshotRef` with the highest fully projected offset.
4. object-log computes a retention plan that keeps segments after the snapshot
   offset plus configured safety margin.
5. object-log rewrites the manifest to mark retired segments before deleting
   objects.

The manifest rewrite is itself CAS-protected. A failed retention CAS produces a
retryable conflict and does not delete objects.

## pqueue Requirements

- pqueue can snapshot SQLite projections to object storage and retire old log
  segments after the snapshot's high watermark.
- pqueue can choose high-latency low-cost batching for S3 and a Postgres/Kafka
  backend for lower latency.
- Retention must never delete commands newer than the last durable SQLite
  snapshot plus replay safety margin.

## Niflheim Requirements

- Niflheim can reuse the same segment put, manifest CAS, retry, checksum, and
  orphan cleanup mechanics for WAL cold-tier storage.
- Niflheim can keep domain-specific row encoding and materialization outside
  object-log.
- Retention must support large batches and multipart segment objects.

## Testing

- Unit tests for provider capability validation.
- Integration tests against `LocalObjectStore` for retention planning,
  manifest rewrite, and orphan cleanup.
- S3 adapter tests against at least one local S3-compatible server before any
  production profile is accepted.
- Corruption and stale-manifest tests reused from TD-001.
- Cost guard test that rejects production configuration below minimum segment
  size or record count thresholds.

## Implementation Sequence

1. Add retention and snapshot model types with pure unit tests.
2. Add retention planning against in-memory manifests.
3. Add manifest rewrite and object deletion flow for local/memory stores.
4. Add S3-compatible store adapter as a normal runtime-configured backend.
5. Add provider capability validation and production config gates.
6. Add local S3-compatible integration tests.
