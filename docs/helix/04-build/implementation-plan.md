---
ddx:
  id: implementation-plan
  depends_on:
    - td-core-and-object-backend
    - td-s3-adapter-retention-snapshots
    - td-conformance-kafka-backend-extraction
    - test-plan
---

# Implementation Plan

## Build Order

object-log must land before Fjord depends on it for durable Kafka produce/fetch
behavior. Work is ordered so Fjord can still build protocol scaffolding against
traits and conformance fixtures while object-log storage matures.

## Milestones

### M1: Harden Existing Core

- Extract conformance tests out of object-backend-specific tests.
- Add pqueue and Niflheim opaque payload fixtures.
- Add negative tests for malformed topics, offset gaps, stale epochs, manifest
  conflicts, and duplicate idempotent appends.
- Gate: `cargo test` and `cargo clippy --all-targets -- -D warnings`.

### M2: Retention and Snapshot Hooks

- Add snapshot and retention model types.
- Implement retention planning without deleting objects.
- Implement CAS-protected manifest rewrite for retired segments.
- Implement orphan cleanup for failed CAS writes.
- Gate: local/memory retention tests and corruption tests pass.

### M3: S3-Compatible Adapter

- Add runtime-configured S3 adapter.
- Add capability detection and production config validation.
- Add local S3-compatible integration tests.
- Gate: adapter passes object-store and log-backend conformance.

### M4: Kafka Backend and Shared Conformance

- Add runtime-configured Kafka backend adapter.
- Add env-gated tests against a real Kafka-compatible broker.
- Publish conformance fixtures for Fjord.
- Gate: memory, local, S3-compatible, and Kafka backends pass shared tests.

### M5: Extraction Readiness

- Document Niflheim object-WAL migration map.
- Document pqueue object-log/SQLite projection integration map.
- Decide whether shared Kafka wire scaffolding becomes a separate sibling crate.
- Gate: downstream integration beads can proceed without changing object-log
  core semantics.

## Test Plan

- Unit: model validation, segment codec, retention planning, provider
  capability checks.
- Integration: append/read, manifest CAS conflict, stale epoch, corruption,
  retention, orphan cleanup.
- Optional external: S3-compatible adapter and Kafka backend.
- Performance: segment encode/decode throughput, records per segment, PUTs per
  million records under representative pqueue and Niflheim batch sizes.

## Exit Criteria

- All P0 FRs in the PRD are covered by named tests or an explicit deferred bead.
- No production profile permits one-record-per-object commits.
- pqueue and Niflheim payload fixtures pass the same conformance suite.
- Fjord can depend on object-log without forking storage or segment logic.
