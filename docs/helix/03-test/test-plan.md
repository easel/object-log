---
ddx:
  id: test-plan
  depends_on:
    - prd
    - contract-core-log-api
    - contract-object-store-api
    - td-core-and-object-backend
---

# Test Plan

## Testing Strategy

**Goals**: prove Kafka-compatible core semantics, object-backend durability boundaries, pqueue/Niflheim opaque payload compatibility, and replay correctness.  
**Out of Scope**: live S3 provider tests, Kafka wire protocol, consumer groups, transactions.  
**Traceability Source**: PRD FR-1 through FR-28, CONTRACT-001, CONTRACT-002, TD-001.

### Test Levels

| Level | Coverage Target | Priority |
|-------|-----------------|----------|
| Contract | Core append/read, ack, idempotence, storage CAS | P0 |
| Integration | Memory and local object-store backend append/read/replay | P0 |
| Unit | Segment codec, object key validation, manifest conflict logic | P0 |
| E2E | pqueue-like command payload and Niflheim-like row payload through backend | P0 |

### Frameworks

| Type | Framework | Reason |
|------|-----------|--------|
| Contract | Rust integration tests | Exercisable directly against public library API |
| Integration | `tokio::test` + `tempfile` | Async storage and local filesystem coverage |
| Unit | Rust test harness | Fast deterministic codec/model checks |
| E2E | Rust integration tests | Library has no service/UI surface |

## Test Data

| Type | Strategy |
|------|----------|
| Fixtures | Small byte payloads representing Kafka records, pqueue commands, and Niflheim row payloads |
| Factories | Helper functions generating records with keys, headers, timestamps, and producer metadata |
| Mocks | Epoch guard closure returning current/fenced outcomes |

## Coverage Requirements

| Metric | Target | Minimum | Enforcement |
|--------|--------|---------|-------------|
| Critical P0 behavior | 100% | 100% | `cargo test` blocks completion |
| Public API compile coverage | 100% of exported v1 APIs used in tests | 100% | integration tests |
| Codec corruption detection | checksum, truncation, wrong topic/partition, discontinuity | all four | unit tests |

### Critical Paths (P0)

1. Append/read contiguous records with committed offsets.
2. `acks=0` returns no committed offset claim.
3. Idempotent producer duplicate retry returns original offsets.
4. Concurrent manifest CAS conflict yields one commit.
5. Stale epoch fails before visibility.
6. pqueue and Niflheim opaque payloads round-trip unchanged.
7. Local and memory object stores pass the same backend checks.

### Secondary Paths (P1-P2)

- P1: local filesystem recovery across backend instances.
- P1: benchmark codec append/replay throughput.
- P2: live S3-compatible adapter conformance.
- P2: Kafka wire/client adapter tests.

## Acceptance Criteria Layer Allocation

| AC class / source | Story Test Plan(s) | Primary Layer | Why this layer |
|-------------------|--------------------|---------------|----------------|
| PRD FR-1..FR-9 Kafka-compatible core semantics | N/A | Contract | Public API behavior is the compatibility promise |
| PRD FR-10..FR-14 object segment durability | N/A | Unit + Integration | Codec and backend append/read both matter |
| PRD FR-15..FR-18 ownership/concurrency | N/A | Integration | Requires async append and manifest conflict behavior |
| PRD FR-19..FR-23 storage adapter surface | N/A | Contract + Integration | Backends must pass shared behavior |
| PRD FR-24..FR-28 pqueue/Niflheim/Kafka use cases | N/A | E2E integration | Proves opaque payload and backend-neutral projection-facing API |

## Implementation Order

1. Unit tests for model validation and segment codec.
2. Object store tests for memory/local CAS behavior.
3. Object backend contract tests.
4. Use-case tests for pqueue, Niflheim, and Kafka producer semantics.
5. Benchmarks after correctness lands.

## Infrastructure

| Requirement | Specification |
|-------------|---------------|
| CI Tool | `cargo test`; future GitHub Actions |
| Test DB | none |
| Services | none for v1 core; live S3/Kafka tests are future optional suites |

## Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Tests prove object backend but not Kafka backend | Med | Contract requires backend-neutral test harness; Kafka adapter must run it before support claim |
| Filesystem CAS is process-local | Med | Mark local backend dev/test only |
| Benchmarks overclaim | High | Keep benchmarks separate from correctness gates and record environment |

**Known Gaps**: live S3 adapter and real Kafka backend are specified but not part of the first code slice.

## Build Handoff

**Commands**: `cargo test`  
**Priority**: Core model, codec, storage, backend, use-case tests.

**Blocking Gate**: all P0 tests pass; no docs claim live S3, Kafka backend, or Kafka wire compatibility before implementation exists.

## Review Checklist

- [x] Test levels cover contract, integration, unit, and E2E with coverage targets.
- [x] Framework choices are justified and consistent with project concerns.
- [x] Critical paths (P0) are identified and have 100% coverage requirements.
- [x] Test data strategy covers fixtures, factories, and mocks.
- [x] Coverage requirements have targets and enforcement rules.
- [x] Infrastructure requirements are specific.
- [x] Risks include unsupported adapter mitigation.
- [x] Known gaps are documented with accepted risk rationale.
- [x] Build handoff commands are concrete and runnable.
