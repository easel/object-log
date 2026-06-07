---
ddx:
  id: concerns
---

# Project Concerns

Project Concerns declare active cross-cutting context for downstream work. They are not principles, requirements, ADRs, test plans, or implementation tasks.

## Active Concerns

| Concern | Source | Areas | Why Active | Key Practices |
|---------|--------|-------|------------|---------------|
| rust-library | project-local | `area:api`, `area:data` | object-log is a Rust embeddable library first | Keep core types small, avoid product-specific dependencies, expose testable traits, deny accidental panics in library paths |
| durability | project-local | `area:data` | The library owns durable append/replay primitives | Acks only after configured durable boundary; validate checksums during replay; make corruption and partial writes explicit errors |
| object-storage | project-local | `area:data`, `area:infra` | Object storage is the production durability substrate | Batch records into sealed segments; reject tiny production segment profiles; use deterministic keys; support S3-compatible stores through a trait |
| kafka-compatibility | project-local | `area:api` | Kafka-compatible semantics are required so callers can swap object storage and real Kafka-like backends | Model topics, partitions, offsets, keys, headers, timestamps, batches, ack levels, idempotent producer hooks, and per-partition errors; keep wire-protocol compatibility behind an adapter contract |
| tenancy-and-isolation | project-local | `area:api`, `area:data` | Consumers need tenant/stream prefix isolation without a baked-in product tenant model | Treat namespace prefixes as caller-supplied; reject path traversal; do not infer authorization inside the core library |
| verification | project-local | `area:api`, `area:data`, `area:infra` | This is correctness-critical shared infrastructure | Every P0 contract rule needs tests; storage adapters need conformance tests; no phantom claims about unsupported Kafka/S3 behavior |

## Project Overrides

| Concern | Practice | Override | Authority |
|---------|----------|----------|-----------|
| kafka-compatibility | "drop-in replacement" wording | The core MUST preserve Kafka producer/log semantics; client and wire drop-in behavior are adapter levels | ADR-001 |
| object-storage | single-record writes | Production profiles must reject one-record-per-object operation unless a test/development flag is explicit | PRD FR-13 |

## Area Labels

- `area:api` — Rust API, Kafka-shaped interfaces, adapter contracts
- `area:data` — log records, segments, manifests, object stores, replay, checksums
- `area:infra` — S3-compatible backends, local filesystem backend, CI and benchmarks
- `area:cli` — future diagnostics and repair tooling

## Concern Conflicts

| Conflict | Resolution |
|----------|------------|
| Kafka compatibility vs. embeddable simplicity | Core types preserve producer/log semantics, but broker metadata, consumer groups, transactions, and wire protocol live in explicit optional adapters |
| Low-latency writes vs. object-storage cost | Users choose segment size/latency thresholds; production object-storage mode optimizes batched durable commits |
| Generic library vs. pqueue/Niflheim needs | Core owns log mechanics; pqueue and Niflheim retain product command schemas, control-plane ownership, and projection semantics |
