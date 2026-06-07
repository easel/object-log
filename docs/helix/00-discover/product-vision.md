---
ddx:
  id: product-vision
---

# Product Vision

## Mission Statement

object-log gives infrastructure teams an embeddable Kafka-compatible log abstraction with an object-storage backend for cheap, durable batched appends.

## Positioning

For teams building high-throughput services that need durable ordered logs and want the option to trade small-write latency for cost,
object-log is an embeddable log library whose core semantics match Kafka's append/read model and whose default low-cost backend stores sealed record segments in object storage.
Unlike product-specific WAL code, object-log lets applications use one log contract and choose either a real Kafka-compatible broker or a cheaper object-storage backend.

## Vision

Durable log-backed systems should not have to couple their domain logic to a single durability substrate. object-log succeeds when projects can share one small, audited log contract across analytics ingestion, queue engines, and other replay-backed systems, while choosing Kafka for lower latency or object storage for lower cost.

**North Star**: object-log becomes the default embeddable log contract for systems that need Kafka-compatible producer semantics and can choose between real Kafka-compatible brokers and object-storage durability.

## User Experience

A service author creates a log, names a topic and partition, appends a batch of records with keys, headers, timestamps, and an expected owner epoch, and receives offsets only after the configured durable boundary commits. In production they can select the object-storage backend for cheap batched commits or a Kafka backend for lower latency; either way their projection code reads the same partition/offset stream.

## Target Market

| Attribute | Description |
|-----------|-------------|
| Who | Rust infrastructure teams building queue engines, ingestion systems, or embedded storage services |
| Pain | They need durable ordered replay but do not want Kafka operational cost or duplicated S3 WAL implementations |
| Current Solution | Kafka/Redpanda, product-local WAL code, Postgres append tables, or one-object-per-batch S3 uploads |
| Why They Switch | Shared Kafka-compatible log semantics let them swap durability backends while object-storage batching lowers durable-write cost for workloads that can tolerate commit latency |

## Key Value Propositions

| Value Proposition | Customer Benefit |
|-------------------|------------------|
| Kafka-compatible core log semantics | pqueue, Niflheim, and other users can move between object-log's object backend and real Kafka-like brokers without rewriting projection logic |
| Object-storage-backed segmented log | Durable writes scale with sealed segments instead of per-message broker storage when cost matters more than commit latency |
| Embeddable control-plane boundary | pqueue, Niflheim, and other systems can supply their own tenancy, shard ownership, and projection logic |
| Checksummed replay and manifest commits | Applications can recover acknowledged state after node loss without local disk authority |

## Success Definition

| Metric | Target |
|--------|--------|
| Primary KPI | pqueue and Niflheim can both target the object-log core contract and choose either object-storage or Kafka-compatible backends without weakening documented durability semantics |
| Contract coverage | 100% of P0 contract requirements have executable tests before a storage adapter is marked supported |
| Batch efficiency | Default production profiles reject one-record-per-object operation and document measured object writes per million records |
| Replay reliability | Segment corruption, manifest CAS conflict, and stale epoch scenarios are covered by deterministic tests |

## Why Now

pqueue and Niflheim now have converging requirements: batched object-storage durability, replayable segments, epoch fencing, and bounded local projection rebuilds. Extracting the common log substrate before either system grows more product-specific storage code avoids duplicated correctness work and creates a reusable open-source component.

## Review Checklist

- [x] Mission statement is specific — names the user, the problem, and the approach
- [x] Positioning statement differentiates from the current alternative
- [x] Vision describes a desired end state, not a feature list
- [x] North star is a single measurable sentence
- [x] User experience section describes a concrete scenario, not abstract benefits
- [x] Target market identifies specific pain points and switching triggers
- [x] Value propositions map to customer benefits, not internal capabilities
- [x] Success metrics are measurable and time-bound
- [x] Why Now section names a specific change, not a vague opportunity
- [x] Business case details, competitor matrices, requirements, and technical choices are left to their own artifacts
- [x] No implementation details (technology choices, architecture) — those belong in design
