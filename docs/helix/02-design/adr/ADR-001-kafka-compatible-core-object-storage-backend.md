---
ddx:
  id: adr-kafka-compatible-core-object-storage-backend
  depends_on:
    - product-vision
    - prd
    - concerns
---

# ADR-001: Kafka-Compatible Core with Object-Storage Backend

## Context

object-log started as an extraction target for shared S3 WAL mechanics from pqueue and Niflheim. The interface question changes the product shape: the useful abstraction is not "a WAL file writer" but a partitioned commit log whose core semantics are compatible with Kafka's producer and partition-log model.

Kafka's producer record model includes topic, optional partition, timestamp, key, value, and headers. Kafka producer acknowledgements distinguish no wait (`acks=0`), leader/local acknowledgement (`acks=1`), and strongest durable acknowledgement (`acks=all`/`-1`). Kafka also has retry and idempotent producer semantics that matter for per-partition ordering and duplicate suppression. These semantics must shape object-log's core even if object-log does not implement the Kafka wire protocol in v1.

pqueue and Niflheim both benefit from this:

- pqueue can use object-log's object-storage backend as the cheap, high-commit-latency durability mode and later use a real Kafka-compatible backend for lower latency.
- Niflheim can use the same log contract for object-storage WAL durability and later use Kafka-like infrastructure for lower-latency ingestion if needed.

## Decision

object-log will define a **Kafka-compatible core log contract** and provide an **object-storage backend** as the first low-cost implementation.

The core contract owns:

- topics, partitions, offsets, record batches, keys, values, headers, timestamps, and append metadata;
- acknowledgement modes that can map to Kafka producer `acks`;
- optional idempotent producer metadata: producer id, producer epoch, and base sequence;
- ordered partition replay from offsets;
- per-partition append errors and retry/fencing outcomes;
- backend-neutral append/read traits that can be implemented by object storage, local test stores, or real Kafka-compatible brokers.

The object-storage backend owns:

- batching records into immutable sealed segments;
- deterministic segment keys;
- manifest compare-and-set as the durable commit boundary;
- checksum validation on replay;
- epoch guard integration for stale owner rejection;
- production configuration that rejects one-record-per-object operation.

Kafka wire protocol, consumer groups, transactions, broker metadata APIs, ACLs, quotas, and rebalancing are **not** core library responsibilities. They may be implemented as optional adapters after the core contract is proven.

## Consequences

### Positive

- pqueue and Niflheim can target one log contract and choose object storage or Kafka-like backends by deployment need.
- The object-storage backend is honestly positioned as the low-cost, higher-latency implementation rather than as a fake Kafka broker.
- Future Kafka producer and wire adapters have a semantic base instead of retrofitting compatibility later.

### Negative

- The core API is broader than a minimal WAL writer because it must carry producer acknowledgement and idempotency metadata.
- The object-storage backend must reject some technically possible configurations because they would violate the cost model.
- Full Kafka drop-in compatibility remains a separate effort and cannot be implied by the core crate.

## Alternatives Considered

### S3 WAL Writer Library Only

Rejected. This would extract useful code from Niflheim but would not give pqueue and Niflheim a shared projection-facing contract or allow swapping in a real Kafka backend.

### Full Kafka Broker Replacement

Rejected for v1. The broker surface includes consumer groups, metadata, transactions, quotas, security, replication, and protocol compatibility. That is a separate product, not the first reusable library.

### Object Storage Only Contract

Rejected. It would optimize the first backend at the cost of making Kafka a later migration rather than a compatible backend.

## Evidence Inputs

- pqueue TD-004 requires manifest-committed object segments, epoch fencing, batching, and replay into SQLite.
- Niflheim ADR-002 and ADR-057 require batched WAL segments, durable cold-tier persistence, checksummed replay, and write-mode-aware acknowledgement.
- Apache Kafka official protocol and producer documentation define the producer/log semantics that object-log must preserve:
  - https://kafka.apache.org/protocol/
  - https://kafka.apache.org/40/configuration/producer-configs/
  - https://downloads.apache.org/kafka/4.1.0/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html

## Review Checklist

- [x] Decision records Kafka-compatible core semantics as P0.
- [x] Object-storage backend remains the cheap, high-commit-latency implementation.
- [x] Full Kafka wire/client drop-in behavior is explicitly out of core scope.
- [x] pqueue and Niflheim backend substitutability is preserved.
