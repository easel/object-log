---
ddx:
  id: feature-registry
  depends_on:
    - prd
---

# Feature Registry

## Features

| Feature ID | Name | PRD Subsystem | Priority | Status | Notes |
|------------|------|---------------|----------|--------|-------|
| FEAT-001 | Kafka-Compatible Core Log | Kafka-Compatible Core Log Model | P0 | defined | Topic/partition/offset records, acks, idempotent producer metadata, backend-neutral append/read |
| FEAT-002 | Object-Storage Segment Backend | Object Segment Durability | P0 | defined | Sealed segments, deterministic keys, manifest CAS, checksum replay, production batching |
| FEAT-003 | Ownership and Fencing | Ownership and Concurrency | P0 | defined | External epoch guard, stale writer rejection, no internal consensus |
| FEAT-004 | Storage Adapter Conformance | Storage Adapter Surface | P0 | defined | Object store trait, in-memory/local adapters, future S3 and Kafka adapters |
| FEAT-005 | pqueue and Niflheim Compatibility | Compatibility Use Cases | P0 | defined | Opaque payloads and backend substitutability for both systems |
| FEAT-006 | Kafka Client/Wire Adapters | Compatibility Use Cases | P2 | deferred | Producer client wrappers and wire protocol only after core semantics land |

## Dependency Notes

FEAT-001 is the authority for the shared core contract. FEAT-002, FEAT-003, and FEAT-004 implement concrete backends and safety checks without changing FEAT-001 semantics. FEAT-005 validates that pqueue and Niflheim can consume the contract without product-specific code in object-log. FEAT-006 is explicitly deferred so the core library does not claim full broker compatibility prematurely.

## Review Checklist

- [x] Every PRD subsystem maps to at least one feature.
- [x] P0 features cover the launch-critical behavior.
- [x] Deferred Kafka wire/client work is separated from core semantic compatibility.
