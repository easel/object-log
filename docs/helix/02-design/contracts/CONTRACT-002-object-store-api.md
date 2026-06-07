---
ddx:
  id: contract-object-store-api
  depends_on:
    - contract-core-log-api
---

# Contract

**Contract ID**: CONTRACT-002  
**Type**: boundary  
**Version**: v1  
**Status**: draft  
**Related**: CONTRACT-001, ADR-001

## Purpose

This contract defines the storage boundary required by the object-storage backend. It is deliberately smaller than a full object-store SDK and includes the conditional operation needed for manifest commits.

## Scope and Boundaries

- In scope: object get/put/delete/list and compare-and-set for manifest bytes.
- Out of scope: authentication, bucket creation, lifecycle policies, multipart upload tuning, and provider-specific retries beyond adapter configuration.
- Owning system or team: object-log storage adapter layer.

## Normative Surface

| Element | Type / Shape | Required | Rules | Notes |
|---------|---------------|----------|-------|-------|
| `ObjectKey` | non-empty path string | yes | MUST reject NUL and `..` segments | Prevent prefix escape |
| `ObjectValue` | bytes | yes | Adapter MUST preserve bytes exactly | |
| `ObjectVersion` | opaque string/bytes | no | Returned by stores that support version/ETag-style CAS | |
| `get(key)` | async | yes | Returns missing vs bytes distinctly | |
| `put_if_absent(key, bytes)` | async | yes | Succeeds only if key is absent | Used for immutable segments |
| `compare_and_set(key, expected_version, bytes)` | async | yes for manifest stores | Succeeds only when current version equals expected | May be delegated to a control plane |
| `list(prefix)` | async | yes | Returns keys under prefix; ordering SHOULD be lexical | Used for inspection/recovery |
| `delete(key)` | async | yes | Idempotent delete preferred | Used after retention decisions |
| `capabilities()` | sync | yes | MUST report conditional write support | Queue/backend setup uses this |

## Precedence and Compatibility

- Versioning: adapters may add capabilities; manifest CAS semantics cannot weaken in v1.
- Ordering or precedence: immutable segment `put_if_absent` must occur before manifest CAS.
- Backward-compatibility rules: adapter errors must map into CONTRACT-001 errors without losing retryability.
- Deprecation rules: unsupported provider-specific capabilities must fail closed.

## Error Semantics

| Condition | Error / Outcome | Retry | Recovery Expectation |
|-----------|------------------|-------|----------------------|
| Key traversal | `InvalidObjectKey` | no | Fix caller key construction |
| Segment already exists with same bytes | success | yes | Idempotent retry |
| Segment already exists with different bytes | `ObjectConflict` | no | Corruption/operator repair |
| CAS expected version mismatch | `Conflict` | yes | Refresh manifest and retry |
| Store lacks CAS | `UnsupportedCapability` | no | Use delegated CAS or different backend |
| Transient store failure | `StorageUnavailable` | yes | Retry within caller deadline |

## Examples

```text
put_if_absent("topics/jobs/partitions/0000000003/segments/00000000000000008100.olseg", bytes)
compare_and_set("topics/jobs/partitions/0000000003/manifest", version="v7", new_manifest_bytes)
```

## Non-Normative Notes

S3-compatible adapters can implement manifest CAS directly only when the provider exposes a reliable conditional-write primitive. Otherwise object-log must use a delegated CAS store, such as a Postgres control-plane row, while segment bytes still live in object storage.

## Validation Checklist

- [x] Normative fields and rules are explicit.
- [x] Compatibility and precedence rules are explicit.
- [x] Error handling is explicit.
- [x] At least one executable test can be derived from this contract.
- [x] Non-normative notes cannot be mistaken for contract requirements.
