# PROPOSAL: n-tier hot/cold model for object-log — **REJECTED**

Status: **REJECTED (2026-06-17, adversarial review — 2 REJECT / 1 isolation-only converge).**
object-log stays cold-only per ADR-002. niflheim integrates at the **cold tier
only** (it keeps its own proven local-disk hot tier). Kept as a design record of
why the full in-object-log n-tier model was not pursued.

## Verdict

The operator's condition was: pursue the full n-tier engine **only if it does not
wholly violate object-log's contract.** Adversarial review found it does:

1. **`HotStore` is not generic (C1 fails).** It is niflheim's WAL hot tier renamed:
   `append → intra-segment byte position`, a `SyncLevel` that is niflheim's
   `FireAndForget/Tracked/Durable` fsync taxonomy, and `seal`/`read(range)` that
   need framing knowledge. Making it serve niflheim forces either payload parsing
   (breaks opaque payloads, C3) or niflheim-specific metadata (breaks genericity).
   No second hot-tier consumer exists — one-consumer contortion.
2. **It re-introduces mutation crash-windows ADR-002 eliminated (C5 fails).** The
   migrator (seal → PUT cold → relocate index → remove hot) mutates a live record's
   location. Crash windows: orphan cold object (crash after PUT, before relocate —
   and ADR-002 has no reaper), orphan hot segment, and `truncate_before` racing
   migration. `relocate` would need a cross-operation atomicity contract vs
   `lookup`/`fetch`/`truncate_before` that the proposal cannot supply without
   rebuilding niflheim's GC safety-margin/pinning/boot-recovery machinery.
3. **It roughly doubles object-log's correctness surface for zero new capability.**
   niflheim already has sub-ms durable acks via its own hot tier; the full
   integration buys *elegance*, not a capability, while destroying the "small,
   auditable" property that justified extracting object-log.

**Isolation was achievable** (keep `BatchLocation` a flat struct — not an enum;
feature-gate the entire tiered surface; CI-guard against cargo feature-unification
enabling `tiered` in fjord). But isolation does not rescue 1–3.

**Decision:** object-log = niflheim's **cold tier**; niflheim keeps its hot tier.
This is the fallback this proposal itself named and the position ADR-002 already
converged on. The cold-tier integration is real code-sharing (niflheim swaps its
cold WAL backend to object-log) and uses the generic primitives ADR-002 already
added (`truncate_before`, index-only offset bounds, optional streaming read).

---

_Original proposal (rejected) below, for the record._

## Goal

Let object-log own an **optional** hot tier + cold tier + hot→cold migration (so
niflheim's full WAL — hot + cold + coordination — can run on object-log), WITHOUT
violating the object-log contract:

- **C1 — generic, no consumer leakage.** No Kafka and no WAL/niflheim concepts
  (tenants, record codecs, fsync-mode names) in the core.
- **C2 — fjord untouched & diskless.** fjord's cold-only path is the ADR-002
  converged design, byte-for-byte; fjord configures zero hot tiers and never
  compiles or calls tiering machinery.
- **C3 — opaque payloads** in every tier.
- **C4 — sequencing/relocation behind seams.**
- **C5 — object-log stays a coherent, small, auditable generic product.**

If any of C1–C5 cannot hold, **reject** and fall back to "object-log is the cold
tier; niflheim keeps its own hot tier."

## Design

### Two storage ports

- **`ColdStore`** = the `BlobStore` from ADR-002 (immutable objects:
  put/get/list/delete). Unchanged.
- **`HotStore`** (NEW, optional) — a generic **durable incremental appender**:
  ```rust
  trait HotStore: Send + Sync {
      fn append(&self, seg: SegId, bytes: &[u8], sync: SyncLevel) -> Result<HotPos>;
      fn seal(&self, seg: SegId) -> Result<Sealed>;   // -> immutable bytes to migrate
      fn read(&self, seg: SegId, range: Range<u64>) -> Result<Bytes>;
      fn list_open(&self) -> Result<Vec<SegId>>;       // for crash recovery
      fn remove(&self, seg: SegId) -> Result<()>;
  }
  ```
  Default impl `LocalDiskHotStore` (segment files + `fdatasync`). The port is
  generic: any low-latency durable appender (local disk, NVMe). No niflheim vocab.

### Engine modes

- **Cold-only mode** (no `HotStore`): EXACTLY ADR-002 — buffer in memory,
  group-commit PUT to cold, `Sequencer.commit`, ack `Sequenced`. **This is fjord.**
- **Tiered mode** (`HotStore` present): `produce` appends to an open hot segment
  with `fdatasync` → resolves `Durable` *fast*; a background **migrator** seals a
  hot segment at size/age, PUTs the sealed bytes to cold as one object, calls the
  sequencer to **relocate** those entries' locations hot→cold, then `remove`s the
  hot segment. `fetch` merges hot (recent) + cold (older).

### Durability levels (already in ADR-002) map onto tiers

`Buffered` (in memory) · `Durable` (in *a* durable tier — hot `fdatasync` in
tiered mode, cold PUT in cold-only mode) · `Sequenced` (offset assigned).

### Sequencer seam: relocation is a SEPARATE trait

```rust
// Cold-only sequencers (fjord's coordinator) implement ONLY this — unchanged.
trait Sequencer { type Meta; fn commit(...); fn lookup(...); fn high_watermark(...);
                  fn log_start_offset(...); fn truncate_before(...); }

// Tiered mode REQUIRES this; fjord never implements or sees it.
trait RelocatableSequencer: Sequencer {
    fn relocate(&self, moves: &[(PartitionKey, OffsetRange, BatchLocation)]) -> Result<()>;
}
```

`BatchLocation` becomes `enum { Hot { seg, off, len }, Cold { object_id, off, len } }`.
In cold-only mode it is **always** `Cold` — fjord sees a single-variant enum, no
behavior change.

### Crash recovery (tiered mode only)

On restart the engine calls `HotStore::list_open`, reconciles against the
sequencer's index (hot entries not yet relocated are replayed/re-committed; sealed-
but-un-migrated segments are re-PUT then relocated). Cold-only mode keeps ADR-002's
recovery (none needed — immutable objects, atomic commit).

### Containment

- The hot tier, migrator, and `RelocatableSequencer` live behind a **`tiered`
  cargo feature**. Default object-log compiles cold-only — the converged ADR-002
  crate. fjord depends without the feature.
- Reframe object-log's identity: *"a tiered append log whose **system of record is
  object storage**; an optional hot tier accelerates durable acks."* Object storage
  remains the durable system of record; the hot tier is a write-ahead accelerator.

## The contested point (for reviewers)

C5 + the name. object-log is "object storage as an append log." A **local-disk**
hot tier is a *different durability substrate*. Does the optional, feature-gated,
generically-named `HotStore` keep object-log coherent — or does it turn a small,
sharp object-storage log into a general tiered-log engine that betrays its
contract and its auditability? Reviewers may REJECT on this basis.

## Rejection triggers (reviewers should test each)

1. The `HotStore` port cannot be made generic without niflheim-shaped assumptions.
2. The migrator/relocation cannot be kept out of fjord's cold-only path (entangles
   the coordinator or the diskless model).
3. The added correctness surface (migrate + relocate + multi-tier read + multi-tier
   crash recovery) is large enough to destroy object-log's "small/auditable" value —
   i.e., it re-implements niflheim's hard-won WAL inside object-log for one consumer.
4. "Object storage as an append log" cannot honestly house a local-disk hot tier.
