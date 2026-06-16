# object-log

An embeddable, append-only log core with a **pluggable object-storage backend**.

`object-log` implements Kafka-style log semantics — topics, partitions, dense
monotonic offsets, idempotent producers, batched append/read — on top of a small
`ObjectStore` trait, so the durable bytes can live in memory, on a local
filesystem, or (via your own adapter) in S3-compatible object storage. It is the
storage core extracted from the [fjord](https://github.com/easel/fjord)
Kafka-compatible broker.

## Highlights

- **`ObjectStore` port** — a minimal async trait (`put` / `get` / `list` /
  `delete` / conditional `compare-and-set`) with `StoreCapabilities`. Ships with
  `MemoryObjectStore` (tests/dev) and `LocalObjectStore` (real filesystem).
- **`LogBackend`** — topic/partition append + read with dense offsets,
  `AppendBatch`/`ReadBatch`, `AckMode`, `TimestampPolicy`, and record headers.
- **`ObjectLogBackend`** — the object-storage-backed `LogBackend`: segmented,
  content-addressed objects with an `EpochGuard` for fencing concurrent writers.
- **Idempotency** — `ProducerState` for de-duplicated, gapless appends.

## Usage

```toml
[dependencies]
object-log = "0.1"
```

```rust
use object_log::{ObjectLogBackend, ObjectLogBackendConfig, MemoryObjectStore};
use std::sync::Arc;

let store = Arc::new(MemoryObjectStore::new());
let backend = ObjectLogBackend::new(store, ObjectLogBackendConfig::default());
// append/read via the LogBackend API …
```

To target a different store (e.g. S3/Garage/MinIO), implement the `ObjectStore`
trait for your client and hand it to `ObjectLogBackend`.

## Status

`0.1.x` — pre-1.0; the API may evolve. Requires Rust 1.85+ (edition 2024).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state otherwise,
any contribution intentionally submitted for inclusion in this crate by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
