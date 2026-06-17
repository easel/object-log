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
use object_log::{
    AppendBatch, AppendRecord, LogBackend, MemoryObjectStore, ObjectLogBackend, PartitionId,
    ReadRequest, TopicName, TopicPartition,
};
use std::sync::Arc;

let store = Arc::new(MemoryObjectStore::default());
let backend = ObjectLogBackend::new(store);

let tp = TopicPartition::new(TopicName::new("events")?, PartitionId(0));
let batch = AppendBatch::new(tp.clone(), vec![AppendRecord::new("hello")]);
let appended = backend.append(batch).await?;
assert_eq!(appended.base_offset, Some(0));

let read = backend
    .read(ReadRequest { topic_partition: tp, start_offset: 0, max_records: 10 })
    .await?;
assert_eq!(read.records.len(), 1);
```

(A runnable version of this is the crate-level doctest — see [docs.rs](https://docs.rs/object-log).)

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
