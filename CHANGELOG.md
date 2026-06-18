# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-06-17

**Breaking re-foundation (ADR-002).** object-log is now a buffered, multiplexing,
durability-aware object-storage **log engine** with a pluggable sequencing seam,
replacing the one-object-per-append `ObjectLogBackend`.

### Added
- `BlobStore` storage port — durable-on-return `put` (multipart on S3), `get`,
  `get_range`, `list`, `delete` — with `MemoryBlobStore`, `LocalBlobStore`, and
  `S3BlobStore` (behind the `s3` feature) adapters.
- `LogEngine<S: Sequencer>` — group-commits many batches into one object, PUTs it
  durably (durable-then-sequence), and resolves produce futures at
  `Durability::{Buffered, Durable, Sequenced}`. `FlushConfig`, `fetch`,
  `truncate_before`.
- `Sequencer` seam (sync, generic over `Meta`) + `InMemorySequencer` and a
  `BlobStore`-persisted `ManifestSequencer` (crash-durable standalone log).

### Removed
- The per-append `ObjectLogBackend`, the segment codec, `EpochGuard`, the record
  model (`AppendRecord`/`RecordHeader`/`TimestampPolicy`/`AckMode`), and the
  CAS `ObjectStore` port.

## [Unreleased]

### Added
- `#![deny(missing_docs)]` with full rustdoc on the public API.
- Runnable crate-level quickstart doctest and `examples/quickstart.rs`.
- Property tests for the segment codec round-trip and append/read offset density.
- Decoder-hardening tests against arbitrary and truncated input.
- Direct unit tests for `MemoryObjectStore` / `LocalObjectStore` conditional writes.
- `CONTRIBUTING.md`, this changelog, and CI jobs for docs, MSRV, and `cargo-deny`.

### Changed
- `ObjectLogBackendConfig::min_records_per_segment` now defaults to `1`, so
  single-record appends succeed by default.
- Declared MSRV corrected to Rust 1.88 (the code uses stable let-chains); the
  previously declared 1.85 never compiled. Enforced by a CI job.

### Removed
- The test-only `allow_tiny_segments_for_tests` config flag (no longer needed).
- The unused `ObjectLogError::UnsupportedIdempotence` variant.
- The vendored `workflows/` tooling tree from the repository.

### Fixed
- README quickstart now reflects the real constructor API.
- `LocalObjectStore` temp-file naming no longer collides across keys.

## [0.1.0]

- Initial extraction from the [fjord](https://github.com/easel/fjord) broker:
  `ObjectStore` port with `MemoryObjectStore` / `LocalObjectStore`, the
  `LogBackend` trait, and the segmented, content-addressed `ObjectLogBackend`
  with idempotent producers and an `EpochGuard` fencing hook.
