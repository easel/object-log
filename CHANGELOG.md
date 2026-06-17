# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
