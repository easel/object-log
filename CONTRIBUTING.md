# Contributing to object-log

Thanks for your interest in improving object-log! This is a small, focused crate
— an append-only log core over a pluggable object store — and contributions that
keep it small and well-tested are very welcome.

## Development

Requires Rust 1.85+ (edition 2024). Before opening a PR, please run the same
checks CI runs:

```sh
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

All public items must be documented (the crate sets `#![deny(missing_docs)]`).

## Guidelines

- Keep the dependency footprint minimal.
- New behavior should come with tests. The segment codec and log invariants are
  covered by property tests under `tests/` — extend them rather than relying on
  example-based tests alone where practical.
- Discuss larger changes (new traits, on-disk format changes) in an issue first.
  The on-disk segment format is versioned; any change must bump the version and
  preserve the ability to read prior versions.

## Reporting bugs / security issues

Open a GitHub issue for bugs. For anything security-sensitive (e.g. a way to make
the segment decoder panic or read out of bounds on untrusted input), please
report it privately to the maintainer rather than in a public issue.

## License

By contributing, you agree that your contributions will be dual-licensed under
the MIT and Apache-2.0 licenses, as described in `README.md`, without any
additional terms or conditions.
