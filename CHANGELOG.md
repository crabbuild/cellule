# Changelog

User-facing changes to the matched Cellule crate set are recorded here.

## [Unreleased]

## [0.2.0] - 2026-10-12

- Add the optional `cellule-axum` crate with a typed handle extractor,
  JSON outputs with receipts, outcome-aware HTTP errors, and a runnable local
  SQL service that drains HTTP before the runtime.
- Add typed application builders, native module registration, scoped bindings,
  OpenAPI integration, and persistent prepared commands for exact retries.
- Add durable Fleet movement, maintenance, and recovery orchestration; retain
  recovery claims during slow publication and preserve follower admission
  deadlines across clock rollback.
- Share and batch Cell publication, pipeline follower replication and checkpoint
  preparation, and bound execution and retained publication work. Performance
  reports are scoped to their measured revisions and environments; the fresh
  three-VM capacity qualification remains pending. This release does not claim
  a throughput or latency improvement over 0.1.0.
- **Breaking persisted-format change:** use Node Bundle v1 exclusively, including
  its authenticated catalog/history encodings and new `.nodebundle` object paths.
  Legacy bundle formats and paths are rejected; there is no fallback reader or
  automatic migration. Upgrade publication and recovery consumers together and
  use a fresh storage prefix. Before retiring an older deployment, drain and
  materialize its complete acknowledged history with the previous binary.
  See the [format and cutover contract](docs/bundle-coverage-implementation.md#format-and-bounds).
- Publish all eight framework crates at the matched 0.2.0 version. Consumers must
  update their Cellule dependencies together; Rust 1.97 or newer is required.

## [0.1.0] - 2026-09-28

- Initial public release of the seven matched Cellule workspace crates.
