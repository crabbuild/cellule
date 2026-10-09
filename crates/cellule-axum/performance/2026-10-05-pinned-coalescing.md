# Bounded pinned capture jobs

Eligible small native cuts previously dispatched separate length, read, length and merge jobs. Each cut now performs those checks and verification in one admitted blocking job. Original capture bounds, digest/index verification, file-backed fallback and the durable response gate remain unchanged.

The existing three-cut delayed-executor fixture reproduces fourteen dispatch waves in the control and verifies five in the candidate. With an injected 100 ms delay per dispatch, simulated preparation falls from 1,400 to 500 ms. This isolates dispatch overhead; it is not a node throughput or request-latency result.

A one-slot job pool completes the same path without nested admission. Cancellation retains the dispatched job and dirty reservations until the pinned read finishes. Injected read errors retain their typed source, and growth during the read fails before immutable uploads. LTX/runtime suites, local-only LTX, strict Clippy, API docs and contract checks pass in an isolated matching snapshot.

The [dataset](2026-10-05-pinned-coalescing.json) pins source identities, critical results and external evidence hashes. The [original node target](node-capacity.md) remains unqualified. Raw logs remain outside Git.
