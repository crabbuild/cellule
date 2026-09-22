# Architecture and ownership

Cellule is a library framework. The host application supplies object-store implementations and credentials, node transport, authentication, routing, and deployment configuration. Cellule owns the mechanics that must be identical in every embedding product.

```text
application service (HTTP, CLI, credentials, node networking)
             │
      cellule-host ───────┐
             │            │
      cellule-app          │
             │            │
      cellule-runtime ◀────┘
             │
      cellule-ltx
             │
      cellule-store ──────┐
          │              │
      cellule-types   object_store providers
```

## Boundaries

- `cellule-types` owns stable, dependency-light provider and bucket identities.
- `cellule-store` wraps `object_store` for bounded reads, retries, compare-and-swap, immutable writes, and classified failures. It cannot decide which Cell root is authoritative.
- `cellule-ltx` owns SQLite WAL capture, LTX encoding and validation, exact restore, immutable root construction, and authenticated sparse pages. It returns a proposed root; it cannot acknowledge an application request.
- `cellule-runtime` owns stable identities, control transitions, owner fencing, release and catalog state, actors, durable request outcomes, primitive implementations, and root publication. One Cell command changes one SQLite database; cross-Cell work uses durable effects and idempotent inboxes.
- `cellule-app` compiles statically linked modules into a bounded application descriptor and author-facing handles. It cannot construct providers or take over a node.
- `cellule-host` owns exactly one runtime and the lifecycle of its registered facilities. It waits for drain and shutdown; the embedding service owns network and authorization policy.

## Persisted contracts

Cell IDs, namespace IDs, partitioning, control revisions, LTX checksums, root object references, and canonical descriptors are persisted or exchanged between nodes. Changes need migration or explicit compatibility proof. The initial extraction preserves LTX and Cell object layout identifiers, but renames application and release descriptor identities and the peer schema package. Crab integration must migrate or qualify its existing persisted data and signed peer messages before switching dependencies.

## Verification levels

Unit and integration tests are carried with each crate. The application reference test exercises all registered primitives against an in-memory object store. Production qualification also requires provider round trips, owner-loss recovery, process interruption, capacity, and cross-node tests in a dedicated environment. This repository does not claim that those external gates have passed.
