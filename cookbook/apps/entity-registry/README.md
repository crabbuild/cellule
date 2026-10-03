# Entity registry

One canonical device key selects one independently owned SQL Cell. Registration
and conditional edits atomically publish the device state and a native Effect
for a separate SQL directory. Directory delivery runs asynchronously; device
writes can succeed while the directory is behind. The source client exposes
explicit projection progress so callers can distinguish pending delivery from a
missing or older directory row.

```sh
sh cookbook/scripts/entity-registry.sh
```

The default demo registers a fresh device, replays its retained identity, proves
that its directory entry is still missing and pending, edits its attributes,
and starts owned signed delivery workers. It loses a receiver reply after actual
publication, resolves the native inbox, verifies revision-two convergence, and
drains the process. Docker and Rust 1.97 or newer are required.

## Persistent CLI journey

Run from the repository root. These commands use one development installation
and the shared persistent local storage. Shell access is the authorization
boundary; embedding applications authenticate and authorize before selecting a
tenant or constructing a client. The binary does not expose a network ingress.

```sh
mkdir -p cookbook/.state/entity-inputs
cat > cookbook/.state/entity-inputs/register.json <<'JSON'
{"key":"weather-roof","expected_revision":0,"attributes":{"name":"Weather sensor","location":"Roof","enabled":true}}
JSON
sh cookbook/scripts/entity-registry.sh prepare \
  cookbook/.state/entity-inputs/register.json cookbook/.state/entity-inputs/register.mutation.json
sh cookbook/scripts/entity-registry.sh apply \
  cookbook/.state/entity-writer-a cookbook/.state/entity-inputs/register.mutation.json
sh cookbook/scripts/entity-registry.sh progress cookbook/.state/entity-writer-a weather-roof
sh cookbook/scripts/entity-registry.sh lookup cookbook/.state/entity-writer-a weather-roof
printf '["weather-roof"]\n' > cookbook/.state/entity-inputs/roster.json
sh cookbook/scripts/entity-registry.sh serve \
  cookbook/.state/entity-writer-b cookbook/.state/entity-inputs/roster.json 3 0 lose-reply
sh cookbook/scripts/entity-registry.sh get cookbook/.state/entity-writer-b weather-roof
sh cookbook/scripts/entity-registry.sh list cookbook/.state/entity-writer-b - 20
sh cookbook/scripts/entity-registry.sh resolve \
  cookbook/.state/entity-writer-b cookbook/.state/entity-inputs/register.mutation.json
```

The writer-a commands finish and drain before writer-b starts. The successor
uses a fresh enrollment session and SQLite working directory, acquiring the
idle device under a higher fencing epoch. `get` reports the observed authority
session, epoch, Cell identity, and incarnation. The key, Cell identity, and
incarnation remain stable through that transfer. A live owner cannot be stolen.

For an edit, prepare a new file with the revision read from `get`:

```json
{"key":"weather-roof","expected_revision":1,"attributes":{"name":"Roof weather sensor","location":"Roof","enabled":false}}
```

Keep each prepared file unchanged. `apply` reuses its exact input, timestamps,
and request identity; `resolve` only observes its original outcome. Preparation
uses create-new files and fsync; it refuses to overwrite existing evidence. An
expired mutation does not prove that it never executed. To register another
logical device, choose another canonical key, not another name for the same key.
There is no delete/recreate operation that resets a key's revision history.

## Contracts and bounds

| Contract | Behavior |
| --- | --- |
| Entity key | 1–64 lowercase ASCII letters, digits, and internal hyphens; no normalization or aliases. Framework version-one hashed entity partition. |
| Attributes | Name and location are 1–120 UTF-8 bytes, without padding or control characters; enabled is a boolean. |
| Source atomicity | One durable singleton state and its Effect intent per accepted change. Revision zero registers; a positive expected revision must match. |
| Conflict | Durable business rejection; retained replay returns the same decision and receipt. It emits no directory intent. |
| Projection | Full-state replacement only at a higher revision. An exact duplicate or older revision is harmless; differing data at the same revision is a terminal conflict. |
| Receiver idempotency | Native signed Effect inbox and retained resolution, with application revision checks beyond inbox retention. |
| Receipt scope | Source receipts gate only that device. Directory receipts gate only the directory. Source success never implies directory visibility. |
| Progress | Pending for ready/leased Effects; delivered after native source acknowledgement; failed for terminal failure; unavailable when ledger evidence is missing. Missing evidence never proves receiver absence. Progress verifies unchanged source state around its ledger read, retrying at most three times if an edit races it. |
| Expiry/repair | Native Effects expire after seven days. Terminal delivery failure closes readiness. Investigate the cause, then prepare an edit at the current source revision to emit a fresh full-state intent. |
| Source roster | Explicit 1–3 distinct keys per local runner; a directory listing cannot enumerate undispatched sources. Larger deployments provide catalog discovery and fleet routing. |
| Directory | At most 1,024 device keys; further registration projections reject durably and stop readiness. Existing-key updates remain possible. |
| Reads | 1–100 devices per keyset page. Each read has its own current directory position; pages are not a shared snapshot. |
| Resource profile | Four resident SQL workers, 64 MiB database / 16 MiB capture per Cell, shared bounded node budgets. One local node fits three sources plus the directory. |
| Process | JSON input at most 4 KiB; bounded 64-event diagnostic channel; nonblocking delivery observations; 15-second native Effect lease; 20-second owned task drain. |

The library can bind source and directory workers to different `LocalNode`
instances. `spawn_delivery` verifies their application scope and pins the exact
destination handle. Its process-local transport signs requests, verifies the
pinned signer, and uses the production peer dispatcher. Network applications
supply authenticated transport, trust discovery, and their domain permissions.
The directory authorizer permits only Describe, the projection command, and
native Effect resolution within its pinned destination and principal scope.
Stop source workers before draining a separate destination node.

## Code and verification

[Domain handlers](src/domain.rs) own transactions and projection invariants;
[models](src/model.rs) own canonical keys and explicit version-one wire codecs;
[clients](src/lib.rs) expose domain capabilities; [delivery](src/service.rs)
owns native Effect supervisors; [binary](src/main.rs) owns CLI, provider,
retained files, diagnostics, and shutdown.

```sh
cargo test --manifest-path cookbook/Cargo.toml \
  -p cellule-cookbook-entity-registry --test application --locked
```

The [public application tests](tests/application.rs) exercise actual framework
node assembly, competing edits, source isolation, receipt scope, monotonic
receiver behavior, signed lost-reply settlement, independent source ownership
transfer while the directory keeps its owner, cold recovery, and terminal
failure evidence and repair. Capacity competition verifies one final admission
and a durable rejection without blocking existing-key updates. The [process scenario](../../scenarios/entity-registry.py)
runs only in CI or an isolated source snapshot against private storage. It
kills a serving process after directory publication, verifies live-owner
refusal, waits for native enrollment expiry, restores under a successor,
resolves delivery, checks bounded pages, drains on SIGTERM, and preserves the
crashed boot's evidence. These local checks do not claim fleet-scale or provider
qualification.
