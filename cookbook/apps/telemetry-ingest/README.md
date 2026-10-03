# Telemetry ingest reference application

This application accepts immutable device-event batches through native Queue,
permanently binds each device sequence in its own SQL Cell, and projects complete
contributions into two bounded SQL summary shards through signed native Effects.
The library owns domain schemas, codecs, typed clients, consumers, and delivery;
the binary owns shell authorization, persistent storage, retained files, and drain.

**Status: qualified and runnable.** Nine public-behavior tests pass, including
native Queue consumption, signed lost-reply resolution, cold restoration,
permanent capacity, maximum supported consumer delays, and roster-aware
publication controls. The independent persistent process scenario passed 35
checks on the final implementation. A focused latest-intent interruption
regression and two retained runs through the documented launcher also passed.

## Run and inspect

Requires Rust 1.97 or newer and Docker Compose. From the repository root:

```sh
sh cookbook/scripts/telemetry-ingest.sh
sh cookbook/scripts/telemetry-ingest.sh help
sh cookbook/scripts/telemetry-ingest.sh info cookbook/.state/telemetry-ingest cookbook-demo
sh cookbook/scripts/telemetry-ingest.sh device cookbook/.state/telemetry-ingest cookbook-demo device-0
sh cookbook/scripts/telemetry-ingest.sh summaries cookbook/.state/telemetry-ingest cookbook-demo 0
sh cookbook/scripts/local-storage.sh down
```

The demo finds two stable keys routed to different summary shards. It submits
higher and lower sequences, repeats the same events under a fresh batch ID,
and verifies source history, exact integer bucket sums, highest reading,
contiguous sequence coverage, completed audits, and settled native projections.
Each retained repeat adds two unique events to the first device and one to the
second. The demo pins a ten-second lost-reply pause to the captured latest intent
after ingress completes; this provides a real interruption point before source
acknowledgment. Event times are simulated constants; admission does not use wall-clock
lateness. The full 128-event device profile eventually refuses new sequences.

Before dispatch, the demo atomically saves `demo-active.json` with original
inputs, identities, admission times, and expected histories. After interruption,
run the demo with the same state directory to resume that exact plan. Completed
plans remain as `demo-completed-<batch>.json`. Do not delete an active plan to
refresh its inputs. If its five-minute native request window expires, inspect
permanent device bindings, audits, and Queue evidence before explicit operator
reconciliation; expired evidence proves no absence.

`down` retains authoritative objects. Explicit storage `reset` deletes them.
Source and summary SQLite working sessions have separate directories beneath
`STATE/source` and `STATE/summaries`. Preserve crash directories for diagnosis;
a fresh state directory recovers from verified authoritative objects.

## Cell boundaries and event policy

```text
retained producer request
       |
       v
native Queue [one shard] -- claims --> device SQL [one Cell per canonical key]
       ^                                  | permanent sequence + native intent
       | ack after complete audit         v
batch audit SQL [one tenant Cell]    signed Effects --> summary SQL [two shards]
```

The local service explicitly rosters one or two device Cells. Its primary node
owns those Cells, the Queue, and audit within four SQL slots. An independently
leased sibling installation session owns the two summary shards. It shares the
same compiled application and object storage prefix, but uses a separate state
root. Source workers drain while the summary receiver remains alive, then both
nodes release their leases, worker tasks, slots, and SQLite handles.

| Contract | Behavior |
| --- | --- |
| Canonical keys | Lowercase ASCII slugs of 1..48 bytes, starting with a letter; no repeated or terminal hyphens. Tenant selection is authorized by local shell access before constructing handles. |
| Device window | Positive minute-aligned immutable start and 1..16 minutes; start is inclusive and end exclusive. Identical registration is harmless; changing a window conflicts. |
| Event identity | Permanent `(device, sequence)` binding to exact timestamp and value. Sequences are positive signed integers below the SQLite maximum. |
| Duplicate events | Identical bytes under any fresh request or batch return `duplicate`; no new event, revision, or projection intent. Changed bytes conflict durably. |
| Lower sequences | Every unique in-window event counts and remains in history. The highest sequence determines the latest reading; lower sequences cannot replace it. |
| Missing sequences | Highest observed sequence and contiguous prefix from sequence one are separate fields. Gaps never imply completion. |
| Aggregation | Count and integer sum of unique admitted events in each event-time minute. Values are bounded to ±1,000,000 milliunits. No floating-point aggregation or Queue FIFO claim. |
| Reorder diagnostic | A unique event arriving below the previously observed maximum is marked reordered. Its count depends on arrival order; bucket counts and sums depend only on the unique event set. |
| Capacity | 128 permanent events per device, eight events per batch, 128 completed physical messages per audit Cell, 16 devices per summary shard. No eviction or silent identity reuse. |
| Receiver replacement | Full newer device contribution replaces earlier metadata and all buckets atomically. Older revisions are harmlessly stale; same revision with changed bytes or a changed window conflicts. |
| Pages | One shard returns 1..16 grouped minute buckets with keyset pagination and shard-local receipts. Pages and shards do not establish a shared snapshot. |

Stable application ID is `[0x39;16]`. Namespace IDs are `[0x81;16]` for ingress,
`[0x82;16]` for entity devices, `[0x83;16]` for two fixed summary shards, and
`[0x84;16]` for audit. Device keys use the framework's canonical entity partition;
summary routing uses the framework's fixed-shard scope hash. SQL migrations,
wire version one, operation IDs, namespace topology, permanent keys, and object
prefix `cookbook/telemetry-ingest/cells` are compatibility contracts.
A different compiled code identity requires an explicit migration with its
retained predecessor registry. Changing only the local state directory does
not migrate stored code. This application serves one compiled inventory;
its process qualification uses private storage for that inventory.

## Submit and retain original evidence

Write an input file, then prepare once before applying it. Preparation needs no
Docker and creates the output without overwriting an existing request file:

```json
{"tenant":"lab","operation":{"type":"register","device":"sensor","window":{"start_ms":120000,"minutes":16}}}
```

```sh
sh cookbook/scripts/telemetry-ingest.sh prepare register.json register.request.json
sh cookbook/scripts/telemetry-ingest.sh apply cookbook/.state/lab register.request.json
sh cookbook/scripts/telemetry-ingest.sh resolve cookbook/.state/lab register.request.json
```

Submit a canonical nonzero UUID batch ID and one to eight exact events:

```json
{"tenant":"lab","operation":{"type":"submit","batch":{"id":"018f2233-4455-7666-8777-8899aabbccdd","events":[{"device":"sensor","sequence":2,"at_ms":180500,"value_milli":20000},{"device":"sensor","sequence":1,"at_ms":120500,"value_milli":10000}]}}}
```

Prepare and apply it as above. Retain the physical `message` UUID returned by
Queue publication to inspect `batch STATE TENANT MESSAGE_UUID`. `record` input
also accepts one direct `event` for source inspection and operator exercises.
All retained files are bounded to 128 KiB, synced before publication, and written
without clobbering. Original mutation windows are five minutes; retries preserve
identity, event bytes, and Queue `available_at_ms`. Native Queue producer dedup
has its own bounded retention; permanent event and audit bindings are separate.

## Consumers and projection progress

Write a roster and serve the bounded local lifetime:

```json
{"tenant":"lab","devices":["sensor"]}
```

```sh
sh cookbook/scripts/telemetry-ingest.sh serve cookbook/.state/lab roster.json 60
sh cookbook/scripts/telemetry-ingest.sh progress cookbook/.state/lab lab sensor
sh cookbook/scripts/telemetry-ingest.sh lookup cookbook/.state/lab lab sensor
```

Two owned consumers claim one message each with a 30-second lease and validate
its published claim receipt before source dispatch. Entries commit independently;
a batch is not a cross-Cell transaction. Unknown roster keys get explicit
`not_in_roster` outcomes without source dispatch. Durable domain refusals are
recorded alongside successes. A complete audit binds the physical message to
its original payload and observed source answers, with source receipts retained
as scoped diagnostics. The embedding application trusts its own consumer to
supply those observations; raw diagnostic receipt bytes are not a substitute
for native signed proof.

The consumer commits the complete audit before ack. After a crash before audit,
redelivery can observe already committed entries as duplicates and finish the
remaining entries. After an audit exists, redelivery reuses its original answers
and receipts without writing devices again. Malformed native payloads, unknown
native outcomes, and exhausted audit capacity stop readiness and leave messages
unacknowledged. Source event capacity is a durable, audited business refusal.
Native Queue exhaustion remains visible as `dead` in `info`; the application
does not silently redrive or discard those messages.

Each accepted source change and its full summary Effect share one transaction.
Native supervisors perform signed delivery and original-inbox outcome resolution.
Peer authorization pins tenant, application, source Cell, destination shard,
device key, command ID, codec, and allowed operations. Conflicting/capacity
receiver answers and exhausted or expired source intents stop readiness.
`progress` reads source and native ledger coherently with bounded retries and
reports `pending`, `delivered`, `failed`, or `unavailable`. A source receipt never
proves receiver visibility; lookup and summary pages return receiver-local receipts.

For process exercises, `serve` accepts an optional controls JSON file:

```json
{"batch":"018f2233-4455-7666-8777-8899aabbccdd","after_event_ms":10000,"before_ack_ms":0,"after_summary_ms":0,"drop_reply":false}
```

`after_event_ms` delays after the first actual source publication;
`before_ack_ms` delays after complete audit publication. Each is at most ten
seconds. Controls are shared across competing consumers, so one selected claim
consumes them. `after_summary_ms` delays after actual signed receiver publication;
`drop_reply` loses that selected reply. Optional `effect` pins those delivery
controls to a nonzero 64-digit lowercase intent ID that must already belong to
one declared source. Bounded nonblocking observations do not control durability.
SIGTERM and interrupt stop new admission, finish accepted publication and
settlement, and emit one `drained` event after both nodes release resources.

## Verification

```sh
cargo test --manifest-path cookbook/Cargo.toml -p cellule-cookbook-telemetry-ingest --test application --locked
cargo clippy --manifest-path cookbook/Cargo.toml -p cellule-cookbook-telemetry-ingest --all-targets --locked -- -D warnings
```

Run [the process scenario](../../scenarios/telemetry-ingest.py) only in CI or an
isolated source snapshot against private retained RustFS storage. Its required
proofs include real crashes after partial device publication, complete audit
publication, and signed summary publication; exact native attempt-two recovery;
original audit answers; reordered/repeated event totals; scope isolation; cold
restoration; and interrupted-demo resumption. Local application qualification
does not establish provider, scale, fault-profile, or upgrade qualification.
