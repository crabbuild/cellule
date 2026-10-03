# Webhook delivery

The HTTP receiver, CLI, durable library, launcher, and persistent process scenario
are implemented and qualified against private persistent storage. Fifteen focused
tests exercise the actual TCP reply drop, receiver admission, all delivery modes,
and drain after a caller disconnects. The isolated cookbook suite passes 109 tests;
the process scenario passes 19 checks, including native Activity redelivery after
receiver publication and SIGKILL.

Run the complete local journey from the repository root:

```sh
sh cookbook/scripts/webhook-delivery.sh
```

The demo registers four subscribers, publishes one event, and checks successful
delivery, an applied request followed by an actual dropped reply, a transient
503 retry, and a terminal 422 rejection. Its JSON output contains the original
source intent, each Workflow attempt, verified acknowledgements, and receiver
records in their separate receipt domains. Repeated demos reuse four subscriber
keys while generating fresh topics and event identities.

The publisher accepts a permanent event identity and atomically snapshots enabled
subscribers of its topic. Each snapshot contains a native source Effect ID and
an immutable ticket with the original destination, subscriber revision, payload,
and five-minute deadline. Replaying the event with a fresh request identity
returns that snapshot. Reusing the event identity with changed content is a
durable rejection. Subscription changes apply to later events.

One fixed SQL publisher Cell delivers signed native Effects to two native
Workflow shards. The receiver is a fourth, separate SQL Cell. Source intent,
Workflow completion, and receiver publication have separate receipt domains.
A source success means its intent is durable; it does not prove HTTP delivery.

The delivery key hashes the publisher Cell identity, event identity, subscriber
key, and subscriber revision. HTTP Activities send it as `Idempotency-Key` in
every logical retry round and native lease redelivery. The receiver permanently
binds that key to the complete canonical ticket and applies its synthetic action
at most once. A different ticket at the same key is rejected. The native
Workflow start receiver also retains a permanent ticket binding after native
terminal history retention expires.

| Contract | Bound or behavior |
| --- | --- |
| Subscribers | 16 permanent keys; optimistic revisions; disable instead of deleting |
| Published events | 1,024 permanent identities per publisher |
| Payload | 1..1,024 UTF-8 bytes; JSON expansion accounted for separately |
| Delivery Workflows | Two fixed shards; at most 16,384 permanent bindings per shard |
| Receiver records | 4,096 permanent delivery keys; request counter saturates at 20 |
| HTTP destination | Canonical numeric loopback `http://127.0.0.1:PORT/deliver`; redirects and proxies disabled |
| HTTP admission | Eight connections and eight independently owned admitted commands; 16 KiB parser buffer, 16 headers, 8 KiB body, two-second header/body deadlines |
| HTTP timeout | 500 ms connection timeout; 2 s total attempt timeout |
| Retry rounds | Three; native Activity lease redelivery uses the same business key |
| Retry delay | Recorded Workflow timers: 100 ms, then 200 ms |
| Deadline | Recorded source time plus five minutes |
| HTTP credential | `CELLULE_WEBHOOK_TOKEN`; explicit synthetic local default; never persisted in tickets |
| Shutdown | Node owns native maintenance, source runner, and two Activity runners; accepted calls finish completion before graceful worker exit |

Transport failures and HTTP 408, 425, 429, and 5xx retry. A 200 response counts as
delivered only when its acknowledgement contains the exact key, canonical ticket
digest, and one applied action. Other statuses and malformed acknowledgements
terminate the journey. Exhaustion and native completion failures preserve
uncertainty about external application. A connection refusal can establish that
that particular attempt never reached the receiver; a dropped reply cannot.

The synthetic receiver policies are `good`, `drop_once`, `transient_once`, and
`terminal`. `drop_once` publishes the receiver action before returning a durable
instruction to the HTTP embedding to close the actual connection. The HTTP
embedding returns a service error after that result, causing Hyper to close the
actual TCP connection without serializing an HTTP response.

The library client assumes the embedding has authenticated and authorized each
capability. Source peer admission permits only the declared subscriber-start
command from the exact publisher Cell, to the key-selected Workflow shard.
HTTP admission must verify the configured bearer credential, exact header key,
and tenant/application ticket scope before dispatching `ReceiveDelivery`.
Neither primitive Workflow start commands nor general SQL belong on public HTTP.

Focused library checks:

```sh
cargo test --manifest-path cookbook/Cargo.toml -p cellule-cookbook-webhook-delivery --locked
cargo clippy --manifest-path cookbook/Cargo.toml -p cellule-cookbook-webhook-delivery --all-targets --locked -- -D warnings
```

Set `CARGO_TARGET_DIR` beneath the mounted Workspace volume as described in the
[cookbook guide](../../AGENTS.md). Run broad and process qualification in an
isolated verification snapshot or CI.

## CLI and retained evidence

Prepare source actions without opening storage. Input event IDs use canonical
UUID strings; retained files and HTTP tickets use the versioned native identity
bytes. The `prepare` command creates and fsyncs a new file, and refuses to replace
existing evidence. `apply` reuses the exact native request identity. `resolve`
never dispatches again; expired evidence reports `absence_proven: false` before
opening providers. Source application, resolution, and event output include
`delivery_keys` with each subscriber and its exact CLI inspection key.

```sh
sh cookbook/scripts/webhook-delivery.sh prepare action.json mutation.json
sh cookbook/scripts/webhook-delivery.sh apply cookbook/.state/webhook-delivery mutation.json
sh cookbook/scripts/webhook-delivery.sh resolve cookbook/.state/webhook-delivery mutation.json
sh cookbook/scripts/webhook-delivery.sh subscriptions cookbook/.state/webhook-delivery
sh cookbook/scripts/webhook-delivery.sh event cookbook/.state/webhook-delivery EVENT_UUID
sh cookbook/scripts/webhook-delivery.sh delivery cookbook/.state/webhook-delivery DELIVERY_KEY_HEX
sh cookbook/scripts/webhook-delivery.sh received cookbook/.state/webhook-delivery DELIVERY_KEY_HEX
sh cookbook/scripts/webhook-delivery.sh policy cookbook/.state/webhook-delivery SUBSCRIBER drop_once
sh cookbook/scripts/webhook-delivery.sh serve cookbook/.state/webhook-delivery 60
```

A subscription action contains `kind: "subscribe"`, `id`, `topic`, `endpoint`,
`enabled`, and `expected_revision`. Revision zero creates; later changes require
the exact observed revision. A publication action contains `kind: "publish"`,
`id` as a UUID string, `topic`, and `payload`.

`CELLULE_WEBHOOK_PORT` selects the serving receiver port, default 19020. Freeze
`http://127.0.0.1:19020/deliver` or the configured port in subscriptions before
publishing. The server permits authenticated `POST /deliver` with one exact
`Idempotency-Key` and JSON content type, plus a small `GET /health` response.
Shell access is the local administrative authorization boundary. The service
owns all four Cells; run administration and inspection after it drains, since
another process cannot steal a live writer. Another embedding can expose its
own authenticated application API over the same library client.

Accepted receiver commands have their own bounded task set. Client disconnection
cannot abandon publication or release its admission slot early. SIGINT/SIGTERM
stops listener admission, drains those tasks and connections, finishes native
Activity completion, and then closes the node. Normal shutdown preserves
unrelated files beneath the configured state directory.

`apply` optionally delays after source publication for up to 10,000 ms; `serve`
optionally delays the first receiver reply after publication for the same bound.
The latter emits a bounded `receiver_published` checkpoint before the delay.
These are diagnostic controls for the
[persistent crash scenario](../../scenarios/webhook-delivery.py), which checks
source-outcome resolution, immutable fan-out, receiver commit before SIGKILL,
lease fencing, native Activity redelivery with the original key, and cold restore.
The scenario owns a temporary receiver port; storage must be private and persistent.
