# Application qualification and performance

The application integration suite tests typed writes, receipt-bound reads, recovery,
peer routing, reader recruitment, rollout, and process fleets. Run performance
scenarios only against a disposable bucket and a fresh evidence directory.

The dated reports below preserve measurements from their stated source revisions.
They are not current capacity claims. Use the [quickstart](../../docs/quickstart.md)
and [qualification runner](qualification/run.sh) for a fresh run.

| Scenario | Entry | Evidence |
| --- | --- | --- |
| Local typed action | [Quickstart](../../docs/quickstart.md) | Write, publish, read-back. |
| Three-process RustFS | [`qualification/run.sh`](qualification/run.sh) | Local and forwarded gateway calls, receipts, drained sessions. |
| Entity fleet and scaling | [`qualification/entities.py`](qualification/entities.py), [`scale.py`](qualification/scale.py) | Isolated Cell ledgers and bounded traffic. |
| Fixed 12-Cell write capacity | [`qualification/scale.py`](qualification/scale.py) with `--workload capacity` | Hot, uniform, and skewed offered-rate ramps with receipt and overload checks. |
| Reader and rollout variants | [Application integration suite](tests/integration.rs) | Selection, replacement, and recovered receipts. |

```mermaid
flowchart LR
    Action[Typed action] --> Gateway[Gateway node]
    Gateway --> Owner[Owner Cell]
    Owner --> Store[Durable root]
    Store --> Read[Receipt-bound read]
    Read --> Report[Integrity and latency evidence]
```

Set `CELLULE_TEST_ENDPOINT`, `CELLULE_TEST_BUCKET`, and a unique
`CELLULE_TEST_PREFIX`; supply matching provider credentials. The
[qualification guide](../cellule-runtime/docs/delivery.md) distinguishes local
correctness from provider, fleet, and production evidence. Local and Compose
runs do not establish production capacity.

## Dated evidence

| Question | Reports |
| --- | --- |
| How did the local and three-process baselines behave? | [Local](performance/2026-09-21-local.md), [three-node](performance/2026-09-21-three-node-local.md), [three-process](performance/2026-09-21-three-process-local.md), [balanced fleet](performance/2026-09-21-balanced-three-process-local.md) |
| What did the public action and deployment probes show? | [Action](performance/2026-09-25-public-host-action.md), [RustFS](performance/2026-09-25-public-host-rustfs.md), [three-node Compose](performance/2026-09-27-three-node-compose.md), [replica Compose](performance/2026-09-27-replica-compose.md) |
| How were reader recruitment and publication evaluated? | [Recruitment](performance/2026-09-27-reader-recruitment.md), [publication](performance/2026-09-27-reader-publication.md), [expiry](performance/2026-09-27-reader-expiry.md), [loss during load](performance/2026-09-27-reader-loss-during-load.md) |
| What were the scale and mixed-load limits? | [Scaling](performance/2026-09-27-reader-scaling.md), [mixed readers](performance/2026-09-27-mixed-readers.md), [host readers Compose](performance/2026-09-27-host-readers-compose.md), [qualification notes](performance/2026-09-27-qualification-notes.md) |
| Is the writable Cell limit established? | [Write capacity measurement status](performance/2026-09-29-write-capacity.md) |

```mermaid
flowchart LR
    Workload[Scheduled action] --> Gateway[Gateway]
    Gateway --> Owner[Fenced writer]
    Owner --> Root[Published root]
    Root --> Receipt[Receipt-bound read]
    Gateway --> Samples[Latency and resource samples]
    Receipt --> Verify[Integrity check]
    Samples --> Report[Capacity report]
    Verify --> Report
```

Read each result with its workload, provider, process topology, failure status,
and source revision. An integrity pass at an overloaded point does not establish
supported throughput.
