# LTX performance harnesses

These harnesses measure local capture and Cell-root costs. Results depend on
payload entropy, provider, hardware, cache state, and activation history; no
number here is a production SLO.

| Harness | Measures | Entry |
| --- | --- | --- |
| Local comparison | Matched capture/restore against the Celld lineage. | [`run.sh`](run.sh) |
| Cell publication | Root preparation, provider calls, sparse activation, and compaction. | [`replica-cost`](replica-cost/) |
| Publish provider split | Provider operations inside one incremental publish, priced with recorded p95 latencies. | [`tests/cell/roots/prepare_cost.rs`](../tests/cell/roots/prepare_cost.rs) |
| Historical measurements | Dated experiments and qualification limits. | [September 2026 record](history/2026-09-benchmark-notes.md) |

The publish provider split answers where an incremental publish spends its
provider budget. Run it with the ignored flag and read the counted operations,
not the wall clock, which depends on the throttle model:

```sh
cargo test -p cellule-ltx --features replica --locked \
  --test cell prepare_cost -- --ignored --nocapture
```

Measured on 2026-09-30 against the in-memory provider with recorded p95
latencies (GET 2 ms, PUT 7 ms): a cold publish costs 8 writes and one multipart
start, while each incremental publish costs **6 writes and 2 HEADs with no
body reads**. Predecessor verification therefore already runs from the process
metadata cache and revalidates origin with one bounded HEAD wave, so the
remaining provider budget is object writes rather than verification.


```mermaid
flowchart LR
    Workload[Fixed workload] --> SQLite[SQLite commit]
    SQLite --> Capture[LTX capture]
    Capture --> Objects[Immutable objects]
    Objects --> Root[Prepared root]
    Root --> Report[Time, bytes, provider calls]
```

Use a unique `CARGO_TARGET_DIR` on the mounted workspace volume. The
[Cellule LTX guide](../docs/README.md) defines correctness; a faster root
proposal does not change the authority CAS required for acknowledgement.
