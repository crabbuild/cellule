# PR67 application write measurements at 4957985

**Performance parity is not delivered.** One fresh matched pair increased
Fleet-configured completed throughput by 19.8%, but successful-write p99 became
4.4 times worse, Fleet rotation/fallback occurred, and audit/drain failed.
Bucket throughput decreased 2.3%. These observations do not establish a
repeatable performance improvement.

## Workload and provenance

Measurements ran on 2026-10-08 UTC against Cellule
`49579857ac4e5b9015ecb88b0a1a78e283154460`, the prior measured WAL NORMAL source
`075b2cd45cb2762f9795aab643e0a6ad14e4ca8d`, and pinned celld v0.6.1
`f2bf648663a610eefde71f3547ad61e9b896b1f0`. The current application was freshly
release-built. The baseline reused its original verified release binary; both
arms had identical client/auditor hashes, fixture bytes and pinned images.
The comparison verified the same Docker host and loaded runner.

Each case used 1,000 uniformly selected Cells, 96-byte values, SQL INSERT plus
SELECT, a two-hour request/result ledger, 128 clients, a 128-offer queue,
30 seconds warmup and a 300-second window. This is a SQL ledger workload.
There was one repetition per point, with no read load. All serving roles,
provider and client shared one ARM64 Linux VM with 8 CPUs and nominal 16 GiB RAM.
Nodes retained the 4 GiB tmpfs ceiling; Cellule retained its 64 MiB memory and
1 GiB managed disk budgets. Another VM was already running on the physical host;
host isolation and A/A variance were not established.

The original 2-CPU/2-GiB RustFS fixture was OOM-killed during the first current
Fleet window. Its observed 302.86 TPS is **invalid for a performance comparison**.
That failed run, its failed 110,112-ACK warm audit, and interrupted cleanup remain
in the evidence. The remaining original-profile cases were not run.

The six cases below use a separate **provider-headroom diagnostic**: RustFS
kept its 2-CPU limit but received an 8-GiB memory ceiling. Every arm used that
same setting and fresh Linux Docker volumes. The external runner records this
distinct profile and forces `diagnostic=true`. Repository qualification
profiles, thresholds, durations and drain deadlines were not changed. Preflight
launch failures before traffic also remain retained. The larger-provider
diagnostic cannot qualify the original profile or physical-device durability.

## Completed window results

TPS counts successful logical writes completed inside the 300-second window.
Late responses, HTTP errors and dropped offers do not count as completed TPS.

| Configured durability / offered writes per second | Prior Cellule TPS | Current Cellule TPS | celld TPS |
| --- | ---: | ---: | ---: |
| Fleet / 15,000 | 287.28 | 344.19 | 1,285.58 |
| Bucket / 2,000 | 151.40 | 147.98 | 553.78 |

Current Cellule achieved 26.8% of celld's Fleet-configured throughput and 26.7%
of its Bucket throughput. **None passed delivery/latency qualification.**
These overloaded completion rates are not sustainable capacities.

| System / durability | Successful-write scheduled p99 ms | All-attempt scheduled p99 ms | Window errors | Window queue drops |
| --- | ---: | ---: | ---: | ---: |
| Prior Cellule / Fleet | 595.18 | 165.3 | 3,316,987 | 1,096,829 |
| Current Cellule / Fleet | 2,625.04 | 159.0 | 2,752,505 | 1,643,987 |
| celld / Fleet | 372.62 | 217.6 | 1,477,993 | 2,636,334 |
| Prior Cellule / Bucket | 7,501.76 | 7,501.8 | 0 | 554,323 |
| Current Cellule / Bucket | 7,889.54 | 7,889.6 | 0 | 555,350 |
| celld / Bucket | 1,214.48 | 1,214.5 | 0 | 433,609 |

Successful-write percentiles use exact nearest-rank journal values for successful
measured offers, including late responses. There were 252 late current Fleet
successes, zero prior/celld Fleet late successes, and 256 late successes in each
Bucket case. All-attempt histograms include errors, use 100-us resolution and
exclude dropped offers. Fast overload errors conceal successful-write latency.
Every case's journal counts reconcile with attempts, errors, window successes
and total offered requests. Warmup failures/drops remain in the external report.

## Audits, mode stability and drain

| Case | ACK read/retry verification | Shutdown / cold recovery |
| --- | --- | --- |
| Prior Cellule / Fleet | 96,373 checked; 465 errors, sampled HTTP 503; 95,908 exact retries passed | Owner exceeded 120-second deadline; cold not reached |
| Current Cellule / Fleet | 119,690 checked; 3,273 errors, sampled HTTP 503; 116,417 exact retries passed | Owner exceeded 120-second deadline; cold not reached |
| celld / Fleet | 463,837 checked; all failed, sampled HTTP 500 after owner scratch exhaustion | Cold not reached |
| Prior Cellule / Bucket | All 50,569 ACKs passed both warm and cold read/exact retry | Drain 18.53 seconds; cold contract retry passed |
| Current Cellule / Bucket | All 50,362 ACKs passed both warm and cold read/exact retry | Drain 6.37 seconds; cold contract retry passed |
| celld / Bucket | All 182,209 ACKs passed both warm and cold read/exact retry | Drain 10.63 seconds; cold contract retry passed |

Prior Cellule maintained active Fleet throughout the sampled window. Current
Cellule entered rotation and disabled Fleet shipping by the final sample, so
its configured-Fleet result includes fallback and cannot claim steady Fleet
capacity. The auditor retains aggregate counts and only four error examples;
the sampled status codes do not classify every failed ACK. Its native tiered
frontier stopped at 60,025 while issuance reached
116,564. Live filesystem evidence records celld's owner tmpfs at 100%, while
each follower used about 16 MiB. Audit HTTP failures are availability failures;
they do not establish acknowledged data loss.

## Architectural implications and remaining qualification

Ordinary application bundle ACKs remain disabled. The measured Bucket fixture
has no active native node log; object-only native publication still needs
integration. Shared verified selection must enter command/read/retry visibility
and exact capture release, with admitted materializers and complete-range drain.

Successful provider PUTs per completed write were 1.75 to 2.10 for prior/current
Fleet and 3.733 to 3.727 for prior/current Bucket. These window ratios include
publication and coordination, exclude trailing drain and SDK-internal retries,
and are affected by outstanding work. They do not prove the complete lifecycle
cost target. Cellule's publication debt grew materially during Fleet load;
rotation, overload availability and drain remain concrete failures.

The next performance claim requires the real application path plus the existing
[qualification gates](write-performance-proposal.md): three matched five-minute
repetitions, zero errors/drops, stable frontiers/debt, all-ACK cold recovery,
complete drain and read guardrails. Component correctness and reduced helper
I/O are not substitutes for those measurements.

Raw builds, manifests, clients, journals, exact percentile collector, failed
runs and retained provider volumes remain outside Git under
`cellule-write-perf-8ad1`. The external evidence index is
`tps-4957985-evidence-index.json`; normalized results are
`tps-4957985-headroom-results.json`. The standard `scripts/perf` build/run/report/
compare workflow is documented in its [runbook](../scripts/perf/README.md).
The retained diagnostic runner differs only in provider memory, explicit
diagnostic provenance, and the relocated output-path guard; it is not a
qualification-profile change.
