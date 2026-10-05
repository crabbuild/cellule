# Final directory upload: controlled diagnostic

Combined compaction and append uploaded an intermediate directory leaf that the
append immediately replaced. Keep that verified leaf private until the final
append when both directories fit one leaf (at most 256 database pages). Larger
directories retain the existing bounded streaming path. Source verification,
immutable formats, fenced publication and admission limits are unchanged.

| Captured cuts | Ordinary PUTs, control / candidate | Compaction PUTs, control / candidate | Compaction median preparation, control / candidate, ms |
| ---: | ---: | ---: | ---: |
| 1 | 4 / 4 | 7 / 6 | 127.15 / 80.66 |
| 4 | 10 / 10 | 13 / 12 | 123.81 / 79.99 |
| 16 | 34 / 34 | 37 / 36 | 253.16 / 210.83 |

Control `4d22ebb` and candidate `afd034a` each ran 18 points: three repetitions of each cut count and variant.
The in-memory provider injected 7 ms per GET and 40 ms per PUT. Every point
replayed all original local cuts and cold-restored byte-identical bytes.
Separate regressions cover final-root identity against the existing two-step
producer, truncation/regrowth and growth into the multi-level directory path.
The upload-count regression fails with seven PUTs on control and passes with six
on candidate; the runtime also verifies one final authority CAS and lineage.

Reproduce the controlled diagnostic in an isolated checkout with an external
`CARGO_TARGET_DIR`:

```sh
cargo test -p cellule-ltx --all-features --locked captured_batch_provider_cost -- --ignored --nocapture
```

The isolated full LTX/Runtime suites, local LTX, SQL example, strict Clippy and
API docs, format, boundaries/layout and documentation gates passed.

These are synthetic stage timings, not durable write TPS or node capacity.
The unchanged 2,000-Cell RustFS workload must establish whether the saved PUT
improves throughput and latency. The target remains three 30-minute runs on
8-vCPU/16-GiB hardware, 10K durable write TPS, 50K owner-ordered read TPS and
original owner-loss/follower-only recovery qualification.

[Critical metrics and raw evidence hashes](2026-10-05-final-directory-upload.json).
Full logs remain outside Git in the evidence directory named by that file.
