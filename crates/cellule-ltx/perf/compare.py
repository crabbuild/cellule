#!/usr/bin/env python3
"""Run alternating independent processes and retain raw latency evidence."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p / 100) - 1)]


def describe(values):
    return {"p50": percentile(values, 50), "p95": percentile(values, 95),
            "p99": percentile(values, 99), "max": max(values), "samples": len(values)}


def remote_summary(reports):
    samples = [s for report in reports for s in report["samples"]]
    totals = [s["command_total_us"] for s in samples]
    rates = [len(r["samples"]) * 1e6 / sum(s["command_total_us"] for s in r["samples"]) for r in reports]
    return {"commands_per_second": statistics.median(rates),
            "process_commands_per_second": rates, "command_us": describe(totals),
            "prepare_us": describe([s["elapsed_us"] for s in samples]),
            "commit_us": describe([s["commit_us"] for s in samples]),
            "capture_us": describe([s["capture_us"] for s in samples]),
            "objects_per_command": sum(s["objects"] for s in samples) / len(samples),
            "bytes_per_command": sum(s["bytes"] for s in samples) / len(samples),
            "restore_us": [r["restore_us"] for r in reports]}


def local_summary(reports, grouped):
    rounds = [s for r in reports for s in r["samples"]]
    n = reports[0]["config"]["transactions"]
    rates = [statistics.median(n * 1e6 / (s["workload_write_us"] + s["capture_us"]) for s in r["samples"]) for r in reports]
    result = {"commands_per_second": statistics.median(rates),
              "process_commands_per_second": rates,
              "round_total_us": describe([s["total_us"] for s in rounds]),
              "round_recovery_us": describe([s["recovery_us"] for s in rounds]),
              "input_ltx_bytes": statistics.median(s["input_ltx_bytes"] for s in rounds),
              "compacted_ltx_bytes": statistics.median(s["compacted_ltx_bytes"] for s in rounds)}
    if not grouped:
        samples = [s for r in rounds for s in r["command_samples"]]
        result["command_us"] = describe([s["commit_us"] + s["capture_us"] for s in samples])
        result["capture_us"] = describe([s["capture_us"] for s in samples])
    else:
        result["latency_contract"] = "group completion only; no independent durable command percentile"
    if "capture_parent_sync_us" in rounds[0]:
        result["phase_us_per_round"] = {key: statistics.median(s[key] for s in rounds)
            for key in rounds[0] if key.startswith("capture_") and key.endswith("_us")}
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--target-dir", type=Path, required=True, help="external per-checkout build directory")
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--processes", type=int, default=3)
    ap.add_argument("--commands", type=int, default=128, help="measured commands per process/round")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=8, help="remote warmup commands")
    ap.add_argument("--payloads", type=int, nargs="+", default=[4096, 65536])
    ap.add_argument("--only", choices=["local", "remote", "all"], default="all")
    args = ap.parse_args()
    if min(args.processes, args.commands, args.rounds, *args.payloads) < 1 or args.warmup < 0:
        ap.error("counts and sizes must be positive; warmup must be nonnegative")
    args.output.mkdir(parents=True, exist_ok=True)
    scratch = args.output / "scratch"
    scratch.mkdir(exist_ok=True)
    env = dict(os.environ, TMPDIR=str(scratch.resolve()))
    binaries = {
        "cellule": args.target_dir / "cellule/release/cellule-ltx-perf-crab",
        "celld": args.target_dir / "celld/release/cellule-ltx-perf-celld",
        "cellule-remote": args.target_dir / "replica/release/cellule-ltx-replica-cost",
        "celld-remote": args.target_dir / "celld/release/replica_cost",
    }
    groups = {}
    start = time.monotonic()
    def run(key, binary, options):
        raw = args.output / f"{key}.json"
        with raw.open("w") as out, (args.output / f"{key}.stderr").open("w") as err:
            completed = subprocess.run([str(binary), *options], env=env, stdout=out, stderr=err)
        if completed.returncode:
            raise RuntimeError(f"{key} exited {completed.returncode}; see {raw.with_suffix('.stderr')}")
        report = json.loads(raw.read_text())
        print(f"{key}: verified ({time.monotonic() - start:.1f}s)", flush=True)
        return report
    for scope in ["local", "remote"]:
        if args.only not in [scope, "all"]:
            continue
        for payload in args.payloads:
            for entropy in ["periodic", "random"]:
                modes = (["cellule", "celld", "celld-sync-parent", "cellule-batch8"] if scope == "local"
                         else ["cellule", "celld", "celld-sync-parent"])
                reports = {mode: [] for mode in modes}
                for process in range(args.processes):
                    for mode in (modes if process % 2 == 0 else list(reversed(modes))):
                        side = "cellule" if mode.startswith("cellule") else "celld"
                        opts = ["--payload-bytes", str(payload)]
                        if entropy == "random": opts += ["--random-payload"]
                        if mode.endswith("sync-parent"): opts += ["--sync-parent"]
                        if scope == "local":
                            opts += ["--transactions", str(args.commands), "--rounds", str(args.rounds), "--warmup", "1"]
                            if mode.endswith("batch8"): opts += ["--durability-batch", "8"]
                        else:
                            opts += ["--commands", str(args.commands + args.warmup), "--warmup", str(args.warmup),
                                     "--endpoint", args.endpoint, "--bucket", args.bucket]
                            if side == "cellule":
                                opts += ["--access-key", env["AWS_ACCESS_KEY_ID"], "--secret-key", env["AWS_SECRET_ACCESS_KEY"]]
                        key = f"{scope}-{payload}-{entropy}-{mode}-{process}"
                        reports[mode].append(run(key, binaries[side + ("-remote" if scope == "remote" else "")], opts))
                for mode in modes:
                    key = f"{scope}-{payload}-{entropy}-{mode}"
                    groups[key] = (local_summary(reports[mode], mode.endswith("batch8")) if scope == "local"
                                   else remote_summary(reports[mode]))
                (args.output / "summary.json").write_text(json.dumps(groups, indent=2) + "\n")
    source_files = [Path(__file__), Path(__file__).with_name("payload.rs")]
    source_files += list(Path(__file__).parent.glob("*/src/**/*.rs"))
    metadata = {"harness_sha256": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files},"platform": platform.platform(), "machine": platform.machine(), "commands": args.commands,
                "processes": args.processes, "local_rounds": args.rounds, "remote_warmup": args.warmup,
                "payloads": args.payloads, "endpoint": args.endpoint, "bucket": args.bucket,
                "binaries_sha256": {k: hashlib.sha256(v.read_bytes()).hexdigest() for k,v in binaries.items() if v.exists()},
                "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "source_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff"])).hexdigest(),
                "elapsed_seconds": time.monotonic() - start}
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
