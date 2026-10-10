"""Paired Docker follower-store diagnostics using immutable, hashed binaries.

This measures append batches and encoded frames, not application TPS, quorum
replication, or the 2,000-Cell node target. Keep fleet qualification separate.
"""

import argparse
from collections import Counter
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys

IMAGE = "rust:1.97-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97"
CASES = ((1, 1, 512, "zero"), (8, 1, 128, "zero"), (8, 1, 128, "advancing"))
PAIRS = 5


def cases(profile: str) -> tuple:
    if profile == "focused":
        return CASES
    assert profile == "d1-matrix", "unknown follower diagnostic profile"
    # Fixed declared windows, bounded by the Rust fixture's retained-frame cap.
    # Each coverage mode includes every requested leader/batch-width pair.
    return tuple((lanes, frames, min(32, 65_536 // (lanes * frames) - 1), coverage)
                 for coverage in ("zero", "advancing")
                 for lanes in (1, 8, 32) for frames in (1, 16, 64))


def frozen(state: Path) -> dict:
    state = state.resolve(strict=True)
    source = json.loads((state / "evidence/source.json").read_text())
    binary = json.loads((state / "evidence/binary.json").read_text())
    path = Path(binary["path"]).resolve(strict=True)
    assert path.is_relative_to(state / "evidence/binaries"), "use a retained binary, not a mutable target"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == binary["sha256"]
    return dict(source=source, binary=binary)


def run(command: list[str], timeout: int = 180) -> str:
    result = subprocess.run(command, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {result.stderr[-2000:]}")
    return result.stdout


def verify_probe(root: Path, role: str, case: tuple, observed: dict) -> dict:
    lanes, frames, rounds, coverage = case
    config, state = observed["HostConfig"], observed["State"]
    assert config["NanoCpus"] == 4_000_000_000
    assert config["Memory"] == config["MemorySwap"] == 4_294_967_296
    assert config["PidsLimit"] == 256 and config["ReadonlyRootfs"]
    assert "ALL" in config["CapDrop"]
    assert state["ExitCode"] == 0 and not state["OOMKilled"]
    volume, = [mount for mount in observed["Mounts"] if mount["Destination"] == "/scratch"]
    assert volume["Type"] == "volume"
    log = (root / f"{role}.log").read_text()
    assert "test result: ok. 1 passed; 0 failed;" in log
    result, = re.findall(r"FOLLOWER_APPEND_DIAGNOSTIC (.*)", log)
    result = dict(field.split("=", 1) for field in result.split())
    assert tuple(int(result[key]) for key in ("lanes", "frames", "rounds")) == case[:3]
    assert result["coverage"] == coverage and int(result["batches"]) == lanes * rounds
    with (root / f"{role}.tsv").open(newline="") as source:
        samples = list(csv.DictReader(source, delimiter="\t"))
    assert len(samples) == lanes * rounds
    assert all(row["succeeded"] == "true" and int(row["frames"]) == frames for row in samples)
    assert len({row["leader"] for row in samples}) == lanes
    assert set(Counter(row["leader"] for row in samples).values()) == {rounds}
    assert all(int(row["data_sync_calls"]) > 0 and int(row["encoded_bytes"]) > 0 for row in samples)
    for row in samples:
        assert re.fullmatch(r"SessionId\([0-9a-f]{32}\)", row["leader"])
        total, append = int(row["total_us"]), int(row["append_us"])
        assert 0 <= append <= total
        assert all(0 <= int(row[key]) <= total for key in ("blocking_queue_us", "accounting_wait_us", "accounting_hold_us", "lane_wait_us", "recount_us"))
        assert all(0 <= int(row[key]) <= append for key in ("prune_us", "data_sync_us", "directory_sync_us"))
    kernel = (root / f"{role}-kernel-after.txt").read_text()
    assert "cpu.max\n400000 100000\n" in kernel
    assert "memory.max\n4294967296\n" in kernel and "memory.swap.max\n0\n" in kernel
    assert re.search(r"^oom 0$", kernel, re.M) and re.search(r"^oom_kill 0$", kernel, re.M)
    elapsed = int(result["elapsed_us"])
    assert elapsed > 0
    phases = {}
    for key in ("total_us", "append_us", "prune_us", "accounting_wait_us", "accounting_hold_us", "data_sync_us", "directory_sync_us", "recount_us"):
        values = sorted(int(row[key]) for row in samples)
        assert values[0] >= 0
        phases[key] = dict(mean=statistics.mean(values), p99=values[(len(values) * 99 + 99) // 100 - 1])
    return dict(role=role, elapsed_us=elapsed, batches_per_second=len(samples) * 1_000_000 / elapsed,
                frames_per_second=len(samples) * frames * 1_000_000 / elapsed,
                encoded_mib_per_second=sum(int(row["encoded_bytes"]) for row in samples) * 1_000_000 / elapsed / 2**20,
                phases=phases, scratch_volume=volume["Name"],
                memory_peak_bytes=int(re.search(r"memory.peak\n(\d+)", kernel)[1]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-state", type=Path, required=True)
    parser.add_argument("--candidate-state", type=Path, required=True)
    parser.add_argument("--docker-context", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--profile", choices=("focused", "d1-matrix"), default="focused")
    args = parser.parse_args()
    selected_cases = cases(args.profile)
    root = args.evidence.resolve()
    root.mkdir(mode=0o1777)
    root.chmod(0o1777)
    assert re.fullmatch(r"[a-z0-9-]{1,40}", root.name), "use a short unique evidence name"
    binaries = {"baseline": frozen(args.baseline_state), "candidate": frozen(args.candidate_state)}
    docker = ["docker", "--context", args.docker_context]
    # The source checkout need not be mounted into the selected Docker VM.
    # Copy the exact helper into the external evidence tree with the binaries.
    helper_source = Path(__file__).with_name("run-follower.sh").resolve(strict=True)
    helper = root / "run-follower.sh"
    helper.write_bytes(helper_source.read_bytes())
    manifest = dict(schema_version=2, scope="Docker follower-store diagnostic; not fleet or node qualification",
                    image=IMAGE, docker_context=args.docker_context, cpus=4, memory_bytes=4_294_967_296,
                    profile=args.profile, cases=selected_cases, pairs=PAIRS, order=[("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline") for pair in range(PAIRS)],
                    binaries=binaries, helper_sha256=hashlib.sha256(helper.read_bytes()).hexdigest(),
                    runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    docker_info=json.loads(run(docker + ["info", "--format", "{{json .}}"])),
                    platform=run(["uname", "-a"]).strip(),
                    host_cpu_count=os.cpu_count(),
                    host_memory=(run(["sysctl", "-n", "hw.memsize"]).strip() if sys.platform == "darwin" else Path("/proc/meminfo").read_text()),
                    environment_keys=sorted(key for key in os.environ if key.startswith("DOCKER_")))
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    results = []
    volumes = set()
    for lanes, frames, rounds, coverage in selected_cases:
        case = (lanes, frames, rounds, coverage)
        for pair, order in enumerate(manifest["order"]):
            paired = {}
            for kind in order:
                role = f"l{lanes}-f{frames}-r{rounds}-{coverage}-p{pair}-{kind}"
                name = f"{root.name}-{role}"
                binary = Path(binaries[kind]["binary"]["path"])
                command = docker + ["run", "--name", name, "--cpus", "4", "--memory", "4g", "--memory-swap", "4g", "--pids-limit", "256", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                    "--mount", f"type=bind,src={binary},dst=/benchmark,readonly",
                    "--mount", f"type=bind,src={helper},dst=/run-follower.sh,readonly",
                    "--mount", f"type=bind,src={root},dst=/evidence", "--mount", "type=volume,dst=/scratch",
                    "--env", "TMPDIR=/scratch", "--env", f"CELLULE_FOLLOWER_BENCH_ROLE={role}",
                    "--env", f"CELLULE_FOLLOWER_BENCH_EVIDENCE=/evidence/{role}.tsv",
                    "--env", f"CELLULE_FOLLOWER_BENCH_LANES={lanes}", "--env", f"CELLULE_FOLLOWER_BENCH_FRAMES={frames}",
                    "--env", f"CELLULE_FOLLOWER_BENCH_ROUNDS={rounds}", "--env", f"CELLULE_FOLLOWER_BENCH_COVERAGE={coverage}",
                    "--entrypoint", "/bin/sh", IMAGE, "/run-follower.sh"]
                result = subprocess.run(command, text=True, capture_output=True, timeout=180)
                (root / f"{role}-docker.log").write_text(result.stdout + result.stderr)
                inspection = subprocess.run(docker + ["inspect", name], text=True, capture_output=True, timeout=30)
                if inspection.returncode:
                    (root / f"{role}-launch-failure.json").write_text(json.dumps(dict(
                        returncode=result.returncode, stderr=result.stderr,
                        inspection_stderr=inspection.stderr), indent=2) + "\n")
                    raise RuntimeError(f"{role}: container did not launch: {result.stderr[-2000:]}")
                observed, = json.loads(inspection.stdout)
                (root / f"{role}-container.json").write_text(json.dumps(observed, indent=2) + "\n")
                assert result.returncode == 0, f"{role}: failed diagnostic retained"
                verified = verify_probe(root, role, case, observed)
                assert verified["scratch_volume"] not in volumes
                volumes.add(verified["scratch_volume"])
                paired[kind] = verified
                print(json.dumps(dict(case=case, pair=pair, kind=kind, batches_per_second=verified["batches_per_second"])), flush=True)
            paired["candidate_ratio"] = paired["candidate"]["batches_per_second"] / paired["baseline"]["batches_per_second"]
            results.append(dict(case=case, pair=pair, **paired))
            (root / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    summary = []
    for case in selected_cases:
        selected = [pair for pair in results if tuple(pair["case"]) == case]
        summary.append(dict(case=case, median_candidate_ratio=statistics.median(pair["candidate_ratio"] for pair in selected),
                            all_ratios=[pair["candidate_ratio"] for pair in selected]))
    (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
