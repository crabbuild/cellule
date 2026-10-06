"""Calibrate absolute generator deadlines in Docker, without a server workload.

Every arrival is retained in a 24-byte big-endian row. This diagnoses timing
and CPU cost; it cannot qualify application TPS or the simultaneous node goal.
"""

import argparse
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import struct
import subprocess
import time

from entities import distribution, rows
from follower import IMAGE, frozen

GUARDS_US = (50, 2000, 500, 12_000)
RATES = (30, 60_000)
ROUNDS = 5
SECONDS = 10


def verify_window(window: dict, raw: bytes) -> dict:
    sample = {key: int(value) for key, value in window.items()}
    round, guard, rate = (sample[key] for key in ("round", "guard_us", "rate"))
    assert 0 <= round < ROUNDS and guard in GUARDS_US and rate in RATES
    assert sample["seconds"] == SECONDS
    assert sample["count"] == rate * SECONDS and len(raw) == sample["count"] * 24, "lost pacer arrivals"
    assert sample["elapsed_us"] >= SECONDS * 1_000_000
    assert 0 <= sample["cpu_us"] <= (sample["elapsed_us"] + 20_000) * 4 + 100_000
    generated, received = [], []
    previous_generation = previous_receipt = 0
    for expected, (arrival, generation, receipt) in enumerate(struct.iter_unpack(">QQQ", raw)):
        assert arrival == expected, "pacer reordered or substituted an arrival"
        scheduled = arrival * 1_000_000 // rate
        assert scheduled <= generation <= receipt <= sample["elapsed_us"], "invalid pacer timestamp"
        assert previous_generation <= generation and previous_receipt <= receipt, "pacer clock moved backward"
        previous_generation, previous_receipt = generation, receipt
        generated.append(generation - scheduled)
        received.append(receipt - scheduled)
    return dict(**sample, generator_delay=distribution(generated), handoff_delay=distribution(received),
                cpu_percent_of_four_cores=sample["cpu_us"] / sample["elapsed_us"] / 4 * 100,
                raw_sha256=hashlib.sha256(raw).hexdigest())


def verify_run(root: Path) -> dict:
    windows = rows(root / "windows.tsv")
    expected = [(round, GUARDS_US[(round + offset) % len(GUARDS_US)], rate)
                for round in range(ROUNDS) for rate in RATES for offset in range(len(GUARDS_US))]
    assert [(int(row["round"]), int(row["guard_us"]), int(row["rate"])) for row in windows] == expected, "missing or reordered calibration window"
    samples, groups = [], defaultdict(list)
    for row in windows:
        raw = root / f"p{row['round']}-r{row['rate']}-g{row['guard_us']}.bin"
        sample = verify_window(row, raw.read_bytes())
        samples.append(sample)
        groups[int(row["guard_us"]), int(row["rate"])].append(sample)
    summary = []
    for (guard, rate), selected in groups.items():
        summary.append(dict(guard_us=guard, rate=rate, rounds=len(selected),
                            median_generator_p99_ms=statistics.median(row["generator_delay"]["p99_ms"] for row in selected),
                            max_generator_p99_ms=max(row["generator_delay"]["p99_ms"] for row in selected),
                            median_handoff_p99_ms=statistics.median(row["handoff_delay"]["p99_ms"] for row in selected),
                            max_handoff_p99_ms=max(row["handoff_delay"]["p99_ms"] for row in selected),
                            median_cpu_percent_of_four_cores=statistics.median(row["cpu_percent_of_four_cores"] for row in selected),
                            every_window_within_5ms=all(row["generator_delay"]["p99_ms"] <= 5 and row["handoff_delay"]["p99_ms"] <= 5 for row in selected)))
    return dict(scope="generator-only Docker calibration; not application qualification", qualification_pass=False,
                arrivals_verified=sum(row["count"] for row in samples), windows=samples, summary=summary)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--docker-context", required=True)
    args = parser.parse_args()
    assert re.fullmatch(r"[a-z0-9][a-z0-9-]{0,55}", args.project)
    state = args.state.resolve(strict=True)
    binary = frozen(state)
    docker = ["docker", "--context", args.docker_context]

    def run(command, timeout=30):
        result = subprocess.run(command, text=True, capture_output=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError(result.stderr[-3000:])
        return result.stdout

    assert not run(docker + ["ps", "-aq", "--filter", f"name=^/{args.project}$"]).strip()
    root = state / "evidence/pacer"
    root.mkdir(mode=0o1777)
    root.chmod(0o1777)
    helper = root / "run-pacer.sh"
    helper.write_bytes(Path(__file__).with_name("run-pacer.sh").read_bytes())
    info = json.loads(run(docker + ["info", "--format", "{{json .}}"] ))
    assert info["NCPU"] >= 4 and info["MemTotal"] >= 4 * 2**30
    manifest = dict(schema_version=1, scope="generator-only Docker calibration", binary=binary,
                    docker_context=args.docker_context, docker_info=info, image=IMAGE, cpus=4, memory_bytes=4 * 2**30,
                    guards_us=GUARDS_US, rates=RATES, rounds=ROUNDS, seconds=SECONDS,
                    order="rotating guard order per round; rates fixed", handoff_capacity=1024,
                    generator_and_handoff_p99_limit_us=5000, default_guard_us=12_000,
                    runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    helper_sha256=hashlib.sha256(helper.read_bytes()).hexdigest(),
                    host_cpu_count=os.cpu_count(), host_load_before=os.getloadavg(), started_epoch=time.time())
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    command = docker + ["run", "--name", args.project, "--cpus", "4", "--memory", "4g", "--memory-swap", "4g", "--pids-limit", "256",
                        "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                        "--mount", f"type=bind,src={binary['binary']['path']},dst=/benchmark,readonly",
                        "--mount", f"type=bind,src={helper},dst=/run-pacer.sh,readonly",
                        "--mount", f"type=bind,src={root},dst=/evidence", "--mount", "type=volume,dst=/scratch",
                        "--env", "TMPDIR=/scratch", "--env", "CELLULE_PACER_BENCH_EVIDENCE=/evidence",
                        "--entrypoint", "/bin/sh", IMAGE, "/run-pacer.sh"]
    result = subprocess.run(command, text=True, capture_output=True, timeout=600)
    (root / "docker.log").write_text(result.stdout + result.stderr)
    observed, = json.loads(run(docker + ["inspect", args.project]))
    (root / "container.json").write_text(json.dumps(observed, indent=2) + "\n")
    assert result.returncode == observed["State"]["ExitCode"] == 0 and not observed["State"]["OOMKilled"], "failed pacer calibration retained"
    limits = observed["HostConfig"]
    assert limits["NanoCpus"] == 4_000_000_000 and limits["Memory"] == limits["MemorySwap"] == 4 * 2**30
    assert limits["ReadonlyRootfs"] and limits["PidsLimit"] == 256 and "ALL" in limits["CapDrop"]
    assert all(not mount["RW"] for mount in observed["Mounts"] if mount["Destination"] in ("/benchmark", "/run-pacer.sh"))
    assert "test result: ok. 1 passed; 0 failed;" in (root / "pacer.log").read_text()
    assert (root / "binary.sha256").read_text().split()[0] == binary["binary"]["sha256"]
    kernel = (root / "kernel-after.txt").read_text()
    assert "cpu.max\n400000 100000\n" in kernel and "memory.max\n4294967296\n" in kernel and "memory.swap.max\n0\n" in kernel
    assert re.search(r"^oom 0$", kernel, re.M) and re.search(r"^oom_kill 0$", kernel, re.M)
    report = dict(**verify_run(root), host_load_after=os.getloadavg(), memory_peak_bytes=int(re.search(r"memory.peak\n(\d+)", kernel)[1]))
    (root / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"Verified generator calibration; application qualification remains pending: {root}", flush=True)


if __name__ == "__main__":
    main()
