"""Measure one resident owner at 64/256/1,000/2,000 Cells in Docker.

Admission, first writes/reads, idle management, and shutdown are exercised.
This diagnostic does not measure open-loop TPS or qualify the simultaneous goal.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

from entities import rows, verify_timing_evidence, verify_trace_counts
from follower import frozen

POPULATIONS = (0, 64, 256, 1000, 2000)


def verify_population(samples: list[dict], cells: list[dict], drained: list[dict]) -> list[dict]:
    assert [int(row["cells"]) for row in samples] == list(POPULATIONS), "missing population stage"
    assert [int(row["entity"]) for row in cells] == list(range(2000)), "missing resident Cell"
    assert len({row["cell"] for row in cells}) == 2000, "duplicate Cell identity"
    assert len({row["incarnation"] for row in cells}) == 2000, "duplicate incarnation"
    for row in cells:
        assert int(row["root_sequence"]) >= int(row["sequence"]) > 0, "uncovered first command"
        assert re.fullmatch(r"Digest\([a-f0-9]{64}\)", row["root_digest"])
    assert len(drained) == 1
    assert drained[0]["active_cells"] == drained[0]["sqlite_descriptors"] == "0", "undrained owner"
    assert int(drained[0]["descriptors"]) > 0
    reports = []
    baseline_rss = int(samples[0]["process_rss_bytes"])
    for row in samples:
        sample = {key: int(value) for key, value in row.items()}
        assert all(value >= 0 for value in sample.values())
        assert 0 < sample["memory_current_bytes"] <= sample["memory_peak_bytes"] <= 16 * 2**30
        assert 0 < sample["process_rss_bytes"]
        assert 0 <= sample["sqlite_descriptors"] <= sample["descriptors"]
        population = sample["cells"]
        if population:
            assert sample["idle_us"] >= 15_000_000 and sample["renewals"] > 0, "idle interval omitted lease renewal"
            assert sample["idle_cpu_us"] <= sample["idle_us"] * 8 + 100_000
            sample["idle_percent_of_eight_cores"] = sample["idle_cpu_us"] / sample["idle_us"] / 8 * 100
            sample["rss_delta_bytes_per_cell"] = (sample["process_rss_bytes"] - baseline_rss) / population
        else:
            assert sample["activation_us"] == sample["idle_us"] == sample["idle_cpu_us"] == sample["renewals"] == 0
        reports.append(sample)
    return reports


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--docker-context", required=True)
    parser.add_argument("--compose-file", type=Path, action="append", default=[])
    args = parser.parse_args()
    execute(args)


def execute(args, read_mode=False, read_verifier=None) -> None:
    assert not read_mode or read_verifier is not None
    assert re.fullmatch(r"[a-z0-9][a-z0-9-]{0,55}", args.project)
    state = args.state.resolve(strict=True)
    binary = frozen(state)
    docker = ["docker", "--context", args.docker_context]
    environment = dict(os.environ, CELLULE_REFERENCE_STATE=str(state))

    def run(command, timeout=30):
        result = subprocess.run(command, env=environment, text=True, capture_output=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError(result.stderr[-3000:])
        return result.stdout

    assert not run(docker + ["ps", "-aq", "--filter", f"label=com.docker.compose.project={args.project}"]).strip(), "choose a fresh retained project"
    root = state / ("evidence/owner-reads" if read_mode else "evidence/population")
    root.mkdir(mode=0o1777)
    root.chmod(0o1777)
    (root / "control").mkdir(mode=0o1777)
    (root / "control").chmod(0o1777)
    compose = docker + ["compose", "-p", args.project, "-f", str(state / "source/crates/cellule-app/qualification/compose.yaml")]
    for path in args.compose_file:
        compose += ["-f", str(path.resolve(strict=True))]
    config = json.loads(run(compose + ["config", "--format", "json"]))
    owner = config["services"]["node-0"]
    owner.update(cpus=8.0, mem_limit=16 * 2**30, memswap_limit=16 * 2**30,
                 entrypoint=["/bin/sh", "/source/crates/cellule-app/qualification/run-population.sh"], command=[])
    owner["ulimits"] = {"nofile": {"soft": 65536, "hard": 65536}}
    owner["environment"].update(CELLULE_WRITE_PROFILE="owner-reads-v1" if read_mode else "population-v1", CELLULE_PERF_PROCESS_ROOT=f"population-{args.project}")
    for volume in owner["volumes"]:
        if volume["target"] == "/evidence":
            volume["source"] = str(root)
    owner["volumes"].append(dict(type="bind", source=binary["binary"]["path"], target="/benchmark", read_only=True))
    config["services"] = {"owner": owner, **{name: config["services"][name] for name in ("rustfs", "bucket-init")}}
    config["services"]["rustfs"].update(cpus=2.0, mem_limit=2 * 2**30, memswap_limit=2 * 2**30)
    compose_file = root / "compose.json"
    compose_file.write_text(json.dumps(config, indent=2) + "\n")
    compose = docker + ["compose", "-p", args.project, "-f", str(compose_file)]
    info = json.loads(run(docker + ["info", "--format", "{{json .}}" ]))
    assert info["NCPU"] >= 8 and info["MemTotal"] >= 15 * 2**30
    manifest = dict(schema_version=1, scope="sparse direct owner-read Docker diagnostic; not mixed-load qualification" if read_mode else "resident-owner population Docker diagnostic; not traffic qualification",
                    binary=binary, compose=config, docker_context=args.docker_context, docker_info=info,
                    runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    helper_sha256=hashlib.sha256((state / "source/crates/cellule-app/qualification/run-population.sh").read_bytes()).hexdigest(),
                    host_cpu_count=os.cpu_count(), host_platform=run(["uname", "-a"]).strip(), host_load=run(["uptime"]).strip(),
                    populations=POPULATIONS, idle_seconds=15, workers=8, max_active_cells=2000,
                    node_resident_budget_bytes=12 * 2**30, sqlite_budget_bytes=8 * 2**30,
                    owner_cpus=8, owner_memory_bytes=16 * 2**30, service_cpus=2, service_memory_bytes=2 * 2**30,
                    owner_nofile_soft=65536, owner_nofile_hard=65536,
                    environment_keys=sorted(key for key in environment if key.startswith("DOCKER_")))
    if read_mode:
        manifest.update(read_rates=[5000, 10_000, 25_000, 50_000], warmup_rate=5000,
                        seconds=60, handoff_capacity=1024, concurrency=1024, cells=2000,
                        payload_bytes=1024, populated_keys_per_cell=1,
                        ingress="public local SQL author API; generator included in owner budget",
                        consistency="owner read at seed minimum receipt; no timed mutations",
                        scheduled_p99_limit_us=50_000, generator_p99_limit_us=5000,
                        generator_spin_guard_us=12_000,
                        provider_trace_flush_seconds=1,
                        evidence_export=dict(owner="dedicated owner-process thread", threads=1,
                                             queue_capacity=1, stack_bytes=512 * 1024,
                                             trace_capacity=65536, queue_full_invalidates=True,
                                             final_drain_acknowledged=True),
                        read_verifier_sha256=hashlib.sha256(Path(read_verifier.__code__.co_filename).read_bytes()).hexdigest())
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    container = ""
    try:
        run(compose + ["up", "-d", "owner"], timeout=180)
        container = run(compose + ["ps", "-aq", "owner"]).strip()
        assert container
        deadline = time.monotonic() + 20 * 60
        while True:
            observed, = json.loads(run(docker + ["inspect", container]))
            if not observed["State"]["Running"]:
                assert observed["State"]["ExitCode"] == 0 and not observed["State"]["OOMKilled"], "population diagnostic failed; raw evidence retained"
                break
            assert time.monotonic() < deadline, "population diagnostic exceeded 20 minutes"
            time.sleep(2)
        limits = observed["HostConfig"]
        assert limits["NanoCpus"] == 8_000_000_000 and limits["Memory"] == limits["MemorySwap"] == 16 * 2**30
        assert limits["PidsLimit"] == 256 and limits["ReadonlyRootfs"] and "ALL" in limits["CapDrop"]
        assert {entry["Name"]: (entry["Soft"], entry["Hard"]) for entry in limits["Ulimits"]}["nofile"] == (65536, 65536)
        volume, = [mount for mount in observed["Mounts"] if mount["Destination"] == "/scratch"]
        assert volume["Type"] == "volume"
        assert all(not mount["RW"] for mount in observed["Mounts"] if mount["Destination"] in ("/source", "/target", "/benchmark"))
        kernel = (root / "owner-kernel-after.txt").read_text()
        assert "cpu.max\n800000 100000\n" in kernel and "memory.max\n17179869184\n" in kernel and "memory.swap.max\n0\n" in kernel
        assert re.search(r"^oom 0$", kernel, re.M) and re.search(r"^oom_kill 0$", kernel, re.M)
        log = (root / "owner.log").read_text()
        assert "test result: ok. 1 passed; 0 failed;" in log
        assert re.search(r"node_0_session_withdrawn: generation=\d+", log)
        assert (root / "owner-binary.sha256").read_text().split()[0] == binary["binary"]["sha256"]
        control = root / "control"
        reports = verify_population(rows(control / "population.tsv"), rows(control / "population-cells.tsv"), rows(control / "population-drained.tsv"))
        resources = rows(control / "node-0-resources.tsv")
        assert resources and int(resources[-1]["active_cells"]) == int(resources[-1]["unpublished_node_log_bytes"]) == 0
        result = dict(scope=manifest["scope"], correctness_verified=True, qualification_pass=False,
                      populations=reports, scratch_volume=volume["Name"],
                      traces=verify_trace_counts(control, 0), timings=verify_timing_evidence(control, 0, []),
                      raw_sha256={path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(control.glob("*.tsv"))})
        if read_mode:
            result["reads"] = read_verifier(control)
        (root / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
        print(f"Verified population diagnostic; traffic qualification remains pending: {root}", flush=True)
    finally:
        (root / "compose.log").write_text(run(compose + ["logs", "--no-color"]))
        containers = run(compose + ["ps", "-aq"]).split()
        if containers:
            (root / "containers.json").write_text(run(docker + ["inspect", *containers]))
        (root / "host-load-after.txt").write_text(run(["uptime"]))
        run(compose + ["stop"], timeout=120)


if __name__ == "__main__":
    main()
