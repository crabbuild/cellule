"""Named uniform-write Docker diagnostics with a frozen binary and complete audit.

This is one repeat of uniform writes, not D0/D6 qualification or node capacity.
Keep the legacy one-CPU Fleet runner and its acceptance rules unchanged.
"""

import argparse
from dataclasses import dataclass, replace
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re

from entities import distribution, rows, verify_follower_proof, verify_timing_evidence, verify_trace_counts, control_transition_summary
from follower import frozen
from scale import Fleet

CELLS = 64
RATES = (30, 60, 120, 240, 480, 960, 1920, 3840, 7680, 15000, 30000)


@dataclass(frozen=True)
class WriteProfile:
    name: str
    cells: int
    payload_bytes: int
    warmup_rate: int
    rates: tuple[int, ...]
    node_cpus: int
    node_memory_gib: int
    tmpfs_bytes: int = 0
    idle_seconds: int = 0
    backend_nofile: int = 0


PRIMARY = WriteProfile("v1", CELLS, 1024, 30, RATES, 4, 8)
SMALL_KV = WriteProfile("small-kv-fleet-v1", 1000, 96, 30,
                        (30, 60, 120, 250, 500, 1000, 2000, 4000, 8000, 15000, 20000), 8, 16, 4 * 2**30)
ATTRIBUTION = WriteProfile("small-kv-attribution-v1", SMALL_KV.cells, SMALL_KV.payload_bytes,
                          SMALL_KV.warmup_rate, SMALL_KV.rates, SMALL_KV.node_cpus,
                          SMALL_KV.node_memory_gib, SMALL_KV.tmpfs_bytes, 60)
ATTRIBUTION_V2 = replace(ATTRIBUTION, name="small-kv-attribution-v2", backend_nofile=65_536)
PROFILES = {profile.name: profile for profile in (PRIMARY, SMALL_KV, ATTRIBUTION, ATTRIBUTION_V2)}


def verify_backend_limits(observed: dict, limits: str, kernel: dict, nofile: int) -> dict:
    config, state = observed["HostConfig"], observed["State"]
    assert config["NanoCpus"] == 2_000_000_000
    assert config["Memory"] == config["MemorySwap"] == 2 * 2**30
    assert state["Running"] and not state["OOMKilled"] and state["Health"]["Status"] == "healthy"
    assert config["Ulimits"] == [dict(Name="nofile", Soft=nofile, Hard=nofile)]
    assert re.search(rf"^Max open files[ \t]+{nofile}[ \t]+{nofile}[ \t]+files[ \t]*$", limits, re.M), "backend process file limit differs"
    assert kernel["cpu.max"].strip() == "200000 100000"
    assert int(kernel["memory.max"]) == 2 * 2**30 and int(kernel["memory.swap.max"]) == 0
    assert re.search(r"^oom 0$", kernel["memory.events"], re.M)
    assert re.search(r"^oom_kill 0$", kernel["memory.events"], re.M)
    current, peak = int(kernel["memory.current"]), int(kernel["memory.peak"])
    assert 0 < current <= peak <= 2 * 2**30
    descriptors = int(kernel["open_descriptors"])
    assert kernel["process_name"].strip() == "rustfs", "backend PID 1 is not the RustFS process"
    assert 0 < descriptors <= nofile
    return dict(nofile_soft=nofile, nofile_hard=nofile, open_descriptors=descriptors,
                memory_current_bytes=current, memory_peak_bytes=peak)


def verify_idle(control: Path, profile: WriteProfile) -> dict | None:
    if not profile.idle_seconds:
        assert not (control / "write-idle.tsv").exists(), "unexpected idle interval in ordinary profile"
        return None
    idle, = rows(control / "write-idle.tsv")
    assert int(idle["seconds"]) == profile.idle_seconds
    assert profile.idle_seconds * 1_000_000 <= int(idle["elapsed_us"]) <= (profile.idle_seconds + 2) * 1_000_000
    assert int(idle["ended_boot_ms"]) - int(idle["started_boot_ms"]) >= profile.idle_seconds * 1000
    start, end = int(idle["started_ms"]), int(idle["ended_ms"])
    assert start < end
    nodes = {}
    for node in range(3):
        prefix = f"node-{node}-"
        # Millisecond timestamps cannot assign an event at an interval edge.
        selected = lambda name: [row for row in rows(control / f"{prefix}{name}.tsv")
                                 if start < int(row["at_ms"]) < end]
        assert not selected("executions"), "application SQL ran during idle control"
        assert not selected("node-log-submissions"), "node-log submission ran during idle control"
        transitions = selected("control-transitions")
        assert any(row["transition"] == "Renew" for row in transitions), "idle control did not exercise Cell renewal"
        operations = selected("object-operations")
        samples = selected("resources")
        assert len(samples) >= 2
        assert all(int(row["active_cells"]) == len(range(node, profile.cells, 3)) for row in samples)
        nodes[node] = dict(control_transitions=control_transition_summary(transitions),
                           provider_operations={operation: dict(
                               count=len(values), duration=distribution([int(row["duration_us"]) for row in values]))
                               for operation in sorted({row["operation"] for row in operations})
                               for values in [[row for row in operations if row["operation"] == operation]]},
                           first_resource_sample=samples[0], last_resource_sample=samples[-1])
    return dict(interval=idle, nodes=nodes,
                scope="no new application SQL; late seed publication and provider maintenance remain visible")


def verify_attempt(row: dict, profile: WriteProfile = PRIMARY) -> None:
    request, entity = int(row["request"]), int(row["entity"])
    assert 0 <= entity < profile.cells and 0 <= request < 2**63
    assert row["command_id"] == request.to_bytes(8, "big").hex() + "00" * 8
    assert 0 <= int(row["key"]) < 100_000
    assert re.fullmatch(r"[a-f0-9]{64}", row["digest"])
    assert int(row["payload_bytes"]) == profile.payload_bytes
    assert 0 <= int(row["scheduled_us"]) <= int(row["generated_us"]) <= int(row["dispatched_us"]) <= int(row["terminal_us"])
    outcome, resolution, sequence = row["outcome"], row["resolution"], int(row["sequence"])
    assert outcome in ("ok", "unknown", "not_started", "client_full", "rejected")
    if outcome == "unknown":
        assert resolution in ("committed", "absent", "rejected")
        assert (sequence > 0) == (resolution == "committed")
    else:
        assert resolution == "none" and (sequence > 0) == (outcome == "ok")


def verify_window(window: dict, attempts: list[dict], profile: WriteProfile = PRIMARY) -> dict:
    id, rate = int(window["window"]), int(window["rate"])
    assert window["phase"] in ("warmup", "measure")
    assert int(window["seconds"]) == 60 and int(window["concurrency"]) == 1024
    assert int(window["started_ms"]) < int(window["ended_ms"])
    assert int(window["started_boot_ms"]) < int(window["ended_boot_ms"])
    assert len(attempts) == rate * 60, "missing intended primary arrivals"
    for arrival, row in enumerate(attempts):
        verify_attempt(row, profile)
        assert int(row["request"]) == (id << 32) | arrival
        assert int(row["entity"]) == arrival % profile.cells
        assert int(row["scheduled_us"]) == arrival * 1_000_000 // rate
    successful = [row for row in attempts if row["outcome"] == "ok"]
    latencies = [int(row["terminal_us"]) - int(row["scheduled_us"]) for row in attempts]
    generator = [int(row["generated_us"]) - int(row["scheduled_us"]) for row in attempts]
    p99 = lambda values: sorted(values)[(len(values) * 99 + 99) // 100 - 1]
    served = len(successful) == len(attempts) and p99(latencies) <= 50_000 and p99(generator) <= 5_000 and int(window["elapsed_us"]) <= 62_000_000
    assert window["fully_served"] == str(served).lower(), "false primary served claim"
    acknowledged = sum(int(row["terminal_us"]) <= 60_000_000 for row in successful)
    per_owner = {}
    for node in range(3):
        owner_attempts = [row for row in attempts if int(row["entity"]) % 3 == node]
        owner_successes = [row for row in owner_attempts if row["outcome"] == "ok"]
        per_owner[node] = dict(planned=len(owner_attempts),
                               outcomes=dict(Counter(row["outcome"] for row in owner_attempts)),
                               acknowledgement_tps=sum(int(row["terminal_us"]) <= 60_000_000 for row in owner_successes) / 60,
                               scheduled_latency=distribution([int(row["terminal_us"]) - int(row["scheduled_us"]) for row in owner_successes]))
    return dict(window=id, phase=window["phase"], rate=rate, planned=len(attempts),
                outcomes=dict(Counter(row["outcome"] for row in attempts)), fully_served=served,
                acknowledgement_tps=acknowledged / 60,
                per_owner=per_owner,
                arrival_completion_tps=len(successful) / 60,
                logical_mib_per_second=acknowledged * profile.payload_bytes / 60 / 2**20,
                scheduled_latency=distribution([int(row["terminal_us"]) - int(row["scheduled_us"]) for row in successful]),
                all_terminal_latency=distribution(latencies), generator_delay=distribution(generator),
                service_latency=distribution([int(row["terminal_us"]) - int(row["dispatched_us"]) for row in successful]),
                nodes=3, started_ms=int(window["started_ms"]), ended_ms=int(window["ended_ms"]), elapsed_us=int(window["elapsed_us"]))


def verify_audit(attempts: list[dict], audit: list[dict], values: list[dict], owners: list[dict], roots: list[dict], profile: WriteProfile = PRIMARY) -> None:
    assert [int(row["entity"]) for row in owners] == list(range(profile.cells))
    assert len({row["cell"] for row in owners}) == profile.cells
    assert len({row["incarnation"] for row in owners}) == profile.cells
    assert all(int(row["owner"]) == int(row["entity"]) % 3 for row in owners)
    assert len({row["request"] for row in attempts}) == len(attempts)
    expected = {int(row["request"]): row for row in attempts if int(row["sequence"]) > 0}
    assert len(audit) == len(expected), "missing or duplicate command audit"
    assert len({row["request"] for row in audit}) == len(audit), "duplicate audit ID"
    assert len({(row["entity"], row["sequence"]) for row in audit}) == len(audit), "duplicate commit sequence"
    latest = {}
    required = [0] * profile.cells
    for record in audit:
        attempt = expected[int(record["request"])]
        assert all(record[field] == attempt[field] for field in ("entity", "command_id", "key", "digest", "sequence")), "audit differs from durable attempt"
        entity, sequence = int(record["entity"]), int(record["sequence"])
        required[entity] = max(required[entity], sequence)
        key = (record["entity"], record["key"])
        if key not in latest or sequence > int(latest[key]["sequence"]):
            latest[key] = record
    assert len(values) == len(latest), "missing final key"
    assert len({(row["entity"], row["key"]) for row in values}) == len(values)
    for row in values:
        record = latest[row["entity"], row["key"]]
        assert row["digest"] == record["digest"] and row["sequence"] == record["sequence"], "final overwritten value differs"
        assert row["payload_bytes"] == str(profile.payload_bytes)
    assert [int(row["entity"]) for row in roots] == list(range(profile.cells))
    for owner, root in zip(owners, roots, strict=True):
        entity = int(owner["entity"])
        assert all(root[field] == owner[field] for field in ("entity", "cell", "owner", "epoch", "incarnation")), "root scope changed"
        assert int(root["required_sequence"]) == required[entity] > 0
        assert int(root["root_sequence"]) >= required[entity], "root does not cover durable suffix"
        assert re.fullmatch(r"Digest\([a-f0-9]{64}\)", root["root_digest"])


def verify_primary(control: Path, profile: WriteProfile = PRIMARY) -> dict:
    seeds = rows(control / "write-seed.tsv")
    assert len(seeds) == profile.cells
    for entity, row in enumerate(seeds):
        verify_attempt(row, profile)
        assert int(row["request"]) == int(row["entity"]) == entity and row["outcome"] == "ok"
    windows = rows(control / "write-windows.tsv")
    assert len(windows) >= 2
    assert [int(row["window"]) for row in windows] == list(range(1, len(windows) + 1))
    assert windows[0]["phase"] == "warmup" and int(windows[0]["rate"]) == profile.warmup_rate
    assert all(row["phase"] == "measure" and int(row["rate"]) == profile.rates[index] for index, row in enumerate(windows[1:]))
    all_attempts = list(seeds)
    reports = []
    for window in windows:
        attempts = rows(control / f"write-{window['window']}.tsv")
        all_attempts.extend(attempts)
        reports.append(verify_window(window, attempts, profile))
    assert all(row["fully_served"] for row in reports[1:-1]), "ramp continued after failure"
    assert not reports[-1]["fully_served"] or len(windows) == len(profile.rates) + 1
    verify_audit(all_attempts, rows(control / "write-audit.tsv"), rows(control / "write-values.tsv"), rows(control / "write-owners.tsv"), rows(control / "write-roots.tsv"), profile)
    resources = {}
    for node in range(3):
        samples = rows(control / f"node-{node}-resources.tsv")
        assert len(samples) > 1 and all(int(row["active_cells"]) == len(range(node, profile.cells, 3)) for row in samples[:-1])
        assert int(samples[-1]["active_cells"]) == 0, "active Cells survived shutdown"
        assert int(samples[-1]["unpublished_node_log_bytes"]) == 0, "undrained follower publication debt"
        resources[node] = dict(durability=verify_timing_evidence(control, node, reports), traces=verify_trace_counts(control, node))
    proof = verify_follower_proof(resources)
    idle = verify_idle(control, profile)
    hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(control.glob("*.tsv"))}
    return dict(scope="one-repeat uniform-write Docker diagnostic; not D0/D6 or node qualification",
                correctness_verified=True, qualification_pass=False, profile=profile.name, cells=profile.cells, payload_bytes=profile.payload_bytes, windows=reports,
                resources=resources, follower_proof=proof, idle_control=idle, raw_sha256=hashes)


class WriteFleet(Fleet):
    def __init__(self, state: Path, project: str, overrides: list[Path], profile: WriteProfile = PRIMARY):
        self.profile = profile
        self.binary = frozen(state)
        super().__init__(state, project, overrides, "capacity-follower")
        config = json.loads(self.compose_file.read_text())
        for name, service in config["services"].items():
            if name.startswith("node-") or name == "driver":
                service["command"] = ["driver" if name == "driver" else "node"]
                service["entrypoint"] = ["/bin/sh", "/source/crates/cellule-app/qualification/run-write.sh"]
                service["cpus"] = 4.0 if name == "driver" else float(profile.node_cpus)
                memory = (4 if name == "driver" else profile.node_memory_gib) * 2**30
                service["mem_limit"] = service["memswap_limit"] = memory
                service["environment"].update(CELLULE_WRITE_PROFILE=profile.name, CELLULE_PERF_PROCESS_ROOT=f"primary-{project}")
                service["volumes"].append(dict(type="bind", source=self.binary["binary"]["path"], target="/benchmark", read_only=True))
                if profile.tmpfs_bytes and name != "driver":
                    service["volumes"] = [mount for mount in service["volumes"] if mount["target"] != "/scratch"]
                    service["volumes"].append(dict(type="tmpfs", target="/scratch",
                                                  tmpfs=dict(size=profile.tmpfs_bytes, mode=0o1777)))
                    service["ulimits"] = dict(nofile=dict(soft=65_536, hard=65_536))
        config["services"]["rustfs"].update(cpus=2.0, mem_limit=2 * 2**30, memswap_limit=2 * 2**30)
        if profile.backend_nofile:
            config["services"]["rustfs"]["ulimits"] = dict(nofile=dict(soft=profile.backend_nofile, hard=profile.backend_nofile))
        self.compose_file.write_text(json.dumps(config, indent=2) + "\n")
        # Freeze before any container starts. Container budgets are distinct
        # from actual VM/host capacity; oversubscription remains visible.
        info = json.loads(self.run("docker", "info", "--format", "{{json .}}"))
        assert info["NCPU"] >= 8 and info["MemTotal"] >= 15 * 2**30, "primary diagnostic needs the resized VM"
        manifest = dict(schema_version=1, scope="Docker simulation diagnostic", binary=self.binary,
                        compose=config, compose_sha256=hashlib.sha256(self.compose_file.read_bytes()).hexdigest(),
                        runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), docker_info=info,
                        host_platform=self.run("uname", "-a").strip(), host_cpu_count=os.cpu_count(),
                        profile=profile.name, cells=profile.cells, payload_bytes=profile.payload_bytes, keys_per_cell=100_000, seed=7, selected_followers=2,
                        local_storage="tmpfs" if profile.tmpfs_bytes else "private persistent Docker volume",
                        tmpfs_bytes_per_node=profile.tmpfs_bytes,
                        idle_control_seconds=profile.idle_seconds,
                        backend_nofile=profile.backend_nofile or None,
                        durability_scope="process failure only" if profile.tmpfs_bytes else "persistent storage; machine failure tests pending",
                        population="one seeded key per Cell; bounded keyspace populated by timed writes",
                        client_transport=dict(adapter="signed TCP test fixture", connection_per_call=True,
                                              gateway_forwarding="fixture balancer and owner gateway"),
                        follower_transport=dict(adapter="pooled signed TCP test fixture", mtls=False,
                                                connections_per_member=1, receiver_authority_loads_per_append=2,
                                                ordered_shipping_window=1, idle_limit_seconds=5),
                        seconds=60, warmup_seconds=60, rates=profile.rates, warmup_rate=profile.warmup_rate, concurrency=1024,
                        scheduled_p99_limit_us=50_000, generator_p99_limit_us=5_000,
                        generator_spin_guard_us=12_000,
                        drain_grace_us=2_000_000, environment_keys=sorted(key for key in self.env if key.startswith("DOCKER_")))
        (self.evidence / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    def backend_snapshot(self, phase: str) -> dict:
        container = self.compose("ps", "-q", "rustfs").strip()
        assert container
        observed = self.inspect(container)
        limits = self.run("docker", "exec", container, "cat", "/proc/1/limits")
        kernel = {counter: self.run("docker", "exec", container, "cat", f"/sys/fs/cgroup/{counter}")
                  for counter in ("cpu.max", "cpu.stat", "memory.max", "memory.swap.max", "memory.current", "memory.peak", "memory.events")}
        kernel["open_descriptors"] = str(len(self.run("docker", "exec", container, "ls", "-1", "/proc/1/fd").splitlines()))
        kernel["process_name"] = self.run("docker", "exec", container, "cat", "/proc/1/comm")
        snapshot = dict(container=observed, process_limits=limits, kernel=kernel)
        path = self.evidence / f"backend-{phase}.json"
        path.write_text(json.dumps(snapshot, indent=2) + "\n")
        report = verify_backend_limits(observed, limits, kernel, self.profile.backend_nofile)
        snapshot["report"] = report
        path.write_text(json.dumps(snapshot, indent=2) + "\n")
        return report

    def start_backend(self) -> None:
        self.compose("up", "-d", "--wait", "rustfs")
        self.backend_snapshot("before")

    def limits(self, container: str) -> dict:
        observed = self.inspect(container)
        config, state = observed["HostConfig"], observed["State"]
        memory = (4 if observed["Name"] == f"/{self.project}-driver" else self.profile.node_memory_gib) * 2**30
        cpus = 4 if observed["Name"] == f"/{self.project}-driver" else self.profile.node_cpus
        assert config["NanoCpus"] == cpus * 1_000_000_000
        assert config["Memory"] == config["MemorySwap"] == memory
        assert config["PidsLimit"] == 256 and config["ReadonlyRootfs"] and "ALL" in config["CapDrop"]
        assert not state["OOMKilled"]
        assert all(not mount["RW"] for mount in observed["Mounts"] if mount["Destination"] in ("/source", "/target", "/benchmark"))
        return observed

    def verify(self) -> None:
        assert set(self.active) == {0, 1, 2} and not self.killed
        assert [(event["action"], event["argument"]) for event in self.events] == [("scale", 3)]
        reports, volumes = {}, set()
        for role, container in {**{f"node-{node}": container for node, container in self.active.items()}, "driver": self.driver}.items():
            assert self.run("docker", "wait", container, timeout=60).strip() == "0"
            observed = self.limits(container)
            volume, = [mount for mount in observed["Mounts"] if mount["Destination"] == "/scratch"]
            if self.profile.tmpfs_bytes and role != "driver":
                assert volume["Type"] == "tmpfs"
                assert "scratch_filesystem\ntmpfs\n" in (self.evidence / f"{role}-kernel-before.txt").read_text()
            else:
                assert volume["Type"] == "volume" and volume["Name"] not in volumes
                volumes.add(volume["Name"])
            log = (self.evidence / f"{role}.log").read_text()
            assert "test result: ok. 1 passed; 0 failed;" in log
            if role != "driver":
                assert re.search(rf"node_{role.split('-')[1]}_session_withdrawn: generation=\d+", log)
            kernel = (self.evidence / f"{role}-kernel-after.txt").read_text()
            memory = (4 if role == "driver" else self.profile.node_memory_gib) * 2**30
            cpus = 4 if role == "driver" else self.profile.node_cpus
            assert f"cpu.max\n{cpus * 100000} 100000\n" in kernel and f"memory.max\n{memory}\n" in kernel
            assert "memory.swap.max\n0\n" in kernel and re.search(r"^oom 0$", kernel, re.M) and re.search(r"^oom_kill 0$", kernel, re.M)
            assert (self.evidence / f"{role}-binary.sha256").read_text().split()[0] == self.binary["binary"]["sha256"]
            reports[role] = dict(memory_peak_bytes=int(re.search(r"memory.peak\n(\d+)", kernel)[1]), scratch=volume)
        backend = None
        if self.profile.backend_nofile:
            backend = self.backend_snapshot("after")
            container = self.compose("ps", "-q", "rustfs").strip()
            log = self.run("docker", "exec", container, "cat", "/data/logs/rustfs.log")
            assert "Too many open files" not in log and "TooManyOpenFiles" not in log, "backend exhausted file descriptors"
        result = dict(binary=self.binary, roles=reports, backend=backend, events=self.events, **verify_primary(self.control, self.profile))
        (self.evidence / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
        print(f"Verified primary diagnostic integrity; qualification remains pending: {self.evidence}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--profile", choices=PROFILES, default=PRIMARY.name)
    parser.add_argument("--compose-file", type=Path, action="append", default=[])
    args = parser.parse_args()
    fleet = WriteFleet(args.state, args.project, args.compose_file, PROFILES[args.profile])
    try:
        if fleet.profile.backend_nofile:
            fleet.start_backend()
        fleet.execute()
    finally:
        fleet.retain()


if __name__ == "__main__":
    main()
