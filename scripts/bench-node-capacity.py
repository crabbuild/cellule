#!/usr/bin/env python3
"""Run offered HTTP load and audit every acknowledged write after fresh cold restore.

Uses the same SQL service lifecycle/receipt checks as bench-axum-rustfs.py.
Raw evidence stays outside the source checkout. This is a development probe;
hardware isolation, follower durability and owner-loss qualification remain gates.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import threading
import time

spec = importlib.util.spec_from_file_location("axum_bench", Path(__file__).with_name("bench-axum-rustfs.py"))
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)
fleet_spec = importlib.util.spec_from_file_location("fleet_bench", Path(__file__).with_name("bench-fleet-fixture.py"))
fleet = importlib.util.module_from_spec(fleet_spec)
fleet_spec.loader.exec_module(fleet)


def records(directory):
    for path in [directory / "setup.jsonl", *sorted(directory.glob("client-*.jsonl"))]:
        with path.open() as source:
            for line in source:
                yield json.loads(line)


def audit(cold, directory, cells):
    local = threading.local()
    connections = []
    lock = threading.Lock()

    def verify(record):
        bench.require(record["error"] is None, "driver retained an uncertain or invalid outcome")
        if not hasattr(local, "connection"):
            local.connection = http.client.HTTPConnection(cold.address, timeout=60)
            with lock:
                connections.append(local.connection)
        connection = local.connection
        original = record["response"]
        if record["request"] is None:
            reply = bench.request(connection, "GET", f'/orders/{record["read_id"]}')
            envelope = original["output"]
        else:
            envelope = record["request"]
            reply = bench.request(connection, "POST", "/orders", envelope)
            bench.verify_order(reply, envelope, original["receipt"], 201)
            bench.require(reply["body"]["receipt"] == original["receipt"], "cold identity replay changed its receipt")
            reply = bench.request(connection, "GET", f'/orders/{envelope["id"]}')
        bench.verify_order(reply, envelope, original["receipt"])
        return record["request"] is not None

    counts = {"writes": 0, "reads": 0}
    try:
        with ThreadPoolExecutor(max_workers=32) as pool:
            batch = []
            for record in records(directory):
                batch.append(record)
                if len(batch) == 1024:
                    for write in pool.map(verify, batch):
                        counts["writes" if write else "reads"] += 1
                    batch.clear()
            for write in pool.map(verify, batch):
                counts["writes" if write else "reads"] += 1
        bench.require(counts["writes"] >= cells, "audit missed seed writes")
        return counts
    finally:
        for connection in connections:
            connection.close()


def point(args, cells, rate, directory):
    directory.mkdir()
    prefix = f'{os.environ["CELLULE_TEST_PREFIX"]}/cells-{cells}-rate-{rate}'
    service = bench.Service(args.binary, directory, prefix, "initial", cells, args.workers, args.startup_timeout)
    startup_ms = service.startup_ms
    activation_ms = service.activation_ms
    sampling_stop = threading.Event()
    sampling_errors = []
    sample_started = time.perf_counter()
    resource_peaks = dict(rss_bytes=0, file_descriptors=0)

    def sample_resources():
        try:
            with (directory / "owner-resources.jsonl").open("x") as output:
                while not sampling_stop.is_set():
                    usage = bench.process_usage(service.process.pid)
                    proc = Path(f"/proc/{service.process.pid}")
                    usage["file_descriptors"] = len(list((proc / "fd").iterdir()))
                    status = (proc / "status").read_text().splitlines()
                    usage["peak_rss_bytes"] = next(int(line.split()[1])*1024 for line in status if line.startswith("VmHWM:"))
                    resource_peaks["rss_bytes"] = max(resource_peaks["rss_bytes"], usage["peak_rss_bytes"])
                    resource_peaks["file_descriptors"] = max(resource_peaks["file_descriptors"], usage["file_descriptors"])
                    usage["seconds"] = time.perf_counter() - sample_started
                    for name in ("memory.current", "memory.peak", "memory.events", "cpu.stat"):
                        path = Path("/sys/fs/cgroup") / name
                        if path.exists():
                            usage[f"shared_container_{name}"] = path.read_text().strip()
                    output.write(json.dumps(usage) + "\n")
                    output.flush()
                    sampling_stop.wait(1)
        except Exception as error:
            sampling_errors.append(str(error))

    sampler = threading.Thread(target=sample_resources, daemon=True)
    sampler.start()
    try:
        bench.verify_active_owner_refused(args.binary, directory, prefix, cells, args.workers)
        host, port = service.address.rsplit(":", 1)
        config = dict(address=f"{host}:{port}", cells=cells, concurrency=args.concurrency,
                      queue_capacity=args.queue_capacity, write_rate=rate, read_rate=args.read_rate,
                      warmup_seconds=args.warmup_seconds, seconds=args.seconds,
                      evidence_directory=str(directory / "load"))
        config_path = directory / "load-config.json"
        config_path.write_text(json.dumps(config, indent=2) + "\n")
        before = bench.process_usage(service.process.pid)
        started = time.perf_counter()
        with (directory / "load.log").open("w") as log:
            load = subprocess.run([str(args.driver), str(config_path)], stdout=log, stderr=subprocess.STDOUT,
                                  timeout=args.seconds + args.warmup_seconds + 1200)
        after = bench.process_usage(service.process.pid)
        elapsed = time.perf_counter() - started
        sampling_stop.set()
        sampler.join(timeout=5)
        bench.require(not sampler.is_alive() and not sampling_errors, f"resource sampling failed: {sampling_errors}")
        bench.require(load.returncode == 0, f"driver failed; inspect {directory / 'load.log'}")
        summary = json.loads((directory / "load" / "summary.json").read_text())
        intervals = [dict(writes=0, reads=0) for _ in range((args.seconds + 9)//10)]
        for record in records(directory / "load"):
            if record["measured"] and record["error"] is None:
                completed = record["completed_seconds"] - args.warmup_seconds
                if 0 <= completed < args.seconds:
                    intervals[int(completed)//10]["writes" if record["request"] is not None else "reads"] += 1
        for index, interval in enumerate(intervals):
            interval["start_seconds"] = index*10
            interval["seconds"] = min(10, args.seconds-index*10)
            interval["write_tps"] = interval["writes"]/interval["seconds"]
            interval["read_tps"] = interval["reads"]/interval["seconds"]
        drained = service.stop()
        service = None
        if args.fleet_directory is not None:
            fleet.verify_durability(drained["query_metrics"], bench)
        cold = bench.Service(args.binary, directory, prefix, "recovered", cells, args.workers, args.startup_timeout)
        service = cold
        bench.require(cold.restored == cells, "not every Cell cold-restored")
        verified = audit(cold, directory / "load", cells)
        connection = http.client.HTTPConnection(cold.address, timeout=60)
        try:
            first_id = cells + rate * (args.seconds + args.warmup_seconds) + cells
            for shard in range(cells):
                envelope = bench.identity(first_id + shard)
                reply = bench.request(connection, "POST", "/orders", envelope)
                bench.verify_order(reply, envelope, status=201)
        finally:
            connection.close()
        recovered_drained = cold.stop()
        service = None
        original = {item["shard"]: item for item in drained["cells"]}
        for item in recovered_drained["cells"]:
            bench.require(item["epoch"] > original[item["shard"]]["epoch"], "recovery did not advance fencing epoch")
            bench.require(item["commit_sequence"] == original[item["shard"]]["commit_sequence"] + 1,
                          "replay mutated state or next write lost its sequence")
        hashes = {}
        for path in sorted((directory / "load").glob("*.jsonl")):
            with path.open("rb") as source:
                hashes[path.name] = hashlib.file_digest(source, "sha256").hexdigest()
        result = dict(cells=cells, write_rate=rate, metrics=summary,
                      startup_ms=startup_ms,
                      activation_ms=activation_ms,
                      completion_intervals=intervals,
                      owner_resource_window_seconds=elapsed,
                      owner_cpu_cores_including_setup_warmup_drain=(after["cpu_seconds"]-before["cpu_seconds"])/elapsed,
                      owner_rss_before_bytes=before["rss_bytes"], owner_rss_after_bytes=after["rss_bytes"],
                      owner_peak_rss_bytes=resource_peaks["rss_bytes"],
                      owner_peak_file_descriptors=resource_peaks["file_descriptors"],
                      cold_audit=verified, raw_journal_sha256=hashes,
                      initial_drain=drained, recovered_drain=recovered_drained,
                      target_established=False)
        (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        return result
    finally:
        sampling_stop.set()
        sampler.join(timeout=5)
        if service is not None:
            service.abort()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cells", type=int, nargs="+", default=[16, 64, 256, 2000])
    parser.add_argument("--write-rates", type=int, nargs="+", default=[1000, 3000, 10000])
    parser.add_argument("--read-rate", type=int, default=50)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--concurrency", type=int, default=256)
    parser.add_argument("--queue-capacity", type=int, default=4096)
    parser.add_argument("--warmup-seconds", type=int, default=5)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--startup-timeout", type=int, default=120, help="bootstrap/restore deadline; existing profiles keep 120 seconds")
    parser.add_argument("--fleet-directory", type=Path, help="three Ed25519 mTLS identities and persistent private follower directories")
    args = parser.parse_args()
    bench.require(os.sys.platform.startswith("linux"), "node capacity resource evidence requires Linux; run in an isolated container/host")
    bench.require(all(1 <= cells <= 2000 for cells in args.cells), "Cell counts must be 1..2000")
    bench.require(all(0 <= rate <= 100000 for rate in args.write_rates), "write rates must be 0..100000")
    bench.require(len(set(args.cells)) == len(args.cells) and len(set(args.write_rates)) == len(args.write_rates), "duplicate points")
    bench.require(0 <= args.read_rate <= 100000 and 1 <= args.workers <= 16, "invalid read rate/worker count")
    bench.require(all(rate + args.read_rate > 0 for rate in args.write_rates), "at least one offered rate must be positive")
    bench.require(1 <= args.concurrency <= 1024 and 1 <= args.queue_capacity <= 16384, "invalid client/queue bound")
    bench.require(1 <= args.seconds <= 3600 and 1 <= args.warmup_seconds <= 60, "invalid duration")
    bench.require(1 <= args.startup_timeout <= 3600, "invalid bootstrap/restore deadline")
    for name in ("CELLULE_TEST_ENDPOINT", "CELLULE_TEST_BUCKET", "CELLULE_TEST_PREFIX", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        bench.require(os.environ.get(name), f"missing {name}")
    args.binary = args.binary.resolve(strict=True)
    args.driver = args.driver.resolve(strict=True)
    args.output = args.output.absolute()
    args.output.mkdir(parents=True, exist_ok=False)
    binary_hashes = {}
    for name, path in (("server", args.binary), ("driver", args.driver)):
        with path.open("rb") as source:
            binary_hashes[name] = hashlib.file_digest(source, "sha256").hexdigest()
    (args.output / "profile.json").write_text(json.dumps(dict(arguments={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()}, binary_sha256=binary_hashes),indent=2)+"\n")
    for cells in args.cells:
        for rate in args.write_rates:
            with fleet.followers(args, cells, rate, bench):
                result = point(args, cells, rate, args.output / f"cells-{cells}-rate-{rate}")
            print(json.dumps({k:result[k] for k in ("cells","write_rate","metrics","cold_audit","owner_rss_after_bytes","owner_cpu_cores_including_setup_warmup_drain")}),flush=True)


if __name__ == "__main__":
    main()
