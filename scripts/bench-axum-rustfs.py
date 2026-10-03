#!/usr/bin/env python3
"""Measure the SQL Axum example against real S3, then verify cold recovery.

Build the release binary in an isolated source snapshot before running this.
Requires CELLULE_TEST_{ENDPOINT,BUCKET,PREFIX} and explicit AWS credentials.
Each point uses a fresh S3 prefix, temporary SQLite, and HTTP/1.1 keepalive.
All timings are client wall time. This is closed-loop local characterization,
not an offered-load capacity qualification or a crash/fault qualification.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import threading
import time
import uuid


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def identity(order_id):
    now = time.time_ns() // 1_000_000
    return {
        "request_id": str(uuid.uuid4()),
        "issued_at_ms": now,
        "expires_at_ms": now + 3_600_000,
        "id": order_id,
        "total_cents": order_id * 13 + 99,
    }


def request(connection, method, path, body=None):
    encoded = None if body is None else json.dumps(body, separators=(",", ":"))
    started = time.perf_counter_ns()
    connection.request(method, path, encoded, {"Content-Type": "application/json"})
    response = connection.getresponse()
    raw = response.read()
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    return {"status": response.status, "body": json.loads(raw), "latency_ms": elapsed_ms}


def receipt_matches(actual, minimum):
    return (
        actual["cell"] == minimum["cell"]
        and actual["incarnation"] == minimum["incarnation"]
        and actual["commit_sequence"] >= minimum["commit_sequence"]
    )


def verify_order(reply, envelope, minimum=None, status=200):
    require(reply["status"] == status, f"unexpected HTTP response: {reply}")
    expected = {key: envelope[key] for key in ("id", "total_cents")}
    require(reply["body"]["output"] == expected, f"wrong order: {reply}")
    receipt = reply["body"]["receipt"]
    require(re.fullmatch(r"[0-9a-f]{64}", receipt["cell"]), "invalid Cell receipt")
    require(re.fullmatch(r"[0-9a-f]{32}", receipt["incarnation"]), "invalid incarnation")
    if minimum is not None:
        require(receipt_matches(receipt, minimum), "read failed its receipt minimum")


def parallel(address, concurrency, inputs, operation):
    """One persistent connection per worker; fail rather than hide transport errors."""
    started = []
    barrier = threading.Barrier(concurrency + 1, action=lambda: started.append(time.perf_counter()))

    def worker(index):
        connection = http.client.HTTPConnection(address, timeout=60)
        try:
            connection.connect()
            barrier.wait(timeout=60)
            return [operation(connection, item) for item in inputs[index::concurrency]]
        except BaseException:
            barrier.abort()
            raise
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, index) for index in range(concurrency)]
        barrier.wait(timeout=60)
        results = [item for future in futures for item in future.result()]
        elapsed = time.perf_counter() - started[0]
    return results, elapsed


def summary(results, elapsed, expected_status):
    latencies = sorted(item["latency_ms"] for item in results)
    successes = sum(item["status"] == expected_status for item in results)
    percentile = lambda percent: latencies[max(0, math.ceil(len(latencies) * percent / 100) - 1)]
    return {
        "attempts": len(results),
        "successes": successes,
        "errors": len(results) - successes,
        "statuses": dict(Counter(str(item["status"]) for item in results)),
        "seconds": elapsed,
        "successful_requests_per_second": successes / elapsed,
        "latency_ms_all_attempts": {"p50": percentile(50), "p95": percentile(95), "p99": percentile(99)},
    }


class Service:
    def __init__(self, binary, directory, prefix, label, cells, workers):
        self.cells = cells
        self.log = directory / f"{label}.log"
        env = os.environ.copy()
        env.update(CELLULE_TEST_PREFIX=prefix, CELLULE_AXUM_BIND="127.0.0.1:0",
                   CELLULE_AXUM_CELLS=str(cells), CELLULE_AXUM_WORKERS=str(workers))
        started = time.perf_counter()
        self.output = self.log.open("w")
        self.process = subprocess.Popen([str(binary)], env=env, stdout=self.output, stderr=subprocess.STDOUT)
        try:
            deadline = started + 120
            while time.perf_counter() < deadline:
                contents = self.log.read_text()
                ready = re.search(r"Orders service: http://(\S+)", contents)
                if ready:
                    self.address = ready.group(1)
                    startup = re.search(r"Cells startup: (\d+); restored: (\d+); workers: (\d+); SQLite directory: (.+)", contents)
                    require(startup is not None and int(startup.group(1)) == cells and int(startup.group(3)) == workers, "wrong service topology")
                    self.sqlite = Path(startup.group(4))
                    self.restored = int(startup.group(2))
                    require("Storage probe: passed all six checks;" in contents, "S3 capability probe missing")
                    self.startup_ms = (time.perf_counter() - started) * 1000
                    return
                require(self.process.poll() is None, f"service failed to start; see {self.log}")
                time.sleep(0.05)
            raise RuntimeError(f"service startup timed out; see {self.log}")
        except BaseException:
            self.abort()
            raise

    def stop(self):
        started = time.perf_counter()
        self.process.send_signal(signal.SIGINT)
        try:
            code = self.process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.abort()
            raise
        finally:
            self.output.close()
        require(code == 0, f"service shutdown failed; see {self.log}")
        drained = [json.loads(line.removeprefix("Cell drained: ")) for line in self.log.read_text().splitlines() if line.startswith("Cell drained: ")]
        require(len(drained) == self.cells and {item["shard"] for item in drained} == set(range(self.cells)), "shutdown did not prove every Cell released authority")
        require(all(item["state"] == "idle" for item in drained), "a drained Cell is not idle")
        require(not self.sqlite.exists(), "temporary SQLite files survived shutdown")
        return {
            "milliseconds": (time.perf_counter() - started) * 1000,
            "cells": drained,
            "query_metrics": next((json.loads(line.removeprefix("Query metrics: ")) for line in self.log.read_text().splitlines() if line.startswith("Query metrics: ")), None),
        }

    def abort(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.output.close()


def process_usage(pid):
    fields = subprocess.check_output(["ps", "-p", str(pid), "-o", "time=", "-o", "rss="], text=True).split()
    cpu = fields[0]
    days, cpu = cpu.split("-", 1) if "-" in cpu else ("0", cpu)
    seconds = float(days) * 86400
    for index, value in enumerate(reversed(cpu.split(":"))):
        seconds += float(value) * 60 ** index
    return {"cpu_seconds": seconds, "rss_bytes": int(fields[1]) * 1024}


def steady_reads(args, directory, service, concurrency, acknowledged):
    config = {
        "address": service.address, "concurrency": concurrency,
        "seconds": args.read_seconds, "warmup_seconds": args.read_warmup_seconds,
        "orders": [body for _, body in acknowledged],
    }
    config_path = directory / "steady-config.json"
    config_path.write_text(json.dumps(config, indent=2) + "\n")
    before = process_usage(service.process.pid)
    profiler = None
    if args.sample and platform.system() == "Darwin":
        profiler = subprocess.Popen(["/usr/bin/sample", str(service.process.pid), "10", "-file", str(directory / "server-sample.txt")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    started = time.perf_counter()
    try:
        load = subprocess.run(["/usr/bin/time", "-p", str(args.read_driver), str(config_path)], capture_output=True, text=True, timeout=args.read_seconds + args.read_warmup_seconds + 90)
    finally:
        if profiler is not None:
            profiler.wait(timeout=30)
    wall = time.perf_counter() - started
    after = process_usage(service.process.pid)
    (directory / "steady-driver.stderr").write_text(load.stderr)
    (directory / "steady-driver.stdout").write_text(load.stdout)
    require(load.returncode == 0, f"steady-state driver failed; see {directory}")
    result = json.loads(load.stdout)
    driver_cpu = sum(float(value) for value in re.findall(r"^(?:user|sys)\s+([\d.]+)$", load.stderr, re.MULTILINE))
    result["resources"] = {
        "window_seconds_including_warmup": wall,
        "server_cpu_seconds": after["cpu_seconds"] - before["cpu_seconds"],
        "server_cpu_cores": (after["cpu_seconds"] - before["cpu_seconds"]) / wall,
        "driver_cpu_seconds": driver_cpu, "driver_cpu_cores": driver_cpu / wall,
        "server_cpu_us_per_read": (after["cpu_seconds"] - before["cpu_seconds"]) * 1_000_000 / (result["attempts"] + result["warmup_successes"]),
        "server_rss_before_bytes": before["rss_bytes"], "server_rss_after_bytes": after["rss_bytes"],
        "sample_requested": args.sample,
    }
    (directory / "steady-reads.json").write_text(json.dumps(result, indent=2) + "\n")
    require(result["errors"] == 0 and all(result["per_order_successes"]), "steady-state reads failed validation or coverage")
    return result


def verify_active_owner_refused(binary, directory, prefix, cells, workers):
    env = os.environ.copy()
    env.update(CELLULE_TEST_PREFIX=prefix, CELLULE_AXUM_BIND="127.0.0.1:0",
               CELLULE_AXUM_CELLS=str(cells), CELLULE_AXUM_WORKERS=str(workers))
    log = directory / "active-owner-refused.log"
    with log.open("w") as output:
        candidate = subprocess.run([str(binary)], env=env, stdout=output, stderr=subprocess.STDOUT, timeout=120)
    require(candidate.returncode != 0, "second service took an active Cell")
    require("idle acquisition requires a published idle control" in log.read_text(), "second service failed for an unexpected reason")


def point(args, directory, repeat, concurrency, cells):
    directory.mkdir()
    suffix = f"-{args.variant}" if args.baseline_binary else ""
    prefix = f"{os.environ['CELLULE_TEST_PREFIX']}/r{repeat}-n{cells}-c{concurrency}{suffix}"
    service = Service(args.binary, directory, prefix, "initial", cells, args.workers)
    acknowledged = []
    try:
        envelopes = [identity(index) for index in range(1, args.warmup + args.writes + 1)]
        (directory / "requests.json").write_text(json.dumps(envelopes, indent=2) + "\n")
        require(service.restored == 0, "benchmark prefix already exists; choose a fresh prefix")
        connection = http.client.HTTPConnection(service.address, timeout=60)
        try:
            initial_receipts = {}
            for shard in range(cells):
                initial = request(connection, "GET", f"/orders/{shard}")
                require(initial["status"] == 200 and initial["body"]["output"] is None, "nonempty initial Cell")
                initial_receipts[shard] = initial["body"]["receipt"]
            require(len({receipt["cell"] for receipt in initial_receipts.values()}) == cells, "HTTP routing did not reach every distinct Cell")
            require(all(receipt["commit_sequence"] == 0 for receipt in initial_receipts.values()), "initial Cell is not at sequence zero")
            for envelope in envelopes[:args.warmup]:
                reply = request(connection, "POST", "/orders", envelope)
                verify_order(reply, envelope, initial_receipts[envelope["id"] % cells], 201)
                acknowledged.append((envelope, reply["body"]))
        finally:
            connection.close()

        verify_active_owner_refused(args.binary, directory, prefix, cells, args.workers)

        def write(connection, envelope):
            reply = request(connection, "POST", "/orders", envelope)
            return dict(reply, request=envelope)

        writes, elapsed = parallel(service.address, concurrency, envelopes[args.warmup:], write)
        (directory / "writes.json").write_text(json.dumps(writes, indent=2) + "\n")
        write_summary = summary(writes, elapsed, 201)
        # Persist every failure before refusing qualification. Never silently
        # retry an uncertain outcome or replace the request identity.
        require(write_summary["errors"] == 0, f"write errors: {write_summary}; see {directory}")
        for reply in writes:
            verify_order(reply, reply["request"], initial_receipts[reply["request"]["id"] % cells], 201)
            acknowledged.append((reply["request"], reply["body"]))
        acknowledged.sort(key=lambda item: item[0]["id"])
        by_shard = {shard: [item for item in acknowledged if item[0]["id"] % cells == shard] for shard in range(cells)}
        maxima = {}
        for shard, items in by_shard.items():
            receipts = sorted(body["receipt"]["commit_sequence"] for _, body in items)
            require(receipts == list(range(1, len(items) + 1)), "Cell has missing/duplicate commit sequences")
            maxima[shard] = receipts[-1]

        def read(connection, item):
            envelope, body = item
            reply = request(connection, "GET", f"/orders/{envelope['id']}")
            verify_order(reply, envelope, body["receipt"])
            return reply

        inputs = [acknowledged[index % len(acknowledged)] for index in range(args.reads)]
        reads, elapsed = parallel(service.address, concurrency, inputs, read)
        (directory / "reads.json").write_text(json.dumps(reads, indent=2) + "\n")
        read_summary = summary(reads, elapsed, 200)
        require(read_summary["errors"] == 0, "read errors")
        steady = steady_reads(args, directory, service, concurrency, acknowledged) if args.read_seconds else None

        def replay(connection, item):
            envelope, original = item
            reply = request(connection, "POST", "/orders", envelope)
            require(reply["status"] == 201 and reply["body"] == original, "retry changed its recorded output/receipt")
            return reply

        replayed, _ = parallel(service.address, concurrency, acknowledged, replay)
        connection = http.client.HTTPConnection(service.address, timeout=60)
        try:
            conflicts, invalid_replies = [], []
            for shard, items in by_shard.items():
                altered = dict(items[0][0], total_cents=-1)
                conflict = request(connection, "POST", "/orders", altered)
                require(conflict["status"] == 409 and conflict["body"]["code"] == "request_conflict", "identity conflict was not rejected")
                conflicts.append(conflict)
                invalid = dict(identity(shard - 2 * cells), issued_at_ms=1, expires_at_ms=2)
                invalid_reply = request(connection, "POST", "/orders", invalid)
                require(invalid_reply["status"] == 400 and invalid_reply["body"]["code"] == "invalid_request", "expired identity was not rejected")
                invalid_replies.append(invalid_reply)
                missing = request(connection, "GET", f"/orders/{invalid['id']}")
                require(missing["status"] == 200 and missing["body"]["output"] is None, "rejected requests changed data")
        finally:
            connection.close()
        drained = service.stop()
        for item in drained["cells"]:
            require(item["cell"] == initial_receipts[item["shard"]]["cell"] and item["commit_sequence"] == maxima[item["shard"]], "retry/conflict changed a Cell's published sequence")
        original_sqlite = service.sqlite
        initial_startup = service.startup_ms
        service = Service(args.binary, directory, prefix, "recovered", cells, args.workers)
        require(service.restored == cells, "restart did not restore every Cell from S3")
        require(service.sqlite != original_sqlite, "restart reused local SQLite")
        recovered, _ = parallel(service.address, concurrency, acknowledged, read)
        recovered_replays, _ = parallel(service.address, concurrency, acknowledged, replay)
        # One shared identity across distinct Cells proves request ledgers are
        # Cell-scoped. Each recovered writer must publish and replay independently.
        shared_identity = identity(len(envelopes) + 1)
        next_replies = []
        connection = http.client.HTTPConnection(service.address, timeout=60)
        try:
            for index in range(cells):
                next_envelope = dict(shared_identity, id=len(envelopes) + index + 1, total_cents=100 + index)
                shard = next_envelope["id"] % cells
                next_reply = request(connection, "POST", "/orders", next_envelope)
                verify_order(next_reply, next_envelope, by_shard[shard][0][1]["receipt"], 201)
                require(next_reply["body"]["receipt"]["commit_sequence"] == maxima[shard] + 1, "recovered writer did not continue its sequence")
                verify_order(request(connection, "GET", f"/orders/{next_envelope['id']}"), next_envelope, next_reply["body"]["receipt"])
                replay_reply = request(connection, "POST", "/orders", next_envelope)
                require(replay_reply["status"] == 201 and replay_reply["body"] == next_reply["body"], "independent Cell ledger replay failed")
                next_replies.append(dict(next_reply, request=next_envelope))
        finally:
            connection.close()
        recovery_startup = service.startup_ms
        recovered_drained = service.stop()
        old_fences = {item["shard"]: item for item in drained["cells"]}
        for item in recovered_drained["cells"]:
            previous = old_fences[item["shard"]]
            require(item["epoch"] > previous["epoch"] and item["cell"] == previous["cell"], "recovery did not acquire the Cell's new fence")
            require(item["commit_sequence"] == maxima[item["shard"]] + 1, "recovery shutdown root is wrong")
        result = {
            "repeat": repeat, "concurrency": concurrency, "cells": cells, "workers": args.workers, "prefix": prefix, "variant": args.variant,
            "writes": write_summary, "reads": read_summary, "steady_reads": steady,
            "verification": {
                "warmup_writes": args.warmup, "acknowledged_rows": len(acknowledged),
                "live_exact_retries": len(replayed), "conflicts": conflicts,
                "invalid_identities": invalid_replies,
                "cold_recovered_rows": len(recovered), "cold_exact_retries": len(recovered_replays),
                "next_recovered_writes": next_replies,
                "independent_request_ledgers": True,
                "per_cell_acknowledged_rows": {shard: len(items) for shard, items in by_shard.items()},
                "initial_startup_ms": initial_startup, "cold_startup_ms": recovery_startup,
                "initial_drain": drained, "recovered_drain": recovered_drained,
                "fresh_sqlite": True, "active_owner_refused": True, "passed": True,
            },
        }
        (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        return result
    finally:
        service.abort()


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--baseline-binary", type=Path, help="alternate baseline/candidate order on each repeat")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=positive, default=3)
    parser.add_argument("--concurrency", type=positive, nargs="+", default=[1, 4, 16])
    parser.add_argument("--cells", type=positive, nargs="+", default=[1])
    parser.add_argument("--workers", type=positive, default=1)
    parser.add_argument("--writes", type=positive, default=300)
    parser.add_argument("--reads", type=positive, default=1500)
    parser.add_argument("--warmup", type=positive, default=20)
    parser.add_argument("--read-driver", type=Path, help="release http_load example binary")
    parser.add_argument("--read-seconds", type=positive, help="steady-state read duration, up to 3600 seconds")
    parser.add_argument("--read-warmup-seconds", type=positive, default=5)
    parser.add_argument("--sample", action="store_true", help="capture a ten-second macOS server stack sample")
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    if args.baseline_binary:
        args.baseline_binary = args.baseline_binary.resolve(strict=True)
    require(bool(args.read_driver) == bool(args.read_seconds), "read driver and duration must be supplied together")
    if args.read_driver:
        args.read_driver = args.read_driver.resolve(strict=True)
        require(args.read_seconds <= 3600 and args.read_warmup_seconds <= 60, "read duration/warmup exceed driver bounds")
        require(max(args.concurrency) <= 128, "steady-state clients are bounded at 128")
    require(len(set(args.concurrency)) == len(args.concurrency), "concurrency points must be distinct")
    require(len(set(args.cells)) == len(args.cells), "Cell counts must be distinct")
    require(max(args.cells) <= 16 and args.workers <= 16, "Cells and workers are bounded at 16")
    require(min(args.warmup, args.writes, args.reads) >= max(args.cells), "each phase must exercise every Cell")
    for name in ("CELLULE_TEST_ENDPOINT", "CELLULE_TEST_BUCKET", "CELLULE_TEST_PREFIX", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        require(os.environ.get(name), f"missing {name}")
    require(not args.output.exists(), "output directory must be new")
    args.output.mkdir(parents=True)
    with args.binary.open("rb") as binary:
        binary_digest = hashlib.file_digest(binary, "sha256").hexdigest()
    driver_digest = None
    if args.read_driver:
        with args.read_driver.open("rb") as driver:
            driver_digest = hashlib.file_digest(driver, "sha256").hexdigest()
    baseline_digest = None
    if args.baseline_binary:
        with args.baseline_binary.open("rb") as binary:
            baseline_digest = hashlib.file_digest(binary, "sha256").hexdigest()
    metadata = {
        "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "python": platform.python_version(), "binary_sha256": binary_digest,
        "endpoint": os.environ["CELLULE_TEST_ENDPOINT"], "bucket": os.environ["CELLULE_TEST_BUCKET"],
        "writes": args.writes, "reads": args.reads, "warmup": args.warmup,
        "repeats": args.repeats, "concurrency": args.concurrency, "cells": args.cells, "workers": args.workers,
        "read_seconds": args.read_seconds, "read_warmup_seconds": args.read_warmup_seconds,
        "read_driver_sha256": driver_digest, "baseline_binary_sha256": baseline_digest,
        "http_tokio_workers": os.environ.get("TOKIO_WORKER_THREADS", "system_default"),
        "workload": "order_id mod active Cells; POST inserts then receipt-bound SELECT; GET owner-ordered SELECT; closed loop",
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    results = []
    candidate_binary = args.binary
    for repeat in range(1, args.repeats + 1):
        for concurrency in args.concurrency:
            for cells in args.cells:
                variants = [("candidate", candidate_binary)]
                if args.baseline_binary:
                    variants.insert(0, ("baseline", args.baseline_binary))
                    if repeat % 2 == 0:
                        variants.reverse()
                for variant, binary in variants:
                    args.variant, args.binary = variant, binary
                    suffix = f"-{variant}" if args.baseline_binary else ""
                    result = point(args, args.output / f"r{repeat}-n{cells}-c{concurrency}{suffix}", repeat, concurrency, cells)
                    results.append(result)
                    (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
                    steady = {key: value for key, value in (result["steady_reads"] or {}).items() if key != "per_order_successes"}
                    print(json.dumps({"repeat": repeat, "concurrency": concurrency, "cells": cells, "workers": args.workers, "variant": variant, "writes": result["writes"], "reads": result["reads"], "steady_reads": steady, "verified": True}), flush=True)


if __name__ == "__main__":
    main()
