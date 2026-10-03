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
    def __init__(self, binary, directory, prefix, label):
        self.log = directory / f"{label}.log"
        env = os.environ.copy()
        env.update(CELLULE_TEST_PREFIX=prefix, CELLULE_AXUM_BIND="127.0.0.1:0")
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
                    self.sqlite = Path(re.search(r"SQLite directory: (.+)", contents).group(1))
                    self.restored = "Cell startup: restored;" in contents
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
        drained = re.search(r"Cell drained: idle; epoch: (\d+); commit_sequence: (\d+)", self.log.read_text())
        require(drained is not None, "shutdown did not prove released authority")
        require(not self.sqlite.exists(), "temporary SQLite files survived shutdown")
        return {
            "milliseconds": (time.perf_counter() - started) * 1000,
            "epoch": int(drained.group(1)),
            "commit_sequence": int(drained.group(2)),
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


def verify_active_owner_refused(binary, directory, prefix):
    env = os.environ.copy()
    env.update(CELLULE_TEST_PREFIX=prefix, CELLULE_AXUM_BIND="127.0.0.1:0")
    log = directory / "active-owner-refused.log"
    with log.open("w") as output:
        candidate = subprocess.run([str(binary)], env=env, stdout=output, stderr=subprocess.STDOUT, timeout=120)
    require(candidate.returncode != 0, "second service took an active Cell")
    require("idle acquisition requires a published idle control" in log.read_text(), "second service failed for an unexpected reason")


def point(args, directory, repeat, concurrency):
    directory.mkdir()
    prefix = f"{os.environ['CELLULE_TEST_PREFIX']}/r{repeat}-c{concurrency}"
    service = Service(args.binary, directory, prefix, "initial")
    acknowledged = []
    try:
        envelopes = [identity(index) for index in range(1, args.warmup + args.writes + 1)]
        (directory / "requests.json").write_text(json.dumps(envelopes, indent=2) + "\n")
        require(not service.restored, "benchmark prefix already exists; choose a fresh prefix")
        connection = http.client.HTTPConnection(service.address, timeout=60)
        try:
            initial = request(connection, "GET", "/orders/0")
            require(initial["status"] == 200 and initial["body"]["output"] is None, "nonempty initial Cell")
            initial_receipt = initial["body"]["receipt"]
            for envelope in envelopes[:args.warmup]:
                reply = request(connection, "POST", "/orders", envelope)
                verify_order(reply, envelope, initial_receipt, 201)
                acknowledged.append((envelope, reply["body"]))
        finally:
            connection.close()

        verify_active_owner_refused(args.binary, directory, prefix)

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
            verify_order(reply, reply["request"], initial_receipt, 201)
            acknowledged.append((reply["request"], reply["body"]))
        receipts = [body["receipt"]["commit_sequence"] for _, body in acknowledged]
        expected = list(range(initial_receipt["commit_sequence"] + 1, initial_receipt["commit_sequence"] + len(acknowledged) + 1))
        require(sorted(receipts) == expected, "acknowledged writes have missing/duplicate commit sequences")

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

        def replay(connection, item):
            envelope, original = item
            reply = request(connection, "POST", "/orders", envelope)
            require(reply["status"] == 201 and reply["body"] == original, "retry changed its recorded output/receipt")
            return reply

        replayed, _ = parallel(service.address, concurrency, acknowledged, replay)
        connection = http.client.HTTPConnection(service.address, timeout=60)
        try:
            altered = dict(acknowledged[0][0], total_cents=-1)
            conflict = request(connection, "POST", "/orders", altered)
            require(conflict["status"] == 409 and conflict["body"]["code"] == "request_conflict", "identity conflict was not rejected")
            invalid = dict(identity(-1), issued_at_ms=1, expires_at_ms=2)
            invalid_reply = request(connection, "POST", "/orders", invalid)
            require(invalid_reply["status"] == 400 and invalid_reply["body"]["code"] == "invalid_request", "expired identity was not rejected")
            missing = request(connection, "GET", "/orders/-1")
            require(missing["status"] == 200 and missing["body"]["output"] is None, "rejected requests changed data")
        finally:
            connection.close()
        drained = service.stop()
        require(drained["commit_sequence"] == max(receipts), "retry/conflict changed publication sequence")
        original_sqlite = service.sqlite
        initial_startup = service.startup_ms
        service = Service(args.binary, directory, prefix, "recovered")
        require(service.restored, "restart bootstrapped instead of restoring S3 state")
        require(service.sqlite != original_sqlite, "restart reused local SQLite")
        recovered, _ = parallel(service.address, concurrency, acknowledged, read)
        recovered_replays, _ = parallel(service.address, concurrency, acknowledged, replay)
        # Also prove the recovered writer can durably publish its next command.
        next_envelope = identity(len(envelopes) + 1)
        connection = http.client.HTTPConnection(service.address, timeout=60)
        try:
            next_reply = request(connection, "POST", "/orders", next_envelope)
            verify_order(next_reply, next_envelope, acknowledged[0][1]["receipt"], 201)
            require(next_reply["body"]["receipt"]["commit_sequence"] == max(receipts) + 1, "recovered writer did not continue its sequence")
            verify_order(request(connection, "GET", f"/orders/{next_envelope['id']}"), next_envelope, next_reply["body"]["receipt"])
        finally:
            connection.close()
        recovery_startup = service.startup_ms
        recovered_drained = service.stop()
        require(recovered_drained["epoch"] > drained["epoch"], "recovery did not acquire a new fence")
        require(recovered_drained["commit_sequence"] == max(receipts) + 1, "recovery shutdown root is wrong")
        result = {
            "repeat": repeat, "concurrency": concurrency, "prefix": prefix,
            "writes": write_summary, "reads": read_summary,
            "verification": {
                "warmup_writes": args.warmup, "acknowledged_rows": len(acknowledged),
                "live_exact_retries": len(replayed), "conflict": conflict,
                "invalid_identity": invalid_reply,
                "cold_recovered_rows": len(recovered), "cold_exact_retries": len(recovered_replays),
                "next_recovered_write": next_reply,
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
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=positive, default=3)
    parser.add_argument("--concurrency", type=positive, nargs="+", default=[1, 4, 16])
    parser.add_argument("--writes", type=positive, default=300)
    parser.add_argument("--reads", type=positive, default=1500)
    parser.add_argument("--warmup", type=positive, default=20)
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    require(len(set(args.concurrency)) == len(args.concurrency), "concurrency points must be distinct")
    for name in ("CELLULE_TEST_ENDPOINT", "CELLULE_TEST_BUCKET", "CELLULE_TEST_PREFIX", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        require(os.environ.get(name), f"missing {name}")
    require(not args.output.exists(), "output directory must be new")
    args.output.mkdir(parents=True)
    with args.binary.open("rb") as binary:
        binary_digest = hashlib.file_digest(binary, "sha256").hexdigest()
    metadata = {
        "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "python": platform.python_version(), "binary_sha256": binary_digest,
        "endpoint": os.environ["CELLULE_TEST_ENDPOINT"], "bucket": os.environ["CELLULE_TEST_BUCKET"],
        "writes": args.writes, "reads": args.reads, "warmup": args.warmup,
        "repeats": args.repeats, "concurrency": args.concurrency,
        "workload": "one SQL Cell; POST inserts then receipt-bound SELECT; GET owner-ordered SELECT; closed loop",
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    results = []
    for repeat in range(1, args.repeats + 1):
        for concurrency in args.concurrency:
            result = point(args, args.output / f"r{repeat}-c{concurrency}", repeat, concurrency)
            results.append(result)
            (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            print(json.dumps({"repeat": repeat, "concurrency": concurrency, "writes": result["writes"], "reads": result["reads"], "verified": True}), flush=True)


if __name__ == "__main__":
    main()
