#!/usr/bin/env python3
"""Run the SQL example in fleet-durability mode and audit every acknowledged write.

Two follower processes hold the node log; the owner acknowledges a write once the
fleet proof lands (or the bucket proof wins). Verification mirrors the object
lane: every acknowledged row is replayed after a cold owner restart and must
return its original receipt, and every Cell must advance its fence.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

HARNESS = Path(__file__).with_name("bench-axum-rustfs.py")
spec = importlib.util.spec_from_file_location("harness", HARNESS)
harness = importlib.util.module_from_spec(spec)
sys.modules["harness"] = harness
spec.loader.exec_module(harness)

require = harness.require
identity = harness.identity
request = harness.request
verify_order = harness.verify_order
summary = harness.summary
timed_writes = harness.timed_writes
parallel = harness.parallel


class Process:
    def __init__(self, binary, directory, label, follower, bind, timeout=180):
        self.label = label
        self.log_path = directory / f"{label}.log"
        env = os.environ.copy()
        env["CELLULE_AXUM_BIND"] = bind
        if follower is not None:
            env["CELLULE_AXUM_FOLLOWER"] = str(follower)
        self.output = self.log_path.open("w")
        self.process = subprocess.Popen([str(binary)], env=env,
                                        stdout=self.output, stderr=subprocess.STDOUT)
        started = time.perf_counter()
        marker = "Follower service:" if follower is not None else "Orders service:"
        deadline = started + timeout
        while time.perf_counter() < deadline:
            contents = self.log_path.read_text()
            if marker in contents:
                match = re.search(re.escape(marker) + r"\s*https?://(\S+)", contents)
                if match:
                    self.address = match.group(1)
                    self.startup_ms = (time.perf_counter() - started) * 1000
                    return
            require(self.process.poll() is None, f"{label} exited early; see {self.log_path}")
            time.sleep(0.05)
        raise RuntimeError(f"{label} did not become ready; see {self.log_path}")

    def message(self, prefix):
        return next((line.removeprefix(prefix) for line
                     in self.log_path.read_text().splitlines() if line.startswith(prefix)), None)

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.output.close()
        return self.process.returncode

    def kill(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self.output.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cells", type=int, default=1)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--write-seconds", type=int, default=30)
    parser.add_argument("--warmup-seconds", type=int, default=5)
    parser.add_argument("--follower-ports", default="18101,18102")
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    require(not args.output.exists(), "output directory must be new")
    args.output.mkdir(parents=True)
    require(os.environ.get("CELLULE_TEST_ENDPOINT"), "missing CELLULE_TEST_ENDPOINT")
    require(os.environ.get("CELLULE_AXUM_FLEET_DIR"), "missing CELLULE_AXUM_FLEET_DIR")

    ports = [int(value) for value in args.follower_ports.split(",")]
    followers, owner = [], None
    try:
        for index, port in enumerate(ports, 1):
            followers.append(Process(args.binary, args.output, f"follower-{index}", index,
                                     f"127.0.0.1:{port}"))
            print(json.dumps({"follower": index, "address": followers[-1].address}), flush=True)
        os.environ["CELLULE_AXUM_CELLS"] = str(args.cells)
        os.environ["CELLULE_AXUM_WORKERS"] = str(args.workers)
        owner = Process(args.binary, args.output, "owner", None, "127.0.0.1:0")
        startup = owner.message("Cells startup: ")
        require(startup is not None, "owner startup line missing")
        print(json.dumps({"owner": owner.address, "startup": startup}), flush=True)

        acknowledged = []
        connection = harness.http.client.HTTPConnection(owner.address, timeout=60)
        try:
            initial = {}
            for shard in range(args.cells):
                reply = request(connection, "GET", f"/orders/{shard}")
                require(reply["status"] == 200 and reply["body"]["output"] is None,
                        "nonempty initial Cell")
                initial[shard] = reply["body"]["receipt"]
            for index in range(1, args.warmup_seconds * args.concurrency + 1):
                envelope = identity(index)
                reply = request(connection, "POST", "/orders", envelope)
                verify_order(reply, envelope, initial[envelope["id"] % args.cells], 201)
                acknowledged.append((envelope, reply["body"]))
        finally:
            connection.close()
        first_id = max(envelope["id"] for envelope, _ in acknowledged) + 1

        writes, elapsed = timed_writes(owner.address, args.concurrency, args.write_seconds,
                                       first_id, args.output, "writes")
        (args.output / "writes.json").write_text(json.dumps(writes, indent=2) + "\n")
        write_summary = summary(writes, elapsed, 201)
        require(write_summary["errors"] == 0, f"write errors: {write_summary}")
        for reply in writes:
            verify_order(reply, reply["request"], initial[reply["request"]["id"] % args.cells], 201)
            acknowledged.append((reply["request"], reply["body"]))
        acknowledged.sort(key=lambda item: item[0]["id"])
        by_shard = {shard: [item for item in acknowledged if item[0]["id"] % args.cells == shard]
                    for shard in range(args.cells)}
        maxima = {}
        for shard, items in by_shard.items():
            receipts = sorted(body["receipt"]["commit_sequence"] for _, body in items)
            require(receipts == list(range(1, len(items) + 1)),
                    "Cell has missing/duplicate commit sequences")
            maxima[shard] = receipts[-1]

        def read(connection, item):
            envelope, body = item
            reply = request(connection, "GET", f"/orders/{envelope['id']}")
            verify_order(reply, envelope, body["receipt"])
            return reply

        reads, read_elapsed = parallel(owner.address, args.concurrency, acknowledged, read)
        read_summary = summary(reads, read_elapsed, 200)
        require(read_summary["errors"] == 0, "read errors")
        require(owner.stop() == 0, "owner shutdown failed")
        drained = [json.loads(line.removeprefix("Cell drained: "))
                   for line in owner.log_path.read_text().splitlines()
                   if line.startswith("Cell drained: ")]
        require(len(drained) == args.cells, "shutdown did not drain every Cell")
        raw_metrics = owner.message("Query metrics: ")
        require(raw_metrics is not None, "owner produced no query metrics at shutdown")
        metrics = json.loads(raw_metrics)
        sources = metrics["response_sources"]
        require(sources.get("fleet", 0) > 0, f"no fleet-proven responses: {sources}")
        (args.output / "query-metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")

        owner = Process(args.binary, args.output, "recovered", None, "127.0.0.1:0")
        require(owner.message("Cells startup: ") is not None, "recovered startup missing")
        require(int(re.search(r"restored: (\d+)",
                              owner.message("Cells startup: ")).group(1)) == args.cells,
                "restart did not restore every Cell from the bucket")
        recovered, _ = parallel(owner.address, args.concurrency, acknowledged, read)
        require(len(recovered) == len(acknowledged), "cold replay lost acknowledged rows")
        recovered_drained = []
        require(owner.stop() == 0, "recovered shutdown failed")
        recovered_drained = [json.loads(line.removeprefix("Cell drained: "))
                             for line in owner.log_path.read_text().splitlines()
                             if line.startswith("Cell drained: ")]
        previous = {item["shard"]: item for item in drained}
        for item in recovered_drained:
            old = previous[item["shard"]]
            require(item["epoch"] > old["epoch"] and item["cell"] == old["cell"],
                    "recovery did not acquire a new fence")
            require(item["commit_sequence"] == maxima[item["shard"]],
                    "recovery shutdown root is wrong")

        result = {
            "durability": "fleet", "cells": args.cells, "workers": args.workers,
            "concurrency": args.concurrency, "write_seconds": args.write_seconds,
            "writes": write_summary, "reads": read_summary,
            "response_sources": sources,
            "submission_sources": metrics.get("submission_sources"),
            "node_log_append": metrics.get("node_log_append"),
            "verification": {
                "acknowledged_rows": len(acknowledged),
                "cold_recovered_rows": len(recovered),
                "per_cell_acknowledged_rows": {shard: len(items)
                                               for shard, items in by_shard.items()},
                "fleet_proven": sources.get("fleet", 0),
                "object_proven": sources.get("object", 0),
                "passed": True,
            },
        }
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result), flush=True)
    finally:
        if owner is not None:
            owner.kill()
        for follower in followers:
            # Drain followers so their shutdown evidence is written; a hard kill
            # would discard the lane's own final accounting.
            follower.stop()


if __name__ == "__main__":
    main()
