#!/usr/bin/env python3
"""Persistent process proof, including SIGKILL between receiver commit and queue ack.

Run only in CI or an isolated source snapshot with its private local S3 provider.
The binary owns four fixed Cells; run no other work-queue instance concurrently.
"""
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import time
import uuid

from process import until


def main():
    binary = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="cellule-work-queue-") as temporary:
        root = Path(temporary)
        state = str(root / "state")
        active = None

        def run(*args, success=True):
            result = subprocess.run([binary, *map(str, args)], text=True, capture_output=True, timeout=60)
            if success and result.returncode:
                raise RuntimeError(f"{args}: {result.stderr}")
            if not success and result.returncode == 0:
                raise AssertionError(f"{args}: unexpectedly succeeded")
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def mutation(operation):
            identity = uuid.uuid4().hex
            source = root / f"{identity}.json"
            retained = root / f"{identity}.mutation.json"
            source.write_text(json.dumps(operation))
            run("prepare", source, retained)
            return retained

        def apply(operation):
            record = mutation(operation)
            return run("apply", state, record)[-1]

        def info():
            rows = run("info", state)[-1]["shards"]
            return {key: sum(row[key] for row in rows) for key in ("ready", "leased", "acked", "dead")}

        def inspect():
            return run("inspect", state)[-1]

        def worker(seconds, delay=0):
            return run("worker", state, seconds, delay)

        def job(body):
            return {"id": str(uuid.uuid4()), "body": body}

        try:
            baseline = inspect()
            if not baseline["enabled"]:
                apply({"operation": "enabled", "value": True})
            initial_queue = info()
            assert all(initial_queue[key] == 0 for key in ("ready", "leased", "dead")), (
                "process proof requires a quiescent queue; retain failed-run evidence and use a fresh private provider",
                initial_queue,
            )
            first = job("Idempotent crash recovery")
            retained = mutation({"operation": "submit", "job": first, "available_at_ms": int(time.time() * 1000)})
            sent = run("apply", state, retained)[-1]
            assert run("apply", state, retained)[-1] == sent
            for shard in range(2):
                apply({"operation": "pause", "shard": shard})
            worker(1)
            paused = info()
            assert paused["ready"] == initial_queue["ready"] + 1
            assert paused["leased"] == 0
            for shard in range(2):
                apply({"operation": "resume", "shard": shard})

            error_log = root / "crash-worker.stderr"
            with error_log.open("w") as errors:
                active = subprocess.Popen([binary, "worker", state, "60", "10000"], stdout=subprocess.PIPE, stderr=errors, bufsize=0)
                selector = selectors.DefaultSelector()
                selector.register(active.stdout, selectors.EVENT_READ)
                deadline = time.monotonic() + 45
                checkpoint = None
                buffered = b""
                while time.monotonic() < deadline:
                    for key, _ in selector.select(timeout=1):
                        chunk = os.read(key.fileobj.fileno(), 4096)
                        if not chunk:
                            raise RuntimeError(f"worker exited before checkpoint: {error_log.read_text()}")
                        buffered += chunk
                        while b"\n" in buffered:
                            line, buffered = buffered.split(b"\n", 1)
                            event = json.loads(line)
                            if event.get("event") == "receiver_published" and event.get("job") == first["id"]:
                                checkpoint = event
                                break
                    if checkpoint:
                        break
                selector.close()
                assert checkpoint and checkpoint["outcome"] == "Recorded" and checkpoint["attempt"] == 1
                # Intentional fault, only after durable receiver publication.
                active.kill()
                active.wait(timeout=10)
                active.stdout.close()
                active = None
            orphan_sessions = set((root / "state").iterdir())
            assert orphan_sessions, "SIGKILL should retain local working evidence"
            # A new process cannot steal a still-live node lease.
            run("inspect", state, success=False)
            time.sleep(32)
            recovered = until(binary, ["worker", state, 240], lambda events: any(
                event.get("job") == first["id"] and event.get("outcome") == "Duplicate" and event.get("attempt") == 2
                for event in events), root / "recovery.stderr")
            assert any(event.get("job") == first["id"] and event.get("outcome") == "Duplicate" and event.get("attempt") == 2 for event in recovered), recovered
            assert inspect()["delivered"] == baseline["delivered"] + 1
            assert info()["acked"] == initial_queue["acked"] + 1
            resolved = run("resolve", state, retained)[-1]
            assert resolved["resolution"] == "committed" and resolved["commit_sequence"] == sent["commit_sequence"]

            apply({"operation": "enabled", "value": False})
            failed = job("Retry, inspect, and redrive")
            apply({"operation": "submit", "job": failed, "available_at_ms": int(time.time() * 1000)})
            until(binary, ["worker", state, 240], lambda events: any(
                event.get("job") == failed["id"] and event.get("dead") and event.get("outcome") == "Recorded"
                for event in events), root / "exhaustion.stderr")
            dead = inspect()
            assert dead["dead"] == baseline["dead"] + 1, dead
            assert dead["delivered"] == baseline["delivered"] + 1
            assert info()["dead"] == initial_queue["dead"] + 1
            apply({"operation": "enabled", "value": True})
            for shard in range(2):
                apply({"operation": "redrive", "shard": shard, "limit": 1})
            until(binary, ["worker", state, 240], lambda events: any(
                event.get("job") == failed["id"] and not event.get("dead") and event.get("outcome") == "Recorded"
                for event in events), root / "redrive.stderr")
            final = inspect()
            assert final["delivered"] == baseline["delivered"] + 2
            assert final["dead"] == baseline["dead"] + 1
            assert info()["leased"] == 0
            changed = mutation({"operation": "submit", "job": {**first, "body": "Changed bytes"}, "available_at_ms": int(time.time() * 1000)})
            rejected = run("apply", state, changed, success=False)[-1]
            assert rejected["status"] == "rejected" and rejected["outcome"] == "ProducerConflict"
            assert run("apply", state, changed, success=False)[-1] == rejected
            assert run("resolve", state, changed)[-1]["outcome"] == "ProducerConflict"
            assert inspect()["delivered"] == baseline["delivered"] + 2
            paged = []
            cursor = "-"
            while True:
                page = run("inspect", state, cursor, 1)[-1]
                paged.extend(page["rows"])
                if page["next"] is None:
                    break
                cursor = json.dumps(page["next"])
            assert len(paged) == final["delivered"] + final["dead"]
            assert len({(row["job"]["id"], row["dead"]) for row in paged}) == len(paged)
            assert set((root / "state").iterdir()) == orphan_sessions, "successful drains must release their files and preserve interrupted-session evidence"
            print(json.dumps({"scenario": "passed", "checks": ["persistent-s3", "independent-processes", "producer-replay", "pause-resume", "kill-after-receiver-publication", "live-owner-refusal", "expired-owner-takeover", "lease-redelivery", "one-logical-external-action", "signed-native-dead-letter", "inspection", "redrive", "durable-conflict", "error-path-drain"]}))
        finally:
            if active and active.poll() is None:
                active.send_signal(signal.SIGTERM)
                try:
                    active.wait(timeout=25)
                except subprocess.TimeoutExpired:
                    active.kill()
                    active.wait(timeout=10)


if __name__ == "__main__":
    main()
