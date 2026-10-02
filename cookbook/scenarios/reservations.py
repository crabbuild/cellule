#!/usr/bin/env python3
"""Persistent inventory, deadline Effect ambiguity, generation fencing and cold restore.

Run a built isolated source snapshot against private local S3, never a live installation.
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


def main():
    binary = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="cellule-reservations-") as temporary:
        root = Path(temporary)
        state = root / "state"
        cold = root / "cold"
        active = None
        event_buffers = {}

        def run(*args, success=True):
            result = subprocess.run([binary, *map(str, args)], capture_output=True, text=True, timeout=75)
            if success and result.returncode:
                raise RuntimeError(f"{args}: {result.stderr}")
            if not success and result.returncode == 0:
                raise AssertionError(f"{args}: unexpectedly succeeded")
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def mutation(event_key, operation, **fields):
            token = uuid.uuid4().hex
            source = root / f"{token}.input.json"
            retained = root / f"{token}.mutation.json"
            source.write_text(json.dumps({"event": event_key, "action": {"operation": operation, **fields}}))
            run("prepare", source, retained)
            return retained

        def apply(event_key, operation, **fields):
            return run("apply", state, mutation(event_key, operation, **fields))[-1]

        def read_hold(event_key, identity, directory=state):
            return run("hold", directory, event_key, identity)[-1]["hold"]

        def inventory(event_key, directory=state):
            return run("inventory", directory, event_key)[-1]["inventory"]

        def event(process, name, timeout=45, hold_id=None):
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)
            buffered = event_buffers.pop(process, b"")
            deadline = time.monotonic() + timeout
            try:
                while time.monotonic() < deadline:
                    if b"\n" not in buffered:
                        selected = selector.select(timeout=1)
                        if not selected:
                            continue
                        chunk = os.read(selected[0][0].fileobj.fileno(), 4096)
                        if not chunk:
                            raise RuntimeError("process exited before checkpoint")
                        buffered += chunk
                    while b"\n" in buffered:
                        line, buffered = buffered.split(b"\n", 1)
                        value = json.loads(line)
                        if value.get("event") == name:
                            if hold_id is None or value["progress"]["ticket"]["id"] == hold_id:
                                event_buffers[process] = buffered
                                return value
                raise TimeoutError(f"no {name} checkpoint")
            finally:
                selector.close()

        def records(event_key, directory=state):
            page = run("list", directory, event_key, "-", "1")[-1]["page"]
            rows = page["holds"]
            while page["next"] is not None:
                page = run("list", directory, event_key, page["next"], "1")[-1]["page"]
                rows.extend(page["holds"])
            assert [r["ticket"]["id"] for r in rows] == sorted(r["ticket"]["id"] for r in rows)
            a = inventory(event_key, directory)
            assert a["history_count"] == len(rows)
            assert a["held"] == sum(r["state"] == "held" for r in rows)
            assert a["confirmed"] == sum(r["state"] == "confirmed" for r in rows)
            assert a["seats"] == a["available"] + a["held"] + a["confirmed"]
            occupied = [r["ticket"]["seat"] for r in rows if r["state"] in {"held", "confirmed"}]
            assert len(set(occupied)) == len(occupied)
            return rows

        def serve(event_key, delay=0):
            nonlocal active
            active = subprocess.Popen([binary, "serve", str(state), event_key, "90", str(delay), "keep"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            event(active, "ready")
            return active

        def stop():
            nonlocal active
            active.send_signal(signal.SIGTERM)
            _, stderr = active.communicate(timeout=30)
            assert active.returncode == 0, stderr.decode()
            active = None

        try:
            event_key = f"event-{uuid.uuid4().hex}"
            other = f"event-{uuid.uuid4().hex}"
            assert inventory(event_key) is None
            apply(event_key, "initialize", seats=3)
            apply(other, "initialize", seats=1)
            ids = [str(uuid.uuid4()), str(uuid.uuid4())]
            deadline = int(time.time() * 1000) + 180000
            contenders = [mutation(event_key, "hold", id=identity, seat=1, buyer=buyer, deadline_ms=deadline) for identity, buyer in zip(ids, ["alice", "bob"])]
            results = run("race", state, *contenders)[-1]["results"]
            assert sum(not r["rejected"] for r in results) == 1
            winner = next(r["outcome"]["hold"] for r in results if not r["rejected"])
            losing_index = next(i for i, r in enumerate(results) if r["rejected"])
            assert results[losing_index]["outcome"]["decision"] == "occupied"
            confirmation = mutation(event_key, "confirm", id=winner["ticket"]["id"], generation=1, buyer=winner["buyer"])
            committed = run("apply", state, confirmation)[-1]
            assert committed["outcome"]["decision"] == "confirmed"
            assert run("apply", state, confirmation)[-1] == committed
            assert apply(event_key, "confirm", id=winner["ticket"]["id"], generation=1, buyer=winner["buyer"])["outcome"]["decision"] == "already_confirmed"
            assert run("apply", state, contenders[losing_index], success=False)[-1]["outcome"]["decision"] == "occupied"
            assert inventory(other)["available"] == 1

            expiring = str(uuid.uuid4())
            original = mutation(event_key, "hold", id=expiring, seat=2, buyer="alice", deadline_ms=int(time.time() * 1000) + 2000)
            original_output = run("apply", state, original)[-1]
            serve(event_key, delay=10000)
            checkpoint = event(active, "expiration_published", hold_id=expiring)
            assert checkpoint["progress"]["outcome"] == "expired"
            active.kill()
            active.wait(timeout=10)
            active.communicate(timeout=10)
            active = None
            orphaned = {str(path.relative_to(state)): path.read_bytes() for path in state.rglob("*") if path.is_file()}
            assert orphaned
            # Enrollment fencing must hold until the native lease expires.
            run("inventory", cold, event_key, success=False)
            time.sleep(32)
            resolved = run("resolve", state, original)[-1]
            assert resolved["resolution"] == "committed" and resolved["outcome"] == original_output["outcome"]
            assert read_hold(event_key, expiring)["state"] == "expired"
            newer = str(uuid.uuid4())
            replacement = apply(event_key, "hold", id=newer, seat=2, buyer="alice", deadline_ms=int(time.time() * 1000) + 60000)["outcome"]["hold"]
            assert replacement["ticket"]["generation"] == 2
            # Recovery resolves or redelivers the original expiration inbox. Its
            # historical outcome cannot release this newer generation.
            serve(event_key)
            time.sleep(1)
            stop()
            assert read_hold(event_key, newer)["state"] == "held"
            apply(event_key, "confirm", id=newer, generation=2, buyer="alice")

            old = str(uuid.uuid4())
            old_deadline = int(time.time() * 1000) + 4000
            apply(event_key, "hold", id=old, seat=3, buyer="alice", deadline_ms=old_deadline)
            apply(event_key, "cancel", id=old, generation=1, buyer="alice")
            newest = str(uuid.uuid4())
            latest = apply(event_key, "hold", id=newest, seat=3, buyer="alice", deadline_ms=int(time.time() * 1000) + 60000)["outcome"]["hold"]
            assert latest["ticket"]["generation"] == 2
            serve(event_key)
            stale = event(active, "expiration_published", hold_id=old)
            assert stale["progress"]["outcome"] == "unchanged"
            stop()
            assert read_hold(event_key, newest)["state"] == "held"
            apply(event_key, "confirm", id=newest, generation=2, buyer="alice")
            # Late original confirmation is durably rejected after expiration.
            expired_confirmation = mutation(event_key, "confirm", id=expiring, generation=1, buyer="alice")
            rejection = run("apply", state, expired_confirmation, success=False)[-1]
            assert rejection["outcome"]["decision"] == "expired"
            assert run("apply", state, expired_confirmation, success=False)[-1] == rejection
            final = inventory(event_key)
            assert final["confirmed"] == 3 and final["held"] == 0 and final["available"] == 0
            rows = records(event_key)
            assert records(event_key, cold) == rows
            assert inventory(event_key, cold) == final
            assert all((state / relative).read_bytes() == data for relative, data in orphaned.items())
            assert not list(cold.iterdir())
            print(json.dumps({"scenario": "passed", "event_key": event_key, "inventory": final, "checkpoint": checkpoint["progress"], "checks": ["same-owner-seat-contention", "retained-confirmation-replay", "fresh-confirmation-idempotency", "durable-occupied-rejection", "event-isolation", "autonomous-workflow-expiration", "sigkill-after-expiration-publication", "live-owner-refusal", "lease-expiry-takeover", "original-hold-resolution", "permanent-expired-hold", "generation-fenced-recovery", "old-timeout-noop", "late-confirmation-rejection", "bounded-coherent-pages", "independent-cold-restore", "orphan-session-preservation", "sigterm-drain"]}))
        finally:
            if active is not None and active.poll() is None:
                active.kill()
                active.communicate(timeout=15)


if __name__ == "__main__":
    main()
