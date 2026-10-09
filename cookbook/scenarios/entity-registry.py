#!/usr/bin/env python3
"""Persistent device ownership, pending projection, and crash during delivery.

Run against a built isolated source snapshot and private local S3 installation.
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
    with tempfile.TemporaryDirectory(prefix="cellule-entity-registry-") as temporary:
        root = Path(temporary)
        state = root / "writer-a"
        successor = root / "writer-b"
        active = None

        def run(*args, success=True):
            result = subprocess.run([binary, *map(str, args)], capture_output=True, text=True, timeout=75)
            if success and result.returncode:
                raise RuntimeError(f"{args}: {result.stderr}")
            if not success and result.returncode == 0:
                raise AssertionError(f"{args}: unexpectedly succeeded")
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def mutation(key, revision, name):
            token = uuid.uuid4().hex
            source = root / f"{token}.input.json"
            retained = root / f"{token}.mutation.json"
            source.write_text(json.dumps({"key": key, "expected_revision": revision, "attributes": {"name": name, "location": "Lab", "enabled": True}}))
            run("prepare", source, retained)
            return retained

        def read_event(process, event_name, timeout=45):
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)
            buffered = b""
            deadline = time.monotonic() + timeout
            try:
                while time.monotonic() < deadline:
                    for selected, _ in selector.select(timeout=1):
                        chunk = os.read(selected.fileobj.fileno(), 4096)
                        if not chunk:
                            raise RuntimeError("server exited before requested checkpoint")
                        buffered += chunk
                        while b"\n" in buffered:
                            line, buffered = buffered.split(b"\n", 1)
                            event = json.loads(line)
                            if event.get("event") == event_name:
                                return event
                raise TimeoutError(f"no {event_name} checkpoint")
            finally:
                selector.close()

        try:
            keys = [f"device-{uuid.uuid4().hex}" for _ in range(3)]
            roster = root / "roster.json"
            roster.write_text(json.dumps(keys))
            retained = mutation(keys[0], 0, "Original")
            assert run("resolve", state, retained)[-1]["resolution"] == "absent"
            sent = run("apply", state, retained)[-1]
            # Intentionally discard a subprocess's returned body, then resolve frozen evidence.
            assert run("resolve", successor, retained)[-1]["outcome"] == sent["outcome"]
            assert run("apply", successor, retained)[-1] == sent
            first = run("get", state, keys[0])[-1]
            second = run("get", successor, keys[0])[-1]
            assert first["ownership"]["cell"] == second["ownership"]["cell"]
            assert first["ownership"]["incarnation"] == second["ownership"]["incarnation"]
            assert first["ownership"]["owner_session"] != second["ownership"]["owner_session"]
            assert first["ownership"]["epoch"] < second["ownership"]["epoch"]
            assert run("progress", state, keys[0])[-1]["device"]["state"] == "pending"
            assert run("lookup", state, keys[0])[-1]["device"] is None
            update = mutation(keys[0], 1, "Renamed")
            updated = run("apply", successor, update)[-1]
            assert updated["outcome"]["published"]["device"]["revision"] == 2
            conflict = mutation(keys[0], 1, "Stale editor")
            rejected = run("apply", state, conflict, success=False)[-1]
            assert rejected["outcome"]["status"] == "conflict"
            assert run("apply", successor, conflict, success=False)[-1] == rejected
            for key in keys[1:]:
                run("apply", state, mutation(key, 0, key))

            # Crash after actual receiver publication and before the source observes its reply.
            errors_path = root / "crash.stderr"
            with errors_path.open("w") as errors:
                active = subprocess.Popen([binary, "serve", str(state), str(roster), "60", "10000"], stdout=subprocess.PIPE, stderr=errors, bufsize=0)
                checkpoint = read_event(active, "destination_published")["progress"]
                active.kill()
                active.wait(timeout=10)
                active.stdout.close()
                active = None
            orphan_files = list(state.rglob("*.sqlite"))
            assert orphan_files, "crash must retain interrupted local evidence"
            run("get", successor, checkpoint["key"], success=False)
            time.sleep(32)  # Native enrollment lease expires; never weaken expiry proof.
            run("serve", successor, roster, "4", "0", "lose-reply")
            for key in keys:
                progress = run("progress", successor, key)[-1]["device"]
                projected = run("lookup", successor, key)[-1]["device"]
                assert progress["state"] == "delivered", progress
                assert projected == progress["published"]["device"], (projected, progress)
            assert all(path.exists() for path in orphan_files), "successor must preserve other boot sessions"
            page = run("list", successor, "-", "1")[-1]["page"]
            listed = page["devices"]
            while page["next"] is not None:
                page = run("list", successor, page["next"], "1")[-1]["page"]
                listed.extend(page["devices"])
            assert [v["key"] for v in listed] == sorted(v["key"] for v in listed)
            assert set(keys).issubset({v["key"] for v in listed})
            assert len({v["key"] for v in listed}) == len(listed)
            for invalid_limit in [0, 101]:
                run("list", successor, "-", invalid_limit, success=False)

            # Graceful cancellation settles accepted work and deletes only its own working session.
            graceful = root / "graceful"
            with (root / "graceful.stderr").open("w") as errors:
                active = subprocess.Popen([binary, "serve", str(graceful), str(roster), "60"], stdout=subprocess.PIPE, stderr=errors, bufsize=0)
                read_event(active, "ready")
                active.send_signal(signal.SIGTERM)
                active.wait(timeout=30)
                assert active.returncode == 0, (root / "graceful.stderr").read_text()
                active.stdout.close()
                active = None
            assert not list(graceful.rglob("*.sqlite"))
            cold = root / "cold"
            for key in keys:
                assert run("get", cold, key)[-1]["device"]["device"] == run("lookup", cold, key)[-1]["device"]
            assert run("apply", cold, retained)[-1] == sent
            assert run("resolve", cold, update)[-1]["outcome"] == updated["outcome"]
            assert not list(cold.rglob("*.sqlite"))
            print(json.dumps({"scenario": "passed", "devices": keys, "checkpoint": checkpoint, "checks": ["frozen-evidence-resolution", "stable-target-owner-session-epoch-change", "delayed-projection-pending", "conditional-update-conflict", "crash-after-destination-publication", "live-owner-refusal", "lease-expiry-takeover", "lost-reply-inbox-resolution", "monotonic-directory-convergence", "bounded-keyset-pages", "graceful-drain", "independent-cold-restore", "orphan-session-preservation"]}))
        finally:
            if active is not None:
                active.kill()
                active.wait(timeout=10)
                if active.stdout:
                    active.stdout.close()


if __name__ == "__main__":
    main()
