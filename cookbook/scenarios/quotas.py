#!/usr/bin/env python3
"""Persistent customer accounting and recovery after release publication.

Run a built isolated snapshot against private local S3 storage, never a live installation.
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
    with tempfile.TemporaryDirectory(prefix="cellule-quotas-") as temporary:
        root = Path(temporary)
        state = root / "state"
        cold = root / "cold"
        active = None

        def run(*args, success=True):
            result = subprocess.run([binary, *map(str, args)], capture_output=True, text=True, timeout=75)
            if success and result.returncode:
                raise RuntimeError(f"{args}: {result.stderr}")
            if not success and result.returncode == 0:
                raise AssertionError(f"{args}: unexpectedly succeeded")
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def mutation(customer, operation, **fields):
            token = uuid.uuid4().hex
            source = root / f"{token}.input.json"
            retained = root / f"{token}.mutation.json"
            source.write_text(json.dumps({"customer": customer, "action": {"operation": operation, **fields}}))
            run("prepare", source, retained)
            return retained

        def apply(customer, operation, **fields):
            return run("apply", state, mutation(customer, operation, **fields))[-1]

        def account(customer, directory=state):
            return run("account", directory, customer)[-1]["account"]

        def event(process, name, timeout=45):
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)
            buffered = b""
            deadline = time.monotonic() + timeout
            try:
                while time.monotonic() < deadline:
                    for selected, _ in selector.select(timeout=1):
                        chunk = os.read(selected.fileobj.fileno(), 4096)
                        if not chunk:
                            raise RuntimeError("process exited before checkpoint")
                        buffered += chunk
                        while b"\n" in buffered:
                            line, buffered = buffered.split(b"\n", 1)
                            value = json.loads(line)
                            if value.get("event") == name:
                                return value
                raise TimeoutError(f"no {name} checkpoint")
            finally:
                selector.close()

        def records(customer):
            page = run("list", state, customer, "-", "1")[-1]["page"]
            rows = page["reservations"]
            while page["next"] is not None:
                page = run("list", state, customer, page["next"], "1")[-1]["page"]
                rows.extend(page["reservations"])
            assert [r["id"] for r in rows] == sorted(r["id"] for r in rows)
            assert len({r["id"] for r in rows}) == len(rows)
            a = account(customer)
            assert a["reservation_count"] == len(rows)
            assert a["reserved"] == sum(r["credits"] for r in rows if r["state"] == "active")
            assert a["consumed"] == sum(r["credits"] for r in rows if r["state"] == "consumed")
            assert a["allowance"] == a["reserved"] + a["consumed"] + a["available"]
            return rows

        try:
            customer = f"customer-{uuid.uuid4().hex}"
            other = f"customer-{uuid.uuid4().hex}"
            assert account(customer) is None
            apply(customer, "open", allowance=100)
            apply(other, "open", allowance=40)
            ids = [str(uuid.uuid4()), str(uuid.uuid4())]
            retained = [mutation(customer, "reserve", id=identity, credits=75) for identity in ids]
            raced = run("race", state, *retained)[-1]["results"]
            assert sorted(r["outcome"]["decision"] for r in raced) == ["insufficient", "reserved"]
            winner = next(i for i, r in enumerate(raced) if not r["rejected"])
            loser = 1 - winner
            assert account(customer)["reserved"] == 75
            rejected = run("apply", state, retained[loser], success=False)[-1]
            assert rejected["outcome"]["decision"] == "insufficient"
            assert rejected["receipt"] == raced[loser]["receipt"]
            # Same reservation UUID belongs independently to another customer's Cell.
            apply(other, "reserve", id=ids[winner], credits=30)
            assert account(other)["reserved"] == 30

            release = mutation(customer, "release", id=ids[winner])
            errors_path = root / "release-crash.stderr"
            with errors_path.open("w") as errors:
                active = subprocess.Popen([binary, "apply", str(state), str(release), "10000"], stdout=subprocess.PIPE, stderr=errors, bufsize=0)
                published = event(active, "published")
                assert published["outcome"]["decision"] == "released"
                assert published["outcome"]["account"]["available"] == 100
                active.kill()
                active.wait(timeout=10)
                active.stdout.close()
                active = None
            orphans = list(state.rglob("*.sqlite"))
            assert orphans, "crashed boot must retain its working evidence"
            run("resolve", cold, release, success=False)
            time.sleep(32)  # Honor native enrollment expiry before successor ownership.
            resolved = run("resolve", cold, release)[-1]
            assert resolved["resolution"] == "committed"
            assert resolved["outcome"] == published["outcome"]
            replayed = run("apply", cold, release)[-1]
            assert replayed["outcome"] == published["outcome"]
            assert replayed["receipt"] == published["receipt"]
            fresh = run("apply", cold, mutation(customer, "release", id=ids[winner]))[-1]
            assert fresh["outcome"]["decision"] == "already_released"
            assert fresh["outcome"]["account"] == published["outcome"]["account"]
            assert run("apply", cold, retained[loser], success=False)[-1] == rejected
            old_id = apply(customer, "reserve", id=ids[winner], credits=75)["outcome"]
            assert old_id["decision"] == "existing_reservation"
            assert old_id["reservation"]["state"] == "released"
            assert account(customer)["reserved"] == 0
            changed = mutation(customer, "reserve", id=ids[winner], credits=74)
            assert run("apply", state, changed, success=False)[-1]["outcome"]["decision"] == "conflict"

            consumed = str(uuid.uuid4())
            apply(customer, "reserve", id=consumed, credits=25)
            spent = apply(customer, "consume", id=consumed)["outcome"]
            repeat = apply(customer, "consume", id=consumed)["outcome"]
            assert repeat["decision"] == "already_consumed"
            assert repeat["account"] == spent["account"]
            closed = mutation(customer, "release", id=consumed)
            assert run("apply", state, closed, success=False)[-1]["outcome"]["decision"] == "closed"
            current = account(customer)
            lowering = mutation(customer, "set_allowance", expected_revision=current["revision"], allowance=24)
            assert run("apply", state, lowering, success=False)[-1]["outcome"]["decision"] == "overcommitted"
            apply(customer, "set_allowance", expected_revision=current["revision"], allowance=150)
            assert account(customer)["consumed"] == 25
            assert account(other)["reserved"] == 30
            rows = records(customer)
            assert len(rows) == 2
            for invalid in [0, 101]:
                run("list", state, customer, "-", invalid, success=False)

            # Owned maintenance/enrollment remain live until SIGTERM drains the node.
            graceful = root / "graceful"
            with (root / "graceful.stderr").open("w") as errors:
                active = subprocess.Popen([binary, "serve", str(graceful), customer, "60"], stdout=subprocess.PIPE, stderr=errors, bufsize=0)
                event(active, "ready")
                active.send_signal(signal.SIGTERM)
                active.wait(timeout=30)
                assert active.returncode == 0, (root / "graceful.stderr").read_text()
                active.stdout.close()
                active = None
            assert not list(graceful.rglob("*.sqlite"))
            assert account(customer, cold) == account(customer)
            assert all(path.exists() for path in orphans), "later boots must preserve interrupted evidence"
            assert not list(cold.rglob("*.sqlite"))
            print(json.dumps({"scenario": "passed", "customer": customer, "account": account(customer), "publication_receipt": published["receipt"], "checks": ["same-owner-concurrent-reservations", "durable-insufficient-rejection", "customer-isolation", "sigkill-after-release-publication", "live-owner-refusal", "lease-expiry-takeover", "original-release-resolution", "retained-release-replay", "fresh-release-idempotency", "permanent-business-identities", "consumption-once", "terminal-release-refusal", "allowance-invariants", "bounded-coherent-pages", "independent-cold-restore", "sigterm-drain", "orphan-session-preservation"]}))
        finally:
            if active is not None:
                active.kill()
                active.wait(timeout=10)
                if active.stdout:
                    active.stdout.close()


if __name__ == "__main__":
    main()
