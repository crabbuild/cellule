#!/usr/bin/env python3
"""Payment publication crash, cancellation compensation, operator review, and cold restore.

Run only in CI or an isolated source snapshot against private authoritative storage.
"""
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid


class Process:
    def __init__(self, binary, arguments, env=None):
        self.process = subprocess.Popen([binary, *map(str, arguments)], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, bufsize=0, env=env)
        self.output = {"stdout": bytearray(), "stderr": bytearray()}
        self.pending = b""

    def checkpoint(self, event, predicate=lambda _: True):
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(self.process.stderr, selectors.EVENT_READ, "stderr")
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                while b"\n" in self.pending:
                    line, self.pending = self.pending.split(b"\n", 1)
                    record = json.loads(line)
                    if record.get("event") == event and predicate(record):
                        return record
                for selected, _ in selector.select(timeout=1):
                    chunk = os.read(selected.fileobj.fileno(), 4096)
                    if not chunk:
                        selector.unregister(selected.fileobj)
                        if not selector.get_map():
                            raise RuntimeError(f"process exited before {event}: {self.output}")
                        continue
                    self.output[selected.data].extend(chunk)
                    if sum(map(len, self.output.values())) > 524288:
                        raise RuntimeError("process diagnostics exceeded 512 KiB")
                    if selected.data == "stdout":
                        self.pending += chunk
            raise AssertionError(f"checkpoint {event} absent: {self.output}")

    def finish(self, crash=False):
        if crash:
            self.process.kill()
        else:
            self.process.send_signal(signal.SIGTERM)
        output, errors = self.process.communicate(timeout=25)
        self.output["stdout"].extend(output)
        self.output["stderr"].extend(errors)
        if not crash and self.process.returncode:
            raise RuntimeError(f"checkout process drain failed: {self.output}")

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)


def main():
    binary = str(Path(sys.argv[1]).resolve())
    active, receiver = None, None
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="cellule-checkout-") as temporary:
        root = Path(temporary)
        state, payment_state = root / "checkout", root / "payments"
        fixture = root / "payment-fault.txt"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}/"
        token = os.environ.get("CELLULE_CHECKOUT_PAYMENT_TOKEN", "cookbook-local-payment")

        def run(*arguments, success=True, env=None):
            result = subprocess.run([binary, *map(str, arguments)], capture_output=True,
                                    text=True, timeout=75, env=env)
            if bool(result.returncode == 0) != success:
                raise RuntimeError(f"{arguments}: exit {result.returncode}; {result.stderr}; {result.stdout}")
            assert len(result.stdout) + len(result.stderr) <= 524288
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def prepare(value):
            name = uuid.uuid4().hex
            change, retained = root / (name + ".json"), root / (name + ".request.json")
            change.write_text(json.dumps(value))
            run("prepare", change, retained)
            run("prepare", change, retained, success=False)
            return retained

        def payment(identifier, authorized=True):
            request = urllib.request.Request(endpoint + "payment/" + identifier,
                                             headers={"Authorization": "Bearer " + token} if authorized else {})
            try:
                response = urllib.request.urlopen(request, timeout=5)
            except urllib.error.HTTPError as response:
                assert not authorized and response.code == 401
                return None
            with response:
                assert response.status == 200
                body = response.read(4097)
                assert len(body) <= 4096
                return json.loads(body)

        def read(op, identifier):
            return run(op, state, identifier)[-1][op]

        def settle(identifier, expected_phase="Completed"):
            deadline = time.monotonic() + 120
            observed = []
            while time.monotonic() < deadline:
                observed += run("serve", state, 15)
                saga = read("saga", identifier)
                if saga and saga["state"]["phase"] == expected_phase:
                    return saga, observed
            raise AssertionError(f"saga did not reach {expected_phase}: {read('saga', identifier)}")

        def listing():
            records, after, positions = {}, 0, []
            for _ in range(128):
                page = run("orders", state, after, 2)[-1]["orders"]
                assert len(page["orders"]) <= 2
                for position, record in page["orders"]:
                    assert position > after and record["spec"]["id"] not in records
                    positions.append(position)
                    records[record["spec"]["id"]] = record
                if page["next"] is None:
                    assert positions == sorted(set(positions))
                    return records
                assert page["orders"] and page["next"] == page["orders"][-1][0]
                after = page["next"]
            raise AssertionError("order traversal exceeded the permanent identity bound")

        def order_spec(policy="Approve", sku="widget"):
            return {"id": str(uuid.uuid4()), "sku": sku, "quantity": 1, "amount": 125,
                    "payment_endpoint": endpoint, "payment_policy": policy}

        try:
            run("payment-state", fixture, "drop-authorize-reply")
            receiver = Process(binary, ["payment-server", payment_state, port, fixture, 600])
            receiver.checkpoint("payment_ready")
            seed = prepare({"operation": "seed", "seed": {"sku": "widget", "units": 100}})
            initial = run("apply", state, seed)[-1]
            assert run("apply", state, seed)[-1] == initial
            assert run("resolve", state, seed)[-1]["resolution"] == "committed"
            baseline_orders = listing()
            baseline_stock = run("stock", state, "widget")[-1]["stock"]
            assert baseline_stock["held"] == 0 and baseline_stock["available"] >= 2
            results = []
            for cancel in [False, True]:
                spec = order_spec()
                identifier = spec["id"]
                retained = prepare({"operation": "place", "spec": spec})
                assert run("resolve", state, retained)[-1]["resolution"] == "absent"
                original = run("apply", state, retained)[-1]
                assert run("apply", state, retained)[-1] == original
                active = Process(binary, ["serve", state, 600],
                                 env={**os.environ, "CELLULE_CHECKOUT_AFTER_AUTHORIZATION_MS": "10000"})
                active.checkpoint("ready")
                published = active.checkpoint("payment_authorized", lambda record: record["order"] == identifier)
                assert published["attempt"] == 1
                external = payment(identifier)
                assert external["spec"] == spec and external["authorizations"] == 1 and external["voids"] == 0
                active.finish(crash=True)
                active = None
                # Independently expires both writer ownership and the 30s native
                # Activity lease. Recovery must reclaim the same native action.
                time.sleep(32)
                before = read("saga", identifier)
                assert before["state"]["phase"] == "Authorizing"
                assert before["state"]["activity"] == published["activity"]
                if cancel:
                    cancellation = prepare({"operation": "cancel", "order": identifier})
                    assert run("apply", state, cancellation)[-1]["outcome"] == "CancellationRequested"
                saga, events = settle(identifier)
                retry = [event for event in events if event.get("event") == "payment_authorized" and event["order"] == identifier]
                assert len(retry) == 1 and retry[0]["activity"] == published["activity"] and retry[0]["attempt"] == 2
                assert saga["run_id"] == before["run_id"]
                expected = "Cancelled" if cancel else "Fulfilled"
                assert saga["state"]["result"] == expected
                terminal = read("order", identifier)
                assert terminal["status"] == {"Finished": expected}
                allocation = read("reservation", identifier)
                assert allocation["status"] == ("Released" if cancel else "Committed")
                external = payment(identifier)
                assert external["authorizations"] == 1 and external["voids"] == int(cancel)
                assert external["status"] == ("Voided" if cancel else "Authorized")
                assert run("resolve", state, retained)[-1]["commit_sequence"] == original["receipt"]["commit_sequence"]
                replay = prepare({"operation": "place", "spec": spec})
                assert run("apply", state, replay)[-1]["outcome"] == original["outcome"]
                results.append({"order": terminal, "reservation": allocation, "payment": external,
                                "activity": published["activity"], "native_attempts": [1, 2]})

            # An actual HTTP outage leaves held stock and no claim of absent payment.
            run("payment-state", fixture, "down")
            review_spec = order_spec()
            reviewed = review_spec["id"]
            retained = prepare({"operation": "place", "spec": review_spec})
            run("apply", state, retained)
            saga, _ = settle(reviewed, "NeedsReview")
            assert saga["status"] == "running" and saga["state"]["result"] is None
            assert read("order", reviewed)["status"] == "NeedsReview"
            assert read("reservation", reviewed)["status"] == "Held"
            run("payment-state", fixture, "up")
            reconciliation = prepare({"operation": "reconcile", "input": {"order": reviewed, "token": str(uuid.uuid4())}})
            reconciled = run("apply", state, reconciliation)[-1]
            assert run("apply", state, reconciliation)[-1] == reconciled
            saga, _ = settle(reviewed)
            assert saga["state"]["reconciliations"] == 1 and saga["state"]["result"] == "Fulfilled"
            assert payment(reviewed)["authorizations"] == 1

            terminal_records = {result["order"]["spec"]["id"]: result["order"] for result in results}
            terminal_records[reviewed] = read("order", reviewed)
            assert listing() == {**baseline_orders, **terminal_records}
            stock = run("stock", state, "widget")[-1]["stock"]
            assert stock["total"] == baseline_stock["total"]
            assert (stock["available"], stock["held"], stock["sold"]) == (baseline_stock["available"] - 2, 0, baseline_stock["sold"] + 2)
            run("apply", state, seed)
            assert run("stock", state, "widget")[-1]["stock"] == stock
            payment(next(iter(terminal_records)), authorized=False)
            # Drained, unowned working directories can be discarded; authority and
            # external business records must restore exactly from retained objects.
            receiver.finish()
            receiver = None
            external_records = {identifier: run("payment", payment_state, identifier)[-1]["payment"] for identifier in terminal_records}
            shutil.rmtree(state)
            shutil.rmtree(payment_state)
            for identifier, expected in terminal_records.items():
                assert read("order", identifier) == expected
                assert run("payment", payment_state, identifier)[-1]["payment"] == external_records[identifier]
            assert run("stock", state, "widget")[-1]["stock"] == stock
            assert listing() == {**baseline_orders, **terminal_records}
            assert run("resolve", state, retained)[-1]["resolution"] == "committed"
            expired = json.loads(retained.read_text())
            expired["issued_at_ms"] = 1
            expired["expires_at_ms"] = 2
            expiry = root / "expired.json"
            expiry.write_text(json.dumps(expired))
            untouched = root / "never-created"
            assert run("resolve", untouched, expiry)[-1] == {"resolution": "expired", "absence_proven": False}
            assert not untouched.exists()
            print(json.dumps({"scenario": "passed", "elapsed_seconds": round(time.monotonic() - started, 3),
                              "results": results, "review_order": reviewed, "stock": stock,
                              "checks": ["retained-request-noclobber", "original-native-receipt", "actual-lost-payment-reply",
                                         "crash-after-authorization", "same-native-action-reclaimed", "fulfillment-recovery",
                                         "cancellation-recovery", "authorization-once", "void-once", "release-once",
                                         "same-native-run", "explicit-external-uncertainty", "held-stock-during-review",
                                         "manual-same-key-reconciliation", "permanent-placement-replay", "bounded-keyset-pages",
                                         "seed-replay-does-not-replenish", "persistent-baseline-preserved", "capacity-conservation", "payment-credential", "owned-drain", "cold-order-restore",
                                         "cold-payment-restore", "expired-offline-resolution"]}))
        finally:
            if active is not None:
                active.close()
            if receiver is not None:
                receiver.close()


if __name__ == "__main__":
    main()
