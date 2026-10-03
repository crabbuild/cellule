#!/usr/bin/env python3
"""Resource creation crash, cancellation, cleanup retry, review, and cold restore.

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
            raise RuntimeError(f"provisioning process drain failed: {self.output}")

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)


def main():
    binary = str(Path(sys.argv[1]).resolve())
    active, receiver = None, None
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="cellule-provisioning-") as temporary:
        root = Path(temporary)
        state, provider_state = root / "source", root / "provider"
        fixture = root / "provider-fault.txt"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}/"
        token = os.environ.get("CELLULE_PROVISIONING_PROVIDER_TOKEN", "cookbook-local-provider")

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

        def external(identifier, authorized=True):
            request = urllib.request.Request(endpoint + "resource/" + identifier,
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

        def settle(identifier, phase):
            deadline = time.monotonic() + 120
            observed = []
            while time.monotonic() < deadline:
                observed += run("serve", state, 15)
                flow = read("workflow", identifier)
                if flow and flow["state"]["phase"] == phase:
                    return flow, observed
            raise AssertionError(f"Workflow did not reach {phase}: {read('workflow', identifier)}")

        def listing():
            records, after, positions = {}, 0, []
            for _ in range(128):
                page = run("resources", state, after, 2)[-1]["resources"]
                assert len(page["resources"]) <= 2
                for position, record in page["resources"]:
                    assert position > after and record["spec"]["id"] not in records
                    positions.append(position)
                    records[record["spec"]["id"]] = record
                if page["next"] is None:
                    assert positions == sorted(set(positions))
                    return records
                assert page["resources"] and page["next"] == page["resources"][-1][0]
                after = page["next"]
            raise AssertionError("directory traversal exceeded permanent identity bound")

        def make():
            return {"id": str(uuid.uuid4()), "name": "scenario-volume", "capacity_mib": 32,
                    "provider_endpoint": endpoint}

        try:
            run("provider-state", fixture, "drop-create-reply")
            receiver = Process(binary, ["provider-server", provider_state, port, fixture, 600])
            receiver.checkpoint("provider_ready")
            baseline = listing()
            results = []
            for cancel in [False, True]:
                spec = make()
                identifier = spec["id"]
                retained = prepare({"operation": "request", "spec": spec})
                assert run("resolve", state, retained)[-1]["resolution"] == "absent"
                original = run("apply", state, retained)[-1]
                assert run("apply", state, retained)[-1] == original
                active = Process(binary, ["serve", state, 600],
                    env={**os.environ, "CELLULE_PROVISIONING_AFTER_CREATE_MS": "10000"})
                active.checkpoint("ready")
                published = active.checkpoint("resource_created", lambda record: record["resource"] == identifier)
                assert published["attempt"] == 1
                created = external(identifier)
                assert created["spec"] == spec and created["creates"] == 1 and created["deletes"] == 0
                active.finish(crash=True)
                active = None
                # Independently expires writer ownership and the 30s native lease.
                time.sleep(32)
                before = read("workflow", identifier)
                assert before["state"]["phase"] == "Creating"
                assert before["state"]["activity"] == published["activity"]
                if cancel:
                    deletion = prepare({"operation": "delete", "resource": identifier})
                    assert run("apply", state, deletion)[-1]["outcome"] == "DeletionRequested"
                flow, events = settle(identifier, "Completed" if cancel else "Active")
                retried = [event for event in events if event.get("event") == "resource_created" and event["resource"] == identifier]
                assert len(retried) == 1 and retried[0]["activity"] == published["activity"] and retried[0]["attempt"] == 2
                assert flow["run_id"] == before["run_id"]
                if not cancel:
                    assert flow["status"] == "running"
                    assert read("resource", identifier)["status"] == "Active"
                    run("provider-state", fixture, "fail-delete-once")
                    deletion = prepare({"operation": "delete", "resource": identifier})
                    run("apply", state, deletion)
                    flow, _ = settle(identifier, "Completed")
                    assert flow["state"]["cleanup_attempts"] >= 2
                terminal = read("resource", identifier)
                assert terminal["status"] == "Deleted" and terminal["delete_requested"]
                deleted = external(identifier)
                assert deleted["resource_id"] == created["resource_id"]
                assert (deleted["creates"], deleted["deletes"], deleted["phase"]) == (1, 1, "Deleted")
                assert run("resolve", state, retained)[-1]["commit_sequence"] == original["receipt"]["commit_sequence"]
                replay = prepare({"operation": "request", "spec": spec})
                assert run("apply", state, replay)[-1]["outcome"] == original["outcome"]
                results.append({"resource": terminal, "provider": deleted, "workflow": flow,
                                "activity": published["activity"], "native_attempts": [1, 2]})

            # Automatic uncertainty remains visible until an explicit retained token.
            run("provider-state", fixture, "down")
            spec = make()
            reviewed = spec["id"]
            retained = prepare({"operation": "request", "spec": spec})
            run("apply", state, retained)
            flow, _ = settle(reviewed, "NeedsReview")
            assert flow["status"] == "running" and flow["state"]["provider_attempts"] == 8
            assert read("resource", reviewed)["status"] == "NeedsReview"
            run("provider-state", fixture, "up")
            assert external(reviewed) is None
            reconciliation = prepare({"operation": "reconcile", "input": {"resource": reviewed, "token": str(uuid.uuid4())}})
            original = run("apply", state, reconciliation)[-1]
            assert run("apply", state, reconciliation)[-1] == original
            flow, _ = settle(reviewed, "Active")
            assert flow["state"]["reconciliations"] == 1
            deletion = prepare({"operation": "delete", "resource": reviewed})
            run("apply", state, deletion)
            flow, _ = settle(reviewed, "Completed")
            assert (external(reviewed)["creates"], external(reviewed)["deletes"]) == (1, 1)
            terminal = {result["resource"]["spec"]["id"]: result["resource"] for result in results}
            terminal[reviewed] = read("resource", reviewed)
            assert listing() == {**baseline, **terminal}
            external(reviewed, authorized=False)
            receiver.finish()
            receiver = None
            provider_records = {identifier: run("provider", provider_state, identifier)[-1]["provider"] for identifier in terminal}
            shutil.rmtree(state)
            shutil.rmtree(provider_state)
            for identifier, expected in terminal.items():
                assert read("resource", identifier) == expected
                assert run("provider", provider_state, identifier)[-1]["provider"] == provider_records[identifier]
                assert read("workflow", identifier)["state"]["phase"] == "Completed"
            assert listing() == {**baseline, **terminal}
            assert run("resolve", state, retained)[-1]["resolution"] == "committed"
            expired = json.loads(retained.read_text())
            expired["issued_at_ms"], expired["expires_at_ms"] = 1, 2
            expiry = root / "expired.json"
            expiry.write_text(json.dumps(expired))
            untouched = root / "never-created"
            assert run("resolve", untouched, expiry)[-1] == {"resolution": "expired", "absence_proven": False}
            assert not untouched.exists()
            print(json.dumps({"scenario": "passed", "elapsed_seconds": round(time.monotonic() - started, 3),
                "results": results, "review_resource": reviewed,
                "checks": ["retained-request-noclobber", "original-native-receipt", "actual-lost-create-reply",
                    "crash-after-creation", "same-native-action-reclaimed", "active-lifetime-recovery",
                    "cancellation-recovery", "create-once", "delete-once", "cleanup-retry", "same-native-run",
                    "bounded-provider-uncertainty", "manual-same-key-reconciliation", "permanent-request-replay",
                    "bounded-keyset-pages", "persistent-baseline-preserved", "provider-credential", "owned-drain",
                    "cold-directory-restore", "cold-provider-restore", "cold-workflow-restore", "expired-offline-resolution"]}))
        finally:
            if active is not None:
                active.close()
            if receiver is not None:
                receiver.close()


if __name__ == "__main__":
    main()
