#!/usr/bin/env python3
"""Independent deployment crash, real code rollout, compensation, and cold restore.

Run only in CI or an isolated source snapshot with a fresh private storage installation.
"""
import copy
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
                    chunk = os.read(selected.fileobj.fileno(), 8192)
                    if not chunk:
                        selector.unregister(selected.fileobj)
                        if not selector.get_map():
                            raise RuntimeError(f"process exited before {event}: {self.output}")
                        continue
                    self.output[selected.data].extend(chunk)
                    if sum(map(len, self.output.values())) > 1048576:
                        raise RuntimeError("process diagnostics exceeded one MiB")
                    if selected.data == "stdout":
                        self.pending += chunk
            raise AssertionError(f"checkpoint {event} absent: {self.output}")

    def finish(self, crash=False, interrupted=False):
        if crash:
            self.process.kill()
        else:
            self.process.send_signal(signal.SIGTERM)
        output, errors = self.process.communicate(timeout=25)
        self.output["stdout"].extend(output)
        self.output["stderr"].extend(errors)
        if not crash and self.process.returncode != (1 if interrupted else 0):
            raise RuntimeError(f"release process drain failed: {self.output}")
        if not crash:
            records = [json.loads(line) for line in bytes(self.output["stdout"]).splitlines() if line]
            assert sum(record.get("event") == "drained" for record in records) == (2 if interrupted else 1)
            if interrupted:
                assert b"interrupted; retain the prepared release request" in self.output["stderr"]

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)


def port():
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return reservation.getsockname()[1]


def identifier(value):
    return bytes(value).hex()


def main():
    binary = str(Path(sys.argv[1]).resolve())
    active, target, demo = None, None, None
    started = time.monotonic()
    results = []
    with tempfile.TemporaryDirectory(prefix="cellule-release-pipeline-") as temporary:
        root = Path(temporary)
        state, target_state = root / "flow", root / "target"
        fault = root / "target-fault.txt"
        source = root / "source.bin"
        source.write_bytes(bytes([255]) * 4096)
        target_port, source_port = port(), port()
        target_url, endpoint = f"http://127.0.0.1:{target_port}/", f"http://127.0.0.1:{source_port}/"
        credentials = {
            "reader": os.environ.get("CELLULE_RELEASE_ARTIFACT_TOKEN", "cookbook-local-release"),
            "target": os.environ.get("CELLULE_RELEASE_TARGET_TOKEN", "cookbook-local-release"),
            "submitter": os.environ.get("CELLULE_RELEASE_SUBMITTER_TOKEN", "cookbook-local-submitter"),
            "approver": os.environ.get("CELLULE_RELEASE_APPROVER_TOKEN", "cookbook-local-approver"),
            "operator": os.environ.get("CELLULE_RELEASE_OPERATOR_TOKEN", "cookbook-local-operator"),
        }

        def run(*arguments, success=True):
            value = subprocess.run([binary, *map(str, arguments)], capture_output=True,
                                   text=True, timeout=75)
            if bool(value.returncode == 0) != success:
                raise RuntimeError(f"{arguments}: exit {value.returncode}: {value.stdout}: {value.stderr}")
            assert len(value.stdout) + len(value.stderr) <= 524288
            return [json.loads(line) for line in value.stdout.splitlines() if line]

        def http(url, route, role, value=None, status=200, authorized=True):
            headers = {"Authorization": "Bearer " + credentials[role]} if authorized else {}
            data = None
            if value is not None:
                data = json.dumps(value).encode()
                headers["Content-Type"] = "application/json"
            request = urllib.request.Request(url + route, data=data, headers=headers)
            try:
                response = urllib.request.urlopen(request, timeout=10)
            except urllib.error.HTTPError as response:
                assert response.code == status
                response.close()
                return None
            with response:
                assert response.status == status
                body = response.read(65537)
                assert len(body) <= 65536
                return json.loads(body)

        def read(kind, spec):
            return http(endpoint, f"{kind}/" + identifier(spec["release"]), "reader")[kind]

        def external(spec):
            return http(target_url, "operation/" + identifier(spec["release"]), "target")

        def selection(spec):
            return http(target_url, "target/" + spec["target"], "target")

        def phase(spec, expected):
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                value = read("workflow", spec)
                if value and value["state"]["phase"] == expected:
                    return value
                if value and value["state"]["phase"] == "NeedsReview" and expected != "NeedsReview":
                    raise AssertionError(f"unexpected unresolved release: {value}")
                if active.process.poll() is not None:
                    raise RuntimeError(f"source exited: {active.output}")
                time.sleep(0.1)
            raise AssertionError(f"release did not reach {expected}: {read('workflow', spec)}")

        def make(name=None, generation=0, deadline=300):
            name = name or "p-" + uuid.uuid4().hex[:20]
            retained = root / (uuid.uuid4().hex + ".request.json")
            result = run("prepare-release", source, name, generation, target_url,
                         endpoint, deadline, retained)[-1]
            run("prepare-release", source, name, generation, target_url,
                endpoint, deadline, retained, success=False)
            return retained, result["request"]["input"]["spec"]

        def control(kind, original, vote=None):
            retained = root / (uuid.uuid4().hex + ".request.json")
            if vote is None:
                run("prepare-" + kind, original, retained)
            else:
                run("prepare-" + kind, original, vote, retained)
            return retained

        def send(retained):
            return run("send", endpoint, retained)[-1]

        def activate(retained, spec):
            send(retained)
            phase(spec, "AwaitingApproval")
            vote = control("approval", retained, "approve")
            send(vote)
            return phase(spec, "Active")

        def finish_release(retained, spec, outcome="RolledBack"):
            rollback = control("rollback", retained)
            first = send(rollback)
            settled = phase(spec, "Done")
            assert settled["status"] == "completed"
            assert send(rollback) == first
            assert external(spec)["outcome"] == outcome
            assert read("record", spec)["status"] == outcome
            return settled

        def expected_bytes(spec):
            return (b"CELLULE-RELEASE\x01" + bytes(spec["release"])
                    + len(spec["source"]).to_bytes(4, "big") + bytes(spec["source"]))

        try:
            run("target-fault", fault, "drop-deploy-reply")
            target = Process(binary, ["target-server", target_state, target_port, 600, fault])
            target.checkpoint("target_ready")
            active = Process(binary, ["serve", state, 1, source_port, 600],
                env={**os.environ, "CELLULE_RELEASE_AFTER_DEPLOY_MS": "10000"})
            active.checkpoint("ready")
            http(endpoint, "ready", "reader", status=401, authorized=False)
            original, spec1 = make()
            request = json.loads(original.read_text())
            http(endpoint, "start", "approver", request, status=401)
            absence = http(endpoint, "resolve", "reader", request)
            assert absence == {"resolution": "absent", "absence_proven": True}
            first = send(original)
            assert send(original) == first
            phase(spec1, "AwaitingApproval")
            vote = control("approval", original, "approve")
            http(endpoint, "approve", "submitter", json.loads(vote.read_text()), status=401)
            send(vote)
            checkpoint = active.checkpoint("release_deployed",
                lambda value: value["release"] == spec1["release"])
            assert checkpoint["attempt"] == 1
            applied = external(spec1)
            assert applied["deployment"]["expected_generation"] == 0
            assert (applied["deploys"], applied["rollbacks"]) == (1, 0)
            assert http(target_url, "artifact/" + identifier(spec1["release"]), "target") == list(expected_bytes(spec1))
            active.finish(crash=True)
            active = None
            # This is an intentional native lease-expiry boundary, not a polling timeout restart.
            time.sleep(32)
            before = run("workflow", state, 1, identifier(spec1["release"]))[0]["workflow"]
            assert before["state"]["stage"] == "Deploy" and before["state"]["version"] == 1
            run("workflow", state, 2, identifier(spec1["release"]), success=False)
            run("rollout", state)
            active = Process(binary, ["serve", state, 2, source_port, 600])
            active.checkpoint("ready")
            resumed = active.checkpoint("release_deployed",
                lambda value: value["release"] == spec1["release"])
            assert resumed["activity"] == checkpoint["activity"] and resumed["attempt"] == 2
            old = phase(spec1, "Active")
            assert old["definition"] == before["definition"]
            assert old["state"]["run_id"] == before["state"]["run_id"]
            assert old["state"]["version"] == 1 and not old["state"]["rebuilt"]
            assert external(spec1)["deploys"] == 1
            assert read("record", spec1)["definition_version"] == 1
            results += ["human-capability-authorization", "prepared-start-replay", "lost-deploy-reply",
                        "same-native-Activity-reclaimed", "actual-registry-code-rollout", "old-definition-pinned"]

            active.finish()
            active = None
            shutil.rmtree(state)
            cold = run("workflow", state, 2, identifier(spec1["release"]))[0]["workflow"]
            assert cold == old
            resolved = run("resolve", state, 2, original)[0]
            assert resolved["resolution"] == "committed"
            assert resolved["commit_sequence"] == first["receipt"]["commit_sequence"]
            download = root / "restored-artifact.bin"
            reference = old["state"]["publication"]["artifact"]
            run("download", state, 2, identifier(reference["key"]), download)
            assert download.read_bytes() == expected_bytes(spec1)
            active = Process(binary, ["serve", state, 2, source_port, 600])
            active.checkpoint("ready")
            results += ["cold-Workflow-restore", "original-outcome-resolution", "byte-identical-Blob-restore"]

            run("target-fault", fault, "up")
            second, spec2 = make(spec1["target"], 1)
            new = activate(second, spec2)
            assert new["state"]["version"] == 2 and new["state"]["rebuilt"]
            assert new["definition"] != old["definition"]
            finish_release(second, spec2)
            restored = selection(spec1)
            assert restored["generation"] == 3 and restored["selected"]["release"] == spec1["release"]
            finish_release(original, spec1, "Superseded")
            assert selection(spec1) == restored
            assert external(spec1)["rollbacks"] == 0
            results += ["new-definition-rebuild-required", "captured-predecessor-restored", "ABA-compensation-refused"]

            # Interrupt a separate accepted deployment, then record compensation
            # before reclaiming the old native Activity. Its external result stays bound.
            active.finish()
            active = None
            active = Process(binary, ["serve", state, 2, source_port, 600],
                env={**os.environ, "CELLULE_RELEASE_AFTER_DEPLOY_MS": "10000"})
            active.checkpoint("ready")
            cancelled, spec3 = make()
            send(cancelled)
            phase(spec3, "AwaitingApproval")
            send(control("approval", cancelled, "approve"))
            accepted = active.checkpoint("release_deployed",
                lambda value: value["release"] == spec3["release"])
            assert external(spec3)["deploys"] == 1
            active.finish(crash=True)
            active = None
            time.sleep(32)
            rollback = control("rollback", cancelled)
            run("apply", state, 2, rollback)
            run("target-fault", fault, "fail-rollback-once")
            active = Process(binary, ["serve", state, 2, source_port, 600])
            active.checkpoint("ready")
            reclaimed = active.checkpoint("release_deployed",
                lambda value: value["release"] == spec3["release"])
            assert reclaimed["activity"] == accepted["activity"] and reclaimed["attempt"] == 2
            done = phase(spec3, "Done")
            assert done["status"] == "completed" and done["state"]["rollback_requested"]
            assert (external(spec3)["deploys"], external(spec3)["rollbacks"]) == (1, 1)
            assert external(spec3)["outcome"] == "RolledBack"
            results += ["compensation-during-accepted-deployment", "transient-compensation-retried"]

            run("target-fault", fault, "down")
            reviewed, spec4 = make()
            send(reviewed)
            phase(spec4, "AwaitingApproval")
            send(control("approval", reviewed, "approve"))
            review = phase(spec4, "NeedsReview")
            assert review["state"]["spec"] == spec4 and review["status"] == "running"
            assert read("record", spec4)["status"] == "NeedsReview"
            run("target-fault", fault, "up")
            assert external(spec4) is None
            retry = control("reconcile", reviewed)
            first_retry = send(retry)
            assert send(retry) == first_retry
            current = phase(spec4, "Active")
            assert current["state"]["spec"] == spec4 and current["state"]["reconciliations"] == 1
            changed = copy.deepcopy(json.loads(retry.read_text())["input"])
            changed["input"]["input_digest"][0] ^= 1
            changed_input, changed_request = root / "changed-input.json", root / "changed-request.json"
            changed_input.write_text(json.dumps(changed))
            run("prepare", changed_input, changed_request)
            http(endpoint, "reconcile", "operator", json.loads(changed_request.read_text()), status=409)
            finish_release(reviewed, spec4)
            results += ["bounded-uncertainty-review", "same-input-operator-reconciliation", "token-cannot-rebind-input"]

            early, spec5 = make()
            cancellation = control("rollback", early)
            send(cancellation)
            assert read("workflow", spec5) is None
            send(early)
            phase(spec5, "Done")
            record = read("record", spec5)
            assert record["status"] == "Cancelled" and record["publication"] is None
            assert (external(spec5)["deploys"], external(spec5)["rollbacks"]) == (0, 1)
            assert selection(spec5)["generation"] == 0
            results += ["early-cancel-tombstone-without-build"]

            rejected, spec6 = make()
            send(rejected)
            phase(spec6, "AwaitingApproval")
            send(control("approval", rejected, "reject"))
            phase(spec6, "Done")
            assert not read("workflow", spec6)["state"]["approved"]
            assert external(spec6)["outcome"] == "Cancelled"
            results += ["human-rejection-prevents-installation"]

            timeout, spec7 = make(deadline=4)
            send(timeout)
            initial = read("workflow", spec7)
            assert initial["state"]["spec"]["approval_deadline_ms"] > int(time.time() * 1000)
            phase(spec7, "Done")
            assert external(spec7)["outcome"] == "Cancelled"
            assert external(spec7)["deploys"] == 0
            results += ["autonomous-human-approval-deadline"]

            # Prove a disconnected HTTP caller does not own publication lifetime.
            run("target-fault", fault, "delay-deploy")
            work = root / "drain-work.json"
            drain_id = uuid.uuid4().hex
            run("prepare-target", source, drain_id, "d-" + uuid.uuid4().hex[:20], 0, "deploy", work)
            value = json.loads(work.read_text())
            key = run("target-key", work)[0]["idempotency_key"]
            body = work.read_bytes()
            connection = socket.create_connection(("127.0.0.1", target_port), timeout=5)
            wire = (f"POST /operation HTTP/1.1\r\nHost: 127.0.0.1:{target_port}\r\n"
                    f"Authorization: Bearer {credentials['target']}\r\nContent-Type: application/json\r\n"
                    f"Idempotency-Key: {key}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode()
            connection.sendall(wire + body)
            target.checkpoint("target_accepted", lambda row: identifier(row["release"]) == drain_id)
            connection.close()
            target.finish()
            target = None
            run("target-fault", fault, "up")
            target = Process(binary, ["target-server", target_state, target_port, 600, fault])
            target.checkpoint("target_ready")
            published = http(target_url, "operation/" + drain_id, "target")
            assert published["deployment"] == value["deployment"] and published["deploys"] == 1
            results += ["disconnected-accepted-work-owned", "target-drain-before-publication"]

            target.finish()
            target = None
            shutil.rmtree(target_state)
            target = Process(binary, ["target-server", target_state, target_port, 600, fault])
            target.checkpoint("target_ready")
            assert http(target_url, "operation/" + drain_id, "target") == published
            assert external(spec1)["outcome"] == "Superseded"
            assert selection(spec1) == restored
            assert http(target_url, "artifact/" + identifier(spec1["release"]), "target") == list(expected_bytes(spec1))
            results += ["cold-independent-target-restore"]
            active.finish()
            active = None
            target.finish()
            target = None
            demo_state = root / "interrupted-demo"
            demo = Process(binary, ["demo", demo_state],
                           env={**os.environ, "CELLULE_RELEASE_AFTER_DEPLOY_MS": "10000"})
            publication = demo.checkpoint("artifact_published")["publication"]
            demo.finish(interrupted=True)
            demo = None
            retained = list(demo_state.glob("*.request.json"))
            assert len(retained) == 1
            original_demo = json.loads(retained[0].read_text())
            demo_spec = original_demo["input"]["spec"]
            # Remove only local working directories: the request and authority-pinned
            # objects survive the interrupt and must restore without another writer.
            shutil.rmtree(demo_state / "flow")
            shutil.rmtree(demo_state / "target")
            recovered = run("workflow", demo_state / "flow", 2, identifier(demo_spec["release"]))[0]["workflow"]
            assert recovered["state"]["spec"] == demo_spec and recovered["status"] == "running"
            assert run("resolve", demo_state / "flow", 2, retained[0])[0]["resolution"] == "committed"
            demo_artifact = root / "interrupted-artifact.bin"
            run("download", demo_state / "flow", 2, identifier(publication["artifact"]["key"]), demo_artifact)
            assert demo_artifact.read_bytes() == expected_bytes(demo_spec)
            results += ["interrupted-demo-drains-both-applications", "interrupted-demo-cold-request-and-Blob-restore"]
            print(json.dumps({"scenario": "passed", "application": "release-pipeline", "checks": results,
                              "seconds": round(time.monotonic() - started, 3)}))
        finally:
            if active:
                active.close()
            if target:
                target.close()
            if demo:
                demo.close()


if __name__ == "__main__":
    main()
