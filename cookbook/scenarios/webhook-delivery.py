#!/usr/bin/env python3
"""Durable subscriber snapshots, actual HTTP faults, lost replies and cold recovery.

Run a built isolated source snapshot against private local S3, never a live installation.
"""
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def main():
    binary = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="cellule-webhook-") as temporary:
        root = Path(temporary)
        state = root / "state"
        cold = root / "cold"
        active = None
        buffers = {}
        checks = []
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        environment = dict(os.environ, CELLULE_WEBHOOK_PORT=str(port))
        endpoint = f"http://127.0.0.1:{port}/deliver"
        topic = "process-" + uuid.uuid4().hex
        stderr_path = root / "serve.stderr"
        stderr_file = None

        def run(*args, success=True):
            result = subprocess.run([binary, *map(str, args)], env=environment,
                                    capture_output=True, text=True, timeout=75)
            if success and result.returncode:
                raise RuntimeError(f"{args}: {result.stderr}")
            if not success and result.returncode == 0:
                raise AssertionError(f"{args}: unexpectedly succeeded")
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def mutation(action):
            token = uuid.uuid4().hex
            source = root / f"{token}.input.json"
            retained = root / f"{token}.mutation.json"
            source.write_text(json.dumps(action))
            run("prepare", source, retained)
            return retained

        def apply(action, success=True):
            return run("apply", state, mutation(action), success=success)[-1]

        def subscribe(name, enabled=True, target=endpoint):
            subscriptions = run("subscriptions", state)[-1]["subscriptions"]
            previous = next((value for value in subscriptions if value["id"] == name), None)
            return apply({"kind": "subscribe", "id": name, "topic": topic,
                          "endpoint": target, "enabled": enabled,
                          "expected_revision": previous["revision"] if previous else 0})

        def publish(payload):
            identity = str(uuid.uuid4())
            action = {"kind": "publish", "id": identity, "topic": topic, "payload": payload}
            retained = mutation(action)
            return identity, action, retained

        def diagnostics():
            return stderr_path.read_text() if stderr_path.exists() else "receiver has not started"

        def event(process, name, key=None, requests=None, timeout=45):
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)
            buffered = buffers.pop(process, b"")
            deadline = time.monotonic() + timeout
            try:
                while time.monotonic() < deadline:
                    if b"\n" not in buffered:
                        selected = selector.select(timeout=1)
                        if not selected:
                            if process.poll() is not None:
                                raise RuntimeError(f"server exited before {name}: {diagnostics()}")
                            continue
                        chunk = os.read(selected[0][0].fileobj.fileno(), 8192)
                        if not chunk:
                            raise RuntimeError(f"server closed output before {name}: {diagnostics()}")
                        buffered += chunk
                    while b"\n" in buffered:
                        line, buffered = buffered.split(b"\n", 1)
                        value = json.loads(line)
                        if value.get("event") != name:
                            continue
                        progress = value.get("progress", {})
                        if key is not None and progress.get("key") != key:
                            continue
                        if requests is not None and progress.get("receiver_requests") != requests:
                            continue
                        buffers[process] = buffered
                        return value
                raise TimeoutError(f"no {name} checkpoint for {key}")
            finally:
                selector.close()

        def serve(delay=0):
            nonlocal active, stderr_file
            stderr_file = stderr_path.open("w")
            active = subprocess.Popen([binary, "serve", str(state), "180", str(delay)],
                                      env=environment, stdout=subprocess.PIPE, stderr=stderr_file)
            ready = event(active, "ready")
            assert ready["receiver"] == endpoint
            return active

        def stop(kill=False):
            nonlocal active, stderr_file
            if active is None:
                return
            process = active
            process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            _, _ = process.communicate(timeout=30)
            expected = -signal.SIGKILL if kill else 0
            if process.returncode != expected:
                raise RuntimeError(f"server exit {process.returncode}: {diagnostics()}")
            active = None
            buffers.pop(process, None)
            stderr_file.close()
            stderr_file = None

        def delivery(key, directory=state):
            return run("delivery", directory, key)[-1]["delivery"]

        def received(key, directory=state):
            return run("received", directory, key)[-1]["received"]

        def http(ticket, bearer=True):
            key = ticket_key(ticket)
            headers = {"Content-Type": "application/json", "Idempotency-Key": key}
            if bearer:
                headers["Authorization"] = "Bearer " + environment.get(
                    "CELLULE_WEBHOOK_TOKEN", "cellule-cookbook-local-webhook")
            request = urllib.request.Request(endpoint, data=json.dumps(ticket).encode(),
                                             headers=headers, method="POST")
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            try:
                with opener.open(request, timeout=10) as response:
                    return response.status, json.loads(response.read(4097))
            except urllib.error.HTTPError as error:
                return error.code, json.loads(error.read(4097))

        known_keys = {}

        def ticket_key(ticket):
            # Use original source/native key evidence; never invent another replay key.
            return known_keys[json.dumps(ticket, sort_keys=True)]

        try:
            names = ["process-good", "process-drop", "process-transient", "process-terminal"]
            for name, mode in zip(names, ["good", "drop_once", "transient_once", "terminal"]):
                subscribe(name)
                run("policy", state, name, mode)
            identity, action, retained = publish("original subscriber snapshot")
            original_file = retained.read_bytes()
            source_logs = root / "source.stderr"
            with source_logs.open("w") as source_stderr:
                active = subprocess.Popen([binary, "apply", str(state), str(retained), "10000"],
                                          env=environment, stdout=subprocess.PIPE, stderr=source_stderr)
                published = event(active, "source_published")
                active.kill()
                active.communicate(timeout=15)
                assert active.returncode == -signal.SIGKILL
                active = None
            assert retained.read_bytes() == original_file
            run("event", root / "live-steal", identity, success=False)
            checks.append("live-owner-lease-refuses-steal")
            time.sleep(32)
            resolved = run("resolve", cold, retained)[-1]
            assert resolved["resolution"] == "committed"
            assert resolved["commit_sequence"] == published["receipt"]["commit_sequence"]
            frozen = resolved["outcome"]["event"]
            assert len(frozen["deliveries"]) == 4
            checks.extend(["kill-after-source-publication", "original-source-outcome-resolution"])
            replay = run("apply", state, retained)[-1]
            assert replay["outcome"] == resolved["outcome"]
            assert replay["receipt"]["commit_sequence"] == resolved["commit_sequence"]
            subscribe("process-drop", enabled=False, target=f"http://127.0.0.1:{port-1 if port == 65535 else port+1}/deliver")
            repeated = apply(action)
            assert repeated["outcome"]["decision"] == "existing_event"
            assert repeated["outcome"]["event"] == frozen
            checks.extend(["retained-source-replay", "fresh-event-idempotency", "immutable-subscriber-snapshot"])
            conflict_file = mutation(dict(action, payload="changed content"))
            conflict = run("apply", state, conflict_file, success=False)[-1]
            conflict_replay = run("apply", state, conflict_file, success=False)[-1]
            assert conflict["outcome"]["decision"] == "conflict"
            assert conflict == conflict_replay
            assert run("resolve", state, conflict_file)[-1]["resolution"] == "committed"
            checks.append("durable-content-conflict")
            keys_by_subscription = {item["subscription"]: item["key"] for item in resolved["delivery_keys"]}
            assert len(set(keys_by_subscription.values())) == 4
            process = serve()
            expected = {keys_by_subscription[name]: 2 if name in {"process-drop", "process-transient"} else 1 for name in names}
            until = time.monotonic() + 45
            seen = {}
            while any(seen.get(key, 0) < count for key, count in expected.items()):
                value = event(process, "receiver_published", timeout=max(1, until-time.monotonic()))["progress"]
                seen[value["key"]] = value["receiver_requests"]
                if time.monotonic() >= until:
                    raise TimeoutError("all subscriber paths did not reach receiver checkpoints")
            stop()
            for value in frozen["deliveries"]:
                ticket = value["ticket"]
                key = keys_by_subscription[ticket["subscription"]]
                view = delivery(key)
                assert view["state"]["ticket"] == ticket
                known_keys[json.dumps(ticket, sort_keys=True)] = key
                record = received(key)
                assert record["ticket"] == ticket
                phase = view["state"]["phase"]
                if ticket["subscription"] == "process-terminal":
                    assert phase == "failed" and not record["applied"]
                    assert view["state"]["attempts"][0]["status"] == 422
                else:
                    assert phase == "delivered" and record["applied"]
                    ack = view["state"]["attempts"][-1]["acknowledgement"]
                    assert ack["key"] == key and ack["applied_count"] == 1
                if ticket["subscription"] == "process-drop":
                    assert record["requests"] >= 2
                    assert view["state"]["attempts"][0]["may_have_applied"]
                    assert view["state"]["attempts"][0]["status"] is None
                if ticket["subscription"] == "process-transient":
                    assert record["requests"] >= 2
                    assert view["state"]["attempts"][0]["status"] == 503
            checks.extend(["actual-socket-reply-drop", "stable-delivery-key", "transient-http-retry", "terminal-http-rejection"])
            for name in names:
                subscribe(name, enabled=name == "process-drop")
            identity2, _, retained2 = publish("crash after receiver application")
            second_output = run("apply", state, retained2)[-1]
            second = second_output["outcome"]["event"]
            assert len(second["deliveries"]) == 1
            process = serve(10000)
            checkpoint = event(process, "receiver_published")["progress"]
            assert checkpoint["applied"] and checkpoint["receiver_requests"] == 1
            assert checkpoint["drop_reply"]
            crash_key = checkpoint["key"]
            assert crash_key == second_output["delivery_keys"][0]["key"]
            stop(kill=True)
            run("received", root / "receiver-live-steal", crash_key, success=False)
            time.sleep(32)
            restored = received(crash_key, cold)
            assert restored["ticket"] == second["deliveries"][0]["ticket"]
            assert restored["applied"] and restored["requests"] == 1
            checks.append("kill-after-receiver-publication")
            process = serve()
            retry = event(process, "receiver_published", key=crash_key, requests=2)["progress"]
            assert retry["applied"] and not retry["drop_reply"]
            ticket = second["deliveries"][0]["ticket"]
            known_keys[json.dumps(ticket, sort_keys=True)] = crash_key
            assert http(ticket, bearer=False)[0] == 401
            code, ack = http(ticket)
            assert code == 200 and ack["key"] == crash_key and ack["applied_count"] == 1
            stop()
            view = delivery(crash_key)
            assert view["state"]["phase"] == "delivered"
            assert view["state"]["round"] == 1
            assert view["state"]["attempts"][-1]["native_attempt"] >= 2
            assert received(crash_key)["requests"] == 3
            checks.extend(["native-activity-redelivery", "same-business-key-after-crash", "receiver-auth-before-dispatch", "one-permanent-receiver-action"])
            cold.mkdir(exist_ok=True)
            orphan = cold / "unrelated-evidence.txt"
            orphan.write_text("retain this file")
            assert delivery(crash_key, cold)["state"] == view["state"]
            assert received(crash_key, cold)["applied"]
            assert run("event", cold, identity2)[-1]["event"] == second
            assert orphan.read_text() == "retain this file"
            checks.extend(["exact-root-cold-restore", "unrelated-evidence-preserved", "sigterm-owned-drain"])
            print(json.dumps({"scenario": "passed", "checks": checks,
                              "first_event_uuid": identity, "crash_event_uuid": identity2,
                              "crash_delivery_key": crash_key,
                              "native_redelivery_attempt": view["state"]["attempts"][-1]["native_attempt"]}))
        finally:
            if active is not None:
                active.kill()
                active.communicate(timeout=15)
            if stderr_file is not None:
                stderr_file.close()


if __name__ == "__main__":
    main()
