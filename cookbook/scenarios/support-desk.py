#!/usr/bin/env python3
"""Crash and cold-restart qualification for the retained support-desk demo."""
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
import uuid


LIMIT = 1 << 20


class Process:
    def __init__(self, binary, arguments, environment):
        self.process = subprocess.Popen(
            [binary, *map(str, arguments)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            bufsize=0,
        )
        self.output = {"stdout": bytearray(), "stderr": bytearray()}
        self.pending = b""
        self.events = []
        self.closed = set()

    def pump(self, timeout):
        with selectors.DefaultSelector() as selector:
            for name, stream in (("stdout", self.process.stdout), ("stderr", self.process.stderr)):
                if name not in self.closed:
                    selector.register(stream, selectors.EVENT_READ, name)
            for selected, _ in selector.select(timeout=timeout):
                chunk = os.read(selected.fileobj.fileno(), 8192)
                if not chunk:
                    self.closed.add(selected.data)
                    continue
                self.output[selected.data].extend(chunk)
                if sum(map(len, self.output.values())) > LIMIT:
                    raise RuntimeError("support-desk process output exceeded one MiB")
                if selected.data == "stdout":
                    self.pending += chunk
                    while b"\n" in self.pending:
                        line, self.pending = self.pending.split(b"\n", 1)
                        if line:
                            value = json.loads(line)
                            if not isinstance(value, dict):
                                raise RuntimeError("support-desk stdout event is not an object")
                            self.events.append(value)

    def checkpoint(self, event, predicate=lambda _: True, timeout=150):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for value in self.events:
                if value.get("event") == event and predicate(value):
                    return value
            if self.process.poll() is not None and len(self.closed) == 2:
                raise RuntimeError(f"process exited before {event}: {self.diagnostics()}")
            self.pump(min(1.0, max(0.0, deadline - time.monotonic())))
        raise AssertionError(f"checkpoint {event} absent: {self.diagnostics()}")

    def diagnostics(self):
        return {
            "returncode": self.process.poll(),
            "events": self.events[-12:],
            "stderr": bytes(self.output["stderr"]).decode(errors="replace")[-12000:],
        }

    def finish(self, crash=False, terminate=False, timeout=35):
        if self.process.poll() is None:
            if crash:
                self.process.kill()
            elif terminate:
                self.process.send_signal(signal.SIGTERM)
        stdout, stderr = self.process.communicate(timeout=timeout)
        self.output["stdout"].extend(stdout)
        self.output["stderr"].extend(stderr)
        if sum(map(len, self.output.values())) > LIMIT:
            raise RuntimeError("support-desk process output exceeded one MiB")
        self.closed.update(("stdout", "stderr"))
        for line in bytes(self.output["stdout"]).splitlines():
            value = json.loads(line)
            if isinstance(value, dict) and value not in self.events:
                self.events.append(value)
        if crash:
            assert self.process.returncode == -signal.SIGKILL, self.diagnostics()
        else:
            assert self.process.returncode == 0, self.diagnostics()
            assert sum(value.get("event") == "drained" for value in self.events) == 1, self.events
        return self.events

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        if self.closed != {"stdout", "stderr"}:
            stdout, stderr = self.process.communicate(timeout=10)
            self.output["stdout"].extend(stdout)
            self.output["stderr"].extend(stderr)
            self.closed.update(("stdout", "stderr"))
            for line in stdout.splitlines():
                if line:
                    value = json.loads(line)
                    if isinstance(value, dict) and value not in self.events:
                        self.events.append(value)


def main():
    binary = str(Path(sys.argv[1]).resolve())
    environment = os.environ.copy()
    environment["CELLULE_SUPPORT_DESK_TOKEN"] = "cookbook-" + uuid.uuid4().hex
    started = time.monotonic()
    checks = []
    active = receiver = resumed_receiver = resumed = None
    with tempfile.TemporaryDirectory(prefix="cellule-support-desk-") as temporary:
        root = Path(temporary)
        state, receiver_state = root / "source", root / "receiver"
        controls = root / "receiver-controls.json"
        controls.write_text(json.dumps({"after_publication_ms": 10000, "drop_reply_once": True}))
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}/notifications"
        environment["CELLULE_SUPPORT_DESK_NOTIFICATION_ENDPOINT"] = endpoint

        def run(*arguments, success=True):
            result = subprocess.run(
                [binary, *map(str, arguments)],
                capture_output=True,
                text=True,
                timeout=90,
                env=environment,
            )
            if len(result.stdout) + len(result.stderr) > LIMIT:
                raise RuntimeError("support-desk command output exceeded one MiB")
            if (result.returncode == 0) != success:
                raise RuntimeError(
                    f"{arguments}: exit {result.returncode}: {result.stdout}: {result.stderr}"
                )
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def wait_for_publication(app, external, key, timeout=90):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                for value in external.events:
                    if value.get("event") == "receiver_published" and value.get("selected") is True:
                        assert value.get("created") is True
                        assert value["acknowledgement"]["key"] == key
                        return value
                if app.process.poll() is not None and len(app.closed) == 2:
                    raise RuntimeError(f"demo exited before external publication: {app.diagnostics()}")
                app.pump(0.05)
                external.pump(0.05)
            raise AssertionError(f"external publication absent: app={app.diagnostics()} receiver={external.diagnostics()}")

        try:
            receiver = Process(binary, ["receiver", receiver_state, port, controls], environment)
            ready = receiver.checkpoint("receiver_ready")
            assert ready["endpoint"] == endpoint, ready
            active = Process(binary, ["demo", state], environment)
            plan_event = active.checkpoint("demo_plan")
            plan_path = Path(plan_event["path"])
            plan_bytes = plan_path.read_bytes()
            plan = json.loads(plan_bytes)
            assert plan["notification_endpoint"] == endpoint
            assert plan_event["notification_key"] == plan["notification_key"]
            assert plan["race_ticket"] != plan["notification_ticket"]
            request_ids = [tuple(item["identity"]["request"]) for item in plan["requests"]]
            attachment = plan["attachment"]["plan"]
            attachment_ids = [tuple(item["request"]) for item in attachment["identities"]]
            attachment_ids.append(tuple(attachment["upload"]))
            all_ids = request_ids + attachment_ids
            assert len(all_ids) == len(set(all_ids)) == 11
            assert all(len(identity) == 16 and any(identity) for identity in all_ids)
            checks += ["external-receiver-is-separate-process", "complete-retained-plan-before-dispatch", "all-command-and-attachment-identities-frozen"]

            published = wait_for_publication(active, receiver, plan["notification_key"])
            assert published["acknowledgement"]["key"] == plan["notification_key"]
            assert plan_path.read_bytes() == plan_bytes
            initial_events = active.finish(crash=True)
            active = None
            checks += ["actual-native-notification-reaches-independent-receiver", "sigkill-after-fsynced-external-publication-before-reply", "retained-active-plan-is-byte-identical-after-crash"]

            receiver.finish(terminate=True)
            receiver = None
            record = run("receiver-record", receiver_state, plan["notification_key"])[0]["external_record"]
            assert record["acknowledgement"] == published["acknowledgement"]
            assert record["acknowledgement"]["applied_count"] == 1
            assert record["notification"]["endpoint"] == endpoint
            checks.append("external-permanent-record-survives-receiver-drain")

            time.sleep(32)  # Expire source writer, coordinator host, and native Activity leases.
            resumed_receiver = Process(binary, ["receiver", receiver_state, port], environment)
            assert resumed_receiver.checkpoint("receiver_ready")["endpoint"] == endpoint
            resumed = Process(binary, ["demo", state], environment)
            resumed_plan = resumed.checkpoint("demo_plan")
            assert Path(resumed_plan["path"]).read_bytes() == plan_bytes
            complete = resumed.checkpoint("demo_complete", timeout=150)
            assert complete["race_ticket"] == plan["race_ticket"]
            assert complete["receiver_record"] == plan["notification_key"]
            events = resumed.finish()
            resumed = None
            assert not (state / "support-desk-demo-active.json").exists()
            completed_path = Path(complete["retained_plan"])
            assert completed_path.read_bytes() == plan_bytes
            attempts = complete["notification"]["state"]["attempts"]
            assert attempts and attempts[-1]["classification"] == "delivered", attempts
            assert attempts[-1]["acknowledgement"] == record["acknowledgement"]
            assert attempts[-1]["acknowledgement"]["applied_count"] == 1
            if len(attempts) == 1:
                assert attempts[0]["round"] == 1 and attempts[0]["native_attempt"] >= 2
            else:
                assert attempts[0]["round"] == 1 and attempts[0]["classification"] == "retryable"
                assert attempts[0]["may_have_applied"] is True
                assert attempts[-1]["round"] == 2
            assert complete["ticket"]["status"] == "open" and complete["ticket"]["escalated"] is True
            all_events = initial_events + events
            assert any(value.get("event") == "demo_race_resolved" for value in all_events) or any(
                value.get("event") == "demo_race_resolution_replayed" for value in all_events
            )
            receiver_record = run("receiver-record", receiver_state, plan["notification_key"])[0]["external_record"]
            assert receiver_record == record
            checks += ["receiver-restarts-from-exact-endpoint-and-store", "native-leases-expire-before-identical-plan-resume", "same-external-key-reclaims-unfinished-Activity", "receiver-application-count-remains-one", "completed-plan-is-byte-identical"]

            resumed_receiver.finish(terminate=True)
            resumed_receiver = None
            race_ticket = run("get", state, plan["tenant"], plan["race_ticket"])[0]["ticket"]
            notify_ticket = run("get", state, plan["tenant"], plan["notification_ticket"])[0]["ticket"]
            assert race_ticket["status"] == "resolved" and race_ticket["generation"] == 3
            assert race_ticket["escalated"] is False and race_ticket["message_count"] == 1
            assert race_ticket["generation"] == 3
            assert len(race_ticket["attachments"]) == 1
            assert notify_ticket["status"] == "open" and notify_ticket["escalated"] is True
            assert notify_ticket["notifications"][-1]["notification"]["deadline"]["ticket"] == plan["notification_ticket"]
            page = run("messages", state, plan["tenant"], plan["race_ticket"])[0]["page"]
            assert page["ticket"]["status"] == "resolved" and len(page["messages"]) == 1
            assert page["messages"][0]["message"]["id"] == "demo-message-" + plan["race_ticket"].removeprefix("demo-race-")
            progress = run("progress", state, plan["tenant"], plan["notification_ticket"])[0]
            latest = progress["latest_notification"]
            assert latest["status"] == "completed" and latest["state"]["phase"] == "delivered"
            assert latest["state"]["attempts"] == attempts
            race_progress = run("progress", state, plan["tenant"], plan["race_ticket"])[0]
            assert race_progress["deadline"]["status"] == "completed"
            assert race_progress["deadline"]["state"]["fired_at_ms"] is not None

            attachment_file = root / "attachment-plan.json"
            attachment_file.write_text(json.dumps(plan["attachment"], separators=(",", ":")))
            restored_file = root / "restored-attachment.bin"
            run("download-attachment", state, attachment_file, restored_file)
            assert restored_file.read_bytes() == bytes(attachment["bytes"])
            callback_path = state / f"support-desk-demo-callback-{plan['race_ticket']}.json"
            callback = json.loads(callback_path.read_text())
            assert callback["deadline"] == plan["race_deadline"]
            callback_effect = run(
                "callback-effect", state, plan["tenant"], callback["effect_id"]
            )[0]["effect"]
            assert callback_effect["state"].lower() == "delivered" and callback_effect["result"] is not None, callback_effect
            checks += ["native-deadline-callback-races-transactional-resolution", "cold-authoritative-ticket-and-conversation", "cold-deadline-and-notification-workflow-ledgers", "cold-native-callback-effect-receipt", "cold-byte-identical-private-attachment-restore"]
            print(json.dumps({"scenario": "passed", "application": "support-desk", "checks": checks,
                              "seconds": round(time.monotonic() - started, 3)}))
        finally:
            for process in (active, resumed, receiver, resumed_receiver):
                if process is not None:
                    process.close()
            if sys.exc_info()[0] is not None:
                print(json.dumps({"process_diagnostics": [
                    process.diagnostics()
                    for process in (active, resumed, receiver, resumed_receiver)
                    if process is not None
                ]}), file=sys.stderr)


if __name__ == "__main__":
    main()
