#!/usr/bin/env python3
"""Crash, reconcile delayed Effects, and cold-read a sealed usage statement."""
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


OUTPUT_LIMIT = 1 << 20


def event_line(line):
    try:
        value = json.loads(line)
    except json.JSONDecodeError as source:
        raise RuntimeError(f"stdout contains a non-JSON event line: {line[:4096]!r}") from source
    if not isinstance(value, dict):
        raise RuntimeError("usage-ledger stdout event is not an object")
    return value


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
                if sum(map(len, self.output.values())) > OUTPUT_LIMIT:
                    raise RuntimeError("usage-ledger process output exceeded one MiB")
                if selected.data == "stdout":
                    self.pending += chunk
                    while b"\n" in self.pending:
                        line, self.pending = self.pending.split(b"\n", 1)
                        if line:
                            self.events.append(event_line(line))

    def checkpoint(self, name, predicate=lambda _: True, timeout=150):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for value in self.events:
                if value.get("event") == name and predicate(value):
                    return value
            if self.process.poll() is not None and len(self.closed) == 2:
                raise RuntimeError(f"process exited before {name}: {self.diagnostics()}")
            self.pump(min(1.0, max(0.0, deadline - time.monotonic())))
        raise AssertionError(f"checkpoint {name} absent: {self.diagnostics()}")

    def diagnostics(self):
        return {
            "returncode": self.process.poll(),
            "events": self.events[-12:],
            "stderr": bytes(self.output["stderr"]).decode(errors="replace")[-12000:],
        }

    def finish(self, killed=False, timeout=35):
        stdout, stderr = self.process.communicate(timeout=timeout)
        self.output["stdout"].extend(stdout)
        self.output["stderr"].extend(stderr)
        if sum(map(len, self.output.values())) > OUTPUT_LIMIT:
            raise RuntimeError("usage-ledger process output exceeded one MiB")
        self.closed.update(("stdout", "stderr"))
        for line in bytes(self.output["stdout"]).splitlines():
            if not line:
                continue
            value = event_line(line)
            if value not in self.events:
                self.events.append(value)
        if killed:
            assert self.process.returncode == -signal.SIGKILL, self.diagnostics()
        else:
            assert self.process.returncode == 0, self.diagnostics()
        return self.events

    def kill(self):
        if self.process.poll() is None:
            self.process.kill()

    def close(self):
        self.kill()
        if len(self.closed) != 2:
            self.finish(killed=True, timeout=15)


def main():
    binary = str(Path(sys.argv[1]).resolve())
    environment = os.environ.copy()
    environment["CELLULE_USAGE_LEDGER_ADAPTER_TOKEN"] = "cookbook-" + uuid.uuid4().hex
    with socket.socket() as listener_probe:
        listener_probe.bind(("127.0.0.1", 0))
        port = listener_probe.getsockname()[1]
    environment["CELLULE_USAGE_LEDGER_ADAPTER_ENDPOINT"] = f"http://127.0.0.1:{port}/"
    environment["CELLULE_USAGE_LEDGER_AFTER_PUBLICATION_MS"] = "10000"
    started = time.monotonic()
    checks = []
    active = resumed = None
    with tempfile.TemporaryDirectory(prefix="cellule-usage-ledger-") as temporary:
        state = Path(temporary) / "state"
        plan_path = state / "usage-ledger-demo-plan.json"
        try:
            active = Process(binary, ["demo", state], environment)
            plan_event = active.checkpoint("demo_plan")
            plan_bytes = plan_path.read_bytes()
            plan = json.loads(plan_bytes)
            assert plan_event["tenant"] == plan["tenant"]
            assert plan_event["period_id"] == "".join(f"{byte:02x}" for byte in plan["spec"]["id"])

            projection = active.checkpoint("projection_before_close", timeout=60)
            assert projection["expected_projected"] == 2
            assert projection["actual_projected"] == 2
            assert projection["source_event_count"] == 4
            assert projection["delayed_account"] == "demo-beta"
            checks += ["explicit-roster-and-event-identities-retained", "second-account-effects-delayed-before-close"]

            published = active.checkpoint("statement_published", timeout=180)
            assert published["event_count"] == 4
            assert published["artifact"]["bytes"] > 0
            assert plan_path.read_bytes() == plan_bytes
            active.kill()
            active.finish(killed=True)
            active = None
            checks += ["sigkill-after-content-addressed-blob-publication-before-activity-completion"]

            # The Activity lease is 60 seconds; the LocalNode writer lease expires in 30.
            time.sleep(62)
            environment.pop("CELLULE_USAGE_LEDGER_AFTER_PUBLICATION_MS", None)
            resumed = Process(binary, ["demo", state], environment)
            completed_event = resumed.checkpoint("demo_complete", timeout=240)
            sealed_event = resumed.checkpoint("period_sealed_before_delayed_effects")
            events = resumed.finish()
            resumed = None
            assert plan_path.read_bytes() == plan_bytes
            assert completed_event["period_id"] == projection["period_id"]
            assert completed_event["source_event_count"] == 4
            assert completed_event["reconciled_event_count"] == 4
            assert completed_event["projected_after_late_effects"] == 4
            assert sealed_event["sealed_event_count"] == 4
            assert sealed_event["delayed_account"] == "demo-beta"
            assert any(value.get("event") == "statement_published" for value in events)
            statement = Path(completed_event["statement_path"])
            lines = statement.read_text().splitlines()
            assert len(lines) == 5 and lines[0].startswith("period_id,account,event_id,")
            checks += [
                "native-activity-reclaimed-after-lease-expiry",
                "same-retained-plan-resumes-with-byte-identical-source-identities",
                "source-snapshots-repair-delayed-events-before-sealing",
                "late-effects-settle-as-idempotent-projections-after-seal",
                "blob-csv-verified-and-exported-to-local-file",
            ]

            inspected = subprocess.run(
                [binary, "inspect", state, plan["tenant"], completed_event["period_id"]],
                capture_output=True,
                text=True,
                timeout=90,
                env=environment,
                check=True,
            )
            assert len(inspected.stdout) + len(inspected.stderr) < OUTPUT_LIMIT
            status = json.loads(inspected.stdout.splitlines()[0])["period"]
            assert status["status"] == "sealed" and status["projected_events"] == 4
            assert status["report"]["event_count"] == 4
            checks.append("cold-period-read-preserves-sealed-report-and-full-projection")

            print(json.dumps({
                "scenario": "passed",
                "application": "usage-ledger",
                "checks": checks,
                "seconds": round(time.monotonic() - started, 3),
            }))
        finally:
            for process in (active, resumed):
                if process is not None:
                    process.close()


if __name__ == "__main__":
    main()
