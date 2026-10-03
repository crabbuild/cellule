#!/usr/bin/env python3
"""Outage observation, restart after SQL publication, exact redelivery, and cold restore.

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
import uuid


class Process:
    def __init__(self, binary, arguments):
        self.process = subprocess.Popen([binary, *map(str, arguments)], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, bufsize=0)
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
            raise RuntimeError(f"process drain failed: {self.output}")

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)


def main():
    binary = str(Path(sys.argv[1]).resolve())
    monitor_process, target_process = None, None
    with tempfile.TemporaryDirectory(prefix="cellule-monitor-") as temporary:
        root = Path(temporary)
        state = root / "state"
        target_state = root / "target.txt"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        monitor, version = str(uuid.uuid4()), str(uuid.uuid4())

        def run(*arguments, success=True, env=None):
            result = subprocess.run([binary, *map(str, arguments)], capture_output=True,
                                    text=True, timeout=75, env=env)
            if bool(result.returncode == 0) != success:
                raise RuntimeError(f"{arguments}: exit {result.returncode}; {result.stderr}; {result.stdout}")
            assert len(result.stdout) + len(result.stderr) <= 524288
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def prepare(value, name):
            change, retained = root / (name + ".json"), root / (name + ".request.json")
            change.write_text(json.dumps(value))
            run("prepare", change, retained)
            run("prepare", change, retained, success=False)
            return retained

        def inspection():
            after, rows, result = 0, [], None
            while True:
                result = run("inspect", state, monitor, version, after, 100)[-1]["inspection"]
                rows.extend(result["checks"])
                if result["next"] is None:
                    return {"state": result["state"], "checks": rows, "next": None}
                after = result["next"]

        try:
            run("target-state", target_state, "down")
            target_process = Process(binary, ["target", port, target_state, 240])
            target_process.checkpoint("target_ready")
            configured = prepare({"operation": "upsert", "monitor": monitor,
                                  "definition": {"id": version, "label": "Local API",
                                                 "endpoint": f"http://127.0.0.1:{port}/probe"},
                                  "interval_ms": 1000, "start_in_ms": 200}, "configure")
            assert run("resolve", state, configured)[-1]["resolution"] == "absent"
            original = run("apply", state, configured)[-1]
            assert run("apply", state, configured)[-1] == original
            monitor_process = Process(binary, ["serve", state, 240, 10000])
            monitor_process.checkpoint("ready")
            first = monitor_process.checkpoint("check_recorded")["progress"]
            assert first["check"]["probe"]["health"] == "Down"
            first_outcome = first["outcome"]["Recorded"]
            assert first_outcome["edge"]["kind"] == "Opened" and first_outcome["current"]
            ticket_file = root / "ticket.json"
            ticket_file.write_text(json.dumps(first["check"]["ticket"]))
            run("target-state", target_state, "up")
            monitor_process.finish(crash=True)
            monitor_process = None
            orphan_sessions = set(state.iterdir())
            assert orphan_sessions
            run("get", state, monitor, success=False)
            time.sleep(32)  # signed cookbook serving lease lasts 30 seconds
            restored = inspection()
            assert restored["checks"][0]["check"] == first["check"]
            assert restored["checks"][0]["outcome"] == first["outcome"]
            assert restored["checks"][0]["row"] == first_outcome["row"]
            workflow = run("workflow", state, ticket_file)[-1]["workflow"]
            assert workflow["status"] == "completed" and workflow["state"]["check"] == first["check"]
            assert set(state.iterdir()) == orphan_sessions

            monitor_process = Process(binary, ["serve", state, 240, 0, "lose-reply"])
            monitor_process.checkpoint("ready")
            repeated = monitor_process.checkpoint("check_recorded", lambda value:
                                                 value["progress"]["effect_id"] == first["effect_id"])["progress"]
            assert repeated == first
            recovered = monitor_process.checkpoint("check_recorded", lambda value:
                                                  value["progress"]["outcome"]["Recorded"]["edge"] is not None and
                                                  value["progress"]["outcome"]["Recorded"]["edge"]["kind"] == "Closed")["progress"]
            assert recovered["check"]["probe"]["health"] == "Up"
            assert recovered["outcome"]["Recorded"]["edge"]["incident"] == first_outcome["edge"]["incident"]
            monitor_process.finish()
            monitor_process = None
            paused = prepare({"operation": "pause", "monitor": monitor}, "pause")
            run("apply", state, paused)
            # Published probes and their signed notification intents remain eligible after pause.
            schedule = run("get", state, monitor)[-1]["schedule"]
            assert not schedule["enabled"]
            deadline = time.monotonic() + 120
            while True:
                # Let all pipeline stages make progress in one serving lifetime;
                # repeated two-second boots spend most of the bound on startup
                # and drain, stopping newly started stages before their next cycle.
                run("serve", state, 20, 0)
                completed = inspection()
                alerts = run("alerts", state, monitor, version, 0, 100)[-1]["alerts"]
                assert len(completed["checks"]) <= schedule["occurrence"]
                if len(completed["checks"]) == schedule["occurrence"] and len(alerts["edges"]) == 2:
                    break
                assert time.monotonic() < deadline, (
                    f"paused source did not reconcile: {len(completed['checks'])}/"
                    f"{schedule['occurrence']} checks, {len(alerts['edges'])} alerts")
            checks = completed["checks"]
            assert len(checks) == schedule["occurrence"]
            assert len({row["check"]["ticket"]["occurrence"] for row in checks}) == len(checks)
            assert sorted(row["check"]["ticket"]["occurrence"] for row in checks) == list(range(1, len(checks) + 1))
            assert completed["state"]["health"] == "Up" and completed["state"]["incident"] is None
            edges = [row["outcome"]["Recorded"]["edge"] for row in checks if row["outcome"]["Recorded"]["edge"] is not None]
            assert len(edges) == 2 and edges[0]["kind"] == "Opened" and edges[1]["kind"] == "Closed"
            alerts = run("alerts", state, monitor, version, 0, 100)[-1]["alerts"]
            assert alerts["edges"] == edges and alerts["next"] is None
            assert run("apply", state, configured)[-1] == original
            assert run("resolve", state, configured)[-1]["commit_sequence"] == original["receipt"]["commit_sequence"]
            assert set(state.iterdir()) == orphan_sessions

            conflict = json.loads(configured.read_text())
            conflict["request_id"] = str(uuid.uuid4())
            conflict["change"]["definition"]["label"] = "Changed configuration"
            conflict_file = root / "conflict.json"
            conflict_file.write_text(json.dumps(conflict))
            rejected = run("apply", state, conflict_file, success=False)[-1]
            assert rejected["outcome"] == "Conflict"
            assert run("apply", state, conflict_file, success=False)[-1] == rejected
            assert run("get", state, monitor)[-1]["schedule"] == schedule
            shutil.rmtree(state)
            assert inspection() == completed
            assert run("alerts", state, monitor, version, 0, 100)[-1]["alerts"] == alerts
            assert run("workflow", state, ticket_file)[-1]["workflow"] == workflow
            assert run("get", state, monitor)[-1]["schedule"] == schedule
            assert run("resolve", state, configured)[-1]["commit_sequence"] == original["receipt"]["commit_sequence"]

            expired = json.loads(configured.read_text())
            expired["expires_at_ms"] = int(time.time() * 1000) - 1
            expired["issued_at_ms"] = expired["expires_at_ms"] - 300000
            expired_file = root / "expired.json"
            expired_file.write_text(json.dumps(expired))
            result = run("resolve", root / "never-provisioned", expired_file,
                         env={**os.environ, "CELLULE_COOKBOOK_ENDPOINT": "http://127.0.0.1:1"})[-1]
            assert result == {"resolution": "expired", "absence_proven": False}
            assert not (root / "never-provisioned").exists()
            target_process.finish()
            target_process = None
            print(json.dumps({"scenario": "passed", "monitor": monitor, "definition": version,
                              "checks_count": len(checks), "incident": edges[0]["incident"],
                              "checks": ["persistent-native-Cron", "retained-config-replay", "real-HTTP-outage",
                                         "native-Activity-observation", "completed-observation-retained",
                                         "atomic-incident-notification-intent", "kill-after-SQL-publication",
                                         "live-owner-refusal", "expired-owner-takeover", "same-source-effect",
                                         "same-SQL-row-edge-receipt", "target-recovery-during-restart",
                                         "one-open-one-close", "same-incident-recovery", "actual-lost-reply-resolution",
                                         "owned-SIGTERM-drain", "pause-keeps-published-probes", "complete-occurrence-reconciliation",
                                         "no-duplicate-checks-or-edges", "signed-notification-inbox",
                                         "durable-definition-conflict", "original-outcome-resolution",
                                         "orphan-session-preserved", "cold-restore", "expired-evidence-does-not-prove-absence"]}))
        finally:
            if monitor_process:
                monitor_process.close()
            if target_process:
                target_process.close()


if __name__ == "__main__":
    main()
