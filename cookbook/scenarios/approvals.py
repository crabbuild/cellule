#!/usr/bin/env python3
"""Persistent human approvals and a crash after durable external publication.

Run in CI or an isolated snapshot against private storage. One fixed application
installation owns its two Workflow shards; processes deliberately run in order.
"""
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import tempfile
import time
import uuid


class ReminderServer:
    """Own a bounded process and observe the post-fsync Activity checkpoint."""

    def __init__(self, binary, state):
        environment = {**os.environ, "RUST_LOG": "error,cellule_cookbook_approvals=info"}
        self.process = subprocess.Popen(
            [binary, "serve", str(state), "240"], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=environment, bufsize=0,
        )
        self.output = {"stdout": bytearray(), "stderr": bytearray()}

    def checkpoint(self, created):
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(self.process.stderr, selectors.EVENT_READ, "stderr")
            deadline = time.monotonic() + 60
            pending = b""
            while time.monotonic() < deadline:
                for item, _ in selector.select(timeout=1):
                    chunk = os.read(item.fileobj.fileno(), 4096)
                    if not chunk:
                        selector.unregister(item.fileobj)
                        if not selector.get_map():
                            raise RuntimeError(f"server exited before checkpoint: {self.output}")
                        continue
                    self.output[item.data].extend(chunk)
                    if sum(map(len, self.output.values())) > 262144:
                        raise RuntimeError("server diagnostic output exceeded 256 KiB")
                    if item.data != "stderr":
                        continue
                    pending += chunk
                    while b"\n" in pending:
                        line, pending = pending.split(b"\n", 1)
                        if b'mailbox_published' not in line:
                            continue
                        key = re.search(rb'\bkey=([0-9a-f]{64})\b', line)
                        attempt = re.search(rb'\battempt=(\d+)\b', line)
                        expected = b"created=true" if created else b"created=false"
                        if key and attempt and expected in line:
                            return key.group(1).decode(), int(attempt.group(1))
            raise AssertionError(f"mailbox checkpoint absent: {self.output}")

    def finish(self, crash=False):
        if crash:
            self.process.kill()
        else:
            self.process.send_signal(signal.SIGTERM)
        tail, errors = self.process.communicate(timeout=25)
        self.output["stdout"].extend(tail)
        self.output["stderr"].extend(errors)
        if not crash and self.process.returncode:
            raise RuntimeError(f"server failed during drain: {self.output}")

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)


def main():
    binary = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="cellule-approvals-") as temporary:
        root = Path(temporary)
        state = root / "state"
        mailbox = root / "mailbox"
        active = None

        def run(*arguments, success=True):
            result = subprocess.run([binary, *map(str, arguments)], capture_output=True,
                                    text=True, timeout=75)
            if bool(result.returncode == 0) != success:
                raise RuntimeError(f"{arguments}: exit {result.returncode}; {result.stderr}; {result.stdout}")
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def mutation(actor, operation):
            name = uuid.uuid4().hex
            source = root / f"{name}.json"
            retained = root / f"{name}.mutation.json"
            source.write_text(json.dumps(operation))
            run("prepare", actor, mailbox, source, retained)
            # Preparation must preserve the original evidence, never overwrite it.
            run("prepare", actor, mailbox, source, retained, success=False)
            return retained

        def apply(actor, operation, success=True):
            retained = mutation(actor, operation)
            events = run("apply", state, actor, retained, success=success)
            return retained, events[-1] if events else None

        def view(purchase):
            return run("get", state, "alice", purchase)[-1]["view"]

        def run_uuid(value):
            return str(uuid.UUID(bytes=bytes(value["run_id"])))

        def purchase(identifier, **overrides):
            return {"operation": "submit", "id": identifier, "title": "Team workstations",
                    "units": 2400, "approvers": ["bob", "carol"], "timeout_ms": 600000,
                    "remind_in_ms": 590000, **overrides}

        def vote(identifier, run_id, signal_id, choice="approve"):
            return {"operation": "vote", "id": identifier, "run_id": run_id,
                    "signal_id": signal_id, "choice": choice}

        try:
            identifier = str(uuid.uuid4())
            submitted = mutation("alice", purchase(identifier, remind_in_ms=0,
                                                   publication_delay_ms=10000))
            assert run("resolve", state, "alice", submitted)[-1]["resolution"] == "absent"
            original = run("apply", state, "alice", submitted)[-1]
            assert run("apply", state, "alice", submitted)[-1] == original
            initial = view(identifier)
            scoped_run = run_uuid(initial)
            apply("alice", vote(identifier, scoped_run, str(uuid.uuid4())), success=False)
            apply("dana", vote(identifier, scoped_run, str(uuid.uuid4())), success=False)
            apply("bob", {"operation": "pause", "id": identifier, "run_id": scoped_run}, success=False)
            run("apply", state, "bob", submitted, success=False)
            assert not list(mailbox.glob("*.json")), "ticks alone cannot execute Activities"

            active = ReminderServer(binary, state)
            key, attempt = active.checkpoint(created=True)
            record_path = mailbox / f"{key}.json"
            external_bytes = record_path.read_bytes()
            record = json.loads(external_bytes)
            assert record["purchase_id"] == identifier and record["run_id"] == initial["run_id"]
            active.finish(crash=True)  # after both file and directory fsync, before completion
            active = None
            orphan_sessions = set(state.iterdir())
            assert orphan_sessions
            run("get", state, "alice", identifier, success=False)  # live lease is fenced
            time.sleep(32)  # documented signed node lease is 30 seconds
            recovered = view(identifier)
            assert recovered["run_id"] == initial["run_id"]
            assert recovered["state"]["reminder"]["status"] == "queued", recovered
            active = ReminderServer(binary, state)
            repeated_key, repeated_attempt = active.checkpoint(created=False)
            assert repeated_key == key and repeated_attempt > attempt
            active.finish()  # accepted completion must finish during drain
            active = None
            delivered = view(identifier)
            assert delivered["state"]["reminder"]["status"] == "delivered", delivered
            assert delivered["state"]["reminder"]["receipt"]["key"] == key
            assert record_path.read_bytes() == external_bytes and len(list(mailbox.glob("*.json"))) == 1
            resolved = run("resolve", state, "alice", submitted)[-1]
            assert resolved["commit_sequence"] == original["receipt"]["commit_sequence"]

            signal_id = str(uuid.uuid4())
            bob_record, bob = apply("bob", vote(identifier, scoped_run, signal_id))
            assert run("apply", state, "bob", bob_record)[-1] == bob
            _, duplicate = apply("bob", vote(identifier, scoped_run, signal_id))
            assert "Duplicate" in duplicate["outcome"], duplicate
            conflict_record, conflict = apply("bob", vote(identifier, scoped_run, signal_id, "reject"), success=False)
            assert "IdentityConflict" in conflict["outcome"], conflict
            assert run("apply", state, "bob", conflict_record, success=False)[-1] == conflict
            assert run("resolve", state, "bob", conflict_record)[-1]["outcome"] == conflict["outcome"]
            apply("bob", vote(identifier, scoped_run, str(uuid.uuid4()), "reject"))
            assert view(identifier)["state"]["votes"] == {"bob": "approve"}
            apply("carol", vote(identifier, scoped_run, str(uuid.uuid4())))
            completed = view(identifier)
            assert completed["status"] == "completed" and completed["state"]["phase"] == "approved"
            terminal_record, terminal = apply("carol", vote(identifier, scoped_run, str(uuid.uuid4())), success=False)
            assert "NotRunning" in terminal["outcome"], terminal
            assert run("apply", state, "carol", terminal_record, success=False)[-1] == terminal
            entries, cursor = [], "-"
            while True:
                page = run("history", state, "alice", identifier, scoped_run, cursor, 1)[-1]["page"]
                assert page["run_id"] == initial["run_id"] and len(page["entries"]) <= 1
                entries.extend(page["entries"])
                if page["next"] is None:
                    break
                assert page["next"] != cursor
                cursor = page["next"]
            assert entries == completed["state"]["audit"]
            run("history", state, "alice", identifier, scoped_run, "-", 11, success=False)

            restarted_record, _ = apply("alice", {**purchase(identifier, approvers=["dana"]),
                                                   "operation": "restart", "run_id": scoped_run})
            fresh = view(identifier)
            assert fresh["run_id"] != initial["run_id"] and not fresh["state"]["votes"]
            assert run("resolve", state, "bob", bob_record)[-1]["commit_sequence"] == bob["receipt"]["commit_sequence"]
            run("apply", state, "bob", bob_record, success=False)  # no longer assigned
            _, stale = apply("dana", vote(identifier, scoped_run, str(uuid.uuid4())), success=False)
            assert "RunMismatch" in stale["outcome"], stale
            assert run("apply", state, "alice", submitted)[-1] == original
            assert view(identifier)["run_id"] == fresh["run_id"]
            run("history", state, "alice", identifier, scoped_run, "-", 1, success=False)
            current_run = run_uuid(fresh)
            for operation, status in [("pause", "paused"), ("resume", "running"), ("cancel", "cancelled")]:
                retained, outcome = apply("alice", {"operation": operation, "id": identifier, "run_id": current_run})
                assert view(identifier)["status"] == status
                assert run("apply", state, "alice", retained)[-1] == outcome
            assert run("resolve", state, "alice", restarted_record)[-1]["resolution"] == "committed"

            late_id = str(uuid.uuid4())
            retained, _ = apply("alice", purchase(late_id, approvers=["bob"], timeout_ms=30000,
                                                 remind_in_ms=29900))
            late_view = view(late_id)
            late_vote = mutation("bob", vote(late_id, run_uuid(late_view), str(uuid.uuid4())))
            deadline = late_view["state"]["purchase"]["deadline_ms"]
            time.sleep(max(0, (deadline - time.time() * 1000) / 1000) + 0.1)
            # Native maintenance may win the race before this command is dispatched.
            result = subprocess.run([binary, "apply", str(state), "bob", str(late_vote)],
                                    capture_output=True, text=True, timeout=75)
            events = [json.loads(line) for line in result.stdout.splitlines() if line]
            assert events and ("Applied" in events[-1]["outcome"] or "NotRunning" in events[-1]["outcome"])
            assert run("apply", state, "bob", late_vote, success=result.returncode == 0)[-1] == events[-1]
            timed_out = view(late_id)
            assert timed_out["state"]["phase"] == "timed_out" and not timed_out["state"]["votes"]
            assert sum(entry["event"] == "timed_out" for entry in timed_out["state"]["audit"]) == 1
            assert run("resolve", state, "alice", retained)[-1]["resolution"] == "committed"
            assert set(state.iterdir()) == orphan_sessions, "drain must preserve only interrupted-session evidence"
            assert len(list(mailbox.glob("*.json"))) == 1
            print(json.dumps({"scenario": "passed", "purchase": identifier, "mailbox_key": key,
                              "checks": ["persistent-s3", "retained-outcomes", "principal-authorization",
                                         "owned-timers", "kill-after-external-fsync", "live-owner-refusal",
                                         "expired-owner-takeover", "same-key-activity-retry", "one-external-record",
                                         "activity-drain", "immutable-human-votes", "signal-deduplication",
                                         "durable-identity-conflict", "bounded-run-pinned-history",
                                         "terminal-rejection", "restart-reassignment", "old-vote-resolution",
                                         "stale-run-rejection", "pause-resume-cancel", "deadline-precedence",
                                         "error-path-drain"]}))
        finally:
            if active:
                active.close()


if __name__ == "__main__":
    main()
