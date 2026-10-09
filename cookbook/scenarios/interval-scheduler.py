#!/usr/bin/env python3
"""Persistent Cron/effect recovery, including SIGKILL after destination publication.

Run in CI or an isolated source snapshot with private local S3 storage. Do not
run another interval-scheduler instance against the same fixed installation.
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

from process import until


def main():
    binary = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="cellule-interval-scheduler-") as temporary:
        root = Path(temporary)
        state = str(root / "state")
        active = None

        def run(*args, success=True):
            result = subprocess.run([binary, *map(str, args)], capture_output=True, text=True, timeout=75)
            if success and result.returncode:
                raise RuntimeError(f"{args}: {result.stderr}")
            if not success and result.returncode == 0:
                raise AssertionError(f"{args}: unexpectedly succeeded")
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def mutation(change):
            name = uuid.uuid4().hex
            source = root / f"{name}.json"
            retained = root / f"{name}.mutation.json"
            source.write_text(json.dumps(change))
            run("prepare", source, retained)
            return retained

        def apply(change):
            record = mutation(change)
            return run("apply", state, record)[-1]

        def get(schedule):
            return run("get", state, schedule)[-1]["schedule"]

        def deliveries(schedule, after="-", limit=100):
            return run("deliveries", state, schedule, after, limit)[-1]["page"]

        def serve(seconds, delay=0, lose_reply=False):
            args = ["serve", state, seconds, delay]
            if lose_reply:
                args.append("lose-reply")
            return run(*args)

        def assert_complete(schedule, source, definition, expected_due):
            first = deliveries(schedule, "-", 2)
            rows = first["deliveries"]
            cursor = first["next"]
            while cursor is not None:
                page = deliveries(schedule, cursor, 2)
                rows.extend(page["deliveries"])
                cursor = page["next"]
            assert len(rows) == source["occurrence"], (source, rows)
            assert len({row["row"] for row in rows}) == len(rows)
            assert {row["definition"] for row in rows} == {definition}
            assert sorted(row["occurrence"] for row in rows) == list(range(1, source["occurrence"] + 1))
            for row in rows:
                assert row["scheduled_at_ms"] == expected_due[row["occurrence"]]
            return rows

        try:
            schedule = str(uuid.uuid4())
            change = {"operation": "upsert", "id": schedule, "reminder": {"title": "Durable reminder", "message": "Inspect every due occurrence"}, "interval_ms": 1000, "start_in_ms": 0}
            retained = mutation(change)
            frozen = json.loads(retained.read_text())
            assert frozen["change"]["next_due_ms"] == frozen["issued_at_ms"]
            assert run("resolve", state, retained)[-1]["resolution"] == "absent"
            sent = run("apply", state, retained)[-1]
            assert run("apply", state, retained)[-1] == sent
            listed = []
            for shard in range(2):
                listed.extend(run("list", state, shard, "-", 100)[-1]["page"]["schedules"])
            assert any(item["id"] == schedule for item in listed)
            assert not deliveries(schedule)["deliveries"], "compilation and ticks do not install effect runners"

            errors_path = root / "interrupted-server.stderr"
            with errors_path.open("w") as errors:
                active = subprocess.Popen([binary, "serve", state, "60", "10000"], stdout=subprocess.PIPE, stderr=errors, bufsize=0)
                selector = selectors.DefaultSelector()
                selector.register(active.stdout, selectors.EVENT_READ)
                buffered = b""
                checkpoint = None
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    for key, _ in selector.select(timeout=1):
                        chunk = os.read(key.fileobj.fileno(), 4096)
                        if not chunk:
                            raise RuntimeError(f"server exited before checkpoint: {errors_path.read_text()}")
                        buffered += chunk
                        while b"\n" in buffered:
                            line, buffered = buffered.split(b"\n", 1)
                            event = json.loads(line)
                            if event.get("event") == "destination_published" and event["progress"]["schedule"] == schedule:
                                checkpoint = event["progress"]
                                break
                    if checkpoint:
                        break
                selector.close()
                assert checkpoint, "no durable destination publication checkpoint"
                # Intentional crash after the inbox committed, before its reply
                # returned to the source's native effect supervisor.
                active.kill()
                active.wait(timeout=10)
                active.stdout.close()
                active = None
            orphan_sessions = set((root / "state").iterdir())
            assert orphan_sessions
            run("get", state, schedule, success=False)  # live node lease cannot be stolen
            time.sleep(32)
            apply({"operation": "pause", "id": schedule})
            paused = get(schedule)
            assert not paused["enabled"] and paused["occurrence"] >= 1
            before = deliveries(schedule)["deliveries"]
            assert any(row["row"] == checkpoint["row"] for row in before)
            before_occurrences = {row["occurrence"] for row in before}

            def recovered_all(events):
                progress = [event["progress"] for event in events if event.get("event") == "destination_published" and event["progress"]["schedule"] == schedule]
                occurrences = before_occurrences | {item["occurrence"] for item in progress}
                return occurrences == set(range(1, paused["occurrence"] + 1)) and any(item["effect_id"] == checkpoint["effect_id"] for item in progress)

            recovered = until(binary, ["serve", state, 240], recovered_all, root / "recovery.stderr")
            repeated = [event["progress"] for event in recovered if event.get("event") == "destination_published" and event["progress"]["effect_id"] == checkpoint["effect_id"]]
            assert len(repeated) == 1, recovered
            assert repeated[0]["row"] == checkpoint["row"]
            assert repeated[0]["destination_commit_sequence"] == checkpoint["destination_commit_sequence"]
            due = frozen["change"]["next_due_ms"]
            expected_due = {occurrence: due + (occurrence - 1) * 1000 for occurrence in range(1, paused["occurrence"] + 1)}
            rows = assert_complete(schedule, paused, frozen["request_id"], expected_due)
            time.sleep(1.1)
            assert get(schedule)["occurrence"] == paused["occurrence"]
            resolved = run("resolve", state, retained)[-1]
            assert resolved["resolution"] == "committed" and resolved["commit_sequence"] == sent["receipt"]["commit_sequence"]

            resumed_record = mutation({"operation": "resume", "id": schedule, "start_in_ms": 100})
            resumed_frozen = json.loads(resumed_record.read_text())
            run("apply", state, resumed_record)
            serve(2, lose_reply=True)
            apply({"operation": "pause", "id": schedule})
            resumed = get(schedule)
            assert resumed["occurrence"] > paused["occurrence"]
            assert resumed["generation"] > paused["generation"]
            existing = {row["occurrence"] for row in deliveries(schedule)["deliveries"]}
            if len(existing) < resumed["occurrence"]:
                until(binary, ["serve", state, 240], lambda events: existing | {
                    event["progress"]["occurrence"] for event in events
                    if event.get("event") == "destination_published" and event["progress"]["schedule"] == schedule
                } == set(range(1, resumed["occurrence"] + 1)), root / "resume.stderr")
            for occurrence in range(paused["occurrence"] + 1, resumed["occurrence"] + 1):
                expected_due[occurrence] = resumed_frozen["change"]["next_due_ms"] + (occurrence - paused["occurrence"] - 1) * 1000
            rows = assert_complete(schedule, resumed, frozen["request_id"], expected_due)
            deletion = mutation({"operation": "delete", "id": schedule})
            deleted = run("apply", state, deletion)[-1]
            assert deleted["outcome"]["status"] == "deleted" and get(schedule) is None
            assert run("apply", state, deletion)[-1] == deleted
            missing = mutation({"operation": "pause", "id": schedule})
            rejected = run("apply", state, missing, success=False)[-1]
            assert rejected["outcome"]["status"] == "not_found"
            assert run("apply", state, missing, success=False)[-1] == rejected
            assert run("resolve", state, missing)[-1]["outcome"]["status"] == "not_found"

            recreated = mutation({**change, "interval_ms": 60_000})
            new_definition = json.loads(recreated.read_text())["request_id"]
            run("apply", state, recreated)
            # Replaying an old successful deletion cannot delete this lifetime.
            assert run("apply", state, deletion)[-1] == deleted
            assert get(schedule)["definition"] == new_definition
            serve(3)
            fresh = deliveries(schedule, rows[-1]["row"])["deliveries"]
            assert len(fresh) == 1 and fresh[0]["definition"] == new_definition
            assert fresh[0]["generation"] == 1 and fresh[0]["occurrence"] == 1
            assert fresh[0]["row"] not in {row["row"] for row in rows}
            apply({"operation": "delete", "id": schedule})
            assert set((root / "state").iterdir()) == orphan_sessions, "drain must release its working files and preserve interrupted-session evidence"
            print(json.dumps({"scenario": "passed", "schedule": schedule, "checks": ["persistent-s3", "independent-processes", "frozen-due-time", "retained-replay", "owned-ticks", "signed-effect-delivery", "kill-after-inbox-publication", "live-owner-refusal", "expired-owner-takeover", "inbox-redelivery-deduplication", "no-lost-due-occurrences", "bounded-pages", "pause", "resume", "lost-reply-resolution", "delete-recreate-lifetimes", "durable-not-found", "error-path-drain"]}))
        finally:
            if active and active.poll() is None:
                active.send_signal(signal.SIGTERM)
                try:
                    active.wait(timeout=25)
                except subprocess.TimeoutExpired:
                    active.kill()
                    active.wait(timeout=10)


if __name__ == "__main__":
    main()
