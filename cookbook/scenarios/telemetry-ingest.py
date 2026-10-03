#!/usr/bin/env python3
"""Native Queue partial-batch, original-audit, and signed-summary recovery across real crashes.

Run only in CI or an isolated snapshot with private authoritative storage.
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
                    value = json.loads(line)
                    if value.get("event") == event and predicate(value):
                        return value
                for selected, _ in selector.select(timeout=1):
                    chunk = os.read(selected.fileobj.fileno(), 8192)
                    if not chunk:
                        selector.unregister(selected.fileobj)
                        if not selector.get_map():
                            raise RuntimeError(f"process exited before {event}: {self.output}")
                        continue
                    self.output[selected.data].extend(chunk)
                    if sum(map(len, self.output.values())) > 1048576:
                        raise RuntimeError("telemetry diagnostics exceeded one MiB")
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
        if not crash:
            assert self.process.returncode == (1 if interrupted else 0), self.output
            records = [json.loads(line) for line in bytes(self.output["stdout"]).splitlines() if line]
            assert sum(value.get("event") == "drained" for value in records) == 1
            if interrupted:
                assert b"interrupted; retain the original telemetry request" in self.output["stderr"]

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)

def main():
    binary = str(Path(sys.argv[1]).resolve())
    active = None
    checks = []
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="cellule-telemetry-ingest-") as temporary:
        root = Path(temporary)
        state, successor, cold = root / "source", root / "successor", root / "cold"
        tenant = "tenant-" + uuid.uuid4().hex[:20]
        roster = root / "roster.json"
        roster.write_text(json.dumps({"tenant": tenant, "devices": ["thermometer", "barometer"]}))

        def run(*args, success=True):
            result = subprocess.run([binary, *map(str, args)], capture_output=True, text=True, timeout=75)
            if bool(result.returncode == 0) != success:
                raise RuntimeError(f"{args}: exit {result.returncode}: {result.stdout}: {result.stderr}")
            assert len(result.stdout) + len(result.stderr) <= 1048576
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def prepare(operation):
            input_path, request = root / (uuid.uuid4().hex + ".input.json"), root / (uuid.uuid4().hex + ".request.json")
            input_path.write_text(json.dumps({"tenant": tenant, "operation": operation}))
            run("prepare", input_path, request)
            run("prepare", input_path, request, success=False)
            return request

        def event(sequence, at_ms, value, device="thermometer"):
            return {"device": device, "sequence": sequence, "at_ms": at_ms, "value_milli": value}

        def submit(events, directory=successor):
            batch = {"id": str(uuid.uuid4()), "events": events}
            request = prepare({"type": "submit", "batch": batch})
            result = run("apply", directory, request)[0]
            return batch, request, result["outcome"]["message"]

        def controls(**values):
            path = root / (uuid.uuid4().hex + ".controls.json")
            path.write_text(json.dumps(values))
            return path

        def get(directory=successor, device="thermometer"):
            return run("device", directory, tenant, device)[0]["device"]

        def progress(directory=successor, device="thermometer"):
            return run("progress", directory, tenant, device)[0]["progress"]

        def assert_source(value, count, highest, contiguous, reordered, total):
            assert len(value["events"]) == count and value["revision"] == count + 1
            snapshot = progress()["version"]["snapshot"]
            assert (snapshot["accepted"], snapshot["max_sequence"], snapshot["contiguous_sequence"], snapshot["reordered"]) == (count, highest, contiguous, reordered)
            assert sum(bucket["sum_milli"] for bucket in snapshot["buckets"]) == total
            assert snapshot["latest"]["sequence"] == highest
            return snapshot

        try:
            original = prepare({"type": "register", "device": "thermometer", "window": {"start_ms": 120000, "minutes": 16}})
            first = run("apply", state, original)[0]
            assert run("apply", successor, original)[0] == first
            resolved = run("resolve", successor, original)[0]
            assert resolved["outcome"] == first["outcome"] and resolved["commit_sequence"] == first["receipt"]["commit_sequence"]
            run("apply", successor, prepare({"type": "register", "device": "barometer", "window": {"start_ms": 120000, "minutes": 16}}))
            assert progress()["state"] == "pending"
            assert run("lookup", successor, tenant, "thermometer")[0]["summary"] is None
            checks += ["retained-request-noclobber", "original-source-outcome", "source-pending-explicit", "receiver-absence-not-source-absence"]

            batch, request, message = submit([event(3, 180500, 20), event(1, 120500, 10), event(1, 120500, -5, "barometer"), event(1, 120500, 999, "not-rostered")])
            sent = run("apply", state, request)[0]
            assert sent["outcome"]["message"] == message
            assert run("resolve", state, request)[0]["outcome"] == sent["outcome"]
            active = Process(binary, ["serve", successor, roster, 600, controls(batch=batch["id"], after_event_ms=10000)])
            active.checkpoint("ready")
            partial = active.checkpoint("consumer", lambda v: "EventPublished" in v["progress"] and v["progress"]["EventPublished"]["index"] == 0)["progress"]["EventPublished"]
            assert partial["message"] == message and partial["attempt"] == 1
            assert partial["result"]["outcome"]["decision"] == "applied"
            assert partial["result"]["outcome"]["version"]["snapshot"]["accepted"] == 1
            active.finish(crash=True)
            active = None
            orphan = list(successor.rglob("*.sqlite"))
            assert orphan, "actual crash must retain local source evidence"
            run("device", state, tenant, "thermometer", success=False)
            time.sleep(32)  # Native writer/session and Queue claim leases must expire.
            assert run("batch", state, tenant, message)[0]["batch"] is None
            assert len(get(state)["events"]) == 1
            active = Process(binary, ["serve", state, roster, 600, controls(drop_reply=True)])
            active.checkpoint("ready")
            recovered = active.checkpoint("consumer", lambda v: "AuditPublished" in v["progress"] and v["progress"]["AuditPublished"]["completion"]["message"] == message)["progress"]["AuditPublished"]
            assert recovered["attempt"] == 2
            completion = recovered["completion"]
            assert completion["batch"] == batch
            assert [v["outcome"]["decision"] for v in completion["results"]] == ["duplicate", "applied", "applied", "not_in_roster"]
            assert completion["results"][3]["source"] is None
            active.finish()
            active = None
            snapshot = assert_source(get(), 2, 3, 1, 1, 30)
            assert snapshot["latest"] == event(3, 180500, 20)
            assert all(path.exists() for path in orphan)
            assert run("batch", successor, tenant, message)[0]["batch"] == completion
            checks += ["crash-after-first-source-publication", "cross-Cell-batch-is-partial", "live-owner-refusal", "lease-expiry-takeover", "same-physical-message-attempt-two", "permanent-event-dedup-after-partial-batch", "complete-payload-audit", "explicit-roster-exclusion", "reordered-event-counts-without-latest-regression", "orphan-session-preserved"]

            batch_two, _, message_two = submit([event(2, 120750, 7), event(4, 180750, 30)])
            active = Process(binary, ["serve", successor, roster, 600, controls(batch=batch_two["id"], before_ack_ms=10000)])
            active.checkpoint("ready")
            published = active.checkpoint("consumer", lambda v: "AuditPublished" in v["progress"] and v["progress"]["AuditPublished"]["completion"]["message"] == message_two)["progress"]["AuditPublished"]
            assert published["attempt"] == 1
            original_completion = published["completion"]
            assert [v["outcome"]["decision"] for v in original_completion["results"]] == ["applied", "applied"]
            active.finish(crash=True)
            active = None
            time.sleep(32)
            before = get(state)
            assert run("batch", state, tenant, message_two)[0]["batch"] == original_completion
            active = Process(binary, ["serve", state, roster, 600])
            active.checkpoint("ready")
            replay = active.checkpoint("consumer", lambda v: "AuditPublished" in v["progress"] and v["progress"]["AuditPublished"]["completion"]["message"] == message_two)["progress"]["AuditPublished"]
            assert replay["attempt"] == 2 and replay["completion"] == original_completion
            active.finish()
            active = None
            assert get() == before
            snapshot = assert_source(get(), 4, 4, 4, 2, 67)
            checks += ["crash-after-complete-audit-before-ack", "attempt-two-audit-replay", "original-entry-answers-and-receipts-preserved", "audit-recovery-does-not-write-devices", "sequence-gap-filled"]

            latest_request = prepare({"type": "record", "event": event(5, 180900, 40)})
            latest = run("apply", successor, latest_request)[0]
            effect = bytes(latest["outcome"]["version"]["effect_id"]).hex()
            latest_snapshot = latest["outcome"]["version"]["snapshot"]
            active = Process(binary, ["serve", successor, roster, 600, controls(effect=effect, after_summary_ms=10000, drop_reply=True)])
            active.checkpoint("ready")
            delivery = active.checkpoint("summary", lambda v: v["progress"]["effect_id"] == effect)["progress"]
            assert delivery["summary"] == latest_snapshot and delivery["outcome"] == "applied"
            active.finish(crash=True)
            active = None
            time.sleep(32)
            assert run("lookup", state, tenant, "thermometer")[0]["summary"] == latest_snapshot
            leased = run("effect", state, tenant, "thermometer", effect)[0]["effect"]
            assert leased["attempt"] == 1 and leased["result"] is None, leased
            if leased["state"] == "Ready":
                assert not leased["token_present"] and leased["lease_until_ms"] is None, leased
            else:
                assert leased["state"] == "Leased" and leased["lease_until_ms"] <= int(time.time() * 1000), leased
            active = Process(binary, ["serve", state, roster, 600])
            active.checkpoint("ready")
            settled_progress = active.checkpoint("projection_progress", lambda v: v["device"] == "thermometer" and v["progress"] is not None and bytes(v["progress"]["version"]["effect_id"]).hex() == effect and v["progress"]["state"] == "delivered")["progress"]
            assert settled_progress["attempts"] == 2
            active.finish()
            active = None
            settled = run("effect", successor, tenant, "thermometer", effect)[0]["effect"]
            assert settled["state"] == "Delivered" and settled["attempt"] == 2 and settled["result"] == [1, 1]
            assert run("lookup", successor, tenant, "thermometer")[0]["summary"] == latest_snapshot
            checks += ["crash-after-actual-signed-summary-publication", "receiver-survives-lost-reply", "original-intent-attempt-two", "native-original-inbox-resolution", "no-double-aggregation"]

            refusal_request = prepare({"type": "record", "event": event(5, 180900, 41)})
            refused = run("apply", successor, refusal_request, success=False)[0]
            assert refused["outcome"]["decision"] == "conflict"
            assert run("apply", state, refusal_request, success=False)[0] == refused
            assert run("resolve", state, refusal_request)[0]["outcome"] == refused["outcome"]
            outside = prepare({"type": "record", "event": event(6, 1080000, 1)})
            assert run("apply", successor, outside, success=False)[0]["outcome"]["decision"] == "outside_window"
            assert assert_source(get(), 5, 5, 5, 2, 107) == latest_snapshot
            _, _, repeated_message = submit(batch["events"])
            active = Process(binary, ["serve", successor, roster, 600])
            active.checkpoint("ready")
            repeated = active.checkpoint("consumer", lambda v: "AuditPublished" in v["progress"] and v["progress"]["AuditPublished"]["completion"]["message"] == repeated_message)["progress"]["AuditPublished"]["completion"]
            assert [v["outcome"]["decision"] for v in repeated["results"]] == ["duplicate", "duplicate", "duplicate", "not_in_roster"]
            active.finish()
            active = None
            assert run("lookup", cold, tenant, "thermometer")[0]["summary"] == latest_snapshot
            assert get(cold) == get()
            assert run("batch", cold, tenant, message)[0]["batch"] == completion
            assert run("batch", cold, tenant, message_two)[0]["batch"] == original_completion
            info = run("info", cold, tenant)[0]["info"]
            assert info["acked"] == 3 and info["ready"] == info["leased"] == info["dead"] == 0
            assert run("device", cold, "other-" + tenant, "thermometer")[0]["device"] is None
            assert run("lookup", cold, "other-" + tenant, "thermometer")[0]["summary"] is None
            assert run("batch", cold, "other-" + tenant, message)[0]["batch"] is None
            assert run("info", cold, "other-" + tenant)[0]["info"]["acked"] == 0
            page = run("summaries", cold, tenant, 0, 120000, 1)[0]["page"]
            assert len(page["buckets"]) <= 1
            run("summaries", cold, tenant, 2, success=False)
            checks += ["durable-conflicting-sequence-refusal", "original-refusal-resolution", "exclusive-window-end-refusal", "fresh-batch-permanent-source-dedup", "cold-device-audit-Queue-summary-restore", "tenant-isolation-all-read-domains", "bounded-summary-pagination"]

            demo_state = root / "demo"
            active = Process(binary, ["demo", demo_state])
            active.checkpoint("demo_plan")
            active_plan = demo_state / "demo-active.json"
            assert active_plan.is_file()
            retained_bytes = active_plan.read_bytes()
            frozen = json.loads(retained_bytes)
            active.checkpoint("summary", lambda value: value["progress"]["summary"]["device"] == frozen["devices"][0] and value["progress"]["summary"]["revision"] == frozen["expected"][0]["revision"])
            assert active_plan.read_bytes() == retained_bytes
            active.finish(interrupted=True)
            active = None
            assert active_plan.read_bytes() == retained_bytes
            resumed = run("demo", demo_state)
            completed = next(v for v in resumed if v.get("event") == "demo_complete")
            assert Path(completed["retained_plan"]).read_bytes() == retained_bytes and not active_plan.exists()
            repeated = run("demo", demo_state)
            devices = [v["summary"] for v in repeated if v.get("event") == "verified_device"]
            expected_counts = {key: len(state["events"]) + delta for key, state, delta in zip(frozen["devices"], frozen["expected"], [2, 1])}
            assert {value["device"]: value["accepted"] for value in devices} == expected_counts
            original_sum = sum(row["event"]["value_milli"] for state in frozen["expected"] for row in state["events"])
            assert sum(sum(b["sum_milli"] for b in value["buckets"]) for value in devices) == original_sum + 25000
            checks += ["SIGTERM-after-real-demo-publication", "owned-drain-keeps-original-demo-plan", "resume-original-demo-identities-and-sequences", "retained-repeat-demo-without-reset"]
            print(json.dumps({"checks": len(checks), "evidence": checks, "seconds": round(time.monotonic() - started, 2)}))
        finally:
            if active is not None:
                active.close()


if __name__ == "__main__":
    main()
