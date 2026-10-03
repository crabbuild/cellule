#!/usr/bin/env python3
"""Actual Blob publication-before-link and signed summary recovery across process loss.

Run only in CI or an isolated snapshot with private authoritative storage.
"""
import json
import os
from pathlib import Path
import selectors
import shutil
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
                        raise RuntimeError("tracker diagnostics exceeded one MiB")
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
                assert b"interrupted; retain the original tracker request" in self.output["stderr"]

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)


def main():
    binary = str(Path(sys.argv[1]).resolve())
    active = None
    started = time.monotonic()
    checks = []
    with tempfile.TemporaryDirectory(prefix="cellule-project-tracker-") as temporary:
        root = Path(temporary)
        state, successor = root / "source", root / "successor"
        tenant = "tenant-" + uuid.uuid4().hex[:20]
        source = root / "notes.bin"
        source.write_bytes(bytes([255]) * 65536)
        roster = root / "roster.json"
        roster.write_text(json.dumps({"tenant": tenant, "projects": ["roadmap"]}))

        def run(*arguments, success=True):
            value = subprocess.run([binary, *map(str, arguments)], capture_output=True, text=True, timeout=75)
            if bool(value.returncode == 0) != success:
                raise RuntimeError(f"{arguments}: exit {value.returncode}: {value.stdout}: {value.stderr}")
            assert len(value.stdout) + len(value.stderr) <= 1048576
            return [json.loads(line) for line in value.stdout.splitlines() if line]

        def mutation(operation, project="roadmap", **fields):
            input_path, retained = root / (uuid.uuid4().hex + ".input.json"), root / (uuid.uuid4().hex + ".request.json")
            input_path.write_text(json.dumps({"tenant": tenant, "change": {"project": project, "mutation": {"operation": operation, **fields}}}))
            run("prepare", input_path, retained)
            run("prepare", input_path, retained, success=False)
            return retained

        def fields(status="open"):
            return {"title": "Process-qualified issue", "description": "Independent publication and references", "assignee": "alice", "status": status}

        def prepare_attachment(id, revision, name="notes.bin"):
            retained = root / (uuid.uuid4().hex + ".plan.json")
            run("prepare-attachment", tenant, "roadmap", "one", id, name, source, revision, retained)
            run("prepare-attachment", tenant, "roadmap", "one", id, name, source, revision, retained, success=False)
            return retained

        def issue_state(directory=successor):
            return run("issue", directory, tenant, "roadmap", "one")[0]["issue"]

        try:
            original = mutation("create", name="Roadmap")
            first = run("apply", state, original)[0]
            assert run("apply", successor, original)[0] == first
            resolved = run("resolve", successor, original)[0]
            assert resolved["outcome"] == first["outcome"]
            assert resolved["commit_sequence"] == first["receipt"]["commit_sequence"]
            run("apply", state, mutation("create_issue", id="one", fields=fields()))
            assert run("progress", state, tenant, "roadmap")[0]["progress"]["state"] == "pending"
            assert run("lookup", state, tenant, "roadmap")[0]["summary"] is None
            checks += ["retained-request-noclobber", "original-native-outcome", "source-pending-is-explicit", "dashboard-absence-does-not-prove-source-absence"]

            plan = prepare_attachment("guide", 1)
            active = Process(binary, ["publish", state, plan, 10000])
            publication = active.checkpoint("attachment_published")["publication"]
            active.finish(crash=True)
            active = None
            orphan_files = list(state.rglob("*.sqlite"))
            assert orphan_files, "crash must retain interrupted local evidence"
            run("download", successor, plan, root / "live-owner.bin", success=False)
            time.sleep(32)  # Native writer/session lease expiry is required evidence.
            download = root / "restored.bin"
            restored = run("download", successor, plan, download)[0]
            assert restored["publication"] == publication and download.read_bytes() == source.read_bytes()
            assert issue_state()["attachments"] == []
            linked = run("reconcile", successor, plan)[0]
            assert linked["outcome"]["decision"] == "applied"
            assert run("reconcile", state, plan)[0] == linked
            resolved = run("resolve-link", successor, plan)[0]
            assert resolved["outcome"] == linked["outcome"] and resolved["commit_sequence"] == linked["receipt"]["commit_sequence"]
            assert issue_state()["attachments"] == [publication]
            assert all(path.exists() for path in orphan_files)
            checks += ["crash-after-actual-Blob-publication", "live-Blob-owner-refusal", "lease-expiry-takeover", "publication-before-link-reconciled", "original-link-replay", "original-link-outcome-resolution", "complete-byte-restore", "orphan-session-preservation"]

            stale = prepare_attachment("later", 2)
            later_publication = run("publish", successor, stale)[0]["publication"]
            run("apply", successor, mutation("edit_issue", id="one", expected_revision=2, fields=fields("closed")))
            refusal = run("reconcile", successor, stale, success=False)[0]
            assert refusal["outcome"]["decision"] == "conflict"
            assert run("reconcile", state, stale, success=False)[0] == refusal
            assert run("resolve-link", successor, stale)[0]["outcome"] == refusal["outcome"]
            assert issue_state()["attachments"] == [publication]
            explicit = prepare_attachment("later", 3)
            assert run("publish", successor, explicit)[0]["publication"] == later_publication
            run("reconcile", successor, explicit)
            assert run("resolve-link", successor, stale)[0]["outcome"]["decision"] == "conflict"
            assert issue_state()["attachments"] == [publication, later_publication]
            checks += ["stale-link-precondition-preserved", "durable-link-refusal-replays", "explicit-operator-link-precondition", "old-refusal-remains-original"]

            latest = run("get", successor, tenant, "roadmap")[0]["project"]
            latest_revision = latest["revision"]
            active = Process(binary, ["serve", successor, roster, 600, 4000, 10000])
            active.checkpoint("ready")
            new = active.checkpoint("projected", lambda value: value["progress"]["summary"]["revision"] == latest_revision)["progress"]
            old = active.checkpoint("projected", lambda value: value["progress"]["outcome"] == "stale")["progress"]
            assert old["summary"]["revision"] < new["summary"]["revision"]
            active.finish(crash=True)
            active = None
            run("get", state, tenant, "roadmap", success=False)
            time.sleep(32)  # Both writer/session and the original native Effect lease must expire.
            active = Process(binary, ["serve", state, roster, 600, 0, 0, "drop-reply"])
            active.checkpoint("ready")
            replay = active.checkpoint("projected", lambda value: value["progress"]["effect_id"] == old["effect_id"])["progress"]
            assert replay == old, "native inbox replay must preserve original dashboard receipt and decision"
            active.finish()
            active = None
            effect = run("effect", state, tenant, "roadmap", old["effect_id"])[0]["effect"]
            assert effect["state"] == "delivered" and effect["attempt"] == 2 and effect["result"] == "stale", effect
            progress = run("progress", state, tenant, "roadmap")[0]["progress"]
            projected = run("lookup", state, tenant, "roadmap")[0]["summary"]
            assert progress["state"] == "delivered" and projected == progress["version"]["summary"]
            assert projected["revision"] == latest_revision and projected["attachments"] == 2 and projected["open"] == 0
            checks += ["actual-out-of-order-signed-delivery", "older-summary-does-not-regress", "crash-after-dashboard-publication", "same-native-Effect-reclaimed", "original-inbox-receipt-restored", "lost-reply-inbox-resolution", "source-ledger-attempt-and-settlement", "dashboard-source-digest-converges", "graceful-serving-drain"]

            run("apply", state, mutation("create", project="operations", name="Operations"))
            roster.write_text(json.dumps({"tenant": tenant, "projects": ["roadmap", "operations"]}))
            run("serve", state, roster, 2, 0, 0)
            page = run("dashboard", state, tenant)[0]["dashboard"]
            assert [value["project"] for value in page["projects"]] == ["operations", "roadmap"]
            second = run("dashboard", state, tenant, "operations", 1)[0]["dashboard"]
            assert [value["project"] for value in second["projects"]] == ["roadmap"] and second["next"] is None
            run("dashboard", state, tenant, "operations", 0, success=False)
            assert run("get", state, "other-tenant", "roadmap")[0]["project"] is None
            assert run("lookup", state, "other-tenant", "roadmap")[0]["summary"] is None
            checks += ["independent-project-aggregate", "bounded-canonical-dashboard-pages", "tenant-source-and-dashboard-isolation"]

            before = run("get", state, tenant, "roadmap")[0]["project"]
            projected = run("lookup", state, tenant, "roadmap")[0]["summary"]
            shutil.rmtree(state)
            assert run("get", state, tenant, "roadmap")[0]["project"] == before
            assert run("lookup", state, tenant, "roadmap")[0]["summary"] == projected
            assert run("effect", state, tenant, "roadmap", old["effect_id"])[0]["effect"] == effect
            final_download = root / "cold-notes.bin"
            assert run("download", state, plan, final_download)[0]["publication"] == publication
            assert final_download.read_bytes() == source.read_bytes()
            assert run("resolve-link", state, plan)[0]["outcome"] == linked["outcome"]
            checks += ["cold-project-reference-restore", "cold-dashboard-and-inbox-restore", "cold-native-ledger-restore", "cold-byte-identical-Blob-restore", "cold-original-link-resolution"]

            demo = root / "interrupted-demo"
            active = Process(binary, ["demo", demo])
            accepted = active.checkpoint("projected")["progress"]
            active.finish(interrupted=True)
            active = None
            settled = run("effect", demo, "cookbook-demo", "roadmap", accepted["effect_id"])[0]["effect"]
            assert settled["state"] == "delivered" and settled["attempt"] == 1 and settled["result"] == accepted["outcome"], settled
            plans = list(demo.glob("attachment-*.json"))
            assert len(plans) == 1, "interrupted demo must retain its original plan"
            retained = root / "interrupted-demo.plan.json"
            shutil.copyfile(plans[0], retained)
            frozen = json.loads(retained.read_text())
            before = run("get", demo, "cookbook-demo", "roadmap")[0]["project"]
            original = run("resolve-link", demo, retained)[0]
            assert original["resolution"] == "committed" and original["outcome"]["decision"] == "applied"
            shutil.rmtree(demo)
            assert run("get", demo, "cookbook-demo", "roadmap")[0]["project"] == before
            assert run("effect", demo, "cookbook-demo", "roadmap", accepted["effect_id"])[0]["effect"] == settled
            assert run("resolve-link", demo, retained)[0] == original
            downloaded = root / "interrupted-demo.bin"
            run("download", demo, retained, downloaded)
            assert downloaded.read_bytes() == bytes(frozen["plan"]["bytes"])
            checks += ["demo-signal-after-real-dashboard-publication", "accepted-native-Effect-settled-on-demo-drain", "retained-interrupted-demo-plan", "cold-interrupted-demo-source-restore", "cold-interrupted-demo-ledger-restore", "cold-original-demo-link-resolution", "cold-interrupted-demo-byte-restore"]
            print(json.dumps({"scenario": "passed", "application": "project-tracker", "checks": checks,
                              "seconds": round(time.monotonic() - started, 3)}))
        finally:
            if active:
                active.close()


if __name__ == "__main__":
    main()
