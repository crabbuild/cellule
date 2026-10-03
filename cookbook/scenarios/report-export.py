#!/usr/bin/env python3
"""Sealed CSV export under draft changes, native Activity redelivery, and cold restore.

Run in CI or an isolated source snapshot against a private local object provider.
"""
import csv
import io
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


class Server:
    def __init__(self, binary, state, port, delay):
        self.process = subprocess.Popen(
            [binary, "serve", str(state), str(port), "240"], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, bufsize=0,
            env={**os.environ, "CELLULE_EXPORT_AFTER_PAGE_MS": str(delay)},
        )
        self.output = {"stdout": bytearray(), "stderr": bytearray()}
        self.pending = b""

    def checkpoint(self, event, kind=None):
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(self.process.stderr, selectors.EVENT_READ, "stderr")
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                while b"\n" in self.pending:
                    line, self.pending = self.pending.split(b"\n", 1)
                    record = json.loads(line)
                    if record.get("event") == event and (kind is None or kind in record.get("completion", {})):
                        return record
                for selected, _ in selector.select(timeout=1):
                    chunk = os.read(selected.fileobj.fileno(), 4096)
                    if not chunk:
                        selector.unregister(selected.fileobj)
                        if not selector.get_map():
                            raise RuntimeError(f"server exited before {event}: {self.output}")
                        continue
                    self.output[selected.data].extend(chunk)
                    if sum(map(len, self.output.values())) > 262144:
                        raise RuntimeError("server diagnostics exceeded 256 KiB")
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
            raise RuntimeError(f"export server drain failed: {self.output}")

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)


def main():
    binary = str(Path(sys.argv[1]).resolve())
    active = None
    with tempfile.TemporaryDirectory(prefix="cellule-report-") as temporary:
        root = Path(temporary)
        state = root / "state"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}/"
        token = os.environ.get("CELLULE_EXPORT_TOKEN", "cellule-cookbook-local-export")

        def run(*arguments, success=True, env=None):
            result = subprocess.run([binary, *map(str, arguments)], capture_output=True,
                                    text=True, timeout=75, env=env)
            if bool(result.returncode == 0) != success:
                raise RuntimeError(f"{arguments}: exit {result.returncode}; {result.stderr}; {result.stdout}")
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def prepare(change):
            name = uuid.uuid4().hex
            source = root / f"{name}.change.json"
            request = root / f"{name}.mutation.json"
            source.write_text(json.dumps(change))
            run("prepare-data", source, request)
            run("prepare-data", source, request, success=False)
            return request

        def http(path, value, status=200):
            request = urllib.request.Request(endpoint + path, data=json.dumps(value).encode(),
                                             headers={"Content-Type": "application/json", "Authorization": "Bearer " + token})
            try:
                response = urllib.request.urlopen(request, timeout=15)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                assert response.status == status
                body = response.read(32769)
                assert len(body) <= 32768
                return json.loads(body)

        def view(identifier):
            return run("get", state, identifier)[-1]["view"]

        def snapshot(version):
            document = run("snapshot", state, version)[-1]
            path = root / f"{version}.snapshot.json"
            path.write_text(json.dumps(document))
            return document, path

        try:
            revision = run("info", state)[-1]["dataset"]["revision"]
            source_file = root / "sample.json"
            run("sample", 70, revision, source_file)
            original_rows = json.loads(source_file.read_text())["rows"]
            data_file = prepare(json.loads(source_file.read_text()))
            assert run("resolve-data", state, data_file)[-1]["resolution"] == "absent"
            replaced = run("data", state, data_file)[-1]
            assert run("data", state, data_file)[-1] == replaced
            revision = replaced["outcome"]["Applied"]
            version = str(uuid.uuid4())
            seal_file = root / "seal.json"
            run("prepare-seal", revision, version, seal_file)
            sealed = run("data", state, seal_file)[-1]
            source, source_path = snapshot(version)
            assert source["snapshot"] == sealed["outcome"]["Sealed"]
            traversed, after = [], 0
            while True:
                page = run("page", state, source_path, after)[-1]["page"]
                assert page["snapshot"] == source["snapshot"] and len(page["rows"]) <= 32
                traversed.extend(page["rows"])
                after = page["rows"][-1]["id"]
                if not page["more"]:
                    break
            assert traversed == original_rows
            assert 17 not in [row["id"] for row in traversed]

            request_file = root / "export.json"
            run("prepare-run", source_path, endpoint, request_file)
            request = json.loads(request_file.read_text())
            identifier = str(uuid.UUID(bytes=bytes(request["request"]["id"])))
            assert run("resolve", state, request_file)[-1]["resolution"] == "absent"
            original = run("start", state, request_file)[-1]
            assert run("start", state, request_file)[-1] == original
            initial = view(identifier)
            assert initial["state"]["rows"] == 0 and not initial["state"]["chunks"]
            active = Server(binary, state, port, 10000)
            active.checkpoint("ready")
            invalid = urllib.request.Request(endpoint + "draft", data=b"not-json",
                                             headers={"Content-Type": "application/json"})
            try:
                urllib.request.urlopen(invalid, timeout=5)
                raise AssertionError("unauthenticated draft request accepted")
            except urllib.error.HTTPError as response:
                assert response.code == 401
            published = active.checkpoint("export_published", "Page")
            first_chunk = published["completion"]["Page"]
            assert first_chunk["after"] == 0 and first_chunk["rows"] == 32 and published["native_attempt"] == 1

            changed_file = prepare({"operation": "put", "expected_revision": revision,
                                    "row": {"id": 1, "label": "Later draft", "units": 999}})
            changed_request = json.loads(changed_file.read_text())
            changed = http("draft", changed_request)
            assert changed["outcome"]["Applied"] == revision + 1
            assert http("draft", changed_request) == changed
            newer_version = str(uuid.uuid4())
            newer_file = root / "newer-seal.json"
            run("prepare-seal", revision + 1, newer_version, newer_file)
            newer = http("draft", json.loads(newer_file.read_text()))
            assert newer["outcome"]["Sealed"]["digest"] != source["snapshot"]["digest"]
            # Kill after the first page and draft edits are durable, before native completion records its cursor.
            active.finish(crash=True)
            active = None
            interrupted_sessions = set(state.iterdir())
            assert interrupted_sessions
            run("get", state, identifier, success=False)
            time.sleep(32)  # signed cookbook serving lease is 30 seconds
            recovered = view(identifier)
            assert recovered["run_id"] == initial["run_id"]
            assert recovered["state"]["rows"] == 0 and not recovered["state"]["chunks"]
            page_before = root / "page-before.csv"
            assert run("download", state, bytes(first_chunk["artifact"]["key"]).hex(), page_before)[-1]["artifact"] == first_chunk["artifact"]
            assert snapshot(version)[0] == source
            assert run("resolve-data", state, changed_file)[-1]["commit_sequence"] == changed["receipt"]["commit_sequence"]
            assert set(state.iterdir()) == interrupted_sessions

            active = Server(binary, state, port, 0)
            active.checkpoint("ready")
            repeated = active.checkpoint("export_published", "Page")
            assert repeated["native_attempt"] == 2
            assert repeated["run_id"] == published["run_id"] and repeated["activity_id"] == published["activity_id"]
            assert repeated["completion"]["Page"] == first_chunk
            finished = active.checkpoint("export_published", "Report")
            active.finish()
            active = None
            completed = view(identifier)
            assert completed["status"] == "completed" and completed["state"]["failure"] is None
            assert completed["state"]["request"] == request["request"]
            assert completed["state"]["rows"] == len(original_rows)
            assert len(completed["state"]["chunks"]) == 3
            assert completed["state"]["chunks"][0] == first_chunk
            report = completed["state"]["report"]
            assert report == finished["completion"]["Report"]
            assert report["dataset_digest"] == source["snapshot"]["digest"]
            report_path = root / "report.csv"
            assert run("download", state, bytes(report["artifact"]["key"]).hex(), report_path)[-1]["artifact"] == report["artifact"]
            with report_path.open(newline="", encoding="utf-8") as data:
                reader = csv.DictReader(data)
                actual = [{"id": int(row["id"]), "label": row["label"], "units": int(row["units"])} for row in reader]
            assert actual == original_rows
            assert len({row["id"] for row in actual}) == len(original_rows)
            assert actual[0]["units"] == 10 and "\n" in actual[2]["label"]
            assert run("start", state, request_file)[-1] == original
            assert run("resolve", state, request_file)[-1]["commit_sequence"] == original["receipt"]["commit_sequence"]
            assert run("data", state, seal_file)[-1] == sealed

            conflict = json.loads(request_file.read_text())
            conflict["identity"]["request"] = list(uuid.uuid4().bytes)
            conflict["request"]["snapshot"] = newer["outcome"]["Sealed"]
            conflict_file = root / "conflict.json"
            conflict_file.write_text(json.dumps(conflict))
            rejected = run("start", state, conflict_file, success=False)[-1]
            assert rejected["outcome"] == "Conflict"
            assert run("start", state, conflict_file, success=False)[-1] == rejected
            assert view(identifier)["state"]["request"] == request["request"]
            assert set(state.iterdir()) == interrupted_sessions
            shutil.rmtree(state)
            assert view(identifier) == completed
            cold_report = root / "cold.csv"
            run("download", state, bytes(report["artifact"]["key"]).hex(), cold_report)
            assert cold_report.read_bytes() == report_path.read_bytes()
            assert snapshot(version)[0] == source
            assert run("resolve", state, request_file)[-1]["commit_sequence"] == original["receipt"]["commit_sequence"]

            expired = json.loads(request_file.read_text())
            expired["identity"]["expires_at_ms"] = int(time.time() * 1000) - 1
            expired["identity"]["issued_at_ms"] = expired["identity"]["expires_at_ms"] - 300000
            expired_file = root / "expired.json"
            expired_file.write_text(json.dumps(expired))
            evidence = run("resolve", root / "never-provisioned", expired_file,
                           env={**os.environ, "CELLULE_COOKBOOK_ENDPOINT": "http://127.0.0.1:1"})[-1]
            assert evidence == {"resolution": "expired", "absence_proven": False}
            assert not (root / "never-provisioned").exists()
            print(json.dumps({"scenario": "passed", "workflow": identifier, "report": report,
                              "checks": ["persistent-SQL-and-Blob", "conditional-draft", "atomic-seal",
                                         "bounded-keyset-pages", "valid-ID-gaps", "retained-command-replay",
                                         "auth-before-dispatch", "draft-edit-during-export", "newer-version-isolated",
                                         "kill-after-page-publication", "live-owner-refusal", "expired-owner-takeover",
                                         "same-native-action-retry", "same-page-manifest", "three-durable-pages",
                                         "canonical-unicode-CSV", "exact-original-sealed-rows", "no-duplicates-or-missing-rows",
                                         "complete-dataset-digest", "SIGTERM-final-completion-drain", "durable-export-conflict",
                                         "original-outcome-resolution", "orphan-session-preserved", "cold-restore",
                                         "expired-evidence-does-not-prove-absence"]}))
        finally:
            if active:
                active.close()


if __name__ == "__main__":
    main()
