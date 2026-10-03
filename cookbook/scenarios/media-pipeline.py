#!/usr/bin/env python3
"""Persistent native Blob publication followed by an interrupted Activity completion.

Use CI or an isolated source snapshot and a private object-provider installation.
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
import urllib.error
import urllib.request
import uuid


class Server:
    def __init__(self, binary, state, port, delay):
        self.process = subprocess.Popen(
            [binary, "serve", str(state), str(port), "240"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0,
            env={**os.environ, "CELLULE_MEDIA_AFTER_PUBLICATION_MS": str(delay)},
        )
        self.output = {"stdout": bytearray(), "stderr": bytearray()}
        self.pending = b""

    def checkpoint(self, event):
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(self.process.stderr, selectors.EVENT_READ, "stderr")
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                while b"\n" in self.pending:
                    line, self.pending = self.pending.split(b"\n", 1)
                    record = json.loads(line)
                    if record.get("event") == event:
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
                    if selected.data != "stdout":
                        continue
                    self.pending += chunk
            raise AssertionError(f"missing {event}: {self.output}")

    def finish(self, crash=False):
        if crash:
            self.process.kill()
        else:
            self.process.send_signal(signal.SIGTERM)
        output, errors = self.process.communicate(timeout=25)
        self.output["stdout"].extend(output)
        self.output["stderr"].extend(errors)
        if not crash and self.process.returncode:
            raise RuntimeError(f"server drain failed: {self.output}")

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)


def main():
    binary = str(Path(sys.argv[1]).resolve())
    active = None
    with tempfile.TemporaryDirectory(prefix="cellule-media-") as temporary:
        root = Path(temporary)
        state = root / "state"
        source_png = root / "source.png"
        upload = root / "upload.json"
        source_json = root / "source.json"
        request_file = root / "request.json"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}/"

        def run(*arguments, success=True, env=None):
            result = subprocess.run([binary, *map(str, arguments)], capture_output=True,
                                    text=True, timeout=75, env=env)
            if bool(result.returncode == 0) != success:
                raise RuntimeError(f"{arguments}: exit {result.returncode}; {result.stderr}; {result.stdout}")
            return [json.loads(line) for line in result.stdout.splitlines() if line]

        def view(identifier):
            return run("get", state, identifier)[-1]["view"]

        def hex_key(value):
            return bytes(value).hex()

        try:
            run("sample", source_png)
            run("prepare", source_png, upload)
            run("prepare", source_png, upload, success=False)
            artifact = run("upload", state, upload)[-1]
            assert run("upload", state, upload)[-1] == artifact
            source_json.write_text(json.dumps(artifact))
            run("prepare-run", source_json, 128, endpoint, request_file)
            request = json.loads(request_file.read_text())
            identifier = str(uuid.UUID(bytes=bytes(request["request"]["id"])))
            assert run("resolve", state, request_file)[-1]["resolution"] == "absent"
            original = run("start", state, request_file)[-1]
            assert run("start", state, request_file)[-1] == original
            assert run("resolve", state, request_file)[-1]["commit_sequence"] == original["receipt"]["commit_sequence"]
            initial = view(identifier)
            assert initial["state"]["result"] is None and initial["state"]["action"] is not None

            active = Server(binary, state, port, 10000)
            active.checkpoint("ready")
            invalid = urllib.request.Request(endpoint + "source", data=b"not-json",
                                             headers={"Content-Type": "application/json"})
            try:
                urllib.request.urlopen(invalid, timeout=5)
                raise AssertionError("unauthenticated adapter request was accepted")
            except urllib.error.HTTPError as response:
                assert response.code == 401
            published = active.checkpoint("output_published")
            assert published["native_attempt"] == 1
            active.finish(crash=True)
            active = None
            interrupted_sessions = set(state.iterdir())
            assert interrupted_sessions
            run("get", state, identifier, success=False)
            # Node enrollment uses a 30-second signed serving lease.
            time.sleep(32)
            recovered = view(identifier)
            assert recovered["run_id"] == initial["run_id"]
            assert recovered["state"]["result"] is None
            output_before = root / "before.png"
            output_artifact = run("download", state, "result", hex_key(published["artifact"]["key"]), output_before)[-1]["artifact"]
            assert output_artifact == published["artifact"]
            assert set(state.iterdir()) == interrupted_sessions

            active = Server(binary, state, port, 0)
            active.checkpoint("ready")
            retried = active.checkpoint("output_published")
            assert retried["native_attempt"] == 2
            assert retried["run_id"] == published["run_id"]
            assert retried["activity_id"] == published["activity_id"]
            assert retried["artifact"] == published["artifact"]
            active.finish()
            active = None
            completed = view(identifier)
            assert completed["status"] == "completed"
            assert completed["state"]["result"] == published["artifact"]
            assert completed["state"]["failure"] is None
            output_after = root / "after.png"
            assert run("download", state, "result", hex_key(published["artifact"]["key"]), output_after)[-1]["artifact"] == output_artifact
            assert output_after.read_bytes() == output_before.read_bytes()
            assert output_artifact["width"] == 128 and output_artifact["height"] == 64
            assert run("start", state, request_file)[-1] == original
            assert view(identifier)["run_id"] == initial["run_id"]

            changed = json.loads(request_file.read_text())
            changed["identity"]["request"] = list(uuid.uuid4().bytes)
            changed["request"]["side"] = 64
            changed_file = root / "changed.json"
            changed_file.write_text(json.dumps(changed))
            rejected = run("start", state, changed_file, success=False)[-1]
            assert rejected["outcome"] == "Conflict"
            assert run("start", state, changed_file, success=False)[-1] == rejected
            assert run("resolve", state, changed_file)[-1]["commit_sequence"] == rejected["receipt"]["commit_sequence"]
            assert view(identifier)["state"]["request"] == completed["state"]["request"]

            assert set(state.iterdir()) == interrupted_sessions
            # Remove only this scenario's drained local working state, then reconstruct every Cell.
            shutil.rmtree(state)
            restored = view(identifier)
            assert restored == completed
            cold_output = root / "cold.png"
            assert run("download", state, "result", hex_key(output_artifact["key"]), cold_output)[-1]["artifact"] == output_artifact
            assert cold_output.read_bytes() == output_before.read_bytes()
            assert run("upload", state, upload)[-1] == artifact

            expired = json.loads(request_file.read_text())
            expired["identity"]["expires_at_ms"] = int(time.time() * 1000) - 1
            expired["identity"]["issued_at_ms"] = expired["identity"]["expires_at_ms"] - 300000
            expired_file = root / "expired.json"
            expired_file.write_text(json.dumps(expired))
            evidence = run("resolve", root / "never-provisioned", expired_file,
                           env={**os.environ, "CELLULE_COOKBOOK_ENDPOINT": "http://127.0.0.1:1"})[-1]
            assert evidence == {"resolution": "expired", "absence_proven": False}
            assert not (root / "never-provisioned").exists()
            print(json.dumps({"scenario": "passed", "workflow": identifier,
                              "output": output_artifact,
                              "checks": ["persistent-native-Blob", "retained-upload", "retained-start",
                                         "original-outcome-resolution", "auth-before-dispatch", "live-owner-refusal",
                                         "kill-after-output-publication", "expired-owner-takeover",
                                         "same-native-run-and-action", "native-attempt-two", "same-output-manifest",
                                         "same-verified-PNG", "Workflow-artifact-link", "SIGTERM-completion-drain",
                                         "durable-input-conflict", "orphan-session-preserved", "cold-restore",
                                         "expired-evidence-does-not-prove-absence"]}))
        finally:
            if active:
                active.close()


if __name__ == "__main__":
    main()
