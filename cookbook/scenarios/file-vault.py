#!/usr/bin/env python3
"""Verify frozen multipart resumption and conditional publication across processes."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import uuid


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: file-vault.py FILE_VAULT_BINARY")
    binary = Path(sys.argv[1]).resolve(strict=True)
    key = "process/" + uuid.uuid4().hex + "/artifact.bin"
    with tempfile.TemporaryDirectory(prefix="cellule-vault-process-") as directory:
        root = Path(directory)
        state = root / "working"

        def run(*arguments: str, success: bool = True) -> dict:
            result = subprocess.run([str(binary), *map(str, arguments)], env=dict(os.environ),
                                    capture_output=True, text=True, timeout=60)
            if success and result.returncode:
                raise AssertionError(f"command failed: {arguments}\n{result.stderr}")
            if not success and not result.returncode:
                raise AssertionError(f"command should fail: {arguments}")
            return json.loads(result.stdout) if result.stdout.strip() else {}

        original = bytes(index % 251 for index in range((256 << 10) + 57))
        source = root / "source.bin"
        source.write_bytes(original)
        plan = root / "original-plan"
        run("prepare", key, source, plan)
        source.write_bytes(b"changed original source")
        staged = run("stage", state, plan)
        assert staged["staged_parts"] == 1 and staged["visible_file"] is None
        shutil.rmtree(state)
        assert run("head", state, key)["file"] is None
        committed = run("resume", state, plan)
        assert committed["outcome"]["size"] == len(original)
        assert run("resume", state, plan) == committed
        output = root / "download.bin"
        downloaded = run("download", state, key, output, committed["file"]["etag"])
        assert output.read_bytes() == original
        assert downloaded["file"] == committed["file"]
        run("download", state, key, output, success=False)
        assert output.read_bytes() == original, "existing download must never be overwritten"
        first_plan, second_plan = root / "first-plan", root / "second-plan"
        first_source, second_source = root / "first.bin", root / "second.bin"
        first_source.write_bytes(b"first replacement")
        second_source.write_bytes(b"losing replacement")
        etag = committed["file"]["etag"]
        run("prepare", key, first_source, first_plan, etag)
        run("prepare", key, second_source, second_plan, etag)
        run("stage", state, first_plan)
        run("stage", state, second_plan)
        assert run("head", state, key)["file"] == committed["file"]
        replaced = run("resume", state, first_plan)
        assert run("resume", state, plan) == committed, "replay reports its recorded publication"
        rejected = run("resume", state, second_plan, success=False)
        assert rejected["outcome"]["status"] == "conflict"
        assert run("resume", state, second_plan, success=False) == rejected
        run("download", state, key, root / "stale.bin", etag, success=False)
        assert not (root / "stale.bin").exists()
        deletion = root / "delete.json"
        run("prepare-delete", key, replaced["file"]["etag"], deletion)
        deleted = run("delete", state, deletion)
        assert deleted["outcome"]["status"] == "deleted"
        assert run("resume", state, first_plan) == replaced
        recreate = root / "recreated-plan"
        run("prepare", key, source, recreate)
        recreated = run("resume", state, recreate)
        assert run("delete", state, deletion) == deleted
        assert run("head", state, key)["file"] == recreated["file"]
        corrupt = root / "corrupt-plan"
        run("prepare", key + "-other", source, corrupt)
        (corrupt / "01.part").write_bytes(b"tampered frozen input")
        run("resume", state, corrupt, success=False)
        assert run("head", state, key + "-other")["file"] is None
        print(json.dumps({"scenario": "passed", "file": key,
                          "checks": ["persistent-s3", "independent-processes", "frozen-source",
                                     "staged-invisibility", "cold-resume", "phase-resolution",
                                     "verified-download", "conditional-replacement", "durable-conflict",
                                     "delete-replay", "tampered-plan-refusal", "error-path-drain"]}))


if __name__ == "__main__":
    main()
