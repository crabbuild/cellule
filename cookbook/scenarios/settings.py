#!/usr/bin/env python3
"""Verify conditional settings across independent processes against private local S3."""

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
        raise SystemExit("usage: settings.py SETTINGS_BINARY")
    binary = Path(sys.argv[1]).resolve(strict=True)
    organization = "process-" + uuid.uuid4().hex
    with tempfile.TemporaryDirectory(prefix="cellule-settings-process-") as directory:
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

        def prepare(name: str, edits: list) -> Path:
            source = root / f"{name}.json"
            source.write_text(json.dumps(edits))
            mutation = root / f"{name}.mutation.json"
            run("prepare", organization, source, mutation)
            return mutation

        def edit(key: str, value, version=None) -> dict:
            return {"key": key, "expected": {"kind": "absent"} if version is None else
                    {"kind": "version", "version": version}, "value": value, "expires_at_ms": None}

        dark = {"type": "text", "value": "dark"}
        create = prepare("create", [edit("theme", dark)])
        created = run("apply", state, organization, create)
        assert created["outcome"]["status"] == "applied"
        assert run("apply", state, organization, create) == created
        initial = run("get", state, organization, "theme")["setting"]
        assert initial["value"] == dark
        assert len(initial["version"]) == 56
        run("apply", state, organization + "-wrong", create, success=False)
        delete = prepare("delete", [edit("theme", None, initial["version"])])
        removed = run("apply", state, organization, delete)
        recreate = prepare("recreate", [edit("theme", {"type": "text", "value": "new"})])
        run("apply", state, organization, recreate)
        assert run("apply", state, organization, delete) == removed
        current = run("get", state, organization, "theme")["setting"]
        assert current["version"] != initial["version"]
        stale = prepare("stale-bundle", [edit("feature.beta", {"type": "boolean", "value": True}),
                                        edit("theme", dark, initial["version"])])
        rejected = run("apply", state, organization, stale, success=False)
        assert rejected["outcome"]["status"] == "conflict"
        assert run("apply", state, organization, stale, success=False) == rejected
        assert run("get", state, organization, "feature.beta")["setting"] is None
        shutil.rmtree(state)
        assert run("get", state, organization, "theme")["setting"] == current
        resolved = run("resolve", state, organization, create)
        assert resolved["resolution"] == "committed"
        assert resolved["outcome"] == created["outcome"]
        assert resolved["commit_sequence"] == created["receipt"]["commit_sequence"]
        assert run("resolve", state, organization, stale)["outcome"] == rejected["outcome"]
        assert run("list", state, organization + "-other")["page"]["settings"] == []
        run("list", state, organization, "-", "-", "101", success=False)
        assert run("get", state, organization, "theme")["setting"] == current
        print(json.dumps({"scenario": "passed", "organization": organization,
                          "checks": ["persistent-s3", "independent-processes", "request-replay",
                                     "organization-binding", "delete-recreate-version", "atomic-bundle-rejection",
                                     "cold-restore", "outcome-resolution", "scope-isolation", "error-path-drain"]}))


if __name__ == "__main__":
    main()
