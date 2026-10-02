#!/usr/bin/env python3
"""Verify an already-built taskboard across independent processes and cold restore.

Run from an isolated source snapshot or CI after starting the private provider:
python3 cookbook/scenarios/taskboard.py /absolute/path/to/cellule-cookbook-taskboard
"""

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
        raise SystemExit("usage: taskboard.py TASKBOARD_BINARY")
    binary = Path(sys.argv[1]).resolve(strict=True)
    project = "process-" + uuid.uuid4().hex
    environment = dict(os.environ)
    with tempfile.TemporaryDirectory(prefix="cellule-taskboard-process-") as directory:
        root = Path(directory)
        state = root / "working"

        def run(*arguments: str, success: bool = True) -> dict:
            result = subprocess.run(
                [str(binary), *map(str, arguments)],
                env=environment,
                capture_output=True,
                text=True,
                timeout=60,
            )
            if success and result.returncode:
                raise AssertionError(f"command failed: {arguments}\n{result.stderr}")
            if not success and not result.returncode:
                raise AssertionError(f"command should fail: {arguments}")
            return json.loads(result.stdout) if result.stdout.strip() else {}

        def prepare(name: str, change: dict) -> Path:
            source = root / f"{name}.json"
            source.write_text(json.dumps(change))
            mutation = root / f"{name}.mutation.json"
            run("prepare", project, source, mutation)
            return mutation

        create = prepare("create", {"operation": "create", "id": 1, "title": "Recover this task"})
        created = run("apply", state, project, create)
        assert created["outcome"]["status"] == "applied"
        replayed = run("apply", state, project, create)
        assert replayed == created, "request replay must return the original outcome and receipt"
        run("apply", state, project + "-wrong", create, success=False)
        assign = prepare("assign", {"operation": "assign", "id": 1, "expected_revision": 1, "assignee": "alice"})
        assigned = run("apply", state, project, assign)
        assert assigned["outcome"]["task"]["revision"] == 2
        stale = prepare("stale", {"operation": "assign", "id": 1, "expected_revision": 1, "assignee": "bob"})
        rejected = run("apply", state, project, stale, success=False)
        assert rejected["outcome"]["status"] == "conflict"
        assert run("apply", state, project, stale, success=False) == rejected
        close = prepare("close", {"operation": "close", "id": 1, "expected_revision": 2})
        closed = run("apply", state, project, close)
        shutil.rmtree(state)
        observed = run("list", state, project)
        assert observed["page"]["tasks"] == [closed["outcome"]["task"]]
        resolved = run("resolve", state, project, create)
        assert resolved["resolution"] == "committed"
        assert resolved["outcome"] == created["outcome"]
        assert resolved["commit_sequence"] == created["receipt"]["commit_sequence"]
        resolved_rejection = run("resolve", state, project, stale)
        assert resolved_rejection["outcome"] == rejected["outcome"]
        assert run("list", state, project + "-other")["page"]["tasks"] == []
        run("list", state, project, "0", "101", success=False)
        assert run("list", state, project)["page"]["tasks"] == observed["page"]["tasks"]
        print(json.dumps({"scenario": "passed", "project": project,
                          "checks": ["persistent-s3", "independent-processes", "request-replay",
                                     "project-binding", "durable-rejection", "cold-restore",
                                     "outcome-resolution", "project-isolation", "error-path-drain"]}))


if __name__ == "__main__":
    main()
