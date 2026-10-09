#!/usr/bin/env python3
"""Backport the fixed observation fixture without copying candidate policy."""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess


def git(source, *args):
    return subprocess.run(
        ["git", "-C", str(source), *args], capture_output=True, text=True
    )


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def instrument(candidate, baseline, evidence):
    patch = candidate / "scripts/axum-write-telemetry.patch"
    inventory = git(candidate, "apply", "--numstat", str(patch))
    inventory.check_returncode()
    targets = []
    for line in inventory.stdout.splitlines():
        added, removed, target = line.split("\t")
        path = PurePosixPath(target)
        if added == "-" or removed == "-" or path.is_absolute() or ".." in path.parts:
            raise ValueError(f"unsupported telemetry patch target: {target}")
        targets.append(target)
    if not targets or len(targets) != len(set(targets)):
        raise ValueError("telemetry patch must have distinct text targets")
    before = {target: digest(baseline / target) for target in targets}

    if git(baseline, "apply", "--check", str(patch)).returncode == 0:
        applied = git(baseline, "apply", str(patch))
        applied.check_returncode()
        methods = {target: "fixed-patch-applied" for target in targets}
    elif git(baseline, "apply", "--reverse", "--check", str(patch)).returncode == 0:
        methods = {target: "fixed-patch-reverse-check" for target in targets}
    else:
        # Reorganized observation code can defeat a whole-patch reverse check.
        # Prove each target independently; never transfer candidate policy.
        methods = {}
        for target in targets:
            checked = git(
                baseline, "apply", "--reverse", "--check",
                f"--include={target}", str(patch)
            )
            if checked.returncode == 0:
                methods[target] = "fixed-target-reverse-check"
            elif (baseline / target).read_bytes() == (candidate / target).read_bytes():
                methods[target] = "candidate-byte-equality"
            else:
                raise ValueError(
                    f"cannot prove baseline telemetry in {target}:\n{checked.stderr}"
                )

    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "telemetry-targets.tsv").write_text(inventory.stdout)
    audit = {
        "patch_sha256": digest(patch),
        "targets": [
            {
                "path": target,
                "proof": methods[target],
                "baseline_before_sha256": before[target],
                "baseline_after_sha256": digest(baseline / target),
                "candidate_sha256": digest(candidate / target),
            }
            for target in targets
        ],
    }
    (evidence / "telemetry-admission.json").write_text(
        json.dumps(audit, indent=2) + "\n"
    )
    return audit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    instrument(args.candidate.resolve(), args.baseline.resolve(), args.evidence.resolve())


if __name__ == "__main__":
    main()
