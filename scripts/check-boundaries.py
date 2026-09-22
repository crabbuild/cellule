#!/usr/bin/env python3
"""Check Cellule's crate layering and coordination ownership."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAYERS = {
    "cellule-types": set(),
    "cellule-store": {"cellule-types"},
    "cellule-ltx": {"cellule-store"},
    "cellule-runtime": {"cellule-ltx", "cellule-store"},
    "cellule-app": {"cellule-runtime"},
    "cellule-host": {"cellule-app", "cellule-runtime"},
}
KERNEL = ROOT / "crates/cellule-runtime/src/coordination.rs"
ACTOR = ROOT / "crates/cellule-runtime/src/actor.rs"
FORBIDDEN_KERNEL = (
    "async fn", ".await", "tokio::", "object_store", "rusqlite",
    "reqwest::", "std::fs", "std::net", "std::time", "rand::",
    "getrandom", "spawn_blocking", "Command::new(",
)


def main() -> int:
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"], cwd=ROOT,
    ))
    problems = []
    packages = {package["name"]: package for package in metadata["packages"]}
    if packages.keys() != LAYERS.keys():
        problems.append(f"workspace packages differ: {sorted(packages.keys() ^ LAYERS.keys())}")
    for name, package in packages.items():
        for dependency in package["dependencies"]:
            dependency_name = dependency["name"]
            if dependency_name == "crab" or dependency_name.startswith("crab-"):
                problems.append(f"{name} depends on Crab package {dependency_name}")
            if dependency["kind"] is not None or not dependency_name.startswith("cellule-"):
                continue
            if dependency_name not in LAYERS.get(name, set()):
                problems.append(f"{name} depends on forbidden layer {dependency_name}")
            if dependency.get("path") is None:
                problems.append(f"{name} must use the local {dependency_name} workspace crate")

    kernel = KERNEL.read_text()
    actor = ACTOR.read_text()
    for required in (
        "pub(crate) enum CoordinationInput", "pub(crate) enum CoordinationDecision",
        "pub(crate) struct CoordinationState", "pub(crate) fn step(&mut self, input: CoordinationInput)",
    ):
        if required not in kernel:
            problems.append(f"coordination kernel lost {required}")
    for number, line in enumerate(kernel.splitlines(), 1):
        if any(pattern in line for pattern in FORBIDDEN_KERNEL):
            problems.append(f"coordination.rs:{number}: adapter dependency in pure kernel")
    for required in ("CoordinationState", "coordination.step(CoordinationInput::"):
        if required not in actor:
            problems.append(f"actor lost coordination adapter {required}")

    retired = re.compile(r"\b(?:ReplicaHead|Replica|PagedDatabase|PagedConnection|CompactionSchedule)\b|ltx/<epoch>|head\.json|manifest\.json")
    for base in (ROOT / "crates/cellule-ltx/src", ROOT / "crates/cellule-ltx/examples"):
        for source in base.rglob("*.rs"):
            for number, line in enumerate(source.read_text().splitlines(), 1):
                if source.name == "cell_layout.rs" and "catalog/{shard:02x}/head.json" in line:
                    continue
                if retired.search(line):
                    problems.append(f"{source.relative_to(ROOT)}:{number}: retired LTX surface")

    if problems:
        for problem in problems:
            print(f"error: {problem}")
        return 1
    print("ok: Cellule crate layers, coordination kernel, and LTX hard cut")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
