#!/usr/bin/env python3
"""Compile authored website Rust examples against the current framework APIs.

Uses Cargo's reported artifacts, never a glob that might select a stale build.
Set CARGO_TARGET_DIR to the checkout's verification directory on workstations.
The examples are complete functions; rustdoc compiles their bodies without
requiring a deployed provider or invoking an external service.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def build_libraries(packages: list[str], expected: set[str]) -> dict[str, Path] | None:
    command = ["cargo", "build"]
    for package in packages:
        command.extend(["-p", package])
    command.extend(["--lib", "--locked", "--message-format=json"])
    build = subprocess.run(
        command,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        text=True,
    )
    if build.returncode:
        return None

    libraries: dict[str, Path] = {}
    for line in build.stdout.splitlines():
        artifact = json.loads(line)
        if artifact.get("reason") != "compiler-artifact":
            continue
        name = artifact["target"]["name"]
        if name not in expected:
            continue
        for filename in artifact["filenames"]:
            if filename.endswith(".rlib"):
                libraries[name] = Path(filename)

    missing = expected - libraries.keys()
    if missing:
        print(
            "error: Cargo did not report required example libraries: "
            + ", ".join(sorted(missing)),
            file=sys.stderr,
        )
        return None
    return libraries


def rustdoc_arguments(libraries: dict[str, Path]) -> list[str]:
    arguments = []
    for name, file in libraries.items():
        arguments.extend(["--extern", f"{name}={file}"])
    for directory in sorted({file.parent for file in libraries.values()}):
        arguments.extend(["-L", f"dependency={directory}"])
    return arguments


def main() -> int:
    documents = [
        path
        for path in sorted((ROOT / "apps/web/content/authored").rglob("*.mdx"))
        if "```rust" in path.read_text()
    ]
    if not documents:
        print("error: no authored Rust examples found", file=sys.stderr)
        return 1
    framework_libraries = build_libraries(
        ["cellule-app", "cellule-types"],
        {"cellule_app", "cellule_runtime", "cellule_types"},
    )
    if framework_libraries is None:
        return 1
    store_libraries = build_libraries(
        ["cellule-store"],
        {"cellule_store", "cellule_types", "bytes", "object_store"},
    )
    if store_libraries is None:
        return 1
    failures = []
    for document in documents:
        relative = document.relative_to(ROOT)
        # Build this group separately so direct imports such as bytes::Bytes
        # use the same feature-specific crate artifact as cellule-store.
        libraries = (
            store_libraries if "cellule_store::" in document.read_text() else framework_libraries
        )
        arguments = rustdoc_arguments(libraries)
        # rustdoc recognizes .md as Markdown; .mdx is otherwise parsed as Rust.
        # Copy unchanged text so fence line numbers still match the source.
        with tempfile.TemporaryDirectory(prefix="cellule-doc-examples-") as temporary:
            markdown = Path(temporary) / f"{document.stem}.md"
            markdown.write_text(document.read_text())
            result = subprocess.run(
                ["rustdoc", "--test", "--edition=2024", str(markdown), *arguments],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
        if result.returncode:
            failures.append(str(relative))
            print(result.stdout + result.stderr, file=sys.stderr)
        else:
            print(f"ok: Rust examples compile in {relative}")
    if failures:
        print(f"error: example compilation failed in {len(failures)} documents", file=sys.stderr)
        return 1
    print(f"ok: all Rust examples compile across {len(documents)} authored guides")
    return 0


if __name__ == "__main__":
    sys.exit(main())
