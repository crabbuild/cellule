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


def main() -> int:
    documents = [
        path
        for path in sorted((ROOT / "apps/web/content/authored").rglob("*.mdx"))
        if "```rust" in path.read_text()
    ]
    if not documents:
        print("error: no authored Rust examples found", file=sys.stderr)
        return 1
    build = subprocess.run(
        ["cargo", "build", "-p", "cellule-app", "--lib", "--locked", "--message-format=json"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        text=True,
    )
    if build.returncode:
        return build.returncode
    libraries: dict[str, Path] = {}
    for line in build.stdout.splitlines():
        artifact = json.loads(line)
        if artifact.get("reason") != "compiler-artifact":
            continue
        name = artifact["target"]["name"]
        if name not in ("cellule_app", "cellule_runtime"):
            continue
        for filename in artifact["filenames"]:
            if filename.endswith(".rlib"):
                libraries[name] = Path(filename)
    if set(libraries) != {"cellule_app", "cellule_runtime"}:
        print("error: Cargo did not report both framework libraries", file=sys.stderr)
        return 1
    arguments = []
    for name, file in libraries.items():
        arguments.extend(["--extern", f"{name}={file}"])
    for directory in sorted({file.parent for file in libraries.values()}):
        arguments.extend(["-L", f"dependency={directory}"])
    failures = []
    for document in documents:
        relative = document.relative_to(ROOT)
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
