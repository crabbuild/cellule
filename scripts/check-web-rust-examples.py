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
EXAMPLE_LIBRARIES = {
    "cellule_app", "cellule_runtime", "cellule_store", "cellule_types",
    "bytes", "object_store",
}


def main() -> int:
    documents = [
        path
        for path in sorted((ROOT / "apps/web/content/authored").rglob("*.mdx"))
        if "```rust" in path.read_text()
    ]
    if not documents:
        print("error: no authored Rust examples found", file=sys.stderr)
        return 1
    version = subprocess.check_output(["rustc", "-vV"], text=True)
    host = next(line.removeprefix("host: ") for line in version.splitlines() if line.startswith("host: "))
    # An explicit target separates target libraries from host build-dependencies.
    # Both can have the same package name but different Rust crate identities.
    build = subprocess.run(
        ["cargo", "build", "-p", "cellule-app", "--lib", "--locked", "--target", host, "--message-format=json"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        text=True,
    )
    if build.returncode:
        return build.returncode
    artifacts = []
    dependency_directories = set()
    for line in build.stdout.splitlines():
        artifact = json.loads(line)
        if artifact.get("reason") != "compiler-artifact":
            continue
        dependency_directories.update(Path(file).parent for file in artifact["filenames"])
        name = artifact["target"]["name"]
        if name not in EXAMPLE_LIBRARIES:
            continue
        for filename in artifact["filenames"]:
            if filename.endswith(".rlib"):
                artifacts.append((name, Path(filename)))
    app = next((file for name, file in artifacts if name == "cellule_app"), None)
    if app is None:
        print("error: Cargo did not report cellule_app", file=sys.stderr)
        return 1
    libraries = {
        name: file for name, file in artifacts if file.is_relative_to(app.parent)
    }
    missing = EXAMPLE_LIBRARIES - libraries.keys()
    if missing:
        print(f"error: Cargo did not report example libraries: {', '.join(sorted(missing))}", file=sys.stderr)
        return 1
    arguments = []
    for name, file in libraries.items():
        arguments.extend(["--extern", f"{name}={file}"])
    # Target crates also depend on host-built procedural macros.
    for directory in sorted(dependency_directories):
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
                ["rustdoc", "--test", "--edition=2024", "--target", host, str(markdown), *arguments],
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
