#!/usr/bin/env python3
"""Check that Rust fences in the Cell and LTX documentation parse.

A reader copies documentation examples, so a fence that is not valid Rust is a
defect even when no test compiles it: the docs are `rust,ignore` precisely
because they need a provider, not because they may be syntactically broken.

Rules:
  1. Every ```rust fence in the root README, framework guides, crates, and
     authored website MDX
     parses after being wrapped in `fn main() { ... }`, so a statement snippet
     parses while a broken one fails.
  2. Incomplete pseudocode uses a text fence; Rust fences must parse.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CRATES = (
    "crates/cellule-runtime",
    "crates/cellule-app",
    "crates/cellule-host",
    "crates/cellule-ltx",
    "crates/cellule-peer-http",
    "crates/cellule-store",
    "crates/cellule-types",
    "docs",
)



def fences(text: str) -> list[tuple[str, int, str]]:
    """Returns one (language, first line number, body) entry per fenced block."""
    found: list[tuple[str, int, str]] = []
    body: list[str] | None = None
    language = ""
    start = 0
    for number, line in enumerate(text.splitlines(), 1):
        if line.startswith("```"):
            if body is None:
                body, language, start = [], line.strip("`").strip(), number + 1
            else:
                found.append((language, start, "\n".join(body)))
                body = None
        elif body is not None:
            body.append(line)
    return found


def parses(body: str) -> str | None:
    """Returns the first parse error, or None when the wrapped snippet parses."""
    # Rustdoc hides setup lines prefixed with `# ` while still compiling them.
    visible = "\n".join(line[2:] if line.startswith("# ") else line for line in body.splitlines())
    wrapped = "fn main() {\n" + visible + "\n}\n"
    with tempfile.NamedTemporaryFile("w", suffix=".rs") as file:
        file.write(wrapped)
        file.flush()
        result = subprocess.run(
            ["rustfmt", "--edition", "2024", "--emit", "stdout", file.name],
            capture_output=True,
            text=True,
        )
    if result.returncode == 0:
        return None
    return next(
        (line for line in result.stderr.splitlines() if line.startswith("error")),
        "rustfmt rejected the snippet",
    )


def main() -> int:
    problems: list[str] = []
    checked = 0
    paths = [ROOT / "README.md"]
    for crate in CRATES:
        paths.extend(sorted((ROOT / crate).rglob("*.md")))
    paths.extend(sorted((ROOT / "apps/web/content/authored").rglob("*.mdx")))
    for path in paths:
        if "target" in path.parts:
            continue
        relative = str(path.relative_to(ROOT))
        for language, start, body in fences(path.read_text()):
            if not language.startswith("rust"):
                continue
            checked += 1
            error = parses(body)
            if error is not None:
                problems.append(f"{relative}:{start}: {error}")
    for problem in problems:
        print(f"error: {problem}", file=sys.stderr)
    if problems:
        return 1
    print(f"ok: {checked} documented Rust snippets parse")
    return 0


if __name__ == "__main__":
    sys.exit(main())
