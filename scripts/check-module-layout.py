#!/usr/bin/env python3
"""Verify that Cellule modules, unit tests, and integration suites are compiled."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CRATES = tuple(sorted((ROOT / "crates").glob("cellule-*")))
SUITES = {
    "cellule-runtime": ("runtime", "primitives", "protocol", "contracts", "fleet", "qualification"),
    "cellule-host": ("node",),
    "cellule-ltx": ("cell", "ltx", "host"),
}
FLAT_SUITES = {"cellule-app": ("contracts", "integration")}
DECL = re.compile(r"^\s*(?:pub(?:\([^)]*\))? )?mod ([a-z_][a-z_0-9]*)\s*;", re.M)
TEST = re.compile(r"^\s*#\[(?:tokio::)?test", re.M)
PATH = re.compile(r"#\[path\s*=")
GUIDE_PATH = re.compile(r"`((?:src|tests)/[^`]+)`")
DOC_TEST_PATH = re.compile(r"`(tests/[^`\s]+?\.rs)`")
ROOT_RE_EXPORT = re.compile(r"^pub use ([^;]+);", re.M)
COMMENT = re.compile(r"//[^\n]*")
SANS_IO = (
    ("async", re.compile(r"\basync\b")),
    ("await", re.compile(r"\.await\b")),
    ("tokio", re.compile(r"\btokio::")),
    ("storage", re.compile(r"\brusqlite\b|\bobject_store\b|\bstd::fs\b")),
    ("clock", re.compile(r"\bSystemTime\b|\bInstant\b|\bstd::time\b")),
)


def declaration(file: Path, base: Path) -> Path:
    """Return the module entry that must declare one child source file."""
    module_dir = file.parent.parent if file.name == "mod.rs" else file.parent
    if module_dir == base:
        return base / "lib.rs"
    entry = module_dir / "mod.rs"
    if entry.is_file():
        return entry
    return module_dir.with_suffix(".rs")


def declared(file: Path, owner: Path, name: str) -> bool:
    return owner.is_file() and name in DECL.findall(COMMENT.sub("", owner.read_text()))


def check_source(crate: Path) -> list[str]:
    src = crate / "src"
    problems = []
    for file in sorted(src.rglob("*.rs")):
        relative = file.relative_to(ROOT)
        text = file.read_text()
        if PATH.search(COMMENT.sub("", text)):
            problems.append(f"{relative}: #[path] bypasses normal module ownership")
        if "bin" in file.relative_to(src).parts or file == src / "lib.rs":
            continue
        owner = declaration(file, src)
        name = file.parent.name if file.name == "mod.rs" else file.stem
        if not declared(file, owner, name):
            problems.append(f"{relative}: {owner.relative_to(ROOT)} does not declare mod {name}")
        if file.name != "mod.rs" and file.with_suffix("").is_dir():
            problems.append(f"{relative}: place module entry at {file.with_suffix('') / 'mod.rs'}")
    return problems


def check_suites(crate: Path) -> list[str]:
    tests = crate / "tests"
    problems = []
    if crate.name in FLAT_SUITES:
        return check_flat_suites(crate)
    for suite in SUITES.get(crate.name, ()):
        root = tests / f"{suite}.rs"
        directory = tests / suite
        if not root.is_file() or not directory.is_dir():
            problems.append(f"{root.relative_to(ROOT)}: missing integration suite or directory")
            continue
        modules = sorted(directory.rglob("*.rs"))
        if not TEST.search(root.read_text()) and not any(TEST.search(p.read_text()) for p in modules):
            problems.append(f"{root.relative_to(ROOT)}: suite has no tests")
        for file in modules:
            owner = root if file.parent == directory else declaration(file, tests)
            name = file.parent.name if file.name == "mod.rs" else file.stem
            if not declared(file, owner, name):
                problems.append(f"{file.relative_to(ROOT)}: {owner.relative_to(ROOT)} does not declare mod {name}")
    if tests.is_dir():
        for directory in tests.iterdir():
            if not directory.is_dir() or directory.name in {"support", "vectors"}:
                continue
            if not (tests / f"{directory.name}.rs").is_file():
                problems.append(f"{directory.relative_to(ROOT)}: no integration suite root")
        for file in tests.glob("*.rs"):
            if file.stem not in SUITES.get(crate.name, ()) and (
                (crate / "src" / file.name).exists()
                or (crate / "src" / file.stem / "mod.rs").exists()
            ):
                problems.append(f"{file.relative_to(ROOT)}: shadows a source module")
    return problems


def check_flat_suites(crate: Path) -> list[str]:
    """Check explicit test targets whose modules live directly in tests/."""
    tests = crate / "tests"
    suites = FLAT_SUITES[crate.name]
    problems = []
    manifest = tomllib.loads((crate / "Cargo.toml").read_text())
    declared_targets = {
        target.get("name"): target.get("path") for target in manifest.get("test", [])
    }
    if manifest["package"].get("autotests") is not False:
        problems.append(f"{crate.relative_to(ROOT)}/Cargo.toml: flat tests require autotests = false")
    if declared_targets != {suite: f"tests/{suite}.rs" for suite in suites}:
        problems.append(f"{crate.relative_to(ROOT)}/Cargo.toml: explicit test targets differ from {suites}")

    roots = [tests / f"{suite}.rs" for suite in suites]
    for root in roots:
        if not root.is_file():
            problems.append(f"{root.relative_to(ROOT)}: missing integration suite")
        elif not TEST.search(root.read_text()):
            problems.append(f"{root.relative_to(ROOT)}: suite has no tests")

    for file in tests.glob("*.rs"):
        if file in roots:
            continue
        owners = [root for root in roots if declared(file, root, file.stem)]
        if len(owners) != 1:
            problems.append(f"{file.relative_to(ROOT)}: expected one flat suite to declare mod {file.stem}")

    for directory in tests.iterdir():
        if not directory.is_dir() or directory.name in {"support", "vectors"}:
            continue
        entry = tests / f"{directory.name}.rs"
        if not entry.is_file():
            problems.append(f"{directory.relative_to(ROOT)}: missing module entry {entry.name}")
            continue
        for file in directory.rglob("*.rs"):
            owner = declaration(file, tests)
            name = file.parent.name if file.name == "mod.rs" else file.stem
            if not declared(file, owner, name):
                problems.append(f"{file.relative_to(ROOT)}: {owner.relative_to(ROOT)} does not declare mod {name}")
    return problems


def check_guides(crate: Path) -> list[str]:
    problems = []
    if not (crate / "docs/README.md").is_file():
        problems.append(f"{crate.relative_to(ROOT)}: missing docs/README.md")
    guide = crate / "AGENTS.md"
    if guide.is_file():
        for token in GUIDE_PATH.findall(guide.read_text()):
            if "<" not in token and "{" not in token and not (crate / token).exists():
                problems.append(f"{guide.relative_to(ROOT)}: missing {token}")
    for doc in crate.rglob("*.md"):
        for token in DOC_TEST_PATH.findall(doc.read_text()):
            if "<" not in token and "{" not in token and not (crate / token).exists():
                problems.append(f"{doc.relative_to(ROOT)}: missing {token}")
    return problems


def check_prelude(crate: Path) -> list[str]:
    inventory = crate / "api-prelude.txt"
    if not inventory.is_file():
        return []
    expected = {line.strip() for line in inventory.read_text().splitlines() if line.strip()}
    actual = set()
    for statement in ROOT_RE_EXPORT.findall((crate / "src/lib.rs").read_text()):
        statement = " ".join(statement.split())
        if "{" in statement:
            for part in statement[statement.index("{") + 1 : statement.rindex("}")].split(","):
                if part.strip():
                    actual.add(part.split(" as ")[-1].strip())
        else:
            actual.add(statement.split("::")[-1].strip())
    if actual != expected:
        return [f"{inventory.relative_to(ROOT)}: root exports differ: missing {sorted(expected - actual)}, extra {sorted(actual - expected)}"]
    return []


def check_kernel(crate: Path) -> list[str]:
    if crate.name != "cellule-runtime":
        return []
    problems = []
    sources = [
        *(crate / "src/coordination").rglob("*.rs"),
        *(crate / "src/fleet/operations").rglob("*.rs"),
    ]
    for file in sources:
        text = COMMENT.sub("", file.read_text())
        for label, pattern in SANS_IO:
            if pattern.search(text):
                problems.append(f"{file.relative_to(ROOT)}: pure decision kernel uses {label}")
    return problems


def main() -> int:
    problems = []
    for crate in CRATES:
        problems.extend(check_source(crate))
        problems.extend(check_suites(crate))
        problems.extend(check_guides(crate))
        problems.extend(check_prelude(crate))
        problems.extend(check_kernel(crate))
    if problems:
        for problem in problems:
            print(f"error: {problem}")
        return 1
    print(f"ok: module and test ownership for {len(CRATES)} Cellule crates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
