#!/usr/bin/env python3
"""Run codespell over every tracked file with the pre-commit hook's settings.

The codespell hook in .pre-commit-config.yaml is the single source of truth for
the pinned version, the ignore-words list and the excluded paths. This script
reads them from that file, so the CI step and the local hook cannot drift.

  check_spelling.py                 # scan; exit 1 on any misspelling
  check_spelling.py --print-version # the pinned codespell version, for the installer
"""
from __future__ import annotations

import argparse
import ast
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
CONFIG = ROOT / ".pre-commit-config.yaml"
CODESPELL_REPO = "codespell-project/codespell"
CHUNK = 400


def hook_settings(text: str) -> tuple[str, list[str], str]:
    """(version, args, exclude regex) of the codespell hook in a pre-commit config."""
    lines = text.splitlines()
    start = next((i for i, line in enumerate(lines) if "repo:" in line and CODESPELL_REPO in line), None)
    if start is None:
        raise SystemExit(f"{CONFIG}: no {CODESPELL_REPO} hook")
    version, args, exclude = "", [], ""
    i = start + 1
    while i < len(lines) and not re.match(r"\s*-\s*repo:", lines[i]):
        line = lines[i]
        if m := re.match(r"\s*rev:\s*v?(\S+)", line):
            version = m.group(1)
        elif m := re.match(r"\s*args:\s*(\[.*\])\s*$", line):
            args = [str(a) for a in ast.literal_eval(m.group(1))]
        elif m := re.match(r"(\s*)exclude:\s*\|\s*$", line):
            indent = len(m.group(1))
            block = []
            i += 1
            while i < len(lines) and (not lines[i].strip() or len(lines[i]) - len(lines[i].lstrip()) > indent):
                block.append(lines[i].strip())
                i += 1
            exclude = "".join(block)
            continue
        i += 1
    if not version:
        raise SystemExit(f"{CONFIG}: codespell hook has no rev")
    return version, args, exclude


def is_text(path: pathlib.Path) -> bool:
    try:
        with path.open("rb") as fh:
            return b"\0" not in fh.read(8192)
    except OSError:
        return False


def tracked_text_files(exclude: str) -> list[str]:
    out = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True).stdout
    pattern = re.compile(exclude) if exclude else None
    files = []
    for raw in out.split(b"\0"):
        if not raw:
            continue
        rel = raw.decode()
        if pattern and pattern.search(rel):
            continue
        if (ROOT / rel).is_file() and is_text(ROOT / rel):
            files.append(rel)
    return files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--print-version", action="store_true", help="print the pinned codespell version and exit")
    opts = parser.parse_args(argv)
    version, args, exclude = hook_settings(CONFIG.read_text())
    if opts.print_version:
        print(version)
        return 0
    files = tracked_text_files(exclude)
    failed = False
    for start in range(0, len(files), CHUNK):
        result = subprocess.run([sys.executable, "-m", "codespell_lib", *args, "--", *files[start:start + CHUNK]], cwd=ROOT)
        if result.returncode not in (0, 65):
            return result.returncode
        failed = failed or result.returncode == 65
    print(f"codespell {version}: {'misspellings found' if failed else 'clean'} ({len(files)} files)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
