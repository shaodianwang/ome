#!/usr/bin/env python3
"""Check that a machine's cells and the Go tests point at each other.

Coverage is recorded twice, on purpose, and the two halves can drift:

  * a cell names the checks that prove it, in `coverage.unit`/`perm`/`kind`;
  * a Go test names the cells it exercises, in a `// Cells: <id>, <id>`
    comment above the test function.

Neither half is derivable from the other, so each is checked against
something real. A `coverage.unit` name must resolve to a Go test function
that exists under `pkg/` or `tests/`: a renamed or deleted test otherwise
leaves a cell claiming proof that no longer runs. A `// Cells:` id must
resolve to a grid cell: a mistyped or stale coordinate otherwise points a
reader at nothing. `perm` and `kind` entries name suite specs rather than
Go functions and are not resolved here.

The third output is informational: per machine, how many decided cells
carry coverage and which ones do not. That is the remaining work, not a
failure — the frozen contract in the rolloutspec validator is what turns
it into one, once a machine is frozen.

Usage:

    go run ./hack/omenative-testing/statemachine/cmd/rolloutspec-grid \\
        --machine instance --format json --out /tmp/instance.grid.json
    python3 hack/omenative-testing/statemachine/check_coverage.py \\
        --grid /tmp/instance.grid.json --repo .
"""

import argparse
import json
import re
import sys
from pathlib import Path

DECIDED = "decided"
NA = "n/a"

# Source roots a coverage.unit name may be defined in, and the root whose
# tests may cite cell ids.
TEST_ROOTS = ("pkg", "tests")
CELL_REF_ROOT = "pkg"

CELLS_COMMENT = re.compile(r"^\s*//\s*Cells:(.*)$")
GO_FUNC = re.compile(r"^func\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.MULTILINE)


def _roots():
    return ", ".join(f"{r}/" for r in TEST_ROOTS)


def parse_cells_comment(line):
    """Return the cell ids a `// Cells:` line cites, or None if it is not one.

    An empty list means the line is a Cells comment that cites nothing.
    """
    m = CELLS_COMMENT.match(line)
    if not m:
        return None
    return [part.strip() for part in m.group(1).split(",") if part.strip()]


def go_test_funcs(repo):
    """Collect every function name declared in a _test.go file under repo."""
    names = set()
    for root in TEST_ROOTS:
        for path in sorted(Path(repo).joinpath(root).rglob("*_test.go")):
            names.update(GO_FUNC.findall(path.read_text(encoding="utf-8", errors="replace")))
    return names


def cell_refs(repo):
    """Collect (path, line number, cell id) for every `// Cells:` citation."""
    out = []
    base = Path(repo)
    for path in sorted(base.joinpath(CELL_REF_ROOT).rglob("*_test.go")):
        text = path.read_text(encoding="utf-8", errors="replace")
        for lineno, line in enumerate(text.splitlines(), start=1):
            ids = parse_cells_comment(line)
            if not ids:
                continue
            rel = path.relative_to(base).as_posix()
            out.extend((rel, lineno, cid) for cid in ids)
    return out


def check(grid, funcs, refs):
    """Return the list of violation messages, empty when the two agree."""
    problems = []
    for cell in grid:
        if cell.get("status") != DECIDED:
            continue
        for name in (cell.get("coverage") or {}).get("unit") or []:
            if name not in funcs:
                problems.append(
                    f"{cell['id']}: coverage.unit names {name}, "
                    f"which is no Go test function under {_roots()}"
                )
    ids = {cell.get("id") for cell in grid}
    for path, lineno, cid in refs:
        if cid not in ids:
            problems.append(f"{path}:{lineno}: Cells comment references unknown cell id {cid}")
    return problems


def report(grid):
    """Summarize decided-cell coverage per machine.

    An n/a cell describes a key that cannot occur, so it is left out: there
    is nothing for a test to exercise, and counting it as a gap would make
    the backlog unreachable.
    """
    out = {}
    for cell in grid:
        if cell.get("status") != DECIDED or cell.get("outcome") == NA:
            continue
        machine = cell["id"].split("/", 1)[0]
        bucket = out.setdefault(machine, {"covered": 0, "uncovered": []})
        coverage = cell.get("coverage") or {}
        if any(coverage.get(kind) for kind in ("unit", "perm", "kind")):
            bucket["covered"] += 1
        else:
            bucket["uncovered"].append(cell["id"])
    return out


def load_grid(path):
    with open(path) as fh:
        return json.load(fh)["grid"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grid", required=True, help="grid JSON from rolloutspec-grid")
    parser.add_argument("--repo", required=True, help="repository root to scan for Go tests")
    args = parser.parse_args(argv)

    grid = load_grid(args.grid)
    problems = check(grid, go_test_funcs(args.repo), cell_refs(args.repo))

    for machine, counts in sorted(report(grid).items()):
        total = counts["covered"] + len(counts["uncovered"])
        print(
            f"{machine}: {total} decided cells, {counts['covered']} with coverage, "
            f"{len(counts['uncovered'])} without"
        )
        for cid in counts["uncovered"]:
            print(f"  uncovered: {cid}")

    for p in problems:
        print(p, file=sys.stderr)
    if problems:
        print(f"{len(problems)} coverage mismatch(es)", file=sys.stderr)
        return 1
    print("cell coverage names and Cells comments resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main())
