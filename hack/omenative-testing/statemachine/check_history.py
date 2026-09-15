#!/usr/bin/env python3
"""Check that review cells and history sidecar entries agree.

A cell in a machine table carries no PR numbers: nothing under pkg/ may.
The provenance lives in a sidecar under history/, and `status: review` on
the cell is the marker that says provenance exists. A `status: decided`
cell may also carry a sidecar entry, when an owner's decision came from a
merged PR, but it is not required to. The two halves can drift apart in
one direction, and that drift is silent:

  * a review cell with no sidecar entry claims a bugfix history nobody can
    look up.

A sidecar entry must select at least one `review` or `decided` cell: one
that selects only cells with some other status cites PRs the reviewer will
never see, because the heatmap only shows history on review and decided
cells.

The renderer already rejects a sidecar entry that selects no grid cell at
all. This check is the stricter pair of that one.

Usage:

    go run ./hack/omenative-testing/statemachine/cmd/rolloutspec-grid \\
        --machine instance --format json \\
        --history hack/omenative-testing/statemachine/history/instance-noop.yaml \\
        ... --out /tmp/instance.grid.json
    python3 hack/omenative-testing/statemachine/check_history.py \\
        --grid /tmp/instance.grid.json \\
        --history hack/omenative-testing/statemachine/history/instance-*.yaml
"""

import argparse
import json
import sys

import yaml

REVIEW = "review"
DECIDED = "decided"


def selects(variants, windows, transition, entry):
    """Report whether a selector matches one grid entry.

    Mirrors rolloutspec.Selects: an empty list, or an empty transition,
    matches every value, and a transition-scoped selector never matches a
    before-window key, whose transition is always empty.
    """
    if transition and (entry.get("window") == "before" or entry.get("transition") != transition):
        return False
    return _selects_value(variants, entry.get("variant", "")) and _selects_value(
        windows, entry.get("window", "")
    )


def _selects_value(selector, value):
    return not selector or value in selector


def load_grid(path):
    with open(path) as fh:
        return json.load(fh)["grid"]


def load_history(paths):
    """Read every sidecar, returning (file, index, entry) triples."""
    out = []
    for path in paths:
        with open(path) as fh:
            doc = yaml.safe_load(fh) or {}
        for i, entry in enumerate(doc.get("history") or []):
            out.append((path, i, entry))
    return out


def check(grid, entries):
    """Return the list of violation messages, empty when the two agree."""
    problems = []
    for cell in grid:
        if cell.get("status") == REVIEW and not cell.get("history"):
            problems.append(f"review cell with no history entry: {cell['id']}")
    for path, i, entry in entries:
        state = entry.get("state", "")
        event = entry.get("event", "")
        hit = any(
            cell.get("state") == state
            and cell.get("event") == event
            and cell.get("status") in (REVIEW, DECIDED)
            and selects(
                entry.get("variants") or [],
                entry.get("windows") or [],
                entry.get("transition") or "",
                cell,
            )
            for cell in grid
        )
        if not hit:
            problems.append(
                f"{path}[{i}]: history entry for {state}/{event} selects no review or decided cell"
            )
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grid", required=True, help="grid JSON from rolloutspec-grid")
    parser.add_argument(
        "--history", action="append", default=[], help="history sidecar (repeatable)"
    )
    args = parser.parse_args(argv)

    problems = check(load_grid(args.grid), load_history(args.history))
    for p in problems:
        print(p, file=sys.stderr)
    if problems:
        print(f"{len(problems)} provenance mismatch(es)", file=sys.stderr)
        return 1
    print("history sidecars and review/decided cells agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
