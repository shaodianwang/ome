#!/usr/bin/env python3
"""Check the workload tree's test layout, and who is allowed to write a row.

Two properties, both structural, both cheap enough to run before any Go
compiles.

Test layout. One production file, one test file, named for each other:

  * a production file that declares at least one function has a
    `<file>_test.go`. A file that declares none — a file of type, constant
    or variable declarations — is exempt: there is nothing to exercise, and
    it is covered wherever it is consumed.
  * a production file has at most one test file. Where Go forces the split
    (an internal test reaching unexported state, an external test pinning
    the package's surface as a consumer sees it), `<file>_test.go` and
    `<file>_external_test.go` are both allowed for that file and nothing
    else.
  * a test file names a production file. A scenario-named test file that
    matches none is what a reader greps for and does not find, and what the
    next restructure leaves stale without noticing.
  * `helpers_test.go` is the one shared-fixture file per package, and is
    exempt from all three. Where the package's tests split across the
    internal and external test packages, its external half is
    `helpers_external_test.go` — a fixture a `package x_test` file calls
    cannot be declared in a `package x` file, so the same forcing that
    splits a test file splits the fixture file.

Row writes. A row-mutation closure — `func(s *InstanceStatus) bool`, the
callback the status adapter applies — may be declared only in the status
package. Every other package decides and calls a named stamp: the
operation passes, the escalation pass and the reconcile root hold no
mutation closure of their own. The check is an allow-list of one writer
rather than a list of readers so that a package added later is checked by
default: the hold pass, the evidence classifiers, the value types, the
queries and the gang, audit and revision helpers read the row and decide
from it. They do not write it, and a closure appearing in one of them is
that separation breaking down.

Two places outside the status package spell the callback's signature and
are not writers, which the check tells apart by shape rather than by
name. `types/input.go` declares the callback's TYPE on the adapter
contract, and `replay/rowstore.go` — the harness's in-memory store — takes
the callback as a PARAMETER; neither has a body, and a closure without a
body writes nothing. A closure that does not name its parameter,
`func(*InstanceStatus) bool { return false }`, is the no-op carrier a batch
preflight hangs its snapshot guard on; it cannot touch a row it cannot
refer to, so it is not a write either.

Usage:

    python3 check_test_layout.py [--repo .]

Exits non-zero on any violation, printing one line per violation with the
file path and the rule it breaks.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

# The tree the layout rule governs.
WORKLOAD_ROOT = os.path.join("pkg", "controller", "v1beta1", "workload")

# The shared-fixture files per package: the internal one, and the external
# half for the fixtures an external test file must be able to call.
HELPERS = ("helpers_test.go", "helpers_external_test.go")

# The suffix that marks the external half of a forced internal/external split.
EXTERNAL_SUFFIX = "_external"

# A Go function declaration at the start of a line.
FUNC_DECL = re.compile(r"^func\b", re.MULTILINE)

# A row-mutation closure literal: the callback the status adapter applies to
# one Instance row, with a body, on one line or spread over several. The
# parameter must be named — an unnamed parameter cannot reach the row — and
# the trailing brace is what separates authoring the closure from declaring
# the callback's type or taking it as a parameter, which the adapter
# contract (types/input.go) and the replay store (replay/rowstore.go)
# legitimately do.
ROW_MUTATION = re.compile(
    r"func\(\s*\w+\s+\*(?:\w+\.)?InstanceStatus\s*,?\s*\)\s*bool\s*\{"
)

# Where a row-mutation closure may be declared, as repo-relative package
# directories. Matched exactly, not by prefix: a subpackage of one of these
# is a new package and is checked.
ROW_WRITERS = (
    os.path.join(WORKLOAD_ROOT, "status"),
)


def is_test_file(name: str) -> bool:
    return name.endswith("_test.go")


def declares_func(path: str) -> bool:
    """Whether the file declares at least one function."""
    with open(path, encoding="utf-8") as handle:
        return FUNC_DECL.search(handle.read()) is not None


def production_stem(test_name: str) -> tuple[str, str]:
    """The production stem a test file names, and the split half it is.

    `create_test.go` -> ("create", "internal");
    `create_external_test.go` -> ("create", "external").
    """
    stem = test_name[: -len("_test.go")]
    if stem.endswith(EXTERNAL_SUFFIX):
        return stem[: -len(EXTERNAL_SUFFIX)], "external"
    return stem, "internal"


def walk_packages(root: str):
    """Yield (directory, production file names, test file names) per package."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        go_files = sorted(f for f in filenames if f.endswith(".go"))
        if not go_files:
            continue
        production = [f for f in go_files if not is_test_file(f)]
        tests = [f for f in go_files if is_test_file(f)]
        yield dirpath, production, tests


def check_layout(repo: str) -> list[str]:
    root = os.path.join(repo, WORKLOAD_ROOT)
    if not os.path.isdir(root):
        return [f"{WORKLOAD_ROOT}: no such directory under {repo}"]

    violations: list[str] = []
    for dirpath, production, tests in walk_packages(root):
        rel = os.path.relpath(dirpath, repo)

        # Which production files a test file may name, and which of them
        # owe a test file of their own.
        owes_test = {
            name[: -len(".go")]
            for name in production
            if declares_func(os.path.join(dirpath, name))
        }
        known = {name[: -len(".go")] for name in production}

        seen: dict[tuple[str, str], str] = {}
        for test in tests:
            if test in HELPERS:
                continue
            stem, half = production_stem(test)
            # A production file literally named `<x>_external.go` owns
            # `<x>_external_test.go`; the split reading is the fallback.
            if half == "external" and f"{stem}{EXTERNAL_SUFFIX}" in known:
                stem, half = f"{stem}{EXTERNAL_SUFFIX}", "internal"
            if stem not in known:
                violations.append(
                    f"{os.path.join(rel, test)}: names no production file "
                    f"(expected {stem}.go in this package)"
                )
                continue
            if (stem, half) in seen:
                violations.append(
                    f"{os.path.join(rel, test)}: second {half} test file for "
                    f"{stem}.go (already {seen[(stem, half)]})"
                )
                continue
            seen[(stem, half)] = test

        tested = {stem for stem, _ in seen}
        for stem in sorted(owes_test - tested):
            violations.append(
                f"{os.path.join(rel, stem + '.go')}: declares functions and has "
                f"no {stem}_test.go"
            )
    return violations


def check_row_writers(repo: str) -> list[str]:
    allowed = {os.path.normpath(os.path.join(repo, d)) for d in ROW_WRITERS}
    violations: list[str] = []
    for dirpath, production, _ in walk_packages(os.path.join(repo, WORKLOAD_ROOT)):
        if os.path.normpath(dirpath) in allowed:
            continue
        for name in production:
            path = os.path.join(dirpath, name)
            with open(path, encoding="utf-8") as handle:
                if ROW_MUTATION.search(handle.read()):
                    violations.append(
                        f"{os.path.relpath(path, repo)}: declares a row-mutation "
                        f"closure outside the package that writes rows "
                        f"({', '.join(ROW_WRITERS)})"
                    )
    return violations


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".", help="repository root")
    args = parser.parse_args(argv)

    violations = check_layout(args.repo) + check_row_writers(args.repo)
    if violations:
        print(f"❌ test layout: {len(violations)} violation(s)", file=sys.stderr)
        for line in violations:
            print(f"  {line}", file=sys.stderr)
        return 1
    print("✅ test layout: one test file per production file, rows written in one place")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
