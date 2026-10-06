#!/usr/bin/env python3
"""Refuse a rewrite of the rollout behavior lock that names no eval rerun.

The lock is descriptive: a golden records what the engine did, a decided
cell records what the table says it must do, and the perm corpus's case
headers, run.py's contract narrative and verdict lines, and coverage.py's
operator stories record what the harness holds the engine to. A change that
rewrites one of them alongside an engine change passes every fast gate by
construction; only an eval run of the families around the change can tell a
reversal from a fix. This check reads the change set between a base and
HEAD and fails when such a rewrite arrives with no `Eval:` line in the HEAD
commit message, the trailer the pipeline already turns into an eval run.

What counts as a rewrite:

  * a golden under tests/rolloutspec/replay/golden/ modified, deleted or
    renamed with its content changed (a new golden is not one);
  * an authored row of tests/rolloutspec/machines/<m>/cells-*.yaml whose
    status at the base is decided and which, at HEAD, is gone (its
    state/event/variants/windows/transition key no longer appears in any
    cells file of the machine), or whose outcome, to, placement or status
    changed, or which lost a coverage entry (an added entry is not one);
  * a harness contract string:
      - line 1 (the header) or line 2 (the reason) of a case under
        hack/omenative-testing/perm/isvcs/ changed, or the case deleted;
      - the module docstring of run.py changed;
      - a run.py string literal carrying a verdict glyph (one of ✓ ✗ ⚠ ⓘ,
        the check lines a verdict is built from) removed or changed;
      - a coverage.py STORIES entry removed or its text changed.

The Eval: line is read the way the pipeline reads it (the newest line
wins) and must name a run: families=, cases= or lanes= with at least one
name, or the smoke, targeted or full mode. `Eval: off` names nothing.

Usage:

    python3 hack/check_lock_rewrite.py [--base origin/main] [--head HEAD] [--repo .]

The change set is `git diff <merge-base of base and head> <head>`; when
head is already on the base branch, the merge base is head itself and the
change set becomes head's own first-parent diff, so a main build judges the
commit that landed. Exits 1 on a rewrite with no acceptable Eval: line, 2
on a usage or git error, 0 otherwise.
"""

from __future__ import annotations

import argparse
import ast
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass

try:
    import yaml
except ImportError:  # pragma: no cover - reported at run time
    yaml = None

# Where each half of the lock lives.
GOLDEN_DIR = "tests/rolloutspec/replay/golden/"
MACHINES_DIR = "tests/rolloutspec/machines/"
CASES_DIR = "hack/omenative-testing/perm/isvcs/"
RUN_PY = "hack/omenative-testing/perm/run.py"
COVERAGE_PY = "hack/omenative-testing/perm/coverage.py"

# The glyphs a run.py check line carries; a string literal with one is a
# verdict string.
VERDICT_GLYPHS = ("✓", "✗", "⚠", "ⓘ")

# The trailer exactly as .buildkite/generate_pipeline.py parses it; the
# unit tests hold the two in lockstep.
EVAL_TRAILER = re.compile(r"^[ \t]*eval:[ \t]*(\S.*?)[ \t]*$", re.IGNORECASE | re.MULTILINE)
EVAL_SELECTION_KINDS = ("families", "cases", "lanes")
EVAL_MODES_THAT_RUN = ("smoke", "targeted", "full")

# The cell fields a decided row is held to, besides its coverage.
DECIDED_FIELDS = ("outcome", "to", "placement", "status")
COVERAGE_KINDS = ("unit", "perm", "kind", "replay")
STATUS_DECIDED = "decided"

# How many removed verdict strings a finding spells out.
VERDICT_SAMPLE = 5


@dataclass(frozen=True)
class Change:
    """One entry of `git diff --name-status`: status letter, the path at
    the base (None when added) and the path at head (None when deleted)."""

    status: str
    old: str | None
    new: str | None

    @property
    def path(self) -> str:
        return self.new if self.new is not None else self.old  # type: ignore[return-value]


# ── git ─────────────────────────────────────────────────────────────────────


class GitError(Exception):
    pass


def git(repo: str, *args: str, data: bool = False) -> str | bytes:
    proc = subprocess.run(
        ["git", "-C", repo, *args], capture_output=True, text=not data
    )
    if proc.returncode != 0:
        err = proc.stderr if isinstance(proc.stderr, str) else proc.stderr.decode(errors="replace")
        raise GitError(f"git {' '.join(args)}: {err.strip()}")
    return proc.stdout


def resolve(repo: str, ref: str) -> str:
    return str(git(repo, "rev-parse", "--verify", f"{ref}^{{commit}}")).strip()


def change_set(repo: str, base: str, head: str) -> tuple[str, str, list[Change]]:
    """(base sha, head sha, changes) for base...head. A head already on the
    base branch is judged against its first parent."""
    head_sha = resolve(repo, head)
    base_sha = str(git(repo, "merge-base", base, head_sha)).strip()
    if base_sha == head_sha:
        parents = str(git(repo, "rev-list", "--parents", "-n", "1", head_sha)).split()[1:]
        if not parents:
            return head_sha, head_sha, []
        base_sha = parents[0]
    raw = git(repo, "diff", "--name-status", "-z", "-M", base_sha, head_sha, data=True)
    return base_sha, head_sha, parse_name_status(raw)


def parse_name_status(raw: bytes) -> list[Change]:
    fields = [f.decode("utf-8", errors="surrogateescape") for f in raw.split(b"\0") if f]
    changes: list[Change] = []
    i = 0
    while i < len(fields):
        status = fields[i]
        letter = status[0]
        if letter in ("R", "C"):
            changes.append(Change(status, fields[i + 1], fields[i + 2]))
            i += 3
        elif letter == "A":
            changes.append(Change(status, None, fields[i + 1]))
            i += 2
        elif letter == "D":
            changes.append(Change(status, fields[i + 1], None))
            i += 2
        else:
            changes.append(Change(status, fields[i + 1], fields[i + 1]))
            i += 2
    return changes


def show(repo: str, sha: str, path: str) -> str | None:
    """The file's text at sha, or None when it does not exist there."""
    proc = subprocess.run(
        ["git", "-C", repo, "show", f"{sha}:{path}"], capture_output=True
    )
    if proc.returncode != 0:
        return None
    return proc.stdout.decode("utf-8", errors="surrogateescape")


def ls_tree(repo: str, sha: str, directory: str) -> list[str]:
    proc = subprocess.run(
        ["git", "-C", repo, "ls-tree", "--name-only", sha, directory],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return []
    return [line for line in proc.stdout.splitlines() if line]


def commit_message(repo: str, sha: str) -> str:
    return str(git(repo, "log", "-1", "--format=%B", sha))


# ── goldens ─────────────────────────────────────────────────────────────────


def golden_rewrites(changes: list[Change]) -> list[str]:
    out = []
    for c in changes:
        if not (c.path.startswith(GOLDEN_DIR) or (c.old or "").startswith(GOLDEN_DIR)):
            continue
        letter = c.status[0]
        if letter == "M" or letter == "T":
            out.append(f"golden rewritten: {c.path}")
        elif letter == "D":
            out.append(f"golden deleted: {c.old}")
        elif letter == "R" and c.status != "R100":
            out.append(f"golden rewritten under a new name: {c.old} -> {c.new}")
    return out


# ── decided cells ───────────────────────────────────────────────────────────


def cell_key(cell: dict) -> tuple:
    """The identity of an authored row: its coordinate and selector."""
    return (
        str(cell.get("state", "")),
        str(cell.get("event", "")),
        tuple(str(v) for v in (cell.get("variants") or [])),
        tuple(str(w) for w in (cell.get("windows") or [])),
        str(cell.get("transition") or ""),
    )


def describe_key(machine: str, key: tuple) -> str:
    state, event, variants, windows, transition = key
    return (f"{machine} {state}/{event} windows=[{' '.join(windows)}] "
            f"variants=[{' '.join(variants)}] transition={transition}")


def parse_cells(text: str, where: str) -> dict[tuple, dict]:
    """Authored rows of one cells fragment, keyed by cell_key."""
    if yaml is None:
        raise GitError("PyYAML is required to read the cells files (python3-yaml)")
    try:
        doc = yaml.safe_load(text) or {}
    except yaml.YAMLError as err:
        raise GitError(f"{where}: {err}") from err
    rows = doc.get("cells") if isinstance(doc, dict) else None
    out: dict[tuple, dict] = {}
    for cell in rows or []:
        if isinstance(cell, dict):
            out[cell_key(cell)] = cell
    return out


def cells_of_machine(repo: str, sha: str, machine_dir: str) -> dict[tuple, dict]:
    """Every authored row of the machine at sha, across its cells files."""
    cells: dict[tuple, dict] = {}
    for path in ls_tree(repo, sha, machine_dir):
        name = path.rsplit("/", 1)[-1]
        if name.startswith("cells-") and name.endswith(".yaml"):
            text = show(repo, sha, path)
            if text is not None:
                cells.update(parse_cells(text, path))
    return cells


def coverage_names(cell: dict) -> dict[str, set[str]]:
    cov = cell.get("coverage") or {}
    return {kind: set(str(n) for n in (cov.get(kind) or [])) for kind in COVERAGE_KINDS}


def decided_rewrites(machine: str, base: dict[tuple, dict], head: dict[tuple, dict]) -> list[str]:
    """Findings for the base's decided rows that head rewrote."""
    out = []
    for key in sorted(base):
        before = base[key]
        if str(before.get("status", "")) != STATUS_DECIDED:
            continue
        after = head.get(key)
        where = describe_key(machine, key)
        if after is None:
            out.append(f"decided row removed or re-keyed: {where}")
            continue
        for field in DECIDED_FIELDS:
            was, now = before.get(field), after.get(field)
            if was != now:
                out.append(f"decided row changed: {where}: {field} {was!r} -> {now!r}")
        was_cov, now_cov = coverage_names(before), coverage_names(after)
        for kind in COVERAGE_KINDS:
            lost = sorted(was_cov[kind] - now_cov[kind])
            if lost:
                out.append(f"decided row lost coverage: {where}: {kind} {', '.join(lost)}")
    return out


def machines_touched(changes: list[Change]) -> set[str]:
    dirs = set()
    for c in changes:
        for path in (c.old, c.new):
            if path and path.startswith(MACHINES_DIR):
                rest = path[len(MACHINES_DIR):]
                if "/" in rest and rest.rsplit("/", 1)[-1].startswith("cells-"):
                    dirs.add(MACHINES_DIR + rest.split("/", 1)[0] + "/")
    return dirs


# ── harness contract strings ────────────────────────────────────────────────


def header_lines(text: str | None) -> list[str]:
    """The header and the reason: a case's first two lines."""
    if text is None:
        return []
    return text.splitlines()[:2]


def case_rewrites(changes: list[Change], base_text, head_text) -> list[str]:
    out = []
    for c in changes:
        in_cases = (c.new or "").startswith(CASES_DIR) or (c.old or "").startswith(CASES_DIR)
        if not in_cases or not c.path.endswith(".yaml") or c.status[0] == "A":
            continue
        if c.status[0] == "D":
            out.append(f"case deleted: {c.old}")
            continue
        before = header_lines(base_text(c.old))
        after = header_lines(head_text(c.new))
        for n, (was, now) in enumerate(zip(before, after), start=1):
            if was != now:
                out.append(f"case header changed: {c.new}:{n}")
        if len(after) < len(before):
            out.append(f"case header shortened: {c.new}")
    return out


def module_docstring(src: str) -> str | None:
    try:
        return ast.get_docstring(ast.parse(src), clean=False)
    except SyntaxError:
        return None


def verdict_strings(src: str) -> Counter:
    """Every string literal of src that carries a verdict glyph. An f-string
    counts as its literal parts joined with {} for the formatted ones."""
    found: Counter = Counter()
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return found
    nodes = list(ast.walk(tree))
    # An f-string's literal parts are Constant nodes of their own; only the
    # joined form is counted.
    parts_of_fstrings = {
        id(value) for node in nodes if isinstance(node, ast.JoinedStr) for value in node.values
    }
    for node in nodes:
        if isinstance(node, ast.JoinedStr):
            parts = []
            for value in node.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    parts.append(value.value)
                else:
                    parts.append("{}")
            text = "".join(parts)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in parts_of_fstrings:
            text = node.value
        else:
            continue
        if any(g in text for g in VERDICT_GLYPHS):
            found[text] += 1
    return found


def removed_verdicts(base: Counter, head: Counter) -> list[str]:
    return sorted(text for text, n in base.items() if head[text] < n)


def stories(src: str) -> dict[int, str] | None:
    """{number: text} of coverage.py's STORIES, or None when the list is
    not where the check expects it."""
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return None
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "STORIES" for t in node.targets):
            continue
        if not isinstance(node.value, ast.List):
            return None
        out: dict[int, str] = {}
        for elt in node.value.elts:
            if (isinstance(elt, ast.Tuple) and len(elt.elts) >= 2
                    and isinstance(elt.elts[0], ast.Constant) and isinstance(elt.elts[1], ast.Constant)):
                out[int(elt.elts[0].value)] = str(elt.elts[1].value)
        return out
    return None


def story_rewrites(base: dict[int, str] | None, head: dict[int, str] | None) -> list[str]:
    if base is None:
        return []
    if head is None:
        return ["STORIES is no longer a literal list the check can read"]
    out = []
    for number in sorted(base):
        if number not in head:
            out.append(f"story {number} removed: {base[number]}")
        elif head[number] != base[number]:
            out.append(f"story {number} changed: {base[number]!r} -> {head[number]!r}")
    return out


def harness_rewrites(changes: list[Change], base_text, head_text) -> list[str]:
    """base_text(path) and head_text(path) return a file's text or None."""
    out = case_rewrites(changes, base_text, head_text)
    touched = {c.old for c in changes if c.old} | {c.new for c in changes if c.new}
    if RUN_PY in touched:
        before, after = base_text(RUN_PY), head_text(RUN_PY)
        if before is not None and after is None:
            out.append(f"{RUN_PY} deleted")
        elif before is not None:
            if module_docstring(before) != module_docstring(after):
                out.append(f"{RUN_PY}: contract docstring changed")
            gone = removed_verdicts(verdict_strings(before), verdict_strings(after))
            if gone:
                sample = "; ".join(repr(s.strip()) for s in gone[:VERDICT_SAMPLE])
                more = f" and {len(gone) - VERDICT_SAMPLE} more" if len(gone) > VERDICT_SAMPLE else ""
                out.append(f"{RUN_PY}: {len(gone)} verdict string(s) removed or changed: {sample}{more}")
    if COVERAGE_PY in touched:
        before, after = base_text(COVERAGE_PY), head_text(COVERAGE_PY)
        if before is not None and after is None:
            out.append(f"{COVERAGE_PY} deleted")
        elif before is not None:
            out.extend(f"{COVERAGE_PY}: {f}" for f in story_rewrites(stories(before), stories(after)))
    return out


# ── the Eval: line ──────────────────────────────────────────────────────────


def eval_line(message: str) -> tuple[str | None, str | None]:
    """(value, why it does not name a run). The newest Eval: line wins, as
    in the pipeline. value is None when the message has no such line."""
    value = None
    for match in EVAL_TRAILER.finditer(message):
        value = match.group(1)
    if value is None:
        return None, "no Eval: line in the HEAD commit message"
    kind, sep, names = value.lower().partition("=")
    kind = kind.strip()
    if kind in EVAL_SELECTION_KINDS:
        if any(n.strip() for n in names.split(",")):
            return value, None
        return value, f"Eval: {kind}= names nothing"
    if not sep and kind in EVAL_MODES_THAT_RUN:
        return value, None
    return value, (f"Eval: {value!r} runs nothing; name families=, cases=, lanes= "
                   f"or one of {', '.join(EVAL_MODES_THAT_RUN)}")


# ── main ────────────────────────────────────────────────────────────────────


def findings_for(repo: str, base_sha: str, head_sha: str, changes: list[Change]) -> list[str]:
    base_text = lambda path: show(repo, base_sha, path) if path else None  # noqa: E731
    head_text = lambda path: show(repo, head_sha, path) if path else None  # noqa: E731
    out = golden_rewrites(changes)
    for machine_dir in sorted(machines_touched(changes)):
        machine = machine_dir[len(MACHINES_DIR):].rstrip("/")
        out.extend(decided_rewrites(
            machine,
            cells_of_machine(repo, base_sha, machine_dir),
            cells_of_machine(repo, head_sha, machine_dir),
        ))
    out.extend(harness_rewrites(changes, base_text, head_text))
    return out


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", default=".", help="repository root")
    parser.add_argument("--base", default="origin/main", help="the branch the change set is measured from")
    parser.add_argument("--head", default="HEAD", help="the commit judged, with its message")
    args = parser.parse_args(argv)

    try:
        base_sha, head_sha, changes = change_set(args.repo, args.base, args.head)
        findings = findings_for(args.repo, base_sha, head_sha, changes)
        value, refusal = eval_line(commit_message(args.repo, head_sha))
    except GitError as err:
        print(f"❌ lock rewrite: {err}", file=sys.stderr)
        return 2

    span = f"{base_sha[:9]}..{head_sha[:9]}"
    if not findings:
        print(f"✅ lock rewrite: {span} rewrites no golden, decided row or harness contract")
        return 0
    if refusal is None:
        print(f"✅ lock rewrite: {span} has {len(findings)} rewrite(s), covered by Eval: {value}")
        for line in findings:
            print(f"  {line}")
        return 0
    print(f"❌ lock rewrite: {span} has {len(findings)} rewrite(s) and {refusal}", file=sys.stderr)
    for line in findings:
        print(f"  {line}", file=sys.stderr)
    print("  add an Eval: line to the commit message naming the eval you reran, "
          "e.g. Eval: families=<family> or Eval: cases=<id>,<id>", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
