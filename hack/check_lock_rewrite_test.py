"""Unit tests for hack/check_lock_rewrite.py (run: python3 -m unittest hack/check_lock_rewrite_test.py)."""

import importlib.util
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    # Registered first: a dataclass with postponed annotations resolves
    # them through sys.modules.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


check = load(HERE / "check_lock_rewrite.py", "check_lock_rewrite_under_test")

CELLS = textwrap.dedent("""\
    cells:
      - state: Ready
        event: pod.ready
        variants: [source]
        outcome: ignore
        placement: unit
        coverage: {perm: [], unit: [TestA, TestB], kind: []}
        status: decided
      - state: Ready
        event: pod.deleted
        windows: [before]
        outcome: transition
        to: Pending
        placement: kind
        coverage: {unit: [TestC]}
        status: decided
      - state: Ready
        event: spec.strategy
        outcome: ignore
        status: current
    """)

RUN_PY = textwrap.dedent('''\
    """The contract: a pod that fails readiness is never rebuilt."""
    def judge(comp, n):
        checks = []
        checks.append(f"  ✓ {comp}: {n} pods back on the new revision")
        checks.append("  ✗ status never reported the loss")
        checks.append("plain string, no glyph")
        return checks
    ''')

COVERAGE_PY = textwrap.dedent('''\
    STORIES = [
        (1, "I push a new revision and it rolls out.",
         lambda c: c.operation == "push"),
        (2, "The running revision breaks and I push the fix; "
            "nothing rebuilds the broken revision.",
         lambda c: c.has_fault()),
    ]
    ''')


def cells(text):
    return check.parse_cells(text, "cells-test.yaml")


def edit(text, old, new):
    """text with old replaced by new; old must be there."""
    assert old in text, old
    return text.replace(old, new)


def change(status, old=None, new=None):
    return check.Change(status, old, new)


class Goldens(unittest.TestCase):
    def test_modified_deleted_and_renamed_with_changes_fire_added_does_not(self):
        found = check.golden_rewrites([
            change("M", check.GOLDEN_DIR + "a.trace", check.GOLDEN_DIR + "a.trace"),
            change("D", check.GOLDEN_DIR + "b.trace", None),
            change("R087", check.GOLDEN_DIR + "c.trace", check.GOLDEN_DIR + "d.trace"),
            change("R100", check.GOLDEN_DIR + "e.trace", check.GOLDEN_DIR + "f.trace"),
            change("A", None, check.GOLDEN_DIR + "g.trace"),
            change("M", "tests/rolloutspec/replay/scenarios/a.yaml", "tests/rolloutspec/replay/scenarios/a.yaml"),
        ])
        self.assertEqual(found, [
            "golden rewritten: " + check.GOLDEN_DIR + "a.trace",
            "golden deleted: " + check.GOLDEN_DIR + "b.trace",
            "golden rewritten under a new name: " + check.GOLDEN_DIR + "c.trace -> " + check.GOLDEN_DIR + "d.trace",
        ])


class DecidedCells(unittest.TestCase):
    def test_unchanged_table_is_clean(self):
        self.assertEqual(check.decided_rewrites("instance", cells(CELLS), cells(CELLS)), [])

    def test_outcome_placement_status_and_to_changes_fire(self):
        head = edit(edit(CELLS, "outcome: ignore\n    placement: unit", "outcome: defer\n    placement: perm"),
                    "to: Pending", "to: Failed")
        found = check.decided_rewrites("instance", cells(CELLS), cells(head))
        self.assertEqual(found, [
            "decided row changed: instance Ready/pod.deleted windows=[before] variants=[] transition=: to 'Pending' -> 'Failed'",
            "decided row changed: instance Ready/pod.ready windows=[] variants=[source] transition=: outcome 'ignore' -> 'defer'",
            "decided row changed: instance Ready/pod.ready windows=[] variants=[source] transition=: placement 'unit' -> 'perm'",
        ])
        head = edit(CELLS, "status: decided\n  - state: Ready\n    event: spec", "status: current\n  - state: Ready\n    event: spec")
        found = check.decided_rewrites("instance", cells(CELLS), cells(head))
        self.assertEqual(found, [
            "decided row changed: instance Ready/pod.deleted windows=[before] variants=[] transition=: status 'decided' -> 'current'",
        ])

    def test_lost_coverage_fires_and_added_coverage_does_not(self):
        head = edit(CELLS, "unit: [TestA, TestB]", "unit: [TestB, TestD]")
        found = check.decided_rewrites("instance", cells(CELLS), cells(head))
        self.assertEqual(found, [
            "decided row lost coverage: instance Ready/pod.ready windows=[] variants=[source] transition=: unit TestA",
        ])
        head = edit(CELLS, "unit: [TestA, TestB]", "unit: [TestA, TestB, TestD], replay: [walk]")
        self.assertEqual(check.decided_rewrites("instance", cells(CELLS), cells(head)), [])

    def test_removed_or_rekeyed_row_fires(self):
        head = edit(CELLS, "windows: [before]\n", "windows: [before, boundary]\n")
        found = check.decided_rewrites("instance", cells(CELLS), cells(head))
        self.assertEqual(found, [
            "decided row removed or re-keyed: instance Ready/pod.deleted windows=[before] variants=[] transition=",
        ])

    def test_a_current_row_may_change_freely(self):
        head = edit(CELLS, "event: spec.strategy\n    outcome: ignore", "event: spec.strategy\n    outcome: reject")
        self.assertEqual(check.decided_rewrites("instance", cells(CELLS), cells(head)), [])

    def test_deciding_a_current_row_is_not_a_rewrite(self):
        head = edit(CELLS, "outcome: ignore\n    status: current", "outcome: ignore\n    placement: unit\n    coverage: {unit: [TestE]}\n    status: decided")
        self.assertEqual(check.decided_rewrites("instance", cells(CELLS), cells(head)), [])

    def test_machines_touched_names_the_machine_of_a_cells_file_only(self):
        touched = check.machines_touched([
            change("M", check.MACHINES_DIR + "instance/cells-noop.yaml", check.MACHINES_DIR + "instance/cells-noop.yaml"),
            change("M", check.MACHINES_DIR + "instance/states.yaml", check.MACHINES_DIR + "instance/states.yaml"),
            change("A", None, check.MACHINES_DIR + "other/cells-a.yaml"),
            change("M", check.MACHINES_DIR + "instance.coverage-floor.yaml", check.MACHINES_DIR + "instance.coverage-floor.yaml"),
        ])
        self.assertEqual(touched, {check.MACHINES_DIR + "instance/", check.MACHINES_DIR + "other/"})


class HarnessContract(unittest.TestCase):
    def texts(self, base, head):
        return (lambda p: base.get(p)), (lambda p: head.get(p))

    def test_case_header_and_reason_lines_fire_body_does_not(self):
        path = check.CASES_DIR + "gen-rollout/p1000.yaml"
        base = {path: "# p1000: push  Ne=4 expect=done\n# reason: plain bump\nspec: a\n"}
        changes = [change("M", path, path)]
        head = {path: "# p1000: push  Ne=4 expect=held\n# reason: plain bump\nspec: a\n"}
        self.assertEqual(check.case_rewrites(changes, *self.texts(base, head)), [f"case header changed: {path}:1"])
        head = {path: "# p1000: push  Ne=4 expect=done\n# reason: a bump that holds\nspec: a\n"}
        self.assertEqual(check.case_rewrites(changes, *self.texts(base, head)), [f"case header changed: {path}:2"])
        head = {path: "# p1000: push  Ne=4 expect=done\n# reason: plain bump\nspec: b\n"}
        self.assertEqual(check.case_rewrites(changes, *self.texts(base, head)), [])

    def test_deleted_case_fires_added_case_does_not(self):
        path = check.CASES_DIR + "gen-rollout/p1000.yaml"
        base = {path: "# p1000: push\n# reason: r\n"}
        found = check.case_rewrites([change("D", path, None), change("A", None, check.CASES_DIR + "x/p9.yaml")],
                                    *self.texts(base, {}))
        self.assertEqual(found, [f"case deleted: {path}"])

    def test_docstring_change_fires(self):
        changes = [change("M", check.RUN_PY, check.RUN_PY)]
        head = RUN_PY.replace("is never rebuilt", "is rebuilt under RecreateInstanceOnPodRestart")
        found = check.harness_rewrites(changes, *self.texts({check.RUN_PY: RUN_PY}, {check.RUN_PY: head}))
        self.assertEqual(found, [f"{check.RUN_PY}: contract docstring changed"])

    def test_verdict_strings_are_the_glyph_literals_with_fstrings_joined_once(self):
        self.assertEqual(check.verdict_strings(RUN_PY), Counter({
            "  ✓ {}: {} pods back on the new revision": 1,
            "  ✗ status never reported the loss": 1,
        }))

    def test_removed_or_changed_verdict_string_fires_added_does_not(self):
        changes = [change("M", check.RUN_PY, check.RUN_PY)]
        head = RUN_PY.replace("never reported the loss", "reported the loss late")
        found = check.harness_rewrites(changes, *self.texts({check.RUN_PY: RUN_PY}, {check.RUN_PY: head}))
        self.assertEqual(found, [
            f"{check.RUN_PY}: 1 verdict string(s) removed or changed: '✗ status never reported the loss'",
        ])
        head = RUN_PY.replace("return checks", 'checks.append("  ⓘ not judged")\n    return checks')
        self.assertEqual(check.harness_rewrites(changes, *self.texts({check.RUN_PY: RUN_PY}, {check.RUN_PY: head})), [])

    def test_moving_a_verdict_string_is_not_a_rewrite(self):
        changes = [change("M", check.RUN_PY, check.RUN_PY)]
        head = RUN_PY.replace('        checks.append("  ✗ status never reported the loss")\n', "") \
            .replace("return checks", 'checks.append("  ✗ status never reported the loss")\n    return checks')
        self.assertEqual(check.harness_rewrites(changes, *self.texts({check.RUN_PY: RUN_PY}, {check.RUN_PY: head})), [])

    def test_story_removed_or_changed_fires_added_does_not(self):
        changes = [change("M", check.COVERAGE_PY, check.COVERAGE_PY)]
        base = {check.COVERAGE_PY: COVERAGE_PY}
        head = COVERAGE_PY.replace("nothing rebuilds the broken revision", "the broken revision may be rebuilt")
        found = check.harness_rewrites(changes, *self.texts(base, {check.COVERAGE_PY: head}))
        self.assertEqual(len(found), 1)
        self.assertTrue(found[0].startswith(f"{check.COVERAGE_PY}: story 2 changed:"), found[0])
        head = COVERAGE_PY.replace("    (2, ", "    (3, \"A new story.\", lambda c: True),\n    (2, ")
        self.assertEqual(check.harness_rewrites(changes, *self.texts(base, {check.COVERAGE_PY: head})), [])
        head = edit(COVERAGE_PY, '    (1, "I push a new revision and it rolls out.",\n     lambda c: c.operation == "push"),\n', "")
        found = check.harness_rewrites(changes, *self.texts(base, {check.COVERAGE_PY: head}))
        self.assertEqual(found, [f"{check.COVERAGE_PY}: story 1 removed: I push a new revision and it rolls out."])

    def test_the_real_coverage_py_stories_parse(self):
        found = check.stories((REPO / check.COVERAGE_PY).read_text())
        self.assertIsNotNone(found)
        self.assertGreaterEqual(len(found), 10)
        self.assertIn(1, found)

    def test_the_real_run_py_has_verdict_strings_and_a_docstring(self):
        src = (REPO / check.RUN_PY).read_text()
        self.assertGreater(sum(check.verdict_strings(src).values()), 100)
        self.assertTrue(check.module_docstring(src))

    def test_untouched_files_are_not_read(self):
        def boom(path):
            raise AssertionError(f"read {path}")
        self.assertEqual(check.harness_rewrites([change("M", "pkg/x.go", "pkg/x.go")], boom, boom), [])


class EvalLine(unittest.TestCase):
    def test_a_selection_or_a_running_mode_is_accepted(self):
        for value in ("families=gen-heal", "cases=p3000,p3001", "lanes=rollout/engine@default", "smoke", "Targeted", "full"):
            with self.subTest(value=value):
                got, refusal = check.eval_line(f"[Core] a fix\n\nbody\n\nEval: {value}\n")
                self.assertEqual(got, value)
                self.assertIsNone(refusal)

    def test_missing_off_unknown_and_empty_selections_are_refused(self):
        for message in ("[Core] a fix\n", "Eval: off\n", "Eval: sometimes\n", "Eval: families=\n", "Eval: cases= , \n"):
            with self.subTest(message=message):
                _, refusal = check.eval_line(message)
                self.assertIsNotNone(refusal)

    def test_the_newest_line_wins_as_in_the_pipeline(self):
        got, refusal = check.eval_line("Eval: families=gen-heal\nEval: off\n")
        self.assertEqual(got, "off")
        self.assertIsNotNone(refusal)

    def test_the_trailer_grammar_matches_the_pipeline(self):
        gp = load(REPO / ".buildkite" / "generate_pipeline.py", "generate_pipeline_for_tripwire")
        self.assertEqual(check.EVAL_TRAILER.pattern, gp.EVAL_TRAILER.pattern)
        self.assertEqual(check.EVAL_TRAILER.flags, gp.EVAL_TRAILER.flags)
        self.assertEqual(check.EVAL_SELECTION_KINDS, gp.EVAL_SELECTION_KINDS)
        self.assertEqual(check.EVAL_MODES_THAT_RUN, tuple(m for m in gp.EVAL_MODES if m != "off"))


class NameStatus(unittest.TestCase):
    def test_parses_every_status_shape(self):
        raw = b"M\0a\0A\0b\0D\0c\0R090\0d\0e\0C100\0f\0g\0T\0h\0"
        self.assertEqual(check.parse_name_status(raw), [
            change("M", "a", "a"), change("A", None, "b"), change("D", "c", None),
            change("R090", "d", "e"), change("C100", "f", "g"), change("T", "h", "h"),
        ])


class EndToEnd(unittest.TestCase):
    """A throwaway repository with a base commit and one commit on top."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = self.tmp.name
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "test@example.com")
        self.git("config", "user.name", "test")
        self.write(check.GOLDEN_DIR + "walk.trace", "tick=1 pass result=idle\n")
        self.write(check.MACHINES_DIR + "instance/cells-ready.yaml", CELLS)
        self.write(check.CASES_DIR + "gen-rollout/p1000.yaml", "# p1000: push expect=done\n# reason: r\nspec: a\n")
        self.write(check.RUN_PY, RUN_PY)
        self.write(check.COVERAGE_PY, COVERAGE_PY)
        self.write("pkg/engine.go", "package engine\n")
        self.commit("base")
        self.base = self.git("rev-parse", "HEAD").strip()

    def git(self, *args):
        return subprocess.run(["git", "-C", self.repo, *args], check=True, capture_output=True, text=True).stdout

    def write(self, rel, text):
        path = os.path.join(self.repo, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)

    def commit(self, message):
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message)

    def run_check(self, *args):
        return subprocess.run(
            [sys.executable, str(HERE / "check_lock_rewrite.py"), "--repo", self.repo, "--base", self.base, *args],
            capture_output=True, text=True,
        )

    def test_an_engine_only_change_passes(self):
        self.write("pkg/engine.go", "package engine\n\nfunc Fix() {}\n")
        self.commit("[Bugfix] engine: fix")
        proc = self.run_check()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("rewrites no golden", proc.stdout)

    def test_a_golden_rewrite_without_an_eval_line_fails(self):
        self.write(check.GOLDEN_DIR + "walk.trace", "tick=1 pass result=after:5s\n")
        self.commit("[Bugfix] engine: fix")
        proc = self.run_check()
        self.assertEqual(proc.returncode, 1)
        self.assertIn("golden rewritten: " + check.GOLDEN_DIR + "walk.trace", proc.stderr)
        self.assertIn("no Eval: line", proc.stderr)

    def test_a_golden_rewrite_with_an_eval_line_passes_and_lists_it(self):
        self.write(check.GOLDEN_DIR + "walk.trace", "tick=1 pass result=after:5s\n")
        self.commit("[Bugfix] engine: fix\n\nEval: families=gen-rollout\n")
        proc = self.run_check()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("covered by Eval: families=gen-rollout", proc.stdout)
        self.assertIn("golden rewritten", proc.stdout)

    def test_a_decided_row_moved_between_files_is_not_a_rewrite(self):
        text = CELLS
        first, rest = text.split("  - state: Ready\n    event: pod.deleted", 1)
        self.write(check.MACHINES_DIR + "instance/cells-ready.yaml", first)
        self.write(check.MACHINES_DIR + "instance/cells-ready-leave.yaml", "cells:\n  - state: Ready\n    event: pod.deleted" + rest)
        self.commit("[Misc] split the cells file")
        proc = self.run_check()
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_a_decided_row_rewritten_fails_and_names_the_row(self):
        self.write(check.MACHINES_DIR + "instance/cells-ready.yaml", edit(CELLS, "to: Pending", "to: Failed"))
        self.commit("[Core] a reversal")
        proc = self.run_check()
        self.assertEqual(proc.returncode, 1)
        self.assertIn("decided row changed: instance Ready/pod.deleted", proc.stderr)

    def test_a_case_header_and_a_verdict_string_are_named(self):
        self.write(check.CASES_DIR + "gen-rollout/p1000.yaml", "# p1000: push expect=held\n# reason: r\nspec: a\n")
        self.write(check.RUN_PY, RUN_PY.replace("never reported", "reported"))
        self.commit("[CI/Tests] perm: loosen")
        proc = self.run_check()
        self.assertEqual(proc.returncode, 1)
        self.assertIn("case header changed: " + check.CASES_DIR + "gen-rollout/p1000.yaml:1", proc.stderr)
        self.assertIn("verdict string(s) removed or changed", proc.stderr)

    def test_a_head_on_the_base_branch_is_judged_against_its_parent(self):
        self.write(check.GOLDEN_DIR + "walk.trace", "tick=1 pass result=after:5s\n")
        self.commit("[Bugfix] landed on main")
        head = self.git("rev-parse", "HEAD").strip()
        proc = subprocess.run(
            [sys.executable, str(HERE / "check_lock_rewrite.py"), "--repo", self.repo, "--base", head, "--head", head],
            capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 1)
        self.assertIn("golden rewritten", proc.stderr)

    def test_an_unknown_ref_is_a_usage_error(self):
        proc = self.run_check("--head", "no-such-ref")
        self.assertEqual(proc.returncode, 2)


if __name__ == "__main__":
    unittest.main()
