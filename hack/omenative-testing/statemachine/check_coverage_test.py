#!/usr/bin/env python3
import json
import tempfile
import unittest
from pathlib import Path

import check_coverage


def cell(
    state,
    event,
    status="decided",
    outcome="transition",
    unit=(),
    perm=(),
    kind=(),
    replay=(),
):
    return {
        "id": f"m/{state}/{event}/before",
        "state": state,
        "event": event,
        "status": status,
        "outcome": outcome,
        "coverage": {
            "unit": list(unit),
            "perm": list(perm),
            "kind": list(kind),
            "replay": list(replay),
        },
    }


class ParseCellsCommentTest(unittest.TestCase):
    def test_single_id(self):
        self.assertEqual(
            check_coverage.parse_cells_comment("// Cells: m/A/e/before"), ["m/A/e/before"]
        )

    def test_comma_separated_ids_and_indentation(self):
        self.assertEqual(
            check_coverage.parse_cells_comment("\t// Cells: m/A/e/before, m/B/f/after@t"),
            ["m/A/e/before", "m/B/f/after@t"],
        )

    def test_non_cells_comment_is_not_a_reference(self):
        self.assertIsNone(check_coverage.parse_cells_comment("// covers the surge path"))

    def test_empty_list_is_not_a_reference(self):
        self.assertEqual(check_coverage.parse_cells_comment("// Cells:"), [])


class CheckTest(unittest.TestCase):
    def test_known_test_name_and_known_cell_id_are_clean(self):
        grid = [cell("A", "e", unit=["TestA"])]
        refs = [("pkg/x_test.go", 3, "m/A/e/before")]
        self.assertEqual(check_coverage.check(grid, {"TestA"}, refs), [])

    def test_unknown_unit_test_name_is_reported(self):
        grid = [cell("A", "e", unit=["TestGone"])]
        problems = check_coverage.check(grid, set(), [])
        self.assertEqual(len(problems), 1)
        self.assertIn("TestGone", problems[0])
        self.assertIn("m/A/e/before", problems[0])

    def test_non_decided_cell_name_is_not_checked(self):
        grid = [cell("A", "e", status="current", unit=["TestGone"])]
        self.assertEqual(check_coverage.check(grid, set(), []), [])

    def test_unknown_cell_id_in_comment_is_reported(self):
        grid = [cell("A", "e", unit=["TestA"])]
        refs = [("pkg/x_test.go", 7, "m/A/e/nowhere")]
        problems = check_coverage.check(grid, {"TestA"}, refs)
        self.assertEqual(len(problems), 1)
        self.assertIn("pkg/x_test.go:7", problems[0])
        self.assertIn("m/A/e/nowhere", problems[0])

    def test_perm_and_kind_names_are_not_go_functions(self):
        grid = [cell("A", "e", perm=["p18"], kind=["rollout converges"])]
        self.assertEqual(check_coverage.check(grid, set(), []), [])

    def test_known_replay_scenario_is_clean(self):
        grid = [cell("A", "e", replay=["T-create-ready"])]
        self.assertEqual(check_coverage.check(grid, set(), [], {"T-create-ready"}), [])

    def test_unknown_replay_scenario_is_reported(self):
        grid = [cell("A", "e", replay=["T-gone"])]
        problems = check_coverage.check(grid, set(), [], {"T-create-ready"})
        self.assertEqual(len(problems), 1)
        self.assertIn("T-gone", problems[0])
        self.assertIn("m/A/e/before", problems[0])


class ReportTest(unittest.TestCase):
    def test_counts_split_by_machine(self):
        grid = [
            cell("A", "e", unit=["TestA"]),
            cell("B", "f"),
            cell("C", "g", status="current"),
            cell("D", "h", outcome="n/a"),
        ]
        report = check_coverage.report(grid)
        self.assertEqual(report["m"]["covered"], 1)
        self.assertEqual(report["m"]["uncovered"], ["m/B/f/before"])

    def test_any_placement_counts_as_covered(self):
        grid = [cell("A", "e", kind=["some kind spec"])]
        self.assertEqual(check_coverage.report(grid)["m"]["covered"], 1)

    def test_replay_scenario_counts_as_covered(self):
        grid = [cell("A", "e", replay=["T-create-ready"])]
        self.assertEqual(check_coverage.report(grid)["m"]["covered"], 1)


class ScanTest(unittest.TestCase):
    def test_go_test_funcs_reads_pkg_and_tests(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "pkg" / "a").mkdir(parents=True)
            (root / "tests" / "b").mkdir(parents=True)
            (root / "pkg" / "a" / "x_test.go").write_text("func TestOne(t *testing.T) {}\n")
            (root / "tests" / "b" / "y_test.go").write_text("func TestTwo(t *testing.T) {}\n")
            (root / "pkg" / "a" / "x.go").write_text("func NotATest() {}\n")
            funcs = check_coverage.go_test_funcs(root)
            self.assertIn("TestOne", funcs)
            self.assertIn("TestTwo", funcs)
            self.assertNotIn("NotATest", funcs)

    def test_cell_refs_scans_pkg_test_files_only(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "pkg").mkdir()
            (root / "tests").mkdir()
            (root / "pkg" / "x_test.go").write_text("// Cells: m/A/e/before\nfunc TestA() {}\n")
            (root / "tests" / "y_test.go").write_text("// Cells: m/Z/z/before\n")
            refs = check_coverage.cell_refs(root)
            self.assertEqual([r[2] for r in refs], ["m/A/e/before"])
            self.assertEqual(refs[0][1], 1)

    def test_replay_scenarios_are_named_by_file_stem(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            scenarios = root / check_coverage.REPLAY_SCENARIO_DIR
            scenarios.mkdir(parents=True)
            (scenarios / "T-create-ready.yaml").write_text("scenario: T-create-ready\n")
            (scenarios / "notes.txt").write_text("not a scenario\n")
            self.assertEqual(check_coverage.replay_scenarios(root), {"T-create-ready"})

    def test_replay_scenarios_tolerate_a_missing_directory(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(check_coverage.replay_scenarios(Path(d)), set())


class MainTest(unittest.TestCase):
    def _repo(self, root, func_name):
        (root / "pkg").mkdir()
        (root / "pkg" / "x_test.go").write_text(f"func {func_name}(t *testing.T) {{}}\n")

    def test_exit_codes(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            repo = root / "repo"
            repo.mkdir()
            self._repo(repo, "TestA")
            grid = root / "g.json"
            grid.write_text(json.dumps({"machine": "m", "grid": [cell("A", "e", unit=["TestA"])]}))
            self.assertEqual(
                check_coverage.main(["--grid", str(grid), "--repo", str(repo)]), 0
            )

            grid.write_text(json.dumps({"machine": "m", "grid": [cell("A", "e", unit=["TestB"])]}))
            self.assertEqual(
                check_coverage.main(["--grid", str(grid), "--repo", str(repo)]), 1
            )

    def test_uncovered_cells_alone_do_not_fail(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            repo = root / "repo"
            repo.mkdir()
            self._repo(repo, "TestA")
            grid = root / "g.json"
            grid.write_text(json.dumps({"machine": "m", "grid": [cell("A", "e")]}))
            self.assertEqual(
                check_coverage.main(["--grid", str(grid), "--repo", str(repo)]), 0
            )


if __name__ == "__main__":
    unittest.main()
