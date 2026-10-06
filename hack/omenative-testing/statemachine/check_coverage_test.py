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

    def test_subtest_name_resolves_through_its_function(self):
        grid = [cell("A", "e", unit=["TestA/m/A/e"])]
        self.assertEqual(check_coverage.check(grid, {"TestA"}, []), [])

    def test_subtest_of_unknown_function_is_reported(self):
        grid = [cell("A", "e", unit=["TestGone/m/A/e"])]
        problems = check_coverage.check(grid, {"TestA"}, [])
        self.assertEqual(len(problems), 1)
        self.assertIn("TestGone/m/A/e", problems[0])

    def test_cells_comment_for_another_machine_is_left_to_its_render(self):
        grid = [cell("A", "e", unit=["TestA"])]
        refs = [("pkg/x_test.go", 7, "other/A/e/before")]
        self.assertEqual(check_coverage.check(grid, {"TestA"}, refs, machine="m"), [])

    def test_cells_comment_for_this_machine_is_checked_when_scoped(self):
        grid = [cell("A", "e", unit=["TestA"])]
        refs = [("pkg/x_test.go", 7, "m/A/e/nowhere")]
        problems = check_coverage.check(grid, {"TestA"}, refs, machine="m")
        self.assertEqual(len(problems), 1)
        self.assertIn("m/A/e/nowhere", problems[0])

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


def transition(tid, lane="workload"):
    return {"id": tid, "lane": lane}


class ParseArrowsCommentTest(unittest.TestCase):
    def test_comma_separated_ids(self):
        self.assertEqual(
            check_coverage.parse_arrows_comment("// Arrows: T-a, T-b"), ["T-a", "T-b"]
        )

    def test_non_arrows_comment_is_not_a_reference(self):
        self.assertIsNone(check_coverage.parse_arrows_comment("// Cells: m/A/e/before"))


class CheckArrowsTest(unittest.TestCase):
    def test_adapter_lane_arrow_with_a_test_is_clean(self):
        problems = check_coverage.check_arrows(
            [transition("T-a", "adapter")], [("pkg/x_test.go", 1, "T-a")]
        )
        self.assertEqual(problems, [])

    def test_adapter_lane_arrow_without_a_test_is_reported(self):
        problems = check_coverage.check_arrows([transition("T-a", "adapter")], [])
        self.assertEqual(len(problems), 1)
        self.assertIn("named by no Go test", problems[0])

    def test_workload_lane_arrow_may_not_be_tagged(self):
        problems = check_coverage.check_arrows(
            [transition("T-a")], [("pkg/x_test.go", 3, "T-a")]
        )
        self.assertEqual(len(problems), 1)
        self.assertIn("workload-lane", problems[0])

    def test_unknown_arrow_is_reported(self):
        problems = check_coverage.check_arrows(
            [transition("T-a", "adapter")], [("pkg/x_test.go", 3, "T-typo")]
        )
        self.assertEqual(len(problems), 2)
        self.assertIn("unknown transition T-typo", problems[0])

    def test_has_adapter_lane_reads_the_resolved_lane(self):
        self.assertTrue(check_coverage.has_adapter_lane([transition("T-a", "adapter")]))
        self.assertFalse(check_coverage.has_adapter_lane([transition("T-a"), {"id": "T-b"}]))

    def test_missing_lane_defaults_to_workload(self):
        problems = check_coverage.check_arrows(
            [{"id": "T-a"}], [("pkg/x_test.go", 3, "T-a")]
        )
        self.assertIn("workload-lane", problems[0])

    def test_adapter_lane_arrow_from_a_planned_state_owes_no_test(self):
        transitions = [
            {"id": "T-a", "lane": "adapter", "from": "Planned"},
            {"id": "T-b", "lane": "adapter", "from": "Bound"},
        ]
        problems = check_coverage.check_arrows(transitions, [], planned_states=["Planned"])
        self.assertEqual(len(problems), 1)
        self.assertIn("T-b", problems[0])

    def test_planned_state_arrow_may_still_be_tagged(self):
        transitions = [{"id": "T-a", "lane": "adapter", "from": "Planned"}]
        problems = check_coverage.check_arrows(
            transitions, [("pkg/x_test.go", 3, "T-a")], planned_states=["Planned"]
        )
        self.assertEqual(problems, [])


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

    def test_arrow_refs_scan_pkg_test_files_only(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "pkg").mkdir()
            (root / "tests").mkdir()
            (root / "pkg" / "x_test.go").write_text("// Arrows: T-a\nfunc TestA() {}\n")
            (root / "tests" / "y_test.go").write_text("// Arrows: T-z\n")
            refs = check_coverage.arrow_refs(root)
            self.assertEqual([r[2] for r in refs], ["T-a"])

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

    def test_untagged_adapter_lane_transition_fails(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            repo = root / "repo"
            repo.mkdir()
            self._repo(repo, "TestA")
            grid = root / "g.json"
            grid.write_text(
                json.dumps(
                    {
                        "machine": "m",
                        "transitions": [transition("T-a", "adapter")],
                        "grid": [cell("A", "e", unit=["TestA"])],
                    }
                )
            )
            self.assertEqual(
                check_coverage.main(["--grid", str(grid), "--repo", str(repo)]), 1
            )

    def test_arrow_citations_are_checked_only_with_adapter_arrows(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            repo = root / "repo"
            repo.mkdir()
            self._repo(repo, "TestA")
            (repo / "pkg" / "y_test.go").write_text(
                "// Arrows: T-elsewhere\nfunc TestB(t *testing.T) {}\n"
            )
            grid = root / "g.json"
            grid.write_text(
                json.dumps(
                    {
                        "machine": "m",
                        "transitions": [transition("T-a")],
                        "grid": [cell("A", "e", unit=["TestA"])],
                    }
                )
            )
            self.assertEqual(
                check_coverage.main(["--grid", str(grid), "--repo", str(repo)]), 0
            )
            grid.write_text(
                json.dumps(
                    {
                        "machine": "m",
                        "transitions": [transition("T-a", "adapter")],
                        "grid": [cell("A", "e", unit=["TestA"])],
                    }
                )
            )
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
