#!/usr/bin/env python3
import json
import tempfile
import unittest
from pathlib import Path

import check_history


def cell(state, event, variant="", window="before", transition="", status="review", history=()):
    cid = f"m/{state}/{event}"
    if variant:
        cid += f"[{variant}]"
    cid += f"/{window}"
    if transition:
        cid += f"@{transition}"
    return {
        "id": cid,
        "state": state,
        "event": event,
        "variant": variant,
        "window": window,
        "transition": transition,
        "status": status,
        "history": list(history),
    }


def entry(state, event, variants=(), windows=(), transition="", prs=("#1",), decision=""):
    return {
        "state": state,
        "event": event,
        "variants": list(variants),
        "windows": list(windows),
        "transition": transition,
        "prs": list(prs),
        "decision": decision,
    }


class SelectsTest(unittest.TestCase):
    def test_empty_selector_matches_everything(self):
        self.assertTrue(check_history.selects([], [], "", cell("A", "e", variant="x")))

    def test_variant_and_window_narrow(self):
        c = cell("A", "e", variant="x", window="boundary", transition="t")
        self.assertTrue(check_history.selects(["x"], ["boundary"], "", c))
        self.assertFalse(check_history.selects(["y"], [], "", c))
        self.assertFalse(check_history.selects([], ["after"], "", c))

    def test_transition_selector_never_matches_before(self):
        self.assertFalse(check_history.selects([], [], "t", cell("A", "e")))
        self.assertTrue(
            check_history.selects([], [], "t", cell("A", "e", window="after", transition="t"))
        )


class CheckTest(unittest.TestCase):
    def test_agreeing_pair_is_clean(self):
        grid = [cell("A", "e", history=["#1"])]
        self.assertEqual(check_history.check(grid, [("f", 0, entry("A", "e"))]), [])

    def test_review_cell_without_history_is_reported(self):
        problems = check_history.check([cell("A", "e")], [])
        self.assertEqual(len(problems), 1)
        self.assertIn("m/A/e/before", problems[0])

    def test_entry_matching_only_current_cells_is_reported(self):
        grid = [cell("A", "e", status="current")]
        problems = check_history.check(grid, [("f", 0, entry("A", "e"))])
        self.assertEqual(len(problems), 1)
        self.assertIn("selects no review or decided cell", problems[0])

    def test_current_cells_need_no_history(self):
        self.assertEqual(check_history.check([cell("A", "e", status="current")], []), [])

    def test_decided_cell_may_carry_history_entry(self):
        grid = [cell("A", "e", status="decided", history=["#1"])]
        self.assertEqual(check_history.check(grid, [("f", 0, entry("A", "e"))]), [])

    def test_decided_cell_needs_no_history(self):
        self.assertEqual(check_history.check([cell("A", "e", status="decided")], []), [])

    def test_decision_reference_stands_in_for_prs(self):
        grid = [cell("A", "e", status="decided", history=["owner-review"])]
        e = entry("A", "e", prs=(), decision="owner-review")
        self.assertEqual(check_history.check(grid, [("f", 0, e)]), [])

    def test_entry_without_any_reference_is_reported(self):
        grid = [cell("A", "e", history=["#1"])]
        problems = check_history.check(grid, [("f", 0, entry("A", "e", prs=()))])
        self.assertEqual(len(problems), 1)
        self.assertIn("no prs and no decision reference", problems[0])


class MainTest(unittest.TestCase):
    def test_exit_codes(self):
        with tempfile.TemporaryDirectory() as d:
            grid = Path(d) / "g.json"
            grid.write_text(json.dumps({"grid": [cell("A", "e", history=["#1"])]}))
            hist = Path(d) / "h.yaml"
            hist.write_text('history:\n  - {state: A, event: e, prs: ["#1"]}\n')
            self.assertEqual(
                check_history.main(["--grid", str(grid), "--history", str(hist)]), 0
            )

            grid.write_text(json.dumps({"grid": [cell("A", "e")]}))
            self.assertEqual(check_history.main(["--grid", str(grid)]), 1)


if __name__ == "__main__":
    unittest.main()
