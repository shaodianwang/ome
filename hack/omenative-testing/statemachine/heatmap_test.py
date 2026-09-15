#!/usr/bin/env python3
import html
import tempfile
import unittest
from pathlib import Path

import heatmap

HERE = Path(__file__).parent.resolve()


def fixture():
    """Two states, one plain event, one event with two variants. State A has
    two outgoing transitions so boundary/after must split into sub-rows."""
    states = [{"id": "A", "phase": "Ready", "decidedIn": "", "note": ""},
              {"id": "B", "phase": "Updating", "decidedIn": "", "note": ""}]
    events = [{"id": "e1", "family": "pod", "variants": [], "description": ""},
              {"id": "e2", "family": "operator", "variants": ["x", "y"], "description": ""}]
    transitions = [{"id": "t1", "from": "A", "on": "e1", "variant": "", "guard": "g1", "to": "B", "decidedIn": ""},
                   {"id": "t2", "from": "A", "on": "e2", "variant": "x", "guard": "g2", "to": "B", "decidedIn": ""}]
    cols = [("e1", ""), ("e2", "x"), ("e2", "y")]
    grid = []

    def entry(state, ev, var, window, transition, outcome, status):
        cid = f"demo/{state}/{ev}"
        if var:
            cid += f"[{var}]"
        cid += f"/{window}"
        if transition:
            cid += f"@{transition}"
        return {"id": cid, "state": state, "event": ev, "variant": var, "window": window,
                "transition": transition, "outcome": outcome, "to": "B" if outcome == "transition" else "",
                "status": status, "note": "n", "reason": "r" if outcome == "n/a" else "", "guard": "",
                "invariants": [], "placement": "unit", "history": ["#1"], "synthesized": outcome == "n/a"}

    for ev, var in cols:
        grid.append(entry("A", ev, var, "before", "", "transition", "review"))
        grid.append(entry("B", ev, var, "before", "", "ignore", "current"))
        for t in ("t1", "t2"):
            grid.append(entry("A", ev, var, "boundary", t, "DECIDE", "current"))
            grid.append(entry("A", ev, var, "after", t, "defer", "decided"))
        grid.append(entry("B", ev, var, "boundary", "", "n/a", "current"))
        grid.append(entry("B", ev, var, "after", "", "n/a", "current"))
    return {"machine": "demo", "scope": "s", "frozen": False, "states": states,
            "events": events, "transitions": transitions, "grid": grid}


class OutcomeClasses(unittest.TestCase):
    def test_every_outcome_has_a_class(self):
        for o in ("transition", "ignore", "defer", "reject", "escalate", "n/a", "DECIDE"):
            self.assertIn(o, heatmap.OUTCOME_CLASS)
        self.assertEqual(heatmap.OUTCOME_CLASS["DECIDE"], "o-decide")

    def test_status_classes(self):
        self.assertEqual(heatmap.STATUS_CLASS["review"], "s-review")
        self.assertEqual(heatmap.STATUS_CLASS["current"], "s-current")


class Columns(unittest.TestCase):
    def test_variants_expand_to_columns(self):
        labels = [c[2] for c in heatmap.columns(fixture()["events"])]
        self.assertEqual(labels, ["e1", "e2[x]", "e2[y]"])


class Rows(unittest.TestCase):
    def test_before_is_one_row_per_state(self):
        self.assertEqual(heatmap.rows_for_window(fixture(), "before"), [("A", ""), ("B", "")])

    def test_boundary_splits_multi_transition_states(self):
        self.assertEqual(heatmap.rows_for_window(fixture(), "boundary"),
                         [("A", "t1"), ("A", "t2"), ("B", "")])


class Counts(unittest.TestCase):
    def test_window_counts(self):
        c = heatmap.window_counts(fixture()["grid"], "boundary")
        self.assertEqual(c["DECIDE"], 6)
        self.assertEqual(c["n/a"], 3)
        c = heatmap.window_counts(fixture()["grid"], "before")
        self.assertEqual(c["review"], 3)
        self.assertEqual(c["current"], 3)


class Render(unittest.TestCase):
    def test_html_contains_every_id_and_three_tables(self):
        out = heatmap.render(fixture())
        for needle in ("demo", ">A<", ">B<", "e1", "e2[x]", "e2[y]", "t1", "t2"):
            self.assertIn(needle, out)
        self.assertEqual(out.count("<table"), 3)
        self.assertIn("o-decide", out)
        self.assertIn("s-review", out)
        self.assertNotIn("<script", out)

    def test_html_contains_every_cell_id(self):
        env = fixture()
        out = heatmap.render(env)
        for g in env["grid"]:
            self.assertIn(g["id"], out)

    def test_state_and_transition_columns_are_sticky(self):
        out = heatmap.render(fixture())
        self.assertIn('th class="state"', out)
        self.assertIn('td class="state"', out)
        self.assertIn('th class="transition"', out)
        self.assertIn('td class="transition"', out)
        self.assertIn("position:sticky", heatmap.CSS.replace(" ", ""))
        self.assertIn(f"left:{heatmap.STATE_COL_PX}px", heatmap.CSS.replace(" ", ""))

    def test_new_state_starts_a_banded_row(self):
        out = heatmap.render(fixture())
        # Each of the 3 tables has 2 states, so 2 state-start rows per table
        # regardless of how many transition sub-rows a state expands into.
        self.assertEqual(out.count('class="state-start"'), 6)

    def test_counts_line_has_a_total(self):
        out = heatmap.render(fixture())
        self.assertEqual(out.count("total: 6"), 1, "before window: 3 review + 3 current")
        self.assertEqual(out.count("total: 9"), 2, "boundary and after windows: 6 + 3 each")

    def test_frozen_renders_lowercase_like_json(self):
        env = fixture()
        env["frozen"] = True
        self.assertIn("frozen: true", heatmap.render(env))
        env["frozen"] = False
        self.assertIn("frozen: false", heatmap.render(env))

    def test_main_writes_file(self):
        with tempfile.TemporaryDirectory() as d:
            grid = Path(d) / "g.json"
            out = Path(d) / "g.html"
            grid.write_text(heatmap.json.dumps(fixture()))
            heatmap.main(["--grid", str(grid), "--out", str(out)])
            self.assertTrue(out.exists())
            self.assertIn("<html", out.read_text())


class CellTitle(unittest.TestCase):
    def test_empty_placement_renders_as_dash(self):
        g = {"id": "demo/A/e1/before", "to": "", "transition": "", "note": "", "reason": "no operation in flight",
             "guard": "", "placement": "", "history": [], "invariants": []}
        self.assertIn("placement: —", heatmap.cell_title(g))

    def test_populated_placement_shown_verbatim(self):
        g = {"id": "demo/A/e1/before", "to": "", "transition": "", "note": "", "reason": "",
             "guard": "", "placement": "unit", "history": [], "invariants": []}
        self.assertIn("placement: unit", heatmap.cell_title(g))

    def test_id_is_first_line(self):
        g = {"id": "demo/A/e1/before", "to": "", "transition": "", "note": "", "reason": "",
             "guard": "", "placement": "unit", "history": [], "invariants": []}
        self.assertEqual(heatmap.cell_title(g).splitlines()[0], "id: demo/A/e1/before")


class Coverage(unittest.TestCase):
    def test_stray_grid_entry_raises(self):
        env = fixture()
        env["grid"].append({
            "id": "demo/Z/stray/before", "state": "Z", "event": "stray", "variant": "",
            "window": "before", "transition": "", "outcome": "ignore", "to": "",
            "status": "current", "note": "", "reason": "", "guard": "",
            "invariants": [], "placement": "", "history": [], "synthesized": False,
        })
        with self.assertRaises(heatmap.GridCoverageError):
            heatmap.render(env)

    def test_main_exits_nonzero_on_stray_grid_entry(self):
        env = fixture()
        env["grid"].append({
            "id": "demo/Z/stray/before", "state": "Z", "event": "stray", "variant": "",
            "window": "before", "transition": "", "outcome": "ignore", "to": "",
            "status": "current", "note": "", "reason": "", "guard": "",
            "invariants": [], "placement": "", "history": [], "synthesized": False,
        })
        with tempfile.TemporaryDirectory() as d:
            grid = Path(d) / "g.json"
            out = Path(d) / "g.html"
            grid.write_text(heatmap.json.dumps(env))
            with self.assertRaises(SystemExit):
                heatmap.main(["--grid", str(grid), "--out", str(out)])
            self.assertFalse(out.exists())


class Escaping(unittest.TestCase):
    def test_script_payload_in_note_is_escaped(self):
        env = fixture()
        payload = "<script>alert(1)</script>"
        env["grid"][0]["note"] = payload
        out = heatmap.render(env)
        self.assertIn(html.escape(payload), out)
        self.assertNotIn(payload, out)


if __name__ == "__main__":
    unittest.main()
