#!/usr/bin/env python3
"""Tests for the pure parts of ci_build.py: reading and tallying the eval
lanes' verdict files. Standard library only; no token and no network."""

from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path

_PATH = Path(__file__).resolve().with_name("ci_build.py")
_spec = importlib.util.spec_from_file_location("ci_build_under_test", _PATH)
cb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cb)


def document(step, lane, cases, scheduler=""):
    """One lane's verdict file: cases is [(case, verdict, reason, fault)]."""
    return {
        "format": cb.VERDICTS_FORMAT, "build": "6822", "job": "0199-" + step, "step": step, "lane": lane,
        "scheduler": scheduler, "commit": "c" * 40, "run": f"6822-{step}", "seconds": 100.0, "counts": {},
        "cases": [
            {
                "case": case, "feature": "rollout", "topology": "pd", "scheduler": scheduler, "verdict": verdict,
                "reason": reason, "exit": 0, "seconds": 12.4, "fault": fault, "log": f"rollout/pd/{case}.log",
                "result": f"rollout/pd/{case}-100.tsv",
            }
            for case, verdict, reason, fault in cases
        ],
    }


class VerdictWordTest(unittest.TestCase):
    def test_the_first_word_names_the_verdict(self):
        self.assertEqual(cb.verdict_word("PASS"), "PASS")
        self.assertEqual(cb.verdict_word("XFAIL (known bug: #1)"), "XFAIL")
        self.assertEqual(cb.verdict_word("INFRA-FAIL"), "INFRA-FAIL")
        self.assertEqual(cb.verdict_word(""), "?")
        self.assertEqual(cb.verdict_word(None), "?")


class ParseVerdictsTest(unittest.TestCase):
    def test_a_verdict_file_parses_and_anything_else_is_refused(self):
        good = document("omenative-e2e-rollout-pd", "rollout-pd", [("p1", "PASS", "", None)])
        self.assertEqual(cb.parse_verdicts(json.dumps(good))["cases"][0]["case"], "p1")
        with self.assertRaisesRegex(ValueError, "not JSON"):
            cb.parse_verdicts("{")
        with self.assertRaisesRegex(ValueError, "not a"):
            cb.parse_verdicts(json.dumps({"format": "other", "cases": []}))
        with self.assertRaisesRegex(ValueError, "not a"):
            cb.parse_verdicts(json.dumps([good]))
        with self.assertRaisesRegex(ValueError, "cases"):
            cb.parse_verdicts(json.dumps({**good, "cases": None}))


class TallyTest(unittest.TestCase):
    def setUp(self):
        self.documents = [
            document("omenative-e2e-rollout-pd", "rollout-pd", [
                ("p1", "PASS", "", None),
                ("p2", "FAIL", "engine never reached 3/3 Ready", "quota:old"),
                ("p3", "XFAIL (known bug: #1)", "parked", None),
            ]),
            document("omenative-e2e-canary-eng", "canary-eng-ome-scheduler", [
                ("p4", "PASS", "", None),
                ("p5", "SKIP", "no node to stop", "node:new"),
            ], scheduler="ome-scheduler"),
            document("omenative-e2e-migration-pd", "migration-pd", [
                ("p6", "INFRA-FAIL", "quota apparatus failed", "quota:all"),
                ("p7", "?", "no result file", None),
            ]),
        ]

    def test_counts_per_word_and_the_red_cases_per_lane(self):
        tally = cb.tally_verdicts(self.documents)
        self.assertEqual(tally["cases"], 7)
        self.assertEqual(tally["counts"], {"?": 1, "FAIL": 1, "INFRA-FAIL": 1, "PASS": 2, "SKIP": 1, "XFAIL": 1})
        self.assertEqual([lane["step"] for lane in tally["lanes"]],
                         ["omenative-e2e-rollout-pd", "omenative-e2e-canary-eng", "omenative-e2e-migration-pd"])
        rollout, canary, migration = tally["lanes"]
        self.assertEqual(rollout["counts"], {"FAIL": 1, "PASS": 1, "XFAIL": 1})
        self.assertEqual([row["case"] for row in rollout["red"]], ["p2"])
        self.assertEqual(canary["red"], [])
        self.assertEqual((canary["scheduler"], canary["cases"], canary["run"]), ("ome-scheduler", 2, "6822-omenative-e2e-canary-eng"))
        self.assertEqual([row["case"] for row in migration["red"]], ["p6", "p7"])

    def test_the_text_names_only_the_lanes_with_a_red_case_unless_asked_for_all(self):
        tally = cb.tally_verdicts(self.documents)
        missing = [{"step_key": "omenative-e2e-reject-eng", "state": "failed", "exit_status": 125}]
        lines = cb.format_tally(6822, tally, missing)
        self.assertEqual(lines[0], "build 6822: 7 cases in 3 lanes with a verdict file: ? 1, FAIL 1, INFRA-FAIL 1, PASS 2, SKIP 1, XFAIL 1")
        lanes = [line for line in lines if line.startswith("- ")]
        self.assertEqual([line.split(":")[0] for line in lanes],
                         ["- omenative-e2e-rollout-pd", "- omenative-e2e-migration-pd", "- omenative-e2e-reject-eng"])
        self.assertIn("    p2     FAIL              12s  fault=quota:old  engine never reached 3/3 Ready", lines)
        self.assertIn("           log rollout/pd/p2.log", lines)
        self.assertIn("(case logs under 6822-omenative-e2e-rollout-pd/ in the results archive)", lines[1])
        self.assertTrue(lines[-1].startswith("- omenative-e2e-reject-eng: no verdict file (state=failed exit=125)"))
        with_all = cb.format_tally(6822, tally, [], show_all=True)
        self.assertEqual(sum(1 for line in with_all if line.startswith("- ")), 3)


if __name__ == "__main__":
    unittest.main()
