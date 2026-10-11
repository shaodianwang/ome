#!/usr/bin/env python3

import importlib.util
import json
import re
import tempfile
import threading
import time
import unittest
from pathlib import Path

import batch
from run import TSV_HEADER

HERE = Path(__file__).resolve().parent


def load_eval_verdicts():
    """The round record's format module from the pipeline scripts, so the
    file batch.py writes is checked against the reader of it."""
    path = HERE.parents[2] / ".buildkite" / "eval_verdicts.py"
    spec = importlib.util.spec_from_file_location("eval_verdicts_for_batch", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ConcurrentCaseRunnerTest(unittest.TestCase):
    def test_limits_sum_of_active_case_peaks(self):
        peaks = {"a": 21, "b": 21, "c": 21, "d": 29, "e": 8}
        lock = threading.Lock()
        active_pods = 0
        max_active_pods = 0
        recorded = []

        def runner(case_id):
            nonlocal active_pods, max_active_pods
            with lock:
                active_pods += peaks[case_id]
                max_active_pods = max(max_active_pods, active_pods)
            time.sleep(0.02)
            with lock:
                active_pods -= peaks[case_id]
            return case_id, 0, 0.02

        batch.run_concurrent_cases(
            list(peaks), 4, 64, peaks,
            lambda case_id, _rc, _secs: recorded.append(case_id),
            runner=runner,
        )

        self.assertCountEqual(recorded, peaks)
        self.assertLessEqual(max_active_pods, 64)
        self.assertGreater(max_active_pods, max(peaks.values()))

    def test_canary_pd_peaks_are_budgeted_from_headers(self):
        self.assertEqual(batch.case_peak_pods("p309"), 21)
        self.assertEqual(batch.case_peak_pods("p354"), 29)


class CaseOutcomeTest(unittest.TestCase):
    """case_outcome reads a finished case the way the summary and the JUnit
    do, and adds the one-line reason, the exit and the result file."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.result = None
        original = batch.latest_tsv
        batch.latest_tsv = lambda case_id: self.result
        self.addCleanup(setattr, batch, "latest_tsv", original)

    def write_result(self, verdict, note, text=None):
        columns = TSV_HEADER.rstrip("\n").split("\t")
        row = {name: "0" for name in columns}
        row.update(id="p01", desc="fixture", policy="none", verdict=verdict, note=note)
        path = self.dir / "p01-100.tsv"
        path.write_text(text if text is not None else TSV_HEADER + "\t".join(row[name] for name in columns) + "\n")
        self.result = str(path)
        return str(path)

    def test_a_green_exit_keeps_the_verdict_and_the_note(self):
        path = self.write_result("PASS", "settled  in   61s")
        self.assertEqual(batch.case_outcome("p01", 0, 61.26), {
            "verdict": "PASS", "reason": "settled in 61s", "exit": 0, "seconds": 61.3, "result": path,
        })

    def test_a_red_verdict_keeps_its_reason(self):
        self.write_result("FAIL", "engine never reached 3/3 Ready")
        outcome = batch.case_outcome("p01", 1, 600)
        self.assertEqual((outcome["verdict"], outcome["reason"], outcome["exit"]),
                         ("FAIL", "engine never reached 3/3 Ready", 1))

    def test_a_nonzero_exit_after_a_green_verdict_is_a_process_exit(self):
        self.write_result("PASS", "")
        outcome = batch.case_outcome("p01", 2, 5)
        self.assertEqual(outcome["verdict"], "PROCESS_EXIT_2")
        self.assertEqual(outcome["reason"], "run.py exited 2 after recording PASS")
        self.write_result("XFAIL", "parked")
        outcome = batch.case_outcome("p01", 1, 5)
        self.assertEqual(outcome["verdict"], "PROCESS_EXIT_1")
        self.assertEqual(outcome["reason"], "run.py exited 1 after recording XFAIL: parked")

    def test_a_case_without_a_result_file_or_a_verdict_is_unfinished(self):
        self.assertEqual(batch.case_outcome("p01", 1, 2), {
            "verdict": "?", "reason": "no result file", "exit": 1, "seconds": 2, "result": None,
        })
        path = self.write_result("", "", text=TSV_HEADER)
        outcome = batch.case_outcome("p01", 0, 2)
        self.assertEqual((outcome["verdict"], outcome["reason"], outcome["result"]),
                         ("?", "result file without a verdict", path))


class VerdictFileTest(unittest.TestCase):
    """batch.py writes the run's verdicts as one JSON document beside
    summary.txt: one object per case and the lane the run was, in the format
    the round record reads."""

    CI_ENV = {
        "BUILDKITE_BUILD_NUMBER": "6822",
        "BUILDKITE_JOB_ID": "0199d1b2-aaaa-bbbb-cccc-0123456789ab",
        "BUILDKITE_STEP_KEY": "omenative-e2e-rollout-pd-2-of-3",
        "BUILDKITE_COMMIT": "0123456789abcdef0123456789abcdef01234567",
        "OME_EVAL_LANE": "rollout-pd-ome-scheduler",
        "OME_EVAL_SCHEDULER": "ome-scheduler",
    }

    @classmethod
    def setUpClass(cls):
        # One corpus case without a fault and one with, read from the headers
        # directly so the lookup stays cheap.
        files = sorted((batch.HERE / "isvcs").glob("**/*.yaml"))
        headers = {path.stem: path.read_text().splitlines()[0] for path in files}
        cls.plain = next(case for case, header in headers.items() if "fault=" not in header)
        cls.faulty = next(case for case, header in headers.items() if " fault=" in header)
        cls.run_dir = batch.HERE / "results" / batch.RUN_TS

    def outcomes(self):
        def finished(case, verdict, reason, rc, secs):
            feature, topo = batch.case_category(case)
            return {"verdict": verdict, "reason": reason, "exit": rc, "seconds": secs,
                    "result": str(self.run_dir / feature / topo / f"{case}-100.tsv")}
        return {
            self.plain: finished(self.plain, "PASS", "", 0, 61.3),
            self.faulty: finished(self.faulty, "XFAIL (known bug: #1)", "parked", 0, 5.0),
            "p99999": {"verdict": "?", "reason": "no result file", "exit": 1, "seconds": 2.0, "result": None},
        }

    def test_one_object_per_case_in_the_run_order_and_the_lane(self):
        cases = [self.plain, self.faulty, "p99999"]
        document = batch.job_verdicts(cases, self.outcomes(), 1234.56, env=self.CI_ENV)
        self.assertEqual(document["format"], batch.VERDICTS_FORMAT)
        self.assertEqual(
            {key: document[key] for key in ("build", "job", "step", "lane", "scheduler", "commit", "run", "seconds")},
            {"build": "6822", "job": self.CI_ENV["BUILDKITE_JOB_ID"], "step": "omenative-e2e-rollout-pd-2-of-3",
             "lane": "rollout-pd-ome-scheduler", "scheduler": "ome-scheduler",
             "commit": self.CI_ENV["BUILDKITE_COMMIT"], "run": batch.RUN_TS, "seconds": 1234.6},
        )
        self.assertEqual(document["counts"], {"?": 1, "PASS": 1, "XFAIL": 1})
        self.assertEqual([row["case"] for row in document["cases"]], cases)
        plain, faulty, unknown = document["cases"]
        feature, topo = batch.case_category(self.plain)
        self.assertEqual(plain, {
            "case": self.plain, "feature": feature, "topology": topo, "scheduler": "ome-scheduler",
            "verdict": "PASS", "reason": "", "exit": 0, "seconds": 61.3, "fault": None,
            "log": f"{feature}/{topo}/{self.plain}.log", "result": f"{feature}/{topo}/{self.plain}-100.tsv",
        })
        self.assertEqual(faulty["fault"], batch.case_header_fields(self.faulty)["fault"])
        self.assertTrue(faulty["fault"])
        self.assertEqual(faulty["verdict"], "XFAIL (known bug: #1)")
        self.assertEqual(
            {key: unknown[key] for key in ("feature", "topology", "verdict", "reason", "fault", "result")},
            {"feature": "uncategorized", "topology": "unknown", "verdict": "?", "reason": "no result file",
             "fault": None, "result": None},
        )

    def test_outside_ci_the_lane_is_null_and_a_case_without_an_outcome_is_unfinished(self):
        document = batch.job_verdicts([self.plain], {}, 0.0, env={})
        self.assertEqual(
            {key: document[key] for key in ("build", "job", "step", "lane", "scheduler")},
            {"build": None, "job": None, "step": None, "lane": None, "scheduler": ""},
        )
        self.assertTrue(document["commit"] is None or re.fullmatch(r"[0-9a-f]{40}", document["commit"]))
        (row,) = document["cases"]
        self.assertEqual((row["verdict"], row["reason"], row["result"], row["exit"]), ("?", "no result file", None, None))
        self.assertEqual(document["counts"], {"?": 1})

    def test_the_file_round_trips_through_the_round_record_reader(self):
        ev = load_eval_verdicts()
        self.assertEqual(ev.JOB_VERDICTS_FILE, batch.VERDICTS_FILE)
        self.assertEqual(ev.JOB_VERDICTS_FORMAT, batch.VERDICTS_FORMAT)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run" / batch.VERDICTS_FILE
            batch.write_verdicts(path, batch.job_verdicts([self.plain, self.faulty, "p99999"], self.outcomes(), 9.0, env=self.CI_ENV))
            text = path.read_text()
        self.assertTrue(text.endswith("}\n"))
        json.loads(text)
        warnings = []
        rows = ev.read_job_verdicts(text, warnings.append)
        self.assertEqual(warnings, [])
        # The unfinished case yields no row; the others carry their own lane.
        self.assertEqual(rows, [
            batch.case_category(self.plain) + ("ome-scheduler", self.plain, "PASS"),
            batch.case_category(self.faulty) + ("ome-scheduler", self.faulty, "XFAIL (known bug: #1)"),
        ])

    def test_verdict_word_is_the_first_word(self):
        self.assertEqual(batch.verdict_word("XFAIL (known bug: #1)"), "XFAIL")
        self.assertEqual(batch.verdict_word("PASS"), "PASS")
        self.assertEqual(batch.verdict_word("PROCESS_EXIT_1"), "PROCESS_EXIT_1")
        self.assertEqual(batch.verdict_word(""), "?")
        self.assertEqual(batch.verdict_word(None), "?")


if __name__ == "__main__":
    unittest.main()
