"""Tests for the ome-scheduler dimension of the OMENative eval group (run: python3 -m unittest discover -s .buildkite -p 'test_*.py')."""

from __future__ import annotations

import importlib
import os
import re
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


def load(env: dict[str, str]):
    with mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "main", "BUILDKITE_COMMIT": "0123456789abcdef", **env}, clear=False):
        import generate_pipeline as module
        return importlib.reload(module)


class EvalSchedulerDimension(unittest.TestCase):
    def test_default_emits_both_schedulers_with_advisory_gang_jobs(self):
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "1", "GKE_EVAL_SCHEDULER_HARD_FAIL": "0"})
        out = gp._gke_eval_group()
        self.assertIn("omenative-e2e-", out)
        self.assertIn("-ome-scheduler", out)
        self.assertIn('OME_EVAL_SCHEDULER: "ome-scheduler"', out)
        self.assertIn("soft_fail: true", out)
        job_keys = [l for l in out.splitlines() if 'key: "omenative-e2e-' in l and "omenative-e2e-kind-gke" not in l]
        default_jobs = [l for l in job_keys if "-ome-scheduler" not in l]
        sched_jobs = [l for l in job_keys if "-ome-scheduler" in l and not l.rstrip('"').endswith("-skipped")]
        # Multi-node twins are re-sharded to their node budget, so the counts
        # differ; every default lane still has at least one twin or a skip note.
        self.assertGreater(len(sched_jobs), 0)
        self.assertGreater(len(default_jobs), 0)

    def test_hard_fail_switch_removes_soft_fail(self):
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "1", "GKE_EVAL_SCHEDULER_HARD_FAIL": "1"})
        out = gp._gke_eval_group()
        self.assertIn("-ome-scheduler", out)
        self.assertNotIn("soft_fail: true", out)

    def test_kill_switch_keeps_only_the_default_scheduler(self):
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "0"})
        out = gp._gke_eval_group()
        self.assertNotIn("-ome-scheduler", out)
        self.assertNotIn("OME_EVAL_SCHEDULER", out)

    def test_eval_image_waits_for_the_scheduler_mirror(self):
        gp = load({})
        self.assertIn('- "mirror-ome-scheduler"', gp._package_kind_eval_image_step())


if __name__ == "__main__":
    unittest.main()


class KindSchedulerDimension(unittest.TestCase):
    def test_main_emits_scheduler_twins_in_their_own_group(self):
        gp = load({"GKE_KIND_SCHEDULER_ENABLED": "1", "GKE_KIND_SCHEDULER_HARD_FAIL": "0"})
        out = gp._gke_kind_scheduler_group()
        self.assertIn('key: "integration-test-kind-scheduler-gke"', out)
        self.assertIn('OME_TEST_SCHEDULER: "ome-scheduler"', out)
        self.assertIn("soft_fail: true", out)
        twins = [l for l in out.splitlines() if 'key: "kind-gke-' in l]
        self.assertEqual(len(twins), len(gp._kind_spec_groups()))
        self.assertTrue(all(l.rstrip('"').endswith("-ome-scheduler") for l in twins), twins[:3])

    def test_hard_fail_switch_removes_soft_fail_on_kind_twins(self):
        gp = load({"GKE_KIND_SCHEDULER_HARD_FAIL": "1"})
        self.assertNotIn("soft_fail", gp._gke_kind_scheduler_group())

    def test_twins_are_main_only_and_switchable(self):
        with mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "feature/x", "BUILDKITE_COMMIT": "0123456789abcdef", "GKE_KIND_SCHEDULER_ENABLED": "1"}, clear=False):
            import generate_pipeline as module
            pr = importlib.reload(module).generate_pipeline()
        self.assertNotIn("integration-test-kind-scheduler-gke", pr)
        main_off = load({"GKE_KIND_SCHEDULER_ENABLED": "0"}).generate_pipeline()
        self.assertNotIn("integration-test-kind-scheduler-gke", main_off)
        main_on = load({"GKE_KIND_SCHEDULER_ENABLED": "1"}).generate_pipeline()
        self.assertIn("integration-test-kind-scheduler-gke", main_on)
        self.assertIn('- "mirror-ome-scheduler"', main_on)


class SchedulerTwinNodeSizing(unittest.TestCase):
    def test_multi_node_twins_carry_a_node_count_equal_to_their_budget(self):
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "1", "GKE_EVAL_SCHEDULER_MAX_WORKERS": "8"})
        out = gp._gke_eval_group()
        blocks = out.split("- label:")
        mn_twins = [b for b in blocks if "-mn-ome-scheduler" in b and "-skipped" not in b]
        self.assertTrue(mn_twins)
        for b in mn_twins:
            m = re.search(r'OME_EVAL_KIND_WORKERS: "(\d+)"', b)
            self.assertIsNotNone(m, b[:200])
            self.assertLessEqual(int(m.group(1)), 8)
            self.assertIn(f'OME_EVAL_POD_BUDGET: "{m.group(1)}"', b)
        for b in blocks:
            if "omenative-e2e-" in b and "-mn-ome-scheduler" not in b:
                self.assertNotIn("OME_EVAL_KIND_WORKERS", b)

    def test_cases_above_the_cap_are_left_out_and_named(self):
        def skipped(cap):
            gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "1", "GKE_EVAL_SCHEDULER_MAX_WORKERS": str(cap)})
            out = gp._gke_eval_group()
            named = set()
            for m in re.finditer(r'OME_EVAL_SCHEDULER_SKIPPED: "([^"]*)"', out):
                named.update(m.group(1).split())
            for m in re.finditer(r"not run in this twin: ([^']*)'", out):
                named.update(m.group(1).split())
            return named, out
        small, out_small = skipped(3)
        large, out_large = skipped(64)
        self.assertTrue(small)
        self.assertLess(len(large), len(small))
        # a case is either run by the twin or named as skipped, never dropped silently
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "1", "GKE_EVAL_SCHEDULER_MAX_WORKERS": "3"})
        committed = {c for f, t, cases in gp._eval_case_groups() if t.endswith("-mn") for c in cases}
        run_in_twins = set()
        for block in out_small.split("- label:"):
            if "-mn-ome-scheduler" in block and "-skipped" not in block:
                for cmd in re.findall(r"run_omenative_eval_gke\.sh \S+ \S+ ([^\"]+)", block):
                    run_in_twins.update(cmd.split())
        self.assertEqual(committed, run_in_twins | small)

