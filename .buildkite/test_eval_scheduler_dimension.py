"""Tests for the ome-scheduler dimension of the OMENative eval group (run: python3 -m unittest discover -s .buildkite -p 'test_*.py')."""

from __future__ import annotations

import importlib
import os
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
        sched_jobs = [l for l in job_keys if "-ome-scheduler" in l]
        self.assertEqual(len(default_jobs), len(sched_jobs))
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
