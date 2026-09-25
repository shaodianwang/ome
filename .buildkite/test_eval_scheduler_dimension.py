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
