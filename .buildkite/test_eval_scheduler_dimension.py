"""Tests for the ome-scheduler dimension of the OMENative eval group (run: python3 -m unittest discover -s .buildkite -p 'test_*.py')."""

from __future__ import annotations

import datetime
import importlib
import os
import re
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


# The renders here cover the whole corpus, which only a build carrying the
# override may run; the environment holds it for the module.
_HEAVY = mock.patch.dict(os.environ, {"OME_EVAL_ALLOW_HEAVY": "1"})


def setUpModule():
    _HEAVY.start()


def tearDownModule():
    _HEAVY.stop()


def load(env: dict[str, str]):
    with mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "main", "BUILDKITE_COMMIT": "HEAD", "OME_EVAL_MODE": "full", **env}, clear=False):
        import generate_pipeline as module
        return importlib.reload(module)


# run_omenative_eval_gke.sh and run_kind_tests_gke.sh leave with 125 when a
# job never reached its cases or specs; the infra preset, and only the infra
# preset, reruns that.
INFRA_RETRY_BLOCK = (
    "        retry:\n"
    "          automatic:\n"
    "            - exit_status: 125\n"
    "              limit: 2\n"
    '            - signal_reason: "agent_stop"\n'
    "              limit: 2\n"
    '            - signal_reason: "process_run_error"\n'
    "              limit: 2\n"
    '            - signal_reason: "stack_error"\n'
    "              limit: 2"
)


def job_blocks(yaml_text: str) -> list[str]:
    """One string per emitted job, skip notes included."""
    return yaml_text.split("- label:")[1:]


class EvalSchedulerDimension(unittest.TestCase):
    def test_default_emits_both_schedulers_and_gates_the_twins(self):
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "1"})
        with mock.patch.object(gp, "EVAL_SOFT_FEATURES", []), mock.patch.object(gp, "EVAL_SCHEDULER_SOFT_TOPOLOGIES", []):
            out = gp._gke_eval_group()
        self.assertIn("omenative-e2e-", out)
        self.assertIn("-ome-scheduler", out)
        self.assertIn('OME_EVAL_SCHEDULER: "ome-scheduler"', out)
        self.assertNotIn("soft_fail", out)
        job_keys = [l for l in out.splitlines() if 'key: "omenative-e2e-' in l and "omenative-e2e-kind-gke" not in l]
        default_jobs = [l for l in job_keys if "-ome-scheduler" not in l]
        sched_jobs = [l for l in job_keys if "-ome-scheduler" in l and not l.rstrip('"').endswith("-skipped")]
        # Multi-node twins are re-sharded to their node budget, so the counts
        # differ; every default lane still has at least one twin or a skip note.
        self.assertGreater(len(sched_jobs), 0)
        self.assertGreater(len(default_jobs), 0)

    def test_advisory_switch_softens_the_twins_only(self):
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "1", "GKE_EVAL_SCHEDULER_HARD_FAIL": "0"})
        with mock.patch.object(gp, "EVAL_SOFT_FEATURES", []), mock.patch.object(gp, "EVAL_SCHEDULER_SOFT_TOPOLOGIES", []):
            out = gp._gke_eval_group()
        seen_twin = seen_default = False
        for block in job_blocks(out):
            if "-skipped" in block:
                continue
            if "-ome-scheduler" in block:
                seen_twin = True
                self.assertIn("soft_fail: true", block)
            else:
                seen_default = True
                self.assertNotIn("soft_fail", block)
        self.assertTrue(seen_twin and seen_default)

    def test_kill_switch_keeps_only_the_default_scheduler(self):
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "0"})
        out = gp._gke_eval_group()
        self.assertNotIn("-ome-scheduler", out)
        self.assertNotIn("OME_EVAL_SCHEDULER", out)

    def test_every_eval_job_retries_only_on_infra_failure(self):
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "1"})
        out = gp._gke_eval_group()
        jobs = out.count("run_omenative_eval_gke.sh")
        self.assertGreater(jobs, 0)
        self.assertEqual(out.count(INFRA_RETRY_BLOCK), jobs)
        # The doctrine: a red case is a finding to fix or park, never a retry.
        self.assertNotIn('exit_status: "*"', out)
        self.assertNotIn("exit_status: 1\n", out)
        first = out.index("        retry:")
        self.assertLess(first, out.index("        plugins:", first),
                        "retry must be a step-level key, not nested under plugins")

    def test_eval_image_waits_for_the_scheduler_mirror(self):
        gp = load({})
        self.assertIn('- "mirror-ome-scheduler"', gp._package_kind_eval_image_step())


class KindSchedulerDimension(unittest.TestCase):
    def soft_entry(self, gp, group, **overrides):
        entry = {
            "group": group,
            "owner": "tester",
            "reason": "known gang-scheduler defect",
            "expires": (gp._today() + datetime.timedelta(days=7)).isoformat(),
        }
        entry.update(overrides)
        return entry

    def test_main_emits_gating_scheduler_twins_in_their_own_group(self):
        gp = load({"GKE_KIND_SCHEDULER_ENABLED": "1"})
        with mock.patch.object(gp, "KIND_SCHEDULER_SOFT_GROUPS", []):
            out = gp._gke_kind_scheduler_group()
        self.assertIn('key: "integration-test-kind-scheduler-gke"', out)
        self.assertIn('OME_TEST_SCHEDULER: "ome-scheduler"', out)
        self.assertNotIn("soft_fail", out)
        twins = [l for l in out.splitlines() if 'key: "kind-gke-' in l]
        self.assertEqual(len(twins), len(gp._kind_spec_groups()))
        self.assertTrue(all(l.rstrip('"').endswith("-ome-scheduler") for l in twins), twins[:3])
        self.assertEqual(out.count(INFRA_RETRY_BLOCK), len(twins))

    def test_soft_listed_twins_stay_advisory_until_their_expiry(self):
        gp = load({"GKE_KIND_SCHEDULER_ENABLED": "1"})
        slug = gp.KIND_SPEC_GROUPS[0][0]
        entry = self.soft_entry(gp, slug)
        with mock.patch.object(gp, "KIND_SCHEDULER_SOFT_GROUPS", [entry]):
            out = gp._gke_kind_scheduler_group()
        soft = [b for b in job_blocks(out) if "soft_fail: true" in b]
        self.assertEqual(len(soft), 1)
        self.assertIn(f'key: "kind-gke-{slug}-ome-scheduler"', soft[0])
        self.assertIn(f"advisory until {entry['expires']}", soft[0])

    def test_soft_list_rejects_stale_or_anonymous_entries(self):
        gp = load({"GKE_KIND_SCHEDULER_ENABLED": "1"})
        slug = gp.KIND_SPEC_GROUPS[0][0]
        yesterday = (gp._today() - datetime.timedelta(days=1)).isoformat()
        for entry, message in (
            (self.soft_entry(gp, "no-such-group"), "not a spec group"),
            (self.soft_entry(gp, slug, owner=""), "has no owner"),
            (self.soft_entry(gp, slug, reason=" "), "has no reason"),
            (self.soft_entry(gp, slug, expires="soon"), "needs an expires date"),
            (self.soft_entry(gp, slug, expires=yesterday), "was soft until"),
        ):
            with self.subTest(message=message):
                with mock.patch.object(gp, "KIND_SCHEDULER_SOFT_GROUPS", [entry]):
                    with self.assertRaisesRegex(RuntimeError, message):
                        gp._kind_scheduler_soft_groups()
        twice = [self.soft_entry(gp, slug), self.soft_entry(gp, slug)]
        with mock.patch.object(gp, "KIND_SCHEDULER_SOFT_GROUPS", twice):
            with self.assertRaisesRegex(RuntimeError, "duplicate"):
                gp._kind_scheduler_soft_groups()

    def test_checked_in_soft_list_is_valid(self):
        gp = load({})
        soft = gp._kind_scheduler_soft_groups()
        known = {slug for slug, _, _ in gp.KIND_SPEC_GROUPS}
        self.assertTrue(set(soft) <= known)

    def test_soft_list_is_validated_on_every_branch(self):
        with mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "feature/x", "BUILDKITE_COMMIT": "HEAD", "GKE_KIND_SCHEDULER_ENABLED": "1"}, clear=False):
            import generate_pipeline as module
            pr = importlib.reload(module)
            with mock.patch.object(pr, "KIND_SCHEDULER_SOFT_GROUPS", [self.soft_entry(pr, "no-such-group")]):
                with self.assertRaisesRegex(RuntimeError, "not a spec group"):
                    pr.generate_pipeline()

    def test_advisory_switch_softens_every_twin(self):
        gp = load({"GKE_KIND_SCHEDULER_HARD_FAIL": "0"})
        with mock.patch.object(gp, "KIND_SCHEDULER_SOFT_GROUPS", []):
            out = gp._gke_kind_scheduler_group()
        twins = [l for l in out.splitlines() if 'key: "kind-gke-' in l]
        self.assertEqual(out.count("soft_fail: true"), len(twins))

    def test_twins_are_main_only_and_switchable(self):
        with mock.patch.dict(os.environ, {"BUILDKITE_BRANCH": "feature/x", "BUILDKITE_COMMIT": "HEAD", "GKE_KIND_SCHEDULER_ENABLED": "1"}, clear=False):
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


if __name__ == "__main__":
    unittest.main()


class EvalSchedulerSoftTopologies(unittest.TestCase):
    def soft_entry(self, gp, topology, **overrides):
        entry = {
            "topology": topology,
            "owner": "tester",
            "reason": "known gang-scheduler defect",
            "expires": (gp._today() + datetime.timedelta(days=7)).isoformat(),
        }
        entry.update(overrides)
        return entry

    def test_soft_listed_topology_softens_only_its_scheduler_twins(self):
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "1"})
        topology = gp._eval_case_groups()[0][1]
        entry = self.soft_entry(gp, topology)
        with mock.patch.object(gp, "EVAL_SOFT_FEATURES", []), mock.patch.object(gp, "EVAL_SCHEDULER_SOFT_TOPOLOGIES", [entry]):
            out = gp._gke_eval_group()
        soft = [b for b in job_blocks(out) if "soft_fail: true" in b]
        self.assertGreater(len(soft), 0)
        for block in soft:
            self.assertIn("-ome-scheduler", block)
            self.assertIn(f"/ {topology}", block)
            self.assertIn(f"advisory until {entry['expires']}", block)
        hard = [b for b in job_blocks(out) if "soft_fail" not in b and 'key: "omenative-e2e-' in b]
        self.assertTrue(any("-ome-scheduler" in b for b in hard), "twins of other topologies must still gate")
        self.assertTrue(any("-ome-scheduler" not in b and f"/ {topology}" in b for b in hard), "the default-scheduler shards of the topology must still gate")

    def test_soft_list_rejects_stale_or_anonymous_entries(self):
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "1"})
        topology = gp._eval_case_groups()[0][1]
        yesterday = (gp._today() - datetime.timedelta(days=1)).isoformat()
        for entry, message in (
            (self.soft_entry(gp, "no-such-topology"), "not a corpus topology"),
            (self.soft_entry(gp, topology, owner=""), "has no owner"),
            (self.soft_entry(gp, topology, reason=" "), "has no reason"),
            (self.soft_entry(gp, topology, expires="soon"), "needs an expires date"),
            (self.soft_entry(gp, topology, expires=yesterday), "were soft until"),
        ):
            with self.subTest(message=message):
                with mock.patch.object(gp, "EVAL_SOFT_FEATURES", []), mock.patch.object(gp, "EVAL_SCHEDULER_SOFT_TOPOLOGIES", [entry]):
                    with self.assertRaisesRegex(RuntimeError, message):
                        gp._eval_scheduler_soft_topologies()
        twice = [self.soft_entry(gp, topology), self.soft_entry(gp, topology)]
        with mock.patch.object(gp, "EVAL_SOFT_FEATURES", []), mock.patch.object(gp, "EVAL_SCHEDULER_SOFT_TOPOLOGIES", twice):
            with self.assertRaisesRegex(RuntimeError, "duplicate"):
                gp._eval_scheduler_soft_topologies()

    def test_checked_in_soft_list_is_valid(self):
        gp = load({})
        soft = gp._eval_scheduler_soft_topologies()
        known = {topology for _, topology, _ in gp._eval_case_groups()}
        self.assertTrue(set(soft) <= known)

    def test_empty_soft_list_keeps_every_twin_gating(self):
        gp = load({"GKE_EVAL_SCHEDULER_ENABLED": "1"})
        with mock.patch.object(gp, "EVAL_SOFT_FEATURES", []), mock.patch.object(gp, "EVAL_SCHEDULER_SOFT_TOPOLOGIES", []):
            out = gp._gke_eval_group()
        self.assertNotIn("soft_fail", out)
