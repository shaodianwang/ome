#!/usr/bin/env python3
import copy
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

import generate_cases
import run

HERE = Path(__file__).parent.resolve()


def pod(component, index, runner="default", ready=True, serving=True):
    return {
        "comp": component,
        "inst": str(index),
        "runner": runner,
        "alive": True,
        "ready": ready,
        "serving": serving,
    }


def inference_replica(component, replicas, *, phase="Ready", operation=None,
                      current="revision-a", update="revision-a", serving=None,
                      ready=None, generation=1, observed_generation=1):
    if serving is None:
        serving = replicas
    if ready is None:
        ready = replicas
    return {
        "metadata": {"generation": generation},
        "spec": {"component": component},
        "status": {
            "observedGeneration": observed_generation,
            "replicas": replicas,
            "readyReplicas": ready,
            "servingReplicas": serving,
            "updatedReplicas": replicas,
            "updatedReadyReplicas": ready,
            "currentRevision": current,
            "updateRevision": update,
            "instanceStatuses": [
                {"index": index, "phase": phase, "operation": operation}
                for index in range(replicas)
            ],
        },
    }


class InitialRolloutSettlementTest(unittest.TestCase):
    def assert_not_settled(self, pod_state, pods_per_inst, expected, items, message):
        settled, detail = run._initial_rollout_settled(
            pod_state, pods_per_inst, expected, set(expected), items)
        self.assertFalse(settled, detail)
        self.assertIn(message, detail)

    def test_container_ready_but_not_serving_is_not_settled(self):
        pods = {
            "leader": pod("engine", 0, "leader", serving=False),
            "worker": pod("engine", 0, "worker", serving=False),
        }
        item = inference_replica("engine", 1, phase="Creating", serving=0)
        self.assert_not_settled(pods, {"engine": 2}, {"engine": 1}, [item], "serving 0/1")

    def test_serving_pods_do_not_hide_creating_instance(self):
        pods = {
            "leader": pod("engine", 0, "leader"),
            "worker": pod("engine", 0, "worker"),
        }
        item = inference_replica("engine", 1, phase="Creating")
        self.assert_not_settled(pods, {"engine": 2}, {"engine": 1}, [item], "not settled")

    def test_fully_settled_multi_component_fleet_passes(self):
        pods = {
            "engine-leader": pod("engine", 0, "leader"),
            "engine-worker": pod("engine", 0, "worker"),
            "decoder": pod("decoder", 0),
        }
        items = [inference_replica("engine", 1), inference_replica("decoder", 1)]
        settled, detail = run._initial_rollout_settled(
            pods, {"engine": 2, "decoder": 1}, {"engine": 1, "decoder": 1},
            {"engine", "decoder"}, items)
        self.assertTrue(settled, detail)

    def test_each_authoritative_status_guard_blocks_settlement(self):
        pods = {"engine": pod("engine", 0)}
        baseline = inference_replica("engine", 1)
        mutations = {
            "stale generation": lambda item: item["status"].update(observedGeneration=1),
            "replica deficit": lambda item: item["status"].update(replicas=0),
            "ready deficit": lambda item: item["status"].update(readyReplicas=0),
            "serving deficit": lambda item: item["status"].update(servingReplicas=0),
            "updated deficit": lambda item: item["status"].update(updatedReplicas=0),
            "updated-ready deficit": lambda item: item["status"].update(updatedReadyReplicas=0),
            "revision mismatch": lambda item: item["status"].update(currentRevision="revision-old"),
            "missing instance": lambda item: item["status"].update(instanceStatuses=[]),
            "active operation": lambda item: item["status"]["instanceStatuses"][0].update(
                operation={"type": "Create"}),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                item = copy.deepcopy(baseline)
                if name == "stale generation":
                    item["metadata"]["generation"] = 2
                mutate(item)
                settled, detail = run._initial_rollout_settled(
                    pods, {"engine": 1}, {"engine": 1}, {"engine"}, [item])
                self.assertFalse(settled, detail)

    def test_missing_or_duplicate_component_ir_is_not_settled(self):
        pods = {"engine": pod("engine", 0)}
        item = inference_replica("engine", 1)
        for items in ([], [item, copy.deepcopy(item)]):
            with self.subTest(count=len(items)):
                settled, detail = run._initial_rollout_settled(
                    pods, {"engine": 1}, {"engine": 1}, {"engine"}, items)
                self.assertFalse(settled, detail)


class CanaryCommandStateTest(unittest.TestCase):
    def test_initial_stable_revision_is_not_commandable(self):
        got = run._canary_command_state(
            {"canaryRevisionHash": "stable"}, None, {"stable"}, 4)
        self.assertEqual(("stable", -1, False, False, True), got)

    def test_new_live_target_rearms_manual_promotion(self):
        got = run._canary_command_state(
            {"canaryRevisionHash": "new"}, "stable", {"stable"}, 0)
        self.assertEqual(("new", -1, True, True, True), got)

    def test_next_step_waits_for_durable_promote_cleanup(self):
        got = run._canary_command_state(
            {"canaryRevisionHash": "new", "promotedThrough": "new"},
            "new", {"stable"}, 0)
        self.assertEqual(("new", 0, True, False, False), got)

    def test_same_target_is_reusable_after_promote_cleanup(self):
        got = run._canary_command_state(
            {"canaryRevisionHash": "new"}, "new", {"stable"}, 0)
        self.assertEqual(("new", 0, True, True, False), got)


class GeneratedCorpusTest(unittest.TestCase):
    def test_regeneration_is_byte_stable(self):
        """The gen-* corpus is a pure function of generate_cases.py: a hand
        edit to a generated file (or a generator edit without a regen) must
        fail here, not surface as review noise."""
        r = subprocess.run([sys.executable, str(HERE / "generate_cases.py"), "--check"],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, msg=(r.stdout + r.stderr))

    def test_corpus_stays_under_lane_cap(self):
        # 3000 committed case files is the agreed ceiling for running the
        # whole corpus in one eval lane; growth past it needs a lane split
        # decision, not a bigger PR.
        n = sum(1 for _ in (HERE / "isvcs").glob("**/*.yaml"))
        self.assertLessEqual(n, 3000)

    def test_parked_cell_emits_xfail_header_and_reason(self):
        """A PARKED entry must turn its cell into notimpl=1 issue=N with the
        parked reason as the line-2 rationale — the only quiet path for a
        generated case (build_outputs is pure; nothing is written here).
        The fixture cell is one that is NOT parked for real, so the test
        stays independent of the live parking table."""
        cell = ("kill", "engine", 0, "RecreatePod", "None", "", None)
        self.assertNotIn(cell, generate_cases.PARKED)
        with mock.patch.dict(generate_cases.PARKED,
                             {cell: (999999, "parked on a tracked bug (fixture)")}):
            outputs, rows = generate_cases.build_outputs()
        parked = [(f, n) for f, n, _kv, m in rows if m.get("parked") == 999999]
        self.assertEqual(len(parked), 1, "exactly the parked cell is marked")
        folder, name = parked[0]
        text = outputs[f"{folder}/{name}.yaml"]
        header = text.splitlines()[0]
        self.assertIn("notimpl=1", header)
        self.assertIn("issue=999999", header)
        self.assertEqual(text.splitlines()[1],
                         "# reason: parked on a tracked bug (fixture)")

    def test_stale_parked_key_fails_the_regen(self):
        cell = ("kill", "no-such-topo", 0, "SurgeThenDrain", "None", "", None)
        with mock.patch.dict(generate_cases.PARKED, {cell: (999999, "stale")}):
            with self.assertRaises(SystemExit):
                generate_cases.build_outputs()


if __name__ == "__main__":
    unittest.main()
