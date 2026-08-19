#!/usr/bin/env python3
import copy
import unittest

import run


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


if __name__ == "__main__":
    unittest.main()
