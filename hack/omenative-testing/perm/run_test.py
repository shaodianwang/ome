#!/usr/bin/env python3
import copy
import json
import subprocess
import sys
import time
import unittest
from pathlib import Path
from unittest import mock

import batch
import generate_cases
import irstatus
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


def _normalized(rows):
    """Rows as the Go codec serializes them: default-valued fields absent."""
    def strip(value):
        if isinstance(value, dict):
            return {k: strip(v) for k, v in value.items() if v not in (None, 0, "", False, [], {})}
        if isinstance(value, list):
            return [strip(v) for v in value]
        return value
    return strip(rows)


class ColumnarStatusDecodingTest(unittest.TestCase):
    """The harness decodes ColumnarV2 exactly as the Go codec does; the golden
    file is written by goldengen from that codec."""

    def test_golden_rows_match_the_go_codec(self):
        goldens = json.loads((HERE / "testdata" / "columnar_golden.json").read_text())
        self.assertTrue(goldens)
        for golden in goldens:
            with self.subTest(golden["name"]):
                status = {"instanceStatusEncoding": "ColumnarV2",
                          "instanceStatusColumns": golden["columns"]}
                self.assertEqual(_normalized(golden["rows"]),
                                 _normalized(irstatus.instance_statuses(status)))

    def test_dense_rows_pass_through_unchanged(self):
        rows = [{"index": 0, "phase": "Ready"}]
        self.assertIs(rows, irstatus.instance_statuses({"instanceStatuses": rows}))

    def test_status_without_rows_is_none(self):
        self.assertIsNone(irstatus.instance_statuses({}))
        self.assertIsNone(irstatus.instance_statuses(None))

    def test_union_violations_are_rejected(self):
        columns = {"members": "0", "phases": [{"value": "Ready", "indexes": "0"}]}
        statuses = {
            "columns without marker": {"instanceStatusColumns": columns},
            "marker without columns": {"instanceStatusEncoding": "ColumnarV2"},
            "both representations": {"instanceStatusEncoding": "ColumnarV2",
                                     "instanceStatusColumns": columns,
                                     "instanceStatuses": [{"index": 0}]},
            "unknown marker": {"instanceStatusEncoding": "DenseV9", "instanceStatuses": []},
        }
        for name, status in statuses.items():
            with self.subTest(name):
                with self.assertRaises(irstatus.DecodeError):
                    irstatus.instance_statuses(status)

    def test_index_set_grammar(self):
        self.assertEqual([0, 1, 2, 5, 7, 8], irstatus.parse_index_set("0-2,5,7-8"))
        for raw in ("", "01", "1-1", "2-1", "0,", "3,2", "0-2,3", "0,1", "a", "0--1", None):
            with self.subTest(raw=raw):
                with self.assertRaises(irstatus.DecodeError):
                    irstatus.parse_index_set(raw)

    def test_structural_violations_are_rejected(self):
        base = {"members": "0-1", "phases": [{"value": "Ready", "indexes": "0-1"}]}
        cases = {
            "phase coverage gap": {**base, "phases": [{"value": "Ready", "indexes": "0"}]},
            "overlapping groups": {**base, "phases": [{"value": "Failed", "indexes": "0-1"},
                                                      {"value": "Ready", "indexes": "1"}]},
            "groups out of order": {**base, "phases": [{"value": "Ready", "indexes": "0"},
                                                       {"value": "Failed", "indexes": "1"}]},
            "empty optional column": {**base, "runningRevisions": []},
            "non-member index": {**base, "admitted": "2"},
            "zero incarnation": {**base, "incarnations": [{"value": 0, "indexes": "0"}]},
            "zero count": {**base, "podCounts": [{"value": 0, "indexes": "0"}]},
            "ascending rowOrder spelled out": {**base, "rowOrder": [0, 1]},
            "rowOrder not a permutation": {**base, "rowOrder": [1, 1]},
            "entry without record": {**base, "entries": [{"index": 0}]},
            "entries out of order": {**base, "entries": [{"index": 1, "operation": {"type": "Create"}},
                                                         {"index": 0, "operation": {"type": "Create"}}]},
        }
        for name, columns in cases.items():
            with self.subTest(name):
                with self.assertRaises(irstatus.DecodeError):
                    irstatus.decode_columns(columns)

    def test_row_order_and_defaults_expand(self):
        columns = {
            "members": "0-2",
            "rowOrder": [2, 0, 1],
            "phases": [{"value": "Ready", "indexes": "0,2"}, {"value": "Updating", "indexes": "1"}],
            "runningRevisions": [{"value": "rev-a", "indexes": "0-2"}],
            "incarnations": [{"value": 3, "indexes": "1"}],
            "admitted": "0-1",
            "activeOrdinalOne": "2",
            "entries": [{"index": 1, "operation": {"type": "Update"}}],
        }
        rows = irstatus.decode_columns(columns)
        self.assertEqual([2, 0, 1], [row["index"] for row in rows])
        by_index = {row["index"]: row for row in rows}
        self.assertEqual({"index": 0, "phase": "Ready", "runningRevision": "rev-a", "admitted": True},
                         by_index[0])
        self.assertEqual({"index": 1, "phase": "Updating", "runningRevision": "rev-a",
                          "incarnation": 3, "admitted": True, "operation": {"type": "Update"}},
                         by_index[1])
        self.assertEqual({"index": 2, "phase": "Ready", "runningRevision": "rev-a", "activeOrdinal": 1},
                         by_index[2])


class ColumnarSettlementTest(unittest.TestCase):
    """The settle check reads rows through the decoder, so an object stored as
    ColumnarV2 settles exactly like a dense one."""

    @staticmethod
    def columnar(item):
        rows = item["status"].pop("instanceStatuses")
        indexes = "0" if len(rows) == 1 else f"0-{len(rows) - 1}"
        item["status"]["instanceStatusEncoding"] = "ColumnarV2"
        item["status"]["instanceStatusColumns"] = {
            "members": indexes,
            "phases": [{"value": rows[0]["phase"], "indexes": indexes}],
        }
        return item

    def test_columnar_object_settles(self):
        pods = {"engine-0": pod("engine", 0), "engine-1": pod("engine", 1)}
        item = self.columnar(inference_replica("engine", 2))
        settled, detail = run._initial_rollout_settled(
            pods, {"engine": 1}, {"engine": 2}, {"engine"}, [item])
        self.assertTrue(settled, detail)

    def test_undecodable_object_names_the_reason(self):
        pods = {"engine": pod("engine", 0)}
        item = inference_replica("engine", 1)
        item["status"]["instanceStatusColumns"] = {"members": "0"}
        settled, detail = run._initial_rollout_settled(
            pods, {"engine": 1}, {"engine": 1}, {"engine"}, [item])
        self.assertFalse(settled)
        self.assertIn("undecodable", detail)


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


class CanaryUnitTest(unittest.TestCase):
    """The unit is the granularity a canary run owns: the router alone, or
    engine+decoder. The harness has to resolve the same unit the product does,
    or it observes a Component that carries no run."""

    @staticmethod
    def _ctx(groups, configured=("engine", "decoder", "router"), hdr=None):
        return {
            "configured": set(configured),
            "hdr": hdr or {},
            "info": {"canary_groups": [
                {"unit": run.canary_unit_entrypoint(m), "members": list(m),
                 "order": [], "policy": "auto", "steps": []}
                for m in groups]},
        }

    def test_decoder_belongs_to_the_engine_unit(self):
        self.assertEqual("engine", run.canary_unit("decoder"))
        self.assertEqual("engine", run.canary_unit("engine"))
        self.assertEqual("router", run.canary_unit("router"))

    def test_entrypoint_is_the_group_not_the_isvc(self):
        """An engine unit behind a router drives its own ladder. Reading the
        router there would watch a Component with no canary run at all."""
        ctx = self._ctx([("engine", "decoder")])
        self.assertEqual("engine", run._canary_primary(ctx))

    def test_router_group_keeps_the_router_as_primary(self):
        ctx = self._ctx([("router",)])
        self.assertEqual("router", run._canary_primary(ctx))

    def test_unit_header_selects_among_per_unit_groups(self):
        groups = [("router",), ("engine", "decoder")]
        self.assertEqual("engine", run._canary_primary(
            self._ctx(groups, hdr={"unit": "engine"})))
        self.assertEqual("engine", run._canary_primary(
            self._ctx(groups, hdr={"unit": "decoder"})))
        self.assertEqual("router", run._canary_primary(
            self._ctx(groups, hdr={"unit": "router"})))

    def test_unit_header_naming_an_undeclared_unit_resolves_to_no_group(self):
        ctx = self._ctx([("engine", "decoder")], hdr={"unit": "router"})
        self.assertIsNone(run._canary_group(ctx))

    def test_run_state_is_read_off_the_unit_entrypoint(self):
        """Two units rolling at once: the top-level alias follows the ISVC
        entrypoint, so reading it for the engine would report the router's
        step counter and revision pair."""
        st = {"canary": {"canaryRevisionHash": "router-new", "currentStep": 0},
              "components": {
                  "router": {"canary": {"canaryRevisionHash": "router-new", "currentStep": 0}},
                  "engine": {"canary": {"canaryRevisionHash": "engine-new", "currentStep": 1}}}}
        self.assertEqual("engine-new", run._canary_run(st, "engine")["canaryRevisionHash"])
        self.assertEqual("engine-new", run._canary_run(st, "decoder")["canaryRevisionHash"])
        self.assertEqual("router-new", run._canary_run(st, "router")["canaryRevisionHash"])

    def test_run_state_falls_back_to_the_legacy_alias(self):
        """A run persisted before per-unit state exists only on the alias."""
        st = {"canary": {"canaryRevisionHash": "legacy"}, "components": {"engine": {}}}
        self.assertEqual("legacy", run._canary_run(st, "engine")["canaryRevisionHash"])
        self.assertEqual({}, run._canary_run({"components": {}}, "engine"))


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
                             {cell: (999999, "parked on a tracked bug (fixture)", "A")}):
            outputs, rows = generate_cases.build_outputs()
        parked = [(f, n) for f, n, _kv, m in rows if m.get("parked") == 999999]
        self.assertEqual(len(parked), 1, "exactly the parked cell is marked")
        folder, name = parked[0]
        text = outputs[f"{folder}/{name}.yaml"]
        header = text.splitlines()[0]
        self.assertIn("notimpl=1", header)
        self.assertIn("issue=999999", header)
        self.assertIn("inv_xfail=A", header)
        self.assertEqual(text.splitlines()[1],
                         "# reason: parked on a tracked bug (fixture)")

    def test_stale_parked_key_fails_the_regen(self):
        cell = ("kill", "no-such-topo", 0, "SurgeThenDrain", "None", "", None)
        with mock.patch.dict(generate_cases.PARKED, {cell: (999999, "stale", "")}):
            with self.assertRaises(SystemExit):
                generate_cases.build_outputs()


class ParkedInvariantVerdictTest(unittest.TestCase):
    """The job-exit path for a case parked on an invariant-class bug, run
    through the REAL pipeline pieces in _finish's order: parse the committed
    header, soften, then gate on the invariants, then batch's green check."""

    def _violation(self, invariant, kind, subject, detail):
        now = time.time()
        return {(invariant, kind, subject): {"first": now - 47, "last": now,
                                             "n": 10, "detail": detail}}

    def _verdict(self, hdr, viol):
        inv = run._inv_new("p1632", hdr)
        inv["ir_seen"] = True
        inv["viol"].update(viol)
        checks, notes = [], []
        verdict = run.soften_expected_fail(hdr, "PASS", checks)
        return run.invariant_checks(inv, verdict, notes, checks), checks

    def test_in_scope_invariant_violation_records_as_known_bug_xfail(self):
        _first, hdr = run.parse_header(run.find_isvc("p1632"))
        self.assertEqual(hdr.get("notimpl"), 1)
        self.assertEqual(hdr.get("inv_xfail"), "A")
        verdict, checks = self._verdict(hdr, self._violation(
            "A", "ready-without-pods", "engine/0",
            "engine/0 Phase=Ready while the IR's own podCount is 0"))
        self.assertEqual(verdict, f"XFAIL (known bug: #{hdr['issue']})")
        self.assertTrue(batch.is_green(verdict))
        self.assertTrue(any("tracked bug this case is parked on" in c for c in checks))

    def test_out_of_scope_invariant_violation_still_fails(self):
        _first, hdr = run.parse_header(run.find_isvc("p1632"))
        verdict, _checks = self._verdict(hdr, self._violation(
            "B", "missing-endpoint", "engine/0",
            "Ready+serving pod is not an endpoint of its Service"))
        self.assertEqual(verdict, "FAIL")
        self.assertFalse(batch.is_green(verdict))

    def test_unscoped_parked_case_still_hardens_on_invariants(self):
        """A notimpl case without inv_xfail keeps today's contract: an
        invariant violation fails it even though ordinary checks are parked."""
        _first, hdr = run.parse_header(run.find_isvc("p416"))
        self.assertEqual(hdr.get("notimpl"), 1)
        self.assertNotIn("inv_xfail", hdr)
        verdict, _checks = self._verdict(hdr, self._violation(
            "A", "ready-without-pods", "engine/0",
            "engine/0 Phase=Ready while the IR's own podCount is 0"))
        self.assertEqual(verdict, "FAIL")
        self.assertFalse(batch.is_green(verdict))


if __name__ == "__main__":
    unittest.main()
