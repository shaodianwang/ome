#!/usr/bin/env python3
import copy
import json
import subprocess
import sys
import time
import re
import unittest
from pathlib import Path
from unittest import mock

import batch
import generate_cases
import irstatus
import run
import runtime_fixtures

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


class PodRecordBuilders(unittest.TestCase):
    """Every loop that builds watcher records must carry the placement fields
    invariant D reads; a builder without them silently blinds the invariant."""

    def test_every_record_builder_includes_placement(self):
        src = (HERE / "run.py").read_text()
        builders = [m.start() for m in re.finditer(r'pod_state(?:"\])?\[[^\]]+\] = \{', src)]
        self.assertGreaterEqual(len(builders), 3)
        for start in builders:
            block = src[start:src.index("}", start + 1) + 1]
            # the closing brace of a nested dict literal ends the block early; look a bit further
            block = src[start:start + 1200]
            self.assertIn("_pod_placement(obj, labels)", block, src[start:start + 200])


if __name__ == "__main__":
    unittest.main()


class CanaryHookTest(unittest.TestCase):
    """The canary verb exposes the grid state as a hook; verbs and faults
    keyed on it fire in that state, optionally at one step."""

    def test_phase_maps_to_a_hook(self):
        self.assertEqual("canary-staging", run._canary_hook("Pending"))
        self.assertEqual("canary-serving", run._canary_hook("Canarying"))
        self.assertEqual("canary-serving", run._canary_hook("Paused"))
        self.assertEqual("canary-draining", run._canary_hook("Promoting"))
        self.assertEqual("canary-rollingback", run._canary_hook("RollingBack"))
        self.assertEqual("canary-failed", run._canary_hook("Failed"))
        self.assertIsNone(run._canary_hook("Stable"))
        self.assertIsNone(run._canary_hook(""))

    def test_hook_matches_state_and_optional_step(self):
        self.assertTrue(run._hook_due("canary-serving", "canary-serving", 1))
        self.assertTrue(run._hook_due("canary-serving@1", "canary-serving", 1))
        self.assertFalse(run._hook_due("canary-serving@0", "canary-serving", 1))
        self.assertFalse(run._hook_due("canary-failed", "canary-serving", 0))
        self.assertFalse(run._hook_due("canary-serving", None, 0))

    def test_hook_spec_validates(self):
        self.assertTrue(run._canary_hook_ok("canary-failed"))
        self.assertTrue(run._canary_hook_ok("canary-serving@2"))
        self.assertFalse(run._canary_hook_ok("canary-serving@x"))
        self.assertFalse(run._canary_hook_ok("rotating"))

    def test_stable_victim_is_an_old_revision_pod(self):
        pods = {"a": {"alive": True, "hash": "old", "inst": "0", "created": "1"},
                "b": {"alive": True, "hash": "new", "inst": "1", "created": "2"}}
        self.assertEqual("a", run._fault_pick_pod(pods, {"engine": 1}, {"old"}, victim="stable"))
        self.assertEqual("b", run._fault_pick_pod(pods, {"engine": 1}, {"old"}))

    def test_fault_tick_fires_on_the_canary_hook(self):
        fired = []
        f = {"case_id": "p1", "kind": "podkill", "param": "canary-serving@1", "victim": "",
             "fired": False, "infra_fail": None, "notes": []}
        ctx = {"fault": f, "trigger_ts": 1.0, "pod_state": {}, "pods_per_inst": {}, "old_hashes": set(),
               "canary_hook": "canary-serving", "canary_step": 0}
        orig = run._fault_fire
        run._fault_fire = lambda *a, **k: fired.append(a)
        try:
            run._fault_tick(ctx)
            self.assertEqual([], fired)
            ctx["canary_step"] = 1
            run._fault_tick(ctx)
            self.assertEqual(1, len(fired))
        finally:
            run._fault_fire = orig


class CanaryHeaderTest(unittest.TestCase):
    """validate-only knows the canary-mode knobs and refuses them elsewhere."""

    def _errs(self, header):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_canary_hooks_need_canary_mode(self):
        base = "# p999: x mode=rollout Ne=1 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 topo=engine"
        self.assertTrue(any("during=canary-serving" in e for e in self._errs(base + " during=canary-serving")))
        self.assertTrue(any("verb=" in e for e in self._errs(base + " verb=pause")))
        self.assertTrue(any("cell=" in e for e in self._errs(base + " cell=canary/Serving/pause")))

    def test_canary_knobs_validate(self):
        base = "# p999: x mode=canary Ne=1 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 canary=manual topo=engine"
        self.assertEqual([], self._errs(base + " during=canary-serving verb=pause verbhold=30 cell=canary/Serving/pause"))
        self.assertEqual([], self._errs(base + " fault=podkill:canary-serving victim=stable"))
        self.assertEqual([], self._errs(base + " bump=decoder img=example.com/canary-test/missing:none terminal=Failed"))
        self.assertEqual([], self._errs(base + " during=canary-serving verb=promote skipsoak=90 cell=canary/Serving[T]/promote"))
        self.assertTrue(any("verb=" in e for e in self._errs(base + " verb=pause")))
        self.assertTrue(any("victim=" in e for e in self._errs(base + " victim=stable")))
        self.assertTrue(any("img=" in e for e in self._errs(base + " img=example.com/canary-test/missing:none")))
        self.assertTrue(any("fault=" in e for e in self._errs(base + " fault=podkill:rotating")))
        self.assertTrue(any("cell=" in e for e in self._errs(base + " cell=nonsense")))

class ApplyCaseRuntimeLagTest(unittest.TestCase):
    """apply_case re-applies only the webhook's runtime-lag denial, only for a
    case that ships its own runtime, and only for a bounded window."""

    DENIAL = ('Error from server (Forbidden): admission webhook denied the request: '
              'runtime ome-coordtest-rt-p1 does not support model m: runtime not found')

    def _run(self, body, results, clock=None):
        calls = []
        ticks = iter(clock or [0.0] * 50)

        def fake_kubectl(*args, input=None, **kw):
            calls.append(input)
            code, err = results[min(len(calls) - 1, len(results) - 1)]
            return subprocess.CompletedProcess(args, code, stdout="", stderr=err)

        saved = (run.kubectl, run.render_case, run.time.sleep, run.time.monotonic)
        run.kubectl, run.render_case = fake_kubectl, lambda f: body
        run.time.sleep, run.time.monotonic = (lambda s: None), (lambda: next(ticks))
        try:
            return run.apply_case("case.yaml"), calls
        finally:
            run.kubectl, run.render_case, run.time.sleep, run.time.monotonic = saved

    def test_reapplies_the_lag_denial_until_the_runtime_is_visible(self):
        body = "kind: ClusterServingRuntime\n---\nkind: InferenceService\n"
        r, calls = self._run(body, [(1, self.DENIAL), (1, self.DENIAL), (0, "")])
        self.assertEqual(r.returncode, 0)
        self.assertEqual(len(calls), 3)

    def test_other_denials_and_runtime_less_cases_are_not_retried(self):
        body = "kind: ClusterServingRuntime\n---\nkind: InferenceService\n"
        r, calls = self._run(body, [(1, "Error from server (Forbidden): model m is disabled")])
        self.assertEqual((r.returncode, len(calls)), (1, 1))
        r, calls = self._run("kind: InferenceService\n", [(1, self.DENIAL)])
        self.assertEqual((r.returncode, len(calls)), (1, 1))

    def test_the_window_is_bounded(self):
        body = "kind: ServingRuntime\n---\nkind: InferenceService\n"
        # deadline is 30s after the first clock read; the attempt that sees the
        # window spent returns its denial and nothing runs after it
        clock = [0.0, 10.0, 20.0, 31.0, 40.0, 50.0]
        r, calls = self._run(body, [(1, self.DENIAL)], clock=clock)
        self.assertEqual(r.returncode, 1)
        self.assertEqual(len(calls), 3)

class CaseImageSideLoadTest(unittest.TestCase):
    """The deploy step side-loads every image a case embeds, except the one a
    case names precisely because it must never start."""

    def test_unpullable_image_is_left_out_of_the_side_load_list(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "p1.yaml").write_text(
                "# p1: x mode=canary bump=engine img=%s terminal=Failed\n"
                "spec: {}\n" % runtime_fixtures.UNPULLABLE_IMAGE)
            (root / "p2.yaml").write_text(
                "# p2: y mode=runtimerev img=${OME_EVAL_NGINX_ALT_IMAGE}\n"
                "      image: ${OME_EVAL_NGINX_IMAGE}\n")
            saved = runtime_fixtures.CASE_DIR
            runtime_fixtures.CASE_DIR = root
            try:
                images = runtime_fixtures.case_images(kind=True)
            finally:
                runtime_fixtures.CASE_DIR = saved
        self.assertNotIn(runtime_fixtures.UNPULLABLE_IMAGE, images)
        self.assertEqual(2, len(images))
        self.assertTrue(all("nginx-unprivileged" in i for i in images), images)

    def test_committed_corpus_side_loads_no_unpullable_image(self):
        self.assertNotIn(runtime_fixtures.UNPULLABLE_IMAGE, runtime_fixtures.case_images(kind=True))

class PDCanaryKnobTest(unittest.TestCase):
    """The PD knobs of canary mode: a podkill aimed at one Component, the
    cohort-ratio observer, the scoped park, and the pairing patch trigger."""

    def test_victim_component_narrows_every_rule(self):
        pods = {"e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "created": "1"},
                "d0": {"alive": True, "hash": "old", "inst": "0", "comp": "decoder", "created": "2"},
                "d1": {"alive": True, "hash": "new", "inst": "1", "comp": "decoder", "created": "3"}}
        self.assertEqual("d1", run._fault_pick_pod(pods, {"engine": 1, "decoder": 1}, {"old"}, comp="decoder"))
        self.assertEqual("d0", run._fault_pick_pod(pods, {"engine": 1, "decoder": 1}, {"old"}, victim="stable", comp="decoder"))
        self.assertEqual("e0", run._fault_pick_pod(pods, {"engine": 1, "decoder": 1}, {"old"}, comp="engine"))
        self.assertIsNone(run._fault_pick_pod(pods, {"engine": 1, "decoder": 1}, {"old"}, comp="router"))

    @staticmethod
    def _pod(comp, inst, hash_, ready=True):
        return {"alive": True, "ready": ready, "serving": ready, "comp": comp, "inst": str(inst), "hash": hash_}

    def test_cohort_counts_split_fully_serving_instances_by_revision(self):
        pods = {"e0": self._pod("engine", 0, "old"), "e1": self._pod("engine", 1, "new"),
                "e2": self._pod("engine", 2, "new", ready=False),
                "d0": self._pod("decoder", 0, "old"), "d1": self._pod("decoder", 1, "new")}
        got = run._cohort_serving_instances(pods, {"engine": 1, "decoder": 1}, {"old"})
        self.assertEqual({"engine": (1, 1), "decoder": (1, 1)}, got)

    def test_cohort_ratio_records_only_a_sustained_skew(self):
        inv = run._inv_new("p1", {})
        inv["ratio"] = {"N": {"engine": 4, "decoder": 2}, "tol": 0.25, "old_hashes": {"old"},
                        "samples": 0, "streak": 0, "excursions": 0, "worst": 0.0, "worst_detail": ""}
        skew = {"e1": self._pod("engine", 1, "new"), "d1": self._pod("decoder", 1, "new")}   # 1:1 in a 2:1 fleet
        run._inv_check_cohort_ratio(inv, skew, {"engine": 1, "decoder": 1}, 1.0)
        self.assertEqual((1, 0), (inv["ratio"]["samples"], inv["ratio"]["excursions"]))
        run._inv_check_cohort_ratio(inv, skew, {"engine": 1, "decoder": 1}, 6.0)
        self.assertEqual(1, inv["ratio"]["excursions"])
        self.assertAlmostEqual(0.5, inv["ratio"]["worst"])
        even = {**skew, "e2": self._pod("engine", 2, "new")}                                  # 2:1 again
        run._inv_check_cohort_ratio(inv, even, {"engine": 1, "decoder": 1}, 11.0)
        self.assertEqual(0, inv["ratio"]["streak"])
        checks = []
        run.invariant_checks(inv, "PASS", [], checks)
        self.assertTrue(any("invariant E" in c and "1 sustained excursion" in c for c in checks), checks)
        # informational: never a FAIL
        self.assertEqual("PASS", run.invariant_checks(inv, "PASS", [], []))

    def test_cohort_ratio_is_vacuous_without_canary_capacity_on_both_sides(self):
        inv = run._inv_new("p1", {})
        inv["ratio"] = {"N": {"engine": 4, "decoder": 2}, "tol": 0.25, "old_hashes": {"old"},
                        "samples": 0, "streak": 0, "excursions": 0, "worst": 0.0, "worst_detail": ""}
        run._inv_check_cohort_ratio(inv, {"e1": self._pod("engine", 1, "new")}, {"engine": 1, "decoder": 1}, 1.0)
        self.assertEqual(0, inv["ratio"]["samples"])
        checks = []
        run.invariant_checks(inv, "PASS", [], checks)
        self.assertTrue(any("invariant E" in c and "vacuous" in c for c in checks), checks)

    def test_scoped_park_softens_only_its_own_check(self):
        plain = {"notimpl": 1, "issue": 1346}
        self.assertTrue(run.soften_expected_fail(plain, "FAIL", []).startswith("XFAIL"))
        scoped = {"notimpl": 1, "issue": 1346, "xcheck": "sectraffic"}
        self.assertEqual("FAIL", run.soften_expected_fail(scoped, "FAIL", []))
        self.assertEqual("PASS", run.soften_expected_fail(scoped, "PASS", []))

    def _surface(self, known_gap):
        ctx = {"pod_state": {"e0": {**self._pod("engine", 0, "new"), "pairing": "v2"},
                             "d0": {**self._pod("decoder", 0, "new"), "pairing": "v2"}}}
        status = {"components": {
            "engine": {"traffic": [{"revisionName": "p1-engine-rev-new", "percent": 100, "pairingProtocol": "v2"}]},
            "decoder": {"traffic": []}}}
        saved = (run.fetch_status, run.kubectl)
        run.fetch_status = lambda cid: status
        run.kubectl = lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout=json.dumps({"items": [
            {"metadata": {"name": "p1-engine-rev-new", "labels": {run.PAIRING_LABEL: "v2"}}, "spec": {"selector": {}}}]}), stderr="")
        try:
            notes, checks = [], []
            verdict = run.pairing_surface_checks("p1", ctx, "v2", ["engine", "decoder"], "PASS", notes, checks,
                                                 known_gap=known_gap)
            return verdict, ctx, notes, checks
        finally:
            run.fetch_status, run.kubectl = saved

    def test_secondary_traffic_gap_is_red_unless_scoped(self):
        verdict, ctx, notes, _ = self._surface(known_gap=())
        self.assertEqual("FAIL", verdict)
        self.assertIn("decoder publishes no live traffic target", notes)
        self.assertFalse(ctx.get("xhit"))

    def test_scoped_secondary_traffic_gap_records_the_tracked_bug(self):
        verdict, ctx, notes, checks = self._surface(known_gap={"decoder"})
        self.assertEqual("PASS", verdict)
        self.assertTrue(ctx.get("xhit"))
        self.assertTrue(any("xcheck=sectraffic" in c for c in checks), checks)

    def test_pairing_patch_carries_the_trigger_annotations(self):
        seen = []
        saved = run.kubectl
        run.kubectl = lambda *a, **k: (seen.append(a), subprocess.CompletedProcess(a, 0, stdout="", stderr=""))[1]
        try:
            ok, _ = run._pairing_patch("p1", "v2", extra={"perm.test/error-rate": "0.9"}, comps={"engine", "decoder"})
        finally:
            run.kubectl = saved
        self.assertTrue(ok)
        body = json.loads(seen[0][-1])
        self.assertEqual("v2", body["spec"]["rollout"]["pairingProtocol"])
        self.assertEqual({"perm.test/error-rate": "0.9"}, body["spec"]["decoder"]["annotations"])
        self.assertEqual({"perm.test/error-rate": "0.9"}, body["spec"]["engine"]["annotations"])
        # a clear removes the field and carries nothing else
        run.kubectl = lambda *a, **k: (seen.append(a), subprocess.CompletedProcess(a, 0, stdout="", stderr=""))[1]
        try:
            run._pairing_patch("p1", "")
        finally:
            run.kubectl = saved
        self.assertEqual({"spec": {"rollout": {"pairingProtocol": None}}}, json.loads(seen[1][-1]))


class PDCanaryHeaderTest(unittest.TestCase):
    """validate-only knows the PD knobs of canary mode and their constraints."""

    def _errs(self, header, second=""):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\n" + (second + "\n" if second else "") + "metadata: { name: p999 }\n")
            return run.validate_case_file(p)

    PD = "# p999: x mode=canary Ne=2 Nd=2 Nr=0 ceil_e=3 ceil_d=3 ceil_r=0 canary=auto topo=pd"

    def test_pairing_trigger_validates_in_canary_mode(self):
        self.assertEqual([], self._errs(self.PD + " proto=v2 cell=canary/Serving/pairing"))
        self.assertEqual([], self._errs(self.PD + " proto=v2 proto2=v3 during=canary-serving cell=canary/Serving/retarget"))
        self.assertEqual([], self._errs(self.PD + " proto=v2 ratio_tol=25 cell=canary/Staging/ratio"))
        self.assertTrue(any("needs Ne>0 and Nd>0" in e for e in self._errs(
            "# p999: x mode=canary Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 canary=auto topo=engine proto=v2")))
        self.assertTrue(any("bump= does not apply" in e for e in self._errs(self.PD + " proto=v2 bump=engine")))
        self.assertTrue(any("carries no image" in e for e in self._errs(self.PD + " proto=v2 img=example.com/canary-test/missing:none")))
        self.assertTrue(any("needs proto=" in e for e in self._errs(self.PD + " proto2=v3")))
        self.assertTrue(any("must differ" in e for e in self._errs(self.PD + " proto=v2 proto2=v2")))
        self.assertTrue(any("pairing- or canary-mode" in e for e in self._errs(
            "# p999: x mode=rollout Ne=2 Nd=2 Nr=0 ceil_e=3 ceil_d=3 ceil_r=0 topo=pd proto=v2")))

    def test_scale_fires_through_the_verb(self):
        self.assertEqual([], self._errs(self.PD + " during=canary-serving verb=scale scale_d=4 cell=canary/Serving/scale"))
        self.assertTrue(any("verb=scale needs" in e for e in self._errs(self.PD + " during=canary-serving verb=scale")))
        self.assertTrue(any("verb=scale during" in e for e in self._errs(self.PD + " scale_d=4")))

    def test_component_aimed_knobs_need_their_base_knob(self):
        self.assertEqual([], self._errs(self.PD + " fault=podkill:canary-serving victim=stable victimcomp=decoder"))
        self.assertTrue(any("victimcomp=" in e for e in self._errs(self.PD + " victimcomp=decoder")))
        self.assertEqual([], self._errs(self.PD + " bump=engine,decoder img=example.com/canary-test/missing:none imgcomp=decoder terminal=Failed"))
        self.assertTrue(any("imgcomp=" in e for e in self._errs(self.PD + " imgcomp=decoder")))
        self.assertTrue(any("must be one of the bumped" in e for e in self._errs(
            self.PD + " bump=engine img=example.com/canary-test/missing:none imgcomp=decoder")))

    def test_scoped_park_needs_the_parking_triple(self):
        ok = self._errs(self.PD + " proto=v2 xcheck=sectraffic notimpl=1 issue=1346", "# reason: parked on the secondary traffic gap")
        self.assertEqual([], ok)
        self.assertTrue(any("requires notimpl=1 issue" in e for e in self._errs(self.PD + " proto=v2 xcheck=sectraffic", "# reason: x")))
        self.assertTrue(any("reason:" in e for e in self._errs(self.PD + " proto=v2 xcheck=sectraffic notimpl=1 issue=1346")))
        self.assertTrue(any("needs proto=" in e for e in self._errs(self.PD + " xcheck=sectraffic notimpl=1 issue=1346", "# reason: x")))


class CanaryFloorAndSoakTest(unittest.TestCase):
    """The capacity floor honors the soft opt-out in every handler, and a
    podkill records the Component it hit so the canary verdict can exempt it."""

    def _ctx(self, **hdr):
        return {"min_ready": {"engine": 7}, "floor_sust": {"engine": 20.0}, "hdr": hdr,
                "N": {"engine": 8}, "info": {"max_unavail": {"engine": 0}, "strat": {"engine": "SurgeThenDrain"}}}

    def test_soft_floor_reports_instead_of_failing(self):
        checks = []
        self.assertEqual("FAIL", run.min_ready_checks(self._ctx(), ["engine"], "PASS", [], []))
        self.assertEqual("PASS", run.min_ready_checks(self._ctx(floor_soft=1), ["engine"], "PASS", [], checks))
        self.assertTrue(any("floor_soft=1" in c for c in checks), checks)

    def test_podkill_records_the_victim_component(self):
        pods = {"p1-decoder-0-default-0": {"alive": True, "hash": "new", "inst": "0", "comp": "decoder", "created": "1"}}
        f = {"kind": "podkill", "param": "canary-serving", "victim": "canary", "victimcomp": "decoder",
             "fired": False, "infra_fail": None, "notes": []}
        saved = run.kubectl
        run.kubectl = lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout="", stderr="")
        try:
            run._fault_fire(f, pods, {"decoder": 1}, {"old"})
        finally:
            run.kubectl = saved
        self.assertEqual("decoder", f["victim_comp"])
        self.assertTrue(f["fired"])

class PairAliveOneByOneTest(unittest.TestCase):
    """At 1x1 the unpaired first-mover window is exempt only for drain-first
    strategies; under a surge or in-place strategy it is a violation."""

    @staticmethod
    def _pod(comp, proto):
        return {"alive": True, "ready": True, "serving": True, "comp": comp, "inst": "0", "pairing": proto}

    def _run(self, drain_first):
        inv = run._inv_new("p1", {})
        inv["pairing"] = {"frm": "v1", "to": "v2", "armed": False, "seen": False, "drain_first": drain_first}
        paired = {"e0": self._pod("engine", "v1"), "d0": self._pod("decoder", "v1")}
        split = {"e0": self._pod("engine", "v1"), "d0": self._pod("decoder", "v2")}
        run._inv_check_pair_alive(inv, paired, set(), 1.0)          # arms
        for tick in range(run.INV_SUSTAIN_C):
            hits = set()
            run._inv_check_pair_alive(inv, split, hits, 6.0 + 5 * tick)
        return inv

    def test_surge_one_by_one_split_is_a_violation(self):
        inv = self._run(drain_first=False)
        self.assertIn(("C", "pair-alive"), inv["viol"])

    def test_drain_first_one_by_one_split_is_exempt(self):
        inv = self._run(drain_first=True)
        self.assertEqual({}, inv["viol"])

    def test_drain_first_is_read_from_the_strategies(self):
        self.assertFalse(run._pairing_drain_first({"strat": {"engine": "SurgeThenDrain", "decoder": "InPlaceOnly"}}))
        self.assertTrue(run._pairing_drain_first({"strat": {"engine": "SurgeThenDrain", "decoder": "RecreatePod"}}))
        self.assertFalse(run._pairing_drain_first({"strat": {}}))

class CanaryHandlerBindingsTest(unittest.TestCase):
    """The canary handler keeps one meaning per name: `want` is the replica
    target dict for the whole verdict, never rebound to a phase string."""

    def test_want_is_bound_once_in_the_canary_handler(self):
        import ast
        tree = ast.parse(open("run.py").read())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_canary")
        binds = [n.lineno for n in ast.walk(fn) if isinstance(n, ast.Assign)
                 for tgt in n.targets if isinstance(tgt, ast.Name) and tgt.id == "want"]
        self.assertEqual(1, len(binds), f"`want` rebound inside _canary at lines {binds}")
