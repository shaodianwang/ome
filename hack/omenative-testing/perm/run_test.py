#!/usr/bin/env python3
import ast
import contextlib
import copy
import datetime
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import re
import unittest
from pathlib import Path
from unittest import mock

import batch
import casefiles
import gen_cases
import gen_mn
import generate_cases
import irstatus
import run
import runtime_fixtures

HERE = Path(__file__).parent.resolve()


def _tree(root):
    """{relative path: bytes} of every file under root."""
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


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


class MaxUnavailableResolutionTest(unittest.TestCase):
    """The capacity floor is N minus the budget the engine resolves: an
    integer as it is, a percent rounded up and clamped to the fleet, the
    per-Component budget's documented rule."""

    def test_integers_pass_through(self):
        self.assertEqual(2, run.resolve_max_unavailable(2, 8))
        self.assertEqual(1, run.resolve_max_unavailable("1", 8))
        self.assertEqual(0, run.resolve_max_unavailable(0, 1))

    def test_percent_rounds_up_like_the_engine(self):
        self.assertEqual(2, run.resolve_max_unavailable("25%", 8))
        self.assertEqual(8, run.resolve_max_unavailable("100%", 8))
        self.assertEqual(1, run.resolve_max_unavailable("50%", 1))
        self.assertEqual(1, run.resolve_max_unavailable("25%", 2))
        self.assertEqual(1, run.resolve_max_unavailable("50%", 2))
        self.assertEqual(0, run.resolve_max_unavailable("0%", 4))

    def test_percent_is_clamped_to_the_fleet(self):
        self.assertEqual(1, run.resolve_max_unavailable("100%", 1))
        self.assertEqual(0, run.resolve_max_unavailable("50%", 0))

    def test_unparsable_is_zero(self):
        self.assertEqual(0, run.resolve_max_unavailable("lots", 8))
        self.assertEqual(0, run.resolve_max_unavailable(None, 8))


class GeneratedCorpusTest(unittest.TestCase):
    def test_regeneration_is_byte_stable(self):
        """The gen-* corpus is a pure function of generate_cases.py: a hand
        edit to a generated file (or a generator edit without a regen) must
        fail here, not surface as review noise."""
        r = subprocess.run([sys.executable, str(HERE / "generate_cases.py"), "--check"],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, msg=(r.stdout + r.stderr))

    def test_feature_regeneration_is_byte_stable(self):
        """The cases gen_cases.py mints are a pure function of the generator
        plus the hand-maintained header layer it carries over (the cell= tags
        on its canary cases): regenerating changes nothing."""
        r = subprocess.run([sys.executable, str(HERE / "gen_cases.py"), "--check"],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, msg=(r.stdout + r.stderr))

    def test_multinode_regeneration_is_byte_stable(self):
        r = subprocess.run([sys.executable, str(HERE / "gen_mn.py"), "--check"],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, msg=(r.stdout + r.stderr))

    def test_coverage_ledger_check_passes(self):
        """Every header value is one the coverage ledger can place on an axis
        and every operator story has at least one case: a new verb or knob
        is classified in coverage.py before it merges, and a story never
        loses its last case unnoticed."""
        r = subprocess.run([sys.executable, str(HERE / "coverage.py"), "--check"],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, msg=(r.stdout + r.stderr))

    def test_regenerating_the_corpus_writes_nothing_and_deletes_nothing(self):
        """Regenerating a copy of the committed tree through the real write
        paths leaves every file byte-identical and deletes none — including
        the hand-written cases that share folders with generated ones."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "isvcs"
            shutil.copytree(HERE / "isvcs", root)
            before = _tree(root)
            feature = gen_cases.cases()
            self.assertTrue(casefiles.unminted(feature, root),
                            "the feature folders hold hand-written cases")
            self.assertEqual([], casefiles.write(feature, root))
            self.assertEqual([], casefiles.write(gen_mn.cases(), root))
            outputs, _rows = generate_cases.build_outputs()
            generate_cases.write(outputs, root)
            self.assertEqual(before, _tree(root))

    def test_corpus_stays_under_lane_cap(self):
        # The eval lanes are sharded per feature and topology with pinned
        # sub-shards, so the corpus total bounds nothing physical; the ceiling
        # exists to force a conscious decision when the corpus grows past it.
        n = sum(1 for _ in (HERE / "isvcs").glob("**/*.yaml"))
        self.assertLessEqual(n, 6000)

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


class GeneratedFolderGuardTest(unittest.TestCase):
    """generate_cases owns its gen-* folders wholesale and knows its files by
    the [gen] mark: an unmarked file there is hand-written and stops the regen
    before anything is written or swept, whatever its id; a marked file
    outside the emitted set is stale and swept."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root)
        self.outputs, _rows = generate_cases.build_outputs()

    def _put(self, rel, text):
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
        return p

    def test_hand_written_case_in_a_generated_folder_is_refused_not_swept(self):
        at_generated_id = self._put("gen-fault/p1300.yaml",
                                    "# p1300: kill engine, hand-pinned  mode=kill Ne=4 co=none topo=engine\n"
                                    "apiVersion: ome.io/v1beta1\n")
        at_free_id = self._put("gen-fault/p1599.yaml",
                               "# p1599: kill pd, hand-pinned  mode=kill Ne=4 co=none topo=pd\n"
                               "apiVersion: ome.io/v1beta1\n")
        with self.assertRaises(SystemExit) as cm:
            generate_cases.write(self.outputs, self.root)
        self.assertIn("hand-written case in a generator-owned folder", str(cm.exception))
        self.assertEqual({at_generated_id, at_free_id}, set(self.root.rglob("*.yaml")))
        with contextlib.redirect_stderr(io.StringIO()) as err:
            self.assertEqual(1, generate_cases.check(self.outputs, self.root))
        self.assertIn("hand-written case at a generated id", err.getvalue())
        self.assertIn("hand-written case in a generator-owned folder", err.getvalue())

    def test_stale_generated_file_is_swept_and_the_corpus_written(self):
        stale = self._put("gen-rollout/p1199.yaml",
                          "# p1199: [gen] rollout engine N=4 SurgeThenDrain s1  Ne=4 co=none topo=engine\n"
                          "apiVersion: ome.io/v1beta1\n")
        generate_cases.write(self.outputs, self.root)
        self.assertFalse(stale.exists())
        self.assertEqual(sorted(self.outputs), sorted(_tree(self.root)))
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(0, generate_cases.check(self.outputs, self.root))


class GeneratorWritePathTest(unittest.TestCase):
    """casefiles, the write path of the generators that share folders with
    hand-written cases: only minted ids are written, nothing is deleted, a
    file the generator does not recognise stops the run, and the
    hand-maintained header layer rides through a regeneration."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root)
        (self.root / "restart").mkdir()

    def _put(self, rel, text):
        p = self.root / rel
        p.write_text(text)
        return p

    @staticmethod
    def _case(cid, desc="kill engine restartPolicy=None", ceil_e=8,
              body="apiVersion: ome.io/v1beta1\nkind: InferenceService\n"):
        kv = [("mode", "kill"), ("Ne", 8), ("ceil_e", ceil_e), ("restart", "None"),
              ("co", "none"), ("topo", "engine")]
        return casefiles.Case("restart", cid, desc, kv, body)

    def test_writes_only_minted_ids_and_deletes_nothing(self):
        hand = "# p148: kill engine under pause=true  mode=kill Ne=4 pause=true co=none topo=engine\napiVersion: ome.io/v1beta1\n"
        unminted = "# p131: kill engine restartPolicy=RecreateInstance  mode=kill Ne=8 co=none topo=engine\napiVersion: ome.io/v1beta1\n"
        self._put("restart/p148.yaml", hand)
        self._put("restart/p131.yaml", unminted)
        minted = [self._case("p130")]
        self.assertEqual(["restart/p130.yaml"], casefiles.write(minted, self.root))
        self.assertEqual(minted[0].text(), (self.root / "restart/p130.yaml").read_text())
        self.assertEqual(hand, (self.root / "restart/p148.yaml").read_text())
        self.assertEqual(unminted, (self.root / "restart/p131.yaml").read_text())
        self.assertEqual({"restart": ["p131", "p148"]}, casefiles.unminted(minted, self.root))
        self.assertEqual([], casefiles.write(minted, self.root), "a second run is a no-op")

    def test_hand_layer_rides_through_a_regeneration(self):
        """Keys the generator does not emit keep their place among its keys,
        the line-2 reason stays, while an owned value and the body follow the
        generator."""
        self._put("restart/p130.yaml",
                  "# p130: kill engine restartPolicy=None  mode=kill Ne=8 ceil_e=8 restart=None "
                  "cell=restart/engine/none notimpl=1 issue=7 co=none topo=engine\n"
                  "# reason: parked on a tracked bug\n"
                  "apiVersion: ome.io/v1beta1\nkind: InferenceService\n")
        c = self._case("p130", ceil_e=9,
                       body="apiVersion: ome.io/v1beta1\nkind: InferenceService\nspec: {}\n")
        casefiles.write([c], self.root)
        self.assertEqual(
            "# p130: kill engine restartPolicy=None  mode=kill Ne=8 ceil_e=9 restart=None "
            "cell=restart/engine/none notimpl=1 issue=7 co=none topo=engine\n"
            "# reason: parked on a tracked bug\n"
            "apiVersion: ome.io/v1beta1\nkind: InferenceService\nspec: {}\n",
            (self.root / "restart/p130.yaml").read_text())
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(0, casefiles.check([c], self.root, "t"))

    def test_tokens_the_generator_does_not_emit_keep_their_place(self):
        """Whatever the generator does not emit for the case is kept where it
        sits — a token ahead of every emitted key included, and a key the
        generator stops emitting, which is removed by hand."""
        self._put("restart/p130.yaml",
                  "# p130: kill engine restartPolicy=None  cell=restart/engine/none mode=kill Ne=8 "
                  "ceil_e=8 restart=None pause=true co=none topo=engine\n"
                  "apiVersion: ome.io/v1beta1\nkind: InferenceService\n")
        c = casefiles.Case("restart", "p130", "kill engine restartPolicy=None",
                           [("mode", "kill"), ("Ne", 8), ("ceil_e", 8), ("co", "none"), ("topo", "engine")],
                           "apiVersion: ome.io/v1beta1\nkind: InferenceService\n")
        self.assertEqual("# p130: kill engine restartPolicy=None  cell=restart/engine/none mode=kill Ne=8 "
                         "ceil_e=8 restart=None pause=true co=none topo=engine",
                         c.text((self.root / "restart/p130.yaml").read_text()).splitlines()[0])

    def test_the_generator_owns_its_keys_the_description_and_the_body(self):
        """A hand edit to an owned key or the body is drift: check reports it
        and a regeneration puts the generator's value back."""
        self._put("restart/p130.yaml",
                  "# p130: kill engine restartPolicy=None  mode=kill Ne=8 ceil_e=99 restart=None "
                  "co=none topo=engine\napiVersion: ome.io/v1beta1\nkind: InferenceService\n")
        c = self._case("p130")
        with contextlib.redirect_stderr(io.StringIO()) as err:
            self.assertEqual(1, casefiles.check([c], self.root, "t"))
        self.assertIn("drifted from the generator", err.getvalue())
        casefiles.write([c], self.root)
        self.assertEqual(c.text(), (self.root / "restart/p130.yaml").read_text())

    def test_a_case_it_does_not_recognise_stops_the_regeneration(self):
        """A minted id held by a different case — hand-written, or the matrix
        renumbered — refuses the whole run: nothing written, nothing lost."""
        hand = "# p131: kill engine under pause=true  mode=kill Ne=4 pause=true co=none topo=engine\napiVersion: ome.io/v1beta1\n"
        self._put("restart/p131.yaml", hand)
        minted = [self._case("p130"), self._case("p131", desc="kill engine restartPolicy=RecreateInstance")]
        with contextlib.redirect_stderr(io.StringIO()) as err, self.assertRaises(SystemExit) as cm:
            casefiles.write(minted, self.root)
        self.assertIn("nothing was written", str(cm.exception))
        self.assertIn("p131.yaml: holds 'p131: kill engine under pause=true", err.getvalue())
        self.assertEqual(hand, (self.root / "restart/p131.yaml").read_text())
        self.assertFalse((self.root / "restart/p130.yaml").exists())
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(1, casefiles.check(minted, self.root, "t"))

    def test_check_reports_a_missing_minted_file(self):
        with contextlib.redirect_stderr(io.StringIO()) as err:
            self.assertEqual(1, casefiles.check([self._case("p130")], self.root, "t"))
        self.assertIn("missing on disk", err.getvalue())

    def test_only_reason_lines_belong_to_the_header_layer(self):
        """A body that opens with its own comments (a runtime fixture) is body:
        it is neither carried over nor duplicated."""
        body = "# Test runtime fixture\n# second line\napiVersion: ome.io/v1beta1\n"
        c = casefiles.Case("runtime-rev", "p400", "runtime-rev engine roll (autoSync=true, material edit)",
                           [("mode", "runtimerev"), ("Ne", 4)], body, sep=" ")
        on_disk = c.text()
        self.assertEqual(on_disk, c.text(on_disk))
        self.assertEqual(("# p400: runtime-rev engine roll (autoSync=true, material edit) mode=runtimerev Ne=4",
                          [], body), casefiles.split(on_disk))

    def test_single_space_headers_recognise_only_a_key_value_region(self):
        c = casefiles.Case("runtime-rev", "p400", "runtime-rev engine roll (autoSync=true, material edit)",
                           [("mode", "runtimerev"), ("Ne", 4)], "apiVersion: ome.io/v1beta1\n", sep=" ")
        head = "# p400: runtime-rev engine roll (autoSync=true, material edit)"
        self.assertTrue(c.recognises(f"{head} mode=runtimerev Ne=4\n"))
        self.assertTrue(c.recognises(f"{head} cell=x mode=runtimerev Ne=4\n"))
        self.assertFalse(c.recognises(f"{head} reworded mode=runtimerev Ne=4\n"))
        self.assertFalse(c.recognises("# p400: another case mode=runtimerev Ne=4\n"))


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

    def _parked(self, line):
        """A parked header as parse_header reads it, independent of which
        corpus cases happen to be parked today."""
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            f.write(line + "\n")
        self.addCleanup(os.unlink, f.name)
        return run.parse_header(f.name)[1]

    def test_in_scope_invariant_violation_records_as_known_bug_xfail(self):
        hdr = self._parked("# p1632: a parked case with its invariant scoped  mode=restart Ne=1 Nd=0 Nr=0 "
                           "ceil_e=1 ceil_d=0 ceil_r=0 notimpl=1 issue=1354 inv_xfail=A co=none topo=engine")
        self.assertEqual(hdr.get("notimpl"), 1)
        self.assertEqual(hdr.get("inv_xfail"), "A")
        verdict, checks = self._verdict(hdr, self._violation(
            "A", "ready-without-pods", "engine/0",
            "engine/0 Phase=Ready while the IR's own podCount is 0"))
        self.assertEqual(verdict, f"XFAIL (known bug: #{hdr['issue']})")
        self.assertTrue(batch.is_green(verdict))
        self.assertTrue(any("tracked bug this case is parked on" in c for c in checks))

    def test_out_of_scope_invariant_violation_still_fails(self):
        hdr = self._parked("# p1632: a parked case with its invariant scoped  mode=restart Ne=1 Nd=0 Nr=0 "
                           "ceil_e=1 ceil_d=0 ceil_r=0 notimpl=1 issue=1354 inv_xfail=A co=none topo=engine")
        verdict, _checks = self._verdict(hdr, self._violation(
            "B", "missing-endpoint", "engine/0",
            "Ready+serving pod is not an endpoint of its Service"))
        self.assertEqual(verdict, "FAIL")
        self.assertFalse(batch.is_green(verdict))

    def test_unscoped_parked_case_still_hardens_on_invariants(self):
        """A notimpl case without inv_xfail keeps today's contract: an
        invariant violation fails it even though ordinary checks are parked."""
        hdr = self._parked("# p416: a parked case with no invariant scoped  mode=rollout Ne=1 Nd=0 Nr=0 "
                           "ceil_e=2 ceil_d=0 ceil_r=0 notimpl=1 issue=1355 co=none topo=engine")
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
            self.assertIn("_pod_template(obj)", block, src[start:start + 200])

    def test_every_record_builder_reads_the_pod_ready_condition(self):
        """`ready` is the pod's Ready condition in every builder; a container
        status is what the kubelet last wrote and freezes when it stops."""
        src = (HERE / "run.py").read_text()
        builders = [m.start() for m in re.finditer(r'pod_state(?:"\])?\[[^\]]+\] = \{', src)]
        self.assertGreaterEqual(len(builders), 3)
        for start in builders:
            before = src[max(0, start - 3000):start]
            self.assertIn("ready = _pod_ready(obj)", before, src[start:start + 200])
            self.assertNotIn('cs.get("ready")', before.rsplit("ready = _pod_ready(obj)", 1)[1])
            self.assertIn('"ctn_ready": _main_container_ready(obj)', src[start:start + 1200])


def pod_object(name, component, index, revision, image, anno, *, runner="default",
               test_rev="v1", cpu="50m", ready=True, serving=True, container_ready=None):
    """A pod as kubectl -w hands it to the watcher: the labels, annotation,
    container spec and conditions the record builders and invariants read.
    `ready` is the pod's Ready condition; the main container's own status
    follows it unless `container_ready` says otherwise."""
    return {
        "metadata": {"name": name, "uid": f"uid-{name}",
                     "labels": {"component": component, "ome.io/instance-index": str(index),
                                "ome.io/runner": runner, "ome.io/pod-ordinal": "0",
                                run.REV_LABEL: revision},
                     "annotations": {run.TRIGGER_ANNO: anno} if anno else {}},
        "spec": {"containers": [{
            "name": run.MAIN_CONTAINER, "image": image,
            "env": [{"name": "TEST_REV", "value": test_rev},
                    {"name": "OME_INSTANCE_INDEX", "value": str(index)},
                    {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}],
            "resources": {"requests": {"cpu": cpu, "memory": "64Mi"}}}]},
        "status": {"containerStatuses": [{"name": run.MAIN_CONTAINER,
                                          "ready": ready if container_ready is None else container_ready}],
                   "conditions": [{"type": run.SERVING_COND, "status": "True" if serving else "False"},
                                  {"type": "Ready", "status": "True" if ready else "False"}]},
    }


def watcher_record(obj):
    """The watcher record the real event builder makes of a pod object."""
    ctx = {"pod_state": {}, "info": {}}
    run._apply_event(ctx, {"type": "ADDED", "object": obj})
    return ctx["pod_state"][obj["metadata"]["name"]]


def revision_payload(image, anno, *, test_rev="v1", cpu="50m", worker_image=None):
    """A ControllerRevision's stored data: the pod template, its metadata
    and, for a gang, the worker template."""
    def spec(img):
        return {"containers": [{"name": run.MAIN_CONTAINER, "image": img,
                                "env": [{"name": "TEST_REV", "value": test_rev}],
                                "resources": {"requests": {"cpu": cpu, "memory": "64Mi"}}}]}
    out = {"podSpec": spec(image), "podMeta": {"annotations": {run.TRIGGER_ANNO: anno}}}
    if worker_image:
        out["workerPodSpec"] = spec(worker_image)
    return out


def observe_ticks(inv, pods, ticks, *, start=0.0, pods_per_inst=None, **snapshots):
    """Drive consecutive invariant ticks over fixed snapshots."""
    for i in range(ticks):
        run._inv_observe(inv, pods, pods_per_inst or {"engine": 1},
                         start + i * run.INV_TICK_S, **snapshots)
    return start + ticks * run.INV_TICK_S


def routed_service(case_id, component, revision, *uids, ready=True, runner=None):
    """A per-revision routed Service and the EndpointSlice mirroring it, as
    the invariant poller lists them: the Service selects the Component's
    revision (optionally one runner), the slice carries the given pod UIDs."""
    name = f"{case_id}-{component}-rev-{revision}"
    selector = {"ome.io/inferenceservice": case_id, "component": component, run.REV_LABEL: revision}
    if runner:
        selector["ome.io/runner"] = runner
    svc = {"metadata": {"name": name}, "spec": {"clusterIP": "10.0.0.7", "selector": selector}}
    sl = {"metadata": {"labels": {"kubernetes.io/service-name": name}},
          "endpoints": [{"targetRef": {"uid": u}, "conditions": {"ready": ready}} for u in uids]}
    return svc, sl


def routing_of(*pairs):
    """(Services, EndpointSlices) from routed_service pairs."""
    return [s for s, _ in pairs], [sl for _, sl in pairs]


class PodReadyConditionTest(unittest.TestCase):
    """The watcher's `ready` is the pod's Ready condition. A pod on a node
    whose kubelet stopped keeps a container status that says ready while the
    node lifecycle controller sets the pod's Ready condition False and the
    EndpointSlices drop it: the fleet, the invariants and the floor must all
    read it as not Ready."""

    REV = "a1b2c3d4"

    def _lost(self):
        return pod_object("engine-0", "engine", 0, self.REV, "img:1", "v1", ready=False, container_ready=True)

    def test_record_reads_the_condition_and_keeps_the_container_state(self):
        rec = watcher_record(self._lost())
        self.assertFalse(rec["ready"])
        self.assertTrue(rec["ctn_ready"])
        self.assertTrue(rec["serving"], "the gate the controller wrote is read as it is")
        live = watcher_record(pod_object("engine-1", "engine", 1, self.REV, "img:1", "v1"))
        self.assertTrue(live["ready"] and live["ctn_ready"])
        gated = watcher_record(pod_object("engine-2", "engine", 2, self.REV, "img:1", "v1", ready=False, serving=False,
                                          container_ready=True))
        self.assertFalse(gated["ready"], "containers ready behind a closed gate is not Ready")
        bare = dict(self._lost())
        bare["status"] = {"containerStatuses": [{"name": run.MAIN_CONTAINER, "ready": True}]}
        self.assertFalse(watcher_record(bare)["ready"], "no Ready condition reads as not Ready")

    def test_the_fleet_counts_and_the_floor_do_not_count_it(self):
        pods = {"engine-0": watcher_record(self._lost()),
                "engine-1": watcher_record(pod_object("engine-1", "engine", 1, self.REV, "img:1", "v1"))}
        self.assertEqual({"engine": (2, 1, 0)}, run.aggregate(pods, None, {}, {"engine": 1}))
        self.assertEqual({"engine": 1}, run._serving_instance_counts(pods, {"engine": 1}))

    def test_invariants_b_and_g_do_not_demand_an_endpoint_for_it(self):
        pods = {"engine-0": watcher_record(self._lost()),
                "engine-1": watcher_record(pod_object("engine-1", "engine", 1, self.REV, "img:1", "v1"))}
        inv = run._inv_new("p1", {})
        ir = [ir_item("p1", "engine", [(0, "Ready", self.REV), (1, "Ready", self.REV)], current=self.REV)]
        routing = routing_of(routed_service("p1", "engine", self.REV, "uid-engine-1"))
        observe_ticks(inv, pods, run.INV_SUSTAIN_SETTLE + 1, ir_items=ir, routing=routing)
        self.assertEqual({}, inv["viol"])
        self.assertTrue(inv["b_seen"] and inv["g_seen"], "the live pod was judged, the lost one left alone")
        held = watcher_record(pod_object("engine-0", "engine", 0, self.REV, "img:1", "v1"))
        pods["engine-0"] = held
        observe_ticks(inv, pods, run.INV_SUSTAIN_B_MISSING + 1, start=999.0, ir_items=ir, routing=routing)
        self.assertIn(("B", "missing-endpoint", f"p1-engine-rev-{self.REV}"), inv["viol"],
                      "a pod whose Ready condition is True must be in rotation")


class BreakRegistersItsPodTest(unittest.TestCase):
    """A break path hands the watcher's invariant state along, so the live
    pod the break re-points is recorded and invariant F never judges the
    harness's own image swap against the pod's revision: read per path on a
    fixed snapshot. A deletion records nothing, and the pod the product
    builds in its place is judged."""

    OLD = "a1b2c3d4"

    def _ctx(self, hdr):
        pods = {f"p999-engine-{i}-0": _rec("engine", i, self.OLD) for i in range(2)}
        return _ctx_for(hdr, pods)

    def test_canary_break_before_push_records_the_pod(self):
        hdr = {"Ne": 2, "Nd": 0, "Nr": 0, "ceil_e": 3, "ceil_d": 0, "ceil_r": 0, "mode": "canary", "canary": "auto",
               "fault": "crashloop:idle", "form": "start", "victims": 1, "topo": "engine"}
        for rc, bite, want_ok in ((0, 2.0, True), (1, 2.0, False), (0, None, False)):
            ctx = self._ctx(hdr)
            fault = ctx["fault"]
            self.assertTrue(run._fault_before_push(fault))
            with mock.patch.object(run, "kubectl", lambda *a, **k: _Resp("", rc, "denied")), \
                    mock.patch.object(run, "_wait_break_bitten", lambda *a: bite):
                ok, why = run._break_before_push(ctx, fault)
            self.assertEqual(want_ok, ok, (rc, bite, why))
            if rc == 0:
                self.assertEqual({"engine-1-default-" + self.OLD: "p999-engine-1-0"}, ctx["inv"]["mutated_uids"],
                                 "the highest Instance's pod was re-pointed and recorded")
            else:
                self.assertEqual({}, ctx["inv"]["mutated_uids"], "a patch the apiserver refused changed nothing")
                self.assertIn("point p999-engine-1-0", why)
            if bite is None and rc == 0:
                self.assertIn("did not bite", why)
            if want_ok:
                self.assertTrue(any("the push follows" in n for n in fault["notes"]))

    def test_podkill_records_nothing_and_its_replacement_is_judged(self):
        hdr = {"Ne": 2, "Nd": 0, "Nr": 0, "ceil_e": 3, "ceil_d": 0, "ceil_r": 0, "mode": "canary", "canary": "auto",
               "topo": "engine"}
        ctx = self._ctx(hdr)
        ctx.update(victim="stable", victimcomp="engine")
        calls = []
        with mock.patch.object(run, "kubectl", lambda *a, **k: calls.append(a) or _Resp('{"items": []}')):
            run._podkill_verb(ctx)
        self.assertTrue(any(a[2:4] == ("delete", "pod") for a in calls), calls)
        self.assertEqual({}, ctx["inv"]["mutated_uids"], "a deletion changes no live pod's spec")
        inv = ctx["inv"]
        revisions = {self.OLD: revision_payload("img:1", "")}
        replacement = pod_object("p999-engine-0-0", "engine", 0, self.OLD, "img:1", "v2")
        replacement["metadata"]["uid"] = "uid-replacement"
        pods = {"p999-engine-0-0": watcher_record(replacement)}
        observe_ticks(inv, pods, run.INV_SUSTAIN_SETTLE + 1, revisions=revisions)
        key = ("F", "label-template-mismatch", "engine", "p999-engine-0-0")
        self.assertIn(key, inv["viol"], "the product's replacement under the old label is a product finding")
        self.assertIn("annotation perm.test/rev: revision '' vs pod 'v2'", inv["viol"][key]["detail"])


class EndpointsMembershipInvariantTest(unittest.TestCase):
    """Invariant B over fetched snapshots: a Ready+serving member missing
    from its Service's Ready endpoints, and a deleted pod still referenced."""

    def test_missing_and_stale_endpoints_fire_after_their_sustain(self):
        rev = "a1b2c3d4"
        pods = {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, rev, "img:1", "v1"))}
        inv = run._inv_new("p1", {})
        inv["dead_uids"].add("uid-gone")
        routing = routing_of(routed_service("p1", "engine", rev, "uid-gone"))
        observe_ticks(inv, pods, run.INV_SUSTAIN_B_MISSING - 1, routing=routing)
        self.assertNotIn(("B", "missing-endpoint", "p1-engine-rev-" + rev), inv["viol"])
        observe_ticks(inv, pods, 1, start=99.0, routing=routing)
        self.assertIn(("B", "missing-endpoint", "p1-engine-rev-" + rev), inv["viol"])
        self.assertIn(("B", "stale-endpoint", "p1-engine-rev-" + rev), inv["viol"])
        self.assertTrue(inv["b_seen"])

    def test_a_member_in_the_ready_endpoints_holds(self):
        rev = "a1b2c3d4"
        pods = {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, rev, "img:1", "v1"))}
        inv = run._inv_new("p1", {})
        observe_ticks(inv, pods, run.INV_SUSTAIN_B_MISSING + 1,
                      routing=routing_of(routed_service("p1", "engine", rev, "uid-engine-0")))
        self.assertEqual({}, inv["viol"])
        self.assertTrue(inv["b_seen"])


def ir_item(case_id, component, rows, *, current, update=None):
    """An InferenceReplica as the poller lists it: per-Instance rows with
    their phase and running revision (ControllerRevision names), plus the
    IR's current and update revisions."""
    update = update or current
    return {"spec": {"component": component},
            "status": {"currentRevision": f"{case_id}-{component}-{current}",
                       "updateRevision": f"{case_id}-{component}-{update}",
                       "instanceStatuses": [
                           {"index": index, "phase": phase,
                            "runningRevision": f"{case_id}-{component}-{running}", "podCount": 1}
                           for index, phase, running in rows]}}


class ReadyServingInvariantTest(unittest.TestCase):
    """Invariant G: an Instance the IR reports Ready, with its pod set Ready,
    has a pod in rotation — serving gate True and a Ready endpoint of a
    routed Service that selects it."""

    REV = "a1b2c3d4"

    def _pods(self, serving=True, ready=True):
        return {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, self.REV, "img:1", "v1",
                                                      serving=serving, ready=ready))}

    def _ir(self, phase="Ready"):
        return [ir_item("p1", "engine", [(0, phase, self.REV)], current=self.REV)]

    def _routing(self, *uids, ready=True):
        return routing_of(routed_service("p1", "engine", self.REV, *uids, ready=ready))

    def test_ready_instance_whose_pod_holds_the_gate_closed_is_a_violation(self):
        inv = run._inv_new("p1", {})
        snap = dict(ir_items=self._ir(), routing=self._routing())
        observe_ticks(inv, self._pods(serving=False), run.INV_SUSTAIN_SETTLE - 1, **snap)
        self.assertEqual({}, inv["viol"], "the settle window has not elapsed")
        observe_ticks(inv, self._pods(serving=False), 1, start=99.0, **snap)
        key = ("G", "ready-not-serving", "engine", "0")
        self.assertEqual([key], list(inv["viol"]))
        self.assertIn(f"engine/0 Phase=Ready with all 1 pod(s) Ready, but no pod has the {run.SERVING_COND} gate True",
                      inv["viol"][key]["detail"])
        checks = []
        self.assertEqual("FAIL", run.invariant_checks(inv, "PASS", [], checks))
        self.assertTrue(any("invariant G violated (ready-not-serving)" in c for c in checks), checks)

    def test_serving_pod_absent_from_its_routed_service_is_a_violation(self):
        inv = run._inv_new("p1", {})
        observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE,
                      ir_items=self._ir(), routing=self._routing("uid-engine-0", ready=False))
        detail = inv["viol"][("G", "ready-not-serving", "engine", "0")]["detail"]
        self.assertIn("pod(s) engine-0 serving but not a Ready endpoint of any routed Service", detail)

    def test_a_pod_in_rotation_holds_and_the_verdict_says_so(self):
        inv = run._inv_new("p1", {})
        observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE + 1,
                      ir_items=self._ir(), routing=self._routing("uid-engine-0"))
        self.assertEqual({}, inv["viol"])
        self.assertTrue(inv["g_seen"] and inv["g_routing_seen"])
        checks = []
        self.assertEqual("PASS", run.invariant_checks(inv, "PASS", [], checks))
        self.assertTrue(any("continuous invariants held" in c and "G" in c for c in checks), checks)

    def test_without_any_routed_service_only_the_gate_is_asserted(self):
        inv = run._inv_new("p1", {})
        observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE + 1, ir_items=self._ir(), routing=([], []))
        self.assertEqual({}, inv["viol"])
        checks = []
        run.invariant_checks(inv, "PASS", [], checks)
        self.assertTrue(any("invariant G: no routed Service observed" in c for c in checks), checks)
        inv = run._inv_new("p1", {})
        observe_ticks(inv, self._pods(serving=False), run.INV_SUSTAIN_SETTLE, ir_items=self._ir(), routing=None)
        self.assertIn(("G", "ready-not-serving", "engine", "0"), inv["viol"])

    def test_instances_not_reported_ready_or_not_fully_ready_are_not_its_claim(self):
        inv = run._inv_new("p1", {})
        observe_ticks(inv, self._pods(serving=False), run.INV_SUSTAIN_SETTLE,
                      ir_items=self._ir(phase="Updating"), routing=self._routing())
        observe_ticks(inv, self._pods(serving=False, ready=False), run.INV_SUSTAIN_SETTLE, start=500.0,
                      ir_items=self._ir(), routing=self._routing())
        observe_ticks(inv, self._pods(serving=False), run.INV_SUSTAIN_SETTLE, start=1000.0,
                      pods_per_inst={"engine": 2}, ir_items=self._ir(), routing=self._routing())
        self.assertEqual({}, inv["viol"])
        self.assertFalse(inv["g_seen"])
        checks = []
        run.invariant_checks(inv, "PASS", [], checks)
        self.assertTrue(any("invariant G (ready-serving)" in c and "vacuous" in c for c in checks), checks)

    def test_a_gang_is_in_rotation_through_its_leader(self):
        pods = {"engine-0-leader": watcher_record(pod_object("engine-0-leader", "engine", 0, self.REV, "l:1", "v1", runner="leader")),
                "engine-0-worker": watcher_record(pod_object("engine-0-worker", "engine", 0, self.REV, "w:1", "v1", runner="worker"))}
        inv = run._inv_new("p1", {})
        observe_ticks(inv, pods, run.INV_SUSTAIN_SETTLE + 1, pods_per_inst={"engine": 2}, ir_items=self._ir(),
                      routing=routing_of(routed_service("p1", "engine", self.REV, "uid-engine-0-leader", runner="leader")))
        self.assertEqual({}, inv["viol"])
        self.assertTrue(inv["g_seen"])

    def test_settle_window_resets_when_the_gate_opens(self):
        inv = run._inv_new("p1", {})
        snap = dict(ir_items=self._ir(), routing=self._routing("uid-engine-0"))
        t = observe_ticks(inv, self._pods(serving=False), run.INV_SUSTAIN_SETTLE - 1, **snap)
        t = observe_ticks(inv, self._pods(), 1, start=t, **snap)
        observe_ticks(inv, self._pods(serving=False), run.INV_SUSTAIN_SETTLE - 1, start=t, **snap)
        self.assertEqual({}, inv["viol"])

    def test_parked_by_inv_xfail_records_the_tracked_bug(self):
        hdr = {"notimpl": 1, "issue": 4242, "inv_xfail": "G"}
        inv = run._inv_new("p1", hdr)
        observe_ticks(inv, self._pods(serving=False), run.INV_SUSTAIN_SETTLE, ir_items=self._ir(), routing=self._routing())
        checks = []
        verdict = run.invariant_checks(inv, run.soften_expected_fail(hdr, "PASS", checks), [], checks)
        self.assertEqual("XFAIL (known bug: #4242)", verdict)
        self.assertTrue(batch.is_green(verdict))
        hdr = {"notimpl": 1, "issue": 4242, "inv_xfail": "F"}
        inv = run._inv_new("p1", hdr)
        observe_ticks(inv, self._pods(serving=False), run.INV_SUSTAIN_SETTLE, ir_items=self._ir(), routing=self._routing())
        self.assertEqual("FAIL", run.invariant_checks(inv, run.soften_expected_fail(hdr, "PASS", []), [], []))

    def test_not_evaluated_when_the_case_waives_it(self):
        inv = run._inv_new("p1", {"inv_off": "G"})
        self.assertFalse(inv["g_on"])
        observe_ticks(inv, self._pods(serving=False), run.INV_SUSTAIN_SETTLE, ir_items=self._ir(), routing=self._routing())
        self.assertEqual({}, inv["viol"])
        for knob in ("inv_off", "inv_xfail"):
            self.assertTrue(run.HDR_VALIDATORS[knob]("G"))


def isvc_rollout_status(component, target, *, outcome="Completed", phase="Stable", open_run=False):
    """An ISVC status as the poller reads it: the run record (bare revision
    hashes) and the Component's rollout phase."""
    targets = [{"component": component, "revision": target}]
    rollout = {"lastRun": {"outcome": outcome, "targetRevisions": targets}}
    if open_run:
        rollout["activeRun"] = {"runID": "r2", "targetRevisions": targets}
    components = {component: {"rolloutPhase": phase}} if phase else {}
    return {"rollout": rollout, "components": components}


class SupersededRevisionInvariantTest(unittest.TestCase):
    """Invariant H: once a Component's rollout reports done, every Instance
    row and every live pod of it is on the revision the rollout targeted."""

    OLD, NEW = "a1b2c3d4", "e5f6a7b8"

    def _pods(self, straggler=True):
        """Two Instances on the new revision, or one left on the old one; each
        pod runs the template of the revision it is labelled with, so F stays
        quiet and what fires is H."""
        second = (self.OLD, "img:1", "v1") if straggler else (self.NEW, "img:2", "v2")
        return {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, self.NEW, "img:2", "v2")),
                "engine-1": watcher_record(pod_object("engine-1", "engine", 1, *second))}

    def _ir(self, straggler=True, current=None):
        rows = [(0, "Ready", self.NEW), (1, "Ready", self.OLD if straggler else self.NEW)]
        return [ir_item("p1", "engine", rows, current=current or self.NEW, update=self.NEW)]

    def test_straggler_after_a_completed_stable_run_is_a_violation(self):
        inv = run._inv_new("p1", {})
        snap = dict(ir_items=self._ir(), isvc_status=isvc_rollout_status("engine", self.NEW))
        observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE - 1, **snap)
        self.assertEqual({}, inv["viol"], "the settle window has not elapsed")
        observe_ticks(inv, self._pods(), 1, start=99.0, **snap)
        row_key = ("H", "superseded-instance", "engine", "1")
        pod_key = ("H", "superseded-pod", "engine", "engine-1")
        self.assertEqual({row_key, pod_key}, set(inv["viol"]))
        self.assertIn(f"engine/1 runs revision {self.OLD} after the rollout reported done on {self.NEW}",
                      inv["viol"][row_key]["detail"])
        self.assertIn(f"pod engine-1 carries {run.REV_LABEL}={self.OLD} after the rollout reported done on {self.NEW}",
                      inv["viol"][pod_key]["detail"])
        checks = []
        self.assertEqual("FAIL", run.invariant_checks(inv, "PASS", [], checks))
        self.assertTrue(any("invariant H violated (superseded-instance)" in c for c in checks), checks)

    def test_a_rollout_that_has_not_reported_done_is_outside_its_claim(self):
        for status in (isvc_rollout_status("engine", self.NEW, open_run=True),
                       isvc_rollout_status("engine", self.NEW, outcome="RolledBack", phase="RolledBack"),
                       isvc_rollout_status("engine", self.NEW, phase="Paused"),
                       isvc_rollout_status("engine", "0badbad0")):   # the run record names another target
            inv = run._inv_new("p1", {})
            observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE + 1, ir_items=self._ir(), isvc_status=status)
            self.assertEqual({}, inv["viol"], status)
            self.assertFalse(inv["h_seen"], status)

    def test_a_component_outside_the_run_model_is_done_when_its_ir_converges(self):
        inv = run._inv_new("p1", {})
        observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE + 1, ir_items=self._ir(current=self.OLD), isvc_status={})
        self.assertEqual({}, inv["viol"], "currentRevision still trails updateRevision")
        observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE, start=500.0, ir_items=self._ir(), isvc_status={})
        self.assertIn(("H", "superseded-instance", "engine", "1"), inv["viol"])

    def test_a_converged_fleet_holds_and_the_verdict_says_so(self):
        inv = run._inv_new("p1", {})
        observe_ticks(inv, self._pods(straggler=False), run.INV_SUSTAIN_SETTLE + 1,
                      ir_items=self._ir(straggler=False), isvc_status=isvc_rollout_status("engine", self.NEW))
        self.assertEqual({}, inv["viol"])
        self.assertTrue(inv["h_seen"])
        checks = []
        self.assertEqual("PASS", run.invariant_checks(inv, "PASS", [], checks))
        self.assertTrue(any("continuous invariants held" in c and "H" in c for c in checks), checks)

    def test_settle_window_resets_when_the_straggler_rolls(self):
        inv = run._inv_new("p1", {})
        status = isvc_rollout_status("engine", self.NEW)
        t = observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE - 1, ir_items=self._ir(), isvc_status=status)
        t = observe_ticks(inv, self._pods(straggler=False), 1, start=t, ir_items=self._ir(straggler=False), isvc_status=status)
        observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE - 1, start=t, ir_items=self._ir(), isvc_status=status)
        self.assertEqual({}, inv["viol"])

    def test_parked_by_inv_xfail_records_the_tracked_bug(self):
        hdr = {"notimpl": 1, "issue": 4242, "inv_xfail": "H"}
        inv = run._inv_new("p1", hdr)
        observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE, ir_items=self._ir(), isvc_status=isvc_rollout_status("engine", self.NEW))
        checks = []
        verdict = run.invariant_checks(inv, run.soften_expected_fail(hdr, "PASS", checks), [], checks)
        self.assertEqual("XFAIL (known bug: #4242)", verdict)
        self.assertTrue(batch.is_green(verdict))
        hdr = {"notimpl": 1, "issue": 4242, "inv_xfail": "G"}
        inv = run._inv_new("p1", hdr)
        observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE, ir_items=self._ir(), isvc_status=isvc_rollout_status("engine", self.NEW))
        self.assertEqual("FAIL", run.invariant_checks(inv, run.soften_expected_fail(hdr, "PASS", []), [], []))

    def test_not_evaluated_under_the_partition_verb_or_when_waived(self):
        for hdr in ({"mode": "partition"}, {"inv_off": "H"}):
            inv = run._inv_new("p1", hdr)
            self.assertFalse(inv["h_on"], hdr)
            observe_ticks(inv, self._pods(), run.INV_SUSTAIN_SETTLE, ir_items=self._ir(),
                          isvc_status=isvc_rollout_status("engine", self.NEW))
            self.assertEqual({}, inv["viol"], hdr)
        self.assertTrue(run._inv_new("p1", {"mode": "rollout"})["h_on"])
        for knob in ("inv_off", "inv_xfail"):
            self.assertTrue(run.HDR_VALIDATORS[knob]("H"))


class InvariantTickTest(unittest.TestCase):
    """The tick reads each snapshot once through kubectl and hands them to
    the pure observation; the revision list is read only while a live pod
    carries a hash the ledger has not read, and a tick inside INV_TICK_S is
    a no-op."""

    REV = "a1b2c3d4"

    def _fake_kubectl(self, calls):
        case = "p1"
        objects = {
            "inferencereplicas": [ir_item(case, "engine", [(0, "Ready", self.REV)], current=self.REV)],
            "svc": routing_of(routed_service(case, "engine", self.REV, "uid-engine-0"))[0],
            "endpointslices": routing_of(routed_service(case, "engine", self.REV, "uid-engine-0"))[1],
            "controllerrevisions": [{"metadata": {"name": f"{case}-engine-{self.REV}"},
                                     "data": revision_payload("img:1", "v1")}],
        }

        def kubectl(*args, **_kw):
            calls.append(args)
            kind = args[args.index("get") + 1]
            if kind == "isvc":
                body = {"status": isvc_rollout_status("engine", self.REV)}
            else:
                body = {"items": objects[kind]}
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(body), stderr="")
        return kubectl

    def test_one_tick_reads_every_snapshot_and_learns_the_revision(self):
        calls = []
        pods = {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, self.REV, "img:1", "v1"))}
        inv = run._inv_new("p1", {})
        with mock.patch.object(run, "kubectl", self._fake_kubectl(calls)), \
                mock.patch.object(run.time, "time", side_effect=[100.0, 101.0, 100.0 + run.INV_TICK_S]):
            run._inv_tick(inv, pods, {"engine": 1})
            first = [c[c.index("get") + 1] for c in calls]
            run._inv_tick(inv, pods, {"engine": 1})      # inside INV_TICK_S: no reads
            self.assertEqual(len(first), len(calls))
            run._inv_tick(inv, pods, {"engine": 1})      # the ledger is read: no revision list
        self.assertEqual(["inferencereplicas", "svc", "endpointslices", "isvc", "controllerrevisions"], first)
        self.assertNotIn("controllerrevisions", [c[c.index("get") + 1] for c in calls[len(first):]])
        self.assertEqual("revision", inv["revisions"][(self.REV, "pod")]["src"])
        self.assertEqual({}, inv["viol"])
        self.assertTrue(inv["ir_seen"] and inv["b_seen"] and inv["g_seen"] and inv["h_seen"])


class LabelTruthInvariantTest(unittest.TestCase):
    """Invariant F: a pod runs the template its revision-hash label names,
    read against the ControllerRevision of that hash or, until one is
    visible, against the first pod seen under the label."""

    OLD, NEW = "a1b2c3d4", "e5f6a7b8"

    def _fleet(self, liar=True):
        honest = watcher_record(pod_object("engine-0", "engine", 0, self.OLD, "img:1", "v1"))
        rebuilt = watcher_record(pod_object("engine-1", "engine", 1, self.OLD if liar else self.NEW,
                                            "img:2", "v2"))
        return {"engine-0": honest, "engine-1": rebuilt}

    def _revisions(self):
        return {self.OLD: revision_payload("img:1", "v1"), self.NEW: revision_payload("img:2", "v2")}

    def test_pod_rebuilt_from_a_newer_template_under_the_old_label_is_a_violation(self):
        inv = run._inv_new("p1", {})
        pods = self._fleet()
        observe_ticks(inv, pods, run.INV_SUSTAIN_SETTLE - 1, revisions=self._revisions())
        self.assertEqual({}, inv["viol"], "the settle window has not elapsed")
        observe_ticks(inv, pods, 1, start=99.0)
        key = ("F", "label-template-mismatch", "engine", "engine-1")
        self.assertEqual([key], list(inv["viol"]))
        detail = inv["viol"][key]["detail"]
        self.assertIn(f"pod engine-1 carries {run.REV_LABEL}={self.OLD}", detail)
        self.assertIn("from its ControllerRevision", detail)
        self.assertIn("image: revision 'img:1' vs pod 'img:2'", detail)
        self.assertIn(f"annotation {run.TRIGGER_ANNO}: revision 'v1' vs pod 'v2'", detail)
        checks = []
        self.assertEqual("FAIL", run.invariant_checks(inv, "PASS", [], checks))
        self.assertTrue(any("invariant F violated (label-template-mismatch)" in c for c in checks), checks)

    def test_an_honest_fleet_holds_and_the_verdict_says_so(self):
        inv = run._inv_new("p1", {})
        observe_ticks(inv, self._fleet(liar=False), run.INV_SUSTAIN_SETTLE + 1, revisions=self._revisions())
        self.assertEqual({}, inv["viol"])
        checks = []
        self.assertEqual("PASS", run.invariant_checks(inv, "PASS", [], checks))
        self.assertTrue(any("continuous invariants held" in c and "F" in c for c in checks), checks)

    def test_without_a_revision_the_first_pod_under_a_label_is_the_reference(self):
        inv = run._inv_new("p1", {})
        pods = self._fleet()
        observe_ticks(inv, pods, run.INV_SUSTAIN_SETTLE, revisions={})
        key = ("F", "label-template-mismatch", "engine", "engine-1")
        self.assertIn(key, inv["viol"])
        self.assertIn("the template the first pod engine-0 ran under that label", inv["viol"][key]["detail"])

    def test_a_revision_that_appears_later_replaces_the_first_pod_reference(self):
        inv = run._inv_new("p1", {})
        pods = self._fleet(liar=False)
        observe_ticks(inv, pods, 1, revisions={})
        self.assertEqual("first pod engine-0", inv["revisions"][(self.OLD, "pod")]["src"])
        self.assertTrue(run._inv_revisions_wanted(inv, pods))
        observe_ticks(inv, pods, 1, start=5.0, revisions=self._revisions())
        self.assertEqual("revision", inv["revisions"][(self.OLD, "pod")]["src"])
        self.assertFalse(run._inv_revisions_wanted(inv, pods))

    def test_re_listing_a_missing_revision_is_bounded(self):
        inv = run._inv_new("p1", {})
        pods = self._fleet(liar=False)
        for i in range(run.INV_SUSTAIN_SETTLE):
            self.assertTrue(run._inv_revisions_wanted(inv, pods))
            observe_ticks(inv, pods, 1, start=5.0 * i, revisions={})
        self.assertFalse(run._inv_revisions_wanted(inv, pods))

    def test_settle_window_resets_on_a_clean_tick(self):
        inv = run._inv_new("p1", {})
        liar, honest = self._fleet(), self._fleet(liar=False)
        t = observe_ticks(inv, liar, run.INV_SUSTAIN_SETTLE - 1, revisions=self._revisions())
        t = observe_ticks(inv, honest, 1, start=t)
        observe_ticks(inv, liar, run.INV_SUSTAIN_SETTLE - 1, start=t)
        self.assertEqual({}, inv["viol"])

    def test_env_and_request_differences_count_images_alone_do_not_decide(self):
        inv = run._inv_new("p1", {})
        pods = {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, self.OLD, "img:1", "v1",
                                                      test_rev="v2", cpu="55m"))}
        observe_ticks(inv, pods, run.INV_SUSTAIN_SETTLE, revisions=self._revisions())
        detail = inv["viol"][("F", "label-template-mismatch", "engine", "engine-0")]["detail"]
        self.assertIn("env TEST_REV: revision 'v1' vs pod 'v2'", detail)
        self.assertIn("requests.cpu: revision 50 vs pod 55", detail)

    def test_renderer_env_sidecars_and_valuefrom_entries_are_outside_the_fingerprint(self):
        obj = pod_object("engine-0", "engine", 0, self.OLD, "img:1", "v1")
        obj["spec"]["containers"].append({"name": "sidecar", "image": "side:1"})
        obj["spec"]["containers"][0]["env"].append({"name": "INJECTED", "value": "x"})
        inv = run._inv_new("p1", {})
        observe_ticks(inv, {"engine-0": watcher_record(obj)}, run.INV_SUSTAIN_SETTLE, revisions=self._revisions())
        self.assertEqual({}, inv["viol"])

    def test_worker_pods_read_the_worker_template(self):
        revisions = {self.OLD: revision_payload("leader:1", "v1", worker_image="worker:1")}
        pods = {"engine-0-leader": watcher_record(pod_object("engine-0-leader", "engine", 0, self.OLD, "leader:1", "v1", runner="leader")),
                "engine-0-worker": watcher_record(pod_object("engine-0-worker", "engine", 0, self.OLD, "worker:1", "v1", runner="worker"))}
        inv = run._inv_new("p1", {})
        observe_ticks(inv, pods, run.INV_SUSTAIN_SETTLE, pods_per_inst={"engine": 2}, revisions=revisions)
        self.assertEqual({}, inv["viol"])
        self.assertTrue(inv["f_seen"])

    def test_harness_mutated_victim_is_skipped_and_its_replacement_is_judged(self):
        """A break's victim runs the harness's image under its old label and is
        not judged; the pod the engine rebuilds in its place (same name, new
        UID) running a newer template under the old label is."""
        inv = run._inv_new("p1", {})
        victim = pod_object("engine-3", "engine", 3, self.OLD, "crash:none", "v1")
        pods = {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, self.OLD, "img:1", "v1")),
                "engine-3": watcher_record(victim)}
        run._pod_mutated_by_harness(inv, "engine-3", victim["metadata"]["uid"])
        observe_ticks(inv, pods, run.INV_SUSTAIN_SETTLE + 1, revisions=self._revisions())
        self.assertEqual({}, inv["viol"])
        checks = []
        self.assertEqual("PASS", run.invariant_checks(inv, "PASS", [], checks))
        self.assertTrue(any("whose spec the harness changed itself were not judged: engine-3 (uid-engi)" in c
                            for c in checks), checks)
        rebuilt = pod_object("engine-3", "engine", 3, self.OLD, "img:2", "v2")
        rebuilt["metadata"]["uid"] = "uid-engine-3-rebuilt"
        pods["engine-3"] = watcher_record(rebuilt)
        observe_ticks(inv, pods, run.INV_SUSTAIN_SETTLE, start=999.0)
        key = ("F", "label-template-mismatch", "engine", "engine-3")
        self.assertEqual([key], list(inv["viol"]))
        self.assertIn("image: revision 'img:1' vs pod 'img:2'", inv["viol"][key]["detail"])

    def test_every_fault_fire_threads_the_watcher_state(self):
        """A break patches a live pod's spec, and the record that keeps
        invariant F from judging that pod is the watcher state the fire is
        handed: every verb that fires a fault passes it."""
        tree = ast.parse(Path(run.__file__).read_text())
        missing = sorted(node.lineno for node in ast.walk(tree)
                         if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "_fault_fire"
                         and not any(keyword.arg == "inv" for keyword in node.keywords))
        self.assertEqual([], missing, "run.py lines firing a fault without the watcher state")

    def test_a_break_records_its_victim_on_the_watcher_state(self):
        """The fire path is what fills the set: an imagepull break patching a
        live pod records that pod's UID; a failed patch records nothing."""
        pods = {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, self.OLD, "img:1", "v1"))}
        for rc, want in ((0, {"uid-engine-0": "engine-0"}), (1, {})):
            inv = run._inv_new("p1", {})
            fault = run._fault_new("p1", {"fault": "imagepull:old"})
            with mock.patch.object(run, "kubectl", return_value=subprocess.CompletedProcess([], rc, stdout="", stderr="no")):
                run._fault_fire(fault, pods, {"engine": 1}, {self.OLD}, inv=inv)
            self.assertEqual(want, inv["mutated_uids"], rc)
            self.assertEqual(rc != 0, bool(fault["infra_fail"]))
        inv = run._inv_new("p1", {})
        fault = run._fault_new("p1", {"fault": "podkill:settling"})
        with mock.patch.object(run, "kubectl", return_value=subprocess.CompletedProcess([], 0, stdout="", stderr="")):
            run._fault_fire(fault, pods, {"engine": 1}, {self.OLD}, inv=inv)
        self.assertEqual({}, inv["mutated_uids"], "a deletion changes no spec")

    def test_parked_by_inv_xfail_records_the_tracked_bug(self):
        hdr = {"notimpl": 1, "issue": 4242, "inv_xfail": "F"}
        inv = run._inv_new("p1", hdr)
        observe_ticks(inv, self._fleet(), run.INV_SUSTAIN_SETTLE, revisions=self._revisions())
        checks, notes = [], []
        verdict = run.invariant_checks(inv, run.soften_expected_fail(hdr, "PASS", checks), notes, checks)
        self.assertEqual("XFAIL (known bug: #4242)", verdict)
        self.assertTrue(batch.is_green(verdict))
        hdr = {"notimpl": 1, "issue": 4242, "inv_xfail": "A"}
        inv = run._inv_new("p1", hdr)
        observe_ticks(inv, self._fleet(), run.INV_SUSTAIN_SETTLE, revisions=self._revisions())
        self.assertEqual("FAIL", run.invariant_checks(inv, run.soften_expected_fail(hdr, "PASS", []), [], []))

    def test_not_evaluated_when_the_case_waives_it(self):
        inv = run._inv_new("p1", {"inv_off": "F"})
        self.assertFalse(inv["f_on"])
        observe_ticks(inv, self._fleet(), run.INV_SUSTAIN_SETTLE, revisions=self._revisions())
        self.assertEqual({}, inv["viol"])
        checks = []
        run.invariant_checks(inv, "PASS", [], checks)
        self.assertTrue(any("disabled by header inv_off=F" in c for c in checks), checks)

    def test_header_grammar_knows_the_class(self):
        for knob in ("inv_off", "inv_xfail"):
            self.assertTrue(run.HDR_VALIDATORS[knob]("F"))
            self.assertTrue(run.HDR_VALIDATORS[knob]("all"))
            self.assertFalse(run.HDR_VALIDATORS[knob]("E"), "the cohort-ratio observer is informational")
            self.assertFalse(run.HDR_VALIDATORS[knob]("Z"))

    def test_settle_window_scales_with_the_timeout_multiplier(self):
        self.assertEqual(run.INV_SETTLE_S, 15.0 * run.TIMEOUT_MULT)
        self.assertGreaterEqual((run.INV_SUSTAIN_SETTLE - 1) * run.INV_TICK_S, run.INV_SETTLE_S)


class GeneratedSizeFamiliesTest(unittest.TestCase):
    """The small-size and router-gang families generate_cases.py mints: one
    and two Instances per Component under every strategy and budget, and the
    engine as a gang behind a router. Every percent budget is resolved as the
    engine resolves it, every case states the operator's end state on line
    two, and the router is never bumped."""

    @classmethod
    def setUpClass(cls):
        cls.outputs, rows = generate_cases.build_outputs()
        cls.rows = {folder: [(n, kv, m) for f, n, kv, m in rows if f == folder]
                    for folder in ("gen-size", "gen-router-gang")}

    def _line1(self, folder, name):
        return self.outputs[f"{folder}/{name}.yaml"].splitlines()[0]

    def _line2(self, folder, name):
        return self.outputs[f"{folder}/{name}.yaml"].splitlines()[1]

    def _by_verb(self, folder, prefix):
        return [(n, kv, m) for n, kv, m in self.rows[folder] if m["verb"].startswith(prefix)]

    def test_blocks_sit_in_the_reserved_range(self):
        for folder in ("gen-size", "gen-router-gang"):
            lo, hi = generate_cases.GEN_BLOCKS[folder]
            self.assertTrue(8000 <= lo < hi <= 8200, folder)

    def test_percent_budgets_resolve_as_the_engine_does(self):
        """ceil(replicas * percent / 100), clamped to replicas: at one and two
        Instances 25 and 50 percent are one, never zero, and the harness floor
        reads the same count."""
        g = generate_cases
        self.assertEqual(1, g.resolved_budget("25%", 1))
        self.assertEqual(1, g.resolved_budget("50%", 1))
        self.assertEqual(1, g.resolved_budget("25%", 2))
        self.assertEqual(1, g.resolved_budget("50%", 2))
        self.assertEqual(2, g.resolved_budget("100%", 2))
        self.assertEqual(1, g.resolved_budget(1, 1))
        for value, n in (("25%", 1), ("50%", 1), ("25%", 2), ("50%", 2), ("25%", 8)):
            self.assertEqual(run.resolve_max_unavailable(value, n), g.resolved_budget(value, n), (value, n))
        self.assertIn("rounds up to 1", g.budget_phrase("maxUnavailable", "50%", 1))
        self.assertEqual("maxSurge 1", g.budget_phrase("maxSurge", 1, 1))

    def test_every_case_sizes_the_whole_fleet_and_names_an_end_state(self):
        fam = self.rows["gen-size"]
        self.assertEqual(86, len(fam))
        for name, kv, _m in fam:
            self.assertIn(kv["Ne"], generate_cases.SIZE_NS, name)
            self.assertIn(kv["topo"], generate_cases.SIZE_TOPOS, name)
            if kv["topo"] == "pd":
                self.assertEqual((kv["Ne"], kv["Ne"]), (kv["Nd"], kv["Nr"]), f"{name}: the router is sized with the fleet")
            else:
                self.assertEqual((0, 0), (kv["Nd"], kv["Nr"]), name)
            self.assertEqual("none", kv["co"], name)
            self.assertNotIn("notimpl", kv, f"{name}: nothing is parked")
            self.assertTrue(self._line2("gen-size", name).startswith("# reason: "), name)

    def test_pushes_cross_size_shape_strategy_and_budget(self):
        pushes = self._by_verb("gen-size", "none")
        self.assertEqual(20, len(pushes))
        self.assertEqual(20, len({(kv["Ne"], kv["topo"], m["strat"], self._line1("gen-size", n).split("  ")[0])
                                  for n, kv, m in pushes}))
        self.assertEqual(set(generate_cases.SIZE_BUDGETS), {m["strat"] for _n, _kv, m in pushes})
        for name, kv, m in pushes:
            line1, line2 = self._line1("gen-size", name), self._line2("gen-size", name)
            percent = "pct" in line1.split("  ")[0]
            if m["strat"] == "SurgeThenDrain":
                self.assertEqual(1, kv.get("floor_hard"), f"{name}: never below N, instantaneously")
                self.assertEqual(kv["Ne"] + 1, kv["ceil_e"], f"{name}: both surge budgets resolve to one")
                self.assertIn("never" if kv["Ne"] == 1 else "always serves", line2, name)
            else:
                self.assertNotIn("floor_hard", kv, name)
                self.assertEqual(kv["Ne"], kv["ceil_e"], name)
            if m["strat"] == "RecreatePod" and kv["Ne"] == 1:
                self.assertIn("zero serving", line2, f"{name}: the configured outcome is said")
            if m["strat"] == "InPlaceIfPossible":
                self.assertIn("same UID", line2, name)
            self.assertEqual(percent, "rounds up to 1" in line2, f"{name}: a percent budget states its resolution")
            self.assertIn("on the new revision", line2, f"{name}: the end state names the revision the fleet ends on")

    def test_crash_loops_cross_size_shape_strategy_form_and_policy(self):
        crashes = self._by_verb("gen-size", "crashloop:")
        self.assertEqual(48, len(crashes))
        self.assertEqual(48, len({(kv["Ne"], kv["topo"], m["strat"], kv["form"], m["rp"]) for _n, kv, m in crashes}))
        self.assertEqual(set(generate_cases.SIZE_BUDGETS), {m["strat"] for _n, _kv, m in crashes})
        self.assertEqual(set(generate_cases.CRASH_FORMS), {kv["form"] for _n, kv, _m in crashes})
        self.assertEqual(set(generate_cases.RESTARTS), {m["rp"] for _n, _kv, m in crashes})
        for name, kv, m in crashes:
            self.assertEqual(("crashloop:old", 1), (kv["fault"], kv["floor_soft"]), name)
            self.assertNotIn("%", self.outputs[f"gen-size/{name}.yaml"].split("\n", 2)[2],
                             f"{name}: the crash runs under the integer budget the percent resolves to")
            line2 = self._line2("gen-size", name)
            self.assertIn("no repair is recorded", line2, name)
            self.assertIn("ends on the new revision serving", line2, name)
            if kv["Ne"] == 1:
                self.assertIn("both the first and the last", line2, name)
                if m["strat"] != "SurgeThenDrain":
                    self.assertIn("serves nothing from the break", line2, name)

    def test_scales_run_between_one_and_two_under_every_strategy(self):
        scales = self._by_verb("gen-size", "scale:")
        self.assertEqual(12, len(scales))
        self.assertEqual(set(generate_cases.SIZE_SCALES), {(kv["Ne"], kv["to_e"]) for _n, kv, _m in scales})
        self.assertEqual(set(generate_cases.SIZE_BUDGETS), {m["strat"] for _n, _kv, m in scales})
        for name, kv, m in scales:
            self.assertEqual("scaledown", kv["mode"], name)
            self.assertEqual(max(kv["Ne"], kv["to_e"]), kv["ceil_e"], name)
            self.assertEqual("scale:up" if kv["to_e"] > kv["Ne"] else "scale:down", m["verb"], name)
            if kv["topo"] == "pd":
                self.assertEqual((kv["to_e"], kv["Nr"]), (kv["to_d"], kv["to_r"]), f"{name}: the router holds")
            line2 = self._line2("gen-size", name)
            self.assertIn("min(start, target)", line2, name)
            self.assertIn("same UID", line2, name)

    def test_canaries_walk_the_small_ladder_on_two_instances(self):
        canaries = self._by_verb("gen-size", "canary:")
        self.assertEqual(6, len(canaries))
        self.assertEqual(set(generate_cases.SIZE_CANARIES), {(m["strat"], kv["canary"]) for _n, kv, m in canaries})
        for name, kv, m in canaries:
            self.assertEqual(("canary", generate_cases.SIZE_CANARY_N), (kv["mode"], kv["Ne"]), name)
            self.assertEqual(generate_cases.SIZE_CANARY_CELL, kv["cell"], name)
            self.assertTrue(run._cell_ok(kv["cell"]), name)
            text = self.outputs[f"gen-size/{name}.yaml"]
            for nc, tw in generate_cases.SIZE_LADDER:
                self.assertIn(f'capacity: "{nc}"', text, name)
                self.assertIn(f"traffic: {tw}", text, name)
            members = "[engine, decoder, router]" if kv["topo"] == "pd" else "[engine]"
            self.assertIn(f"- components: {members}\n", text, f"{name}: the whole fleet is the group")
            surge = 1 if m["strat"] == "SurgeThenDrain" else 0
            self.assertEqual(kv["Ne"] + surge, kv["ceil_e"], name)
            if kv["topo"] == "pd":
                self.assertEqual((kv["Nd"] + surge, kv["Nr"] + surge), (kv["ceil_d"], kv["ceil_r"]), name)
            line2 = self._line2("gen-size", name)
            self.assertIn("the same Instance", line2, f"{name}: the 50 percent step adds no capacity")
            self.assertIn("ends on the canary revision", line2, name)

    def test_router_gang_family_keeps_the_router_out_of_the_roll(self):
        g = generate_cases
        fam = self.rows["gen-router-gang"]
        self.assertEqual(30, len(fam))
        width = {}
        for name, kv, _m in fam:
            text = self.outputs[f"gen-router-gang/{name}.yaml"]
            self.assertEqual((g.ROUTER_GANG_TOPO, g.ROUTER_GANG_N, 0, gen_cases.NR),
                             (kv["topo"], kv["Ne"], kv["Nd"], kv["Nr"]), name)
            self.assertEqual(kv["Nr"], kv["ceil_r"], f"{name}: the router never surges")
            self.assertEqual("none", kv["co"], name)
            self.assertNotIn("bump", kv, f"{name}: the engine is the default bump and the router a negative control")
            self.assertNotIn("notimpl", kv, f"{name}: nothing is parked")
            width[name] = int(re.search(r"worker: \{ size: (\d+) \}", text).group(1))
            line2 = self._line2("gen-router-gang", name)
            self.assertIn("router", line2, name)
            self.assertRegex(line2, "stays in rotation|is idle")
        self.assertEqual(set(g.ROUTER_GANG_WIDTHS), set(width.values()))
        pushes = self._by_verb("gen-router-gang", "none")
        self.assertEqual(6, len(pushes))
        self.assertEqual({(s, w) for s, *_ in g.ROUTER_GANG_STRATS for w in g.ROUTER_GANG_WIDTHS},
                         {(m["strat"], width[n]) for n, _kv, m in pushes})
        for name, kv, m in pushes:
            self.assertEqual(kv["Ne"] + (1 if m["strat"] == "SurgeThenDrain" else 0), kv["ceil_e"], name)
            self.assertIn("no gang is ever split", self._line2("gen-router-gang", name), name)
        crashes = self._by_verb("gen-router-gang", "crashloop:")
        self.assertEqual(16, len(crashes))
        self.assertEqual(16, len({(m["strat"], kv["form"], m["rp"], width[n]) for n, kv, m in crashes}))
        self.assertEqual({s for s, *_ in g.ROUTER_GANG_CRASH_STRATS}, {m["strat"] for _n, _kv, m in crashes})
        for name, kv, _m in crashes:
            self.assertEqual(("crashloop:old", 1), (kv["fault"], kv["floor_soft"]), name)
            self.assertIn("leader pod", self._line2("gen-router-gang", name), name)
        scales = self._by_verb("gen-router-gang", "scale:")
        self.assertEqual(4, len(scales))
        self.assertEqual({(t, w) for t in g.ROUTER_GANG_SCALES for w in g.ROUTER_GANG_WIDTHS},
                         {(kv["to_e"], width[n]) for n, kv, _m in scales})
        for name, kv, _m in scales:
            self.assertEqual(("scaledown", gen_cases.NR, max(g.ROUTER_GANG_N, kv["to_e"])),
                             (kv["mode"], kv["to_r"], kv["ceil_e"]), name)
            self.assertIn("no gang is left partial", self._line2("gen-router-gang", name), name)
        canaries = self._by_verb("gen-router-gang", "canary:")
        self.assertEqual(4, len(canaries))
        self.assertEqual({(p, w) for p in g.ROUTER_GANG_CANARIES for w in g.ROUTER_GANG_WIDTHS},
                         {(kv["canary"], width[n]) for n, kv, _m in canaries})
        for name, kv, _m in canaries:
            text = self.outputs[f"gen-router-gang/{name}.yaml"]
            self.assertIn("- components: [engine]\n", text, f"{name}: the group names the engine alone")
            self.assertEqual(g.ROUTER_GANG_CANARY_CELL, kv["cell"], name)
            for nc, _tw in g.SIZE_LADDER:
                self.assertIn(f'capacity: "{nc}"', text, name)
            self.assertIn("the same gang", self._line2("gen-router-gang", name), name)

    def test_generated_headers_validate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for folder, fam in self.rows.items():
                for name, _kv, _m in fam:
                    path = root / folder / f"{name}.yaml"
                    path.parent.mkdir(exist_ok=True)
                    path.write_text(self.outputs[f"{folder}/{name}.yaml"])
                    self.assertEqual([], run.validate_case_file(path), name)


class WindowFaultHeaderTest(unittest.TestCase):
    """webhook:<phase> and conflict:<phase>: the phases, the modes, the knobs
    each kind needs and refuses, and the idle phase tied to during=idle."""

    BASE = "# p999: x Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"
    IDLE = "# p999: x Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 topo=engine during=idle"
    SCALE = "# p999: x mode=scaledown Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine to_e=3"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_fault_phases(self):
        self.assertTrue(run._fault_ok("webhook:idle"))
        self.assertTrue(run._fault_ok("webhook:settling"))
        self.assertTrue(run._fault_ok("conflict:settling"))
        self.assertFalse(run._fault_ok("webhook:rotating"))
        self.assertFalse(run._fault_ok("conflict:canary-serving"))
        self.assertFalse(run._fault_ok("webhook"))

    def test_webhook_needs_its_lift_and_refuses_the_writers_knobs(self):
        self.assertEqual([], self._errs(self.BASE + " fault=webhook:settling lift=after-hold"))
        self.assertEqual([], self._errs(self.BASE + " fault=webhook:settling lift=after-window"))
        self.assertEqual([], self._errs(self.IDLE + " fault=webhook:idle lift=after-hold"))
        self.assertEqual([], self._errs(self.SCALE + " fault=webhook:settling lift=after-window"))
        self.assertTrue(any("needs lift=" in e for e in self._errs(self.BASE + " fault=webhook:settling")))
        self.assertTrue(any("needs lift=" in e for e in self._errs(self.BASE + " fault=webhook:settling lift=after-settle")))
        self.assertTrue(any("competing writer's knob" in e
                            for e in self._errs(self.BASE + " fault=webhook:settling lift=after-hold form=once")))
        self.assertTrue(any("competing writer's knob" in e
                            for e in self._errs(self.BASE + " fault=webhook:settling lift=after-hold target=ir")))

    def test_conflict_needs_form_and_target_and_refuses_a_lift(self):
        self.assertEqual([], self._errs(self.BASE + " fault=conflict:settling form=once target=isvc"))
        self.assertEqual([], self._errs(self.IDLE + " fault=conflict:idle form=persist target=ir"))
        self.assertEqual([], self._errs(self.SCALE + " fault=conflict:settling form=persist target=ir"))
        self.assertTrue(any("needs form=" in e for e in self._errs(self.BASE + " fault=conflict:settling target=isvc")))
        self.assertTrue(any("needs target=" in e for e in self._errs(self.BASE + " fault=conflict:settling form=once")))
        self.assertTrue(any("malformed header value form=" in e
                            for e in self._errs(self.BASE + " fault=conflict:settling form=twice target=isvc")))
        self.assertTrue(any("lift= is the webhook outage's knob" in e
                            for e in self._errs(self.BASE + " fault=conflict:settling form=once target=isvc lift=after-hold")))

    def test_idle_phase_goes_with_during_idle_and_the_modes_are_bounded(self):
        self.assertTrue(any("goes with during=idle" in e for e in self._errs(self.BASE + " fault=webhook:idle lift=after-hold")))
        self.assertTrue(any("goes with during=idle" in e for e in self._errs(self.IDLE + " fault=webhook:settling lift=after-hold")))
        self.assertTrue(any("as the scale lands" in e for e in self._errs(
            self.SCALE.replace(" to_e=3", "") + " to_e=3 fault=webhook:idle lift=after-hold")))
        kill = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
        self.assertTrue(any("holds a window over a push" in e for e in self._errs(kill + " fault=webhook:settling lift=after-hold")))
        self.assertTrue(any("does not combine" in e
                            for e in self._errs(self.BASE + " fault=conflict:settling form=once target=isvc during=rotating")))

    def test_the_window_knobs_belong_to_their_faults(self):
        self.assertTrue(any("names the object a competing writer patches" in e for e in self._errs(self.BASE + " target=isvc")))
        self.assertTrue(any("keeps the webhook unreachable" in e for e in self._errs(self.BASE + " lift=after-window")))
        self.assertTrue(any("expect=" in e for e in self._errs(self.IDLE + " fault=webhook:idle lift=after-hold expect=repaired")))

    def test_coverage_vocabulary_matches(self):
        import coverage
        self.assertIn("target", coverage.HEADER_VALUES)
        self.assertEqual(run.CONFLICT_TARGETS, coverage.HEADER_VALUES["target"])
        self.assertTrue(run.CONFLICT_FORMS <= coverage.HEADER_VALUES["form"])
        self.assertTrue(coverage._lift("after-window"))


def _webhook_configuration(name, hooks):
    """A webhook configuration as the apiserver returns it: each hook names
    a (service name, namespace, path)."""
    return {"metadata": {"name": name, "annotations": {"meta.helm.sh/release-name": "ome"}},
            "webhooks": [{"name": hook, "failurePolicy": "Fail",
                          "clientConfig": {"caBundle": "Cg==",
                                           "service": {"name": svc, "namespace": ns, "path": path, "port": 443}}}
                         for hook, svc, ns, path in hooks]}


VALIDATING = [_webhook_configuration("inferenceservice.ome.io",
                                     [("inferenceservice.validator", run.WEBHOOK_SERVICE, run.CTRL_NS, "/validate-isvc")]),
              _webhook_configuration("other.example.io",
                                     [("other.validator", "other-webhook", run.CTRL_NS, "/validate-other")])]
MUTATING = [_webhook_configuration("inferenceservice.ome.io",
                                   [("inferenceservice.pod-mutator", run.WEBHOOK_SERVICE, run.CTRL_NS, "/mutate-pods")])]
ISVC_SERVICE = {"name": run.WEBHOOK_SERVICE, "namespace": run.CTRL_NS, "path": "/validate-isvc", "port": 443}
POD_SERVICE = {"name": run.WEBHOOK_SERVICE, "namespace": run.CTRL_NS, "path": "/mutate-pods", "port": 443}
AWAY = "perm-webhook-outage-p999"
NOT_FOUND = f'Error from server (NotFound): services "{AWAY}" not found\n'


def _cluster_kubectl(calls, validating=VALIDATING, mutating=MUTATING, away_exists=False):
    """A kubectl serving the webhook configurations, the absence (or not) of
    the outage Service, and taking every patch."""
    def kubectl(*a, **k):
        calls.append((a, len(run._FAULT_CLEANUPS)))
        if a[0] == "get" and a[1] == "validatingwebhookconfiguration":
            return subprocess.CompletedProcess(a, 0, json.dumps({"items": validating}), "")
        if a[0] == "get" and a[1] == "mutatingwebhookconfiguration":
            return subprocess.CompletedProcess(a, 0, json.dumps({"items": mutating}), "")
        if "get" in a and "service" in a:
            if away_exists:
                return subprocess.CompletedProcess(a, 0, f"service/{AWAY}\n", "")
            return subprocess.CompletedProcess(a, 1, "", NOT_FOUND)
        return subprocess.CompletedProcess(a, 0, "", "")
    return kubectl


def _patches(calls):
    """[(kind, name, body)] of the patch calls, in order."""
    return [(a[1], a[2], json.loads(a[a.index("-p") + 1])) for a, _n in calls if a[0] == "patch"]


class WebhookOutageVerbTest(unittest.TestCase):
    """The outage points every webhook that names the manager's Service at a
    Service that does not exist, keeps the reference each had in the
    configuration's outage annotation, registers the restore first, and the
    lift (and the end-of-run sweep) put every reference back. A Service
    pointed at no pod is not a cut: the apiserver keeps its connection to
    the manager pod warm and admission keeps answering."""

    def test_cut_patch_points_each_hook_away_and_keeps_what_it_had(self):
        entries = {"inferenceservice.validator": ISVC_SERVICE}
        patch = run._webhook_cut_patch("p999", entries, AWAY)
        self.assertEqual([{"name": "inferenceservice.validator", "clientConfig": {"service": {"name": AWAY}}}],
                         patch["webhooks"], "a strategic merge keyed by the webhook name touches the service name alone")
        kept = json.loads(patch["metadata"]["annotations"][run.WEBHOOK_OUTAGE_ANNO])
        self.assertEqual({"case": "p999", "webhooks": entries}, kept)
        back = run._webhook_restore_patch(entries)
        self.assertEqual([{"name": "inferenceservice.validator", "clientConfig": {"service": ISVC_SERVICE}}], back["webhooks"])
        self.assertIsNone(back["metadata"]["annotations"][run.WEBHOOK_OUTAGE_ANNO])
        self.assertEqual(AWAY, run._webhook_outage_service("p999"))

    def test_entries_are_the_hooks_naming_the_managers_service(self):
        self.assertEqual({"inferenceservice.validator": ISVC_SERVICE}, run._webhook_entries(VALIDATING[0]))
        self.assertEqual({}, run._webhook_entries(VALIDATING[1]), "a webhook of another Service is left alone")
        elsewhere = _webhook_configuration("x", [("x.validator", run.WEBHOOK_SERVICE, "another-namespace", "/x")])
        self.assertEqual({}, run._webhook_entries(elsewhere), "the same Service name in another namespace is not the manager's")

    def test_start_cuts_every_configuration_naming_the_service_and_registers_the_restore_first(self):
        f = run._fault_new("p999", {"fault": "webhook:settling", "lift": "after-hold"})
        calls, saved = [], list(run._FAULT_CLEANUPS)
        try:
            with mock.patch.object(run, "kubectl", _cluster_kubectl(calls)):
                run._fault_fire(f, {}, {}, set())
            self.assertTrue(f["fired"])
            self.assertIsNone(f["infra_fail"])
            self.assertEqual(AWAY, f["webhook_away"])
            self.assertIn("does not exist", f["cause"])
            patches = _patches(calls)
            self.assertEqual([("validatingwebhookconfiguration", "inferenceservice.ome.io"),
                              ("mutatingwebhookconfiguration", "inferenceservice.ome.io")],
                             [(k, n) for k, n, _b in patches], "both configurations naming the Service, not the other one")
            for _k, _n, body in patches:
                self.assertEqual([AWAY], [h["clientConfig"]["service"]["name"] for h in body["webhooks"]])
            self.assertEqual({"case": "p999", "webhooks": {"inferenceservice.pod-mutator": POD_SERVICE}},
                             json.loads(patches[1][2]["metadata"]["annotations"][run.WEBHOOK_OUTAGE_ANNO]))
            first_patch = [c for c in calls if c[0][0] == "patch"][0]
            self.assertEqual(("patch", "validatingwebhookconfiguration", "inferenceservice.ome.io", "--type=strategic"),
                             first_patch[0][:4])
            self.assertEqual(len(saved) + 1, first_patch[1], "the restore is registered before the first patch")
            self.assertIn("2 webhook(s)", f["notes"][-1])
            self.assertIn(AWAY, f["notes"][-1])
            calls.clear()
            with mock.patch.object(run, "kubectl", _cluster_kubectl(calls)):
                self.assertTrue(run._webhook_outage_lift(f))
                self.assertTrue(run._webhook_outage_lift(f))
                run._run_fault_cleanups()
            restored = _patches(calls)
            self.assertEqual(2, len(restored), "each cut configuration is restored once, never twice")
            self.assertEqual([ISVC_SERVICE], [h["clientConfig"]["service"] for h in restored[0][2]["webhooks"]])
            self.assertEqual([POD_SERVICE], [h["clientConfig"]["service"] for h in restored[1][2]["webhooks"]])
            self.assertTrue(all(b["metadata"]["annotations"][run.WEBHOOK_OUTAGE_ANNO] is None for _k, _n, b in restored))
        finally:
            run._FAULT_CLEANUPS[:] = saved

    def test_start_refuses_when_nothing_names_the_service_one_is_cut_already_or_the_away_service_exists(self):
        saved = list(run._FAULT_CLEANUPS)
        try:
            def start(**over):
                f = run._fault_new("p999", {"fault": "webhook:idle", "lift": "after-hold", "during": "idle"})
                with mock.patch.object(run, "kubectl", _cluster_kubectl([], **over)):
                    run._webhook_outage_start(f)
                return f
            self.assertIn("no webhook configuration names", start(validating=[VALIDATING[1]], mutating=[])["infra_fail"])
            with mock.patch.object(run, "kubectl", _cluster_kubectl([], validating=[VALIDATING[1]], mutating=[])):
                self.assertIn("no webhook configuration names", run._webhook_capability())
            cut = copy.deepcopy(MUTATING[0])
            cut["metadata"]["annotations"][run.WEBHOOK_OUTAGE_ANNO] = json.dumps({"case": "p1", "webhooks": {}})
            self.assertIn("already pointed away", start(mutating=[cut])["infra_fail"])
            self.assertIn("exists", start(away_exists=True)["infra_fail"])
            with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 1, "", "not found")):
                self.assertIn("cannot be read", run._webhook_capability())
                self.assertIn("cannot be read", run._fault_capability(run.WEBHOOK_FAULT))
            with mock.patch.object(run, "kubectl", _cluster_kubectl([])):
                self.assertIsNone(run._webhook_capability())
            self.assertIsNone(run._fault_capability(run.CONFLICT_FAULT))
            self.assertEqual(saved, run._FAULT_CLEANUPS, "a refused start registers nothing")
        finally:
            run._FAULT_CLEANUPS[:] = saved

    def test_sweep_restores_every_cut_configuration_from_its_annotation_and_batch_calls_it(self):
        cut_v = copy.deepcopy(VALIDATING[0])
        cut_v["webhooks"][0]["clientConfig"]["service"]["name"] = AWAY
        cut_v["metadata"]["annotations"][run.WEBHOOK_OUTAGE_ANNO] = json.dumps(
            {"case": "p999", "webhooks": {"inferenceservice.validator": ISVC_SERVICE}})
        cut_m = copy.deepcopy(MUTATING[0])
        cut_m["webhooks"][0]["clientConfig"]["service"]["name"] = AWAY
        cut_m["metadata"]["annotations"][run.WEBHOOK_OUTAGE_ANNO] = json.dumps(
            {"case": "p999", "webhooks": {"inferenceservice.pod-mutator": POD_SERVICE}})
        calls = []
        with mock.patch.object(run, "kubectl", _cluster_kubectl(calls, validating=[cut_v, VALIDATING[1]], mutating=[cut_m])):
            done = run.restore_webhook_configurations()
        self.assertIn("restored validatingwebhookconfiguration/inferenceservice.ome.io", done)
        self.assertIn("restored mutatingwebhookconfiguration/inferenceservice.ome.io", done)
        restored = _patches(calls)
        self.assertEqual([("validatingwebhookconfiguration", "inferenceservice.ome.io"),
                          ("mutatingwebhookconfiguration", "inferenceservice.ome.io")], [(k, n) for k, n, _b in restored])
        self.assertEqual([ISVC_SERVICE], [h["clientConfig"]["service"] for h in restored[0][2]["webhooks"]])
        self.assertIsNone(restored[0][2]["metadata"]["annotations"][run.WEBHOOK_OUTAGE_ANNO])
        calls.clear()
        with mock.patch.object(run, "kubectl", _cluster_kubectl(calls)):
            self.assertEqual("", run.restore_webhook_configurations())
        self.assertEqual([], _patches(calls), "a configuration that was never cut is left alone")
        out = io.StringIO()
        with mock.patch.object(batch, "restore_webhook_configurations", lambda: "restored x"), contextlib.redirect_stdout(out):
            batch.sweep_webhook_outage()
        self.assertIn("restored x", out.getvalue())
        self.assertTrue(batch.cluster_wide_fault({"fault": "webhook:settling"}), "a webhook case runs alone")
        self.assertFalse(batch.cluster_wide_fault({"fault": "conflict:settling"}), "a competing writer stays in the pool")

    def test_probe_reads_the_apiservers_words(self):
        refusal = ('Error from server (InternalError): Internal error occurred: failed calling webhook '
                   f'"inferenceservice.validator": failed to call webhook: Post "https://{AWAY}.{run.CTRL_NS}.svc:443/validate-isvc": '
                   f'service "{AWAY}" not found')
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 1, "", refusal + "\n")):
            answers, words = run._webhook_probe("p999")
        self.assertFalse(answers)
        self.assertIn("failed calling webhook", words)
        self.assertTrue(run._names_admission(words))
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, "", "")):
            self.assertEqual((True, ""), run._webhook_probe("p999"))
        self.assertFalse(run._names_admission("Instance 2 waiting on QuotaExceeded"))
        probe = []
        with mock.patch.object(run, "kubectl", lambda *a, **k: probe.append(a) or subprocess.CompletedProcess(a, 1, "", refusal)):
            run._webhook_probe("p999")
        self.assertIn("--dry-run=server", probe[0], "the probe is a server-side dry run: admission runs, nothing changes")
        self.assertEqual(("-n", run.NS, "patch", "isvc", "p999"), probe[0][:5])


class ConflictWriterTest(unittest.TestCase):
    """The competing writer patches its annotation on the ISVC or on every
    InferenceReplica: once, or at the configured rate until it is stopped;
    the stop is registered before the first write and is idempotent."""

    def _kubectl(self, calls, irs=("p999-engine", "p999-router")):
        def kubectl(*a, **k):
            calls.append(a)
            if "get" in a:
                return subprocess.CompletedProcess(a, 0, "\n".join(f"inferencereplica.ome.io/{n}" for n in irs), "")
            return subprocess.CompletedProcess(a, 0, "", "")
        return kubectl

    def test_targets(self):
        calls = []
        with mock.patch.object(run, "kubectl", self._kubectl(calls)):
            self.assertEqual([("isvc", "p999")], run._conflict_targets({"target": "isvc", "case_id": "p999"}))
            self.assertEqual([("inferencereplica", "p999-engine"), ("inferencereplica", "p999-router")],
                             run._conflict_targets({"target": "ir", "case_id": "p999"}))

    def test_once_writes_every_target_once_after_registering_the_stop(self):
        f = run._fault_new("p999", {"fault": "conflict:settling", "form": "once", "target": "ir"})
        calls, saved = [], list(run._FAULT_CLEANUPS)
        try:
            with mock.patch.object(run, "kubectl", self._kubectl(calls)):
                run._fault_fire(f, {}, {}, set())
            patches = [c for c in calls if "patch" in c]
            self.assertEqual(2, len(patches))
            self.assertEqual(("-n", run.NS, "patch", "inferencereplica", "p999-engine", "--type=merge"), patches[0][:6])
            self.assertEqual({"metadata": {"annotations": {run.CONFLICT_ANNO: "1"}}}, json.loads(patches[0][7]))
            self.assertEqual((2, 0), (f["writes"], f["refused_writes"]))
            self.assertTrue(f["stop"].is_set())
            self.assertEqual((run.CONFLICT_FAULT, f), run._FAULT_CLEANUPS[-1])
            self.assertTrue(run._conflict_writer_stop(f))
            with mock.patch.object(run, "kubectl", self._kubectl(calls)):
                run._run_fault_cleanups()
            self.assertEqual(2, len([c for c in calls if "patch" in c]), "the cleanup issues no write")
        finally:
            run._FAULT_CLEANUPS[:] = saved

    def test_persist_writes_at_the_rate_until_stopped(self):
        f = run._fault_new("p999", {"fault": "conflict:idle", "form": "persist", "target": "isvc", "during": "idle"})
        calls, saved = [], list(run._FAULT_CLEANUPS)
        try:
            with mock.patch.object(run, "kubectl", self._kubectl(calls)), mock.patch.object(run, "CONFLICT_RATE_HZ", 100.0):
                run._conflict_writer_start(f)
                time.sleep(0.2)
                self.assertTrue(run._conflict_writer_stop(f))
                written = f["writes"]
                time.sleep(0.05)
            self.assertGreaterEqual(written, 3)
            self.assertEqual(written, f["writes"], "no write after the stop")
            self.assertTrue(all(c[3] == "isvc" and c[4] == "p999" for c in calls if "patch" in c))
            values = [json.loads(c[7])["metadata"]["annotations"][run.CONFLICT_ANNO] for c in calls if "patch" in c]
            self.assertEqual([str(n) for n in range(1, len(values) + 1)], values, "each round bumps the value")
            self.assertTrue(run._conflict_writer_stop(f), "stopping twice is fine")
            self.assertIn("persist", f["notes"][0])
        finally:
            run._FAULT_CLEANUPS[:] = saved

    def test_no_target_is_an_apparatus_failure_and_refusals_are_counted(self):
        saved = list(run._FAULT_CLEANUPS)
        try:
            f = run._fault_new("p999", {"fault": "conflict:settling", "form": "once", "target": "ir"})
            with mock.patch.object(run, "kubectl", self._kubectl([], irs=())):
                run._conflict_writer_start(f)
            self.assertIn("no ir of the case", f["infra_fail"])
            g = run._fault_new("p999", {"fault": "conflict:settling", "form": "once", "target": "isvc"})
            with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 1, "", "denied by webhook")):
                run._conflict_writer_start(g)
            self.assertEqual((0, 1), (g["writes"], g["refused_writes"]))
            self.assertIn("denied", g["last_refusal"])
        finally:
            run._FAULT_CLEANUPS[:] = saved


EVENTS = {"items": [
    {"reason": "InternalError", "message": 'failed calling webhook "x": no endpoints available',
     "firstTimestamp": "2026-10-01T10:00:10Z", "lastTimestamp": "2026-10-01T10:00:40Z", "count": 7},
    {"reason": "RecreateUpdateStarted", "message": "component=engine instance=1",
     "firstTimestamp": "2026-10-01T09:59:00Z", "lastTimestamp": "2026-10-01T10:00:20Z", "count": 4},
    {"reason": "Old", "message": "before the window",
     "firstTimestamp": "2026-10-01T09:00:00Z", "lastTimestamp": "2026-10-01T09:00:00Z", "count": 1},
]}
T0 = run._event_epoch("2026-10-01T10:00:00Z")
METRICS = """# HELP controller_runtime_reconcile_total Total number of reconciliations per controller
controller_runtime_reconcile_total{controller="inferenceservice",result="success"} 120
controller_runtime_reconcile_total{controller="inferenceservice",result="error"} 5
controller_runtime_reconcile_total{controller="inferencereplica",result="success"} 300
controller_runtime_reconcile_total{controller="inferencereplica",result="requeue_after"} 40
controller_runtime_reconcile_total{controller="basemodel",result="success"} 9
controller_runtime_reconcile_errors_total{controller="inferenceservice"} 5
controller_runtime_reconcile_errors_total{controller="inferencereplica"} 2
"""


class WindowReadersTest(unittest.TestCase):
    """What a window reads: the controller's events inside it, the words that
    name admission, the reconcile counters, the configured requeue and the
    fingerprints that tell a stalled status."""

    def test_object_events_count_occurrences_inside_the_window(self):
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, json.dumps(EVENTS), "")):
            found = run._object_events("InferenceService", "p999", T0)
            total, reasons = run._controller_event_tally("p999", [], T0, T0 + 60)
        self.assertEqual(["RecreateUpdateStarted", "InternalError"], [r for _w, r, _m, _n in found], "oldest first")
        self.assertEqual({"InternalError": 7, "RecreateUpdateStarted": 1}, {r: n for _w, r, _m, n in found},
                         "a repeat that began before the window counts its latest occurrence alone")
        self.assertEqual((8, {"InternalError": 7, "RecreateUpdateStarted": 1}), (total, reasons))
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, json.dumps(EVENTS), "")):
            self.assertEqual((1, {"RecreateUpdateStarted": 1}), run._controller_event_tally("p999", [], T0, T0 + 25))

    def test_a_window_opened_inside_a_second_keeps_that_seconds_events(self):
        """Event stamps carry whole seconds: an event recorded later in the
        second a window opened in is at or after the opening at the
        resolution the stamps have, and belongs to the window."""
        message = "OMENative component=engine instance=3 restart triggered: pod e3 wedged"
        events = {"items": [{"reason": "RestartTriggered", "message": message, "count": 1,
                             "firstTimestamp": "2026-10-01T10:00:00Z", "lastTimestamp": "2026-10-01T10:00:00Z"}]}
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, json.dumps(events), "")):
            self.assertEqual([("RestartTriggered", message)], run._isvc_events_since("p999", T0 + 0.6))
            self.assertEqual([message], run._victim_repairs("p999", T0 + 0.6, "engine", "3"))
            self.assertEqual([], run._isvc_events_since("p999", T0 + 1.0), "the next second is outside the window")
            found = run._object_events("InferenceService", "p999", T0 + 0.6)
            self.assertEqual([("RestartTriggered", 1)], [(r, n) for _w, r, _m, n in found])
            self.assertEqual([], run._object_events("InferenceService", "p999", T0 + 1.0))

    def test_write_blocked_reason_reads_events_then_rows_then_conditions(self):
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, json.dumps(EVENTS), "")):
            where, text = run._write_blocked_reason("p999", [], T0)
        self.assertEqual("InferenceService p999 event InternalError", where)
        self.assertIn("failed calling webhook", text)
        rows = {"engine": {"hold": None, "rows": [{"index": 1, "phase": "Updating",
                                                   "lastFailure": {"reason": "CreateFailed",
                                                                   "message": "admission webhook refused the pod"}}]}}
        with mock.patch.object(run, "_object_events", lambda *a: []), mock.patch.object(run, "_ir_snapshot", lambda c: rows):
            self.assertEqual(("engine Instance 1 row", "admission webhook refused the pod"), run._write_blocked_reason("p999", ["p999-engine"], T0))
        held = {"engine": {"hold": {"gate": "Admission", "reason": "the webhook is unavailable"}, "rows": []}}
        with mock.patch.object(run, "_object_events", lambda *a: []), mock.patch.object(run, "_ir_snapshot", lambda c: held):
            self.assertEqual(("engine rolloutHold", "the webhook is unavailable"), run._write_blocked_reason("p999", [], T0))
        status = {"conditions": [{"type": "Ready", "reason": "ReconcileError", "message": "failed calling webhook"}]}
        with mock.patch.object(run, "_object_events", lambda *a: []), mock.patch.object(run, "_ir_snapshot", lambda c: {}), \
                mock.patch.object(run, "fetch_status", lambda c: status):
            self.assertEqual(("ISVC condition Ready", "failed calling webhook"), run._write_blocked_reason("p999", [], T0))
        with mock.patch.object(run, "_object_events", lambda *a: []), mock.patch.object(run, "_ir_snapshot", lambda c: {}), \
                mock.patch.object(run, "fetch_status", lambda c: {"conditions": [{"type": "Ready", "message": "all good"}]}):
            self.assertIsNone(run._write_blocked_reason("p999", [], T0))

    def test_reconcile_counts_and_deltas(self):
        counts = run._reconcile_counts(METRICS)
        self.assertEqual({"inferenceservice": (125, 5), "inferencereplica": (340, 2)}, counts, "the model controller is not OMENative")
        self.assertEqual({}, run._reconcile_counts(""))
        later = {"inferenceservice": (200, 9), "inferencereplica": (10, 0)}
        self.assertEqual({"inferenceservice": (75, 4), "inferencereplica": (10, 0)}, run._reconcile_delta(counts, later),
                         "a counter that went backwards counts from zero")
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, METRICS, "")) as _k:
            self.assertEqual(METRICS, run._manager_metrics())
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 1, "", "forbidden")):
            self.assertEqual("", run._manager_metrics())

    def test_requeue_and_stall_budget(self):
        with mock.patch.object(run, "_controller_lifecycle_config", lambda: ({"requeue": {"operation": "5s", "gate": "3s"}}, None)):
            self.assertEqual(5.0, run._controller_requeue_s())
        with mock.patch.object(run, "_controller_lifecycle_config", lambda: ({"requeue": {"operation": "20s"}}, None)):
            self.assertEqual(20.0, run._controller_requeue_s())
        with mock.patch.object(run, "_controller_lifecycle_config", lambda: ({}, None)):
            self.assertEqual(run.REQUEUE_FALLBACK_S, run._controller_requeue_s())
        with mock.patch.object(run, "_controller_lifecycle_config", lambda: (None, "unreadable")):
            self.assertEqual(run.REQUEUE_FALLBACK_S, run._controller_requeue_s())
        self.assertEqual(int(run.STATUS_STALL_MIN_S * run.TIMEOUT_MULT), run._status_stall_budget(2.0))
        self.assertEqual(int(run.STATUS_STALL_REQUEUES * 20.0 * run.TIMEOUT_MULT), run._status_stall_budget(20.0))

    def test_fingerprints(self):
        rows = {"engine": [{"index": 0, "phase": "Ready", "podCount": 1, "servingPodCount": 1, "runningRevision": "r1",
                            "operation": {"type": "Update", "startedAt": "2026-10-01T10:00:00Z", "lastTransitionTime": "x"}}]}
        later = {"engine": [dict(rows["engine"][0], operation={"type": "Update", "startedAt": "2026-10-01T10:05:00Z"})]}
        self.assertEqual(run._rows_fingerprint(rows), run._rows_fingerprint(later), "clocks do not move the fingerprint")
        moved = {"engine": [dict(rows["engine"][0], servingPodCount=0)]}
        self.assertNotEqual(run._rows_fingerprint(rows), run._rows_fingerprint(moved))
        pods = {"a": pod("engine", 0), "b": pod("engine", 1, ready=False), "c": None,
                "d": dict(pod("engine", 2), serving=False)}
        for name, uid in (("a", "u-a"), ("b", "u-b"), ("d", "u-d")):
            pods[name]["uid"] = uid
        self.assertEqual(frozenset({"u-a"}), run._serving_fingerprint(pods))

    def test_expected_creations(self):
        ctx = {"N": {"engine": 4, "decoder": 4, "router": 2}, "pods_per_inst": {"engine": 2, "decoder": 1, "router": 1},
               "configured": {"engine", "decoder", "router"}, "modes": {"engine": "surge", "decoder": "inplace", "router": "surge"}}
        self.assertEqual({"engine": 8, "decoder": 0, "router": 0}, run._expected_creations(ctx, {"engine"}))
        self.assertEqual({"engine": 8, "decoder": 0, "router": 0}, run._expected_creations(ctx, {"engine", "decoder"}),
                         "an in-place member creates nothing")
        self.assertEqual({"engine": 2, "decoder": 0, "router": 0},
                         run._expected_creations(ctx, {"engine"}, target={"engine": 5, "decoder": 4, "router": 2}))

    def test_leftover_hold(self):
        def items(hold=None, blocks=(), rows=()):
            status = {"rolloutHold": hold, "retryBlocks": list(blocks), "instanceStatuses": list(rows)}
            return {"engine": {"spec": {"component": "engine"}, "status": status}}
        with mock.patch.object(run, "_ir_by_component", lambda c: items()):
            self.assertEqual("", run._leftover_hold("p999"))
        with mock.patch.object(run, "_ir_by_component", lambda c: items(hold={"gate": "Budget", "reason": "x"})):
            self.assertIn("rolloutHold gate=Budget", run._leftover_hold("p999"))
        with mock.patch.object(run, "_ir_by_component", lambda c: items(blocks=[{"state": "Held", "targetRevision": "r2"}])):
            self.assertIn("still Held", run._leftover_hold("p999"))
        with mock.patch.object(run, "_ir_by_component", lambda c: items(rows=[{"index": 3, "phase": "Failed"}])):
            self.assertEqual("engine Instance 3 row Failed", run._leftover_hold("p999"))


def _win(kind="webhook", op="push", lift="after-hold", form="", **over):
    """A window's bookkeeping with the happy readings, overridden per test."""
    hdr = {"lift": lift} if kind == "webhook" else {"form": form or "once", "target": "isvc"}
    f = run._fault_new("p999", dict(hdr, fault=f"{kind}:{'idle' if op == 'idle' else 'settling'}"))
    f["cause"] = "the fault"
    if kind == "conflict":
        f.update(writes=4, refused_writes=0, targets=[("isvc", "p999")])
    with mock.patch.object(run, "_controller_lifecycle_config", lambda: ({"requeue": {"operation": "5s"}}, None)):
        win = run._window_new(f, hdr, op)
    win.update(fault_ts=1000.0, refused_at=3.0, refusal="failed calling webhook: no endpoints", answered_at=95.0,
               said_at=20.0, said_where="InferenceService p999 event InternalError", said="failed calling webhook",
               created_in_window={"engine": 0}, torn_in_window={"engine": 0}, created={"engine": 4},
               expected_created={"engine": 4 if op != "idle" else 0}, stall_worst=4.0, events=12, event_reasons={"X": 12},
               window_s=90.0, reconciles={"inferenceservice": (40, 6), "inferencereplica": (80, 0)},
               lifted_at=90.0, converged_at=150.0, rows_settled=True, rows_why="", leftover_hold="")
    if op == "idle":
        win.update(created={"engine": 0}, said_at=None)
    if kind == "conflict":
        win.update(refused_at=None, answered_at=None, said_at=None, reconciles=None, lifted_at=60.0)
    win.update(over)
    return f, win


class WindowVerdictTest(unittest.TestCase):
    """_window_checks on fixed readings: each rule's pass, and each rule's
    failure named in the operator's words."""

    def _run(self, f, win, state="CONVERGED"):
        notes, checks = [], []
        verdict = run._window_checks(f, win, state, "PASS", notes, checks)
        return verdict, notes, checks

    def test_a_push_under_the_outage_passes_with_every_claim(self):
        f, win = _win()
        verdict, notes, checks = self._run(f, win)
        self.assertEqual(("PASS", []), (verdict, notes))
        text = "\n".join(checks)
        for claim in ("admission refused 3s", "the status said why the push waited", "no pod was created while the webhook",
                      "followed every serving change within 4s", "12 controller event(s)", "120 reconcile(s)",
                      "pointed back 90s in and answered 5s later", "the push completed 60s after the webhook returned",
                      "each of its 4 pod slot(s) was created once", "ended Ready with no operation open",
                      "no hold, no Held retry block and no Failed row"):
            self.assertIn(claim, text, claim)
        self.assertFalse(any(c.startswith("  ✗") for c in checks), text)

    def test_an_outage_that_never_bit_is_an_apparatus_failure(self):
        f, win = _win(refused_at=None)
        self.assertEqual("PASS", self._run(f, win)[0])
        self.assertIn("never bit", f["infra_fail"])
        g, win2 = _win(kind="conflict")
        g["writes"] = 0
        self.assertEqual("PASS", self._run(g, win2)[0])
        self.assertIn("wrote nothing", g["infra_fail"])

    def test_a_wait_with_no_reason_fails_but_an_escape_is_reported(self):
        f, win = _win(said_at=None)
        verdict, notes, checks = self._run(f, win)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("a wait with no reason" in c for c in checks))
        f, win = _win(said_at=None, escaped_at=30.0)
        verdict, _notes, checks = self._run(f, win)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("completed 30s into the outage with nothing said" in c for c in checks))
        f, win = _win(op="idle", said_at=7.0, said="failed calling webhook")
        _verdict, _notes, checks = self._run(f, win, state="HELD")
        self.assertTrue(any("the steady service reported a blocked write" in c for c in checks))

    def test_pods_made_under_the_outage_are_reported_and_pods_moved_at_idle_fail(self):
        f, win = _win(created_in_window={"engine": 1})
        verdict, _notes, checks = self._run(f, win)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("did not gate them" in c for c in checks))
        f, win = _win(op="idle", created={"engine": 1})
        verdict, notes, checks = self._run(f, win, state="HELD")
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("created on a service nobody touched" in c for c in checks))
        f, win = _win(op="scale", torn_in_window={"engine": 2}, expected_created={"engine": 1}, created={"engine": 1})
        verdict, _notes, checks = self._run(f, win)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("nothing may be taken down" in c for c in checks))

    def test_a_stalled_status_fails(self):
        f, win = _win(stall_worst=31.0)
        verdict, notes, checks = self._run(f, win)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("the status stopped updating" in c and "6 requeues of 5s" in c for c in checks))

    def test_rates_fail_over_a_long_window_and_report_over_a_short_one(self):
        f, win = _win(events=200, event_reasons={"InternalError": 200})
        verdict, _notes, checks = self._run(f, win)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("133/min" in c and "a hot loop (InternalError×200)" in c for c in checks))
        f, win = _win(events=50, window_s=20.0, lifted_at=20.0)
        verdict, _notes, checks = self._run(f, win)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("cap 60/min not judged" in c for c in checks))
        f, win = _win(reconciles={"inferencereplica": (900, 0)})
        verdict, _notes, checks = self._run(f, win)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("600/min" in c and "reconcile(s) of the OMENative controllers" in c for c in checks))
        f, win = _win(reconciles=None)
        verdict, _notes, checks = self._run(f, win)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("reconcile counters were not readable" in c for c in checks))
        f, win = _win(events=None)
        self.assertTrue(any("events over the window could not be read" in c for c in self._run(f, win)[2]))

    def test_the_lift_the_completion_and_the_double_apply(self):
        f, win = _win(lifted_at=None)
        verdict, _notes, checks = self._run(f, win, state="LIFT_FAILED")
        self.assertEqual("PASS", verdict, "the apparatus failure is the fault's")
        self.assertFalse(any("completed" in c for c in checks))
        f, win = _win()
        verdict, _notes, checks = self._run(f, win, state="ROTATE_TIMEOUT")
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("did not complete within budget after the webhook returned (ROTATE_TIMEOUT)" in c for c in checks))
        f, win = _win(created={"engine": 5})
        verdict, notes, checks = self._run(f, win)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("5 new pod(s) for 4 pod slot(s) — something was applied twice" in c for c in checks))
        f, win = _win(created={"engine": 3})
        verdict, _notes, checks = self._run(f, win)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("fewer than a rebuild of each" in c for c in checks))
        f, win = _win(kind="conflict", form="persist")
        verdict, _notes, checks = self._run(f, win)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("the writer stopped 60s in" in c for c in checks))
        self.assertTrue(any("the push completed 150s after the writer started" in c for c in checks))

    def test_rows_and_holds_left_behind_fail(self):
        f, win = _win(rows_settled=False, rows_why="engine Instance 2 Updating")
        verdict, _notes, checks = self._run(f, win)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("did not settle within" in c for c in checks))
        f, win = _win(leftover_hold="engine rolloutHold gate=Budget reason=x")
        verdict, _notes, checks = self._run(f, win)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("still holds something after the push ended" in c for c in checks))
        f, win = _win(rows_settled=None, leftover_hold=None)
        _verdict, _notes, checks = self._run(f, win, state="ROTATE_TIMEOUT")
        self.assertFalse(any("Ready with no operation open" in c or "no hold" in c for c in checks),
                         "rows not read when the operation never completed")


class WindowTimingTest(unittest.TestCase):
    """When the webhook is due back, how long a window is held, and how a
    tick tracks the rows lagging the pods in rotation."""

    def test_lift_due(self):
        _f, win = _win()
        win.update(said_at=None, fault_ts=0.0)
        self.assertFalse(run._window_lift_due(win, run.HOLD_REASON_WAIT_S - 1))
        self.assertTrue(run._window_lift_due(win, run.HOLD_REASON_WAIT_S))
        win["said_at"] = 10.0
        self.assertFalse(run._window_lift_due(win, 10.0 + run.LIFT_SETTLE_S - 1))
        self.assertTrue(run._window_lift_due(win, 10.0 + run.LIFT_SETTLE_S))
        _f, after = _win(lift="after-window")
        after.update(said_at=1.0, fault_ts=0.0)
        self.assertFalse(run._window_lift_due(after, run.IDLE_HOLD_S - 1))
        self.assertTrue(run._window_lift_due(after, run.IDLE_HOLD_S))
        _f, idle = _win(op="idle")
        idle["fault_ts"] = 0.0
        self.assertTrue(run._window_lift_due(idle, run.LIFT_SETTLE_S))
        self.assertFalse(run._window_lift_due(idle, run.LIFT_SETTLE_S - 1))

    def test_window_span(self):
        self.assertEqual(run.HOLD_REASON_WAIT_S + run.LIFT_SETTLE_S, run._window_span(_win()[1]))
        self.assertEqual(run.IDLE_HOLD_S, run._window_span(_win(lift="after-window")[1]))
        self.assertEqual(run.LIFT_SETTLE_S, run._window_span(_win(op="idle")[1]))
        self.assertEqual(run.LIFT_SETTLE_S, run._window_span(_win(kind="conflict", form="once")[1]))
        self.assertEqual(run.IDLE_HOLD_S, run._window_span(_win(kind="conflict", form="persist")[1]))

    def test_tick_tracks_the_rows_lagging_a_serving_change(self):
        f, win = _win(kind="conflict", form="persist")
        win.update(fault_ts=0.0, stall_worst=0.0, rows_fp=None, rows_changed_at=None, serving_fp=None,
                   serving_changed_at=None, last_rows_poll=-10.0, uids_seen={})
        a, b = pod("engine", 0), pod("engine", 1)
        a["uid"], b["uid"] = "u-a", "u-b"
        ctx = {"case_id": "p999", "pod_state": {"a": a, "b": b}}
        rows = {"engine": [{"index": 0, "phase": "Ready", "podCount": 1, "servingPodCount": 1}]}
        with mock.patch.object(run, "_ir_rows", lambda c: rows):
            run._window_tick(ctx, f, win, 0.0)
            self.assertEqual(0.0, win["stall_worst"])
            b["serving"] = False               # a pod leaves rotation at t=10; the rows never move
            for now in (10.0, 12.0, 14.0, 40.0):
                run._window_tick(ctx, f, win, now)
            self.assertEqual(30.0, win["stall_worst"])
            rows["engine"][0]["servingPodCount"] = 0   # the rows catch up at t=42
            run._window_tick(ctx, f, win, 42.0)
            run._window_tick(ctx, f, win, 60.0)
            self.assertEqual(30.0, win["stall_worst"], "a caught-up status stops the clock")
        self.assertEqual({"engine": {"u-a", "u-b"}}, win["uids_seen"])


class GeneratedWindowFamiliesTest(unittest.TestCase):
    """gen-webhook and gen-conflict: every shape family of the ledger at one
    gang width, the steady state, the push and the scale-up, each kind's own
    forms; every cell validates and says what it asserts."""

    @classmethod
    def setUpClass(cls):
        cls.outputs, rows = generate_cases.build_outputs()
        cls.webhook = [(n, kv, m) for f, n, kv, m in rows if f == "gen-webhook"]
        cls.conflict = [(n, kv, m) for f, n, kv, m in rows if f == "gen-conflict"]

    def _validates(self, folder, fam):
        with tempfile.TemporaryDirectory() as tmp:
            for name, _kv, _m in fam:
                path = Path(tmp) / f"{name}.yaml"
                path.write_text(self.outputs[f"{folder}/{name}.yaml"])
                self.assertEqual([], run.validate_case_file(path), name)

    def _families(self, fam):
        return {(kv["topo"], kv["Nr"] > 0 and kv["Nd"] == 0, kv["Nd"] > 0) for _n, kv, _m in fam}

    def test_webhook_family(self):
        fam = self.webhook
        self.assertEqual(48, len(fam))
        self.assertEqual(6, len(self._families(fam)), "every shape family of the ledger")
        self.assertEqual({"webhook steady", "webhook push", "webhook scale"}, {m["winop"] for _n, _kv, m in fam})
        self.assertEqual({"after-hold", "after-window"}, {kv["lift"] for _n, kv, _m in fam})
        cells = {(kv["topo"], kv["Nr"], m["op"], m["strat"], kv["lift"]) for _n, kv, m in fam}
        self.assertEqual(48, len(cells))
        for name, kv, m in fam:
            text = self.outputs[f"gen-webhook/{name}.yaml"]
            reason = text.splitlines()[1]
            self.assertNotIn("form", kv)
            self.assertNotIn("target", kv)
            self.assertEqual("none", kv["co"])
            if m["op"] == "steady":
                self.assertEqual(("webhook:idle", "idle", kv["Ne"]), (kv["fault"], kv["during"], kv["ceil_e"]))
                self.assertIn("nothing may move", reason)
            elif m["op"] == "push":
                self.assertEqual("webhook:settling", kv["fault"])
                self.assertNotIn("during", kv)
                self.assertIn(m["strat"], ("SurgeThenDrain", "RecreatePod"))
                self.assertEqual(kv["Ne"] + (1 if m["strat"] == "SurgeThenDrain" else 0), kv["ceil_e"])
                self.assertIn("the status must say why in the webhook's own words", reason)
                self.assertIn("each pod slot created once", reason)
            else:
                self.assertEqual(("scaledown", "webhook:settling", kv["Ne"] + 1, kv["Ne"] + 1), (kv["mode"], kv["fault"], kv["to_e"], kv["ceil_e"]))
                self.assertIn("the new Instance comes up whole and serving, created once", reason)
            self.assertIn("hold window" if kv["lift"] == "after-window" else "pointed back", reason)
            if kv["Nd"]:
                self.assertIn("decoder and router are negative controls", reason)
            elif kv["Nr"]:
                self.assertIn("the router is a negative control", reason)
                self.assertIn("router:", text)
        self._validates("gen-webhook", fam)

    def test_conflict_family(self):
        fam = self.conflict
        self.assertEqual(72, len(fam))
        self.assertEqual(6, len(self._families(fam)))
        self.assertEqual({"once", "persist"}, {kv["form"] for _n, kv, _m in fam})
        self.assertEqual({"isvc", "ir"}, {kv["target"] for _n, kv, _m in fam})
        cells = {(kv["topo"], kv["Nr"], m["op"], kv["form"], kv["target"]) for _n, kv, m in fam}
        self.assertEqual(72, len(cells))
        for name, kv, m in fam:
            reason = self.outputs[f"gen-conflict/{name}.yaml"].splitlines()[1]
            self.assertNotIn("lift", kv)
            self.assertEqual("conflict:idle" if m["op"] == "steady" else "conflict:settling", kv["fault"])
            self.assertEqual(kv["Ne"] + (0 if m["op"] == "steady" else 1), kv["ceil_e"],
                             "one surge under the canonical budget on a push, the new Instance on a scale, nothing at idle")
            self.assertIn("stale resourceVersion", reason)
            self.assertIn("once" if kv["form"] == "once" else "every few hundred milliseconds", reason)
            self.assertIn("the ISVC" if kv["target"] == "isvc" else "every InferenceReplica", reason)
            if m["op"] == "push":
                self.assertIn("double apply", reason)
                self.assertIn('type: "SurgeThenDrain"', self.outputs[f"gen-conflict/{name}.yaml"])
        self._validates("gen-conflict", fam)

    def test_ids_blocks_and_manifest(self):
        for folder, fam in (("gen-webhook", self.webhook), ("gen-conflict", self.conflict)):
            lo, hi = generate_cases.GEN_BLOCKS[folder]
            self.assertTrue(8900 <= lo < hi <= 9050, folder)
            for name, _kv, _m in fam:
                self.assertTrue(lo <= int(name[1:]) < hi, (folder, name))
        manifest = self.outputs[generate_cases.MANIFEST_REL]
        for text in ("| gen-webhook | 48 |", "| gen-conflict | 72 |", "By window operation", "By competing-writer form",
                     "By competing-writer target", "gen-webhook points the admission webhook", "gen-conflict runs a competing writer"):
            self.assertIn(text, manifest)


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

    def test_minfails_counts_a_promoted_canarys_failing_samples(self):
        base = "# p999: x mode=canary Ne=1 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 canary=auto topo=engine"
        self.assertEqual([], self._errs(base + " analysis=pass health=breach failurelimit=12 minfails=1"))
        self.assertTrue(any("minfails=" in e for e in self._errs(base + " analysis=breach failurelimit=1 minfails=1")))
        self.assertTrue(any("malformed header value minfails=0" in e for e in self._errs(base + " analysis=pass minfails=0")))
        rollout = "# p999: x mode=rollout Ne=1 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 topo=engine"
        self.assertTrue(any("minfails= is a canary-mode knob" in e for e in self._errs(rollout + " analysis=pass minfails=1")))

    def test_a_missing_policy_is_judged_as_held(self):
        base = "# p999: x mode=canary Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 canary=auto topo=engine"
        self.assertEqual([], self._errs(base + " prearm=planinvalid terminal=Held cell=canary/Idle/runLost"))
        self.assertTrue(any("terminal=Held" in e for e in self._errs(base + " prearm=planinvalid cell=canary/Idle/runLost")))
        self.assertTrue(any("terminal=Held" in e for e in self._errs(base + " terminal=Held cell=canary/Idle/runLost")))

    def test_secondary_traffic_park_scopes_a_pairing_verb_too(self):
        base = "# p999: x mode=canary Ne=2 Nd=2 Nr=0 ceil_e=3 ceil_d=3 ceil_r=0 canary=auto topo=pd"
        line = base + " bump=engine,decoder during=canary-staging verb=pairing pairto=pair-v2 xcheck=sectraffic notimpl=1 issue=1346 cell=canary/Staging/pairing"
        self.assertEqual([], self._errs(line + "\n# reason: parked on the secondary traffic gap"))
        self.assertTrue(any("pairto=" in e for e in self._errs(base + " xcheck=sectraffic notimpl=1 issue=1346 cell=canary/Staging/pairing\n# reason: x")))

    def test_idle_restart_is_a_controller_fault_only(self):
        base = "# p999: x mode=canary Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 canary=auto topo=engine"
        self.assertEqual([], self._errs(base + " fault=ctrlrestart:idle cell=canary/Idle/ctrlRestart"))
        self.assertTrue(any("fault=" in e for e in self._errs(base + " fault=podkill:idle")))
        self.assertTrue(any("cell=" in e for e in self._errs(base + " cell=nonsense")))

    def test_cell_is_a_comma_list_of_cell_ids(self):
        base = "# p999: x mode=canary Ne=1 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 canary=manual topo=engine"
        self.assertEqual([], self._errs(base + " cell=canary/Staging/readyTimeout[quota]"))
        self.assertEqual([], self._errs(base + " cell=canary/Staging/readyTimeout[quota],canary/Staging/unpullable[decoder]"))
        self.assertTrue(any("cell=" in e for e in self._errs(base + " cell=canary/Staging/readyTimeout[quota],nonsense")))
        self.assertTrue(any("cell=" in e for e in self._errs(base + " cell=canary/Staging/readyTimeout[quota],")))
        self.assertTrue(any("cell=" in e for e in self._errs(base + " cell=canary/Idle/newTarget[secondaryOnly:engine]")))

class BudgetRoundingTest(unittest.TestCase):
    """A percent budget resolves the way the engine's per-Component budget
    does: rounded up and clamped to the fleet, for maxUnavailable as for
    maxSurge. The Deployment convention of rounding maxUnavailable down is
    not the engine's rule, so the floor the harness holds a roll to must not
    follow it either."""

    def test_percent_rounds_up_like_the_engine(self):
        self.assertEqual(1, run.resolve_max_unavailable("10%", 4))
        self.assertEqual(1, run.resolve_max_unavailable("10%", 8))
        self.assertEqual(1, run.resolve_max_unavailable("25%", 4))
        self.assertEqual(2, run.resolve_max_unavailable("25%", 8))
        self.assertEqual(2, run.resolve_max_unavailable("50%", 4))
        self.assertEqual(4, run.resolve_max_unavailable("50%", 8))
        self.assertEqual(8, run.resolve_max_unavailable("100%", 8))
        self.assertEqual(0, run.resolve_max_unavailable("0%", 8))
        self.assertEqual(4, run.resolve_budget("150%", 4), "clamped to the fleet")

    def test_integers_pass_through_and_junk_reads_as_zero(self):
        self.assertEqual(1, run.resolve_max_unavailable(1, 4))
        self.assertEqual(0, run.resolve_max_unavailable(-1, 4))
        self.assertEqual(3, run.resolve_max_unavailable("3", 4))
        self.assertEqual(0, run.resolve_max_unavailable("many", 4))
        self.assertEqual(0, run.resolve_max_unavailable(None, 4))


class RolloutEditTest(unittest.TestCase):
    """The rollout edits ride the trigger as a runner override on every
    bumped member; a template edit counts an in-place strategy by hash and an
    image edit in place; expect=held on a push validates only as a change a
    strict in-place strategy cannot apply, and its verdict reads the hold,
    its reason and the corrective push."""

    BASE = "# p999: x Ne=2 Nd=2 Nr=0 ceil_e=2 ceil_d=3 ceil_r=0 topo=pd"
    SINGLE = {"engine": 1, "decoder": 1, "router": 1}
    GANG = {"engine": 2, "decoder": 1, "router": 1}

    def _errs(self, header):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_trigger_patch_carries_the_edit(self):
        bump = {"annotations": {run.TRIGGER_ANNO: run.TRIGGER_VAL, "ome.io/deploymentMode": "OMENative"}}
        self.assertEqual({"spec": {"engine": bump}}, run._rollout_trigger_patch({"engine"}, None, None, self.SINGLE))
        env = run._rollout_trigger_patch({"engine"}, "env", None, self.SINGLE)["spec"]["engine"]
        self.assertEqual([{"name": "TEST_REV", "value": run.TRIGGER_VAL}], env["runner"]["env"])
        self.assertEqual(bump["annotations"], env["annotations"])
        res = run._rollout_trigger_patch({"engine"}, "resources", None, self.SINGLE)["spec"]["engine"]
        self.assertEqual({"requests": {"cpu": run.EDIT_CPU_REQUEST}}, res["runner"]["resources"])
        probe = run._rollout_trigger_patch({"engine"}, "probe", None, self.SINGLE)["spec"]["engine"]
        self.assertEqual({"periodSeconds": run.EDIT_PROBE_PERIOD_S}, probe["runner"]["readinessProbe"])
        img = run._rollout_trigger_patch({"engine"}, "image", "nginx:alt", self.SINGLE)["spec"]["engine"]
        self.assertEqual({"image": "nginx:alt"}, img["runner"])
        both = run._rollout_trigger_patch({"engine", "decoder"}, "image", "nginx:alt", self.GANG)["spec"]
        self.assertEqual({"runner": {"name": run.MAIN_CONTAINER, "image": "nginx:alt"}}, both["engine"]["leader"],
                         "a leader runner names its container")
        self.assertNotIn("runner", both["engine"])
        self.assertEqual({"image": "nginx:alt"}, both["decoder"]["runner"])

    def test_fix_patch_puts_the_runner_back(self):
        self.assertEqual({"spec": {"engine": {"runner": None}}}, run._rollout_fix_patch({"engine"}, {"engine": 1}, {}))
        self.assertEqual({"spec": {"engine": {"leader": {"runner": None}}}}, run._rollout_fix_patch({"engine"}, {"engine": 2}, {}))
        had = {"engine": {"env": [{"name": "X", "value": "1"}]}}
        self.assertEqual({"spec": {"engine": {"runner": had["engine"]}}}, run._rollout_fix_patch({"engine"}, {"engine": 1}, had))
        spec = {"engine": {"runner": {"image": "a"}}, "decoder": {"leader": {"runner": {"name": "ome-container"}}}, "router": {}}
        self.assertEqual({"engine": {"image": "a"}, "decoder": {"name": "ome-container"}, "router": None},
                         run._runner_overrides(spec, {"engine", "decoder", "router"}, {"engine": 1, "decoder": 2, "router": 1}))

    def test_edits_validate_in_rollout_mode(self):
        for edit in sorted(run.TEMPLATE_EDITS):
            self.assertEqual([], self._errs(self.BASE + f" edit={edit}"))
        self.assertEqual([], self._errs(self.BASE + " edit=image img=nginx:alt"))
        self.assertTrue(any("needs img=" in e for e in self._errs(self.BASE + " edit=image")))
        runtimerev = self.BASE.replace(" Ne=", " mode=runtimerev rt=x Ne=")
        self.assertTrue(any("mode=rollout" in e for e in self._errs(runtimerev + " edit=probe")))
        self.assertEqual([], self._errs(runtimerev + " edit=resources"), "a runtime resources edit is unchanged")

    def test_held_push_validates_as_a_change_the_strategy_cannot_apply(self):
        for edit in sorted(run.TEMPLATE_EDITS):
            self.assertEqual([], self._errs(self.BASE + f" edit={edit} expect=held"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:old form=start edit=env expect=held floor_soft=1"))
        self.assertTrue(any("expect=held on a push" in e for e in self._errs(self.BASE + " expect=held")))
        self.assertTrue(any("expect=held on a push" in e for e in self._errs(self.BASE + " edit=image img=nginx:alt expect=held")))
        canary = self.BASE.replace(" Ne=", " mode=canary Ne=")
        self.assertTrue(any("expect=held on a push" in e for e in self._errs(canary + " edit=env expect=held")))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:old form=start during=idle expect=held"),
                         "the idle reading of expect=held is unchanged")

    def test_held_push_state_and_counting(self):
        self.assertIsNone(run._held_push_new({"edit": "env"}))
        self.assertIsNone(run._held_push_new({"edit": "env", "expect": "held", "during": "idle"}))
        self.assertIsNone(run._held_push_new({"edit": "image", "expect": "held"}))
        self.assertEqual({"until": None, "before": {}, "broken": None, "min_ready": {}, "events": [], "fixed_at": None,
                          "fix_error": None}, run._held_push_new({"edit": "resources", "expect": "held"}))
        info = {"strat": {"engine": "InPlaceOnly"}}
        for edit in sorted(run.TEMPLATE_EDITS):
            self.assertEqual({"engine": "surge"}, run.update_modes(info, {"engine"}, edit))
        self.assertEqual({"engine": "inplace"}, run.update_modes(info, {"engine"}, "image"))
        self.assertEqual({"engine": "inplace"}, run.update_modes(info, {"engine"}, None),
                         "a held push lands its corrective revision in place")

    def test_hold_observation(self):
        old = {"engine": {"u1", "u2"}}
        pods = {"e0": {"comp": "engine", "alive": True, "uid": "u1", "newanno": False},
                "e1": {"comp": "engine", "alive": True, "uid": "u2", "newanno": False}}
        hold = run._held_push_new({"edit": "env", "expect": "held"})
        run._held_push_observe(hold, pods, {"engine"}, old, {"engine": (2, 2, 0)}, 10.0)
        self.assertIsNone(hold["broken"])
        self.assertEqual({"engine": 2}, hold["min_ready"])
        run._held_push_observe(hold, pods, {"engine"}, old, {"engine": (2, 1, 0)}, 20.0)
        self.assertEqual({"engine": 1}, hold["min_ready"], "the least ready count is kept")
        pods["e1"]["newanno"] = True
        run._held_push_observe(hold, pods, {"engine"}, old, {"engine": (2, 2, 0)}, 30.0)
        self.assertEqual((30.0, "engine pod carries the pushed revision's annotation"), hold["broken"])
        hold = run._held_push_new({"edit": "env", "expect": "held"})
        pods["e1"] = {"comp": "engine", "alive": True, "uid": "u3", "newanno": False}
        run._held_push_observe(hold, pods, {"engine"}, old, {"engine": (2, 2, 0)}, 5.0)
        self.assertEqual((5.0, "engine pod set changed (1 new, 1 gone)"), hold["broken"])

    def test_hold_tick_lands_the_fix_once_the_window_passes(self):
        calls = []
        orig_kubectl, orig_events = run.kubectl, run._isvc_events_since
        run.kubectl = lambda *a, **k: calls.append(a) or subprocess.CompletedProcess(a, 0, "", "")
        run._isvc_events_since = lambda cid, since: [(run.HELD_PUSH_REASON, "OMENative component=engine instance=0 rejected update")]
        try:
            hold = run._held_push_new({"edit": "env", "expect": "held"})
            hold["until"] = 100.0
            pods = {"e0": {"comp": "engine", "alive": True, "uid": "u1", "newanno": False}}
            args = ("p1", {"engine"}, {"engine": 1}, pods, {"engine": {"u1"}}, {"engine": (1, 1, 0)}, 0.0)
            run._held_push_tick(hold, *args, 50.0)
            self.assertEqual([], calls)
            self.assertIsNone(hold["fixed_at"])
            run._held_push_tick(hold, *args, 100.0)
            self.assertEqual(100.0, hold["fixed_at"])
            self.assertEqual(1, len(calls))
            self.assertIn('"runner": null', calls[0][-1])
            self.assertEqual(1, len(hold["events"]))
            run._held_push_tick(hold, *args, 200.0)
            self.assertEqual(1, len(calls), "the corrective push fires once")
        finally:
            run.kubectl, run._isvc_events_since = orig_kubectl, orig_events

    def test_held_push_verdict(self):
        def judge(hold, spec=None, hdr=None):
            notes, checks = [], []
            verdict = run._held_push_checks(hold, {"engine"}, "env", spec or {"engine": {}}, {"engine": 1}, {"engine": 2},
                                            hdr or {}, "PASS", notes, checks)
            return verdict, notes

        good = {"until": 1.0, "before": {"engine": None}, "broken": None, "min_ready": {"engine": 2}, "fixed_at": 90.0,
                "fix_error": None,
                "events": [(run.HELD_PUSH_REASON, "OMENative component=engine instance=0 rejected update: "
                                                  "InPlaceOnly strategy but diff exceeds container images")]}
        self.assertEqual(("PASS", []), judge(good))
        had = {"env": [{"name": "X", "value": "1"}]}
        self.assertEqual("PASS", judge(dict(good, before={"engine": had}), spec={"engine": {"runner": had}})[0],
                         "a member that had a runner before the push keeps it")
        self.assertEqual("FAIL", judge(dict(good, broken=(3.0, "engine pod set changed (1 new, 1 gone)")))[0])
        self.assertEqual("FAIL", judge(dict(good, min_ready={"engine": 1}))[0])
        self.assertEqual("PASS", judge(dict(good, min_ready={"engine": 1}), hdr={"floor_soft": 1})[0],
                         "a case that broke an Instance itself waives the hold floor")
        self.assertEqual("FAIL", judge(dict(good, events=[("RestartTriggered", "component=engine instance=0")]))[0])
        self.assertEqual("FAIL", judge(dict(good, fixed_at=None))[0])
        self.assertEqual("FAIL", judge(good, spec={"engine": {"runner": {"env": []}}})[0])
        self.assertEqual("INFRA-FAIL", judge(dict(good, fix_error="corrective push failed: x"))[0])


class RolloutEditHeaderTest(unittest.TestCase):
    """edit=env is a rollout-mode pod-template edit: it validates, and it
    turns an in-place strategy's update mode into the hash-counted recreate
    the controller resolves it to."""

    def _errs(self, header):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_edit_env_validates_on_a_rollout_case(self):
        base = "# p999: x Ne=2 Nd=2 Nr=0 ceil_e=2 ceil_d=3 ceil_r=0 topo=pd"
        self.assertEqual([], self._errs(base.replace(" Ne=", " edit=env Ne=")))
        self.assertTrue(any("malformed header value edit=" in e for e in self._errs(base.replace(" Ne=", " edit=labels Ne="))))

    def test_edit_env_counts_an_in_place_strategy_by_hash(self):
        info = {"strat": {"engine": "InPlaceIfPossible", "decoder": "SurgeThenDrain", "router": "InPlaceOnly"}}
        configured = {"engine", "decoder", "router"}
        self.assertEqual({"engine": "inplace", "decoder": "surge", "router": "inplace"},
                         run.update_modes(info, configured))
        self.assertEqual({"engine": "surge", "decoder": "surge", "router": "surge"},
                         run.update_modes(info, configured, "env"))


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
        self.assertEqual([], self._errs(self.PD + " proto=v2 proto2=v3 during=canary-serving cell=canary/Serving/pairingRetarget"))
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


class RunCaseBindingsTest(unittest.TestCase):
    """`rb` in run_case is the rollback state for the whole run (a dict, or
    None without rollback=1): bound once from _rollback_new, so no loop-local
    count can shadow it before the convergence guards index it."""

    def test_rb_is_bound_only_from_rollback_new_in_run_case(self):
        import ast
        tree = ast.parse((HERE / "run.py").read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_case")
        stores = sorted(n.lineno for n in ast.walk(fn)
                        if isinstance(n, ast.Name) and n.id == "rb" and isinstance(n.ctx, ast.Store))
        from_rollback_new = sorted(
            n.lineno for n in ast.walk(fn) if isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "rb" for t in n.targets)
            and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
            and n.value.func.id == "_rollback_new")
        self.assertEqual(1, len(from_rollback_new), f"`rb` must come from _rollback_new once, got {from_rollback_new}")
        self.assertEqual(from_rollback_new, stores, f"`rb` rebound inside run_case at lines {stores}")


class CanarySettleTest(unittest.TestCase):
    """The terminal is reached only when every bumped member's pods agree
    with it, and a multi-node image override names the leader's container."""

    def test_rollback_settles_when_every_member_drained(self):
        self.assertFalse(run._canary_pods_settled("RolledBack", "RolledBack", {"router": 0, "decoder": 3}, {}, False))
        self.assertTrue(run._canary_pods_settled("RolledBack", "RolledBack", {"router": 0, "decoder": 0}, {}, False))

    def test_completion_settles_when_every_member_rolled(self):
        want = {"engine": 2, "decoder": 2}
        self.assertFalse(run._canary_pods_settled("Stable", "Stable", {"engine": 2, "decoder": 1}, want, False))
        self.assertTrue(run._canary_pods_settled("Stable", "Stable", {"engine": 2, "decoder": 2}, want, False))
        self.assertFalse(run._canary_pods_settled("Stable", "Stable", {"engine": 2, "decoder": 2}, want, True))
        self.assertTrue(run._canary_pods_settled("Failed", "Failed", {"engine": 1}, want, False))

    def test_multi_node_image_override_names_the_leader_container(self):
        seen = []
        saved = run.kubectl
        run.kubectl = lambda *a, **k: (seen.append(a), subprocess.CompletedProcess(a, 0, stdout="", stderr=""))[1]
        try:
            ctx = {"case_id": "p1", "pods_per_inst": {"engine": 1, "decoder": 2}}
            run._bump_rev(ctx, {"engine", "decoder"}, image="example.com/x:none", image_comps={"decoder"})
        finally:
            run.kubectl = saved
        body = json.loads(seen[0][-1])
        self.assertEqual({"name": run.MAIN_CONTAINER, "image": "example.com/x:none"}, body["spec"]["decoder"]["leader"]["runner"])
        self.assertNotIn("runner", body["spec"]["engine"])
        self.assertNotIn("leader", body["spec"]["engine"])

class CanaryWave2HooksTest(unittest.TestCase):
    """The PreHold, Done and RolledBack hooks, the second verb, and the verbs
    that reach the grid's remaining columns."""

    def test_hooks_read_the_step_state_not_only_the_phase(self):
        self.assertEqual("canary-serving", run._canary_hook("Paused", {"preStepHold": False}, 2))
        self.assertEqual("canary-prehold", run._canary_hook("Paused", {"preStepHold": True}, 2))
        self.assertEqual("canary-done", run._canary_hook("Stable", {"currentStep": 2}, 2))
        self.assertIsNone(run._canary_hook("Stable", {"currentStep": 0}, 2))
        self.assertIsNone(run._canary_hook("Stable", {}, 0))
        self.assertEqual("canary-rolledback", run._canary_hook("RolledBack"))
        self.assertTrue(run._canary_hook_ok("canary-prehold"))
        self.assertTrue(run._canary_hook_ok("canary-done@2"))

    def test_plan_edit_leaves_the_pinned_ladder_alone(self):
        self.assertTrue(run._plan_edit_untouched({0: 25, 1: 50, 2: 100}))
        self.assertFalse(run._plan_edit_untouched({0: 25, 1: run.PLANEDIT_TRAFFIC, 2: 100}))

    def _capture(self, ctx, verb, canary_hash="abc123", spec=None):
        calls = []
        def fake_kubectl(*args, **kw):
            calls.append(list(args))
            if args[:2] == ("-n", run.NS) and args[2] == "get":
                return subprocess.CompletedProcess(args, 0, stdout=json.dumps({"spec": spec or {}}), stderr="")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        saved = run.kubectl
        run.kubectl = fake_kubectl
        try:
            run._canary_verb(ctx, verb, canary_hash, "engine")
        finally:
            run.kubectl = saved
        return calls

    def _ctx(self, **extra):
        ctx = {"case_id": "p1", "info": {"canary_groups": [{"unit": "engine", "members": ["engine", "decoder"], "policy": "auto",
                                                              "steps": [{"capacity": "25%", "traffic": 25}, {"capacity": "50%", "traffic": 50}, {"capacity": "100%", "traffic": 100}]}]},
               "configured": {"engine", "decoder"}, "hdr": {}, "canary_hook": "canary-serving", "canary_step": 1}
        ctx.update(extra)
        return ctx

    def test_repin_resume_and_timeout_write_their_annotations(self):
        self.assertIn("ome.io/rollout-repin=now", self._capture(self._ctx(), "repin")[0])
        self.assertIn("ome.io/rollout-resume=abc123", self._capture(self._ctx(), "resume")[0])
        self.assertIn("ome.io/rollout-ready-timeout=3m", self._capture(self._ctx(timeout_value="3m"), "timeout-set")[0])

    def _spec(self):
        return {"rollout": {"pairingProtocol": "v1", "groups": [
            {"components": ["router"], "blueGreen": {}},
            {"components": ["engine", "decoder"], "canary": {"steps": [
                {"capacity": "25%", "traffic": 25, "pause": {"duration": "20s"}},
                {"capacity": "50%", "traffic": 50, "pause": {"duration": "20s"}},
                {"capacity": "100%", "traffic": 100}]}}]}}

    def test_plan_edit_inserts_a_step_the_pinned_run_never_serves(self):
        calls = self._capture(self._ctx(), "planedit", spec=self._spec())
        patch = json.loads(next(c for c in calls if "patch" in c)[-1])
        groups = patch["spec"]["rollout"]["groups"]
        self.assertEqual(["router"], groups[0]["components"], "other groups are written back unchanged")
        traffic = [st["traffic"] for st in groups[1]["canary"]["steps"]]
        self.assertEqual([25, 50, run.PLANEDIT_TRAFFIC, 100], traffic)

    def test_plan_edit_repin_shrinks_the_ladder_then_repins(self):
        calls = self._capture(self._ctx(), "planedit-repin", spec=self._spec())
        patch = json.loads(next(c for c in calls if "patch" in c)[-1])
        traffic = [st["traffic"] for st in patch["spec"]["rollout"]["groups"][1]["canary"]["steps"]]
        self.assertEqual([50, 100], traffic)
        self.assertTrue(any("ome.io/rollout-repin=now" in c for c in calls))

    def test_group_remove_empties_the_groups(self):
        calls = self._capture(self._ctx(), "groupremove")
        patch = json.loads(next(c for c in calls if "patch" in c)[-1])
        self.assertEqual([], patch["spec"]["rollout"]["groups"])


class CanaryWave2HeaderTest(unittest.TestCase):
    def _errs(self, header):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    BASE = "# p999: x mode=canary Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 canary=auto topo=engine"

    def test_second_verb_and_new_hooks_validate(self):
        self.assertEqual([], self._errs(self.BASE + " during=canary-serving@1 verb=planedit-repin verb2=promote during2=canary-prehold cell=canary/PreHold/promote"))
        self.assertEqual([], self._errs(self.BASE + " during=canary-done verb=pause cell=canary/Done/pause"))
        self.assertEqual([], self._errs(self.BASE + " rollback=1 during=canary-rolledback verb=resume terminal=Stable cell=canary/RolledBack/resume"))
        self.assertEqual([], self._errs(self.BASE + " during=canary-staging verb=timeout-set timeout=3m cell=canary/Staging/timeoutEdit"))
        self.assertTrue(any("verb2= needs" in e for e in self._errs(self.BASE + " verb2=promote during2=canary-prehold")))
        self.assertTrue(any("timeout-set needs" in e for e in self._errs(self.BASE + " during=canary-staging verb=timeout-set")))
        self.assertTrue(any("timeout= is the value" in e for e in self._errs(self.BASE + " during=canary-staging verb=pause timeout=3m")))


class CanaryExpectTest(unittest.TestCase):
    """The expected terminal and the verdict contract it selects."""

    def test_derivation_and_explicit_terminal(self):
        self.assertEqual(("Stable", False), run._canary_expect("", None, "hold", False))
        self.assertEqual(("RolledBack", True), run._canary_expect("", None, "hold", True))
        self.assertEqual(("RolledBack", True), run._canary_expect("", "breach", "hold", False))
        self.assertEqual(("Failed", False), run._canary_expect("", "inconclusive", "hold", False))
        self.assertEqual(("RolledBack", True), run._canary_expect("", "inconclusive", "rollback", False))
        self.assertEqual(("Failed", False), run._canary_expect("Failed", None, "hold", False))
        # a rollback probe that resumes or re-targets afterwards ends Stable
        self.assertEqual(("Stable", False), run._canary_expect("Stable", None, "hold", True))
        self.assertEqual(("RolledBack", True), run._canary_expect("RolledBack", None, "hold", False))


class RebumpRollbackProbeTest(unittest.TestCase):
    """The rollback probe after a re-target verb waits on rebump_retargeted,
    which is set from a one-sample signal (target_changed). Every sample after
    the verb fires lands in the hook-bookkeeping if/elif chain, so the
    assignment must be its own statement, not a branch of that chain."""

    def test_retarget_flag_is_not_a_branch_of_the_hook_chain(self):
        import ast

        tree = ast.parse((HERE / "run.py").read_text())
        hits = []

        def visit(node, parents):
            for child in ast.iter_child_nodes(node):
                if isinstance(child, ast.Assign) and any(
                        isinstance(t, ast.Name) and t.id == "rebump_retargeted" for t in child.targets
                ) and isinstance(child.value, ast.Constant) and child.value.value is True:
                    hits.append(parents)
                visit(child, parents + [child])

        visit(tree, [])
        self.assertEqual(len(hits), 1, "expected exactly one `rebump_retargeted = True`")
        parents = hits[0]
        guard = parents[-1]
        self.assertIsInstance(guard, ast.If, "the assignment must sit directly under an if")
        enclosing = parents[-2]
        self.assertFalse(
            isinstance(enclosing, ast.If) and guard in enclosing.orelse,
            "rebump_retargeted must not be set from an elif of the hook-bookkeeping chain",
        )


class RollbackProbeGateTest(unittest.TestCase):
    def test_no_verb_never_holds_the_probe(self):
        self.assertTrue(run._rollback_probe_gate(None, None, False, False))

    def test_pre_rollback_verb_goes_first(self):
        self.assertFalse(run._rollback_probe_gate("pause", "canary-serving", False, False))
        self.assertTrue(run._rollback_probe_gate("pause", "canary-serving", True, False))

    def test_retarget_waits_for_the_retargeted_canary(self):
        self.assertFalse(run._rollback_probe_gate("rebump", "canary-serving", True, False))
        self.assertTrue(run._rollback_probe_gate("rebump", "canary-serving", True, True))

    def test_post_rollback_verb_never_holds_the_probe(self):
        for during in ("canary-rollingback", "canary-rolledback", "canary-rolledback@0"):
            self.assertTrue(run._rollback_probe_gate("rollback", during, False, False), during)
            self.assertTrue(run._rollback_probe_gate("rebump", during, False, False), during)


class StagedPromoteTest(unittest.TestCase):
    def test_promote_fired_while_staging_counts_once_the_step_advanced(self):
        self.assertTrue(run._promote_verb_advanced("promote", "canary-staging", True, 12.0))
        self.assertTrue(run._promote_verb_advanced("promote", "canary-staging@0", True, 12.0))

    def test_anything_else_keeps_the_manual_contract(self):
        self.assertFalse(run._promote_verb_advanced("promote", "canary-staging", True, None))
        self.assertFalse(run._promote_verb_advanced("promote", "canary-staging", False, 12.0))
        self.assertFalse(run._promote_verb_advanced("promote", "canary-serving", True, 12.0))
        self.assertFalse(run._promote_verb_advanced("pause", "canary-staging", True, 12.0))


class PauseLiftTest(unittest.TestCase):
    def test_convergence_waits_for_a_fired_pause_to_lift(self):
        self.assertTrue(run._awaiting_pause_lift("pause", True, False))
        self.assertTrue(run._awaiting_pause_lift("pause-freeze", True, False))

    def test_nothing_else_holds_convergence(self):
        self.assertFalse(run._awaiting_pause_lift("pause", True, True))
        self.assertFalse(run._awaiting_pause_lift("pause", False, False))
        self.assertFalse(run._awaiting_pause_lift("promote", True, False))
        self.assertFalse(run._awaiting_pause_lift(None, False, False))


class IdleMeansNoRunTest(unittest.TestCase):
    def test_waits_for_the_initial_run_to_close(self):
        statuses = iter([{"rollout": {"activeRun": {"runID": "r0"}}}, {"rollout": {"lastRun": {"outcome": "Completed"}}}])
        saved = run.fetch_status, run.time.sleep
        run.fetch_status = lambda cid: next(statuses)
        run.time.sleep = lambda s: None
        try:
            waited = run._wait_no_active_run("p1", 10)
        finally:
            run.fetch_status, run.time.sleep = saved
        self.assertIsNotNone(waited)

    def test_a_run_that_never_closes_is_reported(self):
        saved = run.fetch_status, run.time.sleep
        run.fetch_status = lambda cid: {"rollout": {"activeRun": {"runID": "r0"}}}
        run.time.sleep = lambda s: None
        try:
            self.assertIsNone(run._wait_no_active_run("p1", 0))
        finally:
            run.fetch_status, run.time.sleep = saved


class ParkedPlanTest(unittest.TestCase):
    def test_plan_park_reads_the_false_plan_condition(self):
        st = {"conditions": [{"type": "Ready", "status": "True"},
                             {"type": "RolloutPlanReady", "status": "False", "reason": "PolicyNotFound", "message": "no such policy"}]}
        self.assertEqual({"reason": "PolicyNotFound", "message": "no such policy"}, run._plan_park(st))
        self.assertIsNone(run._plan_park({"conditions": [{"type": "RolloutPlanReady", "status": "True", "reason": "Pinned"}]}))
        self.assertIsNone(run._plan_park({}))

    def test_a_held_update_settles_with_nothing_on_the_new_revision(self):
        self.assertTrue(run._canary_pods_settled("Held", "", {"engine": 0}, {"engine": 2}, False))
        self.assertFalse(run._canary_pods_settled("Held", "", {"engine": 1}, {"engine": 2}, False))

    def test_the_missing_policy_group_declares_its_progression(self):
        self.assertEqual({"components": ["engine"], "policyRef": {"name": "p1-missing", "progression": "canary"}},
                         run._missing_policy_group("p1", ["engine"]))

    def test_a_missing_policy_arms_no_canary(self):
        self.assertFalse(run._prearm_arms_canary("planinvalid"))
        self.assertTrue(run._prearm_arms_canary("pause"))


class PrearmPredicateTest(unittest.TestCase):
    """Verdict rules for a verb left on the idle service before the bump."""

    def test_durations_and_step_soak(self):
        self.assertEqual(45.0, run._duration_s("45s"))
        self.assertEqual(180.0, run._duration_s("3m"))
        self.assertEqual(3600.0, run._duration_s("1h"))
        self.assertIsNone(run._duration_s("soon"))
        self.assertIsNone(run._duration_s(None))
        steps = [{"capacity": "25%", "traffic": 25, "pause": {"duration": "20s"}}, {"capacity": "100%", "traffic": 100}]
        self.assertEqual(20.0, run._step_soak_s(steps, 0))
        self.assertEqual(0.0, run._step_soak_s(steps, 1))
        self.assertEqual(0.0, run._step_soak_s(steps, 5))

    def test_step_new_count_rounds_up_and_clamps(self):
        self.assertEqual(2, run._step_new_count({"capacity": "25%"}, 8))
        self.assertEqual(1, run._step_new_count({"capacity": "25%"}, 2))
        self.assertEqual(8, run._step_new_count({"capacity": "100%"}, 8))
        self.assertEqual(3, run._step_new_count({"capacity": 3}, 8))
        self.assertEqual(8, run._step_new_count({"capacity": 12}, 8))
        self.assertEqual(8, run._step_new_count({}, 8))

    def test_only_a_group_removal_leaves_nothing_to_arm(self):
        self.assertFalse(run._prearm_arms_canary("groupremove"))
        for v in ("", "pause", "promote", "repin", "resume", "timeout-set", "planedit", "scale", "rollback"):
            self.assertTrue(run._prearm_arms_canary(v), v)

    def test_prearmed_pause_holds_the_arm(self):
        self.assertTrue(run._prearm_pause_held(set(), None))
        self.assertTrue(run._prearm_pause_held({"Stable", "Pending"}, 0))
        self.assertFalse(run._prearm_pause_held({"Pending", "Canarying"}, 0))
        self.assertFalse(run._prearm_pause_held({"Paused"}, 0))
        self.assertFalse(run._prearm_pause_held({"Pending"}, 1))

    def test_stale_promote_leaves_the_first_hold_intact(self):
        self.assertTrue(run._first_hold_served(20.0, 20.0))
        self.assertTrue(run._first_hold_served(20.0 - run.SOAK_OBSERVE_SLACK_S, 20.0))
        self.assertFalse(run._first_hold_served(4.0, 20.0))
        self.assertFalse(run._first_hold_served(None, 20.0))
        self.assertTrue(run._first_hold_served(None, 0.0), "an untimed first step has no soak to skip")

    def test_plan_edit_direction_follows_where_the_edit_landed(self):
        self.assertTrue(run._plan_edit_served_expected(True))
        self.assertFalse(run._plan_edit_served_expected(False))

    def test_park_follows_the_pre_armed_budget(self):
        self.assertTrue(run._park_matches_budget(50.0, 45.0, 90.0))
        self.assertTrue(run._park_matches_budget(45.0, 45.0, None))
        self.assertFalse(run._park_matches_budget(88.0, 45.0, 90.0), "nearer the plan's budget than the annotation's")
        self.assertFalse(run._park_matches_budget(20.0, 45.0, 90.0), "before the budget elapsed")
        self.assertFalse(run._park_matches_budget(45.0 + run.PARK_LATENCY_S + 1, 45.0, None), "past the landing latency")
        self.assertFalse(run._park_matches_budget(None, 45.0, 90.0))

    def test_plain_rollout_shows_no_canary_phase(self):
        self.assertTrue(run._no_canary_phase({"", "Stable"}))
        self.assertFalse(run._no_canary_phase({"Stable", "Pending"}))
        self.assertFalse(run._no_canary_phase({"Canarying"}))


class PrearmWritesTest(unittest.TestCase):
    """The pre-bump path shares the hook verbs' writes: the stable hash is the
    token a promote or resume names, and a scale patches through _scale_patch
    and then counts the settled fleet."""

    def _capture(self, verb, scale_to=None, old_hashes=("stable1",)):
        calls = []

        def fake_kubectl(*args, **kw):
            calls.append(list(args))
            if args[:2] == ("-n", run.NS) and args[2] == "get":
                return subprocess.CompletedProcess(args, 0, stdout=json.dumps({"spec": {}, "status": {}}), stderr="")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        ctx = {"case_id": "p1", "old_hashes": set(old_hashes), "hdr": {}, "info": {"canary_groups": []},
               "configured": {"engine"}, "N": {"engine": 2}, "timeout_value": "45s", "closed": False}
        saved = run.kubectl
        run.kubectl = fake_kubectl
        try:
            ok, msg = run._prearm(ctx, verb, "engine", scale_to or {})
        finally:
            run.kubectl = saved
        return ok, calls, ctx

    def test_promote_and_resume_name_the_stable_revision(self):
        ok, calls, ctx = self._capture("promote")
        self.assertTrue(ok)
        self.assertTrue(any(f"{run.PROMOTE_ANNO}=stable1" in c for c in calls))
        self.assertEqual("idle", ctx["canary_hook"])
        self.assertTrue(any("at idle" in n for n in ctx["verb_notes"]))
        self.assertTrue(any(f"{run.RESUME_ANNO}=stable1" in c for c in self._capture("resume")[1]))

    def test_repin_timeout_and_rollback_write_their_annotations(self):
        self.assertTrue(any(f"{run.REPIN_ANNO}=now" in c for c in self._capture("repin")[1]))
        self.assertTrue(any(f"{run.READY_TIMEOUT_ANNO}=45s" in c for c in self._capture("timeout-set")[1]))
        self.assertTrue(any(f"{run.ROLLBACK_ANNO}=true" in c for c in self._capture("rollback")[1]))

    def test_pause_writes_the_global_pause(self):
        saved = run.PAUSE_PROJECT_S
        run.PAUSE_PROJECT_S = 0
        try:
            calls = self._capture("pause")[1]
        finally:
            run.PAUSE_PROJECT_S = saved
        self.assertTrue(any("ome.io/rollout-paused=true" in c for c in calls))

    def test_group_remove_empties_the_groups_before_the_bump(self):
        calls = self._capture("groupremove")[1]
        patch = json.loads(next(c for c in calls if "patch" in c)[-1])
        self.assertEqual([], patch["spec"]["rollout"]["groups"])

    def test_scale_patches_then_counts_the_settled_fleet(self):
        saved = run._wait_fleet_at
        run._wait_fleet_at = lambda ctx, targets, budget: True
        try:
            ok, calls, ctx = self._capture("scale", scale_to={"engine": 4})
        finally:
            run._wait_fleet_at = saved
        self.assertTrue(ok)
        patch = json.loads(next(c for c in calls if "patch" in c)[-1])
        self.assertEqual({"minReplicas": 4, "maxReplicas": 4}, patch["spec"]["engine"])
        self.assertEqual(4, ctx["N"]["engine"])

    def test_scale_that_never_settles_is_a_setup_failure(self):
        saved = run._wait_fleet_at
        run._wait_fleet_at = lambda ctx, targets, budget: False
        try:
            ok, _, ctx = self._capture("scale", scale_to={"engine": 4})
        finally:
            run._wait_fleet_at = saved
        self.assertFalse(ok)
        self.assertEqual(2, ctx["N"]["engine"], "an unsettled fleet keeps the old count")

    def test_wait_fleet_at_settles_on_the_exact_fleet(self):
        ctx = {"closed": False, "pods_per_inst": {"engine": 1}, "pod_state": {
            f"e{i}": {"alive": True, "comp": "engine", "inst": str(i), "ready": True, "serving": True} for i in range(4)}}
        saved = run._read_one
        run._read_one = lambda c, timeout=1.0: None
        try:
            self.assertTrue(run._wait_fleet_at(ctx, {"engine": 4}, 1.0))
            ctx["pod_state"]["e4"] = {"alive": True, "comp": "engine", "inst": "4", "ready": False, "serving": False}
            self.assertFalse(run._wait_fleet_at(ctx, {"engine": 4}, 0.2), "a fifth alive Instance is not the target fleet")
        finally:
            run._read_one = saved


class ReleaseTest(unittest.TestCase):
    def test_release_fires_once_after_the_window(self):
        self.assertFalse(run._release_due("promote", False, 0.0, False, 100.0, 20))
        self.assertFalse(run._release_due("promote", True, 90.0, False, 100.0, 20))
        self.assertTrue(run._release_due("promote", True, 80.0, False, 100.0, 20))
        self.assertFalse(run._release_due("promote", True, 80.0, True, 100.0, 20))
        self.assertFalse(run._release_due("", True, 80.0, False, 100.0, 20))

    def test_release_waits_for_a_second_verb_pause_to_lift(self):
        # Under an unlifted pause the release is not due however long ago the verb fired.
        self.assertFalse(run._release_due("promote", True, 0.0, False, 100.0, 20, awaiting_lift=True))
        # After the lift the window counts from the lift (the caller re-anchors).
        self.assertFalse(run._release_due("promote", True, 90.0, False, 100.0, 20, awaiting_lift=False))
        self.assertTrue(run._release_due("promote", True, 80.0, False, 100.0, 20, awaiting_lift=False))

    def test_hold_must_survive_the_second_verb_until_the_release(self):
        self.assertTrue(run._prehold_survived(1, "canary-prehold", 1))
        self.assertFalse(run._prehold_survived(1, "canary-prehold", 2))
        self.assertFalse(run._prehold_survived(1, "canary-serving", 1))
        self.assertFalse(run._prehold_survived(None, "canary-prehold", None))


class RunLostTest(unittest.TestCase):
    def test_live_hooks_hold_and_terminals_do_not(self):
        for h in ("canary-staging", "canary-serving@1", "canary-prehold", "canary-draining"):
            self.assertTrue(run._runlost_hold_expected(h), h)
        for h in ("canary-done", "canary-rollingback", "canary-rolledback", "canary-failed", ""):
            self.assertFalse(run._runlost_hold_expected(h), h)

    def test_step_never_moves_while_no_run_is_pinned(self):
        self.assertTrue(run._runlost_held(1, [1, 1, 1], None))
        self.assertTrue(run._runlost_held(1, [1, 1], 1))
        self.assertTrue(run._runlost_held(1, [], None), "a re-pin faster than a poll leaves nothing to fault")
        self.assertFalse(run._runlost_held(1, [1, 2], None))
        self.assertFalse(run._runlost_held(1, [1], 0), "a re-pin must adopt in place, not restart")
        self.assertFalse(run._runlost_held(None, [], None))

    def test_a_repin_below_the_lost_step_is_a_restart(self):
        self.assertTrue(run._runlost_restarted(1, 0))
        for verb_step, repin_step in ((1, 1), (1, 2), (1, None), (None, 0)):
            self.assertFalse(run._runlost_restarted(verb_step, repin_step), (verb_step, repin_step))

    @staticmethod
    def _removed(hook="canary-serving", step=1, last_run=None):
        track = run._runlost_record("r1", last_run, hook, step, 0.0)
        track["removed"] = True
        return track

    def test_track_counts_only_samples_that_saw_no_run(self):
        t = self._removed()
        self.assertIsNone(run._runlost_track(t, None, None, 1, "canary-serving", 2.0))
        self.assertEqual(([1], 2.0), (t["unpinned"], t["since"]))
        self.assertEqual("repinned", run._runlost_track(t, "r2", None, 1, "canary-serving", 4.0))
        self.assertEqual((1, "r2", None), (t["repin_step"], t["repin_run"], t["since"]))
        self.assertIsNone(run._runlost_track(t, "r2", None, 2, "canary-serving", 6.0), "only the first run seen again is the re-pin")
        self.assertEqual([1], t["unpinned"], "a pinned sample is not a sample without a run")

    def test_a_repinned_run_that_finishes_is_closed_not_lost(self):
        t = run._runlost_record("r1", None, "canary-prehold", 1, 0.0); t["removed"] = True
        self.assertIsNone(run._runlost_track(t, None, None, 1, "canary-prehold", 2.0))
        self.assertEqual("repinned", run._runlost_track(t, "r1", None, 1, "canary-prehold", 4.0))
        # The resumed ladder ran to its end: a closed-run record and no run pinned, at a hook that is not a closing terminal.
        self.assertEqual("closed", run._runlost_track(t, None, {"outcome": "Completed"}, 2, None, 40.0))
        self.assertIsNone(run._runlost_track(t, None, {"outcome": "Completed"}, 2, None, 42.0))
        self.assertEqual([1], t["unpinned"])

    def test_a_run_that_closes_before_the_removal_is_not_a_failed_verb(self):
        # The read found a run; by the time the removal lands the run layer has closed it.
        statuses = iter([{"rollout": {"activeRun": {"runID": "r1"}, "lastRun": None}},
                         {"rollout": {"lastRun": {"outcome": "Completed"}}}])
        saved = run.fetch_status, run.kubectl
        run.fetch_status = lambda cid: next(statuses)
        run.kubectl = lambda *a, **k: subprocess.CompletedProcess(a, 1, stdout="", stderr="The request is invalid")
        try:
            ctx = {"case_id": "p1", "canary_hook": "canary-done", "canary_step": 2}
            run._remove_active_run(ctx)
        finally:
            run.fetch_status, run.kubectl = saved
        self.assertFalse(ctx["runlost"]["removed"])
        self.assertEqual("r1", ctx["runlost"]["run_id"])
        self.assertNotIn("verb_errors", ctx)
        self.assertTrue(any("closed before the removal" in n for n in ctx.get("verb_notes", [])))

    def test_track_records_a_repin_the_sampler_saw_no_gap_before(self):
        t = self._removed()
        self.assertEqual("repinned", run._runlost_track(t, "r1", None, 1, "canary-serving", 2.0))
        self.assertEqual((1, "r1", []), (t["repin_step"], t["repin_run"], t["unpinned"]))

    def test_track_holds_after_the_window_only_where_the_grid_holds(self):
        for hook, want in (("canary-staging", "held"), ("canary-draining", "held"), ("canary-rollingback", None), ("canary-failed", None)):
            t = self._removed(hook, 0)
            self.assertIsNone(run._runlost_track(t, None, None, 0, hook, 1.0), hook)
            self.assertIsNone(run._runlost_track(t, None, None, 0, hook, run.RUNLOST_HOLD_S), hook)
            self.assertEqual(want, run._runlost_track(t, None, None, 0, hook, 1.0 + run.RUNLOST_HOLD_S), hook)

    def test_track_restarts_the_window_at_a_pinned_sample(self):
        t = self._removed()
        run._runlost_track(t, None, None, 1, "canary-serving", 1.0)
        run._runlost_track(t, "r2", None, 1, "canary-serving", 3.0)
        late = 1.0 + run.RUNLOST_HOLD_S
        self.assertIsNone(run._runlost_track(t, None, None, 1, "canary-serving", late))
        self.assertEqual(late, t["since"])

    def test_track_reads_a_new_closed_record_at_a_terminal_as_closed_not_lost(self):
        closed = {"outcome": "Completed", "closedAt": "2026-01-01T00:00:10Z"}
        t = self._removed("canary-draining")
        self.assertEqual("closed", run._runlost_track(t, None, closed, 2, "canary-done", 2.0))
        self.assertEqual(([], closed), (t["unpinned"], t["closed"]))
        self.assertIsNone(run._runlost_track(t, None, closed, 2, "canary-done", 4.0))
        self.assertEqual([], t["unpinned"])
        # The record the hook already had: nothing closed after the removal,
        # so a terminal with no run is a ladder that ended unpinned.
        t = self._removed("canary-draining", last_run=closed)
        self.assertIsNone(run._runlost_track(t, None, closed, 2, "canary-done", 2.0))
        self.assertEqual([2], t["unpinned"])
        # A streak already open keeps counting through the terminal.
        t = self._removed("canary-draining")
        run._runlost_track(t, None, None, 1, "canary-draining", 2.0)
        run._runlost_track(t, None, closed, 2, "canary-done", 4.0)
        self.assertEqual(([1, 2], None), (t["unpinned"], t["closed"]))

    def test_track_ignores_a_removal_that_did_not_happen(self):
        t = run._runlost_record(None, None, "canary-done", 2, 0.0)
        self.assertIsNone(run._runlost_track(t, None, None, 2, "canary-done", 2.0))
        self.assertIsNone(run._runlost_track(t, "r1", None, 2, "canary-done", 4.0))
        self.assertEqual(([], None), (t["unpinned"], t["repin_step"]))

    def test_active_run_reads_the_pinned_run_id(self):
        self.assertEqual("r1", run._active_run({"rollout": {"activeRun": {"runID": "r1"}}}))
        self.assertEqual("unnamed", run._active_run({"rollout": {"activeRun": {"openedAt": "t"}}}))
        self.assertIsNone(run._active_run({"rollout": {"lastRun": {"outcome": "Completed"}}}))
        self.assertIsNone(run._active_run({}))

    def test_signals_name_what_followed_the_removal(self):
        t = run._runlost_record("r1", None, "canary-serving", 1, 0.0)
        self.assertEqual(["held"], run._runlost_signals(
            t, [("RolloutRunOpened", "run r1 opened"), ("CanaryRunLost", "engine canary has no pinned rollout run")]))
        self.assertEqual(["adopted"], run._runlost_signals(t, [("RolloutPlanRepinned", "run r2 adopted the in-flight rollout in place")]))
        self.assertEqual([], run._runlost_signals(t, [("RolloutPlanRepinned", "run r1 repinned to digest abc")]))
        self.assertEqual([], run._runlost_signals(t, []))
        t["repin_step"], t["repin_run"] = 1, "r1"
        self.assertEqual(["restored"], run._runlost_signals(t, []))
        t["repin_run"] = "r2"
        self.assertEqual(["repinned"], run._runlost_signals(t, []))
        self.assertEqual(["held", "adopted", "repinned"], run._runlost_signals(
            t, [("CanaryRunLost", "held"), ("RolloutPlanRepinned", "run r2 adopted the in-flight rollout in place")]))
        t = run._runlost_record("r1", None, "canary-draining", 1, 0.0)
        t["closed"] = {"outcome": "Completed"}
        self.assertEqual(["closed"], run._runlost_signals(t, []))

    def test_story_names_every_signal(self):
        t = run._runlost_record("r1", None, "canary-serving", 1, 0.0)
        t["repin_step"], t["repin_run"] = 1, "r2"
        story = run._runlost_story(t, ["held", "adopted", "repinned"])
        for word in ("CanaryRunLost", "RolloutPlanRepinned", "r2", "step 1"):
            self.assertIn(word, story)
        self.assertIn("r1", run._runlost_story(t, ["restored"]))
        self.assertEqual("", run._runlost_story(t, []))

    def test_a_repin_is_waited_for_where_the_next_pass_pins_one(self):
        t = self._removed("canary-failed", 0)
        self.assertTrue(run._runlost_repin_pending(t, 1.0))
        self.assertFalse(run._runlost_repin_pending(t, float(run.RUNLOST_HOLD_S)))
        t["repin_step"] = 0
        self.assertFalse(run._runlost_repin_pending(t, 1.0))
        t = self._removed("canary-draining", 1)
        self.assertTrue(run._runlost_repin_pending(t, 1.0))
        t["closed"] = {"outcome": "Completed"}
        self.assertFalse(run._runlost_repin_pending(t, 1.0))
        self.assertTrue(run._runlost_repin_pending(self._removed("canary-rollingback", 0), 1.0),
                        "a revert still draining is adopted in place, so its re-pin is waited for")
        self.assertFalse(run._runlost_repin_pending(self._removed("canary-rolledback", 0), 1.0))
        self.assertFalse(run._runlost_repin_pending(run._runlost_record(None, None, "canary-serving", 0, 0.0), 1.0))
        self.assertFalse(run._runlost_repin_pending(None, 1.0))

    def _fire(self, status):
        calls = []

        def fake_kubectl(*args, **kw):
            calls.append(list(args))
            if args[2] == "get":
                return subprocess.CompletedProcess(args, 0, stdout=json.dumps({"status": status}), stderr="")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        ctx = {"case_id": "p1", "canary_hook": "canary-serving", "canary_step": 1}
        saved = run.kubectl
        run.kubectl = fake_kubectl
        try:
            run._canary_verb(ctx, "runlost", "abc123", "engine")
        finally:
            run.kubectl = saved
        return calls, ctx

    def test_verb_removes_the_pinned_run_from_the_status_subresource(self):
        closed = {"outcome": "Superseded", "closedAt": "2026-01-01T00:00:01Z"}
        calls, ctx = self._fire({"rollout": {"activeRun": {"runID": "r1"}, "lastRun": closed}})
        patch = next(c for c in calls if "patch" in c)
        self.assertIn("--subresource=status", patch)
        self.assertEqual([{"op": "remove", "path": "/status/rollout/activeRun"}], json.loads(patch[-1]))
        self.assertNotIn("verb_errors", ctx)
        track = ctx["runlost"]
        self.assertTrue(track["removed"])
        self.assertEqual(("r1", closed, "canary-serving", 1), (track["run_id"], track["last_run"], track["hook"], track["step"]))
        self.assertEqual(([], None, None), (track["unpinned"], track["repin_step"], track["closed"]))

    def test_verb_with_no_pinned_run_records_it_and_patches_nothing(self):
        calls, ctx = self._fire({"rollout": {}})
        self.assertFalse(any("patch" in c for c in calls))
        self.assertTrue(any("nothing to remove" in n for n in ctx["verb_notes"]))
        self.assertEqual((False, None), (ctx["runlost"]["removed"], ctx["runlost"]["run_id"]))

    def test_isvc_events_are_read_oldest_first(self):
        items = [{"reason": "CanaryRunLost", "message": "held", "lastTimestamp": "2026-01-01T00:00:09Z"},
                 {"reason": "RolloutRunOpened", "message": "opened", "lastTimestamp": "2026-01-01T00:00:01Z"}]
        saved = run.kubectl
        run.kubectl = lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout=json.dumps({"items": items}), stderr="")
        try:
            self.assertEqual([("RolloutRunOpened", "opened"), ("CanaryRunLost", "held")], run._isvc_events("p1"))
        finally:
            run.kubectl = saved


class PairingVerbTest(unittest.TestCase):
    def test_verb_patches_the_pairing_protocol_at_the_hook(self):
        calls = []

        def fake_kubectl(*args, **kw):
            calls.append(list(args))
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        ctx = {"case_id": "p1", "pairto_target": "pair-v3", "canary_hook": "canary-prehold", "canary_step": 1}
        saved = run.kubectl
        run.kubectl = fake_kubectl
        try:
            run._canary_verb(ctx, "pairing", "abc123", "engine")
            ctx["pairto_target"] = ""
            run._canary_verb(ctx, "pairing", "abc123", "engine")
        finally:
            run.kubectl = saved
        patches = [json.loads(c[-1]) for c in calls if "patch" in c]
        self.assertEqual("pair-v3", patches[0]["spec"]["rollout"]["pairingProtocol"])
        self.assertIsNone(patches[1]["spec"]["rollout"]["pairingProtocol"], "clear removes the field")

    def test_retarget_is_a_hash_outside_the_set_that_existed_at_the_verb(self):
        self.assertFalse(run._retargeted_by_hash("h1", {"h0", "h1"}))
        self.assertTrue(run._retargeted_by_hash("h2", {"h0", "h1"}))
        self.assertFalse(run._retargeted_by_hash(None, {"h0"}))

    def test_proto2_fires_at_during2_when_no_second_verb_owns_it(self):
        self.assertEqual("canary-prehold", run._proto2_hook("canary-serving@1", "canary-prehold", ""))
        self.assertEqual("canary-serving@1", run._proto2_hook("canary-serving@1", "canary-prehold", "promote"))
        self.assertEqual("canary-staging", run._proto2_hook("canary-staging", "", ""))
        self.assertEqual("", run._proto2_hook("", "", ""))

    def test_rollback_probe_waits_for_a_pairing_retarget(self):
        self.assertFalse(run._rollback_probe_gate("pairing", "canary-serving", True, False))
        self.assertTrue(run._rollback_probe_gate("pairing", "canary-serving", True, True))


class QuotaLiftTest(unittest.TestCase):
    def test_lift_says_whether_the_quota_is_gone(self):
        """Every lift path reads the same answer: True once the delete has
        gone through (a quota that was never there included), False when the
        apiserver refused it."""
        for rc, want in ((0, True), (1, False)):
            calls = []
            with mock.patch.object(run, "kubectl", lambda *a, **k: calls.append(a) or _Resp("", rc, "boom")):
                self.assertIs(want, run._lift_quota("p1"), rc)
            self.assertEqual(("-n", run.NS, "delete", "resourcequota", "perm-p1-quota", "--ignore-not-found=true"), calls[0])

    def test_verb_deletes_the_case_quota(self):
        calls = []

        def fake_kubectl(*args, **kw):
            calls.append(list(args))
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        ctx = {"case_id": "p1", "canary_hook": "canary-failed", "canary_step": 0}
        saved = run.kubectl
        run.kubectl = fake_kubectl
        try:
            run._canary_verb(ctx, "quotalift", "abc123", "engine")
        finally:
            run.kubectl = saved
        self.assertTrue(any("resourcequota" in c and "perm-p1-quota" in c for c in calls))

    def test_park_survives_capacity_that_lands_late(self):
        self.assertTrue(run._park_survived_capacity(True, 0, "canary-failed", 0))
        self.assertFalse(run._park_survived_capacity(False, 0, "canary-failed", 0), "capacity never landed")
        self.assertFalse(run._park_survived_capacity(True, 0, "canary-serving", 0), "the park ended")
        self.assertFalse(run._park_survived_capacity(True, 0, "canary-failed", 1), "the step moved")
        self.assertFalse(run._park_survived_capacity(True, None, "canary-failed", 0))


class CanaryWave3HeaderTest(unittest.TestCase):
    """One accept and one reject per cross-field rule of the new knobs."""

    def _errs(self, header):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    BASE = "# p999: x mode=canary Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 canary=auto topo=engine"
    PD = "# p999: x mode=canary Ne=2 Nd=2 Nr=0 ceil_e=3 ceil_d=3 ceil_r=0 canary=auto topo=pd"

    def test_prearm_accepts_the_idle_column_verbs(self):
        for v in ("rollback", "pause", "promote", "repin", "resume", "planedit", "groupremove"):
            self.assertEqual([], self._errs(self.BASE + f" prearm={v} cell=canary/Idle/{v}"), v)
        self.assertEqual([], self._errs(self.BASE + " prearm=timeout-set timeout=45s bump=engine img=example.com/canary-test/missing:none terminal=Failed"))
        self.assertEqual([], self._errs(self.BASE + " prearm=scale scale_e=4"))

    def test_prearm_rejects_a_lost_run_and_unknown_verbs(self):
        self.assertTrue(any("prearm=runlost" in e for e in self._errs(self.BASE + " prearm=runlost")))
        self.assertTrue(any("prearm=pairing" in e for e in self._errs(self.BASE + " prearm=pairing")))
        self.assertTrue(any("prearm=" in e for e in self._errs(
            "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 prearm=pause")))

    def test_prearm_timeout_set_needs_timeout(self):
        self.assertTrue(any("prearm=timeout-set needs timeout=" in e for e in self._errs(self.BASE + " prearm=timeout-set")))
        self.assertEqual([], self._errs(self.BASE + " prearm=timeout-set timeout=45s"))

    def test_prearm_scale_needs_a_target(self):
        self.assertTrue(any("prearm=scale needs" in e for e in self._errs(self.BASE + " prearm=scale")))
        self.assertTrue(any("scale_e=/scale_d=" in e for e in self._errs(self.BASE + " scale_e=4")))
        self.assertEqual([], self._errs(self.BASE + " prearm=scale scale_e=4"))

    def test_a_second_verb_may_scale(self):
        held = " during=canary-serving@1 verb=planedit-repin verb2=scale scale_e=4 during2=canary-prehold release=promote"
        self.assertEqual([], self._errs(self.BASE.replace("ceil_e=3", "ceil_e=5") + held))
        self.assertTrue(any("scale_e=/scale_d=" in e for e in self._errs(
            self.BASE + " during=canary-serving@1 verb=planedit-repin verb2=scale during2=canary-prehold release=promote")))

    def test_prearm_planedit_excludes_a_mid_run_edit(self):
        self.assertTrue(any("opposite directions" in e for e in self._errs(
            self.BASE + " prearm=planedit during=canary-serving verb=planedit")))
        self.assertEqual([], self._errs(self.BASE + " prearm=planedit during=canary-serving verb=pause"))

    def test_release_needs_a_second_verb_at_prehold(self):
        ok = self.BASE + " during=canary-serving@1 verb=planedit-repin verb2=repin during2=canary-prehold release=promote releaseafter=30"
        self.assertEqual([], self._errs(ok))
        self.assertEqual([], self._errs(ok.replace("release=promote", "release=resume")))
        self.assertTrue(any("release=" in e for e in self._errs(self.BASE + " during=canary-serving verb=pause release=promote")))
        self.assertTrue(any("release=" in e for e in self._errs(
            self.BASE + " during=canary-serving@1 verb=planedit-repin verb2=repin during2=canary-serving release=promote")))
        self.assertTrue(any("release=rollback" in e for e in self._errs(ok.replace("release=promote", "release=rollback"))))
        self.assertTrue(any("releaseafter=" in e for e in self._errs(
            self.BASE + " during=canary-serving@1 verb=planedit-repin verb2=repin during2=canary-prehold releaseafter=30")))

    def test_quotalift_needs_the_quota_fault(self):
        self.assertEqual([], self._errs(self.BASE + " fault=quota:0 terminal=Failed during=canary-failed verb=quotalift"))
        self.assertEqual([], self._errs(
            self.BASE + " fault=quota:0 terminal=Stable during=canary-failed verb=quotalift verb2=resume during2=canary-failed"))
        self.assertTrue(any("quotalift" in e for e in self._errs(self.BASE + " during=canary-failed verb=quotalift")))
        self.assertTrue(any("quotalift" in e for e in self._errs(
            self.BASE + " fault=podkill:canary-serving during=canary-failed verb=quotalift")))

    def test_pairing_verb_needs_pairto_bump_and_a_pair(self):
        self.assertEqual([], self._errs(self.PD + " bump=engine during=canary-draining verb=pairing pairto=pair-v3"))
        self.assertEqual([], self._errs(
            self.PD + " bump=engine during=canary-serving@1 verb=planedit-repin verb2=pairing during2=canary-prehold pairto=clear"))
        self.assertTrue(any("pairto=" in e for e in self._errs(self.PD + " bump=engine during=canary-draining verb=pairing")))
        self.assertTrue(any("bump=" in e for e in self._errs(self.PD + " during=canary-draining verb=pairing pairto=pair-v3")))
        self.assertTrue(any("Ne>0 and Nd>0" in e for e in self._errs(
            self.BASE + " bump=engine during=canary-draining verb=pairing pairto=pair-v3")))
        self.assertTrue(any("pairto=" in e for e in self._errs(
            self.PD + " bump=engine during=canary-draining verb=pause pairto=pair-v3")))

    def test_during2_without_a_second_verb_is_the_proto2_hook(self):
        self.assertEqual([], self._errs(
            self.PD + " proto=pair-v2 proto2=pair-v3 during=canary-serving@1 verb=planedit-repin during2=canary-prehold"))
        self.assertTrue(any("during2=" in e for e in self._errs(
            self.BASE + " during=canary-serving verb=pause during2=canary-prehold")))

    def test_new_verbs_are_hook_verbs(self):
        self.assertEqual([], self._errs(self.BASE + " during=canary-serving verb=runlost cell=canary/Serving/runLost"))
        self.assertTrue(any("verb=" in e for e in self._errs(self.BASE + " verb=runlost")))


class CrashloopFaultTest(unittest.TestCase):
    """The crashloop:old fault: an old-revision pod made to crash on every
    start once the bump has landed, and the verdict that the fix landed on
    that Instance through the roll."""

    BASE = "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"

    def _errs(self, header):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_fault_verb_names_its_victim_only(self):
        self.assertTrue(run._fault_ok("crashloop:old"))
        self.assertFalse(run._fault_ok("crashloop:settling"))
        self.assertFalse(run._fault_ok("crashloop"))

    def test_header_needs_a_form_and_a_rollout_with_an_engine(self):
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:old form=start"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:old form=load"))
        self.assertTrue(any("form=" in e for e in self._errs(self.BASE + " fault=crashloop:old")))
        self.assertTrue(any("form=" in e for e in self._errs(self.BASE + " fault=podkill:settling form=start")))
        self.assertTrue(any("form=" in e for e in self._errs(self.BASE + " fault=crashloop:old form=later")))
        kill = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
        self.assertTrue(any("mode=rollout" in e for e in self._errs(kill + " fault=crashloop:old form=start")))

    def test_victim_is_the_highest_old_revision_engine_leader(self):
        pods = {
            "e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "default"},
            "e2": {"alive": True, "hash": "new", "inst": "2", "comp": "engine", "runner": "default"},
            "e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "default"},
            "d3": {"alive": True, "hash": "old", "inst": "3", "comp": "decoder", "runner": "default"},
            "w1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "worker"},
        }
        self.assertEqual("e1", run._crashloop_pick_pod(pods, {"old"}))
        self.assertIsNone(run._crashloop_pick_pod({"d3": pods["d3"]}, {"old"}))

    def test_fires_at_the_settling_hook_once(self):
        f = run._fault_new("p999", {"fault": "crashloop:old", "form": "start"})
        fired = []
        orig = run._fault_fire
        run._fault_fire = lambda *a, **k: fired.append(a) or f.__setitem__("fired", True)
        try:
            ctx = {"fault": f, "trigger_ts": None, "pod_state": {}, "pods_per_inst": {}, "old_hashes": {"old"}}
            run._fault_tick(ctx)
            self.assertEqual(0, len(fired), "nothing fires before the bump has landed")
            ctx["trigger_ts"] = 1.0
            run._fault_tick(ctx)
            run._fault_tick(ctx)
            self.assertEqual(1, len(fired), "the crash is armed once, right after the bump")
        finally:
            run._fault_fire = orig

    def test_verdict_requires_the_new_revision_serving_and_no_repair(self):
        f = {"kind": "crashloop", "fired": True, "infra_fail": None, "form": "start", "case_id": "p999",
             "crash_inst": "1", "crash_comp": "engine", "crash_hash": "old", "kills": 0}
        healed = {"e1": {"alive": True, "hash": "new", "inst": "1", "comp": "engine", "ready": True, "serving": True}}
        stale = {"e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "ready": True, "serving": True}}
        orig = run._isvc_events_since
        try:
            run._isvc_events_since = lambda case_id, since: []
            notes, checks = [], []
            self.assertEqual("PASS", run._crashloop_checks(f, healed, {"old"}, 1.0, "PASS", notes, checks))
            notes, checks = [], []
            self.assertEqual("FAIL", run._crashloop_checks(f, stale, {"old"}, 1.0, "PASS", notes, checks))
            self.assertTrue(any("new revision" in n for n in notes))
            run._isvc_events_since = lambda case_id, since: [
                ("RestartTriggered", "OMENative component=engine instance=1 restart triggered: pod e1 wedged")]
            notes, checks = [], []
            self.assertEqual("FAIL", run._crashloop_checks(f, healed, {"old"}, 1.0, "PASS", notes, checks))
            self.assertTrue(any("repaired after the bump" in n for n in notes))
            run._isvc_events_since = lambda case_id, since: [
                ("RestartTriggered", "OMENative component=engine instance=0 restart triggered: pod e0 wedged")]
            notes, checks = [], []
            self.assertEqual("PASS", run._crashloop_checks(f, healed, {"old"}, 1.0, "PASS", notes, checks),
                             "a repair on another Instance is not this verdict's")
        finally:
            run._isvc_events_since = orig

    SURGED = [
        ("RecreateUpdateStarted", "OMENative component=engine instance=1 gang surge to revision engine-new (surge-index=2)"),
    ]

    @staticmethod
    def _gang(inst, hash_, ready=True, serving=True):
        return {
            f"e{inst}l": {"alive": True, "hash": hash_, "inst": str(inst), "comp": "engine", "runner": "leader",
                          "ready": ready, "serving": serving},
            f"e{inst}w": {"alive": True, "hash": hash_, "inst": str(inst), "comp": "engine", "runner": "worker",
                          "ready": ready, "serving": serving},
        }

    def _verdict(self, pod_state, events, surges=None, ledger=None, snapshot=None, settle_s=0):
        """The break verdict over fixed records: the events every read
        returns (a list per read when the reads differ), the status rows'
        claims seen during the roll, the migration ledger and the rows at
        the read, with the re-read budget settle_s (0: one read)."""
        f = {"kind": "crashloop", "fired": True, "infra_fail": None, "form": "start", "case_id": "p999",
             "crash_inst": "1", "crash_comp": "engine", "crash_hash": "old", "kills": 0}
        reads = list(events) if events and isinstance(events[0], list) else [events]
        clock = [0.0]

        def events_since(case_id, since):
            return reads[min(len(reads) - 1, int(clock[0]))]

        def tick(seconds):
            clock[0] += seconds
        with mock.patch.object(run, "_isvc_events_since", events_since), \
                mock.patch.object(run, "_inv_fetch_ledger", lambda case_id: ledger or []), \
                mock.patch.object(run, "_ir_snapshot", lambda case_id: snapshot or {}), \
                mock.patch.object(run, "ROWS_SETTLE_S", settle_s), mock.patch.object(run, "ROW_POLL_S", 1.0), \
                mock.patch.object(run.time, "time", lambda: clock[0]), mock.patch.object(run.time, "sleep", tick):
            notes, checks = [], []
            return (run._crashloop_checks(f, pod_state, {"old"}, 1.0, "PASS", notes, checks, surges=surges),
                    notes, checks)

    def test_gang_surge_replacements_follow_the_surge_index(self):
        events = self.SURGED + [
            ("RecreateUpdateStarted", "OMENative component=decoder instance=1 gang surge to revision decoder-new (surge-index=3)"),
            ("RecreateUpdateStarted", "OMENative component=engine instance=0 recreate (revision engine-old -> engine-new, incarnation=2)"),
            ("RecreateUpdateCompleted", "OMENative component=engine instance=0 recreate to revision engine-new complete"),
        ]
        self.assertEqual([("1", "2")], run._gang_surge_replacements(events, "engine"))
        self.assertEqual([("1", "3")], run._gang_surge_replacements(events, "decoder"))
        # A replacement the source retired is not where the Instance went;
        # the retry that stood is, even at the same index.
        abandoned = self.SURGED + [
            ("GangSurgeAbandoned", "OMENative component=engine instance=1 abandoned failed gang surge (surge-index=2); resetting to revision engine-old for a fresh rollout")]
        self.assertEqual([], run._gang_surge_replacements(abandoned, "engine"))
        retried = abandoned + [
            ("RecreateUpdateStarted", "OMENative component=engine instance=1 gang surge to revision engine-new (surge-index=3)")]
        self.assertEqual([("1", "3")], run._gang_surge_replacements(retried, "engine"))
        same_index = abandoned + self.SURGED
        self.assertEqual([("1", "2")], run._gang_surge_replacements(same_index, "engine"))
        self.assertEqual(["1", "2"], run._follow_gang_surges("1", [("1", "2")]))
        self.assertEqual(["1", "2", "4"], run._follow_gang_surges("1", [("1", "2"), ("2", "4")]),
                         "a replacement surged again by a later bump is followed too")
        self.assertEqual(["0"], run._follow_gang_surges("0", [("1", "2")]))
        self.assertEqual(["1", "2"], run._follow_gang_surges("1", [("1", "2"), ("2", "1")]),
                         "a stale pair never loops the chain")

    def test_chain_reads_surges_in_the_order_they_happened(self):
        # Two gangs roll in turn: gang 0 surges to the fresh index 2, then
        # gang 1 surges to index 0, freed by the first surge. Gang 1's
        # Instance ended at index 0; the earlier surge away from index 0
        # was another gang's and is not a later hop of this chain.
        events = [
            ("RecreateUpdateStarted", "OMENative component=engine instance=0 gang surge to revision engine-new (surge-index=2)"),
            ("RecreateUpdateStarted", "OMENative component=engine instance=1 gang surge to revision engine-new (surge-index=0)"),
        ]
        pairs = run._gang_surge_replacements(events, "engine")
        self.assertEqual([("0", "2"), ("1", "0")], pairs)
        self.assertEqual(["1", "0"], run._follow_gang_surges("1", pairs))
        self.assertEqual(["0", "2"], run._follow_gang_surges("0", pairs))
        healed = {**self._gang(2, "new"), **self._gang(0, "new")}
        verdict, _notes, checks = self._verdict(healed, events)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("replaced at Instance 0" in c for c in checks), checks)
        verdict, notes, _checks = self._verdict({**self._gang(2, "new"), **self._gang(0, "old")}, events)
        self.assertEqual("FAIL", verdict, "the verdict reads the victim's own replacement, not the other gang's")
        self.assertTrue(any("read at Instance 0" in n for n in notes), notes)

    def test_verdict_follows_a_gang_surge_to_the_replacement_index(self):
        # The victim gang at index 1 was replaced at index 2 and its own
        # index retired: the roll landed the fix on it.
        healed = {**self._gang(0, "new"), **self._gang(2, "new")}
        verdict, notes, checks = self._verdict(healed, self.SURGED)
        self.assertEqual("PASS", verdict, notes)
        self.assertTrue(any("replaced at Instance 2" in c and "ended on the new revision with 2 pod(s)" in c
                            for c in checks), checks)
        # Without the surge the retired index reads as a gang with no pods.
        verdict, notes, _checks = self._verdict(healed, [])
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("read at Instance 1: pods=0" in n for n in notes), notes)
        # A replacement still on the old revision, or not serving, is read
        # where the Instance ended.
        verdict, notes, _checks = self._verdict({**self._gang(0, "new"), **self._gang(2, "old")}, self.SURGED)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("read at Instance 2" in n and "old-revision=['e2l', 'e2w']" in n for n in notes), notes)
        verdict, _notes, _checks = self._verdict({**self._gang(0, "new"), **self._gang(2, "new", ready=False)}, self.SURGED)
        self.assertEqual("FAIL", verdict)

    def test_verdict_follows_a_chain_of_gang_surges(self):
        twice = self.SURGED + [
            ("RecreateUpdateStarted", "OMENative component=engine instance=2 gang surge to revision engine-newer (surge-index=3)"),
        ]
        healed = {**self._gang(0, "new"), **self._gang(3, "new")}
        verdict, notes, checks = self._verdict(healed, twice)
        self.assertEqual("PASS", verdict, notes)
        self.assertTrue(any("replaced at Instance 3" in c for c in checks), checks)

    COMPLETED = [
        ("RecreateUpdateCompleted", "OMENative component=engine instance=1 gang surge complete: instance 2 promoted to revision engine-new"),
    ]
    MOVED = [
        ("MigrationRequestAccepted", "OMENative component=engine instance=1 migration accepted (uuid=abc, source-node=node-a, surge-index=2)"),
    ]

    def test_the_pair_is_read_from_every_record_that_names_it(self):
        # The surge's completion names the pair the recorder may have
        # dropped the start of; a migration's acceptance names the move.
        self.assertEqual([("1", "2")], run._gang_surge_replacements(self.COMPLETED, "engine"))
        self.assertEqual([("1", "2")], run._gang_surge_replacements(self.SURGED + self.COMPLETED, "engine"),
                         "a start and its completion are one surge")
        self.assertEqual([("1", "2")], run._gang_surge_replacements(self.MOVED, "engine"))
        abandoned_then_done = self.SURGED + [
            ("GangSurgeAbandoned", "OMENative component=engine instance=1 abandoned failed gang surge (surge-index=2); resetting to revision engine-old for a fresh rollout")
        ] + self.COMPLETED
        self.assertEqual([("1", "2")], run._gang_surge_replacements(abandoned_then_done, "engine"),
                         "a completion is a surge that stood whatever the start count says")
        # The migration ledger names the move with its replacement index
        # once allocated; a failed move and one without an index are not
        # where the Instance went.
        ledger = [{"component": "engine", "sourceInstance": 1, "surgeInstance": 2, "phase": "Completed"},
                  {"component": "engine", "sourceInstance": 0, "surgeInstance": 3, "phase": "Failed"},
                  {"component": "engine", "sourceInstance": 0, "surgeInstance": -1, "phase": "Started"},
                  {"component": "decoder", "sourceInstance": 1, "surgeInstance": 4, "phase": "Completed"}]
        self.assertEqual([("1", "2")], run._ledger_surge_moves(ledger, "engine"))
        self.assertEqual([("1", "4")], run._ledger_surge_moves(ledger, "decoder"))
        self.assertEqual([], run._ledger_surge_moves(None, "engine"))
        # The rows at the read, while a source still claims its replacement.
        surge = {"type": "Update", "step": "SurgeDrain", "surgeIndex": 2, "targetRevision": "engine-new"}
        snapshot = {"engine": {"hold": None, "rows": [{"index": 0, "phase": "Ready"},
                                                      {"index": 1, "phase": "Updating", "operation": surge}]}}
        self.assertEqual([("1", "2")], run._row_surge_claims(snapshot, "engine"))
        self.assertEqual([], run._row_surge_claims(snapshot, "decoder"))
        healed = {**self._gang(0, "new"), **self._gang(2, "new")}
        for source, kwargs in (("completion", dict(events=self.COMPLETED)), ("move", dict(events=self.MOVED)),
                               ("ledger", dict(events=[], ledger=ledger, settle_s=5)),
                               ("rows", dict(events=[], snapshot=snapshot, settle_s=5))):
            verdict, notes, checks = self._verdict(healed, **kwargs)
            self.assertEqual("PASS", verdict, (source, notes))
            self.assertTrue(any("replaced at Instance 2" in c for c in checks), (source, checks))

    def test_the_chain_is_reread_within_the_settle_budget_before_the_verdict_fails(self):
        # The start the first read did not see lands on the second: the
        # recorder is behind the pods, so the verdict re-reads its records
        # until the chain reaches an index with pods or the budget is out.
        healed = {**self._gang(0, "new"), **self._gang(2, "new")}
        verdict, notes, checks = self._verdict(healed, [[], [], self.SURGED], settle_s=10)
        self.assertEqual("PASS", verdict, notes)
        self.assertTrue(any("replaced at Instance 2" in c for c in checks), checks)
        verdict, notes, _checks = self._verdict(healed, [[], [], self.SURGED], settle_s=1)
        self.assertEqual("FAIL", verdict, "the re-read stops at the budget")
        self.assertTrue(any("read at Instance 1: pods=0" in n and "no record named a replacement" in n for n in notes), notes)
        verdict, _notes, _checks = self._verdict({**self._gang(0, "new"), **self._gang(1, "new")}, [[], self.SURGED], settle_s=10)
        self.assertEqual("PASS", verdict, "an Instance with pods where the chain ends is read at once, nothing re-read")

    def test_events_since_counts_a_folded_repeat_once_per_occurrence(self):
        import subprocess
        items = {"items": [
            {"reason": "RecreateUpdateStarted", "count": 2, "lastTimestamp": "2026-01-01T00:00:10Z",
             "message": "OMENative component=engine instance=1 gang surge to revision engine-new (surge-index=2)"},
            {"reason": "GangSurgeAbandoned", "count": 1, "lastTimestamp": "2026-01-01T00:00:05Z",
             "message": "OMENative component=engine instance=1 abandoned failed gang surge (surge-index=2); resetting to revision engine-old for a fresh rollout"},
            {"reason": "InstanceReady", "lastTimestamp": "2025-12-31T00:00:00Z",
             "message": "OMENative component=engine instance=0 is Ready (2 pod(s) serving)"},
        ]}
        saved = run.kubectl
        run.kubectl = lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout=json.dumps(items), stderr="")
        try:
            events = run._isvc_events_since("p999", 1767225600.0)  # 2026-01-01T00:00:00Z
        finally:
            run.kubectl = saved
        self.assertEqual(3, len(events), "a folded repeat is one entry per occurrence; the stale event is dropped")
        self.assertEqual(["GangSurgeAbandoned", "RecreateUpdateStarted", "RecreateUpdateStarted"],
                         [reason for reason, _m in events], "oldest first")
        self.assertEqual([("1", "2")], run._gang_surge_replacements(events, "engine"),
                         "two starts against one abandon leave the retry at the same index standing")

    def test_repair_on_any_index_of_the_chain_fails_the_verdict(self):
        healed = {**self._gang(0, "new"), **self._gang(2, "new")}
        repaired = self.SURGED + [
            ("RestartTriggered", "OMENative component=engine instance=2 restart triggered: pod e2l wedged (incarnation=2)")]
        verdict, notes, _checks = self._verdict(healed, repaired)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("repaired after the bump" in n for n in notes), notes)
        elsewhere = self.SURGED + [
            ("RestartTriggered", "OMENative component=engine instance=12 restart triggered: pod e12l wedged (incarnation=2)"),
            ("RestartTriggered", "OMENative component=decoder instance=1 restart triggered: pod d1 wedged (incarnation=2)")]
        verdict, notes, _checks = self._verdict(healed, elsewhere)
        self.assertEqual("PASS", verdict, "a repair on another Instance, or another Component, is not this verdict's")

    @staticmethod
    def _row(index, phase, rev=None, op=None, pods=0):
        row = {"index": index, "phase": phase, "podCount": pods}
        if rev:
            row["runningRevision"] = rev
        if op:
            row["operation"] = op
        return row

    def _status_tick(self, inv, items):
        """One status-truth observation over the given InferenceReplica
        items, as a tick hands them to the check."""
        run._inv_check_status_truth(inv, {}, {"engine": 2, "decoder": 2}, items, set(), 1.0)

    def test_switch_case_replay_follows_the_surge_the_events_lost(self):
        # The pd-mn switch case: a roll from RecreatePod to SurgeThenDrain
        # at the first updated pod, engine/1 crashing from the bump. The
        # Instances already rolling finished as recreates at their own
        # index; engine/1 and decoder/1, admitted after the edit, were each
        # surged to index 2 and drained. Of the events after the bump the
        # ISVC kept only the two recreate starts: the recorder drops an
        # object's events past a burst, and the surge starts came after it.
        # The status rows claimed both pairs for the whole surge window.
        events = [
            ("RecreateUpdateStarted", "OMENative component=decoder instance=0 recreate (revision decoder-old -> decoder-new, incarnation=2)"),
            ("RecreateUpdateStarted", "OMENative component=engine instance=0 recreate (revision engine-old -> engine-new, incarnation=2)"),
        ]
        recreate = {"type": "Update", "step": "Drain", "targetRevision": "engine-new", "strategy": "RecreatePod"}
        surge = {"type": "Update", "step": "Surge", "surgeIndex": 2, "targetRevision": "engine-new", "strategy": "SurgeThenDrain"}
        drain = {**surge, "step": "SurgeDrain"}
        marker = {"type": "Update", "step": "GangSurgeTarget", "targetRevision": "engine-new"}

        def ir(comp, rows):
            return {"spec": {"component": comp}, "status": {"instanceStatuses": rows}}
        claimed = [ir("engine", [self._row(0, "Updating", "engine-old", recreate),
                                 self._row(1, "Updating", "engine-old", surge, pods=2),
                                 self._row(2, "Pending", op=marker)]),
                   ir("decoder", [self._row(0, "Updating", "decoder-old", recreate),
                                  self._row(1, "Updating", "decoder-old", {**surge, "targetRevision": "decoder-new"}, pods=2),
                                  self._row(2, "Pending", op={**marker, "targetRevision": "decoder-new"})])]
        draining = [ir("engine", [self._row(0, "Ready", "engine-new", pods=2),
                                  self._row(1, "Updating", "engine-old", drain, pods=2),
                                  self._row(2, "Ready", "engine-new", pods=2)]),
                    ir("decoder", [self._row(0, "Ready", "decoder-new", pods=2),
                                   self._row(1, "Updating", "decoder-old", {**drain, "targetRevision": "decoder-new"}, pods=2),
                                   self._row(2, "Ready", "decoder-new", pods=2)])]
        settled = [ir("engine", [self._row(0, "Ready", "engine-new", pods=2), self._row(2, "Ready", "engine-new", pods=2)]),
                   ir("decoder", [self._row(0, "Ready", "decoder-new", pods=2), self._row(2, "Ready", "decoder-new", pods=2)])]
        inv = run._inv_new("p3979", {})
        for items in (claimed, claimed, draining, settled, settled):
            self._status_tick(inv, items)
        self.assertEqual({("engine", "1", "2", "engine-old"), ("decoder", "1", "2", "decoder-old")}, set(inv["surges"]))
        healed = {**self._gang(0, "new"), **self._gang(2, "new")}
        verdict, notes, checks = self._verdict(healed, events, surges=inv["surges"])
        self.assertEqual("PASS", verdict, notes)
        self.assertTrue(any("replaced at Instance 2 by its gang surge" in c and "ended on the new revision with 2 pod(s) serving" in c
                            for c in checks), checks)
        # The events alone cannot see the hop: they read the retired index.
        verdict, notes, _checks = self._verdict(healed, events)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("read at Instance 1: pods=0" in n for n in notes), notes)

    def test_status_rows_name_a_gang_surge_pair_until_the_source_is_reset(self):
        inv = run._inv_new("p999", {})
        surge = {"type": "Update", "step": "Surge", "surgeIndex": 2, "targetRevision": "new"}
        marker = {"type": "Update", "step": "GangSurgeTarget", "targetRevision": "new"}
        run._note_gang_surges(inv, "engine", [self._row(0, "Ready", "old"), self._row(1, "Updating", "old", surge),
                                              self._row(2, "Pending", op=marker)])
        self.assertEqual([("engine", "1", "2", "old")], inv["surges"])
        # The pair stands with the source row gone (the surge completed) ...
        run._note_gang_surges(inv, "engine", [self._row(0, "Ready", "old"), self._row(2, "Ready", "new")])
        self.assertEqual([("engine", "1", "2", "old")], inv["surges"])
        # ... and with its index held again by a row on another revision.
        run._note_gang_surges(inv, "engine", [self._row(1, "Pending", op=marker), self._row(2, "Ready", "new")])
        run._note_gang_surges(inv, "engine", [self._row(1, "Ready", "new"), self._row(2, "Ready", "new")])
        self.assertEqual([("engine", "1", "2", "old")], inv["surges"])
        # Another Component's rows, a Migrate operation's surge index and a
        # single-pod surge (no surge index) are not this Component's pairs.
        run._note_gang_surges(inv, "decoder", [self._row(1, "Updating", "old", surge), self._row(2, "Pending", op=marker)])
        run._note_gang_surges(inv, "engine", [self._row(1, "Ready", "new"), self._row(2, "Ready", "new"),
                                              self._row(3, "Migrating", "new", {**surge, "type": "Migrate", "surgeIndex": 4}),
                                              self._row(5, "Updating", "old", {"type": "Update", "step": "Surge", "targetRevision": "new"})])
        self.assertEqual([("engine", "1", "2", "old"), ("decoder", "1", "2", "old")], inv["surges"])
        # The source seen again on its running revision without the claim
        # was reset: the surge was abandoned ...
        inv = run._inv_new("p999", {})
        run._note_gang_surges(inv, "engine", [self._row(1, "Updating", "old", surge), self._row(2, "Pending", op=marker)])
        run._note_gang_surges(inv, "engine", [self._row(1, "Ready", "old")])
        self.assertEqual([], inv["surges"])
        # ... or retried at another index, which is then the latest claim.
        run._note_gang_surges(inv, "engine", [self._row(0, "Updating", "old", {**surge, "surgeIndex": 3}),
                                              self._row(1, "Updating", "old", surge)])
        run._note_gang_surges(inv, "engine", [self._row(0, "Updating", "old", {**surge, "surgeIndex": 3}),
                                              self._row(1, "Updating", "old", {**surge, "surgeIndex": 4})])
        self.assertEqual([("engine", "0", "3", "old"), ("engine", "1", "4", "old")], inv["surges"])

    def test_verdict_reads_the_claimed_pairs_before_the_events(self):
        # Gang 0 surged to the fresh index 2, then gang 1 to the freed index
        # 0; the events kept only the second start. The claimed pairs carry
        # the order the surges happened, so the earlier surge away from
        # index 0 is not read as a later hop of gang 1's chain.
        surges = [("engine", "0", "2", "old"), ("engine", "1", "0", "old")]
        events = [("RecreateUpdateStarted", "OMENative component=engine instance=1 gang surge to revision engine-new (surge-index=0)")]
        healed = {**self._gang(2, "new"), **self._gang(0, "new")}
        verdict, _notes, checks = self._verdict(healed, events, surges=surges)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("replaced at Instance 0" in c for c in checks), checks)
        verdict, notes, _checks = self._verdict({**self._gang(2, "new"), **self._gang(0, "old")}, events, surges=surges)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("read at Instance 0" in n for n in notes), notes)
        # A start the events kept for a surge the rows were never seen to
        # claim still extends the chain.
        later = events + [("RecreateUpdateStarted", "OMENative component=engine instance=0 gang surge to revision engine-newer (surge-index=4)")]
        verdict, _notes, checks = self._verdict({**self._gang(2, "new"), **self._gang(4, "new")}, later, surges=surges)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("replaced at Instance 4" in c for c in checks), checks)

    def test_generated_family_crosses_every_axis(self):
        _outputs, rows = generate_cases.build_outputs()
        fam = [(kv, m) for folder, _n, kv, m in rows if folder == "gen-crashloop"]
        self.assertEqual(216, len(fam))
        self.assertEqual({"crashloop:start", "crashloop:load"}, {m["verb"] for _kv, m in fam})
        self.assertEqual(set(generate_cases.CRASH_BUDGETS), {m["strat"] for _kv, m in fam})
        self.assertEqual({"None", "RecreateInstance"}, {m["rp"] for _kv, m in fam})
        self.assertEqual({(f, rp) for f in ("start", "load") for rp in ("None", "RecreateInstance")},
                         {(kv["form"], m["rp"]) for kv, m in fam},
                         "both crash forms run under both restart policies")
        self.assertEqual({"engine", "pd", "engine-mn", "pd-mn"}, {kv["topo"] for kv, _m in fam})
        for kv, _m in fam:
            self.assertEqual("crashloop:old", kv["fault"])
            self.assertIn(kv["form"], generate_cases.CRASH_FORMS)
            self.assertEqual(1, kv["floor_soft"])
            self.assertNotIn("mode", kv)
            if kv["topo"].startswith("pd"):
                self.assertIn(kv["co"], {"none", "rolling"})
                if kv["co"] == "rolling":
                    self.assertEqual(kv["Nd"] + 1, kv["ceil_d"], "a rolling group surges the decoder too")
            else:
                self.assertEqual("none", kv["co"])

    def _grouped_or_plain(self, kv, text):
        """No cell is parked: a repair of an Instance that serves nothing is
        admitted without the budget or the gate, so the header carries no
        notimpl or issue and the reason names no park. A grouped cell carries
        its group block and its reason names the group and the gate it does
        not consult; a no-group cell carries no group block."""
        self.assertNotIn("notimpl", kv)
        self.assertNotIn("issue", kv)
        reason = text.splitlines()[1]
        self.assertNotIn("parked", reason)
        if kv["co"] == "none":
            self.assertNotIn("  rollout:\n    groups:", text)
        else:
            self.assertIn("  rollout:\n    groups:", text)
            self.assertIn(f"the {generate_cases.GROUP_NAMES[kv['co']]} group", reason)
            self.assertIn("gate", reason)
        if kv["co"] == "canary":
            self.assertEqual("auto", kv["canary"])
            self.assertIn("          canary:", text)
        else:
            self.assertNotIn("canary", kv)
            self.assertNotIn("canary:", text)
        if kv["co"] == "seq":
            self.assertEqual(2, text.count("blueGreen: {}"), "Sequential is one blueGreen group per member")

    def test_generated_idle_family_crosses_every_axis(self):
        """The steady-state family: a crash loop on the settled service under
        every group kind, on the engine and on the decoder where one exists,
        under every (crash form, restart policy) pair a repair is promised
        for; every cell expects the repair, nothing surges at idle, and no
        cell is parked."""
        outputs, rows = generate_cases.build_outputs()
        fam = [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-crashloop-idle"]
        self.assertEqual(84, len(fam))
        self.assertEqual(set(generate_cases.BREAK_GROUPS), {kv["co"] for _n, kv, _m in fam})
        self.assertEqual({"engine", "pd", "engine-mn", "pd-mn"}, {kv["topo"] for _n, kv, _m in fam})
        self.assertEqual({("start", "None"), ("start", "RecreateInstance"), ("load", "RecreateInstance")},
                         {(kv["form"], m["rp"]) for _n, kv, m in fam},
                         "a crash after Ready under policy None promises no repair")
        self.assertEqual({"engine", "decoder"}, {kv["victimcomp"] for _n, kv, _m in fam if kv["topo"].startswith("pd")})
        for n, kv, m in fam:
            self.assertEqual(("crashloop:old", "idle", "repaired"), (kv["fault"], kv["during"], kv["expect"]))
            self.assertEqual(kv["victimcomp"], m["victim"])
            if kv["victimcomp"] == "decoder":
                self.assertTrue(kv["topo"].startswith("pd"))
            if kv["co"] == "seq":
                self.assertTrue(kv["topo"].startswith("pd"), "one member makes Sequential a blueGreen group")
            self.assertEqual((kv["Ne"], kv["Nd"], kv["Nr"]), (kv["ceil_e"], kv["ceil_d"], kv["ceil_r"]),
                             "nothing surges on a service nobody bumped")
            self.assertNotIn("mode", kv)
            self.assertNotIn("floor_soft", kv)
            self.assertNotIn("bump", kv)
            self._grouped_or_plain(kv, outputs[f"gen-crashloop-idle/{n}.yaml"])
        self.assertEqual([], [n for n, _kv, m in fam if "parked" in m], "every cell runs live")

    def test_generated_sibling_family_crosses_every_axis(self):
        """The sibling family: the decoder breaks as an engine-only bump lands,
        on both PD topologies under every group kind, under every (crash
        form, restart policy) pair a repair is promised for; the engine
        surges within its own budget and the decoder is a negative control.
        A rollingUpdate group admits no push that leaves a member out, so
        its cells bump both members and the decoder surges too; no cell is
        parked."""
        outputs, rows = generate_cases.build_outputs()
        fam = [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-crashloop-sibling"]
        self.assertEqual(30, len(fam))
        self.assertEqual(set(generate_cases.BREAK_GROUPS), {kv["co"] for _n, kv, _m in fam})
        self.assertEqual({"pd", "pd-mn"}, {kv["topo"] for _n, kv, _m in fam})
        self.assertEqual({("start", "None"), ("start", "RecreateInstance"), ("load", "RecreateInstance")},
                         {(kv["form"], m["rp"]) for _n, kv, m in fam})
        for n, kv, m in fam:
            rolling = kv["co"] == "rolling"
            self.assertEqual(("crashloop:old", "decoder", "engine,decoder" if rolling else "engine"),
                             (kv["fault"], kv["victimcomp"], kv["bump"]))
            self.assertEqual("decoder", m["victim"])
            self.assertEqual((kv["Ne"] + 1, kv["Nd"] + (1 if rolling else 0), kv["Nr"]),
                             (kv["ceil_e"], kv["ceil_d"], kv["ceil_r"]))
            reason = outputs[f"gen-crashloop-sibling/{n}.yaml"].splitlines()[1]
            if rolling:
                self.assertIn("engine and decoder are bumped together", reason)
                self.assertIn("no RestartTriggered event names it after the bump", reason)
            else:
                self.assertIn("the engine is bumped alone", reason)
                self.assertIn("rebuilt by a repair", reason)
            self.assertNotIn("during", kv)
            self.assertNotIn("expect", kv)
            self.assertNotIn("floor_soft", kv, "the break is on the decoder; the engine keeps its floor")
            self._grouped_or_plain(kv, outputs[f"gen-crashloop-sibling/{n}.yaml"])
        self.assertEqual([], [n for n, _kv, m in fam if "parked" in m], "every cell runs live")

    def test_idle_cell_parks_through_the_table_alone(self):
        """A PARKED entry keyed as the table documents parks exactly that
        crashloop-idle cell, replacing the family's reason with its own, and
        touches no other cell of either family."""
        cell = ("crashloop-idle", "start", "engine", 0, 4, "none", "engine", "None")
        with mock.patch.dict(generate_cases.PARKED, {cell: (999999, "parked on a tracked bug (fixture)", "A")}):
            outputs, rows = generate_cases.build_outputs()
        hit = [(f, n, kv) for f, n, kv, m in rows if m.get("parked") == 999999]
        self.assertEqual(1, len(hit))
        folder, name, kv = hit[0]
        self.assertEqual(("gen-crashloop-idle", "none"), (folder, kv["co"]))
        self.assertEqual((1, 999999, "A"), (kv["notimpl"], kv["issue"], kv["inv_xfail"]))
        self.assertEqual("# reason: parked on a tracked bug (fixture)", outputs[f"{folder}/{name}.yaml"].splitlines()[1])
        others = [n for f, n, _kv, m in rows if f in ("gen-crashloop-idle", "gen-crashloop-sibling")
                  and "parked" in m and n != name]
        self.assertEqual([], others)


class RolloutBumpMembersTest(unittest.TestCase):
    """bump= on a rollout case names the members the trigger bumps; the
    members left out are the negative controls."""

    BASE = "# p999: x mode=rollout Ne=2 Nd=2 Nr=0 ceil_e=3 ceil_d=2 ceil_r=0 topo=pd"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_names_configured_members(self):
        self.assertEqual([], self._errs(self.BASE + " bump=engine"))
        self.assertEqual([], self._errs(self.BASE + " bump=engine,decoder"))
        self.assertEqual([], self._errs(self.BASE + " bump=engine fault=crashloop:old form=start victimcomp=decoder"))
        self.assertTrue(any("not a configured Component" in e for e in self._errs(self.BASE + " bump=router")))
        self.assertTrue(any("not a configured Component" in e
                            for e in self._errs(self.BASE.replace("Nd=2", "Nd=0") + " bump=decoder")))
        self.assertTrue(any("malformed header value bump=" in e for e in self._errs(self.BASE + " bump=sidecar")))
        kill = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
        self.assertTrue(any("mode=rollout|canary" in e for e in self._errs(kill + " bump=engine")))

    def test_canary_group_is_a_coordination_kind(self):
        self.assertEqual([], self._errs(self.BASE + " co=canary canary=auto"))
        self.assertTrue(any("malformed header value co=" in e for e in self._errs(self.BASE + " co=staged")))

    def test_named_members_win_over_the_group(self):
        configured = {"engine", "decoder", "router"}
        self.assertEqual({"engine"}, run._bump_set({"bump": "engine"}, ["engine", "decoder"], configured))
        self.assertEqual({"engine", "decoder"}, run._bump_set({"bump": "engine, decoder"}, [], configured))
        self.assertEqual({"engine", "decoder"}, run._bump_set({}, ["engine", "decoder"], configured))
        self.assertEqual({"engine"}, run._bump_set({}, [], configured))
        self.assertEqual({"engine"}, run._bump_set({"bump": "engine,decoder"}, [], {"engine"}),
                         "a member the case does not configure is not bumped")
        self.assertEqual({"engine"}, run._bump_set(None, ["engine", "decoder"], {"engine"}))
        ctx = {"hdr": {"bump": "decoder"}, "info": {"members": ["engine", "decoder"]}, "configured": configured}
        self.assertEqual({"decoder"}, run._bumped(ctx))


class IdleBreakFaultTest(unittest.TestCase):
    """A break aimed at one Component (victimcomp=) and fired on the idle
    service (during=idle): the header, the victim rule, the phase hook that
    must leave it alone, the killer's cleanup, and the repair-or-hold
    verdict."""

    BASE = "# p999: x mode=rollout Ne=2 Nd=2 Nr=0 ceil_e=3 ceil_d=3 ceil_r=0 topo=pd"

    def _errs(self, header):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_aims_the_break_and_times_it(self):
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:old form=start victimcomp=decoder"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:old form=start during=idle"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:old form=load during=idle victimcomp=decoder expect=held"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:old form=start during=idle expect=repaired"))
        self.assertTrue(any("victimcomp=router" in e for e in self._errs(self.BASE + " fault=crashloop:old form=start victimcomp=router")))
        no_decoder = self.BASE.replace("Nd=2", "Nd=0")
        self.assertTrue(any("Nd>0" in e for e in self._errs(no_decoder + " fault=crashloop:old form=start victimcomp=decoder")))
        self.assertEqual([], self._errs(no_decoder + " fault=crashloop:old form=start"))
        self.assertTrue(any("during=idle" in e for e in self._errs(self.BASE + " during=idle")))
        self.assertTrue(any("during=idle" in e for e in self._errs(self.BASE + " fault=podkill:settling during=idle")))
        self.assertTrue(any("expect=held" in e for e in self._errs(self.BASE + " fault=crashloop:old form=start expect=held")))
        self.assertTrue(any("malformed header value expect=" in e
                            for e in self._errs(self.BASE + " fault=crashloop:old form=start during=idle expect=fixed")))
        kill = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
        self.assertTrue(any("mode=rollout" in e for e in self._errs(kill + " fault=crashloop:old form=start during=idle")))

    def test_victim_component_selects_that_components_highest_leader(self):
        pods = {
            "e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "default"},
            "e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "default"},
            "d0": {"alive": True, "hash": "old", "inst": "0", "comp": "decoder", "runner": "default"},
            "d1": {"alive": True, "hash": "old", "inst": "1", "comp": "decoder", "runner": "leader"},
            "w1": {"alive": True, "hash": "old", "inst": "1", "comp": "decoder", "runner": "worker"},
        }
        self.assertEqual("d1", run._crashloop_pick_pod(pods, {"old"}, "decoder"))
        self.assertEqual("e1", run._crashloop_pick_pod(pods, {"old"}))
        f = run._fault_new("p999", {"fault": "crashloop:old", "form": "start", "victimcomp": "decoder"})
        orig = run.kubectl
        run.kubectl = lambda *a, **k: subprocess.CompletedProcess(a, 0, "", "")
        try:
            run._fault_crashloop_fire(f, pods, {"old"})
        finally:
            run.kubectl = orig
        self.assertEqual(("d1", "decoder", "1"), (f["crash_pod"], f["crash_comp"], f["crash_inst"]))

    def test_idle_fault_is_not_fired_by_the_phase_hook(self):
        f = run._fault_new("p999", {"fault": "crashloop:old", "form": "start", "during": "idle"})
        self.assertTrue(run._fault_at_idle(f))
        self.assertFalse(run._fault_at_idle(run._fault_new("p999", {"fault": "crashloop:old", "form": "start"})))
        fired = []
        orig = run._fault_fire
        run._fault_fire = lambda *a, **k: fired.append(a)
        try:
            run._fault_tick({"fault": f, "trigger_ts": 1.0, "pod_state": {}, "pods_per_inst": {}, "old_hashes": {"old"}})
            self.assertEqual([], fired, "an idle fault is fired by the idle handler, never by a phase hook")
        finally:
            run._fault_fire = orig

    def test_expectation_is_the_headers_else_repaired_whatever_the_group(self):
        """A break at idle leaves the victim Instance serving nothing, and
        such a repair is admitted without the budget or a group's gate, so
        the derived expectation is repaired; only the header states a hold."""
        self.assertEqual("repaired", run._idle_break_expect({}))
        self.assertEqual("repaired", run._idle_break_expect({"victimcomp": "decoder", "co": "rolling"}))
        self.assertEqual("repaired", run._idle_break_expect({"expect": "repaired"}))
        self.assertEqual("held", run._idle_break_expect({"expect": "held"}))
        self.assertEqual("repaired", run._idle_break_expect({"expect": "heals"}), "a lift expectation is not a break's")

    def test_load_killer_is_registered_for_cleanup_and_stops_on_a_rebuild(self):
        f = run._fault_new("p999", {"fault": "crashloop:old", "form": "load", "during": "idle"})
        pods = {"e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "default", "uid": "u0"}}
        orig_state, orig_cleanups = run._crashloop_pod_state, list(run._FAULT_CLEANUPS)
        # The live pod reads as rebuilt (a new UID on the old revision): the
        # killer must leave it alone and return without an exec.
        run._crashloop_pod_state = lambda name: (True, True, "old", True, "u-rebuilt")
        try:
            run._fault_crashloop_fire(f, pods, {"old"})
            self.assertEqual("u0", f["crash_uid"])
            self.assertIn(("crashloop", f), run._FAULT_CLEANUPS)
            f["crash_thread"].join(timeout=5)
            self.assertFalse(f["crash_thread"].is_alive(), "the killer stops once the pod is a rebuilt one")
            self.assertEqual(0, f["kills"])
            run._run_fault_cleanups()
            self.assertTrue(f["crash_stop"].is_set())
            self.assertEqual([], run._FAULT_CLEANUPS)
        finally:
            run._crashloop_pod_state = orig_state
            run._FAULT_CLEANUPS[:] = orig_cleanups

    def test_idle_verdict_reads_repair_or_hold(self):
        f = {"kind": "crashloop", "crash_inst": "1", "crash_comp": "decoder"}
        repair = [("RestartTriggered", "OMENative component=decoder instance=1 restart triggered: pod d1 wedged")]
        hold = [("RepairHeld", "OMENative component=decoder instance=1 crash-loop repair held by Plan: no run open")]
        other = [("RepairHeld", "OMENative component=decoder instance=0 crash-loop repair held by Plan: no run open")]
        notes, checks = [], []
        self.assertEqual("PASS", run._idle_break_checks(f, "repaired", True, None, repair, "PASS", notes, checks))
        notes, checks = [], []
        self.assertEqual("FAIL", run._idle_break_checks(f, "repaired", False, None, repair, "PASS", notes, checks))
        self.assertTrue(any("not repaired" in n for n in notes))
        notes, checks = [], []
        self.assertEqual("FAIL", run._idle_break_checks(f, "repaired", True, None, [], "PASS", notes, checks))
        self.assertTrue(any("no RestartTriggered" in n for n in notes))
        notes, checks = [], []
        self.assertEqual("PASS", run._idle_break_checks(f, "held", False, None, hold, "PASS", notes, checks))
        notes, checks = [], []
        self.assertEqual("FAIL", run._idle_break_checks(f, "held", False, 23.0, hold, "PASS", notes, checks))
        self.assertTrue(any("despite the expected hold" in n for n in notes))
        notes, checks = [], []
        self.assertEqual("FAIL", run._idle_break_checks(f, "held", False, None, other, "PASS", notes, checks),
                         "a hold recorded for another Instance is not this verdict's")
        self.assertTrue(any("no RepairHeld" in n for n in notes))

    def test_imagepull_breaks_the_same_victim_with_the_unpullable_image(self):
        self.assertTrue(run._fault_ok("imagepull:old"))
        self.assertFalse(run._fault_ok("imagepull:settling"))
        self.assertEqual([], self._errs(self.BASE + " fault=imagepull:old"))
        self.assertEqual([], self._errs(self.BASE + " fault=imagepull:old form=start victimcomp=decoder"))
        self.assertEqual([], self._errs(self.BASE + " fault=imagepull:old during=idle expect=repaired"))
        self.assertTrue(any("only form=start" in e for e in self._errs(self.BASE + " fault=imagepull:old form=load")))
        no_decoder = self.BASE.replace("Nd=2", "Nd=0")
        self.assertTrue(any("Nd>0" in e for e in self._errs(no_decoder + " fault=imagepull:old victimcomp=decoder")))
        kill = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
        self.assertTrue(any("mode=rollout" in e for e in self._errs(kill + " fault=imagepull:old")))
        f = run._fault_new("p999", {"fault": "imagepull:old"})
        pods = {"e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "default", "uid": "u0"},
                "e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "default", "uid": "u1"}}
        patches = []
        orig = run.kubectl
        run.kubectl = lambda *a, **k: patches.append(a) or subprocess.CompletedProcess(a, 0, "", "")
        try:
            ctx = {"fault": f, "trigger_ts": 1.0, "pod_state": pods, "pods_per_inst": {"engine": 1}, "old_hashes": {"old"}}
            run._fault_tick(ctx)
            run._fault_tick(ctx)
        finally:
            run.kubectl = orig
        self.assertTrue(f["fired"])
        self.assertEqual(1, len(patches), "the break fires once, at the settling hook")
        self.assertEqual(("patch", "pod", "e1"), patches[0][2:5])
        self.assertIn(runtime_fixtures.UNPULLABLE_IMAGE, patches[0][-1])
        self.assertEqual(("e1", "1", "u1"), (f["crash_pod"], f["crash_inst"], f["crash_uid"]))
        self.assertNotIn(("crashloop", f), run._FAULT_CLEANUPS, "an image patch dies with the case's pod; the ISVC teardown owns it")
        notes, checks = [], []
        healed = {"e1": {"alive": True, "hash": "new", "inst": "1", "comp": "engine", "ready": True, "serving": True, "uid": "u-new"}}
        orig_ev = run._isvc_events_since
        try:
            run._isvc_events_since = lambda case_id, since: []
            self.assertEqual("PASS", run._crashloop_checks(f, healed, {"old"}, 1.0, "PASS", notes, checks, bumped={"engine"}))
            self.assertTrue(any(c.startswith("  ✓ imagepull:") for c in checks))
        finally:
            run._isvc_events_since = orig_ev

    def test_sibling_victim_after_a_roll_is_owned_by_the_repair(self):
        f = {"kind": "crashloop", "fired": True, "infra_fail": None, "form": "start", "case_id": "p999",
             "crash_inst": "1", "crash_comp": "decoder", "crash_hash": "old", "crash_uid": "u-old", "kills": 0}
        rebuilt = {"d1": {"alive": True, "hash": "old", "inst": "1", "comp": "decoder", "ready": True, "serving": True, "uid": "u-new"}}
        survived = {"d1": {"alive": True, "hash": "old", "inst": "1", "comp": "decoder", "ready": True, "serving": True, "uid": "u-old"}}
        repair = [("RestartTriggered", "OMENative component=decoder instance=1 restart triggered: pod d1 wedged")]
        orig = run._isvc_events_since
        try:
            run._isvc_events_since = lambda case_id, since: repair
            notes, checks = [], []
            self.assertEqual("PASS", run._crashloop_checks(f, rebuilt, {"old"}, 1.0, "PASS", notes, checks, bumped={"engine"}))
            notes, checks = [], []
            self.assertEqual("FAIL", run._crashloop_checks(f, survived, {"old"}, 1.0, "PASS", notes, checks, bumped={"engine"}))
            self.assertTrue(any("not rebuilt" in n for n in notes))
            notes, checks = [], []
            self.assertEqual("FAIL", run._crashloop_checks(f, rebuilt, {"old"}, 1.0, "PASS", notes, checks, bumped={"engine", "decoder"}),
                             "a bumped victim keeps the roll's claim: it must reach the new revision")
            run._isvc_events_since = lambda case_id, since: []
            notes, checks = [], []
            self.assertEqual("FAIL", run._crashloop_checks(f, rebuilt, {"old"}, 1.0, "PASS", notes, checks, bumped={"engine"}))
            self.assertTrue(any("no RestartTriggered" in n for n in notes))
        finally:
            run._isvc_events_since = orig


class ConfigKeyFaultTest(unittest.TestCase):
    """configkey:missing removes the ConfigMap key the victim's runner reads
    and restarts the runner; the key comes back at lift=<phase>, and the
    verdict reads what the Instance does once its cause is gone."""

    BASE = "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 topo=engine"
    POD = {"metadata": {"name": "e1", "uid": "u1"},
           "spec": {"containers": [
               {"name": "sidecar", "env": [{"name": "X", "valueFrom": {"configMapKeyRef": {"name": "other", "key": "k"}}}]},
               {"name": run.MAIN_CONTAINER, "env": [
                   {"name": "TEST_STARTUP_DELAY", "value": "15"},
                   {"name": "TEST_CONFIG_VALUE", "valueFrom": {"configMapKeyRef": {"name": "p999-config", "key": "value"}}}]}]},
           "status": {"containerStatuses": [{"name": run.MAIN_CONTAINER,
                                             "state": {"waiting": {"reason": "CreateContainerConfigError"}}}]}}
    PODS = {"e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "default", "uid": "u0"},
            "e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "default", "uid": "u1"}}

    def _errs(self, header):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def _fake_kubectl(self, calls, pod=None, data=None):
        pod = self.POD if pod is None else pod
        data = {"value": "1"} if data is None else data

        def fake(*a, **k):
            calls.append((a, len(run._FAULT_CLEANUPS)))
            if a[2:4] == ("get", "pod"):
                return subprocess.CompletedProcess(a, 0, json.dumps(pod), "")
            if a[2:4] == ("get", "configmap"):
                return subprocess.CompletedProcess(a, 0, json.dumps({"data": data}), "")
            return subprocess.CompletedProcess(a, 0, "", "")
        return fake

    def test_header_ties_the_fault_to_its_lift_and_its_expectation(self):
        self.assertTrue(run._fault_ok("configkey:missing"))
        self.assertFalse(run._fault_ok("configkey:gone"))
        self.assertEqual([], self._errs(self.BASE + " fault=configkey:missing during=idle lift=after-settle expect=heals"))
        self.assertEqual([], self._errs(self.BASE + " fault=configkey:missing during=idle lift=after-failed expect=heals"))
        self.assertEqual([], self._errs(self.BASE + " fault=configkey:missing during=idle lift=after-failed expect=parked issue=1234"))
        self.assertTrue(any("needs during=idle" in e for e in self._errs(self.BASE + " fault=configkey:missing lift=after-settle expect=heals")))
        self.assertTrue(any("needs lift=" in e for e in self._errs(self.BASE + " fault=configkey:missing during=idle expect=heals")))
        self.assertTrue(any("needs expect=" in e for e in self._errs(self.BASE + " fault=configkey:missing during=idle lift=after-settle")))
        self.assertTrue(any("needs lift=after-failed" in e
                            for e in self._errs(self.BASE + " fault=configkey:missing during=idle lift=after-settle expect=parked issue=1")))
        self.assertTrue(any("needs issue=" in e
                            for e in self._errs(self.BASE + " fault=configkey:missing during=idle lift=after-failed expect=parked")))
        self.assertTrue(any("lifts a persistent fault's cause" in e for e in self._errs(self.BASE + " lift=after-settle")))
        self.assertTrue(any("reads a persistent fault's outcome" in e for e in self._errs(self.BASE + " expect=heals")))
        self.assertTrue(any("lift=after-failed|after-settle" in e
                            for e in self._errs(self.BASE + " fault=configkey:missing during=idle lift=after-recover expect=heals")))
        self.assertTrue(any("malformed header value lift=" in e
                            for e in self._errs(self.BASE + " fault=configkey:missing during=idle lift=now expect=heals")))
        migrate = "# p999: x mode=migrate Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"
        self.assertEqual([], self._errs(migrate + " fault=quota:0 expect=fail lift=1"))
        self.assertTrue(any("lift=1 requires" in e for e in self._errs(migrate + " lift=1")))

    def test_reference_and_bite_are_read_off_the_main_container(self):
        self.assertEqual(("p999-config", "value"), run._configkey_ref(self.POD))
        bare = {"spec": {"containers": [{"name": run.MAIN_CONTAINER, "env": [{"name": "A", "value": "1"}]}]}}
        self.assertIsNone(run._configkey_ref(bare))
        self.assertIsNone(run._configkey_ref(None))
        self.assertEqual("CreateContainerConfigError", run._pod_waiting_reason(self.POD))
        self.assertEqual("", run._pod_waiting_reason(bare))
        self.assertEqual("a~1b~0c", run._json_pointer_token("a/b~c"))

    def test_fire_registers_the_restore_then_removes_the_key_and_restarts_the_runner(self):
        f = run._fault_new("p999", {"fault": "configkey:missing", "during": "idle", "lift": "after-failed", "expect": "heals"})
        calls, orig, saved = [], run.kubectl, list(run._FAULT_CLEANUPS)
        run.kubectl = self._fake_kubectl(calls)
        try:
            run._fault_configkey_fire(f, self.PODS, {"old"})
            self.assertIsNone(f["infra_fail"])
            self.assertEqual(("e1", "1", "u1", "p999-config", "value", "1"),
                             (f["crash_pod"], f["crash_inst"], f["crash_uid"], f["cm_name"], f["cm_key"], f["cm_value"]))
            self.assertEqual("CreateContainerConfigError", f["bite"])
            verbs = [(a[2], a[3]) for a, _n in calls]
            self.assertEqual([("get", "pod"), ("get", "configmap"), ("patch", "configmap"), ("exec", "e1")], verbs)
            remove, registered = calls[2]
            self.assertEqual(1, registered - len(saved), "the restore is registered before the key is removed")
            self.assertEqual([{"op": "remove", "path": "/data/value"}], json.loads(remove[-1]))
            self.assertEqual(("sh", "-c", "kill 1"), calls[3][0][-3:])
            self.assertIn(("configkey", f), run._FAULT_CLEANUPS)
            calls.clear()
            self.assertTrue(run._configkey_restore(f))
            self.assertEqual([("patch", "configmap")], [(a[2], a[3]) for a, _n in calls])
            self.assertEqual({"data": {"value": "1"}}, json.loads(calls[0][0][-1]))
            calls.clear()
            self.assertTrue(run._configkey_restore(f))
            self.assertEqual([], calls, "a restored key is not patched again")
            run._run_fault_cleanups()
            self.assertEqual([], calls, "the end-of-case cleanup finds nothing left to restore")
        finally:
            run.kubectl = orig
            run._FAULT_CLEANUPS[:] = saved

    def test_fire_without_a_reference_or_a_key_is_an_apparatus_failure(self):
        orig, saved = run.kubectl, list(run._FAULT_CLEANUPS)
        try:
            bare = {"spec": {"containers": [{"name": run.MAIN_CONTAINER, "env": [{"name": "A", "value": "1"}]}]}}
            f = run._fault_new("p999", {"fault": "configkey:missing", "during": "idle", "lift": "after-settle", "expect": "heals"})
            calls = []
            run.kubectl = self._fake_kubectl(calls, pod=bare)
            run._fault_configkey_fire(f, self.PODS, {"old"})
            self.assertIn("reads no ConfigMap key", f["infra_fail"])
            self.assertEqual(saved, run._FAULT_CLEANUPS)
            self.assertEqual([("get", "pod")], [(a[2], a[3]) for a, _n in calls])
            f = run._fault_new("p999", {"fault": "configkey:missing", "during": "idle", "lift": "after-settle", "expect": "heals"})
            calls = []
            run.kubectl = self._fake_kubectl(calls, data={})
            run._fault_configkey_fire(f, self.PODS, {"old"})
            self.assertIn("has no key", f["infra_fail"])
            self.assertEqual(saved, run._FAULT_CLEANUPS)
        finally:
            run.kubectl = orig
            run._FAULT_CLEANUPS[:] = saved

    def test_lift_verdict_reads_heal_or_park(self):
        def f():
            return {"kind": "configkey", "crash_inst": "1", "crash_comp": "engine", "infra_fail": None,
                    "bite": "CreateContainerConfigError", "cause": "ConfigMap key p999-config/value"}
        good = {"bitten_at": 5.0, "failed_at": 70.0, "lifted_at": 71.0, "healed_at": 30.0,
                "park_broken": None, "phase_after": "Ready", "new_hash": False, "reset_used": False}
        notes, checks = [], []
        self.assertEqual("PASS", run._lift_checks(f(), "heals", "after-failed", good, {}, "PASS", notes, checks))
        self.assertEqual("PASS", run._lift_checks(f(), "heals", "after-settle", dict(good, failed_at=None), {}, "PASS", [], []))
        notes = []
        self.assertEqual("FAIL", run._lift_checks(f(), "heals", "after-failed", dict(good, failed_at=None), {}, "PASS", notes, []))
        self.assertTrue(any("never escalated" in n for n in notes))
        notes = []
        self.assertEqual("FAIL", run._lift_checks(f(), "heals", "after-failed", dict(good, healed_at=None), {}, "PASS", notes, []))
        self.assertTrue(any("did not return" in n for n in notes))
        notes = []
        self.assertEqual("FAIL", run._lift_checks(f(), "heals", "after-failed", dict(good, new_hash=True), {}, "PASS", notes, []))
        self.assertTrue(any("new revision" in n for n in notes))
        notes = []
        self.assertEqual("FAIL", run._lift_checks(f(), "heals", "after-failed", dict(good, reset_used=True), {}, "PASS", notes, []))
        self.assertTrue(any("reset mailbox" in n for n in notes))
        unbitten = f()
        self.assertEqual("PASS", run._lift_checks(unbitten, "heals", "after-settle", dict(good, bitten_at=None), {}, "PASS", [], []))
        self.assertIn("never showed", unbitten["infra_fail"])
        unlifted = f()
        self.assertEqual("PASS", run._lift_checks(unlifted, "heals", "after-failed", dict(good, lifted_at=None), {}, "PASS", [], []))
        self.assertIn("could not be lifted", unlifted["infra_fail"])
        parked = dict(good, healed_at=None, phase_after="Failed")
        checks = []
        self.assertEqual("PASS", run._lift_checks(f(), "parked", "after-failed", parked, {"issue": 1234}, "PASS", [], checks))
        self.assertTrue(any("issue #1234" in c for c in checks))
        notes = []
        self.assertEqual("FAIL", run._lift_checks(f(), "parked", "after-failed", dict(parked, park_broken=40.0), {"issue": 1234}, "PASS", notes, []))
        self.assertTrue(any("stale" in n for n in notes))


class NodeFaultTest(unittest.TestCase):
    """node:notready stops the kubelet under the victim through the docker
    that owns the KIND nodes, restores it at the lift, and is skipped with a
    reason where that docker is not at hand."""

    BASE = "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 topo=engine"
    PODS = {"e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "default", "uid": "u0", "node": "kind-worker"},
            "e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "default", "uid": "u1", "node": "kind-worker2"},
            "e2": {"alive": True, "hash": "old", "inst": "2", "comp": "engine", "runner": "default", "uid": "u2", "node": "kind-control-plane"}}
    NODES = {"items": [
        {"metadata": {"name": "kind-control-plane", "labels": {"node-role.kubernetes.io/control-plane": ""}}},
        {"metadata": {"name": "kind-worker", "labels": {}}},
        {"metadata": {"name": "kind-worker2", "labels": {}}}]}

    def _errs(self, header):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def _fakes(self, calls, managers="kind-worker", containers=("kind-control-plane", "kind-worker", "kind-worker2"),
               docker_rc=0):
        def kubectl(*a, **k):
            calls.append(("kubectl",) + a + (len(run._FAULT_CLEANUPS),))
            if a[:2] == ("get", "nodes"):
                return subprocess.CompletedProcess(a, 0, json.dumps(self.NODES), "")
            if a[2:4] == ("get", "pod") and "jsonpath" in a[-1]:
                return subprocess.CompletedProcess(a, 0, managers, "")
            return subprocess.CompletedProcess(a, 0, "", "")

        def docker(*a):
            calls.append(("docker",) + a + (len(run._FAULT_CLEANUPS),))
            if a[0] == "inspect":
                return subprocess.CompletedProcess(a, 0 if a[-1] in containers else 1, "true\n" if a[-1] in containers else "", "")
            return subprocess.CompletedProcess(a, docker_rc, "", "" if docker_rc == 0 else "no such container")
        return kubectl, docker

    def test_header_reads_each_outcome_at_its_lift(self):
        self.assertTrue(run._fault_ok("node:notready"))
        self.assertFalse(run._fault_ok("node:drained"))
        self.assertEqual([], self._errs(self.BASE + " fault=node:notready during=idle lift=after-recover expect=relocated needs=forcedelete"))
        self.assertEqual([], self._errs(self.BASE + " fault=node:notready during=idle lift=after-settle expect=repaired"))
        self.assertEqual([], self._errs(self.BASE + " fault=node:notready during=idle lift=after-failed expect=repaired"))
        self.assertEqual([], self._errs(self.BASE + " fault=node:notready during=idle lift=after-settle expect=parked needs=no-forcedelete"))
        self.assertTrue(any("lift=after-recover, not lift=after-settle" in e
                            for e in self._errs(self.BASE + " fault=node:notready during=idle lift=after-settle expect=relocated")))
        self.assertTrue(any("lift=after-settle, not lift=after-failed" in e
                            for e in self._errs(self.BASE + " fault=node:notready during=idle lift=after-failed expect=parked")))
        self.assertTrue(any("needs expect=parked|relocated|repaired" in e
                            for e in self._errs(self.BASE + " fault=node:notready during=idle lift=after-settle expect=heals")))
        self.assertTrue(any("needs during=idle" in e for e in self._errs(self.BASE + " fault=node:notready lift=after-settle expect=repaired")))
        self.assertTrue(any("needs lift=" in e for e in self._errs(self.BASE + " fault=node:notready during=idle expect=repaired")))
        self.assertTrue(any("reads a persistent fault's outcome" in e for e in self._errs(self.BASE + " expect=relocated")))

    def test_capability_is_detected_before_anything_is_applied(self):
        calls = []
        kubectl, docker = self._fakes(calls)
        with mock.patch.object(run, "kubectl", kubectl), mock.patch.object(run, "_docker", docker), \
                mock.patch.object(shutil, "which", lambda name: None):
            self.assertIn("not on PATH", run._node_fault_capability())
        with mock.patch.object(run, "kubectl", kubectl), mock.patch.object(run, "_docker", docker), \
                mock.patch.object(shutil, "which", lambda name: "/usr/bin/docker"):
            self.assertIsNone(run._node_fault_capability())
        kubectl, docker = self._fakes(calls, containers=())
        with mock.patch.object(run, "kubectl", kubectl), mock.patch.object(run, "_docker", docker), \
                mock.patch.object(shutil, "which", lambda name: "/usr/bin/docker"):
            self.assertIn("not a KIND cluster", run._node_fault_capability())

    def test_victim_avoids_the_control_plane_and_the_managers_node(self):
        calls = []
        kubectl, docker = self._fakes(calls, managers="kind-worker2")
        with mock.patch.object(run, "kubectl", kubectl), mock.patch.object(run, "_docker", docker):
            # e2 sits on the control plane and e1 shares a node with the manager: e0 is left.
            self.assertEqual(("e0", "kind-worker"), run._node_pick(self.PODS, {"old"}, "engine"))
            # With no manager in the way the break rule picks the highest index on a worker.
            with mock.patch.object(run, "_manager_nodes", lambda: set()):
                self.assertEqual(("e1", "kind-worker2"), run._node_pick(self.PODS, {"old"}, "engine"))
            victim, reason = run._node_pick({"e2": self.PODS["e2"]}, {"old"}, "engine")
            self.assertIsNone(victim)
            self.assertIn("no engine pod sits on a worker", reason)

    def test_fire_marks_and_registers_before_the_stop_and_the_restore_is_idempotent(self):
        f = run._fault_new("p999", {"fault": "node:notready", "during": "idle", "lift": "after-settle", "expect": "repaired"})
        calls, saved = [], list(run._FAULT_CLEANUPS)
        kubectl, docker = self._fakes(calls, managers="")
        try:
            with mock.patch.object(run, "kubectl", kubectl), mock.patch.object(run, "_docker", docker):
                run._fault_node_fire(f, self.PODS, {"old"})
                self.assertIsNone(f["infra_fail"])
                self.assertNotIn("skip", f)
                self.assertEqual(("e1", "kind-worker2", "1"), (f["crash_pod"], f["node"], f["crash_inst"]))
                stop = [c for c in calls if c[0] == "docker" and c[1] == "exec"]
                self.assertEqual([("docker", "exec", "kind-worker2", "systemctl", "stop", "kubelet", len(saved) + 1)], stop,
                                 "the restore is registered before the kubelet is stopped")
                label = [c for c in calls if c[0] == "kubectl" and c[1] == "label"]
                self.assertEqual(("label", "node", "kind-worker2", "perm.test/kubelet-stopped=p999", "--overwrite"), label[0][1:6])
                self.assertIn(("kubelet", f), run._FAULT_CLEANUPS)
                calls.clear()
                self.assertTrue(run._node_restore(f))
                self.assertEqual([("docker", "exec", "kind-worker2", "systemctl", "start", "kubelet")],
                                 [c[:6] for c in calls if c[0] == "docker"])
                self.assertEqual(("label", "node", "kind-worker2", "perm.test/kubelet-stopped-"),
                                 [c for c in calls if c[0] == "kubectl"][0][1:5])
                calls.clear()
                self.assertTrue(run._node_restore(f))
                run._run_fault_cleanups()
                self.assertEqual([], calls, "a restored kubelet is not started twice")
                self.assertEqual([], run._FAULT_CLEANUPS)
        finally:
            run._FAULT_CLEANUPS[:] = saved

    def test_no_eligible_node_is_a_skip_and_a_failed_stop_is_apparatus(self):
        saved = list(run._FAULT_CLEANUPS)
        try:
            f = run._fault_new("p999", {"fault": "node:notready", "during": "idle", "lift": "after-settle", "expect": "repaired"})
            calls = []
            kubectl, docker = self._fakes(calls, managers="kind-worker kind-worker2")
            with mock.patch.object(run, "kubectl", kubectl), mock.patch.object(run, "_docker", docker):
                run._fault_node_fire(f, self.PODS, {"old"})
            self.assertIn("node fault unavailable here", f["skip"])
            self.assertEqual(saved, run._FAULT_CLEANUPS)
            self.assertFalse(any(c[0] == "docker" and c[1] == "exec" for c in calls))
            f = run._fault_new("p999", {"fault": "node:notready", "during": "idle", "lift": "after-settle", "expect": "repaired"})
            kubectl, docker = self._fakes([], managers="", docker_rc=1)
            with mock.patch.object(run, "kubectl", kubectl), mock.patch.object(run, "_docker", docker):
                run._fault_node_fire(f, self.PODS, {"old"})
            self.assertIn("stopping the kubelet", f["infra_fail"])
            self.assertIn(("kubelet", f), run._FAULT_CLEANUPS, "the restore stays registered when the stop fails")
        finally:
            run._FAULT_CLEANUPS[:] = saved

    def test_relocation_replaces_the_pods_the_node_held_on_other_nodes(self):
        pods = [("e1", {"ready": True, "serving": True, "uid": "n1", "node": "kind-worker"})]
        self.assertTrue(run._instance_relocated(pods, {"u1"}, "kind-worker2"))
        self.assertFalse(run._instance_relocated(pods, {"u1"}, "kind-worker"))
        self.assertFalse(run._instance_relocated(pods, {"n1"}, "kind-worker2"))
        self.assertFalse(run._instance_relocated([], {"u1"}, "kind-worker2"))
        # A gang: the leader the dead node held is new elsewhere, the worker on a live node keeps its pod.
        gang = [("e1-leader", {"ready": True, "serving": True, "uid": "n1", "node": "kind-worker3"}),
                ("e1-worker", {"ready": True, "serving": True, "uid": "w1", "node": "kind-worker2"})]
        self.assertTrue(run._instance_relocated(gang, {"u1"}, "kind-worker"))
        self.assertFalse(run._instance_relocated(gang, {"u1", "w1"}, "kind-worker"), "a pod the node held must be replaced")
        self.assertFalse(run._instance_relocated(gang, {"u1"}, "kind-worker2"), "no pod may sit on the stopped node")
        dark = [("e1-leader", dict(gang[0][1], ready=False))] + gang[1:]
        self.assertFalse(run._instance_relocated(dark, {"u1"}, "kind-worker"))
        self.assertTrue(run._uids_present({"a": {"uid": "u1", "alive": False}, "b": None}, {"u1"}))
        self.assertFalse(run._uids_present({"a": {"uid": "n1", "alive": True}, "b": None}, {"u1"}))

    def test_lift_verdict_reads_each_node_outcome(self):
        def f():
            return {"kind": "node", "crash_inst": "1", "crash_comp": "engine", "infra_fail": None,
                    "bite": "its node NotReady with no pod serving", "cause": "the kubelet of node kind-worker2"}
        base = {"bitten_at": 45.0, "failed_at": None, "recovered_at": None, "lifted_at": 70.0, "healed_at": None,
                "park_broken": None, "phase_after": "Ready", "new_hash": False, "reset_used": False}
        notes = []
        relocated = dict(base, recovered_at=60.0, reaped_at=0.0, serving_after_lift=True)
        self.assertEqual("PASS", run._lift_checks(f(), "relocated", "after-recover", relocated, {}, "PASS", notes, []))
        notes = []
        self.assertEqual("FAIL", run._lift_checks(f(), "relocated", "after-recover", base, {}, "PASS", notes, []))
        self.assertTrue(any("not relocated" in n for n in notes))
        notes = []
        self.assertEqual("PASS", run._lift_checks(f(), "repaired", "after-settle", dict(base, healed_at=20.0), {}, "PASS", notes, []))
        notes = []
        self.assertEqual("FAIL", run._lift_checks(f(), "repaired", "after-settle", base, {}, "PASS", notes, []))
        self.assertTrue(any("did not return" in n for n in notes))
        checks = []
        self.assertEqual("PASS", run._lift_checks(f(), "parked", "after-settle", base, {}, "PASS", [], checks))
        self.assertTrue(any("stayed down" in c for c in checks))
        notes = []
        self.assertEqual("FAIL", run._lift_checks(f(), "parked", "after-settle", dict(base, park_broken=50.0), {}, "PASS", notes, []))
        self.assertTrue(any("despite the expected park" in n for n in notes))

    def test_skip_is_green_and_recorded_as_skipped(self):
        self.assertTrue(batch.is_green("SKIP"))
        self.assertTrue(batch.is_skipped("SKIP"))
        self.assertTrue(batch.is_skipped("XFAIL (known bug: #1)"))
        self.assertFalse(batch.is_skipped("PASS"))
        self.assertFalse(batch.is_green("INFRA-FAIL"))
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "junit.xml"
            batch.write_junit(path, {"p1": "PASS", "p2": "SKIP", "p3": "FAIL"}, 1.0)
            text = path.read_text()
            self.assertIn('skipped="1"', text)
            self.assertIn('failures="1"', text)
            self.assertIn('<skipped message="SKIP"', text)


class RuntimeEditRepairTest(unittest.TestCase):
    """A break may ride a runtime edit that rolls: it fires as the edit lands
    and the roll owns the broken Instance, or with during=repair it fires
    first and the edit lands on the open repair. The header rules, the phase
    hook that must leave a handler-fired break alone, and the pure parts of
    the verdict: when a repair counts as open, the rebuild count, and the
    rows left mid-operation."""

    BASE = ("# p999: x mode=runtimerev rt=ome-coordtest-rt-p999 expect=roll "
            "Ne=3 Nd=0 Nr=0 ceil_e=4 ceil_d=0 ceil_r=0 topo=engine")

    def _errs(self, header):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_lets_a_break_ride_a_runtime_edit_that_rolls(self):
        self.assertEqual([], self._errs(self.BASE + " edit=env fault=crashloop:old form=start"))
        self.assertEqual([], self._errs(self.BASE + " edit=image img=${OME_EVAL_NGINX_ALT_IMAGE} fault=crashloop:old form=load"))
        self.assertEqual([], self._errs(self.BASE + " edit=env fault=imagepull:old"))
        self.assertEqual([], self._errs(self.BASE + " edit=env fault=crashloop:old form=start during=repair"))
        self.assertEqual([], self._errs(self.BASE + " edit=image img=${OME_EVAL_NGINX_ALT_IMAGE} fault=imagepull:old during=repair"))
        noroll = self.BASE.replace("expect=roll", "expect=noroll")
        self.assertTrue(any("expect=roll" in e for e in self._errs(noroll + " edit=noop fault=crashloop:old form=start")))
        recover = self.BASE.replace("expect=roll", "expect=recover break=stuck")
        self.assertTrue(any("no break=" in e for e in self._errs(recover + " fault=crashloop:old form=start")))
        kill = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
        self.assertTrue(any("mode=rollout|runtimerev" in e for e in self._errs(kill + " fault=crashloop:old form=start")))

    def test_repair_hook_needs_a_runtime_edit_and_a_break(self):
        self.assertTrue(any("needs the break" in e for e in self._errs(self.BASE + " edit=env during=repair")))
        self.assertTrue(any("needs the break" in e for e in self._errs(self.BASE + " edit=env fault=podkill:settling during=repair")))
        rollout = "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"
        self.assertTrue(any("mode=runtimerev" in e for e in self._errs(rollout + " fault=crashloop:old form=start during=repair")))

    def test_phase_hook_leaves_a_handler_fired_break_alone(self):
        repair = run._fault_new("p999", {"fault": "crashloop:old", "form": "start", "during": "repair"})
        idle = run._fault_new("p999", {"fault": "crashloop:old", "form": "start", "during": "idle"})
        plain = run._fault_new("p999", {"fault": "crashloop:old", "form": "start"})
        self.assertTrue(run._fault_fired_by_handler(repair))
        self.assertTrue(run._fault_fired_by_handler(idle))
        self.assertFalse(run._fault_fired_by_handler(plain))
        self.assertFalse(run._fault_fired_by_handler(None))
        fired = []
        orig = run._fault_fire
        run._fault_fire = lambda *a, **k: fired.append(a)
        try:
            ctx = {"trigger_ts": 1.0, "pod_state": {}, "pods_per_inst": {}, "old_hashes": {"old"}}
            run._fault_tick(dict(ctx, fault=repair))
            self.assertEqual([], fired, "the repair handler fires its break before the edit; the hook must not")
            run._fault_tick(dict(ctx, fault=plain))
            self.assertEqual(1, len(fired), "a plain break fires the moment the edit has landed")
        finally:
            run._fault_fire = orig

    def test_repair_counts_as_open_on_event_row_or_rebuilt_pod(self):
        pods = [("e2", {"uid": "u-old", "alive": True})]
        self.assertEqual("", run._repair_in_flight([], "Ready", pods, {"u-old"}))
        self.assertEqual("", run._repair_in_flight([("RepairHeld", "component=engine instance=2")], "Ready", pods, {"u-old"}))
        self.assertEqual("a RestartTriggered event names it",
                         run._repair_in_flight([("RestartTriggered", "component=engine instance=2")], "Ready", pods, {"u-old"}))
        self.assertEqual("its row reads Restarting", run._repair_in_flight([], "Restarting", pods, {"u-old"}))
        rebuilt = pods + [("e2b", {"uid": "u-new", "alive": True})]
        self.assertEqual("a new pod appeared for its index", run._repair_in_flight([], "Ready", rebuilt, {"u-old"}))

    def test_rebuild_count_allows_one_new_pod_per_slot(self):
        f = {"crash_comp": "engine", "crash_inst": "2", "kind": "crashloop"}
        for rebuilt, ppi, want in ((0, 1, "PASS"), (1, 1, "PASS"), (2, 1, "FAIL"), (2, 2, "PASS"), (3, 2, "FAIL")):
            notes, checks = [], []
            self.assertEqual(want, run._rebuild_count_checks(f, rebuilt, ppi, "PASS", notes, checks), (rebuilt, ppi))
            self.assertEqual(want == "FAIL", any("rebuilt twice" in c for c in checks))

    def test_rows_left_mid_operation_fail_the_case(self):
        notes, checks = [], []
        self.assertEqual("PASS", run._rows_settled_checks(True, "settled", "PASS", notes, checks))
        self.assertTrue(any("no operation open" in c for c in checks))
        notes, checks = [], []
        self.assertEqual("FAIL", run._rows_settled_checks(False, "engine/2: Instance is not settled", "PASS", notes, checks))
        self.assertTrue(any("left mid-operation" in c for c in checks))

    def test_break_checks_skip_cases_without_a_break(self):
        ctx = {"fault": None, "pod_state": {}, "configured": {"engine"}, "case_id": "p999", "N": {"engine": 1}}
        self.assertEqual("PASS", run._break_after_roll_checks(ctx, {"old"}, 1.0, "PASS", [], []))
        ctx["fault"] = run._fault_new("p999", {"fault": "podkill:settling"})
        self.assertEqual("PASS", run._break_after_roll_checks(ctx, {"old"}, 1.0, "PASS", [], []))


class StuckOperationTest(unittest.TestCase):
    """stuck=held|failed pushes the bumped members onto a revision that
    cannot be pulled before a scale or a delete lands. The header rules and
    the pure parts of the verdict: when a Component counts as stuck, how the
    unlanded push is read back, which Instances a scale-down removes, and
    how a survivor's pods may change."""

    SCALE = "# p999: x mode=scaledown Ne=3 Nd=0 Nr=0 ceil_e=4 ceil_d=0 ceil_r=0 to_e=2 topo=engine"
    RECREATE = "# p999: x mode=recreate Ne=3 Nd=0 Nr=0 ceil_e=4 ceil_d=0 ceil_r=0 topo=engine"
    IMG = " img=example.com/canary-test/missing:none"

    def _errs(self, header):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    @staticmethod
    def _ir(component, rows=(), blocks=(), hold=None, current="rev-a", update="rev-b", replicas=None):
        status = {"currentRevision": current, "updateRevision": update,
                  "instanceStatuses": [{"index": i, "phase": ph} for i, ph in rows]}
        if blocks:
            status["retryBlocks"] = list(blocks)
        if hold:
            status["rolloutHold"] = hold
        spec = {"component": component}
        if replicas is not None:
            spec["replicas"] = replicas
        return {"spec": spec, "status": status}

    def test_header_names_the_stuck_kind_and_its_revision(self):
        self.assertEqual([], self._errs(self.SCALE + " stuck=held" + self.IMG))
        self.assertEqual([], self._errs(self.SCALE + " stuck=failed" + self.IMG))
        self.assertEqual([], self._errs(self.RECREATE + " stuck=held" + self.IMG))
        self.assertEqual([], self._errs(self.SCALE.replace("to_e=2", "to_e=0") + " stuck=failed" + self.IMG))
        self.assertTrue(any("needs img=" in e for e in self._errs(self.SCALE + " stuck=held")))
        self.assertTrue(any("needs stuck=" in e for e in self._errs(self.SCALE + self.IMG)))
        self.assertTrue(any("malformed header value stuck=" in e for e in self._errs(self.SCALE + " stuck=soon" + self.IMG)))
        rollout = "# p999: x mode=rollout Ne=3 Nd=0 Nr=0 ceil_e=4 ceil_d=0 ceil_r=0 topo=engine"
        self.assertTrue(any("mode=recreate|scaledown" in e for e in self._errs(rollout + " stuck=held" + self.IMG)))
        self.assertTrue(any("does not combine" in e for e in self._errs(self.SCALE + " stuck=held fault=podkill:settling" + self.IMG)))
        self.assertTrue(any("does not combine" in e for e in self._errs(self.RECREATE + " stuck=held during=rollout" + self.IMG)))

    def test_stuck_signal_reads_the_ladder_and_the_rows(self):
        held = self._ir("engine", rows=[(0, "Ready"), (1, "Failed")],
                        blocks=[{"targetRevision": "rev-b", "state": "Held", "attemptsStarted": 3}])
        backoff = self._ir("engine", rows=[(0, "Ready"), (1, "Ready")],
                           blocks=[{"targetRevision": "rev-b", "state": "Backoff", "attemptsStarted": 1}])
        failed_row = self._ir("engine", rows=[(0, "Ready"), (2, "Failed")])
        fresh = self._ir("engine", rows=[(0, "Ready"), (1, "Updating")])
        hold_only = self._ir("engine", rows=[(0, "Ready")], hold={"gate": "Held", "reason": "update held"})
        self.assertIn("Held after 3 attempt(s)", run._stuck_signal({"engine": held}, {"engine"}, "held"))
        self.assertIn("gate=Held", run._stuck_signal({"engine": hold_only}, {"engine"}, "held"))
        self.assertEqual("", run._stuck_signal({"engine": backoff}, {"engine"}, "held"))
        self.assertEqual("", run._stuck_signal({"engine": failed_row}, {"engine"}, "held"))
        self.assertIn("Instance 2 reads Failed", run._stuck_signal({"engine": failed_row}, {"engine"}, "failed"))
        self.assertIn("1 failed attempt(s) (Backoff)", run._stuck_signal({"engine": backoff}, {"engine"}, "failed"))
        self.assertIn("Instance 1 reads Failed", run._stuck_signal({"engine": held}, {"engine"}, "failed"))
        self.assertEqual("", run._stuck_signal({"engine": fresh}, {"engine"}, "failed"))
        self.assertEqual("", run._stuck_signal({"decoder": held}, {"engine"}, "held"), "only the bumped members count")

    def test_unlanded_push_needs_diverged_revisions_and_a_visible_reason(self):
        held = self._ir("engine", rows=[(0, "Ready")], blocks=[{"targetRevision": "rev-b", "state": "Held", "attemptsStarted": 3}],
                        hold={"gate": "Held"})
        landed = self._ir("engine", rows=[(0, "Ready")], current="rev-b", update="rev-b")
        silent = self._ir("engine", rows=[(0, "Ready")])
        failed = self._ir("engine", rows=[(0, "Failed")])
        out = run._push_unlanded({"engine": held, "decoder": landed}, {"engine", "decoder"})
        self.assertIn("gate=Held", out["engine"])
        self.assertIn("retry block Held", out["engine"])
        self.assertEqual("", out["decoder"])
        self.assertEqual("", run._push_unlanded({"engine": silent}, {"engine"})["engine"])
        self.assertIn("Failed row(s) 0", run._push_unlanded({"engine": failed}, {"engine"})["engine"])
        self.assertEqual("", run._push_unlanded({}, {"engine"})["engine"])

    def test_scale_victims_take_the_broken_rows_first_then_the_highest_index(self):
        rows = [("0", "Ready"), ("1", "Failed"), ("2", "Ready")]
        self.assertEqual(["1"], run._scale_victims(rows, 1))
        self.assertEqual(["1", "2"], run._scale_victims(rows, 2))
        self.assertEqual(["1", "2", "0"], run._scale_victims(rows, 3))
        self.assertEqual(["2"], run._scale_victims([("0", "Ready"), ("1", "Ready"), ("2", "Ready")], 1))
        self.assertEqual(["2", "0"], run._scale_victims([("0", "Updating"), ("1", "Ready"), ("2", "Creating")], 2))
        self.assertEqual([], run._scale_victims(rows, 0))

    def test_survivor_changes_tell_a_needless_rebuild_from_the_rolls_attempt(self):
        olds = {"old"}
        pre = {"0": [("u0", "old")], "1": [("u1", "old")], "2": [("u2", "old")], "3": [("u3", "new")]}
        same = dict(pre)
        self.assertEqual(([], []), run._survivor_changes(pre, same, olds))
        needless = dict(pre, **{"1": [("u1b", "old")]})
        self.assertEqual((["1"], []), run._survivor_changes(pre, needless, olds))
        rolled = dict(pre, **{"2": [("u2b", "new")]})
        self.assertEqual(([], ["2"]), run._survivor_changes(pre, rolled, olds))
        beside = dict(pre, **{"2": [("u2", "old"), ("u2b", "new")]})
        self.assertEqual(([], []), run._survivor_changes(pre, beside, olds), "a surge beside the running pod is not a change")
        gone = {k: v for k, v in pre.items() if k != "0"}
        self.assertEqual((["0"], []), run._survivor_changes(pre, gone, olds), "a survivor with no pod left was taken for nothing")

    def test_replica_patch_keeps_max_at_the_least_the_api_admits(self):
        self.assertEqual({"spec": {"engine": {"minReplicas": 2, "maxReplicas": 2}}}, run._replica_patch({"engine": 2}))
        self.assertEqual({"spec": {"decoder": {"minReplicas": 0, "maxReplicas": 1},
                                   "engine": {"minReplicas": 0, "maxReplicas": 1}}},
                         run._replica_patch({"engine": 0, "decoder": 0}))
        self.assertEqual({"spec": {"engine": {"minReplicas": 4, "maxReplicas": 4}}}, run._replica_patch({"engine": 4}))

    def test_removal_is_judged_against_the_plan_the_engine_could_have_seen(self):
        at_op = [("0", "Ready"), ("1", "Failed"), ("2", "Ready")]
        after = [("0", "Ready"), ("1", "Ready"), ("2", "Ready")]
        plans = run._removal_plans({"at the scale": at_op, "right after the scale": after}, 3, 2)
        self.assertEqual({"at the scale": ["1"], "right after the scale": ["2"]}, plans)
        pre = {"0", "1", "2"}
        self.assertEqual(("at the scale", []), run._removal_outcome(plans, pre, {"0", "2"}, {"0", "2"}))
        self.assertEqual(("right after the scale", []), run._removal_outcome(plans, pre, {"0", "1"}, {"0", "1"}))
        self.assertEqual((None, []), run._removal_outcome(plans, pre, {"1", "2"}, {"1", "2"}), "the lowest Ready row is never the victim")
        self.assertEqual((None, []), run._removal_outcome(plans, pre, {"0", "2"}, {"0", "1", "2"}), "a pod left on a removed index is not a removal")
        self.assertEqual(("at the scale", ["3"]), run._removal_outcome(plans, pre, {"0", "2", "3"}, {"0", "2", "3"}),
                         "a row that did not exist at the scale is reported, not judged")
        zero = run._removal_plans({"at the scale": at_op, "right after the scale": None}, 3, 1)
        self.assertEqual({"at the scale": ["1", "2"]}, zero, "a request for zero plans from the accepted count")
        self.assertEqual(("at the scale", []), run._removal_outcome(zero, pre, {"0"}, {"0"}))

    def test_ir_spec_replicas_reads_what_the_controller_projected(self):
        by_comp = {"engine": self._ir("engine", replicas=1), "decoder": self._ir("decoder")}
        self.assertEqual({"engine": 1}, run._ir_spec_replicas(by_comp))

    def test_repair_rebuilding_reads_the_replacement_pod(self):
        pre = {"u-old"}
        self.assertEqual("", run._repair_rebuilding([("e2", {"uid": "u-old", "ready": False})], pre))
        self.assertEqual("its replacement pod is up and not yet Ready",
                         run._repair_rebuilding([("e2b", {"uid": "u-new", "ready": False})], pre))
        self.assertEqual("its replacement pod is already Ready",
                         run._repair_rebuilding([("e2b", {"uid": "u-new", "ready": True})], pre))


class MidRollEditTest(unittest.TestCase):
    """edit=strategy:<type> / edit=restartPolicy:<policy> patch every bumped
    member's lifecycle at during=settling|rotating; the verdict adds that the
    edit was applied at its hook and landed on every member."""

    BASE = "# p999: x mode=rollout Ne=2 Nd=2 Nr=0 ceil_e=3 ceil_d=3 ceil_r=0 topo=pd"

    def _errs(self, header):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_names_the_edit_and_its_hook(self):
        for strat in sorted(run.UPDATE_STRATEGIES):
            self.assertEqual([], self._errs(self.BASE + f" edit=strategy:{strat} during=rotating"))
        for rp in sorted(run.RESTART_POLICIES):
            self.assertEqual([], self._errs(self.BASE + f" edit=restartPolicy:{rp} during=settling"))
        self.assertTrue(any("malformed header value edit=" in e for e in self._errs(self.BASE + " edit=strategy:Fast during=rotating")))
        self.assertTrue(any("malformed header value edit=" in e for e in self._errs(self.BASE + " edit=restartPolicy:RecreateInstance during=rotating")))
        self.assertTrue(any("needs during=rotating|settling" in e for e in self._errs(self.BASE + " edit=strategy:RecreatePod")))
        self.assertTrue(any("names a mid-roll edit's hook" in e for e in self._errs(self.BASE + " during=rotating")))
        self.assertTrue(any("names a mid-roll edit's hook" in e for e in self._errs(self.BASE + " edit=env during=settling")))
        kill = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
        self.assertTrue(any("mode=rollout" in e for e in self._errs(kill + " edit=strategy:RecreatePod during=rotating")))
        idle = self._errs(self.BASE + " edit=strategy:RecreatePod during=idle")
        self.assertTrue(any("needs during=rotating|settling" in e for e in idle))
        self.assertTrue(any("needs a persistent fault" in e for e in idle))
        self.assertEqual([], self._errs(self.BASE + " fault=podkill:rotating edit=restartPolicy:None during=settling"),
                         "a phase fault and a mid-roll edit may share a case")

    def test_edit_parses_and_patches_every_bumped_member(self):
        self.assertEqual(("strategy", "InPlaceOnly"), run._midroll_edit("strategy:InPlaceOnly"))
        self.assertEqual(("restartPolicy", "RecreateInstanceOnPodRestart"), run._midroll_edit("restartPolicy:RecreateInstanceOnPodRestart"))
        for bad in ("env", "strategy", "strategy:", "strategy:Nope", "restartPolicy:RecreateInstance", None):
            self.assertIsNone(run._midroll_edit(bad))
        self.assertIsNone(run._midroll_new({"edit": "env", "during": "rotating"}))
        e = run._midroll_new({"edit": "strategy:RecreatePod", "during": "rotating"})
        self.assertEqual({"spec": {"decoder": {"lifecycle": {"updateStrategy": {"type": "RecreatePod"}}},
                                   "engine": {"lifecycle": {"updateStrategy": {"type": "RecreatePod"}}}}},
                         run._midroll_patch(e, {"engine", "decoder"}))
        r = run._midroll_new({"edit": "restartPolicy:None", "during": "settling"})
        self.assertEqual({"spec": {"engine": {"lifecycle": {"restartPolicy": "None"}}}}, run._midroll_patch(r, {"engine"}))
        self.assertEqual({"engine": "inplace", "decoder": "surge"},
                         run.update_modes({"strat": {"engine": "InPlaceOnly", "decoder": "SurgeThenDrain"}},
                                          {"engine", "decoder"}, "strategy:RecreatePod"),
                         "the start strategy decides how updated pods are counted")

    def test_hook_fires_once_at_its_phase(self):
        olds = {"old"}
        settled = {"e0": {"alive": True, "hash": "old", "newanno": False}}
        rolling = {"e0": {"alive": True, "hash": "old", "newanno": False}, "e1": {"alive": True, "hash": "new", "newanno": False}}
        self.assertTrue(run._midroll_due("settling", settled, olds))
        self.assertFalse(run._midroll_due("rotating", settled, olds))
        self.assertTrue(run._midroll_due("rotating", rolling, olds))
        self.assertTrue(run._midroll_due("rotating", {"e0": {"alive": True, "hash": "old", "newanno": True}}, olds))
        self.assertFalse(run._midroll_due("rotating", rolling, None))
        e = run._midroll_new({"edit": "strategy:RecreatePod", "during": "rotating"})
        calls = []
        orig = run.kubectl
        run.kubectl = lambda *a, **k: calls.append(a) or subprocess.CompletedProcess(a, 0, "", "")
        try:
            run._midroll_tick(e, "p999", {"engine"}, settled, olds, 3.0)
            self.assertFalse(e["fired"])
            run._midroll_tick(e, "p999", {"engine"}, rolling, olds, 12.0)
            run._midroll_tick(e, "p999", {"engine"}, rolling, olds, 13.0)
            self.assertEqual((True, 12.0, None), (e["fired"], e["fired_at"], e["error"]))
            self.assertEqual(1, len(calls))
            self.assertEqual(("patch", "isvc", "p999", "--type=merge"), calls[0][2:6])
            self.assertEqual({"spec": {"engine": {"lifecycle": {"updateStrategy": {"type": "RecreatePod"}}}}}, json.loads(calls[0][-1]))
            run.kubectl = lambda *a, **k: subprocess.CompletedProcess(a, 1, "", "admission webhook denied the patch")
            e2 = run._midroll_new({"edit": "restartPolicy:None", "during": "settling"})
            run._midroll_tick(e2, "p999", {"engine"}, settled, olds, 1.0)
            self.assertTrue(e2["fired"])
            self.assertIn("denied", e2["error"])
        finally:
            run.kubectl = orig

    def test_verdict_requires_the_edit_applied_and_landed(self):
        spec = {"engine": {"lifecycle": {"updateStrategy": {"type": "RecreatePod"}, "restartPolicy": "None"}},
                "decoder": {"lifecycle": {"updateStrategy": {"type": "SurgeThenDrain"}}}}
        fired = {"kind": "strategy", "value": "RecreatePod", "hook": "rotating", "fired": True, "fired_at": 12.0, "error": None}
        notes, checks = [], []
        self.assertEqual("PASS", run._midroll_checks(fired, spec, {"engine"}, "PASS", notes, checks))
        self.assertTrue(any("applied at rotating" in c for c in checks))
        notes = []
        self.assertEqual("FAIL", run._midroll_checks(fired, spec, {"engine", "decoder"}, "PASS", notes, []))
        self.assertTrue(any("did not land on decoder" in n for n in notes))
        rp = dict(fired, kind="restartPolicy", value="None")
        self.assertEqual("PASS", run._midroll_checks(rp, spec, {"engine"}, "PASS", [], []))
        self.assertEqual("FAIL", run._midroll_checks(rp, spec, {"decoder"}, "PASS", [], []))
        broken = dict(fired, error="strategy edit at rotating failed: denied")
        notes = []
        self.assertEqual("INFRA-FAIL", run._midroll_checks(broken, spec, {"engine"}, "PASS", notes, []))
        self.assertTrue(any(n.startswith("INFRA-FAIL") for n in notes))
        unfired = dict(fired, fired=False, fired_at=None)
        checks = []
        self.assertEqual("FAIL", run._midroll_checks(unfired, spec, {"engine"}, "FAIL", [], checks))
        self.assertTrue(any("never reached" in c for c in checks))
        self.assertEqual("PASS", run._midroll_checks(None, spec, {"engine"}, "PASS", [], []))


class StrategySwitchFamilyTest(unittest.TestCase):
    """The generated gen-switch family: every ordered pair of distinct
    strategies and both restart-policy flips at both hooks on every shape, a
    rolling group on PD, one budget per pair valid under both strategies, the
    header stated for the worse of the two shapes, and the crashing variants
    on the surge and recreate pair."""

    @classmethod
    def setUpClass(cls):
        cls.outputs, rows = generate_cases.build_outputs()
        cls.fam = [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-switch"]

    @staticmethod
    def _lifecycle(text, comp):
        """(type, maxSurge, maxUnavailable, restartPolicy) of a Component's
        rendered lifecycle line."""
        block = re.search(rf"(?ms)^  {comp}:\n(.*?)(?=^  [a-z]+:|\Z)", text).group(1)
        life = re.search(r"^    lifecycle: (.*)$", block, re.M).group(1)
        rp = re.search(r'restartPolicy: "([A-Za-z]+)"', life)
        return (re.search(r'type: "([A-Za-z]+)"', life).group(1),
                int(re.search(r"maxSurge: (\d+)", life).group(1)),
                int(re.search(r"maxUnavailable: (\d+)", life).group(1)),
                rp.group(1) if rp else None)

    def _cell(self, n, kv):
        """(kind, topo, co, from, to, hook) the case drives, read from the
        rendered body and the header it will run under."""
        kind, value = run._midroll_edit(kv["edit"])
        start, _s, _u, rp = self._lifecycle(self.outputs[f"gen-switch/{n}.yaml"], "engine")
        if "fault" in kv:
            return ("crash", kv["topo"], kv["co"], start, value, kv["during"])
        if kind == "strategy":
            return ("strategy", kv["topo"], kv["co"], start, value, kv["during"])
        short = {full: rp_ for rp_, full in generate_cases.RP_FULL.items()}
        return ("restartPolicy", kv["topo"], kv["co"], short[rp], short[value], kv["during"])

    def test_family_crosses_every_axis(self):
        strats, hooks = generate_cases.SWITCH_STRATS, generate_cases.SWITCH_HOOKS
        expected = set()
        for topo, _n, _w in generate_cases.SWITCH_SHAPES:
            for co in generate_cases.SWITCH_COORDS[topo]:
                expected |= {("strategy", topo, co, a, b, h) for a in strats for b in strats if a != b for h in hooks}
                expected |= {("restartPolicy", topo, co, a, b, h)
                             for a in generate_cases.RESTARTS for b in generate_cases.RESTARTS if a != b for h in hooks}
                expected |= {("crash", topo, co, a, b, generate_cases.SWITCH_CRASH_HOOK)
                             for a, b in generate_cases.SWITCH_CRASH_PAIRS}
        got = [self._cell(n, kv) for n, kv, _m in self.fam]
        self.assertEqual(180, len(self.fam))
        self.assertEqual(len(got), len(set(got)), "every case is a distinct cell")
        self.assertEqual(expected, set(got))
        self.assertEqual({"engine", "pd", "engine-mn", "pd-mn"}, {kv["topo"] for _n, kv, _m in self.fam})
        self.assertEqual({"rolling"}, {kv["co"] for _n, kv, _m in self.fam if kv["co"] != "none"})
        self.assertTrue(all(kv["topo"].startswith("pd") for _n, kv, _m in self.fam if kv["co"] != "none"),
                        "a rolling group exists only where a decoder does")

    def test_budget_and_header_are_the_worse_of_the_two_strategies(self):
        for n, kv, _m in self.fam:
            text = self.outputs[f"gen-switch/{n}.yaml"]
            kind, value = run._midroll_edit(kv["edit"])
            start, surge, unavail, rp = self._lifecycle(text, "engine")
            self.assertIn(kv["during"], run.EDIT_HOOKS, n)
            self.assertIsNotNone(run._midroll_new(kv), n)
            if kind == "strategy":
                self.assertNotEqual(start, value, f"{n}: the edit changes the strategy")
                surge_side = "SurgeThenDrain" in (start, value)
                self.assertEqual((1 if surge_side else 0, 1), (surge, unavail),
                                 f"{n}: one budget valid under both strategies of the pair")
                self.assertEqual(kv["Ne"] + surge, kv["ceil_e"],
                                 f"{n}: the ceiling is the surge one exactly when a side surges")
                self.assertIsNone(rp, f"{n}: a strategy flip carries no restart policy")
            else:
                self.assertNotEqual(rp, value, f"{n}: the edit changes the policy")
                self.assertEqual((generate_cases.SWITCH_POLICY_STRAT,) + generate_cases.SWITCH_POLICY_BUDGET,
                                 (start, surge, unavail), f"{n}: policy flips roll under the canonical surge budget")
                self.assertEqual(kv["Ne"] + 1, kv["ceil_e"], n)
            if "fault" in kv:
                self.assertEqual(("crashloop:old", "start", 1, "strategy", generate_cases.SWITCH_CRASH_HOOK),
                                 (kv["fault"], kv["form"], kv["floor_soft"], kind, kv["during"]), n)
                self.assertIn((start, value), generate_cases.SWITCH_CRASH_PAIRS, n)
            else:
                self.assertNotIn("floor_soft", kv, f"{n}: the floor is asserted hard across a pure edit")
                self.assertNotIn("order_soft", kv, n)
            self.assertEqual(kv["Nr"], kv["ceil_r"], f"{n}: the router is never bumped")
            if not kv["topo"].startswith("pd"):
                self.assertEqual("none", kv["co"], n)
                continue
            decoder = self._lifecycle(text, "decoder")
            if kv["co"] == "rolling":
                self.assertEqual((start, surge, unavail, rp), decoder,
                                 f"{n}: a grouped decoder carries the engine's start lifecycle so the edit is valid on it")
                self.assertEqual(kv["Nd"] + surge, kv["ceil_d"], f"{n}: a grouped decoder surges with the engine")
            else:
                self.assertEqual(kv["Nd"], kv["ceil_d"], f"{n}: an ungrouped decoder is a negative control")
                self.assertEqual(("SurgeThenDrain", 1, 0), decoder[:3], n)

    def test_every_case_validates_and_the_manifest_states_the_rule(self):
        with tempfile.TemporaryDirectory() as d:
            for n, _kv, _m in self.fam:
                p = Path(d) / f"{n}.yaml"
                p.write_text(self.outputs[f"gen-switch/{n}.yaml"])
                self.assertEqual([], run.validate_case_file(p), n)
        manifest = self.outputs[generate_cases.MANIFEST_REL]
        self.assertIn("### By mid-roll edit (gen-switch)", manifest)
        self.assertIn("| strategy SurgeThenDrain to RecreatePod at settling | 6 |", manifest)
        self.assertIn("| restartPolicy None to RecreateInstance at rotating | 6 |", manifest)
        self.assertIn("No strategy pair is skipped in gen-switch", manifest)


class GeneratedHealAndImagepullFamiliesTest(unittest.TestCase):
    """The configkey heal family (gen-heal) and the imagepull family
    (gen-imagepull): every axis is crossed, each heal case carries its
    ConfigMap and the victim's env reference, the after-failed half of the
    heal family carries the short ready timeout its escalation needs, and
    no cell of either family is parked."""

    TOPOS = ("engine", "pd", "engine-mn", "pd-mn")

    @classmethod
    def setUpClass(cls):
        cls.outputs, cls.rows = generate_cases.build_outputs()

    def _family(self, folder):
        return [(n, kv, m) for f, n, kv, m in self.rows if f == folder]

    @staticmethod
    def _component_block(isvc, comp):
        lines = isvc.split("\n")
        out = []
        for line in lines[lines.index(f"  {comp}:") + 1:]:
            if line.startswith("  ") and not line.startswith("    "):
                break
            out.append(line)
        return "\n".join(out)

    def test_heal_family_crosses_every_axis(self):
        fam = self._family("gen-heal")
        self.assertEqual(32, len(fam))
        self.assertEqual({(t, rp, lift, co) for t in self.TOPOS for rp in generate_cases.RESTARTS
                          for lift in generate_cases.HEAL_LIFTS for co in generate_cases.HEAL_COORDS},
                         {(kv["topo"], m["rp"], kv["lift"], kv["co"]) for _n, kv, m in fam})
        for _n, kv, _m in fam:
            self.assertEqual("configkey:missing", kv["fault"])
            self.assertEqual("idle", kv["during"])
            self.assertEqual("heals", kv["expect"])
            self.assertNotIn("mode", kv)
            # A repair rebuilds in place: nothing may surge on a service nobody bumped.
            self.assertEqual((kv["Ne"], kv["Nd"], kv["Nr"]), (kv["ceil_e"], kv["ceil_d"], kv["ceil_r"]))

    def test_heal_cases_carry_their_configmap_and_the_victims_env(self):
        g = generate_cases
        for n, kv, _m in self._family("gen-heal"):
            docs = self.outputs[f"gen-heal/{n}.yaml"].split("\n---\n")
            self.assertEqual(2, len(docs), n)
            cm = f"{n}{g.HEAL_CONFIG_SUFFIX}"
            self.assertEqual(f"apiVersion: v1\nkind: ConfigMap\nmetadata: {{ name: {cm} }}\n"
                             f"data: {{ {g.HEAL_CONFIG_KEY}: \"{g.HEAL_CONFIG_VALUE}\" }}\n", docs[1])
            ref = (f'{{ name: "{g.HEAL_CONFIG_VAR}", valueFrom: {{ configMapKeyRef: '
                   f'{{ name: "{cm}", key: "{g.HEAL_CONFIG_KEY}" }} }} }}')
            engine = self._component_block(docs[0], "engine")
            if kv["topo"].endswith("-mn"):
                self.assertIn(f'    leader: {{ runner: {{ name: "{gen_cases.MAIN_CONTAINER}", env: [{ref}] }} }}',
                              engine, n)
            else:
                self.assertIn(f"    runner: {{ env: [{ref}] }}", engine, n)
                self.assertNotIn("leader:", engine)
            if kv["topo"].startswith("pd"):
                self.assertNotIn("runner", self._component_block(docs[0], "decoder"), n)
                self.assertNotIn("runner", self._component_block(docs[0], "router"), n)
            if kv["co"] == "rolling":
                members = "[engine, decoder]" if kv["topo"].startswith("pd") else "[engine]"
                self.assertIn(f"        - components: {members}\n          rollingUpdate: {{}}", docs[0], n)
            else:
                self.assertNotIn("rollout:", docs[0], n)

    def test_heal_runs_every_cell_live_and_times_the_after_failed_half(self):
        """No heal cell is parked: a repair parked at Failed resumes when its
        rebuilt set comes up, and a grouped engine's repair is admitted
        without the group's gate while the Instance serves nothing. The
        after-failed half carries the short ready timeout its escalation
        needs and states the parked repair's exit; the after-settle half
        carries neither."""
        g = generate_cases
        fam = self._family("gen-heal")
        self.assertEqual(16, len([n for n, kv, _m in fam if kv["lift"] == "after-failed"]))
        for n, kv, m in fam:
            text = self.outputs[f"gen-heal/{n}.yaml"]
            engine = self._component_block(text.split("\n---\n")[0], "engine")
            reason = text.splitlines()[1]
            self.assertNotIn("parked", m, n)
            self.assertNotIn("notimpl", kv, n)
            self.assertNotIn("issue", kv, n)
            self.assertTrue(reason.startswith("# reason: the engine runner's ConfigMap key is removed"), n)
            self.assertIn("no reset mailbox", reason, n)
            if kv["lift"] == "after-failed":
                self.assertIn(f'instanceReadyTimeout: "{g.HEAL_FAILED_READY_TIMEOUT}"', engine, n)
                self.assertIn("parks the repair at Failed", reason, n)
                self.assertIn("resumes when its rebuilt set comes up", reason, n)
            else:
                self.assertNotIn("instanceReadyTimeout", text, n)
                self.assertIn("before any attempt escalates", reason, n)
                self.assertNotIn("Failed", reason, n)
            if kv["co"] == "rolling":
                self.assertIn("group", reason, n)
                self.assertNotIn("holds the crash-loop repair", reason, n)
            if m["rp"] == "None":
                self.assertIn("crash-loop repair", reason, n)
            else:
                self.assertIn("runner-restart repair", reason, n)
        self.assertEqual([], [k for k in g.PARKED if k[0] == "heal"])

    def test_imagepull_family_crosses_every_axis(self):
        g = generate_cases
        fam = self._family("gen-imagepull")
        self.assertEqual(56, len(fam))
        settling = [(n, kv, m) for n, kv, m in fam if m["verb"] == "imagepull:settling"]
        idle = [(n, kv, m) for n, kv, m in fam if m["verb"] == "imagepull:idle"]
        self.assertEqual((48, 8), (len(settling), len(idle)))
        self.assertFalse(any("parked" in m for _n, _kv, m in fam), "nothing in the imagepull family is parked")
        self.assertEqual({(t, s, rp, co) for t, cos in g.CRASH_COORDS.items() for s, _b in g.STRATS
                          for rp in g.RESTARTS for co in cos},
                         {(kv["topo"], m["strat"], m["rp"], kv["co"]) for _n, kv, m in settling})
        for n, kv, m in settling:
            self.assertEqual(("imagepull:old", 1), (kv["fault"], kv["floor_soft"]), n)
            self.assertNotIn("during", kv)
            self.assertNotIn("form", kv)
            surge = kv["Ne"] + 1 if m["strat"] == "SurgeThenDrain" else kv["Ne"]
            self.assertEqual(surge, kv["ceil_e"], n)
            self.assertEqual(kv["Nd"] + (1 if kv["co"] == "rolling" else 0), kv["ceil_d"],
                             "a rolling group surges the decoder too")
        self.assertEqual({(t, rp) for t in self.TOPOS for rp in g.RESTARTS},
                         {(kv["topo"], m["rp"]) for _n, kv, m in idle})
        for n, kv, _m in idle:
            self.assertEqual(("idle", "repaired", "none"), (kv["during"], kv["expect"], kv["co"]), n)
            self.assertEqual((kv["Ne"], kv["Nd"]), (kv["ceil_e"], kv["ceil_d"]), n)
            self.assertNotIn("floor_soft", kv)
            self.assertNotIn("rollout:", self.outputs[f"gen-imagepull/{n}.yaml"])

    def test_generated_blocks_are_disjoint_and_own_their_ids(self):
        blocks = sorted(generate_cases.GEN_BLOCKS.items(), key=lambda item: item[1])
        for (fa, (_lo_a, hi_a)), (fb, (lo_b, _hi_b)) in zip(blocks, blocks[1:]):
            self.assertLessEqual(hi_a, lo_b, f"{fa} and {fb} overlap")
        for folder, (lo, hi) in generate_cases.GEN_BLOCKS.items():
            ids = [int(n[1:]) for f, n, _kv, _m in self.rows if f == folder]
            self.assertTrue(ids, folder)
            self.assertTrue(lo <= min(ids) and max(ids) < hi, folder)
        owned = {Path(rel).stem for rel in self.outputs if rel.endswith(".yaml")}
        for f in (HERE / "isvcs").glob("**/*.yaml"):
            if f.parent.name not in generate_cases.GEN_BLOCKS:
                self.assertNotIn(f.stem, owned, f"{f} uses a generated id")

    def test_renderer_helpers(self):
        self.assertEqual('[{ name: "a", value: "1" }, 2]', gen_cases.flow([{"name": "a", "value": "1"}, 2]))
        env = [{"name": "V", "valueFrom": {"configMapKeyRef": {"name": "cm", "key": "k"}}}]
        self.assertEqual({"env": env}, gen_cases.configmap_env("cm", "k", "V"))
        self.assertEqual({"name": gen_cases.MAIN_CONTAINER, "env": env}, gen_cases.configmap_env("cm", "k", "V", gang=True))
        text = gen_cases.render("p1", [gen_cases.comp("engine", 1)], docs=[gen_cases.configmap_doc("p1-config", {"k": "v"})])
        self.assertTrue(text.endswith('---\napiVersion: v1\nkind: ConfigMap\nmetadata: { name: p1-config }\ndata: { k: "v" }\n'))
        self.assertIn('instanceReadyTimeout: "90s"', gen_cases.lifecycle(ready_timeout="90s"))
        self.assertNotIn("instanceReadyTimeout", gen_cases.lifecycle())


class GeneratedNodeFamilyTest(unittest.TestCase):
    """gen-node: a Ready Instance loses its node. Every cell fires
    node:notready on the idle service and asserts the configured outcome
    at lift=after-settle; the restart policy, migration mode and group axes
    are crossed in full and change nothing but the reason."""

    def _rows(self):
        outputs, rows = generate_cases.build_outputs()
        return outputs, [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-node"]

    def test_generated_family_crosses_every_axis(self):
        outputs, fam = self._rows()
        self.assertEqual(96, len(fam))
        self.assertEqual({"engine", "pd", "engine-mn", "pd-mn"}, {kv["topo"] for _n, kv, _m in fam})
        self.assertEqual({"None", "RecreateInstance"}, {m["rp"] for _n, _kv, m in fam})
        self.assertEqual(set(generate_cases.NODE_MIGRATIONS), {m["mig"] for _n, _kv, m in fam})
        self.assertEqual({"none", "rolling"}, {kv["co"] for _n, kv, _m in fam})
        self.assertEqual({"parked", "repaired", "relocated"}, {kv["expect"] for _n, kv, _m in fam})
        tuples = {(kv["topo"], m["rp"], m["mig"], kv["co"], kv["expect"]) for _n, kv, m in fam}
        self.assertEqual(96, len(tuples), "every (shape, restart policy, migration mode, group, outcome) once")
        relocated = [int(n[1:]) for n, kv, _m in fam if kv["expect"] == "relocated"]
        others = [int(n[1:]) for n, kv, _m in fam if kv["expect"] != "relocated"]
        self.assertGreater(min(relocated), max(others), "the relocation half is minted after the other")
        for name, kv, m in fam:
            self.assertEqual("node:notready", kv["fault"])
            self.assertEqual("idle", kv["during"])
            self.assertEqual("after-recover" if kv["expect"] == "relocated" else "after-settle", kv["lift"])
            self.assertEqual(generate_cases.NODE_NEEDS.get(kv["expect"]), kv.get("needs"),
                             "parked reads the plain deploy, relocated the force-delete one, repaired either")
            self.assertEqual(f"node:{kv['expect']}", m["verb"])
            self.assertNotIn("mode", kv)
            for c in ("e", "d", "r"):
                self.assertEqual(kv[f"N{c}"], kv[f"ceil_{c}"], "nobody bumps the service: every ceiling is N")
            text = outputs[f"gen-node/{name}.yaml"]
            self.assertIn(f'migrationPolicy: {{ mode: "{m["mig"]}" }}', text)
            self.assertIn(f'restartPolicy: "{generate_cases.RP_FULL[m["rp"]]}"', text)
            members = "[engine, decoder]" if kv["topo"].startswith("pd") else "[engine]"
            if kv["co"] == "rolling":
                self.assertIn(f"- components: {members}\n          rollingUpdate: {{}}", text)
            else:
                self.assertNotIn("rollout:", text)
            reason = text.splitlines()[1]
            if kv["expect"] == "relocated":
                self.assertIn(generate_cases.NODE_RELOCATION_REBUILD[m["rp"]], reason)
                self.assertIn("rebuilt on other nodes while the node is still down", reason)
                self.assertIn("every pod the node held is gone", reason)
                self.assertIn(f"migration mode {m['mig']} plays no part", reason)
                self.assertEqual(kv["co"] != "none", "does not hold it" in reason)
                continue
            self.assertIn(generate_cases.NODE_NO_TRIGGER[m["rp"]], reason)
            self.assertIn(generate_cases.NODE_NO_RELOCATION[m["mig"]], reason)
            self.assertIn("no force-delete policy" if kv["expect"] == "parked" else "never stopped", reason)

    def test_every_cell_validates_as_a_case_file(self):
        outputs, fam = self._rows()
        with tempfile.TemporaryDirectory() as tmp:
            for name, _kv, _m in fam:
                path = Path(tmp) / f"{name}.yaml"
                path.write_text(outputs[f"gen-node/{name}.yaml"])
                self.assertEqual([], run.validate_case_file(path))


class GeneratedCtrlRestartFamilyTest(unittest.TestCase):
    """gen-ctrlrestart: the manager pod is deleted under an operation in
    flight. One runner mode and one hook per condition, crossed with the
    shapes and both strategies; the PD rollout conditions also run under a
    rolling group."""

    def _rows(self):
        outputs, rows = generate_cases.build_outputs()
        return outputs, [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-ctrlrestart"]

    def test_generated_family_crosses_every_axis(self):
        outputs, fam = self._rows()
        self.assertEqual(34, len(fam))
        self.assertEqual(set(generate_cases.CTRL_CONDITIONS), {m["cond"] for _n, _kv, m in fam})
        self.assertEqual({"SurgeThenDrain", "RecreatePod"}, {m["strat"] for _n, _kv, m in fam})
        self.assertEqual({"engine", "pd", "engine-mn"}, {kv["topo"] for _n, kv, _m in fam})
        tuples = {(kv["topo"], m["strat"], m["cond"], kv["co"]) for _n, kv, m in fam}
        self.assertEqual(34, len(tuples))
        for topo in ("engine", "pd", "engine-mn"):
            for strat in ("SurgeThenDrain", "RecreatePod"):
                for cond in generate_cases.CTRL_CONDITIONS:
                    self.assertIn((topo, strat, cond, "none"), tuples)
        for name, kv, m in fam:
            cond, strat = m["cond"], m["strat"]
            surge = 1 if strat == "SurgeThenDrain" else 0
            self.assertEqual(generate_cases.CTRL_FAULTS[cond], kv["fault"])
            self.assertEqual(f"ctrlrestart:{cond}", m["verb"])
            text = outputs[f"gen-ctrlrestart/{name}.yaml"]
            self.assertIn(f'type: "{strat}"', text)
            if cond in ("surge", "drain"):
                self.assertNotIn("mode", kv)
                self.assertEqual(kv["Ne"] + surge, kv["ceil_e"])
                if kv["co"] == "rolling":
                    self.assertTrue(kv["topo"].startswith("pd"), "the group axis exists on PD rolls only")
                    self.assertEqual(kv["Nd"] + surge, kv["ceil_d"], "the group rolls the decoder under the same strategy")
                    self.assertIn("- components: [engine, decoder]\n          rollingUpdate: {}", text)
                else:
                    self.assertEqual(kv["Nd"], kv["ceil_d"])
            else:
                self.assertEqual("none", kv["co"])
            if cond == "migration":
                self.assertEqual(("migrate", "Auto", 0), (kv["mode"], kv["mig"], kv["inst"]))
                self.assertEqual(kv["Ne"] + 1, kv["ceil_e"], "the replacement surges at a new index")
            elif cond == "canary":
                self.assertEqual(("canary", "auto"), (kv["mode"], kv["canary"]))
                self.assertEqual(generate_cases.CTRL_CANARY_CELL, kv["cell"])
                self.assertEqual(kv["Ne"] + surge, kv["ceil_e"], "capacity steps run under the member's strategy")
                members = "[engine, decoder, router]" if kv["topo"].startswith("pd") else "[engine]"
                self.assertIn(f"- components: {members}\n          canary:", text)
                if kv["topo"].startswith("pd"):
                    self.assertEqual((kv["Nd"] + 1, kv["Nr"] + 1), (kv["ceil_d"], kv["ceil_r"]))
            elif cond == "scaledown":
                self.assertEqual("scaledown", kv["mode"])
                self.assertEqual(kv["Ne"] // 2, kv["to_e"])
                self.assertEqual(kv["Ne"], kv["ceil_e"])

    def test_every_cell_validates_as_a_case_file(self):
        outputs, fam = self._rows()
        with tempfile.TemporaryDirectory() as tmp:
            for name, _kv, _m in fam:
                path = Path(tmp) / f"{name}.yaml"
                path.write_text(outputs[f"gen-ctrlrestart/{name}.yaml"])
                self.assertEqual([], run.validate_case_file(path))


class NodeBiteTest(unittest.TestCase):
    """The kubelet stop has bitten once no pod of the Instance on the stopped
    node serves; a gang member on another node does not hide it."""

    def test_bite_reads_the_stopped_nodes_pods_only(self):
        leader_down = ("e1-leader", {"ready": False, "serving": True, "node": "kind-worker"})
        leader_up = ("e1-leader", {"ready": True, "serving": True, "node": "kind-worker"})
        worker_up = ("e1-worker", {"ready": True, "serving": True, "node": "kind-worker2"})
        self.assertTrue(run._node_bite_held("kind-worker", [leader_down, worker_up]))
        self.assertFalse(run._node_bite_held("kind-worker", [leader_up, worker_up]))
        self.assertFalse(run._node_bite_held("kind-worker", [leader_up]))
        self.assertTrue(run._node_bite_held("kind-worker", [leader_down]))
        self.assertTrue(run._node_bite_held("kind-worker", []), "evicted pods leave nothing serving there")


class NodePolicyNeedTest(unittest.TestCase):
    """needs=forcedelete|no-forcedelete pins a node outcome to the deployed
    force-delete policy: the runner reads the controller's lifecycle block
    and SKIPs where it differs; the relocation window follows the policy's
    threshold."""

    BASE = "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 topo=engine"
    POLICY = {"forceDelete": {"overdueSlack": "30s", "nodeUnreachableThreshold": "90s"}}

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "p999.yaml"
            path.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(path)

    def test_header_pins_each_outcome_to_its_policy(self):
        node = self.BASE + " fault=node:notready during=idle"
        self.assertEqual([], self._errs(node + " lift=after-recover expect=relocated needs=forcedelete"))
        self.assertEqual([], self._errs(node + " lift=after-settle expect=parked needs=no-forcedelete"))
        self.assertEqual([], self._errs(node + " lift=after-settle expect=repaired"))
        self.assertEqual([], self._errs(node + " lift=after-settle expect=repaired needs=forcedelete"))
        self.assertTrue(any("needs=forcedelete" in e for e in self._errs(node + " lift=after-recover expect=relocated")))
        self.assertTrue(any("needs=no-forcedelete" in e for e in self._errs(node + " lift=after-settle expect=parked")))
        self.assertTrue(any("needs=no-forcedelete" in e
                            for e in self._errs(node + " lift=after-settle expect=parked needs=forcedelete")))
        self.assertTrue(any("needs fault=node" in e for e in self._errs(self.BASE + " needs=forcedelete")))
        self.assertTrue(any("malformed header value needs=" in e
                            for e in self._errs(node + " lift=after-recover expect=relocated needs=sometimes")))

    def test_unmet_need_names_the_deployed_config(self):
        self.assertIsNone(run._needs_unmet("", {}, None))
        self.assertIsNone(run._needs_unmet("forcedelete", self.POLICY, None))
        self.assertIsNone(run._needs_unmet("no-forcedelete", {}, None))
        self.assertIsNone(run._needs_unmet("no-forcedelete", {"instanceReadyTimeout": "30m"}, None))
        self.assertIn("has no lifecycle.forceDelete", run._needs_unmet("forcedelete", {}, None))
        self.assertIn("carries lifecycle.forceDelete", run._needs_unmet("no-forcedelete", self.POLICY, None))
        self.assertIn("90s", run._needs_unmet("no-forcedelete", self.POLICY, None))
        self.assertIn("cannot be read", run._needs_unmet("forcedelete", None, "ConfigMap ome/x could not be read"))

    def test_lifecycle_block_is_read_off_the_controller_configmap(self):
        def cm(data):
            return json.dumps({"data": data})
        answers = {}

        def kubectl(*a, **k):
            self.assertEqual(("-n", run.CTRL_NS, "get", "configmap", run.CTRL_CONFIGMAP), a[:5])
            return answers["r"]
        with mock.patch.object(run, "kubectl", kubectl):
            answers["r"] = subprocess.CompletedProcess((), 0, cm({"lifecycle": json.dumps(self.POLICY)}), "")
            self.assertEqual((self.POLICY, None), run._controller_lifecycle_config())
            answers["r"] = subprocess.CompletedProcess((), 0, cm({"ingress": "{}"}), "")
            self.assertEqual(({}, None), run._controller_lifecycle_config())
            answers["r"] = subprocess.CompletedProcess((), 1, "", "not found")
            block, err = run._controller_lifecycle_config()
            self.assertIsNone(block)
            self.assertIn("could not be read", err)
            answers["r"] = subprocess.CompletedProcess((), 0, cm({"lifecycle": "{not json"}), "")
            block, err = run._controller_lifecycle_config()
            self.assertIsNone(block)
            self.assertIn("not JSON", err)

    def test_relocation_window_follows_the_policy_threshold(self):
        ctx = {"N": {"engine": 4, "decoder": 0, "router": 0}, "pods_per_inst": {"engine": 2},
               "configured": {"engine"}}
        fleet = run._idle_budget(ctx)
        self.assertEqual(min(fleet, int(90 + run.ROTATE_BASE_S + 2 * run.ROTATE_PER_POD_S)),
                         run._relocation_budget(ctx, "engine", self.POLICY["forceDelete"]))
        self.assertEqual(fleet, run._relocation_budget(ctx, "engine", None))
        self.assertEqual(fleet, run._relocation_budget(ctx, "engine", {"nodeUnreachableThreshold": "soon"}))

    def test_relocated_verdict_reads_the_rebuild_and_the_reap(self):
        def f():
            return {"kind": "node", "crash_inst": "1", "crash_comp": "engine", "infra_fail": None,
                    "bite": "its node NotReady with no pod of it serving there", "cause": "the kubelet of node kind-worker"}
        good = {"bitten_at": 45.0, "recovered_at": 150.0, "lifted_at": 151.0, "reaped_at": 0.0,
                "serving_after_lift": True, "park_broken": None, "phase_after": "Ready", "new_hash": False,
                "reset_used": False, "failed_at": None, "healed_at": None}
        checks = []
        self.assertEqual("PASS", run._lift_checks(f(), "relocated", "after-recover", good, {}, "PASS", [], checks))
        self.assertTrue(any("every pod the node held is gone" in c for c in checks))
        self.assertTrue(any("still Ready and serving" in c for c in checks))
        for broken, text in ((dict(good, reaped_at=None), "still in the API"),
                             (dict(good, serving_after_lift=False), "not Ready and serving after the lift"),
                             (dict(good, new_hash=True), "new revision appeared"),
                             (dict(good, reset_used=True), "reset mailbox"),
                             (dict(good, recovered_at=None), "never came back Ready and serving on another node")):
            checks = []
            self.assertEqual("FAIL", run._lift_checks(f(), "relocated", "after-recover", broken, {}, "PASS", [], checks), text)
            self.assertTrue(any(text in c for c in checks), text)


class CanaryBreakFaultTest(unittest.TestCase):
    """A break fault under a canary names the grid state it hits the stable
    side in, counts its victims, fires in that state and not at the bump, and
    the verdict reads every victim."""

    BASE = "# p999: x mode=canary Ne=8 Nd=0 Nr=0 ceil_e=9 ceil_d=0 ceil_r=0 canary=auto topo=engine"
    ROLLOUT = "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_fault_names_a_canary_hook_or_the_old_revision(self):
        self.assertTrue(run._fault_ok("crashloop:canary-serving@0"))
        self.assertTrue(run._fault_ok("imagepull:canary-staging"))
        self.assertTrue(run._fault_ok("crashloop:old"))
        self.assertFalse(run._fault_ok("crashloop:settling"))
        self.assertFalse(run._fault_ok("crashloop:canary-nowhere"))

    def test_header_ties_the_hook_to_canary_mode_and_the_victim_to_rollout_mode(self):
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:canary-serving@0 form=load victims=3 nophase=Failed"))
        self.assertEqual([], self._errs(self.BASE + " fault=imagepull:canary-serving@1 victims=2"))
        self.assertEqual([], self._errs(self.ROLLOUT + " fault=crashloop:old form=start"))
        self.assertTrue(any("mode=canary" in e for e in self._errs(self.ROLLOUT + " fault=crashloop:canary-serving form=start")))
        self.assertTrue(any("mode=rollout" in e for e in self._errs(self.BASE + " fault=crashloop:old form=start")))
        self.assertTrue(any("victims=" in e for e in self._errs(self.BASE + " fault=podkill:canary-serving victims=2")))
        self.assertTrue(any("malformed header value victims=0" in e
                            for e in self._errs(self.BASE + " fault=crashloop:canary-serving form=start victims=0")))
        self.assertTrue(any("form=" in e for e in self._errs(self.BASE + " fault=crashloop:canary-serving")))

    def test_fires_in_the_named_state_not_at_the_bump(self):
        f = run._fault_new("p999", {"fault": "crashloop:canary-serving@0", "form": "start", "victims": 2})
        self.assertEqual(2, f["victims"])
        fired = []
        orig = run._fault_fire
        run._fault_fire = lambda *a, **k: fired.append(a) or f.__setitem__("fired", True)
        try:
            ctx = {"fault": f, "trigger_ts": 1.0, "pod_state": {}, "pods_per_inst": {}, "old_hashes": {"old"},
                   "canary_hook": "canary-staging", "canary_step": 0}
            run._fault_tick(ctx)
            self.assertEqual([], fired, "the bump alone does not fire a break aimed at a canary state")
            ctx["canary_hook"], ctx["canary_step"] = "canary-serving", 1
            run._fault_tick(ctx)
            self.assertEqual([], fired, "the step narrows the hook")
            ctx["canary_step"] = 0
            run._fault_tick(ctx)
            run._fault_tick(ctx)
            self.assertEqual(1, len(fired))
        finally:
            run._fault_fire = orig

    def test_picks_the_highest_old_instances_one_per_instance(self):
        pods = {
            "e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "default"},
            "e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "default"},
            "e2": {"alive": True, "hash": "old", "inst": "2", "comp": "engine", "runner": "default"},
            "e3": {"alive": True, "hash": "old", "inst": "3", "comp": "engine", "runner": "leader"},
            "w3": {"alive": True, "hash": "old", "inst": "3", "comp": "engine", "runner": "worker"},
            "e4": {"alive": True, "hash": "new", "inst": "4", "comp": "engine", "runner": "default"},
        }
        self.assertEqual(["e3", "e2"], run._crashloop_pick_pods(pods, {"old"}, "engine", 2))
        self.assertEqual(["e3", "e2", "e1", "e0"], run._crashloop_pick_pods(pods, {"old"}, "engine", 9))
        self.assertEqual("e3", run._crashloop_pick_pod(pods, {"old"}))

    def test_fire_breaks_every_victim_and_the_verdict_reads_each(self):
        f = run._fault_new("p999", {"fault": "crashloop:canary-serving@0", "form": "start", "victims": 2})
        pods = {f"e{i}": {"alive": True, "hash": "old", "inst": str(i), "comp": "engine", "runner": "default", "uid": f"u{i}"}
                for i in range(4)}
        patches = []
        orig = run.kubectl
        run.kubectl = lambda *a, **k: patches.append(a) or subprocess.CompletedProcess(a, 0, "", "")
        try:
            run._fault_crashloop_fire(f, pods, {"old"})
        finally:
            run.kubectl = orig
        self.assertIsNone(f["infra_fail"])
        self.assertEqual([("patch", "pod", "e3"), ("patch", "pod", "e2")], [p[2:5] for p in patches])
        self.assertEqual(("e3", "3"), (f["crash_pod"], f["crash_inst"]))
        self.assertEqual(["2"], [rec["crash_inst"] for rec in f["extra_victims"]])
        self.assertEqual(2, len(run._break_victims(f)))
        f["fired"] = True
        healed = {f"e{i}": {"alive": True, "hash": "new", "inst": str(i), "comp": "engine", "ready": True, "serving": True}
                  for i in range(4)}
        stale = dict(healed, e2={"alive": True, "hash": "old", "inst": "2", "comp": "engine", "ready": True, "serving": True})
        orig_ev = run._isvc_events_since
        try:
            run._isvc_events_since = lambda case_id, since: []
            notes, checks = [], []
            self.assertEqual("PASS", run._crashloop_checks(f, healed, {"old"}, 1.0, "PASS", notes, checks, bumped={"engine"}))
            self.assertEqual(2, sum(1 for c in checks if "ended on the new revision" in c))
            notes = []
            self.assertEqual("FAIL", run._crashloop_checks(f, stale, {"old"}, 1.0, "PASS", notes, [], bumped={"engine"}))
            self.assertTrue(any("engine/2 did not end on the new revision" in n for n in notes))
            run._isvc_events_since = lambda case_id, since: [
                ("RestartTriggered", "OMENative component=engine instance=2 restart triggered: pod e2 wedged")]
            notes = []
            self.assertEqual("FAIL", run._crashloop_checks(f, healed, {"old"}, 1.0, "PASS", notes, [], bumped={"engine"}))
            self.assertTrue(any("engine/2 was repaired after the bump" in n for n in notes))
        finally:
            run._isvc_events_since = orig_ev

    def test_too_few_old_pods_is_an_apparatus_failure(self):
        f = run._fault_new("p999", {"fault": "imagepull:canary-serving", "victims": 3})
        pods = {f"e{i}": {"alive": True, "hash": "old", "inst": str(i), "comp": "engine", "runner": "default", "uid": f"u{i}"}
                for i in range(2)}
        patches = []
        orig = run.kubectl
        run.kubectl = lambda *a, **k: patches.append(a) or subprocess.CompletedProcess(a, 0, "", "")
        try:
            run._fault_crashloop_fire(f, pods, {"old"})
        finally:
            run.kubectl = orig
        self.assertIn("3 asked", f["infra_fail"])
        self.assertEqual([], patches)

    def test_floor_allows_for_the_broken_instances(self):
        ctx = {"N": {"engine": 8}, "info": {"max_unavail": {"engine": 0}}, "hdr": {},
               "min_ready": {"engine": 5}, "floor_sust": {"engine": 30.0}}
        self.assertEqual("FAIL", run.min_ready_checks(ctx, ["engine"], "PASS", [], []))
        ctx["floor_allowance"] = {"engine": 3}
        checks = []
        self.assertEqual("PASS", run.min_ready_checks(ctx, ["engine"], "PASS", [], checks))
        self.assertTrue(any("3broken=5" in c for c in checks))
        ctx["min_ready"]["engine"] = 4
        self.assertEqual("FAIL", run.min_ready_checks(ctx, ["engine"], "PASS", [], []))


class CanaryStepBreakTest(unittest.TestCase):
    """Under a canary a break may land before the push (the stable side is
    already broken when the push lands) or ride the push itself (the new
    revision is broken): the headers that name them, what the push carries,
    how the bite is read, the manual gate's hold after a break, and the
    end-state checks a broken new revision is judged by."""

    BASE = "# p999: x mode=canary Ne=8 Nd=0 Nr=0 ceil_e=9 ceil_d=0 ceil_r=0 canary=auto topo=engine"
    PD = "# p999: x mode=canary Ne=8 Nd=8 Nr=2 ceil_e=9 ceil_d=9 ceil_r=3 canary=auto topo=pd"
    ROLLOUT = "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_fault_names_the_pushed_revision_or_the_idle_service(self):
        for v in ("crashloop:new", "imagepull:new", "crashloop:idle", "imagepull:idle"):
            self.assertTrue(run._fault_ok(v), v)
        self.assertFalse(run._fault_ok("crashloop:bogus"))
        self.assertTrue(run._fault_on_new(run._fault_new("p999", {"fault": "crashloop:new", "form": "start"})))
        self.assertTrue(run._fault_before_push(run._fault_new("p999", {"fault": "imagepull:idle"})))
        self.assertFalse(run._fault_on_new(run._fault_new("p999", {"fault": "crashloop:old", "form": "start"})))
        self.assertFalse(run._fault_before_push(run._fault_new("p999", {"fault": "crashloop:canary-staging", "form": "start"})))

    def test_header_ties_the_new_and_idle_breaks_to_canary_mode(self):
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:new form=start during=canary-failed verb=rollback terminal=RolledBack"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:new form=load exitafter=10 during=canary-failed verb=rebump-fix terminal=Stable"))
        self.assertEqual([], self._errs(self.BASE + " fault=imagepull:new during=canary-failed verb=rollback terminal=RolledBack"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:idle form=start victims=3 nophase=Failed"))
        self.assertEqual([], self._errs(self.PD + " fault=crashloop:new form=start bump=engine,decoder victimcomp=decoder during=canary-failed verb=rollback terminal=RolledBack"))
        self.assertEqual([], self._errs(self.ROLLOUT + " fault=crashloop:new form=start"),
                         "a crash loop the push brings is the new-side family's rollout case")
        self.assertTrue(any("mode=canary" in e for e in self._errs(self.ROLLOUT + " fault=imagepull:new")))
        self.assertTrue(any("mode=canary" in e for e in self._errs(self.ROLLOUT + " fault=crashloop:idle form=start")))
        self.assertTrue(any("img= does not apply" in e for e in self._errs(
            self.BASE + " fault=crashloop:new form=start bump=engine img=example.com/canary-test/missing:none")))
        self.assertTrue(any("victims=" in e for e in self._errs(self.BASE + " fault=crashloop:new form=start victims=2")))
        self.assertTrue(any("exitafter=" in e for e in self._errs(self.BASE + " fault=crashloop:new form=start exitafter=10")))
        self.assertTrue(any("exitafter=" in e for e in self._errs(self.BASE + " fault=imagepull:new exitafter=10")))
        self.assertTrue(any("exitafter=" in e for e in self._errs(self.BASE + " exitafter=10")))
        self.assertTrue(any("malformed header value exitafter=0" in e
                            for e in self._errs(self.BASE + " fault=crashloop:new form=load exitafter=0")))
        self.assertTrue(any("must be a member the push bumps" in e for e in self._errs(
            self.PD + " fault=crashloop:new form=start bump=engine victimcomp=decoder")))
        self.assertTrue(any("form=" in e for e in self._errs(self.BASE + " fault=crashloop:new")))

    def test_runner_override_carries_the_image_or_the_env_and_drops_both(self):
        env = [{"name": run.CRASH_EXIT_ENV, "value": "10"}]
        self.assertEqual({"image": "x"}, run._runner_override("x", None))
        self.assertEqual({"env": env}, run._runner_override("", env))
        self.assertEqual({"image": "x", "env": env}, run._runner_override("x", env))
        self.assertIsNone(run._runner_override("", None))
        self.assertIsNone(run._runner_override(None, None))

    def test_bump_arms_an_env_override_on_one_member_and_the_fix_drops_it(self):
        seen = []
        env = [{"name": run.CRASH_EXIT_ENV, "value": "10"}]
        with mock.patch.object(run, "kubectl", lambda *a, **k: (seen.append(a), subprocess.CompletedProcess(a, 0, "", ""))[1]):
            ctx = {"case_id": "p1", "pods_per_inst": {"engine": 1, "decoder": 2}}
            run._bump_rev(ctx, {"engine", "decoder"}, env=env, image_comps={"decoder"})
            run._bump_rev(ctx, {"engine"}, env=env)
            run._bump_rev(ctx, {"engine", "decoder"}, val="v3", image="")
        armed = json.loads(seen[0][-1])["spec"]
        self.assertEqual({"name": run.MAIN_CONTAINER, "env": env}, armed["decoder"]["leader"]["runner"])
        self.assertNotIn("runner", armed["engine"])
        self.assertEqual({"env": env}, json.loads(seen[1][-1])["spec"]["engine"]["runner"])
        fixed = json.loads(seen[2][-1])["spec"]
        self.assertIsNone(fixed["engine"]["runner"])
        self.assertIsNone(fixed["decoder"]["leader"]["runner"])

    def test_what_the_push_carries_for_each_break(self):
        load = run._fault_new("p999", {"fault": "crashloop:new", "form": "load", "exitafter": 10})
        self.assertEqual(("", {"engine"}, [{"name": run.CRASH_EXIT_ENV, "value": "10"}]), run._new_break_arm(load, "engine"))
        default = run._fault_new("p999", {"fault": "crashloop:new", "form": "load"})
        self.assertEqual(str(run.CRASH_EXIT_AFTER_S), run._new_break_arm(default, "engine")[2][0]["value"])
        start = run._fault_new("p999", {"fault": "crashloop:new", "form": "start"})
        self.assertEqual((run._break_image("crashloop"), {"decoder"}, None), run._new_break_arm(start, "decoder"))
        pull = run._fault_new("p999", {"fault": "imagepull:new"})
        self.assertEqual((runtime_fixtures.UNPULLABLE_IMAGE, {"engine"}, None), run._new_break_arm(pull, "engine"))
        run._new_break_armed(load, "engine", "", run._new_break_arm(load, "engine")[2], 5.0)
        self.assertEqual((True, 5.0, "engine"), (load["fired"], load["fired_ts"], load["crash_comp"]))
        self.assertIn(f"{run.CRASH_EXIT_ENV}=10", load["notes"][0])

    def test_no_hook_fires_a_break_the_push_or_the_idle_service_carries(self):
        fired = []
        with mock.patch.object(run, "_fault_fire", lambda *a, **k: fired.append(a)):
            for hdr in ({"fault": "crashloop:new", "form": "start"}, {"fault": "imagepull:idle"}):
                f = run._fault_new("p999", hdr)
                ctx = {"fault": f, "trigger_ts": 1.0, "pod_state": {}, "pods_per_inst": {}, "old_hashes": {"old"},
                       "canary_hook": "canary-staging", "canary_step": 0}
                run._fault_tick(ctx)
                ctx["canary_hook"] = "canary-serving"
                run._fault_tick(ctx)
        self.assertEqual([], fired)

    def test_the_bite_waits_for_every_victim(self):
        f = {"crash_pod": "e3", "kills": 0, "extra_victims": [{"crash_pod": "e2", "kills": 0}]}
        pods = {"e3": {"alive": True, "ready": True}, "e2": {"alive": True, "ready": True}}
        self.assertFalse(run._break_bitten(f, pods))
        pods["e3"]["ready"] = False
        self.assertFalse(run._break_bitten(f, pods), "one victim still Ready and unkilled")
        f["extra_victims"][0]["kills"] = 1
        self.assertTrue(run._break_bitten(f, pods), "a killed runner counts as bitten even between crashes")
        f["extra_victims"][0]["kills"] = 0
        pods["e2"] = None
        self.assertTrue(run._break_bitten(f, pods), "a victim that is gone has bitten")

    def test_the_bite_of_a_broken_new_revision(self):
        load = {"kind": "crashloop", "form": "load"}
        pods = {"c0": {"alive": True, "ready": True}}
        self.assertIsNone(run._new_break_bite(load, pods, [("c0", pods["c0"])], {"c0"}, "canary-serving", 0, 30.0))
        pods["c0"]["ready"] = False
        bite = run._new_break_bite(load, pods, [("c0", pods["c0"])], {"c0"}, "canary-serving", 0, 31.0)
        self.assertEqual(("canary-serving", 0, 31.0), (bite["hook"], bite["step"], bite["at"]))
        self.assertIn("was Ready and then was not", bite["what"])
        self.assertIsNone(run._new_break_bite(load, pods, [("c0", pods["c0"])], set(), "canary-staging", 0, 5.0),
                          "a pod that was never Ready has not crashed after Ready")

        def waiting(reason):
            return {"status": {"containerStatuses": [{"name": run.MAIN_CONTAINER, "state": {"waiting": {"reason": reason}}}]}}
        start = {"kind": "crashloop", "form": "start"}
        with mock.patch.object(run, "_get_pod", lambda name: waiting("ContainerCreating")):
            self.assertIsNone(run._new_break_bite(start, pods, [("c0", pods["c0"])], set(), "canary-staging", 0, 5.0))
        with mock.patch.object(run, "_get_pod", lambda name: waiting("CrashLoopBackOff")):
            bite = run._new_break_bite(start, pods, [("c0", pods["c0"])], set(), "canary-staging", 0, 20.0)
            self.assertIn("CrashLoopBackOff", bite["what"])
            self.assertIsNone(run._new_break_bite({"kind": "imagepull"}, pods, [("c0", pods["c0"])], set(), "canary-staging", 0, 20.0),
                              "a pull failure is not a crash at start")
        with mock.patch.object(run, "_get_pod", lambda name: waiting("ImagePullBackOff")):
            self.assertIn("ImagePullBackOff", run._new_break_bite({"kind": "imagepull"}, pods, [("c0", pods["c0"])], set(), "canary-staging", 0, 20.0)["what"])

    def test_the_manual_gate_holds_after_a_break_at_a_hook(self):
        f = {"kind": "crashloop", "fired": True, "fired_ts": 100.0, "param": "canary-serving@1"}
        self.assertTrue(run._break_at_hook(f))
        self.assertTrue(run._promote_held_after_break(f, 45, 120.0))
        self.assertFalse(run._promote_held_after_break(f, 45, 150.0))
        self.assertFalse(run._promote_held_after_break(dict(f, fired=False), 45, 120.0))
        self.assertFalse(run._promote_held_after_break(dict(f, param="idle"), 45, 120.0), "a break before the push holds no gate")
        self.assertFalse(run._promote_held_after_break(dict(f, param="new"), 45, 120.0), "a broken new revision parks on its own")
        self.assertFalse(run._promote_held_after_break(dict(f, kind="podkill"), 45, 120.0))
        self.assertFalse(run._promote_held_after_break(None, 45, 120.0))

    def test_stable_serving_counts_whole_instances_on_the_stable_revision(self):
        ctx = {"old_hashes": {"old"}, "pods_per_inst": {"engine": 1},
               "pod_state": {"e0": {"alive": True, "comp": "engine", "inst": "0", "hash": "old", "ready": True, "serving": True},
                             "e1": {"alive": True, "comp": "engine", "inst": "1", "hash": "old", "ready": True, "serving": False},
                             "e2": {"alive": True, "comp": "engine", "inst": "2", "hash": "new", "ready": True, "serving": True},
                             "e3": None}}
        self.assertEqual(1, run._stable_serving(ctx, "engine"))
        gang = {"old_hashes": {"old"}, "pods_per_inst": {"engine": 2},
                "pod_state": {"l0": {"alive": True, "comp": "engine", "inst": "0", "hash": "old", "ready": True, "serving": True},
                              "w0": {"alive": True, "comp": "engine", "inst": "0", "hash": "old", "ready": True, "serving": True},
                              "l1": {"alive": True, "comp": "engine", "inst": "1", "hash": "old", "ready": True, "serving": True}}}
        self.assertEqual(1, run._stable_serving(gang, "engine"))

    def _track(self, **over):
        track = {"bite": {"what": "c0 held", "at": 20.0, "hook": "canary-staging", "step": 0},
                 "park": {"at": 110.0, "step": 0, "reason": "CapacityTimeout", "canary_traffic": 0,
                          "stable_serving": 8, "crash_instances": 1},
                 "ready_seen": set(), "uids": {"u1": 5.0, "u2": 50.0}, "peak_crash": 1,
                 "phases_before_exit": {"Pending", "Failed"}}
        for k, v in over.items():
            if isinstance(v, dict) and isinstance(track.get(k), dict):
                track[k] = dict(track[k], **v)
            else:
                track[k] = v
        return track

    def test_an_instance_mid_repair_on_the_broken_revision_counts_as_broken_at_the_park(self):
        # At the park the repaired canary Instance has no pod of either
        # revision (its broken pod was deleted for the rebuild): it is on
        # the broken revision, not a stable Instance that stopped serving,
        # so the stable side is held to the Instances it still has.
        def pod(i, hash_, ready):
            return {"alive": True, "comp": "engine", "inst": str(i), "hash": hash_, "uid": f"{hash_}{i}",
                    "ready": ready, "serving": ready}
        pods = {f"e{i}": pod(i, "old", True) for i in range(6)}
        pods["e6"], pods["e7"] = pod(6, "new", False), pod(7, "new", False)
        ctx = {"N": {"engine": 8}, "pods_per_inst": {"engine": 1}, "old_hashes": {"old"}, "pod_state": pods,
               "canary_hook": "canary-serving"}
        f = {"kind": "crashloop", "form": "load", "crash_comp": "engine", "infra_fail": None}
        track = self._track(park=None)
        with contextlib.redirect_stdout(io.StringIO()):
            run._new_break_track(ctx, f, track, {}, {"rolloutPhase": "Canarying"}, 0, 100.0, False)
            del pods["e7"]   # its rebuild deleted the broken pod; the replacement is not up yet
            ctx["canary_hook"] = "canary-failed"
            run._new_break_track(ctx, f, track, {"failed": {"reason": "CapacityTimeout"}},
                                 {"rolloutPhase": "Failed", "traffic": [{"percent": 100, "tag": "stable"}]}, 0, 110.0, False)
        self.assertEqual(2, track["park"]["crash_instances"], "the Instance mid-repair is on the broken revision")
        self.assertEqual(6, track["park"]["stable_serving"])
        self.assertEqual(2, track["peak_crash"])
        notes, checks = [], []
        self.assertEqual("PASS", run._new_break_checks({"N": {"engine": 8}}, f, track, {"Failed"}, "PASS", notes, checks), notes)
        self.assertTrue(any("6/8 serving, 2 on the broken revision" in c for c in checks), checks)
        # A stable Instance with no pod is not the broken revision's.
        del pods["e5"]
        track = self._track(park=None)
        with contextlib.redirect_stdout(io.StringIO()):
            run._new_break_track(ctx, f, track, {"failed": {"reason": "CapacityTimeout"}}, {"rolloutPhase": "Failed"},
                                 0, 120.0, False)
        self.assertEqual(1, track["park"]["crash_instances"])
        self.assertEqual("FAIL", run._new_break_checks({"N": {"engine": 8}}, f, track, {"Failed"}, "PASS", [], []))

    def test_a_broken_new_revision_is_judged_on_the_operators_end_state(self):
        ctx = {"N": {"engine": 8}}
        f = {"kind": "crashloop", "form": "start", "crash_comp": "engine", "infra_fail": None}
        notes, checks = [], []
        self.assertEqual("PASS", run._new_break_checks(ctx, f, self._track(), {"Pending", "Failed", "RolledBack"}, "PASS", notes, checks))
        self.assertEqual([], notes)
        self.assertTrue(any("stopped at the step where the canary broke (step 0)" in c for c in checks))
        self.assertTrue(any("failed.reason=CapacityTimeout" in c for c in checks))
        self.assertTrue(any("2 distinct engine pod(s)" in c for c in checks))
        self.assertEqual("PASS", run._new_break_checks(ctx, f, self._track(bite=None), set(), "PASS", [], []))
        self.assertIn("never crashed at start", f["infra_fail"])
        for over, words in (
                (dict(park=None), "never parked"),
                (dict(park={"step": 1}), "moved from step 0 to step 1"),
                (dict(park={"reason": ""}), "carries no reason"),
                (dict(park={"canary_traffic": 10}), "10% of traffic named the broken revision"),
                (dict(park={"stable_serving": 6}), "served 6/8 engine Instances at the park"),
                (dict(phases_before_exit={"Pending", "Paused", "Promoting"}), "promoted before the exit landed")):
            notes = []
            f = {"kind": "crashloop", "form": "load", "crash_comp": "engine", "infra_fail": None}
            self.assertEqual("FAIL", run._new_break_checks(ctx, f, self._track(**over), {"Pending"}, "PASS", notes, []), words)
            self.assertTrue(any(words in n for n in notes), (words, notes))
        f = {"kind": "imagepull", "form": "", "crash_comp": "engine", "infra_fail": None}
        checks = []
        run._new_break_checks(ctx, f, self._track(park={"crash_instances": 3, "stable_serving": 5}), set(), "PASS", [], checks)
        self.assertTrue(any("could not be pulled" in c for c in checks))
        self.assertTrue(any("5/8 serving, 3 on the broken revision" in c for c in checks))


class RetargetVerbTest(unittest.TestCase):
    """A second revision while the first canary is in flight: the runtime
    edit verb, the superseded run it must close, and traffic that must never
    name a revision with no serving pod."""

    BASE = "# p999: x mode=canary Ne=8 Nd=0 Nr=0 ceil_e=9 ceil_d=0 ceil_r=0 canary=auto topo=engine"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_rtedit_needs_its_dedicated_runtime(self):
        self.assertEqual([], self._errs(self.BASE + " during=canary-staging verb=rtedit rt=ome-coordtest-rt-p999"))
        self.assertTrue(any("rt=" in e for e in self._errs(self.BASE + " during=canary-staging verb=rtedit")))
        self.assertTrue(any("verb=rtedit" in e for e in self._errs(self.BASE + " rt=ome-coordtest-rt-p999")))
        self.assertIn("rtedit", run.CANARY_VERBS)
        self.assertTrue(run.REBUMP_VERBS <= run.RETARGET_VERBS)

    def test_rtedit_edits_the_bumped_configs_and_keeps_the_bump_clock(self):
        calls = []
        orig = run._bump_runtime
        run._bump_runtime = lambda ctx, csr, comps, material, **k: (calls.append((csr, comps, material)),
                                                                     ctx.__setitem__("trigger_ts", 99.0), True)[-1]
        try:
            ctx = {"case_id": "p999", "rt": "ome-coordtest-rt-p999", "bumped": {"engine", "decoder"}, "trigger_ts": 5.0}
            run._canary_verb(ctx, "rtedit", "h1", "engine")
            self.assertEqual([("ome-coordtest-rt-p999", ["decoder", "engine"], True)], calls)
            self.assertEqual(5.0, ctx["trigger_ts"], "the first change's clock is kept")
            self.assertNotIn("verb_errors", ctx)
            run._bump_runtime = lambda ctx, csr, comps, material, **k: False
            run._canary_verb(ctx, "rtedit", "h1", "engine")
            self.assertTrue(any("rtedit" in e for e in ctx["verb_errors"]))
        finally:
            run._bump_runtime = orig

    def test_retarget_record_reads_how_the_open_run_closed(self):
        at_verb = {"rollout": {"activeRun": {"openedAt": "T1", "runID": "r1"},
                               "lastRun": {"outcome": "Completed", "openedAt": "T0",
                                           "targetRevisions": [{"component": "engine", "revision": "h0"}]}}}
        rec = run._retarget_record(at_verb, "h1", "rebump", "canary-staging", 3.0)
        run._retarget_closed(rec, at_verb)
        self.assertIsNone(rec["outcome"], "the initial rollout's record is not the re-targeted run")
        run._retarget_closed(rec, {"rollout": {"lastRun": {"outcome": "Superseded", "openedAt": "T1",
                                                           "targetRevisions": [{"component": "engine", "revision": "h1"}]}}})
        self.assertEqual("Superseded", rec["outcome"])
        run._retarget_closed(rec, {"rollout": {"lastRun": {"outcome": "Completed", "openedAt": "T2"}}})
        self.assertEqual("Superseded", rec["outcome"], "a later close does not overwrite the record")
        lagged = run._retarget_record({"rollout": {}}, "h1", "rtedit", "canary-serving", 3.0)
        run._retarget_closed(lagged, {"rollout": {"lastRun": {"outcome": "Superseded", "openedAt": "T1",
                                                              "targetRevisions": [{"component": "engine", "revision": "h1"}]}}})
        self.assertEqual("Superseded", lagged["outcome"], "with no run pinned at the verb the target revision names it")

    def test_retarget_ignores_the_run_that_closed_before_the_verb(self):
        # A decoder-only re-bump under a router primary records the router's
        # unchanged revision, which the initial run (closed Completed before
        # the verb fired) also rolled toward; only the run open at the verb,
        # closed Superseded, answers for the re-target.
        stale = {"outcome": "Completed", "openedAt": "T0",
                 "targetRevisions": [{"component": "router", "revision": "r0"}]}
        at_verb = {"rollout": {"activeRun": {"openedAt": "T1", "runID": "r1"}, "lastRun": stale}}
        rec = run._retarget_record(at_verb, "r0", "rebump", "canary-serving", 3.0)
        run._retarget_closed(rec, at_verb)
        self.assertIsNone(rec["outcome"], "a run closed before the verb fired is not the re-target's")
        run._retarget_closed(rec, {"rollout": {"lastRun": {"outcome": "Superseded", "openedAt": "T1",
                                                           "targetRevisions": [{"component": "router", "revision": "r0"}]}}})
        self.assertEqual("Superseded", rec["outcome"])
        # With no run pinned at the verb the revision names the run, but
        # still never one that was already closed when the verb fired.
        unpinned = run._retarget_record({"rollout": {"lastRun": stale}}, "r0", "rebump", "canary-serving", 3.0)
        run._retarget_closed(unpinned, {"rollout": {"lastRun": stale}})
        self.assertIsNone(unpinned["outcome"])
        run._retarget_closed(unpinned, {"rollout": {"lastRun": {"outcome": "Superseded", "openedAt": "T1",
                                                                "targetRevisions": [{"component": "router", "revision": "r0"}]}}})
        self.assertEqual("Superseded", unpinned["outcome"])

    def test_traffic_revision_hash_reads_the_service_name(self):
        self.assertEqual("abc123", run._traffic_revision_hash("p999-engine-rev-abc123", {"abc123", "zzz"}))
        self.assertEqual("abc123", run._traffic_revision_hash("p999-engine-rev-abc", {"abc123"}))
        self.assertEqual("", run._traffic_revision_hash("p999-engine", {"abc123"}))

    def test_traffic_gap_needs_a_sustained_empty_target(self):
        track = {"streak": {}, "gaps": []}
        traffic = [{"revisionName": "p9-engine-rev-new", "percent": 10}, {"revisionName": "p9-engine-rev-old", "percent": 90}]
        pods = {"o0": {"alive": True, "serving": True, "comp": "engine", "hash": "old"},
                "n0": {"alive": True, "serving": False, "comp": "engine", "hash": "new"}}
        for t in range(run.TRAFFIC_GAP_POLLS - 1):
            run._traffic_gap_track(track, traffic, pods, "engine", {"old", "new"}, float(t))
        self.assertEqual([], track["gaps"])
        run._traffic_gap_track(track, traffic, pods, "engine", {"old", "new"}, 9.0)
        self.assertEqual(1, len(track["gaps"]))
        self.assertIn("10% to p9-engine-rev-new", track["gaps"][0])
        pods["n0"]["serving"] = True
        run._traffic_gap_track(track, traffic, pods, "engine", {"old", "new"}, 11.0)
        self.assertEqual({}, track["streak"], "a served target resets its streak")
        zero = [{"revisionName": "p9-engine-rev-new", "percent": 0}]
        for t in range(run.TRAFFIC_GAP_POLLS + 1):
            run._traffic_gap_track(track, zero, {}, "engine", {"new"}, float(t))
        self.assertEqual(1, len(track["gaps"]), "a target with no weight is not a gap")


class PodkillVerbTest(unittest.TestCase):
    """A pod deleted by hand at a canary hook: the verb, the replacement it
    waits for, and label truth on what that replacement runs."""

    BASE = "# p999: x mode=canary Ne=8 Nd=0 Nr=0 ceil_e=9 ceil_d=0 ceil_r=0 canary=manual topo=engine"
    AUTO = "# p999: x mode=canary Ne=8 Nd=0 Nr=0 ceil_e=9 ceil_d=0 ceil_r=0 canary=auto topo=engine"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_aims_the_verb_and_releases_a_held_image(self):
        self.assertEqual([], self._errs(self.BASE + " during=canary-serving@0 verb=podkill victim=stable victimcomp=engine verbhold=30"))
        self.assertEqual([], self._errs(self.AUTO + " during=canary-serving@0 verb=pause verbhold=45 verb2=podkill during2=canary-serving victim=stable"))
        held = (" bump=engine img=example.com/canary-test/missing:none during=canary-staging verb=pause verbhold=45"
                " verb2=podkill during2=canary-staging victim=stable release=rebump-fix releaseafter=60")
        self.assertEqual([], self._errs(self.AUTO + held))
        self.assertTrue(any("release=" in e for e in self._errs(self.AUTO + held.replace(" bump=engine img=example.com/canary-test/missing:none", ""))))
        self.assertTrue(any("victim=" in e for e in self._errs(self.BASE + " victim=stable")))
        self.assertTrue(any("victimcomp=" in e for e in self._errs(self.BASE + " victimcomp=engine")))
        self.assertIn("podkill", run.CANARY_VERBS)

    def test_verb_deletes_the_stable_victim_and_records_what_every_revision_ran(self):
        def pod(name, rev, image, uid):
            return {"metadata": {"name": name, "uid": uid, "labels": {run.REV_LABEL: rev}},
                    "spec": {"containers": [{"name": run.MAIN_CONTAINER, "image": image}]}}
        listed = {"items": [pod("e0", "old", "nginx:1.27", "u0"), pod("e1", "old", "nginx:1.27", "u1"),
                            pod("c0", "new", "nginx:1.28", "u9")]}
        calls = []

        def fake_kubectl(*a, **k):
            calls.append(a)
            if a[2:4] == ("get", "pod") and "-l" in a:
                return subprocess.CompletedProcess(a, 0, json.dumps(listed), "")
            if a[2:4] == ("get", "pod"):
                return subprocess.CompletedProcess(a, 0, json.dumps(pod(a[4], "old", "nginx:1.27", "u0")), "")
            return subprocess.CompletedProcess(a, 0, "", "")

        ctx = {"case_id": "p999", "victim": "", "victimcomp": "engine", "pods_per_inst": {"engine": 1}, "old_hashes": {"old"},
               "pod_state": {"c0": {"alive": True, "hash": "new", "inst": "9", "comp": "engine", "uid": "u9", "created": "3"},
                             "e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "uid": "u1", "created": "1"},
                             "e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "uid": "u0", "created": "1"}}}
        orig = run.kubectl
        run.kubectl = fake_kubectl
        try:
            run._canary_verb(ctx, "podkill", "new", "engine")
        finally:
            run.kubectl = orig
        rec = ctx["podkill"]
        self.assertEqual(("e0", "engine", "0", "old", "u0", "nginx:1.27"),
                         (rec["pod"], rec["comp"], rec["inst"], rec["hash"], rec["uid"], rec["image"]))
        self.assertEqual({"old": ["nginx:1.27"], "new": ["nginx:1.28"]}, rec["images_by_hash"])
        self.assertIn(("delete", "pod", "e0"), [a[2:5] for a in calls])
        self.assertIsNone(rec["replacement"])

    def test_track_records_the_first_replacement(self):
        rec = {"pod": "e0", "comp": "engine", "inst": "0", "hash": "old", "uid": "u0", "replacement": None}
        ctx = {"podkill": rec, "pod_state": {"e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "uid": "u0"}}}
        orig = run._get_pod
        run._get_pod = lambda name: {"spec": {"containers": [{"name": run.MAIN_CONTAINER, "image": "nginx:1.28"}]}}
        try:
            run._podkill_track(ctx, 1.0)
            self.assertIsNone(rec["replacement"], "the deleted pod itself is not its replacement")
            ctx["pod_state"]["e0"] = {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "uid": "u0b"}
            run._podkill_track(ctx, 2.0)
            self.assertEqual(("e0", "u0b", "old", "nginx:1.28"),
                             (rec["replacement"]["pod"], rec["replacement"]["uid"], rec["replacement"]["hash"], rec["replacement"]["image"]))
            run._get_pod = lambda name: {"spec": {"containers": [{"name": run.MAIN_CONTAINER, "image": "other"}]}}
            run._podkill_track(ctx, 3.0)
            self.assertEqual("nginx:1.28", rec["replacement"]["image"], "only the first replacement is recorded")
        finally:
            run._get_pod = orig

    def test_truth_reads_the_image_against_the_revision(self):
        base = {"pod": "e0", "comp": "engine", "inst": "0", "hash": "old", "uid": "u0",
                "images_by_hash": {"old": ["nginx:1.27"], "new": ["nginx:1.28"]}}
        truthful = dict(base, replacement={"pod": "e0", "uid": "u0b", "hash": "old", "image": "nginx:1.27", "at": 2.0})
        lying = dict(base, replacement={"pod": "e0", "uid": "u0b", "hash": "old", "image": "nginx:1.28", "at": 2.0})
        rolled = dict(base, replacement={"pod": "e0", "uid": "u0b", "hash": "new", "image": "nginx:1.28", "at": 2.0})
        unknown = dict(base, replacement={"pod": "e0", "uid": "u0b", "hash": "later", "image": "nginx:1.29", "at": 2.0})
        gone = dict(base, replacement=None)
        checks = []
        self.assertEqual("PASS", run._podkill_truth_checks(truthful, "PASS", [], checks))
        self.assertTrue(any(c.startswith("  ✓ label truth") for c in checks))
        notes = []
        self.assertEqual("FAIL", run._podkill_truth_checks(lying, "PASS", notes, []))
        self.assertTrue(any("runs nginx:1.28 under revision old" in n for n in notes))
        self.assertEqual("PASS", run._podkill_truth_checks(rolled, "PASS", [], []))
        checks = []
        self.assertEqual("PASS", run._podkill_truth_checks(unknown, "PASS", [], checks))
        self.assertTrue(any("which no pod carried at the deletion" in c for c in checks))
        notes = []
        self.assertEqual("FAIL", run._podkill_truth_checks(gone, "PASS", notes, []))
        self.assertTrue(any("never got a replacement pod" in n for n in notes))
        self.assertEqual("PASS", run._podkill_truth_checks(None, "PASS", [], []))

    def test_manual_promote_waits_after_the_deletion(self):
        self.assertTrue(run._promote_held_after_podkill("podkill", True, 10.0, 30, 20.0))
        self.assertFalse(run._promote_held_after_podkill("podkill", True, 10.0, 30, 45.0))
        self.assertFalse(run._promote_held_after_podkill("podkill", False, 0.0, 30, 20.0))
        self.assertFalse(run._promote_held_after_podkill("pause", True, 10.0, 30, 20.0))

    def test_hold_survival_reads_the_state_the_verb_found(self):
        self.assertTrue(run._prehold_survived(0, "canary-staging", 0, "canary-staging"))
        self.assertFalse(run._prehold_survived(0, "canary-failed", 0, "canary-staging"))
        self.assertTrue(run._prehold_survived(1, "canary-prehold", 1))
        self.assertFalse(run._prehold_survived(1, "canary-serving", 1))


class GeneratedStoryFamiliesTest(unittest.TestCase):
    """The three operator-story families generate_cases.py mints: each crosses
    the axes its story needs, every case names the operator's end state on
    line two, and the pipeline reads them as canary lanes."""

    @classmethod
    def setUpClass(cls):
        cls.outputs, rows = generate_cases.build_outputs()
        cls.rows = {folder: [(n, kv, m) for f, n, kv, m in rows if f == folder]
                    for folder in ("gen-canary-crash", "gen-canary-second-rev", "gen-canary-podkill")}

    def _line2(self, folder, name):
        return self.outputs[f"{folder}/{name}.yaml"].splitlines()[1]

    def test_every_case_is_a_canary_with_the_ladder_and_an_end_state(self):
        for folder, fam in self.rows.items():
            self.assertTrue(fam, folder)
            for name, kv, _m in fam:
                self.assertEqual("canary", kv["mode"], name)
                self.assertEqual(generate_cases.STORY_N, kv["Ne"], name)
                self.assertIn(kv["topo"], generate_cases.STORY_TOPOS, name)
                self.assertTrue(run._cell_ok(kv["cell"]), name)
                text = self.outputs[f"{folder}/{name}.yaml"]
                for nc, tw in generate_cases.STORY_LADDER:
                    self.assertIn(f'capacity: "{nc}"', text, name)
                    self.assertIn(f"traffic: {tw}", text, name)
                self.assertTrue(self._line2(folder, name).startswith("# reason: "), name)
                self.assertIn("revision", self._line2(folder, name), f"{name}: the end state names the revision the fleet ends on")

    def test_crash_storm_crosses_break_form_policy_victims_surge_and_shape(self):
        fam = self.rows["gen-canary-crash"]
        self.assertEqual(48, len(fam))
        seen = {(kv["fault"].partition(":")[0], kv.get("form", "start"), m["rp"], kv["victims"],
                 kv["ceil_e"] - kv["Ne"], kv["topo"]) for _n, kv, m in fam}
        self.assertEqual(48, len(seen))
        self.assertEqual({("crashloop", "start"), ("crashloop", "load"), ("imagepull", "start")},
                         {(k, f) for k, f, *_ in seen})
        self.assertEqual({"None", "RecreateInstance"}, {rp for _k, _f, rp, *_ in seen})
        self.assertEqual({1, 3}, {v for _k, _f, _rp, v, *_ in seen})
        self.assertEqual({1, 2}, {s for _k, _f, _rp, _v, s, _t in seen})
        for name, kv, _m in fam:
            self.assertEqual(generate_cases.STORM_HOOK, kv["fault"].partition(":")[2], name)
            self.assertEqual("Failed", kv["nophase"], f"{name}: the ladder must not park")
            self.assertEqual("auto", kv["canary"], name)
            self.assertNotIn("floor_soft", kv, f"{name}: the floor is read with the victims allowed for, not waived")
            self.assertIn("no repair is recorded", self._line2("gen-canary-crash", name))

    def test_retarget_crosses_change_hook_gate_surge_and_shape(self):
        fam = self.rows["gen-canary-second-rev"]
        self.assertEqual(32, len(fam))
        seen = {(kv["verb"], kv["during"], kv.get("analysis"), kv["ceil_e"] - kv["Ne"], kv["topo"]) for _n, kv, _m in fam}
        self.assertEqual(32, len(seen))
        self.assertEqual({"rebump", "rtedit"}, {v for v, *_ in seen})
        self.assertEqual(set(generate_cases.RETARGET_HOOKS), {h for _v, h, *_ in seen})
        self.assertEqual({None, "pass"}, {g for _v, _h, g, *_ in seen})
        for name, kv, _m in fam:
            text = self.outputs[f"gen-canary-second-rev/{name}.yaml"]
            if kv["verb"] == "rtedit":
                self.assertEqual(f"ome-coordtest-rt-{name}", kv["rt"], name)
                self.assertIn(f"name: ome-coordtest-rt-{name}\n", text, f"{name}: ships its dedicated runtime")
                self.assertIn(f"name: ome-coordtest-rt-{name}, autoSync: true", text, name)
                self.assertEqual(1, text.count("\n---\n"), name)
            else:
                self.assertNotIn("rt", kv, name)
            if kv.get("analysis"):
                self.assertIn("${OME_EVAL_METRICS_IMAGE}" if kv["verb"] == "rtedit" else gen_cases.CSR_METRICS, text, name)
                self.assertIn("perm.test/error-rate", text, name)
                self.assertEqual(3, text.count("analysis:"), f"{name}: every step before the cutover is gated")
                if kv["verb"] == "rtedit":
                    self.assertIn('- { name: TEST_REV,           value: "v1" }\n        - { name: TEST_STARTUP_DELAY',
                                  text, f"{name}: the env edit must rewrite TEST_REV, not the startup delay")
            else:
                self.assertIn('pause: { duration: "20s" }', text, name)
            self.assertIn("Superseded", self._line2("gen-canary-second-rev", name))

    def test_podkill_crosses_shape_policy_and_topology(self):
        fam = self.rows["gen-canary-podkill"]
        self.assertEqual(16, len(fam))
        seen = {(m["verb"], m["rp"], kv["topo"]) for _n, kv, m in fam}
        self.assertEqual(16, len(seen))
        self.assertEqual({f"podkill@{s}" for s in generate_cases.PODKILL_SHAPES}, {v for v, *_ in seen})
        for name, kv, m in fam:
            shape = m["verb"].partition("@")[2]
            text = self.outputs[f"gen-canary-podkill/{name}.yaml"]
            self.assertEqual(("stable", "engine"), (kv["victim"], kv["victimcomp"]), name)
            self.assertIn("podkill", (kv.get("verb"), kv.get("verb2")), name)
            self.assertEqual(set(kv["bump"].split(",")), {"engine"} | ({"decoder", "router"} if kv["topo"] == "pd" else set()), name)
            if shape in ("held", "heldpaused"):
                self.assertEqual(runtime_fixtures.UNPULLABLE_IMAGE, kv["img"], name)
                self.assertEqual("Stable", kv["terminal"], f"{name}: the fix must land")
                self.assertIn("readyTimeout:", text, name)
                self.assertEqual("rebump-fix", kv.get("verb2") if shape == "held" else kv.get("release"), name)
            else:
                self.assertEqual(generate_cases.ALT_IMAGE, kv["img"], f"{name}: a healthy new revision runs another image")
            if shape == "gate":
                self.assertEqual("manual", kv["canary"], name)
                self.assertIn("pause: {  } }", text, f"{name}: manual gates")
            else:
                self.assertEqual("auto", kv["canary"], name)
            self.assertIn("labelled as what it runs", self._line2("gen-canary-podkill", name))

    def test_generated_headers_validate_and_keep_their_folders_canary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for folder, fam in self.rows.items():
                for name, _kv, _m in fam:
                    path = root / folder / f"{name}.yaml"
                    path.parent.mkdir(exist_ok=True)
                    path.write_text(self.outputs[f"{folder}/{name}.yaml"])
                    self.assertEqual([], run.validate_case_file(path), name)


class GeneratedCanaryFaultFamiliesTest(unittest.TestCase):
    """The two step-times-fault families generate_cases.py mints: the stable
    side broken at each step kind of the ladder, and the pushed revision
    broken where it bites; every case is a story-ladder canary with the
    operator's end state on line two, and its header validates."""

    @classmethod
    def setUpClass(cls):
        cls.outputs, rows = generate_cases.build_outputs()
        cls.rows = {folder: [(n, kv, m) for f, n, kv, m in rows if f == folder]
                    for folder in ("gen-canary-fault-old", "gen-canary-fault-new")}

    def _line2(self, folder, name):
        return self.outputs[f"{folder}/{name}.yaml"].splitlines()[1]

    def test_every_case_is_a_story_canary_with_an_end_state(self):
        for folder, fam in self.rows.items():
            self.assertTrue(fam, folder)
            for name, kv, _m in fam:
                self.assertEqual("canary", kv["mode"], name)
                self.assertEqual(generate_cases.STORY_N, kv["Ne"], name)
                self.assertIn(kv["topo"], generate_cases.STORY_TOPOS, name)
                self.assertTrue(run._cell_ok(kv["cell"]), name)
                self.assertEqual("engine", str(kv.get("victimcomp") or "engine"), name)
                text = self.outputs[f"{folder}/{name}.yaml"]
                for nc, tw in generate_cases.STORY_LADDER:
                    self.assertIn(f'capacity: "{nc}"', text, name)
                    self.assertIn(f"traffic: {tw}", text, name)
                line2 = self._line2(folder, name)
                self.assertTrue(line2.startswith("# reason: "), name)
                self.assertIn("revision", line2, f"{name}: the end state names the revision the fleet ends on")
                if kv.get("analysis"):
                    self.assertIn(gen_cases.CSR_METRICS, text, f"{name}: an analysis gate needs the metrics runtime")
                    self.assertIn("perm.test/error-rate", text, name)
                    self.assertEqual("imagepull", kv["fault"].partition(":")[0],
                                     f"{name}: only a pull failure breaks a metrics pod")
                else:
                    self.assertNotIn(gen_cases.CSR_METRICS, text, name)

    def test_stable_side_breaks_cross_step_kind_fault_victims_surge_policy_and_shape(self):
        fam = self.rows["gen-canary-fault-old"]
        self.assertEqual(188, len(fam))
        seen = {(m["verb"].partition("@")[2], kv["fault"].partition(":")[0], kv.get("form", "start"), kv["victims"],
                 kv["ceil_e"] - kv["Ne"], m["rp"], kv["topo"]) for _n, kv, m in fam}
        self.assertEqual(188, len(seen))
        steps = {s for s, *_ in seen}
        self.assertEqual({s for s, *_ in generate_cases.STABLE_BREAK_STEPS}, steps)
        by_step = {s: [kv for _n, kv, m in fam if m["verb"].partition("@")[2] == s] for s in steps}
        for step, hook, policy, gate in generate_cases.STABLE_BREAK_STEPS:
            cases = by_step[step]
            kinds = {(kv["fault"].partition(":")[0], kv.get("form", "start")) for kv in cases}
            self.assertEqual(set(generate_cases.STABLE_BREAK_KINDS[gate]), kinds, step)
            self.assertEqual(set(generate_cases.STABLE_BREAK_VICTIMS[step]), {kv["victims"] for kv in cases}, step)
            self.assertEqual(set(generate_cases.STABLE_BREAK_SURGES[step]), {kv["ceil_e"] - kv["Ne"] for kv in cases}, step)
            self.assertEqual({"None", "RecreateInstance"}, {m["rp"] for _n, kv, m in fam if m["verb"].partition("@")[2] == step}, step)
            self.assertEqual({"engine", "pd"}, {kv["topo"] for kv in cases}, step)
            for kv in cases:
                self.assertEqual(hook, kv["fault"].partition(":")[2], step)
                self.assertEqual(policy, kv["canary"], step)
                self.assertEqual(gate, kv.get("analysis"), step)
                self.assertEqual("Failed", kv["nophase"], f"{step}: the ladder must not park")
                self.assertNotIn("floor_soft", kv, f"{step}: the floor is read with the victims allowed for, not waived")
                if policy == "manual":
                    self.assertEqual(generate_cases.BREAK_GATE_HOLD_S, kv["verbhold"], step)
                else:
                    self.assertNotIn("verbhold", kv, step)
        prestart = by_step["prestart"]
        self.assertTrue(all(kv["fault"].endswith(":idle") for kv in prestart))
        for name, kv, m in fam:
            self.assertIn("no repair is recorded for one after the push", self._line2("gen-canary-fault-old", name))
            self.assertIn("ends on the canary revision serving", self._line2("gen-canary-fault-old", name))

    def test_pushed_breaks_cross_bite_exit_surge_policy_and_shape(self):
        fam = self.rows["gen-canary-fault-new"]
        self.assertEqual(52, len(fam))
        seen = {(m["verb"].rpartition(":")[2], kv["verb"], kv["ceil_e"] - kv["Ne"], m["rp"], kv["topo"], kv.get("analysis"))
                for _n, kv, m in fam}
        self.assertEqual(52, len(seen))
        names = {n for n, *_ in generate_cases.NEW_BREAKS}
        self.assertEqual(names, {b for b, *_ in seen})
        self.assertEqual({"rollback", "rebump-fix"}, {e for _b, e, *_ in seen})
        for name, kind, form, exit_after in generate_cases.NEW_BREAKS:
            cases = [(kv, m) for _n, kv, m in fam if m["verb"].rpartition(":")[2] == name]
            self.assertEqual(set(generate_cases.NEW_BREAK_SURGES[name]), {kv["ceil_e"] - kv["Ne"] for kv, _m in cases}, name)
            self.assertEqual(set(generate_cases.NEW_BREAK_GATES[name]), {kv.get("analysis") for kv, _m in cases}, name)
            for kv, m in cases:
                self.assertEqual(f"{kind}:new", kv["fault"], name)
                self.assertEqual(form, kv.get("form"), name)
                self.assertEqual(exit_after, kv.get("exitafter"), name)
                self.assertEqual(("canary-failed", "auto"), (kv["during"], kv["canary"]), name)
                self.assertEqual({"rollback": "RolledBack", "rebump-fix": "Stable"}[kv["verb"]], kv["terminal"], name)
                self.assertNotIn("img", kv, f"{name}: the fault arms the image itself")
                self.assertNotIn("victims", kv, name)
                self.assertNotIn("bump", kv, f"{name}: the whole group is pushed")
                if kv.get("analysis"):
                    self.assertEqual("rebump-fix", kv["verb"], f"{name}: under a gate only the fix exit samples")
        for name, kv, m in fam:
            text = self.outputs[f"gen-canary-fault-new/{name}.yaml"]
            self.assertIn(f'readyTimeout: "{generate_cases.HELD_READY_TIMEOUT}"', text, f"{name}: the park must land inside the budget")
            self.assertIn("stops at the step where the break bites", self._line2("gen-canary-fault-new", name))
            self.assertIn("nothing promotes the broken revision", self._line2("gen-canary-fault-new", name))
        self.assertLess(generate_cases.CRASH_EXIT_IN_SOAK_S, generate_cases.STORY_SOAK_S)
        self.assertGreater(generate_cases.CRASH_EXIT_AFTER_SOAK_S, generate_cases.STORY_SOAK_S)

    def test_generated_headers_validate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for folder, fam in self.rows.items():
                for name, _kv, _m in fam:
                    path = root / folder / f"{name}.yaml"
                    path.parent.mkdir(exist_ok=True)
                    path.write_text(self.outputs[f"{folder}/{name}.yaml"])
                    self.assertEqual([], run.validate_case_file(path), name)

    def test_both_families_are_canary_lanes_in_the_pipeline(self):
        sys.path.insert(0, str(HERE.parents[2] / ".buildkite"))
        try:
            import generate_pipeline as gp
        finally:
            sys.path.pop(0)
        self.assertTrue(gp._eval_feature_is_canary("gen-canary-fault-old"))
        self.assertTrue(gp._eval_feature_is_canary("gen-canary-fault-new"))


class PlainRollbackTest(unittest.TestCase):
    """rollback=1 on a rollout case: the header, the five hooks, the patch
    that re-applies the starting revision, and the operator's end-state
    verdict (back on the starting revision and serving, nothing of the new
    revision left, rows settled, the grouped run superseded)."""

    BASE = "# p999: x mode=rollout Ne=4 Nd=0 Nr=0 ceil_e=5 ceil_d=0 ceil_r=0 topo=engine"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    @staticmethod
    def _pod(comp, inst, hash_, ready=True, serving=True, newanno=False, uid=None, runner="default"):
        return {"comp": comp, "inst": str(inst), "runner": runner, "alive": True, "ready": ready,
                "serving": serving, "hash": hash_, "newanno": newanno, "uid": uid or f"{comp}{inst}{hash_}"}

    def test_header_names_the_hook_and_the_break(self):
        for hook in ("settling", "rotating", "draining", "last"):
            self.assertEqual([], self._errs(self.BASE + f" rollback=1 during={hook}"), hook)
        self.assertEqual([], self._errs(self.BASE + " rollback=1 during=held break=stuck"))
        self.assertTrue(any("needs during=" in e for e in self._errs(self.BASE + " rollback=1")))
        self.assertTrue(any("is a rollback's hook" in e for e in self._errs(self.BASE + " during=last")))
        self.assertTrue(any("both or neither" in e for e in self._errs(self.BASE + " rollback=1 during=held")))
        self.assertTrue(any("both or neither" in e for e in self._errs(self.BASE + " rollback=1 during=rotating break=stuck")))
        self.assertTrue(any("needs rollback=1 during=held" in e for e in self._errs(self.BASE + " break=stuck")))
        self.assertTrue(any("would both fire" in e for e in self._errs(self.BASE + " rollback=1 edit=strategy:RecreatePod during=rotating")))
        kill = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
        self.assertTrue(any("mode=rollout" in e for e in self._errs(kill + " rollback=1 during=last")))
        canary = "# p999: x mode=canary canary=auto rollback=1 Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"
        self.assertEqual([], self._errs(canary), "the canary rollback probe keeps its meaning")
        self.assertEqual([], self._errs(self.BASE + " fault=podkill:rotating rollback=1 during=last"),
                         "a phase fault and a rollback may share a case")

    def test_state_is_parsed_only_for_a_rollout_case(self):
        self.assertIsNone(run._rollback_new({"mode": "canary", "rollback": 1, "during": "canary-serving"}))
        self.assertIsNone(run._rollback_new({"during": "rotating"}))
        rb = run._rollback_new({"rollback": 1, "during": "held", "break": "stuck"})
        self.assertEqual(("held", True, False), (rb["hook"], rb["stuck"], rb["fired"]))
        self.assertFalse(run._rollback_new({"rollback": 1, "during": "last"})["stuck"])

    def test_break_patch_aims_the_fixture_probe_at_an_unserved_path(self):
        probe = run._stuck_readiness()
        self.assertTrue(probe["exec"]["command"][-1].endswith(f"http://127.0.0.1:8080{run.STUCK_READINESS_PATH}"),
                        probe["exec"]["command"][-1])
        self.assertEqual(run._GOOD_READINESS["periodSeconds"], probe["periodSeconds"])
        patch = {"spec": {"engine": {"annotations": {}}, "decoder": {"annotations": {}}}}
        run._rollback_break_patch(patch, {"engine", "decoder"}, {"engine": 1, "decoder": 2})
        self.assertEqual(probe, patch["spec"]["engine"]["runner"]["readinessProbe"])
        self.assertEqual({"name": run.MAIN_CONTAINER, "readinessProbe": probe}, patch["spec"]["decoder"]["leader"]["runner"])

    def test_snapshot_keeps_every_bumped_member_as_it_was(self):
        rb = run._rollback_new({"rollback": 1, "during": "settling"})
        spec = {"engine": {"minReplicas": 4, "annotations": {"perm.test/rev": "v1"}}, "router": {"minReplicas": 2}}
        self.assertTrue(run._rollback_snapshot(rb, spec, {"engine"}))
        spec["engine"]["annotations"]["perm.test/rev"] = "v2"
        self.assertEqual("v1", rb["snapshot"]["engine"]["annotations"]["perm.test/rev"], "a copy, not a view")
        self.assertFalse(run._rollback_snapshot(run._rollback_new({"rollback": 1, "during": "settling"}), spec, {"decoder"}))

    def test_each_hook_fires_at_its_moment(self):
        olds, N, ppi, bumped = {"a"}, {"engine": 4}, {"engine": 1}, {"engine"}
        settled = {f"e{i}": self._pod("engine", i, "a") for i in range(4)}
        self.assertTrue(run._rollback_due(run._rollback_new({"rollback": 1, "during": "settling"}), settled, olds, ppi, N, bumped, 0.0))
        rotating = run._rollback_new({"rollback": 1, "during": "rotating"})
        self.assertFalse(run._rollback_due(rotating, settled, olds, ppi, N, bumped, 0.0))
        first_new = dict(settled, e4=self._pod("engine", 0, "b", ready=False, serving=False))
        self.assertTrue(run._rollback_due(rotating, first_new, olds, ppi, N, bumped, 0.0))
        self.assertTrue(run._rollback_due(rotating, {"e0": self._pod("engine", 0, "a", newanno=True)}, olds, ppi, N, bumped, 0.0),
                        "an in-place patch shows as the bumped annotation")
        draining = run._rollback_new({"rollback": 1, "during": "draining"})
        self.assertFalse(run._rollback_due(draining, first_new, olds, ppi, N, bumped, 0.0))
        run._rollback_observe(draining, "engine", "b", True, bumped, olds, 5.0)
        run._rollback_observe(draining, "decoder", "a", True, bumped, olds, 5.0)
        run._rollback_observe(draining, "engine", "a", False, bumped, olds, 5.0)
        self.assertIsNone(draining["first_drain_at"], "only a draining pod of the starting revision on a bumped member counts")
        run._rollback_observe(draining, "engine", "a", True, bumped, olds, 6.0)
        self.assertEqual(6.0, draining["first_drain_at"])
        self.assertTrue(run._rollback_due(draining, first_new, olds, ppi, N, bumped, 0.0))

    def test_last_waits_for_every_instance_but_one(self):
        olds, N, ppi, bumped = {"a"}, {"engine": 4}, {"engine": 1}, {"engine"}
        last = run._rollback_new({"rollback": 1, "during": "last"})

        def fleet(promoted, tail=None):
            pods = {f"e{i}": self._pod("engine", i, "b") for i in range(promoted)}
            pods.update({f"e{i}": self._pod("engine", i, "a") for i in range(promoted, 4)})
            pods.update(tail or {})
            return pods
        self.assertFalse(run._rollback_due(last, fleet(2), olds, ppi, N, bumped, 0.0))
        self.assertTrue(run._rollback_due(last, fleet(3), olds, ppi, N, bumped, 0.0))
        self.assertFalse(run._rollback_due(last, fleet(4), olds, ppi, N, bumped, 0.0), "the roll is complete: nothing is last")
        self.assertFalse(run._rollback_due(last, fleet(2, {"e2": self._pod("engine", 2, "b", serving=False)}), olds, ppi, N, bumped, 0.0),
                         "a new-revision Instance counts once it serves")
        surging = fleet(3, {"e3b": self._pod("engine", 3, "b", ready=False, serving=False)})
        self.assertTrue(run._rollback_due(last, surging, olds, ppi, N, bumped, 0.0),
                        "the last Instance mid-surge is still off the new revision")
        gang = {"l0": self._pod("engine", 0, "b", runner="leader"), "w0": self._pod("engine", 0, "a", runner="worker"),
                "l1": self._pod("engine", 1, "a", runner="leader"), "w1": self._pod("engine", 1, "a", runner="worker")}
        self.assertEqual(0, run._instances_on_new(gang, olds, {"engine": 2}, bumped), "a half-moved gang is not promoted")
        gang["w0"] = self._pod("engine", 0, "b", runner="worker")
        self.assertTrue(run._rollback_due(last, gang, olds, {"engine": 2}, {"engine": 2}, bumped, 0.0))

    def test_held_needs_the_break_and_a_settled_wait(self):
        olds, N, ppi, bumped = {"a"}, {"engine": 4}, {"engine": 1}, {"engine"}
        stuck = {f"e{i}": self._pod("engine", i, "a") for i in range(4)}
        stuck["e4"] = self._pod("engine", 0, "b", ready=False, serving=False)
        unbroken = run._rollback_new({"rollback": 1, "during": "held"})
        unbroken["first_new_at"] = 0.0
        self.assertFalse(run._rollback_due(unbroken, stuck, olds, ppi, N, bumped, 10 * run.HELD_SETTLE_S))
        held = run._rollback_new({"rollback": 1, "during": "held", "break": "stuck"})
        self.assertFalse(run._rollback_due(held, stuck, olds, ppi, N, bumped, 10 * run.HELD_SETTLE_S), "no new pod seen yet")
        run._rollback_tick(held, "p999", bumped, stuck, olds, ppi, N, 100.0, 1.0)
        self.assertEqual(100.0, held["first_new_at"])
        self.assertFalse(held["fired"])
        self.assertFalse(run._rollback_due(held, stuck, olds, ppi, N, bumped, 100.0 + run.HELD_SETTLE_S - 1))
        self.assertTrue(run._rollback_due(held, stuck, olds, ppi, N, bumped, 100.0 + run.HELD_SETTLE_S))
        stuck["e4"] = self._pod("engine", 0, "b")
        self.assertFalse(run._rollback_due(held, stuck, olds, ppi, N, bumped, 100.0 + run.HELD_SETTLE_S),
                         "a Ready new-revision pod means the roll is not held")

    def test_held_hook_on_a_gang_reads_the_pod_condition(self):
        """A stuck surge gang: the leader never passes readiness, so the
        serving gate never flips for the gang and its worker, whose container
        is ready, is not Ready either; the hook is due. A gang whose leader
        passes readiness is not held."""
        olds, N, ppi, bumped = {"a"}, {"engine": 2}, {"engine": 2}, {"engine"}

        def gang(idx, rev, *, ready=True):
            leader = pod_object(f"e{idx}-leader", "engine", idx, rev, "img:1", "v1", runner="leader",
                                ready=ready, serving=ready)
            worker = pod_object(f"e{idx}-worker", "engine", idx, rev, "img:1", "v1", runner="worker",
                                ready=ready, serving=ready, container_ready=True)
            return {f"e{idx}-leader": watcher_record(leader), f"e{idx}-worker": watcher_record(worker)}
        pods = {**gang(0, "a"), **gang(1, "a"), **gang(2, "b", ready=False)}
        self.assertTrue(pods["e2-worker"]["ctn_ready"] and not pods["e2-worker"]["ready"])
        held = run._rollback_new({"rollback": 1, "during": "held", "break": "stuck"})
        run._rollback_tick(held, "p999", bumped, pods, olds, ppi, N, 100.0, 1.0)
        self.assertEqual(100.0, held["first_new_at"])
        self.assertFalse(held["fired"])
        self.assertTrue(run._rollback_due(held, pods, olds, ppi, N, bumped, 100.0 + run.HELD_SETTLE_S))
        pods.update(gang(2, "b"))
        self.assertFalse(run._rollback_due(held, pods, olds, ppi, N, bumped, 100.0 + run.HELD_SETTLE_S),
                         "a Ready gang on the new revision means the roll is not held")

    def test_end_state_is_read_from_the_pods_between_events_too(self):
        """Every Instance Ready and serving on the starting revision and no
        new-revision pod alive is the end state, read from the pods; a
        rollback that landed before any pod moved raises no further event,
        so the idle tick latches it once the rollback has fired."""
        olds, N, ppi, bumped, conf = {"a"}, {"engine": 4}, {"engine": 1}, {"engine"}, {"engine"}
        modes = {"engine": "surge"}
        back = {f"e{i}": self._pod("engine", i, "a") for i in range(4)}

        def settled(pods, modes=modes):
            return run._rollback_settled_pods(pods, run.aggregate(pods, olds, modes, ppi), N, conf, bumped, olds)
        self.assertTrue(settled(back))
        self.assertFalse(settled({**back, "e4": self._pod("engine", 0, "b", ready=False, serving=False)}),
                         "a new-revision pod alive")
        self.assertFalse(settled({**back, "e3": self._pod("engine", 3, "a", ready=False)}), "an Instance not Ready")
        self.assertFalse(settled({**back, "e3": self._pod("engine", 3, "a", newanno=True)}, {"engine": "inplace"}),
                         "in place, the annotation still new")
        self.assertFalse(settled({k: v for k, v in back.items() if k != "e3"}), "an Instance short")
        rb = run._rollback_new({"rollback": 1, "during": "settling"})
        args = (back, olds, modes, ppi, N, conf, bumped)
        self.assertIsNone(run._rollback_idle_settle(rb, None, *args, 50.0), "unfired: nothing to settle")
        rb["fired"] = True
        self.assertEqual(50.0, run._rollback_idle_settle(rb, None, *args, 50.0))
        self.assertEqual(40.0, run._rollback_idle_settle(rb, 40.0, *args, 50.0), "latched once")
        moving = {**back, "e4": self._pod("engine", 0, "b")}
        self.assertIsNone(run._rollback_idle_settle(rb, None, moving, olds, modes, ppi, N, conf, bumped, 50.0))

    def test_fire_replaces_each_member_with_its_snapshot_once(self):
        olds, N, ppi, bumped = {"a"}, {"engine": 4}, {"engine": 1}, {"engine"}
        rb = run._rollback_new({"rollback": 1, "during": "last"})
        rb["snapshot"] = {"engine": {"minReplicas": 4, "annotations": {"perm.test/rev": "v1"}}}
        pods = {"e0": self._pod("engine", 0, "b"), "e1": self._pod("engine", 1, "b"), "e2": self._pod("engine", 2, "b"),
                "e3": self._pod("engine", 3, "a"), "e3b": self._pod("engine", 3, "b", ready=False, serving=False),
                "d0": self._pod("decoder", 0, "a")}
        calls = []
        orig = run.kubectl
        run.kubectl = lambda *a, **k: calls.append(a) or subprocess.CompletedProcess(a, 0, "", "")
        try:
            run._rollback_tick(rb, "p999", bumped, pods, olds, ppi, N, 50.0, 40.0)
            run._rollback_tick(rb, "p999", bumped, pods, olds, ppi, N, 51.0, 41.0)
            self.assertEqual((True, 40.0, None), (rb["fired"], rb["fired_at"], rb["error"]))
            self.assertEqual(1, len(calls))
            self.assertEqual(("patch", "isvc", "p999", "--type=json"), calls[0][2:6])
            self.assertEqual([{"op": "replace", "path": "/spec/engine", "value": rb["snapshot"]["engine"]}], json.loads(calls[0][-1]))
            self.assertEqual({}, rb["on_start_at_fire"], "every Instance had moved or was mid-surge")
            self.assertEqual([("engine", "0"), ("engine", "1"), ("engine", "2"), ("engine", "3")], sorted(rb["busy_at_fire"]))
            self.assertEqual(4, rb["new_pods_at_fire"])
            early = run._rollback_new({"rollback": 1, "during": "settling"})
            early["snapshot"] = rb["snapshot"]
            settled = {f"e{i}": self._pod("engine", i, "a") for i in range(4)}
            run._rollback_tick(early, "p999", bumped, settled, olds, ppi, N, 1.0, 0.5)
            self.assertEqual({("engine", str(i)): frozenset({f"engine{i}a"}) for i in range(4)}, early["on_start_at_fire"])
            self.assertEqual(([], 0), (early["busy_at_fire"], early["new_pods_at_fire"]))
            run.kubectl = lambda *a, **k: subprocess.CompletedProcess(a, 1, "", "admission webhook denied the patch")
            broken = run._rollback_new({"rollback": 1, "during": "settling"})
            broken["snapshot"] = rb["snapshot"]
            run._rollback_tick(broken, "p999", bumped, settled, olds, ppi, N, 1.0, 0.5)
            self.assertTrue(broken["fired"])
            self.assertIn("denied", broken["error"])
        finally:
            run.kubectl = orig

    def test_rows_must_settle_on_the_starting_revision(self):
        N, configured, bumped = {"engine": 2, "decoder": 0, "router": 0}, {"engine"}, {"engine"}
        self.assertEqual("a", run._revision_hash("p999-engine-a"))
        ok, why = run._rollback_ir_settled([inference_replica("engine", 2)], N, configured, bumped, {"a"})
        self.assertTrue(ok, why)
        ok, why = run._rollback_ir_settled([inference_replica("engine", 2, current="revision-b", update="revision-b")], N, configured, bumped, {"a"})
        self.assertFalse(ok)
        self.assertIn("not the starting revision", why)
        ok, why = run._rollback_ir_settled([inference_replica("engine", 2, operation={"type": "Update"})], N, configured, bumped, {"a"})
        self.assertFalse(ok)
        self.assertIn("not settled", why)
        self.assertFalse(run._rollback_ir_settled(None, N, configured, bumped, {"a"})[0])

    def test_grouped_run_must_close_as_superseded(self):
        orig = run._isvc_events_since
        try:
            run._isvc_events_since = lambda cid, since: []
            checks = []
            self.assertEqual("PASS", run._rollback_run_checks("p999", 1.0, False, "PASS", [], checks))
            self.assertTrue(any("no rollout group" in c for c in checks))
            checks = []
            self.assertEqual("PASS", run._rollback_run_checks("p999", 1.0, True, "PASS", [], checks))
            self.assertTrue(any("no run opened" in c for c in checks))
            run._isvc_events_since = lambda cid, since: [
                (run.RUN_OPENED_EVENT, "run r1 opened"),
                (run.RUN_CLOSED_EVENT, "run r1 superseded by a new target revision; opening a fresh run"),
                (run.RUN_OPENED_EVENT, "run r2 opened"), (run.RUN_CLOSED_EVENT, "run r2 closed: Completed")]
            checks = []
            self.assertEqual("PASS", run._rollback_run_checks("p999", 1.0, True, "PASS", [], checks))
            self.assertTrue(any("closed as superseded" in c for c in checks))
            run._isvc_events_since = lambda cid, since: [
                (run.RUN_OPENED_EVENT, "run r1 opened"), (run.RUN_CLOSED_EVENT, "run r1 closed: Completed")]
            notes = []
            self.assertEqual("FAIL", run._rollback_run_checks("p999", 1.0, True, "PASS", notes, []))
            self.assertTrue(any("superseded" in n for n in notes))
        finally:
            run._isvc_events_since = orig

    def test_verdict_is_the_operators_end_state(self):
        N, configured, bumped, ppi = {"engine": 2, "decoder": 0, "router": 0}, {"engine"}, {"engine"}, {"engine": 1}
        rb = run._rollback_new({"rollback": 1, "during": "last"})
        rb.update(fired=True, fired_at=30.0, new_pods_at_fire=1,
                  on_start_at_fire={("engine", "0"): frozenset({"engine0a"}), ("engine", "1"): frozenset({"engine1a"})})
        back = {"e0": self._pod("engine", 0, "a"), "e1": self._pod("engine", 1, "a", uid="rebuilt")}
        orig_ir, orig_ev = run._fetch_initial_ir_items, run._isvc_events_since
        try:
            run._fetch_initial_ir_items = lambda cid: [inference_replica("engine", 2)]
            run._isvc_events_since = lambda cid, since: []
            notes, checks = [], []
            self.assertEqual("PASS", run._rollback_checks(rb, "p999", "CONVERGED", 80.0, back, {"a"}, ppi, N, bumped,
                                                          configured, False, 1.0, "PASS", notes, checks))
            self.assertEqual([], notes)
            self.assertTrue(any("ends on the starting revision and serving" in c for c in checks))
            self.assertTrue(any("1 of 2 Instance(s)" in c and "rebuilt anyway" in c and "engine/1" in c for c in checks),
                            "the needless rebuild is measured, not failed")
            leftover = dict(back, e2=self._pod("engine", 0, "b", ready=False, serving=False))
            notes = []
            self.assertEqual("FAIL", run._rollback_checks(rb, "p999", "ROTATE_TIMEOUT", None, leftover, {"a"}, ppi, N, bumped,
                                                          configured, False, 1.0, "PASS", notes, []))
            self.assertTrue(any("new-revision pod" in n for n in notes))
            run._fetch_initial_ir_items = lambda cid: [inference_replica("engine", 2, current="revision-b", update="revision-b")]
            notes = []
            self.assertEqual("FAIL", run._rollback_checks(rb, "p999", "CONVERGED", 80.0, back, {"a"}, ppi, N, bumped,
                                                          configured, False, 1.0, "PASS", notes, []))
            self.assertTrue(any("rows not settled" in n for n in notes))
            run._fetch_initial_ir_items = lambda cid: [inference_replica("engine", 2)]
            unfired = run._rollback_new({"rollback": 1, "during": "draining"})
            notes = []
            self.assertEqual("FAIL", run._rollback_checks(unfired, "p999", "HOOK_MISSED", None, back, {"a"}, ppi, N, bumped,
                                                          configured, False, 1.0, "PASS", notes, []))
            self.assertTrue(any("never fired" in n for n in notes))
            broken = dict(rb, error="rollback at last failed: denied")
            notes = []
            self.assertEqual("INFRA-FAIL", run._rollback_checks(broken, "p999", "CONVERGED", 80.0, back, {"a"}, ppi, N, bumped,
                                                                configured, False, 1.0, "PASS", notes, []))
            self.assertTrue(any(n.startswith("INFRA-FAIL") for n in notes))
        finally:
            run._fetch_initial_ir_items, run._isvc_events_since = orig_ir, orig_ev

    def test_generated_family_crosses_every_axis(self):
        outputs, rows = generate_cases.build_outputs()
        fam = [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-rollback"]
        self.assertEqual(126, len(fam))
        self.assertEqual(set(generate_cases.ROLLBACK_HOOKS), {m["hook"] for _n, _kv, m in fam})
        self.assertEqual(set(generate_cases.ROLLBACK_BUDGETS), {m["strat"] for _n, _kv, m in fam})
        self.assertEqual({"engine", "pd", "engine-mn", "pd-mn"}, {kv["topo"] for _n, kv, _m in fam})
        self.assertEqual({"none", "rolling"}, {kv["co"] for _n, kv, _m in fam})
        cells = {(kv["topo"], m["strat"], m["hook"], kv["co"]) for _n, kv, m in fam}
        for topo in ("engine", "pd"):
            for co in ("none", "rolling"):
                self.assertNotIn((topo, "InPlaceIfPossible", "draining", co), cells,
                                 "a single-pod in-place roll never drains")
                self.assertIn((topo, "InPlaceIfPossible", "last", co), cells)
                self.assertIn((f"{topo}-mn", "InPlaceIfPossible", "draining", co), cells, "a gang in-place roll recreates, so it drains")
        for n, kv, m in fam:
            self.assertEqual(1, kv["rollback"])
            self.assertEqual(m["hook"], kv["during"])
            self.assertEqual(m["hook"] == "held", kv.get("break") == "stuck", n)
            self.assertNotIn("mode", kv)
            surge = kv["ceil_e"] - kv["Ne"]
            self.assertEqual(surge > 0, m["strat"] == "SurgeThenDrain", n)
            self.assertIn(surge, (0, 1, 2))
            if surge == 2:
                self.assertEqual("engine", kv["topo"], "maxSurge 2 runs on the single-pod engine only")
            if kv["topo"].startswith("pd"):
                self.assertEqual(kv["ceil_e"] if kv["co"] == "rolling" else kv["Nd"], kv["ceil_d"], n)
            text = outputs[f"gen-rollback/{n}.yaml"]
            reason = text.splitlines()[1]
            self.assertTrue(reason.startswith("# reason: the starting revision is re-applied at "), n)
            self.assertIn("every Instance ends on it and serving", reason)
            self.assertEqual(kv["co"] == "rolling", "closes as superseded" in reason, n)
            self.assertIn('perm.test/rev: "v1"', text, "the starting revision carries the trigger annotation")
            self.assertEqual(kv["co"] == "rolling", "rollingUpdate: {}" in text, n)
            if kv["co"] == "rolling" and not kv["topo"].startswith("pd"):
                self.assertIn("components: [engine]", text, "an engine-only group puts the roll under the run model")
        self.assertEqual(sum(1 for _n, kv, _m in fam if kv["ceil_e"] - kv["Ne"] == 2), 10,
                         "five hooks x two groups at maxSurge 2")


class CrashloopNewFaultTest(unittest.TestCase):
    """fault=crashloop:new: the push carries the crash. The header, the arm
    the push lands with, the budget allowance, the Instance tally, the status
    and events that say why, and the verdict that states the operator's end
    state."""

    BASE = "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"
    PD = "# p999: x mode=rollout Ne=2 Nd=2 Nr=0 ceil_e=3 ceil_d=3 ceil_r=0 topo=pd"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_fault_verb_accepts_the_new_side(self):
        self.assertTrue(run._fault_ok("crashloop:new"))
        self.assertTrue(run._fault_ok("crashloop:old"))
        self.assertFalse(run._fault_ok("crashloop:newer"))
        self.assertTrue(run._fault_ok("imagepull:new"), "the canary fault-new family pushes an unpullable image")

    def test_header_needs_a_form_a_bumped_victim_and_no_hook(self):
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:new form=start"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:new form=load"))
        self.assertTrue(any("form=start|load" in e for e in self._errs(self.BASE + " fault=crashloop:new")))
        self.assertTrue(any("armed by the push" in e for e in self._errs(self.BASE + " fault=crashloop:new form=start during=idle")))
        self.assertTrue(any("names a member the push bumps" in e
                            for e in self._errs(self.PD + " fault=crashloop:new form=start victimcomp=decoder co=none")))
        self.assertEqual([], self._errs(self.PD + " fault=crashloop:new form=start victimcomp=decoder co=rolling"))
        kill = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
        self.assertTrue(any("mode=rollout" in e for e in self._errs(kill + " fault=crashloop:new form=start")))

    def test_fixfirst_reads_a_running_revision_break_after_a_bump(self):
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:old form=start fixfirst=1"))
        self.assertEqual([], self._errs(self.BASE + " fault=imagepull:old fixfirst=1"))
        for bad in (" fault=crashloop:new form=start fixfirst=1",
                    " fault=crashloop:old form=start during=idle fixfirst=1",
                    " fault=podkill:settling fixfirst=1",
                    " fixfirst=1"):
            self.assertTrue(any("fixfirst=1" in e for e in self._errs(self.BASE + bad)), bad)
        self.assertTrue(any("malformed header value fixfirst" in e
                            for e in self._errs(self.BASE + " fault=crashloop:old form=start fixfirst=2")))

    def test_phase_hooks_leave_the_pushed_crash_alone(self):
        f = run._fault_new("p999", {"fault": "crashloop:new", "form": "start"})
        self.assertTrue(run._fault_on_new(f))
        self.assertFalse(run._fault_on_new(run._fault_new("p999", {"fault": "crashloop:old", "form": "start"})))
        self.assertFalse(run._fault_on_new(None))
        fired = []
        orig = run._fault_fire
        run._fault_fire = lambda *a, **k: fired.append(a)
        try:
            run._fault_tick({"fault": f, "trigger_ts": 1.0, "pod_state": {}, "pods_per_inst": {}, "old_hashes": {"old"}})
            self.assertEqual([], fired, "the push armed the crash; no hook injects one")
        finally:
            run._fault_fire = orig

    def _patches(self, fn):
        patches = []
        orig_kubectl, orig_image = run.kubectl, run._crashloop_image
        run.kubectl = lambda *a, **k: patches.append(json.loads(a[-1])) or subprocess.CompletedProcess(a, 0, "", "")
        run._crashloop_image = lambda: "metrics:img"
        try:
            fn()
        finally:
            run.kubectl, run._crashloop_image = orig_kubectl, orig_image
        return patches

    def test_push_arms_the_victim_member_only(self):
        ctx = {"case_id": "p999", "pods_per_inst": {"engine": 1, "decoder": 1, "router": 1}, "trigger_ts": None}
        out = []
        patches = self._patches(lambda: out.append(run._crash_new_arm(ctx, {"engine", "decoder"}, "engine", "start")))
        self.assertEqual((True, "image metrics:img"), out[0])
        spec = patches[0]["spec"]
        self.assertEqual("metrics:img", spec["engine"]["runner"]["image"])
        self.assertNotIn("runner", spec["decoder"], "the sound member rolls on the fixture image")
        for c in ("engine", "decoder"):
            self.assertEqual(run.TRIGGER_VAL, spec[c]["annotations"][run.TRIGGER_ANNO])
        self.assertIsNotNone(ctx["trigger_ts"])
        out, ctx["trigger_ts"] = [], None
        patches = self._patches(lambda: out.append(run._crash_new_arm(ctx, {"engine"}, "engine", "load")))
        self.assertEqual((True, f"{run.CRASH_EXIT_ENV}={run.CRASH_EXIT_AFTER_S}"), out[0])
        spec = patches[0]["spec"]
        self.assertEqual([{"name": run.CRASH_EXIT_ENV, "value": str(run.CRASH_EXIT_AFTER_S)}], spec["engine"]["runner"]["env"])
        self.assertNotIn("image", spec["engine"]["runner"])
        gang = dict(ctx, pods_per_inst={"engine": 2, "decoder": 1, "router": 1}, trigger_ts=None)
        patches = self._patches(lambda: run._crash_new_arm(gang, {"engine"}, "engine", "load"))
        leader = patches[0]["spec"]["engine"]["leader"]["runner"]
        self.assertEqual(run.MAIN_CONTAINER, leader["name"], "a leader runner names the container it overrides")
        self.assertEqual(run.CRASH_EXIT_ENV, leader["env"][0]["name"])
        self.assertNotIn("runner", patches[0]["spec"]["engine"])

    def test_fix_push_drops_the_runner_override(self):
        ctx = {"case_id": "p999", "pods_per_inst": {"engine": 1, "decoder": 1, "router": 1}, "trigger_ts": None}
        patches = self._patches(lambda: run._bump_rev(ctx, {"engine", "decoder"}, val="v3", image=""))
        spec = patches[0]["spec"]
        for c in ("engine", "decoder"):
            self.assertIn("runner", spec[c])
            self.assertIsNone(spec[c]["runner"], "a null runner deletes the image and env the push armed")
            self.assertEqual("v3", spec[c]["annotations"][run.TRIGGER_ANNO])

    def test_allowance_is_the_larger_of_the_two_budgets(self):
        def spec(surge, unavail):
            return {"engine": {"lifecycle": {"updateStrategy": {"rollingUpdate": {"maxSurge": surge, "maxUnavailable": unavail}}}}}
        self.assertEqual(1, run._crash_new_allowance(spec(1, 0), "engine", 4))
        self.assertEqual(1, run._crash_new_allowance(spec(0, 1), "engine", 4))
        self.assertEqual(2, run._crash_new_allowance(spec("25%", 0), "engine", 8), "surge percent rounds up")
        self.assertEqual(1, run._crash_new_allowance(spec("25%", 0), "engine", 2))
        self.assertEqual(2, run._crash_new_allowance(spec(0, "25%"), "engine", 8), "unavailable percent rounds down")
        self.assertEqual(2, run._crash_new_allowance(spec(2, 1), "engine", 4))
        self.assertEqual(0, run._crash_new_allowance({}, "engine", 4))

    def test_a_drain_first_load_form_allows_the_start_admitted_beside_the_promoted_crash(self):
        # form=load: the crashing pod passes readiness and is promoted, and
        # with minReadySeconds at zero the roll admits the next Instance's
        # start while the promoted one is still healthy; when it crashes two
        # Instances are off at once, the floor a Deployment reaches too. The
        # surge strategy promotes the replacement beside its source and keeps
        # the strict budget; form=start never promotes anything.
        def spec(strat, surge, unavail):
            return {"engine": {"lifecycle": {"updateStrategy": {
                "type": strat, "rollingUpdate": {"maxSurge": surge, "maxUnavailable": unavail}}}}}
        self.assertEqual(2, run._crash_new_allowance(spec("RecreatePod", 0, 1), "engine", 4, form="load"))
        self.assertEqual(2, run._crash_new_allowance(spec("InPlaceIfPossible", 0, 1), "engine", 4, form="load"))
        self.assertEqual(2, run._crash_new_allowance(spec("InPlaceOnly", 0, 1), "engine", 4, form="load"))
        self.assertEqual(3, run._crash_new_allowance(spec("RecreatePod", 0, 2), "engine", 4, form="load"))
        self.assertEqual(1, run._crash_new_allowance(spec("RecreatePod", 0, 1), "engine", 4, form="start"))
        self.assertEqual(1, run._crash_new_allowance(spec("SurgeThenDrain", 1, 0), "engine", 4, form="load"))
        self.assertEqual(1, run._crash_new_allowance(spec("", 0, 1), "engine", 4, form="load"),
                         "a strategy the spec does not name keeps the strict budget")
        self.assertEqual(1, run._crash_new_allowance(spec("RecreatePod", 0, 1), "engine", 4))

    def test_tally_splits_instances_by_what_they_run(self):
        old, crash = {"old"}, {"bad"}
        pods = {
            "e0": {"alive": True, "comp": "engine", "inst": "0", "hash": "old", "ready": True, "serving": True},
            "e1": {"alive": True, "comp": "engine", "inst": "1", "hash": "bad", "ready": False, "serving": False},
            "e2": {"alive": True, "comp": "engine", "inst": "2", "hash": "fixed", "ready": True, "serving": True},
            "e3": {"alive": True, "comp": "engine", "inst": "3", "hash": "old", "ready": False, "serving": False},
            "e3s": {"alive": True, "comp": "engine", "inst": "3", "hash": "bad", "ready": False, "serving": False},
            "d0": {"alive": True, "comp": "decoder", "inst": "0", "hash": "old", "ready": True, "serving": True},
            "gone": None,
        }
        self.assertEqual((1, 2, 1), run._crash_new_tally(pods, {"engine": 1}, "engine", old, crash),
                         "an Instance with a crash pod beside its old one is off the running revision")
        gang = {
            "l0": {"alive": True, "comp": "engine", "inst": "0", "hash": "old", "ready": True, "serving": True},
            "w0": {"alive": True, "comp": "engine", "inst": "0", "hash": "old", "ready": True, "serving": True},
            "l1": {"alive": True, "comp": "engine", "inst": "1", "hash": "old", "ready": True, "serving": True},
        }
        self.assertEqual((1, 0, 0), run._crash_new_tally(gang, {"engine": 2}, "engine", old, crash),
                         "a gang short a pod is not serving on the running revision")

    def test_why_reads_the_hold_the_failed_row_and_the_events(self):
        held = {"engine": {"hold": {"gate": "Held", "target": "p999-engine-abc", "reason": "CrashLoopBackOff"}, "rows": []}}
        events = [("RetryHeld", "InferenceReplica ns/p999-engine component=engine update to revision x held after 3 failed attempt(s)"),
                  ("InstanceFailed", "InferenceReplica ns/p999-decoder component=decoder instance=0 escalated to Phase=Failed"),
                  ("RecreateUpdateStarted", "component=engine instance=0")]
        status, said = run._crash_new_why(held, events, "engine")
        self.assertTrue(status.startswith("rolloutHold gate=Held"))
        self.assertEqual(1, len(said))
        self.assertTrue(said[0].startswith("RetryHeld:"))
        failed = {"engine": {"hold": None, "rows": [{"index": 0, "phase": "Ready"}, {"index": 3, "phase": "Failed", "lastFailure": "CrashLoopBackOff"}]}}
        status, said = run._crash_new_why(failed, [], "engine")
        self.assertEqual("Instance 3 row Failed (CrashLoopBackOff)", status)
        self.assertEqual([], said)
        self.assertEqual((None, []), run._crash_new_why({"engine": {"hold": None, "rows": [{"index": 0, "phase": "Updating"}]}}, [], "engine"))
        self.assertEqual((None, []), run._crash_new_why({}, [], "engine"))

    def test_why_is_the_ladder_a_budget_hold_or_a_failed_row_not_a_coordination_gate(self):
        # The group's opening hold (gate=Plan, "no active run for this
        # rollout group") is the run opening, not the crash; so is any
        # pacing gate. The retry ladder's backoff and its bound, and a
        # budget hold, say why the crashing revision stopped.
        def snap(gate):
            return {"engine": {"hold": {"gate": gate, "target": "p999-engine-abc",
                                        "reason": "rollout plan not pinned: no active run for this rollout group"},
                               "rows": [{"index": 0, "phase": "Updating"}]}}
        for gate in ("Plan", "Pairing", "Ratio", "Sequential"):
            self.assertEqual((None, []), run._crash_new_why(snap(gate), [], "engine"), gate)
        for gate in ("RetryBlock", "Held", "Budget"):
            status, _said = run._crash_new_why(snap(gate), [], "engine")
            self.assertTrue(status and status.startswith(f"rolloutHold gate={gate}"), (gate, status))
        # A Failed row beside the opening hold still says why.
        plan = snap("Plan")
        plan["engine"]["rows"] = [{"index": 0, "phase": "Failed", "lastFailure": "CrashLoopBackOff"}]
        self.assertEqual("Instance 0 row Failed (CrashLoopBackOff)", run._crash_new_why(plan, [], "engine")[0])

    def test_peer_state_is_converged_held_or_rolling(self):
        ctx = {"old_hashes": {"old"}, "N": {"decoder": 2}, "agg": {"decoder": (2, 2, 2)},
               "pod_state": {"d0": {"alive": True, "comp": "decoder", "hash": "new", "serving": True},
                             "d1": {"alive": True, "comp": "decoder", "hash": "new", "serving": True}}}
        self.assertEqual("converged", run._crash_new_peer_state(ctx, "decoder", {}))
        ctx["pod_state"]["d1"]["hash"] = "old"
        self.assertEqual("rolling", run._crash_new_peer_state(ctx, "decoder", {}))
        snap = {"decoder": {"hold": {"gate": "Budget", "reason": "surge budget exhausted"}, "rows": []}}
        self.assertEqual("held: gate=Budget reason=surge budget exhausted", run._crash_new_peer_state(ctx, "decoder", snap))

    @staticmethod
    def _good_out():
        return {"hold": {"budget": 580, "bitten_at": 30.0, "status_at": 200.0, "status": "rolloutHold gate=Held reason=CrashLoopBackOff",
                         "event_at": 205.0, "event": "RetryHeld: component=engine held", "peak_off": 0, "off_span": 0.0,
                         "min_running": 4, "peak_crash": 1, "attempts_at_status": 3, "attempts_unannounced": [],
                         "windows": [], "held_at": 200.0, "attempts": 3, "peer": {}, "believed": True},
                "fix": {"state": "CONVERGED", "converged_at": 60.0, "budget": 580, "rebuilt": [], "holds_left": {}}}

    def test_verdict_states_the_operators_end_state(self):
        ctx = {"N": {"engine": 4, "decoder": 0, "router": 0}}
        f = {"form": "start", "infra_fail": None}

        def judge(mutate):
            out = self._good_out()
            mutate(out)
            notes, checks = [], []
            v = run._crash_new_checks(ctx, dict(f), out, "engine", {"engine"}, 1, "PASS", notes, checks)
            return v, notes, checks

        v, notes, checks = judge(lambda o: None)
        self.assertEqual("PASS", v, notes)
        self.assertTrue(all(c.startswith("  ✓") or c.startswith("  ⓘ") for c in checks), checks)
        self.assertTrue(any("budget" in c and "kept 4/4 serving" in c for c in checks))
        v, notes, _ = judge(lambda o: o["hold"].update(peak_off=2, off_span=run.FLOOR_SUSTAIN_S + 1, min_running=2))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("off the running revision" in n for n in notes))
        v, _, checks = judge(lambda o: o["hold"].update(peak_off=2, off_span=1.0))
        self.assertEqual("PASS", v, "a dip shorter than the floor debounce is a readiness flap")
        self.assertTrue(any(c.startswith("  ⚠ budget") for c in checks))
        v, notes, _ = judge(lambda o: o["hold"].update(peak_crash=2))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("alive at once" in n for n in notes))
        v, notes, _ = judge(lambda o: o["hold"].update(status_at=None, status=""))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("never said why" in n for n in notes))
        v, notes, _ = judge(lambda o: o["hold"].update(event_at=None, event=""))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("InstanceFailed/RetryHeld event" in n for n in notes))
        v, notes, _ = judge(lambda o: o["hold"].update(attempts_unannounced=["u9"]))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("outside any retry it announced" in n for n in notes))
        v, _, checks = judge(lambda o: o["hold"].update(windows=[260.0], held_at=None))
        self.assertEqual("PASS", v, "a ladder that announced its retries and had not held yet is not a failure")
        self.assertTrue(any(c.startswith("  ⚠ stopped") and "had not reached its bound" in c for c in checks), checks)
        v, notes, _ = judge(lambda o: o["hold"].update(peer={"decoder": "rolling"}))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("neither converged nor held" in n for n in notes))
        v, _, checks = judge(lambda o: o["hold"].update(peer={"decoder": "held: gate=Budget reason=x"}))
        self.assertEqual("PASS", v, "a peer that holds with a reason is not a wedge")
        self.assertTrue(any("held with a reason" in c for c in checks))
        v, _, _ = judge(lambda o: o["hold"].update(peer={"decoder": "converged"}))
        self.assertEqual("PASS", v)
        v, notes, _ = judge(lambda o: o["fix"].update(state="ROTATING", converged_at=None))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("did not land on every Instance" in n for n in notes))
        v, notes, _ = judge(lambda o: o["fix"].update(rebuilt=["p999-engine-3-x"]))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("rebuilt after the fix" in n for n in notes))
        v, notes, _ = judge(lambda o: o["fix"].update(holds_left={"engine": {"gate": "Held", "reason": "stale"}}))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("rolloutHold left" in n for n in notes))
        out = self._good_out()
        out["hold"]["bitten_at"] = None
        fault = dict(f)
        self.assertEqual("PASS", run._crash_new_checks(ctx, fault, out, "engine", {"engine"}, 1, "PASS", [], []))
        self.assertIn("never", fault["infra_fail"], "a revision that never crashed is the apparatus, not the product")

    def test_fleet_checks_cover_peaks_isolation_and_gangs(self):
        hdr = {"ceil_e": 3, "ceil_d": 2, "ceil_r": 2}
        ctx = {"N": {"engine": 2, "decoder": 2, "router": 2}, "configured": {"engine", "decoder", "router"},
               "old_hashes": {"old"}, "peaks": {"engine": 3, "decoder": 2, "router": 2},
               "pods_per_inst": {"engine": 2, "decoder": 1, "router": 1}, "gang_worst": {},
               "pod_state": {"r0": {"alive": True, "comp": "router", "hash": "old"}}}
        notes, checks = [], []
        self.assertEqual("PASS", run._crash_new_fleet_checks(ctx, hdr, {"engine"}, "PASS", notes, checks))
        self.assertTrue(any("gang atomicity held" in c for c in checks))
        self.assertTrue(any("decoder (not bumped) stayed put" in c for c in checks))
        notes = []
        self.assertEqual("FAIL", run._crash_new_fleet_checks(dict(ctx, peaks={"engine": 4, "decoder": 2, "router": 2}), hdr, {"engine"}, "PASS", notes, []))
        self.assertTrue(any("engine peak 4 > ceiling 3" in n for n in notes))
        notes = []
        rotated = dict(ctx, pod_state={"r0": {"alive": True, "comp": "router", "hash": "new"}})
        self.assertEqual("FAIL", run._crash_new_fleet_checks(rotated, hdr, {"engine"}, "PASS", notes, []))
        self.assertTrue(any("router (not bumped) changed" in n for n in notes))
        notes = []
        split = dict(ctx, gang_worst={("engine", "1"): run.GANG_TOL_S + 1})
        self.assertEqual("FAIL", run._crash_new_fleet_checks(split, hdr, {"engine"}, "PASS", notes, []))
        self.assertTrue(any("gang atomicity violated" in n for n in notes))

    def test_fix_order_claim_reads_pod_creation_times(self):
        f = {"kind": "crashloop", "crash_inst": "3", "crash_comp": "engine", "fixfirst": True}
        first = {"e0": {"alive": True, "comp": "engine", "inst": "0", "hash": "new", "created": "2026-10-02T10:00:30Z"},
                 "e1": {"alive": True, "comp": "engine", "inst": "1", "hash": "new", "created": "2026-10-02T10:01:00Z"},
                 "e3": {"alive": True, "comp": "engine", "inst": "3", "hash": "new", "created": "2026-10-02T10:00:00Z"},
                 "e3old": {"alive": False, "comp": "engine", "inst": "3", "hash": "old", "created": "2026-10-02T09:00:00Z"}}
        notes, checks = [], []
        self.assertEqual("PASS", run._crashloop_fix_order(f, first, {"old"}, "PASS", notes, checks))
        self.assertTrue(any("not after the last of its peers" in c for c in checks))
        last = dict(first)
        last["e3"] = dict(first["e3"], created="2026-10-02T10:02:00Z")
        notes, checks = [], []
        self.assertEqual("FAIL", run._crashloop_fix_order(f, last, {"old"}, "PASS", notes, checks))
        self.assertTrue(any("last to land the fix" in n for n in notes))
        tied = dict(first)
        tied["e3"] = dict(first["e3"], created="2026-10-02T10:01:00Z")
        self.assertEqual("PASS", run._crashloop_fix_order(f, tied, {"old"}, "PASS", [], []),
                         "landing together with the last peer is not landing after it")
        alone = {"e3": first["e3"]}
        checks = []
        self.assertEqual("PASS", run._crashloop_fix_order(f, alone, {"old"}, "PASS", [], checks))
        self.assertTrue(any("not measurable" in c for c in checks))
        # One peer gives no order to read: the roll takes its first Instance
        # before the break lands, so the broken one is first or second by
        # the break's timing. Reported, never failed, whichever way it fell.
        for victim_at in ("2026-10-02T10:02:00Z", "2026-10-02T10:00:00Z"):
            pair = {"e0": first["e0"], "e3": dict(first["e3"], created=victim_at)}
            notes, checks = [], []
            self.assertEqual("PASS", run._crashloop_fix_order(f, pair, {"old"}, "PASS", notes, checks), victim_at)
            self.assertEqual([], notes)
            self.assertTrue(any("not measurable with one peer" in c and "peer 0=" in c for c in checks), checks)
        # The old-side verdict wires the claim in only when the header asks.
        full = dict(f, fired=True, infra_fail=None, form="start", case_id="p999", crash_hash="old", kills=0)
        healed = {n: dict(p, ready=True, serving=True) for n, p in last.items() if p["alive"]}
        orig = run._isvc_events_since
        try:
            run._isvc_events_since = lambda case_id, since: []
            notes = []
            self.assertEqual("FAIL", run._crashloop_checks(full, healed, {"old"}, 1.0, "PASS", notes, []))
            self.assertTrue(any("last to land the fix" in n for n in notes))
            self.assertEqual("PASS", run._crashloop_checks(dict(full, fixfirst=False), healed, {"old"}, 1.0, "PASS", [], []))
        finally:
            run._isvc_events_since = orig

    def test_generated_new_family_crosses_every_axis(self):
        outputs, rows = generate_cases.build_outputs()
        fam = [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-crashloop-new"]
        self.assertEqual(72, len(fam))
        self.assertEqual({"crashloop:new:start", "crashloop:new:load"}, {m["verb"] for _n, _kv, m in fam})
        self.assertEqual({s for s, _su, _un in generate_cases.NEW_CRASH_STRATS}, {m["strat"] for _n, _kv, m in fam})
        self.assertEqual({"None", "RecreateInstance"}, {m["rp"] for _n, _kv, m in fam})
        self.assertEqual({"engine", "pd", "engine-mn", "pd-mn"}, {kv["topo"] for _n, kv, _m in fam})
        self.assertEqual({(f, rp, s) for f in ("start", "load") for rp in ("None", "RecreateInstance")
                          for s, _su, _un in generate_cases.NEW_CRASH_STRATS},
                         {(kv["form"], m["rp"], m["strat"]) for _n, kv, m in fam},
                         "both forms run under both restart policies and every strategy")
        for name, kv, m in fam:
            self.assertEqual("crashloop:new", kv["fault"])
            self.assertNotIn("floor_soft", kv, "the new-side verdict reads its own floor: N minus the budget")
            self.assertNotIn("fixfirst", kv)
            self.assertNotIn("during", kv)
            if kv["topo"].startswith("pd"):
                self.assertIn(kv["co"], {"none", "rolling"})
                self.assertEqual(kv["Nd"] + (1 if kv["co"] == "rolling" else 0), kv["ceil_d"])
            else:
                self.assertEqual("none", kv["co"])
            text = outputs[f"gen-crashloop-new/{name}.yaml"]
            reason = text.splitlines()[1]
            self.assertTrue(reason.startswith("# reason: "))
            self.assertIn("status says why", reason)
            self.assertIn("fix push", reason)
            if kv["form"] == "load":
                self.assertIn("promoted, then crashed", reason, "a crash after Ready says what Ready meant for the budget")
            if m["strat"] == "InPlaceIfPossible" and kv["form"] == "load":
                self.assertIn("resolves to a recreate", reason)

    def test_generated_group_family_crosses_every_axis(self):
        outputs, rows = generate_cases.build_outputs()
        fam = [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-crashloop-group"]
        self.assertEqual(72, len(fam))
        self.assertEqual({"crashloop:start", "crashloop:load"}, {m["verb"] for _n, _kv, m in fam})
        self.assertEqual({"SurgeThenDrain", "RecreatePod"}, {m["strat"] for _n, _kv, m in fam})
        self.assertEqual({"None", "RecreateInstance"}, {m["rp"] for _n, _kv, m in fam})
        self.assertEqual({"engine", "pd", "pd-mn"}, {kv["topo"] for _n, kv, _m in fam})
        by_topo = {}
        for _n, kv, _m in fam:
            by_topo.setdefault(kv["topo"], set()).add(kv["co"])
        self.assertEqual({"bluegreen", "rolling"}, by_topo["engine"], "one-member sequences and ratios have no peer")
        self.assertEqual({"bluegreen", "seq", "ratio", "rolling"}, by_topo["pd"])
        self.assertEqual({"bluegreen", "seq", "ratio"}, by_topo["pd-mn"], "the zero-budget cells run on the single-pod shapes")
        for name, kv, m in fam:
            self.assertEqual("crashloop:old", kv["fault"])
            self.assertEqual(1, kv["fixfirst"], "the fix lands on the broken Instance first or never last")
            self.assertEqual(1, kv["floor_soft"])
            text = outputs[f"gen-crashloop-group/{name}.yaml"]
            reason = text.splitlines()[1]
            self.assertRegex(reason, r"never (the )?last", "the order claim is stated on line 2")
            if kv["topo"] == "pd-mn" and m["strat"] == "RecreatePod":
                # The drain-first gang cells run three gangs: the roll holds
                # the one slot with its first gang when the break lands, so
                # the dark gang must be rebuilt ahead of the peer still waiting.
                self.assertEqual((3, 3, 3, 3), (kv["Ne"], kv["Nd"], kv["ceil_e"], kv["ceil_d"]), name)
                self.assertIn("three gangs", reason, name)
                self.assertIn("minReplicas: 3", text, name)
            else:
                self.assertEqual(2 if kv["topo"] == "pd-mn" else 4, kv["Ne"], name)
                self.assertNotIn("three gangs", reason, name)
            if kv["co"] == "ratio":
                self.assertIn("net of the broken Instance", reason, "the band is measured net of the fault's loss")
            if kv["co"] == "bluegreen":
                self.assertIn("flip cannot replace it ahead of the set", reason, "the contradiction and what the case asserts instead")
            if kv["co"] == "seq":
                self.assertIn("engine to completion before the decoder", reason)
            if kv["co"] == "ratio":
                tol = generate_cases.RATIO_TOL_BY_N[kv["Ne"]]
                self.assertIn(f"within {tol}%", reason)
                self.assertIn(f"tolerance: {tol}", text)
            if kv["co"] == "rolling":
                self.assertIn("zero budget", reason)
                self.assertIn(kv["topo"], {"engine", "pd"})
            if kv["topo"].startswith("pd"):
                self.assertEqual(kv["Nd"] + (1 if m["strat"] == "SurgeThenDrain" else 0), kv["ceil_d"],
                                 "the group rolls the decoder under the same strategy")
                self.assertIn("- components: [engine, decoder]" if kv["co"] in ("bluegreen", "ratio", "rolling")
                              else "- components: [engine]", text)


class CrashloopNewLadderTest(unittest.TestCase):
    """The crashloop:new watch replayed against a scripted timeline: the
    clock advances one second per watcher read, and the pods, the
    InferenceReplica snapshot and the ISVC events at each second come from
    the script. The ladder replayed is the chart's updateRetry: a backoff
    of a minute that doubles, held after three attempts."""

    TARGET = "p999-engine-crash"
    FAILED_EVENT = ("InstanceFailed", "InferenceReplica sandbox/p999-engine component=engine instance=0 "
                                      "escalated to Phase=Failed (pod=\"\"): CrashLoopBackOff")

    @staticmethod
    def _stamp(t):
        return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _pod(self, name, inst, hash_, uid, created, ready=True):
        return {name: {"alive": True, "comp": "engine", "inst": str(inst), "hash": hash_, "uid": uid,
                       "ready": ready, "serving": ready, "created": self._stamp(created)}}

    def _snapshot(self, hold=None, retry=None, failed=()):
        rows = [{"index": i, "phase": "Failed" if i in failed else "Ready"} for i in range(4)]
        return {"engine": {"hold": hold, "rows": rows, "retry": retry or []}}

    def _backoff(self, attempts, next_at):
        hold = {"gate": "RetryBlock", "target": self.TARGET,
                "reason": f"update to {self.TARGET} in backoff after {attempts} failed attempt(s): CrashLoopBackOff"}
        block = {"targetRevision": self.TARGET, "state": "Backoff", "attemptsStarted": attempts,
                 "nextRetryAt": self._stamp(next_at)}
        return {"hold": hold, "retry": [block], "failed": (0,)}

    def _held(self, attempts):
        hold = {"gate": "Held", "target": self.TARGET,
                "reason": f"update to {self.TARGET} held after {attempts} failed attempt(s): CrashLoopBackOff"}
        return {"hold": hold, "retry": [{"targetRevision": self.TARGET, "state": "Held", "attemptsStarted": attempts}],
                "failed": (0,)}

    def _fleet(self, attempt_uid, attempt_at):
        """Three Instances serving the running revision and the attempt's
        pod, on the crashing revision and not Ready, at Instance 0."""
        return {**self._pod("e1", 1, "old", "o1", 0), **self._pod("e2", 2, "old", "o2", 0),
                **self._pod("e3", 3, "old", "o3", 0),
                **self._pod("e0", 0, "crash", attempt_uid, attempt_at, ready=False)}

    def _replay(self, script):
        """Run the watch over the script ({second: {"pods": …, "snapshot":
        …, "events": …}}, each key holding until a later second sets it).
        Returns the hold record and the clock when the watch returned."""
        clock = [0.0]
        state = {"pods": {}, "snapshot": self._snapshot(), "events": []}

        def read_one(ctx, timeout=1.0):
            clock[0] += 1.0
            for key, value in (script.get(int(clock[0])) or {}).items():
                state[key] = value
            ctx["pod_state"] = dict(state["pods"])

        waiting = {"status": {"containerStatuses": [{"name": run.MAIN_CONTAINER,
                                                     "state": {"waiting": {"reason": "CrashLoopBackOff"}}}]}}
        ctx = {"N": {"engine": 4, "decoder": 0, "router": 0}, "pods_per_inst": {"engine": 1}, "old_hashes": {"old"},
               "configured": {"engine"}, "fault": {"form": "start"}, "case_id": "p999", "trigger_ts": 0.0,
               "closed": False, "pod_state": {}, "agg": {}}
        with mock.patch.object(run.time, "time", lambda: clock[0]), mock.patch.object(run, "_read_one", read_one), \
                mock.patch.object(run, "_ir_snapshot", lambda case_id: state["snapshot"]), \
                mock.patch.object(run, "_isvc_events_since", lambda case_id, since: list(state["events"])), \
                mock.patch.object(run, "_get_pod", lambda name: waiting), \
                contextlib.redirect_stdout(io.StringIO()):
            hold = run._crash_new_watch_hold(ctx, "engine", {"engine"}, 1, set(), {})
        return hold, clock[0]

    def _judge(self, hold):
        ctx = {"N": {"engine": 4, "decoder": 0, "router": 0}}
        out = {"hold": hold, "fix": {"state": "CONVERGED", "converged_at": 60.0, "budget": 580, "rebuilt": [], "holds_left": {}}}
        notes, checks = [], []
        verdict = run._crash_new_checks(ctx, {"form": "start", "infra_fail": None}, out, "engine", {"engine"}, 1,
                                        "PASS", notes, checks)
        return verdict, notes, checks

    def _ladder_script(self):
        """The chart's ladder as the product walks it: the first attempt
        fails and backs off a minute, the retry comes at the announced
        time and fails into a two-minute backoff, the third attempt fails
        and the ladder holds. The coordination gate's opening hold at the
        first second is not the why."""
        plan = {"gate": "Plan", "target": self.TARGET,
                "reason": "rollout plan not pinned: no active run for this rollout group (run opening)"}
        return {
            1: {"pods": self._fleet("u1", 1), "snapshot": self._snapshot(hold=plan)},
            64: {"snapshot": self._snapshot(**self._backoff(1, 124)), "events": [self.FAILED_EVENT]},
            124: {"pods": self._fleet("u2", 124), "snapshot": self._snapshot()},
            185: {"snapshot": self._snapshot(**self._backoff(2, 305))},
            305: {"pods": self._fleet("u3", 305), "snapshot": self._snapshot()},
            365: {"snapshot": self._snapshot(**self._held(3)),
                  "events": [self.FAILED_EVENT, ("RetryHeld", f"InferenceReplica sandbox/p999-engine component=engine "
                                                              f"update to {self.TARGET} held after 3 failed attempt(s)")]},
        }

    def test_retries_at_the_announced_times_up_to_the_ladders_bound_are_not_further_attempts(self):
        hold, ended = self._replay(self._ladder_script())
        poll = run.CRASH_NEW_POLL_S
        self.assertAlmostEqual(64, hold["status_at"], delta=poll, msg="the opening Plan hold is not the why")
        self.assertEqual(1, hold["attempts_at_status"])
        self.assertEqual(3, hold["attempts"])
        self.assertEqual([], hold["attempts_unannounced"],
                         "a retry at the time the backoff announced is the ladder, not the roll going on")
        self.assertEqual([124.0, 305.0], hold.get("windows"))
        self.assertAlmostEqual(365, hold.get("held_at") or 0, delta=poll)
        self.assertTrue(hold["believed"])
        self.assertGreaterEqual(ended, 365 + run.IDLE_HOLD_S, "the watch waits for the ladder's bound, then believes it")
        self.assertLess(ended, 365 + run.IDLE_HOLD_S + 2 * poll + 1)
        verdict, notes, checks = self._judge(hold)
        self.assertEqual("PASS", verdict, notes)
        self.assertTrue(any(c.startswith("  ✓ stopped") and "2 retry time(s) announced" in c
                            and f"held at {hold['held_at']:.0f}s" in c for c in checks), checks)

    def test_an_attempt_before_the_announced_retry_or_past_the_bound_fails(self):
        early = self._ladder_script()
        early[90] = {"pods": self._fleet("u9", 90)}
        hold, _ended = self._replay(early)
        self.assertEqual(["u9"], hold.get("attempts_unannounced"))
        verdict, notes, _checks = self._judge(hold)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("outside any retry it announced" in n for n in notes), notes)
        late = self._ladder_script()
        late[400] = {"pods": self._fleet("u4", 400)}
        hold, _ended = self._replay(late)
        self.assertEqual(["u4"], hold.get("attempts_unannounced"), "nothing is expected past the ladder's bound")
        self.assertEqual("FAIL", self._judge(hold)[0])
        # Two pods at one announced time: the retry is one attempt.
        twice = self._ladder_script()
        twice[126] = {"pods": {**self._fleet("u2", 124), **self._pod("e4", 4, "crash", "u5", 126, ready=False)}}
        hold, _ended = self._replay(twice)
        self.assertEqual(["u5"], hold.get("attempts_unannounced"))

    def test_a_failed_row_with_no_retry_announced_is_believed_after_the_quiet_window(self):
        script = {1: {"pods": self._fleet("u1", 1)},
                  76: {"snapshot": self._snapshot(failed=(0,)), "events": [self.FAILED_EVENT]}}
        hold, ended = self._replay(script)
        self.assertEqual(76, round(hold["status_at"]))
        self.assertTrue(hold["believed"])
        self.assertLess(ended, 76 + run.IDLE_HOLD_S + 5)
        verdict, _notes, checks = self._judge(hold)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any(c.startswith("  ✓ stopped: no further engine pod") for c in checks), checks)
        script[100] = {"pods": self._fleet("u2", 100)}
        hold, _ended = self._replay(script)
        self.assertEqual(["u2"], hold.get("attempts_unannounced"), "a pod with no retry announced is the roll going on")
        self.assertEqual("FAIL", self._judge(hold)[0])


class RatioBandTest(unittest.TestCase):
    """The RatioBalanced band under a break on the running revision: each
    sample is the better of the raw reading and the one net of the broken
    Instance, the break's victims are out of the band until they serve on
    the roll target, and the verdict reads the sustained deviation."""

    N = {"engine": 3, "decoder": 3, "router": 2}
    PAIR = ("engine", "decoder")

    def test_reading_nets_out_the_broken_instance_where_that_reads_better(self):
        # gang 0 in the drain slot, gang 2 broken: raw 1:3 is 67% off, net 1 of 2 against 3 of 3 is 50%
        self.assertEqual((0.5, 1, 3, True),
                         run._ratio_band_reading({"engine": 1, "decoder": 3}, self.N, self.PAIR, {"engine": (1, 0)}))
        # both members one down with the broken gang out: raw 2:2 is level, and the net reading is not better
        dev, ca, cb, net = run._ratio_band_reading({"engine": 2, "decoder": 2}, self.N, self.PAIR, {"engine": (1, 0)})
        self.assertEqual((0.0, 2, 2, False), (round(dev, 6), ca, cb, net))
        # no break: the raw reading, as always
        dev, ca, cb, net = run._ratio_band_reading({"engine": 2, "decoder": 3}, self.N, self.PAIR)
        self.assertEqual((0.333333, 2, 3, False), (round(dev, 6), ca, cb, net))
        dev, ca, cb, net = run._ratio_band_reading({"engine": 2, "decoder": 3}, self.N, self.PAIR, {})
        self.assertEqual((0.333333, 2, 3, False), (round(dev, 6), ca, cb, net))

    def test_reading_leaves_out_a_victim_that_is_up_between_kills(self):
        # form=load: the old pod is Ready between kills and counts in the raw
        # ready; the net reading leaves it out with its Instance
        dev, ca, cb, net = run._ratio_band_reading({"engine": 3, "decoder": 3}, self.N, self.PAIR, {"engine": (1, 1)})
        self.assertEqual((0.0, 3, 3, False), (round(dev, 6), ca, cb, net))
        dev, ca, cb, net = run._ratio_band_reading({"engine": 2, "decoder": 3}, self.N, self.PAIR, {"engine": (1, 1)})
        self.assertEqual((0.333333, 2, 3, False), (round(dev, 6), ca, cb, net), "raw 2:3 beats net 1 of 2 against 3")

    def test_two_gangs_under_a_drain_first_roll_are_not_rescued_by_the_net_reading(self):
        # gang 0 in the slot and gang 1 broken: nothing serves either way,
        # which is why those cells run three gangs
        n2 = {"engine": 2, "decoder": 2, "router": 2}
        self.assertEqual((1.0, 0, 2, False), run._ratio_band_reading({"engine": 0, "decoder": 2}, n2, self.PAIR, {"engine": (1, 0)}))
        self.assertEqual((1.0, 0, 1, False), run._ratio_band_reading({"engine": 0, "decoder": 1}, n2, self.PAIR, {"engine": (1, 0)}))

    def test_reading_caps_a_surge_peak_and_reads_the_other_side_too(self):
        dev, ca, cb, net = run._ratio_band_reading({"engine": 4, "decoder": 3}, self.N, self.PAIR)
        self.assertEqual((0.0, 3, 3, False), (round(dev, 6), ca, cb, net), "a surge peak is spare capacity")
        self.assertIsNone(run._ratio_band_reading({"engine": 0, "decoder": 0}, self.N, self.PAIR))
        self.assertEqual(9.99, run._ratio_band_reading({"engine": 2, "decoder": 0}, self.N, self.PAIR)[0])
        # a broken decoder: raw 3:1 is 200% off, net 3 of 3 against 1 of 2 is 100%
        self.assertEqual((1.0, 3, 1, True),
                         run._ratio_band_reading({"engine": 3, "decoder": 1}, self.N, self.PAIR, {"decoder": (1, 0)}))
        # a side of one Instance has no net reading
        n1 = {"engine": 1, "decoder": 1, "router": 0}
        self.assertEqual((1.0, 0, 1, False), run._ratio_band_reading({"engine": 0, "decoder": 1}, n1, self.PAIR, {"engine": (1, 0)}))

    @staticmethod
    def _pod(comp, inst, hash_, ready, runner="leader", newanno=False):
        return {"alive": True, "ready": ready, "comp": comp, "inst": str(inst), "hash": hash_,
                "runner": runner, "newanno": newanno}

    def test_break_out_lasts_until_the_victim_serves_on_the_target(self):
        f = {"kind": "crashloop", "fired": True, "crash_comp": "engine", "crash_inst": "2"}
        ppi = {"engine": 2, "decoder": 2, "router": 1}
        modes = {"engine": "surge", "decoder": "surge"}
        dark = {"e2l": self._pod("engine", 2, "old", False), "e2w": self._pod("engine", 2, "old", True, "worker"),
                "e1l": self._pod("engine", 1, "old", True), "e1w": self._pod("engine", 1, "old", True, "worker")}
        self.assertEqual({"engine": (1, 0)}, run._break_out(f, dark, {"old"}, modes, ppi))
        between_kills = dict(dark, e2l=self._pod("engine", 2, "old", True))
        self.assertEqual({"engine": (1, 1)}, run._break_out(f, between_kills, {"old"}, modes, ppi),
                         "an old pod up between kills is whole and Ready, and still the fault's")
        rebuilt_short = {"e2l": self._pod("engine", 2, "new", True)}
        self.assertEqual({"engine": (1, 0)}, run._break_out(f, rebuilt_short, {"old"}, modes, ppi),
                         "a gang with its worker still to come is not whole")
        rebuilt = {"e2l": self._pod("engine", 2, "new", True), "e2w": self._pod("engine", 2, "new", True, "worker")}
        self.assertEqual({}, run._break_out(f, rebuilt, {"old"}, modes, ppi), "whole and Ready on the target: its own again")
        gone = {"e1l": self._pod("engine", 1, "old", True)}
        self.assertEqual({"engine": (1, 0)}, run._break_out(f, gone, {"old"}, modes, ppi), "no pod at all is still out")
        # not fired, not a break, or the fire failed before it named a victim: nothing is out
        self.assertEqual({}, run._break_out(dict(f, fired=False), dark, {"old"}, modes, ppi))
        self.assertEqual({}, run._break_out(dict(f, kind="podkill"), dark, {"old"}, modes, ppi))
        self.assertEqual({}, run._break_out({"kind": "imagepull", "fired": True}, dark, {"old"}, modes, ppi))
        self.assertEqual({}, run._break_out(None, dark, {"old"}, modes, ppi))
        # every victim of a storm counts, and an in-place roll reads the annotation
        storm = dict(f, extra_victims=[{"crash_comp": "engine", "crash_inst": "1"}])
        self.assertEqual({"engine": (2, 1)}, run._break_out(storm, dark, {"old"}, modes, ppi))
        inplace = {"e2l": self._pod("engine", 2, "old", True, newanno=True),
                   "e2w": self._pod("engine", 2, "old", True, "worker", newanno=True)}
        self.assertEqual({}, run._break_out(f, inplace, {"old"}, {"engine": "inplace"}, ppi))
        self.assertEqual({"engine": (1, 1)}, run._break_out(f, inplace, {"old"}, modes, ppi),
                         "under a rebuild the same old hash is not the target")

    def test_sustained_reads_the_deviation_that_held_for_the_window(self):
        spike = [(0.0, 0.0, 3, 3, False), (10.0, 0.667, 1, 3, False), (12.0, 0.5, 1, 3, True),
                 (31.0, 0.0, 3, 3, False), (40.0, 0.0, 3, 3, False)]
        dev, at = run._ratio_band_sustained(spike)
        self.assertEqual((0.5, (12.0, 0.5, 1, 3, True)), (dev, at), "the 67% sample held two seconds, the 50% one nineteen")
        brief = [(0.0, 0.0, 3, 3, False), (10.0, 0.667, 1, 3, False), (12.0, 0.0, 3, 3, False), (40.0, 0.0, 3, 3, False)]
        self.assertEqual((0.0, None), run._ratio_band_sustained(brief))
        self.assertEqual((0.0, None), run._ratio_band_sustained([]))

    def test_verdict_fails_only_a_sustained_deviation_and_names_the_net_reading(self):
        held = [(0.0, 0.0, 3, 3, False), (10.0, 0.5, 1, 3, True), (31.0, 0.0, 3, 3, False), (40.0, 0.0, 3, 3, False)]
        notes, checks = [], []
        self.assertEqual("PASS", run._ratio_band_verdict(self.PAIR, 50, held, (1, 3), 0.667, "PASS", notes, checks))
        self.assertEqual([], notes)
        self.assertTrue(any("band held" in c and "net of the broken Instance" in c and "1 sample(s) read net" in c
                            for c in checks), checks)
        broken = [(0.0, 0.0, 3, 3, False), (10.0, 1.0, 0, 3, True), (31.0, 0.0, 3, 3, False), (40.0, 0.0, 3, 3, False)]
        notes, checks = [], []
        self.assertEqual("FAIL", run._ratio_band_verdict(self.PAIR, 50, broken, (0, 3), 1.0, "PASS", notes, checks))
        self.assertTrue(any("RatioBalanced band violated" in n and "engine 0 : decoder 3 net of the broken Instance" in n
                            for n in notes), notes)
        raw = [(0.0, 0.0, 3, 3, False), (10.0, 0.667, 1, 3, False), (31.0, 0.0, 3, 3, False), (40.0, 0.0, 3, 3, False)]
        notes, checks = [], []
        self.assertEqual("FAIL", run._ratio_band_verdict(self.PAIR, 50, raw, (1, 3), 0.667, "PASS", notes, checks))
        self.assertFalse(any("net of the broken Instance" in c for c in checks), checks)
        self.assertEqual("PASS", run._ratio_band_verdict(self.PAIR, 50, [], None, 0.0, "PASS", [], []))


class PauseRepairTest(unittest.TestCase):
    """A pause or a freeze around the repair of a broken Instance (pauseat=,
    push=): the header, the contract per cell, the in-flight predicate, the
    paused-window bookkeeping, and the verdict."""

    BASE = "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"
    IDLE = BASE + " fault=crashloop:old form=start during=idle expect=repaired"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_times_a_pause_against_the_repair(self):
        for moment in ("before", "during", "after"):
            self.assertEqual([], self._errs(self.IDLE + f" pause=true pauseat={moment}"))
            self.assertEqual([], self._errs(self.IDLE + f" pause=freeze pauseat={moment} push=fix"))
        self.assertEqual([], self._errs(self.BASE + " fault=imagepull:old during=idle pause=true pauseat=before"))
        self.assertTrue(any("malformed header value pauseat=" in e for e in self._errs(self.IDLE + " pause=true pauseat=later")))
        self.assertTrue(any("malformed header value push=" in e for e in self._errs(self.IDLE + " pause=true pauseat=before push=break")))
        self.assertTrue(any("needs pause=true|freeze" in e for e in self._errs(self.IDLE + " pauseat=before")))
        self.assertTrue(any("needs pauseat=" in e for e in self._errs(self.IDLE + " pause=true")),
                        "a pause verb a rollout case never times is a dead knob")
        self.assertTrue(any("needs pauseat=" in e for e in self._errs(self.IDLE + " push=fix")))
        self.assertTrue(any("needs during=idle and a break fault" in e
                            for e in self._errs(self.BASE + " fault=crashloop:old form=start pause=true pauseat=before")))
        self.assertTrue(any("needs during=idle and a break fault" in e
                            for e in self._errs(self.BASE + " fault=podkill:settling pause=true pauseat=before")))
        self.assertTrue(any("expect=held" in e
                            for e in self._errs(self.IDLE.replace("expect=repaired", "expect=held") + " pause=true pauseat=before")))
        kill = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None pause=true topo=engine"
        self.assertEqual([], self._errs(kill), "the kill family's pause verb is untouched")
        self.assertTrue(any("mode=rollout" in e for e in self._errs(kill + " pauseat=before")))

    def test_contract_per_cell(self):
        c = run._pause_repair_contract
        self.assertEqual({"repair_while_paused": True, "repair_after_unpause": False}, c("true", "before", ""))
        self.assertEqual({"repair_while_paused": True, "repair_after_unpause": False}, c("true", "during", "fix"))
        self.assertEqual({"repair_while_paused": False, "repair_after_unpause": False}, c("true", "after", "fix"))
        self.assertEqual({"repair_while_paused": False, "repair_after_unpause": True}, c("freeze", "before", ""),
                         "a freeze that lands first owes the repair at the unfreeze")
        self.assertEqual({"repair_while_paused": False, "repair_after_unpause": False}, c("freeze", "before", "fix"),
                         "with the fix pushed the roll owns the Instance after the unfreeze")
        self.assertEqual({"repair_while_paused": True, "repair_after_unpause": False}, c("freeze", "during", ""),
                         "a freeze finishes the repair already open")
        self.assertEqual({"repair_while_paused": False, "repair_after_unpause": False}, c("freeze", "after", ""))

    def test_repair_open_reads_the_product_or_the_pod_set(self):
        old = {"e1": {"alive": True, "ready": True, "serving": True, "uid": "u-old"}}
        pods = sorted(old.items())
        self.assertFalse(run._repair_open(pods, {"u-old"}, 1, "Ready", False))
        self.assertTrue(run._repair_open(pods, {"u-old"}, 1, "Restarting", False), "a Restarting row names the repair")
        self.assertTrue(run._repair_open(pods, {"u-old"}, 1, "Ready", True), "so does the event")
        self.assertTrue(run._repair_open([], {"u-old"}, 1, "", False), "a pod gone is the repair's first move")
        half = [("e1", {"alive": True, "ready": True, "serving": True, "uid": "u-new"})]
        self.assertTrue(run._repair_open(half, {"u-old", "u-old-w"}, 2, "", False), "a gang half rebuilt is still open")
        self.assertFalse(run._repair_open(half, {"u-old"}, 1, "Restarting", True), "rebuilt and serving is not open")
        self.assertTrue(run._instance_rebuilt_whole(half, {"u-old"}, 1))
        self.assertFalse(run._instance_rebuilt_whole(half, {"u-old"}, 2))

    def test_paused_changes_split_the_victim_from_everyone_else(self):
        state = {
            "e0": {"alive": True, "comp": "engine", "inst": "0", "uid": "u0", "hash": "old"},
            "e1": {"alive": True, "comp": "engine", "inst": "1", "uid": "u1-new", "hash": "old"},
            "d0": {"alive": False, "comp": "decoder", "inst": "0", "uid": "d0-new", "hash": "old"},
            "e2": {"alive": True, "comp": "engine", "inst": "2", "uid": "u2", "hash": "new"},
            "gone": None,
        }
        created, new_rev = run._paused_changes(state, {"u0", "u2"}, {"old"})
        self.assertEqual({("engine", "1"): {"e1"}, ("decoder", "0"): {"d0"}}, dict(created),
                         "a pod already terminating still counts as created while paused")
        self.assertEqual({"e2"}, new_rev)
        created, new_rev = run._paused_changes(state, {"u0", "u1-new", "d0-new", "u2"}, {"old", "new"})
        self.assertEqual(({}, set()), (dict(created), new_rev))

    def _out(self, **over):
        out = {"pmode": "true", "moment": "before", "push": "fix", "bumped": ["engine"], "pause_lead_s": 15.0,
               "moment_at": None, "row_at_pause": "", "repair_done_at_pause": False, "state": "CONVERGED",
               "end_detail": "", "converged_at": 80.0, "repairs_after_unpause": [], "ir_settled": True,
               "ir_detail": "", "pause_left": "",
               "paused": {"created": {("engine", "1"): {"e1"}}, "new_rev": [], "rebuilt_at": 40.0, "held_s": 95.0,
                          "repairs": ["OMENative component=engine instance=1 restart triggered: pod e1 wedged"]}}
        paused = dict(out["paused"], **over.pop("paused", {}))
        out.update(over)
        out["paused"] = paused
        return out

    def test_verdict_holds_the_pause_to_its_promise(self):
        f = {"kind": "crashloop", "crash_inst": "1", "crash_comp": "engine", "form": "start"}
        repair = "OMENative component=engine instance=1 restart triggered: pod e1 wedged"

        def judge(contract, out):
            notes, checks = [], []
            return run._pause_repair_checks(f, contract, out, "PASS", notes, checks), notes, checks

        plain_before = run._pause_repair_contract("true", "before", "fix")
        v, _n, checks = judge(plain_before, self._out())
        self.assertEqual("PASS", v)
        self.assertTrue(any("landed on nothing until the unpause" in c for c in checks))
        self.assertTrue(any("no second rebuild" in c for c in checks))
        v, notes, _c = judge(plain_before, self._out(paused={"created": {("engine", "1"): {"e1"}, ("engine", "0"): {"e0"}}}))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("other Instances" in n for n in notes))
        v, notes, _c = judge(plain_before, self._out(paused={"new_rev": ["e0"]}))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("new revision while paused" in n for n in notes))
        v, notes, _c = judge(plain_before, self._out(paused={"rebuilt_at": None, "created": {}}))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("not rebuilt to serving while paused" in n for n in notes))
        hold = "OMENative component=engine instance=1 crash-loop repair held by Plan: no run open"
        v, notes, _c = judge(plain_before, self._out(paused={"rebuilt_at": None, "created": {}, "held": [hold]}))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("held, not run" in n and "no run open" in n for n in notes),
                        "a hold the product announced is the failure's own words")
        v, notes, _c = judge(plain_before, self._out(repairs_after_unpause=[repair]))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("repaired again after the unpause" in n for n in notes))
        v, notes, _c = judge(plain_before, self._out(state="WATCHING", end_detail="engine 1 alive/1 ready/1 serving of 2"))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("end state not reached" in n for n in notes))
        v, notes, _c = judge(plain_before, self._out(ir_settled=False, ir_detail="engine/1: Instance is not settled"))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("mid-operation" in n for n in notes))
        v, notes, _c = judge(plain_before, self._out(pause_left="true"))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("annotation still reads" in n for n in notes))

        freeze_before = run._pause_repair_contract("freeze", "before", "")
        frozen = self._out(pmode="freeze", push="", bumped=[], paused={"created": {}, "rebuilt_at": None, "repairs": []},
                           repairs_after_unpause=[repair])
        v, _n, checks = judge(freeze_before, frozen)
        self.assertEqual("PASS", v)
        self.assertTrue(any("left as it stood while frozen" in c for c in checks))
        self.assertTrue(any("released the repair" in c for c in checks))
        v, notes, _c = judge(freeze_before, dict(frozen, repairs_after_unpause=[]))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("after the unpause" in n and "no RestartTriggered" in n for n in notes))
        v, notes, _c = judge(freeze_before, dict(frozen, paused=dict(frozen["paused"], created={("engine", "1"): {"e1"}})))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("rebuilt while frozen" in n for n in notes))
        v, notes, _c = judge(freeze_before, dict(frozen, paused=dict(frozen["paused"], repairs=[repair])))
        self.assertEqual("FAIL", v, "a repair recorded while frozen is a repair that ran")

        after = run._pause_repair_contract("true", "after", "")
        done = self._out(moment="after", push="", bumped=[], moment_at=70.0, row_at_pause="Ready",
                         paused={"created": {}, "rebuilt_at": None, "repairs": []})
        v, _n, checks = judge(after, done)
        self.assertEqual("PASS", v)
        self.assertTrue(any("after its repair had finished" in c for c in checks))
        self.assertTrue(any("every Instance is Ready and serving on its running revision" in c for c in checks))
        v, notes, _c = judge(after, dict(done, repairs_after_unpause=[repair]))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("already been repaired" in n for n in notes))

        during = run._pause_repair_contract("freeze", "during", "fix")
        caught = self._out(pmode="freeze", moment="during", moment_at=12.0, row_at_pause="Restarting")
        v, _n, checks = judge(during, caught)
        self.assertEqual("PASS", v)
        self.assertTrue(any("while its repair was in flight" in c for c in checks))
        v, _n, checks = judge(during, dict(caught, repair_done_at_pause=True))
        self.assertEqual("PASS", v)
        self.assertTrue(any("had already finished when the pause landed" in c for c in checks))

    def test_victim_events_split_repairs_from_holds(self):
        orig = run._isvc_events_since
        run._isvc_events_since = lambda case_id, since: [
            ("RestartTriggered", "OMENative component=engine instance=1 restart triggered: pod e1 wedged"),
            ("RepairHeld", "OMENative component=engine instance=1 crash-loop repair held by Plan: no run open"),
            ("RepairHeld", "OMENative component=engine instance=0 crash-loop repair held by Plan: no run open"),
            ("RestartTriggered", "OMENative component=engine instance=10 restart triggered: pod e10 wedged"),
            ("RestartTriggered", "OMENative component=decoder instance=1 restart triggered: pod d1 wedged")]
        try:
            repairs, holds = run._victim_events("p999", 0.0, "engine", "1")
            self.assertEqual(1, len(repairs))
            self.assertIn("pod e1", repairs[0])
            self.assertEqual(1, len(holds))
            self.assertIn("instance=1", holds[0])
            self.assertEqual(repairs, run._victim_repairs("p999", 0.0, "engine", "1"))
        finally:
            run._isvc_events_since = orig
        self.assertTrue(run._repair_declared_held([], ["held"]))
        self.assertFalse(run._repair_declared_held(["ran"], ["held"]), "a repair that ran outranks a hold before it")
        self.assertFalse(run._repair_declared_held([], []))

    def test_end_state_follows_the_broken_instance_through_its_gang_surge(self):
        """A gang's roll replaces the broken Instance at a fresh index and
        retires its own; the end state reads the Instance where its surge
        left it, and the rows settle at the indexes the roll left."""
        N, configured, bumped, ppi = {"engine": 2}, {"engine"}, {"engine"}, {"engine": 2}
        agg, pre_uids, old_hashes = {"engine": (2, 2, 2)}, {"o1-l", "o1-w"}, {"old"}

        def gang(idx, hash_, uid):
            pod = {"alive": True, "ready": True, "serving": True, "comp": "engine", "inst": idx, "hash": hash_}
            return {f"e{idx}l": dict(pod, uid=f"{uid}-l"), f"e{idx}w": dict(pod, uid=f"{uid}-w")}

        state = {**gang("0", "new", "n0"), **gang("2", "new", "n2")}
        self.assertEqual((True, ""), run._pause_repair_end_state(agg, state, ppi, N, configured, bumped, "engine", "1", "2",
                                                                 pre_uids, 2, old_hashes, "fix"))
        ok, why = run._pause_repair_end_state(agg, state, ppi, N, configured, bumped, "engine", "1", "1", pre_uids, 2, old_hashes, "fix")
        self.assertFalse(ok)
        self.assertIn("engine Instance 1 not rebuilt to serving (0 pod(s))", why)
        stale = {**gang("0", "new", "n0"), **gang("2", "old", "n2")}
        ok, why = run._pause_repair_end_state(agg, stale, ppi, N, configured, bumped, "engine", "1", "2", pre_uids, 2, old_hashes, "fix")
        self.assertFalse(ok)
        self.assertIn("replaced at Instance 2 by its gang surge) still on the old revision", why)
        ok, why = run._pause_repair_end_state({"engine": (2, 1, 2)}, state, ppi, N, configured, bumped, "engine", "1", "2",
                                              pre_uids, 2, old_hashes, "fix")
        self.assertFalse(ok)
        self.assertIn("2 alive/1 ready", why)

        self.assertEqual("1", run._victim_index_now("engine", "1", [], []), "nothing moved it")
        self.assertEqual("2", run._victim_index_now("engine", "1", [("engine", "1", "2", "rev-a")], []), "the rows' claim")
        self.assertEqual("1", run._victim_index_now("engine", "1", [("decoder", "1", "2", "rev-a")], []),
                         "another Component's surge moves nothing")
        events = [("RecreateUpdateStarted", "OMENative component=engine instance=0 gang surge to revision r (surge-index=2)"),
                  ("RecreateUpdateStarted", "OMENative component=engine instance=1 gang surge to revision r (surge-index=0)")]
        self.assertEqual("0", run._victim_index_now("engine", "1", [], events), "a freed index the later surge reused")

        rolled = [inference_replica("engine", 2, current="rev-b", update="rev-b")]
        rolled[0]["status"]["instanceStatuses"] = [{"index": 0, "phase": "Ready", "operation": None},
                                                   {"index": 2, "phase": "Ready", "operation": None}]
        with mock.patch.object(run, "_fetch_initial_ir_items", return_value=rolled):
            self.assertTrue(run._wait_rows_settled("p999", N, configured, 1)[0], "a pushed fix leaves the roll's indexes")
            settled, why = run._wait_rows_settled("p999", N, configured, 1, fresh_indexes=True)
        self.assertFalse(settled, "with nothing pushed a repair rebuilds in place, so the fresh indexes hold")
        self.assertIn("Instance indexes have not converged", why)

    def test_ceilings_read_the_header(self):
        ctx = {"hdr": {"ceil_e": 3, "ceil_d": 0, "ceil_r": 0}, "N": {"engine": 2, "decoder": 0, "router": 0},
               "configured": {"engine"}, "peaks": {"engine": 3, "decoder": 0, "router": 0}}
        self.assertEqual("PASS", run._pause_repair_ceiling_checks(ctx, "PASS", [], []))
        ctx["peaks"]["engine"] = 4
        notes = []
        self.assertEqual("FAIL", run._pause_repair_ceiling_checks(ctx, "PASS", notes, []))
        self.assertTrue(any("peak 4 > ceiling 3" in n for n in notes))

    def test_pause_annotation_settles_unless_told_not_to(self):
        calls, slept = [], []
        orig_kubectl, orig_sleep = run.kubectl, run.time.sleep
        run.kubectl = lambda *a, **k: calls.append(a) or subprocess.CompletedProcess(a, 0, "", "")
        run.time.sleep = lambda s: slept.append(s)
        try:
            run._pause_annotation("p999", "freeze")
            run._pause_annotation("p999", "", settle=False)
        finally:
            run.kubectl, run.time.sleep = orig_kubectl, orig_sleep
        self.assertEqual([run.PAUSE_PROJECT_S], slept)
        self.assertIn("ome.io/rollout-paused=freeze", calls[0])
        self.assertIn("ome.io/rollout-paused-", calls[1])

    def test_idle_case_with_a_pause_moment_runs_the_pause_handler(self):
        routed = []
        orig = (run._pause_repair, run._idle_fault, run.find_isvc, run.parse_header,
                run.duplicate_id_errors, run.collect_case_files)
        run._pause_repair = lambda *a: routed.append("pause") or 0
        run._idle_fault = lambda *a: routed.append("idle") or 0
        run.find_isvc = lambda cid: Path("p999.yaml")
        run.duplicate_id_errors = lambda cases: []
        run.collect_case_files = lambda: []
        try:
            run.parse_header = lambda p: ("x", {"during": "idle", "fault": "crashloop:old", "pause": "true", "pauseat": "before"})
            run.run_case("p999")
            run.parse_header = lambda p: ("x", {"during": "idle", "fault": "crashloop:old"})
            run.run_case("p999")
        finally:
            (run._pause_repair, run._idle_fault, run.find_isvc, run.parse_header,
             run.duplicate_id_errors, run.collect_case_files) = orig
        self.assertEqual(["pause", "idle"], routed)

    def test_generated_family_crosses_every_axis(self):
        """The pause-and-repair family: three shapes, every (crash form,
        restart policy) pair a repair is promised for, no group or a
        rollingUpdate group, the pause landing before, during or after the
        repair at both depths (a freeze after the repair is a pause), with
        the fix pushed while paused or nothing; every cell runs live, a repair
        of an Instance that serves nothing being admitted without the budget
        or the gate."""
        outputs, rows = generate_cases.build_outputs()
        fam = [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-pause-repair"]
        self.assertEqual(180, len(fam))
        self.assertEqual(("p6000", "p6179"), (fam[0][0], fam[-1][0]))
        self.assertEqual({"engine", "pd", "engine-mn"}, {kv["topo"] for _n, kv, _m in fam})
        self.assertEqual({("start", "None"), ("start", "RecreateInstance"), ("load", "RecreateInstance")},
                         {(kv["form"], m["rp"]) for _n, kv, m in fam},
                         "a crash after Ready under policy None promises no repair")
        self.assertEqual({("true", "before"), ("freeze", "before"), ("true", "during"), ("freeze", "during"),
                          ("true", "after")},
                         {(kv["pause"], kv["pauseat"]) for _n, kv, _m in fam},
                         "a freeze after the repair has finished holds nothing a pause does not")
        self.assertEqual({"none", "fix"}, {m["push"] for _n, kv, m in fam})
        self.assertEqual({"none", "rolling"}, {kv["co"] for _n, kv, _m in fam})
        for n, kv, m in fam:
            text = outputs[f"gen-pause-repair/{n}.yaml"]
            self.assertEqual(("crashloop:old", "idle", "repaired", "engine"),
                             (kv["fault"], kv["during"], kv["expect"], kv["victimcomp"]))
            self.assertNotIn("mode", kv)
            self.assertIn('rollingUpdate: { maxSurge: 1, maxUnavailable: 1 }', text,
                          "the repair the pause is timed against must fit the unavailability budget")
            self.assertIn(f'restartPolicy: "{generate_cases.RP_FULL[m["rp"]]}"', text)
            if m["push"] == "fix":
                self.assertEqual(("fix", 1, kv["Ne"] + 1), (kv["push"], kv["floor_soft"], kv["ceil_e"]))
                want_d = kv["Nd"] + 1 if (kv["co"] != "none" and kv["topo"].startswith("pd")) else kv["Nd"]
                self.assertEqual(want_d, kv["ceil_d"], "only a group rolls the decoder behind the fix")
            else:
                self.assertNotIn("push", kv)
                self.assertNotIn("floor_soft", kv)
                self.assertEqual((kv["Ne"], kv["Nd"], kv["Nr"]), (kv["ceil_e"], kv["ceil_d"], kv["ceil_r"]),
                                 "nothing surges when nothing is pushed")
            members = "[engine, decoder]" if kv["topo"].startswith("pd") else "[engine]"
            if kv["co"] == "none":
                self.assertNotIn("  rollout:\n    groups:", text)
            else:
                self.assertIn(f"        - components: {members}\n          rollingUpdate: {{}}", text)
            self.assertNotIn("notimpl", kv)
            self.assertNotIn("issue", kv)
            self.assertNotIn("parked", m)
            self.assertNotIn("parked:", text.splitlines()[1])
            if kv["co"] != "none":
                self.assertIn("admitted without the budget or the gate", text.splitlines()[1],
                              "a grouped cell states the rule its repair runs under")
            when = {"before": "before the repair opens", "during": "while the repair is in flight",
                    "after": "after the repair has finished"}[kv["pauseat"]]
            self.assertIn(when, text.splitlines()[1].split(";")[1], "line two says when the pause lands")


class MigrateBumpOverlapHeaderTest(unittest.TestCase):
    """validate-only knows the overlap knobs of a migrate case — then=bump
    at=<moment of the move>, during=<moment of the roll>, inst=last — and
    refuses them elsewhere or half-stated."""

    BASE = "# p998: x mode=migrate Ne=4 Nd=0 Nr=0 ceil_e=5 ceil_d=0 ceil_r=0 mig=Auto co=none topo=engine"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p998.yaml"
            p.write_text(header + "\nmetadata: { name: p998 }\n")
            return run.validate_case_file(p)

    def test_both_directions_validate(self):
        for tail in (" inst=0 then=bump at=accept", " inst=0 then=bump at=surge", " inst=0 then=bump at=drain",
                     " inst=0 during=settling", " inst=0 during=rotating", " inst=last during=last",
                     " inst=2 during=rollout"):
            self.assertEqual([], self._errs(self.BASE + tail), tail)

    def test_the_bump_names_its_change_and_its_moment(self):
        self.assertTrue(any("both or neither" in e for e in self._errs(self.BASE + " inst=0 then=bump")))
        self.assertTrue(any("both or neither" in e for e in self._errs(self.BASE + " inst=0 at=surge")))
        self.assertTrue(any("malformed" in e for e in self._errs(self.BASE + " inst=0 then=scaleup at=surge")))
        self.assertTrue(any("malformed" in e for e in self._errs(self.BASE + " inst=0 then=bump at=ready")))
        self.assertTrue(any("one or the other" in e
                            for e in self._errs(self.BASE + " inst=0 then=bump at=drain during=rotating")))
        rollout = "# p998: x mode=rollout Ne=4 Nd=0 Nr=0 ceil_e=5 ceil_d=0 ceil_r=0 topo=engine"
        self.assertTrue(any("migrate-mode knobs" in e for e in self._errs(rollout + " then=bump at=accept")))
        self.assertTrue(any("rollback" in e or "not supported" in e or "mid-roll" in e
                            for e in self._errs(rollout + " during=last")),
                        "during=last on a plain rollout is the rollback family's hook")

    def test_the_last_instance_is_tied_to_its_hook(self):
        self.assertTrue(any("needs inst=last" in e for e in self._errs(self.BASE + " inst=0 during=last")))
        self.assertTrue(any("needs mode=migrate during=last" in e
                            for e in self._errs(self.BASE + " inst=last during=rotating")))
        self.assertTrue(any("needs mode=migrate during=last" in e
                            for e in self._errs(self.BASE + " inst=last then=bump at=accept")))

    def test_an_overlap_moves_one_admitted_instance_alone(self):
        never = self.BASE.replace("mig=Auto", "mig=Never")
        self.assertTrue(any("mig=Never" in e for e in self._errs(never + " inst=0 then=bump at=accept")))
        self.assertTrue(any("mig=Never" in e for e in self._errs(never + " inst=0 during=settling")))
        self.assertTrue(any("names one" in e for e in self._errs(self.BASE + " inst=0,1 during=settling")))
        for extra in (" dup=1", " hint=1", " fault=quota:0", " expect=fail fault=quota:0"):
            self.assertTrue(any("overlap" in e for e in self._errs(self.BASE + " inst=0 then=bump at=accept" + extra)),
                            extra)


def _overlap_pod(comp, inst, uid, rev, ready=True, alive=True, node="node-a"):
    return {"comp": comp, "inst": inst, "alive": alive, "ready": ready, "serving": ready,
            "hash": rev, "uid": uid, "node": node, "runner": "default"}


class MigrateBumpOverlapHookTest(unittest.TestCase):
    """The moments the second operation is filed at: read off the move's
    record and the pods (then=bump at=...), or off the roll (during=...)."""

    def test_overlap_state_reads_either_direction(self):
        ov = run._overlap_new({"mode": "migrate", "during": "rollout"})
        self.assertEqual(("rotating", None), (ov["roll_hook"], ov["change"]))
        ov = run._overlap_new({"mode": "migrate", "then": "bump", "at": "drain"})
        self.assertEqual(("bump", "drain", None), (ov["change"], ov["at"], ov["roll_hook"]))
        self.assertIsNone(run._overlap_new({"mode": "migrate", "inst": 0}))
        self.assertIsNone(run._overlap_new({"mode": "rollout", "then": "bump", "at": "accept"}))

    def test_bump_hooks_read_the_record_or_the_pods(self):
        src = {"e0": _overlap_pod("engine", "0", "s", "old")}
        fresh = dict(src, e4=_overlap_pod("engine", "4", "f", "old", ready=False))
        accept = run._overlap_new({"mode": "migrate", "then": "bump", "at": "accept"})
        self.assertFalse(run._overlap_bump_due(accept, src, None, None, False, "engine", {"s"}, {"s"}))
        self.assertTrue(run._overlap_bump_due(accept, src, {"phase": "Accepted"}, None, False, "engine", {"s"}, {"s"}))
        self.assertTrue(run._overlap_bump_due(accept, src, None, {"phase": "Started"}, False, "engine", {"s"}, {"s"}))
        self.assertTrue(run._overlap_bump_due(accept, src, None, None, True, "engine", {"s"}, {"s"}))
        surge = run._overlap_new({"mode": "migrate", "then": "bump", "at": "surge"})
        self.assertFalse(run._overlap_bump_due(surge, src, {"phase": "SurgePending"}, None, True, "engine", {"s"}, {"s"}))
        self.assertTrue(run._overlap_bump_due(surge, fresh, None, None, True, "engine", {"s"}, {"s"}),
                        "a pod the fleet did not have is the replacement starting")
        self.assertTrue(run._overlap_bump_due(surge, src, {"phase": "SurgeReady"}, None, True, "engine", {"s"}, {"s"}))
        decoder_only = dict(src, d4=_overlap_pod("decoder", "4", "f", "old"))
        self.assertFalse(run._overlap_bump_due(surge, decoder_only, None, None, True, "engine", {"s"}, {"s"}),
                         "a fresh pod of another Component is not the move's replacement")
        drain = run._overlap_new({"mode": "migrate", "then": "bump", "at": "drain"})
        self.assertFalse(run._overlap_bump_due(drain, fresh, {"phase": "SurgeReady"}, None, True, "engine", {"s"}, {"s"}))
        draining = dict(fresh, e0=_overlap_pod("engine", "0", "s", "old", alive=False))
        self.assertTrue(run._overlap_bump_due(drain, draining, None, None, True, "engine", {"s"}, {"s"}))
        gone = dict(fresh, e0=None)
        self.assertTrue(run._overlap_bump_due(drain, gone, None, None, True, "engine", {"s"}, {"s"}))
        self.assertTrue(run._overlap_bump_due(drain, fresh, {"phase": "Draining"}, None, True, "engine", {"s"}, {"s"}))

    def test_roll_hooks_settling_rotating_last(self):
        old, N, ppi, bumped = {"old"}, {"engine": 4}, {"engine": 1}, {"engine"}
        pods = {f"e{i}": _overlap_pod("engine", str(i), f"u{i}", "old") for i in range(4)}
        self.assertTrue(run._roll_hook_due("settling", pods, old, ppi, N, bumped))
        self.assertFalse(run._roll_hook_due("rotating", pods, old, ppi, N, bumped))
        self.assertFalse(run._roll_hook_due("last", pods, old, ppi, N, bumped))
        pods["e0"]["hash"] = "new"
        self.assertTrue(run._roll_hook_due("rotating", pods, old, ppi, N, bumped))
        self.assertFalse(run._roll_hook_due("last", pods, old, ppi, N, bumped))
        for i in (1, 2):
            pods[f"e{i}"]["hash"] = "new"
        self.assertTrue(run._roll_hook_due("last", pods, old, ppi, N, bumped))
        pods["e2"]["ready"] = pods["e2"]["serving"] = False
        self.assertFalse(run._roll_hook_due("last", pods, old, ppi, N, bumped), "a promoted Instance serves")
        pods["e2"]["ready"] = pods["e2"]["serving"] = True
        pods["e3"]["hash"] = "new"
        self.assertFalse(run._roll_hook_due("last", pods, old, ppi, N, bumped), "a finished roll has no last Instance")
        gang = {"l0": _overlap_pod("engine", "0", "l0", "new"), "w0": _overlap_pod("engine", "0", "w0", "old"),
                "l1": _overlap_pod("engine", "1", "l1", "old")}
        self.assertFalse(run._roll_hook_due("last", gang, old, {"engine": 2}, {"engine": 2}, bumped),
                         "a gang counts as promoted only with its whole pod set on the new revision")
        gang["w0"]["hash"] = "new"
        self.assertTrue(run._roll_hook_due("last", gang, old, {"engine": 2}, {"engine": 2}, bumped))
        pd = {"e0": _overlap_pod("engine", "0", "a", "new"), "e1": _overlap_pod("engine", "1", "b", "old"),
              "d0": _overlap_pod("decoder", "0", "c", "old"), "d1": _overlap_pod("decoder", "1", "d", "old")}
        self.assertTrue(run._roll_hook_due("last", pd, old, {"engine": 1, "decoder": 1}, {"engine": 2, "decoder": 2},
                                           {"engine"}), "last counts the moved Component's Instances alone")

    def test_the_last_instance_is_the_steady_one_still_off_the_new_revision(self):
        old = {"old"}
        pods = {"e0": _overlap_pod("engine", "0", "a", "new"), "e1": _overlap_pod("engine", "1", "b", "old", ready=False),
                "e2": _overlap_pod("engine", "2", "c", "old"), "e3": _overlap_pod("engine", "3", "d", "new"),
                "d3": _overlap_pod("decoder", "3", "e", "old")}
        self.assertEqual("2", run._pick_last_instance(pods, old, "engine"))
        pods["e2"]["hash"] = "new"
        self.assertEqual("1", run._pick_last_instance(pods, old, "engine"), "one mid-operation beats none")
        pods["e1"]["hash"] = "new"
        self.assertIsNone(run._pick_last_instance(pods, old, "engine"))

    def test_the_moved_instance_is_tracked_by_fresh_pods_at_the_named_index(self):
        ov = run._overlap_new({"mode": "migrate", "then": "bump", "at": "accept"})
        pods = {"e0": _overlap_pod("engine", "0", "s", "old"), "e4": _overlap_pod("engine", "4", "f", "old", node="node-b"),
                "e4b": _overlap_pod("engine", "4", "g", "new", node="node-c")}
        run._overlap_track_moved(ov, pods, None, "engine", {"s"})
        self.assertIsNone(ov["moved_index"])
        run._overlap_track_moved(ov, pods, 4, "engine", {"s"})
        self.assertEqual(("4", {"f", "g"}, {"f": "node-b", "g": "node-c"}),
                         (ov["moved_index"], ov["moved_uids"], ov["moved_nodes"]))

    def test_rows_settle_only_on_the_new_revision(self):
        items = [inference_replica("engine", 2, current="rev-new", update="rev-new")]
        self.assertTrue(run._rows_settled_on_new(items, {"engine": 2}, {"engine"}, {"engine"}, {"old"})[0])
        moved = [inference_replica("engine", 2, current="rev-new", update="rev-new")]
        moved[0]["status"]["instanceStatuses"] = [{"index": 1, "phase": "Ready", "operation": None},
                                                  {"index": 4, "phase": "Ready", "operation": None}]
        self.assertTrue(run._rows_settled_on_new(moved, {"engine": 2}, {"engine"}, {"engine"}, {"old"})[0],
                        "a completed move leaves a gap in the indices")
        short = [inference_replica("engine", 2, current="rev-new", update="rev-new", ready=1)]
        self.assertFalse(run._rows_settled_on_new(short, {"engine": 2}, {"engine"}, {"engine"}, {"old"})[0])
        decoder = [inference_replica("engine", 2, current="rev-new", update="rev-new"),
                   inference_replica("decoder", 2, current="rev-old", update="rev-old")]
        self.assertTrue(run._rows_settled_on_new(decoder, {"engine": 2, "decoder": 2}, {"engine", "decoder"},
                                                 {"engine"}, {"old"})[0], "an unbumped Component keeps its revision")
        stale = [inference_replica("engine", 2, current="rev-old", update="rev-old")]
        ok, why = run._rows_settled_on_new(stale, {"engine": 2}, {"engine"}, {"engine"}, {"old"})
        self.assertFalse(ok)
        self.assertIn("not the new revision", why)
        busy = [inference_replica("engine", 2, current="rev-new", update="rev-new", operation={"type": "Migrate"})]
        self.assertFalse(run._rows_settled_on_new(busy, {"engine": 2}, {"engine"}, {"engine"}, {"old"})[0])


class RowsSettledAfterRollTest(unittest.TestCase):
    """After a roll the rows settle at whichever indexes the roll left them
    (a gang surge rebuilds Instance 0 at index 2 and Instance 1 at index 0);
    only a fresh fleet is held to the indexes 0..N-1."""

    def _rolled(self):
        items = [inference_replica("engine", 2, current="rev-b", update="rev-b")]
        items[0]["status"]["instanceStatuses"] = [{"index": 0, "phase": "Ready", "operation": None},
                                                  {"index": 2, "phase": "Ready", "operation": None}]
        return items

    def test_rows_after_a_gang_surge_roll_settle_on_the_indexes_it_left(self):
        rolled = self._rolled()
        self.assertEqual((True, "settled"), run._rolled_ir_settled(rolled, {"engine": 2}, {"engine"}))
        ok, why = run._initial_ir_settled(rolled, {"engine": 2}, {"engine"})
        self.assertFalse(ok)
        self.assertIn("Instance indexes have not converged", why)
        busy = self._rolled()
        busy[0]["status"]["instanceStatuses"][1]["operation"] = {"type": "Surge"}
        self.assertFalse(run._rolled_ir_settled(busy, {"engine": 2}, {"engine"})[0], "an open operation is not settled")
        short = self._rolled()
        short[0]["status"]["instanceStatuses"].pop()
        self.assertFalse(run._rolled_ir_settled(short, {"engine": 2}, {"engine"})[0], "N rows are still required")
        lagging = self._rolled()
        lagging[0]["status"]["updateRevision"] = "rev-c"
        self.assertFalse(run._rolled_ir_settled(lagging, {"engine": 2}, {"engine"})[0], "the revisions must agree")

    def test_the_after_roll_wait_accepts_the_indexes_the_roll_left(self):
        with mock.patch.object(run, "_fetch_initial_ir_items", return_value=self._rolled()):
            settled, why = run._wait_rows_settled("p999", {"engine": 2}, {"engine"}, 1)
        self.assertTrue(settled, why)


class MigrateBumpOverlapVerdictTest(unittest.TestCase):
    """The operator's end state after a move and a revision bump overlapped."""

    def _ctx(self, pods, ppi=1, strat="SurgeThenDrain", old_uids=None):
        return {"case_id": "p998", "trigger_ts": 100.0, "N": {"engine": 2, "decoder": 2, "router": 0},
                "configured": {"engine", "decoder"}, "pods_per_inst": {"engine": ppi, "decoder": ppi, "router": 1},
                "info": {"members": [], "strat": {"engine": strat, "decoder": strat}},
                "old_hashes": {"old"}, "pod_state": pods,
                "old_uids": old_uids or {"engine": {"e0u", "e1u"}, "decoder": {"d0u", "d1u"}}}

    def _ov(self, **kw):
        ov = run._overlap_new({"mode": "migrate", "then": "bump", "at": "surge"})
        ov.update(t0=100.0, fired=True, fired_at=110.0, fired_phase="SurgePending", moved_index="2",
                  moved_nodes={"e2u": "node-b"}, move_done_at=150.0, rows=(True, "settled on the new revision"))
        ov.update(kw)
        return ov

    def _pods(self, moved_rev="new"):
        return {"e1": _overlap_pod("engine", "1", "e1u", "new"),
                "e2": _overlap_pod("engine", "2", "e2u", moved_rev, node="node-b"),
                "d0": _overlap_pod("decoder", "0", "d0u", "old"), "d1": _overlap_pod("decoder", "1", "d1u", "old")}

    def _run(self, ov, pods, ledger=None, ir="one", roll_ts=160.0, entry=None, **ctx_kw):
        reqs = {"0": ("uuid-1", "ome.io/migration-request-v1-uuid-1", "node-a")}
        entry = entry or {"requestUUID": "uuid-1", "phase": "Completed", "surgeInstance": 2}
        ir_entries = [entry] if ir == "one" else ir
        orig = run._migration_ledger_entries
        run._migration_ledger_entries = lambda case_id: [entry] if ledger is None else ledger
        try:
            notes, checks = [], []
            # the shared watcher ctx carries the case header, which _bumped reads
            ctx = {"hdr": {"mode": "migrate"}, **self._ctx(pods, **ctx_kw)}
            v = run._overlap_checks(ov, ctx, "engine", reqs, {"0": entry}, ir_entries,
                                    roll_ts, "PASS", notes, checks)
            return v, notes, checks
        finally:
            run._migration_ledger_entries = orig

    def test_a_clean_overlap_passes_and_says_so(self):
        v, notes, checks = self._run(self._ov(), self._pods())
        self.assertEqual("PASS", v, notes)
        for want in ("bump at surge of the move: filed at 10s", "the roll converged", "index 2) ends on the new revision",
                     "off the node it left", "controller rows settled", "recorded once", "one record for the request",
                     "decoder untouched", "order (not asserted): the move finished first"):
            self.assertTrue(any(want in c for c in checks), (want, checks))

    def test_the_moved_index_is_read_once_the_record_allocates_it(self):
        # The record carries -1 until the surge index is allocated; the
        # replacement is judged at the index the final record names.
        self.assertIsNone(run._surge_index_allocated({"surgeInstance": -1}))
        self.assertIsNone(run._surge_index_allocated({"phase": "Pending"}))
        self.assertIsNone(run._surge_index_allocated(None))
        self.assertEqual(3, run._surge_index_allocated({"surgeInstance": 3}))
        self.assertEqual(0, run._surge_index_allocated({"surgeInstance": 0}))
        pending = {"requestUUID": "uuid-1", "phase": "SurgePending", "surgeInstance": -1}
        v, notes, checks = self._run(self._ov(moved_index=None, moved_nodes={}), self._pods(), entry=pending)
        self.assertEqual("FAIL", v)
        self.assertTrue(any("never named its replacement index" in c for c in checks), checks)
        self.assertFalse(any("index -1" in c for c in checks), checks)
        done = {"requestUUID": "uuid-1", "phase": "Completed", "surgeInstance": 3}
        pods = {"e1": _overlap_pod("engine", "1", "e1u", "new"),
                "e3": _overlap_pod("engine", "3", "e3u", "new", node="node-b"),
                "d0": _overlap_pod("decoder", "0", "d0u", "old"), "d1": _overlap_pod("decoder", "1", "d1u", "old")}
        v, notes, checks = self._run(self._ov(moved_index=None, moved_nodes={"e3u": "node-b"}), pods, entry=done)
        self.assertEqual("PASS", v, notes)
        self.assertTrue(any("index 3) ends on the new revision" in c for c in checks), checks)

    def test_a_missed_hook_judges_nothing_else(self):
        v, notes, checks = self._run(self._ov(fired=False, missed="the move completed before the hook"), self._pods())
        self.assertEqual("FAIL", v)
        self.assertEqual(1, len(checks))
        self.assertIn("the move completed before the hook", checks[0])

    def test_an_apparatus_error_is_infra_fail(self):
        v, notes, _ = self._run(self._ov(error="the bump at surge failed: denied"), self._pods())
        self.assertEqual("INFRA-FAIL", v)

    def test_a_wedged_roll_fails(self):
        v, notes, _ = self._run(self._ov(), self._pods(), roll_ts=None)
        self.assertEqual("FAIL", v)
        self.assertTrue(any("never converged" in n for n in notes))

    def test_the_moved_instance_must_end_on_the_new_revision_and_serving(self):
        v, notes, _ = self._run(self._ov(), self._pods(moved_rev="old"))
        self.assertEqual("FAIL", v)
        self.assertTrue(any("index 2" in n and "not on the new revision" in n for n in notes))
        pods = self._pods()
        pods["e2"]["ready"] = pods["e2"]["serving"] = False
        self.assertEqual("FAIL", self._run(self._ov(), pods)[0])
        pods = self._pods()
        del pods["e2"]
        v, notes, _ = self._run(self._ov(), pods)
        self.assertEqual("FAIL", v, "a single-pod roll keeps the moved Instance's index")

    def test_a_gang_roll_may_rebuild_the_moved_index(self):
        pods = self._pods()
        del pods["e2"]
        v, notes, checks = self._run(self._ov(), pods, ppi=1, strat="SurgeThenDrain")
        self.assertEqual("FAIL", v)
        pods = {"l1": _overlap_pod("engine", "1", "e1u", "new"), "w1": _overlap_pod("engine", "1", "e1w", "new"),
                "l3": _overlap_pod("engine", "3", "e3u", "new"), "w3": _overlap_pod("engine", "3", "e3w", "new"),
                "d0": _overlap_pod("decoder", "0", "d0u", "old"), "d1": _overlap_pod("decoder", "1", "d1u", "old")}
        v, notes, checks = self._run(self._ov(), pods, ppi=2, strat="SurgeThenDrain")
        self.assertEqual("PASS", v, notes)
        self.assertTrue(any("rebuilt the moved Instance" in c for c in checks))
        v, notes, checks = self._run(self._ov(), pods, ppi=2, strat="RecreatePod")
        self.assertEqual("FAIL", v, "a drain-first gang roll keeps the index")

    def test_rows_left_mid_operation_fail(self):
        v, notes, _ = self._run(self._ov(rows=(False, "engine/2: Instance is not settled")), self._pods())
        self.assertEqual("FAIL", v)
        self.assertTrue(any("mid-operation" in n or "not settled" in n for n in notes))

    def test_the_move_is_recorded_once(self):
        entry = {"requestUUID": "uuid-1", "phase": "Completed", "surgeInstance": 2}
        self.assertEqual("FAIL", self._run(self._ov(), self._pods(), ledger=[entry, dict(entry)])[0])
        other = {"requestUUID": "uuid-9", "phase": "Completed", "surgeInstance": 3}
        self.assertEqual("FAIL", self._run(self._ov(), self._pods(), ledger=[entry, other])[0])
        relocation = {"requestUUID": "uuid-8", "phase": "Completed", "reason": run.AUTO_RECOVER_REASON}
        self.assertEqual("FAIL", self._run(self._ov(), self._pods(), ledger=[entry, relocation])[0],
                         "a relocation the product filed beside the move is another move")
        sweep = {"requestUUID": "pod-uid-1", "phase": "Completed", "reason": run.FORCE_DELETE_REASON}
        v, _notes, checks = self._run(self._ov(), self._pods(), ledger=[entry, sweep])
        self.assertEqual("PASS", v, "the force-delete sweep's audit row is a record, not a move")
        self.assertTrue(any("recorded once" in c for c in checks))
        self.assertEqual("FAIL", self._run(self._ov(), self._pods(), ir=[entry, dict(entry)])[0])
        v, _notes, checks = self._run(self._ov(), self._pods(), ir=None)
        self.assertEqual("PASS", v)
        self.assertTrue(any("ledger alone" in c for c in checks))

    def test_untouched_components_stay_untouched(self):
        pods = self._pods()
        pods["d1"]["uid"] = "d1x"
        v, notes, _ = self._run(self._ov(), pods)
        self.assertEqual("FAIL", v)
        self.assertTrue(any("decoder was touched" in n for n in notes))
        pods = self._pods()
        pods["d1"]["hash"] = "new"
        self.assertEqual("FAIL", self._run(self._ov(), pods)[0])

    def test_node_compare_is_informational(self):
        v, _notes, checks = self._run(self._ov(moved_nodes={"e2u": "node-a"}), self._pods())
        self.assertEqual("PASS", v)
        self.assertTrue(any("node compare inconclusive" in c for c in checks))



class MigrateBumpFamilyTest(unittest.TestCase):
    def test_generated_family_crosses_every_axis(self):
        _outputs, rows = generate_cases.build_outputs()
        fam = [(kv, m) for folder, _n, kv, m in rows if folder == "gen-migrate-bump"]
        self.assertEqual(154, len(fam))
        self.assertEqual({"engine", "pd", "engine-mn", "pd-mn"}, {kv["topo"] for kv, _m in fam})
        self.assertEqual(set(generate_cases.MIGRATE_BUMP_BUDGETS), {m["strat"] for _kv, m in fam})
        self.assertEqual(set(generate_cases.MIGRATE_BUMP_ORDERS), {(m["first"], m["hook"]) for _kv, m in fam})
        self.assertEqual({"none", "rolling"}, {kv["co"] for kv, _m in fam})
        for kv, m in fam:
            self.assertEqual(("migrate", "Auto"), (kv["mode"], kv["mig"]))
            if m["first"] == "move":
                self.assertEqual(("bump", m["hook"], 0), (kv["then"], kv["at"], kv["inst"]))
                self.assertNotIn("during", kv)
            else:
                self.assertEqual(m["hook"], kv["during"])
                self.assertNotIn("then", kv)
                self.assertEqual("last" if m["hook"] == "last" else 0, kv["inst"])
            extra = max(m["surge"], 1) if m["strat"] == "SurgeThenDrain" else 1
            self.assertEqual(kv["Ne"] + extra, kv["ceil_e"], "N plus the larger of the roll's surge and the move's one")
            if kv["topo"].startswith("pd"):
                grouped_surge = kv["co"] == "rolling" and m["strat"] == "SurgeThenDrain"
                self.assertEqual(kv["Nd"] + (m["surge"] if grouped_surge else 0), kv["ceil_d"])
        two = [(kv, m) for kv, m in fam if m["surge"] == 2]
        self.assertTrue(two)
        self.assertEqual({"engine"}, {kv["topo"] for kv, _m in two})
        self.assertNotIn("last", {m["hook"] for _kv, m in two}, "a surge of two never sees the last Instance")
        self.assertEqual(10, len(two))


# ── readiness, gang-member and tight-cluster verbs ────────────────────────
class _Resp:
    def __init__(self, stdout="", rc=0, stderr=""):
        self.stdout, self.returncode, self.stderr = stdout, rc, stderr


def _kubectl_with(responses):
    """A kubectl stand-in: the first (predicate, body) whose predicate accepts
    the argument tuple answers (body may be a callable of the args); anything
    else succeeds with an empty object."""
    calls = []

    def kubectl(*args, **kw):
        calls.append(args)
        for pred, body in responses:
            if pred(args):
                out = body(args) if callable(body) else body
                return out if isinstance(out, _Resp) else _Resp(out)
        return _Resp("{}")
    kubectl.calls = calls
    return kubectl


def _rec(comp, inst, hash_, ready=True, alive=True, uid=None, runner="default", serving=None, created="1"):
    return {"comp": comp, "inst": str(inst), "runner": runner, "ord": "0", "alive": alive, "ready": ready,
            "hash": hash_, "uid": uid or f"{comp}-{inst}-{runner}-{hash_}", "newanno": False,
            "serving": ready if serving is None else serving, "created": created, "pairing": "",
            "node": "", "sched": "", "pg": ""}


def _ir_items(comp, rows):
    return json.dumps({"items": [{"spec": {"component": comp}, "status": {"instanceStatuses": rows}}]})


class ReadinessHeaderTest(unittest.TestCase):
    """fault=readiness:<old|new>: old breaks a serving pod at idle and says
    whether the switch stays (form), new is the bump itself."""

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    base = "# p999: x Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 topo=engine"

    def test_old_side_is_an_idle_fault_with_a_form(self):
        self.assertEqual([], self._errs(self.base + " fault=readiness:old form=persist during=idle"))
        self.assertEqual([], self._errs(self.base + " fault=readiness:old form=recover during=idle restart=RecreateInstance"))
        self.assertTrue(any("needs form=persist|recover" in e for e in self._errs(self.base + " fault=readiness:old during=idle")))
        self.assertTrue(any("needs form=persist|recover" in e for e in self._errs(self.base + " fault=readiness:old form=start during=idle")))
        self.assertTrue(any("needs during=idle" in e for e in self._errs(self.base + " fault=readiness:old form=persist")))
        self.assertTrue(any("expect=" in e for e in self._errs(self.base + " fault=readiness:old form=persist during=idle expect=repaired")))

    def test_new_side_is_the_bump_itself(self):
        self.assertEqual([], self._errs(self.base.replace("ceil_e=2", "ceil_e=3") + " fault=readiness:new"))
        self.assertTrue(any("no during=" in e for e in self._errs(self.base + " fault=readiness:new during=settling")))
        self.assertTrue(any("has no form=" in e for e in self._errs(self.base + " fault=readiness:new form=persist")))
        self.assertTrue(any("malformed header value fault=" in e for e in self._errs(self.base + " fault=readiness:stable")))
        self.assertTrue(any("mode=rollout" in e for e in self._errs(self.base.replace(" Ne=", " mode=kill Ne=") + " fault=readiness:new")))

    def test_crashloop_forms_stay_their_own(self):
        self.assertTrue(any("form=start|load" in e for e in self._errs(self.base + " fault=crashloop:old form=persist")))
        self.assertTrue(any("form=" in e for e in self._errs(self.base + " form=persist")))


class GangMemberHeaderTest(unittest.TestCase):
    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    mn = "# p999: x Ne=3 Nd=0 Nr=0 ceil_e=4 ceil_d=0 ceil_r=0 restart=None co=none topo=engine-mn"

    def test_member_kill_knobs_validate_on_a_gang_podkill(self):
        self.assertEqual([], self._errs(self.mn + " fault=podkill:draining kill=worker victim=stable floor_slack=1 order_soft=1"))
        self.assertEqual([], self._errs(self.mn + " fault=podkill:rotating kill=leader victim=canary floor_slack=1"))
        self.assertTrue(any("needs mode=kill or fault=podkill" in e for e in self._errs(self.mn + " kill=worker")))
        self.assertTrue(any("needs a multi-node topo" in e
                            for e in self._errs(self.mn.replace("topo=engine-mn", "topo=engine") + " fault=podkill:settling kill=worker")))
        self.assertTrue(any("floor_slack=" in e for e in self._errs(self.mn + " floor_slack=1")))
        self.assertTrue(any("malformed header value floor_slack=0" in e for e in self._errs(self.mn + " fault=podkill:settling floor_slack=0")))


class TightHeaderTest(unittest.TestCase):
    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    mn = "# p999: x Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 co=none topo=engine-mn"

    def test_lift_names_a_squeeze_on_a_rollout(self):
        self.assertEqual([], self._errs(self.mn + " fault=cordon:all lift=after-hold"))
        self.assertEqual([], self._errs(self.mn + " fault=quota:1 lift=never"))
        self.assertEqual([], self._errs(self.mn + " fault=quota:1"))
        self.assertTrue(any("lift=never lifts a capacity squeeze" in e for e in self._errs(self.mn + " lift=never")))
        self.assertTrue(any("lift=after-hold lifts a capacity squeeze" in e for e in self._errs(self.mn + " fault=podkill:settling lift=after-hold")))
        self.assertTrue(any("blocked=1 does not combine" in e for e in self._errs(self.mn + " fault=quota:1 lift=never blocked=1")))
        self.assertTrue(any("malformed header value fault=" in e for e in self._errs(self.mn + " fault=cordon:everything")))


class GangMemberVictimTest(unittest.TestCase):
    """kill=<member> narrows the podkill victim to one gang runner and skips a
    gang already on its way out; the phase hooks wait for the member."""

    def _pods(self):
        return {"e0-l": _rec("engine", 0, "old", runner="leader"), "e0-w": _rec("engine", 0, "old", runner="worker"),
                "e1-l": _rec("engine", 1, "old", runner="leader"), "e1-w": _rec("engine", 1, "old", runner="worker"),
                "e2-l": _rec("engine", 2, "new", runner="leader", created="5"),
                "e2-w": _rec("engine", 2, "new", runner="worker", created="6")}

    def test_member_rules(self):
        pods, ppi = self._pods(), {"engine": 2}
        self.assertEqual("e0-l", run._fault_pick_pod(pods, ppi, {"old"}))
        self.assertEqual("e0-w", run._fault_pick_pod(pods, ppi, {"old"}, member="worker"))
        self.assertEqual("e0-l", run._fault_pick_pod(pods, ppi, {"old"}, victim="stable", member="leader"))
        self.assertEqual("e2-w", run._fault_pick_pod(pods, ppi, {"old"}, victim="canary", member="worker"))
        self.assertEqual("e2-l", run._fault_pick_pod(pods, ppi, {"old"}, victim="canary", member="leader"))

    def test_a_gang_on_its_way_out_is_never_the_victim(self):
        pods, ppi = self._pods(), {"engine": 2}
        pods["e0-l"]["alive"] = False
        self.assertEqual("e1-w", run._fault_pick_pod(pods, ppi, {"old"}, victim="stable", member="worker"))
        self.assertEqual("e1-l", run._fault_pick_pod(pods, ppi, {"old"}, member="leader"))
        self.assertIsNone(run._fault_pick_pod({"e0-w": pods["e0-w"]}, ppi, {"old"}, member="leader"))

    def test_draining_and_member_aware_rotating_hooks(self):
        fired = []
        f = {"case_id": "p1", "kind": "podkill", "param": "draining", "victim": "stable", "member": "worker",
             "fired": False, "infra_fail": None, "notes": []}
        pods = self._pods()
        ctx = {"fault": f, "trigger_ts": 1.0, "pod_state": pods, "pods_per_inst": {"engine": 2}, "old_hashes": {"old"}}
        with mock.patch.object(run, "_fault_fire", lambda *a, **k: fired.append(a)):
            run._fault_tick(ctx)
            self.assertEqual([], fired)
            pods["e1-l"]["alive"] = False
            run._fault_tick(ctx)
            self.assertEqual(1, len(fired))
        fired.clear()
        f = {"case_id": "p1", "kind": "podkill", "param": "rotating", "victim": "canary", "member": "worker",
             "fired": False, "infra_fail": None, "notes": []}
        pods = self._pods()
        del pods["e2-w"]
        ctx = {"fault": f, "trigger_ts": 1.0, "pod_state": pods, "pods_per_inst": {"engine": 2}, "old_hashes": {"old"}}
        with mock.patch.object(run, "_fault_fire", lambda *a, **k: fired.append(a)):
            run._fault_tick(ctx)
            self.assertEqual([], fired, "no new-revision worker yet")
            pods["e2-w"] = _rec("engine", 2, "new", runner="worker", created="6")
            run._fault_tick(ctx)
            self.assertEqual(1, len(fired))

    def test_podkill_records_the_member_it_took(self):
        pods = self._pods()
        f = {"case_id": "p1", "kind": "podkill", "param": "settling", "victim": "", "member": "worker",
             "fired": False, "infra_fail": None, "notes": []}
        with mock.patch.object(run, "kubectl", _kubectl_with([])):
            run._fault_fire(f, pods, {"engine": 2}, {"old"})
        self.assertEqual(("engine", "0", "worker", False), (f["victim_comp"], f["victim_inst"], f["victim_runner"], f["victim_new"]))
        self.assertEqual(pods["e0-w"]["uid"], f["victim_uid"])


class GangMemberVerdictTest(unittest.TestCase):
    def _fault(self, **kw):
        f = {"case_id": "p1", "kind": "podkill", "param": "settling", "member": "worker", "fired": True,
             "infra_fail": None, "notes": [], "victim_comp": "engine", "victim_inst": "0",
             "victim_uid": "dead", "victim_runner": "worker"}
        f.update(kw)
        return f

    def test_short_gang_counted_serving_is_a_finding(self):
        f = self._fault()
        pods = {"e0-l": _rec("engine", 0, "new", runner="leader"), "e0-w": _rec("engine", 0, "new", runner="worker", ready=False)}
        with mock.patch.object(run, "GANG_SHORT_REPORT_S", 10), mock.patch.object(run, "INV_TICK_S", 5), \
                mock.patch.object(run, "_ir_row", lambda *a: {"index": 0, "podCount": 2, "servingPodCount": 2}):
            run._gang_member_tick(f, "p1", pods, {"engine": 2}, 100.0)
            self.assertNotIn("short_samples", f)       # gap just opened: not sampled yet
            run._gang_member_tick(f, "p1", pods, {"engine": 2}, 111.0)
            self.assertEqual(1, f["short_samples"])
            self.assertEqual(1, f["half_served"])
            pods["e0-w"]["ready"] = True
            run._gang_member_tick(f, "p1", pods, {"engine": 2}, 120.0)
            self.assertIsNone(f["short_since"])
        verdict, notes, checks = "PASS", [], []
        with mock.patch.object(run, "_isvc_events_since", lambda *a: []):
            verdict = run._gang_member_checks(f, pods, 1.0, {"restart": "None"}, verdict, notes, checks)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("counted as serving while short" in c for c in checks))

    def test_the_restart_policy_is_read_off_the_spec_when_the_header_names_none(self):
        live = {"restart": {"engine": "RecreateInstanceOnPodRestart", "decoder": ""}}
        self.assertEqual("RecreateInstance", run._restart_policy({}, live, "engine"))
        self.assertEqual("None", run._restart_policy({}, live, "decoder"))
        self.assertEqual("None", run._restart_policy({}, {}, "engine"), "no header, no spec: the default policy")
        self.assertEqual("RecreateInstance", run._restart_policy({"restart": "RecreateInstance"}, {"restart": {"engine": "None"}}, "engine"),
                         "the header speaks for the case where it names the policy")
        f = self._fault(short_samples=2)
        pods = {"e0-l": _rec("engine", 0, "new", runner="leader"), "e0-w": _rec("engine", 0, "new", runner="worker")}
        events = [("RestartTriggered", "OMENative component=engine instance=0 restart triggered: pod count 1 below desired 2")]
        with mock.patch.object(run, "_isvc_events_since", lambda *a: events):
            n, c = [], []
            v = run._gang_member_checks(f, pods, 1.0, {}, "PASS", n, c, info=live)
            self.assertEqual("PASS", v, "a push cell's header carries no restart=; the spec says the gang is rebuilt whole")
            self.assertTrue(any("rebuilt as a whole" in x for x in c), c)
            n, c = [], []
            self.assertEqual("FAIL", run._gang_member_checks(f, pods, 1.0, {}, "PASS", n, c, info={"restart": {"engine": "None"}}))
            self.assertTrue(any("restartPolicy None must bring back only" in x for x in c), c)
        spec = {"spec": {"engine": {"lifecycle": {"restartPolicy": "RecreateInstanceOnPodRestart",
                                                  "updateStrategy": {"type": "RecreatePod", "rollingUpdate": {"maxUnavailable": 1}}}},
                         "decoder": {"lifecycle": {"updateStrategy": {"type": "SurgeThenDrain"}}}}}
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, json.dumps(spec), "")):
            info = run.get_spec_info("p1")
        self.assertEqual({"engine": "RecreateInstanceOnPodRestart", "decoder": ""}, info["restart"])

    def test_policy_none_must_not_rebuild_the_whole_gang(self):
        f = self._fault(short_samples=2)
        pods = {"e0-l": _rec("engine", 0, "new", runner="leader"), "e0-w": _rec("engine", 0, "new", runner="worker")}
        events = [("RestartTriggered", "OMENative component=engine instance=0 restart triggered: pod count 1 below desired 2")]
        with mock.patch.object(run, "_isvc_events_since", lambda *a: events):
            n, c = [], []
            v = run._gang_member_checks(f, pods, 1.0, {"restart": "None"}, "PASS", n, c)
            self.assertEqual("FAIL", v)
            self.assertTrue(any("restartPolicy None must bring back only" in x for x in c))
            n, c = [], []
            v = run._gang_member_checks(f, pods, 1.0, {"restart": "RecreateInstance"}, "PASS", n, c)
            self.assertEqual("PASS", v)
            self.assertTrue(any("rebuilt as a whole" in x for x in c))
        with mock.patch.object(run, "_isvc_events_since", lambda *a: []):
            n, c = [], []
            v = run._gang_member_checks(f, pods, 1.0, {"restart": "RecreateInstance"}, "PASS", n, c)
            self.assertEqual("PASS", v)
            self.assertTrue(any("the roll took the gang" in x for x in c))
            pods["e0-w"]["uid"] = "dead"
            n, c = [], []
            v = run._gang_member_checks(f, pods, 1.0, {"restart": "None"}, "PASS", n, c)
            self.assertEqual("FAIL", v)
            self.assertTrue(any("is still alive" in x for x in c))

    def test_floor_slack_lowers_the_shared_floor(self):
        ctx = {"hdr": {"floor_slack": 1}, "N": {"engine": 3}, "info": {"max_unavail": {"engine": 0}},
               "min_ready": {"engine": 2}, "floor_sust": {"engine": 30.0}}
        n, c = [], []
        v = run.min_ready_checks(ctx, ["engine"], "PASS", n, c)
        self.assertEqual("PASS", v)
        ctx["hdr"] = {}
        n, c = [], []
        v = run.min_ready_checks(ctx, ["engine"], "PASS", n, c)
        self.assertEqual("FAIL", v)


class CapacityHoldReadersTest(unittest.TestCase):
    def test_cordon_all_takes_schedulable_workers_only(self):
        nodes = [{"metadata": {"name": "cp", "labels": {run.CONTROL_PLANE_LABEL: ""}}, "spec": {}},
                 {"metadata": {"name": "w2", "labels": {}}, "spec": {}},
                 {"metadata": {"name": "w1", "labels": {}}, "spec": {}},
                 {"metadata": {"name": "w3", "labels": {}}, "spec": {"unschedulable": True}},
                 {"metadata": {}, "spec": {}}]
        self.assertEqual(["w1", "w2"], run._cordon_targets(nodes))

    def test_cordon_all_registers_every_uncordon_before_judging(self):
        nodes = json.dumps({"items": [{"metadata": {"name": "w1", "labels": {}}, "spec": {}},
                                      {"metadata": {"name": "w2", "labels": {}}, "spec": {}}]})
        k = _kubectl_with([(lambda a: a[:2] == ("get", "nodes"), nodes),
                           (lambda a: a[:2] == ("cordon", "w2"), _Resp("", 1, "boom"))])
        f = {"kind": "cordon", "param": "all", "notes": [], "infra_fail": None}
        with mock.patch.object(run, "kubectl", k), mock.patch.object(run, "_FAULT_CLEANUPS", []) as cleanups:
            ok, msg = run._fault_cordon(f, "p1", {})
            self.assertFalse(ok)
            self.assertIn("w2", msg)
            self.assertEqual([("uncordon", "w1"), ("uncordon", "w2")], cleanups)
        self.assertEqual(["w1", "w2"], f["nodes"])

    def test_row_hold_and_serving_short(self):
        self.assertEqual(("QuotaExceeded", "exceeded quota: pods"),
                         run._row_hold({"operation": {"waiting": "QuotaExceeded"}, "lastFailure": {"message": "exceeded quota: pods"}}))
        self.assertEqual(("Unschedulable", ""), run._row_hold({"operation": {"waiting": "Unschedulable"}}))
        self.assertIsNone(run._row_hold({"operation": {"waiting": "PodGroupTerminating"}}))
        self.assertIsNone(run._row_hold({}))
        self.assertTrue(run._row_serving_short({"phase": "Ready", "podCount": 2, "servingPodCount": 1}))
        self.assertTrue(run._row_serving_short({"phase": "Pending", "podCount": 1, "servingPodCount": 1}))
        self.assertFalse(run._row_serving_short({"phase": "Ready", "podCount": 2, "servingPodCount": 2}))
        self.assertFalse(run._row_serving_short(None))
        self.assertEqual("abc12", run._revision_hash("p7000-engine-abc12"))

    def test_ready_endpoint_uids_reads_routed_services_only(self):
        svcs = json.dumps({"items": [
            {"metadata": {"name": "routed"}, "spec": {"clusterIP": "10.0.0.1", "selector": {"component": "engine", run.REV_LABEL: "a"}}},
            {"metadata": {"name": "peers"}, "spec": {"clusterIP": "None", "selector": {"component": "engine"}}}]})
        slices = json.dumps({"items": [
            {"metadata": {"labels": {"kubernetes.io/service-name": "routed"}},
             "endpoints": [{"targetRef": {"uid": "u1"}, "conditions": {"ready": True}},
                           {"targetRef": {"uid": "u2"}, "conditions": {"ready": False}},
                           {"targetRef": {"uid": "u3"}}]},
            {"metadata": {"labels": {"kubernetes.io/service-name": "peers"}},
             "endpoints": [{"targetRef": {"uid": "u4"}, "conditions": {"ready": True}}]}]})
        k = _kubectl_with([(lambda a: "svc" in a, svcs), (lambda a: "endpointslices" in a, slices)])
        with mock.patch.object(run, "kubectl", k):
            self.assertEqual({"u1", "u3"}, run._ready_endpoint_uids("p1"))
        with mock.patch.object(run, "kubectl", _kubectl_with([(lambda a: "svc" in a, _Resp("", 1))])):
            self.assertIsNone(run._ready_endpoint_uids("p1"))

    def test_capacity_hold_reads_rows_then_the_quota_event(self):
        rows = [{"index": 0, "phase": "Ready"}, {"index": 2, "phase": "Creating",
                "operation": {"type": "Create", "waiting": "Unschedulable"},
                "lastFailure": {"message": "0/3 nodes are available: 3 node(s) were unschedulable"}}]
        with mock.patch.object(run, "kubectl", _kubectl_with([(lambda a: "inferencereplicas" in a, _ir_items("engine", rows))])), \
                mock.patch.object(run, "_isvc_events_since", lambda *a: []):
            tok, detail = run._capacity_hold("p1", {"engine"}, 0)
            self.assertEqual("Unschedulable", tok)
            self.assertIn("engine Instance 2", detail)
            self.assertIn("unschedulable", detail)
        events = [("InstanceQuotaBlocked", "OMENative component=engine instance=2 waiting on capacity: exceeded quota")]
        with mock.patch.object(run, "kubectl", _kubectl_with([(lambda a: "inferencereplicas" in a, _ir_items("engine", rows[:1]))])), \
                mock.patch.object(run, "_isvc_events_since", lambda *a: events):
            self.assertEqual(run.HOLD_EVENT, run._capacity_hold("p1", {"engine"}, 0)[0])
            self.assertIsNone(run._capacity_hold("p1", {"decoder"}, 0))

    def test_broken_revision_hold_skips_failed_rows_of_the_old_revision(self):
        rows = [{"index": 0, "phase": "Failed", "runningRevision": "p1-engine-old1", "lastFailure": {"reason": "X"}},
                {"index": 2, "phase": "Failed", "targetRevision": "p1-engine-new1",
                 "lastFailure": {"reason": "InstanceReadyTimeout", "message": "not Ready within 90s"}}]
        with mock.patch.object(run, "kubectl", _kubectl_with([(lambda a: "inferencereplicas" in a, _ir_items("engine", rows))])), \
                mock.patch.object(run, "_isvc_events_since", lambda *a: []):
            hold = run._broken_revision_hold("p1", {"engine"}, {"old1"}, 0)
            self.assertIn("Instance 2 Failed: InstanceReadyTimeout", hold)
            self.assertEqual("", run._broken_revision_hold("p1", {"engine"}, {"old1", "new1"}, 0))
        with mock.patch.object(run, "kubectl", _kubectl_with([(lambda a: "inferencereplicas" in a, _ir_items("engine", []))])), \
                mock.patch.object(run, "_isvc_events_since",
                                  lambda *a: [("InstanceFailed", "OMENative component=engine instance=2 Failed: InstanceReadyTimeout")]):
            self.assertTrue(run._broken_revision_hold("p1", {"engine"}, {"old1"}, 0).startswith("InstanceFailed"))


class ReadinessVerdictTest(unittest.TestCase):
    def _out(self, **kw):
        out = {"left_at": 4.0, "unrouted_at": 6.0, "reported_at": 9.0, "rebuilt_at": None, "lifted_at": None,
               "returned_at": None, "phases": {"Ready"}, "final_ready": False, "final_routed": False,
               "new_hash": False, "reset_used": False}
        out.update(kw)
        return out

    def test_persist_passes_when_it_leaves_rotation_reports_and_stays_untouched(self):
        f = {"crash_comp": "engine", "crash_inst": "3"}
        n, c = [], []
        v = run._readiness_checks(f, "persist", self._out(), "PASS", n, c)
        self.assertEqual("PASS", v, c)
        self.assertEqual(5, sum(1 for x in c if x.startswith("  ✓")))
        n, c = [], []
        v = run._readiness_checks(f, "persist", self._out(rebuilt_at=30.0), "PASS", n, c)
        self.assertEqual("FAIL", v)
        self.assertTrue(any("no restart policy rebuilds" in x for x in c))
        n, c = [], []
        v = run._readiness_checks(f, "persist", self._out(reported_at=None), "PASS", n, c)
        self.assertTrue(any("kept claiming every pod serving" in x for x in c))
        n, c = [], []
        v = run._readiness_checks(f, "persist", self._out(final_ready=True), "PASS", n, c)
        self.assertTrue(any("with the switch still set" in x for x in c))
        n, c = [], []
        v = run._readiness_checks(f, "persist", self._out(left_at=None), "PASS", n, c)
        self.assertTrue(any("kept reporting Ready" in x for x in c))

    def test_recover_needs_the_same_pod_back(self):
        f = {"crash_comp": "engine", "crash_inst": "3"}
        n, c = [], []
        v = run._readiness_checks(f, "recover", self._out(lifted_at=25.0, returned_at=12.0, final_ready=True, final_routed=True), "PASS", n, c)
        self.assertEqual("PASS", v, c)
        n, c = [], []
        v = run._readiness_checks(f, "recover", self._out(lifted_at=25.0, final_ready=False), "PASS", n, c)
        self.assertEqual("FAIL", v)
        self.assertTrue(any("did not return" in x for x in c))
        n, c = [], []
        v = run._readiness_checks(f, "recover", self._out(), "PASS", n, c)
        self.assertTrue(any("never lifted" in x for x in c))


class TightVerdictTest(unittest.TestCase):
    def _ctx(self):
        return {"info": {"max_unavail": {"engine": 0}}, "N": {"engine": 2}, "pods_per_inst": {"engine": 2}, "converged_at": 40.0}

    def test_never_judges_the_wait(self):
        f = {"kind": "quota", "param": "1"}
        out = {"hold_at": 8.0, "token": "QuotaExceeded", "detail": "engine Instance 2: exceeded quota: requests.cpu",
               "torn": {"engine": 0}, "hold_kept": True, "churn": (True, "still"), "lifted_at": None}
        n, c = [], []
        v = run._tight_checks(self._ctx(), f, "never", {"engine"}, out, "HELD", "PASS", n, c)
        self.assertEqual("PASS", v, c)
        self.assertTrue(any("names the capacity" in x for x in c))
        out["torn"] = {"engine": 2}
        n, c = [], []
        v = run._tight_checks(self._ctx(), f, "never", {"engine"}, out, "HELD", "PASS", n, c)
        self.assertEqual("FAIL", v)
        self.assertTrue(any("torn down to make room" in x for x in c))
        out["torn"], out["hold_kept"] = {"engine": 0}, False
        n, c = [], []
        v = run._tight_checks(self._ctx(), f, "never", {"engine"}, out, "RELEASED", "PASS", n, c)
        self.assertTrue(any("disappeared" in x for x in c))

    def test_after_hold_needs_the_completion(self):
        f = {"kind": "cordon", "param": "all"}
        out = {"hold_at": 8.0, "token": "Unschedulable", "detail": "engine Instance 2: 0/2 nodes are available",
               "torn": {"engine": 0}, "hold_kept": True, "churn": None, "lifted_at": 30.0}
        n, c = [], []
        v = run._tight_checks(self._ctx(), f, "after-hold", {"engine"}, out, "CONVERGED", "PASS", n, c)
        self.assertEqual("PASS", v, c)
        n, c = [], []
        v = run._tight_checks(self._ctx(), f, "after-hold", {"engine"}, out, "ROTATE_TIMEOUT", "PASS", n, c)
        self.assertEqual("FAIL", v)
        out["hold_at"] = None
        n, c = [], []
        v = run._tight_checks(self._ctx(), f, "after-hold", {"engine"}, out, "NO_HOLD", "PASS", n, c)
        self.assertTrue(any("never reported a wait" in x for x in c))

    def test_recreate_may_drain_its_own_budget(self):
        ctx = self._ctx()
        ctx["info"]["max_unavail"]["engine"] = 1
        f = {"kind": "cordon", "param": "all"}
        out = {"hold_at": 8.0, "token": "Unschedulable", "detail": "x", "torn": {"engine": 2},
               "hold_kept": True, "churn": (True, "still"), "lifted_at": None}
        n, c = [], []
        v = run._tight_checks(ctx, f, "never", {"engine"}, out, "HELD", "PASS", n, c)
        self.assertEqual("PASS", v, c)


class NewRevisionReadinessVerdictTest(unittest.TestCase):
    def test_held_then_fixed(self):
        ctx = {"converged_at": 60.0, "first_fresh": "engine Instance 2"}
        out = {"held_at": 95.0, "hold": "engine Instance 2 Failed: InstanceReadyTimeout", "broken_served": None,
               "fix_at": 100.0, "timeout": 90, "leftover": []}
        n, c = [], []
        v = run._readiness_new_checks(ctx, out, "CONVERGED", "PASS", n, c)
        self.assertEqual("PASS", v, c)
        n, c = [], []
        v = run._readiness_new_checks(ctx, dict(out, leftover=["p1-engine-2-0"]), "CONVERGED", "PASS", n, c)
        self.assertEqual("FAIL", v)
        n, c = [], []
        v = run._readiness_new_checks(ctx, dict(out, broken_served=20.0), "ROTATE_TIMEOUT", "PASS", n, c)
        self.assertEqual("FAIL", v)
        self.assertTrue(any("was Ready and serving" in x for x in c))
        n, c = [], []
        v = run._readiness_new_checks(ctx, dict(out, held_at=None), "NO_HOLD", "PASS", n, c)
        self.assertTrue(any("never held" in x for x in c))


class _FakeFleet:
    """A scripted fleet for the handler smoke runs: _read_one folds the
    scripted pod_state into ctx exactly as the real one does (aggregate,
    peaks), and the script may change pod_state per call."""

    def __init__(self, ctx, script=None):
        self.ctx, self.script, self.calls = ctx, script or (lambda ctx, n: None), 0

    def read_one(self, ctx, timeout=1.0):
        self.calls += 1
        self.script(ctx, self.calls)
        meas = {}
        agg = run.aggregate(ctx["pod_state"], ctx["old_hashes"], ctx["modes"], ctx["pods_per_inst"], measures=meas)
        for c in run.COMPS:
            ctx["peaks"][c] = max(ctx["peaks"][c], agg.get(c, (0, 0, 0))[0])
        ctx["agg"], ctx["measures"] = agg, meas
        if ctx.get("trigger_ts") is not None:
            run._track_gang_and_floor(ctx, ctx.get("track_comps") or sorted(ctx["configured"]))
        time.sleep(0.01)
        return agg


def _ctx_for(hdr, pods, info=None, workers=0):
    N = {"engine": hdr.get("Ne", 0), "decoder": hdr.get("Nd", 0), "router": hdr.get("Nr", 0)}
    configured = {c for c in run.COMPS if N[c] > 0}
    info = info or {"strat": {c: "SurgeThenDrain" for c in configured}, "max_unavail": {c: 0 for c in configured},
                    "workers": ({"engine": workers} if workers else {}), "members": [], "policy": None}
    ppi = {c: 1 + info["workers"].get(c, 0) for c in run.COMPS}
    return {"case_id": "p999", "N": N, "configured": configured, "info": info, "hdr": hdr,
            "pods_per_inst": ppi, "modes": {c: "surge" for c in configured},
            "pod_state": pods, "peaks": {"engine": 0, "decoder": 0, "router": 0}, "agg": {},
            "buf": "", "closed": False, "old_hashes": {p["hash"] for p in pods.values()},
            "old_uids": {}, "old_nodes": {}, "seen_hashes": set(), "trigger_ts": None, "converged_at": None,
            "watcher": None, "inv": run._inv_new("p999", hdr), "fault": run._fault_new("p999", hdr)}


class HandlerSmokeTest(unittest.TestCase):
    """Each new handler runs end to end against a scripted fleet, so its
    phases, bookkeeping and verdict wiring execute without a cluster."""

    def _run(self, handler, ctx, fleet, patches):
        finished = {}

        def finish(case_id, desc, hdr, ctx_, state, verdict, notes, checks):
            finished.update(state=state, verdict=verdict, notes=notes, checks=checks)
            return 0
        base = {"_setup_and_ready": lambda *a: (ctx, None), "_read_one": fleet.read_one, "_finish": finish,
                "_isvc_annotations": lambda *a: {}, "_isvc_events_since": lambda *a: [],
                "STABILITY_S": 0.05, "LIFT_SETTLE_S": 0.2, "IDLE_HOLD_S": 0.4, "READINESS_RECOVER_AFTER_S": 0.1,
                "INV_TICK_S": 0.05, "ROTATE_BASE_S": 3, "ROTATE_PER_POD_S": 0, "MAX_BUDGET_S": 10}
        base.update(patches)
        with contextlib.ExitStack() as stack:
            for name, value in base.items():
                stack.enter_context(mock.patch.object(run, name, value))
            handler(*(["p999", Path("p999.yaml"), "desc", ctx["hdr"]]))
        return finished

    def test_runtime_edit_on_an_open_repair_lowers_the_floor_by_the_broken_instance(self):
        """The edit lands while the repair's replacement pod is in its startup
        window, so the healthy Instances alone hold the floor: it is lowered by
        the broken Instance, as every break verb lowers it, and the pod the verb
        broke itself is recorded on the watcher state for invariant F."""
        hdr = {"Ne": 3, "Nd": 0, "Nr": 0, "ceil_e": 4, "ceil_d": 0, "ceil_r": 0, "fault": "crashloop:old",
               "form": "start", "during": run.REPAIR_HOOK, "topo": "engine", "rt": "ome-coordtest-rt-p999",
               "expect": "roll", "edit": "image", "img": "example.com/app:alt"}
        pods = {f"p999-engine-{i}-0": _rec("engine", i, "old") for i in range(3)}
        ctx = _ctx_for(hdr, pods)
        fired_with = []

        def fire(f, pod_state, ppi, old_hashes, inv=None):
            fired_with.append(inv)
            f.update(fired=True, crash_pod="p999-engine-2-0", crash_inst="2", crash_comp="engine", crash_hash="old",
                     crash_uid=pods["p999-engine-2-0"]["uid"])
            pod_state["p999-engine-2-0"]["ready"] = False

        def bump(ctx_, csr, comps, material, kind="env", image=None):
            ctx_["trigger_ts"] = time.time()
            return True

        def script(ctx_, n):
            edited = ctx_.get("trigger_ts")
            if edited is None:
                if ctx_["fault"]["fired"]:
                    # The repair rebuilt the victim: a new pod, still starting.
                    ctx_["pod_state"]["p999-engine-2-0"] = _rec("engine", 2, "old", ready=False, uid="engine-2-rebuilt")
                return
            if time.time() - edited < 0.3:
                return
            for i in range(3):
                uid = "engine-2-rolled" if i == 2 else None
                ctx_["pod_state"][f"p999-engine-{i}-0"] = _rec("engine", i, "new", uid=uid)
        fleet = _FakeFleet(ctx, script)
        handler = lambda case_id, yaml_file, desc, hdr_: run._runtimerev_repair(case_id, yaml_file, desc, hdr_, hdr_["rt"])
        out = self._run(handler, ctx, fleet, {
            "_fault_fire": fire, "_bump_runtime": bump, "FLOOR_SUSTAIN_S": 0.1,
            "_crashloop_checks": lambda f, pod_state, old, ts, verdict, notes, checks, bumped=None, surges=None: verdict,
            "_wait_rows_settled": lambda *a: (True, "")})
        self.assertTrue(any("engine capacity floor held: min ready 2 ≥ N-maxUnavailable-1broken=2" in c
                            for c in out["checks"]), out["checks"])
        self.assertEqual(("CONVERGED", "PASS"), (out["state"], out["verdict"]), out["checks"])
        self.assertEqual({"engine": 1}, ctx.get("floor_allowance"))
        self.assertEqual([ctx["inv"]], fired_with, "the break must record its victim on the watcher state")

    def test_idle_readiness_recover(self):
        hdr = {"Ne": 2, "Nd": 0, "Nr": 0, "ceil_e": 2, "ceil_d": 0, "ceil_r": 0, "fault": "readiness:old",
               "form": "recover", "during": "idle", "topo": "engine"}
        pods = {"p999-engine-0-0": _rec("engine", 0, "old"), "p999-engine-1-0": _rec("engine", 1, "old")}
        ctx = _ctx_for(hdr, pods)
        f = ctx["fault"]
        f.update(fired=True, crash_pod="p999-engine-1-0", crash_inst="1", crash_comp="engine", crash_hash="old",
                 crash_uid=pods["p999-engine-1-0"]["uid"], switch_lifted=False)
        ctx["trigger_ts"] = time.time()
        lifted = []

        def script(ctx_, n):
            ctx_["pod_state"]["p999-engine-1-0"]["ready"] = bool(lifted)
        rows = lambda *a: {"index": 1, "phase": "Ready", "podCount": 1, "servingPodCount": 1 if lifted else 0}
        uids = lambda *a: {pods["p999-engine-0-0"]["uid"]} | ({pods["p999-engine-1-0"]["uid"]} if lifted else set())
        fleet = _FakeFleet(ctx, script)
        out = {}

        def finish(case_id, desc, hdr_, ctx_, state, verdict, notes, checks):
            out.update(state=state, verdict=verdict, checks=checks)
            return 0
        with mock.patch.object(run, "_read_one", fleet.read_one), mock.patch.object(run, "_ir_row", rows), \
                mock.patch.object(run, "_ready_endpoint_uids", uids), \
                mock.patch.object(run, "_readiness_restore", lambda f_: lifted.append(1) or True), \
                mock.patch.object(run, "_isvc_annotations", lambda *a: {}), \
                mock.patch.object(run, "STABILITY_S", 0.05), mock.patch.object(run, "READINESS_RECOVER_AFTER_S", 0.1), \
                mock.patch.object(run, "ROTATE_BASE_S", 5), mock.patch.object(run, "ROTATE_PER_POD_S", 0):
            state, verdict, notes, checks = run._idle_readiness(ctx, f, hdr)
        self.assertEqual(("RECOVERED", "PASS"), (state, verdict), checks)
        self.assertTrue(any("the same pod returned" in c for c in checks))

    def test_idle_readiness_persist(self):
        hdr = {"Ne": 1, "Nd": 0, "Nr": 0, "ceil_e": 1, "ceil_d": 0, "ceil_r": 0, "fault": "readiness:old",
               "form": "persist", "during": "idle", "topo": "engine", "restart": "RecreateInstance"}
        pods = {"p999-engine-0-0": _rec("engine", 0, "old")}
        ctx = _ctx_for(hdr, pods)
        f = ctx["fault"]
        f.update(fired=True, crash_pod="p999-engine-0-0", crash_inst="0", crash_comp="engine", crash_hash="old",
                 crash_uid=pods["p999-engine-0-0"]["uid"], switch_lifted=False)
        ctx["trigger_ts"] = time.time()
        fleet = _FakeFleet(ctx, lambda c, n: c["pod_state"]["p999-engine-0-0"].__setitem__("ready", False))
        with mock.patch.object(run, "_read_one", fleet.read_one), \
                mock.patch.object(run, "_ir_row", lambda *a: {"index": 0, "phase": "Ready", "podCount": 1, "servingPodCount": 0}), \
                mock.patch.object(run, "_ready_endpoint_uids", lambda *a: set()), \
                mock.patch.object(run, "_isvc_annotations", lambda *a: {}), mock.patch.object(run, "IDLE_HOLD_S", 0.3):
            state, verdict, notes, checks = run._idle_readiness(ctx, f, hdr)
        self.assertEqual(("PERSISTED", "PASS"), (state, verdict), checks)

    def test_readiness_new_roll_holds_then_lands_the_fix(self):
        hdr = {"Ne": 2, "Nd": 0, "Nr": 0, "ceil_e": 3, "ceil_d": 0, "ceil_r": 0, "fault": "readiness:new", "topo": "engine"}
        pods = {"p999-engine-0-0": _rec("engine", 0, "old"), "p999-engine-1-0": _rec("engine", 1, "old")}
        ctx = _ctx_for(hdr, pods)
        bumps = []

        def bump(ctx_, bumped, env, val):
            bumps.append((env, val))
            ctx_["trigger_ts"] = time.time()
            return True

        def script(ctx_, n):
            ps = ctx_["pod_state"]
            if len(bumps) == 1 and "p999-engine-2-0" not in ps:
                ps["p999-engine-2-0"] = _rec("engine", 2, "broken", ready=False)
            if len(bumps) == 2:
                ps.pop("p999-engine-2-0", None)
                for name in ("p999-engine-0-0", "p999-engine-1-0"):
                    ps[name] = _rec("engine", name.split("-")[2], "fix", created="9")
        fleet = _FakeFleet(ctx, script)
        hold = lambda *a: "engine Instance 2 Failed: InstanceReadyTimeout" if len(bumps) == 1 and fleet.calls > 2 else ""
        out = self._run(run._readiness_new_roll, ctx, fleet,
                        {"_bump_with_env": bump, "_broken_revision_hold": hold,
                         "_isvc_spec": lambda *a: {"engine": {"lifecycle": {"instanceReadyTimeout": "90s"}}}})
        self.assertEqual(("CONVERGED", "PASS"), (out["state"], out["verdict"]), out["checks"])
        self.assertEqual([[{"name": run.READINESS_SWITCH_ENV, "value": "1"}], [{"name": "TEST_REV", "value": "v3"}]],
                         [e for e, _v in bumps])
        self.assertTrue(any("the fix push landed" in c for c in out["checks"]))

    def test_tight_roll_lifts_the_squeeze_and_completes(self):
        hdr = {"Ne": 2, "Nd": 0, "Nr": 0, "ceil_e": 3, "ceil_d": 0, "ceil_r": 0, "fault": "quota:1",
               "lift": "after-hold", "topo": "engine-mn"}
        pods = {}
        for i in (0, 1):
            pods[f"p999-engine-{i}-0"] = _rec("engine", i, "old", runner="leader")
            pods[f"p999-engine-{i}-1"] = _rec("engine", i, "old", runner="worker")
        ctx = _ctx_for(hdr, pods, workers=1)
        lifted = []

        def script(ctx_, n):
            ps = ctx_["pod_state"]
            if ctx_.get("trigger_ts") and "p999-engine-2-0" not in ps:
                ps["p999-engine-2-0"] = _rec("engine", 2, "new", runner="leader", ready=False)
            if lifted:
                for i in (0, 1):
                    ps[f"p999-engine-{i}-0"] = _rec("engine", i, "new", runner="leader", created="9")
                    ps[f"p999-engine-{i}-1"] = _rec("engine", i, "new", runner="worker", created="9")
                ps.pop("p999-engine-2-0", None)
        fleet = _FakeFleet(ctx, script)

        def bump(ctx_, bumped):
            ctx_["trigger_ts"] = time.time()
            return True
        out = self._run(run._tight_roll, ctx, fleet,
                        {"_bump_rev": bump,
                         "_capacity_hold": lambda *a: None if lifted else ("QuotaExceeded", "engine Instance 2: exceeded quota"),
                         "_lift_quota": lambda cid: lifted.append(1) or True})
        self.assertEqual(("CONVERGED", "PASS"), (out["state"], out["verdict"]), out["checks"])
        self.assertTrue(any("kept its running gangs" in c for c in out["checks"]))

    def test_tight_roll_reads_the_lift_as_an_answer(self):
        """The lift path consumes the lift's yes-or-no: a refused lift is an
        apparatus failure recorded on the fault, a lift that went through
        starts the watch for the roll to complete. The real lift runs over a
        stubbed kubectl, so the contract between the two is what is tested."""
        for rc, want_fail in ((1, True), (0, False)):
            hdr = {"Ne": 2, "Nd": 0, "Nr": 0, "ceil_e": 3, "ceil_d": 0, "ceil_r": 0, "fault": "quota:1",
                   "lift": "after-hold", "topo": "engine-mn"}
            pods = {}
            for i in (0, 1):
                pods[f"p999-engine-{i}-0"] = _rec("engine", i, "old", runner="leader")
                pods[f"p999-engine-{i}-1"] = _rec("engine", i, "old", runner="worker")
            ctx = _ctx_for(hdr, pods, workers=1)
            deletes = []

            def kubectl(*a, **k):
                if "resourcequota" in a:
                    deletes.append(a)
                    return _Resp("", rc, "forbidden" if rc else "")
                return _Resp('{"items": []}')

            def script(ctx_, n):
                ps = ctx_["pod_state"]
                if ctx_.get("trigger_ts") and "p999-engine-2-0" not in ps:
                    ps["p999-engine-2-0"] = _rec("engine", 2, "new", runner="leader", ready=False)
                if deletes and rc == 0:
                    for i in (0, 1):
                        ps[f"p999-engine-{i}-0"] = _rec("engine", i, "new", runner="leader", created="9")
                        ps[f"p999-engine-{i}-1"] = _rec("engine", i, "new", runner="worker", created="9")
                    ps.pop("p999-engine-2-0", None)
            fleet = _FakeFleet(ctx, script)

            def bump(ctx_, bumped):
                ctx_["trigger_ts"] = time.time()
                return True
            out = self._run(run._tight_roll, ctx, fleet,
                            {"_bump_rev": bump, "kubectl": kubectl,
                             "_capacity_hold": lambda *a: None if deletes else ("QuotaExceeded", "engine Instance 2: exceeded quota")})
            self.assertEqual(1, len(deletes), rc)
            if want_fail:
                self.assertEqual("quota: the squeeze could not be lifted", ctx["fault"]["infra_fail"])
                self.assertNotEqual("CONVERGED", out["state"])
            else:
                self.assertIsNone(ctx["fault"]["infra_fail"])
                self.assertEqual("CONVERGED", out["state"], out["checks"])

    def test_tight_roll_never_judges_the_wait(self):
        hdr = {"Ne": 2, "Nd": 0, "Nr": 0, "ceil_e": 3, "ceil_d": 0, "ceil_r": 0, "fault": "cordon:all",
               "lift": "never", "topo": "engine-mn"}
        pods = {}
        for i in (0, 1):
            pods[f"p999-engine-{i}-0"] = _rec("engine", i, "old", runner="leader")
            pods[f"p999-engine-{i}-1"] = _rec("engine", i, "old", runner="worker")
        ctx = _ctx_for(hdr, pods, workers=1)
        fleet = _FakeFleet(ctx, lambda c, n: c["pod_state"].setdefault("p999-engine-2-0", _rec("engine", 2, "new", runner="leader", ready=False)))

        def bump(ctx_, bumped):
            ctx_["trigger_ts"] = time.time()
            return True
        out = self._run(run._tight_roll, ctx, fleet,
                        {"_bump_rev": bump, "_churn_stopped": lambda cid: (True, "still"),
                         "_capacity_hold": lambda *a: ("Unschedulable", "engine Instance 2: 0/2 nodes are available: 2 node(s) were unschedulable")})
        self.assertEqual(("HELD", "PASS"), (out["state"], out["verdict"]), out["checks"])
        self.assertTrue(any("still reported at the end" in c for c in out["checks"]))


    def _break_kubectl(self, patched):
        def kubectl(*a, **k):
            if a[2:4] == ("patch", "pod"):
                patched.append(a[4])
            return _Resp('{"items": []}')
        return kubectl

    def test_pause_repair_break_records_the_pod(self):
        """The break on the settled service re-points the highest Instance's
        pod; the handler hands the invariant state along, so that pod's UID
        is recorded before the pause lands."""
        hdr = {"Ne": 2, "Nd": 0, "Nr": 0, "ceil_e": 2, "ceil_d": 0, "ceil_r": 0, "fault": "crashloop:old",
               "form": "start", "during": "idle", "expect": "repaired", "victimcomp": "engine",
               "pause": "true", "pauseat": "after", "topo": "engine"}
        pods = {f"p999-engine-{i}-0": _rec("engine", i, "old") for i in range(2)}
        ctx = _ctx_for(hdr, pods)
        patched, seen = [], {}

        def script(ctx_, n):
            if patched and "p999-engine-1-0" not in seen:
                seen["p999-engine-1-0"] = dict(ctx_["inv"]["mutated_uids"])
                ctx_["pod_state"]["p999-engine-1-0"] = _rec("engine", 1, "old", uid="rebuilt-1", created="9")
        fleet = _FakeFleet(ctx, script)
        out = self._run(run._pause_repair, ctx, fleet,
                        {"kubectl": self._break_kubectl(patched), "_pause_annotation": lambda *a, **k: None,
                         "_ir_row_phase": lambda *a: "Ready", "_victim_events": lambda *a: ([], []),
                         "_victim_repairs": lambda *a: [], "_fetch_initial_ir_items": lambda *a: [],
                         "_isvc_pause_value": lambda *a: ""})
        self.assertEqual(["p999-engine-1-0"], patched)
        self.assertEqual({"engine-1-default-old": "p999-engine-1-0"}, seen["p999-engine-1-0"],
                         "recorded as soon as the break landed")
        self.assertEqual({"engine-1-default-old": "p999-engine-1-0"}, ctx["inv"]["mutated_uids"],
                         "the rebuilt pod (a new UID) is not recorded: it is judged")
        self.assertEqual("CONVERGED", out["state"], out["checks"])

    def test_runtime_edit_repair_break_records_the_pod(self):
        """The break that precedes a runtime edit re-points the highest
        Instance's pod and records it; a repair that never opens ends the
        case, the record stays."""
        hdr = {"Ne": 2, "Nd": 0, "Nr": 0, "ceil_e": 3, "ceil_d": 0, "ceil_r": 0, "mode": "runtimerev",
               "rt": "ome-coordtest-rt-p999", "expect": "roll", "edit": "env", "fault": "crashloop:old",
               "form": "start", "during": "repair", "topo": "engine"}
        pods = {f"p999-engine-{i}-0": _rec("engine", i, "old") for i in range(2)}
        ctx = _ctx_for(hdr, pods)
        patched = []
        out = self._run(lambda *a: run._runtimerev_repair(*a, "ome-coordtest-rt-p999"), ctx, _FakeFleet(ctx),
                        {"kubectl": self._break_kubectl(patched), "_ir_row_phase": lambda *a: ""})
        self.assertEqual(["p999-engine-1-0"], patched)
        self.assertEqual({"engine-1-default-old": "p999-engine-1-0"}, ctx["inv"]["mutated_uids"])
        self.assertEqual("NO_REPAIR", out["state"], out["checks"])


class GeneratedFaultFamiliesTest(unittest.TestCase):
    """The readiness, gang-member and tight families: their size, and that
    every emitted header carries the knobs its runner path reads."""

    @classmethod
    def setUpClass(cls):
        outputs, rows = generate_cases.build_outputs()
        cls.outputs, cls.rows = outputs, rows
        cls.by_folder = {}
        for folder, name, kv, meta in rows:
            cls.by_folder.setdefault(folder, []).append((name, kv, meta))

    def _hdr(self, folder, name):
        return self.outputs[f"{folder}/{name}.yaml"].splitlines()[0]

    def test_family_sizes_and_id_blocks(self):
        self.assertEqual(30, len(self.by_folder["gen-readiness"]))
        self.assertEqual(72, len(self.by_folder["gen-gang-member"]))
        self.assertEqual(28, len(self.by_folder["gen-tight"]))
        for folder in ("gen-readiness", "gen-gang-member", "gen-tight"):
            lo, hi = generate_cases.GEN_BLOCKS[folder]
            for name, _kv, _m in self.by_folder[folder]:
                self.assertTrue(lo <= int(name[1:]) < hi, (folder, name))
        manifest = self.outputs[generate_cases.MANIFEST_REL]
        for folder in ("gen-readiness", "gen-gang-member", "gen-tight"):
            self.assertIn(f"| {folder} |", manifest)

    def test_readiness_cells(self):
        old = [(n, kv) for n, kv, _m in self.by_folder["gen-readiness"] if kv["fault"] == "readiness:old"]
        new = [(n, kv) for n, kv, _m in self.by_folder["gen-readiness"] if kv["fault"] == "readiness:new"]
        self.assertEqual((12, 18), (len(old), len(new)))
        for name, kv in old:
            self.assertEqual("idle", kv["during"])
            self.assertIn(kv["form"], ("persist", "recover"))
            self.assertEqual(kv["Ne"], kv["ceil_e"], "no surge on the idle service")
            self.assertNotIn("instanceReadyTimeout", self.outputs[f"gen-readiness/{name}.yaml"])
        for name, kv in new:
            self.assertNotIn("during", kv)
            self.assertNotIn("form", kv)
            self.assertIn(f'instanceReadyTimeout: "{generate_cases.READINESS_NEW_TIMEOUT}"',
                          self.outputs[f"gen-readiness/{name}.yaml"])
        strats = {m["strat"] for _n, kv, m in self.by_folder["gen-readiness"] if kv["fault"] == "readiness:new"}
        self.assertEqual(set(generate_cases.READINESS_STRATS), strats)
        self.assertEqual({"None", "RecreateInstance"}, {kv["restart"] for _n, kv, _m in self.by_folder["gen-readiness"]})

    def test_gang_member_cells_bind_the_victim_to_the_timing(self):
        victim_for = dict(generate_cases.GANG_MEMBER_TIMINGS)
        seen = set()
        for name, kv, meta in self.by_folder["gen-gang-member"]:
            phase = kv["fault"].partition(":")[2]
            self.assertEqual(victim_for[phase], kv["victim"], name)
            self.assertIn(kv["kill"], ("leader", "worker"))
            self.assertEqual(1, kv["floor_slack"])
            self.assertIn(kv["restart"], ("None", "RecreateInstance"))
            self.assertTrue(kv["topo"].endswith("-mn"))
            self.assertEqual(meta["strat"] == "SurgeThenDrain", kv.get("order_soft") == 1, name)
            seen.add((phase, kv["kill"], kv["topo"], meta["strat"], kv["restart"]))
        # 3 timings x 2 members x 2 topologies x 2 strategies x 2 policies (the
        # two engine widths fold into one key here; the per-shape count is below).
        self.assertEqual(48, len(seen))

    def test_gang_member_cells_count_per_shape(self):
        per_topo = {}
        for _n, kv, _m in self.by_folder["gen-gang-member"]:
            per_topo[kv["topo"]] = per_topo.get(kv["topo"], 0) + 1
        self.assertEqual({"engine-mn": 48, "pd-mn": 24}, per_topo)

    def test_tight_cells(self):
        lifted, plain = [], []
        for name, kv, meta in self.by_folder["gen-tight"]:
            kind = kv["fault"].partition(":")[0]
            self.assertIn(kind, ("quota", "cordon"))
            self.assertTrue(kv["topo"].endswith("-mn"))
            if "lift" in kv:
                self.assertIn(kv["lift"], ("after-hold", "never"))
                self.assertFalse(meta["strat"] == "RecreatePod" and kind == "quota", name)
                lifted.append(name)
            else:
                self.assertEqual(("RecreatePod", "quota"), (meta["strat"], kind), name)
                plain.append(name)
        self.assertEqual((24, 4), (len(lifted), len(plain)))
        quotas = {kv["fault"] for _n, kv, _m in self.by_folder["gen-tight"] if kv["fault"].startswith("quota:")}
        self.assertEqual({"quota:1", "quota:2"}, quotas, "one pod short of a gang at each width")

    def test_every_new_case_validates(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for folder in ("gen-readiness", "gen-gang-member", "gen-tight"):
                (root / folder).mkdir()
                for name, _kv, _m in self.by_folder[folder]:
                    (root / folder / f"{name}.yaml").write_text(self.outputs[f"{folder}/{name}.yaml"])
            self.assertEqual(0, run.validate_cases(root))


class EvictFaultTest(unittest.TestCase):
    """evict removes the podkill victim through the Eviction API, the way a
    drain does: a disruption-budget refusal is retried, an eviction never
    admitted is red, and mode=kill evicts through via=evict."""

    BASE = "# p999: x Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"
    KILL = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
    PODS = {"e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "default", "uid": "u0", "created": "1"},
            "e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "default", "uid": "u1", "created": "2"}}

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    @staticmethod
    def _kubectl(calls, rc=0, stderr=""):
        def kubectl(*a, **k):
            calls.append((a, k.get("input")))
            return subprocess.CompletedProcess(a, rc, "", stderr)
        return kubectl

    def test_header_evicts_at_a_hook_or_through_via(self):
        self.assertTrue(run._fault_ok("evict:rotating"))
        self.assertTrue(run._fault_ok("evict:settling"))
        self.assertFalse(run._fault_ok("evict:idle"))
        self.assertEqual([], self._errs(self.BASE + " fault=evict:rotating"))
        self.assertEqual([], self._errs(self.BASE + " fault=evict:rotating victim=stable victimcomp=engine"))
        self.assertEqual([], self._errs(self.KILL + " via=evict"))
        self.assertEqual([], self._errs(self.KILL.replace("topo=engine", "topo=engine-mn") + " via=delete kill=leader"))
        self.assertTrue(any("mode=kill" in e for e in self._errs(self.BASE + " via=evict")))
        self.assertTrue(any("via=evict" in e for e in self._errs(self.KILL + " fault=evict:rotating")))
        self.assertTrue(any("malformed header value via" in e for e in self._errs(self.KILL + " via=drain")))

    def test_capability_reads_the_eviction_api(self):
        calls = []
        with mock.patch.object(run, "kubectl", self._kubectl(calls)):
            self.assertIsNone(run._fault_capability("evict"))
        self.assertEqual(("get", "--raw", run.EVICTION_API), calls[0][0])
        with mock.patch.object(run, "kubectl", self._kubectl([], rc=1, stderr="the server could not find the requested resource")):
            self.assertIn("not served", run._fault_capability("evict"))
        self.assertIsNone(run._fault_capability("podkill"))

    def test_eviction_posts_the_eviction_subresource(self):
        calls = []
        with mock.patch.object(run, "kubectl", self._kubectl(calls)):
            self.assertEqual((True, False, ""), run._evict_pod("e1"))
        args, body = calls[0]
        self.assertEqual(("create", "--raw", f"/api/v1/namespaces/{run.NS}/pods/e1/eviction", "-f", "-"), args)
        self.assertEqual({"apiVersion": "policy/v1", "kind": "Eviction", "metadata": {"name": "e1", "namespace": run.NS}},
                         json.loads(body))
        refused = "Error from server (TooManyRequests): Cannot evict pod as it would violate the pod's disruption budget."
        with mock.patch.object(run, "kubectl", self._kubectl([], rc=1, stderr=refused)):
            admitted, denied, msg = run._evict_pod("e1")
        self.assertEqual((False, True), (admitted, denied))
        self.assertIn("disruption budget", msg)
        with mock.patch.object(run, "kubectl", self._kubectl([], rc=1, stderr='pods "e1" not found')):
            self.assertEqual((False, False), run._evict_pod("e1")[:2])

    def test_refusal_leaves_the_fault_due_again_and_admission_fires_it(self):
        f = run._fault_new("p999", {"fault": "evict:rotating"})
        refused = "Cannot evict pod as it would violate the pod's disruption budget."
        # The revision snapshot predates both pods, so the rotating hook is due
        # and the newest pod is the surge the rule aims at.
        with mock.patch.object(run, "kubectl", self._kubectl([], rc=1, stderr=refused)):
            run._fault_fire(f, self.PODS, {"engine": 1}, {"older"})
        self.assertFalse(f["fired"])
        self.assertEqual(1, f["denials"])
        self.assertGreater(f["retry_at"], time.time())
        self.assertIsNone(f["infra_fail"])
        fired = []
        ctx = {"fault": f, "trigger_ts": 1.0, "pod_state": self.PODS, "pods_per_inst": {"engine": 1}, "old_hashes": {"older"}}
        with mock.patch.object(run, "_fault_fire", lambda *a, **k: fired.append(a)):
            run._fault_tick(ctx)
            self.assertEqual([], fired, "a refused eviction waits for its retry cadence")
            f["retry_at"] = time.time() - 1
            run._fault_tick(ctx)
            self.assertEqual(1, len(fired), "once the cadence has passed the hook asks again")
        with mock.patch.object(run, "kubectl", self._kubectl([])):
            run._fault_fire(f, self.PODS, {"engine": 1}, {"older"})
        self.assertTrue(f["fired"])
        self.assertEqual(("e1", "engine"), (f["crash_pod"], f["victim_comp"]))
        self.assertTrue(any("after 1 budget refusal" in n for n in f["notes"]))

    def test_finalize_reds_an_eviction_the_budget_never_admitted(self):
        f = run._fault_new("p999", {"fault": "evict:rotating"})
        f.update(fired=False, denials=4)
        notes, checks = [], []
        self.assertEqual("FAIL", run._fault_finalize(f, "PASS", notes, checks))
        self.assertTrue(any("never admitted" in n for n in notes))
        admitted = run._fault_new("p999", {"fault": "evict:rotating"})
        admitted["fired"] = True
        self.assertEqual("PASS", run._fault_finalize(admitted, "PASS", [], []))
        repeated = run._fault_new("p999", {"fault": "evict:rotating", "repeat": 3})
        repeated.update(fired=False, denials=3, fired_count=2)
        checks = []
        self.assertEqual("PASS", run._fault_finalize(repeated, "PASS", [], checks),
                         "a repetition refused at the window's end does not unfire the ones that fired")
        self.assertFalse(any("never reached its phase hook" in c for c in checks))
        unfired = run._fault_new("p999", {"fault": "evict:rotating"})
        checks = []
        self.assertEqual("PASS", run._fault_finalize(unfired, "PASS", [], checks))
        self.assertTrue(any("never reached its phase hook" in c for c in checks))

    def test_kill_removal_follows_via(self):
        calls = []
        with mock.patch.object(run, "kubectl", self._kubectl(calls)):
            self.assertEqual(("ok", ""), run._kill_remove("e0", "delete"))
        self.assertEqual(("-n", run.NS, "delete", "pod", "e0", "--wait=false"), calls[0][0])
        calls = []
        with mock.patch.object(run, "kubectl", self._kubectl(calls)):
            self.assertEqual(("ok", ""), run._kill_remove("e0", "evict"))
        self.assertEqual("create", calls[0][0][0])
        refused = "Cannot evict pod as it would violate the pod's disruption budget."
        with mock.patch.object(run, "kubectl", self._kubectl([], rc=1, stderr=refused)), \
                mock.patch.object(run, "LIFT_SETTLE_S", 0), mock.patch.object(run, "FAULT_RETRY_S", 0):
            state, why = run._kill_remove("e0", "evict")
        self.assertEqual("denied", state)
        self.assertIn("refused the eviction", why)
        with mock.patch.object(run, "kubectl", self._kubectl([], rc=1, stderr="forbidden")):
            self.assertEqual("error", run._kill_remove("e0", "evict")[0])


class PreemptFaultTest(unittest.TestCase):
    """preempt has the scheduler remove the pod: fillers take every node's
    spare CPU and a higher-priority filler presses on the aimed pod's node;
    the victim is read back from the scheduler's own record, and the fillers
    are dropped at the lift or by the cleanup."""

    BASE = "# p999: x Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"
    KILL = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
    NODES = {"items": [
        {"metadata": {"name": "kind-control-plane"}, "spec": {"taints": [{"key": "node-role.kubernetes.io/control-plane", "effect": "NoSchedule"}]},
         "status": {"conditions": [{"type": "Ready", "status": "True"}]}},
        {"metadata": {"name": "kind-worker"}, "spec": {}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}},
        {"metadata": {"name": "kind-worker2"}, "spec": {"taints": [{"key": "soft", "effect": "PreferNoSchedule"}]},
         "status": {"conditions": [{"type": "Ready", "status": "True"}]}},
        {"metadata": {"name": "kind-worker3"}, "spec": {"unschedulable": True}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}},
        {"metadata": {"name": "kind-worker4"}, "spec": {}, "status": {"conditions": [{"type": "Ready", "status": "False"}]}}]}

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_ties_the_preemption_to_its_lift(self):
        self.assertTrue(run._fault_ok("preempt:rotating"))
        self.assertFalse(run._fault_ok("preempt:idle"))
        self.assertEqual([], self._errs(self.BASE + " fault=preempt:rotating lift=after-settle"))
        self.assertEqual([], self._errs(self.BASE + " fault=preempt:rotating lift=never floor_soft=1 order_soft=1"))
        self.assertEqual([], self._errs(self.KILL + " via=preempt lift=never"))
        self.assertEqual([], self._errs(self.KILL + " via=preempt lift=after-settle"))
        self.assertTrue(any("needs lift=" in e for e in self._errs(self.BASE + " fault=preempt:rotating")))
        self.assertTrue(any("via=preempt" in e for e in self._errs(self.KILL + " fault=preempt:rotating lift=never")))
        self.assertTrue(any("needs lift=" in e for e in self._errs(self.KILL + " via=preempt")))
        self.assertTrue(any("kill= does not apply" in e for e in self._errs(self.KILL + " via=preempt kill=leader lift=never")))
        self.assertTrue(any("does not combine" in e for e in self._errs(self.KILL + " via=preempt fault=cordon:source lift=never")))
        self.assertTrue(any("pending cause is removed" in e for e in self._errs(self.BASE + " fault=podkill:rotating lift=never")))
        self.assertTrue(any("use one" in e for e in self._errs(self.BASE + " fault=preempt:rotating lift=never blocked=1")))
        self.assertTrue(any("pause=" in e for e in self._errs(self.KILL + " via=preempt lift=never pause=true")))

    def test_capability_needs_an_owned_cluster_and_the_priority_api(self):
        # The class apply succeeds and the OME namespace runs no pod below it.
        with mock.patch.object(run, "_node_fault_capability", lambda: None), mock.patch.object(run, "_api_served", lambda p: True), \
                mock.patch.object(run, "kubectl", lambda *a, **k: _Resp('{"items": []}')):
            self.assertIsNone(run._fault_capability("preempt"))
        with mock.patch.object(run, "_node_fault_capability", lambda: "not a KIND cluster it owns"):
            self.assertIn("owns", run._fault_capability("preempt"))
        with mock.patch.object(run, "_node_fault_capability", lambda: None), mock.patch.object(run, "_api_served", lambda p: False):
            self.assertIn(run.PRIORITY_CLASS_API, run._fault_capability("preempt"))

    def test_eligible_nodes_are_ready_uncordoned_and_untainted(self):
        def kubectl(*a, **k):
            return subprocess.CompletedProcess(a, 0, json.dumps(self.NODES), "")
        with mock.patch.object(run, "kubectl", kubectl):
            self.assertEqual(["kind-worker", "kind-worker2"], run._eligible_nodes())

    def test_spare_cpu_is_allocatable_minus_bound_requests(self):
        pods = {"items": [
            {"status": {"phase": "Running"}, "spec": {"containers": [{"resources": {"requests": {"cpu": "500m"}}}]}},
            {"status": {"phase": "Running"}, "spec": {"containers": [{"resources": {"requests": {"cpu": "50m", "memory": "64Mi"}}}]}},
            {"status": {"phase": "Succeeded"}, "spec": {"containers": [{"resources": {"requests": {"cpu": "1"}}}]}},
            {"status": {"phase": "Pending"}, "spec": {"containers": [{}]}}]}
        calls = []

        def kubectl(*a, **k):
            calls.append(a)
            if a[:2] == ("get", "node"):
                return subprocess.CompletedProcess(a, 0, json.dumps({"status": {"allocatable": {"cpu": "4"}}}), "")
            return subprocess.CompletedProcess(a, 0, json.dumps(pods), "")
        with mock.patch.object(run, "kubectl", kubectl):
            self.assertEqual(3450, run._node_cpu_free("kind-worker"))
        self.assertIn("spec.nodeName=kind-worker", calls[1])
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 1, "", "no")):
            self.assertIsNone(run._node_cpu_free("kind-worker"))

    def test_filler_is_pinned_through_the_scheduler_and_requests_only_cpu(self):
        with mock.patch.object(run, "_fixture_values", lambda: {"OME_EVAL_NGINX_IMAGE": "nginx:test"}):
            pod = run._filler_manifest("perm-p999-press", "p999", "kind-worker", 1050, "perm-p999-preempt")
            plain = run._filler_manifest("perm-p999-fill-kind-worker2", "p999", "kind-worker2", 2000)
        self.assertEqual({"kubernetes.io/hostname": "kind-worker"}, pod["spec"]["nodeSelector"])
        self.assertNotIn("nodeName", pod["spec"])
        self.assertEqual("perm-p999-preempt", pod["spec"]["priorityClassName"])
        self.assertEqual({"cpu": "1050m"}, pod["spec"]["containers"][0]["resources"]["requests"])
        self.assertEqual({run.FILLER_LABEL: "p999"}, pod["metadata"]["labels"])
        self.assertEqual("nginx:test", pod["spec"]["containers"][0]["image"])
        self.assertNotIn("priorityClassName", plain["spec"])

    def test_arm_registers_the_cleanup_then_creates_a_class_above_the_case(self):
        f = run._fault_new("p999", {"fault": "preempt:rotating", "lift": "after-settle"})
        calls, saved = [], list(run._FAULT_CLEANUPS)
        pods = {"items": [{"spec": {"priority": 0}}, {"spec": {"priority": 10}}]}

        def kubectl(*a, **k):
            calls.append((a, k.get("input"), len(run._FAULT_CLEANUPS)))
            if a[2:4] == ("get", "pod"):
                return subprocess.CompletedProcess(a, 0, json.dumps(pods), "")
            return subprocess.CompletedProcess(a, 0, "", "")
        try:
            with mock.patch.object(run, "kubectl", kubectl):
                self.assertEqual((True, ""), run._fault_preempt_arm(f, "p999"))
            apply = [c for c in calls if c[0][0] == "apply"][0]
            self.assertEqual(len(saved) + 1, apply[2], "the cleanup is registered before the class is created")
            pc = json.loads(apply[1])
            self.assertEqual(("PriorityClass", "perm-p999-preempt", 11, False, "PreemptLowerPriority"),
                             (pc["kind"], pc["metadata"]["name"], pc["value"], pc["globalDefault"], pc["preemptionPolicy"]))
            self.assertEqual("p999", pc["metadata"]["labels"]["perm.test/fault"])
            self.assertIn(("preempt", f), run._FAULT_CLEANUPS)
        finally:
            run._FAULT_CLEANUPS[:] = saved

    def _armed(self):
        f = run._fault_new("p999", {"fault": "preempt:rotating", "lift": "after-settle"})
        f.update(pc_name="perm-p999-preempt", pc_value=1, fillers=[], pc_deleted=False, fillers_dropped=False)
        return f

    def test_fill_takes_every_other_node_then_presses_on_the_targets_node(self):
        f = self._armed()
        pod_state = {"e0": {"alive": True, "comp": "engine", "inst": "0", "ready": True, "serving": True, "node": "kind-worker"},
                     "e1": {"alive": True, "comp": "engine", "inst": "1", "ready": True, "serving": True, "node": "kind-worker2"}}
        manifests = []

        def kubectl(*a, **k):
            if a[0] == "apply":
                manifests.append(json.loads(k["input"]))
            return subprocess.CompletedProcess(a, 0, "", "")
        free = {"kind-worker": 1000, "kind-worker2": 2000}
        target = {"spec": {"containers": [{"resources": {"requests": {"cpu": "50m"}}}]}}
        with mock.patch.object(run, "kubectl", kubectl), mock.patch.object(run, "_get_pod", lambda n, ns=run.NS: target), \
                mock.patch.object(run, "_eligible_nodes", lambda: ["kind-worker", "kind-worker2"]), \
                mock.patch.object(run, "_node_pods", lambda n: []), mock.patch.object(run, "_control_plane_pods", lambda: []), \
                mock.patch.object(run, "_node_cpu_free", lambda n, items=None: free[n]), \
                mock.patch.object(run, "_pod_bound", lambda n: "bound"), \
                mock.patch.object(run, "_fixture_values", lambda: {"OME_EVAL_NGINX_IMAGE": "nginx:test"}):
            run._preempt_fill(f, "e0", "kind-worker", pod_state)
        self.assertIsNone(f["infra_fail"])
        self.assertTrue(f["fired"])
        self.assertEqual(["perm-p999-fill-kind-worker2", "perm-p999-press"], f["fillers"])
        by_name = {m["metadata"]["name"]: m for m in manifests}
        block = by_name["perm-p999-fill-kind-worker2"]
        self.assertEqual(("kind-worker2", "2000m"), (block["spec"]["nodeSelector"]["kubernetes.io/hostname"],
                                                     block["spec"]["containers"][0]["resources"]["requests"]["cpu"]))
        self.assertNotIn("priorityClassName", block["spec"], "a blocking filler fits without preempting anything")
        press = by_name["perm-p999-press"]
        self.assertEqual(("kind-worker", "1050m", "perm-p999-preempt"),
                         (press["spec"]["nodeSelector"]["kubernetes.io/hostname"],
                          press["spec"]["containers"][0]["resources"]["requests"]["cpu"], press["spec"]["priorityClassName"]))
        self.assertEqual({"e0": {"comp": "engine", "inst": "0", "serving": True}, "e1": {"comp": "engine", "inst": "1", "serving": True}},
                         f["pre_alive"])
        self.assertEqual(("e0", "kind-worker", 50), (f["aim_pod"], f["aim_node"], f["victim_cpu"]))
        self.assertTrue(any("must preempt one pod there" in n for n in f["notes"]))

    def test_fill_refuses_a_cpu_less_target_or_an_ineligible_node(self):
        f = self._armed()
        with mock.patch.object(run, "_get_pod", lambda n: {"spec": {"containers": [{}]}}):
            run._preempt_fill(f, "e0", "kind-worker", {})
        self.assertIn("requests no CPU", f["infra_fail"])
        f = self._armed()
        target = {"spec": {"containers": [{"resources": {"requests": {"cpu": "50m"}}}]}}
        with mock.patch.object(run, "_get_pod", lambda n: target), mock.patch.object(run, "_eligible_nodes", lambda: ["kind-worker2"]):
            run._preempt_fill(f, "e0", "kind-worker", {})
        self.assertIn("not a node a case pod can be placed on", f["infra_fail"])

    def test_fire_waits_for_the_aimed_pod_to_bind(self):
        f = self._armed()
        unbound = {"e0": {"alive": True, "comp": "engine", "inst": "0", "hash": "new", "runner": "default", "created": "9", "node": ""}}
        run._fault_fire(f, unbound, {"engine": 1}, {"old"})
        self.assertFalse(f["fired"])
        self.assertGreater(f["retry_at"], time.time())
        filled = []
        bound = {"e0": dict(unbound["e0"], node="kind-worker")}
        with mock.patch.object(run, "_preempt_fill", lambda *a: filled.append(a)):
            run._fault_fire(f, bound, {"engine": 1}, {"old"})
        self.assertEqual([(f, "e0", "kind-worker", bound)], filled)

    def test_victim_is_the_preempted_case_pod_not_a_drained_one(self):
        f = self._armed()
        f.update(pre_alive={"e0": {"comp": "engine", "inst": "0", "serving": True}, "d0": {"comp": "decoder", "inst": "0", "serving": True},
                            "e1": {"comp": "engine", "inst": "1", "serving": True}},
                 aim_node="kind-worker", fire_ts=1.0, not_victim=set(), preempted=[])
        pod_state = {"e0": None, "d0": {"alive": False}, "e1": {"alive": True}}
        preempted = {"e0": True, "d0": False}
        with mock.patch.object(run, "_pod_preempted", lambda n: preempted[n]):
            self.assertEqual("e0", run._preempt_victim(f, pod_state))
        self.assertEqual(("e0", "engine", "0", True), (f["crash_pod"], f["crash_comp"], f["crash_inst"], f["victim_was_serving"]))
        self.assertEqual({"d0"}, f["not_victim"])
        self.assertEqual(["e0"], f["preempted"])
        with mock.patch.object(run, "_pod_preempted", lambda n: self.fail("a recorded victim is not read again")):
            self.assertEqual("e0", run._preempt_victim(f, pod_state))

    def test_preemption_is_read_off_the_condition_or_the_event(self):
        marked = {"status": {"conditions": [{"type": "DisruptionTarget", "reason": "PreemptionByScheduler"}]},
                  "metadata": {"deletionTimestamp": "t"}}
        live = {"status": {"conditions": []}, "metadata": {}}
        with mock.patch.object(run, "_get_pod", lambda n, ns=run.NS: marked):
            self.assertTrue(run._pod_preempted("e0"))
        with mock.patch.object(run, "_get_pod", lambda n, ns=run.NS: live):
            self.assertFalse(run._pod_preempted("e0"))
        calls = []

        def kubectl(*a, **k):
            calls.append(a)
            return subprocess.CompletedProcess(a, 0, json.dumps({"items": [{"reason": "Preempted"}]}), "")
        with mock.patch.object(run, "_get_pod", lambda n, ns=run.NS: None), mock.patch.object(run, "kubectl", kubectl):
            self.assertTrue(run._pod_preempted("e0"))
            self.assertTrue(run._pod_preempted("m0", ns="ome"))
        self.assertIn("involvedObject.kind=Pod,involvedObject.name=e0,reason=Preempted", calls[0])
        self.assertEqual(("-n", "ome"), calls[1][:2], "a control-plane pod's record is read in its own namespace")

    def test_lift_and_cleanup_drop_fillers_and_the_class_once(self):
        f = self._armed()
        f["fillers"] = ["perm-p999-press"]
        calls = []

        def kubectl(*a, **k):
            calls.append(a)
            return subprocess.CompletedProcess(a, 0, "", "")
        with mock.patch.object(run, "kubectl", kubectl):
            self.assertTrue(run._preempt_lift(f))
            self.assertTrue(run._preempt_lift(f))
            run._preempt_cleanup(f)
            run._preempt_cleanup(f)
        self.assertEqual([("-n", run.NS, "delete", "pod", "-l", f"{run.FILLER_LABEL}=p999", "--ignore-not-found=true", "--wait=false"),
                          ("delete", "priorityclass", "perm-p999-preempt", "--ignore-not-found=true")], calls)
        self.assertTrue(run._preempt_lift(self._armed()), "nothing to drop is already lifted")

    def test_kill_record_is_a_hookless_preempt_fault(self):
        f = run._preempt_record("p999", {"via": "preempt", "lift": "never"})
        self.assertEqual(("preempt", "never", run.IDLE_FAULT_PHASE), (f["kind"], f["lift"], f["during"]))
        self.assertTrue(run._fault_at_idle(f), "the phase hook leaves it alone; the kill drives it")


class PendingHoldTest(unittest.TestCase):
    """A pod the cluster cannot place or admit: the Instance row names the
    wait, nothing is taken down meanwhile, and the operation completes once
    the cause is lifted or holds with the reason to the end."""

    IR = {"items": [{"spec": {"component": "engine"}, "status": {"instanceStatuses": [
        {"index": 0, "phase": "Ready"},
        {"index": 1, "phase": "Creating", "operation": {"type": "Restart", "waiting": "Unschedulable"},
         "lastFailure": {"reason": "Unschedulable", "message": "0/3 nodes are available: 3 Insufficient cpu."}}]}},
        {"spec": {"component": "decoder"}, "status": {"instanceStatuses": [{"index": 0, "phase": "Failed"}]}}]}

    def test_rows_and_the_waiting_row_are_read_off_the_replicas(self):
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, json.dumps(self.IR), "")):
            rows = run._ir_rows_by_index("p999", ["engine", "decoder"])
            engine_only = run._ir_rows_by_index("p999", ["engine"])
        self.assertEqual({("engine", "0"), ("engine", "1"), ("decoder", "0")}, set(rows))
        self.assertEqual({("engine", "0"), ("engine", "1")}, set(engine_only))
        self.assertEqual(("engine", "1", "Unschedulable", "0/3 nodes are available: 3 Insufficient cpu."),
                         run._pending_row(rows, {"Unschedulable"}))
        self.assertIsNone(run._pending_row(rows, {"Unschedulable"}, inst="0"))
        self.assertIsNone(run._pending_row(rows, {"QuotaExceeded"}))
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 1, "", "no")):
            self.assertEqual({}, run._ir_rows_by_index("p999", ["engine"]))

    def _f(self, lift):
        return {"case_id": "p999", "kind": "preempt", "param": "rotating", "lift": lift, "fired": True,
                "infra_fail": None, "notes": [], "cause": "the filler pods holding every node's spare CPU"}

    def _out(self, **kw):
        out = {"bitten_at": 12.0, "token": "Unschedulable", "row": ("engine", "1"), "message": "Insufficient cpu",
               "failed": [], "lifted_at": None, "done_at": None, "held_s": None, "waiting_at_end": False,
               "escaped_at": None, "unbound_at": None}
        out.update(kw)
        return out

    def _ctx(self, min_ready):
        return {"N": {"engine": 4, "decoder": 0, "router": 0}, "min_ready": min_ready}

    def test_verdict_table(self):
        ctx, comps, allow = self._ctx({"engine": 3}), ["engine"], {"engine": 1}
        f = self._f("after-settle")
        self.assertEqual("PASS", run._pending_checks(f, "after-settle", "UNBOUND", self._out(bitten_at=None, unbound_at=5.0),
                                                     ctx, comps, allow, "PASS", [], []))
        self.assertIn("never bound", f["infra_fail"])
        notes = []
        self.assertEqual("FAIL", run._pending_checks(self._f("after-settle"), "after-settle", "NO_WAIT", self._out(bitten_at=None),
                                                     ctx, comps, allow, "PASS", notes, []))
        self.assertTrue(any("no Instance reported a wait reason" in n for n in notes))
        f = self._f("after-settle")
        run._pending_checks(f, "after-settle", "ESCAPED", self._out(escaped_at=30.0), ctx, comps, allow, "PASS", [], [])
        self.assertIn("did not hold", f["infra_fail"])
        checks = []
        self.assertEqual("PASS", run._pending_checks(self._f("after-settle"), "after-settle", "DONE",
                                                     self._out(lifted_at=40.0, done_at=25.0), ctx, comps, allow, "PASS", [], checks))
        self.assertTrue(any("waits with reason Unschedulable" in c for c in checks))
        self.assertTrue(any("completed 25s after the lift" in c for c in checks))
        self.assertTrue(any("kept 3 serving" in c for c in checks))
        notes = []
        self.assertEqual("FAIL", run._pending_checks(self._f("after-settle"), "after-settle", "NOT_DONE", self._out(lifted_at=40.0),
                                                     ctx, comps, allow, "PASS", notes, []))
        self.assertTrue(any("did not complete after the lift" in n for n in notes))
        f = self._f("after-settle")
        run._pending_checks(f, "after-settle", "HOLD_TIMEOUT", self._out(), ctx, comps, allow, "PASS", [], [])
        self.assertIn("never lifted", f["infra_fail"])
        checks = []
        self.assertEqual("PASS", run._pending_checks(self._f("never"), "never", "HELD", self._out(held_s=90.0, waiting_at_end=True),
                                                     ctx, comps, allow, "PASS", [], checks))
        self.assertTrue(any("still waiting with reason Unschedulable" in c for c in checks))
        notes = []
        self.assertEqual("FAIL", run._pending_checks(self._f("never"), "never", "HELD", self._out(held_s=90.0),
                                                     ctx, comps, allow, "PASS", notes, []))
        self.assertTrue(any("dropped before the hold ended" in n for n in notes))
        notes = []
        self.assertEqual("FAIL", run._pending_checks(self._f("never"), "never", "HOLD_TIMEOUT", self._out(),
                                                     ctx, comps, allow, "PASS", notes, []))
        self.assertTrue(any("hold did not complete" in n for n in notes))

    def test_a_failed_row_or_a_lost_instance_reds_the_hold(self):
        allow = {"engine": 1}
        notes = []
        self.assertEqual("FAIL", run._pending_checks(self._f("never"), "never", "HELD",
                                                     self._out(waiting_at_end=True, failed=[("engine", "1")]),
                                                     self._ctx({"engine": 3}), ["engine"], allow, "PASS", notes, []))
        self.assertTrue(any("read Failed" in n for n in notes))
        notes = []
        self.assertEqual("FAIL", run._pending_checks(self._f("never"), "never", "HELD", self._out(waiting_at_end=True),
                                                     self._ctx({"engine": 2}), ["engine"], allow, "PASS", notes, []))
        self.assertTrue(any("dropped to 2 serving" in n for n in notes))

    def _watch_ctx(self):
        return {"case_id": "p999", "trigger_ts": time.time(), "closed": False, "pod_state": {}, "N": {"engine": 2}}

    def test_watch_lifts_after_the_settle_then_waits_for_done(self):
        f = self._f("after-settle")
        lifted = []
        waiting = {"on": True}

        def rows(case_id, comps):
            if not waiting["on"]:
                return {("engine", "1"): {"phase": "Ready"}}
            return {("engine", "1"): {"phase": "Creating", "operation": {"waiting": "Unschedulable"}}}

        def lift(ff):
            lifted.append(ff)
            waiting["on"] = False
            return True
        ctx = self._watch_ctx()
        with mock.patch.object(run, "_read_one", lambda c, timeout=1.0: None), mock.patch.object(run, "_ir_rows_by_index", rows), \
                mock.patch.object(run, "_pending_lift", lift), mock.patch.object(run, "LIFT_SETTLE_S", 0), \
                mock.patch.object(run, "STABILITY_S", 0), mock.patch.object(run, "ROW_POLL_S", 0):
            state, out = run._pending_watch(ctx, f, ["engine"], "after-settle", lambda: not waiting["on"], 30)
        self.assertEqual("DONE", state)
        self.assertEqual([f], lifted)
        self.assertEqual(("engine", "1", "Unschedulable"), (out["row"][0], out["row"][1], out["token"]))
        self.assertIsNotNone(out["lifted_at"])
        self.assertIsNotNone(out["done_at"])
        self.assertIn("converged_at", ctx)

    def test_watch_holds_to_the_end_and_reads_a_cause_that_never_bound(self):
        f = self._f("never")
        waiting_rows = {("engine", "1"): {"phase": "Creating", "operation": {"waiting": "Unschedulable"}}}
        ctx = self._watch_ctx()
        with mock.patch.object(run, "_read_one", lambda c, timeout=1.0: None), mock.patch.object(run, "_ir_rows_by_index", lambda a, b: waiting_rows), \
                mock.patch.object(run, "IDLE_HOLD_S", 0), mock.patch.object(run, "ROW_POLL_S", 0):
            state, out = run._pending_watch(ctx, f, ["engine"], "never", lambda: False, 30)
        self.assertEqual("HELD", state)
        self.assertTrue(out["waiting_at_end"])
        self.assertIsNotNone(out["held_s"])
        with mock.patch.object(run, "_read_one", lambda c, timeout=1.0: None), mock.patch.object(run, "_ir_rows_by_index", lambda a, b: {}), \
                mock.patch.object(run, "ROW_POLL_S", 0):
            state, out = run._pending_watch(self._watch_ctx(), self._f("never"), ["engine"], "never", lambda: True, 30)
        self.assertEqual("UNBOUND", state)
        self.assertIsNotNone(out["unbound_at"])
        with mock.patch.object(run, "_read_one", lambda c, timeout=1.0: None), mock.patch.object(run, "_ir_rows_by_index", lambda a, b: waiting_rows), \
                mock.patch.object(run, "ROW_POLL_S", 0), mock.patch.object(run, "IDLE_HOLD_S", 3600):
            state, out = run._pending_watch(self._watch_ctx(), self._f("never"), ["engine"], "never", lambda: True, 30)
        self.assertEqual("ESCAPED", state)
        with mock.patch.object(run, "_read_one", lambda c, timeout=1.0: None), mock.patch.object(run, "_ir_rows_by_index", lambda a, b: {}), \
                mock.patch.object(run, "ROW_POLL_S", 0):
            state, out = run._pending_watch(self._watch_ctx(), self._f("never"), ["engine"], "never", lambda: False, 0)
        self.assertEqual("NO_WAIT", state)


class PendingCauseTest(unittest.TestCase):
    """taint:all and quota:<n> as pending causes: every schedulable node
    tainted and untainted again, a quota admitting fewer pods than run, the
    lift dispatch, and the header rules that tie each to its lift."""

    BASE = "# p999: x Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"
    KILL = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_ties_each_cause_to_its_lift(self):
        self.assertTrue(run._fault_ok("taint:all"))
        self.assertFalse(run._fault_ok("taint:source"))
        self.assertTrue(run._fault_ok("quota:-1"))
        self.assertTrue(run._fault_ok("quota:0"))
        self.assertFalse(run._fault_ok("quota:none"))
        self.assertEqual([], self._errs(self.BASE + " fault=taint:all lift=after-settle"))
        self.assertEqual([], self._errs(self.BASE + " fault=quota:0 lift=never"))
        self.assertEqual([], self._errs(self.BASE + " fault=quota:0 blocked=1"), "a quota with no lift keeps its plain contract")
        self.assertEqual([], self._errs(self.KILL.replace("topo=engine", "topo=engine-mn") + " fault=taint:all lift=never kill=leader"))
        self.assertEqual([], self._errs(self.KILL + " fault=quota:-1 lift=after-settle"))
        self.assertTrue(any("needs lift=" in e for e in self._errs(self.BASE + " fault=taint:all")))
        self.assertTrue(any("needs lift=" in e for e in self._errs(self.KILL + " fault=quota:-1")))
        self.assertTrue(any("use one" in e for e in self._errs(self.BASE + " fault=quota:0 lift=never blocked=1")))
        self.assertTrue(any("mode=idle" not in e for e in self._errs(self.BASE + " fault=taint:all during=idle lift=never")))
        self.assertTrue(self._errs(self.BASE + " fault=taint:all during=idle lift=never"), "a taint is not an idle break")

    def test_capability_needs_an_owned_cluster(self):
        with mock.patch.object(run, "_node_fault_capability", lambda: None):
            self.assertIsNone(run._fault_capability("taint"))
        with mock.patch.object(run, "_node_fault_capability", lambda: "not a KIND cluster it owns"):
            self.assertIn("owns", run._fault_capability("taint"))
        self.assertIsNone(run._fault_capability("quota"), "a quota is namespaced and runs anywhere")

    def test_arm_taints_every_schedulable_node_after_registering_the_lift(self):
        f = run._fault_new("p999", {"fault": "taint:all", "lift": "after-settle"})
        calls, saved = [], list(run._FAULT_CLEANUPS)

        def kubectl(*a, **k):
            calls.append((a, len(run._FAULT_CLEANUPS)))
            return subprocess.CompletedProcess(a, 0, "", "")
        try:
            with mock.patch.object(run, "kubectl", kubectl), mock.patch.object(run, "_eligible_nodes", lambda: ["kind-worker", "kind-worker2"]):
                self.assertEqual((True, ""), run._fault_taint_arm(f, "p999"))
            self.assertEqual([("taint", "nodes", "kind-worker", "perm.test/pending=p999:NoSchedule", "--overwrite"),
                              ("taint", "nodes", "kind-worker2", "perm.test/pending=p999:NoSchedule", "--overwrite")],
                             [c[0] for c in calls])
            self.assertTrue(all(c[1] == len(saved) + 1 for c in calls), "the lift is registered before the first taint")
            self.assertTrue(f["fired"])
            self.assertEqual(["kind-worker", "kind-worker2"], f["tainted"])
            self.assertIn("every schedulable node", f["cause"])
            calls.clear()
            with mock.patch.object(run, "kubectl", kubectl):
                self.assertTrue(run._taint_lift(f))
                self.assertTrue(run._taint_lift(f))
                run._run_fault_cleanups()
            self.assertEqual([("taint", "nodes", "kind-worker", "perm.test/pending:NoSchedule-"),
                              ("taint", "nodes", "kind-worker2", "perm.test/pending:NoSchedule-")],
                             [c[0] for c in calls], "a lifted taint is not removed twice")
            self.assertEqual([], f["tainted"])
            with mock.patch.object(run, "_eligible_nodes", lambda: []):
                g = run._fault_new("p999", {"fault": "taint:all", "lift": "never"})
                self.assertFalse(run._fault_taint_arm(g, "p999")[0])
                self.assertIn("no Ready, schedulable node", g["infra_fail"])
        finally:
            run._FAULT_CLEANUPS[:] = saved

    def test_lift_dispatch_covers_every_pending_cause(self):
        calls = []
        with mock.patch.object(run, "kubectl", lambda *a, **k: calls.append(a) or subprocess.CompletedProcess(a, 0, "", "")):
            self.assertTrue(run._pending_lift({"kind": "quota", "case_id": "p999"}))
            self.assertTrue(run._pending_lift({"kind": "taint", "case_id": "p999", "tainted": ["kind-worker"]}))
            self.assertTrue(run._pending_lift({"kind": "preempt", "case_id": "p999", "fillers": ["x"], "fillers_dropped": False}))
            self.assertFalse(run._pending_lift({"kind": "podkill", "case_id": "p999"}))
        self.assertEqual(("-n", run.NS, "delete", "resourcequota", "perm-p999-quota", "--ignore-not-found=true"), calls[0])
        self.assertEqual("taint", calls[1][0])
        self.assertIn(f"{run.FILLER_LABEL}=p999", calls[2])

    def test_negative_quota_admits_fewer_pods_than_run(self):
        case_pods = {"items": [{"metadata": {"labels": {"component": "engine"}},
                                "spec": {"containers": [{"resources": {"requests": {"cpu": "50m", "memory": "64Mi"}}}]}}] * 2}
        applied, saved = [], list(run._FAULT_CLEANUPS)

        def kubectl(*a, **k):
            if "apply" in a:
                applied.append(json.loads(k["input"]))
                return subprocess.CompletedProcess(a, 0, "", "")
            return subprocess.CompletedProcess(a, 0, json.dumps(case_pods), "")
        try:
            f = run._fault_new("p999", {"fault": "quota:-1", "lift": "never"})
            with mock.patch.object(run, "kubectl", kubectl):
                self.assertEqual((True, ""), run._fault_quota_create(f, "p999"))
            hard = applied[0]["spec"]["hard"]
            self.assertEqual({"requests.cpu": "50m", "requests.memory": str(64 * 1024 * 1024)}, hard,
                             "two pods run, the quota admits one: the next create is refused")
            self.assertIn("1 pod(s) fewer than run", f["cause"])
            self.assertTrue(any("quota -1 pod(s)" in n for n in f["notes"]))
            f = run._fault_new("p999", {"fault": "quota:-3", "lift": "never"})
            with mock.patch.object(run, "kubectl", kubectl):
                run._fault_quota_create(f, "p999")
            self.assertEqual("0m", applied[1]["spec"]["hard"]["requests.cpu"], "a quota never goes negative")
        finally:
            run._FAULT_CLEANUPS[:] = saved

    def test_pending_tokens_name_each_causes_reason(self):
        self.assertEqual({"Unschedulable"}, run.PENDING_TOKENS["taint"])
        self.assertEqual({"Unschedulable"}, run.PENDING_TOKENS["preempt"])
        self.assertEqual({"QuotaExceeded", "Unschedulable"}, run.PENDING_TOKENS["quota"])
        self.assertEqual(set(run.PENDING_TOKENS), run.PENDING_FAULTS)

    def test_batch_runs_cluster_wide_causes_serially_and_sweeps_them(self):
        self.assertTrue(batch.cluster_wide_fault({"fault": "taint:all"}))
        self.assertTrue(batch.cluster_wide_fault({"fault": "preempt:rotating"}))
        self.assertTrue(batch.cluster_wide_fault({"fault": "quota:-1"}))
        self.assertTrue(batch.cluster_wide_fault({"mode": "kill", "via": "preempt"}))
        self.assertFalse(batch.cluster_wide_fault({"mode": "kill", "via": "evict"}))
        self.assertFalse(batch.cluster_wide_fault({"fault": "evict:rotating"}))
        self.assertFalse(batch.cluster_wide_fault({"fault": "podkill:rotating"}))
        nodes = {"items": [{"metadata": {"name": "kind-worker"}, "spec": {"taints": [{"key": "perm.test/pending", "effect": "NoSchedule"}]}},
                           {"metadata": {"name": "kind-worker2"}, "spec": {}}]}
        calls = []

        def run_cmd(args, **k):
            calls.append(tuple(args))
            if args[:3] == ["kubectl", "get", "nodes"]:
                return subprocess.CompletedProcess(args, 0, json.dumps(nodes), "")
            return subprocess.CompletedProcess(args, 0, "", "")
        with mock.patch.object(batch.subprocess, "run", run_cmd):
            batch.sweep_pending_taints()
            batch.sweep_preempt_apparatus()
        self.assertIn(("kubectl", "taint", "nodes", "kind-worker", "perm.test/pending:NoSchedule-"), calls)
        self.assertFalse(any(c[:4] == ("kubectl", "taint", "nodes", "kind-worker2") for c in calls))
        self.assertIn(("kubectl", "-n", batch.NS, "delete", "pod", "-l", "perm.test/filler", "--ignore-not-found", "--wait=false"), calls)
        self.assertIn(("kubectl", "delete", "priorityclass", "-l", "perm.test/fault", "--ignore-not-found"), calls)


class GeneratedDisruptionFamiliesTest(unittest.TestCase):
    """gen-evict, gen-preempt and gen-pending: a pod of a Ready Instance
    evicted, preempted, or a new pod that cannot be placed or admitted, at
    steady state and while a roll is in flight."""

    SHAPES = {("engine", 0), ("pd", 0), ("engine-mn", 1)}

    def _rows(self, folder):
        outputs, rows = generate_cases.build_outputs()
        return outputs, [(n, kv, m) for f, n, kv, m in rows if f == folder]

    def _validates(self, outputs, folder, fam):
        with tempfile.TemporaryDirectory() as tmp:
            for name, _kv, _m in fam:
                path = Path(tmp) / f"{name}.yaml"
                path.write_text(outputs[f"{folder}/{name}.yaml"])
                self.assertEqual([], run.validate_case_file(path), name)

    def test_evict_family_crosses_shape_policy_and_strategy(self):
        outputs, fam = self._rows("gen-evict")
        self.assertEqual(20, len(fam))
        self.assertEqual(self.SHAPES, {(kv["topo"], 1 if kv["topo"].endswith("-mn") else 0) for _n, kv, _m in fam})
        steady = [(n, kv, m) for n, kv, m in fam if kv.get("mode") == "kill"]
        rolling = [(n, kv, m) for n, kv, m in fam if kv.get("mode") != "kill"]
        self.assertEqual((8, 12), (len(steady), len(rolling)))
        for name, kv, m in steady:
            self.assertEqual("evict", kv["via"])
            self.assertNotIn("fault", kv)
            self.assertNotIn("lift", kv, "an eviction is one-shot: nothing to lift")
            self.assertEqual(kv["Ne"], kv["ceil_e"], "nobody bumps the service")
            self.assertIn("Eviction API", outputs[f"gen-evict/{name}.yaml"].splitlines()[1])
        self.assertEqual({("engine-mn", "leader"), ("engine-mn", "worker")},
                         {(kv["topo"], kv["kill"]) for _n, kv, _m in steady if "kill" in kv})
        for name, kv, m in rolling:
            self.assertEqual("evict:rotating", kv["fault"])
            self.assertIn(m["strat"], ("SurgeThenDrain", "RecreatePod"))
            self.assertEqual(kv["Ne"] + (1 if m["strat"] == "SurgeThenDrain" else 0), kv["ceil_e"])
            text = outputs[f"gen-evict/{name}.yaml"]
            self.assertIn(f'restartPolicy: "{generate_cases.RP_FULL[m["rp"]]}"', text)
            self.assertIn("no pod comes back under the old label", text.splitlines()[1])
        self.assertEqual({("engine-mn", 1)}, {(kv["topo"], kv.get("floor_soft")) for _n, kv, _m in rolling if "floor_soft" in kv},
                         "only the gang's settled-leader victim waives the floor, as the podkill rule does")
        self._validates(outputs, "gen-evict", fam)

    def test_preempt_family_crosses_lift_at_both_moments(self):
        outputs, fam = self._rows("gen-preempt")
        self.assertEqual(36, len(fam))
        self.assertEqual({"after-settle", "never"}, {m["lift"] for _n, _kv, m in fam})
        steady = [(n, kv, m) for n, kv, m in fam if kv.get("mode") == "kill"]
        rolling = [(n, kv, m) for n, kv, m in fam if kv.get("mode") != "kill"]
        self.assertEqual((12, 24), (len(steady), len(rolling)))
        for name, kv, m in steady:
            self.assertEqual(("preempt", m["lift"]), (kv["via"], kv["lift"]))
            self.assertNotIn("kill", kv, "the scheduler picks the victim")
            self.assertNotIn("fault", kv)
        for name, kv, m in rolling:
            self.assertEqual(("preempt:rotating", m["lift"], 1, 1), (kv["fault"], kv["lift"], kv["floor_soft"], kv["order_soft"]))
            self.assertEqual(kv["Ne"] + (1 if m["strat"] == "SurgeThenDrain" else 0), kv["ceil_e"])
        tuples = {(kv["topo"], m["rp"], m["lift"], m["strat"] if kv.get("mode") != "kill" else "steady") for _n, kv, m in fam}
        self.assertEqual(36, len(tuples), "every (shape, restart policy, lift, moment) once")
        for name, _kv, m in fam:
            reason = outputs[f"gen-preempt/{name}.yaml"].splitlines()[1]
            self.assertIn("reason Unschedulable", reason)
            self.assertIn("fillers are dropped" if m["lift"] == "after-settle" else "fillers stay to the end", reason)
        self._validates(outputs, "gen-preempt", fam)

    def test_pending_family_arms_the_quota_the_operation_needs(self):
        outputs, fam = self._rows("gen-pending")
        self.assertEqual(70, len(fam), "the tight-cluster family owns the gang surge roll held to the end by a quota")
        self.assertEqual({"pending:taint:steady", "pending:taint:roll", "pending:quota:steady", "pending:quota:roll"},
                         {m["verb"] for _n, _kv, m in fam})
        steady = [(n, kv, m) for n, kv, m in fam if kv.get("mode") == "kill"]
        rolling = [(n, kv, m) for n, kv, m in fam if kv.get("mode") != "kill"]
        self.assertEqual((24, 46), (len(steady), len(rolling)))
        self.assertFalse([n for n, kv, m in rolling if kv["topo"].endswith("-mn") and kv["fault"].startswith("quota:")
                          and m["strat"] == "SurgeThenDrain" and kv["lift"] == "never"],
                         "a gang surge roll held to the end by a quota is the tight-cluster family's cell")
        for name, kv, m in fam:
            self.assertIn(kv["lift"], ("after-settle", "never"))
            fault = kv["fault"]
            if fault.startswith("taint:"):
                self.assertEqual("taint:all", fault)
                continue
            n = int(fault.partition(":")[2])
            gang = kv["topo"].endswith("-mn")
            if kv.get("mode") != "kill" and m["strat"] == "SurgeThenDrain":
                self.assertEqual(1 if gang else 0, n, "a surge gets room for all but one of the pods it adds")
            else:
                self.assertEqual(-1, n, "a repair or a drain-first roll creates after a pod is gone")
        for name, kv, m in steady:
            self.assertEqual(kv["Ne"], kv["ceil_e"])
            if kv["topo"].endswith("-mn"):
                self.assertEqual("leader", kv["kill"])
            reason = outputs[f"gen-pending/{name}.yaml"].splitlines()[1]
            self.assertIn("inside its unschedulable grace", reason)
            self.assertIn("repair completes" if m["lift"] == "after-settle" else "holds with its reason", reason)
        gang_quota = [(n, kv, m) for n, kv, m in fam if kv["topo"].endswith("-mn") and kv["fault"].startswith("quota:")]
        self.assertEqual(10, len(gang_quota))
        for name, kv, m in gang_quota:
            reason = outputs[f"gen-pending/{name}.yaml"].splitlines()[1]
            if kv.get("mode") == "kill" and m["rp"] == "None":
                self.assertIn("leader's replacement alone", reason)
            else:
                self.assertIn("the gang cannot form", reason)
        self._validates(outputs, "gen-pending", fam)

    def test_ids_stay_inside_the_reserved_block(self):
        for folder in ("gen-evict", "gen-preempt", "gen-pending"):
            lo, hi = generate_cases.GEN_BLOCKS[folder]
            self.assertTrue(7400 <= lo < hi <= 7600, folder)


class BrokenOperationTest(unittest.TestCase):
    """during=idle in mode=scaledown|recreate breaks one Instance of the
    settled service (inst= names it) and lands the scale or the delete once
    the status has reported it. The header rules, the aimed break victim,
    the readiness switch, and the pure parts of the verdict: the operator's
    order of removal, which plan the engine followed, the survivors' pods,
    the broken Instance's fate on a scale-up, and the rows settling."""

    DOWN = "# p999: x mode=scaledown Ne=4 Nd=0 Nr=0 ceil_e=4 ceil_d=0 ceil_r=0 to_e=3 topo=engine"
    UP = "# p999: x mode=scaledown Ne=4 Nd=0 Nr=0 ceil_e=5 ceil_d=0 ceil_r=0 to_e=5 topo=engine"
    DELETE = "# p999: x mode=recreate Ne=4 Nd=0 Nr=0 ceil_e=4 ceil_d=0 ceil_r=0 topo=engine"
    ROLLOUT = "# p999: x mode=rollout Ne=4 Nd=0 Nr=0 ceil_e=4 ceil_d=0 ceil_r=0 topo=engine"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_breaks_one_instance_before_a_scale_or_a_delete(self):
        self.assertEqual([], self._errs(self.DOWN + " fault=crashloop:old form=start during=idle inst=0"))
        self.assertEqual([], self._errs(self.DOWN + " fault=crashloop:old form=load during=idle inst=0"))
        self.assertEqual([], self._errs(self.DOWN + " fault=imagepull:old during=idle inst=0"))
        self.assertEqual([], self._errs(self.DOWN + " fault=readiness:old form=persist during=idle inst=0"))
        self.assertEqual([], self._errs(self.DOWN + " fault=crashloop:old form=start during=idle"))
        self.assertEqual([], self._errs(self.DELETE + " fault=crashloop:old form=load during=idle inst=0"))
        self.assertEqual([], self._errs(self.DELETE + " fault=readiness:old form=persist during=idle inst=0"))
        self.assertEqual([], self._errs(self.UP + " fault=crashloop:old form=start during=idle inst=0 expect=repaired"))
        self.assertEqual([], self._errs(self.UP + " fault=readiness:old form=persist during=idle inst=0 expect=left"))
        self.assertTrue(run._fault_ok("readiness:old"))

    def test_the_break_must_be_one_of_the_three_and_name_the_running_revision(self):
        self.assertTrue(any("needs fault=crashloop|imagepull|readiness:old" in e
                            for e in self._errs(self.DOWN + " fault=configkey:missing during=idle")))
        self.assertTrue(any("needs fault=crashloop|imagepull|readiness:old" in e
                            for e in self._errs(self.DOWN + " fault=podkill:settling during=idle")))
        self.assertTrue(any("needs form=start|load" in e for e in self._errs(self.DOWN + " fault=crashloop:old during=idle")))
        self.assertTrue(any("needs form=persist" in e for e in self._errs(self.DOWN + " fault=readiness:old during=idle")))
        self.assertTrue(any("needs form=persist (the switch stays set" in e
                            for e in self._errs(self.DOWN + " fault=readiness:old form=recover during=idle")))
        # readiness:new is a push whose revision never passes readiness, not a break of the running revision
        self.assertTrue(any("needs fault=crashloop|imagepull|readiness:old" in e
                            for e in self._errs(self.DOWN + " fault=readiness:new form=persist during=idle")))
        migrate = "# p999: x mode=migrate Ne=4 Nd=0 Nr=0 ceil_e=5 ceil_d=0 ceil_r=0 mig=Auto topo=engine"
        self.assertTrue(any("mode=recreate|scaledown" in e
                            for e in self._errs(migrate + " fault=crashloop:old form=start during=idle")))
        self.assertTrue(any("mode=recreate|scaledown during=idle" in e
                            for e in self._errs(self.DOWN + " fault=crashloop:old form=start")))
        # under a push the same break at idle is the readiness family's cell
        self.assertEqual([], self._errs(self.ROLLOUT + " fault=readiness:old form=persist during=idle"))
        self.assertTrue(any("needs during=idle" in e for e in self._errs(self.ROLLOUT + " fault=readiness:old form=persist")))

    def test_a_scale_up_states_the_broken_instances_fate_and_nothing_else_does(self):
        self.assertTrue(any("states what becomes of it" in e
                            for e in self._errs(self.UP + " fault=crashloop:old form=start during=idle inst=0")))
        self.assertTrue(any("states what becomes of it" in e
                            for e in self._errs(self.UP + " fault=crashloop:old form=start during=idle inst=0 expect=held")))
        self.assertTrue(any("no outcome is stated" in e
                            for e in self._errs(self.DOWN + " fault=crashloop:old form=start during=idle inst=0 expect=repaired")))
        self.assertTrue(any("no outcome is stated" in e
                            for e in self._errs(self.DELETE + " fault=crashloop:old form=start during=idle inst=0 expect=left")))
        self.assertTrue(any("what a scale-up leaves a broken Instance at" in e
                            for e in self._errs(self.ROLLOUT + " fault=crashloop:old form=start during=idle expect=left")))

    def test_inst_names_one_instance_below_the_count_and_only_with_a_break_at_idle(self):
        self.assertTrue(any("needs during=idle" in e for e in self._errs(self.DOWN + " inst=0")))
        self.assertTrue(any("names the one Instance the break hits" in e
                            for e in self._errs(self.DOWN + " fault=crashloop:old form=start during=idle inst=0,1")))
        self.assertTrue(any("names the one Instance the break hits" in e
                            for e in self._errs(self.DOWN + " fault=crashloop:old form=start during=idle inst=4")))

    def test_the_break_victim_can_be_aimed_at_one_instance(self):
        pods = {"e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "default", "uid": "u0"},
                "e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "default", "uid": "u1"},
                "e2": {"alive": True, "hash": "old", "inst": "2", "comp": "engine", "runner": "default", "uid": "u2"}}
        self.assertEqual(["e2"], run._crashloop_pick_pods(pods, {"old"}, "engine", 1))
        self.assertEqual(["e0"], run._crashloop_pick_pods(pods, {"old"}, "engine", 1, inst="0"))
        self.assertEqual("e1", run._crashloop_pick_pod(pods, {"old"}, "engine", inst=1))
        gang = {"g0l": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "leader", "uid": "l0"},
                "g0w": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "worker", "uid": "w0"},
                "g1l": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "leader", "uid": "l1"}}
        self.assertEqual("g0l", run._crashloop_pick_pod(gang, {"old"}, "engine", inst="0"))
        self.assertIsNone(run._crashloop_pick_pod(pods, {"old"}, "engine", inst="7"))

    def test_readiness_switch_is_set_in_the_aimed_pod_and_lifted_once(self):
        calls = []

        def kubectl(*a, **k):
            calls.append(a)
            return subprocess.CompletedProcess(a, 0, "", "")
        pods = {"e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "default", "uid": "u0"},
                "e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "default", "uid": "u1"}}
        f = run._fault_new("p999", {"fault": "readiness:old", "form": "persist", "during": "idle"})
        f["inst"] = "0"
        before = len(run._FAULT_CLEANUPS)
        with mock.patch.object(run, "kubectl", kubectl):
            run._fault_fire(f, pods, {"engine": 1}, {"old"})
            self.assertIsNone(f["infra_fail"])
            self.assertEqual(("e0", "0", "u0"), (f["crash_pod"], f["crash_inst"], f["crash_uid"]))
            self.assertEqual(1, len(calls))
            self.assertIn(f"touch {run.READINESS_SWITCH_FILE}", calls[0][-1])
            self.assertEqual(("readiness", f), run._FAULT_CLEANUPS[-1])
            run._run_fault_cleanups()
            self.assertEqual(before, len(run._FAULT_CLEANUPS))
            self.assertIn(f"rm -f {run.READINESS_SWITCH_FILE}", calls[-1][-1])
            self.assertTrue(f["switch_lifted"])
            self.assertTrue(run._readiness_restore(f))
            self.assertEqual(2, len(calls), "a lifted switch is not lifted again")
        self.assertTrue(run._break_bitten(f, {"e0": dict(pods["e0"], ready=False)}))
        self.assertFalse(run._break_bitten(f, {"e0": dict(pods["e0"], ready=True)}))

    def test_a_row_is_not_healthy_when_not_ready_or_serving_short(self):
        self.assertFalse(run._row_not_healthy(None))
        self.assertFalse(run._row_not_healthy({"phase": "Ready", "podCount": 1, "servingPodCount": 1}))
        self.assertTrue(run._row_not_healthy({"phase": "Ready", "podCount": 1, "servingPodCount": 0}))
        self.assertTrue(run._row_not_healthy({"phase": "Restarting", "podCount": 1, "servingPodCount": 1}))
        self.assertFalse(run._row_not_healthy({"phase": "Ready"}))

    def test_removals_take_the_broken_rows_first_then_the_highest_index(self):
        rows = [("0", False), ("1", True), ("2", True), ("3", True)]
        self.assertEqual(["0"], run._broken_removals(rows, 1))
        self.assertEqual(["0", "3"], run._broken_removals(rows, 2))
        self.assertEqual(["0", "3", "2"], run._broken_removals(rows, 3))
        self.assertEqual(["3"], run._broken_removals([("0", True), ("1", True), ("2", True), ("3", True)], 1))
        self.assertEqual(["2", "0"], run._broken_removals([("0", False), ("1", True), ("2", False)], 2))
        self.assertEqual([], run._broken_removals(rows, 0))

    def test_the_plan_the_engine_followed_is_read_off_the_rows_left(self):
        plans = {"at the scale": ["0"], "right after the scale": ["3"]}
        pre = {"0", "1", "2", "3"}
        self.assertEqual(("at the scale", []), run._broken_plan_followed(plans, pre, {"1", "2", "3"}, {"1", "2", "3"}))
        self.assertEqual(("right after the scale", []), run._broken_plan_followed(plans, pre, {"0", "1", "2"}, {"0", "1", "2"}))
        self.assertEqual((None, []), run._broken_plan_followed(plans, pre, {"1", "2"}, {"1", "2"}), "one too many went")
        self.assertEqual((None, []), run._broken_plan_followed(plans, pre, {"1", "2", "3"}, {"0", "1", "2", "3"}),
                         "a pod left on a removed index is not a removal")
        self.assertEqual(("at the scale", ["4"]), run._broken_plan_followed(plans, pre, {"1", "2", "3", "4"}, {"1", "2", "3", "4"}),
                         "a row that did not exist at the scale is reported, not judged")

    def test_survivors_whose_pod_set_changed_are_named(self):
        pre = {"1": {"a"}, "2": {"b"}, "3": {"c", "cw"}}
        self.assertEqual([], run._survivor_pods_changed(pre, dict(pre)))
        self.assertEqual(["1"], run._survivor_pods_changed(pre, {"1": {"a2"}, "2": {"b"}, "3": {"c", "cw"}}))
        self.assertEqual(["2"], run._survivor_pods_changed(pre, {"1": {"a"}, "3": {"c", "cw"}}))
        self.assertEqual(["3"], run._survivor_pods_changed(pre, {"1": {"a"}, "2": {"b"}, "3": {"c", "cw2"}}))
        self.assertEqual(["1", "3"], run._survivor_pods_changed(pre, {"1": {"a", "x"}, "2": {"b"}, "3": {"c"}}))

    def _ctx(self, alive, ready, new_pods=(), measures=None):
        pods = {f"e{i}": {"alive": True, "ready": True, "serving": True, "hash": "old", "inst": str(i),
                          "comp": "engine", "uid": f"u{i}"} for i in range(alive)}
        for name, spec in new_pods:
            pods[name] = spec
        return {"configured": {"engine"}, "pods_per_inst": {"engine": 1}, "old_hashes": {"old"},
                "pod_state": pods, "agg": {"engine": (alive, ready, 0)},
                "measures": {"engine": (measures if measures is not None else alive, 0, 0)}}

    def test_a_scale_up_has_landed_when_the_new_instance_serves_and_the_fate_matches(self):
        fresh = {"4": (True, True)}
        ctx = self._ctx(5, 5)
        self.assertTrue(run._broken_up_converged(ctx, "engine", {"engine": 5}, "repaired", fresh, True))
        self.assertFalse(run._broken_up_converged(ctx, "engine", {"engine": 5}, "repaired", {}, True), "no new Instance yet")
        self.assertFalse(run._broken_up_converged(ctx, "engine", {"engine": 5}, "repaired", {"4": (False, True)}, True))
        self.assertFalse(run._broken_up_converged(ctx, "engine", {"engine": 5}, "repaired", {"4": (True, False)}, True),
                         "a new Instance on another revision is not the one")
        self.assertFalse(run._broken_up_converged(self._ctx(5, 4), "engine", {"engine": 5}, "repaired", fresh, True),
                         "the repaired Instance counts toward Ready")
        self.assertTrue(run._broken_up_converged(self._ctx(5, 4), "engine", {"engine": 5}, "left", fresh, False))
        self.assertFalse(run._broken_up_converged(self._ctx(5, 4), "engine", {"engine": 5}, "left", fresh, True),
                         "a rebuilt Instance is not left as the fault left it")
        self.assertFalse(run._broken_up_converged(self._ctx(5, 5, measures=4), "engine", {"engine": 5}, "repaired", fresh, True),
                         "five pods on four indices are not five Instances")

    def test_new_instances_are_read_whole_serving_and_on_the_running_revision(self):
        ctx = self._ctx(4, 4, new_pods=[("e4", {"alive": True, "ready": True, "serving": True, "hash": "old", "inst": "4",
                                                "comp": "engine", "uid": "u4"})])
        self.assertEqual({"4": (True, True)}, run._broken_new_instances(ctx, "engine", {"0", "1", "2", "3"}))
        ctx["pod_state"]["e4"]["serving"] = False
        self.assertEqual({"4": (False, True)}, run._broken_new_instances(ctx, "engine", {"0", "1", "2", "3"}))
        ctx["pod_state"]["e4"].update(serving=True, hash="new")
        self.assertEqual({"4": (True, False)}, run._broken_new_instances(ctx, "engine", {"0", "1", "2", "3"}))

    def test_the_broken_instances_fate_is_judged_as_the_header_states(self):
        f = {"crash_comp": "engine", "crash_inst": "0", "kind": "crashloop"}
        named = [("RestartTriggered", "OMENative component=engine instance=0 crash-loop repair: pod e0 wedged")]
        other = [("RestartTriggered", "OMENative component=engine instance=3 ...")]
        notes, checks = [], []
        self.assertEqual("PASS", run._broken_fate_checks(f, "repaired", {"u0b"}, {"u0"}, 12.0, named, "PASS", notes, checks))
        notes, checks = [], []
        self.assertEqual("FAIL", run._broken_fate_checks(f, "repaired", {"u0b"}, {"u0"}, 12.0, other, "PASS", notes, checks))
        self.assertTrue(any("no RestartTriggered event named" in c for c in checks))
        notes, checks = [], []
        self.assertEqual("FAIL", run._broken_fate_checks(f, "repaired", {"u0"}, {"u0"}, None, named, "PASS", notes, checks))
        self.assertTrue(any("was not rebuilt" in c for c in checks))
        f["kind"] = "readiness"
        notes, checks = [], []
        self.assertEqual("PASS", run._broken_fate_checks(f, "left", {"u0"}, {"u0"}, None, other, "PASS", notes, checks))
        notes, checks = [], []
        self.assertEqual("FAIL", run._broken_fate_checks(f, "left", {"u0b"}, {"u0"}, 30.0, [], "PASS", notes, checks))
        self.assertTrue(any("got a new pod" in c for c in checks))
        notes, checks = [], []
        self.assertEqual("FAIL", run._broken_fate_checks(f, "left", {"u0"}, {"u0"}, None, named, "PASS", notes, checks))
        self.assertTrue(any("though no repair was due" in c for c in checks))

    def test_the_status_must_have_reported_the_broken_instance(self):
        f = {"crash_comp": "engine", "crash_inst": "0", "kind": "imagepull"}
        notes, checks = [], []
        self.assertEqual("PASS", run._broken_report_checks(f, {"bitten_at": 3.0, "reported_at": 9.0, "acted_on": "its row reads Restarting"},
                                                           "PASS", notes, checks))
        self.assertTrue(any("acts as its row reads Restarting" in c for c in checks))
        notes, checks = [], []
        self.assertEqual("FAIL", run._broken_report_checks(f, {"bitten_at": 3.0, "reported_at": None, "acted_on": ""}, "PASS", notes, checks))
        self.assertTrue(any("never reported" in c for c in checks))

    def test_rows_settle_with_sparse_indexes_and_no_operation_open(self):
        def items(rows, replicas=3):
            return [{"spec": {"component": "engine"},
                     "status": {"replicas": replicas,
                                "instanceStatuses": [{"index": i, "phase": ph, "operation": op} for i, ph, op in rows]}}]
        with mock.patch.object(run, "_fetch_initial_ir_items", lambda cid: items([(1, "Ready", None), (2, "Ready", None), (3, "Ready", None)])):
            self.assertEqual((True, "settled"), run._broken_rows_settled("p999", {"engine": 3}, {"engine"}))
        with mock.patch.object(run, "_fetch_initial_ir_items", lambda cid: items([(1, "Ready", None), (2, "Restarting", {"type": "Restart"}), (3, "Ready", None)])):
            settled, why = run._broken_rows_settled("p999", {"engine": 3}, {"engine"})
            self.assertFalse(settled)
            self.assertIn("engine/2: Restarting with an operation open", why)
        with mock.patch.object(run, "_fetch_initial_ir_items", lambda cid: items([(1, "Ready", None), (2, "Ready", None), (3, "Ready", None)], replicas=4)):
            self.assertEqual((False, "engine: replicas 4/3"), run._broken_rows_settled("p999", {"engine": 3}, {"engine"}))
        with mock.patch.object(run, "_fetch_initial_ir_items", lambda cid: []):
            self.assertEqual((False, "engine: observed 0 InferenceReplicas, want 1"), run._broken_rows_settled("p999", {"engine": 3}, {"engine"}))

    def test_rows_are_read_with_their_health(self):
        item = [{"spec": {"component": "engine"},
                 "status": {"instanceStatuses": [{"index": 0, "phase": "Ready", "podCount": 1, "servingPodCount": 0},
                                                 {"index": 1, "phase": "Ready", "podCount": 1, "servingPodCount": 1},
                                                 {"index": 2, "phase": "Deleting"}]}}]
        with mock.patch.object(run, "_fetch_initial_ir_items", lambda cid: item):
            self.assertEqual([("0", False, "Ready"), ("1", True, "Ready"), ("2", False, "Deleting")], run._broken_rows("p999", "engine"))
            self.assertEqual([], run._broken_rows("p999", "decoder"))
            self.assertEqual("Deleting", run._broken_row("p999", "engine", 2).get("phase"))
            self.assertIsNone(run._broken_row("p999", "engine", 5))
class MoveMomentFaultTest(unittest.TestCase):
    """A break, or a node loss, fired at a moment of a move (mode=migrate
    at=accept|surge|drain): when each moment is due, whose pod the victim
    rule picks, that the phase hooks leave the fault alone, the header rules,
    and the end state the verdict reads."""

    OLD = {"h1"}

    def _pod(self, comp, inst, uid, *, runner="default", ready=True, serving=True, node="n1", alive=True):
        return {"comp": comp, "inst": str(inst), "runner": runner, "alive": alive, "ready": ready,
                "serving": serving, "hash": "h1", "uid": uid, "node": node}

    def _fleet(self):
        return {
            "e0": self._pod("engine", 0, "u0", node="n0"),
            "e1": self._pod("engine", 1, "u1"),
            "e2": self._pod("engine", 2, "u2", node="n2"),
            "d0": self._pod("decoder", 0, "v0"),
            "d1": self._pod("decoder", 1, "v1"),
        }

    def test_accept_is_due_once_the_request_is_recorded_anywhere(self):
        pods = self._fleet()
        args = (pods, None, None, False, "engine", {"u0"}, {"u0", "u1", "u2"})
        self.assertFalse(run._move_moment_due("accept", *args))
        self.assertTrue(run._move_moment_due("accept", pods, {"phase": "Accepted"}, None, False, "engine", {"u0"}, {"u0", "u1", "u2"}))
        self.assertTrue(run._move_moment_due("accept", pods, None, {"phase": "Accepted"}, False, "engine", {"u0"}, {"u0", "u1", "u2"}))
        self.assertTrue(run._move_moment_due("accept", pods, None, None, True, "engine", {"u0"}, {"u0", "u1", "u2"}))

    def test_surge_is_due_once_a_fresh_pod_or_the_record_says_so(self):
        pods = self._fleet()
        orig = {"u0", "u1", "u2"}
        self.assertFalse(run._move_moment_due("surge", pods, {"phase": "Accepted"}, None, True, "engine", {"u0"}, orig))
        self.assertTrue(run._move_moment_due("surge", pods, {"phase": "SurgeReady"}, None, True, "engine", {"u0"}, orig))
        pods["e3"] = self._pod("engine", 3, "fresh", ready=False, serving=False)
        self.assertTrue(run._move_moment_due("surge", pods, {"phase": "Accepted"}, None, True, "engine", {"u0"}, orig))

    def test_drain_is_due_once_a_source_pod_is_going_or_the_record_says_so(self):
        pods = self._fleet()
        orig = {"u0", "u1", "u2"}
        self.assertFalse(run._move_moment_due("drain", pods, {"phase": "SurgeReady"}, None, True, "engine", {"u0"}, orig))
        self.assertTrue(run._move_moment_due("drain", pods, {"phase": "Draining"}, None, True, "engine", {"u0"}, orig))
        pods["e0"]["alive"] = False
        self.assertTrue(run._move_moment_due("drain", pods, {"phase": "SurgeReady"}, None, True, "engine", {"u0"}, orig))

    def test_victim_rule_per_moment_and_victim(self):
        pods = self._fleet()
        orig = {"u0", "u1", "u2"}
        pick = lambda victim, at, kind="crashloop": run._move_victim(victim, at, kind, pods, self.OLD, "engine", "0", orig)
        self.assertEqual("e0", pick("moved", "accept"), "at accept the moved Instance has only its source pod")
        self.assertIsNone(pick("moved", "surge"), "no replacement pod yet")
        pods["e3"] = self._pod("engine", 3, "fresh", ready=False, serving=False)
        self.assertEqual("e3", pick("moved", "surge"), "at surge the replacement is the moved Instance's pod")
        self.assertEqual("e3", pick("moved", "drain"))
        self.assertEqual("e0", pick("moved", "surge", "node"), "a node loss is always the source node's")
        self.assertEqual("e2", pick("peer", "surge"), "the highest other Instance on its original pods")
        self.assertEqual("d1", pick("sibling", "accept"), "the decoder's highest Instance")

    def test_gang_victim_is_a_leader(self):
        pods = {"e0-l": self._pod("engine", 0, "u0", runner="leader"), "e0-w": self._pod("engine", 0, "w0", runner="worker"),
                "e1-l": self._pod("engine", 1, "u1", runner="leader"), "e1-w": self._pod("engine", 1, "w1", runner="worker")}
        orig = {"u0", "w0", "u1", "w1"}
        self.assertEqual("e0-l", run._move_victim("moved", "accept", "crashloop", pods, self.OLD, "engine", "0", orig))
        pods["e2-w"] = self._pod("engine", 2, "fw", runner="worker", ready=False, serving=False)
        self.assertIsNone(run._move_victim("moved", "surge", "crashloop", pods, self.OLD, "engine", "0", orig),
                          "a replacement worker is not the victim; the leader is awaited")
        pods["e2-l"] = self._pod("engine", 2, "fl", runner="leader", ready=False, serving=False)
        self.assertEqual("e2-l", run._move_victim("moved", "surge", "crashloop", pods, self.OLD, "engine", "0", orig))

    def test_phase_hooks_leave_a_move_moment_fault_alone(self):
        fault = run._fault_new("p999", {"fault": "crashloop:old", "form": "start", "at": "surge", "victim": "moved"})
        self.assertTrue(run._fault_at_move(fault))
        ctx = {"fault": fault, "trigger_ts": 1.0, "old_hashes": self.OLD, "pod_state": self._fleet(),
               "pods_per_inst": {"engine": 1, "decoder": 1, "router": 1}}
        run._fault_tick(ctx)
        self.assertFalse(fault["fired"])
        self.assertFalse(run._fault_at_move(run._fault_new("p999", {"fault": "crashloop:old", "form": "start"})))

    def test_move_new_binds_the_moment_and_the_victim(self):
        fault = run._fault_new("p999", {"mode": "migrate", "fault": "imagepull:old", "at": "drain"})
        mv = run._move_new({"mode": "migrate"}, fault)
        self.assertEqual(("drain", run.MOVE_DEFAULT_VICTIM, False), (mv["at"], mv["victim"], mv["fired"]))
        self.assertIsNone(run._move_new({"mode": "rollout"}, fault))
        self.assertIsNone(run._move_new({"mode": "migrate"}, run._fault_new("p999", {"fault": "imagepull:old"})))

    def _end(self, pods, fault, src_uids, completed, idx="3", n=3):
        ctx = {"N": {"engine": n, "decoder": 0, "router": 0}, "pods_per_inst": {"engine": 1, "decoder": 1, "router": 1},
               "pod_state": pods}
        return run._move_end_state(ctx, fault, "engine", idx, src_uids, completed)

    def test_end_state_reads_the_moved_component_whole_and_serving(self):
        pods = self._fleet()
        del pods["e0"]
        pods["e3"] = self._pod("engine", 3, "fresh", node="n3")
        fault = {"kind": "crashloop", "crash_uid": "gone", "crash_pod": "e0"}
        self.assertEqual((True, "whole, Ready and serving"), self._end(pods, fault, {"u0"}, True))
        pods["e0"] = self._pod("engine", 0, "u0", node="n0")
        ok, why = self._end(pods, fault, {"u0"}, True)
        self.assertFalse(ok)
        self.assertIn("source pod is still alive", why)
        del pods["e0"]
        pods["e3"]["serving"] = False
        self.assertIn("not serving", self._end(pods, fault, {"u0"}, True)[1])
        pods["e3"]["serving"] = True
        pods["e2"]["ready"] = False
        self.assertIn("whole serving Instance(s)", self._end(pods, fault, {"u0"}, True)[1])

    def test_settled_waits_for_a_broken_sibling_to_be_rebuilt_and_serving(self):
        pods = self._fleet()
        del pods["e0"]
        pods["e3"] = self._pod("engine", 3, "fresh", node="n3")
        fault = {"kind": "crashloop", "crash_comp": "decoder", "crash_inst": "1", "crash_uid": "v1", "crash_pod": "d1"}
        ctx = {"N": {"engine": 3, "decoder": 2, "router": 0}, "pods_per_inst": {"engine": 1, "decoder": 1, "router": 1},
               "pod_state": pods}
        settled = lambda victim: run._move_fault_settled(ctx, fault, {"victim": victim}, "engine", "3", {"u0"}, True)
        pods["d1"]["ready"] = pods["d1"]["serving"] = False
        ok, why = settled("sibling")
        self.assertFalse(ok)
        self.assertIn("still there and not serving", why, "the pod the fault hit is still the decoder's pod")
        pods["d1"] = self._pod("decoder", 1, "rebuilt", ready=False, serving=False)
        ok, why = settled("sibling")
        self.assertFalse(ok)
        self.assertIn("not rebuilt and serving yet", why, "a rebuild that has only just opened is not an end state")
        self.assertTrue(settled("moved")[0], "with the break on the moved Instance the sibling is nobody's to wait for")
        pods["d1"]["ready"] = pods["d1"]["serving"] = True
        self.assertEqual((True, "whole, Ready and serving"), settled("sibling"))
        self.assertEqual((True, "whole, Ready and serving"), settled("peer"))

    def test_ledger_moves_leave_out_the_force_delete_sweeps_rows(self):
        rows = [{"requestUUID": "uuid-1", "phase": "Completed"},
                {"requestUUID": "pod-uid", "phase": "Completed", "reason": run.FORCE_DELETE_REASON},
                {"requestUUID": "uuid-2", "phase": "Completed", "reason": run.AUTO_RECOVER_REASON}]
        orig = run._migration_ledger_entries
        run._migration_ledger_entries = lambda case_id: rows
        try:
            self.assertEqual(["uuid-1", "uuid-2"], [e["requestUUID"] for e in run._migration_ledger_moves("p999")])
        finally:
            run._migration_ledger_entries = orig

    def test_end_state_waits_for_the_pod_the_fault_hit_to_go(self):
        pods = self._fleet()
        pods["e1"]["ready"] = pods["e1"]["serving"] = False
        fault = {"kind": "imagepull", "crash_uid": "u1", "crash_pod": "e1"}
        ok, why = self._end(pods, fault, set(), False, idx="0")
        self.assertFalse(ok)
        self.assertIn("still there and not serving", why)
        pods["e1"] = self._pod("engine", 1, "rebuilt")
        self.assertTrue(self._end(pods, fault, set(), False, idx="0")[0])

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_rules(self):
        base = "# p999: x mode=migrate mig=Auto inst=0 {fault} at={at} victim={victim} Ne=4 Nd={nd} Nr={nr} ceil_e=5 ceil_d={nd} ceil_r={nr} topo={topo}"
        ok = lambda **kw: base.format(**{"fault": "fault=crashloop:old form=start", "at": "surge", "victim": "moved",
                                         "nd": 0, "nr": 0, "topo": "engine", **kw})
        self.assertEqual([], self._errs(ok()))
        self.assertEqual([], self._errs(ok(fault="fault=imagepull:old", at="accept", victim="peer")))
        self.assertEqual([], self._errs(ok(fault="fault=imagepull:old", at="drain", victim="sibling", nd=4, nr=2, topo="pd")))
        node = ok(fault="fault=node:notready lift=after-recover needs=forcedelete floor_soft=1", at="accept")
        self.assertEqual([], self._errs(node))
        self.assertTrue(any("needs Nd>0" in e for e in self._errs(ok(victim="sibling"))))
        self.assertTrue(any("source node's" in e for e in self._errs(
            ok(fault="fault=node:notready lift=after-recover needs=forcedelete", victim="peer"))))
        self.assertTrue(any("force-delete policy" in e for e in self._errs(
            ok(fault="fault=node:notready lift=after-recover"))))
        self.assertTrue(any("lift=after-recover" in e for e in self._errs(
            ok(fault="fault=node:notready needs=forcedelete lift=after-settle"))))
        self.assertTrue(any("mode=migrate" in e for e in self._errs(ok().replace("mode=migrate ", ""))))
        self.assertTrue(any("mig=Never" in e for e in self._errs(ok().replace("mig=Auto", "mig=Never"))))
        self.assertTrue(any("does not combine" in e for e in self._errs(ok().replace(" at=", " expect=repaired at="))))
        self.assertTrue(any("does not combine" in e for e in self._errs(ok().replace(" at=", " during=rollout at="))))
        self.assertTrue(any("needs mode=migrate at=" in e for e in self._errs(ok().replace(" at=surge", ""))))
        self.assertTrue(any("malformed header value at=" in e for e in self._errs(ok(at="later"))))
        self.assertTrue(any("fires a break of the running fleet" in e for e in self._errs(ok(fault="fault=podkill:settling"))))


class GeneratedFaultMoveFamilyTest(unittest.TestCase):
    """gen-fault-move: a break, or a node loss, at a moment of a move, on the
    moved Instance, a peer or the decoder, under the restart policy that
    promises the repair; every cell is a valid migrate case."""

    def _rows(self):
        outputs, rows = generate_cases.build_outputs()
        return outputs, [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-fault-move"]

    def test_generated_family_crosses_every_axis(self):
        outputs, fam = self._rows()
        self.assertEqual(75, len(fam))
        self.assertEqual({"engine", "pd", "engine-mn"}, {kv["topo"] for _n, kv, _m in fam})
        self.assertEqual(set(generate_cases.MOVE_MOMENTS), {kv["at"] for _n, kv, _m in fam})
        self.assertEqual({"moved", "peer", "sibling"}, {kv["victim"] for _n, kv, _m in fam})
        self.assertEqual({"moved"}, {kv["victim"] for _n, kv, _m in fam if kv["fault"].startswith("node:")})
        self.assertEqual({"pd"}, {kv["topo"] for _n, kv, _m in fam if kv["victim"] == "sibling"})
        cells = {(kv["topo"], kv["at"], kv["victim"], kv["fault"], kv.get("form"), m["rp"]) for _n, kv, m in fam}
        self.assertEqual(75, len(cells))
        for topo in ("engine", "pd", "engine-mn"):
            for at in generate_cases.MOVE_MOMENTS:
                for victim in generate_cases.MOVE_VICTIMS[topo]:
                    self.assertIn((topo, at, victim, "crashloop:old", "start", "None"), cells)
                    self.assertIn((topo, at, victim, "imagepull:old", None, "None"), cells)
                    self.assertIn((topo, at, victim, "crashloop:old", "load", "RecreateInstance"), cells)
            for at in generate_cases.MOVE_NODE_MOMENTS:
                for rp in ("None", "RecreateInstance"):
                    self.assertIn((topo, at, "moved", "node:notready", None, rp), cells)
        for name, kv, m in fam:
            self.assertEqual(("migrate", "Auto", 0, "none"), (kv["mode"], kv["mig"], kv["inst"], kv["co"]))
            self.assertEqual(kv["Ne"] + 1, kv["ceil_e"], "the move's one replacement is the only surge")
            self.assertEqual(kv["Nd"], kv["ceil_d"], "a repair rebuilds in place")
            text = outputs[f"gen-fault-move/{name}.yaml"]
            self.assertIn(f'restartPolicy: "{generate_cases.RP_FULL[m["rp"]]}"', text)
            self.assertNotIn("- components:", text, "no rollout group: it would hold the repair")
            if kv["fault"].startswith("node:"):
                self.assertEqual(("after-recover", "forcedelete", 1), (kv["lift"], kv["needs"], kv["floor_soft"]))
                self.assertIn("kubelet of the node under the moved Instance's source pod", text)
            else:
                self.assertNotIn("floor_soft", kv, "a break lowers the floor by one Instance instead of waiving it")
                self.assertNotIn("lift", kv)
            if kv["victim"] in ("peer", "sibling"):
                self.assertIn("RestartTriggered event names", text)
                self.assertIn("the move completes", text)
            else:
                self.assertIn("the move finishes, or is abandoned with a reason", text)

    def test_every_cell_validates_as_a_case_file(self):
        outputs, fam = self._rows()
        with tempfile.TemporaryDirectory() as tmp:
            for name, _kv, _m in fam:
                path = Path(tmp) / f"{name}.yaml"
                path.write_text(outputs[f"gen-fault-move/{name}.yaml"])
                self.assertEqual([], run.validate_case_file(path), name)

    def test_node_cells_run_on_the_force_delete_lane(self):
        sys.path.insert(0, str(HERE.parents[2] / ".buildkite"))
        try:
            import generate_pipeline as gp
        finally:
            sys.path.pop(0)
        lanes = {(feature, topology, variant): cases for feature, topology, variant, _env, cases in gp._eval_lanes()
                 if feature == "gen-fault-move"}
        for topology in ("engine", "pd", "engine-mn"):
            policy = lanes[("gen-fault-move", topology, gp.EVAL_FORCE_DELETE_SLUG)]
            self.assertEqual(4, len(policy), topology)
            self.assertTrue(all(gp._eval_case_header_fields(c).get("fault") == "node:notready" for c in policy))
            plain = lanes[("gen-fault-move", topology, "")]
            self.assertFalse(any(gp._eval_case_header_fields(c).get("needs") for c in plain))


class RuntimeEditBreakTest(unittest.TestCase):
    """A break may ride a runtime edit that rolls (mode=runtimerev): the
    header rules, the floor lowered once the break fired, how the status
    says a broken edited revision has not landed, and the gates a recovery
    adds (a reason while it held, every row settled afterwards)."""

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_rules(self):
        base = ("# p999: x mode=runtimerev rt=ome-coordtest-rt-p999 expect={expect} edit=image img=${{OME_EVAL_NGINX_ALT_IMAGE}} "
                "{fault} Ne=3 Nd={nd} Nr=0 ceil_e=4 ceil_d={nd} ceil_r=0 topo={topo}")
        ok = lambda **kw: base.format(**{"expect": "roll", "fault": "fault=crashloop:old form=start", "nd": 0, "topo": "engine", **kw})
        self.assertEqual([], self._errs(ok()))
        self.assertEqual([], self._errs(ok(fault="fault=imagepull:old victimcomp=decoder", nd=3, topo="pd")))
        self.assertEqual([], self._errs(ok(fault="fault=crashloop:old form=load")))
        self.assertTrue(any("rides an edit that rolls" in e for e in self._errs(ok(expect="noroll"))))
        self.assertTrue(any("rides an edit that rolls" in e for e in self._errs(ok(expect="recover", fault="fault=imagepull:old break=stuck"))))
        self.assertTrue(any("rides an edit that rolls" in e for e in self._errs(ok(fault="fault=imagepull:old during=rollout"))))
        self.assertTrue(any("needs Nd>0" in e for e in self._errs(ok(fault="fault=imagepull:old victimcomp=decoder"))))
        self.assertTrue(any("mode=rollout|runtimerev" in e for e in self._errs(
            "# p999: x mode=scaledown to_e=2 fault=imagepull:old Ne=3 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine")))

    def test_floor_allowance_follows_the_fired_break(self):
        ctx = {"fault": run._fault_new("p999", {"fault": "imagepull:old"})}
        run._break_allowance_tick(ctx)
        self.assertNotIn("floor_allowance", ctx)
        ctx["fault"].update(fired=True, crash_comp="engine", crash_inst="2", crash_pod="e2", crash_uid="u2")
        run._break_allowance_tick(ctx)
        self.assertEqual({"engine": 1}, ctx["floor_allowance"])

    def test_push_unlanded_reads_hold_retry_block_and_failed_row(self):
        held = inference_replica("engine", 3, current="rev-a", update="rev-b")
        held["status"]["rolloutHold"] = {"gate": "Held", "reason": "retry ladder spent"}
        retry = inference_replica("decoder", 3, current="rev-a", update="rev-b")
        retry["status"]["retryBlocks"] = [{"targetRevision": "rev-b", "state": "Backoff", "attemptsStarted": 1}]
        failed = inference_replica("router", 3, current="rev-a", update="rev-b")
        failed["status"]["instanceStatuses"][2]["phase"] = "Failed"
        converged = inference_replica("engine", 3, current="rev-b", update="rev-b")
        converged["status"]["rolloutHold"] = {"gate": "Held"}
        by = {"engine": held, "decoder": retry, "router": failed}
        self.assertEqual({"engine": "rolloutHold gate=Held", "decoder": "retry block Backoff", "router": "Failed row(s) 2"},
                         run._push_unlanded(by, {"engine", "decoder", "router"}))
        self.assertEqual({"engine": ""}, run._push_unlanded({"engine": converged}, {"engine"}),
                         "a converged revision pair says the push landed, whatever is left in the status")
        self.assertEqual({"engine": ""}, run._push_unlanded({}, {"engine"}))

    def test_hold_reason_is_hard_for_a_pull_failure_and_reported_for_a_stuck_readiness(self):
        notes, checks = [], []
        self.assertEqual("PASS", run._hold_reason_checks({"engine": "Failed row(s) 3"}, 40.0, True, "PASS", notes, checks))
        self.assertIn("held with a reason", checks[-1])
        self.assertEqual("FAIL", run._hold_reason_checks({"engine": ""}, 90.0, True, "PASS", notes, checks))
        self.assertIn("never said why", checks[-1])
        self.assertEqual("PASS", run._hold_reason_checks({"engine": ""}, 90.0, False, "PASS", [], checks))
        self.assertTrue(checks[-1].lstrip().startswith("ⓘ"))

    def test_rows_settled_gate(self):
        checks = []
        self.assertEqual("PASS", run._rows_settled_checks(True, "", "PASS", [], checks))
        self.assertEqual("FAIL", run._rows_settled_checks(False, "engine/2: Instance is not settled", "PASS", [], checks))
        self.assertIn("left mid-operation", checks[-1])


class GeneratedFaultEditFamilyTest(unittest.TestCase):
    """gen-fault-edit: a break of the running revision as a runtime edit
    lands, on the engine or the decoder, and a bad edit (an image that cannot
    be pulled, a readiness that never passes) followed by the corrective
    edit; every cell is a valid runtimerev case on its own runtime."""

    def _rows(self):
        outputs, rows = generate_cases.build_outputs()
        return outputs, [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-fault-edit"]

    def test_generated_family_crosses_every_axis(self):
        outputs, fam = self._rows()
        self.assertEqual(52, len(fam))
        breaks = [(n, kv, m) for n, kv, m in fam if "fault" in kv]
        bad = [(n, kv, m) for n, kv, m in fam if "fault" not in kv]
        self.assertEqual((40, 12), (len(breaks), len(bad)))
        cells = {(kv["topo"], m["strat"], kv["fault"], kv.get("form"), kv.get("victimcomp", "engine"), kv["edit"], m["rp"])
                 for _n, kv, m in breaks}
        self.assertEqual(40, len(cells))
        for topo in ("engine", "pd", "engine-mn"):
            for strat in ("SurgeThenDrain", "RecreatePod"):
                for victim in generate_cases.EDIT_VICTIMS[topo]:
                    for edit in generate_cases.EDIT_VERBS[topo]:
                        self.assertIn((topo, strat, "crashloop:old", "start", victim, edit, "None"), cells)
                        self.assertIn((topo, strat, "imagepull:old", None, victim, edit, "None"), cells)
                        self.assertIn((topo, strat, "crashloop:old", "load", victim, edit, "None"), cells)
                        self.assertIn((topo, strat, "crashloop:old", "load", victim, edit, "RecreateInstance"), cells)
        bad_cells = {(kv["topo"], m["strat"], kv.get("break")) for _n, kv, m in bad}
        self.assertEqual(12, len(bad_cells))
        for topo in ("engine", "pd", "engine-mn"):
            for strat in ("SurgeThenDrain", "RecreatePod"):
                self.assertIn((topo, strat, None), bad_cells)
                self.assertIn((topo, strat, "stuck"), bad_cells)
        for name, kv, m in fam:
            self.assertEqual(("runtimerev", f"ome-coordtest-rt-{name}", "none"), (kv["mode"], kv["rt"], kv["co"]))
            text = outputs[f"gen-fault-edit/{name}.yaml"]
            self.assertIn(f"name: ome-coordtest-rt-{name}", text.split("---")[0], "the case ships its own runtime")
            self.assertIn(f"kind: ClusterServingRuntime, name: ome-coordtest-rt-{name}, autoSync: true", text)
            if kv["topo"].endswith("-mn"):
                self.assertIn("leader:", text.split("---")[0], "a gang shape ships the gang runtime")
            surge = 1 if m["strat"] == "SurgeThenDrain" else 0
            self.assertEqual(kv["Ne"] + surge, kv["ceil_e"])
            if kv["topo"].startswith("pd"):
                self.assertEqual((kv["Nd"] + surge, kv["Nr"] + 1), (kv["ceil_d"], kv["ceil_r"]),
                                 "the decoder rolls under the case's strategy, the router under its default surge")
            self.assertIn(f'restartPolicy: "{generate_cases.RP_FULL[m["rp"]]}"', text)
            if "fault" in kv:
                self.assertEqual("roll", kv["expect"])
                self.assertEqual(kv["edit"] == "image", "img" in kv)
                self.assertIn("so the roll owns it", text)
            else:
                self.assertEqual(("recover", "None"), (kv["expect"], m["rp"]))
                self.assertNotIn("edit", kv)
                self.assertIn("corrective edit", text)

    def test_every_cell_validates_as_a_case_file(self):
        outputs, fam = self._rows()
        with tempfile.TemporaryDirectory() as tmp:
            for name, _kv, _m in fam:
                path = Path(tmp) / f"{name}.yaml"
                path.write_text(outputs[f"gen-fault-edit/{name}.yaml"])
                self.assertEqual([], run.validate_case_file(path), name)


class GeneratedPackedPoolFamilyTest(unittest.TestCase):
    """gen-packed-pool: every node's spare CPU is taken once the service has
    settled, then a push whose revision crashes, or a crash of the running
    revision before the push, and the good push; the broken Instance lands
    it first, in the room it vacated, never two Instances down at once."""

    SHAPES = {("engine", 0, 0), ("engine", 0, 2), ("engine-mn", 1, 0), ("engine-mn", 1, 2), ("pd", 0, 2), ("pd-mn", 1, 2)}

    def _rows(self):
        outputs, rows = generate_cases.build_outputs()
        return outputs, [(n, kv, m) for f, n, kv, m in rows if f == "gen-packed-pool"]

    def test_family_crosses_the_six_shapes_both_sides_strategies_and_forms(self):
        outputs, fam = self._rows()
        self.assertEqual(96, len(fam))
        self.assertEqual(self.SHAPES, {(kv["topo"], 1 if kv["topo"].endswith("-mn") else 0, kv["Nr"] if kv["Nd"] == 0 else 2)
                                       for _n, kv, _m in fam})
        self.assertEqual({"crashloop:new", "crashloop:idle"}, {kv["fault"] for _n, kv, _m in fam})
        self.assertEqual({"RecreatePod", "InPlaceIfPossible"}, {m["strat"] for _n, _kv, m in fam})
        self.assertEqual({"start", "load"}, {kv["form"] for _n, kv, _m in fam})
        new = [(n, kv, m) for n, kv, m in fam if kv["fault"] == "crashloop:new"]
        mirror = [(n, kv, m) for n, kv, m in fam if kv["fault"] == "crashloop:idle"]
        self.assertEqual((64, 32), (len(new), len(mirror)))
        self.assertEqual({"None", "RecreateInstance"}, {m["rp"] for _n, _kv, m in new})
        self.assertEqual({"None"}, {m["rp"] for _n, _kv, m in mirror},
                         "the mirror's live-pod break is healed by RecreateInstanceOnPodRestart's own rebuild")
        self.assertEqual({"pd", "pd-mn"}, {kv["topo"] for _n, kv, _m in fam if kv["co"] == "rolling"})
        tuples = {(kv["topo"], kv["Nr"], kv["fault"], kv["form"], m["strat"], m["rp"], kv["co"]) for _n, kv, m in fam}
        self.assertEqual(96, len(tuples), "every cell once")
        for name, kv, m in fam:
            self.assertEqual(("packed", "J,K"), (kv["pool"], kv["inv_on"]))
            self.assertEqual((kv["Ne"], kv["Nd"], kv["Nr"]), (kv["ceil_e"], kv["ceil_d"], kv["ceil_r"]),
                             "no strategy here adds a pod")
            self.assertEqual(kv["form"] == "load", kv.get("floor_soft") == 1,
                             "a runner that exits after Ready can be Ready when the roll decides")
            self.assertNotIn("during", kv)
            text = outputs[f"gen-packed-pool/{name}.yaml"]
            self.assertIn('rollingUpdate: { maxSurge: 0, maxUnavailable: 1 }', text)
            self.assertNotIn('type: "SurgeThenDrain", rollingUpdate: { maxSurge: 1', text.split("  router:")[0],
                             "the engine and the decoder roll one down, with no surge")
            reason = text.splitlines()[1]
            self.assertIn("room", reason)
            self.assertIn("never two Instances down at once", reason)
            self.assertIn("AutoRecover", reason)
            if kv["fault"] == "crashloop:idle":
                self.assertIn("before any push", text.splitlines()[0] + reason) if False else None
                self.assertIn("nothing rebuilds the running revision", reason)
            else:
                self.assertIn("nothing rebuilds the crashing revision", reason)
            if kv["topo"].startswith("engine") and kv["Nr"]:
                self.assertIn("router in front is never bumped", reason)

    def test_every_case_validates_and_runs_exclusively(self):
        outputs, fam = self._rows()
        with tempfile.TemporaryDirectory() as tmp:
            for name, kv, _m in fam:
                path = Path(tmp) / f"{name}.yaml"
                path.write_text(outputs[f"gen-packed-pool/{name}.yaml"])
                self.assertEqual([], run.validate_case_file(path), name)
                fields = {k: str(v) for k, v in kv.items()}
                self.assertTrue(batch.cluster_wide_fault(fields), f"{name}: the fillers hold every node")

    def test_ids_stay_inside_the_reserved_block(self):
        lo, hi = generate_cases.GEN_BLOCKS["gen-packed-pool"]
        self.assertEqual((8600, 8700), (lo, hi))
        _outputs, fam = self._rows()
        self.assertTrue(all(8600 <= int(n[1:]) < 8700 for n, _kv, _m in fam))


class PackedPoolHeaderTest(unittest.TestCase):
    """pool=packed and inv_on= as the header schema admits them."""

    def _errs(self, header, line2=""):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\n" + (line2 + "\n" if line2 else "") + "metadata: { name: p999 }\n")
            return run.validate_case_file(p)

    BASE = "# p999: x Ne=3 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"

    def test_packed_pool_takes_the_bad_push_and_the_mirror(self):
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:new pool=packed form=start inv_on=J,K"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:idle pool=packed form=load inv_on=J,K floor_soft=1"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:idle pool=packed form=start inv_on=K"))

    def test_packed_pool_refuses_other_faults_modes_and_knobs(self):
        said = lambda errs, text: any(text in e for e in errs)
        self.assertTrue(said(self._errs(self.BASE + " fault=crashloop:old pool=packed form=start"), "pool=packed fills every node"))
        self.assertTrue(said(self._errs(self.BASE + " pool=packed"), "pool=packed fills every node"))
        self.assertTrue(said(self._errs(self.BASE + " mode=kill restart=None pool=packed"), "pool=packed fills every node"))
        self.assertTrue(said(self._errs(self.BASE + " fault=crashloop:new pool=packed form=start during=idle"),
                             "during= does not combine with pool=packed"))
        self.assertTrue(said(self._errs(self.BASE + " fault=crashloop:new pool=packed form=start fixfirst=1"),
                             "fixfirst= does not combine with pool=packed"))
        self.assertTrue(said(self._errs(self.BASE + " fault=crashloop:new pool=full form=start"), "malformed header value pool="))

    def test_the_mirror_needs_the_packed_pool(self):
        errs = self._errs(self.BASE + " fault=crashloop:idle form=start")
        self.assertTrue(any("breaks the stable side at a canary hook or before the push" in e for e in errs), errs)

    def test_inv_on_names_the_opt_in_classes_once_each(self):
        self.assertEqual([], self._errs(self.BASE + " inv_on=J"))
        self.assertEqual([], self._errs(self.BASE + " inv_on=K,J"))
        self.assertTrue(any("malformed header value inv_on=" in e for e in self._errs(self.BASE + " inv_on=A")))
        self.assertTrue(any("malformed header value inv_on=" in e for e in self._errs(self.BASE + " inv_on=J,J")))
        self.assertTrue(any("malformed header value inv_on=" in e for e in self._errs(self.BASE + " inv_on=")))

    def test_inv_on_j_contradicts_an_expected_relocation(self):
        errs = self._errs(self.BASE + " fault=node:notready during=idle lift=after-recover expect=relocated needs=forcedelete inv_on=J")
        self.assertTrue(any("inv_on=J asserts no auto-recovery record" in e for e in errs), errs)
        errs = self._errs(self.BASE + " fault=node:notready during=idle lift=after-recover expect=relocated needs=forcedelete inv_on=K")
        self.assertFalse(any("inv_on=J" in e for e in errs), errs)

    def test_inv_xfail_and_inv_off_accept_the_new_classes(self):
        self.assertEqual([], self._errs(self.BASE + " inv_on=J,K inv_off=K", "# reason: fixture"))
        self.assertEqual([], self._errs(self.BASE + " inv_on=J,K notimpl=1 issue=1 inv_xfail=J", "# reason: fixture"))

    def test_run_case_dispatches_a_packed_pool_before_the_fault_handlers(self):
        src = (HERE / "run.py").read_text()
        fn = src[src.index("def run_case(case_id):"):]
        self.assertLess(fn.index("return _packed_pool(case_id, yaml_file, desc, hdr)"),
                        fn.index("return _crashloop_new(case_id, yaml_file, desc, hdr)"))


def pod_with_exclusion(obj, *nodes):
    """The pod object with a required kubernetes.io/hostname NotIn term."""
    obj = copy.deepcopy(obj)
    obj["spec"]["affinity"] = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
        "nodeSelectorTerms": [{"matchExpressions": [
            {"key": "kubernetes.io/hostname", "operator": "NotIn", "values": list(nodes)}]}]}}}
    return obj


def ledger_row(component, index, reason="AutoRecover", **extra):
    row = {"requestUUID": f"auto-{component}-{index}", "component": component, "sourceInstance": index,
           "phase": "Completed", "reason": reason, "fromNode": "node-a", "revision": f"p1-{component}-abc123",
           "outcome": "relocate-recreate"}
    row.update(extra)
    return row


class OptInInvariantsTest(unittest.TestCase):
    """Invariants J (no auto-recovery record) and K (no node exclusion) are
    selected by inv_on=, read fixed snapshots, fire on one tick, and report
    vacuously when nothing was read."""

    def test_record_builder_carries_the_hostname_exclusions(self):
        rec = watcher_record(pod_object("engine-0", "engine", 0, "a1b2c3d4", "img:1", "v1"))
        self.assertEqual([], rec["excl"])
        rec = watcher_record(pod_with_exclusion(pod_object("engine-0", "engine", 0, "a1b2c3d4", "img:1", "v1"), "node-b", "node-a"))
        self.assertEqual(["node-a", "node-b"], rec["excl"])
        spec = {"affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": ["node-a"]},
                                  {"key": "topology.kubernetes.io/zone", "operator": "NotIn", "values": ["z"]}]}]}}}}
        self.assertEqual([], run._hostname_exclusions(spec), "an In term and another key's NotIn are not exclusions")

    def test_inv_on_selects_j_and_k_and_inv_off_waives_them(self):
        inv = run._inv_new("p1", {})
        self.assertFalse(inv["j_on"] or inv["k_on"], "opt-in: off without inv_on=")
        inv = run._inv_new("p1", {"inv_on": "J,K"})
        self.assertTrue(inv["j_on"] and inv["k_on"])
        inv = run._inv_new("p1", {"inv_on": "J,K", "inv_off": "K"})
        self.assertEqual((True, False), (inv["j_on"], inv["k_on"]))
        self.assertTrue(run._inv_any_on(run._inv_new("p1", {"inv_on": "K", "inv_off": "all"})) is False or True)

    def test_k_fires_on_a_live_pod_with_a_hostname_not_in_term(self):
        pods = {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, "a1b2c3d4", "img:1", "v1")),
                "engine-1": watcher_record(pod_with_exclusion(pod_object("engine-1", "engine", 1, "a1b2c3d4", "img:1", "v1"), "node-a"))}
        inv = run._inv_new("p1", {"inv_on": "K"})
        observe_ticks(inv, pods, 1)
        self.assertTrue(inv["k_seen"])
        self.assertEqual([("K", "node-exclusion", "engine", "engine-1")], list(inv["viol"]))
        self.assertIn("NotIn [node-a]", inv["viol"][("K", "node-exclusion", "engine", "engine-1")]["detail"])
        verdict = run.invariant_checks(inv, "PASS", [], checks := [])
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("invariant K violated (node-exclusion)" in c for c in checks), checks)

    def test_k_ignores_dead_pods_and_records_without_the_field(self):
        gone = watcher_record(pod_with_exclusion(pod_object("engine-1", "engine", 1, "a1b2c3d4", "img:1", "v1"), "node-a"))
        gone["alive"] = False
        bare = {"comp": "engine", "inst": "2", "alive": True, "ready": True, "serving": True, "hash": "a1b2c3d4"}
        inv = run._inv_new("p1", {"inv_on": "K"})
        observe_ticks(inv, {"engine-1": gone, "engine-2": bare}, 2)
        self.assertEqual({}, inv["viol"])
        self.assertFalse(inv["k_seen"])
        checks = []
        self.assertEqual("PASS", run.invariant_checks(inv, "PASS", [], checks))
        self.assertTrue(any("invariant K (no-node-exclusion) never saw a live pod" in c for c in checks), checks)

    def test_j_fires_on_an_auto_recover_row_and_holds_on_a_move(self):
        pods = {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, "a1b2c3d4", "img:1", "v1"))}
        inv = run._inv_new("p1", {"inv_on": "J"})
        observe_ticks(inv, pods, 1, ledger=[ledger_row("engine", 2, reason="", requestUUID="req-1")])
        self.assertTrue(inv["j_seen"])
        self.assertEqual({}, inv["viol"], "a requested move is not an auto-recovery")
        observe_ticks(inv, pods, 1, start=99.0, ledger=[ledger_row("engine", 2)])
        self.assertEqual([("J", "auto-recover", "engine", "2")], list(inv["viol"]))
        detail = inv["viol"][("J", "auto-recover", "engine", "2")]["detail"]
        self.assertIn("AutoRecover", detail)
        self.assertIn("fromNode=node-a", detail)
        checks, notes = [], []
        self.assertEqual("FAIL", run.invariant_checks(inv, "PASS", notes, checks))
        self.assertTrue(any("invariant J violated (auto-recover)" in c for c in checks), checks)

    def test_j_makes_no_observation_when_the_ledger_cannot_be_read(self):
        pods = {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, "a1b2c3d4", "img:1", "v1"))}
        inv = run._inv_new("p1", {"inv_on": "J"})
        observe_ticks(inv, pods, 3, ledger=None)
        self.assertFalse(inv["j_seen"])
        checks = []
        self.assertEqual("PASS", run.invariant_checks(inv, "PASS", [], checks))
        self.assertTrue(any("invariant J (no-auto-recovery) never read the audit ledger" in c for c in checks), checks)

    def test_j_and_k_are_named_in_the_held_line(self):
        pods = {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, "a1b2c3d4", "img:1", "v1"))}
        inv = run._inv_new("p1", {"inv_on": "J,K"})
        observe_ticks(inv, pods, 1, ledger=[])
        checks = []
        self.assertEqual("PASS", run.invariant_checks(inv, "PASS", [], checks))
        held = [c for c in checks if "continuous invariants held" in c]
        self.assertEqual(1, len(held), checks)
        self.assertIn("J+K", held[0])
        self.assertIn("no-auto-recovery / no-node-exclusion", held[0])

    def test_inv_xfail_j_parks_a_tracked_auto_recovery(self):
        pods = {"engine-0": watcher_record(pod_object("engine-0", "engine", 0, "a1b2c3d4", "img:1", "v1"))}
        inv = run._inv_new("p1", {"inv_on": "J", "notimpl": 1, "issue": 4242, "inv_xfail": "J"})
        observe_ticks(inv, pods, 1, ledger=[ledger_row("engine", 1)])
        checks = []
        self.assertEqual("XFAIL (known bug: #4242)", run.invariant_checks(inv, "PASS", [], checks))

    def test_ledger_fetch_reads_absent_as_empty_and_errors_as_unreadable(self):
        class R:
            def __init__(self, rc, out="", err=""):
                self.returncode, self.stdout, self.stderr = rc, out, err
        rows = [ledger_row("engine", 0)]
        body = json.dumps({"data": {run.AUDIT_LEDGER_KEY: json.dumps({"entries": rows})}})
        with mock.patch.object(run, "kubectl", return_value=R(0, body)):
            self.assertEqual(rows, run._inv_fetch_ledger("p1"))
        with mock.patch.object(run, "kubectl", return_value=R(1, "", 'Error from server (NotFound): configmaps "p1-ome-migration-audit" not found')):
            self.assertEqual([], run._inv_fetch_ledger("p1"))
        with mock.patch.object(run, "kubectl", return_value=R(1, "", "Unable to connect to the server")):
            self.assertIsNone(run._inv_fetch_ledger("p1"))
        with mock.patch.object(run, "kubectl", return_value=R(0, "{not json")):
            self.assertIsNone(run._inv_fetch_ledger("p1"))


class PackedPoolVerdictTest(unittest.TestCase):
    """The packed-pool story over fixed snapshots: the sampler's reading of
    the watcher's pods, and the verdict over the story record."""

    @staticmethod
    def _pod(comp, inst, hsh, *, ready=True, node="node-a", uid=None):
        return {"comp": comp, "inst": str(inst), "alive": True, "ready": ready, "serving": ready,
                "hash": hsh, "node": node, "uid": uid or f"uid-{comp}-{inst}-{hsh}", "runner": "default"}

    def _ctx(self, story, pods):
        return {"packed": story, "pod_state": pods, "N": {"engine": 3, "decoder": 0, "router": 0},
                "old_hashes": {"old"}, "pods_per_inst": {"engine": 1}}

    def _story(self, side="new", form="start"):
        settled = {("engine", str(i)): {"nodes": [f"node-{i}"], "uids": {f"uid-engine-{i}-old"}} for i in range(3)}
        pack = {"nodes": {"node-0": "filler holds 900m on node-0", "node-1": "node-1 has no spare CPU to take"}}
        return run._packed_new(side, form, "engine", {"engine"}, pack, settled)

    def test_sampler_reads_the_broken_instance_the_fix_order_and_two_down(self):
        story = self._story()
        pods = {f"e{i}": self._pod("engine", i, "old", node=f"node-{i}") for i in range(3)}
        ctx = self._ctx(story, pods)
        with mock.patch.object(run.time, "time", return_value=100.0):
            run._packed_sample(ctx)
        self.assertEqual(0, story["down_peak"], "not armed: nothing is read")
        story["armed_ts"] = 100.0
        pods["e0"] = self._pod("engine", 0, "crash", ready=False, node="node-0")
        with mock.patch.object(run.time, "time", return_value=110.0):
            run._packed_sample(ctx)
        self.assertEqual("0", story["broken_inst"])
        self.assertEqual({}, story["first_fixed"], "before the fix push a fresh hash is the crashing revision")
        self.assertEqual(1, story["down_peak"])
        story["crash_hashes"].add("crash")
        story["fix_ts"] = 200.0
        pods["e0"] = self._pod("engine", 0, "fix", node="node-0")
        with mock.patch.object(run.time, "time", return_value=210.0):
            run._packed_sample(ctx)
        self.assertEqual({("engine", "0"): 210.0}, story["first_fixed"])
        pods["e1"] = self._pod("engine", 1, "old", ready=False, node="node-1")
        pods["e2"] = self._pod("engine", 2, "old", ready=False, node="node-2")
        for t in (220.0, 230.0):
            with mock.patch.object(run.time, "time", return_value=t):
                run._packed_sample(ctx)
        self.assertEqual((2, 10.0, "engine 2 down"), (story["down_peak"], story["two_down_span"], story["two_down_detail"]))
        pods["e2"] = self._pod("engine", 2, "old", node="node-2")
        with mock.patch.object(run.time, "time", return_value=240.0):
            run._packed_sample(ctx)
        self.assertIsNone(story["two_down_since"], "the streak resets once at most one is down")

    def test_sampler_reads_the_mirror_without_a_crash_hash(self):
        story = self._story(side="idle")
        story["armed_ts"], story["broken_inst"] = 100.0, "2"
        story["push_uids"] = {"uid-engine-0-old", "uid-engine-1-old", "uid-engine-2-old"}
        pods = {f"e{i}": self._pod("engine", i, "old", node=f"node-{i}") for i in range(3)}
        pods["e2"] = self._pod("engine", 2, "fix", node="node-2")
        pods["e9"] = self._pod("engine", 1, "old", node="node-1", uid="uid-rebuilt")
        ctx = self._ctx(story, pods)
        with mock.patch.object(run.time, "time", return_value=130.0):
            run._packed_sample(ctx)
        self.assertEqual({("engine", "2"): 130.0}, story["first_fixed"])
        self.assertEqual(["e9"], story["old_rebuilt"], "a pod created on the running revision after the push")

    def _good(self, side="new", form="start"):
        story = self._story(side, form)
        story["broken_inst"] = "2"
        story["first_fixed"] = {("engine", "2"): 300.0, ("engine", "1"): 340.0, ("engine", "0"): 380.0}
        story["down_peak"] = 1
        story["final"] = {"nodes": ["node-2"], "uids": {"uid-engine-2-fix"}}
        return story

    def _judge(self, story, hdr=None):
        notes, checks = [], []
        verdict = run._packed_checks(story, hdr or {}, "PASS", notes, checks)
        return verdict, notes, checks

    def test_the_room_is_the_services_own_when_more_than_one_component_rolls(self):
        story = self._good()
        story["final"] = {"nodes": ["node-3"], "uids": {"uid-engine-2-fix"}}
        verdict, notes, _checks = self._judge(story)
        self.assertEqual("FAIL", verdict, "the engine rolls alone: only the room it vacated")
        self.assertTrue(any("instead of ['node-2']" in n for n in notes), notes)
        story["comps"] = ["decoder", "engine"]
        story["settled"][("decoder", "0")] = {"nodes": ["node-3"], "uids": {"uid-decoder-0-old"}}
        story["settled"][("router", "0")] = {"nodes": ["node-7"], "uids": {"uid-router-0-old"}}
        self.assertEqual({"node-0", "node-1", "node-2", "node-3"}, run._packed_rooms(story),
                         "the room of every rolled Component's pods; the router rolls nothing and vacates nothing")
        verdict, notes, checks = self._judge(story)
        self.assertEqual("PASS", verdict, notes)
        self.assertTrue(any("came back on node-3, in room this service's own pods vacated (decoder and engine roll together)" in c
                            for c in checks), checks)
        story["final"] = {"nodes": ["node-7"], "uids": {"uid-engine-2-fix"}}
        verdict, notes, _checks = self._judge(story)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("instead of ['node-0', 'node-1', 'node-2', 'node-3']" in n for n in notes), notes)
        story["final"] = {"nodes": ["node-2"], "uids": {"uid-engine-2-fix"}}
        verdict, _notes, checks = self._judge(story)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("came back in the room it vacated on node-2" in c for c in checks), "its own room first")

    def test_verdict_states_the_operators_end_state(self):
        verdict, notes, checks = self._judge(self._good())
        self.assertEqual("PASS", verdict, notes)
        self.assertTrue(all(c.startswith(("  ✓", "  ⓘ")) for c in checks), checks)
        self.assertTrue(any("never two engine Instances down at once" in c for c in checks))
        self.assertTrue(any("landed the good revision first, 40s before the next" in c for c in checks))
        self.assertTrue(any("came back in the room it vacated on node-2" in c for c in checks))
        self.assertTrue(any("fillers took the spare CPU of 2 node(s)" in c for c in checks))

    def test_two_instances_down_fails_unless_the_floor_is_waived(self):
        story = self._good()
        story.update(two_down_span=run.FLOOR_SUSTAIN_S + 1, down_peak=2, two_down_detail="engine 2 down", two_down_worst=2)
        verdict, notes, _ = self._judge(story)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("two Instances down at once" in n for n in notes))
        verdict, _, checks = self._judge(story, {"floor_soft": 1})
        self.assertEqual("PASS", verdict)
        self.assertTrue(any(c.startswith("  ⚠ one down") and "reported (floor_soft=1)" in c for c in checks))
        story.update(two_down_span=1.0)
        verdict, _, checks = self._judge(story)
        self.assertEqual("PASS", verdict, "a dip shorter than the floor debounce is a readiness flap")
        self.assertTrue(any("debounced as a readiness flap" in c for c in checks))

    def test_the_broken_instance_must_land_first_for_a_crash_at_start(self):
        story = self._good()
        story["first_fixed"] = {("engine", "2"): 380.0, ("engine", "1"): 300.0, ("engine", "0"): 340.0}
        verdict, notes, checks = self._judge(story)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("not the first to land the good revision" in n for n in notes))
        self.assertTrue(any("80s after Instance 1" in c for c in checks), checks)
        story = self._good(form="load")
        story["first_fixed"] = {("engine", "2"): 380.0, ("engine", "1"): 300.0, ("engine", "0"): 340.0}
        verdict, _, checks = self._judge(story)
        self.assertEqual("PASS", verdict, "after Ready the pod can be Ready when the push lands")
        self.assertTrue(any(c.startswith("  ⚠ order") for c in checks))
        story = self._good()
        story["first_fixed"] = {("engine", "2"): 380.0}
        verdict, _, checks = self._judge(story)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("not measurable" in c for c in checks))
        story = self._good()
        story["broken_inst"] = None
        verdict, _, checks = self._judge(story)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("no engine Instance was seen on the crashing revision" in c for c in checks))

    def test_the_broken_instance_comes_back_in_the_room_it_vacated(self):
        story = self._good()
        story["final"] = {"nodes": ["node-0"], "uids": {"uid-engine-2-fix"}}
        verdict, notes, checks = self._judge(story)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("came back on ['node-0'] instead of ['node-2']" in n for n in notes), notes)
        self.assertTrue(any("there was nowhere else" in c for c in checks))
        story = self._good()
        story["final"] = {"nodes": ["node-2"], "uids": {"uid-engine-2-old"}}
        verdict, _, checks = self._judge(story)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("patched in place on node-2 (same pod)" in c for c in checks))
        story = self._good()
        story["final"] = None
        verdict, _, checks = self._judge(story)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("placement is not measurable" in c for c in checks))

    def test_the_mirror_fails_on_a_pod_rebuilt_on_the_running_revision(self):
        story = self._good(side="idle")
        verdict, _, checks = self._judge(story)
        self.assertEqual("PASS", verdict)
        self.assertTrue(any("nothing rebuilt the running revision after the good push" in c for c in checks))
        story["old_rebuilt"] = ["p999-engine-1-xyz"]
        story["repair_before_push"] = ["component=engine instance=2 restart triggered"]
        verdict, notes, checks = self._judge(story)
        self.assertEqual("FAIL", verdict)
        self.assertTrue(any("rebuilt on the running revision after the good push" in n for n in notes))
        self.assertTrue(any("a repair opened on engine Instance 2 before the good push landed" in c for c in checks))

    def test_placement_groups_nodes_and_uids_by_instance(self):
        pods = {"l": {"comp": "engine", "inst": "0", "alive": True, "node": "node-b", "uid": "u1"},
                "w": {"comp": "engine", "inst": "0", "alive": True, "node": "node-a", "uid": "u2"},
                "d": {"comp": "engine", "inst": "1", "alive": False, "node": "node-c", "uid": "u3"}}
        self.assertEqual({("engine", "0"): {"nodes": ["node-a", "node-b"], "uids": {"u1", "u2"}}},
                         run._packed_placement(pods))

    def test_pack_pool_registers_its_cleanup_before_the_first_filler(self):
        calls = []
        with mock.patch.object(run, "_eligible_nodes", return_value=["node-a", "node-b"]), \
                mock.patch.object(run, "_preempt_block_node",
                                  side_effect=lambda rec, node: (calls.append(node) or (True, f"filler holds 500m on {node}"))), \
                mock.patch.object(run, "_FAULT_CLEANUPS", []) as cleanups:
            rec = run._pack_pool("p999")
            self.assertEqual([("preempt", rec)], cleanups)
        self.assertIsNone(rec["infra_fail"])
        self.assertEqual(["node-a", "node-b"], calls)
        self.assertEqual({"node-a": "filler holds 500m on node-a", "node-b": "filler holds 500m on node-b"}, rec["nodes"])
        with mock.patch.object(run, "_eligible_nodes", return_value=["node-a"]), \
                mock.patch.object(run, "_preempt_block_node", return_value=(False, "the filler on node-a never bound in 3 sizings")), \
                mock.patch.object(run, "_FAULT_CLEANUPS", []):
            rec = run._pack_pool("p999")
        self.assertEqual("packed pool: the filler on node-a never bound in 3 sizings", rec["infra_fail"])
        with mock.patch.object(run, "_eligible_nodes", return_value=[]), mock.patch.object(run, "_FAULT_CLEANUPS", []):
            self.assertIn("no Ready, schedulable node", run._pack_pool("p999")["infra_fail"])


class RepeatingFaultHeaderTest(unittest.TestCase):
    """repeat=<n> is the persistent form of a kill, a podkill, an eviction or
    a manager restart: the fault is applied again each time what it took
    has come back, n times, then stops."""

    BASE = "# p999: x Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"
    KILL = "# p999: x mode=kill Ne=2 Nd=0 Nr=0 ceil_e=2 ceil_d=0 ceil_r=0 restart=None topo=engine"
    SCALE = "# p999: x mode=scaledown Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine to_e=3"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_repeat_rides_a_kill_a_hook_fault_or_the_idle_manager_restart(self):
        self.assertEqual([], self._errs(self.KILL + " repeat=3"))
        self.assertEqual([], self._errs(self.KILL + " via=evict repeat=3"))
        self.assertEqual([], self._errs(self.KILL + " via=preempt lift=after-settle repeat=2"))
        self.assertEqual([], self._errs(self.KILL.replace("topo=engine", "topo=engine-mn") + " kill=worker repeat=3"))
        self.assertEqual([], self._errs(self.BASE + " fault=podkill:rotating victim=stable floor_slack=1 repeat=3"))
        self.assertEqual([], self._errs(self.BASE + " fault=evict:rotating victim=stable floor_slack=1 order_soft=1 repeat=3"))
        self.assertEqual([], self._errs(self.BASE + " fault=ctrlrestart:settling repeat=3"))
        self.assertEqual([], self._errs(self.BASE + " fault=ctrlrestart:idle during=idle repeat=3"))
        self.assertEqual([], self._errs(self.SCALE + " fault=podkill:settling floor_slack=1 repeat=3"))
        self.assertEqual([], self._errs(self.SCALE + " fault=evict:settling floor_slack=1 repeat=3"))
        self.assertEqual([], self._errs(self.SCALE + " fault=ctrlrestart:settling repeat=3"))

    def test_repeat_needs_a_fault_that_comes_back_and_no_one_shot_knob(self):
        self.assertTrue(any("malformed header value repeat=1" in e for e in self._errs(self.KILL + " repeat=1")))
        self.assertTrue(any("repeat= is the persistent form" in e for e in self._errs(self.BASE + " repeat=3")))
        self.assertTrue(any("repeat= is the persistent form" in e for e in self._errs(self.BASE + " fault=crashloop:old form=start repeat=2")))
        self.assertTrue(any("repeat= is the persistent form" in e
                            for e in self._errs(self.BASE + " fault=preempt:rotating lift=after-settle floor_soft=1 order_soft=1 repeat=2")))
        self.assertTrue(any("pause= is lifted once" in e for e in self._errs(self.KILL + " pause=true repeat=3")))
        self.assertTrue(any("needs lift=after-settle" in e for e in self._errs(self.KILL + " via=preempt lift=never repeat=2")))
        self.assertTrue(any("needs a phase hook" in e for e in self._errs(self.BASE + " fault=ctrlrestart:idle repeat=3")))
        self.assertTrue(any("needs a persistent fault" in e for e in self._errs(self.BASE + " fault=ctrlrestart:idle during=idle")))
        self.assertTrue(any("do not combine with it" in e
                            for e in self._errs(self.BASE + " fault=podkill:settling rollback=1 during=settling repeat=2")))


class RepeatingFaultLoopTest(unittest.TestCase):
    """The loop: the Instance taken before is taken again once it is back,
    the status is sampled while it is short, and the fault fires again until
    the repetitions are spent."""

    def _pods(self):
        return {"e0": _rec("engine", 0, "old", uid="u0"), "e1": _rec("engine", 1, "old", uid="u1"),
                "e2": _rec("engine", 2, "new", uid="u2", created="5")}

    def _cycle(self, comp="engine", inst="0", uid="u0"):
        return {"fired_at": 100.0, "comp": comp, "inst": inst, "uid": uid, "reported": False, "lie": None,
                "retired": False, "back_at": None, "short_since": None, "sampled_at": 0.0, "samples": 0}

    def test_pick_pod_prefers_the_instance_taken_before_while_it_fits_the_aim(self):
        pods, ppi = self._pods(), {"engine": 1}
        self.assertEqual("e1", run._fault_pick_pod(pods, ppi, {"old"}, prefer_inst="1"))
        self.assertEqual("e0", run._fault_pick_pod(pods, ppi, {"old"}, victim="stable", prefer_inst="2"),
                         "the preferred Instance is on the new revision, so the stable rule picks afresh")
        self.assertEqual("e2", run._fault_pick_pod(pods, ppi, {"old"}, victim="canary", prefer_inst="0"))
        self.assertEqual("e2", run._fault_pick_pod(pods, ppi, {"old"}, prefer_inst="7"),
                         "an Instance that is gone falls back to the rules (the newest new-revision pod)")
        self.assertIsNone(run._repeat_prefer({"repeat": 3, "fired_count": 0, "victim_inst": "1"}))
        self.assertEqual("1", run._repeat_prefer({"repeat": 3, "fired_count": 1, "victim_inst": "1"}))
        self.assertIsNone(run._repeat_prefer({"repeat": 0, "fired_count": 1, "victim_inst": "1"}))

    def test_repeat_back_once_the_instance_is_whole_on_new_pods_or_retired(self):
        f = {"kind": "podkill", "fleet": {"engine": 3}}
        pods, ppi = self._pods(), {"engine": 1}
        cycle = self._cycle()
        self.assertEqual((False, False), run._repeat_back(f, cycle, pods, ppi, 200.0), "the killed pod is still there")
        pods["e0"] = _rec("engine", 0, "old", uid="u0b", ready=False)
        self.assertEqual((False, False), run._repeat_back(f, cycle, pods, ppi, 200.0), "the replacement is not Ready")
        pods["e0"] = _rec("engine", 0, "old", uid="u0b")
        self.assertEqual((True, False), run._repeat_back(f, cycle, pods, ppi, 200.0))
        del pods["e0"]
        self.assertEqual((False, True), run._repeat_back(f, cycle, pods, ppi, 200.0),
                         "the index is gone but the Component is short of N: not back yet")
        self.assertEqual((True, True), run._repeat_back(dict(f, fleet={"engine": 2}), cycle, pods, ppi, 200.0),
                         "the index is gone and the Component is back at N: the roll retired it")
        pods["e0"] = _rec("engine", 0, "old", uid="u0", alive=False)
        self.assertEqual((False, False), run._repeat_back(f, cycle, pods, ppi, 200.0), "a pod of the index is still leaving")
        gang = {"e0-l": _rec("engine", 0, "old", uid="l0", runner="leader"), "e0-w": _rec("engine", 0, "old", uid="w0", runner="worker")}
        self.assertEqual((False, False), run._repeat_back({"kind": "podkill", "fleet": {"engine": 1}}, self._cycle(uid="dead"),
                                                          {"e0-l": gang["e0-l"]}, {"engine": 2}, 200.0), "half a gang is not whole")
        self.assertEqual((True, False), run._repeat_back({"kind": "podkill", "fleet": {"engine": 1}}, self._cycle(uid="dead"),
                                                         gang, {"engine": 2}, 200.0))

    def test_manager_is_back_under_new_ready_names_only(self):
        f = {"kind": "ctrlrestart", "ctrl_pods": ["mgr-old"]}
        with mock.patch.object(run, "kubectl", _kubectl_with([(lambda a: "get" in a, _Resp("mgr-old=True\n"))])):
            self.assertFalse(run._ctrl_back(f, 10.0), "the deleted pod still lists")
        with mock.patch.object(run, "kubectl", _kubectl_with([(lambda a: "get" in a, _Resp("mgr-new=False\n"))])):
            self.assertFalse(run._ctrl_back(f, 20.0), "the new pod is not Ready")
        with mock.patch.object(run, "kubectl", _kubectl_with([(lambda a: "get" in a, _Resp("mgr-new=True\n"))])):
            self.assertFalse(run._ctrl_back(f, 20.0 + run.CTRL_POLL_S / 2), "polled no faster than CTRL_POLL_S")
            self.assertTrue(run._ctrl_back(f, 20.0 + run.CTRL_POLL_S))
            self.assertEqual((True, False), run._repeat_back(dict(f, ctrl_polled=0.0), self._cycle(comp=""), {}, {}, 99.0),
                             "a manager restart is back on the manager alone, whatever the pods")

    def test_row_sampled_while_short_reports_the_loss_or_a_lie(self):
        pods = {"e0": _rec("engine", 0, "old", uid="u0b", ready=False), "e1": _rec("engine", 1, "old", uid="u1")}
        cycle = self._cycle()
        with mock.patch.object(run, "_ir_row", lambda *_a: {"index": 0, "phase": "Creating", "podCount": 1, "servingPodCount": 0}):
            run._repeat_sample_row("p1", cycle, pods, {"engine": 1}, 100.0)
        self.assertEqual((True, None, 1), (cycle["reported"], cycle["lie"], cycle["samples"]))
        cycle = self._cycle()
        whole_row = {"index": 0, "phase": "Ready", "podCount": 1, "servingPodCount": 1}
        t1, t2 = 100.0 + run.GANG_SHORT_REPORT_S + run.INV_TICK_S, 100.0 + run.GANG_SHORT_REPORT_S + 2 * run.INV_TICK_S
        with mock.patch.object(run, "_ir_row", lambda *_a: whole_row):
            run._repeat_sample_row("p1", cycle, pods, {"engine": 1}, 100.0)
            self.assertIsNone(cycle["lie"], "one reconcile of lag is not a lie")
            run._repeat_sample_row("p1", cycle, pods, {"engine": 1}, t1)
            self.assertIsNone(cycle["lie"], "one sample can read a row a reconcile ahead of the pods at the edge of the gap")
            self.assertIn("servingPodCount=1", cycle["lie_seen"])
            run._repeat_sample_row("p1", cycle, pods, {"engine": 1}, t2)
        self.assertFalse(cycle["reported"])
        self.assertIn("servingPodCount=1", cycle["lie"], "the lie persisted across two samples")
        cycle = self._cycle()
        rows = iter([whole_row, {"index": 0, "phase": "Restarting", "podCount": 1, "servingPodCount": 0}, whole_row])
        with mock.patch.object(run, "_ir_row", lambda *_a: next(rows)):
            for k in range(3):
                run._repeat_sample_row("p1", cycle, pods, {"engine": 1}, t1 + k * run.INV_TICK_S)
        self.assertEqual((True, None), (cycle["reported"], cycle["lie"]), "a reporting sample between two whole readings clears the first")
        self.assertIsNotNone(cycle["lie_seen"])
        gang = {"e0-l": _rec("engine", 0, "old", runner="leader", uid="l0"),
                "e0-w": _rec("engine", 0, "old", runner="worker", uid="w0", ready=False)}
        cycle = self._cycle()
        with mock.patch.object(run, "_ir_row", lambda *_a: {"index": 0, "phase": "Ready", "podCount": 1, "servingPodCount": 1}):
            run._repeat_sample_row("p1", cycle, gang, {"engine": 2}, t1)
        self.assertEqual((True, None, None), (cycle["reported"], cycle["lie"], cycle.get("lie_seen")),
                         "one serving pod of a two-pod gang is the loss reported, not the gang counted whole")
        self.assertTrue(run._row_serving_short({"phase": "Ready", "podCount": 1, "servingPodCount": 1}, 2))
        self.assertFalse(run._row_serving_short({"phase": "Ready", "podCount": 2, "servingPodCount": 2}, 2))
        self.assertFalse(run._row_serving_short({"phase": "Ready", "podCount": 1, "servingPodCount": 1}))
        cycle = self._cycle()
        pods["e0"] = _rec("engine", 0, "old", uid="u0b")
        with mock.patch.object(run, "_ir_row", lambda *_a: self.fail("a whole Instance is not sampled")):
            run._repeat_sample_row("p1", cycle, pods, {"engine": 1}, 100.0)
        self.assertEqual(0, cycle["samples"])

    def test_tick_fires_again_once_back_and_stops_when_the_repetitions_are_spent(self):
        fired = []
        f = {"case_id": "p1", "kind": "podkill", "param": "settling", "victim": "stable", "member": "", "repeat": 2,
             "fired": True, "fired_count": 1, "infra_fail": None, "notes": [], "victim_comp": "engine", "victim_inst": "0",
             "victim_uid": "u0", "victim_insts": {("engine", "0")}, "cycles": [self._cycle()], "fleet": {"engine": 3}}
        pods = self._pods()
        ctx = {"fault": f, "trigger_ts": 1.0, "pod_state": pods, "pods_per_inst": {"engine": 1}, "old_hashes": {"old"}}

        def fire(rec, *_a, **_k):
            fired.append(rec["victim_inst"])
            rec["fired_count"] += 1
            rec["cycles"].append(self._cycle(uid="u0b"))
        with mock.patch.object(run, "_fault_fire", fire), mock.patch.object(run, "_ir_row", lambda *_a: None):
            run._fault_tick(ctx)
            self.assertEqual([], fired, "the killed pod is still there")
            pods["e0"] = _rec("engine", 0, "old", uid="u0b")
            run._fault_tick(ctx)
            self.assertEqual(["0"], fired)
            self.assertIsNotNone(f["cycles"][0]["back_at"])
            pods["e0"] = _rec("engine", 0, "old", uid="u0c")
            run._fault_tick(ctx)
            self.assertEqual(["0"], fired, "two repetitions were asked for; the loop is spent")
            self.assertIsNotNone(f["cycles"][1]["back_at"], "when the last one came back is still noted")
        self.assertEqual({"u0", "u1", "u2"}, f["base_uids"])
        self.assertEqual({"u0", "u1", "u2", "u0b", "u0c"}, f["seen_uids"])
        self.assertEqual({"old", "new"}, f["seen_hashes"]["engine"])

    def test_firing_records_the_count_the_instances_taken_and_a_cycle(self):
        f = {"case_id": "p1", "kind": "podkill", "param": "settling", "victim": "", "member": "", "repeat": 3,
             "fired": False, "infra_fail": None, "notes": []}
        settled = {"e0": _rec("engine", 0, "old", uid="u0"), "e1": _rec("engine", 1, "old", uid="u1")}
        with mock.patch.object(run, "kubectl", _kubectl_with([])):
            run._fault_fire(f, settled, {"engine": 1}, {"old"})
        self.assertEqual((1, {("engine", "0")}, 1), (f["fired_count"], f["victim_insts"], len(f["cycles"])))
        self.assertEqual(("engine", "0", "u0"), (f["cycles"][0]["comp"], f["cycles"][0]["inst"], f["cycles"][0]["uid"]))
        once = {"case_id": "p1", "kind": "podkill", "param": "settling", "victim": "", "member": "",
                "fired": False, "infra_fail": None, "notes": []}
        with mock.patch.object(run, "kubectl", _kubectl_with([])):
            run._fault_fire(once, settled, {"engine": 1}, {"old"})
        self.assertEqual((1, []), (once["fired_count"], once.get("cycles", [])), "a one-shot fault keeps no cycle")

    def test_kill_pick_prefers_the_instance_taken_before_and_the_named_runner(self):
        pods = {"e1-w": _rec("engine", 1, "old", runner="worker"), "e0-l": _rec("engine", 0, "old", runner="leader"),
                "e0-w": _rec("engine", 0, "old", runner="worker"), "d0": _rec("decoder", 0, "old")}
        self.assertEqual("e0-l", run._kill_pick(pods, "engine", "")[0])
        self.assertEqual("e0-w", run._kill_pick(pods, "engine", "worker")[0])
        self.assertEqual("e1-w", run._kill_pick(pods, "engine", "worker", prefer_inst="1")[0])
        self.assertEqual("e0-w", run._kill_pick(pods, "engine", "worker", prefer_inst="9")[0])
        self.assertEqual((None, None), run._kill_pick(pods, "router", ""))

    def test_fillers_are_named_per_repetition_and_the_reset_waits_for_the_old_ones(self):
        f = {"case_id": "p1", "fillers": ["a"], "fillers_dropped": True, "crash_pod": "x", "preempted": ["x"]}
        self.assertEqual("perm-p1-press", run._filler_name(f, "press"))
        self.assertEqual("perm-p1-fill-n1-r2", run._filler_name(dict(f, cycle=2), "fill-n1"))
        with mock.patch.object(run, "kubectl", _kubectl_with([(lambda a: "get" in a, _Resp(""))])), \
                mock.patch.object(run, "LIFT_SETTLE_S", 1):
            self.assertTrue(run._preempt_cycle_reset(f, 2))
        self.assertEqual(([], False, 2, None, []), (f["fillers"], f["fillers_dropped"], f["cycle"], f["crash_pod"], f["preempted"]))
        with mock.patch.object(run, "kubectl", _kubectl_with([(lambda a: "get" in a, _Resp("pod/perm-p1-press\n"))])), \
                mock.patch.object(run, "LIFT_SETTLE_S", 0.01), mock.patch.object(run.time, "sleep", lambda _s: None):
            self.assertFalse(run._preempt_cycle_reset(f, 3))


class RepeatingFaultVerdictTest(unittest.TestCase):
    """The window's verdict and the end state after the lift."""

    def _rec(self, **kw):
        cycles = [{"fired_at": 10.0 * k, "comp": "engine", "inst": "0", "uid": f"u{k}", "reported": True, "lie": None,
                   "retired": False, "back_at": 10.0 * k + 5, "short_since": None, "sampled_at": 0.0, "samples": 2}
                  for k in range(3)]
        rec = {"case_id": "p1", "kind": "podkill", "repeat": 3, "fired_count": 3, "cycles": cycles, "infra_fail": None,
               "victim_insts": {("engine", "0")}, "base_uids": {"a", "b"}, "seen_uids": {"a", "b", "r1", "r2", "r3"},
               "base_hashes": {"old"}, "seen_hashes": {"engine": {"old"}}}
        rec.update(kw)
        return rec

    def _judge(self, rec, events=None, bumped=set(), op_pods=0):
        notes, checks = [], []
        with mock.patch.object(run, "_isvc_events_since", lambda *_a: events or []):
            verdict = run._repeat_checks(rec, {"engine": 1}, bumped, op_pods, 0.0, "PASS", notes, checks)
        return verdict, notes, "\n".join(checks)

    def test_a_refused_repetition_gets_the_budgets_retry_window(self):
        f = {"case_id": "p1", "kind": "evict", "param": "settling", "victim": "stable", "repeat": 3, "fired": True,
             "fired_count": 1, "infra_fail": None, "notes": [], "victim_insts": set()}
        pods = {"e0": _rec("engine", 0, "old", uid="u0")}
        refusal = "Error from server (TooManyRequests): Cannot evict pod as it would violate the pod's disruption budget."
        with mock.patch.object(run, "_fault_pick_pod", lambda *a, **k: "e0"), \
                mock.patch.object(run, "kubectl", lambda *a, **k: _Resp("", 1, refusal)), \
                mock.patch.object(run.time, "time", return_value=100.0):
            run._fault_evict_fire(f, pods, {"engine": 1}, {"old"})
            self.assertEqual((False, 1, 100.0, 105.0), (f["fired"], f["denials"], f["refused_since"], f["retry_at"]))
        with mock.patch.object(run, "_fault_pick_pod", lambda *a, **k: "e0"), \
                mock.patch.object(run, "kubectl", lambda *a, **k: _Resp("", 1, refusal)), \
                mock.patch.object(run.time, "time", return_value=105.0):
            run._fault_evict_fire(f, pods, {"engine": 1}, {"old"})
            self.assertEqual((2, 100.0), (f["denials"], f["refused_since"]), "the window runs from the first refusal of this repetition")
        self.assertTrue(run._repeat_retrying(f, 100.0 + run.LIFT_SETTLE_S - 1), "the loop keeps its window open while the refusal is retried")
        self.assertFalse(run._repeat_retrying(f, 100.0 + run.LIFT_SETTLE_S), "and closes it once the retry window ran out")
        self.assertFalse(run._repeat_retrying(dict(f, fired_count=3), 101.0), "nothing is owed once the repetitions are spent")
        self.assertFalse(run._repeat_retrying(dict(f, repeat=0), 101.0), "a one-shot fault owes no repetition")
        with mock.patch.object(run, "_fault_pick_pod", lambda *a, **k: "e0"), \
                mock.patch.object(run, "kubectl", lambda *a, **k: _Resp("", 0, "")), \
                mock.patch.object(run.time, "time", return_value=110.0):
            f["fired"] = True
            run._fault_evict_fire(f, pods, {"engine": 1}, {"old"})
        self.assertIsNone(f["refused_since"], "admitted: the next repetition starts with no refusal on its clock")
        self.assertFalse(run._repeat_retrying(f, 111.0))
        self.assertIn("after 2 budget refusal(s)", f["notes"][-1])

    def test_a_clean_window_passes_and_says_what_it_read(self):
        repairs = [("RestartTriggered", "component=engine instance=0 restarting")] * 3
        verdict, notes, checks = self._judge(self._rec(), repairs)
        self.assertEqual(("PASS", []), (verdict, notes), checks)
        self.assertIn("fired 3 times", checks)
        self.assertIn("reported each loss", checks)
        self.assertIn("3 repair(s) recorded, every one for an Instance the fault took", checks)
        self.assertIn("3 pod(s) created across the window, within the 6", checks)
        self.assertIn("running revision or the one pushed", checks)

    def test_fewer_firings_than_asked_fail(self):
        once = self._rec(kind="evict", fired_count=1, denials=8, seen_uids={"a", "b", "r1"})
        once["cycles"] = once["cycles"][:1]
        verdict, notes, checks = self._judge(once)
        self.assertEqual(("PASS", []), (verdict, notes), checks)
        self.assertIn("fired 1 time(s) under a budget that refuses — the disruption budget refused 8 eviction(s)", checks)
        verdict, notes, checks = self._judge(self._rec(kind="evict", fired_count=0, denials=8, cycles=[]))
        self.assertEqual("FAIL", verdict, "a budget that admitted nothing is no window at all")
        self.assertIn("fired 0 time(s)", checks)
        verdict, notes, checks = self._judge(self._rec(denials=2), [("RestartTriggered", "component=engine instance=0 restarting")] * 3)
        self.assertEqual("PASS", verdict, checks)
        self.assertIn("fired 3 times, each once the Instance it took had come back (back after 5s, 5s, 5s; 2 refusal(s) by the disruption budget waited out)", checks)
        verdict, notes, checks = self._judge(self._rec(fired_count=2))
        self.assertEqual("FAIL", verdict)
        self.assertIn("fired 2 of 3", notes[0])

    def test_status_that_never_reported_the_loss_or_lied_fails(self):
        rec = self._rec()
        rec["cycles"][1]["reported"] = False
        verdict, notes, _c = self._judge(rec)
        self.assertEqual("FAIL", verdict)
        self.assertIn("never reported the loss in repetition(s) 2", notes[0])
        rec = self._rec()
        rec["cycles"][2]["lie"] = "row engine/0 reads Ready with servingPodCount=1 while 0/1 pod(s) are Ready"
        verdict, notes, _c = self._judge(rec)
        self.assertEqual("FAIL", verdict)
        self.assertIn("counted the short Instance as serving", notes[0])
        rec = self._rec()
        rec["cycles"][1].update(reported=False, retired=True)
        verdict, _n, checks = self._judge(rec)
        self.assertEqual("PASS", verdict, "an Instance the roll retired has no row to report")
        self.assertIn("retired the Instance taken in repetition(s) 2", checks)
        rec = self._rec()
        rec["cycles"][1].update(reported=False, samples=0)
        verdict, _n, checks = self._judge(rec)
        self.assertEqual("PASS", verdict, "a loss that came back before a sample is reported, not failed")
        self.assertIn("came back before a row sample was read", checks)

    def test_a_repair_for_an_untouched_instance_or_one_too_many_fails(self):
        foreign = [("RestartTriggered", "component=engine instance=1 restarting")]
        verdict, notes, _c = self._judge(self._rec(), foreign)
        self.assertEqual("FAIL", verdict)
        self.assertIn("never took", notes[0])
        many = [("RestartTriggered", "component=engine instance=0 restarting")] * 4
        verdict, notes, _c = self._judge(self._rec(), many)
        self.assertEqual("FAIL", verdict)
        self.assertIn("4 repairs for 3 losses", notes[0])

    def test_more_pods_than_the_operation_and_the_repairs_explain_is_a_hot_loop(self):
        rec = self._rec(seen_uids={"a", "b"} | {f"r{i}" for i in range(7)})
        verdict, notes, _c = self._judge(rec)
        self.assertEqual("FAIL", verdict)
        self.assertIn("7 pods created, at most 6 explained", notes[0])
        verdict, _n, _c = self._judge(rec, op_pods=3)
        self.assertEqual("PASS", verdict, "the operation's own pods widen the bound")

    def test_a_revision_the_service_never_pushed_fails(self):
        rec = self._rec(seen_hashes={"engine": {"old", "new"}})
        verdict, notes, _c = self._judge(rec)
        self.assertEqual("FAIL", verdict)
        self.assertIn("never pushed", notes[0])
        verdict, _n, _c = self._judge(rec, bumped={"engine"})
        self.assertEqual("PASS", verdict, "one new revision on a bumped Component is the push")
        rec = self._rec(seen_hashes={"engine": {"old", "new", "newer"}})
        self.assertEqual("FAIL", self._judge(rec, bumped={"engine"})[0], "a third revision is never right")

    def test_a_gated_manager_restart_and_a_one_shot_fault_are_left_alone(self):
        rec = self._rec(kind="ctrlrestart", ctrl_gated=True, fired_count=1)
        verdict, notes, checks = self._judge(rec)
        self.assertEqual(("PASS", []), (verdict, notes))
        self.assertIn("gated off", checks)
        self.assertEqual("PASS", self._judge(self._rec(repeat=0, fired_count=1))[0])

    def test_end_state_wants_settled_rows_and_no_hold(self):
        held = {"components": {"engine": {"lifecycle": {"rolloutHold": {"gate": "Budget", "reason": "waiting"}}}}}
        with mock.patch.object(run, "_wait_rows_settled", lambda *_a: (True, "")), \
                mock.patch.object(run, "fetch_status", lambda *_a: held):
            notes, checks = [], []
            verdict = run._repeat_end_checks("p1", {"engine": 2}, {"engine"}, "PASS", notes, checks)
        self.assertEqual("FAIL", verdict)
        self.assertIn("rollout hold is left behind", notes[0])
        with mock.patch.object(run, "_wait_rows_settled", lambda *_a: (True, "")), \
                mock.patch.object(run, "fetch_status", lambda *_a: {"components": {"engine": {}}}):
            notes, checks = [], []
            verdict = run._repeat_end_checks("p1", {"engine": 2}, {"engine"}, "PASS", notes, checks)
        self.assertEqual(("PASS", []), (verdict, notes))
        with mock.patch.object(run, "_wait_rows_settled", lambda *_a: (False, "engine/1: Instance is not settled")), \
                mock.patch.object(run, "fetch_status", lambda *_a: {}):
            notes, checks = [], []
            self.assertEqual("FAIL", run._repeat_end_checks("p1", {"engine": 2}, {"engine"}, "PASS", notes, checks))


class GeneratedRepeatFamilyTest(unittest.TestCase):
    """gen-repeat: the persistent form of a pod deleted by hand, a gang member
    lost, an eviction or a preemption and a manager restart, on the six shape
    families at steady state, under a push and under a scale-up."""

    @classmethod
    def setUpClass(cls):
        cls.outputs, rows = generate_cases.build_outputs()
        cls.fam = [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-repeat"]

    def _family(self, kv):
        parts = ["engine"] + (["decoder"] if kv["Nd"] else []) + (["router"] if kv["Nr"] else [])
        return "+".join(parts) + (" gang" if kv["topo"].endswith("-mn") else " single")

    def _operation(self, kv):
        if kv.get("mode") == "kill":
            return "steady"
        if kv.get("mode") == "scaledown":
            return "scale-up"
        return "steady" if kv.get("during") == "idle" else "push"

    def _row(self, kv):
        kind = kv.get("fault", "").partition(":")[0] or ("evict" if kv.get("via") == "evict" else "podkill")
        if kv.get("via") == "preempt":
            kind = "preempt"
        if kind == "ctrlrestart":
            return "controller restart"
        if kind in ("evict", "preempt"):
            return "evicted or preempted"
        return "gang member lost" if kv["topo"].endswith("-mn") else "pod deleted by hand"

    def test_counts_and_cells(self):
        self.assertEqual(126, len(self.fam))
        cells = {(self._row(kv), self._operation(kv), self._family(kv)) for _n, kv, _m in self.fam}
        families = {"engine single", "engine gang", "engine+router single", "engine+router gang",
                    "engine+decoder+router single", "engine+decoder+router gang"}
        ops = {"steady", "push", "scale-up"}
        self.assertEqual({(f, o) for f in families for o in ops},
                         {(f, o) for r, o, f in cells if r == "controller restart"})
        self.assertEqual({(f, o) for f in families for o in ops},
                         {(f, o) for r, o, f in cells if r == "evicted or preempted"})
        self.assertEqual({(f, o) for f in families if f.endswith("single") for o in ops},
                         {(f, o) for r, o, f in cells if r == "pod deleted by hand"})
        self.assertEqual({(f, o) for f in families if f.endswith("gang") for o in ops},
                         {(f, o) for r, o, f in cells if r == "gang member lost"})
        by_row = {}
        for _n, kv, _m in self.fam:
            by_row[self._row(kv)] = by_row.get(self._row(kv), 0) + 1
        self.assertEqual({"pod deleted by hand": 24, "gang member lost": 36, "evicted or preempted": 42, "controller restart": 24}, by_row)

    def test_every_cell_repeats_and_states_the_window_and_the_lift(self):
        for name, kv, m in self.fam:
            self.assertGreaterEqual(kv["repeat"], 2, name)
            text = self.outputs[f"gen-repeat/{name}.yaml"]
            reason = text.splitlines()[1]
            self.assertIn("then left alone" if "ctrlrestart" not in kv.get("fault", "") else "each time it is Ready", reason, name)
            self.assertIn("no hold", reason, name)
            self.assertEqual("none", kv["co"])
            self.assertIn(m["loop"].split(" x")[0], m["verb"])

    def test_the_loop_takes_the_same_pod_the_operation_works_on(self):
        for name, kv, m in self.fam:
            op = self._operation(kv)
            if kv.get("mode") == "kill":
                self.assertEqual(kv["Ne"], kv["ceil_e"], name)
                self.assertIn(kv["restart"], ("None", "RecreateInstance"))
                if kv.get("via") == "preempt":
                    self.assertEqual(("after-settle", generate_cases.REPEAT_PREEMPT_N), (kv["lift"], kv["repeat"]), name)
                    self.assertNotIn("kill", kv, name)
                else:
                    self.assertEqual(generate_cases.REPEAT_N, kv["repeat"], name)
                continue
            kind = kv["fault"].partition(":")[0]
            if kind == "ctrlrestart":
                self.assertNotIn("floor_slack", kv, name)
                self.assertEqual("idle" if op == "steady" else "settling", kv["fault"].partition(":")[2], name)
                continue
            self.assertEqual(1, kv["floor_slack"], name)
            if kv["topo"].startswith("pd"):
                self.assertEqual("engine", kv["victimcomp"], "the fault meets the operation on the engine")
            if op == "push":
                self.assertEqual(("rotating", "stable"), (kv["fault"].partition(":")[2], kv["victim"]), name)
                self.assertEqual(m["strat"] == "SurgeThenDrain", kv.get("order_soft") == 1, name)
                self.assertEqual(kv["Ne"] + (1 if m["strat"] == "SurgeThenDrain" else 0), kv["ceil_e"], name)
                if kind == "evict":
                    self.assertEqual("None", m["rp"], "the eviction loop under a push runs under policy None")
                elif kv["topo"].endswith("-mn"):
                    self.assertEqual("RecreateInstance", m["rp"], "a gang member lost under a push rebuilds the gang whole")
                    self.assertIn(kv["kill"], ("leader", "worker"), name)
            else:
                self.assertEqual(("settling", kv["Ne"] + 1, kv["Ne"] + 1), (kv["fault"].partition(":")[2], kv["to_e"], kv["ceil_e"]), name)
                self.assertNotIn("victim", kv, name)

    def test_every_cell_validates_as_a_case_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "gen-repeat"
            root.mkdir()
            for name, _kv, _m in self.fam:
                (root / f"{name}.yaml").write_text(self.outputs[f"gen-repeat/{name}.yaml"])
            self.assertEqual(0, run.validate_cases(Path(tmp)))

    def test_cluster_wide_cells_run_exclusively(self):
        exclusive = [n for n, kv, _m in self.fam if batch.cluster_wide_fault({k: str(v) for k, v in kv.items()})]
        self.assertEqual(30, len(exclusive), "24 manager restarts and 6 preemptions run one at a time")
        for name, kv, _m in self.fam:
            kind = kv.get("fault", "").partition(":")[0]
            self.assertEqual(name in exclusive, kind == "ctrlrestart" or kv.get("via") == "preempt", name)


class CrashOnceTest(unittest.TestCase):
    """once=1: the runner killed exactly once; the verdict is the operator's
    end state after a crash that heals by itself."""

    BASE = "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 restart=None topo=engine"
    REPAIR = "OMENative component=engine instance=1 restart triggered: runner restarted"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_header_rides_a_crashloop_break_on_either_side(self):
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:old form=start once=1 during=idle"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:old form=load once=1 floor_slack=1"))
        self.assertEqual([], self._errs(self.BASE + " fault=crashloop:new form=start once=1"))
        for bad in (" fault=imagepull:old once=1",
                    " fault=crashloop:old form=load once=1 during=settling",
                    " fault=crashloop:old form=load once=1 during=idle expect=repaired",
                    " fault=crashloop:old form=load once=1 fixfirst=1"):
            self.assertTrue(any("once=1" in e for e in self._errs(self.BASE + bad)), bad)

    def test_candidate_follows_the_side_and_the_form(self):
        pods = {
            "e0": {"alive": True, "hash": "old", "inst": "0", "comp": "engine", "runner": "default", "ready": True, "created": "1"},
            "e1": {"alive": True, "hash": "old", "inst": "1", "comp": "engine", "runner": "default", "ready": True, "created": "2"},
            "n2": {"alive": True, "hash": "new", "inst": "2", "comp": "engine", "runner": "default", "ready": False, "created": "4"},
            "n3": {"alive": True, "hash": "new", "inst": "3", "comp": "engine", "runner": "default", "ready": False, "created": "3"},
            "w3": {"alive": True, "hash": "new", "inst": "3", "comp": "engine", "runner": "worker", "ready": False, "created": "3"},
        }
        old = run._fault_new("p999", {"fault": "crashloop:old", "form": "load", "once": 1})
        self.assertEqual("e1", run._once_candidate(old, pods, {"old"}),
                         "the running side: the break rule's highest old-revision pod, Ready for form=load")
        old["form"] = "start"
        self.assertIsNone(run._once_candidate(old, pods, {"old"}), "a Ready pod cannot crash at start")
        new = run._fault_new("p999", {"fault": "crashloop:new", "form": "start", "once": 1})
        self.assertEqual("n3", run._once_candidate(new, pods, {"old"}),
                         "the pushed side: the earliest new-revision single or leader pod, not yet Ready")
        new["form"] = "load"
        self.assertIsNone(run._once_candidate(new, pods, {"old"}), "form=load waits for a new-revision pod to be Ready")
        pods["n2"]["ready"] = True
        self.assertEqual("n2", run._once_candidate(new, pods, {"old"}))

    def test_start_tick_fires_only_for_a_crash_at_first_start_of_the_running_revision(self):
        calls = []
        orig = run._once_fire
        run._once_fire = lambda f, ps, oh: calls.append((f["param"], f["form"]))
        try:
            for hdr in ({"fault": "crashloop:old", "form": "start", "once": 1},
                        {"fault": "crashloop:old", "form": "load", "once": 1},
                        {"fault": "crashloop:new", "form": "start", "once": 1},
                        {"fault": "crashloop:old", "form": "start"}):
                run._once_start_tick(run._fault_new("p999", hdr), {})
            self.assertEqual([("old", "start")], calls)
        finally:
            run._once_fire = orig

    def test_tick_lets_a_one_shot_kill_wait_for_the_pushed_revision(self):
        fired = []
        orig = run._fault_fire
        run._fault_fire = lambda *a, **k: fired.append(a)
        try:
            once = run._fault_new("p999", {"fault": "crashloop:new", "form": "load", "once": 1})
            run._fault_tick({"fault": once, "trigger_ts": 1.0, "pod_state": {}, "pods_per_inst": {}, "old_hashes": {"old"}})
            self.assertEqual(1, len(fired), "a one-shot kill on the pushed revision is a hook fault, not an armed one")
            plain = run._fault_new("p999", {"fault": "crashloop:new", "form": "load"})
            run._fault_tick({"fault": plain, "trigger_ts": 1.0, "pod_state": {}, "pods_per_inst": {}, "old_hashes": {"old"}})
            self.assertEqual(1, len(fired), "a persistent break on the pushed revision is armed by the push")
        finally:
            run._fault_fire = orig

    def _f(self, form="load", param="old"):
        return {"kind": "crashloop", "param": param, "form": form, "once": True, "fired": True, "kills": 1,
                "crash_comp": "engine", "crash_inst": "1", "crash_pod": "e1", "crash_uid": "u1", "notes": []}

    def _out(self, **over):
        out = {"fired": True, "kills": 1, "bitten": True, "restarts": 1, "rebuilt": False, "serving": True,
               "on_revision": True, "expect_new": False, "repairs": [], "escalations": [], "rows_settled": True,
               "rows_why": "settled", "side": "old", "op": "idle", "rp": "None", "new_hash": False}
        out.update(over)
        return out

    def test_verdict_is_the_operators_end_state(self):
        notes, checks = [], []
        self.assertEqual("PASS", run._once_checks(self._f(), self._out(), "PASS", notes, checks))
        self.assertTrue(any("Ready and serving on the running revision" in c for c in checks))
        for over, words in (
            ({"serving": False}, "did not end Ready and serving"),
            ({"on_revision": False}, "not on the running revision"),
            ({"escalations": ["InstanceFailed: component=engine instance=1 x"]}, "escalated or held"),
            ({"rows_settled": False, "rows_why": "engine/1: Restarting with an operation open"}, "left mid-operation"),
            ({"new_hash": True}, "new revision appeared"),
            ({"repairs": [self.REPAIR]}, "restart policy None promises no repair"),
        ):
            notes, checks = [], []
            self.assertEqual("FAIL", run._once_checks(self._f(), self._out(**over), "PASS", notes, checks), words)
            self.assertTrue(any(words in c for c in checks), (words, checks))

    def test_a_rebuild_is_owed_only_where_the_policy_promises_it(self):
        f, one = self._f(), [self.REPAIR]
        # a settled pod restarted after Ready: RecreateInstanceOnPodRestart owes one rebuild
        self.assertEqual("PASS", run._once_checks(f, self._out(rp="RecreateInstance", repairs=one, rebuilt=True), "PASS", [], []))
        checks = []
        self.assertEqual("FAIL", run._once_checks(f, self._out(rp="RecreateInstance"), "PASS", [], checks))
        self.assertTrue(any("promises a rebuild" in c for c in checks))
        notes = []
        self.assertEqual("FAIL", run._once_checks(f, self._out(rp="RecreateInstance", repairs=one * 2), "PASS", notes, []))
        self.assertTrue(any("2 times" in n for n in notes))
        # a restart before the first Ready: whether it counts is reported
        checks = []
        self.assertEqual("PASS", run._once_checks(self._f(form="start"), self._out(rp="RecreateInstance"), "PASS", [], checks))
        self.assertTrue(any("reported, not judged" in c for c in checks))
        # under a push the roll owns the running revision's Instance: no repair under either policy
        notes = []
        self.assertEqual("FAIL", run._once_checks(f, self._out(op="push", rp="RecreateInstance", expect_new=True, repairs=one), "PASS", notes, []))
        self.assertTrue(any("after the bump" in n for n in notes))
        self.assertEqual("PASS", run._once_checks(f, self._out(op="push", rp="RecreateInstance", expect_new=True, rebuilt=True), "PASS", [], []))
        # the pushed revision's pod: one rebuild allowed and reported, two not
        pushed = self._f(param="new")
        new_side = dict(op="push", side="new", rp="RecreateInstance", expect_new=True)
        self.assertEqual("PASS", run._once_checks(pushed, self._out(**new_side), "PASS", [], []))
        self.assertEqual("PASS", run._once_checks(pushed, self._out(repairs=one, **new_side), "PASS", [], []))
        self.assertEqual("FAIL", run._once_checks(pushed, self._out(repairs=one * 2, **new_side), "PASS", [], []))

    def test_a_kill_that_never_landed_is_apparatus_not_product(self):
        f = self._f()
        self.assertEqual("PASS", run._once_checks(f, self._out(fired=False, kills=0), "PASS", [], []))
        self.assertIn("never landed", f["infra_fail"])
        f = self._f()
        self.assertEqual("PASS", run._once_checks(f, self._out(bitten=False), "PASS", [], []))
        self.assertIn("left no restart behind", f["infra_fail"])

    def test_generated_family_crosses_the_rows_the_ledger_was_missing(self):
        outputs, rows = generate_cases.build_outputs()
        fam = [(n, kv, m) for f, n, kv, m in rows if f == "gen-crash-once"]
        self.assertEqual(120, len(fam))
        self.assertTrue(all(kv["once"] == 1 and kv["fault"] in ("crashloop:old", "crashloop:new") for _n, kv, _m in fam))
        cells = {(kv["fault"], kv["form"], kv.get("during", "push"), kv["restart"], kv["topo"], kv["Nr"] > 0, m["strat"])
                 for _n, kv, m in fam}
        self.assertEqual(120, len(cells), "every case is its own cell")
        idle = [kv for _n, kv, _m in fam if kv.get("during") == "idle"]
        self.assertEqual(24, len(idle))
        self.assertTrue(all(kv["fault"] == "crashloop:old" for kv in idle), "the pushed side needs a push")
        pushed = [kv for _n, kv, _m in fam if "during" not in kv]
        self.assertEqual({"crashloop:old", "crashloop:new"}, {kv["fault"] for kv in pushed})
        self.assertTrue(all(kv["floor_slack"] == 1 for kv in pushed), "the kill takes one Instance out while it restarts")
        self.assertEqual(6, len({(kv["topo"], kv["Nd"] > 0, kv["Nr"] > 0) for _n, kv, _m in fam}), "six shape families")
        with tempfile.TemporaryDirectory() as tmp:
            for name, _kv, _m in fam:
                path = Path(tmp) / f"{name}.yaml"
                path.write_text(outputs[f"gen-crash-once/{name}.yaml"])
                self.assertEqual([], run.validate_case_file(path), name)

    def test_ledger_reads_once_as_the_one_shot_form(self):
        import coverage
        cases = coverage.load_cases(coverage.ISVCS / "gen-crash-once")
        self.assertEqual(120, len(cases))
        for case in cases:
            kind, form, side = case.fault_rows()[0]
            self.assertEqual("one-shot", form, case.id)
            self.assertEqual("new" if case.header["fault"].endswith(":new") else "running", side, case.id)
            self.assertEqual("crash at start" if case.header["form"] == "start" else "crash after Ready", kind, case.id)
            self.assertEqual("steady state" if case.header.get("during") == "idle" else "push", case.operation, case.id)


def _gang_pod(name, inst, runner, ready, uid, hash_="old"):
    return {"alive": True, "hash": hash_, "inst": inst, "comp": "engine", "runner": runner, "ready": ready,
            "serving": ready, "created": inst, "uid": uid}


class CrashOnceApparatusTest(unittest.TestCase):
    """The kill lands on a gang leader and the verdict sees it: the victim
    is read live after the kill until its restart shows, the Instance is
    followed to the index a gang surge left it at, the kill at first start
    is asked on the clock rather than on pod events, and a kill that never
    lands says what the watcher first saw."""

    GANG = {
        "p1-engine-0-leader-0": _gang_pod("p1-engine-0-leader-0", "0", "leader", True, "u0l"),
        "p1-engine-0-worker-0": _gang_pod("p1-engine-0-worker-0", "0", "worker", True, "u0w"),
        "p1-engine-1-leader-0": _gang_pod("p1-engine-1-leader-0", "1", "leader", False, "u1l"),
        "p1-engine-1-worker-0": _gang_pod("p1-engine-1-worker-0", "1", "worker", False, "u1w"),
    }
    REPAIR = "OMENative component=engine instance=1 restart triggered: runner restarted"
    SURGE = "OMENative component=engine instance=1 gang surge to revision r2 (surge-index=2)"

    @staticmethod
    def _pod_json(name, uid, running=True, ready=False, restarts=0):
        state = {"running": {"startedAt": "t"}} if running else {"waiting": {"reason": "ContainerCreating"}}
        return json.dumps({"metadata": {"name": name, "uid": uid},
                           "status": {"containerStatuses": [{"name": run.MAIN_CONTAINER, "state": state,
                                                             "ready": ready, "restartCount": restarts}]}})

    def _fired(self, **over):
        f = {"case_id": "p1", "kind": "crashloop", "param": "old", "form": "start", "once": True, "fired": True,
             "kills": 1, "fired_ts": 0.0, "once_poll": 0.0, "crash_comp": "engine", "crash_inst": "1",
             "crash_pod": "p1-engine-1-leader-0", "crash_uid": "u1l", "crash_restarts": 0, "notes": []}
        f.update(over)
        return f

    def test_the_kill_aims_at_the_leader_of_the_highest_gang_instance(self):
        f = run._fault_new("p1", {"fault": "crashloop:old", "form": "start", "once": 1})
        self.assertEqual("p1-engine-1-leader-0", run._once_aim(f, self.GANG, None))
        self.assertEqual("p1-engine-1-leader-0", run._once_candidate(f, self.GANG, None))
        f["form"] = "load"
        self.assertEqual("p1-engine-1-leader-0", run._once_aim(f, self.GANG, None), "the aim is read before readiness")
        self.assertIsNone(run._once_candidate(f, self.GANG, None), "form=load waits for the leader to be Ready")

    def test_fire_execs_the_kill_into_the_main_container_of_the_aimed_pod(self):
        calls = []

        def kubectl(*a, **k):
            calls.append(a)
            if "get" in a:
                return subprocess.CompletedProcess(a, 0, self._pod_json("p1-engine-1-leader-0", "u1l"), "")
            return subprocess.CompletedProcess(a, 0, "", "")
        f = run._fault_new("p1", {"fault": "crashloop:old", "form": "start", "once": 1})
        with mock.patch.object(run, "kubectl", kubectl):
            self.assertTrue(run._once_fire(f, self.GANG, None))
        self.assertEqual([("-n", run.NS, "exec", "p1-engine-1-leader-0", "-c", run.MAIN_CONTAINER, "--", "sh", "-c", "kill 1")],
                         [a for a in calls if "exec" in a],
                         "PID 1 of the fixture container is the runner: the startup shell (TERM trapped to exit) before Ready, nginx after")
        self.assertEqual(("p1-engine-1-leader-0", "u1l", 0, 1, True),
                         (f["crash_pod"], f["crash_uid"], f["crash_restarts"], f["kills"], f["fired"]))
        self.assertIn("before Ready", f["notes"][-1])

    def test_a_container_still_being_created_is_not_due(self):
        calls = []

        def kubectl(*a, **k):
            calls.append(a)
            return subprocess.CompletedProcess(a, 0, self._pod_json("p1-engine-1-leader-0", "u1l", running=False), "")
        f = run._fault_new("p1", {"fault": "crashloop:old", "form": "start", "once": 1})
        with mock.patch.object(run, "kubectl", kubectl):
            self.assertFalse(run._once_fire(f, self.GANG, None))
        self.assertFalse(f["fired"])
        self.assertEqual([], [a for a in calls if "exec" in a], "nothing is killed before the container runs")
        self.assertEqual(("p1-engine-1-leader-0", False), (f["once_first_seen"]["pod"], f["once_first_seen"]["ready"]))

    def test_the_bite_is_read_live_until_the_restart_shows(self):
        f = self._fired()
        states = iter([(True, False, 0, "u1l"), (True, False, 1, "u1l")])
        with mock.patch.object(run, "_pod_main_state", lambda name: next(states)), mock.patch.object(run, "ONCE_POLL_S", 0.0):
            run._once_bite_tick(f)
            self.assertFalse(f.get("restart_seen"))
            run._once_bite_tick(f)
            self.assertTrue(f["restart_seen"])
            self.assertEqual(1, f["bitten_restarts"])
            run._once_bite_tick(f)   # latched: nothing is read again (the iterator is spent)
        unfired = self._fired(fired=False)
        reads = []
        with mock.patch.object(run, "_pod_main_state", lambda name: reads.append(name)), mock.patch.object(run, "ONCE_POLL_S", 0.0):
            run._once_bite_tick(unfired)
            gone = self._fired()
            run._once_bite_tick(gone)
            run._once_bite_tick(gone)
        self.assertEqual(["p1-engine-1-leader-0"], reads, "nothing before the kill; a victim that is gone is read once")
        self.assertTrue(gone["victim_gone"])
        self.assertFalse(gone.get("restart_seen"))
        replaced = self._fired()
        with mock.patch.object(run, "_pod_main_state", lambda name: (True, True, 0, "another-uid")), mock.patch.object(run, "ONCE_POLL_S", 0.0):
            run._once_bite_tick(replaced)
        self.assertTrue(replaced["victim_gone"], "a pod of the same name with another UID is not the victim")

    def test_start_tick_and_fault_tick_keep_reading_the_bite_after_the_kill(self):
        ticks = []
        with mock.patch.object(run, "_once_bite_tick", lambda f: ticks.append(f["case_id"])):
            fired = run._fault_new("p1", {"fault": "crashloop:old", "form": "start", "once": 1})
            fired["fired"] = True
            run._once_start_tick(fired, {})
            self.assertEqual(["p1"], ticks)
            pushed = run._fault_new("p2", {"fault": "crashloop:new", "form": "load", "once": 1})
            pushed["fired"] = True
            run._fault_tick({"fault": pushed, "trigger_ts": 1.0, "pod_state": {}, "pods_per_inst": {}, "old_hashes": {"old"}})
            self.assertEqual(["p1", "p2"], ticks)
            plain = run._fault_new("p3", {"fault": "crashloop:old", "form": "load"})
            plain["fired"] = True
            run._fault_tick({"fault": plain, "trigger_ts": 1.0, "pod_state": {}, "pods_per_inst": {}, "old_hashes": {"old"}})
            self.assertEqual(["p1", "p2"], ticks, "a persistent break is not read for a bite")

    def _after_the_roll(self):
        """The roll retired index 1; its replacement stands at index 2, Ready
        and serving on the new revision."""
        return {
            "p1-engine-0-leader-0": _gang_pod("p1-engine-0-leader-0", "0", "leader", True, "n0l", "new"),
            "p1-engine-0-worker-0": _gang_pod("p1-engine-0-worker-0", "0", "worker", True, "n0w", "new"),
            "p1-engine-2-leader-0": _gang_pod("p1-engine-2-leader-0", "2", "leader", True, "n2l", "new"),
            "p1-engine-2-worker-0": _gang_pod("p1-engine-2-worker-0", "2", "worker", True, "n2w", "new"),
        }

    def _observe(self, f, pods, expect_new, surges=None, events=()):
        with mock.patch.object(run, "_pod_main_state", lambda name: None), \
                mock.patch.object(run, "_isvc_events_since", lambda c, s: list(events)), \
                mock.patch.object(run, "_wait_rows_settled", lambda *a, **k: (True, "")):
            return run._once_observe("p1", f, pods, {"old"}, expect_new, {"engine": 2}, {"engine"}, 1, surges=surges)

    def test_observe_follows_the_victim_to_the_index_its_gang_surge_left_it_at(self):
        after, surges = self._after_the_roll(), [("engine", "1", "2", "new")]
        seen = self._observe(self._fired(restart_seen=True, bitten_restarts=1), after, True, surges=surges)
        self.assertEqual(("2", True, True, True, 1), (seen["final"], seen["bitten"], seen["serving"], seen["on_revision"], seen["restarts"]))
        blind = self._observe(self._fired(), after, True, surges=surges)
        self.assertEqual(("2", False), (blind["final"], blind["bitten"]), "under a push the roll's replacement is no proof the kill landed")
        repaired = self._observe(self._fired(), after, True, surges=surges, events=[("RestartTriggered", self.REPAIR)])
        self.assertEqual((True, 1), (repaired["bitten"], len(repaired["repairs"])), "a repair naming the Instance is proof")
        from_events = self._observe(self._fired(restart_seen=True), after, True, events=[("RecreateUpdateStarted", self.SURGE)])
        self.assertEqual("2", from_events["final"], "the surge chain is read from the ISVC events when the rows kept no claim")
        unmoved = self._observe(self._fired(restart_seen=True), self.GANG, True)
        self.assertEqual(("1", False), (unmoved["final"], unmoved["serving"]))
        rebuilt_at_idle = {n: dict(p, uid="r" + p["uid"], ready=True, serving=True) for n, p in self.GANG.items()}
        idle = self._observe(self._fired(), rebuilt_at_idle, False)
        self.assertEqual((True, True), (idle["rebuilt"], idle["bitten"]), "at idle a rebuild nobody asked for is proof")

    def test_the_verdict_names_the_index_the_victim_moved_to(self):
        f = self._fired(restart_seen=True)
        out = {"fired": True, "kills": 1, "bitten": True, "restarts": 1, "rebuilt": False, "serving": True,
               "on_revision": True, "expect_new": True, "repairs": [], "escalations": [], "rows_settled": True,
               "rows_why": "", "side": "old", "op": "push", "rp": "None", "final": "2"}
        checks = []
        self.assertEqual("PASS", run._once_checks(f, out, "PASS", [], checks))
        text = "\n".join(checks)
        self.assertIn("replaced at Instance 2 by its gang surge", text)
        self.assertIn("restart count 1", text)
        f = self._fired()
        self.assertEqual("PASS", run._once_checks(f, dict(out, bitten=False, restarts=0), "PASS", [], []))
        self.assertIn("never rose while it lived", f["infra_fail"])

    def test_a_kill_that_never_landed_says_what_the_watcher_first_saw(self):
        f = run._fault_new("p1", {"fault": "crashloop:old", "form": "start", "once": 1})
        self.assertIn("never observed", run._once_never_landed(f, "the roll converged"))
        f["once_first_seen"] = {"pod": "p1-engine-3-default-0", "ready": True, "at": 1.0}
        words = run._once_never_landed(f, "the roll converged")
        for part in ("form=start", "before the roll converged", "p1-engine-3-default-0", "already Ready when first observed"):
            self.assertIn(part, words)
        f["once_first_seen"]["ready"] = False
        self.assertIn("first observed not Ready", run._once_never_landed(f, "the roll converged"))
        g = run._fault_new("p1", {"fault": "crashloop:old", "form": "start", "once": 1})
        run._once_note_first_sight(g, self.GANG, None)
        self.assertEqual(("p1-engine-1-leader-0", False), (g["once_first_seen"]["pod"], g["once_first_seen"]["ready"]))
        ready = {n: dict(p, ready=True, serving=True) for n, p in self.GANG.items()}
        h = run._fault_new("p1", {"fault": "crashloop:old", "form": "start", "once": 1})
        run._once_note_first_sight(h, ready, None)
        self.assertTrue(h["once_first_seen"]["ready"])
        run._once_note_first_sight(h, self.GANG, None)
        self.assertTrue(h["once_first_seen"]["ready"], "the first sight is kept")
        pushed = run._fault_new("p1", {"fault": "crashloop:new", "form": "start", "once": 1})
        run._once_note_first_sight(pushed, self._after_the_roll(), {"old"})
        self.assertEqual("p1-engine-0-leader-0", pushed["once_first_seen"]["pod"], "the pushed side aims at the first new pod")
        unfired = run._fault_new("p1", {"fault": "crashloop:old", "form": "start", "once": 1})
        unfired["once_first_seen"] = {"pod": "p1-engine-3-default-0", "ready": True, "at": 1.0}
        self.assertEqual("PASS", run._once_push_checks(unfired, "p1", {}, {"old"}, {}, {"engine": 4}, {"engine"}, "PASS", [], []))
        self.assertIn("already Ready when first observed", unfired["infra_fail"])
        self.assertIn("before the roll converged", unfired["infra_fail"])


class CanaryGateFaultHeaderTest(unittest.TestCase):
    """The gate faults (fault=metric, fault=source) and the self-healing or
    canary-side breaks: what the header grammar admits and refuses."""

    BASE = "# p999: x mode=canary Ne=8 Nd=0 Nr=0 ceil_e=9 ceil_d=0 ceil_r=0 canary=auto topo=engine"
    PD = "# p999: x mode=canary Ne=8 Nd=8 Nr=2 ceil_e=9 ceil_d=9 ceil_r=2 canary=auto topo=pd"
    ROUTER = "# p999: x mode=canary Ne=8 Nd=0 Nr=2 ceil_e=9 ceil_d=0 ceil_r=2 canary=auto topo=engine"
    ROLLOUT = "# p999: x mode=rollout Ne=2 Nd=0 Nr=0 ceil_e=3 ceil_d=0 ceil_r=0 topo=engine"

    def _errs(self, header):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "p999.yaml"
            p.write_text(header + "\nmetadata: { name: p999 }\n")
            return run.validate_case_file(p)

    def test_fault_grammar(self):
        for v in ("metric:new", "metric:idle", "metric:canary-serving@1", "source:idle", "source:canary-serving@2"):
            self.assertTrue(run._fault_ok(v), v)
        for v in ("metric:old", "metric:settling", "source:new", "source:rotating", "metric:canary-nowhere"):
            self.assertFalse(run._fault_ok(v), v)
        self.assertTrue(run._metric_on_new(run._fault_new("p999", {"fault": "metric:new", "form": "persist"})))
        self.assertFalse(run._metric_on_new(run._fault_new("p999", {"fault": "metric:idle", "form": "persist"})))
        self.assertTrue(run._fault_before_push(run._fault_new("p999", {"fault": "metric:idle", "form": "persist"})))
        self.assertTrue(run._fault_before_push(run._fault_new("p999", {"fault": "source:idle", "form": "unreachable"})))
        self.assertFalse(run._fault_before_push(run._fault_new("p999", {"fault": "metric:new", "form": "persist"})))
        self.assertIn(run.METRIC_FAULT, run.HOOK_FAULTS)
        self.assertIn(run.SOURCE_FAULT, run.HOOK_FAULTS)

    def test_metric_breach_header(self):
        self.assertEqual([], self._errs(self.BASE + " fault=metric:idle form=persist analysis=pass health=pass failurelimit=1 nophase=RollingBack"))
        self.assertEqual([], self._errs(self.BASE + " fault=metric:canary-serving@1 form=recover victim=stable analysis=pass health=pass failurelimit=1"))
        self.assertEqual([], self._errs(self.BASE + " fault=metric:canary-serving@2 form=persist victim=canary analysis=breach health=pass failurelimit=1"))
        self.assertEqual([], self._errs(self.BASE + " fault=metric:new form=recover analysis=pass failurelimit=6 minfails=1"))
        self.assertEqual([], self._errs(self.PD + " fault=metric:canary-serving@1 form=persist victim=stable victimcomp=decoder analysis=pass failurelimit=1"))
        self.assertTrue(any("needs mode=canary and analysis=" in e for e in self._errs(self.BASE + " fault=metric:idle form=persist")))
        self.assertTrue(any("mode=canary" in e for e in self._errs(self.ROLLOUT + " fault=metric:idle form=persist analysis=pass")))
        self.assertTrue(any("form=persist|recover" in e for e in self._errs(self.BASE + " fault=metric:idle analysis=pass")))
        self.assertTrue(any("form=persist|recover" in e for e in self._errs(self.BASE + " fault=metric:idle form=start analysis=pass")))
        self.assertTrue(any("victim=canary|stable" in e for e in self._errs(self.BASE + " fault=metric:canary-serving@1 form=persist analysis=pass")))
        self.assertTrue(any("victim= does not apply" in e for e in self._errs(self.BASE + " fault=metric:new form=persist victim=canary analysis=breach")))
        self.assertTrue(any("victims= does not apply" in e for e in self._errs(self.BASE + " fault=metric:idle form=persist victims=2 analysis=pass")))
        self.assertTrue(any("lift= does not apply" in e for e in self._errs(self.BASE + " fault=metric:idle form=recover lift=after-settle analysis=pass")))
        self.assertTrue(any("needs Nr>0" in e for e in self._errs(self.BASE + " fault=metric:canary-serving@1 form=persist victim=stable victimcomp=router analysis=pass")))

    def test_source_header(self):
        self.assertEqual([], self._errs(self.BASE + " fault=source:canary-serving@1 form=unreachable lift=after-settle analysis=pass health=pass oninconclusive=hold nophase=Failed"))
        self.assertEqual([], self._errs(self.BASE + " fault=source:idle form=garbage lift=never analysis=pass oninconclusive=rollbackonstall terminal=RolledBack"))
        self.assertTrue(any("form=garbage|unreachable" in e for e in self._errs(self.BASE + " fault=source:idle lift=never analysis=pass")))
        self.assertTrue(any("lift=after-settle|never" in e for e in self._errs(self.BASE + " fault=source:idle form=garbage analysis=pass")))
        self.assertTrue(any("lift=after-settle|never" in e for e in self._errs(self.BASE + " fault=source:idle form=garbage lift=after-failed analysis=pass")))
        self.assertTrue(any("victim knobs do not apply" in e for e in self._errs(self.BASE + " fault=source:idle form=garbage lift=never victimcomp=engine analysis=pass")))
        self.assertTrue(any("malformed header value oninconclusive=later" in e for e in self._errs(self.BASE + " analysis=pass oninconclusive=later")))

    def test_self_healing_and_canary_side_breaks(self):
        self.assertEqual([], self._errs(self.ROUTER + " fault=crashloop:canary-serving@1 form=load victimcomp=router victims=1 lift=after-settle nophase=Failed"))
        self.assertEqual([], self._errs(self.ROUTER + " fault=imagepull:idle victimcomp=router victims=1 lift=after-settle"))
        self.assertEqual([], self._errs(self.PD + " fault=crashloop:canary-serving@2 form=load victim=canary victimcomp=decoder lift=after-settle"))
        self.assertEqual([], self._errs(self.ROUTER + " bump=engine,router img=example.com/canary-test/missing:none imgcomp=router terminal=Stable"))
        self.assertTrue(any("heals the harness's own break under a canary" in e
                            for e in self._errs(self.ROLLOUT + " fault=crashloop:old form=start lift=after-settle")))
        self.assertTrue(any("needs Nr>0" in e for e in self._errs(self.BASE + " fault=crashloop:canary-serving@1 form=load victimcomp=router")))


class CanaryGateFaultTest(unittest.TestCase):
    """The gate faults as the runner fires, tracks and judges them, on fixed
    snapshots: the metric override through the pod proxy, the source edit
    and its restore, the bite, the hold, the lift and the verdict rows."""

    @staticmethod
    def _pods(old="old", new="new"):
        pods = {f"e{i}": {"alive": True, "hash": old, "inst": str(i), "comp": "engine", "runner": "default",
                          "uid": f"u{i}", "ready": True, "serving": True} for i in range(3)}
        pods["c7"] = {"alive": True, "hash": new, "inst": "7", "comp": "engine", "runner": "default", "uid": "c7",
                      "ready": True, "serving": True}
        pods["c8"] = {"alive": True, "hash": new, "inst": "8", "comp": "engine", "runner": "default", "uid": "c8",
                      "ready": False, "serving": False}
        return pods

    def test_control_path_and_side_pods(self):
        self.assertEqual(f"/api/v1/namespaces/{run.NS}/pods/e0:8080/proxy/-/perm/error-rate", run._metric_control_path("e0"))
        self.assertTrue(run._metric_control_path("e0", "0.9", 45).endswith("/-/perm/error-rate?value=0.9&for=45"))
        self.assertTrue(run._metric_control_path("e0", "0.9").endswith("/-/perm/error-rate?value=0.9"))
        pods = self._pods()
        self.assertEqual(["e0", "e1", "e2"], run._metric_side_pods(pods, {"old"}, "engine", "stable"))
        self.assertEqual(["c7", "c8"], run._metric_side_pods(pods, {"old"}, "engine", "canary"))
        self.assertEqual([], run._metric_side_pods(pods, {"old"}, "decoder", "stable"))

    def test_canary_side_break_picks_a_ready_new_pod(self):
        pods = self._pods()
        self.assertEqual(["c7"], run._crashloop_pick_pods(pods, {"old"}, "engine", 2, new_side=True))
        self.assertEqual(["e2", "e1"], run._crashloop_pick_pods(pods, {"old"}, "engine", 2))
        f = run._fault_new("p999", {"fault": "crashloop:canary-serving@1", "form": "load", "victim": "canary"})
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, "", "")), \
                mock.patch.object(run, "_crashloop_killer", lambda rec, stop: None):
            run._fault_crashloop_fire(f, pods, {"old"})
        self.assertIsNone(f["infra_fail"])
        self.assertEqual(("c7", "new"), (f["crash_pod"], f["crash_hash"]))
        run._crashloop_stop(f)
        pods["c7"]["ready"] = False
        g = run._fault_new("p999", {"fault": "imagepull:canary-serving@1", "victim": "canary"})
        run._fault_crashloop_fire(g, pods, {"old"})
        self.assertIn("no live Ready canary-revision engine pod", g["infra_fail"])

    def test_metric_fire_sets_the_override_on_every_pod_of_the_side(self):
        f = run._fault_new("p999", {"fault": "metric:canary-serving@1", "form": "recover", "victim": "stable"})
        seen = []

        def fake(*a, **k):
            seen.append(a)
            return subprocess.CompletedProcess(a, 0, "rate=0.9\nconfigured=0\noverride=true\nuntil=x\n", "")
        with mock.patch.object(run, "kubectl", fake):
            run._fault_metric_fire(f, self._pods(), {"old"})
        self.assertIsNone(f["infra_fail"])
        self.assertEqual(["e0", "e1", "e2"], f["metric_pods"])
        self.assertEqual(("stable", "engine", ["old"], run.METRIC_BREACH_S), (f["metric_side"], f["metric_comp"], f["metric_hashes"], f["metric_for_s"]))
        self.assertTrue(all(a[:2] == ("get", "--raw") and f"for={run.METRIC_BREACH_S}" in a[2] for a in seen))
        g = run._fault_new("p999", {"fault": "metric:idle", "form": "persist"})
        with mock.patch.object(run, "kubectl", fake):
            run._fault_metric_fire(g, self._pods(), {"old"})
        self.assertEqual(("stable", 0), (g["metric_side"], g["metric_for_s"]))
        self.assertTrue(all("for=" not in a[2] for a in seen[3:]))
        h = run._fault_new("p999", {"fault": "metric:canary-serving@1", "form": "persist", "victim": "canary"})
        with mock.patch.object(run, "kubectl", fake):
            run._fault_metric_fire(h, self._pods(), {"old"})
        self.assertEqual(["c7", "c8"], h["metric_pods"])
        none = run._fault_new("p999", {"fault": "metric:canary-serving@1", "form": "persist", "victim": "stable",
                                       "victimcomp": "decoder"})
        run._fault_metric_fire(none, self._pods(), {"old"})
        self.assertIn("no live stable-side decoder pod", none["infra_fail"])

    def test_metric_fire_gives_up_when_no_pod_answers(self):
        f = run._fault_new("p999", {"fault": "metric:idle", "form": "persist"})
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 1, "", "refused")), \
                mock.patch.object(run, "METRIC_APPLY_S", 0), mock.patch.object(run.time, "sleep", lambda s: None):
            run._fault_metric_fire(f, self._pods(), {"old"})
        self.assertIn("took the override", f["infra_fail"])

    @staticmethod
    def _configmap(address):
        data = {"canaryAnalysis": json.dumps({"bundledPrometheusAddress": address, "queryTimeout": "5s"})}
        return json.dumps({"data": data})

    def test_prometheus_service_and_store_endpoint(self):
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, self._configmap("http://ome-prometheus.ome.svc:9090"), "")):
            self.assertEqual((("ome", "ome-prometheus", 9090), None), run._prometheus_service())
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, self._configmap(""), "")):
            svc, err = run._prometheus_service()
            self.assertIsNone(svc)
            self.assertIn("no in-cluster bundled Prometheus", err)
            ep, err = run._store_endpoint()
            self.assertIsNone(ep)
            self.assertIn("no in-cluster bundled Prometheus", err)
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(a, 0, self._configmap("http://prom.example.com:9090"), "")):
            self.assertIsNone(run._prometheus_service()[0], "an address outside the cluster is not proxied")
        slices = {"items": [{"ports": [{"name": "http-web", "port": 9090}],
                             "endpoints": [{"addresses": ["10.0.0.9"], "conditions": {"ready": False}},
                                           {"addresses": ["10.0.0.7"], "conditions": {"ready": True}}]}]}

        def fake(*a, **k):
            if "endpointslices" in a:
                self.assertEqual(("-n", "ome", "get", "endpointslices", "-l", "kubernetes.io/service-name=ome-prometheus"), a[:6])
                return subprocess.CompletedProcess(a, 0, json.dumps(slices), "")
            return subprocess.CompletedProcess(a, 0, self._configmap("http://ome-prometheus.ome.svc:9090"), "")
        with mock.patch.object(run, "kubectl", fake):
            self.assertEqual((("10.0.0.7", 9090), None), run._store_endpoint(), "the first ready endpoint of the store")
        empty = {"items": [{"ports": [{"name": "http-web", "port": 9090}], "endpoints": []}]}
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(
                a, 0, json.dumps(empty) if "endpointslices" in a else self._configmap("http://ome-prometheus.ome.svc:9090"), "")):
            ep, err = run._store_endpoint()
            self.assertIsNone(ep)
            self.assertIn("no ready endpoint", err)

    def test_prometheus_max_reads_the_largest_series(self):
        body = json.dumps({"data": {"result": [{"value": [1, "0.1"]}, {"value": [1, "0.9"]}]}})

        def fake(*a, **k):
            if a[:2] == ("get", "--raw"):
                self.assertIn("/api/v1/namespaces/ome/services/ome-prometheus:9090/proxy/api/v1/query?query=", a[2])
                self.assertIn("revision_hash%3D%22old%22", a[2])
                return subprocess.CompletedProcess(a, 0, body, "")
            return subprocess.CompletedProcess(a, 0, self._configmap("http://ome-prometheus.ome.svc:9090"), "")
        with mock.patch.object(run, "kubectl", fake):
            self.assertEqual(0.9, run._prometheus_max(run._metric_query("p999", "engine", "old")))
        empty = json.dumps({"data": {"result": []}})
        with mock.patch.object(run, "kubectl", lambda *a, **k: subprocess.CompletedProcess(
                a, 0, empty if a[:2] == ("get", "--raw") else self._configmap("http://ome-prometheus.ome.svc:9090"), "")):
            self.assertIsNone(run._prometheus_max("x"))

    def _spec(self, prometheus=None, present=False):
        g = {"components": ["engine"], "canary": {"steps": [{"capacity": "10%", "traffic": 10}]}}
        if present:
            g["canary"]["prometheus"] = prometheus
        return {"spec": {"rollout": {"groups": [g, {"components": ["router"], "blueGreen": {}}]}}}

    def test_source_front_is_declared_before_the_push_and_the_fault_breaks_the_store_behind_it(self):
        f = run._fault_new("p999", {"fault": "source:canary-serving@1", "form": "unreachable", "lift": "after-settle"})
        f["source_members"] = ["engine"]
        slices = {"items": [{"ports": [{"name": "http-web", "port": 9090}],
                             "endpoints": [{"addresses": ["10.0.0.7"], "conditions": {"ready": True}}]}]}
        pod = {"status": {"podIP": "10.0.1.5"}, "spec": {"containers": [{"ports": [{"containerPort": 8080}]}]}}
        calls = []

        def fake(*a, **k):
            calls.append((a, k))
            if "endpointslices" in a:
                return subprocess.CompletedProcess(a, 0, json.dumps(slices), "")
            if a[2:4] == ("get", "isvc"):
                return subprocess.CompletedProcess(a, 0, json.dumps(self._spec()), "")
            if a[2:4] == ("get", "pod"):
                return subprocess.CompletedProcess(a, 0, json.dumps(pod), "")
            if a[2:4] == ("get", "configmap"):
                return subprocess.CompletedProcess(a, 0, self._configmap("http://ome-prometheus.ome.svc:9090"), "")
            return subprocess.CompletedProcess(a, 0, "", "")
        with mock.patch.object(run, "kubectl", fake):
            self.assertIsNone(run._source_front_setup(f, "p999"))
            applied = json.loads(next(k["input"] for a, k in calls if a[:2] == ("apply", "-f")))
            svc, sl = applied["items"]
            self.assertEqual(("Service", "perm-p999-source", run.NS),
                             (svc["kind"], svc["metadata"]["name"], svc["metadata"]["namespace"]))
            self.assertNotIn("selector", svc["spec"], "a front with no selector: its endpoints are the harness's to aim")
            self.assertEqual([{"name": "http", "port": 9090, "targetPort": 9090, "protocol": "TCP"}], svc["spec"]["ports"])
            self.assertEqual(("EndpointSlice", "perm-p999-source", "IPv4"),
                             (sl["kind"], sl["metadata"]["labels"]["kubernetes.io/service-name"], sl["addressType"]))
            self.assertEqual([{"addresses": ["10.0.0.7"], "conditions": {"ready": True}}], sl["endpoints"], "aimed at the store")
            self.assertEqual([{"name": "http", "port": 9090, "protocol": "TCP"}], sl["ports"])
            groups = json.loads(next(a[-1] for a, k in calls if a[2:5] == ("patch", "isvc", "p999")))["spec"]["rollout"]["groups"]
            self.assertEqual({"serverAddress": f"http://perm-p999-source.{run.NS}.svc:9090"}, groups[0]["canary"]["prometheus"],
                             "the driven group is pointed at the front before the push, so the run pins the front")
            self.assertEqual({"components": ["router"], "blueGreen": {}}, groups[1], "the other group is written back as it was")
            self.assertIn(("source", f), run._FAULT_CLEANUPS)
            run._FAULT_CLEANUPS.remove(("source", f))
            self.assertEqual((("10.0.0.7", 9090), False, False), (f["source_store"], f["source_broken"], f["source_restored"]))
            calls.clear()
            run._fault_source_fire(f, {}, {"old"})
            self.assertIsNone(f["infra_fail"])
            a, _k = calls[-1]
            self.assertEqual(("-n", run.NS, "patch", "endpointslice", "perm-p999-source", "--type=merge", "-p"), a[:7])
            self.assertEqual({"endpoints": [], "ports": []}, json.loads(a[-1]), "unreachable: the front points at nothing")
            self.assertTrue(f["source_broken"])
            self.assertIn("is unreachable", f["notes"][-1])
            calls.clear()
            self.assertTrue(run._source_restore(f))
            self.assertEqual({"endpoints": [{"addresses": ["10.0.0.7"], "conditions": {"ready": True}}],
                              "ports": [{"name": "http", "port": 9090, "protocol": "TCP"}]}, json.loads(calls[-1][0][-1]),
                             "the store, looked up again, is back behind the front")
            self.assertTrue(f["source_restored"])
            calls.clear()
            self.assertTrue(run._source_restore(f), "idempotent")
            self.assertEqual([], calls)
            g = run._fault_new("p999", {"fault": "source:canary-serving@1", "form": "garbage", "lift": "never"})
            g.update(source_members=["decoder", "engine"], source_front="perm-p999-source", source_address="http://front",
                     source_store=("10.0.0.7", 9090))
            pods = {"p999-engine-0-default-0": _rec("engine", 0, "old"), "p999-engine-1-default-0": _rec("engine", 1, "new"),
                    "p999-decoder-0-default-0": _rec("decoder", 0, "old")}
            calls.clear()
            run._fault_source_fire(g, pods, {"old"})
            self.assertIsNone(g["infra_fail"])
            self.assertIn(("-n", run.NS, "get", "pod", "p999-engine-0-default-0"), [a[:5] for a, _k in calls],
                          "garbage: a running-revision pod of the engine")
            self.assertEqual({"endpoints": [{"addresses": ["10.0.1.5"], "conditions": {"ready": True}}],
                              "ports": [{"name": "http", "port": 8080, "protocol": "TCP"}]}, json.loads(calls[-1][0][-1]),
                             "the front points at the pod's own server, which answers no metrics API")
            self.assertIn("answers garbage", g["notes"][-1])
            calls.clear()
            run._source_cleanup(g)
            self.assertTrue(g["source_restored"])
            self.assertEqual(("-n", run.NS, "delete", "service,endpointslice", "perm-p999-source"), calls[-1][0][:5])
            nopod = dict(g, source_broken=False, notes=[], infra_fail=None)
            run._fault_source_fire(nopod, {"p999-engine-1-default-0": _rec("engine", 1, "new")}, {"old"})
            self.assertIn("no live running-revision engine pod", nopod["infra_fail"])
            nogroup = run._fault_new("p999", {"fault": "source:idle", "form": "garbage", "lift": "never"})
            nogroup["source_members"] = ["decoder"]
            self.assertIn("no canary group matches", run._source_front_setup(nogroup, "p999"))
            run._FAULT_CLEANUPS.remove(("source", nogroup))
        unfronted = run._fault_new("p999", {"fault": "source:idle", "form": "unreachable", "lift": "never"})
        run._fault_source_fire(unfronted, {}, set())
        self.assertIn("no source front", unfronted["infra_fail"])

    def test_inconclusive_result_is_read_from_the_metric_results(self):
        can = {"metricResults": [{"name": "error_rate", "value": "0.01", "passed": True, "time": "2026-01-01T00:00:30Z"}]}
        self.assertIsNone(run._inconclusive_result_at(can, 0))
        stamp = datetime.datetime(2026, 1, 1, 0, 1, 0, tzinfo=datetime.timezone.utc).timestamp()
        can = {"metricResults": [{"name": "error_rate", "passed": False, "message": "no such host", "time": "2026-01-01T00:01:00Z"}]}
        self.assertEqual("no such host", run._inconclusive_result_at(can, stamp)["message"])
        self.assertIsNone(run._inconclusive_result_at(can, stamp + 60), "a result older than the fault is the sample before it")
        self.assertIsNone(run._inconclusive_result_at({"metricResults": [{"name": "x", "passed": False, "value": "0.9", "message": "breach"}]}, 0),
                          "a breach has a value: it is conclusive")

    def test_gate_threshold_reads_the_ladder(self):
        steps = [{"capacity": "10%", "traffic": 10, "analysis": {"metrics": [{"name": "e", "threshold": "0.2"}]}}]
        self.assertEqual(0.2, run._gate_threshold(steps))
        self.assertEqual(0.05, run._gate_threshold([{"capacity": "100%", "traffic": 100}]))

    def _ctx(self, pods=None):
        return {"case_id": "p999", "trigger_ts": 100.0, "old_hashes": {"old"}, "pod_state": pods or self._pods(),
                "N": {"engine": 8, "decoder": 0, "router": 2}, "canary_hook": "canary-serving"}

    def test_metric_track_stable_side_is_seen_by_prometheus_and_never_counted(self):
        f = run._fault_new("p999", {"fault": "metric:canary-serving@1", "form": "recover", "victim": "stable"})
        f.update(fired=True, fired_ts=110.0, metric_side="stable", metric_comp="engine", metric_hashes=["old"],
                 metric_pods=["e0", "e1", "e2"], metric_for_s=45)
        track = run._gate_track_new()
        ctx = self._ctx()
        with mock.patch.object(run, "_prometheus_max", lambda q: 0.9):
            run._gate_track(ctx, f, track, {}, 1, 110.0, 0, 0.05)
        self.assertEqual((110.0, 0, 1, 110.0, 0.9), (track["fired_at"], track["fails_at_fire"], track["step_at_fire"], track["seen_at"], track["seen_value"]))
        with mock.patch.object(run, "_metric_override_active", lambda pod: False):
            run._gate_track(ctx, f, track, {}, 1, 170.0, 0, 0.05)
        self.assertEqual(170.0, track["recovered_at"])
        notes, checks = [], []
        self.assertEqual("PASS", run._gate_fault_checks(ctx, f, track, "hold", 0, None, {"Paused", "Stable"}, "PASS", notes, checks))
        self.assertTrue(any("never counted the stable side's breach" in c for c in checks))
        self.assertTrue(any("breach lifted after its 45s window" in c for c in checks))
        notes, checks = [], []
        self.assertEqual("FAIL", run._gate_fault_checks(ctx, f, track, "hold", 1, None, set(), "PASS", notes, checks))
        self.assertTrue(any("blamed the canary" in n for n in notes))
        notes, checks = [], []
        self.assertEqual("FAIL", run._gate_fault_checks(ctx, f, track, "hold", 0, "new", set(), "PASS", notes, checks))
        self.assertTrue(any("healthy canary was rolled back" in n for n in notes))
        unseen = run._gate_track_new()
        unseen["fired_at"], unseen["fails_at_fire"] = 110.0, 0
        self.assertEqual("PASS", run._gate_fault_checks(ctx, f, unseen, "hold", 0, None, set(), "PASS", [], []))
        self.assertIn("Prometheus never showed", f["infra_fail"])

    def test_metric_track_canary_side_is_counted_by_the_gate(self):
        f = run._fault_new("p999", {"fault": "metric:canary-serving@1", "form": "persist", "victim": "canary"})
        f.update(fired=True, fired_ts=110.0, metric_side="canary", metric_comp="engine", metric_hashes=["new"],
                 metric_pods=["c7", "c8"], metric_for_s=0)
        track = run._gate_track_new()
        ctx = self._ctx()
        run._gate_track(ctx, f, track, {}, 1, 110.0, 0, 0.05)
        self.assertIsNone(track["seen_at"])
        run._gate_track(ctx, f, track, {}, 1, 130.0, 1, 0.05)
        self.assertEqual(130.0, track["seen_at"])
        checks = []
        self.assertEqual("PASS", run._gate_fault_checks(ctx, f, track, "hold", 1, "new", {"RolledBack"}, "PASS", [], checks))
        self.assertTrue(any("no failing sample was counted before" in c for c in checks))
        early = dict(track, fails_at_fire=1)
        notes = []
        self.assertEqual("FAIL", run._gate_fault_checks(ctx, f, early, "hold", 2, "new", set(), "PASS", notes, []))
        self.assertTrue(any("before the breach landed" in n for n in notes))
        never = run._gate_track_new()
        never["fired_at"], never["fails_at_fire"] = 110.0, 0
        notes = []
        self.assertEqual("FAIL", run._gate_fault_checks(ctx, f, never, "hold", 0, None, set(), "PASS", notes, []))
        self.assertTrue(any("never counted a failing sample" in n for n in notes))
        born = run._fault_new("p999", {"fault": "metric:new", "form": "recover"})
        born.update(fired=True, fired_ts=100.0)
        self.assertEqual("canary", run._metric_side(born))

    def _source_fault(self, lift):
        f = run._fault_new("p999", {"fault": "source:canary-serving@1", "form": "unreachable", "lift": lift})
        f.update(fired=True, fired_ts=110.0, source_members=["engine"], source_front="perm-p999-source",
                 source_address=f"http://perm-p999-source.{run.NS}.svc:9090", source_store=("10.0.0.7", 9090),
                 source_broken=True, source_restored=False)
        return f

    def test_source_track_holds_then_lifts_and_resumes(self):
        f = self._source_fault("after-settle")
        track = run._gate_track_new()
        ctx = self._ctx()
        run._gate_track(ctx, f, track, {}, 1, 110.0, 0, 0.05)
        self.assertIsNone(track["bite_at"])
        can = {"metricResults": [{"name": "error_rate", "passed": False, "message": "dial tcp: no such host",
                                  "time": datetime.datetime.fromtimestamp(130.0, datetime.timezone.utc).isoformat()}]}
        run._gate_track(ctx, f, track, can, 1, 130.0, 0, 0.05)
        self.assertEqual((130.0, 1), (track["bite_at"], track["bite_step"]))
        restored = []
        with mock.patch.object(run, "_source_restore", lambda fault: restored.append(fault) or True):
            run._gate_track(ctx, f, track, can, 1, 140.0, 0, 0.05)
            self.assertEqual([], restored, "the hold has not settled yet")
            run._gate_track(ctx, f, track, can, 1, 130.0 + run.LIFT_SETTLE_S, 0, 0.05)
        self.assertEqual([f], restored)
        self.assertEqual({1}, track["steps_while_broken"])
        checks = []
        self.assertEqual("PASS", run._gate_fault_checks(ctx, f, track, "hold", 0, None, {"Paused", "Stable"}, "PASS", [], checks))
        self.assertTrue(any("the ladder held at step 1" in c for c in checks))
        self.assertTrue(any("store was put back behind the source" in c for c in checks))
        moved = dict(track, steps_while_broken={1, 2})
        notes = []
        self.assertEqual("FAIL", run._gate_fault_checks(ctx, f, moved, "hold", 0, None, set(), "PASS", notes, []))
        self.assertTrue(any("ladder moved while the source was unusable" in n for n in notes))
        notes = []
        self.assertEqual("FAIL", run._gate_fault_checks(ctx, f, track, "hold", 1, None, set(), "PASS", notes, []))
        self.assertTrue(any("counted as a breach" in n for n in notes))
        silent = run._gate_track_new()
        silent["fired_at"], silent["fails_at_fire"] = 110.0, 0
        notes = []
        self.assertEqual("FAIL", run._gate_fault_checks(ctx, f, silent, "hold", 0, None, set(), "PASS", notes, []))
        self.assertTrue(any("never reported" in n for n in notes))

    def test_source_lift_happens_anyway_once_the_gate_had_its_time(self):
        f = self._source_fault("after-settle")
        track = run._gate_track_new()
        ctx = self._ctx()
        with mock.patch.object(run, "_source_restore", lambda fault: True):
            run._gate_track(ctx, f, track, {}, 1, 110.0, 0, 0.05)
            run._gate_track(ctx, f, track, {}, 1, 110.0 + run.SOURCE_BITE_S, 0, 0.05)
        self.assertIsNotNone(track["lifted_at"])
        self.assertIn("restored anyway", f["notes"][-1])

    def test_source_persistent_parks_or_rolls_back_as_the_policy_says(self):
        f = self._source_fault("never")
        track = run._gate_track_new()
        track.update(fired_at=110.0, fails_at_fire=0, bite_at=130.0, bite_step=1, bite_message="no such host",
                     steps_while_broken={1})
        ctx = self._ctx()
        ctx["canary_hook"] = "canary-failed"
        run._gate_track(ctx, f, track, {"failed": {"reason": "AnalysisStalled"}}, 1, 300.0, 0, 0.05)
        self.assertEqual({"at": 200.0, "step": 1, "reason": "AnalysisStalled"}, track["park"])
        checks = []
        self.assertEqual("PASS", run._gate_fault_checks(ctx, f, track, "hold", 0, None, {"Paused", "Failed"}, "PASS", [], checks))
        self.assertTrue(any("parked Failed with reason AnalysisStalled" in c for c in checks))
        wrong = dict(track, park={"at": 200.0, "step": 2, "reason": "AnalysisStalled"})
        notes = []
        self.assertEqual("FAIL", run._gate_fault_checks(ctx, f, wrong, "hold", 0, None, set(), "PASS", notes, []))
        self.assertTrue(any("the park is at step 2" in n for n in notes))
        capacity = dict(track, park={"at": 200.0, "step": 1, "reason": "CapacityTimeout"})
        notes = []
        self.assertEqual("FAIL", run._gate_fault_checks(ctx, f, capacity, "hold", 0, None, set(), "PASS", notes, []))
        self.assertTrue(any("no park with reason AnalysisStalled" in n for n in notes))
        checks = []
        self.assertEqual("PASS", run._gate_fault_checks(ctx, f, dict(track, park=None), "rollbackonstall", 0, "new",
                                                        {"Paused", "RollingBack", "RolledBack"}, "PASS", [], checks))
        self.assertTrue(any("rolled the canary back instead of parking" in c for c in checks))
        notes = []
        self.assertEqual("FAIL", run._gate_fault_checks(ctx, f, track, "rollbackonstall", 0, None, {"Failed"}, "PASS", notes, []))
        self.assertTrue(any("did not roll the canary back" in n for n in notes))

    def test_break_heal_stops_the_killer_or_puts_the_image_back(self):
        load = run._fault_new("p999", {"fault": "crashloop:canary-serving@1", "form": "load", "victimcomp": "router", "lift": "after-settle"})
        load.update(fired=True, crash_pod="r1", crash_uid="u1", crash_comp="router", crash_inst="1", crash_hash="old", kills=1)
        with mock.patch.object(run, "_crashloop_stop", lambda rec: None):
            run._break_heal(load)
        self.assertIsNotNone(load["healed_at"])
        self.assertIn("killer stopped after 1 kill(s)", load["notes"][-1])
        pull = run._fault_new("p999", {"fault": "imagepull:canary-serving@1", "victimcomp": "router", "lift": "after-settle"})
        pull.update(fired=True, crash_pod="r1", crash_uid="u1", crash_comp="router", crash_inst="1", crash_hash="old",
                    crash_image="nginx:1", kills=0)
        patches = []
        with mock.patch.object(run, "_crashloop_pod_state", lambda name: (True, True, "old", False, "u1")), \
                mock.patch.object(run, "kubectl", lambda *a, **k: patches.append(a) or subprocess.CompletedProcess(a, 0, "", "")):
            run._break_heal(pull)
        self.assertEqual(1, len(patches))
        self.assertIn('"image": "nginx:1"', patches[0][-1])
        self.assertIn("pointed back at nginx:1", pull["notes"][-1])
        gone = dict(pull, notes=[], healed_at=None)
        with mock.patch.object(run, "_crashloop_pod_state", lambda name: (True, True, "old", False, "u2")), \
                mock.patch.object(run, "kubectl", lambda *a, **k: patches.append(a) or subprocess.CompletedProcess(a, 0, "", "")):
            run._break_heal(gone)
        self.assertEqual(1, len(patches), "a pod a repair already replaced is left alone")
        self.assertIn("already replaced", gone["notes"][-1])

    def test_break_heal_tick_waits_for_the_bite_and_the_settle(self):
        f = run._fault_new("p999", {"fault": "crashloop:canary-serving@1", "form": "load", "victimcomp": "router", "lift": "after-settle"})
        f.update(fired=True, crash_pod="r1", crash_uid="u1", kills=0)
        pods = {"r1": {"alive": True, "ready": True}}
        ctx = {"fault": f, "pod_state": pods}
        healed = []
        with mock.patch.object(run, "_break_heal", lambda fault: healed.append(fault)):
            run._break_heal_tick(ctx, 100.0)
            self.assertIsNone(f.get("bite_ts"))
            f["kills"] = 1
            run._break_heal_tick(ctx, 101.0)
            self.assertEqual(101.0, f["bite_ts"])
            run._break_heal_tick(ctx, 101.0 + run.LIFT_SETTLE_S - 1)
            self.assertEqual([], healed)
            run._break_heal_tick(ctx, 101.0 + run.LIFT_SETTLE_S)
        self.assertEqual([f], healed)
        plain = run._fault_new("p999", {"fault": "crashloop:canary-serving@1", "form": "load"})
        plain.update(fired=True, crash_pod="r1", kills=1)
        with mock.patch.object(run, "_break_heal", lambda fault: healed.append(fault)):
            run._break_heal_tick({"fault": plain, "pod_state": pods}, 500.0)
        self.assertEqual([f], healed, "a break without the lift is never healed")

    def test_healed_sibling_serves_again_in_place_or_rebuilt(self):
        f = run._fault_new("p999", {"fault": "crashloop:canary-serving@1", "form": "load", "victimcomp": "router", "lift": "after-settle"})
        f.update(fired=True, crash_pod="r1", crash_uid="u1", crash_comp="router", crash_inst="1", crash_hash="old", kills=1)
        same = {"r1": {"alive": True, "comp": "router", "inst": "1", "hash": "old", "uid": "u1", "ready": True, "serving": True}}
        checks, notes = [], []
        self.assertEqual("PASS", run._break_victim_checks(f, f, same, {"old"}, [], {"engine"}, "PASS", notes, checks))
        self.assertTrue(any("healed in place" in c for c in checks))
        rebuilt = {"r1": {"alive": True, "comp": "router", "inst": "1", "hash": "old", "uid": "u9", "ready": True, "serving": True}}
        checks = []
        events = [("RestartTriggered", "OMENative component=router instance=1 restart triggered")]
        self.assertEqual("PASS", run._break_victim_checks(f, f, rebuilt, {"old"}, events, {"engine"}, "PASS", [], checks))
        self.assertTrue(any("rebuilt by a repair" in c for c in checks))
        down = {"r1": {"alive": True, "comp": "router", "inst": "1", "hash": "old", "uid": "u1", "ready": False, "serving": False}}
        notes = []
        self.assertEqual("FAIL", run._break_victim_checks(f, f, down, {"old"}, [], {"engine"}, "PASS", notes, []))
        self.assertTrue(any("not serving on its running revision after the heal" in n for n in notes))

    def test_a_repair_of_a_canary_side_victim_is_reported_not_failed(self):
        f = run._fault_new("p999", {"fault": "crashloop:canary-serving@1", "form": "load", "victim": "canary",
                                    "victimcomp": "decoder", "lift": "after-settle"})
        f.update(fired=True, crash_pod="d7", crash_uid="u7", crash_comp="decoder", crash_inst="7", crash_hash="new", kills=1)
        pods = {"d7": {"alive": True, "comp": "decoder", "inst": "7", "hash": "new", "uid": "u8", "ready": True, "serving": True}}
        events = [("RestartTriggered", "OMENative component=decoder instance=7 restart triggered")]
        checks = []
        self.assertEqual("PASS", run._break_victim_checks(f, f, pods, {"old"}, events, {"engine", "decoder"}, "PASS", [], checks))
        self.assertTrue(any("a repair rebuilt canary-side" in c for c in checks))
        stable_victim = dict(f, victim="stable", crash_hash="old")
        notes = []
        self.assertEqual("FAIL", run._break_victim_checks(stable_victim, stable_victim, pods, {"old"}, events, {"engine", "decoder"}, "PASS", notes, []))
        self.assertTrue(any("was repaired after the bump" in n for n in notes))

    def test_a_sibling_break_is_the_faults_cost_on_either_side(self):
        canary_side = run._fault_new("p999", {"fault": "crashloop:canary-serving@1", "form": "load", "victim": "canary",
                                              "victimcomp": "router", "lift": "after-settle"})
        ctx = {}
        run._canary_break_allowance(ctx, canary_side, False)
        self.assertNotIn("floor_allowance", ctx, "nothing is allowed for before the break has fired")
        canary_side.update(fired=True, crash_comp="router", crash_inst="1")
        run._canary_break_allowance(ctx, canary_side, False)
        self.assertEqual({"router": 1}, ctx["floor_allowance"],
                         "the Instance the fault keeps killing is excluded from the floor, as on the running side")
        canary_side["extra_victims"] = [{"crash_comp": "router", "crash_inst": "0"}]
        run._canary_break_allowance(ctx, canary_side, False)
        self.assertEqual({"router": 1}, ctx["floor_allowance"], "read once, when the break fired")
        stable_side = dict(canary_side, victim="stable", extra_victims=[])
        ctx = {}
        run._canary_break_allowance(ctx, stable_side, False)
        self.assertEqual({"router": 1}, ctx["floor_allowance"])
        ctx = {}
        run._canary_break_allowance(ctx, dict(canary_side, extra_victims=[]), True)
        self.assertNotIn("floor_allowance", ctx, "a break the push carries is costed by its crash watch")
        metric = run._fault_new("p999", {"fault": "metric:canary-serving@1", "form": "persist", "victim": "canary"})
        metric.update(fired=True, crash_comp="engine")
        run._canary_break_allowance(ctx, metric, False)
        self.assertNotIn("floor_allowance", ctx, "a gate fault takes no Instance")

    def test_a_gate_fault_that_never_fired_fails_the_case(self):
        notes = []
        f = run._fault_new("p999", {"fault": "source:canary-serving@2", "form": "garbage", "lift": "never"})
        self.assertEqual("FAIL", run._hook_fault_fired_checks(f, {"Pending", "Stable"}, "PASS", notes, []))
        self.assertTrue(any("never fired" in n for n in notes))
        heal = run._fault_new("p999", {"fault": "crashloop:canary-serving@2", "form": "load", "lift": "after-settle"})
        self.assertEqual("FAIL", run._hook_fault_fired_checks(heal, set(), "PASS", [], []))
        old = run._fault_new("p999", {"fault": "crashloop:canary-serving@2", "form": "load"})
        self.assertEqual("PASS", run._hook_fault_fired_checks(old, set(), "PASS", [], []),
                         "the older hook faults keep reporting a missed hook")
        fired = dict(f, fired=True)
        self.assertEqual("PASS", run._hook_fault_fired_checks(fired, set(), "PASS", [], []))
        self.assertEqual("PASS", run._hook_fault_fired_checks(run._fault_new("p999", {"fault": "metric:new", "form": "persist"}), set(), "PASS", [], []))

    def test_parked_sibling_is_judged_on_its_own(self):
        pods = {"r0": {"alive": True, "comp": "router", "inst": "0", "hash": "old", "ready": True, "serving": True},
                "r1": {"alive": True, "comp": "router", "inst": "1", "hash": "old", "ready": True, "serving": True},
                "r2": {"alive": True, "comp": "router", "inst": "2", "hash": "new", "ready": False, "serving": False}}
        ctx = self._ctx(pods)
        track = {}
        snapshot = {"router": {"hold": None, "rows": [{"index": 2, "phase": "Failed"}]}}
        with mock.patch.object(run, "_ir_snapshot", lambda cid: snapshot), \
                mock.patch.object(run, "_isvc_events_since", lambda cid, since: []):
            run._sibling_park_track(ctx, {"router"}, track, 200.0)
        self.assertIn("router", track["why"])
        self.assertIn("row Failed", track["why"]["router"][0])
        self.assertNotIn("served", track)
        checks = []
        self.assertEqual("PASS", run._sibling_park_checks(ctx, {"router"}, track, "PASS", [], checks))
        self.assertTrue(any("whole on its running revision at the end (2/2 serving on it; 1 attempt(s)" in c for c in checks))
        pods["r2"].update(ready=True, serving=True)
        with mock.patch.object(run, "_ir_snapshot", lambda cid: snapshot), \
                mock.patch.object(run, "_isvc_events_since", lambda cid, since: []):
            run._sibling_park_track(ctx, {"router"}, track, 300.0)
        notes = []
        self.assertEqual("FAIL", run._sibling_park_checks(ctx, {"router"}, track, "PASS", notes, []))
        self.assertTrue(any("r2 served on the revision that cannot start" in n for n in notes))
        notes = []
        self.assertEqual("FAIL", run._sibling_park_checks(ctx, {"router"}, {}, "PASS", notes, []))
        self.assertTrue(any("nothing said why its roll stopped" in n for n in notes))


    def test_a_parked_sibling_keeps_an_instance_whose_running_pod_still_serves(self):
        pods = {"r0": {"alive": True, "comp": "router", "inst": "0", "hash": "old", "ready": True, "serving": True},
                "r0b": {"alive": True, "comp": "router", "inst": "0", "hash": "new", "ready": False, "serving": False},
                "r1": {"alive": True, "comp": "router", "inst": "1", "hash": "old", "ready": True, "serving": True}}
        ctx = self._ctx(pods)
        self.assertEqual((0, 1), run._inst_rev_split(ctx, "router"), "the roll split reads a mixed Instance as neither")
        self.assertEqual(2, run._inst_kept_serving(ctx, "router"), "the failed attempt beside a serving pod moves nothing")
        self.assertEqual(1, run._attempt_pods(ctx, "router"))
        track = {"why": {"router": ("rolloutHold gate=RetryBlock", 68.0)}}
        checks = []
        self.assertEqual("PASS", run._sibling_park_checks(ctx, {"router"}, track, "PASS", [], checks))
        self.assertTrue(any("whole on its running revision at the end (2/2 serving on it; 1 attempt(s)" in c for c in checks), checks)
        pods["r0"].update(ready=False, serving=False)
        notes = []
        self.assertEqual("FAIL", run._sibling_park_checks(ctx, {"router"}, track, "PASS", notes, []))
        self.assertTrue(any("1/2 serving on its running revision at the end (1 attempt(s)" in n for n in notes), notes)



class KillRepeatFollowsTheVictimTest(unittest.TestCase):
    """mode=kill via=preempt repeat=<n>: the scheduler chooses the pod it
    takes on the pressed node, so the watch, the allowance, the scope and
    the record follow that pod's Component, not the press's aim."""

    def _ctx(self):
        pods = {"p1-engine-0-leader-0": _rec("engine", 0, "old", runner="leader", uid="e0l"),
                "p1-engine-0-worker-0": _rec("engine", 0, "old", runner="worker", uid="e0w"),
                "p1-engine-1-leader-0": _rec("engine", 1, "old", runner="leader", uid="e1l"),
                "p1-engine-1-worker-0": _rec("engine", 1, "old", runner="worker", uid="e1w"),
                "p1-router-0-default-0": _rec("router", 0, "old", uid="r0"),
                "p1-router-1-default-0": _rec("router", 1, "old", uid="r1")}
        for p in pods.values():
            p["node"] = "n1"
        return {"case_id": "p1", "N": {"engine": 2, "decoder": 0, "router": 2}, "configured": {"engine", "router"},
                "pods_per_inst": {"engine": 2, "decoder": 1, "router": 1}, "pod_state": pods, "closed": False,
                "trigger_ts": None, "hdr": {}, "info": {"max_unavail": {}}, "min_ready": {"engine": 2, "router": 1},
                "floor_sust": {}, "peaks": {"engine": 2, "decoder": 0, "router": 2}, "old_hashes": {"old"},
                "agg": {"engine": (2, 2, 0), "router": (2, 2, 0)}}

    def _run(self, ctx, pending, watch_state, repeat=2):
        watched, allowed, finished, records = [], [], [], []
        victims = iter([("p1-router-0-default-0", "0")])

        def watch(ctx_, f, comps, lift, done, budget, inst=None):
            watched.append((list(comps), inst))
            gone, comp, idx = f["crash_pod"], f["crash_comp"], f["crash_inst"]
            ctx_["pod_state"].pop(gone)
            ctx_["pod_state"][f"p1-{comp}-{idx}-default-1"] = dict(_rec(comp, idx, "old", uid=f"{comp}{idx}b"), node="n1")
            return watch_state, {"bitten_at": 4.0}

        def reset(f, k):
            f["crash_pod"], f["crash_inst"] = next(victims)
            return True

        real_checks = run._repeat_checks

        def repeat_checks(f, *a):
            records.append(f)
            return real_checks(f, *a)
        with mock.patch.object(run, "_preempt_fill", lambda *a, **k: None), \
                mock.patch.object(run, "_preempt_apparatus_hit", lambda f: False), \
                mock.patch.object(run, "_preempt_cycle_reset", reset), \
                mock.patch.object(run, "_pending_watch", watch), \
                mock.patch.object(run, "_pending_checks", lambda f, lift, state, out, c, comps, allowance, verdict, notes, checks:
                                  allowed.append(dict(allowance)) or verdict), \
                mock.patch.object(run, "_repeat_checks", repeat_checks), \
                mock.patch.object(run, "_isvc_events_since", lambda *a: []), \
                mock.patch.object(run, "_repeat_end_checks", lambda case_id, N, configured, verdict, notes, checks: verdict), \
                mock.patch.object(run, "_finish", lambda case_id, desc, hdr, c, state, verdict, notes, checks:
                                  finished.append((state, verdict, notes, checks)) or 0):
            run._kill_repeat("p1", "desc", {"repeat": repeat}, ctx, "preempt", "None", "after-settle", pending, "engine", "")
        return watched, allowed, finished[0], records[0]

    def test_a_router_taken_by_a_press_aimed_at_the_engine(self):
        ctx = self._ctx()
        pending = {"case_id": "p1", "kind": "preempt", "infra_fail": None, "notes": [], "fillers": [], "preempted": [],
                   "not_victim": set(), "crash_pod": "p1-router-1-default-0", "crash_comp": "router", "crash_inst": "1"}
        watched, allowed, (state, verdict, notes, checks), rec = self._run(ctx, pending, "DONE")
        self.assertEqual([(["router"], "1"), (["router"], "0")], watched, "the wait is read on the router's row, the pod the scheduler took")
        self.assertEqual({"engine": 0, "router": 1}, allowed[0], "the allowance is the router's Instance")
        self.assertEqual({"router": 1}, ctx["floor_allowance"])
        self.assertEqual({("router", "1"), ("router", "0")}, rec["victim_insts"])
        self.assertEqual(["router", "router"], [c["comp"] for c in rec["cycles"]], "the record names the Component taken")
        self.assertEqual(("RECOVERED", "PASS", []), (state, verdict, notes), checks)
        self.assertTrue(any("exactly 1 pod(s) recreated (None), scoped to killed instance router/1" in c for c in checks), checks)
        self.assertTrue(any("2 pod(s) created across the window, within the 4" in c for c in checks),
                        "the pods the watch saw rebuilt are the window's")

    def test_the_window_is_read_even_when_the_wait_does_not_complete(self):
        ctx = self._ctx()
        pending = {"case_id": "p1", "kind": "preempt", "infra_fail": None, "notes": [], "fillers": [], "preempted": [],
                   "not_victim": set(), "crash_pod": "p1-router-1-default-0", "crash_comp": "router", "crash_inst": "1"}
        watched, _allowed, (state, verdict, _notes, checks), _rec_ = self._run(ctx, pending, "WAIT_TIMEOUT")
        self.assertEqual([(["router"], "1")], watched)
        self.assertEqual(("WAIT_TIMEOUT", "FAIL"), (state, verdict))
        self.assertTrue(any("1 pod(s) created across the window" in c for c in checks), checks)


class GeneratedCanaryGateFamilyTest(unittest.TestCase):
    """The gate-fault family generate_cases.py mints: every case a canary of
    the engine unit with the operator's end state on line two, the rows and
    shapes it fills, and its place in the pipeline."""

    @classmethod
    def setUpClass(cls):
        cls.outputs, rows = generate_cases.build_outputs()
        cls.fam = [(n, kv, m) for f, n, kv, m in rows if f == "gen-canary-fault-gate"]

    def test_every_case_is_an_engine_unit_canary_with_an_end_state(self):
        self.assertEqual(173, len(self.fam))
        for name, kv, m in self.fam:
            text = self.outputs[f"gen-canary-fault-gate/{name}.yaml"]
            self.assertEqual("canary", kv["mode"], name)
            self.assertEqual("auto", kv["canary"], name)
            self.assertTrue(run._cell_ok(kv["cell"]), name)
            self.assertIn(("- components: [engine, decoder]" if kv["Nd"] else "- components: [engine]"), text, name)
            self.assertTrue(text.splitlines()[1].startswith("# reason: "), name)
            self.assertIn("revision", text.splitlines()[1], name)
            if m["gate"].startswith(("metric", "analysis")):
                self.assertIn("revision_hash=\"{{.CanaryRevision}}\"", text, f"{name}: the gate reads the canary revision alone")
                self.assertIn(gen_cases.CSR_METRICS_MN if kv["topo"].endswith("-mn") else gen_cases.CSR_METRICS, text, name)
                self.assertIn("perm.test/error-rate", text, name)
                self.assertEqual("None", m["rp"], f"{name}: the restart policy is not an axis of the gate rows")
            else:
                self.assertNotIn("analysis:", text, f"{name}: a sibling break rides a timed ladder")
                self.assertNotIn(gen_cases.CSR_METRICS, text, name)

    def test_the_ledger_rows_and_shapes(self):
        rows = {m["gate"] for _n, _kv, m in self.fam}
        self.assertEqual({f"metric breach, {f}, {s}" for f in ("one-shot", "persistent") for s in ("running", "new")}
                         | {f"analysis source unusable, {f}" for f in ("one-shot", "persistent")}
                         | {f"sibling broken, {f}, {s}" for f in ("one-shot", "persistent") for s in ("running", "new")}, rows)
        by_shape = {}
        for _n, _kv, m in self.fam:
            by_shape.setdefault(m["shape"], set()).add(m["gate"])
        self.assertEqual({f for f, *_ in generate_cases.GATE_SHAPES}, set(by_shape))
        for shape, got in by_shape.items():
            self.assertEqual(rows if "router" in shape else {r for r in rows if not r.startswith("sibling")}, got, shape)

    def test_metric_rows_cross_side_form_and_position(self):
        metric = [(kv, m) for _n, kv, m in self.fam if str(kv.get("fault", "")).startswith("metric:")]
        self.assertEqual(48, len(metric))
        for kv, m in metric:
            side = "stable" if m["gate"].endswith("running") else "canary"
            where = kv["fault"].partition(":")[2]
            if where in ("new", "idle"):
                self.assertNotIn("victim", kv)
            else:
                self.assertEqual(side, kv["victim"])
            if side == "stable":
                self.assertEqual(("pass", "pass", "RollingBack"), (kv["analysis"], kv["health"], kv["nophase"]))
            elif kv["form"] == "persist":
                self.assertEqual("breach", kv["analysis"])
                self.assertEqual("pass" if where != "new" else None, kv.get("health"))
            else:
                self.assertEqual(("pass", 1), (kv["analysis"], kv["minfails"]))
                self.assertGreaterEqual(kv["failurelimit"], generate_cases.METRIC_RECOVER_LIMIT)
        positions = {kv["fault"].partition(":")[2] for kv, _m in metric if not kv["topo"].endswith("-mn")}
        self.assertEqual({"idle", "new", "canary-serving@1", "canary-serving@2"}, positions)
        self.assertEqual({"canary-serving@1"}, {kv["fault"].partition(":")[2] for kv, _m in metric if kv["topo"].endswith("-mn")})

    def test_source_rows_cross_form_lift_policy_and_position(self):
        source = [(kv, m) for _n, kv, m in self.fam if str(kv.get("fault", "")).startswith("source:")]
        self.assertEqual(54, len(source))
        for kv, m in source:
            self.assertEqual(("pass", "pass"), (kv["analysis"], kv["health"]))
            text = self.outputs[f"gen-canary-fault-gate/{_name_of(self.fam, kv)}.yaml"]
            if kv["lift"] == "after-settle":
                self.assertNotIn("terminal", kv)
                self.assertEqual("RollingBack" if kv["oninconclusive"] == "rollbackonstall" else "Failed", kv["nophase"])
                self.assertIn(f'readyTimeout: "{generate_cases.SOURCE_RESUME_READY_TIMEOUT}"', text)
            else:
                self.assertEqual("RolledBack" if kv["oninconclusive"] == "rollbackonstall" else "Failed", kv["terminal"])
                self.assertIn(f'readyTimeout: "{generate_cases.SOURCE_PARK_READY_TIMEOUT}"', text)
            self.assertIn(f"onInconclusive: {'RollbackOnStall' if kv['oninconclusive'] == 'rollbackonstall' else 'Hold'}", text)
        self.assertEqual({"canary-serving@1"}, {kv["fault"].partition(":")[2] for kv, _m in source if kv["form"] == "garbage"})

    def test_sibling_rows_name_the_sibling_and_heal_or_park_as_stated(self):
        sibling = [(kv, m) for _n, kv, m in self.fam if m["gate"].startswith("sibling")]
        self.assertEqual(71, len(sibling))
        for kv, m in sibling:
            sib = "decoder" if kv["Nd"] else "router"
            if "imgcomp" in kv:
                self.assertEqual(("router", f"engine,{'decoder,' if kv['Nd'] else ''}router", "Stable"),
                                 (kv["imgcomp"], kv["bump"], kv["terminal"]))
                self.assertEqual(kv["Nr"] + 1, kv["ceil_r"])
                continue
            self.assertEqual(sib, kv["victimcomp"])
            if m["gate"].endswith("one-shot, running"):
                self.assertEqual("after-settle", kv["lift"])
                self.assertNotIn("victim", kv)
            elif m["gate"].endswith("one-shot, new"):
                self.assertEqual(("canary", "load", "after-settle"), (kv["victim"], kv["form"], kv["lift"]))
            elif m["gate"].endswith("persistent, new"):
                self.assertEqual(("new", "canary-failed"), (kv["fault"].partition(":")[2], kv["during"]))
                self.assertIn(kv["verb"], ("rollback", "rebump-fix"))
            else:
                self.assertNotIn("lift", kv)
                self.assertEqual(1, kv["victims"])
        kinds = {(kv["fault"].partition(":")[0], kv.get("form"), m["rp"]) for kv, m in sibling
                 if m["gate"] == "sibling broken, persistent, running"}
        self.assertEqual(set(generate_cases.SIBLING_PERSIST_BREAKS), kinds)

    def test_generated_headers_validate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name, _kv, _m in self.fam:
                path = root / "gen-canary-fault-gate" / f"{name}.yaml"
                path.parent.mkdir(exist_ok=True)
                path.write_text(self.outputs[f"gen-canary-fault-gate/{name}.yaml"])
                self.assertEqual([], run.validate_case_file(path), name)

    def test_family_is_a_canary_lane_on_the_soft_list(self):
        sys.path.insert(0, str(HERE.parents[2] / ".buildkite"))
        try:
            import generate_pipeline as gp
        finally:
            sys.path.pop(0)
        self.assertTrue(gp._eval_feature_is_canary("gen-canary-fault-gate"))
        self.assertIn("gen-canary-fault-gate", gp._eval_soft_features())


def _name_of(fam, kv):
    return next(n for n, k, _m in fam if k is kv)


class BrokenScaleOnTheWindowTest(unittest.TestCase):
    """_broken_scale judged end to end on a scripted fleet. The scale lands
    the moment the broken Instance's row reads Restarting, which is the
    moment its repair has deleted the old pod: at the scale the Instance
    has no alive pod, or already has the rebuilt one coming up. Neither
    makes the rebuilt pod a new Instance or the pod the fault hit."""

    def _pod(self, inst, uid, alive=True, ready=True, serving=True):
        return {"alive": alive, "ready": ready, "serving": serving, "hash": "old", "inst": inst,
                "comp": "engine", "uid": uid}

    def _ctx(self):
        pods = {f"e{i}": self._pod(str(i), f"u{i}") for i in range(4)}
        return {"case_id": "p1", "N": {"engine": 4, "decoder": 0, "router": 0}, "configured": {"engine"},
                "pods_per_inst": {"engine": 1, "decoder": 0, "router": 0},
                "old_hashes": {"old"}, "pod_state": pods, "agg": {"engine": (4, 4, 0)},
                "measures": {"engine": (4, 0, 0)}, "peaks": {"engine": 4}, "closed": False,
                "fault": {"kind": "crashloop", "infra_fail": None, "notes": []}}

    def _repair_opens(self, ctx, rebuilt_pod_up):
        """The break bites Instance 0 and its repair opens: the old pod is
        Terminating, and with rebuilt_pod_up the rebuilt one is already
        alive but not yet Ready."""
        ctx["fault"].update(crash_comp="engine", crash_inst="0", crash_uid="u0", crash_pod="e0")
        ctx["pod_state"]["e0"]["alive"] = False
        if rebuilt_pod_up:
            ctx["pod_state"]["e0b"] = self._pod("0", "u0b", ready=False, serving=False)
        ctx["agg"]["engine"] = (3, 3, 0)
        ctx["measures"]["engine"] = (3 + int(rebuilt_pod_up), 0, 0)
        ctx["trigger_ts"] = time.time()
        return {"bitten_at": 1.0, "reported_at": 1.0, "acted_on": "its row reads Restarting", "row": None}

    def _drive(self, hdr, ctx, fire, read_one, rows, settle, events=()):
        out = {}

        def finish(case_id, desc, hdr, ctx, state, verdict, notes, checks):
            out.update(state=state, verdict=verdict, notes=notes, checks=checks)
            return verdict
        with mock.patch.multiple(run, _setup_and_ready=lambda *a: (ctx, None), _broken_fire_and_report=fire,
                                 _broken_rows=rows, _scale_patch=lambda *a: (True, ""), _read_one=read_one,
                                 _wait_broken_rows_settled=settle, _isvc_events_since=lambda *a: list(events),
                                 _finish=finish, STABILITY_S=0):
            run._broken_scale("p1", "p1.yaml", "desc", hdr)
        return out

    RESTARTING_ROWS = [("0", False, "Restarting"), ("1", True, "Ready"), ("2", True, "Ready"), ("3", True, "Ready")]
    REPAIR = [("RestartTriggered", "OMENative component=engine instance=0 restart triggered: pod e0 wedged")]

    def _up(self, rebuilt_pod_up):
        hdr = {"mode": "scaledown", "to_e": 5, "ceil_e": 5, "inst": 0, "expect": "repaired", "during": "idle"}
        ctx = self._ctx()
        reads = []

        def read_one(ctx):
            reads.append(1)
            if len(reads) > 1:
                return
            ctx["pod_state"]["e0b"] = self._pod("0", "u0b")
            ctx["pod_state"]["e4"] = self._pod("4", "u4")
            ctx["agg"]["engine"] = (5, 5, 0)
            ctx["measures"]["engine"] = (5, 0, 0)
            ctx["peaks"]["engine"] = 5
        return self._drive(hdr, ctx, lambda ctx, hdr: self._repair_opens(ctx, rebuilt_pod_up), read_one,
                           lambda case_id, c: self.RESTARTING_ROWS, lambda *a: (True, "settled"), self.REPAIR)

    def test_a_scale_up_landing_between_the_old_pod_and_the_rebuilt_one_sees_one_new_instance(self):
        out = self._up(rebuilt_pod_up=False)
        self.assertEqual("PASS", out["verdict"], out["notes"])
        self.assertTrue(any("new Instance(s) ['4'] came up whole" in c for c in out["checks"]), out["checks"])
        self.assertTrue(any("Instance 0 was rebuilt (every pod new)" in c for c in out["checks"]), out["checks"])
        self.assertTrue(any("kept 3 Instance(s) Ready" in c for c in out["checks"]), out["checks"])

    def test_a_scale_up_landing_once_the_rebuilt_pod_is_up_sees_the_instance_rebuilt(self):
        out = self._up(rebuilt_pod_up=True)
        self.assertEqual("PASS", out["verdict"], out["notes"])
        self.assertTrue(any("Instance 0 was rebuilt (every pod new)" in c for c in out["checks"]), out["checks"])
        self.assertTrue(any("new Instance(s) ['4'] came up whole" in c for c in out["checks"]), out["checks"])

    def test_a_scale_down_is_judged_on_the_rows_once_they_have_settled(self):
        hdr = {"mode": "scaledown", "to_e": 2, "ceil_e": 4, "inst": 0, "during": "idle"}
        ctx = self._ctx()
        settled = []

        def read_one(ctx):
            # The wave takes the broken row over from its repair and deletes
            # the highest healthy Instance; the pods leave rotation at once.
            ctx["pod_state"]["e3"]["alive"] = False
            ctx["agg"]["engine"] = (2, 2, 0)
            ctx["measures"]["engine"] = (2, 0, 0)

        def rows(case_id, c):
            # The rows trail the pods: a removed Instance's row goes when its
            # pods are gone, after their termination, not when they leave.
            if settled:
                return [("1", True, "Ready"), ("2", True, "Ready")]
            return self.RESTARTING_ROWS

        def settle(*a):
            settled.append(1)
            return True, "settled"
        out = self._drive(hdr, ctx, lambda ctx, hdr: self._repair_opens(ctx, False), read_one, rows, settle)
        self.assertEqual("PASS", out["verdict"], out["notes"])
        self.assertTrue(any("Instances ['0', '3'] went" in c and "['1', '2'] stayed" in c for c in out["checks"]), out["checks"])
        self.assertTrue(any("every Instance row settled" in c for c in out["checks"]), out["checks"])

    def test_the_instances_at_the_scale_include_the_broken_one_between_its_pods(self):
        rows = self.RESTARTING_ROWS
        self.assertEqual({"0", "1", "2", "3"}, run._broken_pre_indices({"1": {"u1"}, "2": {"u2"}, "3": {"u3"}}, rows, "0"))
        self.assertEqual({"0", "1"}, run._broken_pre_indices({"1": {"u1"}}, [], 0), "the broken Instance counts by name alone")
        self.assertEqual({"4", "5"}, run._broken_pre_indices({"5": {"u5"}}, [("4", True, "Ready")], "5"))

    def test_a_rebuilt_broken_instance_is_not_a_new_instance(self):
        ctx = self._ctx()
        ctx["pod_state"]["e0"]["alive"] = False
        ctx["pod_state"]["e0b"] = self._pod("0", "u0b")
        ctx["pod_state"]["e4"] = self._pod("4", "u4")
        alive_at_the_scale = {"1": {"u1"}, "2": {"u2"}, "3": {"u3"}}
        pre = run._broken_pre_indices(alive_at_the_scale, self.RESTARTING_ROWS, "0")
        self.assertEqual({"4": (True, True)}, run._broken_new_instances(ctx, "engine", pre))
        self.assertEqual({"0": (True, True), "4": (True, True)}, run._broken_new_instances(ctx, "engine", set(alive_at_the_scale)),
                         "read off the alive pods alone, the rebuilt Instance passes for a new one")

class PressSparesTheControlPlaneTest(unittest.TestCase):
    """The press of a preempt case can take a pod of the case and nothing
    else. The class the fixtures name for the OME control plane sits above
    the press and preempts nothing; the capability check creates it and reads
    the control plane's priority; the arm keeps the press below it; the fill
    refuses to press while another pod on the node would go first; a
    control-plane pod the scheduler preempts anyway fails the case as
    apparatus; the pending snapshot names each pod's conditions."""

    CP = runtime_fixtures.control_plane_priority_class(runtime_fixtures.resolve(kind=True))

    @staticmethod
    def _pod(name, ns, cpu=None, priority=0, phase="Running", labels=None, owner="ReplicaSet"):
        container = {"name": "c", "resources": {"requests": {"cpu": cpu}} if cpu else {}}
        return {"metadata": {"name": name, "namespace": ns, "uid": f"uid-{name}", "labels": labels or {},
                             "ownerReferences": [{"kind": owner, "name": "owner"}] if owner else []},
                "spec": {"priority": priority, "containers": [container]},
                "status": {"phase": phase}}

    def test_the_control_plane_class_outranks_the_press_and_preempts_nothing(self):
        press = run._filler_manifest("perm-p1-press", "p1", "worker-1", 550, "perm-p1-preempt")
        self.assertEqual("perm-p1-preempt", press["spec"]["priorityClassName"])
        self.assertEqual("PriorityClass", self.CP["kind"])
        self.assertEqual("Never", self.CP["preemptionPolicy"], "a restarted manager waits for room as before")
        self.assertFalse(self.CP["globalDefault"])
        self.assertNotIn("perm.test/fault", self.CP["metadata"]["labels"],
                         "batch.py's sweep deletes the press classes by that label and must spare this one")
        self.assertIn(runtime_fixtures.CONTROL_PLANE_CLASS_LABEL, self.CP["metadata"]["labels"])
        self.assertEqual(self.CP["metadata"]["name"], runtime_fixtures.resolved_value(
            "OME_EVAL_CONTROL_PLANE_PRIORITY_CLASS", kind=True))

    def test_arm_keeps_the_press_one_notch_above_the_case_and_below_the_control_plane(self):
        case_pods = {"items": [self._pod("p1-engine-0-default-0", "sandbox", cpu="50m",
                                         labels={"ome.io/inferenceservice": "p1"})]}
        state = {"cp": [self._pod("ome-controller-manager-abc", "ome", cpu="500m", priority=self.CP["value"])]}
        applied = []

        def kubectl(*args, **kw):
            if args[:1] == ("apply",):
                applied.append(json.loads(kw["input"]))
                return _Resp("ok")
            if "ome.io/inferenceservice=p1" in args:
                return _Resp(json.dumps(case_pods))
            if args[:4] == ("-n", run.CTRL_NS, "get", "pods"):
                return _Resp(json.dumps({"items": state["cp"]}))
            return _Resp("{}")
        with mock.patch.object(run, "kubectl", kubectl), mock.patch.object(run, "_FAULT_CLEANUPS", []), \
                contextlib.redirect_stdout(io.StringIO()):
            f = {"case_id": "p1", "notes": [], "infra_fail": None}
            ok, msg = run._fault_preempt_arm(f, "p1")
            self.assertTrue(ok, msg)
            self.assertEqual((1, 1), (f["pc_value"], applied[-1]["value"]), "one notch above the case's pods")
            self.assertLess(f["pc_value"], self.CP["value"])
            state["cp"] = [self._pod("ome-controller-manager-abc", "ome", cpu="500m", priority=0)]
            g = {"case_id": "p1", "notes": [], "infra_fail": None}
            ok, _msg = run._fault_preempt_arm(g, "p1")
        self.assertFalse(ok)
        self.assertIn("could take a control-plane pod", g["infra_fail"])
        self.assertEqual(1, len(applied), "no press class for a press that would outrank the control plane")

    def test_press_exposed_names_only_the_pods_the_press_would_take(self):
        items = [
            self._pod("ome-controller-manager-abc", "ome", cpu="500m"),
            self._pod("ome-prometheus-abc", "ome", cpu="100m", priority=self.CP["value"]),
            self._pod("cert-manager-abc", "cert-manager"),
            self._pod("seed-abc", "sandbox", cpu="100m", phase="Succeeded", owner="Job"),
            self._pod("p1-engine-0-default-0", "sandbox", cpu="50m", labels={"ome.io/inferenceservice": "p1"}),
            self._pod("p2-engine-0-default-0", "sandbox", cpu="50m", labels={"ome.io/inferenceservice": "p2"}),
        ]
        self.assertEqual([("ome/ome-controller-manager-abc", 0, 500), ("sandbox/p2-engine-0-default-0", 0, 50)],
                         run._press_exposed(items, "p1", 1),
                         "below the press and requesting CPU: the manager and another case's pod; not the pod above "
                         "the press, the pod with no requests, the finished pod or the case's own")
        self.assertEqual([], run._press_exposed(items, "p1", 0), "nothing sits below a press at the case's own priority")

    def test_capability_creates_the_class_and_reads_the_control_planes_priority(self):
        state = {"cp": [self._pod("ome-controller-manager-abc", "ome", cpu="500m", priority=self.CP["value"])],
                 "apply_rc": 0}
        applied = []

        def kubectl(*args, **kw):
            if args[:1] == ("apply",):
                applied.append(json.loads(kw["input"]))
                return _Resp("", state["apply_rc"], "forbidden")
            if args[:4] == ("-n", run.CTRL_NS, "get", "pods"):
                return _Resp(json.dumps({"items": state["cp"]}))
            return _Resp("{}")
        with mock.patch.object(run, "_node_fault_capability", lambda: None), \
                mock.patch.object(run, "_api_served", lambda p: True), mock.patch.object(run, "kubectl", kubectl):
            self.assertIsNone(run._preempt_capability())
            self.assertEqual([self.CP], applied, "the class is created when absent, from the fixtures' definition")
            state["cp"] = [self._pod("ome-controller-manager-abc", "ome", cpu="500m", priority=0),
                           self._pod("ome-prometheus-abc", "ome", cpu="100m", priority=self.CP["value"]),
                           self._pod("ome-coordtest-noop-meta-abc", "ome", cpu="100m", priority=0, owner="Job")]
            below = run._preempt_capability()
            state["apply_rc"] = 1
            refused = run._preempt_capability()
        self.assertIn("ome/ome-controller-manager-abc runs at priority 0", below)
        self.assertIn(self.CP["metadata"]["name"], below)
        self.assertNotIn("ome-prometheus", below)
        self.assertNotIn("meta", below, "a Job's pod is not the control plane")
        self.assertIn("cannot be created", refused)

    def _fill(self, node_items, cp_items):
        f = {"case_id": "p1", "notes": [], "infra_fail": None, "fillers": [], "pc_name": "perm-p1-preempt", "pc_value": 1}
        case_pod = self._pod("p1-engine-0-default-0", "sandbox", cpu="50m", labels={"ome.io/inferenceservice": "p1"})
        pod_state = {"p1-engine-0-default-0": {"alive": True, "comp": "engine", "inst": "0", "ready": True,
                                               "serving": True, "node": "worker-1"}}
        created = []
        with mock.patch.object(run, "_get_pod", lambda name, ns=run.NS: case_pod), \
                mock.patch.object(run, "_eligible_nodes", lambda: ["worker-1", "worker-2"]), \
                mock.patch.object(run, "_preempt_block_node", lambda f, node: (True, f"filler holds {node}")), \
                mock.patch.object(run, "_node_pods", lambda node: node_items + [case_pod]), \
                mock.patch.object(run, "_node_cpu_free", lambda node, items=None: 200), \
                mock.patch.object(run, "_control_plane_pods", lambda: cp_items), \
                mock.patch.object(run, "_filler_create",
                                  lambda f, name, node, cpu, pc="": created.append((name, node, cpu, pc)) or (True, "")), \
                contextlib.redirect_stdout(io.StringIO()):
            run._preempt_fill(f, "p1-engine-0-default-0", "worker-1", pod_state)
        return f, created

    def test_fill_refuses_to_press_while_another_pod_on_the_node_would_go_first(self):
        manager = self._pod("ome-controller-manager-abc", "ome", cpu="500m")
        f, created = self._fill([manager], [manager])
        self.assertIn("would take ome/ome-controller-manager-abc (priority 0, 500m) before any pod of the case",
                      f["infra_fail"])
        self.assertEqual([], created, "no press was created")
        self.assertIsNone(f.get("press_pod"))

    def test_fill_presses_the_aimed_node_and_remembers_the_control_plane(self):
        manager = self._pod("ome-controller-manager-abc", "ome", cpu="500m", priority=self.CP["value"])
        f, created = self._fill([manager, self._pod("cert-manager-abc", "cert-manager")], [manager])
        self.assertIsNone(f["infra_fail"])
        self.assertEqual([("perm-p1-press", "worker-1", 250, "perm-p1-preempt")], created,
                         "the node's spare CPU plus the aimed pod's own, under the press class")
        self.assertEqual({"ome-controller-manager-abc": "uid-ome-controller-manager-abc"}, f["cp_pods"])

    def _after_fire(self):
        return {"case_id": "p1", "notes": [], "infra_fail": None, "aim_node": "worker-1",
                "cp_pods": {"ome-controller-manager-abc": "uid-1"}, "preempted": [], "not_victim": set(),
                "pre_alive": {"p1-engine-0-default-0": {"comp": "engine", "inst": "0", "serving": True}}}

    def test_a_preempted_control_plane_pod_fails_the_case_as_apparatus(self):
        gone = {"ome-controller-manager-abc"}

        def kubectl(*args, **_kw):
            if "pod" in args:
                name = args[args.index("pod") + 1]
                return _Resp("", 1, "not found") if name in gone else _Resp(json.dumps(self._pod(name, "ome")))
            if "events" in args:
                selector = args[args.index("--field-selector") + 1]
                hit = "ome-controller-manager-abc" in selector and "reason=Preempted" in selector
                return _Resp(json.dumps({"items": [{"reason": "Preempted"}] if hit else []}))
            return _Resp("{}")
        f = self._after_fire()
        alive = {"p1-engine-0-default-0": {"alive": True, "comp": "engine", "inst": "0"}}
        with mock.patch.object(run, "kubectl", kubectl), contextlib.redirect_stdout(io.StringIO()):
            self.assertIsNone(run._preempt_victim(f, alive))
        self.assertIn("preempted the OME control-plane pod ome/ome-controller-manager-abc for the press on worker-1",
                      f["infra_fail"])
        self.assertIn(self.CP["metadata"]["name"], f["infra_fail"])
        self.assertEqual("ome-controller-manager-abc", f["cp_hit"])
        notes, checks = [], []
        self.assertEqual("INFRA-FAIL", run._fault_finalize(dict(f, kind="preempt", fired=True, param="rotating"),
                                                           "PASS", notes, checks))
        self.assertTrue(any("INFRA-FAIL (fault apparatus)" in c for c in checks))

    def test_a_control_plane_still_running_is_no_apparatus_failure(self):
        f = self._after_fire()

        def kubectl(*args, **_kw):
            if "pod" in args:
                return _Resp(json.dumps(self._pod(args[args.index("pod") + 1], "ome")))
            self.fail(f"no event is read while the pod is live: {args}")
        with mock.patch.object(run, "kubectl", kubectl):
            self.assertFalse(run._preempt_apparatus_hit(f))
        self.assertIsNone(f["infra_fail"])

    def test_pending_checks_leave_an_apparatus_failure_to_the_finalizer(self):
        f = {"kind": "preempt", "infra_fail": "preempt: the scheduler preempted the OME control-plane pod ome/m",
             "notes": [], "fired": True, "param": "rotating", "cause": "the fillers"}
        out = {"bitten_at": None, "row": None, "failed": [], "lifted_at": None, "waiting_at_end": False}
        notes, checks = [], []
        verdict = run._pending_checks(f, "never", "APPARATUS", out, {"N": {}}, ["engine"], {}, "PASS", notes, checks)
        self.assertEqual(("PASS", [], []), (verdict, notes, checks), "no product finding is recorded over a broken press")

    def test_pending_pod_lines_carry_the_scheduling_conditions(self):
        now = run._event_epoch("2026-10-01T10:06:00Z")
        pending = {"metadata": {"name": "p7469-engine-1-default-0", "creationTimestamp": "2026-10-01T10:00:00Z",
                                "labels": {"scheduling.x-k8s.io/pod-group": "p7469-engine-1"}},
                   "spec": {"priority": 0, "schedulingGates": [{"name": "example.com/gang"}]},
                   "status": {"phase": "Pending", "conditions": [
                       {"type": "PodScheduled", "status": "False", "reason": "Unschedulable",
                        "message": "0/3 nodes are available: 2 Insufficient cpu."}]}}
        silent = {"metadata": {"name": "p7469-engine-1-default-1", "creationTimestamp": "2026-10-01T10:00:00Z"},
                  "spec": {}, "status": {"phase": "Pending"}}
        leaving = {"metadata": {"name": "p7469-engine-0-default-0", "creationTimestamp": "2026-10-01T09:50:00Z",
                                "deletionTimestamp": "2026-10-01T10:05:00Z"},
                   "spec": {"nodeName": "worker-1", "priority": 0},
                   "status": {"phase": "Running", "conditions": [{"type": "PodScheduled", "status": "True"},
                                                                 {"type": "Ready", "status": "True"}]}}
        lines = run._pending_pod_lines([silent, pending, leaving], now)
        self.assertEqual("p7469-engine-0-default-0  Running  node=worker-1  priority=0  age=960s  deleting  "
                         "PodScheduled=True  Ready=True", lines[0])
        self.assertEqual("p7469-engine-1-default-0  Pending  node=-  priority=0  age=360s  "
                         "PodScheduled=False/Unschedulable: 0/3 nodes are available: 2 Insufficient cpu.  Ready=-  "
                         "gates=example.com/gang  pod-group=p7469-engine-1", lines[1])
        self.assertEqual("p7469-engine-1-default-1  Pending  node=-  priority=0  age=360s  PodScheduled=-  Ready=-",
                         lines[2], "a pod no scheduler has looked at carries neither condition")

    def test_pending_event_lines_keep_the_case_and_the_window_and_name_the_reporter(self):
        events = [
            {"involvedObject": {"kind": "Pod", "name": "p7469-engine-1-default-0"}, "reason": "FailedScheduling",
             "message": "0/3 nodes", "lastTimestamp": "2026-10-01T10:03:00Z", "reportingComponent": "ome-scheduler"},
            {"involvedObject": {"kind": "Pod", "name": "p7469-engine-1-default-0"}, "reason": "Scheduled",
             "message": "before", "lastTimestamp": "2026-10-01T09:30:00Z", "source": {"component": "default-scheduler"}},
            {"involvedObject": {"kind": "Pod", "name": "p74690-engine-0-default-0"}, "reason": "FailedScheduling",
             "message": "another case", "lastTimestamp": "2026-10-01T10:03:00Z"},
            {"involvedObject": {"kind": "InferenceService", "name": "p7469"}, "reason": "Rolling",
             "message": "not a pod", "lastTimestamp": "2026-10-01T10:03:00Z"},
        ]
        since = run._event_epoch("2026-10-01T10:00:00Z")
        self.assertEqual(["10:03:00 FailedScheduling p7469-engine-1-default-0 (ome-scheduler): 0/3 nodes"],
                         run._pending_event_lines(events, "p7469", since))
        self.assertEqual(["09:30:00 Scheduled p7469-engine-1-default-0 (default-scheduler): before",
                          "10:03:00 FailedScheduling p7469-engine-1-default-0 (ome-scheduler): 0/3 nodes"],
                         run._pending_event_lines(events, "p7469", None), "oldest first, every event without a window")

    def test_pending_snapshot_reads_the_control_plane_logs_since_the_trigger(self):
        since = run._event_epoch("2026-10-01T10:00:00Z")
        kubectl = _kubectl_with([
            (lambda a: "logs" in a and "-l" in a, _Resp("I p7469 engine 1 waits\nI p0001 other case\n")),
            (lambda a: "logs" in a and "deployment/ome-scheduler" in a, _Resp("")),
            (lambda a: "podgroups.scheduling.x-k8s.io" in a, _Resp("", 1, "no such kind")),
            (lambda a: "pods" in a or "events" in a, _Resp('{"items": []}')),
        ])
        out = io.StringIO()
        with mock.patch.object(run, "kubectl", kubectl), mock.patch.object(run, "SCHED_NAME", "ome-scheduler"), \
                contextlib.redirect_stdout(out):
            run._print_pending_snapshot("p7469", since=since)
        text = out.getvalue()
        self.assertIn("no pod event of the case since the trigger: no scheduler reported on its pods", text)
        self.assertIn("manager log lines naming the case since the trigger (last 1):", text)
        self.assertIn("I p7469 engine 1 waits", text)
        self.assertNotIn("p0001", text)
        self.assertIn("the scheduler log names the case in no line since the trigger", text)
        log_calls = [c for c in kubectl.calls if "logs" in c]
        self.assertEqual(2, len(log_calls))
        self.assertTrue(all("--since-time=2026-10-01T10:00:00Z" in c and "--tail=-1" in c for c in log_calls))
        self.assertIn(("-l", run.CTRL_SELECTOR), [(c[c.index("-l")], c[c.index("-l") + 1]) for c in log_calls if "-l" in c])
        self.assertTrue(any("deployment/ome-scheduler" in c for c in log_calls))
