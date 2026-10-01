#!/usr/bin/env python3
import contextlib
import copy
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


def pod_object(name, component, index, revision, image, anno, *, runner="default",
               test_rev="v1", cpu="50m", ready=True, serving=True):
    """A pod as kubectl -w hands it to the watcher: the labels, annotation,
    container spec and conditions the record builders and invariants read."""
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
        "status": {"containerStatuses": [{"name": run.MAIN_CONTAINER, "ready": ready}],
                   "conditions": [{"type": run.SERVING_COND, "status": "True" if serving else "False"}]},
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

    def _verdict(self, pod_state, events, surges=None):
        f = {"kind": "crashloop", "fired": True, "infra_fail": None, "form": "start", "case_id": "p999",
             "crash_inst": "1", "crash_comp": "engine", "crash_hash": "old", "kills": 0}
        orig = run._isvc_events_since
        try:
            run._isvc_events_since = lambda case_id, since: events
            notes, checks = [], []
            return (run._crashloop_checks(f, pod_state, {"old"}, 1.0, "PASS", notes, checks, surges=surges),
                    notes, checks)
        finally:
            run._isvc_events_since = orig

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

    def _grouped_or_live(self, kv, text):
        """Every cell is parked on the dark-repair issue in its header, with
        the park named in its reason: each victim carries maxUnavailable 0,
        under which the repair of a dark Instance is denied. A grouped cell
        also carries its group block; a no-group cell does not."""
        self.assertEqual((1, generate_cases.GROUPED_REPAIR_ISSUE), (kv["notimpl"], kv["issue"]))
        if kv["co"] == "none":
            self.assertNotIn("  rollout:\n    groups:", text)
        else:
            self.assertIn("  rollout:\n    groups:", text)
            self.assertIn("parked:", text.splitlines()[1])
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
        for; every cell expects the repair, nothing surges at idle."""
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
            self._grouped_or_live(kv, outputs[f"gen-crashloop-idle/{n}.yaml"])
        parked = [n for n, kv, m in fam if m.get("parked")]
        self.assertEqual(len(fam), len(parked))

    def test_generated_sibling_family_crosses_every_axis(self):
        """The sibling family: the decoder breaks as an engine-only bump lands,
        on both PD topologies under every group kind, under every (crash
        form, restart policy) pair a repair is promised for; the engine
        surges within its own budget and the decoder is a negative control."""
        outputs, rows = generate_cases.build_outputs()
        fam = [(n, kv, m) for folder, n, kv, m in rows if folder == "gen-crashloop-sibling"]
        self.assertEqual(30, len(fam))
        self.assertEqual(set(generate_cases.BREAK_GROUPS), {kv["co"] for _n, kv, _m in fam})
        self.assertEqual({"pd", "pd-mn"}, {kv["topo"] for _n, kv, _m in fam})
        self.assertEqual({("start", "None"), ("start", "RecreateInstance"), ("load", "RecreateInstance")},
                         {(kv["form"], m["rp"]) for _n, kv, m in fam})
        for n, kv, m in fam:
            self.assertEqual(("crashloop:old", "decoder", "engine"), (kv["fault"], kv["victimcomp"], kv["bump"]))
            self.assertEqual("decoder", m["victim"])
            self.assertEqual((kv["Ne"] + 1, kv["Nd"], kv["Nr"]), (kv["ceil_e"], kv["ceil_d"], kv["ceil_r"]))
            self.assertNotIn("during", kv)
            self.assertNotIn("expect", kv)
            self.assertNotIn("floor_soft", kv, "the break is on the decoder; the engine keeps its floor")
            self._grouped_or_live(kv, outputs[f"gen-crashloop-sibling/{n}.yaml"])
        self.assertEqual(len(fam), len([n for n, kv, m in fam if m.get("parked")]))

    def test_family_park_rides_the_cell_park_path(self):
        """A family park emits the same header keys as a PARKED cell and never
        touches a cell the emitter did not park; a PARKED entry still wins on
        a cell it names."""
        cell = ("crashloop-idle", "start", "engine", 0, 4, "none", "engine", "None")
        with mock.patch.dict(generate_cases.PARKED, {cell: (999999, "parked on a tracked bug (fixture)", "A")}):
            outputs, rows = generate_cases.build_outputs()
        hit = [(f, n, kv) for f, n, kv, m in rows if m.get("parked") == 999999]
        self.assertEqual(1, len(hit))
        folder, name, kv = hit[0]
        self.assertEqual(("gen-crashloop-idle", "none"), (folder, kv["co"]))
        self.assertEqual((1, 999999, "A"), (kv["notimpl"], kv["issue"], kv["inv_xfail"]))
        self.assertEqual("# reason: parked on a tracked bug (fixture)", outputs[f"{folder}/{name}.yaml"].splitlines()[1])
        grouped = [kv for f, _n, kv, m in rows if f == "gen-crashloop-idle" and kv["co"] != "none"]
        self.assertTrue(grouped and all(kv["issue"] == generate_cases.GROUPED_REPAIR_ISSUE
                                        and "inv_xfail" not in kv for kv in grouped))


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

    def test_expectation_derives_from_group_membership(self):
        f = run._fault_new("p999", {"fault": "crashloop:old", "form": "start", "during": "idle", "victimcomp": "decoder"})
        self.assertEqual("held", run._idle_break_expect(f, {"members": ["engine", "decoder"]}, {}))
        self.assertEqual("repaired", run._idle_break_expect(f, {"members": ["engine"]}, {}))
        self.assertEqual("repaired", run._idle_break_expect(f, {"members": ["engine", "decoder"]}, {"expect": "repaired"}))
        engine = run._fault_new("p999", {"fault": "crashloop:old", "form": "start", "during": "idle"})
        self.assertEqual("held", run._idle_break_expect(engine, {"members": ["engine"]}, {}))

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
    ConfigMap and the victim's env reference, exactly the after-failed half of
    the heal family is parked, and nothing in the imagepull family is."""

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

    def test_heal_parks_exactly_the_after_failed_half(self):
        g = generate_cases
        fam = self._family("gen-heal")
        parked = {n for n, _kv, m in fam if "parked" in m}
        self.assertEqual({n for n, kv, _m in fam if kv["lift"] == "after-failed"}, parked)
        self.assertEqual(16, len(parked))
        for n, kv, m in fam:
            text = self.outputs[f"gen-heal/{n}.yaml"]
            engine = self._component_block(text.split("\n---\n")[0], "engine")
            if kv["lift"] == "after-failed":
                self.assertEqual((1, g.HEAL_ISSUE), (kv["notimpl"], kv["issue"]), n)
                self.assertNotIn("inv_xfail", kv)
                self.assertIn(f'instanceReadyTimeout: "{g.HEAL_FAILED_READY_TIMEOUT}"', engine, n)
                self.assertTrue(text.splitlines()[1].startswith("# reason: parked:"), n)
            else:
                self.assertNotIn("notimpl", kv, n)
                self.assertNotIn("instanceReadyTimeout", text, n)
        heal_parked = {k for k in g.PARKED if k[0] == "heal"}
        self.assertEqual(16, len(heal_parked))
        self.assertEqual({"after-failed"}, {k[4] for k in heal_parked})

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
