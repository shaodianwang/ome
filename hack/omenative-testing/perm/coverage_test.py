#!/usr/bin/env python3
"""Unit tests for coverage.py: a handful of synthetic headers per axis, and
the committed corpus as a smoke test."""
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import coverage
import gen_cases

HERE = Path(__file__).parent.resolve()


def header(cid, desc, **kv):
    return f"# {cid}: {desc}  " + " ".join(f"{k}={v}" for k, v in kv.items())


def counts(ne=2, nd=0, nr=0, ceil_e=None):
    return dict(Ne=ne, Nd=nd, Nr=nr, ceil_e=ceil_e if ceil_e is not None else ne + 1, ceil_d=nd, ceil_r=nr)


def body(cid, comps, coord=None):
    return gen_cases.render(cid, comps, coord=coord)


def engine(n=2, w=0, life=None):
    return [gen_cases.comp("engine", n, w, life if life is not None else gen_cases.lifecycle())]


def engine_router(n=2, w=0, nr=2):
    return [gen_cases.comp("engine", n, w, gen_cases.lifecycle()),
            gen_cases.comp("router", nr, 0, gen_cases.lifecycle())]


def pd(n=2, w=0, nr=2, life=None):
    return [gen_cases.comp("engine", n, w, life if life is not None else gen_cases.lifecycle()),
            gen_cases.comp("decoder", n, w, gen_cases.lifecycle()),
            gen_cases.comp("router", nr, 0, gen_cases.lifecycle())]


class Corpus:
    """A scratch case tree: write(...) adds a case, cases() parses the tree."""

    def __init__(self):
        self.dir = tempfile.TemporaryDirectory()
        self.root = Path(self.dir.name) / "isvcs"
        self.n = 9000

    def write(self, desc="a case", comps=None, coord=None, reason=None, folder="scratch", **kv):
        self.n += 1
        cid = f"p{self.n}"
        fields = dict(counts())
        fields.update(kv)
        lines = [header(cid, desc, **fields)]
        if reason:
            lines.append(f"# reason: {reason}")
        text = "\n".join(lines) + "\n" + body(cid, comps if comps is not None else engine(), coord)
        path = self.root / folder / f"{cid}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return coverage.read_case(path)

    def cases(self):
        return coverage.load_cases(self.root)

    def close(self):
        self.dir.cleanup()


class HeaderParsingTest(unittest.TestCase):
    def setUp(self):
        self.corpus = Corpus()
        self.addCleanup(self.corpus.close)

    def test_description_words_that_look_like_keys_stay_description(self):
        """The key region is walked backward and stops at the first word that
        is not a known, unseen key, as run.py does: `maxSurge=2` is never a
        key, and the description's `mode=Auto` repeats a key the region holds."""
        case = self.corpus.write("migrate engine N=8 maxSurge=2 mode=Auto", mode="migrate", mig="Auto", topo="engine")
        self.assertEqual("migrate engine N=8 maxSurge=2 mode=Auto", case.description)
        self.assertNotIn("maxSurge", case.header)
        self.assertEqual("migrate", case.mode)
        self.assertEqual([], case.problems)

    def test_values_are_int_coerced_like_run_py(self):
        case = self.corpus.write(Ne=4, ceil_e=5, topo="engine", issue=12, notimpl=1)
        self.assertEqual(4, case.header["Ne"])
        self.assertEqual(12, case.issue)
        self.assertEqual("parked", case.parked)

    def test_unknown_value_is_a_problem(self):
        case = self.corpus.write(topo="engine", mode="teleport")
        self.assertIn("unknown value mode=teleport", case.problems)

    def test_malformed_typed_value_is_a_problem(self):
        case = self.corpus.write(topo="engine", fault="podkill:lunch")
        self.assertIn("malformed value fault=podkill:lunch", case.problems)

    def test_missing_required_key_is_a_problem(self):
        path = self.corpus.root / "scratch" / "p1.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# p1: no counts  topo=engine\n" + body("p1", engine()))
        case = coverage.read_case(path)
        self.assertIn("header missing Ne=", case.problems)

    def test_reason_is_read_from_line_two_only(self):
        case = self.corpus.write(topo="engine", reason="the operator wants the fleet back at N")
        self.assertEqual("the operator wants the fleet back at N", case.reason)
        plain = self.corpus.write(topo="engine")
        self.assertEqual("", plain.reason)

    def test_vocabulary_covers_every_key_run_py_validates(self):
        import run
        self.assertEqual(set(), set(run.HDR_VALIDATORS) - coverage.HEADER_KEYS)


class ShapeTest(unittest.TestCase):
    def setUp(self):
        self.corpus = Corpus()
        self.addCleanup(self.corpus.close)

    def test_component_set_comes_from_the_counts(self):
        self.assertEqual("engine single", self.corpus.write(topo="engine").shape_family)
        self.assertEqual("engine+decoder+router single",
                         self.corpus.write(comps=pd(), topo="pd", **counts(2, 2, 2)).shape_family)
        self.assertEqual("engine+decoder single",
                         self.corpus.write(comps=pd(nr=0), topo="pd", **counts(2, 2, 0)).shape_family)
        self.assertEqual("engine+router single",
                         self.corpus.write(comps=pd(), topo="engine", **counts(2, 0, 2)).shape_family)

    def test_gang_width_is_read_from_the_body(self):
        case = self.corpus.write(comps=engine(2, w=2), topo="engine-mn")
        self.assertEqual(("engine gang", 2), (case.shape_family, case.gang_width))
        self.assertEqual(0, self.corpus.write(topo="engine").gang_width)

    def test_a_gang_engine_behind_a_router_is_the_engine_router_gang_family(self):
        comps = [gen_cases.comp("engine", 2, 1, gen_cases.lifecycle()),
                 gen_cases.comp("router", 2, 0, gen_cases.lifecycle())]
        case = self.corpus.write(comps=comps, topo="engine-mn", **counts(2, 0, 2))
        self.assertEqual(("engine+router gang", 1, "2"), (case.shape_family, case.gang_width, case.size_class))

    def test_size_class_follows_the_engine_count(self):
        self.assertEqual("1", self.corpus.write(topo="engine", **counts(1)).size_class)
        self.assertEqual("2", self.corpus.write(topo="engine", **counts(2)).size_class)
        self.assertEqual("many", self.corpus.write(topo="engine", **counts(3)).size_class)


class OperationTest(unittest.TestCase):
    def setUp(self):
        self.corpus = Corpus()
        self.addCleanup(self.corpus.close)

    def op(self, **kv):
        case = self.corpus.write(**kv)
        return case.operation, case.second_operation

    def test_plain_bump_is_a_push(self):
        self.assertEqual(("push", ""), self.op(topo="engine"))
        self.assertEqual("env edit", self.corpus.write(topo="engine", edit="env").operation_detail)

    def test_kill_is_steady_state_or_the_pause_it_runs_under(self):
        self.assertEqual(("steady state", ""), self.op(mode="kill", topo="engine", restart="None"))
        self.assertEqual(("pause", ""), self.op(mode="kill", topo="engine", pause="true"))
        self.assertEqual(("freeze", ""), self.op(mode="kill", topo="engine", pause="freeze"))

    def test_scaledown_mode_reads_its_direction_from_the_targets(self):
        self.assertEqual(("scale down", ""), self.op(mode="scaledown", topo="engine", **counts(4), to_e=2))
        self.assertEqual(("scale up", ""), self.op(mode="scaledown", topo="engine", **counts(2), to_e=4))

    def test_operations_during_a_roll_carry_push_as_the_second_operation(self):
        self.assertEqual(("migrate", "push"), self.op(mode="migrate", mig="Auto", during="rollout", topo="engine"))
        self.assertEqual(("delete", "push"), self.op(mode="recreate", during="rollout", topo="engine"))
        self.assertEqual(("delete", "migrate"), self.op(mode="recreate", during="migrate", inst=0, topo="engine"))
        self.assertEqual(("runtime edit", "push"),
                         self.op(mode="runtimerev", rt="rt-x", edit="material", during="rollout", topo="engine"))

    def test_runtime_pairing_partition_and_rebump_modes(self):
        self.assertEqual(("runtime edit", ""), self.op(mode="runtimerev", rt="rt-x", expect="roll", topo="engine"))
        self.assertEqual(("push", ""), self.op(mode="pairing", proto="nixl-v2", comps=pd(), topo="pd", **counts(2, 2, 2)))
        self.assertEqual(("push", "pause"), self.op(mode="pairing", proto="nixl-v2", pause="true", comps=pd(nr=0), topo="pd", **counts(2, 2, 0)))
        self.assertEqual(("push", "scale up"), self.op(mode="pairing", proto="nixl-v2", scale_e=4, comps=pd(nr=0), topo="pd", **counts(2, 2, 0)))
        self.assertEqual(("push", ""), self.op(mode="partition", partition=2, topo="engine"))
        self.assertEqual(("push", ""), self.op(mode="rebump", topo="engine"))
        self.assertEqual(("remove component", ""), self.op(mode="compremove", remove="decoder", comps=pd(nr=0), topo="pd", **counts(2, 2, 0)))

    def test_canary_verbs_that_are_operations_become_the_second_operation(self):
        self.assertEqual(("canary", ""), self.op(mode="canary", canary="auto", topo="engine"))
        self.assertEqual(("canary", "scale up"), self.op(mode="canary", canary="auto", during="canary-serving", verb="scale", scale_e=4, topo="engine"))
        self.assertEqual(("canary", "pause"), self.op(mode="canary", canary="auto", during="canary-serving", verb="pause", topo="engine"))
        self.assertEqual(("canary", "freeze"), self.op(mode="canary", canary="auto", during="canary-serving", verb="pause-freeze", topo="engine"))
        self.assertEqual(("canary", "push"), self.op(mode="canary", canary="auto", during="canary-serving", verb="rebump", topo="engine"))
        self.assertEqual(("canary", ""), self.op(mode="canary", canary="manual", during="canary-serving", verb="promote", topo="engine"))

    def test_mid_roll_edit_and_idle_fault_and_reject(self):
        self.assertEqual(("strategy or policy edit", "push"),
                         self.op(edit="strategy:RecreatePod", during="settling", topo="engine"))
        self.assertEqual(("steady state", ""),
                         self.op(fault="crashloop:old", form="start", during="idle", expect="repaired", topo="engine"))
        self.assertEqual(("admission reject", ""), self.op(mode="canary", expect="reject", topo="engine"))

    def test_window_faults_sit_on_the_operation_they_hold(self):
        idle = self.corpus.write(fault="webhook:idle", during="idle", lift="after-hold", topo="engine")
        self.assertEqual(("steady state", "window fault at idle", "steady state"), (idle.operation, idle.operation_detail, idle.timing))
        push = self.corpus.write(fault="conflict:settling", form="persist", target="isvc", topo="engine")
        self.assertEqual(("push", "settling", ["operation+fault"]), (push.operation, push.timing, push.overlap))
        up = self.corpus.write(mode="scaledown", to_e=3, fault="webhook:settling", lift="after-window", topo="engine")
        self.assertEqual(("scale up", "settling"), (up.operation, up.timing))
        router = [gen_cases.comp("engine", 2, 0, gen_cases.lifecycle()), gen_cases.comp("router", 2, 0, gen_cases.lifecycle())]
        behind = self.corpus.write(fault="webhook:settling", lift="after-hold", comps=router, topo="engine", **counts(2, 0, 2))
        self.assertEqual("engine+router single", behind.shape_family)
        self.assertEqual([("webhook unavailable", "one-shot", "-")], behind.fault_rows())

    def test_a_scale_or_a_delete_on_a_broken_instance_keeps_its_operation(self):
        """A break fired before a scale or a delete (during=idle in those modes)
        is the operation plus a fault on the running revision, landing at
        steady state."""
        down = self.corpus.write(mode="scaledown", to_e=3, fault="crashloop:old", form="start", during="idle", inst=0,
                                 topo="engine", **counts(4, ceil_e=4))
        self.assertEqual(("scale down", "", "steady state", ["operation+fault"]),
                         (down.operation, down.second_operation, down.timing, down.overlap))
        self.assertEqual([("crash at start", "persistent", "running", "engine")], down.faults)
        up = self.corpus.write(mode="scaledown", to_e=5, fault="readiness:old", form="persist", during="idle", inst=0,
                               expect="left", topo="engine", **counts(4, ceil_e=5))
        self.assertEqual(("scale up", "expect=left"), (up.operation, up.outcome))
        self.assertEqual([("readiness never passes", "persistent", "running", "engine")], up.faults)
        self.assertEqual([], up.problems)
        delete = self.corpus.write(mode="recreate", fault="imagepull:old", during="idle", inst=0, topo="engine",
                                   **counts(4, ceil_e=4))
        self.assertEqual(("delete", "", "steady state"), (delete.operation, delete.second_operation, delete.timing))
        self.assertEqual([("image pull", "persistent", "running", "engine")], delete.faults)


class FaultTest(unittest.TestCase):
    def setUp(self):
        self.corpus = Corpus()
        self.addCleanup(self.corpus.close)

    def fault(self, **kv):
        return self.corpus.write(**kv).faults

    def test_no_fault(self):
        self.assertEqual([], self.fault(topo="engine"))

    def test_break_faults_on_the_running_revision(self):
        self.assertEqual([("crash at start", "persistent", "running", "engine")],
                         self.fault(fault="crashloop:old", form="start", topo="engine"))
        self.assertEqual([("crash after Ready", "persistent", "running", "engine")],
                         self.fault(fault="crashloop:old", form="load", topo="engine"))
        self.assertEqual([("image pull", "persistent", "running", "engine")],
                         self.fault(fault="imagepull:old", topo="engine"))

    def test_a_packed_pool_keeps_the_breaks_side_and_marks_the_push(self):
        case = self.corpus.write(fault="crashloop:new", pool="packed", form="start", inv_on="J,K", topo="engine",
                                 **counts(3, 0, 0, ceil_e=3))
        self.assertEqual([("crash at start", "persistent", "new", "engine")], case.faults)
        self.assertEqual(("push", "annotation bump, packed pool", ""), (case.operation, case.operation_detail, case.second_operation))
        self.assertEqual("settling", case.timing)
        case = self.corpus.write(fault="crashloop:idle", pool="packed", form="load", floor_soft=1, inv_on="J,K", topo="engine",
                                 **counts(3, 0, 0, ceil_e=3))
        self.assertEqual([("crash after Ready", "persistent", "running", "engine")], case.faults)
        self.assertEqual(("push", "steady state"), (case.operation, case.timing))
        self.assertEqual([], case.problems)
        self.assertFalse(case.invariants_off)
        bad = self.corpus.write(fault="crashloop:new", pool="full", form="start", inv_on="J,Z", topo="engine", **counts(3, 0, 0, ceil_e=3))
        self.assertEqual({"unknown value pool=full", "malformed value inv_on=J,Z"}, set(bad.problems))

    def test_a_break_on_the_decoder_is_a_broken_sibling(self):
        self.assertEqual([("sibling broken", "persistent", "running", "decoder")],
                         self.fault(fault="crashloop:old", form="start", victimcomp="decoder",
                                    comps=pd(), topo="pd", **counts(2, 2, 2)))

    def test_readiness_that_stops_passing_is_a_persistent_break_of_the_running_revision(self):
        self.assertEqual([("readiness never passes", "persistent", "running", "engine")],
                         self.fault(mode="recreate", fault="readiness:old", form="persist", during="idle", topo="engine"))
        self.assertEqual([("sibling broken", "persistent", "running", "decoder")],
                         self.fault(mode="recreate", fault="readiness:old", form="persist", during="idle",
                                    victimcomp="decoder", comps=pd(), topo="pd", **counts(2, 2, 2)))
        self.assertEqual([("readiness never passes", "persistent", "new", "engine")],
                         self.fault(fault="readiness:new", topo="engine"))
        self.assertEqual([("readiness never passes", "one-shot", "new", "engine")],
                         self.fault(fault="readiness:new", form="recover", topo="engine"))

    def test_lifted_causes_are_the_one_shot_form(self):
        self.assertEqual([("config missing", "one-shot", "running", "engine")],
                         self.fault(fault="configkey:missing", during="idle", lift="after-settle", expect="heals", topo="engine"))
        self.assertEqual([("quota", "one-shot", "-", "headroom 0")],
                         self.fault(mode="migrate", mig="Auto", fault="quota:0", expect="fail", lift=1, topo="engine"))
        self.assertEqual([("quota", "persistent", "-", "headroom 1")],
                         self.fault(fault="quota:1", blocked=1, topo="engine"))

    def test_a_break_healed_at_idle_is_the_one_shot_running_form(self):
        self.assertEqual([("image pull", "one-shot", "running", "engine")],
                         self.fault(fault="imagepull:old", during="idle", lift="after-settle", expect="heals", topo="engine"))
        self.assertEqual([("image pull", "one-shot", "new", "engine")],
                         self.fault(fault="imagepull:new", lift="after-settle", topo="engine"))

    def test_a_cause_kept_to_the_end_is_the_persistent_form(self):
        self.assertEqual([("config missing", "persistent", "running", "engine")],
                         self.fault(fault="configkey:missing", during="idle", lift="never", expect="failed", topo="engine"))

    def test_a_pushed_revision_reading_a_missing_key_is_the_new_side(self):
        self.assertEqual([("config missing", "persistent", "new", "engine")],
                         self.fault(fault="configkey:new", lift="never", topo="engine"))
        self.assertEqual([("config missing", "one-shot", "new", "engine")],
                         self.fault(fault="configkey:new", lift="after-settle", topo="engine"))

    def test_node_fault_is_persistent_only_when_the_engine_must_act_with_the_node_down(self):
        self.assertEqual([("node", "persistent", "-", "engine")],
                         self.fault(fault="node:notready", during="idle", lift="after-recover", expect="relocated", topo="engine"))
        self.assertEqual([("node", "one-shot", "-", "engine")],
                         self.fault(fault="node:notready", during="idle", lift="after-settle", expect="repaired", topo="engine"))
        self.assertEqual([("node", "persistent", "-", "cordoned source node")],
                         self.fault(fault="cordon:source", topo="engine"))

    def test_pod_kills_follow_the_component_blind_victim_rule(self):
        self.assertEqual([("pod deleted by hand", "one-shot", "running", "engine")],
                         self.fault(mode="kill", restart="None", topo="engine"))
        self.assertEqual([("gang member lost", "one-shot", "running", "engine")],
                         self.fault(mode="kill", restart="RecreateInstance", kill="worker", comps=engine(2, w=1), topo="engine-mn"))
        surge = engine(2, life=gen_cases.lifecycle(strat="SurgeThenDrain"))
        self.assertEqual([("pod deleted by hand", "one-shot", "new", "engine")],
                         self.fault(fault="podkill:rotating", comps=surge, topo="engine"))
        self.assertEqual([("pod deleted by hand", "one-shot", "running", "engine")],
                         self.fault(fault="podkill:settling", comps=surge, topo="engine"))
        self.assertEqual([("pod deleted by hand", "one-shot", "running", "decoder")],
                         self.fault(fault="podkill:settling", comps=pd(), topo="pd", **counts(2, 2, 2)))
        self.assertEqual([("pod deleted by hand", "one-shot", "running", "decoder")],
                         self.fault(mode="canary", canary="auto", fault="podkill:canary-serving", victim="stable",
                                    victimcomp="decoder", comps=pd(nr=0), topo="pd", **counts(2, 2, 0)))

    def test_controller_restart_is_one_shot_and_unbound(self):
        self.assertEqual([("controller restart", "one-shot", "-", "manager")],
                         self.fault(fault="ctrlrestart:rotating", topo="engine"))

    def test_a_fault_that_keeps_repeating_is_the_persistent_form(self):
        self.assertEqual([("pod deleted by hand", "persistent", "running", "engine")],
                         self.fault(mode="kill", restart="None", repeat=3, topo="engine"))
        self.assertEqual([("gang member lost", "persistent", "running", "engine")],
                         self.fault(mode="kill", restart="RecreateInstance", kill="worker", repeat=3,
                                    comps=engine(2, w=1), topo="engine-mn"))
        self.assertEqual([("evicted or preempted", "persistent", "running", "engine")],
                         self.fault(mode="kill", restart="None", via="evict", repeat=3, topo="engine"))
        self.assertEqual([("evicted or preempted", "persistent", "running", "engine")],
                         self.fault(mode="kill", restart="None", via="preempt", lift="after-settle", repeat=2, topo="engine"))
        self.assertEqual([("pod deleted by hand", "persistent", "running", "engine")],
                         self.fault(fault="podkill:rotating", victim="stable", floor_slack=1, repeat=3, topo="engine"))
        self.assertEqual([("evicted or preempted", "persistent", "running", "engine")],
                         self.fault(mode="scaledown", to_e=3, fault="evict:settling", floor_slack=1, repeat=3, topo="engine"))
        self.assertEqual([("controller restart", "persistent", "-", "manager")],
                         self.fault(fault="ctrlrestart:settling", repeat=3, topo="engine"))
        self.assertEqual([("controller restart", "persistent", "-", "manager")],
                         self.fault(fault="ctrlrestart:idle", during="idle", repeat=3, topo="engine"))
        case = self.corpus.write(fault="ctrlrestart:idle", during="idle", repeat=3, topo="engine")
        self.assertEqual(("steady state", "steady state", [("controller restart", "persistent", "-")]),
                         (case.operation, case.timing, case.fault_rows()))
        case = self.corpus.write(mode="scaledown", to_e=3, fault="podkill:settling", floor_slack=1, repeat=3, topo="engine")
        self.assertEqual(("scale up", "settling"), (case.operation, case.timing))
    def test_webhook_outage_form_follows_its_lift(self):
        self.assertEqual([("webhook unavailable", "one-shot", "-", "admission webhook")],
                         self.fault(fault="webhook:settling", lift="after-hold", topo="engine"))
        self.assertEqual([("webhook unavailable", "persistent", "-", "admission webhook")],
                         self.fault(fault="webhook:idle", during="idle", lift="after-window", topo="engine"))

    def test_competing_writer_form_follows_its_writes(self):
        self.assertEqual([("apiserver conflict", "one-shot", "-", "competing writer on the isvc")],
                         self.fault(fault="conflict:settling", form="once", target="isvc", topo="engine"))
        self.assertEqual([("apiserver conflict", "persistent", "-", "competing writer on the ir")],
                         self.fault(mode="scaledown", to_e=3, fault="conflict:settling", form="persist", target="ir", topo="engine"))

    def test_canary_breaks_of_the_new_revision(self):
        self.assertEqual([("image pull", "persistent", "new", "engine")],
                         self.fault(mode="canary", canary="auto", bump="engine", img="example.com/x:none", terminal="Failed", topo="engine"))
        self.assertEqual([("sibling broken", "persistent", "new", "decoder")],
                         self.fault(mode="canary", canary="auto", bump="engine,decoder", img="example.com/x:none",
                                    imgcomp="decoder", terminal="Failed", comps=pd(nr=0), topo="pd", **counts(2, 2, 0)))
        self.assertEqual([("metric breach", "persistent", "new", "canary metric")],
                         self.fault(mode="canary", canary="auto", analysis="breach", failurelimit=1, topo="engine"))
        self.assertEqual([("metric breach", "one-shot", "new", "canary metric")],
                         self.fault(mode="canary", canary="auto", analysis="pass", health="breach", failurelimit=12, topo="engine"))
        self.assertEqual([("analysis source unusable", "persistent", "-", "prometheus")],
                         self.fault(mode="canary", canary="auto", analysis="inconclusive", oninconclusive="hold", topo="engine"))

    def test_gate_faults_name_their_rows_sides_and_forms(self):
        canary = dict(mode="canary", canary="auto", analysis="pass", health="pass", failurelimit=1, topo="engine")
        self.assertEqual([("metric breach", "persistent", "running", "engine")],
                         self.fault(fault="metric:idle", form="persist", **canary))
        self.assertEqual([("metric breach", "one-shot", "running", "engine")],
                         self.fault(fault="metric:canary-serving@1", form="recover", victim="stable", **canary))
        self.assertEqual([("metric breach", "persistent", "new", "engine")],
                         self.fault(fault="metric:canary-serving@2", form="persist", victim="canary",
                                    **dict(canary, analysis="breach")))
        self.assertEqual([("metric breach", "one-shot", "new", "engine")],
                         self.fault(fault="metric:new", form="recover", minfails=1, **dict(canary, failurelimit=6)))
        self.assertEqual([("metric breach", "persistent", "new", "engine")],
                         self.fault(fault="metric:new", form="persist", **dict(canary, analysis="breach")),
                         "the pushed revision's own breach counts once, under the fault, not again under analysis=")
        self.assertEqual([("analysis source unusable", "one-shot", "-", "unreachable")],
                         self.fault(fault="source:canary-serving@1", form="unreachable", lift="after-settle",
                                    oninconclusive="hold", **canary))
        self.assertEqual([("analysis source unusable", "persistent", "-", "garbage")],
                         self.fault(fault="source:idle", form="garbage", lift="never", oninconclusive="rollbackonstall",
                                    terminal="RolledBack", **canary))

    def test_a_break_the_harness_heals_is_one_shot_and_a_canary_side_victim_is_the_new_side(self):
        router = dict(mode="canary", canary="auto", comps=engine_router(), topo="engine", **counts(2, 0, 2))
        self.assertEqual([("sibling broken", "one-shot", "running", "router")],
                         self.fault(fault="crashloop:canary-serving@1", form="load", victimcomp="router", lift="after-settle", **router))
        self.assertEqual([("sibling broken", "persistent", "running", "router")],
                         self.fault(fault="imagepull:idle", victimcomp="router", **router))
        self.assertEqual([("sibling broken", "one-shot", "new", "decoder")],
                         self.fault(fault="crashloop:canary-serving@2", form="load", victim="canary", victimcomp="decoder",
                                    lift="after-settle", mode="canary", canary="auto", comps=pd(), topo="pd", **counts(2, 2, 2)))
        case = self.corpus.write(mode="canary", canary="auto", bump="engine,router", img="example.com/x:none",
                                 imgcomp="router", terminal="Stable", comps=engine_router(), topo="engine", **counts(2, 0, 2))
        self.assertEqual([("sibling broken", "persistent", "new", "router")], case.faults)
        self.assertTrue(self.corpus.write(fault="metric:new", form="recover", mode="canary", canary="auto", analysis="pass",
                                          failurelimit=6, minfails=1, topo="engine").fault_lifted)

    def test_runtime_recovery_guards_break_the_new_revision(self):
        self.assertEqual([("image pull", "persistent", "new", "runtime image")],
                         self.fault(mode="runtimerev", rt="rt-x", expect="recover", topo="engine"))
        self.assertEqual([("readiness never passes", "persistent", "new", "engine")],
                         self.fault(mode="runtimerev", rt="rt-x", expect="recover", **{"break": "stuck"}, topo="engine"))

    def test_a_case_with_two_breaks_counts_in_both_rows(self):
        case = self.corpus.write(mode="canary", canary="auto", bump="engine", img="example.com/x:none",
                                 terminal="Failed", fault="ctrlrestart:canary-failed", topo="engine")
        self.assertEqual([("controller restart", "one-shot", "-"), ("image pull", "persistent", "new")],
                         case.fault_rows())

    def test_fault_rows_drop_the_side_of_unbound_faults(self):
        case = self.corpus.write(mode="kill", restart="None", topo="engine")
        self.assertEqual([("pod deleted by hand", "one-shot", "-")], case.fault_rows())
        self.assertEqual([coverage.NO_FAULT], self.corpus.write(topo="engine").fault_rows())


class BodyFactsTest(unittest.TestCase):
    def setUp(self):
        self.corpus = Corpus()
        self.addCleanup(self.corpus.close)

    def test_group_kind_is_read_from_the_rollout_groups(self):
        for kind, group in [("none", "none"), ("blueGreen", "bluegreen"), ("rollingUpdate", "rolling"),
                            ("rollingUpdate", "percomp"), ("rollingUpdate+maintainRatio", "ratio"),
                            ("Sequential", "seq")]:
            case = self.corpus.write(comps=pd(), coord=gen_cases.coord_for(group), topo="pd", co=group, **counts(2, 2, 2))
            self.assertEqual(kind, case.group, group)
        steps = gen_cases.canary_steps("auto", ("50%", 50), ("100%", 100))
        case = self.corpus.write(mode="canary", canary="auto", coord=gen_cases.canary_block(steps), topo="engine")
        self.assertEqual("canary", case.group)

    def test_strategy_budget_and_restart_policy_come_from_the_engine_lifecycle(self):
        case = self.corpus.write(comps=engine(life=gen_cases.lifecycle(strat="RecreatePod", surge=0, unavail="50%",
                                                                       restart="RecreateInstanceOnPodRestart")), topo="engine")
        self.assertEqual(("RecreatePod", "percent", "RecreateInstanceOnPodRestart"),
                         (case.strategy, case.budget, case.restart_policy))
        case = self.corpus.write(comps=engine(life=gen_cases.lifecycle(strat="SurgeThenDrain", surge=2, unavail=0)), topo="engine")
        self.assertEqual(("SurgeThenDrain", "2+", "unset"), (case.strategy, case.budget, case.restart_policy))
        case = self.corpus.write(comps=engine(life=gen_cases.lifecycle(strat="InPlaceOnly", surge=0, unavail=0)), topo="engine")
        self.assertEqual(("InPlaceOnly", "0"), (case.strategy, case.budget))
        case = self.corpus.write(comps=[gen_cases.comp("engine", 2, 0, life="")], topo="engine")
        self.assertEqual(("unset", "unset"), (case.strategy, case.budget))

    def test_block_style_lifecycle_is_read_too(self):
        path = self.corpus.root / "scratch" / "p1.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(header("p1", "block style", **counts(), topo="engine") + "\n"
                        "apiVersion: ome.io/v1beta1\nkind: InferenceService\nmetadata: { name: p1 }\nspec:\n"
                        "  engine:\n    minReplicas: 2\n    maxReplicas: 2\n    lifecycle:\n      restartPolicy: None\n"
                        "      updateStrategy:\n        type: RecreatePod\n        rollingUpdate: { maxSurge: 0, maxUnavailable: 1 }\n")
        case = coverage.read_case(path)
        self.assertEqual(("RecreatePod", "1", "None"), (case.strategy, case.budget, case.restart_policy))

    def test_canary_step_kinds_are_read_from_the_steps(self):
        auto = gen_cases.canary_steps("auto", ("25%", 25), ("50%", 50), ("100%", 100))
        case = self.corpus.write(mode="canary", canary="auto", coord=gen_cases.canary_block(auto), topo="engine")
        self.assertEqual(["traffic step", "timed soak"], case.canary_steps)
        manual = gen_cases.canary_steps("manual", ("50%", 50), ("100%", 100))
        case = self.corpus.write(mode="canary", canary="manual", coord=gen_cases.canary_block(manual), topo="engine")
        self.assertEqual(["traffic step", "manual gate", "promote"], case.canary_steps)
        smoke = gen_cases.canary_steps("auto", ("100%", 0), ("100%", 100))
        case = self.corpus.write(mode="canary", canary="auto", coord=gen_cases.canary_block(smoke), topo="engine")
        self.assertIn("capacity ahead of traffic", case.canary_steps)
        gated = [gen_cases.astep("50%", 50, gen_cases.analysis_spec()), gen_cases.astep("100%", 100)]
        case = self.corpus.write(mode="canary", canary="auto", analysis="breach", failurelimit=1,
                                 coord=gen_cases.canary_block(gated), topo="engine")
        self.assertEqual(["traffic step", "analysis gate", "rollback mid-ladder"], case.canary_steps)

    def test_canary_verbs_add_their_step_kinds(self):
        steps = gen_cases.canary_block(gen_cases.canary_steps("auto", ("50%", 50), ("100%", 100)))
        case = self.corpus.write(mode="canary", canary="auto", coord=steps, during="canary-failed", verb="rebump-fix",
                                 bump="engine", img="example.com/x:none", terminal="Stable", topo="engine")
        self.assertIn("re-target mid-ladder", case.canary_steps)
        case = self.corpus.write(mode="canary", canary="auto", coord=steps, during="canary-serving", verb="planedit", topo="engine")
        self.assertIn("plan or run edit", case.canary_steps)
        case = self.corpus.write(mode="canary", canary="auto", coord=steps, rollback=1, during="canary-rolledback",
                                 verb="resume", terminal="Stable", topo="engine")
        self.assertEqual(["traffic step", "timed soak", "rollback mid-ladder", "resume"], case.canary_steps)
        case = self.corpus.write(mode="canary", canary="auto", coord=steps, bump="engine", img="example.com/x:none",
                                 terminal="Failed", topo="engine")
        self.assertIn("abort", case.canary_steps)


class TimingAndOverlapTest(unittest.TestCase):
    def setUp(self):
        self.corpus = Corpus()
        self.addCleanup(self.corpus.close)

    def timing(self, **kv):
        return self.corpus.write(**kv).timing

    def test_plain_operations_have_no_mid_flight_event(self):
        self.assertEqual("-", self.timing(topo="engine"))
        self.assertEqual("-", self.timing(mode="scaledown", to_e=1, topo="engine"))

    def test_fault_phases_and_hooks(self):
        self.assertEqual("settling", self.timing(fault="podkill:settling", topo="engine"))
        self.assertEqual("rotating", self.timing(fault="podkill:rotating", topo="engine"))
        self.assertEqual("settling", self.timing(fault="crashloop:old", form="start", topo="engine"))
        self.assertEqual("steady state", self.timing(fault="crashloop:old", form="start", during="idle", expect="repaired", topo="engine"))
        self.assertEqual("rotating", self.timing(mode="migrate", mig="Auto", during="rollout", topo="engine"))
        self.assertEqual("migrating", self.timing(mode="recreate", during="migrate", inst=0, topo="engine"))
        self.assertEqual("rotating", self.timing(mode="rebump", topo="engine"))

    def test_kill_lands_at_steady_state_or_while_paused(self):
        self.assertEqual("steady state", self.timing(mode="kill", restart="None", topo="engine"))
        self.assertEqual("paused", self.timing(mode="kill", restart="None", pause="true", topo="engine"))

    def test_canary_hooks_name_the_step_phase(self):
        steps = gen_cases.canary_block(gen_cases.canary_steps("auto", ("50%", 50), ("100%", 100)))
        base = dict(mode="canary", canary="auto", coord=steps, topo="engine")
        self.assertEqual("canary soak", self.timing(during="canary-serving", verb="pause", **base))
        self.assertEqual("canary staging", self.timing(during="canary-staging", verb="rollback", terminal="RolledBack", **base))
        self.assertEqual("canary pre-hold", self.timing(during="canary-serving@1", verb="planedit-repin", verb2="promote",
                                                        during2="canary-prehold", **base))
        self.assertEqual("canary failed", self.timing(fault="ctrlrestart:canary-failed", bump="engine",
                                                      img="example.com/x:none", terminal="Failed", **base))
        self.assertEqual("steady state", self.timing(prearm="pause", **base))
        self.assertEqual("canary staging", self.timing(bump="engine", img="example.com/x:none", terminal="Failed", **base))
        manual = gen_cases.canary_block(gen_cases.canary_steps("manual", ("50%", 50), ("100%", 100)))
        self.assertEqual("canary step", self.timing(mode="canary", canary="manual", coord=manual, during="canary-serving",
                                                    verb="promote", topo="engine"))
        self.assertEqual("canary analysis", self.timing(mode="canary", canary="auto", analysis="pass", failurelimit=1,
                                                        coord=steps, topo="engine"))

    def test_overlap_kinds(self):
        self.assertEqual([], self.corpus.write(topo="engine").overlap)
        self.assertEqual(["operation+fault"], self.corpus.write(fault="podkill:rotating", topo="engine").overlap)
        self.assertEqual([], self.corpus.write(mode="kill", restart="None", topo="engine").overlap)
        self.assertEqual(["operation+operation"],
                         self.corpus.write(mode="migrate", mig="Auto", during="rollout", topo="engine").overlap)
        self.assertEqual(["two revisions in a row"], self.corpus.write(mode="rebump", topo="engine").overlap)
        both = self.corpus.write(mode="canary", canary="auto", during="canary-serving", verb="rebump", topo="engine")
        self.assertEqual(["operation+operation", "two revisions in a row"], both.overlap)

    def test_lifted_faults(self):
        self.assertTrue(self.corpus.write(mode="migrate", mig="Auto", fault="quota:0", expect="fail", lift=1, topo="engine").fault_lifted)
        self.assertTrue(self.corpus.write(fault="configkey:missing", during="idle", lift="after-failed", expect="heals", topo="engine").fault_lifted)
        self.assertFalse(self.corpus.write(fault="quota:0", blocked=1, topo="engine").fault_lifted)


class ParkedTest(unittest.TestCase):
    def setUp(self):
        self.corpus = Corpus()
        self.addCleanup(self.corpus.close)

    def test_parked_kinds_and_issues(self):
        self.assertEqual(("parked", 77), (lambda c: (c.parked, c.issue))(self.corpus.write(notimpl=1, issue=77, topo="engine")))
        self.assertEqual(("infeasible", None), (lambda c: (c.parked, c.issue))(self.corpus.write(infeasible=1, reason="x", topo="engine")))
        parked = self.corpus.write(fault="configkey:missing", during="idle", lift="after-failed", expect="parked", issue=5, topo="engine")
        self.assertEqual(("parked", 5), (parked.parked, parked.issue))
        self.assertEqual(("", None), (lambda c: (c.parked, c.issue))(self.corpus.write(topo="engine")))

    def test_cells_with_only_parked_cases_are_listed_with_their_issues(self):
        self.corpus.write(notimpl=1, issue=77, topo="engine")
        self.corpus.write(mode="kill", restart="None", topo="engine")
        self.corpus.write(mode="kill", restart="None", notimpl=1, issue=78, topo="engine")
        parked = coverage.parked_cells(coverage.matrix_cells(self.corpus.cases()))
        self.assertEqual([(("engine single", "push", coverage.NO_FAULT), [77])],
                         [(key, issues) for key, _ids, issues, _k in parked])


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.corpus = Corpus()
        self.addCleanup(self.corpus.close)

    def test_an_engine_only_service_has_no_sibling_cell(self):
        removed = [(family, op, row) for family, op, row in
                   [(f, "canary", ("sibling broken", "persistent", "running")) for f in coverage.SHAPE_FAMILIES]
                   if coverage.excluded(family, op, row)]
        self.assertEqual([("engine single", "canary", ("sibling broken", "persistent", "running")),
                          ("engine gang", "canary", ("sibling broken", "persistent", "running"))], removed)
        self.assertFalse(coverage.excluded("engine+router single", "canary", ("sibling broken", "one-shot", "new")))

    def test_removed_cells_are_not_counted_empty(self):
        product = coverage.product_cells()
        self.assertNotIn(("engine single", "steady state", coverage.NO_FAULT), product)
        self.assertNotIn(("engine single", "push", ("gang member lost", "one-shot", "-")), product)
        self.assertNotIn(("engine gang", "push", ("pod deleted by hand", "one-shot", "-")), product)
        self.assertIn(("engine gang", "push", ("gang member lost", "one-shot", "-")), product)
        self.assertNotIn(("engine single", "canary", ("sibling broken", "one-shot", "new")), product)
        self.assertIn(("engine+router gang", "canary", ("sibling broken", "one-shot", "new")), product)
        raw = len(coverage.SHAPE_FAMILIES) * len(coverage.OPERATIONS) * len(coverage.all_fault_rows())
        # the steady state with no fault, a lost gang member on single pods, a
        # hand-deleted pod on gangs, and the four sibling rows on the two
        # engine-only families
        self.assertEqual(raw - 6 - 72 - 72 - 96, len(product))

    def test_a_case_fills_its_cell_and_the_rest_stay_empty(self):
        self.corpus.write(fault="podkill:rotating", topo="engine")
        cells = coverage.matrix_cells(self.corpus.cases())
        self.assertIn(("engine single", "push", ("pod deleted by hand", "one-shot", "-")), cells)
        empty = coverage.empty_cells(cells)
        self.assertEqual(len(coverage.product_cells()) - 1, len(empty))

    def test_stories_name_their_cells(self):
        self.corpus.write(fault="crashloop:old", form="start", topo="engine")
        proved = {n: cells for n, _t, _p, cells in coverage.story_cells(self.corpus.cases(), {})}
        self.assertEqual([("engine single", "push", ("crash at start", "persistent", "running"))], proved[2])
        self.assertEqual([], proved[8])

    def test_check_fails_on_an_unknown_value_and_on_a_story_with_no_case(self):
        self.corpus.write(topo="engine", mode="teleport")
        err = io.StringIO()
        with redirect_stdout(io.StringIO()), redirect_stderr(err):
            rc = coverage.check(self.corpus.cases())
        self.assertEqual(1, rc)
        self.assertIn("unknown value mode=teleport", err.getvalue())
        self.assertIn("story 2 has no case", err.getvalue())

    def test_markdown_and_json_render_a_small_corpus(self):
        self.corpus.write(fault="crashloop:old", form="start", topo="engine", reason="the fix lands")
        cases = self.corpus.cases()
        text = coverage.render_markdown(cases, with_cases=True)
        for heading in ("## Summary", "## Operator stories", "## Empty cells, biggest holes first",
                        "## The matrix", "## Policy crossings", "## Canary ladders", "## Parked",
                        "## Not derivable from the headers", "## Cases"):
            self.assertIn(heading, text)
        data = json.loads(coverage.render_json(cases))
        self.assertEqual(1, len(data["cases"]))
        self.assertEqual("crash at start", data["cases"][0]["faults"][0]["kind"])
        self.assertEqual(len(coverage.product_cells()) - 1, len(data["empty_cells"]))


class CommittedCorpusTest(unittest.TestCase):
    """The committed tree as a smoke test: every header classified, every
    story proved, and the generated families land where their generator says."""

    @classmethod
    def setUpClass(cls):
        cls.cases = coverage.load_cases(HERE / "isvcs")
        cls.by_folder = {}
        for case in cls.cases:
            cls.by_folder.setdefault(case.folder, []).append(case)

    def test_every_case_file_is_read_and_clean(self):
        self.assertEqual(len(list((HERE / "isvcs").glob("**/*.yaml"))), len(self.cases))
        self.assertEqual([], [(c.id, p) for c in self.cases for p in c.problems])

    def test_check_passes(self):
        with redirect_stdout(io.StringIO()):
            self.assertEqual(0, coverage.check(self.cases))

    def test_generated_families_classify_as_their_generator_describes(self):
        for case in self.by_folder["gen-rollout"]:
            self.assertEqual(("push", []), (case.operation, case.faults), case.id)
        for case in self.by_folder["gen-fault"]:
            self.assertEqual("push", case.operation, case.id)
            self.assertEqual(["one-shot"], [f[1] for f in case.faults], case.id)
            self.assertIn(case.timing, ("settling", "rotating"), case.id)
        for case in self.by_folder["gen-restart"]:
            self.assertIn(case.operation, ("steady state", "pause", "freeze"), case.id)
            self.assertEqual(1, len(case.faults), case.id)
        for case in self.by_folder["gen-crashloop"]:
            self.assertEqual("push", case.operation, case.id)
            self.assertIn(case.fault_kind, ("crash at start", "crash after Ready"), case.id)
            self.assertEqual(("persistent", "running"), (case.fault_form, case.fault_side), case.id)
        for case in self.by_folder["canary"]:
            self.assertIn(case.operation, ("canary", "admission reject"), case.id)
        rows = {(c.shape_family, c.operation, r) for c in self.by_folder["gen-webhook"] + self.by_folder["gen-conflict"]
                for r in c.fault_rows()}
        for kind in ("webhook unavailable", "apiserver conflict"):
            for form in ("one-shot", "persistent"):
                for family in coverage.SHAPE_FAMILIES:
                    for operation in ("steady state", "push", "scale up"):
                        self.assertIn((family, operation, (kind, form, "-")), rows, (kind, form, family, operation))
        sizes = self.by_folder["gen-size"]
        self.assertEqual({"1", "2"}, {c.size_class for c in sizes})
        self.assertEqual({"engine single", "engine+decoder+router single"}, {c.shape_family for c in sizes})
        self.assertEqual({"push", "scale up", "scale down", "canary"}, {c.operation for c in sizes})
        self.assertEqual({"1", "percent"}, {c.budget for c in sizes if c.operation == "push"})
        gang = self.by_folder["gen-router-gang"]
        self.assertEqual({"engine+router gang"}, {c.shape_family for c in gang})
        self.assertEqual({1, 2}, {c.gang_width for c in gang})
        self.assertEqual({"push", "scale up", "scale down", "canary"}, {c.operation for c in gang})
        for case in sizes + gang:
            self.assertEqual("", case.parked, f"{case.id}: nothing is parked")
        packed = self.by_folder["gen-packed-pool"]
        self.assertEqual(set(coverage.SHAPE_FAMILIES), {c.shape_family for c in packed},
                         "the six shape families of the product")
        self.assertEqual({"push"}, {c.operation for c in packed})
        self.assertEqual({("crash at start", "persistent", "new"), ("crash after Ready", "persistent", "new"),
                          ("crash at start", "persistent", "running"), ("crash after Ready", "persistent", "running")},
                         {(c.fault_kind, c.fault_form, c.fault_side) for c in packed})
        self.assertEqual({"RecreatePod", "InPlaceIfPossible"}, {c.strategy for c in packed})
        self.assertEqual({"1"}, {c.budget for c in packed})
        self.assertEqual({"none", "rollingUpdate"}, {c.group for c in packed})
        self.assertEqual({"None", "RecreateInstanceOnPodRestart"}, {c.restart_policy for c in packed})
        for case in packed:
            self.assertEqual("", case.parked, f"{case.id}: nothing is parked")
            self.assertIn("packed pool", case.operation_detail, case.id)

    def test_the_corpus_markdown_renders(self):
        text = coverage.render_markdown(self.cases)
        self.assertIn("stories with no cell: none", text)
        self.assertIn("## Empty cells, biggest holes first", text)
        self.assertIn("### Size class x shape family", text)


if __name__ == "__main__":
    unittest.main()
