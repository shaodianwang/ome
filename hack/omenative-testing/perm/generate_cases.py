#!/usr/bin/env python3
"""generate_cases.py — the generated hot-core permutation corpus (isvcs/gen-*/).

Emits the constraint-filtered cross product of
    topology x updateStrategy x failure verb x injection phase x
    restartPolicy x pause
into four generator-owned folders, using ONLY verbs and hooks the runners
(run.py + the KIND CI lane) already implement:

    gen-rollout/   p1000+  baseline revision-bump rollouts (failure=none)
    gen-fault/     p1200+  rollouts with fault=podkill:settling|rotating
    gen-restart/   p1600+  mode=kill (steady-state pod kill), pause off|true|freeze
    gen-crashloop/ p2000+  rollouts where one old-revision Instance crashes
                          (fault=crashloop:old, form=start|load) from the
                          moment the bump lands, crossed with topology x
                          strategy x maxUnavailable x restartPolicy x
                          coordination; the fix must land on that Instance

Ownership and determinism:
  - The gen-* folders belong ENTIRELY to this generator: every case there
    carries the `[gen]` mark on line 1, and a regen sweeps and rewrites the
    marked files wholesale. A file without the mark is hand-written, whatever
    its id: it stops the regen before anything is written or swept, and must
    move to a hand-owned folder. When a generated case finds a bug worth
    pinning, copy it into a hand-owned folder under a fresh hand id; never
    edit the generated file.
  - Output is a pure function of this file: no timestamps, no environment.
    `--check` regenerates in memory and byte-compares against the committed
    tree (run_test.py runs it in CI), so hand-edits to generated files are
    rejected — edit the generator and regenerate instead.
  - Case ids are positional (allocation order inside each folder block). A
    matrix change renumbers ids after the edit point; result history is keyed
    per sweep, and cross-sweep tracking belongs to hand-pinned cases.

Constraint ledger (why the raw product is filtered — derived from run.py):
  - pause= is consumed only by mode=kill (_kill): a pause needs a held repair
    to observe, so pause multiplies only the kill family.
  - pause=freeze pairs only with RecreateInstanceOnPodRestart: the freeze lift
    downgrades to a plain pause, and plain pause parks policy None's Create
    recovery pass — freeze+None could never recover while paused.
  - fault=podkill victim selection is component-blind (_fault_pick_pod): on
    single-pod in-place engine-only cases the victim is the in-place-updated
    pod itself, whose recreate trips the in-place UID-stability check by
    harness design — those tuples are excluded, not waived.
  - podkill:migrating needs mode=migrate (out of scope here); kill targets
    (leader|worker) exist only on gang topologies; update budgets are dormant
    under a steady-state kill, so the kill family uses canonical budgets only.
  - Cells already pinned by hand-written restart/ cases are excluded
    (HAND_PINNED_KILL) so the corpus grows coverage instead of echoing pins.
  - crashloop breaks an engine pod, so its coordination axis rolls engine and
    decoder together only where a decoder exists (pd, pd-mn): co=none and
    co=rolling. maxUnavailable=0 is a SurgeThenDrain budget only: a
    drain-first strategy can never roll under it. Percent budgets run at the
    mid size, where the ceil() rounding differs from the integer case. Both
    restart policies run under both crash forms: a crash at start reaches the
    crash-loop repair under every policy, a crash after Ready reaches the
    runner-restart trigger only under RecreateInstance, and the roll must win
    over each.

The t=3 covering fill over the deferred dimensions (coordination/rollout
groups, canary promotion, budget x coordination interactions) is the --fill
knob: documented, deliberately not emitted until this lane has baked in.
"""
import argparse
import math
import subprocess
import sys
from pathlib import Path

import gen_cases as lib   # single source of truth for YAML body rendering

HERE = Path(__file__).parent.resolve()
ISVCS = HERE / "isvcs"

# Generator-owned folders and their id blocks (inclusive start, exclusive end).
GEN_BLOCKS = {
    "gen-rollout": (1000, 1200),
    "gen-fault": (1200, 1600),
    "gen-restart": (1600, 2000),
    "gen-crashloop": (2000, 2400),
}
GEN_ID_LO = min(lo for lo, _hi in GEN_BLOCKS.values())
GEN_ID_HI = max(hi for _lo, hi in GEN_BLOCKS.values())
MANIFEST_REL = "MANIFEST.md"
# Every generated description starts with this mark; it is what tells a
# generated file in a gen-* folder from a hand-written one.
GEN_MARK = "[gen]"

RP_FULL = {"None": "None", "RecreateInstance": "RecreateInstanceOnPodRestart"}
RESTARTS = ["None", "RecreateInstance"]
PHASES = ["settling", "rotating"]
PAUSES = ["", "true", "freeze"]
IN_PLACE = {"InPlaceIfPossible", "InPlaceOnly"}

# The crashloop family: how the old-revision pod fails, the budgets each
# strategy is crossed with (maxSurge, maxUnavailable), both restart policies,
# the shapes (one gang width per topology; the crash is on the leader, so the
# width adds nothing), and the coordination axis per topology.
CRASH_FORMS = ["start", "load"]
CRASH_BUDGETS = {
    "SurgeThenDrain": [(1, 0), (1, 1), (1, "25%")],
    "RecreatePod": [(0, 1), (0, "25%")],
    "InPlaceIfPossible": [(0, 1), (0, "25%")],
    "InPlaceOnly": [(0, 1), (0, "25%")],
}
CRASH_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1), ("pd-mn", 2, 1)]
CRASH_COORDS = {"engine": ["none"], "engine-mn": ["none"], "pd": ["none", "rolling"], "pd-mn": ["none", "rolling"]}

# Canonical shapes: (topo, N, W). Gangs at N=2 with widths 1 and 2; singles at
# N=4 (pd: engine+decoder N, router lib.NR). Percentage budgets additionally
# run a mid-size variant (MID_SHAPE) so the ceil() rounding actually differs.
SHAPES = [
    ("engine", 4, 0),
    ("pd", 4, 0),
    ("engine-mn", 2, 1),
    ("engine-mn", 2, 2),
    ("pd-mn", 2, 1),
    ("pd-mn", 2, 2),
]
MID_SHAPE = {("engine", 0): 8, ("pd", 0): 8, ("engine-mn", 1): 4, ("pd-mn", 1): 4}

# updateStrategy x budget variants. The first budget of each strategy is its
# canonical one (the only budget the kill family uses); the trailing percent
# budget is the one that gets the mid-size variant in gen-fault.
STRATS = [
    ("SurgeThenDrain", [(1, 0), (2, 0), ("25%", 0)]),
    ("RecreatePod", [(0, 1), (0, 2), (0, "50%")]),
    ("InPlaceIfPossible", [(0, 1), (0, 2), (0, "50%")]),
    ("InPlaceOnly", [(0, 1), (0, 2), (0, "50%")]),
]

# Kill-family cells already pinned by hand-written restart/ cases (size-agnostic:
# the recreate-scope contract does not depend on N). Key:
# (topo, W, strat, restart, pause, target) -> hand case id.
HAND_PINNED_KILL = {
    ("engine", 0, "SurgeThenDrain", "None", "", None): "p130",
    ("engine", 0, "SurgeThenDrain", "RecreateInstance", "", None): "p131",
    ("pd", 0, "SurgeThenDrain", "None", "", None): "p132",
    ("pd", 0, "SurgeThenDrain", "RecreateInstance", "", None): "p133",
    ("engine-mn", 1, "SurgeThenDrain", "RecreateInstance", "", "leader"): "p138",
    ("engine-mn", 1, "SurgeThenDrain", "RecreateInstance", "", "worker"): "p139",
    ("engine-mn", 2, "SurgeThenDrain", "RecreateInstance", "", "worker"): "p141",
    ("engine", 0, "SurgeThenDrain", "RecreateInstance", "true", None): "p148",
    ("engine", 0, "SurgeThenDrain", "None", "true", None): "p149",
    ("engine-mn", 1, "SurgeThenDrain", "RecreateInstance", "true", "worker"): "p150",
    ("engine-mn", 1, "SurgeThenDrain", "RecreateInstance", "freeze", "worker"): "p151",
    ("pd", 0, "SurgeThenDrain", "RecreateInstance", "true", None): "p152",
    ("pd-mn", 1, "SurgeThenDrain", "RecreateInstance", "freeze", "leader"): "p153",
}

# Cells parked on tracked product bugs: cell key -> (issue number, reason,
# invariant scope). A parked cell emits notimpl=1 issue=N and carries the
# reason as its line-2 rationale, so the runners record XFAIL (known bug: #N)
# instead of red and the PARKED DEBT table keeps the debt visible. The scope
# ("A"|"B"|"C"|"all", or "" when the bug fails an ordinary check) additionally
# emits inv_xfail=<scope>: it names the continuous invariant whose violation
# IS the tracked bug, which would otherwise fail the verdict after the
# notimpl softening. This is the ONLY way a generated case goes quiet:
# entries require a filed issue number, and a key that matches no emitted
# cell fails the regen (stale parking is invisible debt). Cell keys are the
# per-family tuples the emitters build:
#   ("rollout", topo, w, strat, surge, unavail)
#   ("podkill", phase, topo, w, size, strat, surge, unavail, rp)
#   ("kill", topo, w, strat, rp, pause, target)
# No generated cell is parked today; a park is added here as
# (key): (issue, reason, invariant scope) and rendered into the header.
PARKED = {}

# Runner verbs/hooks this corpus needs but the runners do not implement yet.
# Kept in the manifest so the follow-up surface stays visible next to the
# corpus it would grow.
MISSING_HOOKS = [
    "kill-instance verb (atomic delete of a whole gang) — today only reachable "
    "as the RecreateInstanceOnPodRestart cascade from one pod death",
    "component-scoped podkill victim (fault=podkill:<phase>:<component>) — "
    "victim selection is component-blind, so PD podkill can never target the "
    "engine at settling, and single-pod in-place engine victims are unreachable",
    "mid-surge injection points beyond first-surge-observed (k-th surge, "
    "mid-drain, pre-settle tail) — FAULT_PHASES has settling/rotating/migrating "
    "only",
    "pause-under-rollout hook (ome.io/rollout-paused is only exercised inside "
    "mode=kill; no verb pauses an in-flight rotation)",
    "decoder-targeted mode=kill (the kill handler always picks the engine when "
    "one is configured)",
]


def is_pd(topo):
    return topo.startswith("pd")


def is_mn(topo):
    return topo.endswith("-mn")


def surge_count(n, s):
    if isinstance(s, str) and s.endswith("%"):
        return math.ceil(n * int(s[:-1]) / 100)
    return s


def budget_tag(strat, surge, unavail):
    v = surge if strat == "SurgeThenDrain" else unavail
    p = "s" if strat == "SurgeThenDrain" else "u"
    return f"{p}{v}".replace("%", "pct")


def shape_tag(topo, w):
    return f"{topo} W{w}" if is_mn(topo) else topo


GEN_HDR_ORDER = ["mode", "Ne", "Nd", "Nr", "ceil_e", "ceil_d", "ceil_r",
                 "restart", "kill", "pause", "fault", "floor_soft", "order_soft",
                 "co", "topo"]


def header(name, desc, kv):
    parts = [f"{k}={kv[k]}" for k in GEN_HDR_ORDER if k in kv]
    parts += [f"{k}={v}" for k, v in kv.items() if k not in GEN_HDR_ORDER]
    return f"# {name}: {desc}  " + " ".join(parts)


def counts(topo, n):
    if is_pd(topo):
        return dict(Ne=n, Nd=n, Nr=lib.NR)
    return dict(Ne=n, Nd=0, Nr=0)


def ceilings(topo, n, strat, surge):
    ce = n + surge_count(n, surge) if strat == "SurgeThenDrain" else n
    if is_pd(topo):
        return dict(ceil_e=ce, ceil_d=n, ceil_r=lib.NR)
    return dict(ceil_e=ce, ceil_d=0, ceil_r=0)


def comps(topo, n, w, e_life, d_life=None, r_life=None):
    cw = w if is_mn(topo) else 0
    cs = [lib.comp("engine", n, cw, e_life)]
    if is_pd(topo):
        cs.append(lib.comp("decoder", n, cw, d_life if d_life is not None else lib.lifecycle()))
        cs.append(lib.comp("router", lib.NR, 0, r_life if r_life is not None else lib.lifecycle()))
    return cs


def case_yaml(name, desc, kv, case_comps, reason, coord=None):
    lines = [header(name, desc, kv), f"# reason: {reason}"]
    return "\n".join(lines) + "\n" + lib.render(name, case_comps, coord=coord)


# ── gen-rollout: failure=none baselines ─────────────────────────────────────
def emit_rollout(add):
    for topo, n, w in SHAPES:
        for strat, budgets in STRATS:
            for surge, unavail in budgets:
                kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge),
                          co="none", topo=topo)
                meta = dict(strat=strat, verb="none")
                desc = (f"{GEN_MARK} rollout {shape_tag(topo, w)} N={n} {strat} "
                        f"{budget_tag(strat, surge, unavail)}")
                if strat == "SurgeThenDrain":
                    reason = (f"plain bump: rolls within the surge budget "
                              f"(peak <= {kv['ceil_e']}), never below N ready")
                elif strat == "RecreatePod":
                    reason = ("plain bump: drain-first roll bounded by the "
                              "unavailable budget, no surge above N")
                elif is_mn(topo):
                    reason = ("plain bump: gang in-place falls back to whole-gang "
                              "recreate within the unavailable budget, no surge")
                else:
                    reason = ("plain bump: annotation change patches pods in "
                              "place (UIDs stable), no surge above N")
                if is_pd(topo):
                    reason += "; decoder/router are negative controls (co=none)"
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
                add("gen-rollout", desc, kv, comps(topo, n, w, life), reason, meta,
                    cell=("rollout", topo, w, strat, surge, unavail))


# ── gen-fault: podkill during a rollout ─────────────────────────────────────
def _podkill_waivers(topo, strat, phase):
    """floor_soft/order_soft per the deterministic _fault_pick_pod victim.
    PD topologies never need one: the victim is a decoder pod (or, mid-rotation
    under a surge strategy, an engine new-revision pod), so the bumped engine's
    floor/order checks are untouched."""
    if is_pd(topo):
        return {}
    if topo == "engine":
        if strat == "SurgeThenDrain":
            return {} if phase == "rotating" else dict(floor_soft=1, order_soft=1)
        if strat == "RecreatePod":
            return {} if phase == "rotating" else dict(floor_soft=1)
        return {}
    # engine-mn: the victim is always a settled gang leader.
    if strat == "SurgeThenDrain":
        return dict(floor_soft=1, order_soft=1)
    return dict(floor_soft=1)


def _podkill_victim(topo, strat, phase):
    if topo == "engine-mn":
        return "the lowest settled gang's leader"
    if topo == "pd-mn":
        return "the decoder gang leader (component-blind, name-sorted victim)"
    if topo == "engine":
        if phase == "rotating" and strat in ("SurgeThenDrain", "RecreatePod"):
            return "the newest new-revision pod"
        return "a settled pod"
    # pd single-pod
    if phase == "rotating" and strat in ("SurgeThenDrain", "RecreatePod"):
        return "the newest engine new-revision pod"
    return "a decoder pod (component-blind, name-sorted victim)"


def _podkill_reason(topo, strat, phase, waivers):
    victim = _podkill_victim(topo, strat, phase)
    if is_pd(topo) and "decoder" in victim:
        out = (f"kill {victim}: repair must stay isolated (decoder never "
               f"surges/rotates) while the engine roll converges")
    elif "new-revision" in victim:
        out = (f"kill {victim}: the controller replaces it and the roll still "
               f"converges; settled capacity is untouched")
    else:
        out = f"kill {victim}: repair overlaps the in-flight roll, both must complete"
    if waivers.get("floor_soft"):
        out += ("; floor waived — the kill itself costs settled capacity the "
                "update budget cannot absorb while the repair restarts")
    if waivers.get("order_soft"):
        out += "; order waived — the fault-induced deletion is a drain with no preceding surge"
    return out


def emit_fault(add):
    for topo, n, w in SHAPES:
        mid = MID_SHAPE.get((topo, w))
        for strat, budgets in STRATS:
            if topo == "engine" and strat in IN_PLACE:
                continue   # constraint ledger: in-place single-pod engine victim
            for surge, unavail in budgets:
                pct = isinstance(surge, str) or isinstance(unavail, str)
                sizes = [n] + ([mid] if pct and mid else [])
                for size in sizes:
                    for phase in PHASES:
                        for rp in RESTARTS:
                            waivers = _podkill_waivers(topo, strat, phase)
                            kv = dict(**counts(topo, size),
                                      **ceilings(topo, size, strat, surge),
                                      fault=f"podkill:{phase}", **waivers,
                                      co="none", topo=topo)
                            meta = dict(strat=strat, verb=f"podkill:{phase}", rp=rp)
                            desc = (f"{GEN_MARK} podkill {phase} {shape_tag(topo, w)} "
                                    f"N={size} {strat} "
                                    f"{budget_tag(strat, surge, unavail)} rp {rp}")
                            reason = _podkill_reason(topo, strat, phase, waivers)
                            life = lib.lifecycle(strat=strat, surge=surge,
                                                 unavail=unavail,
                                                 restart=RP_FULL[rp])
                            d_life = (lib.lifecycle(restart=RP_FULL[rp])
                                      if is_pd(topo) else None)
                            add("gen-fault", desc, kv,
                                comps(topo, size, w, life, d_life=d_life),
                                reason, meta,
                                cell=("podkill", phase, topo, w, size, strat,
                                      surge, unavail, rp))


# ── gen-restart: steady-state kill x pause x strategy ───────────────────────
def _kill_reason(topo, rp, pause, target):
    scope = ("the whole instance (leader+workers) recreates"
             if rp == "RecreateInstance" and is_mn(topo)
             else "exactly the killed pod is recreated"
             if rp == "None" and is_mn(topo)
             else "the instance (one pod) recreates")
    tgt = f" from a dead {target}" if target else ""
    if pause == "":
        return f"steady-state kill{tgt}: {scope}, scoped to the killed instance, fleet returns to N"
    if pause == "true" and rp == "RecreateInstance":
        return (f"pause suspends fleet changes, not the RestartPolicy: {scope}{tgt} "
                f"while rollout-paused stays set")
    if pause == "true":
        return (f"plain pause parks the Create recovery pass: holds degraded{tgt} "
                f"through the hold window, recovers only after unpause")
    return (f"freeze holds every repair{tgt}; downgrading to a plain pause must "
            f"repair without ever unpausing")


def emit_restart(add):
    for topo, n, w in SHAPES:
        for strat, budgets in STRATS:
            surge, unavail = budgets[0]   # canonical budget: dormant under a kill
            for pause in PAUSES:
                for rp in RESTARTS:
                    if pause == "freeze" and rp == "None":
                        continue   # constraint ledger: freeze lift needs RecreateInstance
                    targets = ["leader", "worker"] if is_mn(topo) else [None]
                    for target in targets:
                        if (topo, w, strat, rp, pause, target) in HAND_PINNED_KILL:
                            continue
                        kv = dict(mode="kill", **counts(topo, n),
                                  ceil_e=n, ceil_d=(n if is_pd(topo) else 0),
                                  ceil_r=(lib.NR if is_pd(topo) else 0),
                                  restart=rp)
                        if target:
                            kv["kill"] = target
                        if pause:
                            kv["pause"] = pause
                        kv.update(co="none", topo=topo)
                        meta = dict(strat=strat, verb="kill", rp=rp,
                                    pause=pause or "off")
                        strat_note = "" if strat == "SurgeThenDrain" else f" ({strat} dormant)"
                        desc = (f"{GEN_MARK} kill {shape_tag(topo, w)} N={n} {strat} "
                                f"rp {rp}"
                                + (f" target {target}" if target else "")
                                + (f" pause {pause}" if pause else "")
                                + strat_note)
                        reason = _kill_reason(topo, rp, pause, target)
                        life = lib.lifecycle(strat=strat, surge=surge,
                                             unavail=unavail, restart=RP_FULL[rp])
                        d_life = (lib.lifecycle(restart=RP_FULL[rp])
                                  if is_pd(topo) else None)
                        add("gen-restart", desc, kv,
                            comps(topo, n, w, life, d_life=d_life), reason, meta,
                            cell=("kill", topo, w, strat, rp, pause, target))


# ── gen-crashloop: one old-revision Instance crashes while the roll lands ───
def _crash_reason(form, strat, co):
    how = ("one old-revision engine pod is pointed at an image with no shell as the bump "
           "lands, so it crashes at every start and is never Ready again"
           if form == "start" else
           "one old-revision engine pod has its runner killed every few seconds while it "
           "is up, so it dies after Ready and comes back between crashes")
    lands = {"SurgeThenDrain": "the surge brings the fixed replacement up beside it and drains it",
             "RecreatePod": "the recreate drains it and rebuilds the Instance at the fixed revision",
             "InPlaceIfPossible": "the patch lands the fixed image in place",
             "InPlaceOnly": "the patch lands the fixed image in place"}[strat]
    out = (f"{how}; its Instance is off the target from that moment, so the roll owns it: "
           f"{lands}, admitted within the same budget as its peers, and no repair is recorded "
           f"for it after the bump; every Instance ends on the new revision")
    if co != "none":
        out += "; the group paces engine and decoder together as on any coordinated roll"
    out += "; floor waived — the crash itself costs settled capacity for the length of the roll"
    return out


def emit_crashloop(add):
    for topo, n, w in CRASH_SHAPES:
        mid = MID_SHAPE.get((topo, w))
        for strat, budgets in CRASH_BUDGETS.items():
            for surge, unavail in budgets:
                size = mid if (isinstance(unavail, str) and mid) else n
                for rp, form in ((rp, form) for rp in RESTARTS for form in CRASH_FORMS):
                    for co in CRASH_COORDS[topo]:
                        kv = dict(**counts(topo, size), **ceilings(topo, size, strat, surge),
                                  fault="crashloop:old", form=form, floor_soft=1,
                                  co=co, topo=topo)
                        if co != "none":
                            # The group rolls the decoder too, on its own surge.
                            kv["ceil_d"] = size + 1
                        meta = dict(strat=strat, verb=f"crashloop:{form}", rp=rp)
                        desc = (f"[gen] crashloop {form} {shape_tag(topo, w)} N={size} {strat} "
                                f"{budget_tag(strat, surge, unavail)} rp {rp} co {co}")
                        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail,
                                             restart=RP_FULL[rp])
                        d_life = (lib.lifecycle(restart=RP_FULL[rp])
                                  if is_pd(topo) else None)
                        add("gen-crashloop", desc, kv,
                            comps(topo, size, w, life, d_life=d_life),
                            _crash_reason(form, strat, co), meta,
                            cell=("crashloop", form, topo, w, size, strat, surge, unavail, rp, co),
                            coord=lib.coord_for(co) if co != "none" else None)


# ── assembly ─────────────────────────────────────────────────────────────────
def build_outputs():
    """{relative path under isvcs/: file text} — the entire owned output set."""
    outputs = {}
    counters = {f: lo for f, (lo, _hi) in GEN_BLOCKS.items()}
    rows = []   # (folder, case id, kv, meta) for the manifest
    parked_used = set()

    def add(folder, desc, kv, case_comps, reason, meta, cell, coord=None):
        lo, hi = GEN_BLOCKS[folder]
        cid = counters[folder]
        if cid >= hi:
            sys.exit(f"{folder}: id block exhausted at p{cid} (block ends before p{hi})")
        counters[folder] += 1
        name = f"p{cid}"
        if cell in PARKED:
            parked_used.add(cell)
            issue, parked_reason, inv_scope = PARKED[cell]
            kv = dict(kv, notimpl=1, issue=issue)
            if inv_scope:
                kv["inv_xfail"] = inv_scope
            reason = parked_reason
            meta = dict(meta, parked=issue)
        outputs[f"{folder}/{name}.yaml"] = case_yaml(name, desc, kv, case_comps, reason, coord)
        rows.append((folder, name, dict(kv), dict(meta)))

    emit_rollout(add)
    emit_fault(add)
    emit_restart(add)
    emit_crashloop(add)
    stale = set(PARKED) - parked_used
    if stale:
        sys.exit("PARKED keys match no emitted cell (stale after a matrix change): "
                 + "; ".join(repr(k) for k in sorted(stale)))
    outputs[MANIFEST_REL] = manifest(rows)
    return outputs, rows


def manifest(rows):
    def tally(key):
        t = {}
        for _f, _n, kv, meta in rows:
            k = key(kv, meta)
            t[k] = t.get(k, 0) + 1
        return t

    def table(title, t):
        out = [f"### {title}", "", "| value | cases |", "|---|---|"]
        out += [f"| {k} | {v} |" for k, v in sorted(t.items())]
        return out + [""]

    per_folder = {}
    id_range = {}
    for f, n, _kv, _m in rows:
        per_folder[f] = per_folder.get(f, 0) + 1
        lo, hi = id_range.get(f, (n, n))
        id_range[f] = (min(lo, n, key=lambda x: int(x[1:])),
                       max(hi, n, key=lambda x: int(x[1:])))

    lines = [
        "# Generated hot-core corpus",
        "",
        "Emitted by `generate_cases.py` — regenerate with"
        " `python3 generate_cases.py`; CI rejects hand edits via"
        " `python3 generate_cases.py --check` (run_test.py).",
        "The gen-* folders are generator-owned wholesale; promote a bug-finding"
        " case to a hand-owned folder under a fresh id instead of editing it here.",
        "",
        f"Total generated cases: **{len(rows)}**. The whole-corpus per-PR cap"
        " (hand + generated) is enforced by run_test.py, not baked in here.",
        "",
        "## Folders",
        "",
        "| folder | cases | id range |",
        "|---|---|---|",
    ]
    for f in GEN_BLOCKS:
        if f in per_folder:
            lo, hi = id_range[f]
            lines.append(f"| {f} | {per_folder[f]} | {lo}..{hi} |")
    lines += [""]
    lines += table("By topology", tally(lambda kv, m: kv["topo"]))
    lines += table("By update strategy", tally(lambda kv, m: m["strat"]))
    lines += table("By failure verb / phase", tally(lambda kv, m: m["verb"]))
    lines += table("By restartPolicy", tally(lambda kv, m: m.get("rp", "(unset — baseline)")))
    lines += table("By pause", tally(lambda kv, m: m.get("pause", "off")))
    lines += table("By coordination", tally(lambda kv, m: kv.get("co", "none")))
    parked_rows = [(f, n, m["parked"]) for f, n, _kv, m in rows if "parked" in m]
    if parked_rows:
        lines += ["### Parked cells (XFAIL on a tracked issue)", "",
                  "| case | folder | issue |", "|---|---|---|"]
        lines += [f"| {n} | {f} | #{i} |" for f, n, i in parked_rows]
        lines += [""]
    lines += [
        "## Constraint ledger (tuples excluded during construction)",
        "",
        "- `pause` multiplies only `mode=kill`: run.py consumes the pause header"
        " nowhere else, so a paused rollout case would carry a dead knob.",
        "- `pause=freeze` pairs only with `RecreateInstanceOnPodRestart`: the"
        " freeze lift downgrades to a plain pause, which parks policy None's"
        " Create recovery — freeze+None could never repair while paused.",
        "- In-place strategies on single-pod engine-only topologies are excluded"
        " from gen-fault: the component-blind podkill victim is the in-place"
        " pod itself and its recreate trips the UID-stability check by harness"
        " design (a missing component-scoped victim hook, see below).",
        "- `fault=podkill:migrating` needs `mode=migrate` — out of scope here.",
        "- Kill targets (`kill=leader|worker`) exist only on gang topologies.",
        "- Update budgets are dormant under a steady-state kill: the kill family"
        " permutes strategies at their canonical budget only.",
        "- crashloop breaks an engine pod, so its coordination axis (co=none |"
        " co=rolling) exists only on the PD topologies; maxUnavailable=0 is"
        " emitted for SurgeThenDrain only, percent budgets run at the mid"
        " size where the ceil() rounding differs from the integer case, and"
        " both restart policies run under both crash forms.",
        "- Kill-family cells pinned by hand-written restart/ cases are excluded:",
        "",
    ]
    for key, pin in sorted(HAND_PINNED_KILL.items(), key=lambda x: x[1]):
        topo, w, strat, rp, pause, target = key
        cell = f"{topo} W{w}" if w else topo
        lines.append(f"  - {pin}: {cell} {strat} rp={rp}"
                     + (f" pause={pause}" if pause else "")
                     + (f" target={target}" if target else ""))
    lines += [
        "",
        "## Missing runner verbs/hooks (future coverage, not emitted)",
        "",
    ]
    lines += [f"- {h}" for h in MISSING_HOOKS]
    lines += [
        "",
        "## Deferred fill",
        "",
        "`--fill` (t=3 covering array over coordination groups, canary"
        " promotion, and budget interactions) is documented in the generator"
        " and intentionally not emitted until the hot core has baked in the"
        " per-PR lane.",
        "",
    ]
    return "\n".join(lines)


# ── modes ────────────────────────────────────────────────────────────────────
def hand_written(path):
    """A yaml in a gen-* folder is hand-written when line 1 lacks the mark."""
    with open(path) as f:
        return not f.readline().startswith(f"# {path.stem}: {GEN_MARK} ")


def owned_files_on_disk(root=ISVCS):
    files = {}
    for folder in GEN_BLOCKS:
        d = Path(root) / folder
        if d.exists():
            for f in sorted(d.glob("*.yaml")):
                files[f"{folder}/{f.name}"] = f
    mf = Path(root) / MANIFEST_REL
    if mf.exists():
        files[MANIFEST_REL] = mf
    return files


def check(outputs, root=ISVCS):
    disk = owned_files_on_disk(root)
    problems = []
    for rel, text in sorted(outputs.items()):
        if rel not in disk:
            problems.append(f"missing on disk (regenerate): isvcs/{rel}")
        elif rel.endswith(".yaml") and hand_written(disk[rel]):
            problems.append(f"hand-written case at a generated id (the regen refuses to run "
                            f"over it; move it to a hand-owned folder): isvcs/{rel}")
        elif disk[rel].read_text() != text:
            problems.append(f"drifted from generator output (hand-edited?): isvcs/{rel}")
    for rel in sorted(set(disk) - set(outputs)):
        if rel.endswith(".yaml") and hand_written(disk[rel]):
            problems.append(f"hand-written case in a generator-owned folder (the regen refuses "
                            f"to run over it; move it to a hand-owned folder): isvcs/{rel}")
        else:
            problems.append(f"stale generated file (regenerate to sweep it): isvcs/{rel}")
    if problems:
        for p in problems:
            print(p, file=sys.stderr)
        print(f"\ngenerate_cases.py --check: FAIL — {len(problems)} problem(s); "
              f"run `python3 generate_cases.py` and commit the result",
              file=sys.stderr)
        return 1
    print(f"generate_cases.py --check: OK — {len(outputs)} generated file(s) byte-stable")
    return 0


def guard_foreign(outputs, root=ISVCS):
    """Refuse to run over anything that is not this generator's: a yaml in a
    gen-* folder without the `[gen]` mark (hand-written, whatever its id —
    never overwritten, never swept), and a generated id used by a case
    outside the gen-* folders. Exits before anything is written."""
    problems = []
    for folder in GEN_BLOCKS:
        d = Path(root) / folder
        for f in sorted(d.glob("*.yaml")) if d.exists() else []:
            if hand_written(f):
                problems.append(f"{f}: hand-written case in a generator-owned folder — move "
                                f"it to a hand-owned folder; nothing was written or swept")
    owned_ids = {Path(rel).stem for rel in outputs if rel.endswith(".yaml")}
    for f in sorted(Path(root).glob("**/*.yaml")):
        if f.parent.name in GEN_BLOCKS:
            continue
        if f.stem in owned_ids:
            problems.append(f"{f}: case id {f.stem} collides with the generated id block — "
                            f"renumber the hand-written case out of p{GEN_ID_LO}-p{GEN_ID_HI - 1}")
    if problems:
        sys.exit("\n".join(problems))


def write(outputs, root=ISVCS):
    guard_foreign(outputs, root)
    for folder in GEN_BLOCKS:
        d = Path(root) / folder
        d.mkdir(parents=True, exist_ok=True)
        for f in sorted(d.glob("*.yaml")):
            if f"{folder}/{f.name}" not in outputs:
                f.unlink()   # stale: the guard proved every file here carries the mark
    for rel, text in sorted(outputs.items()):
        path = Path(root) / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true",
                    help="regenerate in memory and byte-compare against the "
                         "committed tree; nonzero on any drift")
    ap.add_argument("--fill", action="store_true",
                    help="t=3 covering fill over the deferred dimensions "
                         "(documented follow-up; refuses to emit this round)")
    args = ap.parse_args()
    if args.fill:
        print("--fill: the t=3 covering fill (coordination groups, canary "
              "promotion, budget interactions) is deliberately not emitted "
              "until the hot core has baked in the per-PR lane.", file=sys.stderr)
        return 2

    outputs, rows = build_outputs()
    if args.check:
        return check(outputs)

    write(outputs)
    per_folder = {}
    for f, n, _kv, _d in rows:
        per_folder.setdefault(f, []).append(n)
    print(f"generated {len(rows)} cases:")
    for f in GEN_BLOCKS:
        names = per_folder.get(f, [])
        if names:
            print(f"  {f:12s} {len(names):3d}  {names[0]}..{names[-1]}")
    # A regen must never reintroduce a duplicate/shadowed case id or a
    # malformed header — fail the generation, not the next harness run.
    rc = subprocess.run([sys.executable, str(HERE / "run.py"), "--validate-only"]).returncode
    if rc:
        sys.exit(rc)
    return 0


if __name__ == "__main__":
    sys.exit(main())
