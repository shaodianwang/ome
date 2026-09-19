#!/usr/bin/env python3
"""run.py <case_id> | run.py --validate-only [isvcs_dir]

Watcher-driven test runner for the OMENative rollout playground.
--validate-only checks the whole case tree (id uniqueness + line-1 header
schema) with zero cluster interaction and exits nonzero on any problem.

Architecture:
  1. Apply the ISVC; read its (post-defaulting) spec: per-Component update
     strategy + the spec.rollout.groups (OEP-0017 v2; the harness derives a
     synthetic policy / members / order / soak from the group shapes).
  2. Start `kubectl get pod -w -o json --output-watch-events`; read events line-
     by-line, maintaining a per-pod state map (comp, alive, ready, hash, uid,
     newanno). Compute per-Component (alive, ready, updated) counts.
  3. State machine:
       INIT      → wait until every configured Component has N complete
                   Instances Ready+serving and its IR reports a settled revision.
       (trigger) → snapshot OLD revision hashes + pod UIDs, then bump the
                   `perm.test/rev` annotation on EVERY group member (engine-only
                   when there is no coordination group). → ROTATING
       ROTATING  → wait until every Component is back at N alive AND N ready, AND
                   every BUMPED Component has all N pods on the new revision,
                   sustained STABILITY_S.
       CONVERGED → stop watcher, emit verdict.
  4. Throughout: track peak alive per Component, and per-Component first-change /
     fully-rotated timestamps (for Sequential ordering + soak verification).
  5. Throughout (every mode, whole case duration): continuous invariants —
     (A) status-truth: no IR Instance claims Failed while all its pods serve, or
     Ready with no pods; (B) endpoints-membership: every serving pod is a Ready
     endpoint of its per-revision routed Service and no endpoint references a
     deleted pod; (C) pair-alive (armed only by mode=pairing for a non-empty→
     non-empty protocol transition): whenever both engine and decoder serve, a
     pairable pair exists. Sustained violations FAIL the verdict (INV_*).
  6. Optional fault injection (header fault=<kind>:<param>): quota:<n> (derived
     ResourceQuota admitting exactly +n case-shaped pods), podkill:<phase>,
     ctrlrestart:<phase> (gated on OME_TEST_ALLOW_CTRL_RESTART=1), cordon:source.
     Apparatus errors record the distinct INFRA-FAIL verdict — harness
     infrastructure broke, not the product.

Why bump ALL group members (the 2026-06-01 fix):
  OMENative coordination is a status-only layer + *restrictive* gates. It never
  *initiates* a roll on a Component whose template did not change; each Component
  rolls off its OWN revision hash. The contract (OEP-0007.1) is that the operator
  bumps every member's template and coordination then sequences/paces/flips the
  resulting per-member rolls. Bumping only the engine therefore rolls only the
  engine — by design. To exercise coordination at all, the trigger must perturb
  every member. (The earlier "decoder never surged ⇒ coordination broken" verdict
  was a harness false positive, not a controller bug.)

Update detection per strategy:
  * surge/recreate (SurgeThenDrain / RecreatePod): a pod is "updated" once it
    carries a revision-hash NOT present pre-trigger (a brand-new pod).
  * in-place (InPlaceIfPossible / InPlaceOnly): an annotation diff is patched
    onto the LIVE pod — same UID, same revision-hash label — so hash-watching is
    blind to it. Instead a pod is "updated" once it carries the new
    `perm.test/rev=v2` annotation, and we separately assert its UID never rotated.
"""
import json
import os
import queue
import re
import select
import subprocess
import sys
import threading
import time
import uuid
from collections import defaultdict
from pathlib import Path

import irstatus
import runtime_fixtures

# Env-driven so the harness isn't pinned to one cluster (see batch.py).
KUBECONFIG = os.environ.get("OME_TEST_KUBECONFIG") or os.environ.get("KUBECONFIG") or os.path.expanduser("~/.kube/config")
# Shared per-run stamp: all of a run's results live under results/<RUN_TS>/.
# batch.py exports OME_RUN_TS so its parallel subprocesses share one run folder;
# a standalone run.py generates its own.
RUN_TS = os.environ.get("OME_RUN_TS") or time.strftime("%Y%m%d-%H%M%S")
NS = os.environ.get("OME_TEST_NS", "sandbox")
HERE = Path(__file__).parent.resolve()
REV_LABEL = "ome.io/revision-hash"   # OMENative per-revision pod label (query.LabelRevisionHash)
PAIRING_LABEL = "ome.io/pairing-protocol"  # P/D pairing cohort label (query.LabelPairingProtocol):
                                           # on pods (render + in-place restamp) and per-revision
                                           # routing Services (metadata only — never the selector)
TRIGGER_ANNO = "perm.test/rev"       # annotation we bump to trigger a rollout
TRIGGER_VAL = "v2"
PROMOTE_ANNO = "ome.io/rollout-promote"    # canary manual-promote verb (value = canary revision hash)
ROLLBACK_ANNO = "ome.io/rollout-rollback"  # canary rollback verb (drain to stable + sticky hold)
STABILITY_S = 5     # require target sustained this many seconds before CONVERGED
SEQ_TOL_S = 6       # ordering/soak slack (1Hz sampling + readiness jitter)
GANG_TOL_S = 5.0    # multi-node gang grace: a leader+worker Instance may look mixed-revision
                    # only this briefly during a fast gang flip; sustained longer = piecemeal roll
MINREADY_GRACE_S = 3.0  # skip the first few s after trigger (INIT→ROTATING boundary) before
                        # tracking min ready Instances vs the N - maxUnavailable capacity floor
RATIO_EPS = 0.10    # RatioBalanced band grace for 1Hz sampling jitter + surge transients
RATIO_SUSTAIN_S = 5.0  # assert the worst ratio deviation that PERSISTED this long, not the
                       # instantaneous spike. The gate is eventually-consistent (engine/decoder
                       # on separate dispatchers, peer status lags a reconcile), so when both move
                       # opposite directions a bounded ~1-2s cross-dispatcher overshoot is by-design
                       # and self-corrects. Only an off-band deviation held >= this window is real.
FLOOR_SUSTAIN_S = 5.0  # capacity-floor debounce (mirrors RATIO_SUSTAIN_S): the hard floor check
                       # fails only on a below-floor dip that PERSISTED this long — a single-sample
                       # readiness flap at 1Hz must not red a case. floor_hard=1 skips the debounce.
# Migration source-drain settle budget: mult-scaled + per-migrated-instance,
# defined below alongside the other TIMEOUT_MULT budgets (needs TIMEOUT_MULT).
TEARDOWN_BASE_S, TEARDOWN_PER_POD_S = 120, 5   # post-delete teardown-convergence budget
TEARDOWN_POLL_S = 5.0

# ── Continuous invariants (status-truth + endpoints-membership) ──────────────
# Polled for the WHOLE duration of every case, in every mode. API-load bound
# (shared cluster): the poller adds at most 3 label-selected namespaced GETs
# per tick per running case — one InferenceReplica list (invariant A), one
# Service list + one EndpointSlice list (invariant B). Pod state is reused
# from the watcher (no extra pod reads). At INV_TICK_S=5 that is 0.6 req/s
# per case, ~5 req/s at batch conc-8 — the same order as the existing status
# polls (canary 0.5/s, migrate ledger 0.2/s).
INV_TICK_S = 5.0
# Sustain counts, in CONSECUTIVE tick observations — the transient carve-out:
#   A (status-truth): 2 observations = the condition spanned >= 1 full tick
#     interval (>= 5s; fires 5-10s after onset depending on tick phase), so a
#     just-transitioned phase — read while the controller is mid-flip — can
#     never fire off a single stale sample.
#   B missing-endpoint: 3 observations = the gap spanned >= 2 tick intervals
#     (>= (3-1)*5 = 10s; fires 10-15s after onset). Endpoint controllers lag
#     pod readiness legitimately (slice mirror + kube-proxy churn, normally
#     1-2s); any propagation lag < 10s cannot span 3 consecutive ticks.
#   B stale-endpoint (deleted pod UID still referenced): 2 observations
#     (5-10s), same arithmetic as A — slice purge is near-immediate.
INV_SUSTAIN_A = 2
INV_SUSTAIN_B_MISSING = 3
INV_SUSTAIN_B_STALE = 2
# C pair-alive: 2 observations (>= 1 full tick interval), same arithmetic as A.
# A REAL pair break lasts at least one pod-startup cycle (the new cohort must
# come Ready before a pair can re-form), so it always spans 2+ ticks; a
# single-sample race between a label restamp and a serving flip cannot.
INV_SUSTAIN_C = 2
# IR Instance phases exempt from ALL status-truth checks: transitional by
# definition. A Migrating source keeps serving while its replacement surges;
# Updating/Restarting/Creating/Deleting all legitimately hold pods in states
# that would read as a lie if sampled mid-transition.
INV_TRANSIENT_PHASES = {"Pending", "Creating", "Updating", "Restarting", "Migrating", "Deleting"}
SERVING_COND = "ome.io/serving"   # controller-managed pod readiness gate (query.ServingConditionType)

# Dynamic timeouts: real kubelet + a 15s artificial startup sleep per pod, and
# surge=1 rolls serialize. Static 240s was too tight for N=8 and for two-member
# Sequential rolls (decoder fully, soak, then engine).
# OME_TEST_TIMEOUT_MULT scales every init/rotate budget for a loaded cluster
# where scheduling + CPU-throttled startup drag past the default budgets (a
# busy shared cluster can take 3-4x longer to schedule N=8 than an idle one).
# Default 1.0; set e.g. 2.5 when a smoke run shows INIT_TIMEOUT dominated by
# slow scheduling rather than product non-convergence.
try:
    TIMEOUT_MULT = max(1.0, float(os.environ.get("OME_TEST_TIMEOUT_MULT", "1.0")))
except ValueError:
    TIMEOUT_MULT = 1.0
INIT_BASE_S, INIT_PER_POD_S = int(120 * TIMEOUT_MULT), int(25 * TIMEOUT_MULT)
# Sequential 2-member rolls serialize (decoder fully, then engine) and under
# conc-8 cluster load creep to ~400s for 8 bumped pods; 40s/pod (=410s budget)
# left no headroom for the trailing STABILITY_S window (p34 converged at 406s
# but timed out at 410s before "stable"). 60s/pod gives comfortable margin.
ROTATE_BASE_S, ROTATE_PER_POD_S = int(180 * TIMEOUT_MULT), int(100 * TIMEOUT_MULT)
MAX_BUDGET_S = int(1800 * TIMEOUT_MULT)
# kill-mode pause handling: settle after annotating (annotation → IR
# projection → workload input), and the no-recovery hold window a paused
# case must survive before the lift step.
PAUSE_PROJECT_S = int(15 * TIMEOUT_MULT)
KILL_PAUSE_HOLD_S = int(60 * TIMEOUT_MULT)
# Post-Completed source-drain grace. A source can't drain until its surge
# replacement is Ready (~one pod-init cycle), and multi-instance migrations
# SERIALIZE on a surge budget of 1 (reusing freed indices), so the drain is
# O(migrated-instances), not flat. Budget = base + per-instance, both
# mult-scaled — mirrors the INIT/ROTATE budget shape. A flat 90s spuriously
# reds every 2+-instance migration on a loaded cluster even though the ledger
# records each migration Completed correctly.
MIGRATE_SETTLE_BASE_S, MIGRATE_SETTLE_PER_INST_S = int(90 * TIMEOUT_MULT), int(90 * TIMEOUT_MULT)


def migrate_settle_budget(n_insts):
    """Source-drain settle ceiling for a migration of n_insts instances."""
    return min(MAX_BUDGET_S, MIGRATE_SETTLE_BASE_S + MIGRATE_SETTLE_PER_INST_S * max(1, n_insts))

IN_PLACE_TYPES = {"InPlaceIfPossible", "InPlaceOnly"}
COMPS = ("engine", "decoder", "router")

TSV_HEADER = ("id\tdesc\tNe\tNd\tNr\tceil_e\tceil_d\tceil_r\tpeak_e\tpeak_d\tpeak_r\t"
              "final_e\tfinal_d\tfinal_r\tconv_secs\tpolicy\tverdict\tnote\tnew_e\tnew_d\tnew_r\n")


def kubectl(*args, env=None, **kw):
    env_full = {**os.environ, "KUBECONFIG": KUBECONFIG}
    if env:
        env_full.update(env)
    return subprocess.run(["kubectl", *args], capture_output=True, text=True, env=env_full, **kw)


def apply_runtime_fixtures():
    """Apply the runtime fixtures into NS, idempotently.

    They are templates rather than literal YAML: the PVC, its seed Job and the
    ClusterBaseModel's storageUri all have to name NS, because a pod cannot
    mount a PVC from another namespace and the ISVC validator rejects a model
    that points at one.
    """
    return kubectl("apply", "--validate=false", "-n", NS, "-f", "-", check=False,
                   input=runtime_fixtures.render(kind=runtime_fixtures.kind_from_env()))


def parse_header(path):
    """First line of YAML has comment header with Ne=N Nd=N Nr=N ceil_e=N ceil_d=N ceil_r=N."""
    with open(path) as f:
        first = f.readline().lstrip("#").strip()
    fields = {}
    for tok in first.split():
        if "=" in tok and not tok.startswith("("):
            k, _, v = tok.partition("=")
            try:
                fields[k] = int(v)
            except ValueError:
                fields[k] = v
    return first, fields


def parse_duration(v):
    """metav1.Duration-ish: '30s' '1m' '500ms' or a bare number → seconds (int)."""
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip()
    try:
        if s.endswith("ms"):
            return int(float(s[:-2]) / 1000)
        if s.endswith("s"):
            return int(float(s[:-1]))
        if s.endswith("m"):
            return int(float(s[:-1]) * 60)
        if s.endswith("h"):
            return int(float(s[:-1]) * 3600)
        return int(float(s))
    except ValueError:
        return 0


def resolve_max_unavailable(mu, n):
    """maxUnavailable → absolute Instance count against n. Ints pass through;
    k8s-style percent strings round DOWN (Deployment maxUnavailable semantics),
    so '100%' of 8 = 8 (floor 0) and '25%' of 8 = 2. Anything unparsable = 0."""
    if isinstance(mu, int):
        return mu
    s = str(mu).strip()
    if s.endswith("%"):
        try:
            return (n * int(float(s[:-1]))) // 100
        except ValueError:
            return 0
    try:
        return int(s)
    except (TypeError, ValueError):
        return 0


def canary_unit(comp):
    """The canary unit a Component belongs to, named by the unit's entrypoint.

    The router is its own unit; engine and decoder are one unit, because a P/D
    pair speaks a pairing protocol to each other. A unit is what one canary run
    owns — its step counter, revision pair and traffic entry live on the
    entrypoint. Mirrors rollout.CanaryUnit.
    """
    return "engine" if comp == "decoder" else comp


def canary_unit_entrypoint(components):
    """The Component a canary group's step machine is driven through: the
    entrypoint of the unit the group covers. Mirrors canary.primaryComponent."""
    for c in ("router", "engine", "decoder"):
        if c in components:
            return c
    return ""


def get_spec_info(case_id):
    """Read spec.rollout.groups + per-Component update strategy from the live ISVC.

    Returns dict: {policy, members, order, soak, pacing, ratio_tol, strat:{comp:type},
    canary, ...}. The v2 group shapes are projected back onto the legacy synthetic
    'policy'/'pacing' fields the verdict logic keys on (see the mapping below).
    """
    info = {"policy": None, "members": [], "order": [], "soak": 0, "strat": {},
            "pacing": None, "ratio_tol": None, "workers": {}, "max_unavail": {}, "partition": {},
            "canary": None, "canary_groups": [], "pairing": ""}
    r = kubectl("-n", NS, "get", "isvc", case_id, "-o", "json")
    if r.returncode != 0:
        return info
    try:
        spec = json.loads(r.stdout).get("spec", {})
    except Exception:
        return spec_default(info)
    for c in COMPS:
        cs = spec.get(c)
        if isinstance(cs, dict):
            t = cs.get("lifecycle", {}).get("updateStrategy", {}).get("type", "SurgeThenDrain")
            info["strat"][c] = t or "SurgeThenDrain"
            # Multi-node: a Component with worker.size>0 runs each Instance as
            # 1 leader + worker.size worker pods (otherwise a single 'default'
            # pod). Drives pods-per-Instance for the Instance-level counters.
            w = cs.get("worker")
            if isinstance(w, dict) and w.get("size"):
                info["workers"][c] = int(w["size"])
            # Per-Component maxUnavailable → the capacity floor (Check 10).
            # Stored RAW (int or percent string); every consumer resolves it
            # against N via resolve_max_unavailable so "100%"/"25%" cases get
            # their real floor instead of silently reading as 0.
            ru = cs.get("lifecycle", {}).get("updateStrategy", {}).get("rollingUpdate", {})
            info["max_unavail"][c] = ru.get("maxUnavailable", 0)
            pt = ru.get("partition", 0)
            info["partition"][c] = pt if isinstance(pt, int) else 0
    # OEP-0017 (v2): coordination + canary unified under spec.rollout.groups[].
    # The harness verdict logic still keys on a synthetic 'policy' (+ pacing,
    # ratio_tol, members, order, soak) and a separate 'canary' block, so derive
    # those legacy fields from the v2 group shapes:
    #   - a single .blueGreen group              -> policy "BlueGreen"
    #   - .rollingUpdate group                   -> policy "RollingUpdate"
    #       + .maintainRatio                     -> pacing "RatioBalanced" (ratio_tol = tolerance)
    #   - .canary group                          -> info["canary"] (not a coordination policy)
    #   - MULTIPLE single-Component blueGreen     -> policy "Sequential" (members/order = the chain)
    #     groups, one per component, in list order  (the v2 spelling for Sequential)
    # P/D pairing protocol (opaque cohort token; rides every engine/decoder
    # revision). "" = unset = pairs with anything.
    info["pairing"] = str(spec.get("rollout", {}).get("pairingProtocol") or "")
    groups = spec.get("rollout", {}).get("groups", []) or []
    # Canary groups first (canary lives on a group; it's not a coordination
    # policy). One group per canary UNIT — an ISVC may carry a router run and an
    # engine run at once — so every canary group is recorded, keyed by its unit.
    # info["canary"]/["members"] keep naming the first for the legacy readers;
    # the canary handler selects its group out of canary_groups.
    for g in groups:
        if not (isinstance(g, dict) and isinstance(g.get("canary"), dict)):
            continue
        can, comps = g["canary"], (g.get("components", []) or [])
        cg = {
            "unit": canary_unit_entrypoint(comps),
            "members": comps,
            "order": g.get("order", []) or [],
            "policy": (str(can.get("promotion") or "").lower() or None),
            "steps": can.get("steps", []) or [],
        }
        info["canary_groups"].append(cg)
        if info["canary"] is None:
            info["canary"] = {"policy": cg["policy"], "steps": cg["steps"]}
            info["members"] = cg["members"]
            info["order"] = cg["order"]
    # Coordination groups (the non-canary ones).
    coord_groups = [g for g in groups if isinstance(g, dict) and not isinstance(g.get("canary"), dict)]
    bg_groups = [g for g in coord_groups if isinstance(g.get("blueGreen"), dict)]
    single_bg = [g for g in bg_groups if len(g.get("components", []) or []) == 1]
    if coord_groups and len(coord_groups) == len(single_bg) and len(single_bg) > 1:
        # Multiple single-Component blueGreen groups in order == Sequential.
        chain = [(g.get("components") or [None])[0] for g in single_bg]
        info["policy"] = "Sequential"
        info["members"] = [c for c in chain if c]
        info["order"] = list(info["members"])
        # Soak is carried on each step group; surface the first non-zero (the
        # harness uses a single soak value for its handoff-gap check).
        for g in single_bg:
            s = parse_duration(g.get("soak"))
            if s:
                info["soak"] = s
                break
    elif coord_groups:
        g = coord_groups[0]
        info["members"] = g.get("components", []) or []
        info["order"] = g.get("order", []) or []
        info["soak"] = parse_duration(g.get("soak"))
        if isinstance(g.get("blueGreen"), dict):
            info["policy"] = "BlueGreen"
        elif isinstance(g.get("rollingUpdate"), dict) or "rollingUpdate" in g:
            info["policy"] = "RollingUpdate"
            mr = g.get("maintainRatio")
            if isinstance(mr, dict):
                info["pacing"] = "RatioBalanced"
                info["ratio_tol"] = mr.get("tolerance")
    return info


def spec_default(info):
    return info


def aggregate(state, old_hashes, modes, pods_per_inst, measures=None):
    """state: pod_name → {comp, inst, runner, alive, ready, hash, uid, newanno} | None.

    Groups pods into INSTANCES by (comp, instance-index) and returns
    {comp: (alive_inst, ready_inst, updated_inst)} counted in Instances, not raw
    pods. An Instance is one 'default' pod (single-pod) or 1 'leader' +
    worker.size 'worker' pods (multi-node); pods_per_inst[comp] is that expected
    per-Instance pod count. An Instance is:
      * alive   → it has >=1 pod (a surge adds a whole new Instance index);
      * ready   → it has its full pod set AND every pod is Ready;
      * updated → every pod moved to the new generation, per update mode:
          - surge   → pod's revision-hash is NOT in old_hashes (a new pod)
          - inplace → pod carries the new perm.test/rev annotation (same pod)
    Pre-trigger (old_hashes is None) updated is always 0. Backward-compatible:
    single-pod cases have one pod per Instance, so counts equal the old per-pod
    counts.

    measures (opt-in dict, mutated in place): per-Component distinct-alive-index
    diagnostics that the raw (alive,ready,updated) tuple cannot express. For each
    Component c it records {c: (distinct_alive_idx, new_pods, distinct_new_idx)}:
      * distinct_alive_idx → # of instance indices with >=1 alive pod. A gang
        surge that COLLAPSES many gangs onto ONE index reads as 1 distinct index
        even though the alive-pod count is large — the fleet-wipe signature the
        Instance-count tuple hides.
      * new_pods           → raw count of alive new-revision pods (post-trigger).
      * distinct_new_idx   → # of distinct indices carrying >=1 new-revision pod.
        For a healthy gang roll new_pods == distinct_new_idx * pods_per_inst; a
        shortfall means two gangs share an index (collision).
    """
    insts = defaultdict(lambda: defaultdict(list))  # comp -> instance-index -> [pod records]
    for p in state.values():
        if p is None or not p["alive"]:
            continue
        insts[p["comp"]][p.get("inst", "0")].append(p)
    agg = defaultdict(lambda: [0, 0, 0])
    for c, by_inst in insts.items():
        ppi = max(1, pods_per_inst.get(c, 1))
        distinct_alive_idx = new_pods = distinct_new_idx = 0
        for pods in by_inst.values():
            # alive: gangs count 1 per Instance (a surge is a whole new instance
            # index → already +1). Single-pod (ppi==1) counts alive PODS so a
            # within-instance surge — 2 pods at the SAME inst-index on opposite
            # ActiveOrdinal slots (how the controller surges a single-pod
            # Instance, update.go:1262) — is visible. Instance-counting alone
            # collapses both pods into 1 and silently misses the surge.
            agg[c][0] += len(pods) if ppi == 1 else 1
            distinct_alive_idx += 1
            # Per-pod new-revision signal (independent of whole-Instance updated,
            # so a collision that leaves an index half-new is still counted).
            if old_hashes is not None:
                if modes.get(c) == "inplace":
                    idx_new = [q for q in pods if q.get("newanno")]
                else:
                    idx_new = [q for q in pods if q.get("hash") and q["hash"] not in old_hashes]
                new_pods += len(idx_new)
                if idx_new:
                    distinct_new_idx += 1
            ready_pods = sum(1 for q in pods if q["ready"])
            if len(pods) >= ppi and ready_pods >= ppi:
                agg[c][1] += 1  # ready Instance: full pod set, all Ready
                if old_hashes is None:
                    updated = False
                elif modes.get(c) == "inplace":
                    updated = all(q.get("newanno") for q in pods)
                else:
                    updated = all(q.get("hash") and q["hash"] not in old_hashes for q in pods)
                if updated:
                    agg[c][2] += 1
        if measures is not None:
            measures[c] = (distinct_alive_idx, new_pods, distinct_new_idx)
    return {c: tuple(v) for c, v in agg.items()}


def _dump_index_breakdown(ctx, comp, checks):
    """On a collision / shared-index failure, print the settle-snapshot pod
    layout for `comp` (per instance-index: runner:rev:aliveReady) so the log
    proves whether a gang truly settled short (missing/old pod) vs a transient."""
    old = ctx.get("old_hashes") or set()
    by = defaultdict(list)
    for p in (ctx.get("pod_state") or {}).values():
        if p and p.get("comp") == comp:
            by[p.get("inst", "?")].append(p)
    checks.append(f"    · {comp} settle-snapshot per instance-index:")
    for idx in sorted(by, key=lambda x: str(x)):
        parts = [f"{p.get('runner','?')}:{'new' if (p.get('hash') and p['hash'] not in old) else 'old'}:"
                 f"{'A' if p.get('alive') else 'x'}{'R' if p.get('ready') else '-'}" for p in by[idx]]
        checks.append(f"      idx {idx}: {' '.join(parts)}")


def index_collision_checks(ctx, comps, verdict, notes, checks):
    """Distinct-alive-index invariant (fleet-wipe guard). For each Component in
    `comps` assert: (a) the number of instance indices with >=1 alive pod is at
    least the alive Instance count — a shortfall means two DIFFERENT Instances
    (gangs) collapsed onto one index, the collision the Instance-count tuple
    hides; and (b) for gangs (pods_per_inst>1) the raw alive new-revision pod
    count equals distinct_new_index * pods_per_inst — a shortfall means some
    index holds pods from more than one gang (a shared index). Returns the
    (possibly FAIL) verdict; appends to notes/checks in place.

    This is the check that would have caught the 64→1 fleet-wipe: N gangs
    collapsing onto a single index read as one alive Instance to the tuple, but
    distinct-alive-index falls to 1 and the gang pod count no longer matches
    distinct_new_index * pods_per_inst.
    """
    meas = ctx.get("measures") or {}
    agg = ctx.get("agg") or {}
    ppi = ctx["pods_per_inst"]
    for c in comps:
        if ctx["N"].get(c, 0) == 0:
            continue
        distinct_alive, new_pods, distinct_new = meas.get(c, (0, 0, 0))
        alive_inst = agg.get(c, (0, 0, 0))[0]
        _hit = False
        # (a) distinct alive indices must cover the alive Instance count. For a
        # gang (ppi>1) alive_inst is already Instance-counted, so any shortfall
        # is a collision. For single-pod (ppi==1) alive_inst counts PODS (a
        # within-instance surge legitimately puts 2 pods on 1 index), so compare
        # against ceil(alive_inst / ppi_effective) is not meaningful — restrict
        # the distinct-index floor to gangs where the collapse is the failure.
        if ppi.get(c, 1) > 1 and distinct_alive < alive_inst:
            verdict = "FAIL"
            notes.append(f"{c} index collision: {alive_inst} alive Instances on only {distinct_alive} distinct index(es)")
            checks.append(f"  ✗ {c} index collision: {alive_inst} alive Instance(s) share only {distinct_alive} distinct index(es) — gangs collapsed onto one index (fleet-wipe shape)")
            _hit = True
        # (b) gang new-revision pods must partition cleanly across distinct new
        # indices: new_pods == distinct_new * pods_per_inst. A shortfall means an
        # index carries a partial/second gang's pods (shared index).
        if ppi.get(c, 1) > 1 and new_pods and new_pods != distinct_new * ppi[c]:
            verdict = "FAIL"
            notes.append(f"{c} shared index: {new_pods} new pods across {distinct_new} index(es) (want {distinct_new * ppi[c]})")
            checks.append(f"  ✗ {c} shared index: {new_pods} new-revision pod(s) span {distinct_new} distinct index(es) but a clean gang roll needs {distinct_new * ppi[c]} (={distinct_new}×{ppi[c]}) — a new-rev index holds fewer than {ppi[c]} new pods")
            _hit = True
        if _hit:
            _dump_index_breakdown(ctx, c, checks)
    return verdict


def _track_gang_and_floor(ctx, comps):
    """Mid-loop bookkeeping shared by the _read_one handlers: (1) per-Instance
    mixed-revision span (gang atomicity), and (2) per-Component min ready
    Instances + below-floor dip span (capacity floor with debounce). Mirrors
    the inline tracking in run_case so _partition/_migrate/_canary get the same
    coverage. Stashes results in ctx: gang_worst, min_ready, floor_sust."""
    now = time.time()
    ppi = ctx["pods_per_inst"]
    modes = ctx["modes"]
    agg = ctx.get("agg") or {}
    gang_mixed_since = ctx.setdefault("gang_mixed_since", {})
    gang_worst = ctx.setdefault("gang_worst", {})
    min_ready = ctx.setdefault("min_ready", {})
    floor_since = ctx.setdefault("floor_since", {})
    floor_sust = ctx.setdefault("floor_sust", {})
    by_comp = defaultdict(lambda: defaultdict(list))
    for q in ctx["pod_state"].values():
        if q and q["alive"]:
            by_comp[q["comp"]][q.get("inst", "0")].append(q)
    for c in comps:
        if ppi.get(c, 1) > 1:
            for inst_idx, ipods in by_comp[c].items():
                if modes.get(c) == "inplace":
                    sigs = {bool(q.get("newanno")) for q in ipods}
                else:
                    sigs = {q.get("hash") for q in ipods if q.get("hash")}
                key = (c, inst_idx)
                if len(sigs) >= 2:                       # mixed old+new within one Instance
                    gang_mixed_since.setdefault(key, now)
                    span = now - gang_mixed_since[key]
                    if span > gang_worst.get(key, 0):
                        gang_worst[key] = span
                else:
                    gang_mixed_since.pop(key, None)
        rd = agg.get(c, (0, 0, 0))[1]
        if c not in min_ready or rd < min_ready[c]:
            min_ready[c] = rd
        # Below-floor dip span → min_ready_checks' FLOOR_SUSTAIN_S debounce
        # (mirrors run_case's Check 10 tracking).
        n = ctx["N"].get(c, 0)
        if n:
            fl = n - resolve_max_unavailable(ctx["info"].get("max_unavail", {}).get(c, 0), n)
            if rd < fl:
                floor_since.setdefault(c, now)
                span = now - floor_since[c]
                if span > floor_sust.get(c, 0.0):
                    floor_sust[c] = span
            else:
                floor_since.pop(c, None)


def gang_atomicity_checks(ctx, comps, verdict, notes, checks):
    """Check 9 ported to the _read_one handlers: each bumped multi-node gang must
    roll leader+workers as ONE unit — no Instance sat mixed-revision >= GANG_TOL_S.
    Requires _track_gang_and_floor to have run over the same comps in the loop."""
    gang_worst = ctx.get("gang_worst") or {}
    mn = [c for c in comps if ctx["pods_per_inst"].get(c, 1) > 1]
    if not mn:
        return verdict
    viol = {k: v for k, v in gang_worst.items() if k[0] in mn and v >= GANG_TOL_S}
    if viol:
        verdict = "FAIL"
        wk = max(viol, key=viol.get)
        notes.append(f"gang atomicity violated: {wk[0]} Instance {wk[1]} mixed-revision {viol[wk]:.0f}s")
        checks.append(f"  ✗ gang atomicity violated: {len(viol)} Instance(s) sat mixed-revision ≥{GANG_TOL_S:.0f}s — leader+workers did NOT roll as a unit (worst {wk[0]} inst {wk[1]}: {viol[wk]:.0f}s)")
    else:
        checks.append(f"  ✓ gang atomicity held: every multi-node Instance ({', '.join(mn)}) rolled leader+workers as a unit")
    return verdict


def surge_before_drain_checks(ctx, comps, verdict, notes, checks):
    """Surge-before-drain ordering for the _read_one handlers: a source pod must
    not begin draining before its Ready surge replacement (drained-old must not
    lead ready-new by more than maxUnavailable). Consumes the running worst breach
    accumulated in _apply_event (ctx['order_viol']). SurgeThenDrain components
    only — RecreatePod also aggregates as mode "surge" but drains first by design,
    and _apply_event only tracks SurgeThenDrain."""
    ov = ctx.get("order_viol") or {}
    surge = [c for c in comps if ctx["info"].get("strat", {}).get(c) == "SurgeThenDrain"]
    if not surge:
        return verdict
    viol = {c: ov.get(c, 0) for c in surge if ov.get(c, 0) > 0}
    if viol and (ctx.get("hdr") or {}).get("order_soft"):
        wc = max(viol, key=viol.get)
        checks.append(f"  ⚠ surge-before-drain: {wc} drained {viol[wc]} pod(s) ahead of ready surges — case opted out (order_soft=1, fault-induced drain); informational")
    elif viol:
        verdict = "FAIL"
        wc = max(viol, key=viol.get)
        notes.append(f"surge-before-drain violated: {wc} drained {viol[wc]} pod(s) ahead of ready surges")
        checks.append(f"  ✗ surge-before-drain violated: {len(viol)} Component(s) began draining a source before its replacement was Ready (worst {wc}: {viol[wc]} pod(s) ahead)")
    else:
        checks.append(f"  ✓ surge-before-drain held: every drain ({', '.join(surge)}) followed a Ready surge replacement")
    return verdict


def min_ready_checks(ctx, comps, verdict, notes, checks, label="capacity floor"):
    """Capacity-floor: each Component's min ready Instances mid-roll stays >=
    N - maxUnavailable. Debounced like run_case's Check 10: only a dip that
    PERSISTED >= FLOOR_SUSTAIN_S fails; shorter dips are reported ⚠ as
    readiness flaps. floor_hard=1 keeps the instantaneous semantics. Requires
    _track_gang_and_floor to have run over `comps`."""
    min_ready = ctx.get("min_ready") or {}
    floor_sust = ctx.get("floor_sust") or {}
    floor_hard = bool((ctx.get("hdr") or {}).get("floor_hard"))
    mu = ctx["info"].get("max_unavail", {})
    for c in comps:
        if ctx["N"].get(c, 0) == 0:
            continue
        floor = ctx["N"][c] - resolve_max_unavailable(mu.get(c, 0), ctx["N"][c])
        mr = min_ready.get(c, ctx["N"][c])
        sust = floor_sust.get(c, 0.0)
        if mr >= floor:
            checks.append(f"  ✓ {c} {label} held: min ready {mr} ≥ N-maxUnavailable={floor}")
        elif floor_hard or sust >= FLOOR_SUSTAIN_S:
            verdict = "FAIL"
            held = "" if floor_hard else f", dip held {sust:.0f}s ≥ {FLOOR_SUSTAIN_S:.0f}s"
            notes.append(f"{c} {label} breached: min ready {mr} < N-maxUnavailable={floor}")
            checks.append(f"  ✗ {c} {label} breached: min ready {mr} < N-maxUnavailable={floor}{held} — did NOT preserve capacity")
        else:
            checks.append(f"  ⚠ {c} capacity dipped to {mr} ready Instance(s) (< N-maxUnavailable={floor}) for <{FLOOR_SUSTAIN_S:.0f}s — debounced as a readiness flap (not failed)")
    return verdict


def fetch_status(case_id):
    r = kubectl("-n", NS, "get", "isvc", case_id, "-o", "json")
    if r.returncode != 0:
        return {}
    try:
        return json.loads(r.stdout).get("status", {})
    except Exception:
        return {}


def fetch_rev_services(case_id):
    r = kubectl("-n", NS, "get", "svc", "-l", f"ome.io/inferenceservice={case_id}",
                "-o", "json")
    if r.returncode != 0:
        return []
    try:
        return [i["metadata"]["name"] for i in json.loads(r.stdout).get("items", [])]
    except Exception:
        return []


def _teardown_survivors(case_id):
    """Live leftovers for a case after its delete: [(kind, name, node)] across
    component pods, InferenceReplicas, Services (incl. per-revision), and the
    ISVC object itself. Empty = teardown converged. A missing CRD or NotFound
    reads as gone (version-tolerant: pre-IR builds have no inferencereplicas)."""
    sel = f"ome.io/inferenceservice={case_id}"
    left = []
    for kind, res in (("pod", "pod"), ("inferencereplica", "inferencereplicas"), ("svc", "svc")):
        r = kubectl("-n", NS, "get", res, "-l", sel, "-o", "json")
        if r.returncode != 0:
            continue
        try:
            items = json.loads(r.stdout).get("items", [])
        except Exception:
            continue
        for it in items:
            node = (it.get("spec") or {}).get("nodeName", "") if kind == "pod" else ""
            left.append((kind, (it.get("metadata") or {}).get("name", "?"), node))
    if kubectl("-n", NS, "get", "isvc", case_id, "-o", "name").returncode == 0:
        left.append(("isvc", case_id, ""))
    return left


def teardown_check(case_id, total_pods, verdict, notes, checks):
    """Universal end-of-case teardown gate: after the case's delete, poll
    (bounded) until zero component pods, no InferenceReplicas, no Services and
    no ISVC remain. Anything surviving the budget FAILs the verdict, naming
    each survivor (kind/name/node) — a cleanup that doesn't converge is a
    controller bug (wedged finalizer / orphaned pods), not a mop-up detail."""
    budget = min(MAX_BUDGET_S, TEARDOWN_BASE_S + TEARDOWN_PER_POD_S * max(0, total_pods))
    deadline = time.time() + budget
    left = _teardown_survivors(case_id)
    while left and time.time() < deadline:
        time.sleep(TEARDOWN_POLL_S)
        left = _teardown_survivors(case_id)
    if left:
        named = ", ".join(f"{k}/{n}" + (f"@{node}" if node else "") for k, n, node in left[:8])
        more = f" (+{len(left) - 8} more)" if len(left) > 8 else ""
        verdict = "FAIL"
        notes.append(f"teardown did not converge in {budget}s: {named}{more}")
        checks.append(f"  ✗ teardown did not converge within {budget}s — survivors: {named}{more}")
    else:
        checks.append("  ✓ teardown converged: pods, InferenceReplicas, Services, ISVC all gone")
    return verdict


# ── Continuous invariants ────────────────────────────────────────────────────
# Invariants the pod-observed verdicts are blind to, polled every INV_TICK_S
# for the whole case:
#   A status-truth      — an IR Instance must not claim Phase=Failed while its
#                         full pod set is Ready+serving, nor Phase=Ready with
#                         zero pods for its index.
#   B endpoints-membership — every Ready+serving pod that revision-matches a
#                         per-revision routed Service must be a Ready endpoint
#                         of it, and no endpoint may reference a deleted pod.
#   C pair-alive        — during a non-empty→non-empty pairing-protocol
#                         transition (armed only by mode=pairing), whenever
#                         both engine and decoder serve, a pairable pair must
#                         exist; the gate's no-pair and 1×1-wall escapes are
#                         exempt.
# A violation must hold for its sustain count of CONSECUTIVE ticks to fire
# (see the INV_* constants for the carve-out arithmetic); fired violations
# dedup per (invariant, kind, subject) with first/last timestamps and an
# occurrence count, and FAIL the verdict via invariant_checks.


def _pod_serving(obj):
    """True when the pod's ome.io/serving readiness-gate condition is True."""
    for c in (obj.get("status", {}).get("conditions") or []):
        if c.get("type") == SERVING_COND:
            return c.get("status") == "True"
    return False


def _inv_new(case_id, hdr):
    """Fresh invariant-poller state. Header `inv_off=A|B|C|all` (which requires
    an adjacent '# reason:' comment — enforced by validate-only) disables checks.
    Invariant C (pair-alive) additionally requires arming: a pairing-mode
    handler sets inv["pairing"] at trigger time for a non-empty→non-empty
    protocol transition; without that it never evaluates."""
    off = str(hdr.get("inv_off", "") or "")
    return {
        "case_id": case_id,
        "off": off,
        "a_on": off not in ("A", "all"),
        "b_on": off not in ("B", "all"),
        "c_on": off not in ("C", "all"),
        # inv_xfail scope: a violation of this invariant class is the case's
        # tracked bug (notimpl issue) and records as the XFAIL, never a FAIL.
        "xfail": str(hdr.get("inv_xfail", "") or ""),
        "xfail_issue": hdr.get("issue"),
        "last": 0.0,        # last tick timestamp (self-rate-limited)
        "streaks": {},      # streak_key → consecutive-tick observation count
        "viol": {},         # viol_key → {first, last, n, detail}
        "ir_seen": False,   # instanceStatuses observed at least once (version tolerance)
        "b_seen": False,    # invariant B evaluated ≥1 Ready+serving pod against a routed Service
        "dead_uids": set(), # pod UIDs whose DELETED watch event was observed
        # Set by the pairing handler for a non-empty→non-empty transition:
        # {"frm": F, "to": T, "armed": bool, "seen": bool}. armed flips True on
        # every pairable sample and gates the violation — once the pair is gone
        # (for any reason) the unpaired samples that follow are the product
        # legitimately progressing back toward a pair, not the gate breaking one.
        "pairing": None,
    }


def _instance_rows(ir_status):
    """Decoded per-Instance rows of an InferenceReplica status for a case
    log; none when the status carries no rows or does not decode."""
    try:
        return irstatus.instance_statuses(ir_status) or []
    except irstatus.DecodeError:
        return []


def _inv_hit(inv, hits, streak_key, viol_key, sustain, now, detail):
    """One tick's observation of a violating condition. Fires (records a
    violation) only once the streak reaches `sustain` consecutive ticks."""
    hits.add(streak_key)
    n = inv["streaks"].get(streak_key, 0) + 1
    inv["streaks"][streak_key] = n
    if n < sustain:
        return
    v = inv["viol"].get(viol_key)
    if v is None:
        inv["viol"][viol_key] = {"first": now, "last": now, "n": 1, "detail": detail}
        print(f"    ✗ INVARIANT {viol_key[0]} ({viol_key[1]}): {detail}")
    else:
        v["last"] = now
        v["n"] += 1
        v["detail"] = detail


def _inv_check_status_truth(inv, pod_state, pods_per_inst, hits, now):
    """Invariant A, one tick: compare each IR Instance's claimed phase against
    the watcher's live pods for its index. Version-tolerant: a build without
    the IR CRD / instanceStatuses field leaves ir_seen False and the verdict
    prints one ⓘ note instead of asserting."""
    r = kubectl("-n", NS, "get", "inferencereplicas", "-l",
                f"ome.io/inferenceservice={inv['case_id']}", "-o", "json")
    if r.returncode != 0:
        return
    try:
        items = json.loads(r.stdout).get("items", [])
    except Exception:
        return
    existing = defaultdict(list)   # (comp, inst-index) → pod records still existing
    for p in pod_state.values():
        if p:
            existing[(p["comp"], p.get("inst", "0"))].append(p)
    for it in items:
        comp = (it.get("spec") or {}).get("component", "?")
        try:
            insts = irstatus.instance_statuses(it.get("status") or {})
        except irstatus.DecodeError as e:
            key = ("A", "undecodable-status", comp, "-")
            _inv_hit(inv, hits, key, key, INV_SUSTAIN_A, now,
                     f"{comp}: per-Instance status undecodable: {e}")
            continue
        if insts is None:
            continue   # pre-IR-status build — field absent
        inv["ir_seen"] = True
        ppi = max(1, pods_per_inst.get(comp, 1))
        for ist in insts:
            phase = ist.get("phase")
            idx = str(ist.get("index", "?"))
            if phase in INV_TRANSIENT_PHASES:
                continue   # transitional phases are exempt from all A-checks
            pods = existing.get((comp, idx), [])
            if phase == "Failed":
                # Failed-while-serving: the Instance claims Failed while its
                # FULL pod set is alive, Ready and in the serving rotation.
                alive = [p for p in pods if p["alive"]]
                if len(alive) >= ppi and all(p["ready"] and p.get("serving") for p in alive):
                    key = ("A", "failed-while-serving", comp, idx)
                    _inv_hit(inv, hits, key, key, INV_SUSTAIN_A, now,
                             f"{comp}/{idx} Phase=Failed while all {len(alive)} live pod(s) are Ready+serving")
            elif phase == "Ready":
                # Ready-without-pods reads the IR's own podCount, not
                # the watcher's separately-collected pod_state. The two are
                # independent data sources with independent lag: under slow
                # concurrent scheduling the watcher's pod stream trails the IR,
                # so comparing IR-phase against watcher-pods fires "Ready but I
                # see no pod" for indices whose pod exists from the apiserver's
                # view — a harness split-brain, not a product lie.
                if int(ist.get("podCount", 0)) == 0:
                    key = ("A", "ready-without-pods", comp, idx)
                    _inv_hit(inv, hits, key, key, INV_SUSTAIN_A, now,
                             f"{comp}/{idx} Phase=Ready while the IR's own podCount is 0")


# Selector keys the watcher can evaluate against its pod records. Keys every
# OMENative-managed pod carries by construction (the watcher's own selector /
# the managed-by stamp) are treated as always-matching.
_INV_SEL_KEYS = ("component", REV_LABEL, "ome.io/runner", "ome.io/pod-ordinal")
_INV_SEL_UNIVERSAL = {"ome.io/inferenceservice", "ome.io/managed-by"}


def _inv_check_endpoints(inv, pod_state, hits, now):
    """Invariant B, one tick: for each per-revision ROUTED Service (selector
    carries the revision-hash label; headless peer-DNS Services excluded —
    they publish not-ready addresses by design), every alive+Ready+serving pod
    matching the selector must appear as a Ready endpoint, and no endpoint may
    reference a pod UID whose DELETED event the watcher has seen."""
    sel = f"ome.io/inferenceservice={inv['case_id']}"
    rs = kubectl("-n", NS, "get", "svc", "-l", sel, "-o", "json")
    re_ = kubectl("-n", NS, "get", "endpointslices", "-l", sel, "-o", "json")
    if rs.returncode != 0 or re_.returncode != 0:
        return
    try:
        svcs = json.loads(rs.stdout).get("items", [])
        slices = json.loads(re_.stdout).get("items", [])
    except Exception:
        return
    eps = defaultdict(list)   # service name → endpoints (slices merged; labels are service-mirrored)
    for sl in slices:
        svc_name = ((sl.get("metadata") or {}).get("labels") or {}).get("kubernetes.io/service-name")
        if svc_name:
            eps[svc_name].extend(sl.get("endpoints") or [])
    for s in svcs:
        spec = s.get("spec") or {}
        selector = spec.get("selector") or {}
        name = ((s.get("metadata") or {}).get("name")) or "?"
        if spec.get("clusterIP") == "None" or REV_LABEL not in selector:
            continue   # headless (peer DNS) or not a per-revision routed Service
        if set(selector) - set(_INV_SEL_KEYS) - _INV_SEL_UNIVERSAL:
            continue   # selector uses a label the watcher doesn't track — can't evaluate
        ready_uids, all_uids = set(), set()
        for ep in eps.get(name, []):
            u = (ep.get("targetRef") or {}).get("uid")
            if not u:
                continue
            all_uids.add(u)
            # EndpointConditions contract: nil ready reads as ready.
            if (ep.get("conditions") or {}).get("ready") is not False:
                ready_uids.add(u)
        for pname, p in pod_state.items():
            if not p or not p["alive"] or not p["ready"] or not p.get("serving") or not p.get("uid"):
                continue
            view = {"component": p["comp"], REV_LABEL: p.get("hash", ""),
                    "ome.io/runner": p.get("runner", "default"),
                    "ome.io/pod-ordinal": p.get("ord", "0")}
            if any(k in selector and view[k] != selector[k] for k in _INV_SEL_KEYS):
                continue   # pod is not a member of this Service
            inv["b_seen"] = True   # membership actually evaluated (non-vacuous)
            if p["uid"] not in ready_uids:
                _inv_hit(inv, hits, ("B", "missing-endpoint", name, pname), ("B", "missing-endpoint", name),
                         INV_SUSTAIN_B_MISSING, now,
                         f"svc {name}: pod {pname} is Ready+serving on its revision but absent from the Ready endpoints")
        for u in all_uids & inv["dead_uids"]:
            _inv_hit(inv, hits, ("B", "stale-endpoint", name, u), ("B", "stale-endpoint", name),
                     INV_SUSTAIN_B_STALE, now,
                     f"svc {name}: endpoint still references deleted pod UID {u[:13]}…")


def _pairing_serving_split(pod_state):
    """(engine_insts, decoder_insts): serving Instances per pairing Component,
    each a list of the instance's serving pods' pairing-protocol labels. An
    Instance is serving when >=1 of its pods is alive+Ready+serving (mirrors
    the gate's ServingPodCount>0)."""
    by = {"engine": defaultdict(list), "decoder": defaultdict(list)}
    for p in pod_state.values():
        if p and p["alive"] and p["ready"] and p.get("serving") and p["comp"] in by:
            by[p["comp"]][p.get("inst", "0")].append(p.get("pairing", ""))
    return list(by["engine"].values()), list(by["decoder"].values())


def _inv_check_pair_alive(inv, pod_state, hits, now):
    """Invariant C, one tick: during a non-empty→non-empty pairing-protocol
    transition, whenever both engine and decoder have >=1 serving Instance a
    pairable pair must exist — some serving engine pod and serving decoder pod
    with equal protocols, or either empty (empty pairs with anything).

    The gate's liveness escapes are exempt:
      - no pair to protect: once no pairable pair exists (a side at zero, or
        externally destroyed), the check DISARMS — the unpaired samples that
        follow are the product progressing back toward a pair, exactly what the
        no-pair-pre-existing escape allows. It re-arms on the next pairable
        sample. The last-hop escape's window is covered by the same rule: the
        acting side's final hop passes through zero serving, which disarms.
      - the 1×1 mutual wall: both Components down to <=1 serving Instance is
        the bounded first-mover window the gate deliberately allows.
    """
    st = inv.get("pairing")
    if not st:
        return
    eng, dec = _pairing_serving_split(pod_state)
    if not eng or not dec:
        st["armed"] = False   # a side has nothing serving — no pair to protect
        return
    st["seen"] = True
    pairable = any(pe == "" or pd == "" or pe == pd
                   for insts_e in eng for pe in insts_e
                   for insts_d in dec for pd in insts_d)
    if pairable:
        st["armed"] = True
        return
    if len(eng) <= 1 and len(dec) <= 1:
        return   # 1×1 mutual wall: bounded by-design window; stays armed
    if not st["armed"]:
        return
    key = ("C", "pair-alive")
    eng_p = sorted({pe for insts in eng for pe in insts})
    dec_p = sorted({pd for insts in dec for pd in insts})
    _inv_hit(inv, hits, key, key, INV_SUSTAIN_C, now,
             f"no pairable serving engine+decoder pair during {st['frm']!r}→{st['to']!r} "
             f"(engine serves {eng_p}, decoder serves {dec_p})")


def _inv_tick(inv, pod_state, pods_per_inst):
    """One invariant observation, self-rate-limited to INV_TICK_S. Called from
    every watch loop each iteration; a no-op between ticks and when every
    invariant is opted out. Streaks reset for keys not observed this tick
    (the CONSECUTIVE-tick sustain rule)."""
    if not inv or (not inv["a_on"] and not inv["b_on"] and not inv["c_on"]):
        return
    now = time.time()
    if now - inv["last"] < INV_TICK_S:
        return
    inv["last"] = now
    hits = set()
    if inv["a_on"]:
        _inv_check_status_truth(inv, pod_state, pods_per_inst, hits, now)
    if inv["b_on"]:
        _inv_check_endpoints(inv, pod_state, hits, now)
    if inv["c_on"]:
        _inv_check_pair_alive(inv, pod_state, hits, now)
    for k in [k for k in inv["streaks"] if k not in hits]:
        del inv["streaks"][k]


def invariant_checks(inv, verdict, notes, checks):
    """Verdict gate for the continuous invariants: every violation that fired
    (sustained past its carve-out) FAILs the case, with first/last-seen
    timestamps and the tick occurrence count. The one exception is a parked
    case whose header scopes its tracked bug to an invariant class
    (inv_xfail): violations of exactly that class record as the case's
    known-bug XFAIL — still evaluated, still printed — while any other class
    fails hard, so unrelated regressions can never hide behind the parking."""
    if not inv:
        return verdict
    if inv["off"]:
        checks.append(f"  ⓘ continuous invariant(s) disabled by header inv_off={inv['off']} (reason in the case file)")
    if inv["a_on"] and not inv["ir_seen"]:
        checks.append("  ⓘ invariant A (status-truth) skipped — IR instanceStatuses unavailable on this build")
    if inv["b_on"] and not inv["b_seen"]:
        checks.append("  ⓘ invariant B (endpoints-membership) evaluated zero Ready+serving pods against a routed Service — vacuous (no assertion made)")
    pairing = inv.get("pairing")
    if inv["c_on"] and pairing and not pairing.get("seen"):
        checks.append("  ⓘ invariant C (pair-alive) never saw both engine and decoder serving at a tick — vacuous (no assertion made)")
    if not inv["viol"]:
        active = [x for x, on in (("A", inv["a_on"] and inv["ir_seen"]),
                                  ("B", inv["b_on"] and inv["b_seen"]),
                                  ("C", inv["c_on"] and bool(pairing) and pairing.get("seen", False))) if on]
        if active:
            checks.append(f"  ✓ continuous invariants held ({'+'.join(active)}): no sustained status-truth / endpoints-membership / pair-alive violation")
        return verdict
    scope = inv.get("xfail") or ""
    ref = f"#{inv['xfail_issue']}" if inv.get("xfail_issue") else "UNLINKED"
    expected_hits = 0
    for key, v in sorted(inv["viol"].items()):
        first = time.strftime("%H:%M:%S", time.localtime(v["first"]))
        last = time.strftime("%H:%M:%S", time.localtime(v["last"]))
        span = f"[first {first}, last {last}, {v['n']} tick(s)]"
        if scope == "all" or key[0] == scope:
            expected_hits += 1
            notes.append(f"invariant {key[0]} {key[1]} (known bug: {ref}): {v['detail']}")
            checks.append(f"  ⓘ invariant {key[0]} violated ({key[1]}) — the tracked bug "
                          f"this case is parked on ({ref}): {v['detail']} {span}")
        else:
            verdict = "FAIL"
            notes.append(f"invariant {key[0]} {key[1]}: {v['detail']}")
            checks.append(f"  ✗ invariant {key[0]} violated ({key[1]}): {v['detail']} {span}")
    if expected_hits and verdict == "PASS":
        verdict = f"XFAIL (known bug: {ref})"
    return verdict


# ── Fault injection (header fault=<kind>:<param>) ───────────────────────────
# Runner plumbing for controlled faults. Kinds:
#   quota:<n>          — pre-trigger: create a namespace ResourceQuota sized from
#                        the case's OWN pod requests (never hardcoded) to admit
#                        exactly current-usage + n more pods of the case's shape.
#   podkill:<phase>    — at the phase hook, delete one deterministic pod.
#   ctrlrestart:<phase>— at the phase hook, delete the OME manager pod. ALWAYS
#                        gated on OME_TEST_ALLOW_CTRL_RESTART=1 (shared-cluster
#                        safety); otherwise skipped with an ⓘ note.
#   cordon:source      — pre-trigger: cordon the node hosting the source pod;
#                        ALWAYS uncordoned in cleanup (failure-path safe).
# Phase enum: settling (immediately after the trigger), rotating (first surge/
# updated pod observed post-trigger), migrating (migration accept observed).
# One firing per case (latched). Any fault-APPARATUS error → the distinct
# INFRA-FAIL verdict: the harness broke, not the product — never a FAIL/PASS.
FAULT_PHASES = {"rotating", "migrating", "settling"}
FAULT_TARGETS = {"source"}
# Marker label stamped on a node the harness cordons, so batch.py's end-of-run
# sweep can find (and uncordon) ONLY harness cordons — never an operator's.
CORDON_LABEL = "perm.test/cordoned"
# Manager pod location for ctrlrestart. Label from the chart's Deployment
# selector (charts/ome-resources/templates/ome-controller/deployment.yaml).
CTRL_NS = os.environ.get("OME_TEST_CTRL_NS", "ome")
CTRL_SELECTOR = os.environ.get("OME_TEST_CTRL_SELECTOR", "control-plane=ome-controller-manager")

# Cleanup registry, flushed in __main__'s finally — runs on PASS, FAIL,
# INFRA-FAIL and crash alike. Entries: ("quota", case_id) | ("uncordon", node).
_FAULT_CLEANUPS = []


def _run_fault_cleanups():
    """Idempotent failure-path-safe fault teardown (quota delete + uncordon)."""
    while _FAULT_CLEANUPS:
        kind, arg = _FAULT_CLEANUPS.pop()
        if kind == "quota":
            _lift_quota(arg)
        elif kind == "uncordon":
            kubectl("uncordon", arg)
            kubectl("label", "node", arg, f"{CORDON_LABEL}-")   # drop the harness marker


def _lift_quota(case_id):
    """Remove the case's fault ResourceQuota. Idempotent — callable mid-case
    (a case that lifts the quota to let a blocked surge proceed) and again
    from the end-of-case cleanup."""
    kubectl("-n", NS, "delete", "resourcequota", f"perm-{case_id}-quota",
            "--ignore-not-found=true")


CHURN_SETTLE_S = 20.0   # blocked=1 probe: two pod snapshots this far apart must match


def _churn_stopped(case_id):
    """blocked=1 probe (run after the expected non-convergence): the case's pod
    set — (UID, deleting?) pairs — must be IDENTICAL across two snapshots
    CHURN_SETTLE_S apart. A parked fault-blocked state holds still; a
    create/delete retry loop shows up as a differing set. Quota-REJECTED create
    attempts never materialize as pods, so controller-side retries that the
    apiserver refuses correctly read as 'stopped'."""
    def snap():
        r = kubectl("-n", NS, "get", "pod", "-l", f"ome.io/inferenceservice={case_id}", "-o", "json")
        if r.returncode != 0:
            return None
        try:
            return {((it.get("metadata") or {}).get("uid", ""),
                     bool((it.get("metadata") or {}).get("deletionTimestamp")))
                    for it in json.loads(r.stdout).get("items", [])}
        except Exception:
            return None
    a = snap()
    time.sleep(CHURN_SETTLE_S)
    b = snap()
    if a is None or b is None:
        return False, "churn probe could not read the case pods"
    if a == b:
        return True, f"pod set held still across {CHURN_SETTLE_S:.0f}s ({len(b)} pod(s))"
    return False, (f"pod set still churning across {CHURN_SETTLE_S:.0f}s "
                   f"({len(a)}→{len(b)} pod(s), {len(a ^ b)} differing)")


def _fault_ok(v):
    """Header validator for fault=<kind>:<param> (also the validate-only schema)."""
    kind, _, param = str(v).partition(":")
    if kind == "quota":
        try:
            # n=0 = zero headroom: every further pod admission is rejected
            # (the only way to bound a single-pod surge).
            return int(param) >= 0
        except ValueError:
            return False
    if kind in ("podkill", "ctrlrestart"):
        return param in FAULT_PHASES
    if kind == "cordon":
        return param in FAULT_TARGETS
    return False


def _fault_new(case_id, hdr):
    """Parsed fault header state (or None). fired latches the one-shot phase
    faults; infra_fail routes apparatus errors to the INFRA-FAIL verdict."""
    v = str(hdr.get("fault", "") or "")
    if not v:
        return None
    kind, _, param = v.partition(":")
    return {"case_id": case_id, "kind": kind, "param": param,
            "fired": False, "infra_fail": None, "notes": []}


_QTY_BIN = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4, "Pi": 1024**5, "Ei": 1024**6}
_QTY_DEC = {"k": 1000, "K": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4, "P": 1000**5, "E": 1000**6}


def _qty(res, v):
    """k8s resource quantity → int (cpu in millicores, memory in bytes,
    extended resources — GPUs — in plain counts)."""
    s = str(v).strip()
    if res == "cpu":
        return int(float(s[:-1])) if s.endswith("m") else int(float(s) * 1000)
    for suf, mult in _QTY_BIN.items():
        if s.endswith(suf):
            return int(float(s[: -len(suf)]) * mult)
    for suf, mult in _QTY_DEC.items():
        if s.endswith(suf):
            return int(float(s[:-1]) * mult)
    return int(float(s))


def _pod_requests(obj):
    """Summed container resource REQUESTS of one pod — REGULAR containers only.
    The k8s effective-request rule (max of each init container vs the regular
    sum) is not modeled; a pod whose init containers dominate is undercounted."""
    tot = defaultdict(int)
    for ctn in (obj.get("spec", {}).get("containers") or []):
        for k, v in ((ctn.get("resources") or {}).get("requests") or {}).items():
            tot[k] += _qty(k, v)
    return tot


def _qty_str(res, n):
    return f"{n}m" if res == "cpu" else str(n)


def _fault_quota_create(f, case_id):
    """QUOTA: read the case's engine/decoder pod requests live, take the
    per-resource max as the per-pod shape, sum the namespace's current usage
    (non-terminal pods — what ResourceQuota counts), and create a quota
    admitting exactly usage + n more pods of that shape. All quantities are
    DERIVED from the read values — nothing hardcoded."""
    n = int(f["param"])
    r = kubectl("-n", NS, "get", "pod", "-l", f"ome.io/inferenceservice={case_id}", "-o", "json")
    if r.returncode != 0:
        f["infra_fail"] = f"quota: cannot read case pods: {r.stderr.strip()[:120]}"
        return False, f["infra_fail"]
    shape = defaultdict(int)
    try:
        for it in json.loads(r.stdout).get("items", []):
            comp = ((it.get("metadata") or {}).get("labels") or {}).get("component")
            if comp not in ("engine", "decoder"):
                continue
            for k, v in _pod_requests(it).items():
                shape[k] = max(shape[k], v)
    except Exception as e:
        f["infra_fail"] = f"quota: cannot parse case pods: {e}"
        return False, f["infra_fail"]
    if not shape:
        f["infra_fail"] = "quota: case pods expose no container resource requests"
        return False, f["infra_fail"]
    r2 = kubectl("-n", NS, "get", "pod", "-o", "json")
    if r2.returncode != 0:
        f["infra_fail"] = f"quota: cannot read namespace usage: {r2.stderr.strip()[:120]}"
        return False, f["infra_fail"]
    usage = defaultdict(int)
    for it in json.loads(r2.stdout).get("items", []):
        if ((it.get("status") or {}).get("phase")) in ("Succeeded", "Failed"):
            continue
        for k, v in _pod_requests(it).items():
            usage[k] += v
    hard = {f"requests.{k}": _qty_str(k, usage[k] + n * shape[k]) for k in shape}
    rq = {"apiVersion": "v1", "kind": "ResourceQuota",
          "metadata": {"name": f"perm-{case_id}-quota", "labels": {"perm.test/fault": case_id}},
          "spec": {"hard": hard}}
    rr = kubectl("-n", NS, "apply", "-f", "-", input=json.dumps(rq))
    if rr.returncode != 0:
        f["infra_fail"] = f"quota: create failed: {rr.stderr.strip()[:120]}"
        return False, f["infra_fail"]
    _FAULT_CLEANUPS.append(("quota", case_id))
    f["notes"].append(f"fault armed: quota +{n} pod(s) ({' '.join(f'{k}={v}' for k, v in sorted(hard.items()))})")
    print(f"    ⚡ {f['notes'][-1]}")
    return True, ""


def _fault_cordon(f, case_id, hdr):
    """CORDON: resolve target 'source' — the node hosting the pod the case acts
    on (the engine pod at the header `inst` index when present, else the case's
    first pod by name) — and cordon it. Uncordon is registered unconditionally
    before the cordon attempt is judged, so cleanup is failure-path safe."""
    r = kubectl("-n", NS, "get", "pod", "-l", f"ome.io/inferenceservice={case_id}", "-o", "json")
    if r.returncode != 0:
        f["infra_fail"] = f"cordon: cannot read case pods: {r.stderr.strip()[:120]}"
        return False, f["infra_fail"]
    try:
        items = sorted(json.loads(r.stdout).get("items", []),
                       key=lambda it: (it.get("metadata") or {}).get("name", ""))
    except Exception as e:
        f["infra_fail"] = f"cordon: cannot parse case pods: {e}"
        return False, f["infra_fail"]
    want_inst = str(hdr.get("inst", 0)).split(",")[0].strip()
    pick = None
    for it in items:
        lb = (it.get("metadata") or {}).get("labels") or {}
        if lb.get("component") == "engine" and lb.get("ome.io/instance-index", "0") == want_inst:
            pick = it
            break
    if pick is None and items:
        pick = items[0]
    node = ((pick or {}).get("spec") or {}).get("nodeName", "")
    if not node:
        f["infra_fail"] = "cordon: no node to cordon (no scheduled source pod)"
        return False, f["infra_fail"]
    _FAULT_CLEANUPS.append(("uncordon", node))   # registered BEFORE judging the cordon
    f["node"] = node   # handlers assert placement against the cordoned node
    # Marker label FIRST (so a cordon that lands without the label is impossible);
    # batch.py's sweep uncordons only nodes carrying it.
    kubectl("label", "node", node, f"{CORDON_LABEL}={case_id}", "--overwrite")
    rr = kubectl("cordon", node)
    if rr.returncode != 0:
        f["infra_fail"] = f"cordon {node} failed: {rr.stderr.strip()[:120]}"
        return False, f["infra_fail"]
    pname = ((pick.get("metadata") or {}).get("name", "?"))
    f["notes"].append(f"fault armed: cordon {node} (source pod {pname})")
    print(f"    ⚡ {f['notes'][-1]}")
    return True, ""


def _fault_setup(f, case_id, hdr):
    """Pre-trigger fault apparatus (quota / cordon), run once Initial Ready is
    reached and before the trigger. Returns (ok, msg); not-ok → INFRA-FAIL."""
    if not f or f["kind"] not in ("quota", "cordon"):
        return True, ""
    try:
        if f["kind"] == "quota":
            return _fault_quota_create(f, case_id)
        return _fault_cordon(f, case_id, hdr)
    except Exception as e:
        f["infra_fail"] = f"{f['kind']} setup error: {e}"
        return False, f["infra_fail"]


def _fault_pick_pod(pod_state, pods_per_inst, old_hashes):
    """Deterministic podkill victim: (1) gang topology → the leader pod of the
    lowest live instance index (heads the whole gang); (2) surge in flight (an
    alive new-revision pod exists) → the newest surge pod (latest
    creationTimestamp, name tie-break) — hits the in-flight rollout, not
    settled capacity; (3) otherwise the first live pod sorted by name."""
    def _idx(p):
        try:
            return int(p.get("inst", "0"))
        except (TypeError, ValueError):
            return 0
    live = [(n, p) for n, p in sorted(pod_state.items()) if p and p["alive"]]
    if not live:
        return None
    if any(v > 1 for v in pods_per_inst.values()):
        leaders = [(n, p) for n, p in live if p.get("runner") == "leader"]
        if leaders:
            return min(leaders, key=lambda x: (_idx(x[1]), x[0]))[0]
    if old_hashes is not None:
        surges = [(n, p) for n, p in live if p.get("hash") and p["hash"] not in old_hashes]
        if surges:
            return max(surges, key=lambda x: (x[1].get("created", ""), x[0]))[0]
    return live[0][0]


def _fault_fire(f, pod_state, pods_per_inst, old_hashes):
    """One-shot phase-fault action (latched by the caller's fired check)."""
    f["fired"] = True
    if f["kind"] == "podkill":
        victim = _fault_pick_pod(pod_state, pods_per_inst, old_hashes)
        if not victim:
            f["infra_fail"] = "podkill: no live pod to delete"
            return
        r = kubectl("-n", NS, "delete", "pod", victim, "--wait=false")
        if r.returncode != 0:
            f["infra_fail"] = f"podkill: delete {victim} failed: {r.stderr.strip()[:120]}"
            return
        f["notes"].append(f"fault fired: podkill {victim} ({f['param']})")
        print(f"    ⚡ {f['notes'][-1]}")
        return
    # ctrlrestart — ALWAYS gated (shared-cluster safety): without the explicit
    # env opt-in the fault is skipped with a note, never fired.
    if os.environ.get("OME_TEST_ALLOW_CTRL_RESTART") != "1":
        f["notes"].append("ⓘ fault ctrlrestart skipped — OME_TEST_ALLOW_CTRL_RESTART=1 not set")
        print(f"    {f['notes'][-1]}")
        return
    r = kubectl("-n", CTRL_NS, "get", "pod", "-l", CTRL_SELECTOR, "-o", "name")
    pods = [x for x in (r.stdout or "").split() if x]
    if r.returncode != 0 or not pods:
        f["infra_fail"] = f"ctrlrestart: no manager pod matches -l {CTRL_SELECTOR} in ns {CTRL_NS}"
        return
    rr = kubectl("-n", CTRL_NS, "delete", *pods, "--wait=false")
    if rr.returncode != 0:
        f["infra_fail"] = f"ctrlrestart: delete failed: {rr.stderr.strip()[:120]}"
        return
    f["notes"].append("fault fired: ctrlrestart " +
                      " ".join(p.split("/")[-1] for p in pods) + f" ({f['param']})")
    print(f"    ⚡ {f['notes'][-1]}")


def _fault_tick(ctx):
    """Phase-hook dispatcher for the in-flight faults (podkill/ctrlrestart),
    called every watch iteration; one firing per case (latched):
      settling  → immediately after the trigger was applied;
      rotating  → first observation of a surge/updated pod post-trigger;
      migrating → migration accept observed (_migrate sets ctx['fault_migrating'])."""
    f = ctx.get("fault")
    if not f or f["fired"] or f["kind"] not in ("podkill", "ctrlrestart"):
        return
    if ctx.get("trigger_ts") is None:
        return
    ph = f["param"]
    if ph == "settling":
        due = True
    elif ph == "rotating":
        old = ctx.get("old_hashes")
        due = old is not None and any(
            p and p["alive"] and ((p.get("hash") and p["hash"] not in old) or p.get("newanno"))
            for p in ctx["pod_state"].values())
    else:   # migrating
        due = bool(ctx.get("fault_migrating"))
    if due:
        _fault_fire(f, ctx["pod_state"], ctx["pods_per_inst"], ctx.get("old_hashes"))


def _fault_finalize(f, verdict, notes, checks):
    """Fold fault bookkeeping into the verdict: surface armed/fired notes, and
    route any apparatus error to the distinct INFRA-FAIL verdict — the harness
    broke, not the product (batch.py counts it separately from FAIL)."""
    if not f:
        return verdict
    for x in f["notes"]:
        notes.append(x)
        checks.append(f"  ⓘ {x}")
    if f["kind"] in ("podkill", "ctrlrestart") and not f["fired"]:
        checks.append(f"  ⓘ fault {f['kind']}:{f['param']} never reached its phase hook (not fired)")
    if f["infra_fail"]:
        verdict = "INFRA-FAIL"
        notes.append(f"INFRA-FAIL: {f['infra_fail']}")
        checks.append(f"  ✗ INFRA-FAIL (fault apparatus): {f['infra_fail']}")
    return verdict


def write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, peaks, final,
                 converged_at, policy, verdict, note, new_final):
    ts = int(time.time())
    feature, topo = case_category(case_id)
    out = HERE / "results" / RUN_TS / feature / topo / f"{case_id}-{ts}.tsv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        f.write(TSV_HEADER)
        f.write("\t".join(str(x) for x in [
            case_id, desc, Ne, Nd, Nr, ce, cd, cr,
            peaks["engine"], peaks["decoder"], peaks["router"],
            final["engine"], final["decoder"], final["router"],
            converged_at if converged_at else -1, policy or "none", verdict, note,
            new_final["engine"], new_final["decoder"], new_final["router"],
        ]) + "\n")
    print(f"  result: {out}")
    return out


def find_isvc(case_id):
    """Locate isvcs/<case_id>.yaml at any depth — supports the flat layout
    AND the feature-subfolder layout (isvcs/rollout/, isvcs/migration/, ...).
    The case-id (the filename stem) stays globally unique, so results/logs
    keep keying on it regardless of which feature folder the case lives in."""
    direct = HERE / "isvcs" / f"{case_id}.yaml"
    if direct.exists():
        return direct
    matches = sorted((HERE / "isvcs").glob(f"**/{case_id}.yaml"))
    return matches[0] if matches else None


def case_category(case_id):
    """(feature, topo) for results grouping — feature = the isvcs/ subfolder
    (rollout, migration, ...), topo = the topo= header tag. Mirrors the isvcs/
    taxonomy so results/ lines up with the case layout."""
    yf = find_isvc(case_id)
    if yf is None:
        return ("uncategorized", "unknown")
    rel = yf.relative_to(HERE / "isvcs")
    feature = rel.parts[0] if len(rel.parts) > 1 else "uncategorized"
    topo = "unknown"
    for tok in yf.read_text().splitlines()[0].split():
        if tok.startswith("topo="):
            topo = tok[len("topo="):]
            break
    return (feature, topo)


# ── Non-rollout run modes ────────────────────────────────────────────────────
# A case's header `mode=` selects the trigger/check mode. Default (no mode= or
# mode=rollout) is the surge-budget rollout path in run_case below — untouched.
# Other modes share the apply→wait-Ready→watch scaffolding but swap the Phase-2
# trigger and the verdict. Implemented: scaledown. Planned: migrate, kill,
# partition, rebump (each becomes a branch here on the same scaffolding).
MODE_HANDLERS = {"scaledown", "partition", "rebump", "migrate", "kill", "canary", "runtimerev",
                 "recreate", "compremove", "pairing"}   # modes with a real impl
TO_KEYS = {"engine": "to_e", "decoder": "to_d", "router": "to_r"}  # scaledown target replicas


# ── Case-tree integrity: duplicate-id guard + --validate-only ────────────────
# find_isvc resolves isvcs/**/<id>.yaml alphabetically-first, so a duplicate
# case id silently SHADOWS every other file with that id (scale/p97+p98 and
# rollout/p100 never ran until renamed to p188/p189/p500). run_case therefore
# hard-fails on any duplicate before any cluster op, and `run.py
# --validate-only [dir]` checks the whole tree — id uniqueness + the line-1
# header schema — with zero cluster interaction (no kubeconfig needed; kubectl
# is only ever spawned by the run paths, never by validation).


def collect_case_files(isvcs_dir=None):
    """Every case YAML under the case tree, keyed by case id (filename stem)."""
    root = Path(isvcs_dir) if isvcs_dir else HERE / "isvcs"
    cases = defaultdict(list)
    for f in sorted(root.glob("**/*.yaml")):
        cases[f.stem].append(f)
    return cases


def duplicate_id_errors(cases):
    return [f"duplicate case id {cid}: " + " AND ".join(str(p) for p in paths)
            + " — find_isvc() silently runs only the first"
            for cid, paths in sorted(cases.items()) if len(paths) > 1]


def _int_ge0(v):
    return isinstance(v, int) and v >= 0


def _int_list(v):
    try:
        return all(int(s) >= 0 for s in str(v).split(","))
    except ValueError:
        return False


def _numlike(v):
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False


# CRD pattern for spec.rollout.pairingProtocol (opaque cohort token).
_PAIRING_TOKEN_RE = re.compile(r"^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$")


def _pairing_token(v):
    """`clear` (remove the field) or a CRD-valid opaque token, <= 63 chars."""
    s = str(v)
    return s == "clear" or (len(s) <= 63 and bool(_PAIRING_TOKEN_RE.match(s)))


# Header schema = exactly what the runner consumes (parse_header + the hdr.get
# dispatches above). Enums lowered where run.py lowers; exact where it compares
# exact. Values arrive int-coerced when possible (parse_header semantics).
HDR_VALIDATORS = {
    "mode": lambda v: v == "rollout" or v in MODE_HANDLERS,
    "Ne": _int_ge0, "Nd": _int_ge0, "Nr": _int_ge0,
    "ceil_e": _int_ge0, "ceil_d": _int_ge0, "ceil_r": _int_ge0,
    "to_e": _int_ge0, "to_d": _int_ge0, "to_r": _int_ge0,
    "partition": _int_ge0,
    "mig": lambda v: v in {"Auto", "Surge", "Never"},
    "restart": lambda v: v in {"None", "RecreateInstance"},
    "kill": lambda v: v in {"leader", "worker"},
    "pause": lambda v: v in {"true", "freeze"},
    "inst": _int_list,
    "canary": lambda v: str(v).lower() in {"auto", "manual"},
    # Which canary unit's ladder to drive when the ISVC carries a group per unit
    # (router, or engine+decoder). The units not named are asserted idle.
    "unit": lambda v: str(v).lower() in {"router", "engine", "decoder"},
    "rollback": lambda v: v in (0, 1),
    "analysis": lambda v: str(v).lower() in {"pass", "breach", "inconclusive"},
    "health": lambda v: str(v).lower() in {"pass", "breach", "inconclusive"},
    "errorrate": _numlike,
    "oninconclusive": lambda v: str(v).lower() in {"hold", "rollback"},
    "failurelimit": _int_ge0,
    "expect": lambda v: v in {"reject", "roll", "noroll", "recover", "fail"},
    "reason": lambda v: bool(str(v)),
    "rt": lambda v: bool(str(v)),
    "edit": lambda v: v in {"material", "noop", "image", "resources"},
    "img": lambda v: bool(str(v)),
    "break": lambda v: v == "stuck",
    # Scenario knobs. during= times a destructive op against an in-flight one:
    # recreate deletes mid-rollout/mid-migration; migrate files its request
    # mid-rotate (both must complete; ordering unasserted — the product may
    # preempt the rollout); runtimerev edits the CSR mid-rotate.
    "during": lambda v: v in {"rollout", "migrate"},
    "remove": lambda v: v in {"decoder", "router"},   # compremove: component patched out
    "readd": lambda v: v == 1,    # compremove: re-apply the manifest, assert round-trip
    "dup": lambda v: v == 1,      # migrate: re-file the same request UUID (dedup contract)
    "hint": lambda v: v == 1,     # migrate: carry hint_target_nodes (soft placement assert)
    "co": lambda v: v in {"none", "rolling", "ratio", "percomp", "seq", "bluegreen"},
    # Pairing-mode knobs (mode=pairing). The trigger is a patch of
    # spec.rollout.pairingProtocol: proto= is the target of the transition
    # (`clear` removes the field); proto2= re-patches mid-roll (the X-2 shape);
    # hold=1 asserts a rolloutHold gate=Pairing observation mid-transition;
    # pkill=1 deletes the last old-cohort decoder pod mid-transition;
    # scale_e=/scale_d= re-target a Component's replicas mid-transition.
    "proto": _pairing_token,
    "proto2": _pairing_token,
    "hold": lambda v: v == 1,
    "pkill": lambda v: v == 1,
    "scale_e": lambda v: isinstance(v, int) and v >= 1,
    "scale_d": lambda v: isinstance(v, int) and v >= 1,
    "topo": lambda v: v in {"engine", "pd", "engine-mn", "pd-mn"},
    "infeasible": lambda v: v == 1,
    "notimpl": lambda v: v == 1,
    # GitHub issue number a notimpl=1 case is parked on. REQUIRED with notimpl:
    # an XFAIL with no tracked issue is invisible debt — nothing re-examines it
    # until the parked bug fails for real.
    "issue": lambda v: isinstance(v, int) and v > 0,
    "floor_hard": lambda v: v == 1,   # assert the instantaneous floor min (no debounce)
    "floor_soft": lambda v: v == 1,   # opt out of the (default-hard) capacity floor
    "order_soft": lambda v: v == 1,   # opt out of surge-before-drain (a fault, e.g.
    #                                   podkill:rotating, deletes a pod → drain with no
    #                                   preceding surge; informational, not a FAIL)
    # Opt out of the continuous invariants (A=status-truth, B=endpoints-
    # membership, C=pair-alive). REQUIRES an adjacent '# reason: <why>' comment
    # on line 2 of the case file — validate-only rejects an inv_off without one.
    "inv_off": lambda v: v in {"A", "B", "C", "all"},
    # Scope a notimpl parking to a continuous invariant: a sustained violation
    # of the NAMED invariant is the tracked bug itself, so it records as the
    # case's XFAIL instead of hardening the verdict to FAIL. The invariant is
    # still evaluated and printed (unlike inv_off, nothing is blinded), and a
    # violation of any OTHER invariant fails hard as always. Requires
    # notimpl=1 (whose issue= names the bug) + the line-2 '# reason:' comment.
    "inv_xfail": lambda v: v in {"A", "B", "C", "all"},
    # Fault injection: fault=<kind>:<param> — quota:<n≥0>, podkill:<phase>,
    # ctrlrestart:<phase> (phase ∈ FAULT_PHASES), cordon:<target ∈ FAULT_TARGETS>.
    "fault": _fault_ok,
    # Fault-case contract knobs. blocked=1 (rollout/scaledown): the fault is
    # expected to HOLD the operation — assert parked-no-churn + capacity floor
    # instead of convergence. expect=fail (migrate): the request must end
    # terminal Failed BOUNDED while the source keeps serving and the request is
    # consumed. lift=1 (migrate + fault=quota): after the bounded-failure
    # contract passes, lift the quota and assert a follow-up request completes.
    "blocked": lambda v: v == 1,
    "lift": lambda v: v == 1,
}
HDR_REQUIRED = ("Ne", "Nd", "Nr", "ceil_e", "ceil_d", "ceil_r")


def validate_case_file(path):
    """Line-1 header + identity checks for one case file; returns error strings.

    The k=v field block is recovered by walking the header's tokens BACKWARD
    from the end: stop at the first token that is not key=value with a KNOWN,
    not-yet-seen key. The free-text description also contains k=v-looking words
    (maxSurge=2, N=8, even mode=Auto in a migrate desc) which must not be
    schema-checked; last-wins matches parse_header's forward overwrite. A typo'd
    KEY inside the real block stops the walk and surfaces as missing-required-
    key errors — loud, if indirectly named.
    """
    errs = []
    text = path.read_text()
    first = (text.splitlines() or [""])[0]
    cid = path.stem
    if not first.startswith("#"):
        return [f"{path}:1: missing '# <id>: <desc> k=v ...' header comment"]
    if not first.lstrip("#").strip().startswith(f"{cid}:"):
        errs.append(f"{path}:1: header id does not match filename stem '{cid}'")
    fields = {}
    for tok in reversed(first.lstrip("#").strip().split()):
        if tok.startswith("(") or "=" not in tok:
            break
        k, _, v = tok.partition("=")
        if k not in HDR_VALIDATORS or k in fields:
            break   # description territory (or a desc token repeating a real key)
        try:
            val = int(v)
        except ValueError:
            val = v
        fields[k] = val
        if not HDR_VALIDATORS[k](val):
            errs.append(f"{path}:1: malformed header value {k}={v}")
    for k in HDR_REQUIRED:
        if k not in fields:
            errs.append(f"{path}:1: header missing {k}=")
    if "inv_off" in fields:
        # An invariant opt-out is an assertion hole — it must name why, on the
        # line adjacent to the header, so the reason travels with the case.
        second = text.splitlines()[1] if len(text.splitlines()) > 1 else ""
        if not re.match(r"^\s*#\s*reason:\s*\S", second):
            errs.append(f"{path}:2: inv_off={fields['inv_off']} requires an adjacent '# reason: <why>' comment on line 2")
    if fields.get("notimpl") and "issue" not in fields:
        # A notimpl XFAIL is parked debt on a KNOWN BUG — it must link the
        # tracking issue or the debt is invisible until it fails for real.
        errs.append(f"{path}:1: notimpl=1 requires issue=<github issue number> — an XFAIL without a tracked issue is invisible debt")
    if "inv_xfail" in fields:
        # An expected invariant violation is parking, and parking needs the
        # tracked issue; the rationale must travel with the case like every
        # other invariant-scope change.
        if not fields.get("notimpl"):
            errs.append(f"{path}:1: inv_xfail={fields['inv_xfail']} requires notimpl=1 issue=<n> — an expected invariant violation is parked debt")
        second = text.splitlines()[1] if len(text.splitlines()) > 1 else ""
        if not re.match(r"^\s*#\s*reason:\s*\S", second):
            errs.append(f"{path}:2: inv_xfail={fields['inv_xfail']} requires an adjacent '# reason: <why>' comment on line 2")
    if fields.get("infeasible"):
        # Infeasible-by-design must name WHY the config is unsatisfiable, on
        # the line adjacent to the header, so the rationale travels with the
        # case and can be re-audited when gate semantics change.
        second = text.splitlines()[1] if len(text.splitlines()) > 1 else ""
        if not re.match(r"^\s*#\s*reason:\s*\S", second):
            errs.append(f"{path}:2: infeasible=1 requires an adjacent '# reason: <why unsatisfiable>' comment on line 2")
    mode = fields.get("mode", "rollout")
    if mode == "scaledown" and not any(k in fields for k in ("to_e", "to_d", "to_r")):
        errs.append(f"{path}:1: mode=scaledown needs a to_e/to_d/to_r target")
    if mode == "runtimerev" and "rt" not in fields:
        errs.append(f"{path}:1: mode=runtimerev needs rt=<dedicated CSR name>")
    if fields.get("edit") == "image" and "img" not in fields:
        errs.append(f"{path}:1: edit=image needs img=<image-ref>")
    if mode == "compremove" and "remove" not in fields:
        errs.append(f"{path}:1: mode=compremove needs remove=<component>")
    if "readd" in fields and mode != "compremove":
        errs.append(f"{path}:1: readd=1 only applies to mode=compremove")
    if "during" in fields and fields["during"] not in {"recreate": {"rollout", "migrate"},
                                                       "migrate": {"rollout"},
                                                       "runtimerev": {"rollout"}}.get(mode, set()):
        errs.append(f"{path}:1: during={fields['during']} not supported for mode={mode}")
    if ("dup" in fields or "hint" in fields) and mode != "migrate":
        errs.append(f"{path}:1: dup=/hint= are migrate-mode knobs")
    if fields.get("blocked") and "fault" not in fields:
        errs.append(f"{path}:1: blocked=1 requires a fault= header")
    if mode == "pairing" and "proto" not in fields:
        errs.append(f"{path}:1: mode=pairing needs proto=<token|clear> (the transition target)")
    for k in ("proto", "proto2", "hold", "pkill", "scale_e", "scale_d"):
        if k in fields and mode != "pairing":
            errs.append(f"{path}:1: {k}= is a pairing-mode knob (mode=pairing)")
    if mode == "pairing":
        if "proto2" in fields and fields["proto2"] == fields.get("proto"):
            errs.append(f"{path}:1: proto2= must differ from proto= (a mid-roll re-target)")
        if fields.get("pkill") and fields.get("Nd", 0) == 0:
            errs.append(f"{path}:1: pkill=1 kills the last old-cohort decoder — needs Nd>0")
        if str(fields.get("canary", "")).lower() == "manual":
            errs.append(f"{path}:1: mode=pairing drives canary groups auto-only (canary=manual needs the promote pump)")
        if fields.get("expect") == "reject" and any(k in fields for k in ("proto2", "pkill", "scale_e", "scale_d", "pause", "hold")):
            errs.append(f"{path}:1: expect=reject with mode=pairing asserts the protocol PATCH is denied — no rotation-phase knobs apply")
    if fields.get("expect") == "fail" and (mode != "migrate" or "fault" not in fields):
        errs.append(f"{path}:1: expect=fail is the migrate-mode bounded-failure contract — needs mode=migrate + fault=")
    if fields.get("lift") and not (mode == "migrate" and str(fields.get("fault", "")).startswith("quota:")):
        errs.append(f"{path}:1: lift=1 requires mode=migrate + fault=quota:<n>")
    if not re.search(rf"\bname:\s*{re.escape(cid)}\b", text):
        errs.append(f"{path}:1: no document with metadata name '{cid}' (run.py gets the ISVC by case id)")
    return errs


def validate_cases(isvcs_dir=None):
    """--validate-only: load EVERY case file, check id uniqueness + header
    schema; nonzero exit on any problem. Pure filesystem — no cluster access."""
    root = Path(isvcs_dir) if isvcs_dir else HERE / "isvcs"
    cases = collect_case_files(root)
    if not cases:
        print(f"ERROR: no case YAMLs under {root}", file=sys.stderr)
        return 1
    errs = duplicate_id_errors(cases)
    for cid in sorted(cases):
        for path in cases[cid]:
            errs.extend(validate_case_file(path))
    if errs:
        for e in errs:
            print(e, file=sys.stderr)
        nfiles = sum(len(v) for v in cases.values())
        print(f"\nvalidate-only: FAIL — {len(errs)} problem(s) across {nfiles} case file(s)", file=sys.stderr)
        return 1
    nfiles = sum(len(v) for v in cases.values())
    print(f"validate-only: OK — {nfiles} case files, {len(cases)} unique case ids, headers valid ({root})")
    return 0


WATCH_MAX_RESTARTS = 50  # reconnect kubectl -w on EOF up to this many times before giving up


class _WatchStream:
    """kubectl -w pipe drained by a daemon thread into a queue.

    A select()+buffered readline() loop cannot drain this pipe: readline()
    pulls a chunk into Python's userspace buffer, but select() polls the OS fd,
    so after one line the loop sees "no more data" while whole events sit
    unread in the buffer. Under concurrency the readiness (MODIFIED) events
    pile up and pod_state falls minutes behind — fully-Ready Instances read as
    not-Ready and cases false-time-out. The pump thread does a simple blocking
    `for line in stdout`, so the pipe is always fully drained into an in-memory
    queue; loops read from the queue (instant when data is present, blocking
    only when genuinely idle)."""
    _EOF = object()

    def __init__(self, proc):
        self.proc = proc
        self.q = queue.Queue()
        self._t = threading.Thread(target=self._pump, daemon=True)
        self._t.start()

    def _pump(self):
        try:
            for line in self.proc.stdout:
                self.q.put(line)
        except Exception:
            pass
        self.q.put(self._EOF)

    def readline(self, timeout=1.0):
        """Next line (str) — blocking up to `timeout` only when the queue is
        empty. Returns '' on EOF, None on timeout."""
        try:
            item = self.q.get(timeout=timeout)
        except queue.Empty:
            return None
        return "" if item is self._EOF else item

    def terminate(self):
        try:
            self.proc.terminate()
        except Exception:
            pass

    def wait(self, timeout=None):
        return self.proc.wait(timeout=timeout)

    def kill(self):
        try:
            self.proc.kill()
        except Exception:
            pass


def start_watcher(case_id):
    """Pod event stream for a case (shared by the rollout path and all modes).
    Wrapped in _WatchStream so a pump thread keeps the pipe drained."""
    proc = subprocess.Popen(
        ["kubectl", "-n", NS, "get", "pod", "-l", f"ome.io/inferenceservice={case_id}",
         "-w", "-o", "json", "--output-watch-events"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        env={**os.environ, "KUBECONFIG": KUBECONFIG}, bufsize=1, text=True,
    )
    return _WatchStream(proc)


def _stop_watcher(ctx):
    w = ctx.get("watcher")
    if not w:
        return
    w.terminate()
    try:
        w.wait(timeout=5)
    except subprocess.TimeoutExpired:
        w.kill()


def _apply_event(ctx, ev):
    obj = ev.get("object", {})
    meta = obj.get("metadata", {})
    name = meta.get("name", "?")
    labels = meta.get("labels", {}) or {}
    annos = meta.get("annotations", {}) or {}
    cstats = obj.get("status", {}).get("containerStatuses") or []
    ready = any(cs.get("name") == "ome-container" and cs.get("ready") for cs in cstats)
    comp = labels.get("component", "?")
    rev = labels.get(REV_LABEL, "")
    deletion = meta.get("deletionTimestamp")
    # Surge-before-drain ordering: record (post-trigger, SurgeThenDrain only —
    # RecreatePod also reads as mode "surge" but legitimately drains first) the
    # cumulative OLD-rev pods draining and NEW-rev pods Ready per component, so a
    # handler can assert a drain never led its Ready surge replacement. Captured
    # here (not from pod_state) because a DELETED event drops the record + its rev.
    if ctx.get("trigger_ts") is not None and ctx.get("old_hashes") is not None \
            and ctx["info"].get("strat", {}).get(comp) == "SurgeThenDrain":
        is_new = bool(rev and rev not in ctx["old_hashes"])
        nr = ctx.setdefault("new_ready", defaultdict(set))
        od = ctx.setdefault("old_drained", defaultdict(set))
        if is_new and ready:
            nr[comp].add(name)
        if (not is_new) and rev and (ev.get("type") == "DELETED" or deletion is not None):
            od[comp].add(name)
        mu_pods = resolve_max_unavailable(ctx["info"].get("max_unavail", {}).get(comp, 0),
                                          ctx["N"].get(comp, 0)) * ctx["pods_per_inst"].get(comp, 1)
        breach = len(od[comp]) - len(nr[comp]) - mu_pods
        ov = ctx.setdefault("order_viol", {})
        if breach > ov.get(comp, 0):
            ov[comp] = breach
    if ev.get("type") == "DELETED":
        if meta.get("uid") and ctx.get("inv"):
            ctx["inv"]["dead_uids"].add(meta["uid"])   # stale-endpoint invariant
        ctx["pod_state"][name] = None
    else:
        ctx["pod_state"][name] = {
            "comp": comp,
            "inst": labels.get("ome.io/instance-index", "0"),
            "runner": labels.get("ome.io/runner", "default"),
            "ord": labels.get("ome.io/pod-ordinal", "0"),
            "alive": deletion is None,
            "ready": ready, "hash": rev,
            "pairing": labels.get(PAIRING_LABEL, ""),
            "serving": _pod_serving(obj),
            "uid": meta.get("uid", ""),
            "newanno": annos.get(TRIGGER_ANNO) == TRIGGER_VAL,
            "node": (obj.get("spec") or {}).get("nodeName", ""),
            "created": meta.get("creationTimestamp", ""),
        }
        if rev:
            ctx.setdefault("seen_hashes", set()).add(rev)   # every revision-hash ever observed


def _read_one(ctx, timeout=1.0):
    """DRAIN all currently-available watch events (block up to `timeout` for the
    first, then read the rest non-blocking), fold each into pod_state, then
    refresh peaks + the aggregate once. A single read per call fell minutes
    behind an N-pod startup burst (ADDED×N + many condition MODIFIEDs) — under
    concurrency the watcher's pod_state lagged reality so far that fully-Ready
    Instances still read not-Ready and cases false-timed-out. Draining keeps
    pod_state at most one poll-interval stale regardless of event volume."""
    w = ctx["watcher"]
    wait = timeout
    while True:
        line = w.readline(wait)   # str | "" (EOF) | None (queue empty / timeout)
        if line is None:
            break                 # queue drained for this cycle
        wait = 0.0                # first read blocks up to timeout; the rest drain the backlog
        if line == "":
            # EOF: kubectl -w exited (apiserver watch timeout / network blip). Do
            # NOT treat as terminal — a mid-roll watch drop would otherwise end the
            # case on partial state and masquerade as a ROTATE_TIMEOUT. Reconnect;
            # kubectl re-emits the current pods as ADDED so pod_state refreshes.
            # Cap restarts to avoid a hot loop if the case is genuinely gone.
            if ctx.get("watch_restarts", 0) < WATCH_MAX_RESTARTS:
                ctx["watch_restarts"] = ctx.get("watch_restarts", 0) + 1
                time.sleep(0.5)
                try:
                    w.wait(timeout=1)
                except Exception:
                    pass
                ctx["watcher"] = start_watcher(ctx["case_id"])
                w = ctx["watcher"]
            else:
                ctx["watch_lost"] = True
                ctx["closed"] = True
            break
        line = line.strip()
        if not line:
            continue
        ev = None
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            ctx["buf"] += line
            try:
                ev = json.loads(ctx["buf"]); ctx["buf"] = ""
            except json.JSONDecodeError:
                ev = None
        if ev and ev.get("type") != "BOOKMARK" and ev.get("object"):
            _apply_event(ctx, ev)
    meas = {}
    agg = aggregate(ctx["pod_state"], ctx["old_hashes"], ctx["modes"], ctx["pods_per_inst"], measures=meas)
    for c in COMPS:
        a = agg.get(c, (0, 0, 0))[0]
        if a > ctx["peaks"][c]:
            ctx["peaks"][c] = a
    ctx["agg"] = agg
    ctx["measures"] = meas
    # Post-trigger (past the INIT→ROTATING boundary grace) accumulate gang/floor
    # spans over the components the handler flagged for tracking (defaults to all
    # configured). Handlers set ctx["track_comps"] before their watch loop.
    if ctx.get("trigger_ts") is not None and time.time() - ctx["trigger_ts"] >= MINREADY_GRACE_S:
        _track_gang_and_floor(ctx, ctx.get("track_comps") or sorted(ctx["configured"]))
    # Fault phase hooks (podkill/ctrlrestart): one latched firing per case.
    _fault_tick(ctx)
    # Continuous invariants: one observation per INV_TICK_S (self-rate-limited),
    # for the whole case duration — INIT through the handler's last watch read.
    _inv_tick(ctx.get("inv"), ctx["pod_state"], ctx["pods_per_inst"])
    if ctx.get("trigger_ts") is not None:                       # per-tick progress (post-trigger) for diagnosis
        snap = tuple(agg.get(c, (0, 0, 0)) for c in COMPS)
        if snap != ctx.get("last_snap"):
            ctx["last_snap"] = snap
            t = int(time.time() - ctx["trigger_ts"])
            parts = "  ".join(f"{c[:3]} {agg.get(c, (0, 0, 0))[0]}a/{agg.get(c, (0, 0, 0))[1]}r/{agg.get(c, (0, 0, 0))[2]}n"
                              for c in COMPS if ctx["N"][c] > 0)
            print(f"    [t={t:3d}s] {parts}  peak {ctx['peaks']['engine']}/{ctx['peaks']['decoder']}/{ctx['peaks']['router']}")
    return agg


def _bumped(ctx):
    """Components the trigger acts on: the coordination group members, else engine."""
    members = set(ctx["info"]["members"]) & ctx["configured"]
    return members or ({"engine"} & ctx["configured"])


def _bump_rev(ctx, bumped, val=TRIGGER_VAL, extra=None):
    # extra: additional pod annotations set on the bumped components (analysis
    # cases use it to carry the canary revision's health, e.g. perm.test/error-rate
    # — one patch both rolls the revision AND flips the canary pods' emitted metric).
    anns = {TRIGGER_ANNO: val, "ome.io/deploymentMode": "OMENative"}
    if extra:
        anns.update(extra)
    patch = {"spec": {c: {"annotations": dict(anns)} for c in sorted(bumped)}}
    rr = kubectl("-n", NS, "patch", "isvc", ctx["case_id"], "--type=merge", "-p", json.dumps(patch))
    ctx["trigger_ts"] = time.time()
    return rr.returncode == 0


def _serving_instance_counts(pod_state, pods_per_inst):
    """Return fully Ready+serving Instance counts by Component."""
    by_instance = defaultdict(list)
    for pod in pod_state.values():
        if pod and pod.get("alive"):
            by_instance[(pod.get("comp"), pod.get("inst", "0"))].append(pod)
    counts = defaultdict(int)
    for (component, _), pods in by_instance.items():
        expected = max(1, pods_per_inst.get(component, 1))
        if len(pods) >= expected and sum(
                1 for pod in pods if pod.get("ready") and pod.get("serving")) >= expected:
            counts[component] += 1
    return dict(counts)


def _initial_ir_settled(items, expected, configured):
    """Require authoritative IR status to describe a steady initial fleet."""
    by_component = defaultdict(list)
    for item in items:
        component = (item.get("spec") or {}).get("component")
        if component in configured:
            by_component[component].append(item)

    for component in sorted(configured):
        matches = by_component.get(component, [])
        if len(matches) != 1:
            return False, f"{component}: observed {len(matches)} InferenceReplicas, want 1"
        item = matches[0]
        metadata = item.get("metadata") or {}
        status = item.get("status") or {}
        desired = expected[component]
        try:
            generation = int(metadata.get("generation") or 0)
            observed_generation = int(status.get("observedGeneration") or 0)
        except (TypeError, ValueError):
            return False, f"{component}: generation status is malformed"
        if generation > 0 and observed_generation < generation:
            return False, f"{component}: observedGeneration {observed_generation} < {generation}"
        for field in ("replicas", "readyReplicas", "servingReplicas",
                      "updatedReplicas", "updatedReadyReplicas"):
            try:
                actual = int(status.get(field) or 0)
            except (TypeError, ValueError):
                return False, f"{component}: {field} is malformed"
            if actual != desired:
                return False, f"{component}: {field} {actual}/{desired}"
        current = status.get("currentRevision") or ""
        update = status.get("updateRevision") or ""
        if not current or current != update:
            return False, f"{component}: currentRevision and updateRevision have not converged"
        try:
            instances = irstatus.instance_statuses(status)
        except irstatus.DecodeError as e:
            return False, f"{component}: per-Instance status undecodable: {e}"
        if not isinstance(instances, list) or len(instances) != desired:
            actual = len(instances) if isinstance(instances, list) else 0
            return False, f"{component}: instanceStatuses {actual}/{desired}"
        expected_indexes = set(range(desired))
        try:
            indexes = {int(instance.get("index", -1)) for instance in instances}
        except (AttributeError, TypeError, ValueError):
            return False, f"{component}: Instance indexes are malformed"
        if indexes != expected_indexes:
            return False, f"{component}: Instance indexes have not converged"
        for instance in instances:
            if instance.get("phase") != "Ready" or instance.get("operation") is not None:
                return False, f"{component}/{instance.get('index', '?')}: Instance is not settled"
    return True, "settled"


def _initial_rollout_settled(pod_state, pods_per_inst, expected, configured, ir_items):
    """Require both live serving pods and settled controller state."""
    serving = _serving_instance_counts(pod_state, pods_per_inst)
    for component in sorted(configured):
        if serving.get(component, 0) != expected[component]:
            return False, f"{component}: serving {serving.get(component, 0)}/{expected[component]}"
    return _initial_ir_settled(ir_items, expected, configured)


def _fetch_initial_ir_items(case_id):
    r = kubectl("-n", NS, "get", "inferencereplicas", "-l",
                f"ome.io/inferenceservice={case_id}", "-o", "json")
    if r.returncode != 0:
        return None
    try:
        items = json.loads(r.stdout).get("items")
    except Exception:
        return None
    return items if isinstance(items, list) else None


def _setup_and_ready(case_id, yaml_file, hdr):
    """Apply runtime + ISVC, wait for initial settlement, and snapshot state.

    Returns (ctx, None) or (None, (state, msg)).
    """
    Ne, Nd, Nr = hdr.get("Ne", 0), hdr.get("Nd", 0), hdr.get("Nr", 0)
    N = {"engine": Ne, "decoder": Nd, "router": Nr}
    configured = {c for c in COMPS if N[c] > 0}
    apply_runtime_fixtures()
    r = kubectl("apply", "--validate=false", "-n", NS, "-f", str(yaml_file))
    if r.returncode != 0:
        msg = (r.stderr.strip().splitlines() or ["kubectl apply failed"])[-1]
        print(f"  APPLY FAILED: {msg[:200]}")
        return None, ("APPLY_FAIL", msg[:200])
    info = get_spec_info(case_id)
    ctx = {
        "case_id": case_id, "N": N, "configured": configured, "info": info, "hdr": hdr,
        "pods_per_inst": {c: 1 + info["workers"].get(c, 0) for c in COMPS},
        "modes": {c: ("inplace" if info["strat"].get(c) in IN_PLACE_TYPES else "surge") for c in configured},
        "pod_state": {}, "peaks": {"engine": 0, "decoder": 0, "router": 0}, "agg": {},
        "buf": "", "closed": False, "old_hashes": None, "old_uids": {}, "old_nodes": {}, "seen_hashes": set(),
        "trigger_ts": None, "converged_at": None, "watcher": start_watcher(case_id),
        "inv": _inv_new(case_id, hdr),
        "fault": _fault_new(case_id, hdr),
    }
    budget = min(MAX_BUDGET_S, INIT_BASE_S + INIT_PER_POD_S * sum(N[c] for c in configured))
    print(f"\n  Phase 1: waiting for initial settlement (need {Ne}/{Nd}/{Nr}; ≤{budget}s)")
    t0 = time.time()
    last_wait = None
    while time.time() - t0 < budget and not ctx["closed"]:
        agg = _read_one(ctx)
        if all(agg.get(c, (0, 0, 0))[1] == N[c] for c in configured):
            ir_items = _fetch_initial_ir_items(case_id)
            settled, wait = _initial_rollout_settled(
                ctx["pod_state"], ctx["pods_per_inst"], N, configured, ir_items or [])
            if not settled:
                if wait != last_wait:
                    print(f"  … Pods are container-ready; waiting for settled serving state ({wait})")
                    last_wait = wait
                continue
            alive = [p for p in ctx["pod_state"].values() if p and p["alive"]]
            ctx["old_hashes"] = {p["hash"] for p in alive if p.get("hash")}
            for c in configured:
                ctx["old_uids"][c] = {p["uid"] for p in alive if p["comp"] == c and p.get("uid")}
            ctx["old_nodes"] = {(p["comp"], p["inst"]): p.get("node", "") for p in alive}
            print(f"  ✓ Initial fleet settled after {int(time.time() - t0)}s; old rev {sorted(ctx['old_hashes']) or '(none)'}")
            # Pre-trigger fault apparatus (quota / cordon) arms once Ready.
            ok_f, fmsg = _fault_setup(ctx["fault"], case_id, hdr)
            if not ok_f:
                _stop_watcher(ctx)
                kubectl("delete", "-n", NS, "-f", str(yaml_file), "--wait=false")
                print(f"  ✗ FAULT SETUP FAILED: {fmsg}")
                return None, ("INFRA-FAIL", fmsg)
            return ctx, None
    _stop_watcher(ctx)
    kubectl("delete", "-n", NS, "-f", str(yaml_file), "--wait=false")   # don't leak the ISVC
    print(f"  ✗ INIT TIMEOUT after {budget}s")
    return None, ("INIT_TIMEOUT", "never reached initial settled state")


def _write_fail(case_id, desc, hdr, state, msg):
    Ne, Nd, Nr = hdr.get("Ne", 0), hdr.get("Nd", 0), hdr.get("Nr", 0)
    ce, cd, cr = hdr.get("ceil_e", Ne), hdr.get("ceil_d", Nd), hdr.get("ceil_r", Nr)
    z = {"engine": 0, "decoder": 0, "router": 0}
    d = {"engine": "-", "decoder": "-", "router": "-"}
    print(f"\n=== VERDICT: {state} ===  {msg}")
    # Tear down the ISVC on the fail path too. Without this a TRIGGER_FAIL /
    # APPLY_FAIL case leaks its ISVC (a big N=8 gang = many pods) which squats
    # cluster capacity until batch.py's end-of-run sweep — hours on a long run
    # (the p180 "4 hours" was its failed gang sitting idle, not a slow case).
    # Delete the single named ISVC (no --all, no label selector); a no-op when
    # the apply never landed. Guard on a concrete name so an empty case_id can
    # never issue a broad delete.
    if case_id:
        # Teardown budget scales with the REAL pod count (N × pods-per-Instance),
        # so read worker sizes from the live spec BEFORE deleting; when the ISVC
        # never landed, get_spec_info degrades to workers={} → 1 pod/Instance.
        winfo = get_spec_info(case_id)
        total_pods = sum(n * (1 + winfo["workers"].get(c, 0))
                         for c, n in (("engine", Ne), ("decoder", Nd), ("router", Nr)))
        kubectl("delete", "-n", NS, "isvc", case_id, "--wait=false", check=False)
        # Even a failed case's cleanup must converge (fault cases included);
        # survivors are appended to the recorded note.
        tnotes, tchecks = [], []
        teardown_check(case_id, total_pods, state, tnotes, tchecks)
        for line in tchecks:
            print(line)
        if tnotes:
            msg = f"{msg}; {'; '.join(tnotes)}"
    write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, z, d, None, None, state, msg, z)
    return 0


def soften_expected_fail(hdr, verdict, checks):
    """FAIL → XFAIL for parked cases. A notimpl XFAIL carries its linked issue
    in the verdict itself (e.g. 'XFAIL (known bug: #<issue>)') so every sweep
    summary line names the tracked bug; infeasible stays a plain XFAIL — its
    by-design rationale lives on the case's line-2 '# reason:' comment and in
    batch.py's PARKED DEBT table."""
    if verdict != "FAIL":
        return verdict
    if hdr.get("notimpl"):
        ref = f"#{hdr['issue']}" if hdr.get("issue") else "UNLINKED"
        checks.append(f"  ⓘ marked notimpl (known bug: {ref}) — expected to fail until the tracked issue is fixed; recorded XFAIL, not a regression")
        return f"XFAIL (known bug: {ref})"
    if hdr.get("infeasible"):
        checks.append("  ⓘ marked infeasible — an unsatisfiable config (expected); recorded XFAIL, not a regression")
        return "XFAIL"
    return verdict


def _print_failure_snapshot(case_id, hdr, ctx):
    """Print compact controller state for actionable canary/migration failures."""
    if hdr.get("mode") not in ("canary", "migrate"):
        return

    isvc_result = kubectl("-n", NS, "get", "isvc", case_id, "-o", "json")
    ir_result = kubectl("-n", NS, "get", "inferencereplicas", "-l",
                        f"ome.io/inferenceservice={case_id}", "-o", "json")
    pod_result = kubectl("-n", NS, "get", "pods", "-l",
                         f"ome.io/inferenceservice={case_id}", "-o", "json")
    try:
        isvc = json.loads(isvc_result.stdout) if isvc_result.returncode == 0 else {}
    except Exception:
        isvc = {}
    try:
        irs = (json.loads(ir_result.stdout).get("items", [])
               if ir_result.returncode == 0 else [])
    except Exception:
        irs = []
    try:
        pods = (json.loads(pod_result.stdout).get("items", [])
                if pod_result.returncode == 0 else [])
    except Exception:
        pods = []

    status = isvc.get("status") or {}
    components = {}
    for component, component_status in sorted((status.get("components") or {}).items()):
        lifecycle = component_status.get("lifecycle") or {}
        components[component] = {
            "rolloutPhase": component_status.get("rolloutPhase"),
            "traffic": component_status.get("traffic"),
            "currentRevision": lifecycle.get("currentRevision"),
            "updateRevision": lifecycle.get("updateRevision"),
            "readyReplicas": lifecycle.get("readyReplicas"),
            "updatedReplicas": lifecycle.get("updatedReplicas"),
        }

    replicas = []
    for ir in sorted(irs, key=lambda item: str((item.get("spec") or {}).get("component", ""))):
        ir_status = ir.get("status") or {}
        replicas.append({
            "component": (ir.get("spec") or {}).get("component"),
            "generation": (ir.get("metadata") or {}).get("generation"),
            "observedGeneration": ir_status.get("observedGeneration"),
            "currentRevision": ir_status.get("currentRevision"),
            "updateRevision": ir_status.get("updateRevision"),
            "conditions": ir_status.get("conditions"),
            "migrations": ir_status.get("migrations"),
            "instances": [
                {
                    "index": instance.get("index"),
                    "phase": instance.get("phase"),
                    "incarnation": instance.get("incarnation"),
                    "runningRevision": instance.get("runningRevision"),
                    "targetRevision": instance.get("targetRevision"),
                    "operation": instance.get("operation"),
                    "lastFailure": instance.get("lastFailure"),
                }
                for instance in _instance_rows(ir_status)
            ],
        })

    pod_revisions = defaultdict(lambda: defaultdict(lambda: {"total": 0, "ready": 0}))
    for pod in ctx.get("pod_state", {}).values():
        if not pod or not pod.get("alive"):
            continue
        bucket = pod_revisions[str(pod.get("comp"))][pod.get("hash") or "<empty>"]
        bucket["total"] += 1
        bucket["ready"] += int(bool(pod.get("ready")))

    live_pod_revisions = defaultdict(
        lambda: defaultdict(lambda: {"total": 0, "podReady": 0, "containersReady": 0}))
    not_ready_pods = []
    for pod in pods:
        metadata = pod.get("metadata") or {}
        labels = metadata.get("labels") or {}
        pod_status = pod.get("status") or {}
        conditions = {
            condition.get("type"): condition.get("status")
            for condition in pod_status.get("conditions", [])
            if condition.get("type")
        }
        container_statuses = pod_status.get("containerStatuses") or []
        pod_ready = conditions.get("Ready") == "True"
        containers_ready = bool(container_statuses) and all(
            container.get("ready") for container in container_statuses)
        component = labels.get("component") or "<empty>"
        revision = labels.get(REV_LABEL) or "<empty>"
        bucket = live_pod_revisions[component][revision]
        bucket["total"] += 1
        bucket["podReady"] += int(pod_ready)
        bucket["containersReady"] += int(containers_ready)
        if not pod_ready:
            not_ready_pods.append({
                "name": metadata.get("name"),
                "component": component,
                "instance": labels.get("ome.io/instance-index"),
                "runner": labels.get("ome.io/runner"),
                "revision": revision,
                "phase": pod_status.get("phase"),
                "deleting": bool(metadata.get("deletionTimestamp")),
                "conditions": conditions,
            })

    snapshot = {
        "observedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "canary": status.get("canary"),
        "components": components,
        "inferenceReplicas": replicas,
        "podRevisions": {
            component: dict(sorted(revisions.items()))
            for component, revisions in sorted(pod_revisions.items())
        },
        "livePodRevisions": {
            component: dict(sorted(revisions.items()))
            for component, revisions in sorted(live_pod_revisions.items())
        },
        "notReadyPods": sorted(not_ready_pods, key=lambda pod: str(pod.get("name"))),
    }
    print("\n=== FAILURE SNAPSHOT ===")
    print(json.dumps(snapshot, indent=2, sort_keys=True))


def _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks):
    """Common finalize: infeasible-soften, print, cleanup, write_result."""
    Ne, Nd, Nr = hdr.get("Ne", 0), hdr.get("Nd", 0), hdr.get("Nr", 0)
    ce, cd, cr = hdr.get("ceil_e", Ne), hdr.get("ceil_d", Nd), hdr.get("ceil_r", Nr)
    agg = aggregate(ctx["pod_state"], ctx["old_hashes"], ctx["modes"], ctx["pods_per_inst"])
    final = {c: agg.get(c, (0, 0, 0))[0] for c in COMPS}
    new_final = {c: agg.get(c, (0, 0, 0))[2] for c in COMPS}
    verdict = soften_expected_fail(hdr, verdict, checks)
    # Invariants + fault apparatus run AFTER the softening (same rationale as
    # teardown_check below): a status lie or a broken fault apparatus is never
    # an expected failure, so they re-harden an XFAIL back to FAIL/INFRA-FAIL.
    verdict = invariant_checks(ctx.get("inv"), verdict, notes, checks)
    verdict = _fault_finalize(ctx.get("fault"), verdict, notes, checks)
    if verdict in ("FAIL", "INFRA-FAIL"):
        _print_failure_snapshot(case_id, hdr, ctx)
    _stop_watcher(ctx)
    # Teardown gate runs AFTER the infeasible/notimpl softening: a cleanup
    # that doesn't converge is never an expected failure, so it re-hardens
    # an XFAIL back to FAIL.
    if not os.getenv("OME_TEST_KEEP"):
        kubectl("delete", "-n", NS, "-f", str(find_isvc(case_id)), "--wait=false")
        total_pods = sum(ctx["N"][c] * ctx["pods_per_inst"].get(c, 1) for c in ctx["configured"])
        verdict = teardown_check(case_id, total_pods, verdict, notes, checks)
    else:
        checks.append("  ⓘ OME_TEST_KEEP set — ISVC kept, teardown check skipped")
    note = "; ".join(notes)
    print(f"\n=== VERDICT: {verdict} ===")
    for c in checks:
        print(c)
    if note:
        print(f"\nFailure reasons: {note}")
    write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, ctx["peaks"], final,
                 ctx.get("converged_at"), ctx["info"].get("policy"), verdict, note, new_final)
    return 0


def _inst_rev_split(ctx, comp):
    """(new, old) Instance counts for a component — Instances whose alive pods ALL
    agree on new-rev vs old-rev. A mixed Instance counts as neither (mid-roll)."""
    by = defaultdict(set)
    for p in ctx["pod_state"].values():
        if p and p["alive"] and p["comp"] == comp:
            by[p["inst"]].add(bool(p.get("hash") and p["hash"] not in ctx["old_hashes"]))
    new = sum(1 for s in by.values() if s == {True})
    old = sum(1 for s in by.values() if s == {False})
    return new, old


def _partition(case_id, yaml_file, desc, hdr):
    """Bump the revision with RollingUpdate.Partition=k set; assert the staged
    canary — exactly (N-k) Instances roll to the new revision and k hold on the
    old one. Per-component partition is read from the live spec (a PD group may
    partition only some members), so the boundary is checked per bumped component."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    N, configured = ctx["N"], ctx["configured"]
    ceil = {"engine": hdr.get("ceil_e", N["engine"]), "decoder": hdr.get("ceil_d", N["decoder"]),
            "router": hdr.get("ceil_r", N["router"])}
    bumped = _bumped(ctx)
    part = ctx["info"]["partition"]
    print(f"  Phase 2: bump rev on {sorted(bumped)}  partitions=" +
          " ".join(f"{c}:k={part.get(c, 0)}" for c in sorted(bumped)))
    if not _bump_rev(ctx, bumped):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "patch failed")
    ctx["track_comps"] = sorted(configured)   # gang/floor bookkeeping in _read_one
    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * sum(N[c] * ctx["pods_per_inst"][c] for c in bumped))
    print(f"  Phase 3: watching staged roll (≤{budget}s)")

    def converged():
        for c in configured:
            a, rd, _ = ctx["agg"].get(c, (0, 0, 0))
            if a != N[c] or rd != N[c]:
                return False
        for c in bumped:
            kc = part.get(c, 0)
            new, old = _inst_rev_split(ctx, c)
            if new != N[c] - kc or old != kc:
                return False
        return True

    state, stable = "ROTATING", None
    while ctx["trigger_ts"] and time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
        _read_one(ctx)
        if converged():
            if stable is None:
                stable = time.time()
            elif time.time() - stable >= STABILITY_S:
                state = "CONVERGED"
                ctx["converged_at"] = stable - ctx["trigger_ts"]
                break
        else:
            stable = None

    verdict, notes, checks = "PASS", [], []
    for c in COMPS:
        if N[c] and ctx["peaks"][c] > ceil[c]:
            verdict = "FAIL"; notes.append(f"{c} peak {ctx['peaks'][c]}>ceil {ceil[c]}")
            checks.append(f"  ✗ {c} peak {ctx['peaks'][c]} > ceiling {ceil[c]}")
        elif N[c]:
            checks.append(f"  ✓ {c} peak {ctx['peaks'][c]} ≤ ceiling {ceil[c]}")
    # Distinct-alive-index / shared-index (fleet-wipe guard) on the bumped gangs.
    verdict = index_collision_checks(ctx, sorted(bumped), verdict, notes, checks)
    # Gang atomicity (Check 9): scoped to bumped multi-node gangs.
    verdict = gang_atomicity_checks(ctx, sorted(bumped), verdict, notes, checks)
    # Surge-before-drain ordering on the (N-k) Instances that roll (the held k
    # never drain, so they don't enter the drain/ready ledger).
    verdict = surge_before_drain_checks(ctx, sorted(bumped), verdict, notes, checks)
    if state != "CONVERGED":
        verdict = "FAIL"
        for c in sorted(bumped):
            kc = part.get(c, 0); new, old = _inst_rev_split(ctx, c)
            notes.append(f"{c} not staged (new={new} old={old}, want {N[c]-kc}/{kc}, state={state})")
        checks.append(f"  ✗ staged roll did not converge (state={state}): " +
                      " ".join(f"{c}{_inst_rev_split(ctx, c)}" for c in sorted(bumped)))
    else:
        for c in sorted(bumped):
            kc = part.get(c, 0); new, old = _inst_rev_split(ctx, c)
            checks.append(f"  ✓ {c} partition held: {new} rolled (=N-k={N[c]-kc}), {old} held (=k={kc})")
    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


def _rebump(case_id, yaml_file, desc, hdr):
    """Bump the revision twice (v2, then v3 mid-roll) and assert the rollout
    converges entirely onto the FINAL revision — no Instance stranded on the
    intermediate revision (the X-2 corruption guard)."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    N, configured = ctx["N"], ctx["configured"]
    ceil = {"engine": hdr.get("ceil_e", N["engine"]), "decoder": hdr.get("ceil_d", N["decoder"]),
            "router": hdr.get("ceil_r", N["router"])}
    bumped = _bumped(ctx)
    print(f"  Phase 2a: first bump (v2) on {sorted(bumped)}")
    if not _bump_rev(ctx, bumped, "v2"):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "first patch failed")
    t0 = time.time()
    while time.time() - t0 < 60 and not ctx["closed"]:
        _read_one(ctx)
        if any(_inst_rev_split(ctx, c)[0] > 0 for c in bumped):
            break
    # All hashes seen from setup through now (original + every intermediate v2),
    # so the final (v3) revision is reliably anything that appears ONLY after the
    # v3 bump — even if a v2 pod was transient and already drained.
    pre_final = set(ctx["old_hashes"]) | set(ctx.get("seen_hashes", set()))
    print(f"  Phase 2b: second bump (v3) mid-roll (intermediate rev {sorted(pre_final - ctx['old_hashes']) or '(none yet)'})")
    if not _bump_rev(ctx, bumped, "v3"):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "second patch failed")
    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * sum(N[c] * ctx["pods_per_inst"][c] for c in bumped))
    print(f"  Phase 3: watching convergence to the final revision (≤{budget}s)")

    def final_hashes():
        return {p["hash"] for p in ctx["pod_state"].values()
                if p and p["alive"] and p.get("hash") and p["hash"] not in pre_final}

    def converged():
        for c in configured:
            a, rd, _ = ctx["agg"].get(c, (0, 0, 0))
            if a != N[c] or rd != N[c]:
                return False
        fh = final_hashes()
        live = [p for p in ctx["pod_state"].values() if p and p["alive"] and p["comp"] in bumped]
        return len(fh) == 1 and bool(live) and all(p.get("hash") in fh for p in live)

    state, stable = "ROTATING", None
    while ctx["trigger_ts"] and time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
        _read_one(ctx)
        if converged():
            if stable is None:
                stable = time.time()
            elif time.time() - stable >= STABILITY_S:
                state = "CONVERGED"
                ctx["converged_at"] = stable - ctx["trigger_ts"]
                break
        else:
            stable = None

    verdict, notes, checks = "PASS", [], []
    for c in COMPS:
        if N[c] and ctx["peaks"][c] > ceil[c]:
            verdict = "FAIL"; notes.append(f"{c} peak {ctx['peaks'][c]}>ceil {ceil[c]}")
            checks.append(f"  ✗ {c} peak {ctx['peaks'][c]} > ceiling {ceil[c]}")
        elif N[c]:
            checks.append(f"  ✓ {c} peak {ctx['peaks'][c]} ≤ ceiling {ceil[c]}")
    fh = final_hashes()
    if state != "CONVERGED":
        verdict = "FAIL"
        notes.append(f"did not settle on one final rev (state={state}, final={sorted(fh)})")
        checks.append(f"  ✗ X-2: never settled on a single final revision (state={state}, final hashes={sorted(fh)})")
    else:
        checks.append(f"  ✓ converged to one final revision {sorted(fh)} — no Instance stranded on the intermediate (X-2 safe)")
    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


NOROLL_GRACE_S = 45.0  # how long to watch that a no-op / pinned runtime edit stays put


def _runner_env_paths(spec, cfg):
    """JSON-patch paths to the runner env list of a runtime config, across the
    single-pod shape (flat `runner`) and the gang shape (`leader.runner` +
    `worker.runner`). Only paths present in the live CSR are returned."""
    node = spec.get(cfg) or {}
    paths = []
    if isinstance(node.get("runner"), dict):
        paths.append(f"/spec/{cfg}/runner/env")
    for sub in ("leader", "worker"):
        if isinstance(node.get(sub), dict) and isinstance(node[sub].get("runner"), dict):
            paths.append(f"/spec/{cfg}/{sub}/runner/env")
    return paths


def _csr_bump_cpu(csr, comps):
    """Bump every runner's requests.cpu (flat + leader/worker) by 5m — a material
    resource-shape edit. Values are derived from the live CSR, nothing fixed."""
    r = kubectl("get", "clusterservingruntime", csr, "-o", "json")
    if r.returncode != 0:
        return False
    spec = (json.loads(r.stdout) or {}).get("spec", {})
    ops = []
    for c in comps:
        cfg = f"{c}Config"
        node = spec.get(cfg) or {}
        runners = [(f"/spec/{cfg}/runner", node.get("runner"))]
        runners += [(f"/spec/{cfg}/{s}/runner", (node.get(s) or {}).get("runner"))
                    for s in ("leader", "worker")]
        for path, rn in runners:
            if not isinstance(rn, dict):
                continue
            cur = ((rn.get("resources") or {}).get("requests") or {}).get("cpu")
            if cur is None:
                continue
            ops.append({"op": "replace", "path": f"{path}/resources/requests/cpu",
                        "value": f"{_qty('cpu', cur) + 5}m"})
    if not ops:
        return False
    return kubectl("patch", "clusterservingruntime", csr, "--type=json",
                   "-p", json.dumps(ops)).returncode == 0


def _bump_runtime(ctx, csr, comps, material, kind="env", image=None):
    """Trigger a change on the ISVC's dedicated ClusterServingRuntime.

    material=True: a real pod-spec change that must re-render and roll an
    autoSync=true ISVC (the runtime-side analogue of _bump_rev's ISVC
    annotation). kind picks WHICH field changes: "env" bumps the runner's
    TEST_REV env (index 0), "image" swaps the runner image to `image`,
    "resources" bumps requests.cpu — all shape-aware for single-pod and gang
    runtimes. material=False: touch a CSR label only — no pod-spec change, so
    the revision-hash is unchanged and nothing should roll.
    """
    if not material:
        rr = kubectl("patch", "clusterservingruntime", csr, "--type=merge",
                     "-p", json.dumps({"metadata": {"labels": {"perm.test/touch": TRIGGER_VAL}}}))
        ctx["trigger_ts"] = time.time()
        return rr.returncode == 0
    if kind == "image":
        ok = bool(image) and _csr_set_image(csr, comps, image)
        ctx["trigger_ts"] = time.time()
        return ok
    if kind == "resources":
        ok = _csr_bump_cpu(csr, comps)
        ctx["trigger_ts"] = time.time()
        return ok
    r = kubectl("get", "clusterservingruntime", csr, "-o", "json")
    if r.returncode != 0:
        return False
    spec = (json.loads(r.stdout) or {}).get("spec", {})
    ops = [{"op": "replace", "path": f"{base}/0/value", "value": TRIGGER_VAL}
           for c in comps for base in _runner_env_paths(spec, f"{c}Config")]
    if not ops:
        print(f"  ✗ no runner env paths found on CSR {csr} for {comps}")
        return False
    rr = kubectl("patch", "clusterservingruntime", csr, "--type=json", "-p", json.dumps(ops))
    ctx["trigger_ts"] = time.time()
    return rr.returncode == 0


def _csr_runner_image(csr, comp):
    """Current runner image of a component's runtime config (first runner found)."""
    r = kubectl("get", "clusterservingruntime", csr, "-o", "json")
    if r.returncode != 0:
        return None
    node = ((json.loads(r.stdout) or {}).get("spec", {})).get(f"{comp}Config", {})
    if isinstance(node.get("runner"), dict):
        return node["runner"].get("image")
    for sub in ("leader", "worker"):
        if isinstance(node.get(sub), dict) and isinstance(node[sub].get("runner"), dict):
            return node[sub]["runner"].get("image")
    return None


def _csr_set_image(csr, comps, image):
    """Set every runner image (flat + leader/worker) of each configured component's
    runtime config — used to drive a bad rollout and then correct it."""
    r = kubectl("get", "clusterservingruntime", csr, "-o", "json")
    if r.returncode != 0:
        return False
    spec = (json.loads(r.stdout) or {}).get("spec", {})
    ops = []
    for c in comps:
        cfg = f"{c}Config"
        node = spec.get(cfg) or {}
        if isinstance(node.get("runner"), dict):
            ops.append({"op": "replace", "path": f"/spec/{cfg}/runner/image", "value": image})
        for sub in ("leader", "worker"):
            if isinstance(node.get(sub), dict) and isinstance(node[sub].get("runner"), dict):
                ops.append({"op": "replace", "path": f"/spec/{cfg}/{sub}/runner/image", "value": image})
    if not ops:
        return False
    return kubectl("patch", "clusterservingruntime", csr, "--type=json", "-p", json.dumps(ops)).returncode == 0


# readinessProbe variants for the surge-budget-deadlock guard. _BAD keeps the
# container RUNNING (startup/liveness httpGet still pass — nginx serves) but
# readiness never flips, so a surge pod is alive-but-never-Ready WITHOUT crashing
# → it dodges the stuck-pod breaker (no terminal waiting state) and just holds the
# maxSurge budget. _GOOD mirrors runtime/csr.yaml's real probe (restore).
_BAD_READINESS = {"exec": {"command": ["false"]}}
_GOOD_READINESS = {"httpGet": {"path": "/", "port": 8080}, "periodSeconds": 2, "failureThreshold": 2}


def _csr_patch_readiness(csr, comps, probe):
    """Replace every runner's readinessProbe (flat + leader/worker) of each
    configured component. Used to make a rollout's surge pods run-but-never-Ready
    (never-escalating) and then restore normal readiness."""
    r = kubectl("get", "clusterservingruntime", csr, "-o", "json")
    if r.returncode != 0:
        return False
    spec = (json.loads(r.stdout) or {}).get("spec", {})
    ops = []
    for c in comps:
        cfg = f"{c}Config"
        node = spec.get(cfg) or {}
        if isinstance(node.get("runner"), dict):
            ops.append({"op": "replace", "path": f"/spec/{cfg}/runner/readinessProbe", "value": probe})
        for sub in ("leader", "worker"):
            if isinstance(node.get(sub), dict) and isinstance(node[sub].get("runner"), dict):
                ops.append({"op": "replace", "path": f"/spec/{cfg}/{sub}/runner/readinessProbe", "value": probe})
    if not ops:
        return False
    return kubectl("patch", "clusterservingruntime", csr, "--type=json", "-p", json.dumps(ops)).returncode == 0


def _runtime_recover_stuck(case_id, yaml_file, desc, hdr, csr):
    """Guard for the surge-budget deadlock: a prior rollout's surge pods that
    RUN but never become Ready — and never escalate (no crash, no
    ImagePullBackOff, so the fast stuck-pod breaker never fires) — consume the
    per-Component maxSurge budget, so a later CORRECTIVE rollout can't start a
    single new-revision pod. It deadlocks at 0/N until instanceReadyTimeout
    (~1h30m). MUST roll the corrective revision forward quickly; the case
    header parks this as notimpl while it does not.

    Reproduction: break readiness (readinessProbe -> exec:false; container stays up
    via startup/liveness) + bump a rev -> surge pods run-but-never-Ready, holding
    maxSurge. Then restore readiness + bump a distinct good rev -> the corrective
    rollout must supersede the stuck surge and converge."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    N, configured = ctx["N"], ctx["configured"]
    comps = sorted(configured)

    print("  Phase 2a: break readiness (surge pods run but never become Ready) + bump a broken rev")
    if not _csr_patch_readiness(csr, comps, _BAD_READINESS):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "break-readiness patch failed")
    if not _bump_runtime(ctx, csr, comps, material=True):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "broken-rev bump failed")
    t0, stuck = time.time(), set()
    while time.time() - t0 < 150 and not ctx["closed"]:
        _read_one(ctx)
        for p in ctx["pod_state"].values():
            if p and p["alive"] and p.get("hash") and p["hash"] not in ctx["old_hashes"]:
                stuck.add(p["hash"])
        if stuck:
            break
    if not stuck:
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "no surge pod appeared for the broken rollout")
    time.sleep(20)  # let it settle as stuck: running, not Ready, not escalated
    seen_before = set(ctx["old_hashes"]) | set(ctx.get("seen_hashes", set())) | stuck

    print("  Phase 2b: CORRECTIVE edit — restore readiness + bump a distinct good rev")
    if not _csr_patch_readiness(csr, comps, _GOOD_READINESS):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "restore-readiness patch failed")
    if not _bump_runtime(ctx, csr, comps, material=True):  # resets trigger_ts
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "corrective bump failed")
    seen_before |= set(ctx.get("seen_hashes", set()))

    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S *
                 sum(N[c] * ctx["pods_per_inst"][c] for c in configured))
    print(f"  Phase 3: watching the corrective rollout roll forward (≤{budget}s)")

    def attempted():  # a corrective-rev pod (newer than the stuck attempt) appeared
        return any(p and p["alive"] and p.get("hash") and p["hash"] not in seen_before
                   for p in ctx["pod_state"].values())

    def recovered():
        for c in configured:
            a, rd, _ = ctx["agg"].get(c, (0, 0, 0))
            if a != N[c] or rd != N[c]:
                return False
        for c in configured:
            live = [p for p in ctx["pod_state"].values()
                    if p and p["alive"] and p["comp"] == c and p.get("hash")]
            if not live or any(p["hash"] in seen_before for p in live):
                return False
        return True

    # The corrective rollout is deadlocked behind the stuck surge until
    # instanceReadyTimeout; if NO corrective-rev pod even appears within this grace,
    # it's the budget deadlock — fail fast rather than waiting out the budget.
    ATTEMPT_GRACE_S = 120.0
    state, stable = "WEDGED", None
    while ctx["trigger_ts"] and time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
        _read_one(ctx)
        if recovered():
            if stable is None:
                stable = time.time()
            elif time.time() - stable >= STABILITY_S:
                state = "RECOVERED"
                ctx["converged_at"] = stable - ctx["trigger_ts"]
                break
        else:
            stable = None
        if time.time() - ctx["trigger_ts"] > ATTEMPT_GRACE_S and not attempted():
            state = "WEDGED"
            break

    verdict, notes, checks = "PASS", [], []
    if state != "RECOVERED":
        verdict = "FAIL"
        notes.append("corrective rollout deadlocked behind an unhealthy prior surge (maxSurge held by a never-ready, never-escalated surge)")
        checks.append("  ✗ deadlock: corrective edit never rolled forward — a stuck surge held the maxSurge budget and never escalated")
    else:
        checks.append("  ✓ recovery: corrective edit rolled forward despite the unhealthy prior-rollout surge")
    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


def _runtime_recover(case_id, yaml_file, desc, hdr, csr):
    """Guard for the Failed-escalation wedge: drive the ISVC into a failed rollout
    (bad runtime image → ImagePullBackOff → Instance escalates to Phase=Failed),
    then apply a CORRECTIVE runtime edit (restore the good image + bump the env for
    a distinct revision). The ISVC MUST recover — roll onto the new good revision
    and converge. Today it does not (the escalation is terminal and no later desired
    revision is acted on), so this FAILs until that is fixed."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    N, configured = ctx["N"], ctx["configured"]
    ceil = {"engine": hdr.get("ceil_e", N["engine"]), "decoder": hdr.get("ceil_d", N["decoder"]),
            "router": hdr.get("ceil_r", N["router"])}
    comps = sorted(configured)
    good = _csr_runner_image(csr, comps[0])
    if not good:
        return _write_fail(case_id, desc, hdr, "CASE_ERROR", "could not read runner image")
    bad = good.rsplit(":", 1)[0] + ":NONEXISTENT-omenative-recover-test"

    print(f"  Phase 2a: drive a FAILED rollout — set image -> {bad.rsplit('/',1)[-1]}")
    if not _csr_set_image(csr, comps, bad):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "bad-image patch failed")
    t0 = time.time()
    while time.time() - t0 < 120 and not ctx["closed"]:  # wait for the bad surge pod to appear/fail
        _read_one(ctx)
        if any(p and p["alive"] and p.get("hash") and p["hash"] not in ctx["old_hashes"]
               for p in ctx["pod_state"].values()):
            break
    seen_before = set(ctx["old_hashes"]) | set(ctx.get("seen_hashes", set()))
    time.sleep(10)  # let the bad pod reach ImagePullBackOff / escalate

    print("  Phase 2b: CORRECTIVE edit — restore good image + bump env (distinct good revision)")
    if not _csr_set_image(csr, comps, good):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "good-image restore failed")
    if not _bump_runtime(ctx, csr, comps, material=True):  # also resets trigger_ts
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "corrective env bump failed")
    seen_before |= set(ctx.get("seen_hashes", set()))

    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S *
                 sum(N[c] * ctx["pods_per_inst"][c] for c in configured))
    print(f"  Phase 3: watching RECOVERY onto the corrected revision (≤{budget}s)")

    def attempted():  # any pod on a hash newer than the failed attempt = a fresh roll started
        return any(p and p["alive"] and p.get("hash") and p["hash"] not in seen_before
                   for p in ctx["pod_state"].values())

    def recovered():
        for c in configured:
            a, rd, _ = ctx["agg"].get(c, (0, 0, 0))
            if a != N[c] or rd != N[c]:
                return False
        for c in configured:
            live = [p for p in ctx["pod_state"].values()
                    if p and p["alive"] and p["comp"] == c and p.get("hash")]
            if not live or any(p["hash"] in seen_before for p in live):
                return False
        return True

    # Fast wedge detection: a real recovery starts a fresh roll within seconds. If
    # NO corrected-revision pod appears within this grace, the corrective edit was
    # ignored — the terminal wedge — so fail fast instead of waiting out the budget.
    ATTEMPT_GRACE_S = 75.0
    state, stable = "WEDGED", None
    while ctx["trigger_ts"] and time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
        _read_one(ctx)
        if recovered():
            if stable is None:
                stable = time.time()
            elif time.time() - stable >= STABILITY_S:
                state = "RECOVERED"
                ctx["converged_at"] = stable - ctx["trigger_ts"]
                break
        else:
            stable = None
        if time.time() - ctx["trigger_ts"] > ATTEMPT_GRACE_S and not attempted():
            state = "WEDGED"
            break

    verdict, notes, checks = "PASS", [], []
    if state != "RECOVERED":
        verdict = "FAIL"
        notes.append("failed rollout never recovered after a corrective runtime edit (terminal wedge)")
        checks.append("  ✗ recovery: ISVC stayed wedged on the failed rollout — corrective runtime edit was not acted on")
    else:
        checks.append("  ✓ recovery: corrective runtime edit rolled the ISVC onto a healthy revision")
    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


def _runtimerev_during(case_id, yaml_file, desc, hdr, csr):
    """Edit the runtime while an ISVC-annotation rollout is mid-rotate: the two
    triggers must fold into ONE converged outcome — every Instance on the final
    (post-runtime-edit) revision, ceilings held, nothing stranded on the
    intermediate revision and no wedge (the runtime-edit analogue of _rebump)."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    N, configured = ctx["N"], ctx["configured"]
    ceil = {"engine": hdr.get("ceil_e", N["engine"]), "decoder": hdr.get("ceil_d", N["decoder"]),
            "router": hdr.get("ceil_r", N["router"])}
    edit = hdr.get("edit", "material")
    kind = edit if edit in ("image", "resources") else "env"
    bumped = _bumped(ctx)
    print(f"  Phase 2a: ISVC revision bump (v2) on {sorted(bumped)} — runtime edit lands mid-rotate")
    if not _bump_rev(ctx, bumped, "v2"):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "isvc bump failed")
    t0 = time.time()
    while time.time() - t0 < 60 and not ctx["closed"]:
        _read_one(ctx)
        if any(p and p["alive"] and p.get("hash") and p["hash"] not in ctx["old_hashes"]
               for p in ctx["pod_state"].values()):
            break
    # Final revision = anything appearing only AFTER the CSR edit (rebump-style).
    pre_final = set(ctx["old_hashes"]) | set(ctx.get("seen_hashes", set()))
    print(f"  Phase 2b: {edit} runtime edit on CSR {csr} mid-rotate")
    if not _bump_runtime(ctx, csr, sorted(configured), True, kind=kind, image=hdr.get("img")):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "csr patch failed")
    budget = min(MAX_BUDGET_S, 2 * (ROTATE_BASE_S + ROTATE_PER_POD_S *
                                    sum(N[c] * ctx["pods_per_inst"][c] for c in configured)))
    print(f"  Phase 3: watching convergence onto the post-edit revision (≤{budget:.0f}s)")

    def converged():
        for c in configured:
            a, rd, _ = ctx["agg"].get(c, (0, 0, 0))
            if a != N[c] or rd != N[c]:
                return False
        for c in configured:
            live = [p for p in ctx["pod_state"].values()
                    if p and p["alive"] and p["comp"] == c and p.get("hash")]
            if not live or any(p["hash"] in pre_final for p in live):
                return False
        return True

    state, stable = "ROTATING", None
    while ctx["trigger_ts"] and time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
        _read_one(ctx)
        if converged():
            if stable is None:
                stable = time.time()
            elif time.time() - stable >= STABILITY_S:
                state = "CONVERGED"
                ctx["converged_at"] = stable - ctx["trigger_ts"]
                break
        else:
            stable = None
    verdict, notes, checks = "PASS", [], []
    for c in COMPS:
        if N[c] and ctx["peaks"][c] > ceil[c]:
            verdict = "FAIL"; notes.append(f"{c} peak {ctx['peaks'][c]}>ceil {ceil[c]}")
            checks.append(f"  ✗ {c} peak {ctx['peaks'][c]} > ceiling {ceil[c]}")
        elif N[c]:
            checks.append(f"  ✓ {c} peak {ctx['peaks'][c]} ≤ ceiling {ceil[c]}")
    if state != "CONVERGED":
        verdict = "FAIL"
        notes.append(f"runtime edit mid-rollout did not converge (state={state})")
        checks.append(f"  ✗ concurrent ISVC rollout + runtime edit never converged onto the post-edit revision (state={state})")
    else:
        checks.append("  ✓ concurrent ISVC rollout + runtime edit converged onto the post-edit revision — no wedge, nothing stranded")
    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


def _runtimerev(case_id, yaml_file, desc, hdr):
    """Modify the ISVC's RUNTIME (not the ISVC) and assert the dependent ISVC
    behaves per autoSync — the one rollout trigger the rest of the suite never
    exercises (every other mode patches the ISVC itself).

      - expect=roll  (autoSync=true + a material runtime edit): the ISVC
        re-renders from the live runtime and rolls to a NEW revision, converging
        with peaks ≤ ceiling and per its own updateStrategy.
      - expect=noroll (a no-op runtime edit, OR an autoSync=false pinned ISVC):
        the ISVC must NOT roll — no pod rotates to a new revision.

    The CSR is dedicated per case (`rt=` header, applied as a second doc in the
    case file) so the edit is isolated from concurrently-running cases.
    """
    csr = hdr.get("rt")
    if not csr:
        return _write_fail(case_id, desc, hdr, "CASE_ERROR", "runtimerev case missing rt= header")
    if hdr.get("expect") == "recover":
        if hdr.get("break") == "stuck":
            return _runtime_recover_stuck(case_id, yaml_file, desc, hdr, csr)
        return _runtime_recover(case_id, yaml_file, desc, hdr, csr)
    if hdr.get("during") == "rollout":
        return _runtimerev_during(case_id, yaml_file, desc, hdr, csr)
    expect_roll = hdr.get("expect", "roll") == "roll"
    # Whether the runtime edit is material (changes the pod spec) is independent of
    # whether a roll is expected: a material edit on a PINNED (autoSync=false) ISVC
    # must NOT roll. Default: material when a roll is expected, no-op otherwise.
    edit = hdr.get("edit", "material" if expect_roll else "noop")
    material = edit != "noop"
    kind = edit if edit in ("image", "resources") else "env"
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    N, configured = ctx["N"], ctx["configured"]
    ceil = {"engine": hdr.get("ceil_e", N["engine"]), "decoder": hdr.get("ceil_d", N["decoder"]),
            "router": hdr.get("ceil_r", N["router"])}
    configs = sorted(f"{c}Config" for c in configured)
    old = set(ctx["old_hashes"])

    def new_hashes():
        return {p["hash"] for p in ctx["pod_state"].values()
                if p and p["alive"] and p.get("hash") and p["hash"] not in old}

    print(f"  Phase 2: {edit} runtime edit on CSR {csr} ({configs}); "
          f"expect {'ROLL' if expect_roll else 'NO ROLL'}")
    if not _bump_runtime(ctx, csr, sorted(configured), material, kind=kind, image=hdr.get("img")):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "csr patch failed")

    verdict, notes, checks = "PASS", [], []

    if expect_roll:
        budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S *
                     sum(N[c] * ctx["pods_per_inst"][c] for c in configured))
        print(f"  Phase 3: watching autoSync roll to the new runtime revision (≤{budget}s)")

        def converged():
            # Every configured component must be at N ready, and each component's
            # live pods must ALL be on a new (post-edit) hash. Per-component because
            # a multi-Component ISVC (PD) rolls each component to its OWN new
            # revision — there is one new hash per component, not one globally.
            for c in configured:
                a, rd, _ = ctx["agg"].get(c, (0, 0, 0))
                if a != N[c] or rd != N[c]:
                    return False
            for c in configured:
                live = [p for p in ctx["pod_state"].values()
                        if p and p["alive"] and p["comp"] == c and p.get("hash")]
                if not live or any(p["hash"] in old for p in live):
                    return False
            return True

        state, stable = "ROTATING", None
        while ctx["trigger_ts"] and time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
            _read_one(ctx)
            if converged():
                if stable is None:
                    stable = time.time()
                elif time.time() - stable >= STABILITY_S:
                    state = "CONVERGED"
                    ctx["converged_at"] = stable - ctx["trigger_ts"]
                    break
            else:
                stable = None
        for c in COMPS:
            if N[c] and ctx["peaks"][c] > ceil[c]:
                verdict = "FAIL"; notes.append(f"{c} peak {ctx['peaks'][c]}>ceil {ceil[c]}")
                checks.append(f"  ✗ {c} peak {ctx['peaks'][c]} > ceiling {ceil[c]}")
            elif N[c]:
                checks.append(f"  ✓ {c} peak {ctx['peaks'][c]} ≤ ceiling {ceil[c]}")
        if state != "CONVERGED":
            verdict = "FAIL"
            notes.append(f"runtime edit did not roll (state={state}, new={sorted(new_hashes())})")
            checks.append(f"  ✗ autoSync roll: never converged onto a new runtime revision (state={state})")
        else:
            checks.append(f"  ✓ autoSync roll: converged onto new runtime revision {sorted(new_hashes())}")
        return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)

    # expect=noroll — assert nothing rolls for the grace window.
    print(f"  Phase 3: asserting NO roll for {int(NOROLL_GRACE_S)}s")
    t0 = time.time()
    while time.time() - t0 < NOROLL_GRACE_S and not ctx["closed"]:
        _read_one(ctx)
    nh = new_hashes()
    now_uids = {c: {p["uid"] for p in ctx["pod_state"].values()
                    if p and p["alive"] and p["comp"] == c and p.get("uid")} for c in configured}
    rotated = sorted(c for c in configured if ctx["old_uids"].get(c) and not (ctx["old_uids"][c] <= now_uids.get(c, set())))
    if nh:
        verdict = "FAIL"; notes.append(f"unexpected roll to {sorted(nh)}")
        checks.append(f"  ✗ no-roll violated: pods rolled to a new runtime revision {sorted(nh)} (expected none)")
    elif rotated:
        verdict = "FAIL"; notes.append(f"unexpected pod rotation on {rotated}")
        checks.append(f"  ✗ no-roll violated: pods rotated on {rotated} without a new revision (expected none)")
    else:
        checks.append(f"  ✓ no roll: pods stayed on the original revision {sorted(old)} for {int(NOROLL_GRACE_S)}s")
    return _finish(case_id, desc, hdr, ctx, "CONVERGED" if verdict == "PASS" else "NOROLL_VIOLATED", verdict, notes, checks)


def _migration_ledger_entries(case_id):
    """All rows of the audit ledger CM <isvc>-ome-migration-audit (data key
    history.json). Empty when the CM doesn't exist yet."""
    r = kubectl("-n", NS, "get", "cm", f"{case_id}-ome-migration-audit", "-o", "json")
    if r.returncode != 0:
        return []
    try:
        hist = json.loads(json.loads(r.stdout).get("data", {}).get("history.json", "{}"))
    except Exception:
        return []
    return hist.get("entries", []) or []


def _migration_ledger_entry(case_id, req_uuid):
    """The audit-ledger row for a request UUID; None if it doesn't exist yet."""
    for e in _migration_ledger_entries(case_id):
        if e.get("requestUUID") == req_uuid:
            return e
    return None


def _ir_status_migrations(case_id, comp):
    """status.migrations of the component's InferenceReplica. Builds that report
    it consume the request annotation at ACCEPT and keep migration state here.
    Returns the entry list, or None when unavailable — no IR CRD, no IR for the
    component, or an IR without the field — so callers can stay
    version-tolerant."""
    r = kubectl("-n", NS, "get", "inferencereplicas", "-l",
                f"ome.io/inferenceservice={case_id}", "-o", "json")
    if r.returncode != 0:
        return None
    try:
        items = json.loads(r.stdout).get("items", [])
    except Exception:
        return None
    for it in items:
        if (it.get("spec") or {}).get("component") == comp:
            st = it.get("status") or {}
            if "migrations" in st:
                return st.get("migrations") or []
            return None
    return None


def _isvc_annotations(case_id):
    r = kubectl("-n", NS, "get", "isvc", case_id, "-o", "json")
    if r.returncode != 0:
        return {}
    try:
        return json.loads(r.stdout).get("metadata", {}).get("annotations", {}) or {}
    except Exception:
        return {}


def _migrate(case_id, yaml_file, desc, hdr):
    """Set an explicit migration-request annotation; assert the audit ledger
    reaches Completed, a replacement Instance surges onto a different node, the
    source drains, and the annotation is cleared. mig=Never must be REJECTED
    (no surge, no Completed)."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    N = ctx["N"]
    mig_mode = hdr.get("mig", "Auto")
    mcomp = "engine" if "engine" in ctx["configured"] else sorted(ctx["configured"])[0]
    # inst is a comma list ("0,1") for the concurrent multi-instance case; a bare
    # "0" is the single-instance path (unchanged). Concurrent migrations must
    # surge DISTINCT replacement indices — many sources collapsing onto one surge
    # index is the fleet-wipe class at migration scale.
    req_insts = [s.strip() for s in str(hdr.get("inst", 0)).split(",") if s.strip() != ""]
    inst = req_insts[0]
    orig_insts = {p["inst"] for p in ctx["pod_state"].values()
                  if p and p["alive"] and p["comp"] == mcomp}
    dup = bool(hdr.get("dup"))
    defer_rollout = hdr.get("during") == "rollout"
    hint_node = None
    if hdr.get("hint"):
        # Hint target: a node already hosting a DIFFERENT instance of the same
        # component (schedulable for this workload shape), never the from_node.
        fn0 = ctx["old_nodes"].get((mcomp, inst), "")
        hint_node = next((n for (c, i), n in sorted(ctx["old_nodes"].items())
                          if c == mcomp and i != inst and n and n != fn0), None)
    reqs = {}   # src inst -> (req_uuid, anno_key, from_node)
    annos = {}
    for i in req_insts:
        ru = str(uuid.uuid4())
        ak = f"ome.io/migration-request-v1-{ru}"
        fn = ctx["old_nodes"].get((mcomp, i), "")
        reqs[i] = (ru, ak, fn)
        body = {"schemaVersion": "v1", "component": mcomp, "instance": int(i),
                "from_node": fn, "reason": "perm-test"}
        if hint_node:
            body["hint_target_nodes"] = [hint_node]
        annos[ak] = json.dumps(body)
    if defer_rollout:
        # Mid-rotate migration: start an ISVC rollout and file the migration
        # request only once the rotation is visibly in flight.
        bumped = _bumped(ctx)
        print(f"  Phase 2a: ISVC revision bump (v2) on {sorted(bumped)} — migration request filed mid-rotate")
        if not _bump_rev(ctx, bumped, "v2"):
            return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "isvc bump failed")
        t0 = time.time()
        while time.time() - t0 < 60 and not ctx["closed"]:
            _read_one(ctx)
            if any(p and p["alive"] and p.get("hash") and p["hash"] not in ctx["old_hashes"]
                   for p in ctx["pod_state"].values()):
                break
        for i, (ru, ak, fn) in reqs.items():
            live_fn = next((p.get("node", "") for p in ctx["pod_state"].values()
                            if p and p["alive"] and p["comp"] == mcomp
                            and str(p.get("inst")) == i and p.get("node")), fn)
            reqs[i] = (ru, ak, live_fn)
            body = json.loads(annos[ak])
            body["from_node"] = live_fn
            annos[ak] = json.dumps(body)
    from_node = reqs[inst][2]
    print(f"  Phase 2: migration request (mig={mig_mode}) component={mcomp} instances={req_insts}"
          + (f" hint={hint_node}" if hint_node else ""))
    rr = kubectl("-n", NS, "patch", "isvc", case_id, "--type=merge",
                 "-p", json.dumps({"metadata": {"annotations": annos}}))
    if rr.returncode != 0:
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", rr.stderr[:200])
    ctx["trigger_ts"] = time.time()
    ctx["track_comps"] = sorted(ctx["configured"])   # gang/floor bookkeeping in _read_one
    # Identity baseline for drain + surge detection. A migration legitimately
    # REUSES the source's own index for the replacement pod (on a different
    # node), so index-keyed checks false-positive ("source still present" /
    # "collision"). Key on pod UID instead: src_uids = the source pods being
    # drained; orig_uids = all component pods present now, so any later pod with
    # a fresh UID is a genuine surge regardless of which index it lands on.
    src_uids = {p["uid"] for p in ctx["pod_state"].values()
                if p and p["alive"] and p["comp"] == mcomp
                and str(p.get("inst")) in req_insts and p.get("uid")}
    orig_uids = {p["uid"] for p in ctx["pod_state"].values()
                 if p and p["alive"] and p["comp"] == mcomp and p.get("uid")}
    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * N[mcomp] * ctx["pods_per_inst"][mcomp])
    if defer_rollout:
        budget = min(MAX_BUDGET_S, 2 * budget)   # two ops (rollout + migration) share the window
    never_grace = 90
    print(f"  Phase 3: watching migration (ledger {case_id}-ome-migration-audit; ≤{budget}s)")
    surge_seen, surge_nodes, last_poll, state = set(), {}, 0.0, "WATCHING"
    surge_uids = {}   # fresh pod UID -> node: identity-based surge count (index-reuse safe)
    dup_refiled, rollout_conv_ts = False, None

    def _rollout_done():
        live = [p for p in ctx["pod_state"].values()
                if p and p["alive"] and p["comp"] in _bumped(ctx) and p.get("hash")]
        return bool(live and all(p["hash"] not in ctx["old_hashes"] for p in live)
                    and all(ctx["agg"].get(c, (0, 0, 0))[1] >= N[c] for c in ctx["configured"]))

    while time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
        _read_one(ctx)
        for p in ctx["pod_state"].values():
            if p and p["alive"] and p["comp"] == mcomp and p["inst"] not in orig_insts:
                surge_seen.add(p["inst"])
                surge_nodes[p["inst"]] = p.get("node", "") or surge_nodes.get(p["inst"], "")
            # Identity-based surge: any component pod with a UID not in the
            # trigger-time baseline is a genuine replacement — even when it
            # reuses a source's own index (index-keyed surge_seen misses it).
            if (p and p["alive"] and p["comp"] == mcomp and p.get("uid")
                    and p["uid"] not in orig_uids):
                surge_uids[p["uid"]] = p.get("node", "") or surge_uids.get(p["uid"], "")
        now = time.time()
        if defer_rollout and rollout_conv_ts is None and _rollout_done():
            rollout_conv_ts = now
            print(f"    ↷ in-flight rollout converged at {now - ctx['trigger_ts']:.0f}s")
        if now - last_poll >= 5:
            last_poll = now
            phs = {i: (_migration_ledger_entry(case_id, ru) or {}).get("phase")
                   for i, (ru, _, _) in reqs.items()}
            if any(phs.values()):
                ctx["fault_migrating"] = True   # accept observed → migrating fault phase hook
                if dup and not dup_refiled:
                    # Duplicate delivery: the SAME request UUID re-filed after
                    # the first was observed — must dedup to one entry/one surge.
                    kubectl("-n", NS, "patch", "isvc", case_id, "--type=merge",
                            "-p", json.dumps({"metadata": {"annotations": annos}}))
                    dup_refiled = True
                    print("    ↻ duplicate delivery: same migration-request annotation re-filed")
            if mig_mode != "Never" and all(v == "Completed" for v in phs.values()):
                state = "COMPLETED"; break
            if any(v == "Failed" for v in phs.values()):
                state = "FAILED"; break
            if mig_mode == "Never" and now - ctx["trigger_ts"] > never_grace:
                state = "NEVER_GRACE"; break
    if defer_rollout and rollout_conv_ts is None:
        # The product may PREEMPT: an accepted migration's requeue skips the
        # update pass, so the rollout can finish AFTER the migration completed
        # (the loop above breaks on the ledger phase). Keep watching, within
        # the same overall budget, for the rollout to converge.
        while time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
            _read_one(ctx)
            if _rollout_done():
                rollout_conv_ts = time.time()
                print(f"    ↷ in-flight rollout converged at {rollout_conv_ts - ctx['trigger_ts']:.0f}s (post-migration)")
                break
    # Collapse the per-source ledger into the single-inst vars the verdict block
    # below already consumes; ph=="Completed" only when EVERY request completed.
    entries = {i: (_migration_ledger_entry(case_id, ru) or {}) for i, (ru, _, _) in reqs.items()}
    phs = {i: e.get("phase") for i, e in entries.items()}
    ph = "Completed" if phs and all(v == "Completed" for v in phs.values()) else \
         ("Failed" if any(v == "Failed" for v in phs.values()) else next(iter(phs.values()), None))
    entry = entries[inst]
    ledger_surge = entry.get("surgeInstance") if entry else None
    surge_inst = sorted(surge_seen)[0] if surge_seen else None
    surge_node = surge_nodes.get(surge_inst, "") if surge_inst is not None else None
    surged = bool(surge_seen) or (ledger_surge is not None and ph == "Completed")

    def _src_gone():
        # Source drained iff none of the ORIGINAL source pod UIDs are still
        # alive. Keying on UID (not index) is required: a migration may reuse
        # the source's own index for the replacement on a different node, which
        # an index-occupancy check would misread as "source still present".
        if src_uids:
            live = {p["uid"] for p in ctx["pod_state"].values()
                    if p and p["alive"] and p.get("uid")}
            return not (src_uids & live)
        return not any(p and p["alive"] and p["comp"] == mcomp and p["inst"] in req_insts
                       for p in ctx["pod_state"].values())

    def _anno_cleared():
        return all(ak not in _isvc_annotations(case_id) for (_, ak, _) in reqs.values())

    anno_cleared, src_gone = _anno_cleared(), _src_gone()
    if mig_mode != "Never" and ph == "Completed" and not (anno_cleared and src_gone):
        # The source drain + annotation clear finish asynchronously after the
        # ledger flips Completed (on builds with IR status.migrations the
        # annotation is already consumed at ACCEPT). Settle with a bounded poll
        # before hard-failing on either.
        settle_budget = migrate_settle_budget(len(req_insts))
        settle_deadline = time.time() + settle_budget
        last_anno = 0.0
        while time.time() < settle_deadline and not ctx["closed"]:
            _read_one(ctx)
            src_gone = _src_gone()
            if time.time() - last_anno >= 5:
                last_anno = time.time()
                anno_cleared = _anno_cleared()
            if anno_cleared and src_gone:
                break

    verdict, notes, checks = "PASS", [], []
    # Peak/ceiling: a migration surges +1 (ceil = N+1 from the case header; N for
    # mig=Never). A mass fan-out — many Instances surging at once, the fleet-wipe
    # shape — blows past the ceiling. Same peak gate as _partition/_rollout.
    mceil = {"engine": hdr.get("ceil_e", N["engine"]), "decoder": hdr.get("ceil_d", N["decoder"]),
             "router": hdr.get("ceil_r", N["router"])}
    for c in COMPS:
        if N[c] and ctx["peaks"][c] > mceil[c]:
            verdict = "FAIL"; notes.append(f"{c} peak {ctx['peaks'][c]}>ceil {mceil[c]}")
            checks.append(f"  ✗ {c} peak {ctx['peaks'][c]} > ceiling {mceil[c]}")
        elif N[c]:
            checks.append(f"  ✓ {c} peak {ctx['peaks'][c]} ≤ ceiling {mceil[c]}")
    if mig_mode == "Never":
        if surged or ph == "Completed":
            verdict = "FAIL"; notes.append(f"Never but migrated (surge={surge_inst or ledger_surge}, phase={ph})")
            checks.append(f"  ✗ mig=Never but a migration happened (surge inst={surge_inst}, ledger phase={ph})")
        else:
            checks.append(f"  ✓ mig=Never: request not actioned (no surge; ledger phase={ph or 'none'})")
        # Rejection must still CONSUME the request: annotation cleared + a
        # terminal Failed entry naming the policy. Hard only on builds exposing
        # IR status.migrations; ⓘ-soft on builds without the field.
        mig_entries = _ir_status_migrations(case_id, mcomp)
        if mig_entries is None:
            checks.append(f"  ⓘ build without IR status.migrations — Never-consumption checks soft: "
                          f"annotation {'cleared' if anno_cleared else 'still present'}, ledger phase={ph or 'none'}")
        else:
            if anno_cleared:
                checks.append("  ✓ mig=Never: request annotation consumed (cleared on rejection)")
            else:
                verdict = "FAIL"
                notes.append("Never-rejected request annotation never consumed")
                checks.append("  ✗ mig=Never: request annotation still on the ISVC — a rejected request must still be consumed")
            ir_ent = next((m for m in mig_entries if m.get("requestUUID") == reqs[inst][0]), None)
            eph = (ir_ent or {}).get("phase") or ph
            reason = str((ir_ent or entry or {}).get("reason", ""))
            if eph == "Failed":
                checks.append(f"  ✓ mig=Never: terminal Failed entry recorded (reason={reason or '(none)'})")
            else:
                verdict = "FAIL"
                notes.append(f"Never rejection entry {'missing' if not (ir_ent or entry) else f'phase={eph}'} (want terminal Failed)")
                checks.append(f"  ✗ mig=Never: no terminal Failed entry for the rejected request "
                              f"({'missing' if not (ir_ent or entry) else f'phase={eph}'}) — the policy rejection must be recorded")
    elif hdr.get("expect") == "fail":
        # Bounded-failure contract (fault cases): the armed fault holds the
        # surge, so the request must end terminal Failed WITHIN the budget while
        # the source keeps serving, the request annotation is consumed, and the
        # migration slot frees (terminal IR entry). lift=1 then proves the
        # bound is recoverable: lift the quota → a fresh request completes.
        if ph == "Failed":
            checks.append(f"  ✓ ledger terminal Failed within budget (bounded; state={state})")
        else:
            verdict = "FAIL"
            notes.append(f"expected bounded terminal Failed, got phase={ph} (state={state})")
            checks.append(f"  ✗ expected a bounded terminal Failed entry, got phase={ph} (state={state}) — the fault did not bound the migration")
        src_ready = all(
            any(p and p["alive"] and p["ready"] and p["comp"] == mcomp and p["inst"] == i
                for p in ctx["pod_state"].values())
            for i in req_insts)
        if src_ready:
            checks.append(f"  ✓ source Instance(s) {','.join(req_insts)} still Ready — served throughout the failed migration")
        else:
            verdict = "FAIL"
            notes.append("source lost during failed migration")
            checks.append(f"  ✗ source Instance(s) {','.join(req_insts)} not Ready after the failed migration — a failed migration must retain its source")
        # Request consumed (on builds with IR status.migrations the annotation
        # is consumed at ACCEPT): settle-poll before failing on a still-present
        # annotation.
        settle_deadline = time.time() + migrate_settle_budget(len(reqs))
        last_anno = 0.0
        while not anno_cleared and time.time() < settle_deadline and not ctx["closed"]:
            _read_one(ctx)
            if time.time() - last_anno >= 5:
                last_anno = time.time()
                anno_cleared = _anno_cleared()
        if anno_cleared:
            checks.append("  ✓ migration-request annotation(s) consumed (cleared)")
        else:
            verdict = "FAIL"
            notes.append("failed migration left its request annotation")
            checks.append("  ✗ migration-request annotation(s) still on the ISVC after terminal Failed — request never consumed")
        # Slot freed: the IR entry must be TERMINAL Failed, not a live entry
        # squatting the migration slot. Version-tolerant like the Completed path.
        mig_entries = _ir_status_migrations(case_id, mcomp)
        if mig_entries is None:
            checks.append("  ⓘ IR status.migrations unavailable on this build — terminal-entry check skipped (CM ledger asserted above)")
        else:
            for i, (ru, _, _) in sorted(reqs.items()):
                ent = next((m for m in mig_entries if m.get("requestUUID") == ru), None)
                eph = (ent or {}).get("phase")
                if eph == "Failed":
                    checks.append(f"  ✓ IR status.migrations[{ru[:8]}] terminal Failed (slot freed)")
                else:
                    verdict = "FAIL"
                    notes.append(f"IR migrations entry {ru[:8]} {'missing' if ent is None else f'phase={eph}'} (want terminal Failed)")
                    checks.append(f"  ✗ IR status.migrations entry {ru[:8]} {'missing' if ent is None else f'has phase={eph}'} — want terminal Failed (a live entry means the slot never freed)")
        if hdr.get("lift") and verdict == "PASS":
            _lift_quota(case_id)
            print("    ⚡ fault quota lifted — follow-up migration request")
            checks.append("  ⓘ quota lifted after the bounded failure — follow-up migration")
            ru2 = str(uuid.uuid4())
            ak2 = f"ome.io/migration-request-v1-{ru2}"
            fn2 = next((p.get("node", "") for p in ctx["pod_state"].values()
                        if p and p["alive"] and p["comp"] == mcomp and p["inst"] == inst), "")
            body2 = json.dumps({"schemaVersion": "v1", "component": mcomp, "instance": int(inst),
                                "from_node": fn2, "reason": "perm-test-lift"})
            rr2 = kubectl("-n", NS, "patch", "isvc", case_id, "--type=merge",
                          "-p", json.dumps({"metadata": {"annotations": {ak2: body2}}}))
            if rr2.returncode != 0:
                verdict = "FAIL"
                notes.append("follow-up migration patch failed")
                checks.append(f"  ✗ follow-up migration request patch failed: {rr2.stderr[:120]}")
            else:
                t2, ph2, last2 = time.time(), None, 0.0
                while time.time() - t2 < budget and not ctx["closed"]:
                    _read_one(ctx)
                    if time.time() - last2 >= 5:
                        last2 = time.time()
                        ph2 = (_migration_ledger_entry(case_id, ru2) or {}).get("phase")
                        if ph2 in ("Completed", "Failed"):
                            break
                if ph2 == "Completed":
                    checks.append(f"  ✓ follow-up migration Completed after the quota lift ({time.time() - t2:.0f}s)")
                    sd = time.time() + migrate_settle_budget(1)
                    gone = _src_gone()
                    while not gone and time.time() < sd and not ctx["closed"]:
                        _read_one(ctx)
                        gone = _src_gone()
                    if gone:
                        checks.append(f"  ✓ follow-up drained source Instance {inst}")
                    else:
                        verdict = "FAIL"
                        notes.append("follow-up completed but source never drained")
                        checks.append(f"  ✗ follow-up Completed but source Instance {inst} never drained")
                else:
                    verdict = "FAIL"
                    notes.append(f"follow-up migration did not complete (phase={ph2})")
                    checks.append(f"  ✗ follow-up migration never reached Completed after the quota lift (phase={ph2}) — slot/headroom not actually freed")
            # The follow-up surge happens AFTER the main peak gate ran above —
            # re-assert the ceiling over the whole case (fan-out would show here).
            for c in COMPS:
                if N[c] and ctx["peaks"][c] > mceil[c]:
                    verdict = "FAIL"
                    notes.append(f"{c} post-lift peak {ctx['peaks'][c]}>ceil {mceil[c]}")
                    checks.append(f"  ✗ {c} peak {ctx['peaks'][c]} exceeded ceiling {mceil[c]} during the post-lift follow-up")
        # Capacity floor: the source must have served throughout the bounded
        # failure (and the follow-up, when lifted).
        verdict = min_ready_checks(ctx, sorted(ctx["configured"]), verdict, notes, checks)
    else:
        if ph == "Completed":
            checks.append(f"  ✓ ledger Completed (surgeInstance={entry.get('surgeInstance')})")
        else:
            verdict = "FAIL"; notes.append(f"ledger not Completed (phase={ph}, state={state})")
            checks.append(f"  ✗ ledger did not reach Completed (phase={ph}, state={state})")
        if not surged:
            verdict = "FAIL"; notes.append("no surge Instance observed (watcher or ledger)")
            checks.append("  ✗ no replacement (surge) Instance ever appeared (watcher + ledger)")
        elif surge_node and from_node and surge_node != from_node:
            checks.append(f"  ✓ surge Instance {surge_inst} on a different node ({surge_node} != {from_node})")
        else:
            checks.append(f"  ⓘ surge happened (watcher inst={surge_inst}, ledger surgeInstance={ledger_surge}); node compare inconclusive")
        # Source-drain + request-consumption hygiene: hard checks (the class the
        # expired-mid-drain and same-name-recreate bugs live in). Gated on
        # Completed: a FAILED migration is REQUIRED to keep its source
        # (bounded-failure contract), so asserting src_gone there would demand
        # the wrong behavior — and the ledger check above already reds the case.
        if ph == "Completed":
            if src_gone:
                checks.append(f"  ✓ source Instance(s) {','.join(req_insts)} drained")
            else:
                verdict = "FAIL"
                _sb = migrate_settle_budget(len(req_insts))
                notes.append(f"source Instance(s) {','.join(req_insts)} still present {_sb}s after Completed")
                checks.append(f"  ✗ source Instance(s) {','.join(req_insts)} still present {_sb}s after ledger Completed — source did not drain")
            if anno_cleared:
                checks.append("  ✓ migration-request annotation(s) cleared")
            else:
                verdict = "FAIL"
                notes.append("migration-request annotation(s) still on the ISVC")
                checks.append("  ✗ migration-request annotation(s) still on the ISVC — request never consumed")
            # The authoritative per-request state is the IR's status.migrations;
            # assert a terminal Completed entry per request UUID.
            # Version-tolerant: skipped (with a note) on builds without the
            # field — the CM-ledger assertion above still gates there.
            mig_entries = _ir_status_migrations(case_id, mcomp)
            if mig_entries is None:
                checks.append("  ⓘ IR status.migrations unavailable on this build — per-request IR entry check skipped (CM ledger asserted above)")
            else:
                for i, (ru, _, _) in sorted(reqs.items()):
                    ent = next((m for m in mig_entries if m.get("requestUUID") == ru), None)
                    eph = (ent or {}).get("phase")
                    if eph == "Completed":
                        checks.append(f"  ✓ IR status.migrations[{ru[:8]}] terminal Completed")
                    else:
                        verdict = "FAIL"
                        notes.append(f"IR status.migrations entry {ru[:8]} {'missing' if ent is None else f'phase={eph}'}")
                        checks.append(f"  ✗ IR status.migrations entry for request {ru[:8]} {'missing' if ent is None else f'has phase={eph}'} — want terminal Completed")
        else:
            checks.append("  ⓘ source-drain / annotation-clear / IR-entry checks skipped — migration did not complete (ledger check above already failed)")
        if len(req_insts) > 1:
            # Concurrent/batched migration: each source must surge its OWN
            # replacement — collapsing N sources onto one surge index is the
            # fleet-wipe shape. Watcher indices catch the concurrent case; the
            # ledger's per-request surgeInstance also covers SERIALIZED batches,
            # where a later request may legitimately reuse an index a finished
            # one already freed (so watcher-distinct undercounts).
            ledger_surges = {e.get("surgeInstance") for e in entries.values()
                             if e.get("phase") == "Completed" and e.get("surgeInstance") is not None}
            # Distinct FRESH replacement pods (by UID) is the authoritative
            # count — index-reuse safe. A source that migrates onto its own
            # freed index still produces a new UID, so this catches the genuine
            # fleet-wipe collision (many sources → one shared pod) without
            # false-positiving on legitimate same-index replacement.
            n_surge = max(len(surge_uids), len(surge_seen), len(ledger_surges))
            if n_surge >= len(req_insts):
                checks.append(f"  ✓ {len(req_insts)} migrations each surged their own replacement "
                              f"(fresh pods {len(surge_uids)}, watcher indices {sorted(surge_seen)}, ledger surgeInstances {sorted(ledger_surges)})")
            else:
                verdict = "FAIL"
                notes.append(f"migration collision: {len(req_insts)} sources surged only {n_surge} distinct replacement(s)")
                checks.append(f"  ✗ migration collision: {len(req_insts)} migrations surged only {n_surge} distinct replacement(s) "
                              f"(fresh pods {len(surge_uids)}, watcher indices {sorted(surge_seen)}, ledger surgeInstances {sorted(ledger_surges)}) — sources collapsed onto a shared replacement (fleet-wipe shape)")
        if defer_rollout:
            # Ordering deliberately UNASSERTED: the product PREEMPTS — an
            # accepted migration's requeue skips the update pass (there is no
            # defer-then-run queue). Whether preempt or defer is the contract is
            # an open product-design decision, so this case only asserts both
            # operations complete without wedging each other in bounded total
            # time (migration Completed is gated above).
            if rollout_conv_ts is None:
                verdict = "FAIL"
                notes.append("mid-rotate migration: the in-flight rollout never converged (wedge)")
                checks.append("  ✗ mid-rotate migration: the concurrent rollout never converged onto its new revision within budget — the migration wedged the rollout")
            else:
                checks.append(f"  ✓ mid-rotate migration: rollout converged onto its new revision at {rollout_conv_ts - ctx['trigger_ts']:.0f}s (ordering vs the migration deliberately unasserted)")
        if dup:
            cnt = sum(1 for e in _migration_ledger_entries(case_id)
                      if e.get("requestUUID") == reqs[inst][0])
            if cnt == 1 and len(surge_seen) <= 1:
                checks.append(f"  ✓ duplicate-UUID re-delivery deduped: exactly one ledger entry, {len(surge_seen)} surge index")
            else:
                verdict = "FAIL"
                notes.append(f"duplicate-UUID re-delivery not deduped (entries={cnt}, surges={sorted(surge_seen)})")
                checks.append(f"  ✗ duplicate-UUID re-delivery produced {cnt} ledger entr(ies) and surge indices {sorted(surge_seen)} — want exactly one entry / one migration")
        if hint_node:
            if surge_node and surge_node == hint_node:
                checks.append(f"  ✓ hint honored: surge landed on the hinted node {hint_node}")
            else:
                checks.append(f"  ⓘ hint {hint_node} not taken (surge on {surge_node or '?'}) — "
                              f"a hint is a preference; capacity/schedulability may divert (soft)")
        # A migration surges a replacement before draining the source, so capacity
        # must never drop below the floor, gangs must stay whole, and no two gangs
        # may collapse onto one index (the fleet-wipe shape at migration scale).
        verdict = min_ready_checks(ctx, sorted(ctx["configured"]), verdict, notes, checks)
        verdict = index_collision_checks(ctx, sorted(ctx["configured"]), verdict, notes, checks)
        verdict = gang_atomicity_checks(ctx, sorted(ctx["configured"]), verdict, notes, checks)
    ctx["converged_at"] = (time.time() - ctx["trigger_ts"]) if verdict == "PASS" else None
    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


def _recreate(case_id, yaml_file, desc, hdr):
    """Delete the case ISVC (optionally mid-rollout / mid-migration), require the
    universal teardown to converge, then IMMEDIATELY re-apply the same manifest:
    the same-name recreate must reach Ready on all-fresh pod UIDs — no
    AlreadyExists wedge, no adopted leftovers. With fault=cordon the recreate
    must land only on schedulable nodes; with fault=quota:0 the teardown must
    free the capacity that re-admits the recreate under the same cap."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    verdict, notes, checks = "PASS", [], []
    during = hdr.get("during")
    if during == "rollout":
        print("  Phase 2: ISVC revision bump (v2) — delete lands mid-rotate")
        if not _bump_rev(ctx, _bumped(ctx), "v2"):
            return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "isvc bump failed")
        t0 = time.time()
        while time.time() - t0 < 90 and not ctx["closed"]:
            _read_one(ctx)
            if any(p and p["alive"] and p.get("hash") and p["hash"] not in ctx["old_hashes"]
                   for p in ctx["pod_state"].values()):
                break
    elif during == "migrate":
        mcomp = "engine" if "engine" in ctx["configured"] else sorted(ctx["configured"])[0]
        mi = str(hdr.get("inst", 0)).split(",")[0].strip()
        ru = str(uuid.uuid4())
        body = {"schemaVersion": "v1", "component": mcomp, "instance": int(mi),
                "from_node": ctx["old_nodes"].get((mcomp, mi), ""), "reason": "perm-test-delete-mid-migration"}
        orig = {p["inst"] for p in ctx["pod_state"].values() if p and p["alive"] and p["comp"] == mcomp}
        print("  Phase 2: migration request — delete lands mid-migration")
        rr = kubectl("-n", NS, "patch", "isvc", case_id, "--type=merge",
                     "-p", json.dumps({"metadata": {"annotations":
                                                    {f"ome.io/migration-request-v1-{ru}": json.dumps(body)}}}))
        if rr.returncode != 0:
            return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", rr.stderr[:200])
        t0 = time.time()
        while time.time() - t0 < 90 and not ctx["closed"]:
            _read_one(ctx)
            if any(p and p["alive"] and p["comp"] == mcomp and p["inst"] not in orig
                   for p in ctx["pod_state"].values()) or _migration_ledger_entry(case_id, ru):
                break
    pass1_uids = {p["uid"] for p in ctx["pod_state"].values() if p and p.get("uid")}
    total_pods = sum(ctx["N"][c] * ctx["pods_per_inst"].get(c, 1) for c in ctx["configured"])
    _stop_watcher(ctx)
    print("  Phase 3: delete, teardown convergence, then same-name recreate")
    kubectl("delete", "-n", NS, "isvc", case_id, "--wait=false")
    verdict = teardown_check(case_id, total_pods, verdict, notes, checks)
    hdr2 = dict(hdr)
    hdr2.pop("fault", None)   # pass-1 apparatus (quota/cordon) stays armed; never re-arm
    t_re = time.time()
    ctx2, err2 = _setup_and_ready(case_id, yaml_file, hdr2)
    if err2:
        notes.append(f"same-name recreate failed: {err2[0]} {err2[1]}")
        return _write_fail(case_id, desc, hdr, "RECREATE_FAIL", "; ".join(notes))
    ctx2["fault"] = ctx.get("fault")   # surface pass-1 fault notes/errors in the verdict
    ctx2["converged_at"] = time.time() - t_re
    reused = pass1_uids & {p["uid"] for p in ctx2["pod_state"].values() if p and p.get("uid")}
    if reused:
        verdict = "FAIL"
        notes.append(f"recreate reused {len(reused)} pod UID(s)")
        checks.append(f"  ✗ recreate reused {len(reused)} pre-delete pod UID(s) — pods survived the teardown")
    else:
        checks.append("  ✓ same-name recreate reached Ready on all-fresh pod UIDs (no AlreadyExists wedge, no adopted pods)")
    f = ctx.get("fault")
    if f and f["kind"] == "cordon" and f.get("node"):
        on_bad = sorted({f"{p['comp']}-{p['inst']}" for p in ctx2["pod_state"].values()
                         if p and p["alive"] and p.get("node") == f["node"]})
        if on_bad:
            verdict = "FAIL"
            notes.append(f"recreate scheduled onto cordoned node {f['node']}")
            checks.append(f"  ✗ recreate placed {on_bad} on the cordoned node {f['node']}")
        else:
            checks.append(f"  ✓ recreate avoided the cordoned node {f['node']}")
    return _finish(case_id, desc, hdr, ctx2, "RECREATED", verdict, notes, checks)


def _component_irs(case_id, comp):
    """[(name, deleting)] for the component's InferenceReplicas (label-selected);
    [] when gone or the CRD is absent (version-tolerant)."""
    r = kubectl("-n", NS, "get", "inferencereplicas", "-l",
                f"ome.io/inferenceservice={case_id}", "-o", "json")
    if r.returncode != 0:
        return []
    try:
        items = json.loads(r.stdout).get("items", [])
    except Exception:
        return []
    return [((it.get("metadata") or {}).get("name", "?"),
             bool((it.get("metadata") or {}).get("deletionTimestamp")))
            for it in items if (it.get("spec") or {}).get("component") == comp]


def _compremove(case_id, yaml_file, desc, hdr):
    """Patch a whole component OUT of the ISVC spec: the removed component's IR
    must be swept (drained teardown — DeletionTimestamp observed, then gone)
    along with every one of its pods, while the surviving components' pods stay
    untouched (the removed-component-leak guard). readd=1 then re-applies the
    original manifest and the component must converge back to Ready."""
    comp = str(hdr.get("remove", ""))
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    N = ctx["N"]
    if comp not in ctx["configured"]:
        return _write_fail(case_id, desc, hdr, "CASE_ERROR", f"remove={comp} is not a configured component")
    keep = sorted(ctx["configured"] - {comp})
    keep_uids = {c: {p["uid"] for p in ctx["pod_state"].values()
                     if p and p["alive"] and p["comp"] == c and p.get("uid")} for c in keep}
    print(f"  Phase 2: removing /spec/{comp} (keep {keep})")
    rr = kubectl("-n", NS, "patch", "isvc", case_id, "--type=json",
                 "-p", json.dumps([{"op": "remove", "path": f"/spec/{comp}"}]))
    if rr.returncode != 0:
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", rr.stderr[:200])
    ctx["trigger_ts"] = time.time()
    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * N[comp] * ctx["pods_per_inst"][comp])
    saw_deleting, gone_at, last_poll = False, None, 0.0
    print(f"  Phase 3: watching the removed component sweep (IR + pods; ≤{budget:.0f}s)")
    while time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
        _read_one(ctx)
        now = time.time()
        if now - last_poll >= 2:
            last_poll = now
            irs = _component_irs(case_id, comp)
            saw_deleting = saw_deleting or any(d for _, d in irs)
            pods_left = any(p and p["alive"] and p["comp"] == comp for p in ctx["pod_state"].values())
            if not irs and not pods_left:
                gone_at = now - ctx["trigger_ts"]
                break
    verdict, notes, checks = "PASS", [], []
    state = "REMOVED" if gone_at is not None else "REMOVE_TIMEOUT"
    if gone_at is None:
        irs = _component_irs(case_id, comp)
        pods = sorted({f"{comp}-{p['inst']}" for p in ctx["pod_state"].values()
                       if p and p["alive"] and p["comp"] == comp})
        verdict = "FAIL"
        notes.append(f"removed {comp} not swept in {budget:.0f}s (IRs={len(irs)}, pods={pods})")
        checks.append(f"  ✗ removed component {comp} still has {len(irs)} IR(s) and pods {pods} "
                      f"after {budget:.0f}s — the removed-component leak")
    else:
        ctx["converged_at"] = gone_at
        checks.append(f"  ✓ removed component {comp} swept (IR + pods gone) at {gone_at:.0f}s")
        checks.append("  ✓ IR removal was a drained teardown (DeletionTimestamp observed)" if saw_deleting
                      else "  ⓘ IR DeletionTimestamp not observed (sweep faster than the 2s poll)")
    for c in keep:
        cur = {p["uid"] for p in ctx["pod_state"].values()
               if p and p["alive"] and p["comp"] == c and p.get("uid")}
        if cur == keep_uids[c]:
            checks.append(f"  ✓ {c} untouched during the removal ({len(cur)} pod UID(s) stable)")
        else:
            verdict = "FAIL"
            notes.append(f"{c} pods churned during {comp} removal")
            checks.append(f"  ✗ {c} pods churned during the {comp} removal "
                          f"({len(keep_uids[c] - cur)} gone, {len(cur - keep_uids[c])} new)")
    if hdr.get("readd") and verdict == "PASS":
        print(f"  Phase 4: re-adding {comp} (re-apply the original manifest)")
        rr = kubectl("apply", "--validate=false", "-n", NS, "-f", str(yaml_file))
        rebudget = min(MAX_BUDGET_S, INIT_BASE_S + INIT_PER_POD_S * N[comp] * ctx["pods_per_inst"][comp])
        t0, back = time.time(), False
        while rr.returncode == 0 and time.time() - t0 < rebudget and not ctx["closed"]:
            _read_one(ctx)
            if ctx["agg"].get(comp, (0, 0, 0))[1] == N[comp]:
                back = True
                break
        if back:
            checks.append(f"  ✓ round-trip: {comp} re-added and Ready {N[comp]}/{N[comp]}")
        else:
            verdict = "FAIL"
            notes.append(f"re-added {comp} never came back Ready")
            checks.append(f"  ✗ round-trip: re-added {comp} did not reach Ready {N[comp]} within {rebudget:.0f}s")
    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


def _pause_annotation(case_id, value):
    """Set or clear (value "") the rollout-paused annotation on the case ISVC
    and let it project onto the IR before the next trigger."""
    if value:
        kubectl("-n", NS, "annotate", "isvc", case_id,
                f"ome.io/rollout-paused={value}", "--overwrite")
    else:
        kubectl("-n", NS, "annotate", "isvc", case_id, "ome.io/rollout-paused-")
    time.sleep(PAUSE_PROJECT_S)


def _isvc_pause_value(case_id):
    r = kubectl("-n", NS, "get", "isvc", case_id, "-o",
                r'jsonpath={.metadata.annotations.ome\.io/rollout-paused}')
    return (r.stdout or "").strip() if r.returncode == 0 else ""


def _kill(case_id, yaml_file, desc, hdr):
    """Delete a live pod and assert the RestartPolicy outcome by which pods get
    new UIDs (recreated):
      None             -> only the deleted pod is recreated (1 pod)
      RecreateInstance -> the whole Instance (leader+workers) recreates
    `kill=leader|worker` (gangs) selects which pod dies.

    `pause=` layers the rollout-paused circuit breaker over the kill:
      pause=true   -> RecreateInstance must repair WHILE paused (the pause
                      suspends fleet changes, not the RestartPolicy); policy
                      None must hold degraded until the annotation is removed.
      pause=freeze -> no repair while frozen; downgrading the annotation to
                      "true" must repair without ever unpausing."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    N, configured = ctx["N"], ctx["configured"]
    ppi = ctx["pods_per_inst"]
    policy = hdr.get("restart", "RecreateInstance")
    pmode = str(hdr.get("pause", ""))  # "" | true | freeze
    kcomp = "engine" if "engine" in configured else sorted(configured)[0]
    ktgt = str(hdr.get("kill", ""))   # leader | worker | ""
    if pmode:
        print(f"  Phase 1b: pause rollout (ome.io/rollout-paused={pmode})")
        _pause_annotation(case_id, pmode)
    cands = [(n, p) for n, p in ctx["pod_state"].items()
             if p and p["alive"] and p["comp"] == kcomp]
    if ktgt:
        cands = [x for x in cands if x[1]["runner"] == ktgt] or cands
    if not cands:
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", f"no live {kcomp} pod to kill")
    kill_name, kill_pod = cands[0]
    killed_inst = kill_pod["inst"]
    pre_uids = {p["uid"] for p in ctx["pod_state"].values() if p and p["alive"] and p.get("uid")}
    print(f"  Phase 2: delete pod {kill_name} (comp={kcomp} inst={killed_inst} runner={kill_pod['runner']}) restartPolicy={policy} pause={pmode or 'off'}")
    kubectl("-n", NS, "delete", "pod", kill_name, "--wait=false")
    ctx["trigger_ts"] = time.time()
    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * sum(N[c] * ppi[c] for c in configured))
    exp = {"None": 1, "RecreateInstance": ppi[kcomp]}.get(policy, 1)

    def recreated_now():
        return [p for p in ctx["pod_state"].values()
                if p and p["alive"] and p.get("uid") and p["uid"] not in pre_uids]

    verdict, notes, checks = "PASS", [], []

    # A pause that must hold: freeze holds every policy; plain pause holds
    # policy None (its recovery path is the Create pass, which pause parks).
    held = pmode == "freeze" or (pmode == "true" and policy == "None")
    if held:
        print(f"  Phase 3a: paused hold — expecting NO recreation for {KILL_PAUSE_HOLD_S}s")
        hold_end = time.time() + KILL_PAUSE_HOLD_S
        early = 0
        while time.time() < hold_end and not ctx["closed"]:
            _read_one(ctx)
            if recreated_now():
                early = len(recreated_now())
                break
        if early:
            verdict = "FAIL"
            notes.append(f"pause={pmode}: {early} pod(s) recreated during the hold")
            checks.append(f"  ✗ pause={pmode}: repair ran during the paused hold ({early} recreated)")
        else:
            checks.append(f"  ✓ pause={pmode}: no recreation across the {KILL_PAUSE_HOLD_S}s hold")
        # Lift: freeze downgrades to a plain pause (repair must run while
        # still paused); plain pause + None unpauses (Create recovers).
        lift = "true" if pmode == "freeze" else ""
        print(f"  Phase 3b: lift — set rollout-paused={lift or '(removed)'}")
        _pause_annotation(case_id, lift)
        ctx["trigger_ts"] = time.time()

    print(f"  Phase 3: watching recovery (≤{budget}s)")
    state, stable, dipped = "RECOVERING", None, False
    while time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
        agg = _read_one(ctx)
        if any(agg.get(c, (0, 0, 0))[1] < N[c] for c in configured):
            dipped = True
        # Settle only once recovered to N ready AND the full expected recreation
        # count is observed — so a STAGGERED cascade (recreate killed pod first,
        # then the rest) isn't undercounted by an early first-recovery. If the
        # controller under-recreates, rc stays < exp and it times out (honest FAIL).
        if dipped and all(agg.get(c, (0, 0, 0))[1] == N[c] for c in configured) and len(recreated_now()) >= exp:
            if stable is None:
                stable = time.time()
            elif time.time() - stable >= STABILITY_S:
                state = "RECOVERED"
                ctx["converged_at"] = stable - ctx["trigger_ts"]
                break
        else:
            stable = None

    recreated = recreated_now()
    rc = len(recreated)
    elsewhere = [p for p in recreated if not (p["comp"] == kcomp and p["inst"] == killed_inst)]

    if state != "RECOVERED":
        verdict = "FAIL"; notes.append(f"did not recover (state={state}, recreated={rc})")
        checks.append(f"  ✗ never recovered to N ready (state={state}; {rc} pod(s) recreated so far)")
    else:
        checks.append(f"  ✓ recovered to N ready at {ctx['converged_at']:.1f}s")
    # Only None + RecreateInstance are supported (RecreateISVC was dropped from
    # the API as semantically incoherent on the IR-managed path). Both scope the
    # recreate to the killed Instance: assert no over-cascade + the exact count.
    if elsewhere:
        verdict = "FAIL"; notes.append(f"{policy}: {len(elsewhere)} pods recreated outside killed instance")
        checks.append(f"  ✗ {policy}: {len(elsewhere)} pod(s) recreated OUTSIDE killed instance {kcomp}/{killed_inst} — over-cascade")
    if rc != exp:
        verdict = "FAIL"; notes.append(f"{policy}: recreated {rc}, want {exp}")
        checks.append(f"  ✗ {policy}: {rc} pod(s) recreated, expected {exp} ({'pod-only' if policy == 'None' else 'whole instance'})")
    elif not elsewhere:
        checks.append(f"  ✓ {policy}: exactly {rc} pod(s) recreated, scoped to killed instance {kcomp}/{killed_inst}")
    # Repair-under-pause contract: recovery must have completed with the pause
    # annotation still in force (repair may never require unpausing).
    if pmode and not (held and pmode == "true"):
        want = "true"
        got = _isvc_pause_value(case_id)
        if got != want:
            verdict = "FAIL"
            notes.append(f"pause annotation drifted during recovery (got {got!r}, want {want!r})")
            checks.append(f"  ✗ pause: annotation was {got!r} after recovery, expected {want!r} — repair must not require unpausing")
        else:
            checks.append(f"  ✓ pause: recovery completed with rollout-paused={want} still set")
    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


def _pairing_patch(case_id, target):
    """Patch spec.rollout.pairingProtocol to `target` ("" removes the field —
    JSON merge patch null deletes the key). Returns (ok, last stderr line)."""
    patch = {"spec": {"rollout": {"pairingProtocol": (target or None)}}}
    r = kubectl("-n", NS, "patch", "isvc", case_id, "--type=merge", "-p", json.dumps(patch))
    msg = (r.stderr.strip().splitlines() or [""])[-1]
    return r.returncode == 0, msg


def _sample_rollout_holds(case_id, bumped, holds):
    """One rolloutHold observation: for each bumped Component record the gate
    (status.components.<c>.lifecycle.rolloutHold.gate) currently reported."""
    st = fetch_status(case_id)
    comps = st.get("components") or {}
    for c in bumped:
        rh = ((comps.get(c) or {}).get("lifecycle") or {}).get("rolloutHold")
        if isinstance(rh, dict) and rh.get("gate"):
            holds.setdefault(c, {})[rh["gate"]] = rh.get("reason", "")[:160]


def pairing_surface_checks(case_id, ctx, target, bumped, verdict, notes, checks):
    """Post-convergence pairing surface verdicts. `target` is the protocol every
    engine/decoder revision must have converged onto ("" = cleared):
      1. every alive engine/decoder pod carries ome.io/pairing-protocol=target
         (label absent when target is "");
      2. every live (percent>0) traffic target on the bumped Components
         publishes pairingProtocol=target, and the traffic list is non-empty;
      3. every routing Service named by a live traffic target carries the
         label in METADATA only — and NO Service of the case ever carries it
         in spec.selector (pod selection stays revision-hash based)."""
    # 1. Pod labels (from the watcher's live pod state).
    bad_pods = [f"{n}={p.get('pairing', '')!r}" for n, p in sorted(ctx["pod_state"].items())
                if p and p["alive"] and p["comp"] in ("engine", "decoder")
                and p.get("pairing", "") != target]
    if bad_pods:
        verdict = "FAIL"
        notes.append(f"pod pairing labels != {target!r}: {len(bad_pods)} pod(s)")
        checks.append(f"  ✗ pairing: {len(bad_pods)} engine/decoder pod(s) do not carry {PAIRING_LABEL}={target!r}: "
                      + ", ".join(bad_pods[:4]) + ("…" if len(bad_pods) > 4 else ""))
    else:
        checks.append(f"  ✓ pairing: every engine/decoder pod carries {PAIRING_LABEL}={target!r}" +
                      (" (label absent)" if target == "" else ""))

    # 2. Traffic targets.
    status = fetch_status(case_id)
    comps_status = status.get("components") or {}
    live_rev_names = set()
    for c in sorted(bumped):
        traffic = (comps_status.get(c) or {}).get("traffic") or []
        live = [t for t in traffic if t.get("percent", 0) > 0]
        if not live:
            verdict = "FAIL"
            notes.append(f"{c} publishes no live traffic target")
            checks.append(f"  ✗ pairing: {c} publishes no live (percent>0) traffic target at convergence")
            continue
        bad = [t for t in live if str(t.get("pairingProtocol", "") or "") != target]
        live_rev_names.update(t.get("revisionName", "") for t in live)
        if bad:
            verdict = "FAIL"
            notes.append(f"{c} traffic pairingProtocol != {target!r}")
            checks.append(f"  ✗ pairing: {c} live traffic target(s) publish the wrong protocol: "
                          + ", ".join(f"{t.get('revisionName', '?')}={t.get('pairingProtocol', '')!r}" for t in bad[:3]))
        else:
            checks.append(f"  ✓ pairing: {c} live traffic targets all publish pairingProtocol={target!r}")

    # 3. Services: metadata label on routed per-revision Services; NEVER the selector.
    r = kubectl("-n", NS, "get", "svc", "-l", f"ome.io/inferenceservice={case_id}", "-o", "json")
    svcs = []
    if r.returncode == 0:
        try:
            svcs = json.loads(r.stdout).get("items", [])
        except Exception:
            svcs = []
    by_name = {}
    sel_viol = []
    for s in svcs:
        name = ((s.get("metadata") or {}).get("name")) or "?"
        by_name[name] = s
        if PAIRING_LABEL in ((s.get("spec") or {}).get("selector") or {}):
            sel_viol.append(name)
    if sel_viol:
        verdict = "FAIL"
        notes.append(f"pairing label in Service selector: {sel_viol}")
        checks.append(f"  ✗ pairing: {PAIRING_LABEL} appeared in spec.selector of {sel_viol} — the protocol must never select pods")
    else:
        checks.append(f"  ✓ pairing: no Service selector carries {PAIRING_LABEL} (metadata-only contract held)")
    svc_bad = []
    for name in sorted(n for n in live_rev_names if n):
        s = by_name.get(name)
        if s is None:
            svc_bad.append(f"{name}=MISSING")
            continue
        got = (((s.get("metadata") or {}).get("labels")) or {}).get(PAIRING_LABEL, "")
        if got != target:
            svc_bad.append(f"{name}={got!r}")
    if svc_bad:
        verdict = "FAIL"
        notes.append(f"routing Service pairing labels != {target!r}: {svc_bad}")
        checks.append(f"  ✗ pairing: routing Service(s) named by live traffic lack metadata {PAIRING_LABEL}={target!r}: "
                      + ", ".join(svc_bad[:4]))
    elif live_rev_names:
        checks.append(f"  ✓ pairing: every live routing Service carries metadata {PAIRING_LABEL}={target!r}" +
                      (" (label absent)" if target == "" else ""))
    return verdict


def _pairing(case_id, yaml_file, desc, hdr):
    """mode=pairing: the rollout trigger is a spec.rollout.pairingProtocol
    transition (set / bump / clear) instead of an annotation bump. The token
    rides every engine+decoder revision, so the patch alone re-mints and rolls
    BOTH pairing Components (never the router — isolation is asserted).

    Updated-pod detection is revision-hash based for EVERY strategy: an
    in-place op restamps the revision-hash label (and the pairing label) on the
    live pod, so hash-vs-old works uniformly; UID stability is asserted
    separately for in-place strategies.

    Optional mid-transition verbs (one-shot, after first observed progress):
    pause=true|freeze (hold, assert parked, lift, converge), pkill=1 (delete
    the last old-cohort decoder pod; recovery must still converge — pair-alive
    and the decoder's capacity floor are exempt on such cases since the
    harness, not the product, destroys the pair), scale_e=/scale_d= (re-target
    replicas mid-roll), proto2= (re-patch the protocol once >=1 new-cohort pod
    is READY — a re-target over a partially-landed intermediate; converge on
    the FINAL cohort with no Instance stranded on the intermediate one).

    expect=reject inverts the case into an update-time admission negative: the
    Phase-2 patch itself must be DENIED (reason= substring asserted), because
    an uncoordinated non-empty→non-empty change is rejected by the webhook."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    N, configured = ctx["N"], ctx["configured"]
    ceil = {"engine": hdr.get("ceil_e", N["engine"]), "decoder": hdr.get("ceil_d", N["decoder"]),
            "router": hdr.get("ceil_r", N["router"])}
    info = ctx["info"]
    ppi = ctx["pods_per_inst"]
    from_proto = info.get("pairing") or ""
    target = "" if str(hdr.get("proto")) == "clear" else str(hdr.get("proto"))
    final_target = target
    proto2 = hdr.get("proto2")
    if proto2 is not None:
        final_target = "" if str(proto2) == "clear" else str(proto2)
    # Hash-based updated detection for every strategy (see docstring); the
    # in-place contract is asserted through UID stability below instead.
    ctx["modes"] = {c: "surge" for c in configured}
    bumped = {"engine", "decoder"} & configured
    controls = configured - bumped
    pmode = str(hdr.get("pause", "") or "")

    # Update-time admission negative: the patch itself must be denied.
    if hdr.get("expect") == "reject":
        want = hdr.get("reason")
        print(f"\n  Phase 2: patch pairingProtocol {from_proto!r}→{target!r} — expecting admission DENIAL")
        ok, msg = _pairing_patch(case_id, target)
        verdict, notes, checks = "PASS", [], []
        if ok:
            verdict = "FAIL"
            notes.append("protocol patch ACCEPTED but admission rejection was expected")
            checks.append("  ✗ pairing: uncoordinated protocol change was ACCEPTED — ValidatePairingProtocolUpdate did not fire")
        elif want and want not in msg:
            verdict = "FAIL"
            notes.append(f"rejected but message lacks reason '{want}'")
            checks.append(f"  ✗ pairing: patch rejected for the WRONG reason (want ~{want}): {msg[:160]}")
        else:
            checks.append(f"  ✓ pairing: protocol change denied as expected ({want or 'any reason'}): {msg[:160]}")
        return _finish(case_id, desc, hdr, ctx, "REJECTED", verdict, notes, checks)

    print(f"\n  Phase 2: patch pairingProtocol {from_proto!r}→{target!r} on the ISVC (rolls {sorted(bumped)}; "
          f"controls {sorted(controls) or '[]'})")
    ok, msg = _pairing_patch(case_id, target)
    if not ok:
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", f"protocol patch denied: {msg[:160]}")
    ctx["trigger_ts"] = time.time()
    ctx["track_comps"] = sorted(configured)
    # Arm the pair-alive invariant for a non-empty→non-empty transition with
    # both pairing Components configured (its activation condition mirrors the
    # gate's). Cases that destroy the pair themselves (pkill) waive it via
    # inv_off=C in their header.
    if from_proto and target and bumped == {"engine", "decoder"}:
        ctx["inv"]["pairing"] = {"frm": from_proto, "to": target, "armed": False, "seen": False}

    # Budget: rotate base + per bumped pod, plus canary step pauses when the
    # case ships a canary group (the step machine advances on its own timers).
    extra = 0
    if info.get("canary"):
        extra += sum(parse_duration((s.get("pause") or {}).get("duration"))
                     for s in info["canary"].get("steps", []) if isinstance(s, dict))
    scale_to = {"engine": hdr.get("scale_e"), "decoder": hdr.get("scale_d")}
    scale_to = {c: int(v) for c, v in scale_to.items() if isinstance(v, int) and N.get(c, 0) > 0}
    extra += ROTATE_PER_POD_S * sum(max(0, t - N[c]) * ppi.get(c, 1) for c, t in scale_to.items())
    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * sum(N[c] * ppi[c] for c in bumped) + extra)

    # want = the convergence replica target (scale verbs move it mid-roll);
    # ctx["N"] becomes the floor basis min(start, target) once a scale fires.
    want = dict(N)
    holds = {}
    hold_last = 0.0
    pk_fired = False
    sc_fired = False
    p2_fired = False
    pre_final = None       # hash universe before the proto2 re-patch (X-2 check)
    pause_done = pmode == ""
    verdict, notes, checks = "PASS", [], []

    def progressed_now():
        return any(p and p["alive"] and p.get("hash") and p["hash"] not in ctx["old_hashes"]
                   for p in ctx["pod_state"].values())

    def new_pod_split():
        """(ready, inflight) alive new-rev pod counts across bumped comps."""
        rd = inflight = 0
        for p in ctx["pod_state"].values():
            if p and p["alive"] and p["comp"] in bumped and p.get("hash") and p["hash"] not in ctx["old_hashes"]:
                if p["ready"]:
                    rd += 1
                else:
                    inflight += 1
        return rd, inflight

    def bumped_rotated():
        """Every bumped Component fully on the FINAL cohort at want[c]."""
        for c in bumped:
            a, rd, up = ctx["agg"].get(c, (0, 0, 0))
            if a != want[c] or rd != want[c]:
                return False
            if pre_final is not None:
                live = [p for p in ctx["pod_state"].values() if p and p["alive"] and p["comp"] == c]
                if not live or any(not p.get("hash") or p["hash"] in pre_final for p in live):
                    return False
            elif up != want[c]:
                return False
        return True

    print(f"\n  Phase 3: watching the protocol transition (≤{budget}s; peak ≤ {ceil['engine']}/{ceil['decoder']}/{ceil['router']})")
    state, stable = "ROTATING", None
    progressed = False
    while time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
        agg = _read_one(ctx)
        now = time.time()
        progressed = progressed or progressed_now()
        if now - hold_last >= INV_TICK_S:
            hold_last = now
            _sample_rollout_holds(case_id, bumped, holds)

        # Mid-transition verbs, one-shot each, gated on observed progress.
        if progressed and not pause_done:
            pause_done = True
            print(f"  Phase 3a: pause mid-transition (ome.io/rollout-paused={pmode})")
            _pause_annotation(case_id, pmode)   # sleeps PAUSE_PROJECT_S (settle)
            _read_one(ctx)
            names_at_settle = {n for n, p in ctx["pod_state"].items() if p and p["alive"]}
            rd0, inflight0 = new_pod_split()
            print(f"  Phase 3b: paused hold {KILL_PAUSE_HOLD_S}s (rotated {rd0} ready, {inflight0} in flight at settle)")
            hold_end = time.time() + KILL_PAUSE_HOLD_S
            while time.time() < hold_end and not ctx["closed"]:
                _read_one(ctx)
                if time.time() - hold_last >= INV_TICK_S:
                    hold_last = time.time()
                    _sample_rollout_holds(case_id, bumped, holds)
            new_names = {n for n, p in ctx["pod_state"].items()
                         if p and p["alive"] and p["comp"] in bumped and n not in names_at_settle}
            rd1, _ = new_pod_split()
            if new_names:
                verdict = "FAIL"
                notes.append(f"pause={pmode}: {len(new_names)} new pod(s) created during the hold")
                checks.append(f"  ✗ pause={pmode}: rollout kept creating pods during the hold ({sorted(new_names)[:3]}…) — pause not honored")
            elif rd1 > rd0 + inflight0:
                verdict = "FAIL"
                notes.append(f"pause={pmode}: rotation advanced during the hold ({rd0}+{inflight0} in-flight → {rd1})")
                checks.append(f"  ✗ pause={pmode}: rotated pod count grew {rd0}→{rd1} beyond the {inflight0} in-flight at pause — pause not honored")
            else:
                checks.append(f"  ✓ pause={pmode}: transition parked across the {KILL_PAUSE_HOLD_S}s hold (no new pods; rotation {rd0}→{rd1} ≤ in-flight {inflight0})")
            print("  Phase 3c: lift the pause and resume the transition")
            _pause_annotation(case_id, "")
            continue

        if progressed and hdr.get("pkill") and not pk_fired:
            _new, old = _inst_rev_split(ctx, "decoder")
            if old == 1:
                victims = [(n, p) for n, p in sorted(ctx["pod_state"].items())
                           if p and p["alive"] and p["comp"] == "decoder"
                           and p.get("hash") in ctx["old_hashes"] and p.get("serving")]
                leaders = [x for x in victims if x[1].get("runner") == "leader"]
                pick = (leaders or victims)
                if pick:
                    pk_fired = True
                    vname = pick[0][0]
                    print(f"    ⚡ pkill: deleting the LAST old-cohort decoder pod {vname}")
                    kubectl("-n", NS, "delete", "pod", vname, "--wait=false")
                    checks.append(f"  ⓘ pkill fired: deleted last old-cohort decoder pod {vname} mid-transition")

        if progressed and scale_to and not sc_fired:
            sc_fired = True
            patch = {"spec": {c: {"minReplicas": t, "maxReplicas": t} for c, t in scale_to.items()}}
            print(f"  Phase 3a: scale mid-transition {json.dumps(patch['spec'])}")
            rr = kubectl("-n", NS, "patch", "isvc", case_id, "--type=merge", "-p", json.dumps(patch))
            if rr.returncode != 0:
                verdict = "FAIL"
                notes.append(f"mid-roll scale patch failed: {rr.stderr[:120]}")
                checks.append(f"  ✗ scale: mid-transition replica patch failed: {rr.stderr[:120]}")
            else:
                for c, t in scale_to.items():
                    want[c] = t
                    ctx["N"][c] = min(N[c], t)   # floor basis: never below min(start, target)
                checks.append("  ⓘ scale fired mid-transition: " + " ".join(f"{c}→{t}" for c, t in sorted(scale_to.items())))

        # The re-target fires MID-transition: at least one new-cohort pod must
        # be Ready+counted before the second patch, so the case exercises a
        # re-target over a partially-landed intermediate, not a patch race
        # measured in seconds.
        if proto2 is not None and not p2_fired and new_pod_split()[0] > 0:
            p2_fired = True
            pre_final = set(ctx["old_hashes"]) | set(ctx.get("seen_hashes", set()))
            print(f"  Phase 3a: re-patch pairingProtocol {target!r}→{final_target!r} mid-transition")
            ok2, msg2 = _pairing_patch(case_id, final_target)
            if not ok2:
                verdict = "FAIL"
                notes.append(f"proto2 re-patch denied: {msg2[:120]}")
                checks.append(f"  ✗ proto2: mid-transition protocol re-patch denied: {msg2[:120]}")
            # Re-arm pair-alive onto the final transition (still non-empty→non-empty?).
            if ctx["inv"].get("pairing") and final_target:
                ctx["inv"]["pairing"]["to"] = final_target
            elif ctx["inv"].get("pairing") and not final_target:
                ctx["inv"]["pairing"] = None   # →empty pairs with anything: gate + invariant inactive

        # Convergence: every configured Component at want alive+ready, every
        # bumped one fully on the final cohort, sustained STABILITY_S.
        ok_all = progressed and bumped_rotated() and all(
            ctx["agg"].get(c, (0, 0, 0))[0] == want[c] and ctx["agg"].get(c, (0, 0, 0))[1] == want[c]
            for c in configured)
        # A pending one-shot verb must fire before convergence can latch.
        pending = (not pause_done) or (hdr.get("pkill") and not pk_fired) \
            or (scale_to and not sc_fired) or (proto2 is not None and not p2_fired)
        if ok_all and not pending:
            if stable is None:
                stable = time.time()
            elif time.time() - stable >= STABILITY_S:
                state = "CONVERGED"
                ctx["converged_at"] = stable - ctx["trigger_ts"]
                print(f"\n  ✓ CONVERGED at {ctx['converged_at']:.1f}s post-trigger")
                break
        else:
            stable = None
    if state != "CONVERGED":
        state = "ROTATE_TIMEOUT" if progressed else "NO_ROLLOUT"
        print(f"  ✗ {'ROTATE TIMEOUT' if progressed else 'NO ROLLOUT'} after {budget}s")

    # ── Checks ──
    for c in COMPS:
        if want.get(c, N[c]) == 0 and N[c] == 0:
            continue
        if ctx["peaks"][c] > ceil[c]:
            verdict = "FAIL"
            notes.append(f"{c} peak {ctx['peaks'][c]} > ceiling {ceil[c]}")
            checks.append(f"  ✗ {c} peak {ctx['peaks'][c]} exceeded ceiling {ceil[c]} (surge budget violated)")
        else:
            checks.append(f"  ✓ {c} peak {ctx['peaks'][c]} ≤ ceiling {ceil[c]}")
    if state != "CONVERGED":
        verdict = "FAIL"
        rot = " ".join(f"{c}={ctx['agg'].get(c, (0, 0, 0))[2]}/{want[c]}" for c in sorted(bumped))
        notes.append(f"did not converge (state={state}, rotated {rot})")
        checks.append(f"  ✗ protocol transition never converged (state={state}, rotated {rot})")
    else:
        checks.append(f"  ✓ converged at {ctx['converged_at']:.1f}s: every bumped Component fully on the "
                      f"{final_target!r} cohort at target capacity")
        if pre_final is not None:
            checks.append("  ✓ proto2: no Instance stranded on the intermediate cohort (all pods past the pre-re-patch hash universe)")
    # Isolation: the router never participates in pairing — a protocol change
    # must not surge or rotate it.
    for c in sorted(controls):
        up = ctx["agg"].get(c, (0, 0, 0))[2]
        if ctx["peaks"][c] > N[c] or up > 0:
            verdict = "FAIL"
            notes.append(f"{c} (no pairing) changed: peak={ctx['peaks'][c]} updated={up}")
            checks.append(f"  ✗ {c} does not participate in pairing but it surged/rotated (peak={ctx['peaks'][c]}>{N[c]} or updated={up}) — isolation violated")
        else:
            checks.append(f"  ✓ {c} (no pairing) stayed put (peak={ctx['peaks'][c]}=N, 0 rotated) — protocol change did not touch it")
    # In-place strategies must have restamped the LIVE pods (no UID rotation).
    for c in sorted(bumped):
        if info["strat"].get(c) not in IN_PLACE_TYPES:
            continue
        if ppi.get(c, 1) > 1:
            checks.append(f"  ⓘ {c} in-place on a multi-node gang → whole-gang recreate expected (in-place compares the leader spec only)")
            continue
        cur = {p["uid"] for p in ctx["pod_state"].values()
               if p and p["alive"] and p["comp"] == c and p.get("uid")}
        rotated = cur - ctx["old_uids"].get(c, set())
        if rotated:
            verdict = "FAIL"
            notes.append(f"{c} in-place but {len(rotated)} pod(s) recreated")
            checks.append(f"  ✗ {c} strategy is in-place but {len(rotated)} pod UID(s) rotated — the protocol restamp recreated instead of patching")
        elif state == "CONVERGED":
            checks.append(f"  ✓ {c} protocol restamped in place (labels flipped, UIDs stable)")
    verdict = index_collision_checks(ctx, sorted(bumped), verdict, notes, checks)
    verdict = gang_atomicity_checks(ctx, sorted(bumped), verdict, notes, checks)
    verdict = surge_before_drain_checks(ctx, sorted(bumped), verdict, notes, checks)
    # pkill kills a serving decoder pod itself, so the decoder's ready count
    # legitimately dips below its floor for the repair+startup window; assert
    # the floor only on the untouched Components and report the decoder's dip.
    floor_comps = sorted(configured - ({"decoder"} if pk_fired else set()))
    verdict = min_ready_checks(ctx, floor_comps, verdict, notes, checks)
    if pk_fired:
        dmin = (ctx.get("min_ready") or {}).get("decoder", N["decoder"])
        checks.append(f"  ⓘ decoder capacity floor exempt (pkill deleted its serving pod): min ready {dmin} during the induced repair window")
    if state == "CONVERGED":
        verdict = pairing_surface_checks(case_id, ctx, final_target, bumped, verdict, notes, checks)
    # rolloutHold observations. hold=1 asserts the pair floor visibly held a
    # step (gate=Pairing) at some point mid-transition; otherwise informational.
    seen_gates = sorted({g for by in holds.values() for g in by})
    pairing_held = any("Pairing" in by for by in holds.values())
    if hdr.get("hold"):
        if pairing_held:
            rs = next(by["Pairing"] for by in holds.values() if "Pairing" in by)
            checks.append(f"  ✓ rolloutHold gate=Pairing observed mid-transition ({rs[:110]})")
        else:
            verdict = "FAIL"
            notes.append(f"no rolloutHold gate=Pairing observed (saw {seen_gates or 'none'})")
            checks.append(f"  ✗ hold=1: no rolloutHold gate=Pairing was ever observed mid-transition (gates seen: {seen_gates or 'none'})")
    elif seen_gates:
        checks.append(f"  ⓘ rolloutHold gates observed mid-transition: {seen_gates}" +
                      (" (incl. Pairing)" if pairing_held else ""))
    if state == "CONVERGED":
        # A Pairing hold lingering after convergence would be stale status —
        # reported, not failed, until the clear-on-progress contract is pinned.
        st = fetch_status(case_id)
        lingering = [c for c in sorted(bumped)
                     if ((((st.get("components") or {}).get(c) or {}).get("lifecycle") or {}).get("rolloutHold") or {}).get("gate") == "Pairing"]
        if lingering:
            checks.append(f"  ⚠ rolloutHold gate=Pairing still reported on {lingering} after convergence — stale hold (informational)")
    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


def _canary_group(ctx):
    """The canary group this case drives. With one group that is simply it; with
    several (one per unit) the `unit=` header names which unit's ladder is driven
    and the rest are asserted untouched."""
    cgs = [g for g in (ctx["info"].get("canary_groups") or [])
           if canary_unit_entrypoint(g["members"]) in ctx["configured"]]
    if not cgs:
        return None
    want = str(ctx["hdr"].get("unit") or "").lower()
    if want:
        return next((g for g in cgs if g["unit"] == canary_unit(want)), None)
    return cgs[0]


def _canary_primary(ctx, group=None):
    """The component the canary step machine drives + reports on: the entrypoint
    of the UNIT the canary group covers (mirrors canary.primaryComponent). The
    harness reads status.components.<primary>.rolloutPhase / .traffic / .canary on
    exactly this component.

    It is the group's entrypoint, not the ISVC's: an engine unit behind a router
    runs its own ladder, and reading the router there would watch a Component
    that has no canary run at all."""
    group = group or _canary_group(ctx)
    if group:
        entry = canary_unit_entrypoint(group["members"])
        if entry in ctx["configured"]:
            return entry
    for c in ("router", "engine", "decoder"):
        if c in ctx["configured"]:
            return c
    return ""


def _canary_run(st, primary):
    """The canary run state for the unit `primary` belongs to.

    Run state lives on the unit entrypoint's ComponentStatusSpec; the top-level
    status.canary is an alias for whichever unit owns the ISVC entrypoint, so it
    is the wrong copy to read whenever two units are rolling. Fall back to it
    only when the per-unit entry is absent (a run persisted before per-unit
    state existed)."""
    unit = canary_unit(primary)
    comp = ((st.get("components") or {}).get(unit) or {})
    return comp.get("canary") or st.get("canary") or {}


def _canary_command_state(canary, current_hash, old_hashes, promoted_step):
    """Resolve the live target and whether an operator command is safe to issue."""
    observed_hash = canary.get("canaryRevisionHash") or None
    changed = bool(observed_hash and observed_hash != current_hash)
    if changed:
        current_hash = observed_hash
        promoted_step = -1
    target_ready = bool(current_hash and current_hash not in old_hashes)
    promote_ready = target_ready and not canary.get("promotedThrough")
    return current_hash, promoted_step, target_ready, promote_ready, changed


def _canary(case_id, yaml_file, desc, hdr):
    """Canary rollout (OEP-0017 v2). The ISVC ships a spec.rollout.groups[] canary
    group in the initial apply; once rev A is Ready we bump the primary component's revision to
    trigger the staged canary, then drive + observe the step machine:

        capacity gate (Pending) → traffic split (Canarying) → Pause →
        promote (Auto = pause timer, Manual = ome.io/rollout-promote=<hash>) →
        … → final step 100% → Promoting → Stable (status.canary persists at the
        done sentinel: currentStep == number of steps).

    Header keys: canary=auto|manual (else read from the live spec), rollback=1
    (set ome.io/rollout-rollback mid-rollout and assert the canary drains every
    Instance back to the stable revision, holds there at RolledBack, and records
    the rejected revision in the unit's rolledBackRevisionHash), unit=<component>
    (which unit's ladder to drive when the ISVC carries a group per unit; the
    units not named are asserted to stay on their stable revision with no run).

    Asserts the STATUS-level contract (rolloutPhase progression, currentStep,
    status.components.<primary>.traffic[] weights stepping to 100 on the canary
    revision) AND per-revision pod capacity (the primary fully rolls to the canary
    revision). NOTE: gateway data-plane weighting is a downstream consumer
    (per-revision Services + weighted HTTPRoute) and is out of this harness's
    scope — the harness verifies the producer-side weights OME writes to status."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    cgroup = _canary_group(ctx)
    if hdr.get("unit") and cgroup is None:
        return _write_fail(case_id, desc, hdr, "NO_PRIMARY",
                           f"no canary group covers the {hdr['unit']} unit")
    cinfo = cgroup or ctx["info"].get("canary") or {}
    policy = str(hdr.get("canary") or cinfo.get("policy") or "auto").lower()
    do_rollback = bool(hdr.get("rollback"))
    # Analysis (OEP-0015.2): the gate is a Prometheus query, not a timer/promote.
    # `analysis=pass|breach|inconclusive` is the harness intent — we set the canary
    # revision's health at the bump (stable stays healthy at 0.0). pass < 0.05
    # threshold, breach >> it, inconclusive emits no metric (-> no data).
    analysis_mode = str(hdr.get("analysis") or "").lower() or None
    on_inconclusive = str(hdr.get("oninconclusive") or "hold").lower()
    # health = what to set on the canary pods at the bump. Defaults to the analysis
    # intent, but a case can decouple them (e.g. multi-metric AND: the pods are
    # healthy yet a STRICT second threshold breaches, so health=pass but the
    # expected terminal is RolledBack).
    health = str(hdr.get("health") or analysis_mode or "").lower()
    analysis_extra = {
        "pass":         {"perm.test/error-rate": "0.01", "perm.test/emit": "true"},
        "breach":       {"perm.test/error-rate": "0.9", "perm.test/emit": "true"},
        "inconclusive": {"perm.test/error-rate": "0.0", "perm.test/emit": "false"},
    }.get(health)
    # errorrate header pins the canary's exact emitted error-rate (boundary /
    # operator cases need a precise value, e.g. 0.05 == threshold), overriding the
    # pass/breach default.
    if hdr.get("errorrate") is not None and analysis_extra is not None:
        analysis_extra = {**analysis_extra, "perm.test/error-rate": str(hdr.get("errorrate"))}
    # Expected terminal phase. breach + inconclusive/Rollback drain to stable;
    # inconclusive/Hold parks Failed (stable keeps serving); pass promotes.
    if analysis_mode == "breach" or (analysis_mode == "inconclusive" and on_inconclusive == "rollback"):
        expect = "RolledBack"
    elif analysis_mode == "inconclusive":
        expect = "Failed"
    elif analysis_mode == "pass":
        expect = "Stable"
    else:
        expect = "RolledBack" if do_rollback else "Stable"
    nsteps = len(cinfo.get("steps") or []) or 2
    primary = _canary_primary(ctx, cgroup)
    if not primary:
        return _write_fail(case_id, desc, hdr, "NO_PRIMARY", "no configured component to canary")
    N = ctx["N"]
    # Bump EVERY configured member of the canary group, not just the primary — a
    # coupled multi-component canary (e.g. [engine, decoder]) must roll all
    # members for the secondary-capacity gate to be genuinely exercised (mirrors
    # the coordination trigger path, which bumps all group members). The primary
    # stays the component whose status/traffic the step machine is observed on.
    group = (set(cinfo.get("members") or ctx["info"].get("members") or []) & ctx["configured"]) or {primary}
    # Components of every OTHER canary unit. Nothing bumps them, so they must
    # stay whole on their stable revision with no run of their own — the
    # real-cluster proof that one unit's ladder does not drive another's.
    others = sorted((ctx["configured"] - group) &
                    {c for g in (ctx["info"].get("canary_groups") or []) for c in g["members"]})
    print(f"  Phase 2: canary policy={policy} steps={nsteps} primary={primary} bump={sorted(group)}"
          + (f" idle_units={others}" if others else "")
          + ("  +rollback" if do_rollback else ""))
    if not _bump_rev(ctx, group, extra=analysis_extra):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "rev bump patch failed")
    ctx["track_comps"] = sorted(ctx["configured"])   # gang/floor bookkeeping in _read_one
    # Budget tracks the whole bumped group, not just the primary: a rollback (or a
    # full roll) drains every member, and the primary is merely where status/traffic
    # is observed. For PD primary=router (2 pods) but the drain covers engine+decoder
    # too (18-26 pods), and the router's own readiness-gated SurgeThenDrain back to
    # stable runs ~100-300s — sizing on the primary alone starves the drain window.
    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * sum(N[c] * ctx["pods_per_inst"][c] for c in group) + 120)
    if analysis_mode:
        # Analysis is paced by the bundled Prometheus scrape (~15s) plus each
        # step's initialDelay + failureLimit×interval (+ stall timeout for the
        # Hold path). Give it generous extra headroom over the bare rollout.
        budget = min(MAX_BUDGET_S, budget + 300)
    print(f"  Phase 3: driving + observing the canary (≤{budget}s)")

    phases, traffic_by_step = set(), {}
    canary_hash = None
    saw_metrics = False   # analysis wrote status.canary.metricResults at least once
    rollback_set = did_promote = False
    promoted_step = -1  # highest step index already promoted (multi-step plans pause more than once)
    promote_rejected = None
    rejected = None     # status.canary.rolledBackRevisionHash once a rollback is recorded
    state, stable, last_poll = "ROLLING", None, 0.0

    while ctx["trigger_ts"] and time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
        _read_one(ctx)                                   # fold pod events → peaks / per-revision split
        now = time.time()
        if now - last_poll < 2.0:
            continue
        last_poll = now
        st = fetch_status(case_id)
        comp = (st.get("components", {}) or {}).get(primary, {}) or {}
        phase = comp.get("rolloutPhase", "") or ""
        can = _canary_run(st, primary)
        if phase:
            phases.add(phase)
        previous_canary_hash = canary_hash
        canary_hash, promoted_step, canary_target_ready, promote_ready, target_changed = \
            _canary_command_state(can, canary_hash, ctx["old_hashes"], promoted_step)
        if target_changed and previous_canary_hash:
            print(f"    canary target changed {previous_canary_hash} -> {canary_hash}; "
                  "re-arming manual promotion")
        if can.get("metricResults"):
            saw_metrics = True
        step = can.get("currentStep")
        if step is not None:
            for t in (comp.get("traffic") or []):
                if t.get("latestRevision"):
                    traffic_by_step[step] = t.get("percent", 0)

        # Rollback probe: once the canary is in progress, request a rollback. The
        # canary reverts every Instance to the stable revision and HOLDS there
        # (RolledBack), rejecting the rolled-back revision.
        if (do_rollback and not rollback_set and canary_target_ready and
                phase in ("Canarying", "Paused")):
            rollback_set = True
            kubectl("-n", NS, "annotate", "isvc", case_id, f"{ROLLBACK_ANNO}=true", "--overwrite")
            print("    set ome.io/rollout-rollback=true (expect: drain to stable -> RolledBack)")
        rejected = can.get("rolledBackRevisionHash") or rejected

        # Manual promotion: promote at EACH paused step (multi-step plans pause
        # more than once). Wait for the prior durable promotion record to clear
        # before reusing the same target hash at another step.
        should_promote = (policy == "manual" and not do_rollback and phase == "Paused" and
                          canary_hash and promote_ready and step is not None and step > promoted_step)
        if should_promote:
            rr = kubectl("-n", NS, "annotate", "isvc", case_id,
                         f"{PROMOTE_ANNO}={canary_hash}", "--overwrite")
            if rr.returncode != 0:
                promote_rejected = (rr.stderr.strip().splitlines() or ["rejected"])[-1][:160]
                print(f"    ✗ promote annotation REJECTED: {promote_rejected}")
            else:
                promoted_step = step
                did_promote = True
                print(f"    promoted step {step} (ome.io/rollout-promote={canary_hash})")

        wsteps = " ".join(f"s{k}={v}%" for k, v in sorted(traffic_by_step.items()))
        print(f"    [t={int(now - ctx['trigger_ts']):3d}s] phase={phase:<11} step={step} "
              f"canary={canary_hash} rejected={rejected} traffic[{wsteps}]")

        # Converged: rollback cases settle at RolledBack (drained to stable); normal
        # canaries at Stable. We key on the phase AND on pod convergence: the
        # controller flips Stable when traffic is 100% on Ready new pods, but the
        # old-pod drain finishes asynchronously a few seconds later. Wait for the
        # pods to actually settle (full roll, or 0 canary for a rollback) so we
        # don't snapshot mid-drain.
        cur_new, _ = _inst_rev_split(ctx, primary)
        if expect == "RolledBack":
            pods_settled = (cur_new == 0)
        elif expect == "Failed":
            pods_settled = (phase == "Failed")   # parked at the failed step; pod count is whatever it reached
        else:                                    # Stable
            pods_settled = (cur_new == N[primary])
        if phase == expect and pods_settled:
            stable = stable or now
            if now - stable >= STABILITY_S:
                state = "CONVERGED"
                ctx["converged_at"] = stable - ctx["trigger_ts"]
                break
        else:
            stable = None

    # ── verdict ──
    verdict, notes, checks = "PASS", [], []

    def good(ok, msg_ok, msg_bad):
        nonlocal verdict
        if ok:
            checks.append(f"  ✓ {msg_ok}")
        else:
            verdict = "FAIL"; notes.append(msg_bad); checks.append(f"  ✗ {msg_bad}")

    # Peak/ceiling: the canary surges the primary +1 while the stable revision
    # keeps serving (ceil = N+1 from the case header; NR+1 for PD router). A step
    # that over-surges — e.g. a 2N fan-out, the fleet-wipe shape — must FAIL.
    # Same peak gate as every other handler; it runs before all three verdict
    # return paths below.
    cceil = {"engine": hdr.get("ceil_e", N["engine"]), "decoder": hdr.get("ceil_d", N["decoder"]),
             "router": hdr.get("ceil_r", N["router"])}
    for c in COMPS:
        if N[c] and ctx["peaks"][c] > cceil[c]:
            verdict = "FAIL"; notes.append(f"{c} peak {ctx['peaks'][c]}>ceil {cceil[c]}")
            checks.append(f"  ✗ {c} peak {ctx['peaks'][c]} > ceiling {cceil[c]}")
        elif N[c]:
            checks.append(f"  ✓ {c} peak {ctx['peaks'][c]} ≤ ceiling {cceil[c]}")
    # A canary surges the primary while the stable revision keeps serving, so
    # capacity must hold the floor, gangs stay whole, and no two gangs collapse
    # onto one index (the fleet-wipe shape at canary scale).
    verdict = min_ready_checks(ctx, sorted(ctx["configured"]), verdict, notes, checks)
    verdict = index_collision_checks(ctx, sorted(ctx["configured"]), verdict, notes, checks)
    verdict = gang_atomicity_checks(ctx, sorted(ctx["configured"]), verdict, notes, checks)

    # Unit isolation: a unit nobody bumped must sit out the whole run. Its
    # entrypoint carries no run of its own and every one of its Instances stays
    # on the stable revision — if the ladders shared state it would inherit the
    # driven unit's revision pair and roll behind it, ungated.
    if others:
        idle_st = fetch_status(case_id)
        for c in others:
            idle_new, _ = _inst_rev_split(ctx, c)
            good(idle_new == 0,
                 f"{c} (idle unit) held on its stable revision through the {primary} canary",
                 f"{c} rolled {idle_new} Instance(s) off stable while only {primary} was canaried")
            u = canary_unit(c)
            if u != canary_unit(primary):
                idle_run = ((idle_st.get("components") or {}).get(u) or {}).get("canary")
                good(not idle_run,
                     f"{u} unit recorded no canary run of its own",
                     f"{u} unit picked up a canary run it was never given: {idle_run}")

    want = expect
    good(state == "CONVERGED",
         f"reached {want} in {ctx.get('converged_at')}s",
         f"did not reach {want} (state={state}, phases={sorted(phases)})")
    # NOTE: "Canarying" is transient — the reconciler sets it then immediately
    # supersedes it with "Paused" in the same reconcile for any step that pauses,
    # so it never persists for paused steps. Assert the canary entered an ACTIVE
    # split phase (Canarying OR Paused) instead of jumping straight to terminal;
    # do not assert Canarying alone (it false-fails every paused step). A
    # single-step plan (e.g. a 100%-only analysis gate) has no intermediate split —
    # it serves the canary via Promoting — so assert that instead.
    if nsteps > 1:
        active = ("Canarying" in phases) or ("Paused" in phases)
        # A canary that is REJECTED at the analysis gate (analysis=breach/
        # inconclusive) or manually rolled back before the first traffic step
        # goes Pending → RollingBack → RolledBack without ever entering a
        # traffic-split phase. RollingBack is itself proof the canary engaged
        # and was rejected, so accept it when a rollback is the expected outcome.
        if not active and (do_rollback or expect == "RolledBack") and ("RollingBack" in phases):
            active = True
        good(active,
             "observed an active canary phase (Canarying/Paused/RollingBack)",
             f"never observed an active canary phase (jumped straight to terminal); saw {sorted(phases)}")
    else:
        good("Promoting" in phases,
             "observed Promoting (single-step gate served the canary)",
             f"never observed Promoting on the single-step plan; saw {sorted(phases)}")
    new, _old = _inst_rev_split(ctx, primary)

    if analysis_mode:
        # Analysis verdict: the metric gate, not a timer/promote, drove the outcome.
        # metricResults is cleared on step-advance, so a fast-passing analysis step
        # can clear it between the harness's 2s polls — reaching the gated terminal
        # is itself proof it sampled. Only assert it where the result persists: a
        # breach holds it, an inconclusive keeps rewriting it. (manual promote also
        # short-circuits sampling.)
        if policy != "manual" and expect != "Stable":
            good(saw_metrics, "analysis sampled (status.canary.metricResults recorded)",
                 "no metricResults ever recorded — analysis never sampled")
        if expect == "Stable":
            good(new == N[primary], f"{primary} promoted to canary after passing analysis ({new}/{N[primary]})",
                 f"{primary} not fully on canary after analysis ({new}/{N[primary]})")
            fc = (fetch_status(case_id).get("components", {}) or {}).get(primary, {}) or {}
            canary_pct = next((t.get("percent", 0) for t in (fc.get("traffic") or []) if t.get("latestRevision")), 0)
            good(canary_pct == 100, "canary revision carries 100% traffic after passing analysis",
                 f"canary traffic at convergence is {canary_pct}% (want 100)")
        elif expect == "RolledBack":
            good(new == 0, f"{primary} drained to stable after analysis rollback (0 canary)",
                 f"{primary} still has {new} canary Instances after rollback")
            good(bool(rejected), f"rejected revision recorded ({rejected})", "rolledBackRevisionHash not recorded")
            good(not rollback_set, "rollback was analysis-initiated (no manual rollback annotation)",
                 "rollback annotation was set by the harness (not analysis-initiated)")
        elif expect == "Failed":
            good("Failed" in phases, "escalated to Failed on the inconclusive stall",
                 f"never observed Failed; saw {sorted(phases)}")
            good(not rejected, "did not roll back (inconclusive Hold parks Failed; stable keeps serving)",
                 f"unexpectedly rolled back ({rejected})")
        return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)

    if do_rollback:
        # Rolled back: drained back to stable (0 canary Instances), the rejected
        # revision recorded + held, and RollingBack observed during the drain.
        good(new == 0, f"{primary} drained back to stable (0 canary Instances)",
             f"{primary} still has {new} canary Instances after rollback")
        good(bool(rejected), f"rejected revision recorded ({rejected})", "rolledBackRevisionHash not recorded")
        # RollingBack is a transient drain phase between Paused and RolledBack; a
        # fast drain can advance to the RolledBack terminal within one poll gap.
        # When the rollback is already proven (drained to 0 + rejected recorded),
        # a missed transient RollingBack is an observation gap, not a failure.
        if "RollingBack" in phases:
            checks.append("  ✓ observed RollingBack (draining to stable)")
        elif new == 0 and rejected:
            checks.append("  ⓘ RollingBack not sampled (drain reached RolledBack within a poll gap); rollback proven by drain + rejected-revision")
        else:
            verdict = "FAIL"; notes.append("never observed RollingBack")
            checks.append("  ✗ never observed RollingBack")
        return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)

    # Producer-side traffic: at convergence the canary revision must carry 100%.
    # The per-step weights recorded during the roll are timing-unreliable (the
    # status traffic-targets lag the step machine, and step 0 can momentarily read
    # the pre-shift 100%-stable), so assert the CONVERGED end-state, not the
    # intermediate sequence.
    weights = [traffic_by_step[k] for k in sorted(traffic_by_step)]
    final_comp = (fetch_status(case_id).get("components", {}) or {}).get(primary, {}) or {}
    canary_pct = next((t.get("percent", 0) for t in (final_comp.get("traffic") or []) if t.get("latestRevision")), 0)
    good(canary_pct == 100,
         f"canary revision carries 100% traffic at convergence (per-step: {weights})",
         f"canary revision traffic at convergence is {canary_pct}% (want 100); per-step: {weights}")
    good(new == N[primary],
         f"{primary} fully rolled to canary revision ({new}/{N[primary]})",
         f"{primary} not fully on canary revision ({new}/{N[primary]})")

    if policy == "manual":
        if promote_rejected:
            verdict = "FAIL"
            notes.append(f"promote rejected by webhook (kindPromoteTarget vs revision-hash): {promote_rejected}")
            checks.append(f"  ✗ MANUAL promote annotation rejected by admission: {promote_rejected}")
        else:
            good("Paused" in phases, "MANUAL rollout paused before promote", "MANUAL rollout never Paused")
            good(did_promote, "promote annotation accepted + consumed (advanced)", "never promoted")
    else:
        # AUTO advances on the pause timer with no manual promote. If the PLAN has
        # ≥2 traffic-bearing steps it must step through the intermediate weights on
        # its own (not cut straight to 100%). A capacity-ahead / smoke plan with a
        # single non-zero-traffic step (e.g. warm 100% capacity at traffic:0, then
        # cut to 100%) produces only the final weight, so skip that assertion —
        # reaching the terminal phase + 100% canary traffic already proves
        # auto-completion. (A traffic:0 step records no latest-revision weight.)
        traffic_steps = sum(1 for s in (cinfo.get("steps") or [])
                            if isinstance(s, dict) and (s.get("traffic") or 0) > 0)
        if traffic_steps >= 2:
            good(len(traffic_by_step) >= 2,
                 "AUTO rollout stepped through intermediate traffic weights",
                 f"AUTO rollout did not step through intermediate weights (saw steps {sorted(traffic_by_step)})")
        else:
            checks.append(f"  ✓ AUTO single-traffic-step plan ({traffic_steps} traffic-bearing step) "
                          "auto-completed to terminal — no intermediate weight to assert")

    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


def run_mode_case(case_id, yaml_file, desc, hdr, mode):
    """Trigger/check modes other than the default surge-budget rollout.

    scaledown: lower minReplicas/maxReplicas to the header `to_*` targets and
    assert a graceful drain — capacity falls to the target without ever surging
    above the pre-scaledown ceiling. migrate / kill / partition / rebump land on
    this same scaffolding next; until then they record MODE_TODO (never a silent
    PASS).
    """
    Ne, Nd, Nr = hdr.get("Ne", 0), hdr.get("Nd", 0), hdr.get("Nr", 0)
    ce, cd, cr = hdr.get("ceil_e", Ne), hdr.get("ceil_d", Nd), hdr.get("ceil_r", Nr)
    N = {"engine": Ne, "decoder": Nd, "router": Nr}
    ceil = {"engine": ce, "decoder": cd, "router": cr}
    configured = {c for c in COMPS if N[c] > 0}
    zero = {"engine": 0, "decoder": 0, "router": 0}
    dash = {"engine": "-", "decoder": "-", "router": "-"}
    print(f"\n=== {case_id} [mode={mode}] ===\n{desc}")

    if mode not in MODE_HANDLERS:
        print(f"  ⓘ mode '{mode}' is not implemented yet — recording MODE_TODO (no cluster action)")
        write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, zero, dash,
                     None, None, "MODE_TODO", f"mode={mode} not implemented", zero)
        return 0

    if mode == "partition":
        return _partition(case_id, yaml_file, desc, hdr)
    if mode == "rebump":
        return _rebump(case_id, yaml_file, desc, hdr)
    if mode == "migrate":
        return _migrate(case_id, yaml_file, desc, hdr)
    if mode == "kill":
        return _kill(case_id, yaml_file, desc, hdr)
    if mode == "canary":
        return _canary(case_id, yaml_file, desc, hdr)
    if mode == "runtimerev":
        return _runtimerev(case_id, yaml_file, desc, hdr)
    if mode == "recreate":
        return _recreate(case_id, yaml_file, desc, hdr)
    if mode == "compremove":
        return _compremove(case_id, yaml_file, desc, hdr)
    if mode == "pairing":
        return _pairing(case_id, yaml_file, desc, hdr)

    target = {c: int(hdr.get(TO_KEYS[c], N[c])) for c in COMPS}
    # Survivor set: instances with index < min(start,target) must be left UNTOUCHED
    # by a scale op — scaledown deletes only the tail, scaleup only adds new
    # indices. min() covers both directions.
    survive = {c: min(N[c], target[c]) for c in configured}

    def _is_survivor(q):
        try:
            return q["comp"] in survive and 0 <= int(q.get("inst", "0")) < survive[q["comp"]]
        except (ValueError, TypeError):
            return False

    print(f"  Scale: " + " ".join(f"{c} {N[c]}→{target[c]}" for c in sorted(configured))
          + f"   ceilings e≤{ce} d≤{cd} r≤{cr}")

    apply_runtime_fixtures()
    r = kubectl("apply", "--validate=false", "-n", NS, "-f", str(yaml_file))
    if r.returncode != 0:
        msg = (r.stderr.strip().splitlines() or ["kubectl apply failed"])[-1]
        print(f"  APPLY FAILED: {msg[:200]}")
        write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, zero, dash,
                     None, None, "APPLY_FAIL", msg[:200], zero)
        return 0

    info = get_spec_info(case_id)
    pods_per_inst = {c: 1 + info["workers"].get(c, 0) for c in COMPS}
    surge_modes = {c: "surge" for c in configured}
    inv = _inv_new(case_id, hdr)     # continuous invariants (status-truth + endpoints)
    fault = _fault_new(case_id, hdr)  # optional fault=<kind>:<param> plumbing

    init_budget = min(MAX_BUDGET_S, INIT_BASE_S + INIT_PER_POD_S * sum(N[c] for c in configured))
    conv_budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * sum(N[c] for c in configured))
    print(f"  starting watcher (init≤{init_budget}s, drain≤{conv_budget}s)")
    watcher = start_watcher(case_id)

    state = "INIT"
    pod_state = {}
    peaks = {"engine": 0, "decoder": 0, "router": 0}
    min_ready = {}                   # comp → min READY Instances seen mid-drain (capacity floor, Check C)
    teardown_since = {}              # (comp, inst) → ts an index went 0<live<ppi (orphan worker)
    teardown_worst = {}             # (comp, inst) → longest partial-teardown span (s)
    init_started = time.time()
    trigger_ts = None
    converged_at = None
    stable_since = None
    progressed = False
    initial_pods_ready = False
    initial_wait = None
    deadline_init = init_started + init_budget
    deadline_conv = None
    print(f"\n  Phase 1: waiting for initial settlement (need {Ne}/{Nd}/{Nr})")

    survivor_uids = set()   # pre-scale UIDs of survivor-index pods (stability check)
    buf = ""
    try:
        while True:
            line = watcher.readline(1.0)   # str | "" (EOF) | None (idle); queue-backed
            if line == "":
                print("  watcher closed stdout — bailing")
                break
            if line is not None:
                line = line.strip() or None
            now = time.time()

            if line is None:
                # Queue idle — run the periodic invariant tick + convergence/timeout
                # checks only when caught up, so the multi-second kubectl gets never
                # throttle event consumption (events processed every iteration below).
                _inv_tick(inv, pod_state, pods_per_inst)
                if state == "INIT" and initial_pods_ready:
                    ir_items = _fetch_initial_ir_items(case_id)
                    settled, wait = _initial_rollout_settled(
                        pod_state, pods_per_inst, N, configured, ir_items or [])
                    if not settled:
                        if wait != initial_wait:
                            print(f"  … Pods are container-ready; waiting for settled serving state ({wait})")
                            initial_wait = wait
                    else:
                        print(f"  ✓ Initial fleet settled after {int(now - init_started)}s")
                        survivor_uids = {q["uid"] for q in pod_state.values()
                                         if q and q["alive"] and _is_survivor(q) and q.get("uid")}
                        ok_f, fmsg = _fault_setup(fault, case_id, hdr)
                        if not ok_f:
                            print(f"  ✗ FAULT SETUP FAILED: {fmsg}")
                            state = "INFRA-FAIL"
                            break
                        patch = {"spec": {c: {"minReplicas": target[c], "maxReplicas": target[c]}
                                          for c in configured if target[c] != N[c]}}
                        print(f"\n  Phase 2: scaledown patch {json.dumps(patch['spec'])}")
                        rr = kubectl("-n", NS, "patch", "isvc", case_id, "--type=merge", "-p", json.dumps(patch))
                        if rr.returncode != 0:
                            print(f"  ✗ TRIGGER FAILED: {rr.stderr[:200]}")
                            state = "TRIGGER_FAIL"
                            break
                        trigger_ts = time.time()
                        deadline_conv = trigger_ts + conv_budget
                        print(f"\n  Phase 3: draining to targets (peak must stay ≤ {ce}/{cd}/{cr})")
                        state = "DRAINING"
                if state == "DRAINING" and progressed and stable_since is not None and now - stable_since >= STABILITY_S:
                    state = "CONVERGED"
                    converged_at = stable_since - trigger_ts
                    print(f"\n  ✓ CONVERGED (drained) at {converged_at:.1f}s post-trigger")
                    break
                if state == "INIT" and now > deadline_init:
                    state = "INIT_TIMEOUT"
                    print(f"  ✗ INIT TIMEOUT after {init_budget}s")
                    break
                if state == "DRAINING" and deadline_conv and now > deadline_conv:
                    state = "DRAIN_TIMEOUT" if progressed else "NO_CHANGE"
                    print(f"  ✗ {'DRAIN TIMEOUT' if progressed else 'NO CHANGE'} after {conv_budget}s")
                    break
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                buf += line
                try:
                    ev = json.loads(buf); buf = ""
                except json.JSONDecodeError:
                    continue
            ev_type = ev.get("type", "")
            obj = ev.get("object", {})
            if ev_type == "BOOKMARK" or not obj:
                continue
            meta = obj.get("metadata", {})
            name = meta.get("name", "?")
            labels = meta.get("labels", {}) or {}
            cstats = obj.get("status", {}).get("containerStatuses") or []
            ready = any(cs.get("name") == "ome-container" and cs.get("ready") for cs in cstats)
            if ev_type == "DELETED":
                if meta.get("uid"):
                    inv["dead_uids"].add(meta["uid"])   # stale-endpoint invariant
                pod_state[name] = None
            else:
                pod_state[name] = {"comp": labels.get("component", "?"),
                                   "inst": labels.get("ome.io/instance-index", "0"),
                                   "runner": labels.get("ome.io/runner", "default"),
                                   "ord": labels.get("ome.io/pod-ordinal", "0"),
                                   "alive": meta.get("deletionTimestamp") is None,
                                   "ready": ready, "hash": labels.get(REV_LABEL, ""),
                                   "pairing": labels.get(PAIRING_LABEL, ""),
                                   "uid": meta.get("uid", ""), "newanno": False,
                                   "serving": _pod_serving(obj),
                                   "created": meta.get("creationTimestamp", "")}
            agg = aggregate(pod_state, None, surge_modes, pods_per_inst)
            for c in COMPS:
                if agg.get(c, (0, 0, 0))[0] > peaks[c]:
                    peaks[c] = agg[c][0]
            if state == "INIT":
                initial_pods_ready = all(
                    agg.get(c, (0, 0, 0))[1] == N[c] for c in configured)
            elif state == "DRAINING":
                # Fault phase hook (settling fires right after the trigger).
                _fault_tick({"fault": fault, "trigger_ts": trigger_ts, "old_hashes": None,
                             "pod_state": pod_state, "pods_per_inst": pods_per_inst})
                # Check C: capacity floor — every CONFIGURED component (not just the
                # scaled one) must keep >= its target ready throughout the drain. A
                # scaled component drains N→target (never below target); a non-scaled
                # one stays at N=target.
                for c in configured:
                    rd = agg.get(c, (0, 0, 0))[1]
                    if c not in min_ready or rd < min_ready[c]:
                        min_ready[c] = rd
                # Partial-teardown atomicity: each gang index's live pod set must be
                # {0 or ppi}, never 0<live<ppi sustained (an orphan worker/leader).
                by_comp = defaultdict(lambda: defaultdict(list))
                for q in pod_state.values():
                    if q and q["alive"]:
                        by_comp[q["comp"]][q.get("inst", "0")].append(q)
                for c in configured:
                    if pods_per_inst.get(c, 1) <= 1:
                        continue
                    live_idx = set(by_comp[c].keys())
                    for inst_idx, ipods in by_comp[c].items():
                        key = (c, inst_idx)
                        if 0 < len(ipods) < pods_per_inst[c]:
                            teardown_since.setdefault(key, now)
                            span = now - teardown_since[key]
                            if span > teardown_worst.get(key, 0):
                                teardown_worst[key] = span
                        else:
                            teardown_since.pop(key, None)
                    for key in [k for k in list(teardown_since) if k[0] == c and k[1] not in live_idx]:
                        teardown_since.pop(key, None)
                if all(agg.get(c, (0, 0, 0))[1] == target[c] for c in configured):
                    if stable_since is None:
                        stable_since = now
                        print(f"  → all at target; waiting {STABILITY_S}s for stability")
                else:
                    progressed = True
                    stable_since = None
    finally:
        watcher.terminate()
        try:
            watcher.wait(timeout=5)
        except subprocess.TimeoutExpired:
            watcher.kill()

    agg = aggregate(pod_state, None, surge_modes, pods_per_inst)
    final = {c: agg.get(c, (0, 0, 0))[0] for c in COMPS}
    verdict = "PASS"
    notes = []
    checks = []
    # blocked=1: the armed fault is EXPECTED to hold the scale short of target —
    # the contract inverts to bounded-partial + parked-no-churn (Check B below).
    blocked = bool(hdr.get("blocked")) and fault is not None
    # Check A: no surge UP while scaling down — peak alive ≤ ceiling (= start N).
    for c in sorted(configured):
        if peaks[c] > ceil[c]:
            verdict = "FAIL"
            notes.append(f"{c} peak {peaks[c]} > ceiling {ceil[c]}")
            checks.append(f"  ✗ {c} peak {peaks[c]} > ceiling {ceil[c]} — surged UP during scale-down")
        else:
            checks.append(f"  ✓ {c} peak {peaks[c]} ≤ ceiling {ceil[c]}")
    # Check B: drained to the target Instance count. blocked=1 inverts it: the
    # fault must HOLD the scale at a bounded partial state — for a quota fault
    # exactly start + n admitted on a scale-up — with pod churn stopped (parked,
    # not a retry loop). Converging while blocked=1 means the fault never bound.
    if blocked:
        if state == "CONVERGED":
            verdict = "FAIL"
            notes.append("blocked=1 but the scale converged — fault never bound")
            checks.append("  ✗ blocked=1: scale reached target despite the armed fault — the fault never bound (apparatus or product overshot the quota)")
        else:
            if fault["kind"] == "quota":
                qn = int(fault["param"])
                exp = {c: (min(target[c], N[c] + qn) if target[c] > N[c] else target[c])
                       for c in sorted(configured)}
                bad = [c for c in sorted(configured) if final[c] != exp[c]]
                if bad:
                    al = " ".join(f"{c}={final[c]}/exp {exp[c]}" for c in bad)
                    verdict = "FAIL"
                    notes.append(f"blocked partial state wrong: {al}")
                    checks.append(f"  ✗ blocked=1 (quota +{qn}): held at the wrong partial count ({al}) — want exactly the quota-admitted pods, no more, no fewer")
                else:
                    al = " ".join(f"{c} {final[c]} (start {N[c]}, target {target[c]})" for c in sorted(configured))
                    checks.append(f"  ✓ blocked=1 (quota +{qn}): held bounded at the quota-admitted partial state ({al})")
            else:
                checks.append(f"  ⓘ blocked=1: held short of target as expected (state={state}); exact partial count only asserted for quota faults")
            stable_ok, cdetail = _churn_stopped(case_id)
            if stable_ok:
                checks.append(f"  ✓ churn stopped: {cdetail}")
            else:
                verdict = "FAIL"
                notes.append(f"blocked state churning: {cdetail}")
                checks.append(f"  ✗ blocked state is NOT parked — {cdetail} (retry loop instead of bounded parking)")
    elif state != "CONVERGED":
        verdict = "FAIL"
        al = " ".join(f"{c}={final[c]}/{target[c]}" for c in sorted(configured))
        notes.append(f"did not drain (state={state}, {al})")
        checks.append(f"  ✗ did not drain to target (state={state}, alive {al})")
    else:
        bad = [c for c in sorted(configured) if final[c] != target[c]]
        if bad:
            verdict = "FAIL"
            al = " ".join(f"{c}={final[c]}/{target[c]}" for c in bad)
            notes.append(f"final != target: {al}")
            checks.append(f"  ✗ drained to wrong count: {al}")
        else:
            al = " ".join(f"{c} {final[c]}/{target[c]}" for c in sorted(configured))
            checks.append(f"  ✓ drained to target ({al})")
    # Check C: capacity floor — every configured component stayed >= its target
    # ready throughout the drain (a scaled component never undershoots target, a
    # non-scaled one never dips below N=target). Catches a scaledown that drops
    # capacity below the requested floor mid-drain.
    for c in sorted(configured):
        mr = min_ready.get(c, N[c])
        # Floor = min(start, target): a scaledown must hold >= target; a scaleup
        # legitimately starts at N (< target) and grows, so its floor is the
        # STARTING count, not the target. min() covers both directions.
        floor = min(N[c], target[c])
        if mr < floor:
            verdict = "FAIL"
            notes.append(f"{c} capacity floor breached: min ready {mr} < floor {floor} (min of start {N[c]}, target {target[c]})")
            checks.append(f"  ✗ {c} capacity dipped to {mr} ready mid-scale (< floor {floor}=min(start {N[c]}, target {target[c]})) — undershot")
        else:
            checks.append(f"  ✓ {c} capacity floor held: min ready {mr} ≥ floor {floor}=min(start {N[c]}, target {target[c]})")
    # Check D: partial-teardown atomicity — no gang index sat 0<live<ppi (an
    # orphan worker/leader) sustained >= GANG_TOL_S during the drain.
    mn_conf = [c for c in sorted(configured) if pods_per_inst.get(c, 1) > 1]
    if mn_conf:
        td_viol = {k: v for k, v in teardown_worst.items() if v >= GANG_TOL_S}
        if td_viol:
            verdict = "FAIL"
            wk = max(td_viol, key=td_viol.get)
            notes.append(f"partial teardown: {wk[0]} Instance {wk[1]} orphaned {td_viol[wk]:.0f}s")
            checks.append(f"  ✗ partial teardown: {len(td_viol)} gang Instance(s) sat 0<live<ppi ≥{GANG_TOL_S:.0f}s during drain (worst {wk[0]} inst {wk[1]}: {td_viol[wk]:.0f}s) — orphan worker, not a whole-gang teardown")
        else:
            checks.append(f"  ✓ partial-teardown atomicity held: every gang index ({', '.join(mn_conf)}) tore down whole (never a sustained orphan)")
    # Check E: survivor stability — surviving instances (index < min(start,target))
    # must keep their ORIGINAL pods. A survivor pod with a UID not in the pre-scale
    # survivor set was needlessly RECREATED — a scale-down must delete only the
    # tail, never rebuild the survivors it keeps.
    # Also asserted for a blocked=1 parked partial state — a fault holding the
    # tail must not cost the survivors their pods.
    if state == "CONVERGED" or blocked:
        recreated = defaultdict(int)
        for q in pod_state.values():
            if q and q["alive"] and _is_survivor(q) and q.get("uid") and q["uid"] not in survivor_uids:
                recreated[q["comp"]] += 1
        if recreated:
            verdict = "FAIL"
            rd = ", ".join(f"{c}+{n}" for c, n in sorted(recreated.items()))
            notes.append(f"survivor recreated: {rd}")
            checks.append(f"  ✗ survivor instability: {sum(recreated.values())} pod(s) at surviving indices RECREATED (new UID) instead of untouched ({rd}) — scale needlessly recreated survivors")
        else:
            checks.append("  ✓ survivor stability: every surviving instance (index < min(start,target)) kept its original pods (only the tail changed)")
    verdict = soften_expected_fail(hdr, verdict, checks)
    # Check F: continuous invariants + fault apparatus — AFTER the softening
    # (same rationale as teardown_check): a status lie or a broken apparatus is
    # never an expected failure, so they re-harden an XFAIL.
    verdict = invariant_checks(inv, verdict, notes, checks)
    verdict = _fault_finalize(fault, verdict, notes, checks)

    # Cleanup + universal teardown gate (after the softening — a non-converging
    # cleanup is never an expected failure).
    if not os.getenv("OME_TEST_KEEP"):
        kubectl("delete", "-n", NS, "-f", str(yaml_file), "--wait=false")
        verdict = teardown_check(case_id, sum(N[c] * pods_per_inst.get(c, 1) for c in configured),
                                 verdict, notes, checks)
    else:
        checks.append("  ⓘ OME_TEST_KEEP set — ISVC kept, teardown check skipped")

    note = "; ".join(notes)
    print(f"\n=== VERDICT: {verdict} ===")
    for c in checks:
        print(c)
    if note:
        print(f"\nFailure reasons: {note}")

    write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, peaks, final,
                 converged_at, info.get("policy"), verdict, note, zero)
    return 0


def run_reject_case(case_id, yaml_file, desc, hdr):
    """Negative case: the ISVC spec is INVALID and admission MUST reject the apply.
    PASS  = kubectl apply is denied by the webhook (and, if header `reason=<code>`
            is set, the denial message contains that reason code).
    FAIL  = apply was accepted (the rule has no teeth on the live cluster).
    This is the only end-to-end proof that the validation rules fire on the
    deployed webhook — unit tests can't see it. No watcher / rollout phase."""
    Ne, Nd, Nr = hdr.get("Ne", 0), hdr.get("Nd", 0), hdr.get("Nr", 0)
    ce, cd, cr = hdr.get("ceil_e", Ne), hdr.get("ceil_d", Nd), hdr.get("ceil_r", Nr)
    zero = {"engine": 0, "decoder": 0, "router": 0}
    dash = {"engine": "-", "decoder": "-", "router": "-"}
    want = hdr.get("reason")   # optional single-token substring the denial must contain
    print(f"\n=== {case_id} [expect=reject] ===\n{desc}")
    apply_runtime_fixtures()
    r = kubectl("apply", "--validate=false", "-n", NS, "-f", str(yaml_file))
    msg = (r.stderr.strip().splitlines() or [""])[-1]
    if r.returncode == 0:
        kubectl("delete", "-n", NS, "-f", str(yaml_file), "--wait=false")  # don't leak the wrongly-accepted ISVC
        verdict, note = "FAIL", "apply ACCEPTED but admission rejection was expected"
        print("  ✗ apply ACCEPTED — the rule did not fire")
        # The wrongly-accepted ISVC's cleanup must converge like any other case.
        tnotes, tchecks = [], []
        teardown_check(case_id, Ne + Nd + Nr, verdict, tnotes, tchecks)
        for line in tchecks:
            print(line)
        if tnotes:
            note = f"{note}; {'; '.join(tnotes)}"
    elif want and want not in r.stderr:
        verdict, note = "FAIL", f"rejected but message lacks reason '{want}': {msg[:160]}"
        print(f"  ✗ rejected for the WRONG reason (want ~{want}): {msg[:160]}")
    else:
        verdict, note = "PASS", f"rejected as expected ({want or 'any reason'})"
        print(f"  ✓ apply rejected as expected: {msg[:160]}")
    write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, zero, dash,
                 None, None, verdict, note, zero)
    return 0


def run_case(case_id):
    # Duplicate-id guard: refuse to run ANY case while the tree contains a
    # duplicate id (find_isvc picks alphabetically-first and silently shadows
    # the rest). Fails before any cluster op.
    dups = duplicate_id_errors(collect_case_files())
    if dups:
        for e in dups:
            print(f"ERROR: {e}", file=sys.stderr)
        return 1
    yaml_file = find_isvc(case_id)
    if yaml_file is None:
        print(f"ERROR: isvcs/**/{case_id}.yaml not found", file=sys.stderr)
        return 1

    desc, hdr = parse_header(yaml_file)
    # Negative cases (header `expect=reject`) assert the webhook denies the apply —
    # handled before mode dispatch, since rejection happens at apply for any mode.
    # EXCEPT mode=pairing: an uncoordinated protocol change is an UPDATE-time
    # rule (old and new protocol both non-empty), so the CREATE is admitted and
    # the pairing handler asserts the Phase-2 patch is denied instead.
    if hdr.get("expect") == "reject" and hdr.get("mode") != "pairing":
        return run_reject_case(case_id, yaml_file, desc, hdr)
    mode = hdr.get("mode", "rollout")
    if mode != "rollout":
        return run_mode_case(case_id, yaml_file, desc, hdr, mode)
    Ne, Nd, Nr = hdr.get("Ne", 0), hdr.get("Nd", 0), hdr.get("Nr", 0)
    ce, cd, cr = hdr.get("ceil_e", Ne), hdr.get("ceil_d", Nd), hdr.get("ceil_r", Nr)
    N = {"engine": Ne, "decoder": Nd, "router": Nr}
    ceil = {"engine": ce, "decoder": cd, "router": cr}
    configured = {c for c in COMPS if N[c] > 0}
    zero = {"engine": 0, "decoder": 0, "router": 0}
    dash = {"engine": "-", "decoder": "-", "router": "-"}

    print(f"\n=== {case_id} ===\n{desc}")
    print(f"  Spec: engine={Ne} replicas, decoder={Nd}, router={Nr}")
    print(f"  Ceilings: engine peak ≤ {ce}, decoder peak ≤ {cd}, router peak ≤ {cr}")

    # Ensure runtime is applied (idempotent — silently OK if exists).
    apply_runtime_fixtures()

    # Apply the ISVC. On rejection (bad manifest / webhook denial) RECORD an
    # APPLY_FAIL row — never silently drop the case (a missing result reads as
    # "didn't run", which hides malformed test cases).
    r = kubectl("apply", "--validate=false", "-n", NS, "-f", str(yaml_file))
    if r.returncode != 0:
        msg = (r.stderr.strip().splitlines() or ["kubectl apply failed"])[-1]
        print(f"  APPLY FAILED: {msg[:300]}")
        write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, zero, dash, None,
                     None, "APPLY_FAIL", msg[:200], dash)
        return 0

    # Read post-defaulting spec: coordination group + per-Component strategy.
    info = get_spec_info(case_id)
    policy, members, order, soak = info["policy"], info["members"], info["order"], info["soak"]
    pacing, ratio_tol = info["pacing"], info["ratio_tol"]
    modes = {c: ("inplace" if info["strat"].get(c) in IN_PLACE_TYPES else "surge")
             for c in configured}
    # Pods per Instance: 1 ('default' single-pod runner) or 1 + worker.size
    # (multi-node leader + workers). The Instance-level counters use this to
    # know when a whole Instance is present/ready. N[c] stays the INSTANCE count
    # (minReplicas); ceilings are in Instance units (a surge = +1 Instance).
    pods_per_inst = {c: 1 + info["workers"].get(c, 0) for c in COMPS}
    multinode = {c: pods_per_inst[c] for c in configured if pods_per_inst[c] > 1}
    # Bumped = every member of the coordination group; engine-only when no group.
    bumped = set(members) & configured
    if not bumped:
        bumped = {"engine"} & configured
    controls = configured - bumped   # non-bumped configured comps = negative control
    # RatioBalanced pair: the two bumped members whose live serving ratio the
    # CheckRatio gate must hold within ratioTolerancePercent. Only meaningful for a
    # 2-member RatioBalanced group.
    rp = [c for c in COMPS if c in bumped and N[c] > 0]
    if pacing == "RatioBalanced" and len(rp) == 2:
        ratio_pair = (rp[0], rp[1])
        ratio_target = N[rp[0]] / N[rp[1]]
    else:
        ratio_pair = None
        ratio_target = 1.0
    print(f"  Coordination: policy={policy or 'none'} pacing={pacing or '-'} members={members or '[]'} "
          f"order={order or '[]'} soak={soak}s")
    print(f"  Strategy: " + " ".join(f"{c}={info['strat'].get(c, '?')}" for c in sorted(configured)))
    if multinode:
        print("  Multi-node: " + " ".join(f"{c}=1 leader+{info['workers'][c]} workers/Instance" for c in sorted(multinode))
              + "  (counts are in Instances; ceilings in Instance units)")
    print(f"  Trigger will bump: {sorted(bumped)}   negative-control (must NOT surge): {sorted(controls) or '[]'}")

    # Dynamic timeouts.
    init_budget = min(MAX_BUDGET_S, INIT_BASE_S + INIT_PER_POD_S * sum(N[c] for c in configured))
    rotate_budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * sum(N[c] for c in bumped) + soak)
    # Sequential rolls members one-after-another, not concurrently — the
    # wall-clock is the SUM of per-member rolls + handoffs, not the max. The
    # base formula (tuned for concurrent RollingUpdate/BlueGreen) under-budgets
    # a multi-member Sequential chain (p58: a 3-way chain was still at engine
    # 2/4 when the window expired). Add headroom proportional to the bumped pod
    # count for Sequential groups.
    if policy == "Sequential" and len(bumped) > 1:
        rotate_budget = min(MAX_BUDGET_S, rotate_budget + ROTATE_PER_POD_S * sum(N[c] * (1 + info["workers"].get(c, 0)) for c in bumped))

    # Start watcher subprocess.
    print(f"  starting watcher (init≤{init_budget}s, rotate≤{rotate_budget}s)")
    watcher = start_watcher(case_id)

    # State machine.
    state = "INIT"
    inv = _inv_new(case_id, hdr)     # continuous invariants (status-truth + endpoints)
    fault = _fault_new(case_id, hdr)  # optional fault=<kind>:<param> plumbing
    pod_state = {}                   # pod_name → {comp, alive, ready, hash, uid, newanno} | None
    peaks = {"engine": 0, "decoder": 0, "router": 0}
    last_counts = None
    init_started = time.time()
    trigger_ts = None
    converged_at = None
    stable_since = None
    progressed = False               # have we OBSERVED the rollout perturb the system?
    initial_pods_ready = False
    initial_wait = None
    old_hashes = None                # pre-trigger revision hashes (snapshot at trigger)
    old_uids = {}                    # comp → set(pod uids) pre-trigger (for in-place UID stability)
    rev_aware = False
    started_ts = {}                  # comp → secs-post-trigger when it first changed
    done_ts = {}                     # comp → secs-post-trigger when it first fully rotated
    ratio_dev_max = 0.0              # worst INSTANTANEOUS serving-ratio deviation (info only)
    ratio_worst = None               # (ready_a, ready_b) at the worst instantaneous deviation
    ratio_samples = []               # (tsecs, dev, ready_a, ready_b) time series → sustained-dev check
    gang_mixed_since = {}            # (comp, inst) → ts an Instance went mixed-revision (multi-node gang)
    gang_worst = {}                  # (comp, inst) → longest mixed-revision span observed (s)
    min_ready = {}                   # comp → min READY Instances seen mid-roll (capacity floor, Check 10)
    floor_since = {}                 # comp → ts ready first dipped below the capacity floor (open dip)
    floor_sust = {}                  # comp → longest below-floor dip span observed (s)
    collide_worst = {}               # comp → worst (alive_inst - distinct_alive_idx) shortfall seen mid-roll
    old_drained = defaultdict(set)   # comp → set(old-rev pod names) observed draining (deletionTimestamp)
    new_ready = defaultdict(set)     # comp → set(new-rev pod names) observed Ready
    order_viol = {}                  # comp → worst (drained - ready - maxUnavail) surge-before-drain breach
    deadline_init = init_started + init_budget
    deadline_rotate = None
    print(f"\n  Phase 1: waiting for initial settlement (need engine={Ne}, decoder={Nd}, router={Nr})")

    buf = ""
    try:
        while True:
            line = watcher.readline(1.0)   # str | "" (EOF) | None (idle); queue-backed
            if line == "":
                print("  watcher subprocess closed stdout — bailing")
                break
            if line is not None:
                line = line.strip() or None

            now = time.time()

            if line is None:
                # Queue idle — all currently-available events are drained. Run the
                # periodic invariant tick + convergence/timeout checks ONLY here, so
                # the multi-second kubectl gets in _inv_tick never throttle event
                # consumption (events are processed every iteration below; the slow
                # tick runs only between bursts, when pod_state is already current).
                _inv_tick(inv, pod_state, pods_per_inst)

                if state == "INIT" and initial_pods_ready:
                    ir_items = _fetch_initial_ir_items(case_id)
                    settled, wait = _initial_rollout_settled(
                        pod_state, pods_per_inst, N, configured, ir_items or [])
                    if not settled:
                        if wait != initial_wait:
                            print(f"  … Pods are container-ready; waiting for settled serving state ({wait})")
                            initial_wait = wait
                    else:
                        print(f"\n  ✓ Initial fleet settled after {int(now - init_started)}s")
                        old_hashes = {p["hash"] for p in pod_state.values()
                                      if p and p["alive"] and p.get("hash")}
                        for c in configured:
                            old_uids[c] = {p["uid"] for p in pod_state.values()
                                           if p and p["alive"] and p["comp"] == c and p.get("uid")}
                        rev_aware = bool(old_hashes)
                        print(f"  old revision hash(es): {sorted(old_hashes) or '(none)'}")
                        ok_f, fmsg = _fault_setup(fault, case_id, hdr)
                        if not ok_f:
                            print(f"  ✗ FAULT SETUP FAILED: {fmsg}")
                            state = "INFRA-FAIL"
                            break
                        patch = {"spec": {c: {"annotations": {TRIGGER_ANNO: TRIGGER_VAL,
                                                              "ome.io/deploymentMode": "OMENative"}}
                                          for c in sorted(bumped)}}
                        print(f"\n  Phase 2: triggering rotation (bump {TRIGGER_ANNO}→{TRIGGER_VAL} on {sorted(bumped)})")
                        rr = kubectl("-n", NS, "patch", "isvc", case_id, "--type=merge",
                                     "-p", json.dumps(patch))
                        if rr.returncode != 0:
                            print(f"  ✗ TRIGGER FAILED: {rr.stderr[:200]}")
                            state = "TRIGGER_FAIL"
                            break
                        trigger_ts = time.time()
                        deadline_rotate = trigger_ts + rotate_budget
                        print(f"\n  Phase 3: watching rotation (peak ≤ {ce}/{cd}/{cr}; bumped {sorted(bumped)} → all N on new revision)")
                        state = "ROTATING"

                # CONVERGED is gated on `progressed`: we must have OBSERVED the rollout
                # perturb the pods first, else stable_since latches on the pre-rollout
                # steady state and a slow controller reaction is mis-declared CONVERGED.
                if state == "ROTATING" and progressed and stable_since is not None and now - stable_since >= STABILITY_S:
                    state = "CONVERGED"
                    converged_at = stable_since - trigger_ts
                    print(f"\n  ✓ CONVERGED at {converged_at:.1f}s post-trigger (then stable for {STABILITY_S}s)")
                    break
                if state == "INIT" and now > deadline_init:
                    state = "INIT_TIMEOUT"
                    print(f"  ✗ INIT TIMEOUT after {init_budget}s — never reached initial settled state")
                    break
                if state == "ROTATING" and deadline_rotate and now > deadline_rotate:
                    state = "ROTATE_TIMEOUT" if progressed else "NO_ROLLOUT"
                    if progressed:
                        print(f"  ✗ ROTATE TIMEOUT after {rotate_budget}s — rollout started but never reconverged")
                    else:
                        print(f"  ✗ NO ROLLOUT after {rotate_budget}s — trigger patched but no updated pod ever appeared")
                    break
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                buf += line
                try:
                    ev = json.loads(buf); buf = ""
                except json.JSONDecodeError:
                    continue
            ev_type = ev.get("type", "")
            obj = ev.get("object", {})
            if ev_type == "BOOKMARK" or not obj:
                continue

            meta = obj.get("metadata", {})
            name = meta.get("name", "?")
            labels = meta.get("labels", {}) or {}
            annos = meta.get("annotations", {}) or {}
            comp = labels.get("component", "?")
            rev = labels.get(REV_LABEL, "")
            uid = meta.get("uid", "")
            newanno = annos.get(TRIGGER_ANNO) == TRIGGER_VAL
            deletion = meta.get("deletionTimestamp")
            cstats = obj.get("status", {}).get("containerStatuses") or []
            ready = False
            for cs in cstats:
                if cs.get("name") == "ome-container":
                    ready = bool(cs.get("ready"))
                    break

            # Surge-before-drain ordering (SurgeThenDrain): a source pod must not
            # begin draining before its replacement is Ready. Classify by revision
            # (surge mode only — in-place patches the live pod, no source/surge
            # pair) and record, per bumped surge Component, the cumulative set of
            # OLD-rev pods observed draining and NEW-rev pods observed Ready.
            if trigger_ts is not None and old_hashes is not None and comp in bumped and info["strat"].get(comp) == "SurgeThenDrain":
                is_new = bool(rev and rev not in old_hashes)
                if is_new and ready:
                    new_ready[comp].add(name)
                if (not is_new) and rev and (ev_type == "DELETED" or deletion is not None):
                    old_drained[comp].add(name)
                # Instances of headroom: SurgeThenDrain surges maxUnavailable=0, so
                # drained pods must never lead ready-surge pods (per pods_per_inst).
                mu_pods = resolve_max_unavailable(info["max_unavail"].get(comp, 0),
                                                  N.get(comp, 0)) * pods_per_inst.get(comp, 1)
                breach = len(old_drained[comp]) - len(new_ready[comp]) - mu_pods
                if breach > order_viol.get(comp, 0):
                    order_viol[comp] = breach

            if ev_type == "DELETED":
                if uid:
                    inv["dead_uids"].add(uid)   # stale-endpoint invariant
                pod_state[name] = None
            else:
                pod_state[name] = {"comp": comp,
                                   "inst": labels.get("ome.io/instance-index", "0"),
                                   "runner": labels.get("ome.io/runner", "default"),
                                   "ord": labels.get("ome.io/pod-ordinal", "0"),
                                   "alive": deletion is None,
                                   "ready": ready, "hash": rev, "uid": uid, "newanno": newanno,
                                   "pairing": labels.get(PAIRING_LABEL, ""),
                                   "serving": _pod_serving(obj),
                                   "created": meta.get("creationTimestamp", "")}

            meas = {}
            agg = aggregate(pod_state, old_hashes, modes, pods_per_inst, measures=meas)
            for c in COMPS:
                if agg.get(c, (0, 0, 0))[0] > peaks[c]:
                    peaks[c] = agg[c][0]

            e = agg.get("engine", (0, 0, 0))
            d = agg.get("decoder", (0, 0, 0))
            rt = agg.get("router", (0, 0, 0))
            now = time.time()
            counts = (e, d, rt)
            if counts != last_counts:
                if state == "INIT":
                    t = int(now - init_started)
                    print(f"  [t={t:3d}s INIT]      engine {e[0]}/{Ne} alive {e[1]}/{Ne} ready   decoder {d[0]}/{Nd} {d[1]}/{Nd}   router {rt[0]}/{Nr} {rt[1]}/{Nr}")
                elif state == "ROTATING":
                    t = int(now - trigger_ts)
                    print(f"  [t={t:3d}s ROTATING]  engine {e[0]}/{Ne} ({e[2]} new)  decoder {d[0]}/{Nd} ({d[2]} new)  router {rt[0]}/{Nr} ({rt[2]} new)   peaks: e={peaks['engine']} d={peaks['decoder']} r={peaks['router']}")
                last_counts = counts

            if state == "INIT":
                initial_pods_ready = all(
                    agg.get(c, (0, 0, 0))[1] == N[c] for c in configured)
            elif state == "ROTATING":
                tnow = now - trigger_ts
                # Fault phase hook: settling fires on the first ROTATING
                # iteration; rotating fires once a surge/updated pod is seen.
                _fault_tick({"fault": fault, "trigger_ts": trigger_ts, "old_hashes": old_hashes,
                             "pod_state": pod_state, "pods_per_inst": pods_per_inst})
                # Per-Component timing for Sequential ordering/soak.
                for c in bumped:
                    a, rd, up = agg.get(c, (0, 0, 0))
                    if c not in started_ts and (a != N[c] or up > 0):
                        started_ts[c] = tnow
                    if c not in done_ts and a == N[c] and rd == N[c] and up == N[c]:
                        done_ts[c] = tnow
                # RatioBalanced: track the worst serving-ratio deviation between the
                # two paired members. CheckRatio must keep serving(a):serving(b)
                # within ratioTolerancePercent of N(a):N(b); a large swing means the gate
                # is not holding (e.g. the IR-path bypass let one race ahead).
                if ratio_pair:
                    ra = agg.get(ratio_pair[0], (0, 0, 0))[1]
                    rb = agg.get(ratio_pair[1], (0, 0, 0))[1]
                    # Cap each side's ready at its steady N before measuring the
                    # deviation. SurgeThenDrain transiently lifts the surging side to
                    # N+1 (surge headroom) — extra capacity, not an imbalance: the peer
                    # still has its full N, so nothing is under-served. RatioBalanced
                    # guards against UNDER-serving a side; a side BELOW N is not capped
                    # up, so a real trough still registers here (and is also caught by
                    # the capacity-floor check). Capping only neutralizes the benign
                    # over-capacity surge peak, whose timing across the two independent
                    # gang rollouts is not synchronized.
                    ca = min(ra, N[ratio_pair[0]])
                    cb = min(rb, N[ratio_pair[1]])
                    if ca > 0 or cb > 0:
                        dev = 9.99 if cb == 0 else abs((ca / cb) / ratio_target - 1.0)
                        ratio_samples.append((tnow, dev, ca, cb))
                        if dev > ratio_dev_max:
                            ratio_dev_max = dev
                            ratio_worst = (ca, cb)
                # Gang atomicity (multi-node): a leader+worker Instance must roll
                # as ONE unit. Track how long any Instance sits with its alive pods
                # SPLIT across revisions (surge/recreate) or annotations (in-place)
                # — i.e. leader updated but workers not, or a piecemeal recreate.
                for c in bumped:
                    if pods_per_inst.get(c, 1) <= 1:
                        continue
                    by_inst = defaultdict(list)
                    for q in pod_state.values():
                        if q and q["alive"] and q["comp"] == c:
                            by_inst[q.get("inst", "0")].append(q)
                    for inst_idx, ipods in by_inst.items():
                        if modes.get(c) == "inplace":
                            sigs = {bool(q.get("newanno")) for q in ipods}
                        else:
                            sigs = {q.get("hash") for q in ipods if q.get("hash")}
                        key = (c, inst_idx)
                        if len(sigs) >= 2:                       # mixed old+new within one Instance
                            gang_mixed_since.setdefault(key, now)
                            span = now - gang_mixed_since[key]
                            if span > gang_worst.get(key, 0):
                                gang_worst[key] = span
                        else:
                            gang_mixed_since.pop(key, None)
                # Capacity floor (Check 10): once past the trigger boundary, track
                # the MIN ready Instances per bumped Component. SurgeThenDrain
                # (maxUnavailable=0) surges, so it must never drop below N; a drop
                # means it recreated/dropped the gang instead of surging. Also
                # track how long any below-floor dip PERSISTED — the hard check
                # debounces on FLOOR_SUSTAIN_S so a readiness flap can't red it.
                if tnow >= MINREADY_GRACE_S:
                    for c in bumped:
                        rd = agg.get(c, (0, 0, 0))[1]
                        if c not in min_ready or rd < min_ready[c]:
                            min_ready[c] = rd
                        fl = N[c] - resolve_max_unavailable(info["max_unavail"].get(c, 0), N[c])
                        if rd < fl:
                            floor_since.setdefault(c, now)
                            span = now - floor_since[c]
                            if span > floor_sust.get(c, 0.0):
                                floor_sust[c] = span
                        else:
                            floor_since.pop(c, None)
                # Distinct-alive-index (fleet-wipe guard): track the WORST shortfall
                # of distinct alive indices vs alive Instances, and the worst gang
                # share shortfall, over the whole roll — the collapse is transient
                # (peaks mid-roll) so the final snapshot alone would miss it.
                if tnow >= MINREADY_GRACE_S:
                    for c in bumped:
                        if pods_per_inst.get(c, 1) <= 1:
                            continue
                        distinct_alive = meas.get(c, (0, 0, 0))[0]
                        alive_inst = agg.get(c, (0, 0, 0))[0]
                        short = alive_inst - distinct_alive
                        if short > collide_worst.get(c, 0):
                            collide_worst[c] = short
                # Convergence: every configured Component at N alive+ready, AND every
                # BUMPED Component fully on the new revision. Non-bumped controls only
                # need to stay at N (they should never roll).
                def comp_ok(c):
                    a, rd, up = agg.get(c, (0, 0, 0))
                    if a != N[c] or rd != N[c]:
                        return False
                    if c in bumped:
                        return up == N[c]
                    return True
                target = all(comp_ok(c) for c in configured)
                if target:
                    if stable_since is None:
                        stable_since = now
                        print(f"  → all components at target (bumped fully rotated); waiting {STABILITY_S}s for stability")
                else:
                    progressed = True
                    if stable_since is not None:
                        print(f"  → counts shifted again (rollout still in progress)")
                    stable_since = None
                if any(agg.get(c, (0, 0, 0))[2] > 0 for c in bumped):
                    progressed = True
    finally:
        watcher.terminate()
        try:
            watcher.wait(timeout=5)
        except subprocess.TimeoutExpired:
            watcher.kill()

    # Final aggregates.
    _final_meas = {}
    agg = aggregate(pod_state, old_hashes, modes, pods_per_inst, measures=_final_meas)
    final = {c: agg.get(c, (0, 0, 0))[0] for c in COMPS}        # alive
    new_final = {c: agg.get(c, (0, 0, 0))[2] for c in COMPS}    # updated (new-rev / new-anno)
    print(f"  coordination: policy={policy} members={members}")

    verdict = "PASS"
    notes = []
    checks = []
    # blocked=1: the armed fault is EXPECTED to hold the rollout short of
    # convergence — Check 4 inverts to parked-no-churn, Check 10's floor still
    # applies (the fault must not cost settled capacity), and Check 9b's
    # settle-partition check is moot (nothing ever settles).
    blocked = bool(hdr.get("blocked")) and fault is not None

    # Check 1-3: peak alive ≤ ceiling per configured Component.
    for c in COMPS:
        if N[c] == 0:
            continue
        if peaks[c] > ceil[c]:
            verdict = "FAIL"
            notes.append(f"{c} peak {peaks[c]} > ceiling {ceil[c]}")
            checks.append(f"  ✗ {c} peak {peaks[c]} exceeded ceiling {ceil[c]} (surge budget violated)")
        else:
            checks.append(f"  ✓ {c} peak {peaks[c]} ≤ ceiling {ceil[c]}")

    # Check 4: convergence + every bumped Component fully rotated to the new revision.
    # blocked=1 inverts it: the fault must HOLD the roll (ROTATE_TIMEOUT /
    # NO_ROLLOUT are the expected terminal states) with pod churn stopped —
    # RetryBlock-style parking, not a create/delete loop. Entry-level parking
    # semantics are deliberately NOT asserted; only the observable contract
    # (no convergence, no churn, floor held via Check 10).
    if blocked:
        if state == "CONVERGED":
            verdict = "FAIL"
            notes.append("blocked=1 but the rollout converged — fault never bound")
            checks.append("  ✗ blocked=1: rollout converged despite the armed fault — the fault never bound (apparatus or product overshot the quota)")
        elif state in ("ROTATE_TIMEOUT", "NO_ROLLOUT"):
            checks.append(f"  ✓ blocked=1: rollout held short of convergence as expected (state={state})")
            stable_ok, cdetail = _churn_stopped(case_id)
            if stable_ok:
                checks.append(f"  ✓ churn stopped: {cdetail}")
            else:
                verdict = "FAIL"
                notes.append(f"blocked state churning: {cdetail}")
                checks.append(f"  ✗ blocked state is NOT parked — {cdetail} (retry loop instead of bounded parking)")
        else:
            verdict = "FAIL"
            notes.append(f"blocked=1 but case ended in state={state}")
            checks.append(f"  ✗ blocked=1 expects a held rollout (ROTATE_TIMEOUT/NO_ROLLOUT), got state={state}")
    elif state != "CONVERGED":
        verdict = "FAIL"
        rot = " ".join(f"{c}={new_final[c]}/{N[c]}" for c in sorted(bumped))
        notes.append(f"did not converge (state={state}, rotated {rot})")
        if state == "NO_ROLLOUT":
            checks.append(f"  ✗ NO_ROLLOUT — trigger applied but no updated pod ever appeared (rotated {rot})")
        else:
            alive = " ".join(f"{c}={final[c]}/{N[c]}" for c in sorted(configured))
            checks.append(f"  ✗ never reconverged (state={state}, alive {alive}, rotated {rot})")
    else:
        checks.append(f"  ✓ converged at {converged_at:.1f}s post-trigger")
        unrotated = [c for c in bumped if new_final[c] != N[c]]
        if unrotated:
            verdict = "FAIL"
            rot = " ".join(f"{c}={new_final[c]}/{N[c]}" for c in unrotated)
            notes.append(f"bumped members not fully rotated: {rot}")
            checks.append(f"  ✗ bumped members NOT fully rotated: {rot}")
        else:
            rot = " ".join(f"{c} {new_final[c]}/{N[c]}" for c in sorted(bumped))
            checks.append(f"  ✓ all bumped members fully rotated to new revision ({rot})")

    # Check 5: negative control — non-bumped Components must NOT surge or rotate.
    for c in sorted(controls):
        if peaks[c] > N[c] or new_final[c] > 0:
            verdict = "FAIL"
            notes.append(f"{c} (not bumped) changed: peak={peaks[c]} updated={new_final[c]}")
            checks.append(f"  ✗ {c} is not a bumped member but it surged/rotated (peak={peaks[c]}>{N[c]} or updated={new_final[c]}) — isolation violated")
        else:
            checks.append(f"  ✓ {c} (not bumped) stayed put (peak={peaks[c]}=N, 0 rotated) — isolation held")

    # Check 6: in-place strategies must update IN PLACE (no UID rotation).
    for c in sorted(bumped):
        if modes.get(c) == "inplace":
            # Multi-node (gang) in-place legitimately RECREATES: in-place
            # eligibility compares only the leader's PodSpec, so the
            # controller deliberately falls back to recreate rather than
            # leave workers on the old image (split-brain). UID rotation is
            # the EXPECTED gang behavior here, not a violation — Check 9
            # (gang atomicity) covers that the recreate is whole-gang.
            if pods_per_inst.get(c, 1) > 1:
                checks.append(f"  ⓘ {c} in-place on a multi-node gang → recreate (UID rotation expected; in-place compares the leader spec only)")
                continue
            cur_uids = {p["uid"] for p in pod_state.values()
                        if p and p["alive"] and p["comp"] == c and p.get("uid")}
            recreated = cur_uids - old_uids.get(c, set())
            if recreated:
                verdict = "FAIL"
                notes.append(f"{c} in-place but {len(recreated)} pod(s) recreated (UID rotated)")
                checks.append(f"  ✗ {c} strategy is in-place but {len(recreated)} pod UID(s) rotated — recreated instead of patched in place")
            elif new_final[c] == 0:
                # No UID rotation AND nothing carries the new revision = nothing
                # was patched (a stall), NOT an in-place success. Check 4 already
                # FAILs the verdict; don't print a misleading "updated in place".
                checks.append(f"  ⚠ {c} no UID rotation, but 0 pods carry the new revision — nothing was patched (see convergence check)")
            else:
                checks.append(f"  ✓ {c} updated in place ({new_final[c]}/{N[c]} pods patched, UIDs stable)")

    # Check 7: Sequential ordering + soak (only meaningful with ≥2 bumped members in order).
    if state == "CONVERGED" and policy == "Sequential":
        seq = [c for c in order if c in bumped]
        if len(seq) >= 2:
            for i in range(1, len(seq)):
                a, b = seq[i - 1], seq[i]
                ta, tb = done_ts.get(a), started_ts.get(b)
                if ta is None or tb is None:
                    checks.append(f"  ⚠ Sequential {a}→{b}: could not measure timing (a_done={ta} b_start={tb})")
                    continue
                gap = tb - ta
                if tb < ta - SEQ_TOL_S:
                    verdict = "FAIL"
                    notes.append(f"Sequential overlap: {b} started (t={tb:.0f}) before {a} finished (t={ta:.0f})")
                    checks.append(f"  ✗ Sequential {a}→{b}: {b} started at t={tb:.0f}s before {a} finished at t={ta:.0f}s (overlap {-gap:.0f}s)")
                elif soak > 0 and gap < soak - SEQ_TOL_S:
                    # Soak DURATION is not reliably observable for SurgeThenDrain: the
                    # gate's clock is the previous Component's Available condition, which
                    # stays True throughout a surge roll (AvailableReplicas never drops
                    # below N), so its LastTransitionTime predates the roll and no soak
                    # window is measurable. Report, don't fail — ordering (above) is the
                    # hard check.
                    checks.append(f"  ⚠ Sequential {a}→{b}: gap {gap:.0f}s < soak {soak}s — soak duration not verifiable for surge rolls (informational)")
                else:
                    extra = f" (≥ soak {soak}s)" if soak > 0 else ""
                    checks.append(f"  ✓ Sequential {a}→{b}: {a} done t={ta:.0f}s, {b} started t={tb:.0f}s, gap {gap:.0f}s{extra}")
        else:
            checks.append(f"  ⚠ Sequential but <2 bumped members in order ({seq}) — ordering not exercised")

    # Check 8: RatioBalanced serving-ratio band held throughout the roll —
    # asserted on the worst deviation that PERSISTED >= RATIO_SUSTAIN_S, not the
    # instantaneous spike. The gate is eventually-consistent: engine/decoder run
    # on separate dispatchers whose peer status lags a reconcile, so when both
    # move opposite directions there's a bounded ~1-2s cross-dispatcher overshoot
    # (e.g. 3:5) that self-corrects next reconcile to the intended floor (3:4).
    # That transient is by-design (the gate's own contract); only a deviation
    # held off-band for >= the window is a real gate failure. Surge-only rolls
    # keep serving ≈ N so this stays ~0. The instantaneous worst is still printed
    # so the spike stays visible — it's just not asserted on.
    if state == "CONVERGED" and ratio_pair:
        a, b = ratio_pair
        tol = (ratio_tol or 0) / 100.0
        # Worst deviation sustained >= RATIO_SUSTAIN_S. Each watch sample's state
        # holds until the next sample, so from each sample walk forward while dev
        # stays >= this sample's dev and measure the span to the drop-below point.
        sust_dev, sust_pair = 0.0, None
        ns = len(ratio_samples)
        for i in range(ns):
            ti, di = ratio_samples[i][0], ratio_samples[i][1]
            j = i
            while j + 1 < ns and ratio_samples[j + 1][1] >= di:
                j += 1
            end_t = ratio_samples[j + 1][0] if j + 1 < ns else ratio_samples[-1][0]
            if end_t - ti >= RATIO_SUSTAIN_S and di > sust_dev:
                sust_dev, sust_pair = di, (ratio_samples[i][2], ratio_samples[i][3])
        sust_pct = min(sust_dev, 9.99) * 100
        inst_pct = min(ratio_dev_max, 9.99) * 100
        inst_worst = f"{a} {ratio_worst[0]} : {b} {ratio_worst[1]}" if ratio_worst else "n/a"
        sust_worst = f"{a} {sust_pair[0]} : {b} {sust_pair[1]}" if sust_pair else "n/a"
        if sust_dev > tol + RATIO_EPS:
            verdict = "FAIL"
            notes.append(f"RatioBalanced band violated: SUSTAINED dev {sust_pct:.0f}% > tol {ratio_tol}% (worst sustained [{sust_worst}], instant {inst_pct:.0f}%)")
            checks.append(f"  ✗ RatioBalanced band violated: sustained [{sust_worst}] dev {sust_pct:.0f}% > tol {ratio_tol}% (held ≥{RATIO_SUSTAIN_S:.0f}s) — CheckRatio gate not holding")
        else:
            checks.append(f"  ✓ RatioBalanced band held: sustained dev {sust_pct:.0f}% ≤ tol {ratio_tol}% [{sust_worst}]; instant spike {inst_pct:.0f}% [{inst_worst}] debounced (by-design cross-dispatcher transient)")

    # Check 9: multi-node gang atomicity — each leader+worker Instance must roll
    # as a UNIT (all pods recreate/upgrade together, never split across revisions).
    # A piecemeal roll (leader updated while workers lag) shows as an Instance
    # holding mixed revisions; flag any that sat mixed >= GANG_TOL_S.
    mn_bumped = [c for c in sorted(bumped) if pods_per_inst.get(c, 1) > 1]
    if mn_bumped:
        gang_viol = {k: v for k, v in gang_worst.items() if v >= GANG_TOL_S}
        if gang_viol:
            verdict = "FAIL"
            wk = max(gang_viol, key=gang_viol.get)
            notes.append(f"gang atomicity violated: {wk[0]} Instance {wk[1]} mixed-revision {gang_viol[wk]:.0f}s")
            checks.append(f"  ✗ gang atomicity violated: {len(gang_viol)} Instance(s) sat mixed-revision ≥{GANG_TOL_S:.0f}s — leader+workers did NOT roll as a unit (worst {wk[0]} inst {wk[1]}: {gang_viol[wk]:.0f}s)")
        else:
            checks.append(f"  ✓ gang atomicity held: every multi-node Instance ({', '.join(mn_bumped)}) rolled leader+workers as a unit (no sustained mixed-revision)")

    # Check 9b: distinct-alive-index (the fleet-wipe guard). The Instance-count
    # tuple buckets pods by index and counts 1 per index, so N DIFFERENT gangs
    # colliding on ONE index read as a single alive Instance — the exact reason a
    # 64→1 collapse looked green. Assert per bumped gang that the alive Instance
    # count is backed by that many DISTINCT alive indices (the fleet-wipe
    # collapse detector, tracked mid-roll — a real collision would show two
    # gangs on one index at some sample), AND that at CONVERGENCE the settled
    # new-revision pods partition cleanly (new_pods == distinct_new ×
    # pods_per_inst). The partition-clean check is evaluated on the FINAL
    # snapshot, NOT worst-mid-roll: a gang surge brings its replacement up one
    # pod at a time, so mid-roll an index legitimately shows just its leader —
    # only a shortfall that PERSISTS at settle is a real short/shared gang.
    if mn_bumped:
        collide = {c: collide_worst.get(c, 0) for c in mn_bumped if collide_worst.get(c, 0) > 0}
        share = {}
        for c in mn_bumped:
            _da, np_, dn = _final_meas.get(c, (0, 0, 0))
            want = dn * pods_per_inst.get(c, 1)
            if np_ and np_ != want:
                share[c] = want - np_
        if collide:
            verdict = "FAIL"
            wc = max(collide, key=collide.get)
            notes.append(f"index collision: {wc} had {collide[wc]} alive Instance(s) with no distinct index")
            checks.append(f"  ✗ index collision: {len(collide)} Component(s) saw gangs collapse onto shared indices mid-roll (worst {wc}: {collide[wc]} Instance(s) short a distinct index) — the fleet-wipe shape")
        if share and blocked:
            # A quota-parked half-created surge gang IS the expected blocked
            # state — there is no settle to assert a clean partition against.
            share = {}
            checks.append("  ⓘ settle-partition check skipped — blocked=1 holds a partial surge gang mid-flight by design")
        if share:
            verdict = "FAIL"
            ws = max(share, key=share.get)
            notes.append(f"shared index: {ws} new-revision pods did not partition across distinct indices at convergence (short {share[ws]})")
            checks.append(f"  ✗ shared index: {len(share)} Component(s) settled with new-revision pods that don't partition cleanly across distinct indices (worst {ws}: {share[ws]} pod(s) short AT CONVERGENCE) — a settled gang is missing a pod or shares an index")
            _dump_index_breakdown({"pod_state": pod_state, "old_hashes": old_hashes}, ws, checks)
        if not collide and not share:
            checks.append(f"  ✓ distinct-alive-index held: every bumped gang ({', '.join(mn_bumped)}) kept one clean gang per index (no collapse; clean partition at settle)")

    # Check 9c: surge-before-drain ordering (SurgeThenDrain). A source pod must
    # not begin draining before its surge replacement is Ready — the number of
    # old-rev pods that started draining must never lead the ready new-rev pods by
    # more than maxUnavailable. A breach means the controller drained ahead of the
    # surge (the capacity-losing ordering a fleet-wipe exhibits).
    surge_bumped = [c for c in sorted(bumped) if modes.get(c) == "surge"]
    if surge_bumped:
        ord_viol = {c: order_viol.get(c, 0) for c in surge_bumped if order_viol.get(c, 0) > 0}
        if ord_viol and hdr.get("order_soft"):
            wc = max(ord_viol, key=ord_viol.get)
            checks.append(f"  ⚠ surge-before-drain: {wc} drained {ord_viol[wc]} pod(s) ahead of ready surges — case opted out (order_soft=1, fault-induced drain); informational")
        elif ord_viol:
            verdict = "FAIL"
            wc = max(ord_viol, key=ord_viol.get)
            notes.append(f"surge-before-drain violated: {wc} drained {ord_viol[wc]} pod(s) ahead of ready surges")
            checks.append(f"  ✗ surge-before-drain violated: {len(ord_viol)} Component(s) began draining a source before its replacement was Ready (worst {wc}: {ord_viol[wc]} pod(s) drained ahead of ready surges)")
        else:
            checks.append(f"  ✓ surge-before-drain held: every drain ({', '.join(surge_bumped)}) followed a Ready surge replacement")

    # Check 10: capacity floor during the roll. A bumped Component must keep
    # >= N - maxUnavailable Instances serving throughout — SurgeThenDrain
    # (maxUnavailable=0) surges so it never drops below N. A dip below the floor
    # means it recreated / dropped the gang instead of surging (the p66 0/2 —
    # the fleet-wipe shape).
    #
    # HARD by default: a below-floor dip that PERSISTED >= FLOOR_SUSTAIN_S fails
    # the verdict; shorter dips are 1Hz-sampling/readiness flaps and are
    # reported ⚠ but debounced. floor_hard=1 additionally asserts the
    # instantaneous min (no debounce); floor_soft=1 opts a case out entirely
    # (dip is by-design — reason belongs in the case file's header comment).
    floor_hard = bool(hdr.get("floor_hard"))
    floor_soft = bool(hdr.get("floor_soft"))
    # Also asserted for blocked=1: a fault that holds the surge must never cost
    # settled capacity (the source keeps serving while the roll is parked).
    if state == "CONVERGED" or blocked:
        for c in sorted(bumped):
            floor = N[c] - resolve_max_unavailable(info["max_unavail"].get(c, 0), N[c])
            mr = min_ready.get(c, N[c])
            sust = floor_sust.get(c, 0.0)
            if mr >= floor:
                checks.append(f"  ✓ {c} capacity floor held: min ready {mr} ≥ N-maxUnavailable={floor}")
            elif floor_soft:
                checks.append(f"  ⚠ {c} capacity dipped to {mr} ready Instance(s) mid-roll (< N-maxUnavailable={floor}) — case opted out (floor_soft=1); informational")
            elif floor_hard or sust >= FLOOR_SUSTAIN_S:
                verdict = "FAIL"
                held = "" if floor_hard else f", dip held {sust:.0f}s ≥ {FLOOR_SUSTAIN_S:.0f}s"
                notes.append(f"{c} capacity floor breached: min ready {mr} < N-maxUnavailable={floor}")
                checks.append(f"  ✗ {c} capacity dipped to {mr} ready Instance(s) mid-roll (< N-maxUnavailable={floor}{held}) — did NOT preserve capacity (no surge / dropped the gang)")
            else:
                checks.append(f"  ⚠ {c} capacity dipped to {mr} ready Instance(s) (< N-maxUnavailable={floor}) for <{FLOOR_SUSTAIN_S:.0f}s — debounced as a readiness flap (not failed)")

    # Informational: post-convergence traffic + per-revision Services (BlueGreen
    # contract is currently status-only — reported, not asserted).
    if state == "CONVERGED" and policy:
        status = fetch_status(case_id)
        comps_status = (status.get("components") or {})
        for c in sorted(bumped):
            traffic = (comps_status.get(c) or {}).get("traffic") or []
            tdesc = ", ".join(f"{t.get('revisionName', '?').split('-')[-1]}={t.get('percent', '?')}%"
                              f"{'*' if t.get('latestRevision') else ''}" for t in traffic) or "(none)"
            print(f"  [traffic] {c}: {tdesc}")
        svcs = fetch_rev_services(case_id)
        rev_svcs = [s for s in svcs if "-rev-" in s]
        if rev_svcs:
            print(f"  [per-revision svcs] {rev_svcs}")

    # Parked-case softening: cases the header marks `infeasible=1` are
    # intentionally unsatisfiable (e.g. RatioBalanced band at small N,
    # or maxUnavailable=0 with no surge) — a FAIL there is the gate CORRECTLY
    # refusing to roll; `notimpl=1` cases are parked on a linked known bug.
    # Either way record XFAIL rather than red. A surprise PASS (config turned
    # out feasible / controller improved) is left as PASS. The softening is
    # WHOLESALE on verdict==FAIL — a peak/isolation/gang FAIL on a parked case
    # is softened too; only the checks that run after this point (continuous
    # invariants, fault apparatus, teardown) deliberately stay hard.
    verdict = soften_expected_fail(hdr, verdict, checks)

    # Check 11: continuous invariants + fault apparatus — AFTER the softening
    # (same rationale as teardown_check): a status lie or a broken apparatus is
    # never an expected failure, so they re-harden an XFAIL.
    verdict = invariant_checks(inv, verdict, notes, checks)
    verdict = _fault_finalize(fault, verdict, notes, checks)

    # Cleanup + universal teardown gate (after the infeasible softening — a
    # non-converging cleanup is never an expected failure).
    if not os.getenv("OME_TEST_KEEP"):
        kubectl("delete", "-n", NS, "-f", str(yaml_file), "--wait=false")
        verdict = teardown_check(case_id, sum(N[c] * pods_per_inst.get(c, 1) for c in configured),
                                 verdict, notes, checks)
    else:
        checks.append("  ⓘ OME_TEST_KEEP set — ISVC kept, teardown check skipped")

    note = "; ".join(notes)
    print(f"\n=== VERDICT: {verdict} ===")
    for c in checks:
        print(c)
    if note:
        print(f"\nFailure reasons: {note}")

    write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, peaks, final,
                 converged_at, policy, verdict, note, new_final)
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: run.py <case_id> | run.py --validate-only [isvcs_dir]", file=sys.stderr)
        sys.exit(1)
    if sys.argv[1] == "--validate-only":
        sys.exit(validate_cases(sys.argv[2] if len(sys.argv) > 2 else None))
    _case = sys.argv[1]
    _rc = 1
    try:
        _rc = run_case(_case) or 0
    except Exception:
        # Cleanup-on-crash: a case that errors mid-run (before _finish) would
        # otherwise leak its already-applied ISVC. Always tear it down, then
        # surface the traceback so the failure is still visible in the log.
        import traceback
        traceback.print_exc()
        _f = find_isvc(_case)
        if _f:
            kubectl("delete", "-n", NS, "-f", str(_f), "--wait=false")
    finally:
        # Fault apparatus teardown (quota delete + uncordon) runs no matter how
        # the case ended — PASS, FAIL, INFRA-FAIL or crash.
        _run_fault_cleanups()
    sys.exit(_rc)
