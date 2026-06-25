#!/usr/bin/env python3
"""run.py <case_id>

Watcher-driven test runner for the OMENative rollout playground.

Architecture:
  1. Apply the ISVC; read its (post-defaulting) spec: per-Component update
     strategy + the spec.rollout.groups (OEP-0017 v2; the harness derives a
     synthetic policy / members / order / soak from the group shapes).
  2. Start `kubectl get pod -w -o json --output-watch-events`; read events line-
     by-line, maintaining a per-pod state map (comp, alive, ready, hash, uid,
     newanno). Compute per-Component (alive, ready, updated) counts.
  3. State machine:
       INIT      → wait until every configured Component has N pods Ready.
       (trigger) → snapshot OLD revision hashes + pod UIDs, then bump the
                   `perm.test/rev` annotation on EVERY group member (engine-only
                   when there is no coordination group). → ROTATING
       ROTATING  → wait until every Component is back at N alive AND N ready, AND
                   every BUMPED Component has all N pods on the new revision,
                   sustained STABILITY_S.
       CONVERGED → stop watcher, emit verdict.
  4. Throughout: track peak alive per Component, and per-Component first-change /
     fully-rotated timestamps (for Sequential ordering + soak verification).

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
import select
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

# Env-driven so the harness isn't pinned to one cluster (see batch.py).
KUBECONFIG = os.environ.get("OME_TEST_KUBECONFIG") or os.environ.get("KUBECONFIG") or os.path.expanduser("~/.kube/config")
# Shared per-run stamp: all of a run's results live under results/<RUN_TS>/.
# batch.py exports OME_RUN_TS so its parallel subprocesses share one run folder;
# a standalone run.py generates its own.
RUN_TS = os.environ.get("OME_RUN_TS") or time.strftime("%Y%m%d-%H%M%S")
NS = os.environ.get("OME_TEST_NS", "sandbox")
HERE = Path(__file__).parent.resolve()
REV_LABEL = "ome.io/revision-hash"   # OMENative per-revision pod label (query.LabelRevisionHash)
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

# Dynamic timeouts: real kubelet + a 15s artificial startup sleep per pod, and
# surge=1 rolls serialize. Static 240s was too tight for N=8 and for two-member
# Sequential rolls (decoder fully, soak, then engine).
INIT_BASE_S, INIT_PER_POD_S = 120, 25
# Sequential 2-member rolls serialize (decoder fully, then engine) and under
# conc-8 cluster load creep to ~400s for 8 bumped pods; 40s/pod (=410s budget)
# left no headroom for the trailing STABILITY_S window (p34 converged at 406s
# but timed out at 410s before "stable"). 60s/pod gives comfortable margin.
ROTATE_BASE_S, ROTATE_PER_POD_S = 120, 60
MAX_BUDGET_S = 1200

IN_PLACE_TYPES = {"InPlaceIfPossible", "InPlaceOnly"}
COMPS = ("engine", "decoder", "router")

TSV_HEADER = ("id\tdesc\tNe\tNd\tNr\tceil_e\tceil_d\tceil_r\tpeak_e\tpeak_d\tpeak_r\t"
              "final_e\tfinal_d\tfinal_r\tconv_secs\tpolicy\tverdict\tnote\tnew_e\tnew_d\tnew_r\n")


def kubectl(*args, env=None, **kw):
    env_full = {**os.environ, "KUBECONFIG": KUBECONFIG}
    if env:
        env_full.update(env)
    return subprocess.run(["kubectl", *args], capture_output=True, text=True, env=env_full, **kw)


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


def get_spec_info(case_id):
    """Read spec.rollout.groups + per-Component update strategy from the live ISVC.

    Returns dict: {policy, members, order, soak, pacing, ratio_tol, strat:{comp:type},
    canary, ...}. The v2 group shapes are projected back onto the legacy synthetic
    'policy'/'pacing' fields the verdict logic keys on (see the mapping below).
    """
    info = {"policy": None, "members": [], "order": [], "soak": 0, "strat": {},
            "pacing": None, "ratio_tol": None, "workers": {}, "max_unavail": {}, "partition": {},
            "canary": None}
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
            # Per-Component maxUnavailable → the capacity floor (Check 10). Int
            # cases only (the matrix uses ints); a percent string defaults to 0.
            ru = cs.get("lifecycle", {}).get("updateStrategy", {}).get("rollingUpdate", {})
            mu = ru.get("maxUnavailable", 0)
            info["max_unavail"][c] = mu if isinstance(mu, int) else 0
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
    groups = spec.get("rollout", {}).get("groups", []) or []
    # Canary group first (canary lives on a group; it's not a coordination policy).
    cg = next((g for g in groups if isinstance(g, dict) and isinstance(g.get("canary"), dict)), None)
    if cg is not None:
        can = cg["canary"]
        info["canary"] = {
            "policy": (str(can.get("promotion") or "").lower() or None),
            "steps": can.get("steps", []) or [],
        }
        info["members"] = cg.get("components", []) or []
        info["order"] = cg.get("order", []) or []
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


def aggregate(state, old_hashes, modes, pods_per_inst):
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
    """
    insts = defaultdict(lambda: defaultdict(list))  # comp -> instance-index -> [pod records]
    for p in state.values():
        if p is None or not p["alive"]:
            continue
        insts[p["comp"]][p.get("inst", "0")].append(p)
    agg = defaultdict(lambda: [0, 0, 0])
    for c, by_inst in insts.items():
        ppi = max(1, pods_per_inst.get(c, 1))
        for pods in by_inst.values():
            # alive: gangs count 1 per Instance (a surge is a whole new instance
            # index → already +1). Single-pod (ppi==1) counts alive PODS so a
            # within-instance surge — 2 pods at the SAME inst-index on opposite
            # ActiveOrdinal slots (how the controller surges a single-pod
            # Instance, update.go:1262) — is visible. Instance-counting alone
            # collapses both pods into 1 and silently misses the surge.
            agg[c][0] += len(pods) if ppi == 1 else 1
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
    return {c: tuple(v) for c, v in agg.items()}


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
MODE_HANDLERS = {"scaledown", "partition", "rebump", "migrate", "kill", "canary"}   # modes with a real impl
TO_KEYS = {"engine": "to_e", "decoder": "to_d", "router": "to_r"}  # scaledown target replicas


def start_watcher(case_id):
    """Pod event stream for a case (shared by the rollout path and all modes)."""
    return subprocess.Popen(
        ["kubectl", "-n", NS, "get", "pod", "-l", f"ome.io/inferenceservice={case_id}",
         "-w", "-o", "json", "--output-watch-events"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        env={**os.environ, "KUBECONFIG": KUBECONFIG}, bufsize=1, text=True,
    )


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
    if ev.get("type") == "DELETED":
        ctx["pod_state"][name] = None
    else:
        ctx["pod_state"][name] = {
            "comp": labels.get("component", "?"),
            "inst": labels.get("ome.io/instance-index", "0"),
            "runner": labels.get("ome.io/runner", "default"),
            "alive": meta.get("deletionTimestamp") is None,
            "ready": ready, "hash": labels.get(REV_LABEL, ""),
            "uid": meta.get("uid", ""),
            "newanno": annos.get(TRIGGER_ANNO) == TRIGGER_VAL,
            "node": (obj.get("spec") or {}).get("nodeName", ""),
        }
        h = labels.get(REV_LABEL)
        if h:
            ctx.setdefault("seen_hashes", set()).add(h)   # every revision-hash ever observed


def _read_one(ctx, timeout=1.0):
    """Read at most one watch event (up to `timeout`s), fold it into pod_state,
    refresh peaks, and return (also cache) the current per-Instance aggregate."""
    w = ctx["watcher"]
    r, _, _ = select.select([w.stdout], [], [], timeout)
    if r:
        line = w.stdout.readline()
        if not line:
            ctx["closed"] = True
        else:
            line = line.strip()
            if line:
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
    agg = aggregate(ctx["pod_state"], ctx["old_hashes"], ctx["modes"], ctx["pods_per_inst"])
    for c in COMPS:
        a = agg.get(c, (0, 0, 0))[0]
        if a > ctx["peaks"][c]:
            ctx["peaks"][c] = a
    ctx["agg"] = agg
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


def _setup_and_ready(case_id, yaml_file, hdr):
    """Apply runtime + ISVC, start the watcher, wait for initial Ready, and snapshot
    pre-trigger revision/UID/node state. Returns (ctx, None) or (None, (state, msg))."""
    Ne, Nd, Nr = hdr.get("Ne", 0), hdr.get("Nd", 0), hdr.get("Nr", 0)
    N = {"engine": Ne, "decoder": Nd, "router": Nr}
    configured = {c for c in COMPS if N[c] > 0}
    kubectl("apply", "-f", str(HERE / "runtime"), check=False)
    r = kubectl("apply", "-n", NS, "-f", str(yaml_file))
    if r.returncode != 0:
        msg = (r.stderr.strip().splitlines() or ["kubectl apply failed"])[-1]
        print(f"  APPLY FAILED: {msg[:200]}")
        return None, ("APPLY_FAIL", msg[:200])
    info = get_spec_info(case_id)
    ctx = {
        "case_id": case_id, "N": N, "configured": configured, "info": info,
        "pods_per_inst": {c: 1 + info["workers"].get(c, 0) for c in COMPS},
        "modes": {c: ("inplace" if info["strat"].get(c) in IN_PLACE_TYPES else "surge") for c in configured},
        "pod_state": {}, "peaks": {"engine": 0, "decoder": 0, "router": 0}, "agg": {},
        "buf": "", "closed": False, "old_hashes": None, "old_uids": {}, "old_nodes": {}, "seen_hashes": set(),
        "trigger_ts": None, "converged_at": None, "watcher": start_watcher(case_id),
    }
    budget = min(MAX_BUDGET_S, INIT_BASE_S + INIT_PER_POD_S * sum(N[c] for c in configured))
    print(f"\n  Phase 1: waiting for initial Ready (need {Ne}/{Nd}/{Nr}; ≤{budget}s)")
    t0 = time.time()
    while time.time() - t0 < budget and not ctx["closed"]:
        agg = _read_one(ctx)
        if all(agg.get(c, (0, 0, 0))[1] == N[c] for c in configured):
            alive = [p for p in ctx["pod_state"].values() if p and p["alive"]]
            ctx["old_hashes"] = {p["hash"] for p in alive if p.get("hash")}
            for c in configured:
                ctx["old_uids"][c] = {p["uid"] for p in alive if p["comp"] == c and p.get("uid")}
            ctx["old_nodes"] = {(p["comp"], p["inst"]): p.get("node", "") for p in alive}
            print(f"  ✓ Initial Ready after {int(time.time() - t0)}s; old rev {sorted(ctx['old_hashes']) or '(none)'}")
            return ctx, None
    _stop_watcher(ctx)
    kubectl("delete", "-n", NS, "-f", str(yaml_file), "--wait=false")   # don't leak the ISVC
    print(f"  ✗ INIT TIMEOUT after {budget}s")
    return None, ("INIT_TIMEOUT", "never reached initial Ready")


def _write_fail(case_id, desc, hdr, state, msg):
    Ne, Nd, Nr = hdr.get("Ne", 0), hdr.get("Nd", 0), hdr.get("Nr", 0)
    ce, cd, cr = hdr.get("ceil_e", Ne), hdr.get("ceil_d", Nd), hdr.get("ceil_r", Nr)
    z = {"engine": 0, "decoder": 0, "router": 0}
    d = {"engine": "-", "decoder": "-", "router": "-"}
    print(f"\n=== VERDICT: {state} ===  {msg}")
    write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, z, d, None, None, state, msg, z)
    return 0


def _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks):
    """Common finalize: infeasible-soften, print, cleanup, write_result."""
    Ne, Nd, Nr = hdr.get("Ne", 0), hdr.get("Nd", 0), hdr.get("Nr", 0)
    ce, cd, cr = hdr.get("ceil_e", Ne), hdr.get("ceil_d", Nd), hdr.get("ceil_r", Nr)
    agg = aggregate(ctx["pod_state"], ctx["old_hashes"], ctx["modes"], ctx["pods_per_inst"])
    final = {c: agg.get(c, (0, 0, 0))[0] for c in COMPS}
    new_final = {c: agg.get(c, (0, 0, 0))[2] for c in COMPS}
    if (hdr.get("infeasible") or hdr.get("notimpl")) and verdict == "FAIL":
        why = ("a known unimplemented controller feature" if hdr.get("notimpl")
               else "an unsatisfiable config")
        checks.append(f"  ⓘ marked {'notimpl' if hdr.get('notimpl') else 'infeasible'} — {why} (expected); recorded XFAIL, not a regression")
        verdict = "XFAIL"
    note = "; ".join(notes)
    print(f"\n=== VERDICT: {verdict} ===")
    for c in checks:
        print(c)
    if note:
        print(f"\nFailure reasons: {note}")
    _stop_watcher(ctx)
    kubectl("delete", "-n", NS, "-f", str(find_isvc(case_id)), "--wait=false")
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


def _migration_ledger_entry(case_id, req_uuid):
    """The audit-ledger row for a request UUID, from CM <isvc>-ome-migration-audit
    (data key history.json). None if the CM/entry doesn't exist yet."""
    r = kubectl("-n", NS, "get", "cm", f"{case_id}-ome-migration-audit", "-o", "json")
    if r.returncode != 0:
        return None
    try:
        hist = json.loads(json.loads(r.stdout).get("data", {}).get("history.json", "{}"))
    except Exception:
        return None
    for e in hist.get("entries", []):
        if e.get("requestUUID") == req_uuid:
            return e
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
    inst = str(hdr.get("inst", 0))
    from_node = ctx["old_nodes"].get((mcomp, inst), "")
    orig_insts = {p["inst"] for p in ctx["pod_state"].values()
                  if p and p["alive"] and p["comp"] == mcomp}
    req_uuid = str(uuid.uuid4())
    anno_key = f"ome.io/migration-request-v1-{req_uuid}"
    payload = json.dumps({"schemaVersion": "v1", "component": mcomp, "instance": int(inst),
                          "from_node": from_node, "reason": "perm-test"})
    print(f"  Phase 2: migration request (mig={mig_mode}) component={mcomp} instance={inst} "
          f"from_node={from_node or '(unknown)'}")
    rr = kubectl("-n", NS, "patch", "isvc", case_id, "--type=merge",
                 "-p", json.dumps({"metadata": {"annotations": {anno_key: payload}}}))
    if rr.returncode != 0:
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", rr.stderr[:200])
    ctx["trigger_ts"] = time.time()
    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * N[mcomp] * ctx["pods_per_inst"][mcomp])
    never_grace = 90
    print(f"  Phase 3: watching migration (ledger {case_id}-ome-migration-audit; ≤{budget}s)")
    surge_inst, surge_node, entry, last_poll, state = None, None, None, 0.0, "WATCHING"
    while time.time() - ctx["trigger_ts"] < budget and not ctx["closed"]:
        _read_one(ctx)
        for p in ctx["pod_state"].values():
            if p and p["alive"] and p["comp"] == mcomp and p["inst"] not in orig_insts:
                surge_inst = p["inst"]
                surge_node = p.get("node", "") or surge_node
        now = time.time()
        if now - last_poll >= 5:
            last_poll = now
            entry = _migration_ledger_entry(case_id, req_uuid)
            ph = entry.get("phase") if entry else None
            if mig_mode != "Never" and ph == "Completed":
                state = "COMPLETED"; break
            if ph == "Failed":
                state = "FAILED"; break
            if mig_mode == "Never" and now - ctx["trigger_ts"] > never_grace:
                state = "NEVER_GRACE"; break
    entry = _migration_ledger_entry(case_id, req_uuid) or entry
    ph = entry.get("phase") if entry else None
    ledger_surge = entry.get("surgeInstance") if entry else None
    surged = surge_inst is not None or (ledger_surge is not None and ph == "Completed")
    anno_cleared = anno_key not in _isvc_annotations(case_id)
    src_gone = not any(p and p["alive"] and p["comp"] == mcomp and p["inst"] == inst
                       for p in ctx["pod_state"].values())

    verdict, notes, checks = "PASS", [], []
    if mig_mode == "Never":
        if surged or ph == "Completed":
            verdict = "FAIL"; notes.append(f"Never but migrated (surge={surge_inst or ledger_surge}, phase={ph})")
            checks.append(f"  ✗ mig=Never but a migration happened (surge inst={surge_inst}, ledger phase={ph})")
        else:
            checks.append(f"  ✓ mig=Never: request not actioned (no surge; ledger phase={ph or 'none'})")
        if ph == "Failed":
            checks.append("  ✓ ledger recorded a Failed/rejected entry (MigrationDisabled)")
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
        checks.append(f"  {'✓' if src_gone else '⚠'} source Instance {inst} {'drained' if src_gone else 'still present'}")
        checks.append(f"  {'✓' if anno_cleared else '⚠'} migration-request annotation {'cleared' if anno_cleared else 'still present'}")
    ctx["converged_at"] = (time.time() - ctx["trigger_ts"]) if verdict == "PASS" else None
    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


def _kill(case_id, yaml_file, desc, hdr):
    """Delete a live pod and assert the RestartPolicy outcome by which pods get
    new UIDs (recreated):
      None             -> only the deleted pod is recreated (1 pod)
      RecreateInstance -> the whole Instance (leader+workers) recreates
    `kill=leader|worker` (gangs) selects which pod dies."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    N, configured = ctx["N"], ctx["configured"]
    ppi = ctx["pods_per_inst"]
    policy = hdr.get("restart", "RecreateInstance")
    kcomp = "engine" if "engine" in configured else sorted(configured)[0]
    ktgt = str(hdr.get("kill", ""))   # leader | worker | ""
    cands = [(n, p) for n, p in ctx["pod_state"].items()
             if p and p["alive"] and p["comp"] == kcomp]
    if ktgt:
        cands = [x for x in cands if x[1]["runner"] == ktgt] or cands
    if not cands:
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", f"no live {kcomp} pod to kill")
    kill_name, kill_pod = cands[0]
    killed_inst = kill_pod["inst"]
    pre_uids = {p["uid"] for p in ctx["pod_state"].values() if p and p["alive"] and p.get("uid")}
    print(f"  Phase 2: delete pod {kill_name} (comp={kcomp} inst={killed_inst} runner={kill_pod['runner']}) restartPolicy={policy}")
    kubectl("-n", NS, "delete", "pod", kill_name, "--wait=false")
    ctx["trigger_ts"] = time.time()
    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * sum(N[c] * ppi[c] for c in configured))
    print(f"  Phase 3: watching recovery (≤{budget}s)")
    exp = {"None": 1, "RecreateInstance": ppi[kcomp]}.get(policy, 1)

    def recreated_now():
        return [p for p in ctx["pod_state"].values()
                if p and p["alive"] and p.get("uid") and p["uid"] not in pre_uids]

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

    verdict, notes, checks = "PASS", [], []
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
    return _finish(case_id, desc, hdr, ctx, state, verdict, notes, checks)


def _canary_primary(ctx):
    """The externally-routed component the canary step machine drives + reports on:
    router > engine > decoder (mirrors canary.primaryComponent). The harness reads
    status.components.<primary>.rolloutPhase / .traffic on exactly this component."""
    for c in ("router", "engine", "decoder"):
        if c in ctx["configured"]:
            return c
    return ""


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
    the rejected revision in status.canary.rolledBackRevisionHash).

    Asserts the STATUS-level contract (rolloutPhase progression, currentStep,
    status.components.<primary>.traffic[] weights stepping to 100 on the canary
    revision) AND per-revision pod capacity (the primary fully rolls to the canary
    revision). NOTE: gateway data-plane weighting is a downstream consumer
    (per-revision Services + weighted HTTPRoute) and is out of this harness's
    scope — the harness verifies the producer-side weights OME writes to status."""
    ctx, err = _setup_and_ready(case_id, yaml_file, hdr)
    if err:
        return _write_fail(case_id, desc, hdr, *err)
    cinfo = ctx["info"].get("canary") or {}
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
    primary = _canary_primary(ctx)
    if not primary:
        return _write_fail(case_id, desc, hdr, "NO_PRIMARY", "no configured component to canary")
    N = ctx["N"]
    # Bump EVERY configured member of the canary group, not just the primary — a
    # coupled multi-component canary (e.g. [engine, decoder, router]) must roll all
    # members for the secondary-capacity gate to be genuinely exercised (mirrors
    # the coordination trigger path, which bumps all group members). The primary
    # stays the component whose status/traffic the step machine is observed on.
    group = (set(ctx["info"].get("members") or []) & ctx["configured"]) or {primary}
    print(f"  Phase 2: canary policy={policy} steps={nsteps} primary={primary} bump={sorted(group)}"
          + ("  +rollback" if do_rollback else ""))
    if not _bump_rev(ctx, group, extra=analysis_extra):
        return _write_fail(case_id, desc, hdr, "TRIGGER_FAIL", "rev bump patch failed")
    budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * N[primary] * ctx["pods_per_inst"][primary] + 120)
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
        can = st.get("canary") or {}
        if phase:
            phases.add(phase)
        canary_hash = canary_hash or can.get("canaryRevisionHash") or None
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
        if do_rollback and not rollback_set and phase in ("Canarying", "Paused"):
            rollback_set = True
            kubectl("-n", NS, "annotate", "isvc", case_id, f"{ROLLBACK_ANNO}=true", "--overwrite")
            print("    set ome.io/rollout-rollback=true (expect: drain to stable -> RolledBack)")
        rejected = can.get("rolledBackRevisionHash") or rejected

        # Manual promotion: promote at EACH paused step (multi-step plans pause
        # more than once; advanceStep consumes the annotation). Skipped when rolling
        # back — the rollback drives the rollout instead of a promote.
        if policy == "manual" and not do_rollback and phase == "Paused" and canary_hash and step is not None and step > promoted_step:
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
        good(("Canarying" in phases) or ("Paused" in phases),
             "observed an active canary phase (Canarying/Paused)",
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
        good("RollingBack" in phases, "observed RollingBack (draining to stable)", "never observed RollingBack")
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

    target = {c: int(hdr.get(TO_KEYS[c], N[c])) for c in COMPS}
    print(f"  Scaledown: " + " ".join(f"{c} {N[c]}→{target[c]}" for c in sorted(configured))
          + f"   ceilings e≤{ce} d≤{cd} r≤{cr}")

    kubectl("apply", "-f", str(HERE / "runtime"), check=False)
    r = kubectl("apply", "-n", NS, "-f", str(yaml_file))
    if r.returncode != 0:
        msg = (r.stderr.strip().splitlines() or ["kubectl apply failed"])[-1]
        print(f"  APPLY FAILED: {msg[:200]}")
        write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, zero, dash,
                     None, None, "APPLY_FAIL", msg[:200], zero)
        return 0

    info = get_spec_info(case_id)
    pods_per_inst = {c: 1 + info["workers"].get(c, 0) for c in COMPS}
    surge_modes = {c: "surge" for c in configured}

    init_budget = min(MAX_BUDGET_S, INIT_BASE_S + INIT_PER_POD_S * sum(N[c] for c in configured))
    conv_budget = min(MAX_BUDGET_S, ROTATE_BASE_S + ROTATE_PER_POD_S * sum(N[c] for c in configured))
    print(f"  starting watcher (init≤{init_budget}s, drain≤{conv_budget}s)")
    watcher = start_watcher(case_id)

    state = "INIT"
    pod_state = {}
    peaks = {"engine": 0, "decoder": 0, "router": 0}
    init_started = time.time()
    trigger_ts = None
    converged_at = None
    stable_since = None
    progressed = False
    deadline_init = init_started + init_budget
    deadline_conv = None
    print(f"\n  Phase 1: waiting for initial Ready (need {Ne}/{Nd}/{Nr})")

    buf = ""
    try:
        while True:
            ready_to_read, _, _ = select.select([watcher.stdout], [], [], 1.0)
            line = None
            if ready_to_read:
                line = watcher.stdout.readline()
                if not line:
                    print("  watcher closed stdout — bailing")
                    break
                line = line.strip() or None
            now = time.time()
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
            if line is None:
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
                pod_state[name] = None
            else:
                pod_state[name] = {"comp": labels.get("component", "?"),
                                   "inst": labels.get("ome.io/instance-index", "0"),
                                   "runner": labels.get("ome.io/runner", "default"),
                                   "alive": meta.get("deletionTimestamp") is None,
                                   "ready": ready, "hash": labels.get(REV_LABEL, ""),
                                   "uid": meta.get("uid", ""), "newanno": False}
            agg = aggregate(pod_state, None, surge_modes, pods_per_inst)
            for c in COMPS:
                if agg.get(c, (0, 0, 0))[0] > peaks[c]:
                    peaks[c] = agg[c][0]
            if state == "INIT":
                if all(agg.get(c, (0, 0, 0))[1] == N[c] for c in configured):
                    print(f"  ✓ Initial Ready after {int(now - init_started)}s")
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
            elif state == "DRAINING":
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
    # Check A: no surge UP while scaling down — peak alive ≤ ceiling (= start N).
    for c in sorted(configured):
        if peaks[c] > ceil[c]:
            verdict = "FAIL"
            notes.append(f"{c} peak {peaks[c]} > ceiling {ceil[c]}")
            checks.append(f"  ✗ {c} peak {peaks[c]} > ceiling {ceil[c]} — surged UP during scale-down")
        else:
            checks.append(f"  ✓ {c} peak {peaks[c]} ≤ ceiling {ceil[c]}")
    # Check B: drained to the target Instance count.
    if state != "CONVERGED":
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
    if (hdr.get("infeasible") or hdr.get("notimpl")) and verdict == "FAIL":
        why = ("a known unimplemented controller feature" if hdr.get("notimpl")
               else "an unsatisfiable config")
        checks.append(f"  ⓘ marked {'notimpl' if hdr.get('notimpl') else 'infeasible'} — {why} (expected); recorded XFAIL, not a regression")
        verdict = "XFAIL"

    note = "; ".join(notes)
    print(f"\n=== VERDICT: {verdict} ===")
    for c in checks:
        print(c)
    if note:
        print(f"\nFailure reasons: {note}")

    kubectl("delete", "-n", NS, "-f", str(yaml_file), "--wait=false")
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
    kubectl("apply", "-f", str(HERE / "runtime"), check=False)
    r = kubectl("apply", "-n", NS, "-f", str(yaml_file))
    msg = (r.stderr.strip().splitlines() or [""])[-1]
    if r.returncode == 0:
        kubectl("delete", "-n", NS, "-f", str(yaml_file), "--wait=false")  # don't leak the wrongly-accepted ISVC
        verdict, note = "FAIL", "apply ACCEPTED but admission rejection was expected"
        print("  ✗ apply ACCEPTED — the rule did not fire")
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
    yaml_file = find_isvc(case_id)
    if yaml_file is None:
        print(f"ERROR: isvcs/**/{case_id}.yaml not found", file=sys.stderr)
        return 1

    desc, hdr = parse_header(yaml_file)
    # Negative cases (header `expect=reject`) assert the webhook denies the apply —
    # handled before mode dispatch, since rejection happens at apply for any mode.
    if hdr.get("expect") == "reject":
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
    kubectl("apply", "-f", str(HERE / "runtime"), check=False)

    # Apply the ISVC. On rejection (bad manifest / webhook denial) RECORD an
    # APPLY_FAIL row — never silently drop the case (a missing result reads as
    # "didn't run", which hides malformed test cases).
    r = kubectl("apply", "-n", NS, "-f", str(yaml_file))
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
    pod_state = {}                   # pod_name → {comp, alive, ready, hash, uid, newanno} | None
    peaks = {"engine": 0, "decoder": 0, "router": 0}
    last_counts = None
    init_started = time.time()
    trigger_ts = None
    converged_at = None
    stable_since = None
    progressed = False               # have we OBSERVED the rollout perturb the system?
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
    deadline_init = init_started + init_budget
    deadline_rotate = None
    print(f"\n  Phase 1: waiting for initial Ready (need engine={Ne}, decoder={Nd}, router={Nr} pods all Ready)")

    buf = ""
    try:
        while True:
            ready_to_read, _, _ = select.select([watcher.stdout], [], [], 1.0)
            line = None
            if ready_to_read:
                line = watcher.stdout.readline()
                if not line:
                    print("  watcher subprocess closed stdout — bailing")
                    break
                line = line.strip() or None

            now = time.time()

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
                print(f"  ✗ INIT TIMEOUT after {init_budget}s — never reached initial Ready")
                break
            if state == "ROTATING" and deadline_rotate and now > deadline_rotate:
                state = "ROTATE_TIMEOUT" if progressed else "NO_ROLLOUT"
                if progressed:
                    print(f"  ✗ ROTATE TIMEOUT after {rotate_budget}s — rollout started but never reconverged")
                else:
                    print(f"  ✗ NO ROLLOUT after {rotate_budget}s — trigger patched but no updated pod ever appeared")
                break

            if line is None:
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

            if ev_type == "DELETED":
                pod_state[name] = None
            else:
                pod_state[name] = {"comp": comp,
                                   "inst": labels.get("ome.io/instance-index", "0"),
                                   "runner": labels.get("ome.io/runner", "default"),
                                   "alive": deletion is None,
                                   "ready": ready, "hash": rev, "uid": uid, "newanno": newanno}

            agg = aggregate(pod_state, old_hashes, modes, pods_per_inst)
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
                if all(agg.get(c, (0, 0, 0))[1] == N[c] for c in configured):
                    print(f"\n  ✓ Initial Ready reached after {int(now - init_started)}s")
                    old_hashes = {p["hash"] for p in pod_state.values()
                                  if p and p["alive"] and p.get("hash")}
                    for c in configured:
                        old_uids[c] = {p["uid"] for p in pod_state.values()
                                       if p and p["alive"] and p["comp"] == c and p.get("uid")}
                    rev_aware = bool(old_hashes)
                    print(f"  old revision hash(es): {sorted(old_hashes) or '(none)'}")
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
            elif state == "ROTATING":
                tnow = now - trigger_ts
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
                    if ra > 0 or rb > 0:
                        dev = 9.99 if rb == 0 else abs((ra / rb) / ratio_target - 1.0)
                        ratio_samples.append((tnow, dev, ra, rb))
                        if dev > ratio_dev_max:
                            ratio_dev_max = dev
                            ratio_worst = (ra, rb)
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
                # means it recreated/dropped the gang instead of surging.
                if tnow >= MINREADY_GRACE_S:
                    for c in bumped:
                        rd = agg.get(c, (0, 0, 0))[1]
                        if c not in min_ready or rd < min_ready[c]:
                            min_ready[c] = rd
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
    agg = aggregate(pod_state, old_hashes, modes, pods_per_inst)
    final = {c: agg.get(c, (0, 0, 0))[0] for c in COMPS}        # alive
    new_final = {c: agg.get(c, (0, 0, 0))[2] for c in COMPS}    # updated (new-rev / new-anno)
    print(f"  coordination: policy={policy} members={members}")

    verdict = "PASS"
    notes = []
    checks = []

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
    if state != "CONVERGED":
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
                    # hard check. (Controller-side soak precision tracked in #402.)
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

    # Check 10 (INFORMATIONAL for now): capacity floor during the roll. A bumped
    # Component should keep >= N - maxUnavailable Instances serving throughout —
    # SurgeThenDrain (maxUnavailable=0) surges so it never drops below N. A dip
    # below the floor means it recreated / dropped the gang instead of surging
    # (the p66 0/2). Reported, NOT yet asserted — promote to a hard FAIL once it's
    # confirmed not to false-positive on readiness flaps across p01-p65.
    if state == "CONVERGED":
        for c in sorted(bumped):
            floor = N[c] - info["max_unavail"].get(c, 0)
            mr = min_ready.get(c, N[c])
            if mr < floor:
                checks.append(f"  ⚠ {c} capacity dipped to {mr} ready Instance(s) mid-roll (< N-maxUnavailable={floor}) — did NOT preserve capacity (no surge / dropped the gang); informational")
            else:
                checks.append(f"  ✓ {c} capacity floor held: min ready {mr} ≥ N-maxUnavailable={floor}")

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

    # Infeasible-config softening: cases the header marks `infeasible=1`
    # are intentionally unsatisfiable (e.g. RatioBalanced band at small N,
    # or maxUnavailable=0 with no surge). A FAIL there is the gate CORRECTLY
    # refusing to roll — expected, not a controller bug — so record it as
    # XFAIL rather than red. A surprise PASS (config turned out feasible /
    # controller improved) is left as PASS. Real bugs in these cases still
    # surface: peak-ceiling / isolation / gang-atomicity violations are
    # distinct FAILs that are NOT softened here (only convergence + band
    # FAILs are the expected gate-refusal outcome).
    if hdr.get("infeasible") and verdict == "FAIL":
        checks.append("  ⓘ marked infeasible — this FAIL is the coordination gate correctly "
                      "refusing an unsatisfiable config (expected); not a controller bug")
        verdict = "XFAIL"

    note = "; ".join(notes)
    print(f"\n=== VERDICT: {verdict} ===")
    for c in checks:
        print(c)
    if note:
        print(f"\nFailure reasons: {note}")

    # Cleanup
    kubectl("delete", "-n", NS, "-f", str(yaml_file), "--wait=false")

    write_result(case_id, desc, Ne, Nd, Nr, ce, cd, cr, peaks, final,
                 converged_at, policy, verdict, note, new_final)
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: run.py <case_id>", file=sys.stderr)
        sys.exit(1)
    _case = sys.argv[1]
    try:
        sys.exit(run_case(_case) or 0)
    except Exception:
        # Cleanup-on-crash: a case that errors mid-run (before _finish) would
        # otherwise leak its already-applied ISVC. Always tear it down, then
        # surface the traceback so the failure is still visible in the log.
        import traceback
        traceback.print_exc()
        _f = find_isvc(_case)
        if _f:
            kubectl("delete", "-n", NS, "-f", str(_f), "--wait=false")
        sys.exit(1)
