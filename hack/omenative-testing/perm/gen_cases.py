#!/usr/bin/env python3
"""gen_cases.py — generate the P2–P4 feature cases into isvcs/<feature>/.

Permutes each feature across the SAME axes as the rollout matrix:
  topology  ∈ {engine, pd, engine-mn, pd-mn}   (single engine / PD / +multi-node)
  lifecycle ∈ {SurgeThenDrain, RecreatePod, InPlaceIfPossible, InPlaceOnly}
  coord     ∈ {none, RollingUpdate, RatioBalanced, PerComponent, Sequential, BlueGreen}
…plus each feature's own dimension (migration mode, restart policy, partition
value, scale target, %-budget, ReadyPolicy, in-place tuning). Default scale N=4
(up from the toy N=2). Coordination only applies to the PD topologies (it needs a
≥2-component group), so it multiplies pd / pd-mn, not engine-only.

Each case's first line is the harness header: mode= (run.py dispatch), topo=
(grouping), co= (coordination kind), Ne/Nd/Nr, ceil_*, + per-mode params (to_*,
partition, mig=, restart=, kill=, inst=). Re-run after editing; overwrites the
pNNN.yaml it owns.

Runnable today: edge/ (mode=rollout) + scale/ (mode=scaledown). migration/,
restart/, partition/, rebump record MODE_TODO until their run.py mode lands — the
YAMLs are valid and apply; only the trigger/verdict is pending.

CRD field paths (pkg/apis/ome/v1beta1/lifecycle_types.go):
  lifecycle.restartPolicy            None | RecreateInstanceOnPodRestart
  lifecycle.readyPolicy              AllPodReady | None
  lifecycle.migrationPolicy.mode     Auto | Surge | Never  (+progressDeadlineSeconds>=30, maxAutoRetries 0-10)
  lifecycle.updateStrategy.type      SurgeThenDrain | RecreatePod | InPlaceIfPossible | InPlaceOnly
  ...rollingUpdate.{maxSurge,maxUnavailable}  int OR "NN%" ; ...rollingUpdate.partition  int
  ...inPlaceUpdateStrategy.markNotReadyDuringLifecycle
"""
import math
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent.resolve()
ISVCS = HERE / "isvcs"
CBM, CSR, CSR_MN = "ome-coordtest-noop", "ome-coordtest-cpu", "ome-coordtest-multinode"
N = 8    # default replica Instances (scaled up from 4 so most cases stress >=8)
NR = 2   # router replicas (router is single-pod, never the bumped member here)
TOPOS = ["engine", "pd", "engine-mn", "pd-mn"]
PD_TOPOS = ["pd", "pd-mn"]


def flow(v):
    if isinstance(v, dict):
        return "{ " + ", ".join(f"{k}: {flow(x)}" for k, x in v.items()) + " }"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        return f'"{v}"'
    return str(v)


def lifecycle(strat="SurgeThenDrain", surge=1, unavail=0, partition=None,
              restart=None, ready=None, marknr=None, mig=None):
    us = {"type": strat}
    ru = {}
    if surge is not None:
        ru["maxSurge"] = surge
    if unavail is not None:
        ru["maxUnavailable"] = unavail
    if partition is not None:
        ru["partition"] = partition
    if ru:
        us["rollingUpdate"] = ru
    ip = {}
    if marknr is not None:
        ip["markNotReadyDuringLifecycle"] = marknr
    if ip:
        us["inPlaceUpdateStrategy"] = ip
    life = {}
    if restart:
        life["restartPolicy"] = restart
    if ready:
        life["readyPolicy"] = ready
    if mig is not None:
        life["migrationPolicy"] = mig
    life["updateStrategy"] = us
    return flow(life)


def strat_life(strat, **extra):
    """Lifecycle for a strategy with its valid surge/unavailable budget."""
    if strat == "SurgeThenDrain":
        return lifecycle(strat=strat, surge=1, unavail=0, **extra)
    return lifecycle(strat=strat, surge=0, unavail=1, **extra)   # recreate / in-place


def strat_ceil(strat, n):
    return n + 1 if strat == "SurgeThenDrain" else n


def comp(name, n, w=0, life=None, ann=None):
    # ann: extra pod annotations merged after ome.io/deploymentMode (analysis
    # cases use it for the Downward-API health knobs + prometheus.io/scrape).
    return {"comp": name, "n": n, "w": w, "life": life if life is not None else lifecycle(), "ann": ann}


def _group(components, *, blue_green=False, rolling=None, maintain_ratio=None,
           order=None, soak=None):
    """Render one v2 rollout group. Exactly one progression
    (blueGreen | rollingUpdate) is set; canary groups use _canary_group."""
    lines = [f"        - components: [{', '.join(components)}]"]
    if order:                                  # surge/roll order within the group
        lines.append(f"          order: [{', '.join(order)}]")
    if blue_green:
        lines.append("          blueGreen: {}")
    if rolling is not None:
        lines.append(f"          rollingUpdate: {flow(rolling)}" if rolling
                     else "          rollingUpdate: {}")
    if maintain_ratio is not None:
        lines.append(f"          maintainRatio: {flow(maintain_ratio)}")
    if soak is not None:
        lines.append(f"          soak: {flow(soak)}")
    return lines


def groups_block(groups):
    """Wrap rendered group line-lists into spec.rollout.groups (OEP-0017 v2)."""
    out = ["  rollout:", "    groups:"]
    for g in groups:
        out += g
    return "\n".join(out)


# Back-compat helper for the single-group coordination shapes the emitters use.
def coord_group(components, policy, pacing=None, order=None, soak=None):
    # OEP-0017 (v2): coordination folded into spec.rollout.groups. Each former
    # coordination policy maps onto a presence-based progression:
    #   BlueGreen     -> one blueGreen group
    #   RollingUpdate -> one rollingUpdate group (PerComponent pacing is the
    #                    default — no extra knob); RatioBalanced adds maintainRatio
    #   Sequential    -> one single-Component blueGreen group PER component in
    #                    order (the controller collapses this back to Sequential)
    cl = list(components)
    if policy == "BlueGreen":
        return groups_block([_group(cl, blue_green=True, order=order, soak=soak)])
    if policy == "Sequential":
        seq = order or cl
        return groups_block([_group([c], blue_green=True, soak=soak) for c in seq])
    if policy == "RollingUpdate":
        ptype = (pacing or {}).get("type")
        mr = None
        if ptype == "RatioBalanced":
            tol = (pacing or {}).get("ratioTolerancePercent", 5)
            mr = {"tolerance": tol}
        return groups_block([_group(cl, rolling={}, maintain_ratio=mr, soak=soak)])
    raise ValueError(policy)


COORD_KINDS = ["none", "rolling", "ratio", "percomp", "seq", "bluegreen"]


def coord_for(kind, comps=("engine", "decoder")):
    cl = list(comps)
    if kind == "none":
        return None
    if kind == "rolling":
        return coord_group(cl, "RollingUpdate")
    if kind == "ratio":
        # v1 ratioTolerancePercent=25 -> v2 maintainRatio.tolerance=25.
        return coord_group(cl, "RollingUpdate", {"type": "RatioBalanced", "ratioTolerancePercent": 25})
    if kind == "percomp":
        # PerComponent pacing is the v2 default: a plain rollingUpdate group.
        return coord_group(cl, "RollingUpdate", {"type": "PerComponent"})
    if kind == "seq":
        return coord_group(cl, "Sequential", order=cl)
    if kind == "bluegreen":
        return coord_group(cl, "BlueGreen")
    raise ValueError(kind)


def render(name, comps, coord=None, runtime=None, meta_ann=None):
    if runtime is None:
        runtime = CSR_MN if any(c["w"] > 0 for c in comps) else CSR
    if meta_ann:
        items = ", ".join(f"{k}: {flow(v)}" for k, v in meta_ann.items())
        meta_line = f"metadata: {{ name: {name}, annotations: {{ {items} }} }}"
    else:
        meta_line = f"metadata: {{ name: {name} }}"
    out = ["apiVersion: ome.io/v1beta1", "kind: InferenceService",
           meta_line, "spec:",
           f"  model:   {{ apiGroup: ome.io, kind: ClusterBaseModel,      name: {CBM} }}",
           f"  runtime: {{ apiGroup: ome.io, kind: ClusterServingRuntime, name: {runtime}, autoSync: true }}"]
    for c in comps:
        # Keep the default rendering byte-identical (no churn across existing cases);
        # only merge extra annotations when a case sets them.
        if c.get("ann"):
            ann_str = flow({"ome.io/deploymentMode": "OMENative", **c["ann"]})
        else:
            ann_str = "{ ome.io/deploymentMode: OMENative }"
        out += [f"  {c['comp']}:", f"    minReplicas: {c['n']}", f"    maxReplicas: {c['n']}",
                f"    annotations: {ann_str}"]
        if c["w"] > 0:
            out += ["    leader: {}", f"    worker: {{ size: {c['w']} }}"]
        if c["life"]:
            out.append(f"    lifecycle: {c['life']}")
    if coord:
        out.append(coord)
    return "\n".join(out) + "\n"


# ── topology helpers ─────────────────────────────────────────────────────────
def mn(topo):
    return 1 if topo.endswith("-mn") else 0


def is_pd(topo):
    return topo.startswith("pd")


def comps_for(topo, engine_life, n=N, nd=None, dec_life=None):
    nd = nd if nd is not None else n
    cs = [comp("engine", n, mn(topo), engine_life)]
    if is_pd(topo):
        cs += [comp("decoder", nd, mn(topo), dec_life if dec_life is not None else lifecycle()),
               comp("router", NR, 0, lifecycle())]
    return cs


def nds(topo, n=N, nd=None):
    nd = nd if nd is not None else n
    return dict(Ne=n, Nd=(nd if is_pd(topo) else 0), Nr=(NR if is_pd(topo) else 0))


def ceils(topo, ce, n=N, nd=None, surge_d=0, surge_r=0):
    # ceil_e is surge-adjusted by the caller (passes N + surge). decoder/router
    # only surge when they're rolled group members, so their headroom is opt-in
    # per call (surge_d / surge_r) — steady count otherwise.
    nd = nd if nd is not None else n
    return dict(ceil_e=ce,
                ceil_d=(nd + surge_d if is_pd(topo) else 0),
                ceil_r=(NR + surge_r if is_pd(topo) else 0))


HDR_ORDER = ["mode", "Ne", "Nd", "Nr", "ceil_e", "ceil_d", "ceil_r", "to_e", "to_d", "to_r",
             "partition", "mig", "restart", "kill", "inst", "canary", "rollback",
             "analysis", "health", "errorrate", "oninconclusive", "failurelimit", "minfails", "expect", "reason",
             "co", "topo", "infeasible", "floor_hard"]


def header(name, desc, kv):
    parts = [f"{k}={kv[k]}" for k in HDR_ORDER if k in kv]
    parts += [f"{k}={v}" for k, v in kv.items() if k not in HDR_ORDER]
    return f"# {name}: {desc}  " + " ".join(parts)


CASES = []
COUNTERS = {"migration": 100, "restart": 130, "partition": 160, "scale": 190, "edge": 220, "canary": 300}
# Hand-written cases parked inside generator-owned folders: main()'s idempotency
# sweep preserves them and C() must never mint their ids. (p188/p189 are the
# former scale/p97+p98, renamed out from under rollout/p97+p98's shadow.)
HAND_WRITTEN = {"scale": {"p188", "p189"}}
# isvcs/fault/ (p600+), isvcs/scenario/ (p700+) and isvcs/pairing/ (p800+) are
# entirely hand-written and NOT in COUNTERS — the regen sweep never touches
# them; keep the feature counters below 600.


def C(folder, desc, comps, coord=None, runtime=None, meta_ann=None, **kv):
    # RollingUpdate.Partition is enforced by the OMENative rollout engine
    # (HeldByPartition): Instances with index < k are held on the old revision,
    # so partition cases record real PASS/FAIL. Individual cases can still set
    # notimpl=1 explicitly if a specific shape is unsupported.
    name = f"p{COUNTERS[folder]}"
    COUNTERS[folder] += 1
    CASES.append((folder, name, desc, comps, coord, kv, {"runtime": runtime, "meta_ann": meta_ann}))


RP = {"None": "None", "RecreateInstance": "RecreateInstanceOnPodRestart"}

# ── migration/  (mode=migrate; explicit annotation; surge → peak N+1) ─────────
for topo in TOPOS:
    for migm in ["Auto", "Surge"]:
        life = lifecycle(mig={"mode": migm}) if migm != "Auto" else lifecycle()
        C("migration", f"migrate {topo} mode={migm}", comps_for(topo, life),
          mode="migrate", **nds(topo), **ceils(topo, N + 1), topo=topo, mig=migm, co="none")
for topo in ["engine", "pd"]:
    C("migration", f"migrate {topo} mode=Never (must reject, no surge)",
      comps_for(topo, lifecycle(mig={"mode": "Never"})),
      mode="migrate", **nds(topo), **ceils(topo, N), topo=topo, mig="Never", co="none")
    C("migration", f"migrate {topo} middle Instance idx 2",
      comps_for(topo, lifecycle()), mode="migrate", **nds(topo), **ceils(topo, N + 1),
      topo=topo, mig="Auto", inst=2, co="none")
# Concurrent migrations of inst 0 AND inst 1 (engine) in ONE patch: two requests
# surge two distinct replacement Instances at once → peak N+2 (distinct-surge-index).
C("migration", "migrate engine inst 0+1 concurrent (distinct surge index)",
  comps_for("engine", lifecycle()), mode="migrate", **nds("engine"), **ceils("engine", N + 2),
  topo="engine", mig="Auto", inst="0,1", co="none")
for topo in PD_TOPOS:                         # migration under each coordination policy
    for ck in ["rolling", "ratio", "seq", "bluegreen"]:
        C("migration", f"migrate {topo} Auto under {ck} coordination",
          comps_for(topo, lifecycle()), coord=coord_for(ck),
          mode="migrate", **nds(topo), **ceils(topo, N + 1), topo=topo, mig="Auto", co=ck)

# ── restart/  (mode=kill; delete a live pod, assert RestartPolicy outcome) ─────
for topo in TOPOS:
    for short, full in RP.items():
        C("restart", f"kill {topo} restartPolicy={short}", comps_for(topo, lifecycle(restart=full)),
          mode="kill", **nds(topo), **ceils(topo, N), topo=topo, restart=short, co="none")
for tgt in ["leader", "worker"]:              # gang: which pod dies matters
    C("restart", f"kill {tgt} engine-mn RecreateInstance (whole gang recreated)",
      comps_for("engine-mn", lifecycle(restart=RP["RecreateInstance"])),
      mode="kill", **nds("engine-mn"), **ceils("engine-mn", N), topo="engine-mn",
      restart="RecreateInstance", kill=tgt, co="none")
# Gang recreate-on-pod-death is the DEFAULT for multi-pod: killing ONE gang pod
# must recreate the WHOLE gang, not just the dead pod. Cover the DEFAULT (no
# explicit restartPolicy set) and a 3-pod gang (leader + 2 workers = a,b,c).
C("restart", "kill engine-mn worker, DEFAULT policy — whole gang recreates",
  comps_for("engine-mn", lifecycle()),   # no restartPolicy -> controller default (RecreateInstance for multi-pod)
  mode="kill", **nds("engine-mn"), **ceils("engine-mn", N), topo="engine-mn",
  restart="RecreateInstance", kill="worker", co="none")
C("restart", "kill 3-pod gang (leader+2w) worker — whole a,b,c gang recreates",
  [comp("engine", 2, 2, lifecycle(restart=RP["RecreateInstance"]))],
  mode="kill", Ne=2, Nd=0, Nr=0, ceil_e=2, ceil_d=0, ceil_r=0, topo="engine-mn",
  restart="RecreateInstance", kill="worker", co="none")
for topo in PD_TOPOS:                         # restart under coordination
    for ck in ["rolling", "seq", "bluegreen"]:
        C("restart", f"kill {topo} RecreateInstance under {ck}",
          comps_for(topo, lifecycle(restart=RP["RecreateInstance"])), coord=coord_for(ck),
          mode="kill", **nds(topo), **ceils(topo, N), topo=topo, restart="RecreateInstance", co=ck)

# ── partition/  (mode=partition; rollout-like → full strategy × coordination) ──
for p in [0, 1, 2, 3]:
    C("partition", f"engine N={N} partition={p}", comps_for("engine", strat_life("SurgeThenDrain", partition=p)),
      mode="partition", **nds("engine"), **ceils("engine", N + 1), topo="engine", partition=p, co="none")
for strat in ["RecreatePod", "InPlaceIfPossible"]:
    C("partition", f"engine partition=2 {strat}", comps_for("engine", strat_life(strat, partition=2)),
      mode="partition", **nds("engine"), **ceils("engine", strat_ceil(strat, N)), topo="engine", partition=2, co="none")
for p in [4, 6]:
    C("partition", f"engine N=8 partition={p}", [comp("engine", 8, 0, strat_life("SurgeThenDrain", partition=p))],
      mode="partition", Ne=8, Nd=0, Nr=0, ceil_e=9, ceil_d=0, ceil_r=0, topo="engine", partition=p, co="none")
C("partition", f"engine-mn N={N} W=1 partition=2", comps_for("engine-mn", strat_life("SurgeThenDrain", partition=2)),
  mode="partition", **nds("engine-mn"), **ceils("engine-mn", N + 1), topo="engine-mn", partition=2, co="none")
# maxSurge UNSET (no-limit) at N=8 partition=4: the held-below 4 stay, the rolling
# 4 may all surge at once → peak N + (N - partition) = 12.
C("partition", "engine N=8 partition=4 maxSurge=NoLimit (unset)",
  [comp("engine", 8, 0, lifecycle(strat="SurgeThenDrain", surge=None, unavail=0, partition=4))],
  mode="partition", Ne=8, Nd=0, Nr=0, ceil_e=12, ceil_d=0, ceil_r=0, topo="engine", partition=4, co="none")
# engine-mn RecreatePod partition=2: whole-gang hold on indices < k, no surge (ceil=N).
C("partition", f"engine-mn N={N} W=1 partition=2 RecreatePod", comps_for("engine-mn", strat_life("RecreatePod", partition=2)),
  mode="partition", **nds("engine-mn"), **ceils("engine-mn", N), topo="engine-mn", partition=2, co="none")
for topo in PD_TOPOS:                         # PD partition × every coordination policy
    for ck in COORD_KINDS:
        # decoder is also a bumped group member that surges → ceil_d = N+1 too
        cl = ceils(topo, N + 1); cl["ceil_d"] = N + 1
        C("partition", f"{topo} partition=2 {ck}", comps_for(topo, strat_life("SurgeThenDrain", partition=2)),
          coord=coord_for(ck), mode="partition", **nds(topo), **cl, topo=topo, partition=2, co=ck)

# ── scale/  (mode=scaledown; patch min/maxReplicas to to_*, assert drain/grow) ─
for topo, n, to in [("engine", 4, 1), ("engine", 8, 2), ("engine", 4, 2),
                    ("pd", 4, 2), ("engine-mn", 4, 1), ("pd-mn", 4, 2),
                    ("engine-mn", 8, 2), ("pd-mn", 8, 2)]:
    kv = dict(mode="scaledown", **nds(topo, n), **ceils(topo, n, n), topo=topo, to_e=to, co="none")
    if is_pd(topo):
        kv["to_d"], kv["to_r"] = to, NR
    C("scale", f"scaledown {topo} N={n}->{to}", comps_for(topo, lifecycle(), n=n), **kv)
C("scale", "scaledown PD asymmetric e=4->1 d=4->2 (router holds)", comps_for("pd", lifecycle(), n=4),
  mode="scaledown", Ne=4, Nd=4, Nr=NR, to_e=1, to_d=2, to_r=NR, ceil_e=4, ceil_d=4, ceil_r=NR, topo="pd", co="none")
for topo, to in [("engine", 4), ("engine-mn", 4), ("engine-mn", 8)]:
    C("scale", f"scaleup {topo} N=2->{to}", comps_for(topo, lifecycle(), n=2),
      mode="scaledown", **nds(topo, 2), to_e=to, ceil_e=to, ceil_d=0, ceil_r=0, topo=topo, co="none")
for ck in ["rolling", "ratio"]:               # PD scaledown under coordination
    C("scale", f"scaledown pd e=4->2 d=4->2 under {ck}", comps_for("pd", lifecycle(), n=4),
      coord=coord_for(ck), mode="scaledown", Ne=4, Nd=4, Nr=NR, to_e=2, to_d=2, to_r=NR,
      ceil_e=4, ceil_d=4, ceil_r=NR, topo="pd", co=ck)

# ── gang scale at production shape (leader + 3 workers) ──────────────────────
# Scaling MUST delete only the tail (scaledown) / add only new indices (scaleup)
# and leave surviving gangs UNTOUCHED (Check E) — never recreate them. Both
# directions, engine-only + PD (engine+decoder both 3-worker gangs), across
# lifecycle strategies: a strategy that recreates everything is a
# massive-outage bug. 8 replicas = a common production shape.
for strat in ["SurgeThenDrain", "RecreatePod", "InPlaceIfPossible"]:
    glife = strat_life(strat)
    for to in [16, 4]:
        C("scale", f"gang engine-mn W=3 N=8->{to} {strat}", [comp("engine", 8, 3, glife)],
          mode="scaledown", Ne=8, Nd=0, Nr=0, to_e=to,
          ceil_e=max(8, to), ceil_d=0, ceil_r=0, topo="engine-mn", co="none")
        C("scale", f"gang pd-mn e+d W=3 N=8->{to} {strat}",
          [comp("engine", 8, 3, glife), comp("decoder", 8, 3, glife), comp("router", NR, 0, lifecycle())],
          mode="scaledown", Ne=8, Nd=8, Nr=NR, to_e=to, to_d=to, to_r=NR,
          ceil_e=max(8, to), ceil_d=max(8, to), ceil_r=NR, topo="pd-mn", co="none")
# Missing lifecycle: no updateStrategy block -> the webhook MUST inject the
# OMENative defaults (SurgeThenDrain + 25% budgets), for the annotation-selected
# mode too. Absent defaults leave scaling uncapped -> outage risk.
C("scale", "gang engine-mn W=3 N=8->4 NO-lifecycle (webhook must default)",
  [comp("engine", 8, 3, life="")],
  mode="scaledown", Ne=8, Nd=0, Nr=0, to_e=4, ceil_e=8, ceil_d=0, ceil_r=0, topo="engine-mn", co="none")
C("scale", "gang pd-mn e+d W=3 N=8->4 NO-lifecycle (webhook must default)",
  [comp("engine", 8, 3, life=""), comp("decoder", 8, 3, life=""), comp("router", NR, 0, life="")],
  mode="scaledown", Ne=8, Nd=8, Nr=NR, to_e=4, to_d=4, to_r=NR, ceil_e=8, ceil_d=8, ceil_r=NR, topo="pd-mn", co="none")

# ── edge/  (mode=rollout — runs today; %-budgets / ReadyPolicy / in-place / mixed) ──
for topo in ["engine", "pd"]:
    for s in ["25%", "50%", "100%"]:
        sr = math.ceil(N * int(s[:-1]) / 100)   # surge count scales with N (was hardcoded for N=4)
        C("edge", f"%-budget maxSurge={s} {topo} (surge {sr} -> peak {N + sr})",
          comps_for(topo, lifecycle(surge=s, unavail=0)), **nds(topo), **ceils(topo, N + sr), topo=topo, co="none")
for ck in ["ratio", "percomp"]:               # %-surge under pacing
    # group=[engine, decoder] → both roll. engine 50% surge → N+ceil(N/2); decoder is
    # a rolled member at default maxSurge=1 → N+1 (surge_d=1). router not in group.
    _sr50 = math.ceil(N * 50 / 100)
    C("edge", f"%-budget maxSurge=50% pd under {ck}", comps_for("pd", lifecycle(surge="50%", unavail=0)),
      coord=coord_for(ck), **nds("pd"), **ceils("pd", N + _sr50, surge_d=1), topo="pd", co=ck)
# Integer maxSurge>1 single-pod at N=8: a wide concurrent-surge window (peak N+s).
# floor_hard on: SurgeThenDrain (maxUnavailable=0) must never drop below N ready.
for s in [3, 2]:
    C("edge", f"maxSurge={s} engine N=8 SurgeThenDrain (surge {s} -> peak {8 + s})",
      [comp("engine", 8, 0, lifecycle(surge=s, unavail=0))],
      Ne=8, Nd=0, Nr=0, ceil_e=8 + s, ceil_d=0, ceil_r=0, topo="engine", co="none", floor_hard=1)
for topo in ["engine", "pd"]:
    for u in ["25%", "50%"]:
        C("edge", f"%-budget maxUnavailable={u} {topo} RecreatePod",
          comps_for(topo, lifecycle(strat="RecreatePod", surge=0, unavail=u)),
          **nds(topo), **ceils(topo, N), topo=topo, co="none")
for topo in ["engine-mn", "pd-mn"]:
    for rpol in [None, "AllPodReady"]:
        C("edge", f"ReadyPolicy={rpol or 'default'} {topo}", comps_for(topo, lifecycle(ready=rpol)),
          **nds(topo), **ceils(topo, N + 1), topo=topo, co="none")
for marknr in [None, False]:
    C("edge", f"in-place markNotReady={marknr} engine InPlaceIfPossible",
      comps_for("engine", lifecycle(strat="InPlaceIfPossible", surge=0, unavail=1, marknr=marknr)),
      **nds("engine"), **ceils("engine", N), topo="engine", co="none")
for topo in ["engine", "engine-mn"]:
    C("edge", f"InPlaceOnly {topo}", comps_for(topo, lifecycle(strat="InPlaceOnly", surge=0, unavail=1)),
      **nds(topo), **ceils(topo, N if topo == "engine" else N + 1), topo=topo, co="none")
for es, ds in [("SurgeThenDrain", "RecreatePod"), ("InPlaceIfPossible", "SurgeThenDrain"),
               ("RecreatePod", "SurgeThenDrain")]:
    C("edge", f"mixed-strategy RollingUpdate group: engine={es} decoder={ds}",
      [comp("engine", N, 0, strat_life(es)), comp("decoder", N, 0, strat_life(ds)), comp("router", NR, 0, lifecycle())],
      coord=coord_for("rolling"), Ne=N, Nd=N, Nr=NR,
      ceil_e=strat_ceil(es, N), ceil_d=strat_ceil(ds, N), ceil_r=NR, topo="pd", co="rolling")
for topo in TOPOS:                            # X-2 re-bump (mode=rebump) across topologies
    C("edge", f"X-2 re-bump mid-roll {topo} (status revision == pod revision)",
      comps_for(topo, lifecycle()), mode="rebump", **nds(topo), **ceils(topo, N + 1), topo=topo, co="none")


# ── canary/  (mode=canary; spec.rollout.groups[] canary group, staged rollout) ─
# Permutation of the canary step machine across all topologies × promotion
# policies, plus a multi-step (3-gate) plan, a capacity-ahead smoke step, and a
# rollback probe (drain to stable + sticky hold). The externally-routed primary
# (router>engine>decoder) drives the step machine; every staged component surges
# +1 (maxSurge=1), so ceilings are N+1 / NR+1.
CANARY_PAUSE = {"auto": {"duration": "20s"}, "manual": {}}


def _render_canary_step(s, ind="              "):
    """Render one step as YAML line(s). Simple steps stay flow ({...}); analysis
    steps render block style — the analysis subtree + metrics list don't fit a
    one-line flow mapping. Per-step gate is presence-based (the API dropped the
    top-level promotion enum): pause+duration = timed, bare pause = manual,
    analysis = metric-gated, neither = immediate."""
    if "analysis" not in s:
        item = f'capacity: "{s["nc"]}", traffic: {s["tw"]}'
        if "pause" in s:
            item += f', pause: {flow(s["pause"])}'
        return [f"{ind}- {{ {item} }}"]
    a = s["analysis"]
    out = [f'{ind}- capacity: "{s["nc"]}"', f"{ind}  traffic: {s['tw']}"]
    if "pause" in s:
        out.append(f"{ind}  pause: {flow(s['pause'])}")
    out.append(f"{ind}  analysis:")
    ai = f"{ind}    "
    out.append(f'{ai}interval: "{a["interval"]}"')
    out.append(f"{ai}failureLimit: {a['failureLimit']}")
    if a.get("initialDelay"):
        out.append(f'{ai}initialDelay: "{a["initialDelay"]}"')
    if a.get("onInconclusive"):
        out.append(f"{ai}onInconclusive: {a['onInconclusive']}")
    out.append(f"{ai}metrics:")
    for m in a.get("metrics", []):
        out.append(f"{ai}  - name: {m['name']}")
        out.append(f"{ai}    query: '{m['query']}'")     # single-quoted: PromQL {} + {{.Tmpl}} stay literal
        out.append(f"{ai}    operator: {m['operator']}")
        out.append(f'{ai}    threshold: "{m["threshold"]}"')
    return out


def canary_block(steps, components=("engine",), prometheus=None):
    lines = ["  rollout:", "    groups:",
             f"        - components: [{', '.join(components)}]",
             "          canary:"]
    if prometheus is not None:
        lines.append(f"            prometheus: {flow(prometheus)}")
    lines.append("            steps:")
    for s in steps:
        lines += _render_canary_step(s)
    return "\n".join(lines)


def canary_steps(policy, *pairs):
    """(newCapacity, trafficWeight) pairs → step dicts; every step but the last
    carries a pause so the rollout actually gates (Auto: timed; Manual: promote)."""
    out = []
    for i, (nc, tw) in enumerate(pairs):
        s = {"nc": nc, "tw": tw}
        if i < len(pairs) - 1:
            s["pause"] = CANARY_PAUSE[policy]
        out.append(s)
    return out


def canary_ceils(topo):
    cl = ceils(topo, N + 1)
    if is_pd(topo):
        cl["ceil_d"] = N + 1
        cl["ceil_r"] = NR + 1
    return cl


def Ccanary(desc, topo, policy, steps, **extra):
    comps = comps_for(topo, strat_life("SurgeThenDrain"))
    # v2 canary is a group: its components are the canaried Component(s) — the
    # whole topology (single engine, or router+engine+decoder for PD).
    canary_comps = [c["comp"] for c in comps]
    C("canary", f"canary {topo} {policy} {desc}",
      comps,
      coord=canary_block(steps, components=canary_comps),
      mode="canary", **nds(topo), **canary_ceils(topo),
      topo=topo, canary=policy, co="none", **extra)


# Base matrix: every topology × promotion policy, 2-step 50/100.
for topo in TOPOS:
    for policy in ["auto", "manual"]:
        Ccanary("50/100", topo, policy, canary_steps(policy, ("50%", 50), ("100%", 100)))
# Multi-step (3 gates): Auto on engine + pd; Manual on engine (multi-promote path).
Ccanary("25/50/100 3-step", "engine", "auto", canary_steps("auto", ("25%", 25), ("50%", 50), ("100%", 100)))
Ccanary("25/50/100 3-step", "pd", "auto", canary_steps("auto", ("25%", 25), ("50%", 50), ("100%", 100)))
Ccanary("25/50/100 3-step", "engine", "manual", canary_steps("manual", ("25%", 25), ("50%", 50), ("100%", 100)))
# Capacity-ahead smoke: full new capacity at 0% traffic, then cut over to 100%.
Ccanary("smoke 100cap/0tw->100tw", "engine", "auto", canary_steps("auto", ("100%", 0), ("100%", 100)))
# Rollback probe (drain to stable + sticky hold at RolledBack): single-component and PD.
Ccanary("rollback to stable", "engine", "manual", canary_steps("manual", ("50%", 50), ("100%", 100)), rollback=1)
Ccanary("rollback to stable", "pd", "manual", canary_steps("manual", ("50%", 50), ("100%", 100)), rollback=1)


# Analysis cases live at p330+ to stay clear of the hand-written reject/ cases
# (p320-323) — case-ids are globally unique across folders.
COUNTERS["canary"] = 330

# ── canary/ ANALYSIS (mode=canary + analysis=…; OEP-0015.2 metric gate) ────────
# These run the metrics workload (csr-metrics) instead of nginx and gate steps on
# a Prometheus query. run.py sets each canary's health at bump time via the
# Downward-API annotations (perm.test/error-rate, perm.test/emit), so one bump
# both rolls the revision and makes the canary pods PASS / BREACH / read
# INCONCLUSIVE. The query reduces a vector to MAX (worst pod), so it catches the
# canary pod even at a partial step; the bundled Prometheus emits no revision
# label, hence inferenceservice+component scoping only.
CSR_METRICS = "ome-coordtest-metrics"
ERR_QUERY = 'max(canary_error_rate{inferenceservice="{{.ISVCName}}",component="{{.Component}}"})'
# Downward-API health knobs (stable starts healthy) + prometheus scrape opt-in.
METRICS_ANN = {"perm.test/error-rate": "0.0", "perm.test/emit": "true",
               "prometheus.io/scrape": "true", "prometheus.io/port": "8080",
               "prometheus.io/path": "/metrics"}


def err_metric(threshold="0.05", op="LTE", name="error_rate", query=None):
    return {"name": name, "query": query or ERR_QUERY, "operator": op, "threshold": threshold}


def analysis_spec(failure_limit=1, interval="20s", initial_delay="20s",
                  on_inconclusive=None, metrics=None):
    a = {"interval": interval, "failureLimit": failure_limit,
         "metrics": metrics if metrics is not None else [err_metric()]}
    if initial_delay:
        a["initialDelay"] = initial_delay
    if on_inconclusive:
        a["onInconclusive"] = on_inconclusive
    return a


def astep(nc, tw, a=None, pause=None):
    s = {"nc": nc, "tw": tw}
    if pause is not None:
        s["pause"] = pause
    if a is not None:
        s["analysis"] = a
    return s


# Analysis cases run on realistic fleets — a mix of PD + large engine: engine
# cases default to BIG replicas (big metric vector, fine 25/50/75/100 partitions),
# PD cases use PDN per component (engine+decoder+router, exercising the
# secondary-capacity gate + per-revision Services across components).
BIG, PDN = 12, 8


def Canalysis(desc, topo, analysis_mode, steps, *, n=BIG, health=None, policy="auto",
              prometheus=None, ready_timeout=None, decay=None, **extra):
    """Emit a metric-gated canary case. analysis_mode (pass|breach|inconclusive|None)
    is the EXPECTED-terminal intent — run.py asserts it. `health` decouples what the
    canary pods emit (defaults to analysis_mode); analysis_mode=None makes it a
    plain rollback/interaction case (no analysis= header). failurelimit/oninconclusive
    are derived from the gating analysis step so the spec and harness stay in sync."""
    comps = comps_for(topo, strat_life("SurgeThenDrain"), n=n)
    ann = dict(METRICS_ANN)
    if decay:
        ann["perm.test/decay"] = decay
    if (health or analysis_mode) == "inconclusive":
        # Silence the WHOLE component (stable + canary). The max() query spans all
        # pods, so leaving the stable pods emitting would read their value and never
        # be "no data" — the canary must read genuinely inconclusive.
        ann["perm.test/emit"] = "false"
    for c in comps:
        c["ann"] = dict(ann)
    canary_comps = [c["comp"] for c in comps]
    cl = ceils(topo, n + 1)
    if is_pd(topo):
        cl["ceil_d"] = n + 1
        cl["ceil_r"] = NR + 1
    gate = next((s["analysis"] for s in reversed(steps) if "analysis" in s), {})
    kv = dict(mode="canary", **nds(topo, n), **cl, topo=topo, canary=policy,
              failurelimit=gate.get("failureLimit", 1), co="none", **extra)
    if analysis_mode:
        kv["analysis"] = analysis_mode
    if health:
        kv["health"] = health
    if gate.get("onInconclusive"):
        kv["oninconclusive"] = gate["onInconclusive"].lower()
    meta_ann = {"ome.io/rollout-ready-timeout": ready_timeout} if ready_timeout else None
    C("canary", f"canary-analysis {topo} {desc}", comps,
      coord=canary_block(steps, components=canary_comps, prometheus=prometheus),
      runtime=CSR_METRICS, meta_ann=meta_ann, **kv)


def _gate2(a):
    """2-step plan: validate the 50% canary via analysis a, then cut to 100%."""
    return [astep("50%", 50, a), astep("100%", 100)]


# A. Verdict core (large engine + PD) ----------------------------------------
Canalysis("pass->promote", "engine", "pass", _gate2(analysis_spec(failure_limit=1)))
Canalysis("pass->promote", "pd", "pass", _gate2(analysis_spec(failure_limit=1)), n=PDN)
Canalysis("breach->rollback fl=1", "engine", "breach", _gate2(analysis_spec(failure_limit=1)))
Canalysis("breach->rollback fl=1", "pd", "breach", _gate2(analysis_spec(failure_limit=1)), n=PDN)
# Cumulative per-step budget: 3 breaching samples before rollback.
Canalysis("breach->rollback fl=3", "engine", "breach", _gate2(analysis_spec(failure_limit=3)))
# Inconclusive (canary emits no metric): Hold escalates to Failed past the
# (shortened) stall timeout; Rollback fails safe immediately. ready_timeout gates
# BOTH the capacity gate (fires T after step entry) and the analysis stall (fires
# T after capacity is met), so it must comfortably outlast a PD fleet's canary
# capacity ramp under concurrent load — measured ~120s (conc 2) to ~260s (conc 4)
# for 8 pods. 60s and 120s both tripped capacity-Failed before analysis sampled;
# 360s clears the ramp with margin, then the stall fires (~T after capacity).
Canalysis("inconclusive->Failed", "pd", "inconclusive",
          _gate2(analysis_spec(failure_limit=1, on_inconclusive="Hold")), n=PDN, ready_timeout="360s")
Canalysis("inconclusive->rollback", "engine", "inconclusive",
          _gate2(analysis_spec(failure_limit=1, on_inconclusive="Rollback")))

# B. Step-plan shapes (engine) ------------------------------------------------
Canalysis("single-step gate 100", "engine", "pass", [astep("100%", 100, analysis_spec())])
Canalysis("3-step analysis each", "pd", "pass", n=PDN,
          steps=[astep("25%", 25, analysis_spec()), astep("50%", 50, analysis_spec()), astep("100%", 100)])
Canalysis("breach at step0 25%", "pd", "breach", n=PDN,
          steps=[astep("25%", 25, analysis_spec(failure_limit=1)), astep("50%", 50), astep("100%", 100)])
Canalysis("capacity-ahead then gate", "engine", "pass",
          [astep("100%", 0, analysis_spec()), astep("100%", 100)])
Canalysis("mixed analysis+timed", "engine", "pass",
          [astep("50%", 50, analysis_spec()), astep("75%", 75, pause={"duration": "15s"}), astep("100%", 100)])

# C. Metric semantics ---------------------------------------------------------
# Warm-up: a long initialDelay; healthy -> promote (no early sampling).
Canalysis("initialDelay warmup", "engine", "pass", _gate2(analysis_spec(initial_delay="40s")))
# Multi-metric AND: a strict second threshold fails even though the pods are
# healthy (health=pass) -> the AND rolls back. Decoupled health vs terminal.
Canalysis("multi-metric AND strict-fail", "engine", "breach", health="pass",
          steps=_gate2(analysis_spec(failure_limit=1,
                                     metrics=[err_metric(threshold="0.05"),
                                              err_metric(name="strict", threshold="0.001")])))
# Multi-metric AND: both lenient, healthy -> promote.
Canalysis("multi-metric AND pass", "engine", "pass",
          _gate2(analysis_spec(metrics=[err_metric(threshold="0.05"),
                                        err_metric(name="loose", threshold="0.5")])))
# GTE on a success-rate expression (higher is better): canary drops below 0.95.
SUCC_QUERY = '1 - max(canary_error_rate{inferenceservice="{{.ISVCName}}",component="{{.Component}}"})'
Canalysis("operator GTE success breach", "engine", "breach",
          _gate2(analysis_spec(failure_limit=1,
                               metrics=[err_metric(name="success", query=SUCC_QUERY, op="GTE", threshold="0.95")])))
Canalysis("operator GTE success pass", "engine", "pass",
          _gate2(analysis_spec(metrics=[err_metric(name="success", query=SUCC_QUERY, op="GTE", threshold="0.95")])))
# Transient breach then recovery (budget non-exhaustion): each canary pod
# breaches for its first `decay` seconds of serving, then reads 0 -> promote.
# Sized against the step's clock (starts when its last canary pod turns Ready)
# and the 15s Prometheus scrape: initialDelay 30s covers that pod's first scrape,
# so the first sample fails; at most 8 samples fit in the 120s breach (+1 scrape),
# under failureLimit 12; the 150s bake holds the advance until it has cleared.
Canalysis("transient breach recovers", "engine", "pass", health="breach", decay="120",
          steps=[astep("50%", 50, analysis_spec(failure_limit=12, interval="15s", initial_delay="30s"),
                       pause={"duration": "150s"}),
                 astep("100%", 100)],
          minfails=1)
# Manual promote overrides a pending (breaching) analysis bake -> promote.
Canalysis("manual promote overrides analysis", "engine", "pass", health="breach", policy="manual",
          steps=_gate2(analysis_spec(failure_limit=5)))

# D. Source config ------------------------------------------------------------
# Explicit serverAddress (assumes OME installed in ns "ome" — the chart default).
Canalysis("explicit prometheus source", "engine", "pass",
          _gate2(analysis_spec()), prometheus={"serverAddress": "http://ome-prometheus.ome.svc:9090"})
# Custom headers on the default source (X-Scope-OrgID); the bundled prom ignores
# it but the plumbing must not break -> promote.
Canalysis("prometheus custom headers", "engine", "pass",
          _gate2(analysis_spec()), prometheus={"headers": {"X-Scope-OrgID": "perm-test"}})

# E. Interactions -------------------------------------------------------------
# Manual rollback annotation wins over a healthy in-progress analysis canary (PD).
Canalysis("manual rollback during analysis", "pd", None, _gate2(analysis_spec()), n=PDN, rollback=1)

# F. XL fleet -----------------------------------------------------------------
# A tier above the BIG/PDN default: even finer partitions + bigger vectors.
Canalysis("XL fleet N=16 multi-step", "engine", "pass", n=16,
          steps=[astep("25%", 25, analysis_spec()), astep("50%", 50, analysis_spec()),
                 astep("75%", 75, analysis_spec()), astep("100%", 100)])
Canalysis("XL fleet N=16 breach", "engine", "breach", n=16, steps=_gate2(analysis_spec(failure_limit=1)))
Canalysis("XL fleet pd 12+12 breach", "pd", "breach", n=12, steps=_gate2(analysis_spec(failure_limit=1)))


# G2. Evaluator edge cases ----------------------------------------------------
# The metric EXISTS (health=pass, emit=true) but the QUERY yields a non-usable
# value or errors -> the sample must read Inconclusive (never a spurious
# pass/breach) -> rollback (onInconclusive=Rollback). Exercises the reduce /
# unusable (NaN, +Inf) / query-error paths.
def _incl_rollback(metrics):
    return _gate2(analysis_spec(on_inconclusive="Rollback", metrics=metrics))


Canalysis("query error -> inconclusive", "engine", "inconclusive", health="pass",
          steps=_incl_rollback([err_metric(name="bad", query="not_a_function(")]))
Canalysis("NaN metric -> inconclusive", "engine", "inconclusive", health="pass",
          steps=_incl_rollback([err_metric(name="nan", query="vector(0) / vector(0)")]))
Canalysis("Inf metric -> inconclusive", "engine", "inconclusive", health="pass",
          steps=_incl_rollback([err_metric(name="inf", query="vector(1) / vector(0)")]))
# Multi-metric AND, MIXED outcomes: one healthy (pass) + one inconclusive -> the
# sample is Inconclusive (Fail > Inconclusive > Pass) -> rollback.
Canalysis("multi-metric mixed pass+inconclusive", "engine", "inconclusive", health="pass",
          steps=_incl_rollback([err_metric(threshold="0.05"),
                                err_metric(name="nan", query="vector(0) / vector(0)")]))

# G3. Boundary + operators ----------------------------------------------------
# Value EXACTLY at the threshold, per operator (errorrate pins the emitted value).
Canalysis("boundary LTE value==threshold pass", "engine", "pass", errorrate="0.05",
          steps=_gate2(analysis_spec(metrics=[err_metric(threshold="0.05", op="LTE")])))
Canalysis("boundary LT value==threshold fail", "engine", "breach", errorrate="0.05",
          steps=_gate2(analysis_spec(failure_limit=1, metrics=[err_metric(threshold="0.05", op="LT")])))
# GT on a success-rate expr: success below the bar -> breach.
Canalysis("operator GT success breach", "engine", "breach", errorrate="0.9",
          steps=_gate2(analysis_spec(failure_limit=1,
                                     metrics=[err_metric(name="success", query=SUCC_QUERY, op="GT", threshold="0.95")])))
Canalysis("operator LT error pass", "engine", "pass", errorrate="0.01",
          steps=_gate2(analysis_spec(metrics=[err_metric(threshold="0.05", op="LT")])))

# G4. breach at the FINAL (100%) step -----------------------------------------
# Single-step plan: analysis gates the 100% cutover; a breach there rolls back (PD).
Canalysis("breach at final 100pct step", "pd", "breach", n=PDN,
          steps=[astep("100%", 100, analysis_spec(failure_limit=1))])


def main():
    # Idempotency: drop previously-generated 3-digit cases (p100+) in the feature
    # folders so renumbering never leaves stale duplicates (a duplicate case-id
    # across folders makes run.py.find_isvc pick the wrong file — and run.py now
    # hard-errors on one). HAND_WRITTEN cases are preserved, not regenerated.
    for folder in COUNTERS:
        d = ISVCS / folder
        for f in (d.glob("p[0-9][0-9][0-9].yaml") if d.exists() else []):
            if f.stem in HAND_WRITTEN.get(folder, set()):
                continue
            f.unlink()
    per_folder = {}
    for folder, name, desc, comps, coord, kv, opts in CASES:
        if name in HAND_WRITTEN.get(folder, set()):
            sys.exit(f"generated {folder}/{name} collides with a hand-written case — renumber the counter block or move the hand-written id")
        path = ISVCS / folder / f"{name}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(header(name, desc, kv) + "\n"
                        + render(name, comps, coord, runtime=opts["runtime"], meta_ann=opts["meta_ann"]))
        per_folder.setdefault(folder, []).append(name)
    total = sum(len(v) for v in per_folder.values())
    print(f"generated {total} cases:")
    for folder in sorted(per_folder):
        names = per_folder[folder]
        print(f"  {folder:10s} {len(names):2d}  {names[0]}..{names[-1]}")
    # A regen must never reintroduce a duplicate/shadowed case id or a malformed
    # header — fail the generation, not the next harness run.
    rc = subprocess.run([sys.executable, str(HERE / "run.py"), "--validate-only"]).returncode
    if rc:
        sys.exit(rc)


if __name__ == "__main__":
    main()
