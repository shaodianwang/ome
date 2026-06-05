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
  ...inPlaceUpdateStrategy.{gracePeriodSeconds,markNotReadyDuringLifecycle}
"""
from pathlib import Path

HERE = Path(__file__).parent.resolve()
ISVCS = HERE / "isvcs"
CBM, CSR, CSR_MN = "ome-coordtest-noop", "ome-coordtest-cpu", "ome-coordtest-multinode"
N = 4    # default replica Instances (bumped from the toy N=2)
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
              restart=None, ready=None, grace=None, marknr=None, mig=None):
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
    if grace is not None:
        ip["gracePeriodSeconds"] = grace
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


def comp(name, n, w=0, life=None):
    return {"comp": name, "n": n, "w": w, "life": life if life is not None else lifecycle()}


def coord_group(components, policy, pacing=None, order=None):
    # OEP-0015 (#426): coordination moved from top-level spec.rolloutCoordination
    # to spec.rollout.coordination — one extra nesting level, group internals unchanged.
    lines = ["  rollout:", "    coordination:", "      groups:",
             f"        - components: [{', '.join(components)}]",
             f"          policy: {policy}"]
    if order:                                  # Sequential requires an explicit order
        lines.append(f"          order: [{', '.join(order)}]")
    if pacing:
        lines.append(f"          pacing: {flow(pacing)}")
    return "\n".join(lines)


COORD_KINDS = ["none", "rolling", "ratio", "percomp", "seq", "bluegreen"]


def coord_for(kind, comps=("engine", "decoder")):
    cl = list(comps)
    if kind == "none":
        return None
    if kind == "rolling":
        return coord_group(cl, "RollingUpdate")
    if kind == "ratio":
        return coord_group(cl, "RollingUpdate", {"type": "RatioBalanced", "ratioTolerancePercent": 25})
    if kind == "percomp":
        return coord_group(cl, "RollingUpdate", {"type": "PerComponent"})
    if kind == "seq":
        return coord_group(cl, "Sequential", order=cl)
    if kind == "bluegreen":
        return coord_group(cl, "BlueGreen")
    raise ValueError(kind)


def render(name, comps, coord=None):
    runtime = CSR_MN if any(c["w"] > 0 for c in comps) else CSR
    out = ["apiVersion: ome.io/v1beta1", "kind: InferenceService",
           f"metadata: {{ name: {name} }}", "spec:",
           f"  model:   {{ apiGroup: ome.io, kind: ClusterBaseModel,      name: {CBM} }}",
           f"  runtime: {{ apiGroup: ome.io, kind: ClusterServingRuntime, name: {runtime}, autoSync: true }}"]
    for c in comps:
        out += [f"  {c['comp']}:", f"    minReplicas: {c['n']}", f"    maxReplicas: {c['n']}",
                "    annotations: { ome.io/deploymentMode: OMENative }"]
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


def ceils(topo, ce, n=N, nd=None):
    nd = nd if nd is not None else n
    return dict(ceil_e=ce, ceil_d=(nd if is_pd(topo) else 0), ceil_r=(NR if is_pd(topo) else 0))


HDR_ORDER = ["mode", "Ne", "Nd", "Nr", "ceil_e", "ceil_d", "ceil_r", "to_e", "to_d", "to_r",
             "partition", "mig", "restart", "kill", "inst", "co", "topo", "infeasible"]


def header(name, desc, kv):
    parts = [f"{k}={kv[k]}" for k in HDR_ORDER if k in kv]
    parts += [f"{k}={v}" for k, v in kv.items() if k not in HDR_ORDER]
    return f"# {name}: {desc}  " + " ".join(parts)


CASES = []
COUNTERS = {"migration": 100, "restart": 130, "partition": 160, "scale": 190, "edge": 220}


def C(folder, desc, comps, coord=None, **kv):
    # RollingUpdate.Partition is plumbed through the CRD/conversion but NOT
    # enforced by the OMENative rollout engine (it never holds Instances < k) —
    # a known controller gap. Mark the whole folder notimpl so its FAILs record
    # as XFAIL (documented finding), not red. Flips to PASS automatically if the
    # controller ever implements the hold.
    if folder == "partition":
        kv.setdefault("notimpl", 1)
    name = f"p{COUNTERS[folder]}"
    COUNTERS[folder] += 1
    CASES.append((folder, name, desc, comps, coord, kv))


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
    C("partition", f"engine N=4 partition={p}", comps_for("engine", strat_life("SurgeThenDrain", partition=p)),
      mode="partition", **nds("engine"), **ceils("engine", N + 1), topo="engine", partition=p, co="none")
for strat in ["RecreatePod", "InPlaceIfPossible"]:
    C("partition", f"engine partition=2 {strat}", comps_for("engine", strat_life(strat, partition=2)),
      mode="partition", **nds("engine"), **ceils("engine", strat_ceil(strat, N)), topo="engine", partition=2, co="none")
for p in [4, 6]:
    C("partition", f"engine N=8 partition={p}", [comp("engine", 8, 0, strat_life("SurgeThenDrain", partition=p))],
      mode="partition", Ne=8, Nd=0, Nr=0, ceil_e=9, ceil_d=0, ceil_r=0, topo="engine", partition=p, co="none")
C("partition", "engine-mn N=4 W=1 partition=2", comps_for("engine-mn", strat_life("SurgeThenDrain", partition=2)),
  mode="partition", **nds("engine-mn"), **ceils("engine-mn", N + 1), topo="engine-mn", partition=2, co="none")
for topo in PD_TOPOS:                         # PD partition × every coordination policy
    for ck in COORD_KINDS:
        # decoder is also a bumped group member that surges → ceil_d = N+1 too
        cl = ceils(topo, N + 1); cl["ceil_d"] = N + 1
        C("partition", f"{topo} partition=2 {ck}", comps_for(topo, strat_life("SurgeThenDrain", partition=2)),
          coord=coord_for(ck), mode="partition", **nds(topo), **cl, topo=topo, partition=2, co=ck)

# ── scale/  (mode=scaledown; patch min/maxReplicas to to_*, assert drain/grow) ─
for topo, n, to in [("engine", 4, 1), ("engine", 8, 2), ("engine", 4, 2),
                    ("pd", 4, 2), ("engine-mn", 4, 1), ("pd-mn", 4, 2)]:
    kv = dict(mode="scaledown", **nds(topo, n), **ceils(topo, n, n), topo=topo, to_e=to, co="none")
    if is_pd(topo):
        kv["to_d"], kv["to_r"] = to, NR
    C("scale", f"scaledown {topo} N={n}->{to}", comps_for(topo, lifecycle(), n=n), **kv)
C("scale", "scaledown PD asymmetric e=4->1 d=4->2 (router holds)", comps_for("pd", lifecycle()),
  mode="scaledown", Ne=4, Nd=4, Nr=NR, to_e=1, to_d=2, to_r=NR, ceil_e=4, ceil_d=4, ceil_r=NR, topo="pd", co="none")
for topo, to in [("engine", 4), ("engine-mn", 4)]:
    C("scale", f"scaleup {topo} N=2->{to}", comps_for(topo, lifecycle(), n=2),
      mode="scaledown", **nds(topo, 2), to_e=to, ceil_e=to, ceil_d=0, ceil_r=0, topo=topo, co="none")
for ck in ["rolling", "ratio"]:               # PD scaledown under coordination
    C("scale", f"scaledown pd e=4->2 d=4->2 under {ck}", comps_for("pd", lifecycle()),
      coord=coord_for(ck), mode="scaledown", Ne=4, Nd=4, Nr=NR, to_e=2, to_d=2, to_r=NR,
      ceil_e=4, ceil_d=4, ceil_r=NR, topo="pd", co=ck)

# ── edge/  (mode=rollout — runs today; %-budgets / ReadyPolicy / in-place / mixed) ──
for topo in ["engine", "pd"]:
    for s, sr in [("25%", 1), ("50%", 2), ("100%", 4)]:
        C("edge", f"%-budget maxSurge={s} {topo} (surge {sr} -> peak {N + sr})",
          comps_for(topo, lifecycle(surge=s, unavail=0)), **nds(topo), **ceils(topo, N + sr), topo=topo, co="none")
for ck in ["ratio", "percomp"]:               # %-surge under pacing
    C("edge", f"%-budget maxSurge=50% pd under {ck}", comps_for("pd", lifecycle(surge="50%", unavail=0)),
      coord=coord_for(ck), **nds("pd"), **ceils("pd", N + 2), topo="pd", co=ck)
for topo in ["engine", "pd"]:
    for u in ["25%", "50%"]:
        C("edge", f"%-budget maxUnavailable={u} {topo} RecreatePod",
          comps_for(topo, lifecycle(strat="RecreatePod", surge=0, unavail=u)),
          **nds(topo), **ceils(topo, N), topo=topo, co="none")
for topo in ["engine-mn", "pd-mn"]:
    for rpol in ["None", "AllPodReady"]:
        C("edge", f"ReadyPolicy={rpol} {topo}", comps_for(topo, lifecycle(ready=rpol)),
          **nds(topo), **ceils(topo, N + 1), topo=topo, co="none")
for grace, marknr in [(0, None), (10, None), (None, False), (10, False)]:
    C("edge", f"in-place grace={grace} markNotReady={marknr} engine InPlaceIfPossible",
      comps_for("engine", lifecycle(strat="InPlaceIfPossible", surge=0, unavail=1, grace=grace, marknr=marknr)),
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


def main():
    # Idempotency: drop previously-generated 3-digit cases (p100+) in the feature
    # folders so renumbering never leaves stale duplicates (a duplicate case-id
    # across folders makes run.py.find_isvc pick the wrong file). Hand-written
    # 2-digit cases (scale/p97, p98) are preserved.
    for folder in COUNTERS:
        d = ISVCS / folder
        for f in (d.glob("p[0-9][0-9][0-9].yaml") if d.exists() else []):
            f.unlink()
    per_folder = {}
    for folder, name, desc, comps, coord, kv in CASES:
        path = ISVCS / folder / f"{name}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(header(name, desc, kv) + "\n" + render(name, comps, coord))
        per_folder.setdefault(folder, []).append(name)
    total = sum(len(v) for v in per_folder.values())
    print(f"generated {total} cases:")
    for folder in sorted(per_folder):
        names = per_folder[folder]
        print(f"  {folder:10s} {len(names):2d}  {names[0]}..{names[-1]}")


if __name__ == "__main__":
    main()
