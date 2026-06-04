#!/usr/bin/env python3
"""gen_mn.py — generate the MULTI-NODE test matrix into isvcs/pNN.yaml.

Each multi-node Instance is a gang of 1 leader + W worker pods (LeaderWorkerSet
+ scheduler-plugins PodGroup). The matrix sweeps three axes:
  topology     : worker.size (W), #Instances (N), surge headroom (min<max)
  lifecycle    : SurgeThenDrain | RecreatePod | InPlaceIfPossible
  coordination : none | RatioBalanced | Sequential | BlueGreen | PerComponent

N[comp] is the #Instances (minReplicas); the harness counts INSTANCES not raw
pods. Ceilings are in Instance units: a SurgeThenDrain surge peaks at N+maxSurge
Instances; recreate/in-place peak at N. Requires the ome-coordtest-multinode
runtime (leader.runner + worker.runner). Run: python3 gen_mn.py
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
RT = "ome-coordtest-multinode"


def C(name, N, mx, w, strat, surge, unavail):
    return dict(name=name, N=N, mx=mx, w=w, strat=strat, surge=surge, unavail=unavail)


def _ceil(c):
    return c["N"] + c["surge"] if (c["strat"] == "SurgeThenDrain" and c["surge"] > 0) else c["N"]


def _block(c):
    s = [f"  {c['name']}:",
         f"    minReplicas: {c['N']}",
         f"    maxReplicas: {c['mx']}",
         "    annotations: { ome.io/deploymentMode: OMENative }"]
    if c["w"]:                                  # multi-node: leader + W workers
        s += ["    leader: {}", f"    worker: {{ size: {c['w']} }}"]
    s.append(f"    lifecycle: {{ updateStrategy: {{ type: {c['strat']}, "
             f"rollingUpdate: {{ maxSurge: {c['surge']}, maxUnavailable: {c['unavail']} }} }} }}")
    return "\n".join(s)


def emit(cid, desc, comps, coord=None):
    by = {c["name"]: c for c in comps}
    N = {k: by.get(k, {}).get("N", 0) for k in ("engine", "decoder", "router")}
    ce = {k: (_ceil(by[k]) if k in by else 0) for k in ("engine", "decoder", "router")}
    topo = "pd-mn" if "decoder" in by else "engine-mn"   # categorization tag run.py keys on
    lines = [f"# {cid}: {desc}  Ne={N['engine']} Nd={N['decoder']} Nr={N['router']} "
             f"ceil_e={ce['engine']} ceil_d={ce['decoder']} ceil_r={ce['router']} topo={topo}",
             "apiVersion: ome.io/v1beta1", "kind: InferenceService",
             f"metadata: {{ name: {cid} }}", "spec:",
             "  model:   { apiGroup: ome.io, kind: ClusterBaseModel,      name: ome-coordtest-noop }",
             f"  runtime: {{ apiGroup: ome.io, kind: ClusterServingRuntime, name: {RT}, autoSync: true }}"]
    lines += [_block(c) for c in comps]
    if coord:
        lines += ["  rollout:", "    coordination:", "      groups:",   # OEP-0015 (#426)
                  f"        - components: [{', '.join(coord['members'])}]",
                  f"          policy: {coord['policy']}"]
        if coord.get("order"):
            lines.append(f"          order: [{', '.join(coord['order'])}]")
        if coord.get("pacing"):
            lines.append(f"          pacing: {coord['pacing']}")
    with open(os.path.join(HERE, "isvcs", "rollout", f"{cid}.yaml"), "w") as f:  # canonical location
        f.write("\n".join(lines) + "\n")
    print(f"  {cid}: {desc}")


RB = "{ type: RatioBalanced, ratioTolerance: 25, maxUnavailable: 1 }"
RB10 = "{ type: RatioBalanced, ratioTolerance: 10, maxUnavailable: 1 }"  # tight band (multi-node p53 analog)
PC = "{ type: PerComponent, maxUnavailable: 1 }"
RTR = C("router", 2, 2, 0, "SurgeThenDrain", 1, 0)   # router: single-pod control

COMBOS = [
    # --- single-component (engine) multi-node: lifecycle x topology ---
    ("p66", "MN engine leader+1w N=2 SurgeThenDrain (gang surge)",
     [C("engine", 2, 2, 1, "SurgeThenDrain", 1, 0)], None),
    ("p67", "MN engine leader+2w N=2 RecreatePod (3-pod gang recreate together)",
     [C("engine", 2, 2, 2, "RecreatePod", 0, 1)], None),
    ("p68", "MN engine leader+1w N=2 InPlaceIfPossible (gang in-place together)",
     [C("engine", 2, 2, 1, "InPlaceIfPossible", 0, 1)], None),
    ("p69", "MN engine leader+1w N=2 max=3 SurgeThenDrain (SURGE HEADROOM: should peak 3 Instances)",
     [C("engine", 2, 3, 1, "SurgeThenDrain", 1, 0)], None),
    ("p70", "MN engine leader+2w N=3 SurgeThenDrain (3 instances x 3-pod gangs)",
     [C("engine", 3, 3, 2, "SurgeThenDrain", 1, 0)], None),
    ("p71", "MN engine leader+1w N=3 RecreatePod maxUnavail=1",
     [C("engine", 3, 3, 1, "RecreatePod", 0, 1)], None),
    # --- PD multi-node (engine+decoder gangs, router single): coordination x lifecycle ---
    ("p72", "PD MN e+d leader+1w N=2 RatioBalanced SurgeThenDrain",
     [C("engine", 2, 2, 1, "SurgeThenDrain", 1, 0), C("decoder", 2, 2, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=RB)),
    ("p73", "PD MN e+d leader+1w N=2 Sequential[dec,eng] SurgeThenDrain",
     [C("engine", 2, 2, 1, "SurgeThenDrain", 1, 0), C("decoder", 2, 2, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="Sequential", members=["decoder", "engine"], order=["decoder", "engine"])),
    ("p74", "PD MN e+d leader+1w N=2 BlueGreen (gang flip both)",
     [C("engine", 2, 2, 1, "SurgeThenDrain", 1, 0), C("decoder", 2, 2, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="BlueGreen", members=["engine", "decoder"])),
    ("p75", "PD MN e+d leader+1w N=2 PerComponent SurgeThenDrain",
     [C("engine", 2, 2, 1, "SurgeThenDrain", 1, 0), C("decoder", 2, 2, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=PC)),
    ("p76", "PD MN e+d leader+1w N=2 RatioBalanced RecreatePod (gang drain both) [recreate drain-first under RatioBalanced: band unsatisfiable] infeasible=1",
     [C("engine", 2, 2, 1, "RecreatePod", 0, 1), C("decoder", 2, 2, 1, "RecreatePod", 0, 1), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=RB)),
    ("p77", "PD MN e+d leader+1w N=2 Sequential RecreatePod",
     [C("engine", 2, 2, 1, "RecreatePod", 0, 1), C("decoder", 2, 2, 1, "RecreatePod", 0, 1), RTR],
     dict(policy="Sequential", members=["decoder", "engine"], order=["decoder", "engine"])),
    ("p78", "PD MN ASYMMETRIC e(leader+2w)+d(leader+1w) N=2 RatioBalanced STD",
     [C("engine", 2, 2, 2, "SurgeThenDrain", 1, 0), C("decoder", 2, 2, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=RB)),
    ("p79", "PD MN e+d leader+1w N=2 max=3 RatioBalanced STD (surge headroom)",
     [C("engine", 2, 3, 1, "SurgeThenDrain", 1, 0), C("decoder", 2, 3, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=RB)),

    # === EXPANSION (p80+): bug-prone gang edges under-sampled by p66-p79 ===

    # Tight-band / small-N gang surge — the multi-node analogs of the single-pod
    # p53 (tol=10) / p55 (N=1) surge-deadlock candidates. If the RatioBalanced
    # surge tiebreaker false-deadlocks a legitimate transient gang surge (1
    # surge at N=4 = 25% > 2*tol=20%), these catch it on the production (gang)
    # path. p82 isolates the N=1 gang surge with no coordination.
    ("p80", "PD MN e+d leader+1w N=4 RatioBalanced tol=10 STD (tight band gang surge — p53 gang analog)",
     [C("engine", 4, 5, 1, "SurgeThenDrain", 1, 0), C("decoder", 4, 5, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=RB10)),
    ("p81", "PD MN e+d leader+1w N=1 RatioBalanced STD (minimal gang surge — p55 gang analog)",
     [C("engine", 1, 2, 1, "SurgeThenDrain", 1, 0), C("decoder", 1, 2, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=RB)),
    ("p82", "MN engine leader+1w N=1 SurgeThenDrain (single-component minimal gang surge)",
     [C("engine", 1, 2, 1, "SurgeThenDrain", 1, 0)], None),
    ("p96", "PD MN e+d leader+1w N=2 RatioBalanced tol=10 STD (tight band at N=2 gang)",
     [C("engine", 2, 3, 1, "SurgeThenDrain", 1, 0), C("decoder", 2, 3, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=RB10)),

    # Bigger gangs (W>=2) in PD + under coordination — surge/recreate/atomicity
    # of 3-4 pod gangs, not just leader+1w.
    ("p83", "PD MN e+d leader+2w N=2 RatioBalanced STD (3-pod gangs, surge both)",
     [C("engine", 2, 3, 2, "SurgeThenDrain", 1, 0), C("decoder", 2, 3, 2, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=RB)),
    ("p86", "MN engine leader+3w N=2 RecreatePod (4-pod gang recreate together)",
     [C("engine", 2, 2, 3, "RecreatePod", 0, 1)], None),
    ("p94", "PD MN e+d leader+2w N=2 BlueGreen (3-pod gang flip both)",
     [C("engine", 2, 3, 2, "SurgeThenDrain", 1, 0), C("decoder", 2, 3, 2, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="BlueGreen", members=["engine", "decoder"])),

    # Scale: more gang Instances + multiple surges in flight (maxSurge=2).
    ("p84", "MN engine leader+1w N=4 SurgeThenDrain (scale: 4 gang Instances)",
     [C("engine", 4, 5, 1, "SurgeThenDrain", 1, 0)], None),
    ("p85", "MN engine leader+1w N=4 max=6 SurgeThenDrain maxSurge=2 (two gang surges in flight)",
     [C("engine", 4, 6, 1, "SurgeThenDrain", 2, 0)], None),
    ("p93", "MN engine leader+2w N=4 RecreatePod maxUnavail=2 (two 3-pod gangs recreate at once)",
     [C("engine", 4, 4, 2, "RecreatePod", 0, 2)], None),
    ("p95", "PD MN e+d leader+1w N=3 RatioBalanced STD (odd-N gang surge)",
     [C("engine", 3, 4, 1, "SurgeThenDrain", 1, 0), C("decoder", 3, 4, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=RB)),

    # Asymmetric gang topologies — the coordination gate counts PODS, so a
    # 4-pod vs 2-pod gang pair has pod-ratio 2:1 but Instance-ratio 1:1; this
    # is where pod-based RatioBalanced can mis-balance.
    ("p87", "PD MN ASYMMETRIC e(leader+3w)+d(leader+1w) N=2 RatioBalanced STD (4-pod vs 2-pod gangs)",
     [C("engine", 2, 3, 3, "SurgeThenDrain", 1, 0), C("decoder", 2, 3, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=RB)),
    ("p88", "PD MN ASYMMETRIC e(leader+2w)+d(leader+1w) N=2 Sequential STD (asymmetric serial)",
     [C("engine", 2, 3, 2, "SurgeThenDrain", 1, 0), C("decoder", 2, 3, 1, "SurgeThenDrain", 1, 0), RTR],
     dict(policy="Sequential", members=["decoder", "engine"], order=["decoder", "engine"])),

    # 3-way coordination with gangs — engine+decoder gangs + single-pod router in
    # one ordering chain (the gang analog of single-pod p58).
    ("p89", "PD MN 3-way Sequential[router,decoder,engine] e+d leader+1w STD (gang ordering chain)",
     [C("engine", 2, 3, 1, "SurgeThenDrain", 1, 0), C("decoder", 2, 3, 1, "SurgeThenDrain", 1, 0),
      C("router", 2, 3, 0, "SurgeThenDrain", 1, 0)],
     dict(policy="Sequential", members=["router", "decoder", "engine"], order=["router", "decoder", "engine"])),

    # In-place under coordination + strict in-place — gang in-place falls back to
    # recreate; verify it does so within a coordination group and that
    # InPlaceOnly recreates for gangs rather than erroring.
    ("p90", "PD MN e+d leader+1w N=2 RatioBalanced InPlaceIfPossible (gang in-place->recreate under coord) [in-place->recreate drain-first under RatioBalanced: band unsatisfiable] infeasible=1",
     [C("engine", 2, 2, 1, "InPlaceIfPossible", 0, 1), C("decoder", 2, 2, 1, "InPlaceIfPossible", 0, 1), RTR],
     dict(policy="RollingUpdate", members=["engine", "decoder"], pacing=RB)),
    ("p91", "MN engine leader+1w N=2 InPlaceOnly (strict in-place gang -> recreate, must not error)",
     [C("engine", 2, 2, 1, "InPlaceOnly", 0, 1)], None),

    # RecreatePod at minimal N.
    ("p92", "MN engine leader+1w N=1 RecreatePod (minimal gang recreate)",
     [C("engine", 1, 1, 1, "RecreatePod", 0, 1)], None),
]

if __name__ == "__main__":
    for cid, desc, comps, coord in COMBOS:
        emit(cid, desc, comps, coord)
    print(f"generated {len(COMBOS)} multi-node cases (p66-p{65 + len(COMBOS)})")
