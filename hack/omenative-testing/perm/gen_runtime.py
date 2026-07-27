#!/usr/bin/env python3
"""Generate runtime-rev cases — the rollout trigger the rest of the suite never
exercises: modify the RUNTIME (ClusterServingRuntime), not the ISVC.

Every other case patches the ISVC to trigger a roll. These patch the ISVC's
runtime instead and assert the dependent ISVC behaves per autoSync:

  roll    autoSync=true  + material runtime edit  -> ISVC re-renders and rolls
  noop    autoSync=true  + no-op runtime edit     -> no roll (revision-hash gates)
  pinned  autoSync=false + material runtime edit  -> no roll (drift, not roll)

Each case is a DEDICATED ClusterServingRuntime (name-rewritten from the shared
fixture, so patching it is isolated from concurrent cases) plus an ISVC that
references it, emitted as one multi-doc file. Covers all four topologies so the
runtime-triggered roll is tested against PD coordination and multi-node gang
atomicity, not just single-pod engine.

Run:  python3 gen_runtime.py    (writes isvcs/runtime-rev/*.yaml)
"""
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
OUT = HERE / "isvcs" / "runtime-rev"
CBM = "ome-coordtest-noop"
CSR_SP = (HERE / "runtime" / "csr.yaml").read_text()           # single-pod base
CSR_MN = (HERE / "runtime" / "csr-multinode.yaml").read_text() # gang base

TOPOS = ["engine", "pd", "engine-mn", "pd-mn"]
# Hand-written cases parked in the generated folder (p416 = the break=stuck
# surge-budget-deadlock guard, #719): the regen sweep must preserve them.
HAND_WRITTEN = {"p416"}
BEHAVIORS = [  # (tag, autoSync, expect, edit)
    ("roll",   "true",  "roll",   "material"),
    ("noop",   "true",  "noroll", "noop"),
    ("pinned", "false", "noroll", "material"),
]
N_SP, N_MN, W_MN = 4, 2, 1  # single-pod instances / gang instances / workers per gang


def dedicated_csr(topo, name):
    """The shared runtime fixture with its metadata.name rewritten to `name`, so a
    patch touches only this case's ISVC."""
    if topo.endswith("-mn"):
        return CSR_MN.replace("ome-coordtest-multinode", name)
    return CSR_SP.replace("ome-coordtest-cpu", name)


def comps_for(topo):
    return ["engine", "decoder"] if topo.startswith("pd") else ["engine"]


def isvc(cid, topo, autosync, rt):
    mn = topo.endswith("-mn")
    N = N_MN if mn else N_SP
    lines = ["apiVersion: ome.io/v1beta1", "kind: InferenceService",
             f"metadata: {{ name: {cid} }}", "spec:",
             f"  model:   {{ apiGroup: ome.io, kind: ClusterBaseModel,      name: {CBM} }}",
             f"  runtime: {{ apiGroup: ome.io, kind: ClusterServingRuntime, name: {rt}, autoSync: {autosync} }}"]
    for c in comps_for(topo):
        lines += [f"  {c}:", f"    minReplicas: {N}", f"    maxReplicas: {N}",
                  "    annotations: { ome.io/deploymentMode: OMENative }"]
        if mn:
            lines += ["    leader: {}", f"    worker: {{ size: {W_MN} }}"]
        lines.append("    lifecycle: { updateStrategy: { type: SurgeThenDrain, "
                     "rollingUpdate: { maxSurge: 1, maxUnavailable: 0 } } }")
    return "\n".join(lines) + "\n"


def main():
    if OUT.exists():
        for f in OUT.glob("*.yaml"):
            if f.stem not in HAND_WRITTEN:   # regen everything generated; keep hand-written
                f.unlink()
    OUT.mkdir(parents=True, exist_ok=True)
    n = 400
    for topo in TOPOS:
        mn = topo.endswith("-mn")
        N = N_MN if mn else N_SP
        pd = topo.startswith("pd")
        Ne, Nd = N, (N if pd else 0)
        ce, cd = Ne + 1, (Nd + 1 if pd else 0)  # SurgeThenDrain maxSurge=1
        for tag, autosync, expect, edit in BEHAVIORS:
            cid = f"p{n}"; n += 1
            if cid in HAND_WRITTEN:
                sys.exit(f"generated {cid} collides with a hand-written case — renumber")
            rt = f"ome-coordtest-rt-{cid}"
            desc = (f"{cid}: runtime-rev {topo} {tag} (autoSync={autosync}, {edit} edit) "
                    f"mode=runtimerev rt={rt} expect={expect} edit={edit} "
                    f"Ne={Ne} Nd={Nd} Nr=0 ceil_e={ce} ceil_d={cd} ceil_r=0 topo={topo}")
            body = f"# {desc}\n" + dedicated_csr(topo, rt) + "\n---\n" + isvc(cid, topo, autosync, rt)
            (OUT / f"{cid}.yaml").write_text(body)
            print(f"  {cid}: {topo:10s} {tag}")

    # Recovery guards: drive a FAILED rollout (bad runtime image) then a corrective
    # runtime edit — the ISVC must recover. Known-failing today (Failed escalation is
    # terminal; corrective edits are ignored); guards the fix.
    for topo in TOPOS:
        mn = topo.endswith("-mn")
        N = N_MN if mn else N_SP
        pd = topo.startswith("pd")
        Ne, Nd = N, (N if pd else 0)
        ce, cd = Ne + 1, (Nd + 1 if pd else 0)
        cid = f"p{n}"; n += 1
        if cid in HAND_WRITTEN:
            sys.exit(f"generated {cid} collides with a hand-written case — renumber")
        rt = f"ome-coordtest-rt-{cid}"
        # notimpl=1: recovery-after-failed-rollout is a known-unimplemented
        # controller feature (Failed escalation is terminal; corrective edits are
        # ignored) — record XFAIL, not a regression. A genuine status-lie surfaced
        # meanwhile (invariant A) still re-hardens the verdict back to FAIL, so the
        # marker documents the known wedge without masking a real status violation.
        desc = (f"{cid}: runtime-rev {topo} recover-after-failed-rollout (KNOWN BUG: wedges today) "
                f"mode=runtimerev rt={rt} expect=recover notimpl=1 "
                f"Ne={Ne} Nd={Nd} Nr=0 ceil_e={ce} ceil_d={cd} ceil_r=0 topo={topo}")
        body = f"# {desc}\n" + dedicated_csr(topo, rt) + "\n---\n" + isvc(cid, topo, "true", rt)
        (OUT / f"{cid}.yaml").write_text(body)
        print(f"  {cid}: {topo:10s} recover")

    # A regen must never reintroduce a duplicate/shadowed case id or a malformed
    # header — fail the generation, not the next harness run.
    rc = subprocess.run([sys.executable, str(HERE / "run.py"), "--validate-only"]).returncode
    if rc:
        sys.exit(rc)


if __name__ == "__main__":
    main()
