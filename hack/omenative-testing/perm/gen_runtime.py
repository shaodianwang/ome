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

Run:  python3 gen_runtime.py    (writes isvcs/runtime-rev/*.yaml; `--check`
compares against the committed tree without writing). Hand-written cases share
the folder: the write path (casefiles.py) touches only the ids minted here.
"""
import argparse
import subprocess
import sys
from pathlib import Path

import casefiles

HERE = Path(__file__).parent
CBM = "ome-coordtest-noop"
CSR_SP = (HERE / "runtime" / "csr.yaml").read_text()           # single-pod base
CSR_MN = (HERE / "runtime" / "csr-multinode.yaml").read_text() # gang base

TOPOS = ["engine", "pd", "engine-mn", "pd-mn"]
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


def _case(cid, topo, desc, autosync, **hdr):
    """One case: its dedicated runtime and the ISVC referencing it. Ceilings are
    SurgeThenDrain maxSurge=1 on every rolled component."""
    rt = f"ome-coordtest-rt-{cid}"
    n = N_MN if topo.endswith("-mn") else N_SP
    pd = topo.startswith("pd")
    kv = ([("mode", "runtimerev"), ("rt", rt)] + list(hdr.items())
          + [("Ne", n), ("Nd", n if pd else 0), ("Nr", 0),
             ("ceil_e", n + 1), ("ceil_d", n + 1 if pd else 0), ("ceil_r", 0), ("topo", topo)])
    body = dedicated_csr(topo, rt) + "\n---\n" + isvc(cid, topo, autosync, rt)
    # Single-space header: the key region starts at the first key=value token.
    return casefiles.Case("runtime-rev", cid, desc, kv, body, sep=" ")


def cases():
    """Every minted runtime-rev case, rendered."""
    out = []
    n = 400
    for topo in TOPOS:
        for tag, autosync, expect, edit in BEHAVIORS:
            cid = f"p{n}"; n += 1
            out.append(_case(cid, topo, f"runtime-rev {topo} {tag} (autoSync={autosync}, {edit} edit)",
                             autosync, expect=expect, edit=edit))
    # Recovery guards: drive a FAILED rollout (bad runtime image) then a corrective
    # runtime edit — the ISVC must recover with zero manual intervention,
    # across all four topologies.
    for topo in TOPOS:
        cid = f"p{n}"; n += 1
        out.append(_case(cid, topo, f"runtime-rev {topo} recover-after-failed-rollout",
                         "true", expect="recover"))
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true",
                    help="regenerate in memory and byte-compare against the committed "
                         "tree without writing; nonzero on any drift")
    args = ap.parse_args(argv)
    minted = cases()
    if args.check:
        return casefiles.check(minted, HERE / "isvcs", "gen_runtime.py")
    written = casefiles.write(minted, HERE / "isvcs")
    for c in minted:
        print(f"  {c.cid}: {c.desc}")
    print(f"generated {len(minted)} runtime-rev cases; {len(written)} file(s) written")

    # A regen must never reintroduce a duplicate/shadowed case id or a malformed
    # header — fail the generation, not the next harness run.
    rc = subprocess.run([sys.executable, str(HERE / "run.py"), "--validate-only"]).returncode
    if rc:
        sys.exit(rc)
    return 0


if __name__ == "__main__":
    sys.exit(main())
