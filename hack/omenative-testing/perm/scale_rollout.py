#!/usr/bin/env python3
"""One-time scaler for the static base rollout matrix (isvcs/rollout/p01-p65,
a hand-committed set with no live generator). Bumps N=4 engine/decoder replicas to
N=8 and recomputes per-Component header ceilings from each spec's actual
maxSurge/strategy, preserving file formatting (edits replicas via text, reads
values via yaml, rewrites the header comment). Router (NR=2) is left as-is.

Ceiling per Component: SurgeThenDrain -> N + surge_count(maxSurge); otherwise N.
surge_count resolves int or "NN%". If a rollout group sets a group maxSurge, the
effective per-Component surge is min(component, group) — mirrors the controller.
Run from perm/:  python3 scale_rollout.py
"""
import glob
import math
import re

import yaml

NEWN = 8


def surge_count(ms, n):
    if ms is None:
        return 0
    if isinstance(ms, str) and ms.endswith("%"):
        return math.ceil(n * int(ms[:-1]) / 100)
    return int(ms)


def group_surge(doc):
    """Group-wide maxSurge if the rollout groups set one (else None = uncapped)."""
    groups = (doc.get("spec", {}).get("rollout", {}) or {}).get("groups", []) or []
    for g in groups:
        ru = g.get("rollingUpdate")
        if isinstance(ru, dict) and "maxSurge" in ru:
            return ru["maxSurge"]
    return None


def comp_ceiling(c, n, gsurge):
    strat = ((c.get("lifecycle", {}) or {}).get("updateStrategy", {}) or {}).get("type", "SurgeThenDrain")
    if strat != "SurgeThenDrain":
        return n
    ms = ((c.get("lifecycle", {}) or {}).get("updateStrategy", {}) or {}).get("rollingUpdate", {}) or {}
    ms = ms.get("maxSurge")
    s = surge_count(ms, n)
    if gsurge is not None:
        s = min(s, surge_count(gsurge, n))
    return n + s


changed = 0
for f in sorted(glob.glob("isvcs/rollout/p*.yaml")):
    txt = open(f).read()
    lines = txt.splitlines()
    hdr = lines[0]
    m = re.search(r"Ne=(\d+)", hdr)
    if not m or m.group(1) != "4":
        continue  # only the N=4 base cases
    doc = yaml.safe_load("\n".join(lines[1:]))
    spec = doc.get("spec", {})
    gsurge = group_surge(doc)

    # Bump replicas in the raw text (only the ==4 components; router is 2).
    body = "\n".join(lines[1:])
    body = re.sub(r"minReplicas: 4\b", f"minReplicas: {NEWN}", body)
    body = re.sub(r"maxReplicas: 4\b", f"maxReplicas: {NEWN}", body)

    # Recompute ceilings from the (pre-bump) spec but at the new N.
    def dims(name):
        c = spec.get(name)
        if not c:
            return 0, 0
        n = c.get("minReplicas", 0)
        if n == 4:
            n = NEWN
        return n, comp_ceiling(c, n, gsurge)

    ne, ce = dims("engine")
    nd, cd = dims("decoder")
    nr, cr = dims("router")

    # Rewrite the header's Ne/Nd/Nr + ceil_* (leave the rest of the comment).
    new_hdr = hdr
    new_hdr = re.sub(r"Ne=\d+", f"Ne={ne}", new_hdr)
    new_hdr = re.sub(r"Nd=\d+", f"Nd={nd}", new_hdr)
    new_hdr = re.sub(r"Nr=\d+", f"Nr={nr}", new_hdr)
    new_hdr = re.sub(r"ceil_e=\d+", f"ceil_e={ce}", new_hdr)
    new_hdr = re.sub(r"ceil_d=\d+", f"ceil_d={cd}", new_hdr)
    new_hdr = re.sub(r"ceil_r=\d+", f"ceil_r={cr}", new_hdr)

    open(f, "w").write(new_hdr + "\n" + body + "\n")
    changed += 1
    print(f"{f.split('/')[-1]}: Ne={ne} Nd={nd} Nr={nr} ceil_e={ce} ceil_d={cd} ceil_r={cr}")

print(f"\nscaled {changed} base rollout cases to N={NEWN}")
