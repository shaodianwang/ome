#!/usr/bin/env bash
# Emit fixtures/10-runtime-tpu.yaml adjusted for one topology mode.
#
# Slice co-location can come from two places, and which one you pick changes
# what the runtime has to say:
#
#   podgroup  engineConfig.topologyKey set, so OME annotates the PodGroup with
#             ome.io/topology-key — the production shape, and what OMEGangPack
#             reads. OME would also inject a required worker->leader podAffinity
#             from the same field, which deadlocks against the gang gate, so the
#             worker declares its own required term on that key first: OME
#             defers to a hand-written term and injects nothing. The declared
#             term resolves true in every pool (it anchors on a per-node
#             DaemonSet), leaving co-location entirely to the scheduler.
#
#   plugin    no engineConfig.topologyKey at all. The PodGroup carries no
#             annotation and co-location comes from the scheduler's fleet-wide
#             scheduler.plugin.topologyKey. Simplest, but diverges from
#             production: a real ISVC with leader+worker does annotate its
#             PodGroup.
#
#   strict    production verbatim — topologyKey set, no override, so the real
#             worker->leader podAffinity is injected alongside the PodGroup
#             annotation. Correct under the default scheduler. Under
#             ome-scheduler it wedges; see the README.
#
# Usage: render-runtimes.sh <podgroup|plugin|strict> [--no-scheduler] [--key KEY]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-podgroup}"; shift || true
KEY="cloud.google.com/gke-nodepool"
PIN_SCHEDULER=1

while [ $# -gt 0 ]; do
  case "$1" in
    --no-scheduler) PIN_SCHEDULER=0; shift ;;
    --key) KEY="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "$MODE" in podgroup|plugin|strict) ;; *) echo "unknown mode: $MODE" >&2; exit 2 ;; esac

MODE="$MODE" KEY="$KEY" PIN="$PIN_SCHEDULER" python3 - "$HERE/fixtures/10-runtime-tpu.yaml" <<'PY'
import os, sys

mode, key, pin = os.environ["MODE"], os.environ["KEY"], os.environ["PIN"] == "1"
src = open(sys.argv[1]).read()

if not pin:
    src = "\n".join(l for l in src.splitlines() if l.strip() != "schedulerName: ome-scheduler") + "\n"

if mode in ("podgroup", "strict"):
    src = src.replace("  engineConfig:\n",
                      "  engineConfig:\n    topologyKey: %s\n" % key)

# Only the ome-scheduler path needs the injection suppressed. Under the default
# scheduler the worker->leader term is the mechanism, not the obstacle.
if mode == "podgroup" and pin:
    override = (
        "    worker:\n"
        "      affinity:\n"
        "        podAffinity:\n"
        "          requiredDuringSchedulingIgnoredDuringExecution:\n"
        "            # Pre-empts OME's worker->leader term on this same key.\n"
        "            # kube-proxy runs on every node, so this is true in every\n"
        "            # pool and constrains nothing; OMEGangPack does the real\n"
        "            # co-location off the PodGroup annotation.\n"
        "            - topologyKey: %s\n"
        "              namespaces: [kube-system]\n"
        "              labelSelector:\n"
        "                matchLabels: { k8s-app: kube-proxy }\n" % key
    )
    src = src.replace("    worker:\n", override)

sys.stdout.write(src)
PY
