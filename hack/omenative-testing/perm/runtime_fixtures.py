#!/usr/bin/env python3
"""Render the runtime/ fixtures for a target cluster.

The fixtures are not portable as literal YAML. Three things vary per cluster:

  * the namespace holding the PVC. A ClusterBaseModel is cluster-scoped, so its
    storageUri must spell out the PVC's namespace, and the OME validating
    webhook rejects an InferenceService whose model references a PVC in another
    namespace. PVC, seed Job and storageUri therefore have to agree with
    $OME_TEST_NS, where the cases land.
  * the storage class and PVC access mode.
  * the node architecture and the image registry.

Each is a ${TOKEN} in the YAML, substituted here. Only the tokens below are
touched, so shell expansions inside container commands (${TEST_STARTUP_DELAY})
survive rendering untouched.

Usage:
    python3 runtime_fixtures.py [--kind] > rendered.yaml
    python3 runtime_fixtures.py --kind | kubectl apply -n sandbox -f -
"""
import argparse
import os
import sys
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parent / "runtime"

# Defaults target the shared GKE eval cluster.
DEFAULTS = {
    "OME_TEST_NS": "sandbox",
    "OME_EVAL_STORAGE_CLASS": "shared-vast",
    "OME_EVAL_PVC_ACCESS_MODE": "ReadWriteMany",
    "OME_EVAL_NODE_ARCH": "amd64",
    "OME_EVAL_NGINX_IMAGE": (
        "168653207203.dkr.ecr.us-east-2.amazonaws.com/pt_dockerio/"
        "nginxinc/nginx-unprivileged:1.27-alpine"
    ),
    "OME_EVAL_METRICS_IMAGE": (
        "168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/canary-metrics:latest"
    ),
}

# A single-node KIND cluster has no arch to select for, only the default
# local-path storage class (which is ReadWriteOnce), and no pull credentials
# for the private registry — so the public upstream image is used and
# side-loaded with `kind load`.
KIND_OVERRIDES = {
    "OME_EVAL_STORAGE_CLASS": "standard",
    "OME_EVAL_PVC_ACCESS_MODE": "ReadWriteOnce",
    "OME_EVAL_NODE_ARCH": "",
    "OME_EVAL_NGINX_IMAGE": "nginxinc/nginx-unprivileged:1.27-alpine",
}


def kind_from_env():
    """True when the target is a KIND cluster (set by deploy-ome-kind.sh)."""
    return os.environ.get("OME_EVAL_KIND", "").lower() in ("1", "true", "yes")


def resolve(kind=False):
    """Token values: defaults, then KIND overrides, then the environment."""
    values = dict(DEFAULTS)
    if kind:
        values.update(KIND_OVERRIDES)
    for token in DEFAULTS:
        if token in os.environ:
            values[token] = os.environ[token]
    return values


def render_text(text, values):
    for token, value in values.items():
        text = text.replace("${%s}" % token, value)
    # An empty arch selects nothing; drop the line rather than emit an empty
    # map value, which would make the pod unschedulable on every node.
    if not values["OME_EVAL_NODE_ARCH"]:
        text = "\n".join(
            line for line in text.splitlines()
            if "kubernetes.io/arch" not in line
        ) + "\n"
    return text


def render(kind=False):
    """Return every runtime fixture rendered and joined as one YAML stream."""
    values = resolve(kind=kind)
    docs = [
        render_text(path.read_text(), values)
        for path in sorted(RUNTIME_DIR.glob("*.yaml"))
    ]
    return "\n---\n".join(docs)


def images(kind=False):
    """The container images the rendered fixtures reference, deduplicated.

    A KIND cluster has to side-load these, so the caller needs the resolved
    list rather than a copy of it that can drift from the fixtures.
    """
    seen = []
    for line in render(kind=kind).splitlines():
        stripped = line.strip()
        if stripped.startswith("image:"):
            image = stripped.split(":", 1)[1].strip()
            if image and image not in seen:
                seen.append(image)
    return seen


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--kind", action="store_true",
                    help="render for a local KIND cluster")
    ap.add_argument("--images", action="store_true",
                    help="list the referenced images instead of the YAML")
    args = ap.parse_args()
    if args.images:
        sys.stdout.write("".join(i + "\n" for i in images(kind=args.kind)))
    else:
        sys.stdout.write(render(kind=args.kind))


if __name__ == "__main__":
    main()
