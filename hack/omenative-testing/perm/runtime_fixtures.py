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
survive rendering untouched. Case manifests that embed their own runtime carry
the same tokens; run.py renders them through this module before applying.

Usage:
    python3 runtime_fixtures.py [--kind] > rendered.yaml
    python3 runtime_fixtures.py --kind --stage bootstrap > bootstrap.yaml
    python3 runtime_fixtures.py --kind --stage resources > resources.yaml
    python3 runtime_fixtures.py --kind --value OME_EVAL_NGINX_IMAGE
    python3 runtime_fixtures.py --case-images
    python3 runtime_fixtures.py --kind | kubectl apply -n sandbox -f -
"""
import argparse
import os
import re
import sys
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parent / "runtime"
CASE_DIR = Path(__file__).resolve().parent / "isvcs"

STAGE_ALL = "all"
STAGE_BOOTSTRAP = "bootstrap"
STAGE_RESOURCES = "resources"
STAGES = (STAGE_ALL, STAGE_BOOTSTRAP, STAGE_RESOURCES)

# The PVC must exist and the seed Job must finish before the ClusterBaseModel
# starts its metadata-extraction Job.
BOOTSTRAP_FIXTURES = ("pvc-and-seed.yaml",)
KIND_BOOTSTRAP_FIXTURES = ("pv-kind.yaml",)

# Defaults are the public image names and the shared eval cluster's storage
# layout. A cluster that pulls only through a registry mirror exports the
# image tokens (the CI tree keeps the mirror paths; see
# .buildkite/eval_image_mirror.sh). canary-metrics is built from this repo
# (dockerfiles/canary-metrics.Dockerfile) and has no public registry, so its
# default is a bare tag that must be overridden or side-loaded.
# The image a case names when it needs a revision that can never start (the
# canary capacity-gate failures). It is never side-loaded: case_images() leaves
# it out, so an offline cluster fails to pull it exactly as the case intends.
UNPULLABLE_IMAGE = "example.com/canary-test/missing:none"

DEFAULTS = {
    "OME_TEST_NS": "sandbox",
    "OME_EVAL_STORAGE_CLASS": "shared-vast",
    "OME_EVAL_PVC_ACCESS_MODE": "ReadWriteMany",
    "OME_EVAL_NODE_ARCH": "amd64",
    "OME_EVAL_NGINX_IMAGE": "nginxinc/nginx-unprivileged:1.27-alpine",
    # A second tag of the same image: the runtime-image-bump cases roll onto it.
    "OME_EVAL_NGINX_ALT_IMAGE": "nginxinc/nginx-unprivileged:1.28-alpine",
    "OME_EVAL_METRICS_IMAGE": "canary-metrics:latest",
    "OME_EVAL_PV_HOST_PATH": "/ome-eval-model",
    # Scheduler stamped on every fixture runtime (and on the runtimes the cases
    # embed). Empty renders no schedulerName, so the cluster default places the
    # pods; the scheduler lane sets ome-scheduler and the runner then asserts
    # gang scheduling (invariant D).
    "OME_EVAL_SCHEDULER": "",
}

# KIND has no arch to select for and pulls nothing: its static RWX volume is
# backed by a path mounted into every node, and fixture images are built or
# side-loaded with `kind load` under exactly these names.
KIND_OVERRIDES = {
    "OME_EVAL_STORAGE_CLASS": "ome-eval-shared",
    "OME_EVAL_PVC_ACCESS_MODE": "ReadWriteMany",
    "OME_EVAL_NODE_ARCH": "",
    "OME_EVAL_NGINX_IMAGE": "nginxinc/nginx-unprivileged:1.27-alpine",
    "OME_EVAL_NGINX_ALT_IMAGE": "nginxinc/nginx-unprivileged:1.28-alpine",
    "OME_EVAL_METRICS_IMAGE": "canary-metrics:kind-local",
    "OME_EVAL_SCHEDULER": "",
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


def substitute(text, values):
    """Replace the fixture tokens in `text`; other ${...} expansions survive."""
    for token, value in values.items():
        text = text.replace("${%s}" % token, value)
    return text


def unknown_tokens(text):
    """Fixture-style ${TOKEN} references in `text` that no token resolves."""
    return sorted(set(re.findall(r"\$\{(OME_[A-Z0-9_]+)\}", text)) - set(DEFAULTS))


def render_text(text, values):
    text = substitute(text, values)
    # An empty arch selects nothing; drop the line rather than emit an empty
    # map value, which would make the pod unschedulable on every node.
    if not values["OME_EVAL_NODE_ARCH"]:
        text = "\n".join(
            line for line in text.splitlines()
            if "kubernetes.io/arch" not in line
        ) + "\n"
    # No scheduler selected: drop the emptied schedulerName line so the cluster
    # default applies instead of an empty (invalid) name.
    if not values.get("OME_EVAL_SCHEDULER"):
        text = "\n".join(
            line for line in text.splitlines()
            if line.strip() != "schedulerName:"
        ) + "\n"
    return text


def fixture_paths(stage=STAGE_ALL, kind=False):
    """Return fixture paths in their dependency-safe application order."""
    if stage not in STAGES:
        raise ValueError("unknown fixture stage %r; expected one of %s" %
                         (stage, ", ".join(STAGES)))

    bootstrap_names = set(BOOTSTRAP_FIXTURES)
    kind_names = set(KIND_BOOTSTRAP_FIXTURES)
    bootstrap = []
    if kind:
        bootstrap.extend(RUNTIME_DIR / name for name in KIND_BOOTSTRAP_FIXTURES)
    bootstrap.extend(RUNTIME_DIR / name for name in BOOTSTRAP_FIXTURES)
    resources = [
        path for path in sorted(RUNTIME_DIR.glob("*.yaml"))
        if path.name not in bootstrap_names and path.name not in kind_names
    ]
    if stage == STAGE_BOOTSTRAP:
        return bootstrap
    if stage == STAGE_RESOURCES:
        return resources
    return bootstrap + resources


def render(kind=False, stage=STAGE_ALL):
    """Return the selected fixture stage as one rendered YAML stream."""
    values = resolve(kind=kind)
    docs = [
        render_text(path.read_text(), values)
        for path in fixture_paths(stage, kind=kind)
    ]
    return "\n---\n".join(docs)


def images(kind=False, stage=STAGE_ALL):
    """The container images the rendered fixtures reference, deduplicated.

    A KIND cluster has to side-load these, so the caller needs the resolved
    list rather than a copy of it that can drift from the fixtures.
    """
    seen = []
    for line in render(kind=kind, stage=stage).splitlines():
        stripped = line.strip()
        if stripped.startswith("image:"):
            image = stripped.split(":", 1)[1].strip()
            if image and image not in seen:
                seen.append(image)
    return seen


def case_images(kind=False):
    """Container image references embedded directly in case manifests, as they
    resolve for the target cluster. The unpullable image is not one of them:
    a case that names it wants a pod that never starts."""
    values = resolve(kind=kind)
    seen = []
    for path in sorted(CASE_DIR.glob("**/*.yaml")):
        for line in substitute(path.read_text(), values).splitlines():
            match = re.match(r"^\s*image:\s*[\"']?([^\"'\s]+)", line)
            if match is None:
                match = re.search(r"(?:^|\s)img=([^\s]+)", line)
            if match is None or match.group(1) in seen or not side_loadable(match.group(1), values):
                continue
            seen.append(match.group(1))
    return seen


def side_loadable(image, values):
    """Whether a case image is one the cluster must load from an archive. The
    unpullable image is meant to fail to pull, and the metrics image is built
    from this repository and loaded by the fixture path, so a case that embeds
    either in its own runtime adds nothing to the side-load list."""
    return image != UNPULLABLE_IMAGE and image != values["OME_EVAL_METRICS_IMAGE"]


def resolved_value(token, kind=False):
    """Return one resolved fixture token or raise KeyError if it is unknown."""
    values = resolve(kind=kind)
    if token not in values:
        raise KeyError(token)
    return values[token]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--kind", action="store_true",
                    help="render for a local KIND cluster")
    output = ap.add_mutually_exclusive_group()
    output.add_argument("--images", action="store_true",
                        help="list the referenced images instead of the YAML")
    output.add_argument("--case-images", action="store_true",
                        help="list images embedded directly in case manifests")
    output.add_argument("--value", metavar="TOKEN",
                        help="print one resolved token value")
    ap.add_argument("--stage", choices=STAGES, default=STAGE_ALL,
                    help="render all fixtures or one deployment stage")
    args = ap.parse_args()
    if args.value:
        try:
            value = resolved_value(args.value, kind=args.kind)
        except KeyError:
            ap.error("unknown token %s; expected one of %s" %
                     (args.value, ", ".join(sorted(DEFAULTS))))
        sys.stdout.write(value + "\n")
    elif args.case_images:
        sys.stdout.write("".join(
            image + "\n" for image in case_images(kind=args.kind)
        ))
    elif args.images:
        sys.stdout.write("".join(
            i + "\n" for i in images(kind=args.kind, stage=args.stage)
        ))
    else:
        sys.stdout.write(render(kind=args.kind, stage=args.stage))


if __name__ == "__main__":
    main()
