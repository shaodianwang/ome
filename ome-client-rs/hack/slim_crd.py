#!/usr/bin/env python3
"""Pre-process an OME CRD for kopium codegen.

The OME CRDs inline a full corev1 PodSpec/Container at every component site
(InferenceService engine/decoder/router + their leader/worker; ServingRuntime
engineConfig/decoderConfig/routerConfig). kopium generates a distinct Rust type
for every object in the (flattened) schema with no sharing, so that pod tree is
regenerated dozens of times — ~120k lines, most of it duplicated PodSpec.

This replaces the deep corev1 pod-template fields (by name, recursively) with
opaque `x-kubernetes-preserve-unknown-fields` schemas, so kopium emits them as
`serde_json::Value` instead of the full type tree. OME's own knobs (minReplicas,
autoscaler, supportedModelFormats, ...) keep their generated types — only the
pod internals go opaque.

The real CRD in config/crd/full is untouched; this output is fed to kopium only.
Read-plane consumers don't read pod internals, and the fields stay settable and
readable as JSON.

Usage: slim_crd.py <crd.yaml>   # transformed CRD on stdout
"""
import sys

import yaml

# corev1 PodSpec / Container fields whose schemas are deep and duplicated. These
# are unambiguous Kubernetes pod-spec field names (plus OME's `runner`, a plain
# Container override) — opaquing them by name is safe across the CRD.
OPAQUE = {
    "containers",
    "initContainers",
    "ephemeralContainers",
    "volumes",
    "affinity",
    "runner",
    "securityContext",
    "topologySpreadConstraints",
    "hostAliases",
    "imagePullSecrets",
    "tolerations",
    "readinessGates",
    "schedulingGates",
    "resourceClaims",
}


def opaque(schema: dict) -> dict:
    """An opaque replacement preserving array-ness and the doc comment."""
    out: dict = {"x-kubernetes-preserve-unknown-fields": True}
    if schema.get("type") == "array":
        out["type"] = "array"
        out["items"] = {"type": "object", "x-kubernetes-preserve-unknown-fields": True}
    else:
        out["type"] = "object"
    if "description" in schema:
        out["description"] = schema["description"]
    return out


def walk(node) -> None:
    if isinstance(node, list):
        for item in node:
            walk(item)
        return
    if not isinstance(node, dict):
        return
    props = node.get("properties")
    if isinstance(props, dict):
        for key in list(props.keys()):
            sub = props[key]
            if key in OPAQUE and isinstance(sub, dict):
                props[key] = opaque(sub)
            else:
                walk(sub)
    for key, val in node.items():
        if key != "properties":
            walk(val)


def main() -> None:
    doc = yaml.safe_load(open(sys.argv[1]))
    walk(doc)
    yaml.safe_dump(doc, sys.stdout, sort_keys=False)


if __name__ == "__main__":
    main()
