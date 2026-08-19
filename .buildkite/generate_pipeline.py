#!/usr/bin/env python3
"""Generate the Buildkite pipeline for OME.

Image builds and chart packaging always run; the per-step scripts only
push to ECR when BUILDKITE_BRANCH=main, so PR builds validate without
publishing.

Usage:
    python3 .buildkite/generate_pipeline.py | buildkite-agent pipeline upload

Environment variables read at runtime:
    BUILDKITE_BRANCH          – current branch name
    BUILDKITE_COMMIT          – full commit SHA
    BUILDKITE_BUILD_NUMBER    – pipeline-unique build number
    BUILDKITE_PULL_REQUEST    – pull request number, or "false"
    BUILDKITE_PULL_REQUEST_BASE_BRANCH – pull request base branch
    OME_PIPELINE_MODE         – pipeline to generate ("daily" or "standard")
    AWS_ACCOUNT_ID            – dev AWS account for ECR (default: 588845226011)
    PROD_AWS_ACCOUNT_ID       – prod AWS account for ECR (default: 168653207203)
    AWS_REGION                – AWS region for ECR (default: us-east-2)
    ECR_REGISTRY              – full dev ECR registry override (takes precedence)
"""

from __future__ import annotations

import json
import os
import re
import sys
import textwrap
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "588845226011")
# Prod ECR is the cluster-foundation managed registry; cf-replace-image-registry
# admission policy fast-paths images already on this registry, so consumers in
# policy-restricted clusters need the prod-tagged copy.
PROD_AWS_ACCOUNT_ID = os.environ.get("PROD_AWS_ACCOUNT_ID", "168653207203")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
BUILD_NUMBER = os.environ.get("BUILDKITE_BUILD_NUMBER", "local")
ECR_REGISTRY = os.environ.get(
    "ECR_REGISTRY", f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com"
)
PROD_ECR_REGISTRY = f"{PROD_AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com"

CI_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/golang:1.25-bookworm"
MUSE_RUST_CI_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/rust:1.95-bookworm"
GO_BUILDER_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/golang:1.25"
RUST_BUILDER_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/rust:1-bookworm"

# cf-docker-image plugin handles ECR auth + multi-arch builds. The plugin
# is fetched from mslsrc/dev at job-submit time. See prod_inference for
# precedent.
CF_DOCKER_IMAGE_PLUGIN = (
    "https://github.com/mslsrc/dev.git/buildkite/plugins/cf-docker-image#main"
)
# Runtime image for the cf-docker-image plugin's command hook. Needs
# bash + docker CLI + buildx + aws CLI + uv. We reuse prod_inference's
# ci-base since it has all of those, until we build our own.
CF_DOCKER_PLUGIN_RUNTIME_IMAGE = (
    f"{ECR_REGISTRY}/msl_infra/prod-inference-ci-base"
    "@sha256:fd2e3c825cd063a8b4cd3d7440bf00696c9f8a7f8132e5a7d1e38a3df2f542e8"
)
DOCKER_PLUGIN_REF = "https://github.com/buildkite-plugins/docker-buildkite-plugin.git#v5.13.0"

DEFAULT_QUEUE = "tw-cpu-x86"
BUILD_QUEUE = "tw-cpu-x86"
# GKE-backed queue for the sharded KIND suite. Unlike the TW dind, GKE agents
# run on real VMs / privileged dind where kind can create bridge networks
# (writable /proc/sys/net + ip_forward=1). The queue's agents must provide a
# reachable, privileged Docker daemon — see .buildkite/README-gke-kind.md.
# Default targets the live agent-stack-k8s TPU queue, which lands kind jobs on
# its CPU node pool via the per-step kubernetes plugin. Flip GKE_KIND_QUEUE to a
# dedicated GKE CPU queue (e.g. gke-kind-x86) once infra provisions one.
GKE_QUEUE = os.environ.get("GKE_KIND_QUEUE", "gke-tpu-v7x")
REPO_ROOT = Path(__file__).resolve().parents[1]
KIND_SUITE_DIR = REPO_ROOT / "tests/integration/singlecluster/controller/omenative_coordination_kind"
EVAL_CASE_DIR = REPO_ROOT / "hack/omenative-testing/perm/isvcs"

# Each group gets one three-node KIND cluster. Groups balance observed runtime
# while keeping related specs together. Exact ownership makes a new spec fail
# pipeline generation until it is classified.
KIND_SPEC_GROUPS = (
    ("foundation-lifecycle", "Foundation and lifecycle", (
        "baseline_no_coord", "diagnostic", "lifecycle_basic", "lifecycle",
        "single_replica", "regression", "watches",
    )),
    ("status-events-services", "Status, events, and services", (
        "events", "status", "status_surface", "producer_services",
        "producer_traffic", "peer_discovery", "multipod_rank0_service",
        "concurrent_isvcs",
    )),
    ("runtime-retry-recovery", "Runtime, retry, and recovery", (
        "bad_image", "disposition", "retry_block", "runtime_corrective_recovery",
        "runtime_owned_runner_shape", "runtime_resolution",
    )),
    ("pd-topology-isolation", "PD topology and isolation", (
        "pd", "pd_isolation", "pd_three_way", "pd_concurrent", "independent",
    )),
    ("bluegreen-core", "BlueGreen rollouts", (
        "bluegreen", "bluegreen_n3", "bluegreen_inplaceonly_happy",
        "bluegreen_revision_status", "inplace_maxunavailable",
    )),
    ("canary-rollouts", "Canary rollouts", (
        "canary_retarget_rollback",
    )),
    ("canary-lifecycle", "Canary lifecycle", (
        "canary_lifecycle_promote", "canary_lifecycle_pausefreeze",
        "canary_lifecycle_podloss",
    )),
    ("ratio-core", "RatioBalanced coordination", (
        "ratiobalanced", "ratio_n3", "ratio_pause_resume",
    )),
    ("ratio-edges", "RatioBalanced edge states", (
        "ratio_anchor_scaledown", "ratio_late_member_anchor",
    )),
    ("ratio-load", "RatioBalanced load", (
        "loadtest",
    )),
    ("sequential-core", "Sequential coordination", (
        "sequential", "n3_sequential",
    )),
    ("sequential-variants", "Sequential rollout variants", (
        "sequential_n3_engine_first", "sequential_soak", "sequential_inplace",
        "sequential_recreatepod", "runtime_partition_sequential",
    )),
    ("sequential-interleavings", "Sequential rollout interleavings", (
        "sequential_pause_mid", "sequential_scale_during",
    )),
    ("sequential-holds", "Sequential hold and release", (
        "sequential_failure_hold", "sequential_later_only_bump",
    )),
    ("update-core", "Core update strategies", (
        "update", "inplace", "recreate_pod", "mixed_strategy",
        "percomponent_surge",
    )),
    ("percomponent-maxsurge", "Multi-instance maxSurge", (
        "percomponent_maxsurge_multiinstance",
    )),
    ("rolling-updates", "Rolling update strategies", (
        "rolling_recreatepod", "rolling_inplace", "rolling_inplaceonly_reject",
        "rolling_cancel_revert",
    )),
    ("gang-recovery", "Gang recovery and wedge", (
        "gang_corrective_rollout", "gang_wedge_nonready",
    )),
    ("gang-surge-multipod", "Gang surge and multi-pod", (
        "concurrent_gang_surge", "gang_surge_floor", "multipod_inplace",
        "multipod_recreatepod", "multipod_surge_fallback", "topology_podgroup",
    )),
    ("migration-relocation", "Migration and relocation", (
        "auto_migrate", "auto_migrate_multiinstance", "migrate", "interleave_extra",
    )),
    ("migration-lifecycle", "Migration gang lifecycle", (
        "migration_gang",
    )),
    ("pause-cancel-interleavings", "Pause, cancel, and interleavings", (
        "pause", "pause_midrollout", "inference_replica_pause",
        "cancel_midrollout", "interleave",
    )),
    ("scale-transitions", "Scale transitions", (
        "bounded_scale_down", "scale_during_rollout", "bluegreen_scale_down_during",
    )),
    ("scale-transition-boundaries", "Scale transition boundaries", (
        "scale_tr_oversized_gang", "scale_tr_orphan_refusal",
    )),
    ("scale-transition-races", "Scale transition races", (
        "scale_tr_update_race", "scale_tr_drain_rebound",
    )),
    ("deletion-disruption", "Deletion and disruption", (
        "cleanup_lifecycle", "force_delete", "pdb_eviction", "teardown",
        "partial_failure",
    )),
)


def _truthy(val: str) -> bool:
    return val.strip().lower() in ("1", "true", "yes", "on")


# The GKE KIND step runs by default and gates the build. GKE_QUEUE targets a
# live queue so the step does not wait indefinitely for a nonexistent agent.
# Kill switch: set
# GKE_KIND_ENABLED=0 to drop the step entirely (e.g. if the queue goes away).
# See .buildkite/README-gke-kind.md.
GKE_KIND_ENABLED = _truthy(os.environ.get("GKE_KIND_ENABLED", "1"))
GKE_EVAL_ENABLED = _truthy(os.environ.get("GKE_EVAL_ENABLED", "1"))

BRANCH = os.environ.get("BUILDKITE_BRANCH", "")
COMMIT = os.environ.get("BUILDKITE_COMMIT", "HEAD")
PIPELINE_MODE = os.environ.get("OME_PIPELINE_MODE", "").strip().lower()

# (label-suffix, ECR repo name under msl_infra/, Dockerfile)
# Drop redundant "ome-" prefix in ECR names so we don't get "ome-ome-*".
# model-agent is not built locally; we consume upstream-published images.
IMAGES = [
    ("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile"),
    ("ome-agent", "ome-agent", "dockerfiles/ome-agent.Dockerfile"),
    # CPU-only metrics workload for the OMENative canary-analysis e2e tests
    # (hack/omenative-testing). Pulled by runtime/csr-metrics.yaml.
    ("canary-metrics", "canary-metrics", "dockerfiles/canary-metrics.Dockerfile"),
    # MUSE console: one image serving the read-plane API + the built web UI.
    ("muse-server", "muse-server", "dockerfiles/muse-server.Dockerfile"),
    # ome-scheduler: kube-scheduler + OMEGangPack. Separate Go module
    # under scheduler/; pure-Go static binary, no xet.
    ("ome-scheduler", "ome-scheduler", "dockerfiles/ome-scheduler.Dockerfile"),
    # kind-ci: offline toolchain and image archives for the GKE-backed KIND
    # suites, including cert-manager and the OMENative eval runtime fixture.
    # It is built on TW (with egress) and mirrored to the registry reachable
    # from the restricted GKE queue. See dockerfiles/kind-ci.Dockerfile.
    ("kind-ci", "kind-ci", "dockerfiles/kind-ci.Dockerfile"),
]

CHARTS = ["ome-crd", "ome-resources", "ome-isvc", "muse", "ome-scheduler"]


# ---------------------------------------------------------------------------
# Step helpers
# ---------------------------------------------------------------------------


def _yaml_block(obj, indent: int = 0) -> str:
    """Serialize a dict/list/scalar to an indented YAML *block* fragment.

    Lets pipeline snippets be written as plain Python data instead of
    hand-aligned f-strings — easier to read and write, and impossible to
    misalign. Stdlib only (no PyYAML): scalars go through json.dumps, which
    quotes/escapes correctly and stays valid YAML (YAML is a JSON superset).
    Dict/list values render as nested blocks; empty ones render inline ({} / []).
    """
    pad = " " * indent
    if isinstance(obj, dict):
        lines = []
        for k, v in obj.items():
            if isinstance(v, (dict, list)) and v:
                lines.append(f"{pad}{k}:")
                lines.append(_yaml_block(v, indent + 2))
            else:
                lines.append(f"{pad}{k}: {json.dumps(v)}")
        return "\n".join(lines)
    if isinstance(obj, list):
        lines = []
        for item in obj:
            if isinstance(item, (dict, list)) and item:
                sub = _yaml_block(item, indent + 2).split("\n")
                # First child line carries the "- " marker; the rest stay aligned.
                lines.append(f"{pad}- " + sub[0][indent + 2:])
                lines.extend(sub[1:])
            else:
                lines.append(f"{pad}- {json.dumps(item)}")
        return "\n".join(lines)
    return f"{pad}{json.dumps(obj)}"


def _gh_status(key: str, indent: int = 12) -> str:
    """Per-step GitHub commit status block; without it BuildKite publishes only the pipeline rollup."""
    return _yaml_block({"notify": [{"github_commit_status": {"context": key}}]}, indent)


def _retry(automatic: list, indent: int = 12) -> str:
    """The general, reusable `retry.automatic` builder: pass a list of condition
    dicts (plain data), e.g. [{"exit_status": 125, "limit": 2},
    {"signal_reason": "agent_stop", "limit": 2}]. _auto_retry / _infra_retry are
    the named presets below; call _retry directly for any other retry policy.
    """
    return _yaml_block({"retry": {"automatic": automatic}}, indent)


def _auto_retry(limit: int = 2, indent: int = 12) -> str:
    """Preset: retry on ANY non-zero exit — for the network-heavy image build
    (Go module fetch from proxy.golang.org, then ECR push) and mirror (crane
    copy + cross-account auth) steps, which flake on transient blips (e.g. the
    HTTP/2 INTERNAL_ERROR that sank build 2445). A real Dockerfile/compile error
    still fails once the retries are spent, so it masks nothing, and the bound
    stops a genuinely broken build from looping.
    """
    return _retry([{"exit_status": "*", "limit": limit}], indent)


def _infra_retry(limit: int = 2, indent: int = 12) -> str:
    """Preset: retry ONLY on infrastructure failure — the TW dind daemon dying
    (exit_status 125: docker.sock gone mid-run, or the pre-command docker
    health-repair hook failing before the step runs) or the agent being lost
    (signal_reason agent_stop). It never matches exit 1, so a real test/lint/
    -race failure still blocks merge — safe on merge gates precisely because it
    cannot match a code failure. Seen on build 2523 (scheduler-test: dind
    vanished during `go mod download`) and 2548 (helm-lint: pre-command docker
    health failed before the lint ran).
    """
    return _retry(
        [
            {"exit_status": 125, "limit": limit},
            {"signal_reason": "agent_stop", "limit": limit},
        ],
        indent,
    )


def _tw_docker_plugin(
    image: str,
    indent: int = 6,
    extra_volumes: tuple[str, ...] = (),
) -> str:
    """Emit the TW docker plugin block for a command step.

    TW agents run the Buildkite job on the host and use this plugin to launch the
    command in the requested workload image. The identity/cert mounts mirror the
    shared MSL TW helper so cloud_oss, curl, Go, Helm, and crane can authenticate
    from the TW host identity and cert mounts.

    extra_volumes appends host mounts beyond the identity/cert set (e.g. the
    Docker daemon socket for the KIND step). Never set the plugin's network
    option here: the TW agent's docker CLI wrapper
    (fbsource msl_infra/buildkite buildkite-docker-wrapper.sh) already injects
    --network host into every workload container it launches and hard-fails
    `docker network create`, so the plugin's create-if-missing network path
    must stay unused.
    """
    pad = " " * indent
    extra_volume_lines = "".join(f"{pad}      - {v}\n" for v in extra_volumes)
    return (
        f'{pad}- "{DOCKER_PLUGIN_REF}":\n'
        f'{pad}    image: "{image}"\n'
        f"{pad}    mount-buildkite-agent: true\n"
        f"{pad}    propagate-environment: true\n"
        f"{pad}    environment:\n"
        f"{pad}      - GIT_TERMINAL_PROMPT=0\n"
        f"{pad}      - SSL_CERT_FILE=/etc/meta/certs/ca-bundle.crt\n"
        f"{pad}      - SSL_CERT_DIR=/etc/ssl/certs:/etc/meta/certs\n"
        # Bare names pass the value from the TW agent's process env into
        # the container (propagate-environment only carries the Buildkite
        # job env, not agent env). These wire the TW-provisioned AWS
        # credential chain (cloud_oss path + config-file/credential-
        # process + S32M endpoint) through to _ecr_login.sh — mirrors
        # prod_inference's TW CPU plugin config.
        f"{pad}      - CLOUD_OSS_PATH\n"
        f"{pad}      - AWS_CONFIG_FILE\n"
        f"{pad}      - AWS_SHARED_CREDENTIALS_FILE\n"
        f"{pad}      - S32M_ENDPOINT\n"
        f"{pad}      - AWS_ENDPOINT_URL_S3\n"
        f"{pad}      - PATH=/opt/cloud.cli:/opt/cloud.cli/bin:/go/bin:/usr/local/go/bin:/usr/local/cargo/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin\n"
        f"{pad}    volumes:\n"
        f"{pad}      - /etc/pki/tls/certs:/etc/meta/certs:ro\n"
        f"{pad}      - /etc/pki/ca-trust:/etc/pki/ca-trust:ro\n"
        f"{pad}      - /etc/fbwhoami:/etc/fbwhoami:ro\n"
        f"{pad}      - /etc/smc.tiers:/etc/smc.tiers:ro\n"
        f"{pad}      - /var/facebook/rootcanal:/var/facebook/rootcanal:ro\n"
        f"{pad}      - /var/facebook/x509_identities:/var/facebook/x509_identities:ro\n"
        f"{pad}      - /var/facebook/tupperware/tls:/var/facebook/tupperware/tls:ro\n"
        f"{pad}      - /data/buildkite/cloud.cli:/opt/cloud.cli:ro\n"
        f"{extra_volume_lines}"
    )


def _kind_ci_image(tag: str) -> str:
    return f"{PROD_ECR_REGISTRY}/msl_infra/kind-ci:{tag}"


def _gke_kind_plugins(image: str, shared_model: bool = False) -> list[dict]:
    """Agent-stack pod: offline runner plus a privileged Docker sidecar."""
    runner_mounts = []
    dind_mounts = [{"name": "docker-graph", "mountPath": "/var/lib/docker"}]
    volumes = [{"name": "docker-graph", "emptyDir": {}}]
    if shared_model:
        model_mount = {"name": "eval-model", "mountPath": "/ome-eval-model"}
        runner_mounts.append(model_mount)
        dind_mounts.append(model_mount)
        volumes.append({"name": "eval-model", "emptyDir": {}})

    pod_spec = {
        "containers": [
            {
                "name": "container-0",
                "image": image,
                "env": [{"name": "DOCKER_HOST", "value": "tcp://localhost:2375"}],
                "volumeMounts": runner_mounts,
                "resources": {
                    "requests": {"cpu": "3", "memory": "8Gi", "ephemeral-storage": "8Gi"},
                    "limits": {"cpu": "6", "memory": "12Gi", "ephemeral-storage": "16Gi"},
                },
            },
            {
                "name": "dind",
                "image": f"{PROD_ECR_REGISTRY}/pt_dockerio/library/docker:28-dind",
                "securityContext": {"privileged": True},
                "env": [{"name": "DOCKER_TLS_CERTDIR", "value": ""}],
                "volumeMounts": dind_mounts,
                "resources": {
                    "requests": {"cpu": "2", "memory": "6Gi", "ephemeral-storage": "16Gi"},
                    "limits": {"cpu": "4", "memory": "8Gi", "ephemeral-storage": "24Gi"},
                },
            },
        ],
        "volumes": volumes,
    }
    pod_spec["nodeSelector"] = {"kubernetes.io/arch": "amd64"}
    return [{"kubernetes": {"podSpecPatch": pod_spec}}]


def _kind_spec_files() -> list[str]:
    pattern = re.compile(r"ginkgo\.(?:Describe|DescribeTable)\(")
    return [
        path.name
        for path in sorted(KIND_SUITE_DIR.glob("*_kind_test.go"))
        if pattern.search(path.read_text())
    ]


def _kind_spec_groups() -> list[tuple[str, str, list[str]]]:
    """Return the semantic groups after proving exact ownership of every spec."""
    actual = set(_kind_spec_files())
    owners: dict[str, str] = {}
    groups = []
    slugs = set()
    for slug, label, stems in KIND_SPEC_GROUPS:
        if not stems or slug in slugs:
            raise RuntimeError(f"invalid or duplicate KIND spec group: {slug}")
        slugs.add(slug)
        files = [f"{stem}_kind_test.go" for stem in stems]
        for filename in files:
            if filename in owners:
                raise RuntimeError(
                    f"KIND spec {filename} belongs to both {owners[filename]} and {slug}"
                )
            owners[filename] = slug
        groups.append((slug, label, files))

    mapped = set(owners)
    if missing := sorted(actual - mapped):
        raise RuntimeError(f"unclassified KIND specs: {', '.join(missing)}")
    if stale := sorted(mapped - actual):
        raise RuntimeError(f"mapped KIND specs do not exist: {', '.join(stale)}")
    return groups


def _gke_kind_group() -> str:
    """A small set of named semantic jobs, each with its own KIND cluster."""
    plugins = _gke_kind_plugins(
        _kind_ci_image(f"test-{COMMIT[:8]}-{BUILD_NUMBER}")
    )
    children = []
    for slug, label, files in _kind_spec_groups():
        focus = "/(" + "|".join(re.escape(filename) for filename in files) + ")$"
        file_label = "file" if len(files) == 1 else "files"
        children.append(
            {
                "label": f":test_tube: {label} ({len(files)} {file_label})",
                "key": f"kind-gke-{slug}",
                "depends_on": "package-kind-test-image",
                "env": {"OME_KIND_FOCUS": focus},
                "agents": {"queue": GKE_QUEUE},
                "command": "sh .buildkite/run_kind_tests_gke.sh",
                "plugins": plugins,
            }
        )
    group = {
        "group": ":test_tube: Integration Tests (KIND on GKE)",
        "key": "integration-test-kind-gke",
        "notify": [{"github_commit_status": {"context": "integration-test-kind-gke"}}],
        "steps": children,
    }
    return _yaml_block([group], 2)


def _package_kind_test_image_step() -> str:
    """Put the exact Go test bundle in a commit-tagged runner image for GKE."""
    dependencies = '    depends_on: "test"\n'
    if BRANCH == "main":
        dependencies = (
            "    depends_on:\n"
            '      - "test"\n'
            '      - "mirror-kind-ci"\n'
        )
    return (
        '  - label: ":package: Package exact Go KIND runner"\n'
        '    key: "package-kind-test-image"\n'
        f"{dependencies}"
        f"{_auto_retry(indent=4)}\n"
        "    env:\n"
        f'      AWS_REGION: "{AWS_REGION}"\n'
        f'      PROD_ECR_REGISTRY: "{PROD_ECR_REGISTRY}"\n'
        "    secrets:\n"
        '      - "PROD_ECR_AUTH_TOKEN"\n'
        "    agents:\n"
        f'      queue: "{BUILD_QUEUE}"\n'
        "    command: |\n"
        "      sh .buildkite/package_kind_test_image.sh\n"
        "    plugins:\n"
        f"{_tw_docker_plugin(CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=6)}\n"
    )


def _eval_case_groups() -> list[tuple[str, str, list[str]]]:
    """Group every committed eval case by its stable feature/topology axes."""
    groups: dict[tuple[str, str], list[str]] = {}
    seen: set[str] = set()
    for path in sorted(EVAL_CASE_DIR.glob("*/p*.yaml")):
        case_id = path.stem
        if case_id in seen:
            raise RuntimeError(f"duplicate OMENative eval case id: {case_id}")
        seen.add(case_id)
        header = path.read_text().splitlines()[0].lstrip("#").strip()
        fields = dict(token.split("=", 1) for token in header.split() if "=" in token)
        topology = fields.get("topo")
        if not topology:
            raise RuntimeError(f"OMENative eval case has no topo field: {path}")
        groups.setdefault((path.parent.name, topology), []).append(case_id)

    def case_number(case_id: str) -> int:
        return int(case_id.removeprefix("p"))

    return [
        (feature, topology, sorted(cases, key=case_number))
        for (feature, topology), cases in sorted(groups.items())
    ]


def _eval_case_peak_pods(case_id: str) -> int | None:
    """Return the case's declared peak pod count, including runtime workers."""
    matches = sorted(EVAL_CASE_DIR.glob(f"*/{case_id}.yaml"))
    if len(matches) != 1:
        return None
    text = matches[0].read_text()
    header = text.splitlines()[0].lstrip("#").strip()
    fields = dict(token.split("=", 1) for token in header.split() if "=" in token)
    topology = fields.get("topo", "")
    documents = re.split(r"(?m)^---\s*$", text)
    isvc = next(
        (doc for doc in documents if re.search(r"(?m)^kind:\s*InferenceService\s*$", doc)),
        "",
    )
    if not isvc:
        return None

    total = 0
    for component, suffix in (("engine", "e"), ("decoder", "d"), ("router", "r")):
        replicas = int(fields.get(f"ceil_{suffix}", fields.get(f"N{suffix}", "0")))
        if replicas == 0:
            continue
        block_match = re.search(
            rf"(?ms)^  {component}:\s*\n(.*?)(?=^  [A-Za-z][A-Za-z0-9_-]*:\s*(?:\n|$)|\Z)",
            isvc,
        )
        block = block_match.group(1) if block_match else ""
        inline = re.search(r"(?m)^    worker:\s*\{[^}]*\bsize:\s*(\d+)", block)
        nested = re.search(
            r"(?m)^    worker:\s*\n(?:^      .*\n)*?^      size:\s*(\d+)", block
        )
        worker_size = int((inline or nested).group(1)) if (inline or nested) else 0
        if topology.endswith("-mn") and component != "router" and worker_size == 0:
            return None
        total += replicas * (1 + worker_size)
    return total


def _split_eval_cases(
    cases: list[str],
    case_peaks: dict[str, int | None],
    concurrency: int,
    pod_budget: int,
    target_waves: int,
) -> list[list[str]]:
    """Pack cases into feasible scheduler waves, then bound waves per job."""
    if concurrency < 1 or pod_budget < 1 or target_waves < 1:
        raise RuntimeError(
            "OMENative eval concurrency, pod budget, and target waves must be positive"
        )
    if not cases:
        return []

    # batch.py runs an unknown or over-budget case exclusively, so either shape
    # consumes one full pod-capacity wave for planning purposes.
    def wave_weight(case_id: str) -> int:
        peak = case_peaks.get(case_id)
        if peak is None:
            return pod_budget
        return min(max(peak, 0), pod_budget)

    weights = {case_id: wave_weight(case_id) for case_id in cases}
    order = {case_id: index for index, case_id in enumerate(cases)}
    waves: list[list[str]] = []
    pod_loads: list[int] = []
    for case_id in sorted(cases, key=lambda item: (-weights[item], order[item])):
        wave = next(
            (
                index
                for index, members in enumerate(waves)
                if len(members) < concurrency
                and pod_loads[index] + weights[case_id] <= pod_budget
            ),
            None,
        )
        if wave is None:
            wave = len(waves)
            waves.append([])
            pod_loads.append(0)
        waves[wave].append(case_id)
        pod_loads[wave] += weights[case_id]

    shards = [
        [case_id for wave in waves[start : start + target_waves] for case_id in wave]
        for start in range(0, len(waves), target_waves)
    ]
    for shard in shards:
        shard.sort(key=order.get)
    return shards


def _gke_eval_group() -> str:
    """Main-only deployed OMENative eval, split into named feature/topology jobs."""
    plugins = _gke_kind_plugins(_kind_ci_image(f"eval-{COMMIT[:8]}"), shared_model=True)
    configured_pod_budget = (
        os.environ.get("GKE_EVAL_POD_BUDGET")
        or os.environ.get("GKE_EVAL_EXCLUSIVE_PODS")
    )
    concurrency = int(os.environ.get("GKE_EVAL_CASE_CONCURRENCY", "4"))
    default_target_waves = int(os.environ.get("GKE_EVAL_TARGET_WAVES", "4"))
    canary_target_waves = int(os.environ.get("GKE_EVAL_CANARY_TARGET_WAVES", "2"))
    children = []
    for feature, topology, cases in _eval_case_groups():
        slug = f"{feature}-{topology}".replace("_", "-")
        prometheus = "true" if feature in ("canary", "reject") else "false"
        pod_budget = configured_pod_budget or ("35" if feature == "canary" else "64")
        if feature == "canary":
            pod_budget = os.environ.get("GKE_EVAL_CANARY_POD_BUDGET", pod_budget)
        target_waves = (
            canary_target_waves if feature == "canary" else default_target_waves
        )
        subshards = _split_eval_cases(
            cases,
            {case_id: _eval_case_peak_pods(case_id) for case_id in cases},
            concurrency,
            int(pod_budget),
            target_waves,
        )
        for index, shard_cases in enumerate(subshards, start=1):
            part = f" [{index}/{len(subshards)}]" if len(subshards) > 1 else ""
            key_part = f"-{index}-of-{len(subshards)}" if len(subshards) > 1 else ""
            children.append(
                {
                    "label": f":test_tube: {feature} / {topology}{part} ({len(shard_cases)} cases)",
                    "key": f"omenative-e2e-{slug}{key_part}",
                    "depends_on": "package-kind-eval-image",
                    "timeout_in_minutes": 180,
                    "env": {
                        "OME_KIND_PROMETHEUS_ENABLED": prometheus,
                        "OME_EVAL_CASE_CONCURRENCY": str(concurrency),
                        "OME_EVAL_POD_BUDGET": pod_budget,
                        "PROD_ECR_REGISTRY": PROD_ECR_REGISTRY,
                    },
                    "agents": {"queue": GKE_QUEUE},
                    "command": "sh .buildkite/run_omenative_eval_gke.sh "
                    + " ".join([feature, topology, *shard_cases]),
                    "plugins": plugins,
                }
            )
    group = {
        "group": ":test_tube: OMENative YAML E2E (KIND on GKE)",
        "key": "omenative-e2e-kind-gke",
        "notify": [{"github_commit_status": {"context": "omenative-e2e-kind-gke"}}],
        "steps": children,
    }
    return _yaml_block([group], 2)


def _drift_check_step(key: str, label: str, make_target: str, apt_packages: str = "") -> str:
    """Emit a CI step that runs `make <target>` then `git diff --exit-code`.

    `git diff --exit-code` returns non-zero if the make target produced
    any tracked-file changes, which fails the BK step — that's the whole
    point: catches PRs that forgot to regenerate (fmt, tidy, manifests,
    generate, dep-crds) before pushing.

    apt_packages: space-separated list passed to `apt-get install` for
    targets that need extra system tooling (e.g. `make manifests` needs
    `jq` for CRD post-processing). Empty (the common case) skips apt.

    The block is indented for direct concatenation onto `ci_steps()`'s
    dedented output (steps at 2-space indent under `steps:`).
    """
    apt_block = ""
    if apt_packages:
        apt_block = (
            f"      apt-get update\n"
            f"      apt-get install -y --no-install-recommends {apt_packages}\n"
        )
    return (
        f'  - label: "{label}"\n'
        f'    key: "{key}"\n'
        f"{_gh_status(key, indent=4)}\n"
        f"    command: |\n"
        f"{apt_block}"
        f"      make {make_target}\n"
        f"      git diff --exit-code\n"
        f"    agents:\n"
        f'      queue: "{DEFAULT_QUEUE}"\n'
        f"    plugins:\n"
        f"{_tw_docker_plugin(CI_IMAGE)}\n"
    )


# ---------------------------------------------------------------------------
# CI pipeline (runs on every commit / PR)
# ---------------------------------------------------------------------------


def ci_steps() -> str:
    base = textwrap.dedent(
        f"""\
        steps:
          - label: ":github: Git Author Email"
            key: "git-author-email"
{_gh_status("git-author-email")}
{_infra_retry()}
            command: |
              bash .buildkite/check_git_author_email.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":golangci-lint: Lint"
            key: "lint"
{_gh_status("lint")}
{_infra_retry()}
            command: |
              make ci-lint
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":golang: Vet"
            key: "vet"
{_gh_status("vet")}
{_infra_retry()}
            command: |
              make vet
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":test_tube: Unit Tests + Coverage"
            key: "test"
{_gh_status("test")}
{_infra_retry()}
            command: |
              sh .buildkite/run_tests.sh
            artifact_paths:
              - ".buildkite/artifacts/omenative-kind-test.tgz"
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":test_tube: Integration Tests"
            key: "integration-test"
{_gh_status("integration-test")}
{_infra_retry()}
            # No depends_on: integration tests share no state with unit
            # tests (each step bootstraps its own envtest + xet under a
            # fresh container) so they run in parallel with everything
            # else in the CI fan-out.
            command: |
              sh .buildkite/run_integration_tests.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":racing_car: Perf Tests"
            key: "perf-test"
{_gh_status("perf-test")}
            # The InferenceService scale harness (envtest + fake executor)
            # plus the perf-harness unit tests. Separate from integration-test
            # because the perf packages are plain testing.T (not Ginkgo, so
            # they can't take -ginkgo.trace) and the harness is slower. Runs in
            # parallel with everything else; bootstraps its own envtest + xet.
            #
            # soft_fail: the baseline rangespec is still placeholder/uncalibrated
            # (see configs/baseline/rangespec.yaml), so a threshold breach must
            # not block merges yet — the step's value today is producing the
            # numbers + exercising the harness. Flip to hard fail once the
            # rangespec is calibrated on CI hardware.
            soft_fail: true
            command: |
              sh .buildkite/run_perf_tests.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":test_tube: Scheduler Module"
            key: "scheduler-test"
{_gh_status("scheduler-test")}
{_infra_retry()}
            # ome-scheduler (scheduler/go.mod) is a separate Go module, so
            # the root pipeline's `go test ./...` never reaches it. This is
            # the only step that builds + tests the OMEGangPack plugin, its
            # pure topology/placement packages, and the envtest integration
            # harness. Bootstraps its own envtest (1.35, matching the
            # module's k8s pin); runs in parallel with the rest of the fan-out.
            command: |
              sh .buildkite/run_scheduler_tests.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          # The muse-server image build owns web install/check/build in its
          # cached Node stage. Keep this independent step Rust-native.
          - label: ":rust: MUSE Rust Checks"
            key: "muse-rust-checks"
{_gh_status("muse-rust-checks")}
{_infra_retry()}
            command: |
              apt-get update
              apt-get install -y --no-install-recommends build-essential cmake perl pkg-config
              rustup component add rustfmt clippy
              rustc --version
              make -C muse ci-rust
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(MUSE_RUST_CI_IMAGE, indent=14)}

          - label: ":helm: Helm Lint"
            key: "helm-lint"
{_gh_status("helm-lint")}
{_infra_retry()}
            command: |
              make helm-lint
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}
        """
    )
    # Drift checks: each runs a regen target and fails if the working
    # tree differs from HEAD — catches PRs that forgot to regenerate.
    drift_checks = [
        _drift_check_step("fmt-drift", ":broom: Fmt Drift", "fmt"),
        _drift_check_step("tidy-drift", ":package: Tidy Drift", "tidy"),
        # `make manifests` post-processes CRD YAML through `jq`; the
        # default golang:bookworm image doesn't ship it. perl is already
        # present (pulled in by git in the base image).
        _drift_check_step(
            "manifests-drift",
            ":memo: Manifests Drift",
            "manifests",
            apt_packages="jq",
        ),
        _drift_check_step(
            "generate-drift",
            ":arrows_counterclockwise: Generate Drift",
            "generate",
        ),
        _drift_check_step(
            "dep-crds-drift",
            ":truck: Dep CRDs Drift",
            "dep-crds",
        ),
    ]
    steps = base + "\n" + "\n".join(drift_checks)
    if GKE_KIND_ENABLED:
        steps += "\n" + _package_kind_test_image_step()
        steps += "\n" + _gke_kind_group()
    return steps


# ---------------------------------------------------------------------------
# CD pipeline (build always; push only when run_*.sh sees BUILDKITE_BRANCH=main)
# ---------------------------------------------------------------------------


def _build_image_step(label: str, ecr_name: str, dockerfile: str) -> str:
    """Emit a cf-docker-image plugin step that builds + pushes to dev ECR.

    Builds on every branch (validates the Dockerfile); pushes only on
    main via push-branches. Mirror to prod ECR is handled by
    _mirror_image_step which copies the manifest at the registry layer
    — no rebuild, no layer re-upload.
    """
    build_args = _image_build_args(label)
    args_block = ""
    if build_args:
        args_block = "                  args:\n" + "\n".join(
            f'                    - "{arg}"' for arg in build_args
        ) + "\n"

    return textwrap.dedent(
        f"""\
          - label: ":docker: Build {label}"
            key: "build-{label}"
{_gh_status(f"build-{label}")}
            depends_on: "test"
{_auto_retry()}
            agents:
              queue: "{BUILD_QUEUE}"
            env:
              BUILDX_NO_DEFAULT_ATTESTATIONS: "1"
            plugins:
              - "{CF_DOCKER_IMAGE_PLUGIN}":
                  ecr-name: "msl_infra/{ecr_name}"
                  environment: "dev"
                  region: "{AWS_REGION}"
                  dockerfile: "{dockerfile}"
                  build-context: "."
{args_block.rstrip()}
                  platforms: ["linux/amd64", "linux/arm64"]
                  push-branches: "main"
                  tags: ["latest", "{COMMIT[:8]}"]
                  login: true
                  tw-buildkit: true
        """
    )


def _image_build_args(label: str) -> list[str]:
    if label == "muse-server":
        return [f"RUST_BUILDER_IMAGE={RUST_BUILDER_IMAGE}"]
    return [f"GO_BUILDER_IMAGE={GO_BUILDER_IMAGE}"]


def _mirror_image_step(label: str, ecr_name: str) -> str:
    """Copy the dev-ECR image to prod ECR via `crane copy` — pure-registry
    manifest+blob copy, no rebuild and no docker daemon required (the
    ci-base image doesn't ship one). The script downloads crane on first
    run and uses cloud_oss for cross-account ECR auth via the shared
    _ecr_login.sh helper. --only-main skips on PR branches (build step
    only pushes on main, so there's nothing on dev to mirror otherwise).
    """
    short_sha = "${BUILDKITE_COMMIT:0:8}"
    return textwrap.dedent(
        f"""\
          - label: ":docker: Mirror {label} → prod ECR"
            key: "mirror-{label}"
{_gh_status(f"mirror-{label}")}
            depends_on: "build-{label}"
{_auto_retry()}
            # ECR credentials on TW: the agent resolves these Buildkite
            # cluster secrets into the job env, propagate-environment
            # carries them into the container, and _ecr_login.sh consumes
            # them (tier 2). The MKS-era aws-CLI fallback doesn't exist in
            # TW workload containers.
            secrets:
              - "ECR_AUTH_TOKEN"
              - "PROD_ECR_AUTH_TOKEN"
            agents:
              queue: "{BUILD_QUEUE}"
            command: |
              sh .buildkite/mirror_image.sh --only-main msl_infra/{ecr_name} latest "{short_sha}"
            plugins:
{_tw_docker_plugin(CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=14)}
        """
    )


def _package_kind_eval_image_step() -> str:
    """Add exact main runtime-image archives to the offline KIND runner."""
    return textwrap.dedent(
        f"""\
          - label: ":package: Package exact OMENative eval images"
            key: "package-kind-eval-image"
{_gh_status("package-kind-eval-image")}
            depends_on:
              - "mirror-kind-ci"
              - "mirror-ome-manager"
              - "mirror-ome-agent"
              - "mirror-canary-metrics"
{_auto_retry()}
            secrets:
              - "PROD_ECR_AUTH_TOKEN"
            agents:
              queue: "{BUILD_QUEUE}"
            command: |
              sh .buildkite/package_kind_eval_image.sh
            plugins:
{_tw_docker_plugin(CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=14)}
        """
    )


def _push_chart_step(chart: str, depends_on: str | list[str] = "test") -> str:
    if isinstance(depends_on, list):
        deps = "\n".join(f'              - "{d}"' for d in depends_on)
        depends_block = f"depends_on:\n{deps}"
    else:
        depends_block = f'depends_on: "{depends_on}"'
    return textwrap.dedent(
        f"""\
          - label: ":helm: Package {chart}"
            key: "chart-{chart}"
{_gh_status(f"chart-{chart}")}
            {depends_block}
            # Same Buildkite cluster-secret ECR auth as the mirror steps;
            # push_chart.sh logs in to both the dev and prod registries.
            secrets:
              - "ECR_AUTH_TOKEN"
              - "PROD_ECR_AUTH_TOKEN"
            command: |
              sh .buildkite/push_chart.sh {chart}
            agents:
              queue: "{BUILD_QUEUE}"
            plugins:
{_tw_docker_plugin(CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=14)}
        """
    )


def cd_steps() -> str:
    parts: list[str] = []
    # The ome-resources / ome-scheduler / muse charts stamp an image ref (prod-ECR
    # hub + tag) into their values at publish time, so they must wait for both the
    # build (dev push) AND the mirror (prod push) to finish — otherwise a consumer
    # installing the freshly published chart could hit ImagePullBackOff until the
    # mirror catches up.
    image_keys = []
    for label, ecr_name, dockerfile in IMAGES:
        parts.append(_build_image_step(label, ecr_name, dockerfile))
        parts.append(_mirror_image_step(label, ecr_name))
        # kind-ci is CI tooling, not a runtime image any chart stamps, so keep it
        # out of the chart depends_on set — otherwise chart publishing would
        # needlessly block on its (large, slow) build.
        if label == "kind-ci":
            continue
        image_keys.append(f"build-{label}")
        image_keys.append(f"mirror-{label}")
    if BRANCH == "main" and GKE_EVAL_ENABLED:
        parts.append(_package_kind_eval_image_step())
    for chart in CHARTS:
        if chart in ("ome-resources", "ome-scheduler", "muse"):
            # These charts stamp an image ref, so wait for the image build + mirror.
            parts.append(_push_chart_step(chart, depends_on=image_keys))
        else:
            parts.append(_push_chart_step(chart))
    return textwrap.indent("\n".join(parts), "  ")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def generate_pipeline() -> str:
    pipeline = ci_steps()
    pipeline += "\n" + cd_steps()
    if BRANCH == "main" and GKE_EVAL_ENABLED:
        pipeline += "\n" + _gke_eval_group()
    return pipeline


def _priority_prefix() -> str:
    """Return ``"priority: N\\n"`` if ``MSL_BK_PIPELINE_PRIORITY`` is set.

    The value is set by mslsrc/dev's static pipeline (see
    ``pulumi/deployments/buildkite/prod/pipelines.py``) and propagated to
    every step in the build via Buildkite's top-level ``env:`` mechanism.
    Re-emitting it as top-level YAML here makes the dynamically uploaded
    steps inherit the same priority instead of running at the implicit
    Buildkite default 0. Per-step priorities, if any, still win.
    """
    raw = os.environ.get("MSL_BK_PIPELINE_PRIORITY")
    if not raw:
        return ""
    try:
        return f"priority: {int(raw)}\n"
    except ValueError:
        return ""


def main() -> None:
    if PIPELINE_MODE not in ("", "daily", "standard"):
        sys.stderr.write(
            "invalid OME_PIPELINE_MODE: "
            f"{PIPELINE_MODE!r}; expected 'daily' or 'standard'\n"
        )
        raise SystemExit(2)

    if PIPELINE_MODE == "daily":
        # The daily generator imports this module for shared queue and plugin
        # helpers, so load it lazily after this module has defined them.
        import generate_daily_pipeline as daily

        pipeline = daily.generate_pipeline()
    else:
        pipeline = generate_pipeline()

    sys.stdout.write(_priority_prefix() + pipeline)


if __name__ == "__main__":
    main()
