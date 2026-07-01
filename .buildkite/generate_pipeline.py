#!/usr/bin/env python3
"""Generate the Buildkite pipeline for OME.

Image builds and chart packaging always run; the per-step scripts only
push to ECR when BUILDKITE_BRANCH=main, so PR builds validate without
publishing.

Usage:
    bash .buildkite/upload_pipeline.sh

Environment variables read at runtime:
    BUILDKITE_BRANCH          – current branch name
    BUILDKITE_COMMIT          – full commit SHA
    AWS_ACCOUNT_ID            – dev AWS account for ECR (default: 588845226011)
    PROD_AWS_ACCOUNT_ID       – prod AWS account for ECR (default: 168653207203)
    AWS_REGION                – AWS region for ECR (default: us-east-2)
    ECR_REGISTRY              – full dev ECR registry override (takes precedence)
"""

from __future__ import annotations

import os
import sys
import textwrap

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "588845226011")
# Prod ECR is the cluster-foundation managed registry; cf-replace-image-registry
# admission policy fast-paths images already on this registry, so consumers in
# policy-restricted clusters need the prod-tagged copy.
PROD_AWS_ACCOUNT_ID = os.environ.get("PROD_AWS_ACCOUNT_ID", "168653207203")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
ECR_REGISTRY = os.environ.get(
    "ECR_REGISTRY", f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com"
)
PROD_ECR_REGISTRY = f"{PROD_AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com"

CI_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/golang:1.25-bookworm"

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

DEFAULT_QUEUE = "mks-cpu"
BUILD_QUEUE = "mks-cpu"

BRANCH = os.environ.get("BUILDKITE_BRANCH", "")
COMMIT = os.environ.get("BUILDKITE_COMMIT", "HEAD")

# (label-suffix, ECR repo name under msl_infra/, Dockerfile)
# Drop redundant "ome-" prefix in ECR names so we don't get "ome-ome-*".
# model-agent is not built locally; we consume upstream-published images.
IMAGES = [
    ("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile"),
    ("ome-agent", "ome-agent", "dockerfiles/ome-agent.Dockerfile"),
    # CPU-only metrics workload for the OMENative canary-analysis e2e tests
    # (hack/omenative-testing). Pulled by runtime/csr-metrics.yaml.
    ("canary-metrics", "canary-metrics", "dockerfiles/canary-metrics.Dockerfile"),
    # MUSE console: read-plane API (Rust) + web UI (nginx). Independent of the
    # OME operator images; built from muse/ + ome-client-rs/.
    ("muse-server", "muse-server", "dockerfiles/muse-server.Dockerfile"),
    ("muse-web", "muse-web", "dockerfiles/muse-web.Dockerfile"),
]

CHARTS = ["ome-crd", "ome-resources", "ome-isvc"]


# ---------------------------------------------------------------------------
# Step helpers
# ---------------------------------------------------------------------------


def _gh_status(key: str, indent: int = 12) -> str:
    """Per-step GitHub commit status block; without it BuildKite publishes only the pipeline rollup."""
    pad = " " * indent
    return (
        f"{pad}notify:\n"
        f"{pad}  - github_commit_status:\n"
        f'{pad}      context: "{key}"'
    )


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
        f"      - kubernetes:\n"
        f"          podSpec:\n"
        f"            containers:\n"
        f"              - name: container-0\n"
        f'                image: "{CI_IMAGE}"\n'
    )


# ---------------------------------------------------------------------------
# CI pipeline (runs on every commit / PR)
# ---------------------------------------------------------------------------


def ci_steps() -> str:
    base = textwrap.dedent(
        f"""\
        steps:
          - label: ":golangci-lint: Lint"
            key: "lint"
{_gh_status("lint")}
            command: |
              make ci-lint
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CI_IMAGE}"

          - label: ":golang: Vet"
            key: "vet"
{_gh_status("vet")}
            command: |
              make vet
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CI_IMAGE}"

          - label: ":test_tube: Unit Tests + Coverage"
            key: "test"
{_gh_status("test")}
            command: |
              sh .buildkite/run_tests.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CI_IMAGE}"

          - label: ":test_tube: Integration Tests"
            key: "integration-test"
{_gh_status("integration-test")}
            # No depends_on: integration tests share no state with unit
            # tests (each step bootstraps its own envtest + xet under a
            # fresh container) so they run in parallel with everything
            # else in the CI fan-out.
            command: |
              sh .buildkite/run_integration_tests.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CI_IMAGE}"

          - label: ":test_tube: Integration Tests (KIND)"
            key: "integration-test-kind"
{_gh_status("integration-test-kind")}
            # KIND-backed suites under
            # tests/integration/singlecluster/controller/omenative_coordination_kind/
            # exercise OEP-0007.{{1,2}} + OEP-0011 contracts against real
            # kubelet + scheduler + containerd that the envtest sibling's
            # PodKubeletSimulator can't model (drain races, in-place
            # patches, Sequential handoff timing, RatioBalanced gate at
            # scale). ~5 min wall-clock, 24 specs.
            #
            # Independent from integration-test — they run in parallel.
            # Requires Docker on the agent host; run_kind_tests.sh bails
            # out with a clear error if `docker info` fails.
            #
            # soft_fail: mks-cpu Kubernetes-pod agents don't expose a
            # Docker daemon, so the step currently bails out at the
            # `docker info` check. Keeping it non-blocking so the signal
            # is visible (and turns green once we either move this step
            # to a DinD/sysbox-capable queue or stand up a Docker-on-pod
            # sidecar). Flip back to a hard fail once infra supports it.
            soft_fail: true
            command: |
              sh .buildkite/run_kind_tests.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CI_IMAGE}"

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
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CI_IMAGE}"

          - label: ":helm: Helm Lint"
            key: "helm-lint"
{_gh_status("helm-lint")}
            command: |
              make helm-lint
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CI_IMAGE}"
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
    return base + "\n" + "\n".join(drift_checks)


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
    return textwrap.dedent(
        f"""\
          - label: ":docker: Build {label}"
            key: "build-{label}"
{_gh_status(f"build-{label}")}
            depends_on: "test"
            agents:
              queue: "{BUILD_QUEUE}"
            env:
              BUILDX_NO_DEFAULT_ATTESTATIONS: "1"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CF_DOCKER_PLUGIN_RUNTIME_IMAGE}"
              - "{CF_DOCKER_IMAGE_PLUGIN}":
                  ecr-name: "msl_infra/{ecr_name}"
                  environment: "dev"
                  region: "{AWS_REGION}"
                  dockerfile: "{dockerfile}"
                  build-context: "."
                  platforms: ["linux/amd64", "linux/arm64"]
                  push-branches: "main"
                  tags: ["latest", "{COMMIT[:8]}"]
                  login: true
        """
    )


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
            agents:
              queue: "{BUILD_QUEUE}"
            command: |
              sh .buildkite/mirror_image.sh --only-main msl_infra/{ecr_name} latest "{short_sha}"
            plugins:
              - kubernetes:
                  metadata:
                    annotations:
                      identity.mks.meta.com/default: "true"
                      identity.mks.meta.com/inject-auth-tools: "true"
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CF_DOCKER_PLUGIN_RUNTIME_IMAGE}"
        """
    )


def _push_chart_step(chart: str, depends_on: str | list[str] = "test") -> str:
    # MKS identity annotations bind the pod to the cluster's default
    # IAM role so `aws ecr get-login-password` works without static
    # creds; same path the cf-docker-image plugin uses for image push.
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
            command: |
              sh .buildkite/push_chart.sh {chart}
            agents:
              queue: "{BUILD_QUEUE}"
            plugins:
              - kubernetes:
                  metadata:
                    annotations:
                      identity.mks.meta.com/default: "true"
                      identity.mks.meta.com/inject-auth-tools: "true"
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CF_DOCKER_PLUGIN_RUNTIME_IMAGE}"
        """
    )


def cd_steps() -> str:
    parts: list[str] = []
    # ome-resources chart's default `hub` points at the prod ECR, so it must
    # wait for both the build (dev push) AND the mirror (prod push) to finish
    # — otherwise a consumer installing the freshly published chart could
    # hit ImagePullBackOff until the mirror catches up.
    image_keys = []
    for label, ecr_name, dockerfile in IMAGES:
        parts.append(_build_image_step(label, ecr_name, dockerfile))
        parts.append(_mirror_image_step(label, ecr_name))
        image_keys.append(f"build-{label}")
        image_keys.append(f"mirror-{label}")
    for chart in CHARTS:
        if chart == "ome-resources":
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
    sys.stdout.write(_priority_prefix() + generate_pipeline())


if __name__ == "__main__":
    main()
