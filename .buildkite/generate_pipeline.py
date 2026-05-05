#!/usr/bin/env python3
"""Generate the Buildkite pipeline for OME.

Two modes:
  - PR / branch push:  lint + test to validate the commit
  - Merge to main:     build images, package Helm charts, push to ECR

Usage:
    bash .buildkite/upload_pipeline.sh

Environment variables read at runtime:
    BUILDKITE_BRANCH          – current branch name
    BUILDKITE_COMMIT          – full commit SHA
    AWS_ACCOUNT_ID            – AWS account for ECR (default: 588845226011)
    AWS_REGION                – AWS region for ECR (default: us-east-2)
    ECR_REGISTRY              – full ECR registry override (takes precedence)
"""

from __future__ import annotations

import os
import sys
import textwrap
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "588845226011")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
ECR_REGISTRY = os.environ.get(
    "ECR_REGISTRY", f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com"
)

CI_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/golang:1.25-bookworm"
DOCKER_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/docker:27-dind"

DEFAULT_QUEUE = "mks-cpu"
BUILD_QUEUE = "mks-cpu"

BRANCH = os.environ.get("BUILDKITE_BRANCH", "")
COMMIT = os.environ.get("BUILDKITE_COMMIT", "HEAD")
SHORT_SHA = COMMIT[:8] if len(COMMIT) >= 8 else COMMIT
DATE_TAG = datetime.now(timezone.utc).strftime("%Y%m%d")
IMAGE_TAG = f"{DATE_TAG}-{SHORT_SHA}"

CHART_REPO = f"oci://{ECR_REGISTRY}/ome/charts"
IMAGE_REPO = f"{ECR_REGISTRY}/ome"

IMAGES = [
    ("ome-manager", "dockerfiles/manager.Dockerfile", "./cmd/manager"),
    ("model-agent", "dockerfiles/model-agent.Dockerfile", "./cmd/model-agent"),
    ("ome-agent", "dockerfiles/ome-agent.Dockerfile", "./cmd/ome-agent"),
    ("multinode-prober", "dockerfiles/multinode-prober.Dockerfile", "./cmd/multinode-prober"),
]

CHARTS = ["ome-crd", "ome-resources"]


def is_main_branch() -> bool:
    return BRANCH == "main"


# ---------------------------------------------------------------------------
# CI pipeline (runs on every commit / PR)
# ---------------------------------------------------------------------------


def ci_steps() -> str:
    return textwrap.dedent(
        f"""\
        steps:
          - label: ":golangci-lint: Lint"
            key: "lint"
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

          - label: ":test_tube: Unit Tests"
            key: "test"
            command: |
              set -eu
              # The Buildkite k8s plugin runs commands under /bin/sh (dash on
              # bookworm), so avoid bashisms like `pipefail`.
              #
              # System deps:
              #   - jq: required by `make manifests` CRD post-processing.
              #   - libssl-dev / pkg-config / build-essential: needed to link libxet.
              #   - curl + ca-certificates: bootstraps rustup.
              apt-get update
              apt-get install -y --no-install-recommends jq libssl-dev pkg-config build-essential curl ca-certificates
              # Rust toolchain for `pkg/xet` (CGo wrapper around a Rust staticlib).
              # cmd/manager and cmd/model-agent transitively link libxet, so the
              # "no-xet" target still requires it at link time today. Tracked for
              # follow-up: extract the model controller out of cmd/manager so the
              # manager binary no longer depends on libxet.
              curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
              sh /tmp/rustup.sh -y --default-toolchain stable --profile minimal
              # Buildkite's k8s plugin injects its own PATH that omits the
              # golang image's /usr/local/go/bin and /go/bin, so re-add them
              # explicitly alongside cargo.
              export PATH="$HOME/.cargo/bin:/usr/local/go/bin:/go/bin:$PATH"
              # Forward envtest's kube-apiserver/etcd stdout+stderr into the
              # test log so we can see why the control plane fails to start.
              export KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT=true
              make xet-build
              # The mks-cpu pod's external interface is IPv6. kube-apiserver
              # auto-detects it as the external host, then refuses to start
              # because the IPv6 host doesn't match the IPv4 service-cluster
              # IP range envtest passes (10.0.0.0/24). Wrap the binary with a
              # shim that pins --advertise-address to 127.0.0.1 so the
              # auto-detection is skipped.
              ./bin/setup-envtest use 1.30
              ENVTEST_ASSETS="$(./bin/setup-envtest use 1.30 -p path)"
              if [ ! -f "$ENVTEST_ASSETS/kube-apiserver.real" ]; then
                mv "$ENVTEST_ASSETS/kube-apiserver" "$ENVTEST_ASSETS/kube-apiserver.real"
                cat > "$ENVTEST_ASSETS/kube-apiserver" <<'WRAP'
              #!/bin/sh
              exec "$(dirname "$0")/kube-apiserver.real" --advertise-address=127.0.0.1 "$@"
              WRAP
                chmod +x "$ENVTEST_ASSETS/kube-apiserver"
              fi
              make test-no-xet
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


# ---------------------------------------------------------------------------
# CD pipeline (runs on merge to main)
# ---------------------------------------------------------------------------


def _build_image_step(name: str, dockerfile: str, context: str) -> str:
    full_tag = f"{IMAGE_REPO}/{name}:{IMAGE_TAG}"
    latest_tag = f"{IMAGE_REPO}/{name}:latest"
    return textwrap.dedent(
        f"""\
          - label: ":docker: Build & Push {name}"
            key: "build-{name}"
            depends_on: "test"
            command: |
              aws ecr get-login-password --region {AWS_REGION} | docker login --username AWS --password-stdin {ECR_REGISTRY}
              docker build \\
                --build-arg GIT_TAG={IMAGE_TAG} \\
                --build-arg GIT_COMMIT={COMMIT} \\
                -f {dockerfile} \\
                -t {full_tag} \\
                -t {latest_tag} \\
                .
              docker push {full_tag}
              docker push {latest_tag}
            agents:
              queue: "{BUILD_QUEUE}"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{DOCKER_IMAGE}"
        """
    )


def _push_chart_step(chart: str) -> str:
    return textwrap.dedent(
        f"""\
          - label: ":helm: Package & Push {chart}"
            key: "chart-{chart}"
            depends_on: "test"
            command: |
              aws ecr get-login-password --region {AWS_REGION} | helm registry login --username AWS --password-stdin {ECR_REGISTRY}
              sed -i 's/^version:.*/version: {IMAGE_TAG}/' charts/{chart}/Chart.yaml
              sed -i 's/^appVersion:.*/appVersion: "{IMAGE_TAG}"/' charts/{chart}/Chart.yaml
              helm package charts/{chart}
              helm push {chart}-{IMAGE_TAG}.tgz {CHART_REPO}
            agents:
              queue: "{BUILD_QUEUE}"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CI_IMAGE}"
        """
    )


def cd_steps() -> str:
    parts: list[str] = []
    for name, dockerfile, context in IMAGES:
        parts.append(_build_image_step(name, dockerfile, context))
    for chart in CHARTS:
        parts.append(_push_chart_step(chart))
    return textwrap.indent("\n".join(parts), "  ")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def generate_pipeline() -> str:
    pipeline = ci_steps()
    if is_main_branch():
        pipeline += "\n" + cd_steps()
    return pipeline


def main() -> None:
    sys.stdout.write(generate_pipeline())


if __name__ == "__main__":
    main()
