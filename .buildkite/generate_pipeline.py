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
    AWS_ACCOUNT_ID            – AWS account for ECR (default: 588845226011)
    AWS_REGION                – AWS region for ECR (default: us-east-2)
    ECR_REGISTRY              – full ECR registry override (takes precedence)
"""

from __future__ import annotations

import os
import sys
import textwrap

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "588845226011")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
ECR_REGISTRY = os.environ.get(
    "ECR_REGISTRY", f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com"
)

CI_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/golang:1.25-bookworm"
# Ubuntu-based dind because the official docker:27-dind is alpine and
# BuildKite's agent pre-command hook requires /bin/bash.
DOCKER_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/cruizba/ubuntu-dind:latest"

DEFAULT_QUEUE = "mks-cpu"
BUILD_QUEUE = "mks-cpu"

BRANCH = os.environ.get("BUILDKITE_BRANCH", "")
COMMIT = os.environ.get("BUILDKITE_COMMIT", "HEAD")

# (image-name, Dockerfile path)
IMAGES = [
    ("ome-manager", "dockerfiles/manager.Dockerfile"),
    ("model-agent", "dockerfiles/model-agent.Dockerfile"),
    ("ome-agent", "dockerfiles/ome-agent.Dockerfile"),
    ("multinode-prober", "dockerfiles/multinode-prober.Dockerfile"),
]

CHARTS = ["ome-crd", "ome-resources"]


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
              sh .buildkite/run_tests.sh
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
# CD pipeline (build always; push only when run_*.sh sees BUILDKITE_BRANCH=main)
# ---------------------------------------------------------------------------


# MKS identity annotations: bind the pod to the default cluster identity
# and inject AWS auth tooling so `aws ecr get-login-password` works
# without static credentials. Same pattern as prod_inference's k8s_plugin.
_MKS_IDENTITY_ANNOTATIONS = (
    "                  metadata:\n"
    "                    annotations:\n"
    "                      identity.mks.meta.com/default: \"true\"\n"
    "                      identity.mks.meta.com/inject-auth-tools: \"true\"\n"
)


def _build_image_step(name: str, dockerfile: str) -> str:
    return textwrap.dedent(
        f"""\
          - label: ":docker: Build {name}"
            key: "build-{name}"
            depends_on: "test"
            command: |
              sh .buildkite/build_image.sh {name} {dockerfile}
            agents:
              queue: "{BUILD_QUEUE}"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{DOCKER_IMAGE}"
{_MKS_IDENTITY_ANNOTATIONS}        """
    )


def _push_chart_step(chart: str) -> str:
    return textwrap.dedent(
        f"""\
          - label: ":helm: Package {chart}"
            key: "chart-{chart}"
            depends_on: "test"
            command: |
              sh .buildkite/push_chart.sh {chart}
            agents:
              queue: "{BUILD_QUEUE}"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CI_IMAGE}"
{_MKS_IDENTITY_ANNOTATIONS}        """
    )


def cd_steps() -> str:
    parts: list[str] = []
    for name, dockerfile in IMAGES:
        parts.append(_build_image_step(name, dockerfile))
    for chart in CHARTS:
        parts.append(_push_chart_step(chart))
    return textwrap.indent("\n".join(parts), "  ")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def generate_pipeline() -> str:
    pipeline = ci_steps()
    pipeline += "\n" + cd_steps()
    return pipeline


def main() -> None:
    sys.stdout.write(generate_pipeline())


if __name__ == "__main__":
    main()
