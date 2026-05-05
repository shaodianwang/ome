#!/usr/bin/env python3
"""Generate the Buildkite pipeline for OME.

Two modes:
  - PR / branch push:  lint + test to validate the commit
  - Merge to main:     build images, package Helm charts, push to ECR

Usage:
    cd .buildkite && uv run python generate_pipeline.py | buildkite-agent pipeline upload

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
