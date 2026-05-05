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

# cf-docker-image plugin handles ECR auth + multi-arch builds. The plugin
# is fetched from mslsrc/dev at job-submit time. See prod_inference for
# precedent.
CF_DOCKER_IMAGE_PLUGIN = (
    "https://github.com/mslsrc/dev.git/buildkite/plugins/cf-docker-image#main"
)

DEFAULT_QUEUE = "mks-cpu"
BUILD_QUEUE = "mks-cpu"

BRANCH = os.environ.get("BUILDKITE_BRANCH", "")
COMMIT = os.environ.get("BUILDKITE_COMMIT", "HEAD")

# (label-suffix, ECR repo name under msl_infra/, Dockerfile)
# Drop redundant "ome-" prefix in ECR names so we don't get "ome-ome-*".
IMAGES = [
    ("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile"),
    ("model-agent", "ome-model-agent", "dockerfiles/model-agent.Dockerfile"),
    ("ome-agent", "ome-agent", "dockerfiles/ome-agent.Dockerfile"),
    ("multinode-prober", "ome-multinode-prober", "dockerfiles/multinode-prober.Dockerfile"),
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


def _build_image_step(label: str, ecr_name: str, dockerfile: str) -> str:
    """Emit a cf-docker-image plugin step.

    Builds on every branch (validates the Dockerfile); pushes only on
    main via push-branches. The plugin handles ECR auth + tagging.
    """
    return textwrap.dedent(
        f"""\
          - label: ":docker: Build {label}"
            key: "build-{label}"
            depends_on: "test"
            agents:
              queue: "{BUILD_QUEUE}"
            plugins:
              - kubernetes:
                  podSpec:
                    containers:
                      - name: container-0
                        image: "{CI_IMAGE}"
              - "{CF_DOCKER_IMAGE_PLUGIN}":
                  ecr-name: "msl_infra/{ecr_name}"
                  environment: "dev"
                  region: "{AWS_REGION}"
                  dockerfile: "{dockerfile}"
                  build-context: "."
                  platforms: ["linux/amd64"]
                  push-branches: "main"
                  tags: ["latest"]
                  login: true
        """
    )


def _push_chart_step(chart: str) -> str:
    # TODO: helm OCI push to ECR needs the same AWS auth path the
    # cf-docker-image plugin sets up; fix in a follow-up.
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
        """
    )


def cd_steps() -> str:
    parts: list[str] = []
    for label, ecr_name, dockerfile in IMAGES:
        parts.append(_build_image_step(label, ecr_name, dockerfile))
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
