#!/usr/bin/env python3
"""Generate one of OME's Buildkite pipelines.

Each pipeline name maps to a separate BuildKite pipeline that's wired to
the GitHub repo independently, so each shows as its own commit-status
check on PRs and runs in parallel.

Usage:
    bash .buildkite/upload_pipeline.sh <pipeline>

Pipelines:
    ci      lint + vet + unit tests + helm-lint   (BuildKite pipeline: ome)
    images  cf-docker-image plugin builds         (BuildKite pipeline: ome-images)

Add new pipelines (e.g. integration tests) by adding a function below
and registering it in PIPELINES.

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
IMAGES = [
    ("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile"),
    ("model-agent", "ome-model-agent", "dockerfiles/model-agent.Dockerfile"),
    ("ome-agent", "ome-agent", "dockerfiles/ome-agent.Dockerfile"),
    ("multinode-prober", "ome-multinode-prober", "dockerfiles/multinode-prober.Dockerfile"),
]


# ---------------------------------------------------------------------------
# CI pipeline (BuildKite pipeline: ome)
# ---------------------------------------------------------------------------


def ci_pipeline() -> str:
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
# Images pipeline (BuildKite pipeline: ome-images)
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
            agents:
              queue: "{BUILD_QUEUE}"
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
                  platforms: ["linux/amd64"]
                  push-branches: "main"
                  tags: ["latest"]
                  login: true
        """
    )


def images_pipeline() -> str:
    parts = [_build_image_step(*row) for row in IMAGES]
    return "steps:\n" + "\n".join(parts)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


PIPELINES = {
    "ci": ci_pipeline,
    "images": images_pipeline,
}


def main() -> None:
    name = sys.argv[1] if len(sys.argv) > 1 else "ci"
    if name not in PIPELINES:
        valid = ", ".join(sorted(PIPELINES))
        sys.stderr.write(f"unknown pipeline {name!r}; valid: {valid}\n")
        sys.exit(2)
    sys.stdout.write(PIPELINES[name]())


if __name__ == "__main__":
    main()
