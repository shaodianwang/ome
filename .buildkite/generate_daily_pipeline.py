#!/usr/bin/env python3
"""Generate the OME RC daily smoke and promotion pipeline."""

from __future__ import annotations

import json
import os
import re
import sys

import generate_pipeline as common


GKE_QUEUE = os.environ.get("GKE_KIND_QUEUE", "gke-tpu-v7x")
RC_SERVICE_ACCOUNT = os.environ.get("DAILY_RC_SERVICE_ACCOUNT", "ome-rc-smoke")
KIND_CI_IMAGE = os.environ.get(
    "DAILY_RC_IMAGE",
    "168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/kind-ci:latest",
)
KARGO_BUILD = common._truthy(os.environ.get("KARGO_BUILD", ""))
BUILDKITE_COMMIT = os.environ.get("BUILDKITE_COMMIT", "")


def _kargo_commit() -> str:
    if not re.fullmatch(r"[0-9a-f]{8,64}", BUILDKITE_COMMIT):
        raise ValueError(
            "KARGO_BUILD requires BUILDKITE_COMMIT to be an 8-64 character "
            "lowercase hexadecimal commit ID"
        )
    return BUILDKITE_COMMIT[:8]


def _rc_plugin() -> list[dict[str, object]]:
    return [
        {
            "kubernetes": {
                "podSpecPatch": {
                    "serviceAccountName": RC_SERVICE_ACCOUNT,
                    "containers": [
                        {
                            "name": "container-0",
                            "image": KIND_CI_IMAGE,
                            "resources": {
                                "requests": {"cpu": "250m", "memory": "512Mi"},
                                "limits": {"cpu": "1", "memory": "1Gi"},
                            },
                        }
                    ],
                }
            }
        }
    ]


def generate_pipeline() -> str:
    header = "steps:\n"
    discovery = f"""  - label: ":mag: Discover daily OME candidate"
    key: "daily-discovery"
    timeout_in_minutes: 15
    secrets:
      - "PROD_ECR_AUTH_TOKEN"
    agents:
      queue: "{common.DEFAULT_QUEUE}"
    command: |
      sh .buildkite/run_daily_discover_candidate.sh
    plugins:
{common._tw_docker_plugin(common.CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=6)}
"""
    smoke_dependency = '    depends_on: "daily-discovery"\n'
    if KARGO_BUILD:
        commit = _kargo_commit()
        header = f"""env:
  KARGO_BUILD: "true"
  KARGO_BUILD_COMMIT: "{commit}"
steps:
"""
        discovery = ""
        smoke_dependency = ""

    smoke = f"""  - label: ":test_tube: RC CPU smoke"
    key: "daily-rc-smoke"
{smoke_dependency}    timeout_in_minutes: 45
    concurrency: 1
    concurrency_group: "ome/rc-daily-smoke"
    agents:
      queue: "{GKE_QUEUE}"
    command: |
      sh .buildkite/run_daily_rc_smoke.sh
    plugins: {json.dumps(_rc_plugin())}
"""
    tpu_default_scheduler_smoke = f"""  - label: ":chip: RC TPU default-scheduler smoke"
    key: "daily-rc-tpu-smoke"
{smoke_dependency}    timeout_in_minutes: 270
    concurrency: 1
    concurrency_group: "ome/rc-daily-tpu-smoke"
    agents:
      queue: "{GKE_QUEUE}"
    command: |
      sh .buildkite/run_daily_rc_tpu_smoke.sh
    plugins: {json.dumps(_rc_plugin())}
"""
    tpu_ome_scheduler_smoke = f"""  - label: ":chip: RC TPU OME-scheduler smoke"
    key: "daily-rc-tpu-ome-scheduler-smoke"
{smoke_dependency}    timeout_in_minutes: 270
    concurrency: 1
    concurrency_group: "ome/rc-daily-tpu-ome-scheduler-smoke"
    env:
      DAILY_RC_TPU_SMOKE_NAME: "ome-rc-tpu-smoke-ome-scheduler"
      DAILY_RC_TPU_FIXTURE: ".buildkite/fixtures/rc-tpu-smoke-ome-scheduler-isvc.yaml"
      DAILY_RC_TPU_EXPECTED_ENGINE_SCHEDULER: "ome-scheduler"
      DAILY_RC_TPU_METADATA_KEY: "daily-rc-tpu-ome-scheduler-smoke-b64"
    agents:
      queue: "{GKE_QUEUE}"
    command: |
      sh .buildkite/run_daily_rc_tpu_smoke.sh
    plugins: {json.dumps(_rc_plugin())}
"""
    promotion = f"""  - label: ":github: Open GitOps promotion PR"
    key: "daily-gitops-pr"
    depends_on:
      - "daily-rc-smoke"
      - "daily-rc-tpu-smoke"
      - "daily-rc-tpu-ome-scheduler-smoke"
    timeout_in_minutes: 15
    secrets:
      - "GH_TOKEN"
    agents:
      queue: "{common.DEFAULT_QUEUE}"
    command: |
      sh .buildkite/run_daily_gitops_pr.sh
    plugins:
{common._tw_docker_plugin(common.CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=6)}
"""
    if KARGO_BUILD:
        promotion = ""
    return (
        header
        + discovery
        + smoke
        + tpu_default_scheduler_smoke
        + tpu_ome_scheduler_smoke
        + promotion
    )


def main() -> None:
    sys.stdout.write(common._priority_prefix() + generate_pipeline())


if __name__ == "__main__":
    main()
