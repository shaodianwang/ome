#!/usr/bin/env python3
"""Generate the OME RC daily smoke and promotion pipeline."""

from __future__ import annotations

import json
import os
import sys

import generate_pipeline as common


GKE_QUEUE = os.environ.get("GKE_KIND_QUEUE", "gke-tpu-v7x")
RC_SERVICE_ACCOUNT = os.environ.get("DAILY_RC_SERVICE_ACCOUNT", "ome-rc-smoke")
KIND_CI_IMAGE = os.environ.get(
    "DAILY_RC_IMAGE",
    "168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/kind-ci:latest",
)


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
    discovery = f"""steps:
  - label: ":mag: Discover daily OME candidate"
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
    smoke = f"""  - label: ":test_tube: RC CPU smoke"
    key: "daily-rc-smoke"
    depends_on: "daily-discovery"
    timeout_in_minutes: 45
    concurrency: 1
    concurrency_group: "ome/rc-daily-smoke"
    agents:
      queue: "{GKE_QUEUE}"
    command: |
      sh .buildkite/run_daily_rc_smoke.sh
    plugins: {json.dumps(_rc_plugin())}
"""
    promotion = f"""  - label: ":github: Open GitOps promotion PR"
    key: "daily-gitops-pr"
    depends_on: "daily-rc-smoke"
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
    return discovery + smoke + promotion


def main() -> None:
    sys.stdout.write(common._priority_prefix() + generate_pipeline())


if __name__ == "__main__":
    main()
