#!/usr/bin/env python3
"""Generate the Buildkite pipeline for OME.

Usage:
    cd .buildkite && uv run python generate_pipeline.py | buildkite-agent pipeline upload
"""

from __future__ import annotations

import sys
import textwrap

CI_IMAGE = (
    "588845226011.dkr.ecr.us-east-2.amazonaws.com"
    "/pt_dockerio/astral/uv:python3.12-bookworm"
)
DEFAULT_QUEUE = "mks-cpu"


def generate_pipeline() -> str:
    return textwrap.dedent(
        f"""\
        steps:
          - label: ":buildkite: OME pipeline skeleton"
            key: "pipeline-skeleton"
            command: "echo 'TODO: add OME CI steps'"
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


def main() -> None:
    sys.stdout.write(generate_pipeline())


if __name__ == "__main__":
    main()
