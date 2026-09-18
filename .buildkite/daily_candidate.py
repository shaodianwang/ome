#!/usr/bin/env python3
"""Discover the newest complete OME release in the production OCI registry."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Iterable
from pathlib import Path


CHARTS = ("ome-crd", "ome-resources", "ome-scheduler", "ome-quota-manager")
IMAGES = ("ome-manager", "ome-agent", "ome-scheduler", "ome-quota-manager")
# Keep chart-version structure separate from revision validation. Chart
# packaging has emitted both a bare revision and Git describe's ``g``-prefixed
# spelling, and the abbreviation length is not part of the version contract.
VERSION_RE = re.compile(
    r"""
    ^
    (?P<version>(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*))
    -(?P<timestamp>\d{12})
    \.(?P<revision>[0-9A-Za-z-]+)
    $
    """,
    re.VERBOSE,
)
SHA_RE = re.compile(r"^[0-9a-f]{7,64}$")


def parse_chart_version(version: str) -> tuple[str, str] | None:
    """Return the build timestamp and normalized Git object ID."""

    match = VERSION_RE.fullmatch(version)
    if not match:
        return None
    revision = match.group("revision")
    if revision.startswith("g"):
        revision = revision[1:]
    if not SHA_RE.fullmatch(revision):
        return None
    return match.group("timestamp"), revision


def _run(*args: str) -> str:
    result = subprocess.run(args, check=True, capture_output=True, text=True)
    return result.stdout


def _list_tags(repository: str) -> list[str]:
    return [
        tag.strip()
        for tag in _run("crane", "ls", repository).splitlines()
        if tag.strip()
    ]


def _image_exists(reference: str) -> bool:
    result = subprocess.run(
        ("crane", "manifest", reference),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def select_candidate(
    chart_tags: dict[str, Iterable[str]],
    image_exists: Callable[[str], bool],
    registry: str,
    image_prefix: str,
) -> dict[str, object]:
    """Return the newest SHA with all charts and runtime images present."""

    versions: dict[str, dict[str, tuple[str, str]]] = {}
    for chart in CHARTS:
        by_sha: dict[str, tuple[str, str]] = {}
        for tag in chart_tags.get(chart, ()):
            parsed = parse_chart_version(tag)
            if not parsed:
                print(
                    f"ignoring unrecognized {chart} chart tag: {tag}",
                    file=sys.stderr,
                )
                continue
            timestamp, sha = parsed
            if sha not in by_sha or timestamp > by_sha[sha][0]:
                by_sha[sha] = (timestamp, tag)
        versions[chart] = by_sha

    common_shas = set.intersection(*(set(versions[chart]) for chart in CHARTS))
    ordered_shas = sorted(
        common_shas,
        key=lambda sha: max(versions[chart][sha][0] for chart in CHARTS),
        reverse=True,
    )

    for sha in ordered_shas:
        image_refs = {
            image: f"{registry}/{image_prefix}/{image}:{sha}" for image in IMAGES
        }
        if not all(image_exists(reference) for reference in image_refs.values()):
            continue
        chart_versions = {chart: versions[chart][sha][1] for chart in CHARTS}
        return {
            "short_sha": sha,
            "charts": chart_versions,
            "images": image_refs,
        }

    raise RuntimeError("no complete OME release has all required charts and images")


def discover(registry: str, chart_prefix: str, image_prefix: str) -> dict[str, object]:
    chart_tags = {
        chart: _list_tags(f"{registry}/{chart_prefix}/{chart}") for chart in CHARTS
    }
    candidate = select_candidate(chart_tags, _image_exists, registry, image_prefix)
    candidate["registry"] = registry
    candidate["discovered_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    return candidate


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    registry = os.environ.get(
        "PROD_ECR_REGISTRY",
        "168653207203.dkr.ecr.us-east-2.amazonaws.com",
    )
    chart_prefix = os.environ.get("OME_CHART_REPOSITORY_PREFIX", "msl_infra/ome-charts")
    image_prefix = os.environ.get("OME_IMAGE_REPOSITORY_PREFIX", "msl_infra")

    candidate = discover(registry, chart_prefix, image_prefix)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(candidate, indent=2, sort_keys=True) + "\n")
    print(json.dumps(candidate, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
