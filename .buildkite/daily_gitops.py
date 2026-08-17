#!/usr/bin/env python3
"""Pin an RC-tested OME candidate in the two production GKE overrides."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from daily_candidate import CHARTS, IMAGES, SHA_RE, parse_chart_version


TARGETS = (
    Path("cluster/workload/msl-gcp-usc1-1-tpu/boostrap-override/values.yaml"),
    Path("cluster/workload/msl-gcp-usc1-2-tpu/boostrap-override/values.yaml"),
)


class CandidateError(ValueError):
    """The discovery payload does not describe one complete release."""


def validate_candidate(candidate: dict[str, Any]) -> tuple[str, dict[str, str]]:
    short_sha = candidate.get("short_sha")
    if not isinstance(short_sha, str) or not SHA_RE.fullmatch(short_sha):
        raise CandidateError(
            "candidate short_sha must be 7 to 64 lowercase hex characters"
        )

    charts = candidate.get("charts")
    if not isinstance(charts, dict):
        raise CandidateError("candidate charts must be an object")
    versions: dict[str, str] = {}
    for chart in CHARTS:
        version = charts.get(chart)
        parsed = parse_chart_version(version) if isinstance(version, str) else None
        if not parsed or parsed[1] != short_sha:
            raise CandidateError(f"candidate chart {chart} does not match {short_sha}")
        versions[chart] = version

    images = candidate.get("images")
    if not isinstance(images, dict):
        raise CandidateError("candidate images must be an object")
    for image in IMAGES:
        reference = images.get(image)
        if not isinstance(reference, str) or not reference.endswith(f":{short_sha}"):
            raise CandidateError(f"candidate image {image} does not match {short_sha}")

    return short_sha, versions


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _block_end(lines: list[str], key_index: int) -> int:
    key_indent = _indent(lines[key_index])
    for index in range(key_index + 1, len(lines)):
        stripped = lines[index].strip()
        if not stripped or stripped.startswith("#"):
            continue
        if _indent(lines[index]) <= key_indent:
            return index
    return len(lines)


def _find_key(
    lines: list[str], start: int, end: int, indent: int, key: str, mapping: bool
) -> int | None:
    suffix = r"\s*(?:#.*)?$" if mapping else r"(?:\s|$)"
    pattern = re.compile(rf"^\s{{{indent}}}{re.escape(key)}:{suffix}")
    for index in range(start, end):
        if pattern.match(lines[index].rstrip("\r\n")):
            return index
    return None


def _ensure_mapping(lines: list[str], path: tuple[str, ...]) -> int:
    start, end, parent_indent = 0, len(lines), -2
    key_index = -1
    for key in path:
        key_indent = parent_indent + 2
        key_index = _find_key(lines, start, end, key_indent, key, mapping=True)
        if key_index is None:
            key_index = end
            lines.insert(key_index, f"{' ' * key_indent}{key}:\n")
        start = key_index + 1
        end = _block_end(lines, key_index)
        parent_indent = key_indent
    return key_index


def _set_scalar(
    lines: list[str], path: tuple[str, ...], value: str, comment: str | None = None
) -> None:
    parent_index = _ensure_mapping(lines, path[:-1])
    start, end = parent_index + 1, _block_end(lines, parent_index)
    key_indent = _indent(lines[parent_index]) + 2
    key = path[-1]
    key_index = _find_key(lines, start, end, key_indent, key, mapping=False)
    rendered_comment = f"  # {comment}" if comment else ""
    rendered = f"{' ' * key_indent}{key}: {json.dumps(value)}{rendered_comment}\n"
    if key_index is None:
        # Put new pins directly below their parent. App blocks often have a
        # comment introducing the following app at the end of the current
        # block; appending there would separate that comment from its key.
        lines.insert(start, rendered)
    else:
        lines[key_index] = rendered


def pin_text(text: str, candidate: dict[str, Any]) -> str:
    short_sha, versions = validate_candidate(candidate)
    lines = text.splitlines(keepends=True)
    if lines and not lines[-1].endswith(("\n", "\r")):
        lines[-1] += "\n"

    for chart in CHARTS:
        _set_scalar(lines, ("apps", chart, "version"), versions[chart])
    promotion_comment = "promoted by the OME RC daily smoke"
    _set_scalar(
        lines,
        ("apps", "ome-resources", "values", "ome", "controller", "tag"),
        short_sha,
        promotion_comment,
    )
    _set_scalar(
        lines,
        ("apps", "ome-resources", "values", "ome", "omeAgent", "tag"),
        short_sha,
        promotion_comment,
    )
    return "".join(lines)


def update_repo(repo_root: Path, candidate: dict[str, Any]) -> list[Path]:
    validate_candidate(candidate)
    changed: list[Path] = []
    for relative_path in TARGETS:
        path = repo_root / relative_path
        if not path.is_file():
            raise FileNotFoundError(f"GitOps target does not exist: {relative_path}")
        before = path.read_text()
        after = pin_text(before, candidate)
        if after != before:
            path.write_text(after)
            changed.append(relative_path)
    return changed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--repo-root", required=True, type=Path)
    args = parser.parse_args()

    candidate = json.loads(args.candidate.read_text())
    changed = update_repo(args.repo_root, candidate)
    for path in changed:
        print(path)


if __name__ == "__main__":
    main()
