#!/usr/bin/env python3
"""Render an OME GitHub comparison as a bounded Markdown changeset."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

# A bare "#123" autolinks against whichever repository renders the body, so an
# OME commit subject promoted into a GitOps PR would point at the wrong repo.
_ISSUE_REFERENCE = re.compile(r"(?<![0-9A-Za-z/#-])#(\d+)(?![0-9A-Za-z])")


class CompareError(ValueError):
    """The GitHub comparison cannot describe a forward promotion."""


def _markdown_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")


def _link_references(value: str, repository: str) -> str:
    """Bind "#123" to `repository` so it survives rendering in another repo."""
    return _ISSUE_REFERENCE.sub(
        lambda match: (
            f"[#{match.group(1)}]"
            f"(https://github.com/{repository}/pull/{match.group(1)})"
        ),
        value,
    )


def render_changeset(
    comparison: dict[str, Any],
    repository: str,
    base_sha: str,
    head_sha: str,
    limit: int = 50,
) -> str:
    status = comparison.get("status")
    compare_url = comparison.get("html_url")
    if not isinstance(compare_url, str) or not compare_url:
        compare_url = f"https://github.com/{repository}/compare/{base_sha}...{head_sha}"
    if status == "identical":
        return "\n".join(
            [
                "## Changes",
                "",
                (
                    f"[{base_sha[:8]}...{head_sha[:8]}]({compare_url}) contains no "
                    "new commits; this promotion updates release artifacts for the "
                    "same OME revision."
                ),
                "",
            ]
        )
    if status != "ahead":
        raise CompareError(
            "OME candidate must be ahead of the GitOps pin; "
            f"comparison status is {status!r}"
        )

    commits = comparison.get("commits")
    total = comparison.get("total_commits")
    if not isinstance(commits, list) or not isinstance(total, int) or total < 1:
        raise CompareError("GitHub comparison has no commits")
    if limit < 1:
        raise CompareError("changeset limit must be positive")

    shown = min(len(commits), limit)
    lines = [
        "## Changes",
        "",
        (
            f"[{base_sha[:8]}...{head_sha[:8]}]({compare_url}) contains "
            f"{total} commit{'s' if total != 1 else ''}:"
        ),
        "",
    ]
    for item in commits[:shown]:
        if not isinstance(item, dict):
            raise CompareError("GitHub comparison contains an invalid commit")
        sha = item.get("sha")
        url = item.get("html_url")
        commit = item.get("commit")
        message = commit.get("message") if isinstance(commit, dict) else None
        complete = all(
            isinstance(value, str) and value for value in (sha, url, message)
        )
        if not complete:
            raise CompareError("GitHub comparison contains an incomplete commit")
        subject = _link_references(
            _markdown_text(message.splitlines()[0]), repository
        )
        lines.append(f"- [`{sha[:8]}`]({url}) {subject}")

    omitted = total - shown
    if omitted > 0:
        lines.append(
            f"- …and {omitted} more commit{'s' if omitted != 1 else ''}; "
            f"see the [full comparison]({compare_url})."
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--comparison", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    comparison = json.loads(args.comparison.read_text())
    rendered = render_changeset(
        comparison,
        args.repository,
        args.base_sha,
        args.head_sha,
        args.limit,
    )
    args.output.write_text(rendered)


if __name__ == "__main__":
    main()
