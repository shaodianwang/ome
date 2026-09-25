#!/usr/bin/env python3
"""Post one message to the OME working-group Slack channel.

Wraps `meta slack.message send`, which reaches Slack only through the Slack
API Proxy: no token is ever on this host, and one invocation posts at most one
message. Standard library only.

  slack_post.py --markdown-file report.md              # Markdown document (headings, tables, code fences)
  slack_post.py --text 'main: b4757 passed (f846db86)' # one mrkdwn line
  some-command | slack_post.py --text-file -           # body from stdin
  slack_post.py --markdown-file r.md --thread-ts 1700000000.000100   # reply in a thread
  slack_post.py --markdown-file r.md --dry-run         # the CLI previews the post without sending
  slack_post.py --markdown-file r.md --print           # show the command; run nothing

Identity: with OME_SLACK_APP_ID set (or --as) the post is authored by that
registered app; otherwise it is authored by the operator's own connected Slack
account. Destination: --channel-id, else OME_SLACK_CHANNEL_ID, else the
working-group channel. Workspace alias: OME_SLACK_WORKSPACE, default "meta".
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

DEFAULT_CHANNEL_ID = "C0C37314X6U"
DEFAULT_WORKSPACE = "meta"

# Slack renders at most this much Markdown in one message; longer reports are
# shared as a file instead.
MARKDOWN_LIMIT = 12000
TEXT_LIMIT = 4000

# The same credential shapes the repository's internal-token check refuses.
SECRET_PATTERNS = (
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"ghp_[A-Za-z0-9]{20,}"),
    re.compile(r"github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"bkua_[A-Za-z0-9]{20,}"),
    re.compile(r"xox[abprs]-[A-Za-z0-9-]{10,}"),
    re.compile(r"sk-[A-Za-z0-9]{32,}"),
    re.compile(r"-----BEGIN (RSA |EC |OPENSSH |PGP |DSA )?PRIVATE KEY"),
)

THREAD_TS = re.compile(r"^\d{10}\.\d{6}$")
CHANNEL_ID = re.compile(r"^[CG][A-Z0-9]{8,}$")
APP_ID = re.compile(r"^A[A-Z0-9]{8,}$")


class PostError(Exception):
    """A refusal made before the CLI runs."""


def read_body(args: argparse.Namespace) -> tuple[str, str]:
    """Return (kind, body) where kind is 'markdown' or 'text'."""
    sources = [s for s in (args.markdown_file, args.text_file, args.text) if s is not None]
    if len(sources) != 1:
        raise PostError("pass exactly one of --markdown-file, --text-file, --text")
    if args.text is not None:
        return "text", args.text
    path = args.markdown_file if args.markdown_file is not None else args.text_file
    kind = "markdown" if args.markdown_file is not None else "text"
    body = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
    return kind, body


def check_body(kind: str, body: str) -> None:
    if not body.strip():
        raise PostError("the message body is empty")
    limit = MARKDOWN_LIMIT if kind == "markdown" else TEXT_LIMIT
    if len(body) > limit:
        raise PostError(
            f"the body is {len(body)} characters; Slack renders at most {limit} for this form. "
            "Post a summary and share the full report with `meta slack.message send --attachment-file`."
        )
    for pattern in SECRET_PATTERNS:
        if pattern.search(body):
            raise PostError(f"the body contains a credential-shaped token ({pattern.pattern}); not posting")


def build_command(
    args: argparse.Namespace, kind: str, body_path: str | None, meta_bin: str
) -> list[str]:
    channel = args.channel_id or os.environ.get("OME_SLACK_CHANNEL_ID") or DEFAULT_CHANNEL_ID
    if not CHANNEL_ID.match(channel):
        raise PostError(f"--channel-id must be a Slack conversation ID, got {channel!r}")
    app = args.as_app or os.environ.get("OME_SLACK_APP_ID") or ""
    if app and not APP_ID.match(app):
        raise PostError(f"the app ID must look like A0XXXXXXXXX, got {app!r}")
    if args.require_app and not app:
        raise PostError("OME_SLACK_APP_ID is unset and --require-app was given; not posting as a person")
    if (args.username or args.icon_emoji) and not app:
        raise PostError("--username and --icon-emoji need an app identity (OME_SLACK_APP_ID or --as)")
    if args.thread_ts and not THREAD_TS.match(args.thread_ts):
        raise PostError(f"--thread-ts must look like 1700000000.000100, got {args.thread_ts!r}")

    cmd = [meta_bin, "slack.message", "send", f"--channel-id={channel}"]
    if kind == "markdown":
        cmd.append(f"--markdown-text=file://{body_path}")
    elif body_path is not None:
        cmd.append(f"--text=file://{body_path}")
    else:
        cmd.append(f"--text={args.text}")
    workspace = args.workspace or os.environ.get("OME_SLACK_WORKSPACE") or DEFAULT_WORKSPACE
    cmd.append(f"--workspace={workspace}")
    if app:
        cmd.append(f"--as={app}")
    if args.username:
        cmd.append(f"--username={args.username}")
    if args.icon_emoji:
        cmd.append(f"--icon-emoji={args.icon_emoji}")
    if args.thread_ts:
        cmd.append(f"--thread-ts={args.thread_ts}")
    if args.context:
        cmd.append(f"--context={args.context}")
    for button in args.button or []:
        cmd.append(f"--button={button}")
    if args.dry_run:
        cmd.append("--dry-run")
    return cmd


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--markdown-file", help="Markdown body from this file, or - for stdin")
    parser.add_argument("--text-file", help="mrkdwn body from this file, or - for stdin")
    parser.add_argument("--text", help="one short mrkdwn line")
    parser.add_argument("--channel-id", help="Slack conversation ID (default: OME_SLACK_CHANNEL_ID or the working-group channel)")
    parser.add_argument("--as", dest="as_app", help="registered app ID to post as (default: OME_SLACK_APP_ID; unset posts as you)")
    parser.add_argument("--require-app", action="store_true", help="refuse to post as a person")
    parser.add_argument("--workspace", help="workspace alias (default: OME_SLACK_WORKSPACE or meta)")
    parser.add_argument("--thread-ts", help="reply in the thread of this message timestamp")
    parser.add_argument("--context", help="small grey context line under the message")
    parser.add_argument("--button", action="append", help="link button as 'label|url'; repeatable")
    parser.add_argument("--username", help="sender name shown on the message (app identity only)")
    parser.add_argument("--icon-emoji", help="sender icon shortcode (app identity only)")
    parser.add_argument("--dry-run", action="store_true", help="ask the CLI to preview without posting")
    parser.add_argument("--print", dest="print_only", action="store_true", help="print the command and exit")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        kind, body = read_body(args)
        check_body(kind, body)
        meta_bin = shutil.which("meta") or ""
        if not meta_bin and not args.print_only:
            raise PostError("the meta CLI is not on PATH")
        # Caller-authored content travels as a file, never as an argv value.
        needs_file = kind == "markdown" or args.text is None
        with tempfile.NamedTemporaryFile("w", suffix=".md" if kind == "markdown" else ".txt",
                                         prefix="slack-post-", delete=False, encoding="utf-8") as fh:
            body_path = fh.name
            if needs_file:
                fh.write(body)
        try:
            cmd = build_command(args, kind, body_path if needs_file else None, meta_bin or "meta")
            if args.print_only:
                print(" ".join(cmd))
                return 0
            result = subprocess.run(cmd, check=False)
            return result.returncode
        finally:
            Path(body_path).unlink(missing_ok=True)
    except PostError as err:
        print(f"slack_post: {err}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
