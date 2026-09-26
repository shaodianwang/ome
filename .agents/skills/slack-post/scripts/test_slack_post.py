"""Command construction and refusals of slack_post.py. Runs nothing against Slack.

  python3 -m unittest .agents/skills/slack-post/scripts/test_slack_post.py
"""

from __future__ import annotations

import os
import unittest
from unittest import mock

import slack_post


def parse(*argv: str):
    return slack_post.parse_args(list(argv))


class BuildCommandTest(unittest.TestCase):
    def setUp(self) -> None:
        self.env = mock.patch.dict(os.environ, {}, clear=False)
        self.env.start()
        for key in ("OME_SLACK_BOT", "OME_SLACK_CHANNEL_ID", "OME_SLACK_WORKSPACE"):
            os.environ.pop(key, None)

    def tearDown(self) -> None:
        self.env.stop()

    def test_markdown_as_crewmate_from_env(self) -> None:
        os.environ["OME_SLACK_BOT"] = "1555509462542873"
        args = parse("--markdown-file", "r.md")
        cmd = slack_post.build_command(args, "markdown", "/tmp/x.md", "meta")
        self.assertEqual(cmd[:5], ["meta", "--act-as-crewmate", "1555509462542873", "slack.message", "send"])
        self.assertIn(f"--channel-id={slack_post.DEFAULT_CHANNEL_ID}", cmd)
        self.assertIn("--markdown-text=file:///tmp/x.md", cmd)
        self.assertIn("--workspace=meta", cmd)
        self.assertFalse(any(part.startswith("--as=") for part in cmd))

    def test_crewmate_handle_selects_the_crewmate_lane(self) -> None:
        args = parse("--text", "x", "--as", "cm-msl-ome-bot")
        cmd = slack_post.build_command(args, "text", None, "meta")
        self.assertEqual(cmd[1:3], ["--act-as-crewmate", "cm-msl-ome-bot"])

    def test_app_id_selects_the_app_lane(self) -> None:
        args = parse("--text", "x", "--as", "A0TESTAPP01")
        cmd = slack_post.build_command(args, "text", None, "meta")
        self.assertEqual(cmd[:3], ["meta", "slack.message", "send"])
        self.assertIn("--as=A0TESTAPP01", cmd)

    def test_text_as_self_without_app(self) -> None:
        args = parse("--text", "main: b1 passed")
        cmd = slack_post.build_command(args, "text", None, "meta")
        self.assertIn("--text=main: b1 passed", cmd)
        self.assertFalse(any(part.startswith("--as=") for part in cmd))

    def test_require_app_refuses_person(self) -> None:
        args = parse("--text", "x", "--require-app")
        with self.assertRaises(slack_post.PostError):
            slack_post.build_command(args, "text", None, "meta")

    def test_persona_needs_app(self) -> None:
        args = parse("--text", "x", "--username", "OME")
        with self.assertRaises(slack_post.PostError):
            slack_post.build_command(args, "text", None, "meta")

    def test_thread_dry_run_and_buttons(self) -> None:
        args = parse("--text", "x", "--thread-ts", "1700000000.000100", "--dry-run",
                     "--button", "Build|https://example.com/b/1", "--context", "via ci-triage")
        cmd = slack_post.build_command(args, "text", None, "meta")
        self.assertIn("--thread-ts=1700000000.000100", cmd)
        self.assertIn("--dry-run", cmd)
        self.assertIn("--button=Build|https://example.com/b/1", cmd)
        self.assertIn("--context=via ci-triage", cmd)

    def test_bad_ids_refused(self) -> None:
        with self.assertRaises(slack_post.PostError):
            slack_post.build_command(parse("--text", "x", "--channel-id", "general"), "text", None, "meta")
        with self.assertRaises(slack_post.PostError):
            slack_post.build_command(parse("--text", "x", "--as", "bot"), "text", None, "meta")
        with self.assertRaises(slack_post.PostError):
            slack_post.build_command(parse("--text", "x", "--thread-ts", "now"), "text", None, "meta")


class BodyChecksTest(unittest.TestCase):
    def test_empty_refused(self) -> None:
        with self.assertRaises(slack_post.PostError):
            slack_post.check_body("text", "   \n")

    def test_secret_refused(self) -> None:
        with self.assertRaises(slack_post.PostError):
            slack_post.check_body("text", "token bkua_" + "a" * 24)

    def test_too_long_refused(self) -> None:
        with self.assertRaises(slack_post.PostError):
            slack_post.check_body("markdown", "x" * (slack_post.MARKDOWN_LIMIT + 1))

    def test_exactly_one_source(self) -> None:
        with self.assertRaises(slack_post.PostError):
            slack_post.read_body(parse("--text", "a", "--markdown-file", "b.md"))


if __name__ == "__main__":
    unittest.main()
