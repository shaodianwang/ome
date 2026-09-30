import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import check_spelling  # noqa: E402


SAMPLE = """\
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v5.0.0
    hooks:
      - id: end-of-file-fixer
  - repo: https://github.com/codespell-project/codespell
    rev: v2.4.1
    hooks:
      - id: codespell
        args: ['-L', 'AfterAll,notin']
        exclude: |
          (?x)^(
            charts/.*/templates/.*\\.yaml$|
            .*go\\.sum$
          )$

  - repo: local
    hooks:
      - id: other
"""


class HookSettingsTest(unittest.TestCase):
    def test_reads_version_args_and_exclude(self):
        version, args, exclude = check_spelling.hook_settings(SAMPLE)
        self.assertEqual(version, "2.4.1")
        self.assertEqual(args, ["-L", "AfterAll,notin"])
        pattern = re.compile(exclude)
        self.assertTrue(pattern.search("charts/ome-crd/templates/ome.io_basemodels.yaml"))
        self.assertTrue(pattern.search("scheduler/go.sum"))
        self.assertFalse(pattern.search("pkg/render/model.go"))

    def test_repository_config_parses(self):
        version, args, exclude = check_spelling.hook_settings(check_spelling.CONFIG.read_text())
        self.assertRegex(version, r"^\d+\.\d+\.\d+$")
        self.assertIn("-L", args)
        self.assertTrue(re.compile(exclude).search("config/crd/full/ome.io_inferenceservices.yaml"))

    def test_missing_hook_fails(self):
        with self.assertRaises(SystemExit):
            check_spelling.hook_settings("repos: []\n")


if __name__ == "__main__":
    unittest.main()
