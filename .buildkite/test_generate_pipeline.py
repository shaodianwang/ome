"""Unit tests for generate_pipeline.py's KIND spec classification.

Every ``*_kind_test.go`` spec must be owned by exactly one group in
``KIND_SPEC_GROUPS``; pipeline generation rejects unowned, doubly-owned,
and stale entries. These tests pin that enforcement plus the emitted
shard steps, so a classification mistake fails the unit-test lane with a
clear message instead of only surfacing as a whole-build upload failure.

Run from the repo root (stdlib only; pytest also collects it):

    python3 -m unittest discover -s .buildkite -p 'test_*.py'
    python3 -m pytest .buildkite/test_generate_pipeline.py
"""

from __future__ import annotations

import importlib.util
import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_MODULE_PATH = Path(__file__).resolve().parent / "generate_pipeline.py"
_spec = importlib.util.spec_from_file_location("generate_pipeline_under_test", _MODULE_PATH)
gp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gp)

# Minimal file bodies: discovery counts a file only if it declares a
# ginkgo suite entry, so helpers sharing the suffix stay unowned.
SPEC_BODY = 'package kind\n\nvar _ = ginkgo.Describe("suite", func() {})\n'
TABLE_BODY = 'package kind\n\nvar _ = ginkgo.DescribeTable("cases", func() {})\n'
HELPER_BODY = "package kind\n\nfunc sharedHelper() {}\n"


class PipelineTestCase(unittest.TestCase):
    """Shared helpers to stage a fake suite dir and patch the generator."""

    def make_suite_dir(self, spec_stems, extra_files=()):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        for stem in spec_stems:
            (root / f"{stem}_kind_test.go").write_text(SPEC_BODY)
        for name, body in extra_files:
            (root / name).write_text(body)
        return root

    def patch_generator(self, suite_dir, groups):
        for attr, value in (("KIND_SUITE_DIR", suite_dir), ("KIND_SPEC_GROUPS", groups)):
            patcher = mock.patch.object(gp, attr, value)
            patcher.start()
            self.addCleanup(patcher.stop)


class TestKindSpecDiscovery(PipelineTestCase):
    def test_only_suite_entry_files_are_discovered(self):
        suite_dir = self.make_suite_dir(
            ["alpha", "beta"],
            extra_files=[
                ("helpers_test.go", HELPER_BODY),
                # Shares the classified-file suffix but declares no suite
                # entry, so it must stay invisible to classification.
                ("shared_fixtures_kind_test.go", HELPER_BODY),
            ],
        )
        self.patch_generator(suite_dir, ())
        self.assertEqual(
            gp._kind_spec_files(), ["alpha_kind_test.go", "beta_kind_test.go"]
        )

    def test_describe_table_counts_as_suite_entry(self):
        suite_dir = self.make_suite_dir(
            ["alpha"], extra_files=[("tabled_kind_test.go", TABLE_BODY)]
        )
        self.patch_generator(suite_dir, ())
        self.assertEqual(
            gp._kind_spec_files(), ["alpha_kind_test.go", "tabled_kind_test.go"]
        )


class TestKindSpecClassification(PipelineTestCase):
    def test_unclassified_spec_fails_generation(self):
        suite_dir = self.make_suite_dir(["alpha", "orphan"])
        self.patch_generator(suite_dir, (("group-a", "Group A", ("alpha",)),))
        with self.assertRaisesRegex(
            RuntimeError, r"unclassified KIND specs: orphan_kind_test\.go"
        ):
            gp._kind_spec_groups()

    def test_spec_owned_by_two_groups_fails_generation(self):
        suite_dir = self.make_suite_dir(["alpha", "beta"])
        self.patch_generator(
            suite_dir,
            (
                ("group-a", "Group A", ("alpha", "beta")),
                ("group-b", "Group B", ("beta",)),
            ),
        )
        with self.assertRaisesRegex(
            RuntimeError,
            r"beta_kind_test\.go belongs to both group-a and group-b",
        ):
            gp._kind_spec_groups()

    def test_duplicate_group_slug_fails_generation(self):
        suite_dir = self.make_suite_dir(["alpha", "beta"])
        self.patch_generator(
            suite_dir,
            (
                ("group-a", "Group A", ("alpha",)),
                ("group-a", "Group A again", ("beta",)),
            ),
        )
        with self.assertRaisesRegex(
            RuntimeError, r"invalid or duplicate KIND spec group: group-a"
        ):
            gp._kind_spec_groups()

    def test_empty_group_fails_generation(self):
        suite_dir = self.make_suite_dir(["alpha"])
        self.patch_generator(
            suite_dir,
            (
                ("group-a", "Group A", ("alpha",)),
                ("group-empty", "Empty group", ()),
            ),
        )
        with self.assertRaisesRegex(
            RuntimeError, r"invalid or duplicate KIND spec group: group-empty"
        ):
            gp._kind_spec_groups()

    def test_mapped_spec_without_file_fails_generation(self):
        suite_dir = self.make_suite_dir(["alpha"])
        self.patch_generator(
            suite_dir, (("group-a", "Group A", ("alpha", "ghost")),)
        )
        with self.assertRaisesRegex(
            RuntimeError, r"mapped KIND specs do not exist: ghost_kind_test\.go"
        ):
            gp._kind_spec_groups()

    def test_groups_keep_declared_order_and_expand_filenames(self):
        suite_dir = self.make_suite_dir(["alpha", "beta", "gamma"])
        self.patch_generator(
            suite_dir,
            (
                ("group-b", "Group B", ("beta",)),
                ("group-a", "Group A", ("gamma", "alpha")),
            ),
        )
        self.assertEqual(
            gp._kind_spec_groups(),
            [
                ("group-b", "Group B", ["beta_kind_test.go"]),
                ("group-a", "Group A", ["gamma_kind_test.go", "alpha_kind_test.go"]),
            ],
        )


def _focus_patterns(yaml_text):
    """Extract each shard's OME_KIND_FOCUS value in emission order."""
    values = re.findall(r'OME_KIND_FOCUS: (".*")', yaml_text)
    return [json.loads(value) for value in values]


class TestGkeKindGroupEmission(PipelineTestCase):
    def setUp(self):
        suite_dir = self.make_suite_dir(["alpha", "beta", "gamma"])
        self.patch_generator(
            suite_dir,
            (
                ("pair", "Pair of specs", ("alpha", "gamma")),
                ("solo", "Single spec", ("beta",)),
            ),
        )
        self.yaml_text = gp._gke_kind_group()

    def test_emits_one_gated_step_per_group(self):
        self.assertIn('key: "integration-test-kind-gke"', self.yaml_text)
        self.assertIn('context: "integration-test-kind-gke"', self.yaml_text)
        self.assertIn('key: "kind-gke-pair"', self.yaml_text)
        self.assertIn('key: "kind-gke-solo"', self.yaml_text)
        self.assertEqual(
            self.yaml_text.count('depends_on: "package-kind-test-image"'), 2
        )

    def test_labels_carry_spec_file_counts(self):
        self.assertIn("Pair of specs (2 files)", self.yaml_text)
        self.assertIn("Single spec (1 file)", self.yaml_text)

    def test_focus_selects_exactly_the_owned_files(self):
        patterns = _focus_patterns(self.yaml_text)
        self.assertEqual(len(patterns), 2)
        ownership = {
            0: {"alpha_kind_test.go", "gamma_kind_test.go"},
            1: {"beta_kind_test.go"},
        }
        all_specs = ["alpha_kind_test.go", "beta_kind_test.go", "gamma_kind_test.go"]
        for index, pattern in enumerate(patterns):
            regex = re.compile(pattern)
            matched = {
                name
                for name in all_specs
                if regex.search(f"/repo/tests/{name}")
            }
            self.assertEqual(matched, ownership[index])
            # Anchored at end-of-path: a shard must never pick up a spec
            # whose filename merely embeds an owned stem.
            self.assertIsNone(regex.search("/repo/tests/alpha_kind_test.go.bak"))


class TestCheckedInTreeClassification(unittest.TestCase):
    """The committed spec tree must always generate cleanly."""

    def test_checked_in_specs_are_fully_classified(self):
        groups = gp._kind_spec_groups()
        mapped = [name for _, _, files in groups for name in files]
        self.assertEqual(sorted(mapped), gp._kind_spec_files())

    def test_checked_in_focus_patterns_partition_the_suite(self):
        patterns = _focus_patterns(gp._gke_kind_group())
        groups = gp._kind_spec_groups()
        self.assertEqual(len(patterns), len(groups))
        for name in gp._kind_spec_files():
            owners = [
                slug
                for (slug, _, _), pattern in zip(groups, patterns)
                if re.search(pattern, str(gp.KIND_SUITE_DIR / name))
            ]
            self.assertEqual(
                len(owners), 1, f"{name} matched by {owners or 'no shard'}"
            )

    def test_checked_in_eval_cases_group_cleanly(self):
        groups = gp._eval_case_groups()
        self.assertTrue(groups)
        case_ids = [case for _, _, cases in groups for case in cases]
        self.assertEqual(len(case_ids), len(set(case_ids)))


if __name__ == "__main__":
    unittest.main()
