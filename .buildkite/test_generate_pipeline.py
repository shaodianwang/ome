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
import os
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


class TestImageBuildEmission(unittest.TestCase):
    def test_wrapper_and_generated_build_job_have_automatic_retries(self):
        yaml_text = gp._build_image_step(
            "ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile"
        )
        self.assertIn(
            '  retry:\n'
            '    automatic:\n'
            '      - exit_status: "*"\n'
            '        limit: 2',
            yaml_text,
        )
        plugin_config = yaml_text.split(gp.CF_DOCKER_IMAGE_PLUGIN, 1)[1]
        self.assertIn(
            '        retry:\n'
            '          automatic:\n'
            '            - exit_status: "*"\n'
            '              limit: 2',
            plugin_config,
        )


class TestEvalSubsharding(unittest.TestCase):
    def test_worker_and_pod_limits_each_create_waves(self):
        worker_limited = [f"p{i:02d}" for i in range(1, 10)]
        worker_shards = gp._split_eval_cases(
            worker_limited,
            {case_id: 1 for case_id in worker_limited},
            concurrency=4,
            pod_budget=100,
            target_waves=2,
        )
        self.assertEqual(
            [len(shard) for shard in worker_shards],
            [8, 1],
        )

        pod_limited = [f"p{i:02d}" for i in range(1, 5)]
        pod_shards = gp._split_eval_cases(
            pod_limited,
            {case_id: 30 for case_id in pod_limited},
            concurrency=4,
            pod_budget=35,
            target_waves=2,
        )
        self.assertEqual(
            [len(shard) for shard in pod_shards],
            [2, 2],
        )

    def test_unknown_and_over_budget_cases_take_exclusive_waves(self):
        cases = ["p01", "p02", "p03", "p04", "p05"]
        shards = gp._split_eval_cases(
            cases,
            {"p01": None, "p02": 100, "p03": 35, "p04": None, "p05": 80},
            concurrency=4,
            pod_budget=35,
            target_waves=2,
        )
        self.assertEqual([len(shard) for shard in shards], [2, 2, 1])
        self.assertEqual(
            sorted(case_id for shard in shards for case_id in shard), cases
        )

    def test_assignment_is_deterministic_and_complete(self):
        cases = ["p01", "p02", "p03", "p04", "p05", "p06"]
        peaks = {
            "p01": 21,
            "p02": 9,
            "p03": 13,
            "p04": 21,
            "p05": 9,
            "p06": 13,
        }
        first = gp._split_eval_cases(cases, peaks, 4, 35, 2)
        second = gp._split_eval_cases(cases, peaks, 4, 35, 2)
        self.assertEqual(first, second)
        self.assertEqual(
            sorted(case_id for shard in first for case_id in shard), cases
        )

    def test_peak_pods_includes_multinode_workers(self):
        with tempfile.TemporaryDirectory() as tmp:
            case_dir = Path(tmp) / "canary"
            case_dir.mkdir()
            (case_dir / "p01.yaml").write_text(
                """# p01: fixture  Ne=2 Nd=0 Nr=0 ceil_e=3 topo=engine-mn
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata: { name: p01 }
spec:
  engine:
    worker: { size: 2 }
"""
            )
            with mock.patch.object(gp, "EVAL_CASE_DIR", Path(tmp)):
                self.assertEqual(gp._eval_case_peak_pods("p01"), 9)


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

    def test_checked_in_eval_subshards_are_complete_and_bounded(self):
        assignments = {}
        job_count = 0
        shard_counts = {}
        for feature, topology, cases in gp._eval_case_groups():
            pod_budget = 35 if feature == "canary" else 64
            target_waves = 2 if feature == "canary" else 4
            shards = gp._split_eval_cases(
                cases,
                {case_id: gp._eval_case_peak_pods(case_id) for case_id in cases},
                concurrency=4,
                pod_budget=pod_budget,
                target_waves=target_waves,
            )
            shard_counts[(feature, topology)] = len(shards)
            job_count += len(shards)
            for shard_index, shard in enumerate(shards):
                self.assertTrue(shard)
                waves = gp._split_eval_cases(
                    shard,
                    {case_id: gp._eval_case_peak_pods(case_id) for case_id in shard},
                    concurrency=4,
                    pod_budget=pod_budget,
                    target_waves=1,
                )
                self.assertLessEqual(len(waves), target_waves)
                for case_id in shard:
                    self.assertNotIn(case_id, assignments)
                    assignments[case_id] = (feature, topology, shard_index)

        committed = {
            case_id
            for _, _, cases in gp._eval_case_groups()
            for case_id in cases
        }
        self.assertEqual(set(assignments), committed)
        self.assertEqual(shard_counts[("canary", "engine")], 7)
        self.assertEqual(shard_counts[("canary", "pd")], 6)
        self.assertEqual(job_count, 62)

    def test_eval_group_emits_unique_subshard_keys_and_readable_labels(self):
        with mock.patch.dict(
            os.environ,
            {
                "GKE_EVAL_CASE_CONCURRENCY": "4",
                "GKE_EVAL_TARGET_WAVES": "4",
                "GKE_EVAL_CANARY_TARGET_WAVES": "2",
            },
        ):
            yaml_text = gp._gke_eval_group()
        keys = re.findall(r'key: "(omenative-e2e-[^"]+)"', yaml_text)
        self.assertEqual(len(keys), len(set(keys)))
        self.assertIn("canary / engine [1/7]", yaml_text)
        self.assertIn("canary / pd [6/6]", yaml_text)
        commands = re.findall(
            r'command: "sh \.buildkite/run_omenative_eval_gke\.sh ([^"]+)"',
            yaml_text,
        )
        emitted = [case_id for command in commands for case_id in command.split()[2:]]
        committed = [
            case_id
            for _, _, cases in gp._eval_case_groups()
            for case_id in cases
        ]
        self.assertEqual(len(emitted), len(committed))
        self.assertEqual(set(emitted), set(committed))


if __name__ == "__main__":
    unittest.main()
