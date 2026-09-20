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

import datetime
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

    def patch_generator(self, suite_dir, groups, quarantine=()):
        for attr, value in (
            ("KIND_SUITE_DIR", suite_dir),
            ("KIND_SPEC_GROUPS", groups),
            ("KIND_QUARANTINE", list(quarantine)),
        ):
            patcher = mock.patch.object(gp, attr, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def quarantine_entry(self, spec, owner="tester", reason="known defect", expires=None):
        """A valid entry by default; each test invalidates the one field it pins."""
        if expires is None:
            expires = (gp._today() + datetime.timedelta(days=14)).isoformat()
        return {"spec": spec, "owner": owner, "reason": reason, "expires": expires}


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


class TestKindQuarantine(PipelineTestCase):
    """Quarantine is the one home other than a gating group, and it expires."""

    def test_quarantined_spec_leaves_its_group_and_runs_soft_fail(self):
        suite_dir = self.make_suite_dir(["alpha", "flaky"])
        self.patch_generator(
            suite_dir,
            (("group-a", "Group A", ("alpha",)),),
            quarantine=[self.quarantine_entry("flaky_kind_test")],
        )
        gating = gp._gke_kind_group()
        self.assertNotIn("flaky_kind_test.go", gating)
        self.assertNotIn("soft_fail", gating)

        quarantine = gp._kind_quarantine_group()
        self.assertIn('key: "integration-test-kind-quarantine"', quarantine)
        self.assertIn("soft_fail: true", quarantine)
        self.assertIn("Quarantined KIND specs (1 file)", quarantine)
        self.assertEqual(
            _focus_patterns(quarantine), [gp._kind_focus(["flaky_kind_test.go"])]
        )

    def test_no_quarantine_emits_no_group(self):
        suite_dir = self.make_suite_dir(["alpha"])
        self.patch_generator(suite_dir, (("group-a", "Group A", ("alpha",)),))
        self.assertEqual(gp._kind_quarantine_group(), "")

    def test_unknown_spec_fails_generation(self):
        suite_dir = self.make_suite_dir(["alpha"])
        self.patch_generator(
            suite_dir,
            (("group-a", "Group A", ("alpha",)),),
            quarantine=[self.quarantine_entry("ghost_kind_test")],
        )
        with self.assertRaisesRegex(
            RuntimeError, r"quarantined KIND spec does not exist: ghost_kind_test\.go"
        ):
            gp._kind_spec_groups()

    def test_expired_entry_fails_generation(self):
        suite_dir = self.make_suite_dir(["alpha", "flaky"])
        self.patch_generator(
            suite_dir,
            (("group-a", "Group A", ("alpha",)),),
            quarantine=[
                self.quarantine_entry(
                    "flaky_kind_test",
                    expires=(gp._today() - datetime.timedelta(days=1)).isoformat(),
                )
            ],
        )
        with self.assertRaisesRegex(
            RuntimeError, r"KIND quarantine for flaky_kind_test expired on "
        ):
            gp._kind_spec_groups()

    def test_entry_expiring_today_still_generates(self):
        suite_dir = self.make_suite_dir(["alpha", "flaky"])
        self.patch_generator(
            suite_dir,
            (("group-a", "Group A", ("alpha",)),),
            quarantine=[
                self.quarantine_entry(
                    "flaky_kind_test", expires=gp._today().isoformat()
                )
            ],
        )
        self.assertEqual(gp._kind_quarantine_files(), ["flaky_kind_test.go"])

    def test_missing_owner_or_reason_fails_generation(self):
        suite_dir = self.make_suite_dir(["alpha", "flaky"])
        for field in ("owner", "reason"):
            with self.subTest(field=field):
                entry = self.quarantine_entry("flaky_kind_test", **{field: "  "})
                with mock.patch.object(gp, "KIND_SUITE_DIR", suite_dir), mock.patch.object(
                    gp, "KIND_QUARANTINE", [entry]
                ):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        rf"KIND quarantine entry for flaky_kind_test has no {field}",
                    ):
                        gp._kind_quarantine_files()

    def test_unparseable_expiry_fails_generation(self):
        suite_dir = self.make_suite_dir(["flaky"])
        self.patch_generator(
            suite_dir,
            (),
            quarantine=[self.quarantine_entry("flaky_kind_test", expires="soon")],
        )
        with self.assertRaisesRegex(RuntimeError, r"needs an expires date"):
            gp._kind_quarantine_files()

    def test_spec_in_both_a_group_and_quarantine_fails_generation(self):
        suite_dir = self.make_suite_dir(["alpha", "flaky"])
        self.patch_generator(
            suite_dir,
            (("group-a", "Group A", ("alpha", "flaky")),),
            quarantine=[self.quarantine_entry("flaky_kind_test")],
        )
        with self.assertRaisesRegex(
            RuntimeError,
            r"quarantined KIND specs are still in a gating group: "
            r"flaky_kind_test\.go in group-a",
        ):
            gp._kind_spec_groups()

    def test_spec_in_neither_a_group_nor_quarantine_fails_generation(self):
        suite_dir = self.make_suite_dir(["alpha", "flaky", "orphan"])
        self.patch_generator(
            suite_dir,
            (("group-a", "Group A", ("alpha",)),),
            quarantine=[self.quarantine_entry("flaky_kind_test")],
        )
        with self.assertRaisesRegex(
            RuntimeError, r"unclassified KIND specs: orphan_kind_test\.go"
        ):
            gp._kind_spec_groups()

    def test_duplicate_quarantine_entry_fails_generation(self):
        suite_dir = self.make_suite_dir(["flaky"])
        self.patch_generator(
            suite_dir,
            (),
            quarantine=[
                self.quarantine_entry("flaky_kind_test"),
                self.quarantine_entry("flaky_kind_test"),
            ],
        )
        with self.assertRaisesRegex(
            RuntimeError, r"duplicate KIND quarantine entry: flaky_kind_test\.go"
        ):
            gp._kind_quarantine_files()


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
        # The multi-cluster suite is a separate group, so its soft-fail status
        # cannot muddy this lane's gating context.
        self.assertNotIn("soft_fail", self.yaml_text)

    def test_labels_carry_spec_file_counts(self):
        self.assertIn("Pair of specs (2 files)", self.yaml_text)
        self.assertIn("Single spec (1 file)", self.yaml_text)

    # `kind create cluster` starving on a contended agent fails in BeforeSuite
    # with no spec run; run_kind_tests_gke.sh reports exactly that shape as 125
    # so the infra preset — and only the infra preset — can pick it up.
    INFRA_RETRY_BLOCK = (
        "        retry:\n"
        "          automatic:\n"
        "            - exit_status: 125\n"
        "              limit: 2\n"
        '            - signal_reason: "agent_stop"\n'
        "              limit: 2"
    )

    def test_every_child_retries_only_on_infra_failure(self):
        self.assertEqual(self.yaml_text.count(self.INFRA_RETRY_BLOCK), 2)

    def test_children_never_retry_a_failing_spec(self):
        # The doctrine: a red spec is a defect to fix or quarantine, never a
        # retry. The any-exit wildcard, or a condition matching exit 1, would
        # break it.
        self.assertNotIn('exit_status: "*"', self.yaml_text)
        self.assertNotIn("exit_status: 1\n", self.yaml_text)

    def test_retry_is_a_step_level_key(self):
        self.assertLess(
            self.yaml_text.index("        retry:"),
            self.yaml_text.index("        plugins:"),
            "retry must be a step-level key, not nested under plugins",
        )

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


class TestChartImageOrdering(unittest.TestCase):
    """A chart that stamps an image ref must not publish before that image."""

    def _stamping_charts_per_push_script(self):
        # push_chart.sh decides which charts stamp, one `[ "${CHART}" = "x" ]`
        # block each. Reading it is the point: the two files drifting is the bug
        # this guards, so asserting against a second hardcoded list would prove
        # nothing.
        script = (Path(gp.__file__).resolve().parent / "push_chart.sh").read_text()
        return set(re.findall(r'\[ "\$\{CHART\}" = "([a-z0-9-]+)" \]', script))

    def test_every_chart_that_stamps_an_image_waits_for_it(self):
        stamping = self._stamping_charts_per_push_script()
        self.assertTrue(stamping, "found no stamping blocks — did push_chart.sh change shape?")
        missing = stamping - set(gp.IMAGE_STAMPING_CHARTS)
        self.assertEqual(
            missing, set(),
            f"{sorted(missing)} stamp an image ref in push_chart.sh but are not in "
            "IMAGE_STAMPING_CHARTS, so their chart publishes before the image exists",
        )
        extra = set(gp.IMAGE_STAMPING_CHARTS) - stamping
        self.assertEqual(
            extra, set(),
            f"{sorted(extra)} wait on the image build but stamp nothing, delaying "
            "their publish for no reason",
        )

    def test_stamping_charts_are_real_charts(self):
        unknown = set(gp.IMAGE_STAMPING_CHARTS) - set(gp.CHARTS)
        self.assertEqual(unknown, set(), f"{sorted(unknown)} are not in CHARTS")


class TestReleaseMarkerEmission(unittest.TestCase):
    def test_marker_step_is_after_the_final_wait_gate(self):
        yaml_text = gp.generate_pipeline()
        self.assertEqual(yaml_text.count('key: "chart-ome-release"'), 1)
        wait_index = yaml_text.rfind("  - wait\n")
        marker_index = yaml_text.rfind('key: "chart-ome-release"')
        self.assertGreater(wait_index, 0)
        self.assertGreater(marker_index, wait_index)

    def test_component_chart_steps_export_version_artifacts(self):
        for chart in gp.RELEASE_MARKER_CHARTS:
            with self.subTest(chart=chart):
                yaml_text = gp._push_chart_step(chart)
                self.assertIn(
                    f'".buildkite/artifacts/chart-versions/{chart}"',
                    yaml_text,
                )


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


class TestDriftCheckEmission(unittest.TestCase):
    # A drift check installs its tooling with `go install` before it compares
    # anything, so a module-proxy blip fails the step and reads as drift. The
    # retry has to be the any-non-zero preset: a failed fetch exits 1 or 2,
    # which the infra preset (125 / agent_stop) never matches.
    RETRY_BLOCK = (
        '    retry:\n'
        '      automatic:\n'
        '        - exit_status: "*"\n'
        '          limit: 2'
    )

    def test_every_drift_check_retries_on_any_nonzero_exit(self):
        for key, label, target in [
            ("fmt-drift", ":broom: Fmt Drift", "fmt"),
            ("tidy-drift", ":package: Tidy Drift", "tidy"),
            ("manifests-drift", ":memo: Manifests Drift", "manifests"),
            ("generate-drift", ":arrows_counterclockwise: Generate Drift", "generate"),
            ("dep-crds-drift", ":truck: Dep CRDs Drift", "dep-crds"),
        ]:
            with self.subTest(key=key):
                yaml_text = gp._drift_check_step(key, label, target)
                self.assertIn(self.RETRY_BLOCK, yaml_text)
                self.assertIn(f"      make {target}\n", yaml_text)
                self.assertIn("      git diff --exit-code\n", yaml_text)

    def test_retry_precedes_the_command_it_guards(self):
        yaml_text = gp._drift_check_step("dep-crds-drift", ":truck: Dep CRDs Drift", "dep-crds")
        self.assertLess(
            yaml_text.index("    retry:"),
            yaml_text.index("    command: |"),
            "retry must be a step-level key, not swallowed into the command block",
        )

    def test_apt_packages_still_lead_the_command(self):
        yaml_text = gp._drift_check_step(
            "manifests-drift", ":memo: Manifests Drift", "manifests", apt_packages="jq"
        )
        self.assertLess(
            yaml_text.index("apt-get install -y --no-install-recommends jq"),
            yaml_text.index("      make manifests"),
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
        mapped += gp._kind_quarantine_files()
        self.assertEqual(sorted(mapped), gp._kind_spec_files())

    def test_checked_in_quarantine_is_valid_and_disjoint(self):
        quarantined = gp._kind_quarantine_files()
        grouped = {
            name for _, _, files in gp._kind_spec_groups() for name in files
        }
        self.assertFalse(grouped & set(quarantined))

    def test_checked_in_focus_patterns_partition_the_suite(self):
        patterns = _focus_patterns(gp._gke_kind_group())
        groups = gp._kind_spec_groups()
        self.assertEqual(len(patterns), len(groups))
        homes = [slug for slug, _, _ in groups]
        # An empty quarantine emits no group, so it is a home only when
        # something is quarantined.
        if quarantine := gp._kind_quarantine_group():
            patterns += _focus_patterns(quarantine)
            homes.append("quarantine")
        self.assertEqual(len(patterns), len(homes))
        for name in gp._kind_spec_files():
            owners = [
                home
                for home, pattern in zip(homes, patterns)
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
        self.assertEqual(job_count, 108)

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


class TestMulticlusterKindGroup(PipelineTestCase):
    """The multi-cluster lane: its own group, its own bundle, shard-driven."""

    def setUp(self):
        self.yaml_text = gp._mc_kind_group()

    def test_is_its_own_group_with_its_own_status_context(self):
        self.assertIn('key: "integration-test-kind-mc-gke"', self.yaml_text)
        self.assertIn('context: "integration-test-kind-mc-gke"', self.yaml_text)

    def test_runs_its_own_bundle_and_soft_fails(self):
        self.assertIn(
            'OME_KIND_TEST_BUNDLE: "/kind-test/multicluster-kind-test.tgz"',
            self.yaml_text,
        )
        self.assertIn("soft_fail: true", self.yaml_text)

    def test_shards_partition_the_suite(self):
        patterns = _focus_patterns(self.yaml_text)
        groups = gp._mc_kind_spec_groups()
        self.assertEqual(len(patterns), len(groups))
        for name in gp._mc_kind_spec_files():
            owners = [
                slug
                for (slug, _, _), pattern in zip(groups, patterns)
                if re.search(pattern, str(gp.MC_KIND_SUITE_DIR / name))
            ]
            self.assertEqual(
                len(owners), 1, f"{name} matched by {owners or 'no shard'}"
            )


class TestReleaseContentsMatchDiscovery(unittest.TestCase):
    """The marker and the daily discovery path must mean the same release."""

    def _daily_candidate(self):
        path = Path(gp.__file__).resolve().parent / "daily_candidate.py"
        spec = importlib.util.spec_from_file_location("daily_candidate_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_marker_contents_match_candidate_and_published_components(self):
        # Two hardcoded answers to "what is a complete OME release". Adding a
        # chart to one only makes the marker and the discovery path promote
        # different things under the same name.
        candidate = self._daily_candidate()
        self.assertEqual(
            tuple(gp.RELEASE_MARKER_CHARTS), tuple(candidate.CHARTS)
        )
        self.assertEqual(
            tuple(gp.RELEASE_MARKER_IMAGES), tuple(candidate.IMAGES)
        )

        unknown = set(gp.RELEASE_MARKER_CHARTS) - set(gp.CHARTS)
        self.assertEqual(unknown, set(), f"{sorted(unknown)} are not in CHARTS")

        built = {ecr_name for _, ecr_name, _ in gp.IMAGES}
        unknown = {
            f"{image}" for image in gp.RELEASE_MARKER_IMAGES
            if image not in built and f"ome/{image}" not in built
        }
        self.assertEqual(
            unknown, set(), f"{sorted(unknown)} are published by no build step"
        )
