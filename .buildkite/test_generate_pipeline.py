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
import itertools
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_MODULE_PATH = Path(__file__).resolve().parent / "generate_pipeline.py"
_spec = importlib.util.spec_from_file_location("generate_pipeline_under_test", _MODULE_PATH)
gp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gp)

# The eval runner's cluster-name helper, shared so the derivation it runs is
# the one these tests check against every generated lane.
_CLUSTER_NAME_PATH = Path(__file__).resolve().parent / "eval_cluster_name.py"
_cluster_name_spec = importlib.util.spec_from_file_location(
    "eval_cluster_name_under_test", _CLUSTER_NAME_PATH
)
ecn = importlib.util.module_from_spec(_cluster_name_spec)
_cluster_name_spec.loader.exec_module(ecn)

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

    def patch_attr(self, attr, value):
        patcher = mock.patch.object(gp, attr, value)
        patcher.start()
        self.addCleanup(patcher.stop)

    def patch_generator(self, suite_dir, groups, quarantine=()):
        for attr, value in (
            ("KIND_SUITE_DIR", suite_dir),
            ("KIND_SPEC_GROUPS", groups),
            ("KIND_QUARANTINE", list(quarantine)),
            # Staged groups carry no soft twins; the real list names real groups.
            ("KIND_SCHEDULER_SOFT_GROUPS", []),
        ):
            self.patch_attr(attr, value)

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
        "              limit: 2\n"
        '            - signal_reason: "process_run_error"\n'
        "              limit: 2\n"
        '            - signal_reason: "stack_error"\n'
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

    def test_every_image_dockerfile_exists(self):
        missing = [d for _, _, d in gp.IMAGES if not (gp.REPO_ROOT / d).exists()]
        self.assertEqual(missing, [], "IMAGES names Dockerfiles that are not in the tree")

    def test_alfred_build_passes_the_pinned_upstream_commit(self):
        # alfred is built from upstream source the Dockerfile fetches itself, so
        # the build step must hand it the commit from .alfred-oss-ref.
        pin = gp.ALFRED_OSS_REF_FILE.read_text().strip()
        yaml_text = gp._build_image_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
        self.assertIn(f'- "ALFRED_OSS_REF={pin}"', yaml_text)
        self.assertIn(f'- "GO_BUILDER_IMAGE={gp.GO_BUILDER_IMAGE}"', yaml_text)
        self.assertIn(("alfred", "alfred", "dockerfiles/alfred.Dockerfile"), gp.IMAGES)

    def test_only_alfred_carries_the_upstream_pin(self):
        for label, _, _ in gp.IMAGES:
            if label == "alfred":
                continue
            self.assertFalse(
                any(a.startswith("ALFRED_OSS_REF=") for a in gp._image_build_args(label)),
                f"{label} must not receive the alfred pin",
            )


    def test_ome_bot_build_passes_the_pinned_cli_commit(self):
        # ome-bot builds kubectl-ome from upstream source the Dockerfile fetches
        # itself, so the build step must hand it the commit from
        # .kubectl-ome-oss-ref, and that pin must be a full commit sha: the CLI
        # lives on upstream main and no release tag carries it.
        pin = gp.KUBECTL_OME_OSS_REF_FILE.read_text().strip()
        self.assertRegex(pin, r"^[0-9a-f]{40}$")
        yaml_text = gp._build_image_step("ome-bot", "ome-bot", "dockerfiles/ome-bot.Dockerfile")
        self.assertIn(f'- "KUBECTL_OME_REF={pin}"', yaml_text)
        self.assertNotIn("ALFRED_OSS_REF", yaml_text)

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
            ("oep-status-drift", ":clipboard: OEP Status Drift", "oep-status"),
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
        for feature, topology, variant, _env, cases in gp._eval_lanes():
            pod_budget = 35 if gp._eval_feature_is_canary(feature) else 64
            target_waves = 2 if gp._eval_feature_is_canary(feature) else 4
            shards = gp._split_eval_cases(
                cases,
                {case_id: gp._eval_case_peak_pods(case_id) for case_id in cases},
                concurrency=4,
                pod_budget=pod_budget,
                target_waves=target_waves,
            )
            shard_counts[(feature, topology, variant)] = len(shards)
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
                    assignments[case_id] = (feature, topology, variant, shard_index)

        committed = {
            case_id
            for _, _, cases in gp._eval_case_groups()
            for case_id in cases
        }
        self.assertEqual(set(assignments), committed)
        self.assertEqual(shard_counts[("canary", "engine", "")], 21)
        self.assertEqual(shard_counts[("canary", "pd", "")], 13)
        self.assertEqual(shard_counts[("gen-crashloop", "engine", "")], 3)
        self.assertEqual(shard_counts[("gen-crashloop", "engine-mn", "")], 3)
        self.assertEqual(shard_counts[("gen-crashloop", "pd", "")], 5)
        self.assertEqual(shard_counts[("gen-crashloop", "pd-mn", "")], 5)
        self.assertEqual(shard_counts[("gen-switch", "engine", "")], 2)
        self.assertEqual(shard_counts[("gen-switch", "engine-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-switch", "pd", "")], 4)
        self.assertEqual(shard_counts[("gen-switch", "pd-mn", "")], 4)
        self.assertEqual(shard_counts[("gen-crashloop-idle", "engine", "")], 1)
        self.assertEqual(shard_counts[("gen-crashloop-idle", "engine-mn", "")], 1)
        self.assertEqual(shard_counts[("gen-crashloop-idle", "pd", "")], 2)
        self.assertEqual(shard_counts[("gen-crashloop-idle", "pd-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-crashloop-sibling", "pd", "")], 1)
        self.assertEqual(shard_counts[("gen-crashloop-sibling", "pd-mn", "")], 1)
        for topology in ("engine", "engine-mn", "pd", "pd-mn"):
            self.assertEqual(shard_counts[("gen-heal", topology, "")], 1)
        self.assertEqual(shard_counts[("gen-imagepull", "engine", "")], 1)
        self.assertEqual(shard_counts[("gen-imagepull", "engine-mn", "")], 1)
        self.assertEqual(shard_counts[("gen-imagepull", "pd", "")], 2)
        self.assertEqual(shard_counts[("gen-imagepull", "pd-mn", "")], 2)
        for topology in ("engine", "pd", "engine-mn", "pd-mn"):
            self.assertEqual(shard_counts[("gen-node", topology, "")], 1)
            self.assertEqual(shard_counts[("gen-node", topology, gp.EVAL_FORCE_DELETE_SLUG)], 1)
        for topology in ("engine", "pd", "engine-mn"):
            self.assertEqual(shard_counts[("gen-ctrlrestart", topology, "")], 1)
        self.assertEqual(shard_counts[("gen-canary-crash", "engine", "")], 4)
        self.assertEqual(shard_counts[("gen-canary-crash", "pd", "")], 12)
        self.assertEqual(shard_counts[("gen-canary-second-rev", "engine", "")], 3)
        self.assertEqual(shard_counts[("gen-canary-second-rev", "pd", "")], 8)
        self.assertEqual(shard_counts[("gen-canary-podkill", "engine", "")], 2)
        self.assertEqual(shard_counts[("gen-canary-podkill", "pd", "")], 4)
        self.assertEqual(shard_counts[("gen-canary-fault-old", "engine", "")], 16)
        self.assertEqual(shard_counts[("gen-canary-fault-old", "pd", "")], 47)
        self.assertEqual(shard_counts[("gen-canary-fault-new", "engine", "")], 5)
        self.assertEqual(shard_counts[("gen-canary-fault-new", "pd", "")], 13)
        self.assertEqual(shard_counts[("gen-rollback", "engine", "")], 3)
        self.assertEqual(shard_counts[("gen-rollback", "engine-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-rollback", "pd", "")], 2)
        self.assertEqual(shard_counts[("gen-rollback", "pd-mn", "")], 2)
        self.assertEqual(job_count, 312)

    def test_force_delete_twin_runs_exactly_the_cases_that_need_the_policy(self):
        """A lane whose cases carry needs=forcedelete gets a policy-on twin
        with exactly those cases and the deploy switch; the plain lane keeps
        the rest and no switch, and lanes without such cases get no twin."""
        with mock.patch.dict(
            os.environ,
            {
                "GKE_EVAL_CASE_CONCURRENCY": "4",
                "GKE_EVAL_TARGET_WAVES": "4",
                "GKE_EVAL_CANARY_TARGET_WAVES": "2",
            },
        ):
            yaml_text = gp._gke_eval_group()
        needing = {
            case_id
            for _, _, cases in gp._eval_case_groups()
            for case_id in cases
            if gp._eval_case_header_fields(case_id).get("needs") == gp.EVAL_FORCE_DELETE_NEED
        }
        self.assertTrue(needing)
        self.assertTrue(all(gp._eval_case_header_fields(c).get("fault", "").startswith("node:") for c in needing))
        twin_cases, plain_cases = set(), set()
        for block in yaml_text.split("- label:")[1:]:
            key = re.search(r'key: "(omenative-e2e-[^"]+)"', block)
            command = re.search(r'command: "sh \.buildkite/run_omenative_eval_gke\.sh ([^"]+)"', block)
            if not key or not command:
                continue
            cases = set(command.group(1).split()[2:])
            if f"-{gp.EVAL_FORCE_DELETE_SLUG}" in key.group(1):
                self.assertIn(f'{gp.EVAL_FORCE_DELETE_ENV}: "1"', block)
                self.assertTrue(key.group(1).startswith("omenative-e2e-gen-node-"), key.group(1))
                twin_cases |= cases
            else:
                self.assertNotIn(gp.EVAL_FORCE_DELETE_ENV, block)
                plain_cases |= cases
        self.assertEqual(twin_cases, needing)
        self.assertFalse(plain_cases & needing)
        self.assertIn("gen-node / engine [forcedelete]", yaml_text)
        self.assertIn("gen-node / engine [ome-scheduler] [forcedelete]", yaml_text)
        self.assertIn('key: "omenative-e2e-gen-node-engine-ome-scheduler-forcedelete"', yaml_text)

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
        self.assertIn("canary / engine [1/21]", yaml_text)
        self.assertIn("canary / pd [13/13]", yaml_text)
        self.assertIn("gen-canary-crash / pd [12/12]", yaml_text)
        # The generated canary families are canary lanes: Prometheus for the
        # analysis-gated cases and the canary pod budget.
        crash_env = yaml_text[yaml_text.index('key: "omenative-e2e-gen-canary-second-rev-engine-1-of-3"'):]
        self.assertIn('OME_KIND_PROMETHEUS_ENABLED: "true"', crash_env[:600])
        self.assertIn('OME_EVAL_POD_BUDGET: "35"', crash_env[:600])
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
        # Every case runs once under the default scheduler; the ome-scheduler twin
        # runs every case whose whole-node footprint fits the node cap and names
        # the rest, so nothing is dropped silently.
        self.assertEqual(set(emitted), set(committed))
        job_keys = re.findall(r'key: "(omenative-e2e-(?!kind-gke)[^"]+)"', yaml_text)
        job_keys = [k for k in job_keys if not k.endswith("-skipped")]
        self.assertEqual(len(job_keys), len(commands))
        skipped = set()
        for m in re.finditer(r'OME_EVAL_SCHEDULER_SKIPPED: "([^"]*)"', yaml_text):
            skipped.update(m.group(1).split())
        for m in re.finditer(r"not run in this twin: ([^']*)'", yaml_text):
            skipped.update(m.group(1).split())
        for scheduler in gp.EVAL_SCHEDULERS:
            per_sched = [
                case_id
                for key, command in zip(job_keys, commands)
                if ("-" + gp.EVAL_GANG_SCHEDULER in key) == bool(scheduler)
                for case_id in command.split()[2:]
            ]
            if scheduler:
                self.assertEqual(set(per_sched) | skipped, set(committed), scheduler)
                self.assertFalse(set(per_sched) & skipped)
            else:
                self.assertEqual(sorted(per_sched), sorted(committed), "default")


class TestAlfredKindBundle(unittest.TestCase):
    """The alfred suite ships as its own bundle, like the other KIND lanes."""

    def test_test_step_uploads_the_alfred_bundle(self):
        self.assertIn('".buildkite/artifacts/alfred-kind-test.tgz"', gp.ci_steps())

    def test_run_tests_packages_the_alfred_bundle(self):
        script = (gp.REPO_ROOT / ".buildkite" / "run_tests.sh").read_text()
        self.assertIn(
            "package_kind_bundle alfred-kind-test.tgz alfred_kind.test",
            script,
            "run_tests.sh must compile the alfred suite into alfred-kind-test.tgz",
        )
        self.assertIn("./tests/integration/singlecluster/controller/alfred_kind", script)
        package = (gp.REPO_ROOT / ".buildkite" / "package_kind_test_image.sh").read_text()
        bundles = re.search(r'^artifact_bundles="([^"]*)"', package, re.M)
        self.assertIsNotNone(bundles, "package_kind_test_image.sh must list artifact_bundles")
        self.assertIn("alfred-kind-test.tgz", bundles.group(1).split())


class TestAlfredKindGroup(PipelineTestCase):
    """The alfred lane: its own group, its own bundle, soft-fail, kill switch."""

    def setUp(self):
        self.yaml_text = gp._alfred_kind_group()

    def test_is_its_own_group_with_its_own_status_context(self):
        self.assertIn('key: "integration-test-kind-alfred-gke"', self.yaml_text)
        self.assertIn('context: "integration-test-kind-alfred-gke"', self.yaml_text)

    def test_runs_its_own_bundle_and_soft_fails(self):
        self.assertIn('OME_KIND_TEST_BUNDLE: "/kind-test/alfred-kind-test.tgz"', self.yaml_text)
        self.assertIn("soft_fail: true", self.yaml_text)

    def test_shards_partition_the_suite(self):
        patterns = _focus_patterns(self.yaml_text)
        groups = gp._alfred_kind_spec_groups()
        self.assertEqual(len(patterns), len(groups))
        for name in gp._alfred_kind_spec_files():
            owners = [
                slug
                for (slug, _, _), pattern in zip(groups, patterns)
                if re.search(pattern, str(gp.ALFRED_KIND_SUITE_DIR / name))
            ]
            self.assertEqual(len(owners), 1, f"{name} matched by {owners or 'no shard'}")

    def test_alfred_image_pushes_on_every_branch(self):
        alfred = gp._build_image_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
        self.assertIn("push: true", alfred)
        self.assertNotIn("push-branches", alfred)
        manager = gp._build_image_step("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile")
        self.assertIn('push-branches: "main"', manager)
        self.assertNotIn("push: true", manager)

    def test_every_branch_image_tags_latest_only_on_main(self):
        sha = gp.COMMIT[:8]
        self.patch_attr("BRANCH", "feature/anything")
        alfred = gp._build_image_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
        self.assertIn(f'tags: ["{sha}"]', alfred)
        self.assertNotIn('"latest"', alfred)
        manager = gp._build_image_step("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile")
        self.assertIn(f'tags: ["latest", "{sha}"]', manager)
        self.patch_attr("BRANCH", "main")
        alfred = gp._build_image_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
        self.assertIn(f'tags: ["latest", "{sha}"]', alfred)

    def test_package_step_waits_for_the_alfred_build(self):
        step = gp._package_kind_test_image_step()
        self.assertIn('"build-alfred"', step)
        self.assertIn('OME_ALFRED_IMAGE_REQUIRED: "1"', step)
        self.assertIn('"ECR_AUTH_TOKEN"', step)

    def test_lane_is_emitted_by_default(self):
        self.assertIn("kind-gke-alfred-", gp.ci_steps())

    def test_kill_switch_drops_the_lane_and_the_dependency(self):
        self.patch_attr("GKE_ALFRED_ENABLED", False)
        self.assertNotIn("kind-gke-alfred-", gp.ci_steps())
        step = gp._package_kind_test_image_step()
        self.assertNotIn('"build-alfred"', step)
        self.assertNotIn("OME_ALFRED_IMAGE_REQUIRED", step)
        # Nothing consumes a branch build of the image with the lane off, so it
        # publishes on main only like every other image.
        alfred = gp._build_image_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
        self.assertIn('push-branches: "main"', alfred)
        self.assertNotIn("push: true", alfred)

    def test_lane_is_nested_under_the_kind_switch(self):
        self.patch_attr("GKE_KIND_ENABLED", False)
        self.assertNotIn("kind-gke-alfred-", gp.ci_steps())
        alfred = gp._build_image_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
        self.assertIn('push-branches: "main"', alfred)


class TestPipelineDependenciesResolve(PipelineTestCase):
    """Every depends_on in the rendered pipeline names an emitted key.

    A dependency on a key no step emits fails the whole upload rather than
    one lane, so a step dropped by a branch condition or a kill switch must
    take its dependents with it.
    """

    def _keys_and_dependencies(self, yaml_text):
        keys = set(re.findall(r'^\s*key: "([^"]+)"\s*$', yaml_text, re.M))
        targets = set()
        lines = yaml_text.splitlines()
        index = 0
        while index < len(lines):
            line = lines[index]
            index += 1
            if match := re.match(r'^\s*depends_on: "([^"]+)"\s*$', line):
                targets.add(match.group(1))
            elif match := re.match(r"^\s*depends_on: \[(.*)\]\s*$", line):
                targets.update(re.findall(r'"([^"]+)"', match.group(1)))
            elif re.match(r"^\s*depends_on:\s*$", line):
                while index < len(lines) and (
                    item := re.match(r'^\s*- "([^"]+)"\s*$', lines[index])
                ):
                    targets.add(item.group(1))
                    index += 1
        return keys, targets

    def test_every_dependency_names_an_emitted_key(self):
        cases = itertools.product(("main", "feature/anything"), (True, False), (True, False))
        for branch, kind, alfred in cases:
            with self.subTest(branch=branch, kind=kind, alfred=alfred):
                self.patch_attr("BRANCH", branch)
                self.patch_attr("GKE_KIND_ENABLED", kind)
                self.patch_attr("GKE_ALFRED_ENABLED", alfred)
                keys, targets = self._keys_and_dependencies(gp.generate_pipeline())
                self.assertTrue(keys, "no step keys found; did the YAML shape change?")
                self.assertTrue(targets, "no depends_on found; did the YAML shape change?")
                dangling = targets - keys
                self.assertEqual(
                    dangling, set(),
                    f"{sorted(dangling)} are depended on but no step emits them",
                )


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

    def test_scheduler_shard_runs_on_prs_with_two_workers(self):
        with mock.patch.object(gp, "BRANCH", "feature/test"):
            output = gp._mc_kind_group()
        blocks = output.split("- label:")[1:]
        for block in blocks:
            with self.subTest(block=block.splitlines()[0]):
                if 'key: "kind-gke-mc-scheduling"' in block:
                    self.assertIn('OME_TEST_SCHEDULER: "ome-scheduler"', block)
                    self.assertIn('OME_TEST_SCHEDULER_WORKERS: "2"', block)
                    self.assertIn("scheduling_kind_test", block)
                else:
                    self.assertNotIn("OME_TEST_SCHEDULER", block)
        self.assertIn('key: "kind-gke-mc-scheduling"', output)

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


class TestReleaseMarkerContents(unittest.TestCase):
    def test_marker_contents_are_published(self):
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


class TestRetiredPipelineMode(unittest.TestCase):
    def test_legacy_pipeline_mode_fails_closed(self):
        with mock.patch.dict(os.environ, {"OME_PIPELINE_MODE": "daily"}):
            with self.assertRaisesRegex(SystemExit, "2"):
                gp.main()


class TestEvalClusterName(unittest.TestCase):
    """run_omenative_eval_gke.sh names its KIND cluster after the lane, and
    KIND names every node after the cluster, so the longest node of every
    emitted lane must fit the hostname limit."""

    EVAL_RUNNER = gp.REPO_ROOT / ".buildkite" / "run_omenative_eval_gke.sh"
    OVERFLOWING_LANE = "gen-crashloop-idle-engine-mn-ome-scheduler"

    def default_workers(self) -> int:
        """The worker count the runner uses when the step sets none."""
        match = re.search(
            r'workers="\$\{OME_EVAL_KIND_WORKERS:-(\d+)\}"', self.EVAL_RUNNER.read_text()
        )
        self.assertIsNotNone(match, "the runner must default OME_EVAL_KIND_WORKERS")
        return int(match.group(1))

    def emitted_lanes(self) -> dict[str, int]:
        """{lane: workers} for every job the eval group emits, the lane built
        the way the runner builds it from its arguments and env."""
        with mock.patch.dict(
            os.environ,
            {
                "GKE_EVAL_CASE_CONCURRENCY": "4",
                "GKE_EVAL_TARGET_WAVES": "4",
                "GKE_EVAL_CANARY_TARGET_WAVES": "2",
            },
        ):
            yaml_text = gp._gke_eval_group()
        default_workers = self.default_workers()
        lanes: dict[str, int] = {}
        for block in yaml_text.split("- label:")[1:]:
            command = re.search(
                r'command: "sh \.buildkite/run_omenative_eval_gke\.sh (\S+) (\S+)', block
            )
            if not command:
                continue
            lane = "-".join(command.groups())
            scheduler = re.search(r'OME_EVAL_SCHEDULER: "([^"]+)"', block)
            if scheduler:
                lane += "-" + scheduler.group(1)
            if f'{gp.EVAL_FORCE_DELETE_ENV}: "1"' in block:
                lane += "-" + gp.EVAL_FORCE_DELETE_SLUG
            workers = re.search(r'OME_EVAL_KIND_WORKERS: "(\d+)"', block)
            lanes[lane] = int(workers.group(1)) if workers else default_workers
        return lanes

    def test_every_emitted_lane_gets_a_cluster_whose_nodes_fit(self):
        lanes = self.emitted_lanes()
        self.assertIn(self.OVERFLOWING_LANE, lanes)
        names = {}
        for lane, workers in lanes.items():
            name = ecn.cluster_name(lane, workers)
            names[lane] = name
            for node in ecn.node_names(name, workers):
                self.assertLessEqual(len(node), ecn.HOSTNAME_MAX, f"{lane}: {node}")
                self.assertRegex(node, r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
            plain = ecn.plain_cluster_name(lane)
            if all(len(node) <= ecn.HOSTNAME_MAX for node in ecn.node_names(plain, workers)):
                # A lane that fits keeps its name, so its clusters and logs
                # read as before.
                self.assertEqual(name, plain, lane)
            else:
                head = name[: -(ecn.HASH_LENGTH + 1)]
                self.assertTrue(plain.startswith(head), f"{lane}: {name} is not a prefix of {plain}")
                self.assertRegex(name, rf"-[0-9a-f]{{{ecn.HASH_LENGTH}}}$")
        self.assertEqual(len(set(names.values())), len(names), "one cluster name per lane")

    def test_overflowing_lane_is_shortened_deterministically(self):
        plain = ecn.plain_cluster_name(self.OVERFLOWING_LANE)
        self.assertGreater(len(plain + ecn.CONTROL_PLANE_SUFFIX), ecn.HOSTNAME_MAX)
        name = ecn.cluster_name(self.OVERFLOWING_LANE, 4)
        self.assertEqual(name, "ome-eval-gen-crashloop-idle-engine-mn-ome-d45678")
        self.assertEqual(name, ecn.cluster_name(self.OVERFLOWING_LANE, 4))
        self.assertLessEqual(len(name + ecn.CONTROL_PLANE_SUFFIX), ecn.HOSTNAME_MAX)
        self.assertNotIn("--", name)
        # A longer lane with the same readable head gets its own hash.
        twin = ecn.cluster_name(self.OVERFLOWING_LANE + "-" + gp.EVAL_FORCE_DELETE_SLUG, 4)
        self.assertEqual(twin[: -ecn.HASH_LENGTH], name[: -ecn.HASH_LENGTH])
        self.assertNotEqual(twin, name)

    def test_short_lane_keeps_its_plain_name(self):
        self.assertEqual(ecn.cluster_name("canary-engine", 2), "ome-eval-canary-engine")
        self.assertEqual(ecn.cluster_name("gen_fault-pd_mn", 12), "ome-eval-gen-fault-pd-mn")

    def test_node_names_follow_kind(self):
        self.assertEqual(
            ecn.node_names("c", 3), ["c-control-plane", "c-worker", "c-worker2", "c-worker3"]
        )
        with self.assertRaises(ValueError):
            ecn.node_names("c", 0)

    def test_runner_names_the_cluster_through_the_helper(self):
        script = self.EVAL_RUNNER.read_text()
        self.assertIn('eval_cluster_name.py" "$lane" "$workers"', script)
        self.assertNotIn("printf 'ome-eval-%s'", script)
        self.assertLess(
            script.index('workers="${OME_EVAL_KIND_WORKERS'),
            script.index("eval_cluster_name.py"),
            "the worker count sizes the name budget, so it is parsed first",
        )
        # The command line the runner uses prints what the library computes.
        out = subprocess.run(
            [sys.executable, str(_CLUSTER_NAME_PATH), self.OVERFLOWING_LANE, "4"],
            capture_output=True, text=True, check=True,
        )
        self.assertEqual(out.stdout.strip(), ecn.cluster_name(self.OVERFLOWING_LANE, 4))
        bad = subprocess.run(
            [sys.executable, str(_CLUSTER_NAME_PATH), self.OVERFLOWING_LANE, "0"],
            capture_output=True, text=True,
        )
        self.assertEqual(bad.returncode, 2)
