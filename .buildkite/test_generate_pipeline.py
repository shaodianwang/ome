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

import base64
import contextlib
import datetime
import ast
import contextlib
import importlib.util
import io
import itertools
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
import urllib.parse
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

# The round's verdict file: the recording script and the generator's targeted
# lookup are tested against the same fixture through this module.
_VERDICTS_PATH = Path(__file__).resolve().parent / "eval_verdicts.py"
_verdicts_spec = importlib.util.spec_from_file_location("eval_verdicts_under_test", _VERDICTS_PATH)
ev = importlib.util.module_from_spec(_verdicts_spec)
_verdicts_spec.loader.exec_module(ev)

# Minimal file bodies: discovery counts a file only if it declares a
# ginkgo suite entry, so helpers sharing the suffix stay unowned.
SPEC_BODY = 'package kind\n\nvar _ = ginkgo.Describe("suite", func() {})\n'
TABLE_BODY = 'package kind\n\nvar _ = ginkgo.DescribeTable("cases", func() {})\n'
HELPER_BODY = "package kind\n\nfunc sharedHelper() {}\n"


def uploaded_build_step(label, ecr_name, dockerfile):
    """The plugin build step a probe uploads on a miss, decoded from its env."""
    probe = gp._build_image_step(label, ecr_name, dockerfile)
    match = re.search(r'OME_IMAGE_BUILD_STEP_B64: "([^"]+)"', probe)
    if not match:
        raise AssertionError(f"the {label} step carries no uploaded build step")
    return base64.b64decode(match.group(1)).decode()


def dockerfile_copy_sources(dockerfile):
    """Build-context paths a Dockerfile's COPY lines read (stage copies skipped)."""
    sources = []
    for line in dockerfile.read_text().splitlines():
        parts = line.split()
        if not parts or parts[0] != "COPY":
            continue
        if any(part.startswith("--from=") for part in parts[1:]):
            continue
        operands = [part for part in parts[1:] if not part.startswith("--")]
        sources += [source.rstrip("/") for source in operands[:-1]]
    return sources


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

    def allow_heavy(self):
        """The build carries OME_EVAL_ALLOW_HEAVY=1, as a scheduled round would."""
        patcher = mock.patch.dict(os.environ, {gp.EVAL_ALLOW_HEAVY_VAR: "1"})
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
    def test_marker_is_the_last_step_and_names_every_other_top_level_key(self):
        # With the eval off there is no eval lane, so the single document
        # carries the marker itself, after every step it gates on. The mode is
        # pinned: the unit lane itself runs where the eval is on and the
        # marker travels in the last upload instead.
        with mock.patch.object(gp, "BRANCH", "feature"), mock.patch.object(gp, "EVAL_MODE", "off"):
            yaml_text = gp.generate_pipeline()
        self.assertEqual(yaml_text.count('key: "chart-ome-release"'), 1)
        keys = gp._top_level_keys(yaml_text)
        self.assertEqual(keys[-1], "chart-ome-release")
        self.assertNotIn("- wait", yaml_text)
        marker = yaml_text[yaml_text.index('key: "chart-ome-release"'):]
        gates = re.findall(r'^      - "([^"]+)"$', marker.split("secrets:")[0], re.M)
        self.assertEqual(gates, keys[:-1])
        self.assertNotIn(gp.UPLOAD_STEP_KEY, keys)

    def test_on_main_the_marker_closes_the_last_upload_and_names_every_key(self):
        # With the eval on, its lanes follow in their own uploads and the
        # marker is the one job of the last upload, gating every key of every
        # upload. Smoke is the smallest mode that carries lanes.
        with mock.patch.object(gp, "BRANCH", "main"), mock.patch.object(gp, "EVAL_MODE", "smoke"), mock.patch.dict(
            os.environ, {gp.EVAL_ALLOW_HEAVY_VAR: "1"}
        ):
            batches = gp.generate_pipeline_batches()
        self.assertGreater(len(batches), 1)
        self.assertNotIn('key: "chart-ome-release"', batches[0])
        self.assertIn(gp.UPLOAD_STEP_KEY, gp._top_level_keys(batches[0]))
        self.assertEqual(batches[-1].count('key: "chart-ome-release"'), 1)
        keys_before = [k for b in batches[:-1] for k in gp._top_level_keys(b)]
        marker = batches[-1][batches[-1].index('key: "chart-ome-release"'):]
        gates = re.findall(r'^      - "([^"]+)"$', marker.split("secrets:")[0], re.M)
        self.assertEqual(sorted(gates), sorted(keys_before))

    def test_marker_refuses_an_empty_gate(self):
        with self.assertRaisesRegex(RuntimeError, "keys it gates on"):
            gp._release_marker_step([])

    def test_component_chart_steps_export_version_artifacts(self):
        for chart in gp.RELEASE_MARKER_CHARTS:
            with self.subTest(chart=chart):
                yaml_text = gp._push_chart_step(chart)
                self.assertIn(
                    f'".buildkite/artifacts/chart-versions/{chart}"',
                    yaml_text,
                )


class TestImageBuildEmission(unittest.TestCase):
    def test_probe_and_generated_build_job_have_automatic_retries(self):
        step_retry = (
            '  retry:\n'
            '    automatic:\n'
            '      - exit_status: "*"\n'
            '        limit: 2'
        )
        probe = gp._build_image_step(
            "ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile"
        )
        self.assertIn(step_retry, probe)
        plugin_step = gp._image_build_plugin_step(
            "ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile",
            key="build-ome-manager-image", tags=["latest"],
        )
        self.assertIn(step_retry, plugin_step)
        plugin_config = plugin_step.split(gp.CF_DOCKER_IMAGE_PLUGIN, 1)[1]
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
        yaml_text = uploaded_build_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
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
        yaml_text = uploaded_build_step("ome-bot", "ome-bot", "dockerfiles/ome-bot.Dockerfile")
        self.assertIn(f'- "KUBECTL_OME_REF={pin}"', yaml_text)
        self.assertNotIn("ALFRED_OSS_REF", yaml_text)


class GitRepoTestCase(PipelineTestCase):
    """Helpers for tests that hash image inputs out of a throwaway git repo."""

    GIT_ENV = {
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_AUTHOR_NAME": "tester",
        "GIT_AUTHOR_EMAIL": "tester@example.com",
        "GIT_COMMITTER_NAME": "tester",
        "GIT_COMMITTER_EMAIL": "tester@example.com",
    }

    def setUp(self):
        if shutil.which("git") is None:
            self.skipTest("git is required to hash image inputs")
        gp._tracked_listing.cache_clear()
        self.addCleanup(gp._tracked_listing.cache_clear)

    def git(self, repo, *args):
        return subprocess.run(
            ["git", "-c", "commit.gpgsign=false", "-C", str(repo), *args],
            check=True, capture_output=True, text=True, env={**os.environ, **self.GIT_ENV},
        ).stdout.strip()

    def make_git_repo(self, files):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        repo = Path(tmp.name)
        self.git(repo, "init", "-q")
        self.commit(repo, files)
        return repo

    def commit(self, repo, files):
        for name, body in files.items():
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body)
        self.git(repo, "add", "-A")
        self.git(repo, "commit", "-q", "-m", "change")
        return self.git(repo, "rev-parse", "HEAD")

    def tag_at(self, repo, label, dockerfile="Dockerfile"):
        """The inputs tag at the repo's current HEAD, keyed by its resolved SHA."""
        self.patch_attr("COMMIT", self.git(repo, "rev-parse", "HEAD"))
        return gp._image_inputs_tag(label, dockerfile)


class TestImageInputs(GitRepoTestCase):
    """The table behind image reuse: complete, tracked and sensitive to its inputs."""

    def test_table_covers_every_image_once_with_a_reason(self):
        gp._check_image_inputs_table()
        self.assertEqual(set(gp.IMAGE_INPUTS), {label for label, _, _ in gp.IMAGES})
        for label, (reason, paths) in gp.IMAGE_INPUTS.items():
            with self.subTest(image=label):
                self.assertTrue(reason.strip(), "every image states why its inputs are what they are")
                self.assertEqual(len(paths), len(set(paths)), "an input is listed twice")

    def test_stale_or_missing_entry_fails_generation(self):
        self.patch_attr("IMAGE_INPUTS", {**gp.IMAGE_INPUTS, "ghost": ("gone", ("go.mod",))})
        with self.assertRaisesRegex(RuntimeError, "ghost"):
            gp._check_image_inputs_table()
        table = {label: entry for label, entry in gp.IMAGE_INPUTS.items() if label not in ("ghost", "ome-manager")}
        self.patch_attr("IMAGE_INPUTS", table)
        with self.assertRaisesRegex(RuntimeError, "ome-manager"):
            gp._check_image_inputs_table()
        with self.assertRaisesRegex(RuntimeError, "ome-manager"):
            gp._image_inputs("ome-manager")

    def test_every_checked_in_image_hashes_from_tracked_inputs(self):
        # The table against the real tree: every input names tracked content,
        # the Dockerfile is among them, and no two images share a tag.
        tags = {}
        for label, _, dockerfile in gp.IMAGES:
            with self.subTest(image=label):
                self.assertIn(dockerfile, gp._image_inputs(label))
                self.assertIn(".dockerignore", gp._image_inputs(label))
                tag = gp._image_inputs_tag(label, dockerfile)
                self.assertRegex(tag, r"^inputs-[0-9a-f]{32}$")
                tags[label] = tag
        self.assertEqual(len(set(tags.values())), len(tags), "two images hash alike")

    def test_dockerfile_copy_sources_are_inputs(self):
        # A COPY from the build context that no input covers would change the
        # image without changing its hash, so the next build would reuse a
        # stale image.
        for label, _, dockerfile in gp.IMAGES:
            inputs = gp._image_inputs(label)
            sources = dockerfile_copy_sources(gp.REPO_ROOT / dockerfile)
            for source in sources:
                with self.subTest(image=label, source=source):
                    self.assertTrue(
                        any(gp._covers(path, source) for path in inputs),
                        f"{dockerfile} copies {source}, which no input of {label} covers",
                    )

    def test_a_change_to_an_input_changes_the_hash_and_one_outside_does_not(self):
        repo = self.make_git_repo({
            "Dockerfile": "FROM scratch\nCOPY src /src\n",
            "src/a.go": "package a\n",
            "docs/readme.md": "hello\n",
        })
        self.patch_attr("REPO_ROOT", repo)
        self.patch_attr("COMMON_IMAGE_INPUTS", ())
        self.patch_attr("IMAGE_INPUTS", {"demo": ("test image", ("Dockerfile", "src"))})
        before = self.tag_at(repo, "demo")
        self.assertRegex(before, r"^inputs-[0-9a-f]{32}$")

        self.commit(repo, {"docs/readme.md": "changed\n"})
        self.assertEqual(self.tag_at(repo, "demo"), before, "a file outside the inputs moved the hash")

        self.commit(repo, {"src/b.go": "package a\n"})
        added = self.tag_at(repo, "demo")
        self.assertNotEqual(added, before, "a new file under an input directory left the hash alone")

        self.commit(repo, {"src/a.go": "package a // edited\n"})
        edited = self.tag_at(repo, "demo")
        self.assertNotEqual(edited, added, "an edit under an input directory left the hash alone")

        self.commit(repo, {"Dockerfile": "FROM scratch\nCOPY src /src\nUSER 65532\n"})
        self.assertNotEqual(self.tag_at(repo, "demo"), edited, "a Dockerfile edit left the hash alone")

    def test_the_recipe_is_part_of_the_hash(self):
        repo = self.make_git_repo({"Dockerfile": "FROM scratch\n"})
        self.patch_attr("REPO_ROOT", repo)
        self.patch_attr("COMMON_IMAGE_INPUTS", ())
        self.patch_attr("IMAGE_INPUTS", {"demo": ("test image", ("Dockerfile",))})
        base = self.tag_at(repo, "demo")
        self.patch_attr("IMAGE_PLATFORMS", ("linux/amd64",))
        fewer_platforms = self.tag_at(repo, "demo")
        self.assertNotEqual(fewer_platforms, base)
        self.patch_attr("GO_BUILDER_IMAGE", "example.com/golang:next")
        self.assertNotEqual(self.tag_at(repo, "demo"), fewer_platforms)
        self.assertNotEqual(self.tag_at(repo, "demo", dockerfile="Other.Dockerfile"), self.tag_at(repo, "demo"))

    def test_an_input_that_matches_nothing_fails_generation(self):
        repo = self.make_git_repo({"Dockerfile": "FROM scratch\n"})
        self.patch_attr("REPO_ROOT", repo)
        self.patch_attr("COMMON_IMAGE_INPUTS", ())
        self.patch_attr("IMAGE_INPUTS", {"demo": ("test image", ("Dockerfile", "missing-dir"))})
        with self.assertRaisesRegex(RuntimeError, "missing-dir"):
            self.tag_at(repo, "demo")

    def test_without_git_reuse_is_off_and_the_plugin_step_owns_the_key(self):
        with mock.patch.object(gp.subprocess, "run", side_effect=FileNotFoundError("git")):
            with contextlib.redirect_stderr(io.StringIO()) as err:
                step = gp._build_image_step("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile")
                eval_step = gp._package_kind_eval_image_step()
        self.assertIn("image reuse off", err.getvalue())
        self.assertIn('key: "build-ome-manager"', step)
        self.assertIn('depends_on: "test"', step)
        self.assertIn(gp.CF_DOCKER_IMAGE_PLUGIN, step)
        self.assertNotIn("build_image.sh", step)
        self.assertNotIn("inputs-", step)
        self.assertNotIn("OME_EVAL_IMAGE_INPUTS_TAG", eval_step)


class TestImageReuseEmission(GitRepoTestCase):
    """The emitted steps carry the hash and the reuse logic."""

    MANAGER = ("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile")
    ALFRED = ("alfred", "alfred", "dockerfiles/alfred.Dockerfile")

    def test_probe_owns_the_build_key_and_names_what_it_compares(self):
        self.patch_attr("BRANCH", "main")
        step = gp._build_image_step(*self.MANAGER)
        tag = gp._image_inputs_tag("ome-manager", "dockerfiles/manager.Dockerfile")
        self.assertIn('key: "build-ome-manager"', step)
        self.assertIn('depends_on: "test"', step)
        self.assertIn(f'OME_IMAGE_INPUTS_TAG: "{tag}"', step)
        self.assertIn(
            'OME_IMAGE_INPUTS: ".dockerignore dockerfiles/manager.Dockerfile go.mod go.sum cmd pkg meta-internal"',
            step,
        )
        self.assertIn(f'OME_IMAGE_PUBLISH_TAGS: "latest {gp.COMMIT[:8]}"', step)
        self.assertIn('- "ECR_AUTH_TOKEN"', step)
        self.assertIn(f'ECR_REGISTRY: "{gp.ECR_REGISTRY}"', step)
        self.assertIn("sh .buildkite/build_image.sh msl_infra/ome-manager", step)
        # The plugin step travels encoded; it must not also run under this key.
        self.assertNotIn(gp.CF_DOCKER_IMAGE_PLUGIN, step)

    def test_uploaded_step_is_the_plugin_build_tagged_with_the_hash(self):
        self.patch_attr("BRANCH", "main")
        uploaded = uploaded_build_step(*self.MANAGER)
        tag = gp._image_inputs_tag("ome-manager", "dockerfiles/manager.Dockerfile")
        self.assertTrue(uploaded.startswith("steps:\n"))
        self.assertIn('key: "build-ome-manager-image"', uploaded)
        self.assertNotIn("depends_on", uploaded)
        self.assertIn(gp.CF_DOCKER_IMAGE_PLUGIN, uploaded)
        self.assertIn('dockerfile: "dockerfiles/manager.Dockerfile"', uploaded)
        self.assertIn(f'tags: ["latest", "{gp.COMMIT[:8]}", "{tag}"]', uploaded)
        self.assertIn('push-branches: "main"', uploaded)
        self.assertNotIn("$", uploaded, "the fragment is uploaded without interpolation")

    def test_publish_tags_follow_the_push_policy(self):
        sha = gp.COMMIT[:8]
        self.allow_heavy()
        self.patch_attr("BRANCH", "feature/anything")
        self.patch_attr("EVAL_MODE", "off")
        self.assertIn('OME_IMAGE_PUBLISH_TAGS: ""', gp._build_image_step(*self.MANAGER))
        self.assertIn(f'OME_IMAGE_PUBLISH_TAGS: "{sha}"', gp._build_image_step(*self.ALFRED))
        # A branch whose eval runs publishes every image's commit tag.
        self.patch_attr("EVAL_MODE", "smoke")
        self.assertIn(f'OME_IMAGE_PUBLISH_TAGS: "{sha}"', gp._build_image_step(*self.MANAGER))
        self.patch_attr("BRANCH", "main")
        self.assertIn(f'OME_IMAGE_PUBLISH_TAGS: "latest {sha}"', gp._build_image_step(*self.MANAGER))
        self.assertIn(f'OME_IMAGE_PUBLISH_TAGS: "latest {sha}"', gp._build_image_step(*self.ALFRED))

    def test_kill_switch_builds_under_the_key_and_still_tags_the_hash(self):
        self.patch_attr("IMAGE_REBUILD", True)
        step = gp._build_image_step(*self.MANAGER)
        tag = gp._image_inputs_tag("ome-manager", "dockerfiles/manager.Dockerfile")
        self.assertIn('key: "build-ome-manager"', step)
        self.assertIn('depends_on: "test"', step)
        self.assertIn(gp.CF_DOCKER_IMAGE_PLUGIN, step)
        self.assertIn(f'"{tag}"]', step)
        self.assertNotIn("build_image.sh", step)
        eval_step = gp._package_kind_eval_image_step()
        self.assertIn('OME_IMAGE_REBUILD: "1"', eval_step)
        self.assertIn("OME_EVAL_IMAGE_INPUTS_TAG", eval_step)

    def test_eval_package_step_hashes_its_source_images(self):
        step = gp._package_kind_eval_image_step()
        tag, sources = gp._eval_image_inputs()
        self.assertRegex(tag, r"^eval-inputs-[0-9a-f]{32}$")
        self.assertEqual(tuple(sources), gp.EVAL_IMAGE_SOURCES)
        self.assertIn(f'OME_EVAL_IMAGE_INPUTS_TAG: "{tag}"', step)
        for label, source_tag in sources.items():
            self.assertIn(f"{label}={source_tag}", step)
        self.assertIn(gp.EVAL_IMAGE_SCRIPT, step)
        self.assertNotIn("OME_IMAGE_REBUILD", step)
        # A source image's hash moving moves the eval hash with it.
        with mock.patch.object(gp, "_image_inputs_tag", side_effect=lambda label, _: f"inputs-{label}"):
            moved, _ = gp._eval_image_inputs()
        self.assertNotEqual(moved, tag)

    def test_main_pipeline_probes_every_image_and_the_eval_image(self):
        self.allow_heavy()
        self.patch_attr("BRANCH", "main")
        self.patch_attr("EVAL_MODE", "smoke")
        self.patch_attr("_eval_children", list)
        pipeline = gp.generate_pipeline()
        for _, ecr_name, _ in gp.IMAGES:
            self.assertIn(f"sh .buildkite/build_image.sh msl_infra/{ecr_name}", pipeline)
        self.assertIn("OME_EVAL_IMAGE_INPUTS_TAG", pipeline)

    def test_scripts_compare_probe_retag_and_upload(self):
        script = (gp.REPO_ROOT / ".buildkite/build_image.sh").read_text()
        for needle in (
            'echo "inputs tag:',
            'echo "inputs hashed:',
            'echo "publish tags:',
            'crane digest "$candidate"',
            'crane tag "$candidate" "$tag"',
            "buildkite-agent pipeline upload --no-interpolation",
            "OME_IMAGE_BUILD_STEP_B64",
        ):
            self.assertIn(needle, script)
        eval_script = (gp.REPO_ROOT / gp.EVAL_IMAGE_SCRIPT).read_text()
        for needle in (
            "OME_EVAL_IMAGE_INPUTS_TAG",
            'echo "inputs hashed:',
            'crane tag "$candidate" "eval-${short_sha}"',
            'crane tag "$target" "$inputs_tag"',
            'OME_IMAGE_REBUILD',
        ):
            self.assertIn(needle, eval_script)
        runner = (gp.REPO_ROOT / ".buildkite/run_omenative_eval_gke.sh").read_text()
        self.assertIn("runtime-image-refs.txt", runner)


class TestBranchImagePublishing(GitRepoTestCase):
    """A branch build publishes its images only when its eval runs. The
    generator decides once (_images_published) and every image step and the
    eval image step carry the decision as OME_PUBLISH_IMAGES; a branch
    publishes commit tags to the dev registry and the eval image is packaged
    from them, while the mirror, chart and marker steps stay main-only."""

    MANAGER = ("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile")
    SHA = "0123456789abcdef0123456789abcdef01234567"

    def setUp(self):
        super().setUp()
        self.allow_heavy()
        self.patch_attr("EVAL_SELECTION", None)
        self.patch_attr("GKE_EVAL_ENABLED", True)
        self.patch_attr("GKE_KIND_ENABLED", True)
        self.patch_attr("GKE_ALFRED_ENABLED", True)
        self.patch_attr("IMAGE_REBUILD", False)

    def on(self, branch, mode):
        self.patch_attr("BRANCH", branch)
        self.patch_attr("EVAL_MODE", mode)

    def test_the_gate_is_main_or_an_eval_that_runs(self):
        for branch, mode, enabled, expected in (
            ("main", "off", True, True),
            ("main", "targeted", True, True),
            ("feature/x", "off", True, False),
            ("feature/x", "smoke", True, True),
            ("feature/x", "full", True, True),
            ("feature/x", "smoke", False, False),
        ):
            with self.subTest(branch=branch, mode=mode, enabled=enabled):
                self.on(branch, mode)
                self.patch_attr("GKE_EVAL_ENABLED", enabled)
                self.assertIs(gp._images_published(), expected)

    def test_without_the_override_a_small_selection_publishes_and_a_heavy_mode_is_refused(self):
        patcher = mock.patch.dict(os.environ)
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop(gp.EVAL_ALLOW_HEAVY_VAR, None)
        self.on("feature/x", "cases")
        self.patch_attr("EVAL_SELECTION", ("cases", [gp._eval_case_groups()[0][2][0]]))
        self.assertIs(gp._images_published(), True)
        pipeline = gp.generate_pipeline_batches()[0]
        self.assertIn('key: "package-kind-eval-image"', pipeline)
        self.assertIn("OME_EVAL_SOURCE_REGISTRY", pipeline)
        self.on("feature/x", "smoke")
        self.patch_attr("EVAL_SELECTION", None)
        with self.assertRaisesRegex(RuntimeError, "unless it carries OME_EVAL_ALLOW_HEAVY=1"):
            gp._images_published()

    def test_a_branch_with_the_eval_off_builds_without_publishing(self):
        self.on("feature/x", "off")
        batches = gp.generate_pipeline_batches()
        self.assertEqual(len(batches), 1)
        pipeline = batches[0]
        self.assertNotIn("package-kind-eval-image", pipeline)
        self.assertNotIn("omenative-e2e-", pipeline)
        self.assertNotIn("OME_EVAL_SOURCE_REGISTRY", pipeline)
        every_branch = gp._push_every_branch_images()
        self.assertEqual(pipeline.count('OME_PUBLISH_IMAGES: "1"'), len(every_branch))
        self.assertEqual(pipeline.count('OME_PUBLISH_IMAGES: "0"'), len(gp.IMAGES) - len(every_branch))
        probe = gp._build_image_step(*self.MANAGER)
        self.assertIn('OME_PUBLISH_IMAGES: "0"', probe)
        self.assertIn('OME_IMAGE_PUBLISH_TAGS: ""', probe)
        uploaded = uploaded_build_step(*self.MANAGER)
        self.assertIn('OME_PUBLISH_IMAGES: "0"', uploaded)
        self.assertIn('push-branches: "main"', uploaded)
        self.assertNotIn("push: true", uploaded)
        self.assertEqual(pipeline.count("mirror_image.sh --only-main"), len(gp.IMAGES))
        self.assertIn('if: build.branch == "main"', pipeline)

    def test_a_branch_whose_eval_runs_publishes_commit_tags_for_it(self):
        self.on("feature/x", "smoke")
        sha = gp.COMMIT[:8]
        batches = gp.generate_pipeline_batches()
        first = batches[0]
        # Every image step and the eval image step say they publish.
        self.assertEqual(first.count('OME_PUBLISH_IMAGES: "1"'), len(gp.IMAGES) + 1)
        self.assertNotIn('OME_PUBLISH_IMAGES: "0"', first)
        probe = gp._build_image_step(*self.MANAGER)
        self.assertIn(f'OME_IMAGE_PUBLISH_TAGS: "{sha}"', probe)
        uploaded = uploaded_build_step(*self.MANAGER)
        inputs = gp._image_inputs_tag("ome-manager", "dockerfiles/manager.Dockerfile")
        self.assertIn(f'tags: ["{sha}", "{inputs}"]', uploaded)
        self.assertNotIn('"latest"', uploaded)
        self.assertIn("push: true", uploaded)
        self.assertNotIn("push-branches", uploaded)
        # The eval image is packaged from the dev-registry commit tags.
        step = gp._package_kind_eval_image_step()
        self.assertIn("(this branch publishes its images for the eval)", step)
        self.assertIn('OME_PUBLISH_IMAGES: "1"', step)
        self.assertIn(f'OME_EVAL_SOURCE_REGISTRY: "{gp.ECR_REGISTRY}"', step)
        for source in gp.EVAL_IMAGE_SOURCES:
            self.assertIn(f'- "build-{source}"', step)
        self.assertNotIn("mirror-", step)
        self.assertIn('- "ECR_AUTH_TOKEN"', step)
        self.assertIn('- "PROD_ECR_AUTH_TOKEN"', step)
        self.assertIn('key: "package-kind-eval-image"', first)
        self.assertIn('key: "omenative-e2e-kind-gke"', batches[1])
        # Nothing else publishes from the branch: the mirror steps keep their
        # main-only switch, the chart script its main gate, the marker its
        # branch condition.
        self.assertEqual(first.count("mirror_image.sh --only-main"), len(gp.IMAGES))
        self.assertNotIn("mirror_image.sh msl_infra", first)
        self.assertIn('key: "chart-ome-release"', batches[-1])
        self.assertIn('if: build.branch == "main"', batches[-1])
        mirror = (gp.REPO_ROOT / ".buildkite/mirror_image.sh").read_text()
        self.assertIn('if [ "${BUILDKITE_BRANCH:-}" != "main" ]', mirror)
        chart = (gp.REPO_ROOT / ".buildkite/push_chart.sh").read_text()
        self.assertIn('if [ "${BUILDKITE_BRANCH:-}" = "main" ]', chart)

    def test_main_publishes_as_before(self):
        self.on("main", "smoke")
        sha = gp.COMMIT[:8]
        probe = gp._build_image_step(*self.MANAGER)
        self.assertIn('OME_PUBLISH_IMAGES: "1"', probe)
        self.assertIn(f'OME_IMAGE_PUBLISH_TAGS: "latest {sha}"', probe)
        uploaded = uploaded_build_step(*self.MANAGER)
        self.assertIn('push-branches: "main"', uploaded)
        self.assertIn(f'tags: ["latest", "{sha}", ', uploaded)
        step = gp._package_kind_eval_image_step()
        self.assertNotIn("this branch publishes", step)
        self.assertIn(f'OME_EVAL_SOURCE_REGISTRY: "{gp.PROD_ECR_REGISTRY}"', step)
        for source in gp.EVAL_IMAGE_SOURCES:
            self.assertIn(f'- "mirror-{source}"', step)
        self.assertNotIn('"build-', step)
        self.assertNotIn('- "ECR_AUTH_TOKEN"', step)
        self.assertIn('- "PROD_ECR_AUTH_TOKEN"', step)

    def test_the_eval_kill_switch_turns_the_branch_publish_off(self):
        self.on("feature/x", "smoke")
        self.patch_attr("GKE_EVAL_ENABLED", False)
        self.assertFalse(gp._images_published())
        self.assertIn('OME_IMAGE_PUBLISH_TAGS: ""', gp._build_image_step(*self.MANAGER))
        self.assertIn('push-branches: "main"', uploaded_build_step(*self.MANAGER))
        self.assertNotIn("package-kind-eval-image", gp.generate_pipeline_batches()[0])

    def test_the_rebuild_switch_keeps_the_branch_publish(self):
        self.on("feature/x", "smoke")
        self.patch_attr("IMAGE_REBUILD", True)
        step = gp._build_image_step(*self.MANAGER)
        self.assertIn('key: "build-ome-manager"', step)
        self.assertIn("push: true", step)
        self.assertIn('OME_PUBLISH_IMAGES: "1"', step)
        self.assertNotIn('"latest"', step)

    # -- the scripts, run with crane stubbed -------------------------------

    CRANE_STUB = (
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >> \"$STUB_LOG\"\n"
        "case \"$1\" in\n"
        "  digest)\n"
        "    case \"$2\" in *:eval-inputs-*) echo MANIFEST_UNKNOWN >&2; exit 1 ;; esac\n"
        "    echo sha256:stub ;;\n"
        "  pull) printf layer > \"$5\" ;;\n"
        "esac\n"
        "exit 0\n"
    )

    def shell_fixture(self):
        """A PATH with crane and buildkite-agent stubbed, and the crane log."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        stubs = root / "bin"
        stubs.mkdir()
        (stubs / "crane").write_text(self.CRANE_STUB)
        (stubs / "crane").chmod(0o755)
        (stubs / "buildkite-agent").write_text("#!/bin/sh\nexit 0\n")
        (stubs / "buildkite-agent").chmod(0o755)
        log = root / "crane.log"
        env = {
            key: value for key, value in os.environ.items()
            if not key.startswith(("OME_", "BUILDKITE_", "AWS_", "PROD_", "ECR_"))
        }
        env.update({
            "PATH": f"{stubs}:{os.environ.get('PATH', '')}",
            "STUB_LOG": str(log),
            "BUILDKITE_COMMIT": self.SHA,
            "AWS_REGION": "us-east-2",
            "CLOUD_OSS_PATH": str(root / "no-cloud-oss"),
            "ECR_AUTH_TOKEN": "stub-dev",
            "PROD_ECR_AUTH_TOKEN": "stub-prod",
        })
        return root, env, log

    def crane_calls(self, log):
        return log.read_text().splitlines() if log.exists() else []

    def run_eval_packager(self, branch, **extra):
        root, env, log = self.shell_fixture()
        env.update({
            "BUILDKITE_BRANCH": branch,
            "PROD_ECR_REGISTRY": "prod.example.com",
            "OME_EVAL_IMAGE_INPUTS_TAG": "eval-inputs-" + "0" * 32,
            "OME_EVAL_IMAGE_INPUTS": "stub",
            **extra,
        })
        result = subprocess.run(
            ["sh", str(gp.REPO_ROOT / gp.EVAL_IMAGE_SCRIPT)],
            env=env, cwd=str(root), capture_output=True, text=True,
        )
        return result, self.crane_calls(log)

    def test_eval_packager_refuses_a_build_that_publishes_nothing(self):
        for extra in ({}, {"OME_PUBLISH_IMAGES": "0"}):
            with self.subTest(env=extra):
                result, calls = self.run_eval_packager("feature/x", **extra)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn("publishes no images", result.stderr)
                self.assertEqual(calls, [])

    def test_eval_packager_on_a_branch_packages_the_dev_registry_commit_tags(self):
        result, calls = self.run_eval_packager(
            "feature/x", OME_PUBLISH_IMAGES="1", OME_EVAL_SOURCE_REGISTRY="dev.example.com"
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        sha = self.SHA[:8]
        self.assertIn("auth login --username AWS --password-stdin prod.example.com", calls)
        self.assertIn("auth login --username AWS --password-stdin dev.example.com", calls)
        pulls = [call for call in calls if call.startswith("pull ")]
        self.assertEqual(
            [call.split()[3] for call in pulls],
            [f"dev.example.com/msl_infra/{image}:{sha}"
             for image in ("ome-manager", "ome-agent", "ome-scheduler", "canary-metrics")],
        )
        append = [call for call in calls if call.startswith("append ")]
        self.assertEqual(len(append), 1)
        self.assertIn(f"--base dev.example.com/msl_infra/kind-ci:{sha}", append[0])
        self.assertIn(f"--new_tag prod.example.com/msl_infra/kind-ci:eval-{sha}", append[0])
        self.assertIn(
            f"tag prod.example.com/msl_infra/kind-ci:eval-{sha} eval-inputs-" + "0" * 32, calls
        )
        self.assertFalse([call for call in calls if "prod.example.com/msl_infra/ome-" in call])

    def test_eval_packager_on_main_packages_the_prod_mirrors_by_default(self):
        result, calls = self.run_eval_packager("main")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        sha = self.SHA[:8]
        logins = [call for call in calls if call.startswith("auth login")]
        self.assertEqual(logins, ["auth login --username AWS --password-stdin prod.example.com"])
        pulls = [call.split()[3] for call in calls if call.startswith("pull ")]
        self.assertEqual(pulls[0], f"prod.example.com/msl_infra/ome-manager:{sha}")
        append = [call for call in calls if call.startswith("append ")][0]
        self.assertIn(f"--base prod.example.com/msl_infra/kind-ci:{sha}", append)

    def run_build_probe(self, branch, **extra):
        root, env, log = self.shell_fixture()
        env.update({
            "BUILDKITE_BRANCH": branch,
            "ECR_REGISTRY": "dev.example.com",
            "OME_IMAGE_INPUTS_TAG": "inputs-" + "0" * 32,
            "OME_IMAGE_INPUTS": "stub",
            "OME_IMAGE_PUBLISH_TAGS": self.SHA[:8],
            "OME_IMAGE_BUILD_STEP_B64": base64.b64encode(b"steps: []\n").decode(),
            **extra,
        })
        result = subprocess.run(
            ["sh", str(gp.REPO_ROOT / ".buildkite/build_image.sh"), "msl_infra/ome-manager"],
            env=env, cwd=str(root), capture_output=True, text=True,
        )
        return result, self.crane_calls(log)

    def test_build_probe_retags_only_when_the_build_publishes(self):
        retag = f"tag dev.example.com/msl_infra/ome-manager:inputs-{'0' * 32} {self.SHA[:8]}"
        for branch, extra, expected in (
            ("feature/x", {}, False),
            ("feature/x", {"OME_PUBLISH_IMAGES": "0"}, False),
            ("feature/x", {"OME_PUBLISH_IMAGES": "1"}, True),
            ("main", {}, True),
        ):
            with self.subTest(branch=branch, env=extra):
                result, calls = self.run_build_probe(branch, **extra)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn(f"digest dev.example.com/msl_infra/ome-manager:inputs-{'0' * 32}", calls)
                self.assertEqual(retag in calls, expected, calls)
                self.assertIn("publishes:     " + ("1" if expected else "0"), result.stdout)


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

    def test_exclusive_cases_take_their_own_waves_within_the_minutes_budget(self):
        """A case batch.py runs on its own is a wave of its own: shards carry
        such cases up to the minutes budget (at least one each) on top of
        their pool waves, whatever the cases' peak pods."""
        cases = [f"p{i:02d}" for i in range(1, 11)]
        peaks = {case_id: 4 for case_id in cases}
        # Without the rule ten 4-pod cases are two waves of four and one of two.
        self.assertEqual(
            [len(s) for s in gp._split_eval_cases(cases, peaks, 4, 64, 4)], [10]
        )
        nine_minutes = {case_id: 9 for case_id in cases[:6]}
        shards = gp._split_eval_cases(
            cases, peaks, 4, 64, 4, exclusive_minutes=nine_minutes, exclusive_budget=20
        )
        # Two nine-minute cases fit a twenty-minute budget, three do not; the
        # last exclusive shard also carries the four pool cases (one wave).
        self.assertEqual(shards, [["p01", "p02"], ["p03", "p04"], ["p05", "p06", "p07", "p08", "p09", "p10"]])
        twelve_minutes = {case_id: 12 for case_id in cases[:3]}
        shards = gp._split_eval_cases(
            cases, peaks, 4, 64, 4, exclusive_minutes=twelve_minutes, exclusive_budget=20
        )
        self.assertEqual([s[:1] for s in shards[:3]], [["p01"], ["p02"], ["p03"]])
        # A case rated above the budget still runs, alone.
        shards = gp._split_eval_cases(
            cases[:2], peaks, 4, 64, 4, exclusive_minutes={"p01": 30}, exclusive_budget=20
        )
        self.assertEqual(shards, [["p01", "p02"]])
        with self.assertRaises(RuntimeError):
            gp._split_eval_cases(cases, peaks, 4, 64, 4, exclusive_minutes=nine_minutes)

    def test_exclusive_rule_matches_the_batch_runner(self):
        """The generator plans a case as exclusive exactly when batch.py runs it
        on its own, over every committed case."""
        perm_dir = gp.REPO_ROOT / "hack" / "omenative-testing" / "perm"
        sys.path.insert(0, str(perm_dir))
        try:
            spec = importlib.util.spec_from_file_location("batch_under_test", perm_dir / "batch.py")
            batch = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(batch)
        finally:
            sys.path.remove(str(perm_dir))
        checked = 0
        for _feature, _topology, cases in gp._eval_case_groups():
            for case_id in cases:
                fields = gp._eval_case_header_fields(case_id)
                self.assertEqual(
                    gp._eval_case_runs_exclusively(fields),
                    batch.cluster_wide_fault(batch.case_header_fields(case_id)),
                    case_id,
                )
                checked += 1
        self.assertGreater(checked, 0)

    def test_every_listed_exclusive_family_is_a_corpus_family_with_exclusive_cases(self):
        """A family rated in EVAL_EXCLUSIVE_CASE_MINUTES exists and has cases the
        rule applies to, so a renamed family does not keep a stale figure."""
        groups = gp._eval_case_groups()
        known = {feature for feature, _, _ in groups}
        for feature, minutes in gp.EVAL_EXCLUSIVE_CASE_MINUTES.items():
            self.assertIn(feature, known)
            self.assertGreater(minutes, 0)
            cases = [c for f, _, cs in groups if f == feature for c in cs]
            self.assertTrue(gp._eval_exclusive_case_minutes(feature, cases), feature)

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
        exclusive_budget = 20
        for feature, topology, variant, _env, cases in gp._eval_lanes():
            pod_budget = 35 if gp._eval_feature_is_canary(feature) else 64
            target_waves = 2 if gp._eval_feature_is_canary(feature) else 4
            exclusive = gp._eval_exclusive_case_minutes(feature, cases)
            shards = gp._split_eval_cases(
                cases,
                {case_id: gp._eval_case_peak_pods(case_id) for case_id in cases},
                concurrency=4,
                pod_budget=pod_budget,
                target_waves=target_waves,
                exclusive_minutes=exclusive,
                exclusive_budget=exclusive_budget,
            )
            shard_counts[(feature, topology, variant)] = len(shards)
            job_count += len(shards)
            for shard_index, shard in enumerate(shards):
                self.assertTrue(shard)
                # The pool part of a shard runs in at most target_waves waves;
                # its exclusive part is within the minutes budget unless it is
                # a single case rated above it.
                pool = [case_id for case_id in shard if case_id not in exclusive]
                waves = gp._split_eval_cases(
                    pool,
                    {case_id: gp._eval_case_peak_pods(case_id) for case_id in pool},
                    concurrency=4,
                    pod_budget=pod_budget,
                    target_waves=1,
                )
                self.assertLessEqual(len(waves), target_waves)
                serial = [case_id for case_id in shard if case_id in exclusive]
                self.assertTrue(
                    len(serial) <= 1 or sum(exclusive[c] for c in serial) <= exclusive_budget,
                    (feature, topology, variant, shard_index, serial),
                )
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
        self.assertEqual(shard_counts[("canary", "pd", "")], 12)
        self.assertEqual(shard_counts[("gen-crashloop", "engine", "")], 3)
        self.assertEqual(shard_counts[("gen-crashloop", "engine-mn", "")], 3)
        self.assertEqual(shard_counts[("gen-crashloop", "pd", "")], 5)
        self.assertEqual(shard_counts[("gen-crashloop", "pd-mn", "")], 5)
        # The switch family carries its breaks beside the edit: each running-
        # revision break before the push at four moments and each pushed-
        # revision break at three, kept and once, on every shape.
        self.assertEqual(shard_counts[("gen-switch", "engine", "")], 11)
        self.assertEqual(shard_counts[("gen-switch", "engine-mn", "")], 11)
        self.assertEqual(shard_counts[("gen-switch", "pd", "")], 9)
        self.assertEqual(shard_counts[("gen-switch", "pd-mn", "")], 9)
        self.assertEqual(shard_counts[("gen-crashloop-idle", "engine", "")], 2)
        self.assertEqual(shard_counts[("gen-crashloop-idle", "engine-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-crashloop-idle", "pd", "")], 2)
        self.assertEqual(shard_counts[("gen-crashloop-idle", "pd-mn", "")], 2)
        # The sibling that breaks: the decoder on the prefill/decode shapes,
        # the router on the engine+router shapes.
        for topology in ("pd", "pd-mn", "engine", "engine-mn"):
            self.assertEqual(shard_counts[("gen-crashloop-sibling", topology, "")], 1)
        for topology in ("engine", "engine-mn", "pd", "pd-mn"):
            self.assertEqual(shard_counts[("gen-heal", topology, "")], 1)
        self.assertEqual(shard_counts[("gen-imagepull", "engine", "")], 1)
        self.assertEqual(shard_counts[("gen-imagepull", "engine-mn", "")], 1)
        self.assertEqual(shard_counts[("gen-imagepull", "pd", "")], 2)
        self.assertEqual(shard_counts[("gen-imagepull", "pd-mn", "")], 2)
        # Node losses run one at a time at 6-11 minutes each: two per shard.
        # The idle half is four shapes deep; the hook half adds the six
        # ledger shapes under every operation, the router shapes riding the
        # engine lanes, and the policy twin carries the relocating rows.
        for topology, plain, policy in (("engine", 17, 13), ("pd", 12, 8), ("engine-mn", 17, 13), ("pd-mn", 12, 8)):
            self.assertEqual(shard_counts[("gen-node", topology, "")], plain)
            self.assertEqual(shard_counts[("gen-node", topology, gp.EVAL_FORCE_DELETE_SLUG)], policy)
        # Manager restarts run one at a time at 2-3 minutes each: six per shard.
        # The hook half adds the restart as a rollback, a strategy edit, a
        # scale-up and a delete land and at a runtime edit's first new pod on
        # the six ledger shapes, the router shapes riding the engine lanes;
        # the idle half adds the manager deleted once on the settled service.
        self.assertEqual(shard_counts[("gen-ctrlrestart", "engine", "")], 6)
        self.assertEqual(shard_counts[("gen-ctrlrestart", "engine-mn", "")], 6)
        self.assertEqual(shard_counts[("gen-ctrlrestart", "pd", "")], 4)
        self.assertEqual(shard_counts[("gen-ctrlrestart", "pd-mn", "")], 4)
        self.assertEqual(shard_counts[("gen-canary-crash", "engine", "")], 4)
        self.assertEqual(shard_counts[("gen-canary-crash", "pd", "")], 12)
        self.assertEqual(shard_counts[("gen-canary-second-rev", "engine", "")], 3)
        self.assertEqual(shard_counts[("gen-canary-second-rev", "pd", "")], 8)
        # The podkill and fault families also carry a canary beside the
        # remaining faults on the six shape families, the gangs included.
        self.assertEqual(shard_counts[("gen-canary-podkill", "engine", "")], 4)
        self.assertEqual(shard_counts[("gen-canary-podkill", "engine-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-canary-podkill", "pd", "")], 7)
        self.assertEqual(shard_counts[("gen-canary-podkill", "pd-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-canary-fault-old", "engine", "")], 26)
        self.assertEqual(shard_counts[("gen-canary-fault-old", "engine-mn", "")], 6)
        self.assertEqual(shard_counts[("gen-canary-fault-old", "pd", "")], 61)
        self.assertEqual(shard_counts[("gen-canary-fault-old", "pd-mn", "")], 6)
        self.assertEqual(shard_counts[("gen-canary-fault-new", "engine", "")], 10)
        self.assertEqual(shard_counts[("gen-canary-fault-new", "engine-mn", "")], 6)
        self.assertEqual(shard_counts[("gen-canary-fault-new", "pd", "")], 19)
        self.assertEqual(shard_counts[("gen-canary-fault-new", "pd-mn", "")], 5)
        self.assertEqual(shard_counts[("gen-canary-fault-gate", "engine", "")], 13)
        self.assertEqual(shard_counts[("gen-canary-fault-gate", "engine-mn", "")], 3)
        self.assertEqual(shard_counts[("gen-canary-fault-gate", "pd", "")], 29)
        self.assertEqual(shard_counts[("gen-canary-fault-gate", "pd-mn", "")], 5)
        # The rollback family carries its breaks beside the rollback: four
        # moments per running-revision break on every shape and the held
        # moment under every strategy.
        self.assertEqual(shard_counts[("gen-rollback", "engine", "")], 10)
        self.assertEqual(shard_counts[("gen-rollback", "engine-mn", "")], 10)
        self.assertEqual(shard_counts[("gen-rollback", "pd", "")], 6)
        self.assertEqual(shard_counts[("gen-rollback", "pd-mn", "")], 6)
        self.assertEqual(shard_counts[("gen-crashloop-new", "engine", "")], 1)
        self.assertEqual(shard_counts[("gen-crashloop-new", "engine-mn", "")], 1)
        self.assertEqual(shard_counts[("gen-crashloop-new", "pd", "")], 2)
        self.assertEqual(shard_counts[("gen-crashloop-new", "pd-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-crashloop-group", "engine", "")], 1)
        self.assertEqual(shard_counts[("gen-crashloop-group", "pd", "")], 2)
        self.assertEqual(shard_counts[("gen-crashloop-group", "pd-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-pause-repair", "engine", "")], 4)
        self.assertEqual(shard_counts[("gen-pause-repair", "engine-mn", "")], 4)
        self.assertEqual(shard_counts[("gen-pause-repair", "pd", "")], 4)
        self.assertEqual(shard_counts[("gen-size", "engine", "")], 3)
        self.assertEqual(shard_counts[("gen-size", "pd", "")], 3)
        self.assertEqual(shard_counts[("gen-router-gang", "engine-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-ops-held", "engine", "")], 4)
        self.assertEqual(shard_counts[("gen-ops-held", "engine-mn", "")], 4)
        self.assertEqual(shard_counts[("gen-ops-held", "pd", "")], 3)
        self.assertEqual(shard_counts[("gen-ops-held", "pd-mn", "")], 3)
        self.assertEqual(shard_counts[("gen-runtime-edit-repair", "engine", "")], 3)
        self.assertEqual(shard_counts[("gen-runtime-edit-repair", "pd", "")], 3)
        self.assertEqual(shard_counts[("gen-migrate-bump", "engine", "")], 3)
        self.assertEqual(shard_counts[("gen-migrate-bump", "engine-mn", "")], 3)
        self.assertEqual(shard_counts[("gen-migrate-bump", "pd", "")], 3)
        self.assertEqual(shard_counts[("gen-migrate-bump", "pd-mn", "")], 3)
        self.assertEqual(shard_counts[("gen-readiness", "engine", "")], 2)
        self.assertEqual(shard_counts[("gen-readiness", "pd", "")], 1)
        self.assertEqual(shard_counts[("gen-readiness", "engine-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-readiness", "pd-mn", "")], 1)
        # The gang's leader deleted as a rollback, a strategy edit, a scale up
        # or down and a delete land and at a runtime edit's first new pod adds
        # a shard to the engine gangs; the prefill/decode gangs absorb theirs.
        self.assertEqual(shard_counts[("gen-gang-member", "engine-mn", "")], 4)
        self.assertEqual(shard_counts[("gen-gang-member", "pd-mn", "")], 2)
        # A squeezed roll runs one at a time and may take its whole convergence
        # budget: one per shard. Evictions run in the pool.
        self.assertEqual(shard_counts[("gen-tight", "engine-mn", "")], 14)
        self.assertEqual(shard_counts[("gen-tight", "pd-mn", "")], 14)
        # The engine topologies carry the engine alone and the engine behind
        # a router; the prefill/decode topologies one shape each. The
        # evictions run in the pool; the preemptions and the pending causes
        # run one at a time.
        for topology, evict, preempt, pending in (("engine", 2, 12, 25), ("engine-mn", 3, 12, 24), ("pd", 1, 6, 13),
                                                  ("pd-mn", 2, 6, 12)):
            self.assertEqual(shard_counts[("gen-evict", topology, "")], evict)
            self.assertEqual(shard_counts[("gen-preempt", topology, "")], preempt)
            self.assertEqual(shard_counts[("gen-pending", topology, "")], pending)
        self.assertEqual(shard_counts[("gen-budget", "engine", "")], 3)
        self.assertEqual(shard_counts[("gen-budget", "engine-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-budget", "pd", "")], 3)
        self.assertEqual(shard_counts[("gen-budget", "pd-mn", "")], 1)
        for topology in ("engine", "pd", "engine-mn"):
            self.assertEqual(shard_counts[("gen-inplaceonly", topology, "")], 1)
        # The engine topologies carry the engine alone and the engine behind
        # a router; a broken-Instance operation waits for the status's word
        # and a settled fleet, so the rows pack a few per shard.
        self.assertEqual(shard_counts[("gen-fault-scale", "engine", "")], 13)
        self.assertEqual(shard_counts[("gen-fault-scale", "engine-mn", "")], 11)
        self.assertEqual(shard_counts[("gen-fault-scale", "pd", "")], 8)
        self.assertEqual(shard_counts[("gen-fault-scale", "pd-mn", "")], 6)
        for topology, shards in (("engine", 5), ("engine-mn", 5), ("pd", 3), ("pd-mn", 3)):
            self.assertEqual(shard_counts[("gen-fault-delete", topology, "")], shards)
        # The move beside the remaining faults rides the plain lanes, the
        # transient node loss and the router's held push with them; the
        # exclusive taint and preemption cells shard on their own minutes.
        for topology, plain, policy in (("engine", 13, 4), ("engine-mn", 13, 4), ("pd", 7, 2), ("pd-mn", 8, 2)):
            self.assertEqual(shard_counts[("gen-fault-move", topology, "")], plain)
            self.assertEqual(shard_counts[("gen-fault-move", topology, gp.EVAL_FORCE_DELETE_SLUG)], policy)
        self.assertEqual(shard_counts[("gen-fault-edit", "engine", "")], 6)
        self.assertEqual(shard_counts[("gen-fault-edit", "engine-mn", "")], 5)
        self.assertEqual(shard_counts[("gen-fault-edit", "pd", "")], 3)
        self.assertEqual(shard_counts[("gen-fault-edit", "pd-mn", "")], 3)
        # A packed pool holds every node for the case: two per shard at the
        # family's seven-minute rating.
        for topology in ("engine", "engine-mn", "pd", "pd-mn"):
            self.assertEqual(shard_counts[("gen-packed-pool", topology, "")], 12)
        # The repeating manager restarts and preemptions run one at a time at
        # about eight minutes each: two per shard; the pod loops run in the pool.
        # The manager restarting again under a rollback, a strategy edit, a
        # runtime edit, a scale-down, a move, a delete and a canary adds seven
        # exclusive cases per shape, the router shapes riding the engine lanes;
        # the pod deleted again under a rollback, a strategy edit, a runtime
        # edit and a scale-down runs in the pool and adds a shard to the
        # single-pod engine and prefill/decode lanes.
        self.assertEqual(shard_counts[("gen-repeat", "engine", "")], 14)
        self.assertEqual(shard_counts[("gen-repeat", "engine-mn", "")], 14)
        self.assertEqual(shard_counts[("gen-repeat", "pd", "")], 8)
        self.assertEqual(shard_counts[("gen-repeat", "pd-mn", "")], 7)
        # A webhook outage runs alone at about six minutes: three per shard.
        # A competing writer stays in the pool. The engine topologies carry
        # the engine alone and the engine behind a router.
        self.assertEqual(shard_counts[("gen-webhook", "engine", "")], 20)
        self.assertEqual(shard_counts[("gen-webhook", "engine-mn", "")], 20)
        self.assertEqual(shard_counts[("gen-webhook", "pd", "")], 10)
        self.assertEqual(shard_counts[("gen-webhook", "pd-mn", "")], 10)
        self.assertEqual(shard_counts[("gen-conflict", "engine", "")], 6)
        self.assertEqual(shard_counts[("gen-conflict", "engine-mn", "")], 6)
        self.assertEqual(shard_counts[("gen-conflict", "pd", "")], 3)
        self.assertEqual(shard_counts[("gen-conflict", "pd-mn", "")], 3)
        # The crash that happens once also lands on a sibling, at idle and
        # under a push; the lift family also pushes the fix after a key or a
        # readiness loss and lifts a pull failure as the bump lands.
        self.assertEqual(shard_counts[("gen-crash-once", "engine", "")], 4)
        self.assertEqual(shard_counts[("gen-crash-once", "engine-mn", "")], 4)
        self.assertEqual(shard_counts[("gen-crash-once", "pd", "")], 2)
        self.assertEqual(shard_counts[("gen-crash-once", "pd-mn", "")], 2)
        self.assertEqual(shard_counts[("gen-lift", "engine", "")], 3)
        self.assertEqual(shard_counts[("gen-lift", "engine-mn", "")], 3)
        self.assertEqual(shard_counts[("gen-lift", "pd", "")], 2)
        self.assertEqual(shard_counts[("gen-lift", "pd-mn", "")], 2)
        self.assertEqual(job_count, 1143)

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
                "OME_EVAL_ALLOW_HEAVY": "1",
            },
        ), mock.patch.object(gp, "EVAL_MODE", "full"):
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
                # the node family, and the node loss at a moment of a move
                self.assertTrue(key.group(1).startswith(("omenative-e2e-gen-node-", "omenative-e2e-gen-fault-move-")),
                                key.group(1))
                twin_cases |= cases
            else:
                self.assertNotIn(gp.EVAL_FORCE_DELETE_ENV, block)
                plain_cases |= cases
        self.assertEqual(twin_cases, needing)
        self.assertFalse(plain_cases & needing)
        # The twin's node losses run one at a time, so it is sharded like
        # the plain lane: the variant names the label and the key.
        self.assertIn("gen-node / engine [1/13] [forcedelete]", yaml_text)
        self.assertIn("gen-node / engine [1/13] [ome-scheduler] [forcedelete]", yaml_text)
        self.assertIn('key: "omenative-e2e-gen-node-engine-ome-scheduler-forcedelete-1-of-13"', yaml_text)

    def test_eval_group_emits_unique_subshard_keys_and_readable_labels(self):
        with mock.patch.dict(
            os.environ,
            {
                "GKE_EVAL_CASE_CONCURRENCY": "4",
                "GKE_EVAL_TARGET_WAVES": "4",
                "GKE_EVAL_CANARY_TARGET_WAVES": "2",
                "OME_EVAL_ALLOW_HEAVY": "1",
            },
        ), mock.patch.object(gp, "EVAL_MODE", "full"):
            yaml_text = gp._gke_eval_group()
        keys = re.findall(r'key: "(omenative-e2e-[^"]+)"', yaml_text)
        self.assertEqual(len(keys), len(set(keys)))
        self.assertIn("canary / engine [1/21]", yaml_text)
        self.assertIn("canary / pd [12/12]", yaml_text)
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
        # Pinned to a branch whose build publishes nothing else.
        self.patch_attr("BRANCH", "feature/anything")
        self.patch_attr("EVAL_MODE", "off")
        alfred = uploaded_build_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
        self.assertIn("push: true", alfred)
        self.assertNotIn("push-branches", alfred)
        manager = uploaded_build_step("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile")
        self.assertIn('push-branches: "main"', manager)
        self.assertNotIn("push: true", manager)

    def test_every_branch_image_tags_latest_only_on_main(self):
        sha = gp.COMMIT[:8]
        self.patch_attr("EVAL_MODE", "off")
        alfred_inputs = gp._image_inputs_tag("alfred", "dockerfiles/alfred.Dockerfile")
        manager_inputs = gp._image_inputs_tag("ome-manager", "dockerfiles/manager.Dockerfile")
        self.patch_attr("BRANCH", "feature/anything")
        alfred = uploaded_build_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
        self.assertIn(f'tags: ["{sha}", "{alfred_inputs}"]', alfred)
        self.assertNotIn('"latest"', alfred)
        manager = uploaded_build_step("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile")
        self.assertIn(f'tags: ["latest", "{sha}", "{manager_inputs}"]', manager)
        self.patch_attr("BRANCH", "main")
        alfred = uploaded_build_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
        self.assertIn(f'tags: ["latest", "{sha}", "{alfred_inputs}"]', alfred)

    def test_package_step_waits_for_the_alfred_build(self):
        step = gp._package_kind_test_image_step()
        self.assertIn('"build-alfred"', step)
        self.assertIn('OME_ALFRED_IMAGE_REQUIRED: "1"', step)
        self.assertIn('"ECR_AUTH_TOKEN"', step)

    def test_lane_is_emitted_by_default(self):
        self.assertIn("kind-gke-alfred-", gp.ci_steps())

    def test_kill_switch_drops_the_lane_and_the_dependency(self):
        self.patch_attr("BRANCH", "feature/anything")
        self.patch_attr("EVAL_MODE", "off")
        self.patch_attr("GKE_ALFRED_ENABLED", False)
        self.assertNotIn("kind-gke-alfred-", gp.ci_steps())
        step = gp._package_kind_test_image_step()
        self.assertNotIn('"build-alfred"', step)
        self.assertNotIn("OME_ALFRED_IMAGE_REQUIRED", step)
        # Nothing consumes a branch build of the image with the lane off, so it
        # publishes on main only like every other image.
        alfred = uploaded_build_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
        self.assertIn('push-branches: "main"', alfred)
        self.assertNotIn("push: true", alfred)

    def test_lane_is_nested_under_the_kind_switch(self):
        self.patch_attr("BRANCH", "feature/anything")
        self.patch_attr("EVAL_MODE", "off")
        self.patch_attr("GKE_KIND_ENABLED", False)
        self.assertNotIn("kind-gke-alfred-", gp.ci_steps())
        alfred = uploaded_build_step("alfred", "alfred", "dockerfiles/alfred.Dockerfile")
        self.assertIn('push-branches: "main"', alfred)


class TestPipelineDependenciesResolve(PipelineTestCase):
    """Every depends_on names a key emitted by the same or an earlier upload.

    A dependency on a key no step emits fails the whole upload rather than
    one lane, so a step dropped by a branch condition or a kill switch must
    take its dependents with it, and a document may not lean on a key that
    a later upload would bring.
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

    def test_every_dependency_names_a_key_of_the_same_or_an_earlier_upload(self):
        self.allow_heavy()
        cases = itertools.product(("main", "feature/anything"), (True, False), (True, False), ("smoke",))
        for branch, kind, alfred, mode in [*cases, ("main", True, True, "off")]:
            with self.subTest(branch=branch, kind=kind, alfred=alfred, mode=mode):
                self.patch_attr("BRANCH", branch)
                self.patch_attr("GKE_KIND_ENABLED", kind)
                self.patch_attr("GKE_ALFRED_ENABLED", alfred)
                self.patch_attr("EVAL_MODE", mode)
                uploaded: set[str] = set()
                for index, document in enumerate(gp.generate_pipeline_batches(), start=1):
                    keys, targets = self._keys_and_dependencies(document)
                    self.assertTrue(keys, f"upload {index}: no step keys found; did the YAML shape change?")
                    if index == 1:
                        self.assertTrue(targets, "no depends_on found; did the YAML shape change?")
                    uploaded |= keys
                    dangling = targets - uploaded
                    self.assertEqual(
                        dangling, set(),
                        f"upload {index}: {sorted(dangling)} are depended on but no step "
                        "of this or an earlier upload emits them",
                    )


class TestEvalTargeted(PipelineTestCase):
    """Targeted mode chooses from the newest recorded full round: the lanes
    that were not PASS under their scheduler, the cases the round never saw,
    the trailer's selection and a seeded sample of the passing lanes; with
    no round to read it runs smoke plus the trailer and says why."""

    ENV = {
        "GKE_EVAL_CASE_CONCURRENCY": "4",
        "GKE_EVAL_TARGET_WAVES": "4",
        "GKE_EVAL_CANARY_TARGET_WAVES": "2",
        "OME_EVAL_ALLOW_HEAVY": "1",
        "OME_EVAL_RESULTS_TOKEN": "fake-token",
        "BUILDKITE_ORGANIZATION_SLUG": "org-a",
        "BUILDKITE_PIPELINE_SLUG": "pipeline-a",
    }
    ROUND = "4321"
    RETIRED = "p9999999"
    STORAGE = "https://storage.example.com/rounds/eval-verdicts.tsv?signature=1"
    DOWNLOAD = "https://api.buildkite.com/v2/organizations/org-a/pipelines/pipeline-a/builds/4321/jobs/j-1/artifacts/a-1/download"

    @classmethod
    def setUpClass(cls):
        cls.gang = gp.EVAL_GANG_SCHEDULER
        cls.groups = gp._eval_case_groups()
        cases = [(feature, topology, case) for feature, topology, group in cls.groups for case in group]
        assert cls.RETIRED not in {case for _, _, case in cases}
        # The round ran everything but the last two cases under both
        # schedulers: four lanes failed, one was skipped, one is parked, one
        # case was skipped under both schedulers, and one row names a case
        # absent from the corpus.
        cls.new_cases = [case for _, _, case in cases[-2:]]
        cls.failed = {
            (cases[0][2], ""): "FAIL",
            (cases[1][2], cls.gang): "INFRA-FAIL",
            (cases[2][2], ""): "TRIGGER_FAIL",
            (cases[3][2], cls.gang): "INIT_TIMEOUT",
        }
        cls.skipped_everywhere = cases[6][2]
        cls.aside = {
            (cases[4][2], ""): ev.SKIP,
            (cases[5][2], cls.gang): ev.XFAIL + " (known bug: parked)",
            (cls.skipped_everywhere, ""): ev.SKIP,
            (cls.skipped_everywhere, cls.gang): ev.SKIP,
        }
        rows = [
            ev.Verdict(
                cls.ROUND, "c" * 40, feature, topology, scheduler, case,
                cls.failed.get((case, scheduler)) or cls.aside.get((case, scheduler)) or ev.PASS,
            )
            for feature, topology, case in cases[:-2]
            for scheduler in ("", cls.gang)
        ]
        rows.append(ev.Verdict(cls.ROUND, "c" * 40, "rollout", "eng", "", cls.RETIRED, "FAIL"))
        cls.passing = {(row.case, row.scheduler) for row in rows if row.verdict == ev.PASS}
        cls.verdicts_text = ev.write_verdicts(rows)
        # What targeted falls back to, packed once.
        with mock.patch.dict(os.environ, cls.ENV), mock.patch.object(gp, "BRANCH", "main"), mock.patch.object(
            gp, "EVAL_SELECTION", None
        ), mock.patch.object(gp, "EVAL_MODE", "smoke"):
            cls.smoke = gp._eval_selection()

    def setUp(self):
        patcher = mock.patch.dict(os.environ, self.ENV)
        patcher.start()
        self.addCleanup(patcher.stop)
        for attr, value in (
            ("BRANCH", "main"),
            ("EVAL_MODE", "targeted"),
            ("EVAL_SELECTION", None),
            ("EVAL_SCHEDULERS", ["", self.gang]),
            ("COMMIT", "a" * 40),
        ):
            self.patch_attr(attr, value)
        gp._eval_round.cache_clear()
        self.addCleanup(gp._eval_round.cache_clear)
        self.calls: list[tuple[str, dict]] = []
        self.err = io.StringIO()
        self.patch_attr("EVAL_ROUND_FETCH", self.fake_fetch())

    def build(self, number, state="passed", marked=True, job="j-1", branch="main"):
        jobs = [{"id": job, "step_key": gp.EVAL_VERDICTS_STEP_KEY, "state": "passed"}] if job else []
        jobs.append({"id": "j-lint", "step_key": "lint", "state": "passed"})
        meta = {ev.ROUND_META_KEY: ev.ROUND_META_VALUE} if marked else {}
        return {"number": number, "state": state, "branch": branch, "meta_data": meta, "jobs": jobs}

    def builds(self):
        """What the API answers: the round is 4321, among builds the lookup
        must pass over (running, canceled, unmarked, marked without the
        recording step) and an older recorded one."""
        return [
            self.build(4340, state="running"),
            self.build(4330, state="canceled"),
            self.build(4325, marked=False),
            self.build(4323, state="failed", job=None),
            self.build(4321, state="failed"),
            self.build(4300, job="j-0"),
        ]

    def fake_fetch(self, builds=None, artifacts=None, text=None):
        builds = self.builds() if builds is None else builds
        artifacts = [{"filename": ev.VERDICTS_FILE, "state": "finished", "download_url": self.DOWNLOAD}] if artifacts is None else artifacts
        text = self.verdicts_text if text is None else text

        def fetch(url, headers):
            self.calls.append((url, dict(headers)))
            path, _, _ = url.partition("?")
            if path.endswith("/builds"):
                return 200, {}, json.dumps(builds).encode()
            if path.endswith("/builds/4321/jobs/j-1/artifacts"):
                return 200, {}, json.dumps(artifacts).encode()
            if path.endswith("/builds/4300/jobs/j-0/artifacts"):
                return 200, {}, json.dumps([{"filename": ev.VERDICTS_FILE, "download_url": self.DOWNLOAD}]).encode()
            if url == self.DOWNLOAD:
                return 302, {"Location": self.STORAGE}, b""
            if url == self.STORAGE:
                return 200, {}, text.encode()
            return 404, {}, b"not found"

        return fetch

    def selection(self):
        with contextlib.redirect_stderr(self.err):
            return gp._eval_selection()

    def label(self):
        with contextlib.redirect_stderr(self.err):
            return gp._eval_mode_label()

    @staticmethod
    def pairs(selection):
        return {(case, scheduler) for _, _, cases, scheduler in selection for case in cases}

    def smoke_selection(self):
        return self.smoke

    def test_failed_lanes_rerun_under_their_scheduler_and_unseen_cases_under_the_default(self):
        with mock.patch.dict(os.environ, {"OME_EVAL_SAMPLE_PERCENT": "0"}):
            selection = self.selection()
        expected = set(self.failed) | {(case, "") for case in self.new_cases}
        self.assertEqual(self.pairs(selection), expected)
        # Emission order is scheduler-major, then the corpus order of lanes.
        self.assertEqual([scheduler for _, _, _, scheduler in selection], sorted((s for _, _, _, s in selection), key=["", self.gang].index))
        homes = {case: (feature, topology) for feature, topology, cases in self.groups for case in cases}
        for feature, topology, cases, _ in selection:
            self.assertEqual({homes[case] for case in cases}, {(feature, topology)})
        self.assertIn(f"reading round {self.ROUND}", self.err.getvalue())

    def test_skipped_and_parked_lanes_are_neither_rerun_nor_sampled(self):
        fixed = set(self.failed) | {(case, "") for case in self.new_cases}
        with mock.patch.dict(os.environ, {"OME_EVAL_SAMPLE_PERCENT": "0"}):
            without_sample = self.pairs(self.selection())
        gp._eval_round.cache_clear()
        with mock.patch.dict(os.environ, {"OME_EVAL_SAMPLE_PERCENT": "100"}):
            with_every_pass = self.pairs(self.selection())
        self.assertEqual(without_sample, fixed)
        self.assertEqual(with_every_pass, fixed | self.passing)
        self.assertEqual(set(self.aside) & with_every_pass, set())
        # A case the round only skipped is not new either.
        self.assertNotIn(self.skipped_everywhere, {case for case, _ in with_every_pass})
        for word in (ev.SKIP, ev.XFAIL, ev.XFAIL + " (known bug: parked)", " SKIP "):
            self.assertTrue(ev.is_set_aside(word), word)
        for word in (ev.PASS, "FAIL", "INFRA-FAIL", "TRIGGER_FAIL", "INIT_TIMEOUT", "", "skip"):
            self.assertFalse(ev.is_set_aside(word), word)

    def test_a_scheduler_the_build_does_not_run_folds_onto_the_default(self):
        self.patch_attr("EVAL_SCHEDULERS", [""])
        with mock.patch.dict(os.environ, {"OME_EVAL_SAMPLE_PERCENT": "0"}):
            pairs = self.pairs(self.selection())
        self.assertEqual(pairs, {(case, "") for case, _ in self.failed} | {(case, "") for case in self.new_cases})

    def test_the_trailer_adds_its_cases_under_the_default_scheduler(self):
        passing_case = next(case for case, scheduler in sorted(self.passing) if scheduler == "" and (case, self.gang) in self.passing)
        self.patch_attr("EVAL_SELECTION", ("cases", [passing_case]))
        with mock.patch.dict(os.environ, {"OME_EVAL_SAMPLE_PERCENT": "0"}):
            pairs = self.pairs(self.selection())
            label = self.label()
        self.assertIn((passing_case, ""), pairs)
        self.assertNotIn((passing_case, self.gang), pairs)
        self.assertEqual(len(pairs), len(self.failed) + len(self.new_cases) + 1)
        self.assertTrue(label.endswith(f" (plus cases: {passing_case})"), label)
        family = self.groups[0][0]
        self.patch_attr("EVAL_SELECTION", ("families", [family]))
        with mock.patch.dict(os.environ, {"OME_EVAL_SAMPLE_PERCENT": "0"}):
            pairs = self.pairs(self.selection())
        for feature, _, cases in self.groups:
            if feature == family:
                self.assertTrue({(case, "") for case in cases} <= pairs)

    def test_the_trailer_adds_its_lanes_under_their_schedulers(self):
        feature_a, topology_a, cases_a = self.groups[0]
        feature_b, topology_b, cases_b = self.groups[-1]
        lane_a = f"{feature_a}/{topology_a}@{self.gang}"
        lane_b = f"{feature_b}/{topology_b}"
        named = {(case, self.gang) for case in cases_a} | {(case, "") for case in cases_b}
        self.patch_attr("EVAL_SELECTION", ("lanes", [lane_a, lane_b]))
        with mock.patch.dict(os.environ, {"OME_EVAL_SAMPLE_PERCENT": "0"}):
            pairs = self.pairs(self.selection())
            label = self.label()
        self.assertEqual(pairs, set(self.failed) | {(case, "") for case in self.new_cases} | named)
        self.assertTrue(label.endswith(f" (plus lanes: {lane_a}, {lane_b})"), label)
        # Without a round, targeted is smoke plus exactly what lanes= runs on its own.
        gp._eval_round.cache_clear()
        with mock.patch.dict(os.environ, {"OME_EVAL_RESULTS_TOKEN": ""}):
            fallback = self.pairs(self.selection())
        with mock.patch.object(gp, "EVAL_MODE", "lanes"):
            standalone = self.selection()
        self.assertEqual(self.pairs(standalone), named)
        self.assertEqual([scheduler for _, _, _, scheduler in standalone], ["", self.gang])
        self.assertEqual(fallback, self.pairs(self.smoke_selection()) | named)
        # The twin the build does not run is refused beside targeted as well.
        self.patch_attr("EVAL_SCHEDULERS", [""])
        gp._eval_round.cache_clear()
        with self.assertRaisesRegex(RuntimeError, "does not run.*" + re.escape(lane_a)):
            self.selection()

    def test_the_sample_is_sized_by_the_percent_rounded_up_and_seeded_by_the_commit(self):
        fixed = set(self.failed) | {(case, "") for case in self.new_cases}
        candidates = len(self.passing)
        for raw, share in (("", "5"), ("2.5", "2.5"), ("0", "0"), (" 10 ", "10")):
            with self.subTest(percent=raw):
                gp._eval_round.cache_clear()
                with mock.patch.dict(os.environ, {"OME_EVAL_SAMPLE_PERCENT": raw}):
                    pairs = self.pairs(self.selection())
                sampled = pairs - fixed
                self.assertTrue(fixed <= pairs)
                self.assertTrue(sampled <= self.passing)
                self.assertEqual(len(sampled), -(-candidates * int(float(share) * 10) // 1000))
        gp._eval_round.cache_clear()
        again = self.pairs(self.selection())
        gp._eval_round.cache_clear()
        with mock.patch.object(gp, "COMMIT", "b" * 40):
            other = self.pairs(self.selection())
        self.assertEqual(again, self.pairs(self.selection()))
        self.assertNotEqual(again, other)
        self.assertEqual(len(again), len(other))

    def test_a_bad_sample_percent_is_refused(self):
        for raw in ("abc", "-1", "101", "1/0", "nan"):
            with self.subTest(percent=raw), mock.patch.dict(os.environ, {"OME_EVAL_SAMPLE_PERCENT": raw}):
                with self.assertRaisesRegex(RuntimeError, "OME_EVAL_SAMPLE_PERCENT"):
                    self.selection()

    def test_the_label_names_the_round_and_the_count(self):
        selection = self.selection()
        runs = sum(len(cases) for _, _, cases, _ in selection)
        self.assertGreater(runs, len(self.failed) + len(self.new_cases))
        self.assertEqual(self.label(), f" (targeted from round {self.ROUND}: {runs} cases)")

    def test_the_lookup_reads_finished_main_builds_and_sends_the_token_to_the_api_only(self):
        self.selection()
        urls = [url for url, _ in self.calls]
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(urls[0]).query)
        self.assertEqual(query["branch"], ["main"])
        self.assertEqual(sorted(query["state[]"]), ["failed", "passed"])
        self.assertEqual(query[f"meta_data[{ev.ROUND_META_KEY}]"], [ev.ROUND_META_VALUE])
        self.assertEqual(query["per_page"], [str(gp.EVAL_ROUND_PAGE)])
        prefix = f"{gp.EVAL_ROUND_API}/organizations/org-a/pipelines/pipeline-a/builds"
        self.assertEqual(urls[1:], [f"{prefix}/4321/jobs/j-1/artifacts", self.DOWNLOAD, self.STORAGE])
        for url, headers in self.calls:
            if url.startswith(gp.EVAL_ROUND_API):
                self.assertEqual(headers.get("Authorization"), "Bearer fake-token", url)
            else:
                self.assertNotIn("Authorization", headers, url)
        # The rounds may run in a pipeline of their own.
        gp._eval_round.cache_clear()
        self.calls.clear()
        with mock.patch.dict(os.environ, {"OME_EVAL_RESULTS_PIPELINE": "pipeline-b"}):
            self.assertTrue(self.label().startswith(f" (targeted from round {self.ROUND}: "))
        self.assertIn("/pipelines/pipeline-b/builds?", self.calls[0][0])

    def test_without_a_token_targeted_runs_smoke_plus_the_trailer_and_says_so(self):
        with mock.patch.dict(os.environ, {"OME_EVAL_RESULTS_TOKEN": ""}):
            self.assertEqual(self.selection(), self.smoke_selection())
            self.assertEqual(self.label(), " (targeted: smoke, no results token)")
            self.assertEqual(self.calls, [])
            self.assertIn("no results token", self.err.getvalue())
            gp._eval_round.cache_clear()
            case = self.groups[-1][2][-1]
            self.patch_attr("EVAL_SELECTION", ("cases", [case]))
            self.assertEqual(self.pairs(self.selection()), self.pairs(self.smoke_selection()) | {(case, "")})
            self.assertEqual(self.label(), f" (targeted: smoke, no results token) (plus cases: {case})")

    def test_without_a_recorded_round_or_with_a_failed_lookup_targeted_falls_back(self):
        def raising(url, headers):
            raise OSError("connection refused")

        def failing(url, headers):
            return 500, {}, b"internal error"

        fallbacks = [
            ("no builds", self.fake_fetch(builds=[]), "no recorded round"),
            ("only unusable builds", self.fake_fetch(builds=self.builds()[:4]), "no recorded round"),
            ("no verdict artifact", self.fake_fetch(builds=[self.build(4321)], artifacts=[]), "no recorded round"),
            ("a branch build", self.fake_fetch(builds=[self.build(4321, branch="feature/x")]), "no recorded round"),
            ("a connection error", raising, "round lookup failed"),
            ("a server error", failing, "round lookup failed"),
            ("a malformed verdict file", self.fake_fetch(text="not\ta\tverdict\tfile\n"), "round lookup failed"),
        ]
        for name, fetch, reason in fallbacks:
            with self.subTest(name):
                gp._eval_round.cache_clear()
                self.err = io.StringIO()
                self.patch_attr("EVAL_ROUND_FETCH", fetch)
                self.assertEqual(self.selection(), self.smoke_selection())
                self.assertEqual(self.label(), f" (targeted: smoke, {reason})")
                self.assertIn(reason, self.err.getvalue())
        gp._eval_round.cache_clear()
        with mock.patch.dict(os.environ, {"BUILDKITE_PIPELINE_SLUG": ""}):
            self.assertEqual(self.label(), " (targeted: smoke, round lookup failed)")
        # No fallback reason ever reaches the token.
        self.assertNotIn("fake-token", self.err.getvalue())

    def test_a_targeted_build_from_a_round_uploads_within_the_limit_and_records_nothing(self):
        with contextlib.redirect_stderr(self.err):
            batches = gp.generate_pipeline_batches()
        text = "".join(batches)
        self.assertNotIn(f'key: "{gp.EVAL_VERDICTS_STEP_KEY}"', text)
        self.assertIn(f"(targeted from round {self.ROUND}: ", text)
        for document in batches:
            self.assertLessEqual(gp._job_count(document), gp.UPLOAD_JOB_LIMIT)
        self.assertEqual(gp._top_level_keys(batches[-1]), ["chart-ome-release"])
        # The fixture round, 5% sampled under the pinned commit: the pin
        # moves with the corpus like the full and smoke totals.
        self.assertEqual(sum(gp._job_count(document) for document in batches), 420)


class TestEvalVerdicts(unittest.TestCase):
    """eval_verdicts.py records a round from the lanes' result archives: one
    row per (case, scheduler) the round ran, in the format the generator's
    targeted lookup reads back."""

    BUILD = "4321"
    COMMIT = "0123456789abcdef0123456789abcdef01234567"

    @classmethod
    def setUpClass(cls):
        # The harness's own result header, so the fixture rows are shaped
        # exactly as run.py writes them.
        text = (gp.REPO_ROOT / "hack/omenative-testing/perm/run.py").read_text()
        match = re.search(r"^TSV_HEADER = \((.*?)\)\n", text, re.M | re.S)
        cls.header = ast.literal_eval("(" + match.group(1) + ")")
        cls.columns = cls.header.rstrip("\n").split("\t")

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.archives = self.root / "results"
        self.archives.mkdir()
        self.warnings: list[str] = []

    def result_text(self, case, verdict, columns=None):
        """One case result file as run.py writes it: the header, one row."""
        columns = columns or self.columns
        row = {name: "0" for name in columns}
        row.update(id=case, desc="fixture", policy="none", verdict=verdict, note="")
        return "\t".join(columns) + "\n" + "\t".join(row[name] for name in columns) + "\n"

    def make_archive(self, job, feature, topology, scheduler, results, lane_record=True, force_delete=""):
        """One lane job's archive: results is {member name: text} under the
        job's directory, as the harness lays them out."""
        stamp = f"{self.BUILD}-{job}"
        job_dir = self.root / stamp
        if lane_record:
            job_dir.mkdir(parents=True, exist_ok=True)
            (job_dir / ev.LANE_FILE).write_text(
                "\t".join(ev.LANE_COLUMNS) + "\n" + "\t".join((feature, topology, scheduler, force_delete)) + "\n"
            )
        for name, text in results.items():
            path = job_dir / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        archive = self.archives / f"{stamp}.tgz"
        with tarfile.open(archive, "w:gz") as tar:
            tar.add(job_dir, arcname=stamp)
        return archive

    def lane(self, job, feature, topology, scheduler, verdicts, **kwargs):
        """A lane job whose cases ended in the given verdicts."""
        results = {
            f"{feature}/{topology}/{case}-100.tsv": self.result_text(case, verdict)
            for case, verdict in verdicts.items()
        }
        results[f"junit-{feature}-{topology}.xml"] = "<testsuite/>"
        return self.make_archive(job, feature, topology, scheduler, results, **kwargs)

    def collect(self):
        return ev.collect(sorted(self.archives.glob("*.tgz")), self.BUILD, self.COMMIT, self.warnings.append)

    def test_a_round_is_recorded_one_row_per_lane_the_round_ran(self):
        self.lane("a", "rollout", "eng", "", {"p1": "PASS", "p2": "FAIL"})
        self.lane("b", "rollout", "eng", gp.EVAL_GANG_SCHEDULER, {"p1": "PASS", "p2": "INFRA-FAIL"})
        self.lane("c", "canary", "eng-mn", "", {"p3": "TRIGGER_FAIL", "p4": "INIT_TIMEOUT"})
        # The lane migration/eng ran too, but its archive never arrived.
        rows = self.collect()
        expected = [
            ev.Verdict(self.BUILD, self.COMMIT, "canary", "eng-mn", "", "p3", "TRIGGER_FAIL"),
            ev.Verdict(self.BUILD, self.COMMIT, "canary", "eng-mn", "", "p4", "INIT_TIMEOUT"),
            ev.Verdict(self.BUILD, self.COMMIT, "rollout", "eng", "", "p1", "PASS"),
            ev.Verdict(self.BUILD, self.COMMIT, "rollout", "eng", "", "p2", "FAIL"),
            ev.Verdict(self.BUILD, self.COMMIT, "rollout", "eng", gp.EVAL_GANG_SCHEDULER, "p1", "PASS"),
            ev.Verdict(self.BUILD, self.COMMIT, "rollout", "eng", gp.EVAL_GANG_SCHEDULER, "p2", "INFRA-FAIL"),
        ]
        self.assertEqual(rows, expected)
        self.assertEqual(self.warnings, [])
        # The file round-trips through the one format definition.
        text = ev.write_verdicts(rows)
        self.assertEqual(text.splitlines()[0], "\t".join(ev.COLUMNS))
        self.assertEqual(ev.read_verdicts(text), expected)

    def test_a_malformed_result_is_skipped_with_a_warning(self):
        short = "\t".join(self.columns) + "\n" + "\t".join(["p2"] + ["0"] * 9) + "\n"
        self.lane("a", "rollout", "eng", "", {"p1": "PASS"})
        self.make_archive("b", "rollout", "eng", gp.EVAL_GANG_SCHEDULER, {
            "rollout/eng/p1-100.tsv": self.result_text("p1", "PASS"),
            "rollout/eng/p2-100.tsv": short,
            "rollout/eng/p3-100.tsv": self.result_text("p9", "PASS"),
            "rollout/eng/p4-100.tsv": "",
            "rollout/eng/p5-100.tsv": self.result_text("p5", ""),
            "rollout/eng/p6-100.tsv": self.result_text("p6", "PASS", columns=["id", "desc"]),
        })
        rows = self.collect()
        self.assertEqual([(row.scheduler, row.case, row.verdict) for row in rows],
                         [("", "p1", "PASS"), (gp.EVAL_GANG_SCHEDULER, "p1", "PASS")])
        named = sorted(re.search(r"/(p[0-9]+)-100\.tsv", warning).group(1) for warning in self.warnings)
        self.assertEqual(named, ["p2", "p3", "p4", "p5", "p6"])

    def test_the_newest_result_of_a_case_wins_within_an_archive(self):
        self.make_archive("a", "rollout", "eng", "", {
            "rollout/eng/p1-100.tsv": self.result_text("p1", "FAIL"),
            "rollout/eng/p1-200.tsv": self.result_text("p1", "PASS"),
            "rollout/eng/p2-300.tsv": self.result_text("p2", "FAIL"),
            "rollout/eng/p2-250.tsv": self.result_text("p2", "PASS"),
        })
        rows = self.collect()
        self.assertEqual([(row.case, row.verdict) for row in rows], [("p1", "PASS"), ("p2", "FAIL")])
        self.assertEqual(self.warnings, [])

    def test_an_archive_that_cannot_be_read_yields_nothing(self):
        self.lane("a", "rollout", "eng", "", {"p1": "PASS"})
        self.lane("b", "rollout", "eng", "", {"p2": "PASS"}, lane_record=False)
        (self.archives / f"{self.BUILD}-c.tgz").write_bytes(b"not an archive")
        rows = self.collect()
        self.assertEqual([row.case for row in rows], ["p1"])
        self.assertEqual(len(self.warnings), 2)
        self.assertIn(f"{self.BUILD}-b.tgz", self.warnings[0])
        self.assertIn(ev.LANE_FILE, self.warnings[0])
        self.assertIn(f"{self.BUILD}-c.tgz", self.warnings[1])

    def test_the_verdict_file_refuses_a_header_or_row_it_cannot_read(self):
        with self.assertRaisesRegex(ValueError, "header lacks"):
            ev.read_verdicts("build\tcase\n1\tp1\n")
        with self.assertRaisesRegex(ValueError, "line 2"):
            ev.read_verdicts("\t".join(ev.COLUMNS) + "\n1\tabc\trollout\n")
        with self.assertRaisesRegex(ValueError, "tab"):
            ev.write_verdicts([ev.Verdict("1", "a", "rollout", "eng", "", "p1", "FAIL\tnote")])

    def test_the_command_line_writes_the_file_and_refuses_an_empty_round(self):
        out = self.root / ev.VERDICTS_FILE
        err, log = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(log):
            status = ev.main(["--archives", str(self.archives), "--out", str(out), "--build", self.BUILD, "--commit", self.COMMIT])
        self.assertEqual(status, 1)
        self.assertFalse(out.exists())
        self.assertIn("nothing to record", err.getvalue())
        self.lane("a", "rollout", "eng", "", {"p1": "PASS", "p2": "FAIL"})
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(log):
            status = ev.main(["--archives", str(self.archives), "--out", str(out), "--build", self.BUILD, "--commit", self.COMMIT])
        self.assertEqual(status, 0)
        self.assertEqual([(row.case, row.verdict) for row in ev.read_verdicts(out.read_text())], [("p1", "PASS"), ("p2", "FAIL")])
        self.assertIn("2 verdicts from 1 archives, 1 PASS, 0 skipped or parked, 1 other", log.getvalue())

    def test_the_runner_and_the_recording_step_agree_with_the_script(self):
        runner = (Path(__file__).resolve().parent / "run_omenative_eval_gke.sh").read_text()
        self.assertIn("/" + ev.LANE_FILE + '"', runner)
        self.assertIn("'" + "\\t".join(ev.LANE_COLUMNS) + "\\n", runner)
        self.assertIn(f'results_root="{ev.RESULTS_DIR}/', runner)
        recorder = (Path(__file__).resolve().parent / "record_eval_verdicts.sh").read_text()
        self.assertIn("python3 .buildkite/eval_verdicts.py", recorder)
        for name in ("OME_EVAL_RESULTS_DIR", "OME_EVAL_VERDICTS_FILE", "OME_EVAL_ROUND_META_KEY", "OME_EVAL_ROUND_META_VALUE"):
            self.assertIn("${" + name + ":?", recorder)
        self.assertLess(recorder.index("artifact upload"), recorder.index("meta-data set"))


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
                "OME_EVAL_ALLOW_HEAVY": "1",
            },
        ), mock.patch.object(gp, "EVAL_MODE", "full"):
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


class TestEvalSoftFeatures(unittest.TestCase):
    """A family on the eval soft list runs on every main build and stays advisory
    on both schedulers until its expiry; every other family gates the build."""

    def test_every_listed_feature_is_a_corpus_feature_with_an_owner_and_expiry(self):
        soft = gp._eval_soft_features()
        known = {feature for feature, _, _ in gp._eval_case_groups()}
        self.assertTrue(soft, "the soft list is empty; delete this test with the list")
        for feature, expiry in soft.items():
            self.assertIn(feature, known)
            self.assertRegex(expiry, r"^\d{4}-\d{2}-\d{2}$")

    def test_listed_features_are_advisory_on_both_schedulers_and_others_gate(self):
        soft = gp._eval_soft_features()
        with mock.patch.dict(
            os.environ,
            {
                "GKE_EVAL_CASE_CONCURRENCY": "4",
                "GKE_EVAL_TARGET_WAVES": "4",
                "GKE_EVAL_CANARY_TARGET_WAVES": "2",
                "OME_EVAL_ALLOW_HEAVY": "1",
            },
        ), mock.patch.object(gp, "EVAL_MODE", "full"):
            yaml_text = gp._gke_eval_group()
        seen_soft = seen_hard = 0
        for match in re.finditer(r'- label: "([^"]*)"(.*?)(?=\n\s*- label: "|\Z)', yaml_text, re.S):
            label, body = match.group(1), match.group(2)
            command = re.search(r'run_omenative_eval_gke\.sh ([^"]+)"', body)
            if command is None:
                continue
            feature = command.group(1).split()[0]
            twin = "ome-scheduler" in label
            if feature in soft:
                self.assertIn("soft_fail: true", body, label)
                self.assertIn("advisory until", label)
                seen_soft += 1
            elif not twin:
                self.assertNotIn("soft_fail", body, label)
                seen_hard += 1
        self.assertGreater(seen_soft, 0)
        self.assertGreater(seen_hard, 0)



class TestEvalMode(PipelineTestCase):
    """OME_EVAL_MODE, else the trailer (OME_EVAL_SELECT, else the newest
    Eval: line of the message), else off says what the eval group runs;
    _eval_selection is the one place a mode chooses cases, and the emission
    packs whatever it returns. The eval shares its cluster with production
    work, so without OME_EVAL_ALLOW_HEAVY=1 a build runs only cases= or
    lanes= within OME_EVAL_MAX_CASES; the heavy renders here carry it."""

    EVAL_ENV = {
        "GKE_EVAL_CASE_CONCURRENCY": "4",
        "GKE_EVAL_TARGET_WAVES": "4",
        "GKE_EVAL_CANARY_TARGET_WAVES": "2",
        "OME_EVAL_ALLOW_HEAVY": "1",
    }
    RUNNER = "run_omenative_eval_gke.sh"

    @classmethod
    def setUpClass(cls):
        with mock.patch.dict(os.environ, cls.EVAL_ENV), mock.patch.object(
            gp, "BRANCH", "main"
        ), mock.patch.object(gp, "EVAL_SELECTION", None):
            cls.groups = gp._eval_case_groups()
            with mock.patch.object(gp, "EVAL_MODE", "full"):
                cls.full = gp._eval_children()
            with mock.patch.object(gp, "EVAL_MODE", "smoke"):
                cls.smoke = gp._eval_children()
                cls.smoke_batches = gp.generate_pipeline_batches()

    def setUp(self):
        patcher = mock.patch.dict(os.environ, self.EVAL_ENV)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.patch_attr("BRANCH", "main")
        self.patch_attr("EVAL_SELECTION", None)

    @classmethod
    def lane_of(cls, child):
        """(feature, topology) of a lane job, from the runner's arguments."""
        parts = child["command"].split()
        return parts[2], parts[3]

    @classmethod
    def cases_of(cls, child):
        return child["command"].split()[4:]

    def runner_jobs(self, children, scheduler=None):
        return [
            child
            for owner, _, child in children
            if self.RUNNER in child["command"] and (scheduler is None or owner == scheduler)
        ]

    def group_labels(self, children):
        with mock.patch.object(gp, "_eval_children", lambda: children):
            return [group["group"] for group in gp._gke_eval_groups()]

    def test_the_environment_wins_over_the_trailer_and_no_source_is_off(self):
        resolve = gp._resolve_eval_mode
        self.assertEqual(resolve("full", "Eval: smoke", True), ("full", None))
        self.assertEqual(resolve(" Smoke ", "", True), ("smoke", None))
        self.assertEqual(resolve(None, "Eval: smoke", True), ("smoke", None))
        # No source at all is off, on main as on every other branch.
        self.assertEqual(resolve(None, "", True), ("off", None))
        # An empty variable is unset; the kill switch forces off from any source.
        self.assertEqual(resolve("", "", True), ("off", None))
        self.assertEqual(resolve("full", "Eval: full", False), ("off", None))
        self.assertEqual(resolve(None, "Eval: cases=p3000", False), ("off", ("cases", ["p3000"])))
        # The trailer's selection is kept beside a mode the environment set.
        self.assertEqual(
            resolve("targeted", "Eval: families=gen-heal", True),
            ("targeted", ("families", ["gen-heal"])),
        )

    def without_override(self):
        """The build as a push or a merge makes it: neither the override nor
        a cap of its own in the environment."""
        patcher = mock.patch.dict(os.environ)
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop(gp.EVAL_ALLOW_HEAVY_VAR, None)
        os.environ.pop(gp.EVAL_MAX_CASES_VAR, None)

    @staticmethod
    def runs_of(selection):
        return sum(len(cases) for _, _, cases, _ in selection)

    def test_a_heavy_mode_is_refused_without_the_override_and_runs_with_it(self):
        self.without_override()
        rule = (
            r"runs more than a small named selection: the eval shares its cluster with"
            r" production work.*cases=<ids> or lanes=.*unless it carries OME_EVAL_ALLOW_HEAVY=1"
        )
        for mode, selection in (
            ("smoke", None),
            ("targeted", None),
            ("full", None),
            ("families", ("families", ["gen-heal"])),
            ("targeted", ("cases", ["p3000"])),
        ):
            with self.subTest(mode=mode, selection=selection):
                self.patch_attr("EVAL_MODE", mode)
                self.patch_attr("EVAL_SELECTION", selection)
                with self.assertRaisesRegex(RuntimeError, "names " + repr(mode) + ", which " + rule):
                    gp._eval_mode()
                with self.assertRaisesRegex(RuntimeError, rule):
                    gp.generate_pipeline_batches()
                # On a branch the image gate asks whether the eval runs, and is refused too.
                self.patch_attr("BRANCH", "feature/x")
                with self.assertRaisesRegex(RuntimeError, rule):
                    gp._images_published()
                self.patch_attr("BRANCH", "main")
                with mock.patch.dict(os.environ, {gp.EVAL_ALLOW_HEAVY_VAR: "1"}):
                    self.assertEqual(gp._eval_mode(), mode)
                    self.assertTrue(gp._eval_on())
                # The kill switch is the strongest off: nothing is left to refuse.
                self.patch_attr("GKE_EVAL_ENABLED", False)
                self.assertEqual(gp._eval_mode(), "off")
                self.patch_attr("GKE_EVAL_ENABLED", True)

    def test_a_small_named_selection_runs_on_main_and_on_a_branch_within_the_cap(self):
        self.without_override()
        lanes = {(f, t): cases for f, t, cases in self.groups}
        smallest = min(lanes, key=lambda lane: (len(lanes[lane]), lane))
        named = "/".join(smallest)
        two = [self.groups[0][2][0], self.groups[1][2][0]]
        for mode, selection, runs in (
            ("cases", ("cases", two), 2),
            ("lanes", ("lanes", [named, named + "@" + gp.EVAL_GANG_SCHEDULER]), 2 * len(lanes[smallest])),
        ):
            for branch in ("main", "feature/x"):
                with self.subTest(mode=mode, branch=branch):
                    self.patch_attr("BRANCH", branch)
                    self.patch_attr("EVAL_MODE", mode)
                    self.patch_attr("EVAL_SELECTION", selection)
                    self.assertLessEqual(runs, gp.EVAL_MAX_CASES_DEFAULT)
                    self.assertEqual(self.runs_of(gp._eval_selection()), runs)
                    self.assertTrue(gp._eval_on())
                    self.assertTrue(gp._images_published())
                    # One eval group per scheduler the selection names, each its own upload.
                    batches = gp.generate_pipeline_batches()
                    self.assertEqual(len(batches), 2 + len({s for _, _, _, s in gp._eval_selection()}))
                    self.assertIn('key: "package-kind-eval-image"', batches[0])
                    self.assertIn('key: "omenative-e2e-kind-gke', batches[1])
                    self.assertIn("(KIND on GKE) (" + mode + ": ", batches[1])

    def test_a_selection_above_the_case_cap_is_refused_and_the_override_lifts_it(self):
        self.without_override()
        self.assertEqual(gp.EVAL_MAX_CASES_DEFAULT, 24)
        self.assertEqual(gp._eval_max_cases(), 24)
        lanes = {(f, t): cases for f, t, cases in self.groups}
        biggest = max(lanes, key=lambda lane: (len(lanes[lane]), lane))
        big = lanes[biggest]
        self.assertGreater(len(big), 24)
        named = "/".join(biggest)
        refused = (
            r"the Eval: trailer or OME_EVAL_SELECT names {runs} case runs \(cases times schedulers\),"
            r" above the {cap} of OME_EVAL_MAX_CASES: the eval shares its cluster with production work"
            r".*unless it carries OME_EVAL_ALLOW_HEAVY=1; name fewer with cases=<ids>"
        )
        # Under the default cap a whole big lane is refused, at generation too,
        # while a subset of its cases within the cap runs.
        self.patch_attr("EVAL_MODE", "lanes")
        self.patch_attr("EVAL_SELECTION", ("lanes", [named]))
        with self.assertRaisesRegex(RuntimeError, refused.format(runs=len(big), cap=24)):
            gp._eval_selection()
        with self.assertRaisesRegex(RuntimeError, refused.format(runs=len(big), cap=24)):
            gp.generate_pipeline_batches()
        self.patch_attr("EVAL_MODE", "cases")
        self.patch_attr("EVAL_SELECTION", ("cases", big[:24]))
        self.assertEqual(self.runs_of(gp._eval_selection()), 24)
        self.patch_attr("EVAL_SELECTION", ("cases", big[:25]))
        with self.assertRaisesRegex(RuntimeError, refused.format(runs=25, cap=24)):
            gp._eval_selection()
        # A case counts once per scheduler it is named under.
        self.patch_attr("EVAL_MODE", "lanes")
        self.patch_attr("EVAL_SELECTION", ("lanes", [named, named + "@" + gp.EVAL_GANG_SCHEDULER]))
        with mock.patch.dict(os.environ, {gp.EVAL_MAX_CASES_VAR: str(len(big))}):
            with self.assertRaisesRegex(RuntimeError, refused.format(runs=2 * len(big), cap=len(big))):
                gp._eval_selection()
        with mock.patch.dict(os.environ, {gp.EVAL_MAX_CASES_VAR: " " + str(2 * len(big)) + " "}):
            self.assertEqual(self.runs_of(gp._eval_selection()), 2 * len(big))
        # The override lifts the cap; the cap itself must be a positive integer.
        with mock.patch.dict(os.environ, {gp.EVAL_ALLOW_HEAVY_VAR: "1"}):
            self.assertEqual(self.runs_of(gp._eval_selection()), 2 * len(big))
        for raw in ("0", "-3", "x", "1.5"):
            with self.subTest(raw=raw), mock.patch.dict(os.environ, {gp.EVAL_MAX_CASES_VAR: raw}):
                with self.assertRaisesRegex(RuntimeError, "OME_EVAL_MAX_CASES must be a positive integer"):
                    gp._eval_selection()

    def test_every_trailer_form_is_read_and_the_newest_line_wins(self):
        resolve = gp._resolve_eval_mode
        self.assertEqual(resolve(None, "subject\n\nbody text\n\nEval: Full\n", True), ("full", None))
        self.assertEqual(resolve(None, "eval:OFF", True), ("off", None))
        self.assertEqual(resolve(None, "  EVAL:  targeted  ", True), ("targeted", None))
        self.assertEqual(
            resolve(None, "Eval: families=gen-heal, Reject,", True),
            ("families", ("families", ["gen-heal", "reject"])),
        )
        self.assertEqual(
            resolve(None, "Eval: cases=p3000,p4267", True),
            ("cases", ("cases", ["p3000", "p4267"])),
        )
        self.assertEqual(
            resolve(None, "Eval: smoke\nEval: cases=p3000\n", True),
            ("cases", ("cases", ["p3000"])),
        )
        self.assertEqual(resolve(None, "Eval: cases=p3000\nEval: smoke\n", True), ("smoke", None))
        # Not a trailer: another word, no value, or the keyword inside a line.
        for message in ("Evaluation: full", "Eval:", "Eval:   ", "see Eval: full later"):
            self.assertEqual(resolve(None, message, True), ("off", None), message)

    def test_the_select_variable_is_the_trailer_by_another_name_and_wins_over_the_message(self):
        resolve = gp._resolve_eval_mode
        lanes = ("lanes", ["gen-heal/engine@ome-scheduler", "reject/pd"])
        for env_mode, message, env_select, expected in (
            # Alone, the variable is the trailer: a mode word or a selection.
            (None, "", "smoke", ("smoke", None)),
            (None, "", " Lanes=gen-heal/engine@OME-Scheduler, reject/pd ", ("lanes", lanes)),
            # Beside OME_EVAL_MODE its selection rides along and its mode word loses.
            ("targeted", "", "families=gen-heal", ("targeted", ("families", ["gen-heal"]))),
            ("targeted", "", "smoke", ("targeted", None)),
            ("full", "Eval: smoke", "cases=p3000", ("full", ("cases", ["p3000"]))),
            # The message's trailer loses to the variable, whatever each says.
            (None, "Eval: cases=p3000", "families=gen-heal", ("families", ("families", ["gen-heal"]))),
            (None, "Eval: cases=p3000", "smoke", ("smoke", None)),
            (None, "Eval: smoke", "lanes=gen-heal/engine", ("lanes", ("lanes", ["gen-heal/engine"]))),
            # Empty or blank, the variable is unset and the message decides.
            (None, "Eval: cases=p3000", "", ("cases", ("cases", ["p3000"]))),
            (None, "Eval: cases=p3000", "   ", ("cases", ("cases", ["p3000"]))),
            (None, "", "", ("off", None)),
            # A value naming nothing known is kept for generation to refuse.
            (None, "", "bogus", ("bogus", None)),
            (None, "", "lanes=", ("lanes", ("lanes", []))),
        ):
            with self.subTest(env_mode=env_mode, message=message, env_select=env_select):
                self.assertEqual(resolve(env_mode, message, True, env_select=env_select), expected)
        self.assertEqual(resolve(None, "", True, env_select="full"), ("full", None))
        self.assertEqual(resolve(None, "", False, env_select="full"), ("off", None))
        # The refusals name the variable beside the trailer.
        self.patch_attr("EVAL_MODE", "bogus")
        with self.assertRaisesRegex(RuntimeError, "OME_EVAL_MODE, OME_EVAL_SELECT or the Eval: trailer must name"):
            gp._eval_mode()
        self.patch_attr("EVAL_MODE", "lanes")
        self.patch_attr("EVAL_SELECTION", ("lanes", []))
        with self.assertRaisesRegex(RuntimeError, "the Eval: trailer or OME_EVAL_SELECT names no lane"):
            gp._eval_selection()

    def test_the_generator_reads_the_select_variable_beside_the_mode_and_the_message(self):
        def fresh():
            spec = importlib.util.spec_from_file_location("generate_pipeline_fresh", _MODULE_PATH)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.EVAL_MODE, module.EVAL_SELECTION

        lane = "gen-heal/engine@ome-scheduler"
        env = {"BUILDKITE_BRANCH": "main", "BUILDKITE_MESSAGE": "Eval: smoke", "GKE_EVAL_ENABLED": "1", "OME_EVAL_SELECT": "lanes=" + lane}
        with mock.patch.dict(os.environ, env):
            os.environ.pop("OME_EVAL_MODE", None)
            self.assertEqual(fresh(), ("lanes", ("lanes", [lane])))
            os.environ["OME_EVAL_MODE"] = "targeted"
            self.assertEqual(fresh(), ("targeted", ("lanes", [lane])))
            del os.environ["OME_EVAL_MODE"]
            os.environ["OME_EVAL_SELECT"] = ""
            self.assertEqual(fresh(), ("smoke", None))
            os.environ["BUILDKITE_MESSAGE"] = "a subject with no trailer"
            self.assertEqual(fresh(), ("off", None))

    def test_a_value_naming_nothing_known_is_refused_at_generation(self):
        self.patch_attr("EVAL_MODE", "bogus")
        with self.assertRaisesRegex(RuntimeError, "off, smoke, targeted, full.*'bogus'"):
            gp.generate_pipeline_batches()
        # The selection kinds are trailer forms, not OME_EVAL_MODE values.
        self.patch_attr("EVAL_MODE", "families")
        with self.assertRaisesRegex(RuntimeError, "'families'"):
            gp._eval_mode()
        self.patch_attr("EVAL_SELECTION", ("families", ["gen-heal", "no-such-family"]))
        with self.assertRaisesRegex(RuntimeError, "no corpus family: no-such-family"):
            gp._eval_selection()
        self.patch_attr("EVAL_SELECTION", ("families", []))
        with self.assertRaisesRegex(RuntimeError, "names no family"):
            gp._eval_selection()
        self.patch_attr("EVAL_MODE", "cases")
        self.patch_attr("EVAL_SELECTION", ("cases", ["p3000", "p999999"]))
        with self.assertRaisesRegex(RuntimeError, "no corpus case: p999999"):
            gp._eval_selection()
        self.patch_attr("EVAL_SELECTION", ("cases", []))
        with self.assertRaisesRegex(RuntimeError, "names no case"):
            gp._eval_selection()

    def test_off_emits_neither_the_group_nor_the_image_step_on_main(self):
        self.patch_attr("EVAL_MODE", "off")
        self.assertEqual(gp._eval_selection(), [])
        batches = gp.generate_pipeline_batches()
        self.assertEqual(len(batches), 1)
        self.assertNotIn("package-kind-eval-image", batches[0])
        self.assertNotIn("omenative-e2e-", batches[0])
        self.assertNotIn(gp.UPLOAD_STEP_KEY, batches[0])

    def test_a_mode_other_than_off_brings_the_eval_to_any_branch(self):
        self.patch_attr("BRANCH", "feature/x")
        self.patch_attr("EVAL_MODE", "smoke")
        self.patch_attr("_eval_children", lambda: self.smoke)
        batches = gp.generate_pipeline_batches()
        self.assertIn('key: "package-kind-eval-image"', batches[0])
        self.assertEqual(len(batches), 3)
        self.assertIn('key: "omenative-e2e-kind-gke"', batches[1])
        self.assertIn("(KIND on GKE) (smoke)", batches[1])

    def test_smoke_runs_the_first_shard_of_every_lane_on_the_default_scheduler(self):
        jobs = self.runner_jobs(self.smoke)
        self.assertEqual(len(jobs), len(self.smoke))
        self.assertEqual(len(jobs), len(self.groups))
        self.assertEqual(
            sorted(self.lane_of(child) for child in jobs),
            sorted((feature, topology) for feature, topology, _ in self.groups),
        )
        for child in jobs:
            self.assertNotIn("OME_EVAL_SCHEDULER", child["env"])
            self.assertNotIn(gp.EVAL_FORCE_DELETE_ENV, child["env"])
            self.assertNotIn("-of-", child["key"])
        # Each job is the full run's first default-scheduler shard of its
        # plain lane, cases and all.
        first_shards = {}
        for child in self.runner_jobs(self.full, scheduler=""):
            if gp.EVAL_FORCE_DELETE_ENV not in child["env"]:
                first_shards.setdefault(self.lane_of(child), self.cases_of(child))
        self.assertEqual({self.lane_of(child): self.cases_of(child) for child in jobs}, first_shards)
        # The seam: (feature, topology, cases, scheduler) in emission order.
        self.patch_attr("EVAL_MODE", "smoke")
        selection = gp._eval_selection()
        self.assertEqual(
            [(feature, topology, scheduler) for feature, topology, _, scheduler in selection],
            [(feature, topology, "") for feature, topology, _ in self.groups],
        )
        self.assertEqual({(f, t): cases for f, t, cases, _ in selection}, first_shards)

    def test_targeted_without_a_round_is_smoke_plus_the_trailer_selection_and_the_label_says_so(self):
        # No results token, so no round to read: targeted is smoke plus the
        # trailer; TestEvalTargeted covers the selection from a round.
        self.patch_attr("EVAL_MODE", "targeted")
        patcher = mock.patch.dict(os.environ, {gp.EVAL_RESULTS_TOKEN_VAR: ""})
        patcher.start()
        self.addCleanup(patcher.stop)
        gp._eval_round.cache_clear()
        self.addCleanup(gp._eval_round.cache_clear)
        quiet = contextlib.redirect_stderr(io.StringIO())
        quiet.__enter__()
        self.addCleanup(quiet.__exit__, None, None, None)
        plain = gp._eval_children()
        self.assertEqual([c["key"] for _, _, c in plain], [c["key"] for _, _, c in self.smoke])
        self.assertEqual([c["command"] for _, _, c in plain], [c["command"] for _, _, c in self.smoke])
        self.assertEqual(
            self.group_labels(plain),
            [":test_tube: OMENative YAML E2E (KIND on GKE) (targeted: smoke, no results token)"],
        )
        self.patch_attr("EVAL_SELECTION", ("families", ["gen-switch"]))
        with_family = gp._eval_children()
        smoke_cases = {case for child in self.runner_jobs(self.smoke) for case in self.cases_of(child)}
        family_cases = {case for feature, _, cases in self.groups if feature == "gen-switch" for case in cases}
        self.assertFalse(family_cases <= smoke_cases)
        emitted = [case for child in self.runner_jobs(with_family) for case in self.cases_of(child)]
        self.assertEqual(set(emitted), smoke_cases | family_cases)
        self.assertEqual(len(emitted), len(set(emitted)))
        self.assertEqual({scheduler for scheduler, _, _ in with_family}, {""})
        self.assertIn(
            "(targeted: smoke, no results token) (plus families: gen-switch)",
            self.group_labels(with_family)[0],
        )

    def test_families_run_every_case_of_the_named_folders_on_both_schedulers(self):
        self.patch_attr("EVAL_MODE", "families")
        self.patch_attr("EVAL_SELECTION", ("families", ["gen-heal", "reject"]))
        children = gp._eval_children()
        wanted = {case for feature, _, cases in self.groups if feature in ("gen-heal", "reject") for case in cases}
        self.assertTrue(wanted)
        self.assertEqual({feature for _, feature, _ in children}, {"gen-heal", "reject"})
        for scheduler in gp.EVAL_SCHEDULERS:
            jobs = self.runner_jobs(children, scheduler=scheduler)
            emitted = [case for child in jobs for case in self.cases_of(child)]
            skipped = {case for child in jobs for case in child["env"].get("OME_EVAL_SCHEDULER_SKIPPED", "").split()}
            for owner, _, child in children:
                if owner == scheduler and child["key"].endswith("-skipped"):
                    skipped.update(child["command"].split("twin: ")[1].rstrip("'").split())
            self.assertEqual(set(emitted) | skipped, wanted, scheduler or "default")
            self.assertEqual(len(emitted), len(set(emitted)), scheduler or "default")
            self.assertTrue(jobs, scheduler or "default")
        for label in self.group_labels(children):
            self.assertIn("(KIND on GKE) (families: gen-heal, reject)", label)

    def test_cases_run_exactly_the_named_ids_in_their_lanes_on_the_default_scheduler(self):
        plain = next(cases[0] for feature, topology, cases in self.groups if (feature, topology) == ("gen-heal", "engine"))
        policy = next(
            case
            for feature, _, cases in self.groups
            if feature == "gen-node"
            for case in cases
            if gp._eval_case_header_fields(case).get("needs") == gp.EVAL_FORCE_DELETE_NEED
        )
        self.patch_attr("EVAL_MODE", "cases")
        self.patch_attr("EVAL_SELECTION", ("cases", [plain, policy]))
        children = gp._eval_children()
        self.assertEqual(
            sorted(case for child in self.runner_jobs(children) for case in self.cases_of(child)),
            sorted([plain, policy]),
        )
        self.assertEqual({scheduler for scheduler, _, _ in children}, {""})
        by_case = {self.cases_of(child)[0]: child for child in self.runner_jobs(children)}
        self.assertIn("-" + gp.EVAL_FORCE_DELETE_SLUG, by_case[policy]["key"])
        self.assertEqual(by_case[policy]["env"].get(gp.EVAL_FORCE_DELETE_ENV), "1")
        self.assertNotIn(gp.EVAL_FORCE_DELETE_ENV, by_case[plain]["env"])
        self.assertIn("(KIND on GKE) (cases: " + plain + ", " + policy + ")", self.group_labels(children)[0])

    def test_lanes_resolve_from_the_trailer_with_their_scheduler_suffix(self):
        resolve = gp._resolve_eval_mode
        self.assertEqual(
            resolve(None, "Eval: lanes=gen-heal/engine, gen-heal/engine@ome-scheduler, Reject/PD@Default", True),
            ("lanes", ("lanes", ["gen-heal/engine", "gen-heal/engine@ome-scheduler", "reject/pd@default"])),
        )
        # Beside a mode the environment set, the lanes ride along as its selection.
        self.assertEqual(
            resolve("targeted", "Eval: lanes=gen-heal/engine@ome-scheduler", True),
            ("targeted", ("lanes", ["gen-heal/engine@ome-scheduler"])),
        )
        # lanes is a trailer form, not an OME_EVAL_MODE value; the refusal spells the form.
        self.patch_attr("EVAL_MODE", "lanes")
        with self.assertRaisesRegex(RuntimeError, r"lanes=<feature>/<topology>\[@<scheduler>\].*'lanes'"):
            gp._eval_mode()

    def test_lanes_run_every_case_of_the_named_lanes_under_their_schedulers(self):
        gang = gp.EVAL_GANG_SCHEDULER
        heal = next(cases for feature, topology, cases in self.groups if (feature, topology) == ("gen-heal", "engine"))
        reject = next(cases for feature, topology, cases in self.groups if (feature, topology) == ("reject", "engine"))
        names = ["gen-heal/engine", "gen-heal/engine@" + gang, "reject/engine@default"]
        self.patch_attr("EVAL_MODE", "lanes")
        self.patch_attr("EVAL_SELECTION", ("lanes", names))
        # The seam: every case of each named lane, scheduler-major, then corpus order.
        self.assertEqual(
            gp._eval_selection(),
            [("gen-heal", "engine", heal, ""), ("reject", "engine", reject, ""), ("gen-heal", "engine", heal, gang)],
        )
        children = gp._eval_children()
        self.assertEqual({scheduler for scheduler, _, _ in children}, {"", gang})
        default = self.runner_jobs(children, scheduler="")
        self.assertEqual({self.lane_of(child) for child in default}, {("gen-heal", "engine"), ("reject", "engine")})
        self.assertEqual(sorted(case for child in default for case in self.cases_of(child)), sorted(heal + reject))
        for child in default:
            self.assertNotIn("OME_EVAL_SCHEDULER", child["env"])
        twins = self.runner_jobs(children, scheduler=gang)
        self.assertEqual({self.lane_of(child) for child in twins}, {("gen-heal", "engine")})
        emitted = [case for child in twins for case in self.cases_of(child)]
        skipped = {case for child in twins for case in child["env"].get("OME_EVAL_SCHEDULER_SKIPPED", "").split()}
        for owner, _, child in children:
            if owner == gang and child["key"].endswith("-skipped"):
                skipped.update(child["command"].split("twin: ")[1].rstrip("'").split())
        self.assertEqual(set(emitted) | skipped, set(heal))
        self.assertEqual(len(emitted), len(set(emitted)))
        for child in twins:
            self.assertEqual(child["env"].get("OME_EVAL_SCHEDULER"), gang)
        for label in self.group_labels(children):
            self.assertIn("(KIND on GKE) (lanes: " + ", ".join(names) + ")", label)

    def test_an_unknown_lane_or_scheduler_and_a_twin_that_is_off_are_refused(self):
        self.patch_attr("EVAL_MODE", "lanes")
        for names, message in (
            ([], "names no lane"),
            (["gen-heal/nowhere"], "no corpus lane: gen-heal/nowhere"),
            (["gen-heal", "p3000"], "no corpus lane: gen-heal, p3000"),
            (["gen-heal/engine", "gen-heal/engine@volcano"], r"no scheduler \(@default or @ome-scheduler\): gen-heal/engine@volcano"),
        ):
            with self.subTest(names=names):
                self.patch_attr("EVAL_SELECTION", ("lanes", names))
                with self.assertRaisesRegex(RuntimeError, message):
                    gp._eval_selection()
        # With the twin switched off, naming it is refused; the lane under
        # the default scheduler still runs, named once however often.
        self.patch_attr("EVAL_SCHEDULERS", [""])
        self.patch_attr("EVAL_SELECTION", ("lanes", ["gen-heal/engine@ome-scheduler"]))
        with self.assertRaisesRegex(RuntimeError, "does not run.*: gen-heal/engine@ome-scheduler"):
            gp._eval_selection()
        self.patch_attr("EVAL_SELECTION", ("lanes", ["gen-heal/engine", "gen-heal/engine@default"]))
        self.assertEqual([(f, t, s) for f, t, _, s in gp._eval_selection()], [("gen-heal", "engine", "")])

    def test_every_lane_of_every_mode_is_in_the_one_concurrency_group(self):
        cap = gp._eval_max_concurrency()
        for children in (self.full, self.smoke):
            lanes = self.runner_jobs(children)
            self.assertTrue(lanes)
            for child in lanes:
                self.assertEqual(
                    (child["concurrency_group"], child["concurrency"]),
                    (gp.EVAL_CONCURRENCY_GROUP, cap),
                    child["key"],
                )

    def test_full_keeps_the_whole_corpus_and_the_smoke_totals_are_pinned(self):
        committed = sorted(case for _, _, cases in self.groups for case in cases)
        default = sorted(case for child in self.runner_jobs(self.full, scheduler="") for case in self.cases_of(child))
        self.assertEqual(default, committed)
        self.assertEqual(len(self.runner_jobs(self.full, scheduler="")), 1143)
        self.assertEqual(len(self.runner_jobs(self.full)), 2613)
        self.assertEqual(len(self.full), 2627)
        self.assertEqual(len(self.smoke), 203)
        self.assertEqual(len(self.smoke_batches), 3)
        self.assertEqual(sum(gp._job_count(document) for document in self.smoke_batches), 312)
        # Only a full round records its verdicts.
        self.assertNotIn(f'key: "{gp.EVAL_VERDICTS_STEP_KEY}"', "".join(self.smoke_batches))


class TestEvalConcurrencyCap(PipelineTestCase):
    """Every eval lane is in one concurrency group capped by
    GKE_EVAL_MAX_CONCURRENCY, so the lanes of every build together hold at
    most that many GKE agent pods; a skip note holds none and is in no group."""

    EVAL_ENV = {
        "GKE_EVAL_CASE_CONCURRENCY": "4",
        "GKE_EVAL_TARGET_WAVES": "4",
        "GKE_EVAL_CANARY_TARGET_WAVES": "2",
        "OME_EVAL_ALLOW_HEAVY": "1",
    }

    def setUp(self):
        patcher = mock.patch.dict(os.environ, self.EVAL_ENV)
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop(gp.EVAL_MAX_CONCURRENCY_VAR, None)
        self.patch_attr("BRANCH", "main")
        self.patch_attr("EVAL_MODE", "full")
        self.patch_attr("EVAL_SELECTION", None)
        self.groups = {(f, t): cases for f, t, cases in gp._eval_case_groups()}

    def select(self, *lanes):
        """Pin the selection seam to a few lanes so a render is cheap."""
        self.patch_attr(
            "_eval_selection",
            lambda: [(f, t, list(self.groups[(f, t)]), s) for f, t, s in lanes],
        )

    def test_every_lane_carries_the_group_and_the_cap_and_a_skip_note_neither(self):
        self.select(
            ("gen-heal", "engine", ""),
            ("reject", "pd", gp.EVAL_GANG_SCHEDULER),
            ("gen-heal", "engine-mn", gp.EVAL_GANG_SCHEDULER),
        )
        self.patch_attr("GKE_EVAL_SCHEDULER_MAX_WORKERS", 1)
        children = gp._eval_children()
        lanes = [c for _, _, c in children if "run_omenative_eval_gke.sh" in c["command"]]
        notes = [c for _, _, c in children if c["key"].endswith("-skipped")]
        self.assertTrue(lanes)
        self.assertTrue(notes)
        self.assertEqual(gp.EVAL_MAX_CONCURRENCY_DEFAULT, 40)
        for child in lanes:
            self.assertEqual(child["concurrency_group"], "ome/gke-evals")
            self.assertEqual(child["concurrency"], 40)
        for child in notes:
            self.assertNotIn("concurrency_group", child)
            self.assertNotIn("concurrency", child)
        rendered = gp._gke_eval_group()
        self.assertEqual(rendered.count('concurrency_group: "ome/gke-evals"'), len(lanes))
        self.assertEqual(rendered.count("concurrency: 40"), len(lanes))

    def test_the_cap_reads_the_environment_and_refuses_anything_but_a_positive_integer(self):
        self.select(("gen-heal", "engine", ""))
        with mock.patch.dict(os.environ, {gp.EVAL_MAX_CONCURRENCY_VAR: " 7 "}):
            children = gp._eval_children()
        self.assertEqual({c["concurrency"] for _, _, c in children}, {7})
        with mock.patch.dict(os.environ, {gp.EVAL_MAX_CONCURRENCY_VAR: ""}):
            self.assertEqual(gp._eval_max_concurrency(), 40)
        for raw in ("0", "-3", "x", "1.5"):
            with self.subTest(raw=raw), mock.patch.dict(os.environ, {gp.EVAL_MAX_CONCURRENCY_VAR: raw}):
                with self.assertRaisesRegex(RuntimeError, "GKE_EVAL_MAX_CONCURRENCY must be a positive integer"):
                    gp._eval_children()


class TestPipelineUploads(PipelineTestCase):
    """The build is uploaded as documents of at most UPLOAD_JOB_LIMIT jobs: the
    entrypoint's (every step that is not an eval lane, ending with the upload
    step), one per eval group, then the release marker, which names every
    other top-level key of the build."""

    LANE_KEY = re.compile(r'^\s*key: "(omenative-e2e-(?!kind-gke)[^"]+)"$', re.M)
    # The pins mean the whole corpus; the upload script's own generator run
    # reads the mode from the environment.
    EVAL_ENV = {
        "GKE_EVAL_CASE_CONCURRENCY": "4",
        "GKE_EVAL_TARGET_WAVES": "4",
        "GKE_EVAL_CANARY_TARGET_WAVES": "2",
        "OME_EVAL_ALLOW_HEAVY": "1",
        "OME_EVAL_MODE": "full",
    }

    @classmethod
    def setUpClass(cls):
        with mock.patch.dict(os.environ, cls.EVAL_ENV), mock.patch.object(
            gp, "BRANCH", "main"
        ), mock.patch.object(gp, "EVAL_MODE", "full"):
            cls.children = gp._eval_children()
            cls.batches = gp.generate_pipeline_batches()

    def setUp(self):
        patcher = mock.patch.dict(os.environ, self.EVAL_ENV)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.patch_attr("BRANCH", "main")
        self.patch_attr("EVAL_MODE", "full")

    def marker_dependencies(self, document):
        marker = document[document.index('key: "chart-ome-release"'):]
        return re.findall(r'^\s*- "([^"]+)"$', marker.split("secrets:")[0], re.M)

    def assert_uploads_are_well_formed(self, batches, children):
        for index, document in enumerate(batches, start=1):
            self.assertLessEqual(gp._job_count(document), gp.UPLOAD_JOB_LIMIT, f"upload {index}")
            self.assertTrue(document.startswith("steps:\n"), f"upload {index}")
        # Every eval job is uploaded exactly once, none of them by the entrypoint.
        expected = sorted(child["key"] for _, _, child in children)
        seen = [key for document in batches for key in self.LANE_KEY.findall(document)]
        self.assertEqual(sorted(seen), expected)
        self.assertEqual(len(seen), len(set(seen)))
        self.assertEqual(self.LANE_KEY.findall(batches[0]), [])
        # The marker is the last step of the last upload and depends on every
        # other top-level step and group, the eval groups and the full
        # round's verdict record among them.
        self.assertEqual(sum(d.count('key: "chart-ome-release"') for d in batches), 1)
        self.assertEqual(gp._top_level_keys(batches[-1]), [gp.EVAL_VERDICTS_STEP_KEY, "chart-ome-release"])
        gates = self.marker_dependencies(batches[-1])
        every = [key for document in batches for key in gp._top_level_keys(document)]
        self.assertEqual(gates, every[:-1])
        groups = [key for document in batches[1:-1] for key in gp._top_level_keys(document)]
        self.assertEqual(len(groups), len(batches) - 2)
        self.assertTrue(all(key.startswith("omenative-e2e-kind-gke") for key in groups), groups)
        self.assertTrue(set(groups) <= set(gates))
        self.assertIn(gp.UPLOAD_STEP_KEY, gates)
        self.assertNotIn("- wait", "".join(batches))

    def test_job_count_and_top_level_keys_read_the_generated_shape(self):
        document = (
            'steps:\n'
            '  - label: "a"\n    key: "a"\n'
            '  - wait\n'
            '  - group: "g"\n    key: "g"\n    steps:\n'
            '      - label: "b"\n        key: "b"\n'
            '      - label: "c"\n        key: "c"\n'
        )
        self.assertEqual(gp._job_count(document), 4)
        self.assertEqual(gp._top_level_keys(document), ["a", "g"])
        # Every generated step and group carries a key, so the job count of a
        # document is its keys less its groups.
        for index, document in enumerate(self.batches, start=1):
            keys = len(re.findall(r'^\s*key: "', document, re.M))
            groups = len(re.findall(r'^\s*- group: ', document, re.M))
            self.assertEqual(gp._job_count(document), keys - groups, f"upload {index}")

    def test_first_upload_is_what_the_entrypoint_prints(self):
        self.assertEqual(self.batches[0], gp.generate_pipeline())
        keys = gp._top_level_keys(self.batches[0])
        self.assertEqual(keys[-1], gp.UPLOAD_STEP_KEY)
        self.assertNotIn("chart-ome-release", keys)
        step = self.batches[0][self.batches[0].index(f'key: "{gp.UPLOAD_STEP_KEY}"'):]
        self.assertIn(f"sh {gp.UPLOAD_SCRIPT}", step)
        self.assertIn(f'"{gp.UPLOAD_BATCH_DIR}/*"', step)
        self.assertIn(f'image: "{gp.PIPELINE_UPLOAD_IMAGE}"', step)
        self.assertIn("mount-buildkite-agent: true", step)
        later = len(self.batches) - 1
        jobs = sum(gp._job_count(document) for document in self.batches[1:])
        self.assertIn(
            f'- label: ":gear: Upload eval lanes and release marker ({later} uploads, {jobs} jobs)"',
            self.batches[0],
        )

    def test_todays_corpus_uploads_within_the_limit_and_the_total_is_pinned(self):
        self.assert_uploads_are_well_formed(self.batches, self.children)
        self.assertGreater(len(self.batches), 2)
        self.assertEqual(sum(gp._job_count(document) for document in self.batches), 2737)

    def test_the_full_round_records_its_verdicts_after_every_eval_group(self):
        last = self.batches[-1]
        step = last[last.index(f'key: "{gp.EVAL_VERDICTS_STEP_KEY}"'):last.index('key: "chart-ome-release"')]
        groups = [key for document in self.batches[1:-1] for key in gp._top_level_keys(document)]
        self.assertEqual(re.findall(r'^      - "([^"]+)"$', step.split("allow_dependency_failure")[0], re.M), groups)
        # Failed lanes are what the record is for, and a record that cannot
        # be written never blocks the marker.
        self.assertIn("allow_dependency_failure: true", step)
        self.assertIn("soft_fail: true", step)
        self.assertIn("sh .buildkite/record_eval_verdicts.sh", step)
        self.assertIn(f'queue: "{gp.BUILD_QUEUE}"', step)
        self.assertIn(f'image: "{gp.PIPELINE_UPLOAD_IMAGE}"', step)
        self.assertIn("mount-buildkite-agent: true", step)
        # The script reads the format's names from the step, defined once.
        for name, value in (
            ("OME_EVAL_RESULTS_DIR", ev.RESULTS_DIR),
            ("OME_EVAL_VERDICTS_FILE", ev.VERDICTS_FILE),
            ("OME_EVAL_ROUND_META_KEY", ev.ROUND_META_KEY),
            ("OME_EVAL_ROUND_META_VALUE", ev.ROUND_META_VALUE),
        ):
            self.assertIn(f'{name}: "{value}"', step)
        self.assertIn(gp.EVAL_VERDICTS_STEP_KEY, self.marker_dependencies(last))
        self.assertEqual(gp._job_count(last), 2)

    def test_a_corpus_three_times_larger_still_uploads_within_the_limit(self):
        tripled = [
            (scheduler, feature, {**child, "key": f"{child['key']}-copy-{copy}"})
            for scheduler, feature, child in self.children
            for copy in range(3)
        ]
        self.patch_attr("_eval_children", lambda: tripled)
        batches = gp.generate_pipeline_batches()
        self.assert_uploads_are_well_formed(batches, tripled)
        self.assertGreater(len(batches), len(self.batches))

    def test_a_smaller_limit_cuts_a_scheduler_between_families_and_labels_the_slices(self):
        self.patch_attr("UPLOAD_JOB_LIMIT", 400)
        batches = gp.generate_pipeline_batches()
        self.assert_uploads_are_well_formed(batches, self.children)
        self.assertEqual(
            sum(gp._job_count(document) for document in batches),
            sum(gp._job_count(document) for document in self.batches),
        )
        labels = re.findall(r'^  - group: "([^"]+)"$', "".join(batches[1:-1]), re.M)
        self.assertEqual(len(labels), len(batches) - 2)
        pattern = re.compile(
            r'^:test_tube: OMENative YAML E2E \(KIND on GKE\) \[(\d+)/(\d+): '
            r'(default scheduler|' + re.escape(gp.EVAL_GANG_SCHEDULER) + r')(, [a-z0-9-]+( to [a-z0-9-]+)?)?\]$'
        )
        for index, label in enumerate(labels, start=1):
            match = pattern.match(label)
            self.assertIsNotNone(match, label)
            self.assertEqual((int(match.group(1)), int(match.group(2))), (index, len(labels)))
            # Every slice here is a part of its scheduler, so each names its families.
            self.assertIsNotNone(match.group(4), label)
        self.assertEqual(labels[0].count("default scheduler"), 1)

    def test_eval_batches_keep_order_cut_per_scheduler_and_between_families(self):
        def job(scheduler, feature, number):
            return (scheduler, feature, {"key": f"{feature}-{scheduler or 'default'}-{number}"})

        families = (("a", 3), ("b", 3), ("c", 4), ("d", 2))
        children = [
            job(scheduler, feature, number)
            for scheduler in ("", gp.EVAL_GANG_SCHEDULER)
            for feature, size in families
            for number in range(size)
        ]
        batches = gp._eval_batches(children, 5)
        self.assertEqual([child for batch in batches for child in batch], children)
        self.assertEqual([len(batch) for batch in batches], [3, 3, 4, 2, 3, 3, 4, 2])
        for batch in batches:
            self.assertEqual(len({scheduler for scheduler, _, _ in batch}), 1)
        # At or under the limit a scheduler's jobs stay in one batch.
        self.assertEqual([len(batch) for batch in gp._eval_batches(children, 12)], [12, 12])
        # A family alone above the limit is cut between its jobs.
        big = [job("", "big", number) for number in range(12)]
        self.assertEqual([len(batch) for batch in gp._eval_batches(big, 5)], [5, 5, 2])
        self.assertEqual(gp._eval_batches([], 5), [])
        with self.assertRaisesRegex(RuntimeError, "positive"):
            gp._eval_batches(children, 0)

    def test_a_single_eval_group_keeps_the_plain_key(self):
        # The default scheduler's jobs span more than one upload today, so
        # the single group is read on the slice of them that fits one.
        default_only = [item for item in self.children if item[0] == ""][:gp.UPLOAD_JOB_LIMIT]
        self.patch_attr("_eval_children", lambda: default_only)
        groups = gp._gke_eval_groups()
        self.assertEqual([group["key"] for group in groups], ["omenative-e2e-kind-gke"])
        self.assertEqual(groups[0]["group"], ":test_tube: OMENative YAML E2E (KIND on GKE)")
        self.assertEqual(groups[0]["notify"], [{"github_commit_status": {"context": "omenative-e2e-kind-gke"}}])

    def test_every_eval_group_posts_its_own_commit_status(self):
        for group in gp._gke_eval_groups():
            self.assertEqual(group["notify"], [{"github_commit_status": {"context": group["key"]}}])

    def test_generation_refuses_an_upload_above_the_limit(self):
        self.patch_attr("UPLOAD_JOB_LIMIT", 50)
        with self.assertRaisesRegex(RuntimeError, "upload 1 of .* carries .* jobs"):
            gp.generate_pipeline_batches()

    def test_batch_dir_mode_writes_every_document_and_a_manifest(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(
            os.environ, {"MSL_BK_PIPELINE_PRIORITY": "7"}
        ):
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                gp.main(["--batch-dir", tmp])
            manifest = [line.split() for line in out.getvalue().splitlines()]
            self.assertEqual(
                manifest,
                [[f"batch-{index:02d}.yml", str(gp._job_count(document))]
                 for index, document in enumerate(self.batches, start=1)],
            )
            for (name, _), document in zip(manifest, self.batches):
                self.assertEqual((Path(tmp) / name).read_text(), "priority: 7\n" + document)
            self.assertIn("pipeline upload plan:", err.getvalue())
            self.assertIn(f"{len(self.batches)} uploads", err.getvalue())
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                gp.main([])
            self.assertEqual(out.getvalue(), "priority: 7\n" + self.batches[0])

    def test_upload_script_uploads_every_later_document_in_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stubs = root / "bin"
            stubs.mkdir()
            log = root / "uploads.log"
            agent = stubs / "buildkite-agent"
            agent.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$STUB_LOG"\n')
            agent.chmod(0o755)
            (stubs / "python3").symlink_to(sys.executable)
            batch_dir = root / "batches"
            env = {
                **os.environ,
                "PATH": f"{stubs}:{os.environ.get('PATH', '')}",
                "STUB_LOG": str(log),
                "OME_PIPELINE_BATCH_DIR": str(batch_dir),
                "BUILDKITE_BRANCH": "main",
                "BUILDKITE_COMMIT": "HEAD",
            }
            result = subprocess.run(
                ["sh", str(gp.REPO_ROOT / gp.UPLOAD_SCRIPT)],
                env=env, cwd=tmp, capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            manifest = [line.split() for line in (batch_dir / "manifest.txt").read_text().splitlines()]
            self.assertEqual(len(manifest), len(self.batches))
            uploads = [line.split() for line in log.read_text().splitlines()]
            self.assertEqual(
                uploads,
                [["pipeline", "upload", str(batch_dir / name)] for name, _ in manifest[1:]],
            )
            self.assertIn(f"skip {manifest[0][0]} ({manifest[0][1]} jobs)", result.stdout)
            for name, count in manifest[1:]:
                self.assertIn(f"upload {name} ({count} jobs)", result.stdout)
                self.assertEqual(gp._job_count((batch_dir / name).read_text()), int(count))
            total = sum(int(count) for _, count in manifest[1:])
            self.assertIn(f"uploaded {len(manifest) - 1} documents, {total} jobs", result.stdout)
