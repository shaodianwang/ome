"""Unit tests for hack/check_internal_tokens.py (run: python3 -m unittest hack/check_internal_tokens_test.py)."""

import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
CHECKER = os.path.join(HERE, "check_internal_tokens.py")
PATTERNS = os.path.join(HERE, "internal-tokens.txt")


def run(root, *args, allow="allow"):
    return subprocess.run(
        [sys.executable, CHECKER, "--root", root, "--patterns", PATTERNS, "--allow", os.path.join(root, allow), *args],
        capture_output=True, text=True,
    )


def write(root, rel, text):
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return rel


def git_repo(root, *rels):
    subprocess.run(["git", "-C", root, "init", "-q"], check=True)
    subprocess.run(["git", "-C", root, "add", "--", *rels], check=True)


class CheckInternalTokens(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        write(self.root, "allow", "")

    def tearDown(self):
        self.tmp.cleanup()

    # --- token scan -------------------------------------------------------

    def test_clean_file_passes(self):
        rel = write(self.root, "pkg/x/x.go", 'package x\n\n// Serves example.com traffic for team-a.\nconst host = "svc.ns.svc.cluster.local"\n')
        r = run(self.root, "--files", rel)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_cluster_and_tracker_tokens_fail(self):
        rel = write(self.root, "pkg/x/x_test.go", 'package x\n\n// observed on msl-cw-use2-dev-2 during T123456789\nvar fixture = "sev-1"\n')
        r = run(self.root, "--files", rel)
        self.assertEqual(r.returncode, 1)
        self.assertIn("[clusters]", r.stdout)
        self.assertIn("[trackers]", r.stdout)

    def test_secrets_and_identities_fail(self):
        rel = write(self.root, "hack/tool.sh", 'TOKEN=bkua_0123456789abcdefghijklmnop\nMAIL=someone@meta.com\n')
        r = run(self.root, "--files", rel)
        self.assertEqual(r.returncode, 1)
        self.assertIn("[secrets]", r.stdout)
        self.assertIn("[identities]", r.stdout)

    def test_word_boundaries_hold(self):
        rel = write(self.root, "pkg/x/x.go", 'package x\n\n// several, drift, recreate and modelconfig are ordinary words; UseSecondary too\n')
        r = run(self.root, "--files", rel)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_public_vendor_names_pass(self):
        rel = write(self.root, "pkg/x/x.go", 'package x\n\n// CoreWeave, GKE tpu7x and gb300 nodes; see github.com/facebookresearch/xformers\n')
        r = run(self.root, "--files", rel)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_oep_refs_only_fail_in_shipped_paths(self):
        code = write(self.root, "pkg/x/x.go", "package x\n\n// see OEP-0024\n")
        doc = write(self.root, "oeps/0024-quota/README.md", "# OEP-0024\n")
        self.assertEqual(run(self.root, "--files", code).returncode, 1)
        self.assertEqual(run(self.root, "--files", doc).returncode, 0)

    def test_path_names_are_scanned(self):
        rel = write(self.root, "hack/fixtures/avo-tier5-pd/isvc.yaml", "kind: InferenceService\n")
        r = run(self.root, "--files", rel)
        self.assertEqual(r.returncode, 1)
        self.assertIn("path contains", r.stdout)

    # --- classification and exemptions -------------------------------------

    def test_internal_only_trees_are_exempt(self):
        rel = write(self.root, "muse/deploy/values.yaml", "cluster: msl-cw-use2-2\n")
        self.assertEqual(run(self.root, "--files", rel).returncode, 0)

    def test_generated_files_are_skipped(self):
        rel = write(self.root, "pkg/apis/zz_generated.deepcopy.go", "// msl-cw-use2-2\n")
        self.assertEqual(run(self.root, "--files", rel).returncode, 0)

    def test_allowlist_by_glob_and_group(self):
        write(self.root, "allow", "hack/harness/** storage # harness images\n")
        ok = write(self.root, "hack/harness/p1.yaml", "image: 123456789012.dkr.ecr.us-east-2.amazonaws.com/x\n")
        bad = write(self.root, "hack/harness/p2.yaml", "cluster: msl-cw-use2-2\n")
        self.assertEqual(run(self.root, "--files", ok).returncode, 0)
        self.assertEqual(run(self.root, "--files", bad).returncode, 1)

    def test_exemption_without_reason_is_rejected(self):
        write(self.root, "allow", "hack/harness/** storage\n")
        rel = write(self.root, "hack/harness/p1.yaml", "ok\n")
        r = run(self.root, "--files", rel)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("reason", r.stderr)

    # --- structural rules (tree mode) --------------------------------------

    def test_files_mode_missing_path_fails(self):
        r = run(self.root, "--files", "pkg/x/never_written.go")
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("[missing]", r.stdout)

    def test_files_mode_unclassified_path_fails(self):
        rel = write(self.root, "newtree/x.go", "package x\n")
        r = run(self.root, "--files", rel)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("[unclassified]", r.stdout)
    def test_symlinks_are_skipped(self):
        write(self.root, "hack/notes/real.md", "plain text\n")
        os.symlink("notes", os.path.join(self.root, "hack", "alias"))
        os.symlink("notes/real.md", os.path.join(self.root, "hack", "alias.md"))
        r = run(self.root, "--files", "hack/alias", "hack/alias.md", "hack/notes/real.md")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_tree_mode_flags_unclassified_toplevel_and_stale_exemptions(self):
        a = write(self.root, "pkg/x/x.go", "package x\n")
        b = write(self.root, "mystery/thing.txt", "hello\n")
        write(self.root, "allow", "hack/nothing/** * # nothing matches this\n")
        git_repo(self.root, a, b)
        r = run(self.root, "--tree")
        self.assertEqual(r.returncode, 1)
        self.assertIn("[unclassified] new top-level entry", r.stdout)
        self.assertIn("[stale-exemption]", r.stdout)

    def test_tree_mode_clean_repo_passes(self):
        a = write(self.root, "pkg/x/x.go", "package x\n")
        b = write(self.root, "oeps/README.md", "# OEPs\n")
        git_repo(self.root, a, b)
        r = run(self.root, "--tree")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    # --- import guard ------------------------------------------------------

    def test_guarded_imports_only_from_wireups(self):
        bad = write(self.root, "pkg/x/x.go", 'package x\n\nimport _ "github.com/ome-projects/ome/meta-internal/xdspublisher"\n')
        ok = write(self.root, "cmd/manager/register_meta_internal.go", 'package main\n\nimport _ "github.com/ome-projects/ome/meta-internal/xdspublisher"\n')
        sharded_bad = write(self.root, "pkg/y/y.go", 'package y\n\nimport "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"\n')
        self.assertIn("[import-guard]", run(self.root, "--files", bad).stdout)
        self.assertEqual(run(self.root, "--files", ok).returncode, 0)
        self.assertIn("[import-guard]", run(self.root, "--files", sharded_bad).stdout)


if __name__ == "__main__":
    unittest.main()
