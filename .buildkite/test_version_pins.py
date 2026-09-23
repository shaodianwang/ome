"""Consistency harness for the Kubernetes-version pins scattered across the repo.

Several files must agree on one Kubernetes minor and one kind release, and
nothing but convention kept them in step. When they drifted, the KIND lane
booted a kubelet three minors behind what OME ships onto, and a node-image bump
failed in `kind create cluster` on an agent with no egress to fetch the image it
had been handed.

Two source-of-truth files, and the derived copies each one governs:

    .envtest-k8s-version    the Kubernetes minor everything targets
      -> every k8s.io/* staging module in the three go.mod files
      -> the minor of .kind-node-image

    .kind-node-image        the KIND node image, tag + digest
      -> KIND_NODE_TAG in dockerfiles/kind-ci.Dockerfile
      -> KIND_NODE_IMAGE in .buildkite/gke-kind-validation-job.yaml

    hack/internal/tools/go.mod   the kind and helm releases `make kind` /
                                 `make helm` install
      -> KIND_VERSION, HELM_VERSION in dockerfiles/kind-ci.Dockerfile
      -> KIND_VERSION in .buildkite/gke-kind-validation-job.yaml

The shell scripts under .buildkite/ read the two files directly and so cannot
drift; the copies above exist only where a file cannot read another at runtime
(a Dockerfile ARG, a standalone Job manifest). Those are what this pins.

Run from the repo root (stdlib only; pytest also collects it):

    python3 -m unittest discover -s .buildkite -p 'test_*.py'
    python3 -m pytest .buildkite/test_version_pins.py
"""

from __future__ import annotations

import re
import tempfile
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

ENVTEST_VERSION_FILE = REPO_ROOT / ".envtest-k8s-version"
KIND_NODE_IMAGE_FILE = REPO_ROOT / ".kind-node-image"
TOOLS_GO_MOD = REPO_ROOT / "hack" / "internal" / "tools" / "go.mod"
KIND_CI_DOCKERFILE = REPO_ROOT / "dockerfiles" / "kind-ci.Dockerfile"
VALIDATION_JOB = REPO_ROOT / ".buildkite" / "gke-kind-validation-job.yaml"
ALFRED_OSS_REF_FILE = REPO_ROOT / ".alfred-oss-ref"
ALFRED_DOCKERFILE = REPO_ROOT / "dockerfiles" / "alfred.Dockerfile"
MAKEFILE = REPO_ROOT / "Makefile"

# go.mod files whose k8s.io/* staging modules must share one minor. Only the
# v0.<minor>.<patch> staging scheme is comparable: klog (v2.x), gengo (v2.x) and
# the pseudo-versioned kube-openapi / utils are on their own tracks and are
# skipped by GO_MOD_K8S_REQUIRE below.
GO_MODS = (
    Path("go.mod"),
    Path("hack/internal/tools/go.mod"),
    Path("scheduler/go.mod"),
)

# A require line: `\tk8s.io/api v0.35.3 // indirect`. Any version shape — the
# minor assertion filters to the comparable schemes afterwards.
GO_MOD_K8S_REQUIRE = re.compile(r"^\s*(k8s\.io/[\w./-]+)\s+(v\S+?)\s*(?://.*)?$")

# `k8s.io/api => k8s.io/api v0.35.4`, the versioned `X vA => Y vB` form, and
# either of those as a standalone directive carrying the `replace` keyword
# rather than sitting in a block. scheduler depends on k8s.io/kubernetes, which
# resolves its staging modules to stale published versions and relies on a
# replace block to pull them level; the require line is then cosmetic and the
# replace target is what gets built.
GO_MOD_REPLACE = re.compile(
    r"^\s*(?:replace\s+)?(k8s\.io/[\w./-]+)(?:\s+v\S+)?\s+=>\s+\S+\s+(v\S+)\s*$"
)

# Staging modules publish the Kubernetes minor as v0.<minor>.<patch>;
# k8s.io/kubernetes itself keeps the v1.<minor>.<patch> scheme. Modules on
# neither (klog v2.x, gengo v2.x, pseudo-versioned kube-openapi and utils) are
# on their own release tracks and are not comparable.
STAGING_VERSION = re.compile(r"^v0\.(\d+)\.\d+$")
KUBERNETES_VERSION = re.compile(r"^v1\.(\d+)\.\d+$")

# `kindest/node:v1.35.5@sha256:<64 hex>`; the digest is optional so a local
# override can drop it, but CI pins both.
KIND_NODE_IMAGE = re.compile(
    r"^(?P<repo>[\w./-]+):(?P<tag>v(?P<major>\d+)\.(?P<minor>\d+)\.\d+)"
    r"(?:@(?P<digest>sha256:[0-9a-f]{64}))?$"
)

# Files permitted to spell a concrete kindest/node reference. Everything else
# must read .kind-node-image, so a seventh hand-maintained copy cannot appear.
KIND_NODE_LITERAL_ALLOWLIST = frozenset(
    {
        Path(".kind-node-image"),
        Path("dockerfiles/kind-ci.Dockerfile"),
        Path(".buildkite/gke-kind-validation-job.yaml"),
        Path(".buildkite/README-gke-kind.md"),
        Path(".buildkite/test_version_pins.py"),
    }
)

# Directories the literal sweep never descends into: VCS metadata, build
# output, agent scratch, and vendored third-party trees.
SWEEP_PRUNE_DIRS = frozenset({".git", ".claude", "bin", "node_modules", "target", "vendor"})

# A concrete node reference, as opposed to a templated one
# (`kindest/node:${KIND_NODE_TAG}`) or bare prose (`kindest/node`).
KIND_NODE_LITERAL = re.compile(r"kindest/node:v?\d")

# A full 40-hex commit SHA: the alfred pin must be a commit, never a branch or
# tag, so an image build is reproducible from the pin alone.
FULL_COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")


def read_pin(path: Path) -> str:
    """Return a single-value pin file's contents, whitespace stripped."""
    return path.read_text().strip()


def go_mod_require(path: Path, module: str) -> str:
    """Return the version `module` is required at, ignoring replace directives."""
    pattern = re.compile(rf"^\s*{re.escape(module)}\s+(\S+)")
    for line in path.read_text().splitlines():
        if "=>" in line:
            continue
        match = pattern.match(line)
        if match:
            return match.group(1)
    raise AssertionError(f"{path} has no require line for {module}")


def dockerfile_arg(name: str) -> str:
    """Return the default of `ARG <name>=<value>` in the kind-ci Dockerfile."""
    pattern = re.compile(rf"^ARG {re.escape(name)}=(\S+)$", re.MULTILINE)
    match = pattern.search(KIND_CI_DOCKERFILE.read_text())
    if not match:
        raise AssertionError(f"{KIND_CI_DOCKERFILE} has no `ARG {name}=`")
    return match.group(1)


def validation_job_env(name: str) -> str:
    """Return the value of a `- name: <name>` / `value: <v>` pair in the Job."""
    pattern = re.compile(rf"^\s*- name: {re.escape(name)}\n\s*value: (\S+)$", re.MULTILINE)
    match = pattern.search(VALIDATION_JOB.read_text())
    if not match:
        raise AssertionError(f"{VALIDATION_JOB} has no env var {name}")
    return match.group(1)


def parse_node_image(ref: str) -> re.Match:
    match = KIND_NODE_IMAGE.match(ref)
    if not match:
        raise AssertionError(f"not a kindest/node reference: {ref!r}")
    return match


class SourceOfTruthTest(unittest.TestCase):
    """The two pin files parse, and agree with each other."""

    def test_kind_node_image_is_tag_and_digest_pinned(self):
        match = parse_node_image(read_pin(KIND_NODE_IMAGE_FILE))
        self.assertEqual(
            match.group("repo"),
            "kindest/node",
            ".kind-node-image must name the upstream kind node image",
        )
        self.assertIsNotNone(
            match.group("digest"),
            ".kind-node-image must carry an @sha256: digest — the packaging step "
            "verifies it before pulling by tag",
        )

    def test_node_image_minor_matches_envtest(self):
        match = parse_node_image(read_pin(KIND_NODE_IMAGE_FILE))
        node_minor = f"{match.group('major')}.{match.group('minor')}"
        self.assertEqual(
            node_minor,
            read_pin(ENVTEST_VERSION_FILE),
            "the KIND and envtest lanes must exercise the same Kubernetes minor; "
            "bump .kind-node-image and .envtest-k8s-version together",
        )


def go_mod_effective_k8s_versions(path: Path) -> list[tuple[str, str, int, str]]:
    """Return (module, effective_version, line, origin) for each k8s.io require.

    A module under a replace directive builds at the replace target, so that is
    the version reported. Replaces naming a module nothing requires are inert
    and are not reported at all.
    """
    lines = path.read_text().splitlines()
    replaces = {}
    for line in lines:
        match = GO_MOD_REPLACE.match(line)
        if match:
            replaces[match.group(1)] = match.group(2)

    effective = []
    for number, line in enumerate(lines, start=1):
        if "=>" in line:
            continue
        match = GO_MOD_K8S_REQUIRE.match(line)
        if not match:
            continue
        module, required = match.group(1), match.group(2)
        replaced = replaces.get(module)
        origin = "replace" if replaced else "require"
        effective.append((module, replaced or required, number, origin))
    return effective


class GoModMinorTest(unittest.TestCase):
    """Every k8s.io module tracks the minor in .envtest-k8s-version."""

    def test_modules_share_the_target_minor(self):
        want_minor = read_pin(ENVTEST_VERSION_FILE).split(".", 1)[1]
        offenders = []
        for relative in GO_MODS:
            for module, version, number, origin in go_mod_effective_k8s_versions(
                REPO_ROOT / relative
            ):
                staging = STAGING_VERSION.match(version)
                kubernetes = KUBERNETES_VERSION.match(version)
                got = (staging or kubernetes).group(1) if (staging or kubernetes) else None
                if got is not None and got != want_minor:
                    offenders.append(
                        f"{relative}:{number} {module} {version} ({origin})"
                    )
        self.assertEqual(
            offenders,
            [],
            f"expected every k8s.io module on minor {want_minor} "
            f"(from .envtest-k8s-version); off-minor:\n  " + "\n  ".join(offenders),
        )


class DockerfileArgTest(unittest.TestCase):
    """kind-ci's ARG fallbacks track the files the runner layer reads."""

    def test_kind_node_tag_matches_pin_file(self):
        self.assertEqual(
            dockerfile_arg("KIND_NODE_TAG"),
            parse_node_image(read_pin(KIND_NODE_IMAGE_FILE)).group("tag"),
            "ARG KIND_NODE_TAG must be the tag from .kind-node-image",
        )

    def test_kind_version_matches_tools_go_mod(self):
        self.assertEqual(
            dockerfile_arg("KIND_VERSION"),
            go_mod_require(TOOLS_GO_MOD, "sigs.k8s.io/kind"),
            "ARG KIND_VERSION must be the sigs.k8s.io/kind require that "
            "`make kind` installs",
        )

    def test_helm_version_matches_tools_go_mod(self):
        self.assertEqual(
            dockerfile_arg("HELM_VERSION"),
            go_mod_require(TOOLS_GO_MOD, "helm.sh/helm/v3"),
            "ARG HELM_VERSION must be the helm.sh/helm/v3 require that "
            "`make helm` installs",
        )


class ValidationJobTest(unittest.TestCase):
    """The standalone Job manifest cannot read the pin files, so pin it here."""

    def test_node_image_matches_pin_file(self):
        self.assertEqual(
            validation_job_env("KIND_NODE_IMAGE"),
            read_pin(KIND_NODE_IMAGE_FILE),
            "gke-kind-validation-job.yaml must carry .kind-node-image verbatim, "
            "digest included",
        )

    def test_kind_version_matches_tools_go_mod(self):
        self.assertEqual(
            validation_job_env("KIND_VERSION"),
            go_mod_require(TOOLS_GO_MOD, "sigs.k8s.io/kind"),
            "gke-kind-validation-job.yaml must pin the sigs.k8s.io/kind require",
        )


class AlfredPinTest(unittest.TestCase):
    """The upstream alfred commit has one home, and every builder reads it."""

    def test_pin_is_a_full_commit_sha(self):
        self.assertRegex(
            read_pin(ALFRED_OSS_REF_FILE),
            FULL_COMMIT_SHA,
            ".alfred-oss-ref must hold one full commit SHA of ome-projects/ome",
        )

    def test_dockerfile_requires_the_pin_as_a_build_arg(self):
        text = ALFRED_DOCKERFILE.read_text()
        self.assertRegex(
            text,
            r"(?m)^ARG ALFRED_OSS_REF$",
            "alfred.Dockerfile must declare ARG ALFRED_OSS_REF without a default; "
            "a default would be a second copy of the pin",
        )
        self.assertNotRegex(text, r"(?m)^ARG ALFRED_OSS_REF=")

    def test_makefile_reads_the_pin_file(self):
        self.assertIn(
            ".alfred-oss-ref",
            MAKEFILE.read_text(),
            "make alfred-image must take ALFRED_OSS_REF from .alfred-oss-ref",
        )


class EffectiveVersionTest(unittest.TestCase):
    """Synthetic go.mod bodies pinning how replace directives are resolved."""

    def effective(self, body: str) -> dict:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "go.mod"
            path.write_text(textwrap.dedent(body))
            return {
                module: (version, origin)
                for module, version, _, origin in go_mod_effective_k8s_versions(path)
            }

    def test_replace_wins_over_a_stale_require(self):
        # k8s.io/kubernetes drags staging modules in at stale published
        # versions and levels them with a replace block; the require line is
        # then cosmetic and reading it reports a skew that does not exist.
        got = self.effective(
            """\
            require (
            \tk8s.io/csi-translation-lib v0.32.5 // indirect
            )

            replace (
            \tk8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.35.4
            )
            """
        )
        self.assertEqual(got["k8s.io/csi-translation-lib"], ("v0.35.4", "replace"))

    def test_replace_of_an_unrequired_module_is_inert(self):
        got = self.effective(
            """\
            require (
            \tk8s.io/api v0.35.4
            )

            replace (
            \tk8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.31.8
            )
            """
        )
        self.assertNotIn("k8s.io/legacy-cloud-providers", got)
        self.assertEqual(got["k8s.io/api"], ("v0.35.4", "require"))

    def test_versioned_replace_form_is_resolved(self):
        got = self.effective(
            """\
            require (
            \tk8s.io/api v0.32.5
            )

            replace k8s.io/api v0.32.5 => k8s.io/api v0.35.4
            """
        )
        self.assertEqual(got["k8s.io/api"], ("v0.35.4", "replace"))

    def test_non_staging_schemes_are_not_compared(self):
        got = self.effective(
            """\
            require (
            \tk8s.io/klog/v2 v2.130.1
            \tk8s.io/utils v0.0.0-20251002143259-bc988d571ff4
            \tk8s.io/kubernetes v1.35.4
            )
            """
        )
        # Collected, but only the v1.x scheme carries a comparable minor; klog
        # and the pseudo-version match neither pattern and are skipped.
        self.assertIsNone(STAGING_VERSION.match(got["k8s.io/klog/v2"][0]))
        self.assertIsNone(KUBERNETES_VERSION.match(got["k8s.io/klog/v2"][0]))
        self.assertIsNone(STAGING_VERSION.match(got["k8s.io/utils"][0]))
        self.assertEqual(KUBERNETES_VERSION.match(got["k8s.io/kubernetes"][0]).group(1), "35")


class NoStrayLiteralsTest(unittest.TestCase):
    """No new file may hand-maintain its own kindest/node reference."""

    def test_only_allowlisted_files_spell_a_node_image(self):
        offenders = []
        for path in sorted(REPO_ROOT.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(REPO_ROOT)
            if SWEEP_PRUNE_DIRS.intersection(relative.parts):
                continue
            if relative in KIND_NODE_LITERAL_ALLOWLIST:
                continue
            try:
                text = path.read_text()
            except (UnicodeDecodeError, OSError):
                continue
            for number, line in enumerate(text.splitlines(), start=1):
                if KIND_NODE_LITERAL.search(line):
                    offenders.append(f"{relative}:{number} {line.strip()}")
        self.assertEqual(
            offenders,
            [],
            "read .kind-node-image instead of spelling a node image inline "
            "(or add the file to KIND_NODE_LITERAL_ALLOWLIST and pin it above):\n  "
            + "\n  ".join(offenders),
        )


if __name__ == "__main__":
    unittest.main()
