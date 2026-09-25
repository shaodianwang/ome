#!/usr/bin/env python3
"""Block internal identifiers from entering paths that can leave this repository.

Every tracked text file is scanned unless hack/internal-tokens.allow exempts it
with a reason. The patterns live in hack/internal-tokens.txt. On top of the
token scan the checker enforces three structural rules:

  * every top-level entry of the repository must be classified below as
    SHIPPED (flows to the public repo), INTERNAL_ONLY (deployment config that
    never leaves) or CLEAN (not shipped today but held to the same standard),
    so a new directory cannot slip past the boundary unreviewed;
  * Go files under the shipped roots may import the never-shipped packages
    (meta-internal, the sharded model-cache backend, pkg/kubectl) only from the
    wire-up files listed below, so a new wire-up is a reviewed decision;
  * file paths are scanned like file contents.

Usage:
  check_internal_tokens.py --tree            # every tracked file (CI)
  check_internal_tokens.py --files a b c     # pre-commit passes staged files
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import re
import subprocess
import sys

# Paths the public repository receives (module-path rewrite aside).
SHIPPED = (
    "pkg/", "cmd/", "internal/", "scheduler/",
    "charts/ome-resources/", "charts/ome-quota-manager/", "charts/ome-scheduler/",
    "charts/ome-serving/", "charts/ome-crd/", "config/configmap/",
)
# Trees that are internal through and through (the console, its chart, the
# build-tag extensions). Exempt from the token scan by construction; every
# other path is scanned and exempted only per group, with a reason, in
# hack/internal-tokens.allow.
INTERNAL_ONLY = (
    "muse/", "charts/muse/", "meta-internal/",
)
# Everything else: scanned. Shipped paths additionally get the `upstream`
# scope and the import guard.
CLEAN = (
    "oeps/", "tests/", "hack/", "docs/", "ome-client-rs/", "charts/", "config/",
    ".buildkite/", ".devcontainer/", ".github/", "dockerfiles/", "grafana/",
    "README.md", "CONTRIBUTING.md", "CLAUDE.md", "AGENTS.md", "Makefile", "Makefile-deps.mk",
    ".golangci.yml", ".pre-commit-config.yaml", "go.mod", "go.sum", ".alfred-oss-ref",
    ".gitignore", ".dockerignore", ".envtest-k8s-version", ".kind-node-image",
    ".kueue-crd-version", "PROJECT",
)
# Every top-level entry must appear in one of the three tuples (or be a
# prefix of one). A new top-level directory fails until it is classified.

# Packages the public repository never receives, and the only files allowed
# to import them. Anything else importing them is a new wire-up that the sync
# would have to carve; it must be reviewed here first.
GUARDED_IMPORTS = {
    "github.com/ome-projects/ome/meta-internal/": {
        "cmd/manager/register_meta_internal.go",
    },
    "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded": {
        "cmd/manager/main.go",
        "cmd/manager/main_test.go",
        "pkg/controller/v1beta1/basemodel/controller.go",
        "pkg/controller/v1beta1/basemodel/controller_test.go",
        "pkg/controller/v1beta1/controllerconfig/configmap.go",
        "pkg/controller/v1beta1/controllerconfig/configmap_test.go",
        "pkg/controller/v1beta1/inferenceservice/components/base.go",
        "pkg/controller/v1beta1/inferenceservice/components/base_test.go",
        "pkg/controller/v1beta1/inferenceservice/controller.go",
    },
    "github.com/ome-projects/ome/pkg/kubectl": {
        "cmd/kubectl-qt/main.go",
    },
}
GUARDED_SELF_PREFIXES = (
    "meta-internal/",
    "pkg/controller/v1beta1/basemodel/backends/sharded/",
    "pkg/kubectl/",
)

SKIP_PREFIXES = ("pkg/client/", "pkg/openapi/", "config/crd/", "charts/ome-crd/templates/")
SKIP_SUFFIXES = (
    ".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf", ".zip", ".gz", ".tgz",
    ".safetensors", ".bin", ".a", ".dylib", ".so", ".woff", ".woff2", ".ttf",
    "go.sum", "Cargo.lock", "package-lock.json",
)
IMPORT_LINE = re.compile(r'^\s*(?:import\s+)?(?:\w+\s+)?"(github\.com/ome-projects/ome/[^"]+)"')


class Pattern:
    def __init__(self, group: str, scope: str, regex: re.Pattern):
        self.group, self.scope, self.regex = group, scope, regex


def load_patterns(path: str) -> list[Pattern]:
    patterns: list[Pattern] = []
    with open(path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split("|", 3)
            if len(parts) != 4:
                sys.exit(f"{path}:{lineno}: expected group|scope|flags|regex")
            group, scope, flags = (p.strip() for p in parts[:3])
            regex = parts[3]
            if scope not in ("all", "upstream"):
                sys.exit(f"{path}:{lineno}: scope must be all or upstream")
            try:
                compiled = re.compile(regex, re.IGNORECASE if "i" in flags else 0)
            except re.error as err:
                sys.exit(f"{path}:{lineno}: bad regex: {err}")
            patterns.append(Pattern(group, scope, compiled))
    return patterns


class AllowRule:
    def __init__(self, glob: str, groups: set[str] | None, reason: str, lineno: int):
        self.glob, self.groups, self.reason, self.lineno = glob, groups, reason, lineno
        self.hits = 0

    def matches(self, rel: str) -> bool:
        if fnmatch.fnmatch(rel, self.glob):
            return True
        return self.glob.endswith("/**") and rel.startswith(self.glob[:-2])


def load_allow(path: str) -> list[AllowRule]:
    rules: list[AllowRule] = []
    if not os.path.exists(path):
        return rules
    with open(path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            body, _, reason = line.partition("#")
            fields = body.split()
            if not fields or not reason.strip():
                sys.exit(f"{path}:{lineno}: every exemption needs '<glob> [groups] # reason'")
            groups = None if len(fields) == 1 or fields[1] == "*" else set(fields[1].split(","))
            rules.append(AllowRule(fields[0], groups, reason.strip(), lineno))
    return rules


def classify(rel: str) -> str | None:
    for name, group in (("SHIPPED", SHIPPED), ("INTERNAL_ONLY", INTERNAL_ONLY), ("CLEAN", CLEAN)):
        if any(rel == p or rel.startswith(p) for p in group):
            # INTERNAL_ONLY wins over a CLEAN parent (charts/muse under charts/).
            pass
    if any(rel == p or rel.startswith(p) for p in INTERNAL_ONLY):
        return "INTERNAL_ONLY"
    if any(rel == p or rel.startswith(p) for p in SHIPPED):
        return "SHIPPED"
    if any(rel == p or rel.startswith(p) for p in CLEAN):
        return "CLEAN"
    return None


def allowed_groups(rel: str, rules: list[AllowRule]) -> set[str] | None:
    """None = everything exempt for this path; otherwise the exempt groups."""
    allowed: set[str] = set()
    for rule in rules:
        if rule.matches(rel):
            rule.hits += 1
            if rule.groups is None:
                return None
            allowed |= rule.groups
    return allowed


def tracked_files(root: str) -> list[str]:
    out = subprocess.run(["git", "-C", root, "ls-files", "-z"], check=True, capture_output=True).stdout
    return [p for p in out.decode("utf-8", "replace").split("\0") if p]


def scan_text(rel: str, text: str, patterns: list[Pattern], allowed: set[str], upstream: bool) -> list[str]:
    active = [p for p in patterns if p.group not in allowed and (p.scope == "all" or upstream)]
    findings: list[str] = []
    for p in active:
        m = p.regex.search(rel)
        if m:
            findings.append(f"{rel}: [{p.group}] path contains {m.group(0)!r}")
    for lineno, line in enumerate(text.splitlines(), 1):
        for p in active:
            m = p.regex.search(line)
            if m:
                shown = line.strip()
                if len(shown) > 140:
                    shown = shown[:137] + "..."
                findings.append(f"{rel}:{lineno}: [{p.group}] {m.group(0)!r}: {shown}")
    return findings


def scan_imports(rel: str, text: str) -> list[str]:
    if not rel.endswith(".go") or rel.startswith(GUARDED_SELF_PREFIXES):
        return []
    findings: list[str] = []
    for lineno, line in enumerate(text.splitlines(), 1):
        m = IMPORT_LINE.match(line)
        if not m:
            continue
        imported = m.group(1)
        for prefix, wireups in GUARDED_IMPORTS.items():
            if imported.startswith(prefix) and rel not in wireups:
                findings.append(
                    f"{rel}:{lineno}: [import-guard] imports {imported}, a package the public repository never "
                    f"receives; only the listed wire-up files may (see GUARDED_IMPORTS in hack/check_internal_tokens.py)")
    return findings


def scan_file(root: str, rel: str, patterns, rules) -> list[str]:
    kind = classify(rel)
    if kind == "INTERNAL_ONLY":
        return []
    allowed = allowed_groups(rel, rules)
    if allowed is None:
        return []
    if rel.startswith(SKIP_PREFIXES) or rel.endswith(SKIP_SUFFIXES) or "zz_generated" in os.path.basename(rel):
        return []
    try:
        with open(os.path.join(root, rel), "rb") as fh:
            data = fh.read()
    except OSError:
        return []
    if b"\0" in data[:8192]:
        return []
    text = data.decode("utf-8", "replace")
    upstream = kind == "SHIPPED"
    findings = scan_text(rel, text, patterns, allowed, upstream)
    if upstream:
        findings.extend(scan_imports(rel, text))
    return findings


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", default=None)
    parser.add_argument("--patterns", default=None)
    parser.add_argument("--allow", default=None)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--tree", action="store_true", help="scan every tracked file and check the structural rules")
    mode.add_argument("--files", nargs="*", default=None, help="scan these files (pre-commit)")
    args = parser.parse_args(argv)

    root = args.root or subprocess.run(["git", "rev-parse", "--show-toplevel"], check=True,
                                       capture_output=True, text=True).stdout.strip()
    patterns = load_patterns(args.patterns or os.path.join(root, "hack", "internal-tokens.txt"))
    rules = load_allow(args.allow or os.path.join(root, "hack", "internal-tokens.allow"))

    findings: list[str] = []
    if args.files is not None:
        rels = []
        for f in args.files:
            rel = os.path.relpath(os.path.abspath(f), root) if os.path.isabs(f) else os.path.normpath(f)
            rels.append(rel.replace(os.sep, "/"))
    else:
        rels = tracked_files(root)
        top = sorted({(r.split("/", 1)[0] + "/") if "/" in r else r for r in rels})
        for entry in top:
            if classify(entry) is None and classify(entry.rstrip("/")) is None:
                findings.append(f"{entry}: [unclassified] new top-level entry; add it to SHIPPED, INTERNAL_ONLY or "
                                f"CLEAN in hack/check_internal_tokens.py (and to CLAUDE.md's boundary section)")

    scanned = 0
    for rel in sorted(set(rels)):
        if classify(rel) is None:
            continue  # reported above in tree mode; unknown paths are never silently accepted there
        scanned += 1
        findings.extend(scan_file(root, rel, patterns, rules))

    if args.files is None:
        for rule in rules:
            if rule.hits == 0:
                findings.append(f"hack/internal-tokens.allow:{rule.lineno}: [stale-exemption] '{rule.glob}' matches no tracked file; remove it")

    for line in findings:
        print(line)
    if findings:
        print(f"\n{len(findings)} finding(s) in {scanned} scanned file(s). Replace the identifier with a neutral one; "
              "only deployment configuration that never leaves this repository may be exempted, in "
              "hack/internal-tokens.allow, with a reason.", file=sys.stderr)
        return 1
    print(f"internal-tokens: clean ({scanned} files, {len(rules)} exemptions)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
