#!/usr/bin/env bash
# Fail the build when a scanned path carries an internal identifier.
# hack/check_internal_tokens.py holds the roots, hack/internal-tokens.txt the
# patterns and hack/internal-tokens.allow the deployment-only exemptions.

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

python3 -m unittest hack/check_internal_tokens_test.py
python3 hack/check_internal_tokens.py --tree
