#!/bin/sh
# Read-only candidate discovery step for the OME RC daily pipeline.
set -eu

ARTIFACT_DIR="artifacts/daily-connectivity"
mkdir -p "$ARTIFACT_DIR"

CRANE_VERSION="${CRANE_VERSION:-0.20.6}"
if ! command -v crane >/dev/null 2>&1; then
  case "$(uname -m)" in
    x86_64|amd64) CRANE_ARCH="x86_64" ;;
    aarch64|arm64) CRANE_ARCH="arm64" ;;
    *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
  esac
  TOOL_DIR="$(mktemp -d)"
  trap 'rm -rf "$TOOL_DIR"' EXIT
  curl -fsSL "https://github.com/google/go-containerregistry/releases/download/v${CRANE_VERSION}/go-containerregistry_Linux_${CRANE_ARCH}.tar.gz" \
    | tar -xz -C "$TOOL_DIR" crane
  PATH="$TOOL_DIR:$PATH"
  export PATH
fi

AWS_REGION="${AWS_REGION:-us-east-2}"
PROD_ECR_REGISTRY="${PROD_ECR_REGISTRY:-168653207203.dkr.ecr.us-east-2.amazonaws.com}"
export AWS_REGION PROD_ECR_REGISTRY
# shellcheck source=.buildkite/_ecr_login.sh
. "$(dirname "$0")/_ecr_login.sh"
ecr_login_for prod crane auth login --username AWS --password-stdin "$PROD_ECR_REGISTRY"

echo "--- discover newest complete OME release"
python3 .buildkite/daily_candidate.py --output "$ARTIFACT_DIR/candidate.json"
CANDIDATE_B64="$(base64 <"$ARTIFACT_DIR/candidate.json" | tr -d '\n')"
buildkite-agent meta-data set daily-candidate-b64 "$CANDIDATE_B64"
unset CANDIDATE_B64

echo "--- test read-only argo-gitops access"
ARGO_GITOPS_REPO_URL="${ARGO_GITOPS_REPO_URL:-https://github.com/mslsrc/argo-gitops.git}"
if timeout 30 git ls-remote "$ARGO_GITOPS_REPO_URL" HEAD >"$ARTIFACT_DIR/gitops-ls-remote.txt" 2>"$ARTIFACT_DIR/gitops-ls-remote.err"; then
  GITOPS_READABLE=true
  echo "argo-gitops read access: available"
else
  GITOPS_READABLE=false
  echo "argo-gitops read access: unavailable (see artifact)"
fi
printf '{"repository":%s,"readable":%s}\n' \
  "$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$ARGO_GITOPS_REPO_URL")" \
  "$GITOPS_READABLE" >"$ARTIFACT_DIR/gitops-access.json"

buildkite-agent artifact upload "$ARTIFACT_DIR/*"
