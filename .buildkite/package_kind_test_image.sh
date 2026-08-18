#!/bin/sh
# Add the exact-commit Go KIND test bundle to the runner image GKE can pull.
set -eu

short_sha=$(printf '%.8s' "${BUILDKITE_COMMIT:?BUILDKITE_COMMIT is required}")
case "$short_sha" in
  *[!0-9a-f]*) echo "invalid commit SHA: $short_sha" >&2; exit 1 ;;
esac
build_number="${BUILDKITE_BUILD_NUMBER:?BUILDKITE_BUILD_NUMBER is required}"
case "$build_number" in
  *[!0-9]*) echo "invalid Buildkite build number: $build_number" >&2; exit 1 ;;
esac

AWS_REGION="${AWS_REGION:-us-east-2}"
PROD_AWS_ACCOUNT_ID="${PROD_AWS_ACCOUNT_ID:-168653207203}"
PROD_ECR_REGISTRY="${PROD_ECR_REGISTRY:-${PROD_AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"
repo="${PROD_ECR_REGISTRY}/msl_infra"
base_tag=latest
if [ "${BUILDKITE_BRANCH:-}" = main ]; then
  base_tag="$short_sha"
fi
target="${repo}/kind-ci:test-${short_sha}-${build_number}"
artifact_bundle=".buildkite/artifacts/omenative-kind-test.tgz"

command -v buildkite-agent >/dev/null 2>&1 || {
  echo "buildkite-agent is required to download the test bundle" >&2
  exit 1
}
mkdir -p "$(dirname "$artifact_bundle")"
buildkite-agent artifact download "$artifact_bundle" . --step test
test -s "$artifact_bundle"

if ! command -v crane >/dev/null 2>&1; then
  case "$(uname -m)" in
    x86_64|amd64) crane_arch=x86_64 ;;
    aarch64|arm64) crane_arch=arm64 ;;
    *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
  esac
  crane_version="${CRANE_VERSION:-0.20.6}"
  curl -fsSL "https://github.com/google/go-containerregistry/releases/download/v${crane_version}/go-containerregistry_Linux_${crane_arch}.tar.gz" \
    | tar -xz -C /usr/local/bin crane
fi

. "$(dirname "$0")/_ecr_login.sh"
ecr_login_for prod crane auth login --username AWS --password-stdin "$PROD_ECR_REGISTRY"

stage=$(mktemp -d)
trap 'rm -rf -- "$stage"' EXIT
mkdir -p "$stage/kind-test"
cp "$artifact_bundle" "$stage/kind-test/omenative-kind-test.tgz"
tar -C "$stage" -cf "$stage/kind-test.layer.tar" kind-test

echo "--- publish ${target} from kind-ci:${base_tag} ---"
crane append --platform linux/amd64 \
  --base "${repo}/kind-ci:${base_tag}" \
  --new_layer "$stage/kind-test.layer.tar" \
  --new_tag "$target"

digest=$(crane digest "$target")
echo "published ${target}@${digest}"
