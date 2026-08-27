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
# One bundle per KIND suite; every one is baked into the same runner image and
# selected at run time by OME_KIND_TEST_BUNDLE.
artifact_bundles="omenative-kind-test.tgz multicluster-kind-test.tgz"

command -v buildkite-agent >/dev/null 2>&1 || {
  echo "buildkite-agent is required to download the test bundle" >&2
  exit 1
}
mkdir -p .buildkite/artifacts
for bundle in $artifact_bundles; do
  buildkite-agent artifact download ".buildkite/artifacts/$bundle" . --step test
  test -s ".buildkite/artifacts/$bundle"
done
buildkite-agent artifact download ".buildkite/artifacts/kind" . --step test
test -s .buildkite/artifacts/kind

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
mkdir -p "$stage/kind-test" "$stage/images"
for bundle in $artifact_bundles; do
  cp ".buildkite/artifacts/$bundle" "$stage/kind-test/$bundle"
done

# The multi-cluster suite installs Kueue into its members, and the GKE agents
# have no egress, so the controller image has to arrive with the runner. It is
# pulled here rather than baked into kind-ci because the image build only pushes
# on main: a kind-ci change cannot be exercised on the branch that needs it.
#
# The tag comes from the manifest the bundle already carries, which `make
# kueue-install` rendered from go.mod — so the loaded image and the Deployment's
# image ref are the same by construction. /images is where the runner looks,
# matching the tarballs kind-ci bakes in.
kueue_version=$(tar -xzOf ".buildkite/artifacts/multicluster-kind-test.tgz" \
  ./dep-crds/kueue-install/version)
kueue_version=$(printf '%s' "$kueue_version" | tr -d '[:space:]')
[ -n "$kueue_version" ] || { echo "multi-cluster bundle has no Kueue version" >&2; exit 1; }
kueue_image="${KUEUE_IMAGE_REPO:-registry.k8s.io/kueue/kueue}:${kueue_version}"
echo "--- pull ${kueue_image}"
crane pull --platform linux/amd64 "$kueue_image" "$stage/images/kueue.tar"
crane validate --tarball "$stage/images/kueue.tar"

# The kind binary and node image ship in this layer for the same reason Kueue
# does, and it is load-bearing for them specifically: a bump to either is only
# useful on the branch that raises the KIND_NODE_IMAGE the runner asks for, and
# the base image that would otherwise carry them is a main-only artifact. Both
# paths shadow the base's copies, so what the runner boots is whatever this
# commit pins. Without that, the runner asks for a node image it has no egress
# to fetch and dies in `kind create cluster`.
#
# .kind-node-image is the single source of truth for the node image, the way
# .envtest-k8s-version is for the envtest control plane; its minor tracks that
# file so the KIND and envtest lanes exercise the same Kubernetes API. The kind
# version comes from hack/internal/tools/go.mod, which is also what `make kind`
# installs — read with awk so this step needs no Go toolchain.
node_image=$(tr -d '[:space:]' <.kind-node-image)
[ -n "$node_image" ] || { echo ".kind-node-image is empty" >&2; exit 1; }
node_tag=${node_image%@*}
node_digest=${node_image#*@}
# Pull by tag, not by digest: a digest ref carries no RepoTags, so the tarball
# would `docker load` untagged and kind would try to fetch the image it was
# just handed. Verifying the digest first keeps the pin honest.
if [ "$node_digest" != "$node_image" ]; then
  resolved=$(crane digest "$node_tag")
  [ "$resolved" = "$node_digest" ] || {
    echo "FATAL: $node_tag resolves to $resolved, not the pinned $node_digest" >&2
    exit 1
  }
fi
echo "--- pull ${node_image}"
crane pull --platform linux/amd64 "$node_tag" "$stage/images/kindest-node.tar"
crane validate --tarball "$stage/images/kindest-node.tar"

# The kind binary is built by the test step, which has the Go toolchain and
# the module proxy; this container has neither, and the kind release CDN
# answers 403 through the egress filter. Registries and the artifact store are
# the two transports that work here, and both are already load-bearing above.
mkdir -p "$stage/usr/local/bin"
cp .buildkite/artifacts/kind "$stage/usr/local/bin/kind"
chmod 755 "$stage/usr/local/bin/kind"
echo "--- kind $("$stage/usr/local/bin/kind" --version)"

# /images and /usr/local/bin already exist in the base; mktemp's 0700 would
# otherwise be layered over them and hide the baked content from a non-root
# runner.
chmod 755 "$stage/images" "$stage/usr" "$stage/usr/local" "$stage/usr/local/bin"
tar -C "$stage" -cf "$stage/kind-test.layer.tar" kind-test images usr

echo "--- publish ${target} from kind-ci:${base_tag} ---"
crane append --platform linux/amd64 \
  --base "${repo}/kind-ci:${base_tag}" \
  --new_layer "$stage/kind-test.layer.tar" \
  --new_tag "$target"

digest=$(crane digest "$target")
echo "published ${target}@${digest}"
