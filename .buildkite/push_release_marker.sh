#!/bin/sh
# Package an atomic OME release marker and push it to ECR on main.
#
# Each component chart uploads its exact version as a Buildkite artifact after
# publication succeeds. The final wait gate in generate_pipeline.py ensures
# those artifacts and every hard test result exist before this script runs.
# For local validation, pass --dry-run and provide the versions as space-
# separated name=version pairs in OME_RELEASE_CHART_VERSIONS.
set -eu

DRY_RUN=false
if [ "${1:-}" = "--dry-run" ]; then
  DRY_RUN=true
  shift
fi
if [ "$#" -ne 0 ]; then
  echo "usage: $0 [--dry-run]" >&2
  exit 2
fi

if [ "${BUILDKITE_BRANCH:-}" != "main" ] && [ "$DRY_RUN" = false ]; then
  echo "branch=${BUILDKITE_BRANCH:-?} is not main; skipping release marker"
  exit 0
fi

AWS_REGION="${AWS_REGION:-us-east-2}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-168653207203}"
ECR_REGISTRY="${ECR_REGISTRY:-${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"
CHART_REPO="oci://${ECR_REGISTRY}/msl_infra/ome-charts"
case "$AWS_ACCOUNT_ID" in
  588845226011) ECR_ENV=dev ;;
  168653207203) ECR_ENV=prod ;;
  *) ECR_ENV="${OME_RELEASE_ECR_ENV:?OME_RELEASE_ECR_ENV is required for account ${AWS_ACCOUNT_ID}}" ;;
esac

RELEASE_CHARTS="${OME_RELEASE_CHARTS:?OME_RELEASE_CHARTS is required}"
RELEASE_IMAGES="${OME_RELEASE_IMAGES:?OME_RELEASE_IMAGES is required}"
COMMIT="${BUILDKITE_COMMIT:?BUILDKITE_COMMIT is required}"
SHORT_SHA="$(printf '%s' "$COMMIT" | cut -c1-8)"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT HUP INT TERM
mkdir -p "$WORK_DIR/bin" "$WORK_DIR/chart-versions"

HELM_VERSION="${HELM_VERSION:-v3.16.2}"
if ! command -v helm >/dev/null 2>&1; then
  curl -sSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz" -o "$WORK_DIR/helm.tgz"
  tar -xzf "$WORK_DIR/helm.tgz" -C "$WORK_DIR"
  mv "$WORK_DIR/linux-amd64/helm" "$WORK_DIR/bin/helm"
  PATH="$WORK_DIR/bin:$PATH"
fi

# The commit timestamp keeps SemVer ordering chronological while making the
# marker version deterministic for a commit, including across job retries.
MARKER_VERSION="${OME_RELEASE_VERSION:-}"
if [ -z "$MARKER_VERSION" ]; then
  COMMIT_EPOCH="$(git show -s --format=%ct "$COMMIT")"
  case "$COMMIT_EPOCH" in
    ""|*[!0-9]*)
      echo "could not resolve commit timestamp for ${COMMIT}" >&2
      exit 1
      ;;
  esac
  TIMESTAMP="$(date -u -d "@${COMMIT_EPOCH}" +%Y%m%d%H%M)"
  MARKER_VERSION="1.0.0-${TIMESTAMP}.g${SHORT_SHA}"
fi

for chart in $RELEASE_CHARTS; do
  version=""
  for entry in ${OME_RELEASE_CHART_VERSIONS:-}; do
    case "$entry" in
      "$chart="*) version="${entry#*=}" ;;
    esac
  done
  if [ -z "$version" ]; then
    if ! command -v buildkite-agent >/dev/null 2>&1; then
      echo "set OME_RELEASE_CHART_VERSIONS or run inside Buildkite" >&2
      exit 1
    fi
    artifact=".buildkite/artifacts/chart-versions/${chart}"
    buildkite-agent artifact download "$artifact" . --step "chart-${chart}"
    version="$(sed -n '1p' "$artifact")"
  fi
  case "$version" in
    1.0.0-*.g"$SHORT_SHA") ;;
    *)
      echo "chart ${chart} has invalid version for commit ${SHORT_SHA}: ${version}" >&2
      exit 1
      ;;
  esac
  printf '%s\n' "$version" >"$WORK_DIR/chart-versions/$chart"
done

# The marker asserts a chart version per component and an image tag per runtime
# image. Chart versions come from artifacts the publishing step wrote, so they
# are evidence; the image tags are derived from the commit and would otherwise
# be a guess. Confirm each one resolves in prod ECR, the same gate
# daily_candidate.py applies before calling a SHA a complete release —
# otherwise a mirror that never ran yields a "qualified" marker whose images
# 404, and the failure only surfaces as ErrImagePull after Kargo promotes it.
CRANE_VERSION="${CRANE_VERSION:-0.20.6}"
if ! command -v crane >/dev/null 2>&1; then
  case "$(uname -m)" in
    x86_64|amd64) CRANE_ARCH="x86_64" ;;
    aarch64|arm64) CRANE_ARCH="arm64" ;;
    *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
  esac
  curl -fsSL "https://github.com/google/go-containerregistry/releases/download/v${CRANE_VERSION}/go-containerregistry_Linux_${CRANE_ARCH}.tar.gz" \
    | tar -xz -C "$WORK_DIR/bin" crane
  PATH="$WORK_DIR/bin:$PATH"
fi

. "$(dirname "$0")/_ecr_login.sh"
ecr_login_for "$ECR_ENV" crane auth login --username AWS --password-stdin "$ECR_REGISTRY"

for image in $RELEASE_IMAGES; do
  reference="${ECR_REGISTRY}/msl_infra/${image}:${SHORT_SHA}"
  if ! crane manifest "$reference" >/dev/null 2>&1; then
    echo "image ${reference} is absent from prod ECR; refusing to publish marker" >&2
    exit 1
  fi
done

cp -R charts/ome-release "$WORK_DIR/ome-release"

sed -i \
  -e "s|^  ome.io/source-revision:.*|  ome.io/source-revision: \"${COMMIT}\"|" \
  "$WORK_DIR/ome-release/Chart.yaml"

VALUES_FILE="$WORK_DIR/ome-release/values.yaml"
{
  printf 'release:\n'
  printf '  sourceRevision: "%s"\n' "$COMMIT"
  printf '  # Install ome-crd before installing any other component chart.\n'
  printf '  charts:\n'
  for chart in $RELEASE_CHARTS; do
    version="$(sed -n '1p' "$WORK_DIR/chart-versions/${chart}")"
    printf '    %s:\n' "$chart"
    printf '      repository: "oci://%s/msl_infra/ome-charts/%s"\n' "$ECR_REGISTRY" "$chart"
    printf '      version: "%s"\n' "$version"
  done
  printf '  images:\n'
  for image in $RELEASE_IMAGES; do
    printf '    %s:\n' "$image"
    printf '      repository: "%s/msl_infra/%s"\n' "$ECR_REGISTRY" "$image"
    printf '      tag: "%s"\n' "$SHORT_SHA"
  done
} >"$VALUES_FILE"

helm lint "$WORK_DIR/ome-release"
helm package "$WORK_DIR/ome-release" \
  --version "$MARKER_VERSION" \
  --app-version "$COMMIT" \
  --destination "$WORK_DIR"

if [ "$DRY_RUN" = true ]; then
  echo "validated ome-release-${MARKER_VERSION}.tgz; dry run, skipping push"
  exit 0
fi

ecr_login_for "$ECR_ENV" helm registry login --username AWS --password-stdin "$ECR_REGISTRY"
helm push "$WORK_DIR/ome-release-${MARKER_VERSION}.tgz" "$CHART_REPO"
echo "pushed ome-release-${MARKER_VERSION}.tgz to ${CHART_REPO}"
