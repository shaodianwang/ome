#!/usr/bin/env bash
set -euo pipefail

TAG="${1:-${TAG:-}}"
if [[ -z "${TAG}" ]]; then
  echo "usage: hack/scripts/publish-inference-deployment-operator-release.sh <tag>" >&2
  echo "example: hack/scripts/publish-inference-deployment-operator-release.sh 0.1.3" >&2
  exit 1
fi

AWS_REGION="${AWS_REGION:-us-east-2}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-588845226011}"
REGISTRY="${REGISTRY:-${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"

IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-msl_infra/inference-deployment-operator}"
IMAGE="${IMAGE:-${REGISTRY}/${IMAGE_REPOSITORY}}"

CHART_REPOSITORY="${CHART_REPOSITORY:-msl_infra/inference-deployment-operator-chart}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"

cd "$(dirname "$0")/../.."

GIT_COMMIT="${GIT_COMMIT:-$(git rev-parse HEAD)}"

docker logout "${REGISTRY}" || true

aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${REGISTRY}"

docker buildx build \
  --platform "${PLATFORMS}" \
  --build-arg VERSION="${TAG}" \
  --build-arg GIT_TAG="${TAG}" \
  --build-arg GIT_COMMIT="${GIT_COMMIT}" \
  -f dockerfiles/inference-deployment-operator.Dockerfile \
  -t "${IMAGE}:${TAG}" \
  --push \
  .

helm package charts/inference-deployment-operator \
  --version "${TAG}" \
  --app-version "${TAG}"

aws ecr get-login-password --region "${AWS_REGION}" \
  | helm registry login \
    --username AWS \
    --password-stdin "${REGISTRY}"

helm push "inference-deployment-operator-${TAG}.tgz" \
  "oci://${REGISTRY}/${CHART_REPOSITORY}"
