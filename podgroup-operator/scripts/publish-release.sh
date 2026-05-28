#!/usr/bin/env bash
set -euo pipefail

TAG="${1:-${TAG:-}}"
if [[ -z "${TAG}" ]]; then
  echo "usage: scripts/publish-release.sh <tag>" >&2
  echo "example: scripts/publish-release.sh 0.1.3" >&2
  exit 1
fi

AWS_REGION="${AWS_REGION:-us-east-2}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-588845226011}"
REGISTRY="${REGISTRY:-${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"
IMAGE="${IMAGE:-${REGISTRY}/msl_infra/podgroup-operator}"

cd "$(dirname "$0")/.."

docker logout ${REGISTRY}

aws ecr get-login-password --region "${AWS_REGION}" \
    | docker login --username AWS --password-stdin "${REGISTRY}"

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t "${IMAGE}:${TAG}" \
  --push \
  .

helm package charts/podgroup-operator \
  --version "${TAG}" \
  --app-version "${TAG}"

aws ecr get-login-password --region ${AWS_REGION} \
    | helm registry login \
        --username AWS \
        --password-stdin ${REGISTRY}

helm push "podgroup-operator-${TAG}.tgz" \
  "oci://${REGISTRY}/msl_infra/podgroup-operator-chart"
