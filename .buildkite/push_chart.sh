#!/bin/sh
# Package (and on main, push) one Helm chart to OCI ECR.
#
# Usage: sh .buildkite/push_chart.sh <chart-name>
set -eu

CHART="${1:?chart required}"

AWS_REGION="${AWS_REGION:-us-east-2}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-588845226011}"
ECR_REGISTRY="${ECR_REGISTRY:-${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"
CHART_REPO="oci://${ECR_REGISTRY}/ome/charts"

DATE_TAG="$(date -u +%Y%m%d)"
SHORT_SHA="$(echo "${BUILDKITE_COMMIT:-HEAD}" | cut -c1-8)"
CHART_VERSION="${DATE_TAG}-${SHORT_SHA}"

HELM_VERSION="${HELM_VERSION:-v3.16.2}"
if ! command -v helm >/dev/null 2>&1; then
  apt-get update
  apt-get install -y --no-install-recommends curl ca-certificates
  curl -sSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz" -o /tmp/helm.tgz
  tar -xzf /tmp/helm.tgz -C /tmp
  mv /tmp/linux-amd64/helm /usr/local/bin/helm
fi

# Stamp the version into Chart.yaml before packaging.
sed -i "s/^version:.*/version: ${CHART_VERSION}/" "charts/${CHART}/Chart.yaml"
sed -i "s/^appVersion:.*/appVersion: \"${CHART_VERSION}\"/" "charts/${CHART}/Chart.yaml"

helm package "charts/${CHART}"

if [ "${BUILDKITE_BRANCH:-}" = "main" ]; then
  if ! command -v aws >/dev/null 2>&1; then
    apt-get update
    apt-get install -y --no-install-recommends curl unzip ca-certificates
    curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
    unzip -q /tmp/awscliv2.zip -d /tmp
    /tmp/aws/install
  fi
  aws ecr get-login-password --region "$AWS_REGION" \
    | helm registry login --username AWS --password-stdin "$ECR_REGISTRY"
  helm push "${CHART}-${CHART_VERSION}.tgz" "$CHART_REPO"
  echo "pushed ${CHART}-${CHART_VERSION}.tgz to ${CHART_REPO}"
else
  echo "branch=${BUILDKITE_BRANCH:-?} is not main; skipping push"
fi
