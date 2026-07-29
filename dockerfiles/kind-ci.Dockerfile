# kind-ci: the workload image for the GKE-backed KIND CI step
# (integration-test-kind-gke) and for the offline kind validation Job.
#
# The MSL GCP RC cluster is egress-restricted (no public registry / download
# egress from pods), so the toolchain and the base images kind needs are baked
# in HERE — this image is built on the TW build queue (which has egress) and
# mirrored to prod ECR, which the cluster CAN pull. At run time nothing is
# fetched from a public registry.
#
# Baked in:
#   - kind, kubectl, docker CLI (the runner drives a privileged dind sidecar),
#   - crane, plus kindest/node + busybox pulled to docker-loadable tarballs
#     under /images (loaded into dind at run time so kind boots offline).
#
# Built by generate_pipeline.py via the cf-docker-image plugin (linux/amd64 +
# arm64). TARGETARCH is set automatically by buildx per platform.
FROM docker:28-cli

# cf-docker-image passes GO_BUILDER_IMAGE to every non-muse image build; this
# image doesn't use it, but declare it so the build-arg isn't flagged unused.
ARG GO_BUILDER_IMAGE=""

ARG TARGETARCH
ARG KIND_VERSION=v0.27.0
ARG KIND_NODE_TAG=v1.32.2
ARG CRANE_VERSION=v0.20.2

RUN apk add --no-cache curl bash

# Tooling. crane's release asset uses x86_64/arm64 naming; kind/kubectl use
# amd64/arm64 — map TARGETARCH accordingly so both build legs work.
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) CRANE_ARCH=x86_64 ;; \
      arm64) CRANE_ARCH=arm64 ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSLo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${TARGETARCH}"; \
    chmod +x /usr/local/bin/kind; \
    KVER="$(curl -fsSL https://dl.k8s.io/release/stable.txt)"; \
    curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${KVER}/bin/linux/${TARGETARCH}/kubectl"; \
    chmod +x /usr/local/bin/kubectl; \
    curl -fsSL "https://github.com/google/go-containerregistry/releases/download/${CRANE_VERSION}/go-containerregistry_Linux_${CRANE_ARCH}.tar.gz" \
      | tar -xz -C /usr/local/bin crane

# Bake the images kind needs as docker-loadable tarballs (crane is daemonless).
RUN set -eux; mkdir -p /images; \
    crane pull --platform "linux/${TARGETARCH}" "kindest/node:${KIND_NODE_TAG}" /images/kindest-node.tar; \
    crane pull --platform "linux/${TARGETARCH}" docker.io/library/busybox:latest /images/busybox.tar

# Fail the build early if anything didn't land.
RUN kind --version \
    && kubectl version --client=true --output=yaml >/dev/null \
    && test -s /images/kindest-node.tar \
    && test -s /images/busybox.tar
