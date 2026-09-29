# ome-bot: the deterministic core of the OME Slack bot (meta-internal/omebot).
# It runs read-only kubectl-ome commands against every cluster in its registry
# and reports which clusters run OME; a relay in front of it owns Slack.
#
# Two binaries ship in the image:
#   /ome-bot                  built from this repository with -tags meta_internal
#   /usr/local/bin/kubectl-ome built from the upstream repository at the commit
#                             in .kubectl-ome-oss-ref, exactly like
#                             alfred.Dockerfile builds alfred: the CLI carries
#                             all domain logic and lives upstream only.
#
# The runtime base is a real distro for the same reason as
# ome-quota-manager.Dockerfile: the exec-credential provider the platform
# injects into the pod is dynamically linked and needs a C runtime.
ARG BASE_IMAGE=oraclelinux:10-slim
ARG GO_BUILDER_IMAGE=golang:1.26

FROM ${GO_BUILDER_IMAGE} AS cli-builder
ARG KUBECTL_OME_REPO=https://github.com/ome-projects/ome.git
# Upstream commit the CLI is built from; see .kubectl-ome-oss-ref. Passed
# explicitly because the CLI lives on upstream main and no release tag
# carries it yet.
ARG KUBECTL_OME_REF
RUN test -n "${KUBECTL_OME_REF}" || { echo "KUBECTL_OME_REF is required (see .kubectl-ome-oss-ref)" >&2; exit 1; }
WORKDIR /src/upstream
RUN git init -q . \
    && git remote add origin "${KUBECTL_OME_REPO}" \
    && git fetch -q --depth 1 origin "${KUBECTL_OME_REF}" \
    && git checkout -q FETCH_HEAD
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags "-s -w -X sigs.k8s.io/ome/pkg/version.GitVersion=${KUBECTL_OME_REF} -X sigs.k8s.io/ome/pkg/version.GitCommit=${KUBECTL_OME_REF}" \
      -o /out/kubectl-ome ./cmd/kubectl-ome

FROM ${GO_BUILDER_IMAGE} AS bot-builder
WORKDIR /workspace
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY meta-internal/omebot/ ./meta-internal/omebot/
ARG TARGETOS
ARG TARGETARCH
ARG VERSION
ARG GIT_TAG
ARG GIT_COMMIT
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -tags meta_internal \
      -ldflags "-s -w -X github.com/ome-projects/ome/pkg/version.GitVersion=${GIT_TAG} -X github.com/ome-projects/ome/pkg/version.GitCommit=${GIT_COMMIT}" \
      -o /ome-bot ./meta-internal/omebot/cmd/ome-bot

ARG BASE_IMAGE
FROM ${BASE_IMAGE}
RUN if [ -f /usr/bin/microdnf ]; then \
        microdnf update -y && \
        microdnf install -y glibc libgcc libstdc++ openssl-libs && \
        microdnf clean all; \
    elif [ -f /usr/bin/apt-get ]; then \
        apt-get update && \
        apt-get install -y ca-certificates libc6 libgcc-s1 libstdc++6 libssl3 && \
        apt-get upgrade -y && \
        apt-get clean && rm -rf /var/lib/apt/lists/*; \
    fi
WORKDIR /
COPY --from=cli-builder /out/kubectl-ome /usr/local/bin/kubectl-ome
COPY --from=bot-builder /ome-bot /ome-bot
USER 65532:65532
ENTRYPOINT ["/ome-bot"]
