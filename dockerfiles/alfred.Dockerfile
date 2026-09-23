# alfred: the upstream GPU cluster caretaker (OEP-0008 in ome-projects/ome),
# built unmodified from the upstream source at the commit in .alfred-oss-ref.
# Alfred is upstream-canonical (nothing under pkg/alfred lives in this repo), so
# this Dockerfile fetches that tree itself instead of copying the repo. Pure
# Go, no CGO/xet. The placement simulator is a nested Go module
# (pkg/alfred/simulator) with its own kube-scheduler pin and relative replace
# directives, so both binaries are built from the full source checkout.
ARG BASE_IMAGE=oraclelinux:10-slim
ARG GO_BUILDER_IMAGE=golang:1.26
FROM ${GO_BUILDER_IMAGE} AS builder

ARG ALFRED_OSS_REPO=https://github.com/ome-projects/ome.git
# No default on purpose: .alfred-oss-ref is the only copy of the pin.
# `make alfred-image` and the CI image step pass it through.
ARG ALFRED_OSS_REF
RUN test -n "${ALFRED_OSS_REF}" || { echo "ALFRED_OSS_REF is required (see .alfred-oss-ref)" >&2; exit 1; }

WORKDIR /workspace
RUN git init -q . \
    && git remote add origin "${ALFRED_OSS_REPO}" \
    && git fetch -q --depth 1 origin "${ALFRED_OSS_REF}" \
    && git checkout -q --detach FETCH_HEAD

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags "-s -w -X sigs.k8s.io/ome/pkg/version.GitVersion=${ALFRED_OSS_REF} -X sigs.k8s.io/ome/pkg/version.GitCommit=${ALFRED_OSS_REF}" \
      -o /out/alfred ./cmd/alfred

# The simulator's kube-scheduler pin and staging replaces stay in its own
# module: build it as its own main module so none of that enters alfred's.
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    cd pkg/alfred/simulator \
    && CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
       go build -trimpath -ldflags "-s -w" -o /out/alfred-simulator ./cmd/alfred-simulator

ARG BASE_IMAGE
FROM ${BASE_IMAGE}

RUN if [ -f /usr/bin/microdnf ]; then \
        microdnf update -y && \
        microdnf install -y ca-certificates && \
        microdnf clean all; \
    elif [ -f /usr/bin/apt-get ]; then \
        apt-get update && apt-get install -y ca-certificates && \
        apt-get upgrade -y && \
        apt-get clean && rm -rf /var/lib/apt/lists/*; \
    fi
WORKDIR /
COPY --from=builder /out/alfred /out/alfred-simulator ./
USER 65532:65532

ENTRYPOINT ["/alfred"]
