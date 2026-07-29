# kind-ci: the offline workload image for the GKE-backed KIND CI step
# (integration-test-kind-gke) and the kind validation Job.
#
# The MSL GCP RC cluster has NO public egress from pods — it can't reach Docker
# Hub, dl.k8s.io, proxy.golang.org, crates.io, or gcr.io at run time. So every
# tool, module, crate, and base image the KIND suite needs is baked in HERE and
# pulled from prod ECR (which the cluster CAN pull). Built on the TW build queue
# (which HAS egress) and mirrored to prod ECR by generate_pipeline.py.
#
# Baked in:
#   - Go 1.25 toolchain + a pre-populated Go module cache (go test runs offline),
#   - Rust toolchain + a pre-fetched cargo cache for pkg/xet (make xet-build
#     runs offline; the KIND suite transitively links the xet staticlib),
#   - docker CLI, kind, kubectl, crane, jq + build deps,
#   - docker-loadable image tarballs under /images: kindest/node (the kind node),
#     golang:1.25 and gcr.io/distroless/static:nonroot (the two FROM stages of
#     tests/integration/kindutil/fake-serving/Dockerfile, which the suite
#     `docker build`s at run time), and busybox (validation Job probe).
#
# Built by generate_pipeline.py via the cf-docker-image plugin with build-context
# "." (repo root) — so the COPY steps below see the repo. TARGETARCH is set by
# buildx per platform.
#
# NOTE (iteration 1): this image has not yet been built on TW or exercised by a
# real shard. The pieces most likely to need adjustment on first build: the
# docker static-binary version, `go mod download all` coverage, `cargo fetch`
# for pkg/xet, and whether the suite's `docker build` of fake-serving runs fully
# offline against the loaded golang/distroless bases.
ARG GO_BUILDER_IMAGE=golang:1.25
FROM ${GO_BUILDER_IMAGE}

ARG TARGETARCH
ARG DOCKER_VERSION=28.0.1
ARG KIND_VERSION=v0.27.0
ARG KIND_NODE_TAG=v1.32.2
ARG CRANE_VERSION=v0.20.2
ARG FAKE_SERVING_BUILD_IMAGE=golang:1.25
ARG FAKE_SERVING_BASE_IMAGE=gcr.io/distroless/static:nonroot

# System build deps the xet staticlib + CRD tooling need (mirror run_tests.sh).
RUN apt-get update && apt-get install -y --no-install-recommends \
      jq libssl-dev pkg-config build-essential curl ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

# docker CLI (talks to the dind sidecar at run time), kind, kubectl, crane.
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) CRANE_ARCH=x86_64; DOCKER_ARCH=x86_64 ;; \
      arm64) CRANE_ARCH=arm64;  DOCKER_ARCH=aarch64 ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://download.docker.com/linux/static/stable/${DOCKER_ARCH}/docker-${DOCKER_VERSION}.tgz" \
      | tar -xz -C /usr/local/bin --strip-components=1 docker/docker; \
    curl -fsSLo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${TARGETARCH}"; \
    chmod +x /usr/local/bin/kind; \
    KVER="$(curl -fsSL https://dl.k8s.io/release/stable.txt)"; \
    curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${KVER}/bin/linux/${TARGETARCH}/kubectl"; \
    chmod +x /usr/local/bin/kubectl; \
    curl -fsSL "https://github.com/google/go-containerregistry/releases/download/${CRANE_VERSION}/go-containerregistry_Linux_${CRANE_ARCH}.tar.gz" \
      | tar -xz -C /usr/local/bin crane

# Rust toolchain for pkg/xet.
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile minimal

# Bake the Go module cache. Copy just the module manifests first so this layer
# caches unless deps change, then download every module the build graph needs.
ENV GOMODCACHE=/go/pkg/mod
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download all

# Bake the cargo cache for the xet staticlib (crates.io is unreachable at run
# time). Cargo.lock is gitignored, so copy the whole crate dir and let cargo
# fetch resolve + populate CARGO_HOME (index + crates) for offline builds.
COPY pkg/xet/ pkg/xet/
RUN cd pkg/xet && cargo fetch

# Bake the images kind + the suite need as docker-loadable tarballs (crane is
# daemonless; --platform pins to this build leg's arch).
RUN set -eux; mkdir -p /images; \
    crane pull --platform "linux/${TARGETARCH}" "kindest/node:${KIND_NODE_TAG}"  /images/kindest-node.tar; \
    crane pull --platform "linux/${TARGETARCH}" "${FAKE_SERVING_BUILD_IMAGE}"    /images/fake-serving-build.tar; \
    crane pull --platform "linux/${TARGETARCH}" "${FAKE_SERVING_BASE_IMAGE}"     /images/fake-serving-base.tar; \
    crane pull --platform "linux/${TARGETARCH}" docker.io/library/busybox:latest /images/busybox.tar

# Sanity: fail the build early if anything is missing.
RUN go version && kind --version && kubectl version --client=true -o yaml >/dev/null \
    && cargo --version && docker --version \
    && test -s /images/kindest-node.tar && test -s /images/fake-serving-build.tar \
    && test -s /images/fake-serving-base.tar && test -s /images/busybox.tar

WORKDIR /
