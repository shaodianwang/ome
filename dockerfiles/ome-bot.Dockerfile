# ome-bot: the deterministic core of the OME Slack bot (meta-internal/omebot).
# It runs read-only kubectl-ome commands against every cluster in its registry
# and reports which clusters run OME; a relay in front of it owns Slack.
#
# Two binaries ship in the image:
#   /ome-bot                  built from this repository with -tags meta_internal
#   /usr/local/bin/kubectl-ome built from the kubectl-ome source repository at the commit
#                             in .kubectl-ome-oss-ref, exactly like
#                             alfred.Dockerfile builds alfred: the CLI carries
#                             all domain logic and lives in the kubectl-ome source only.
#
# The runtime stage also carries python3 for the standard-library CI scripts
# the tool registry runs, and read-only trees for the built-in docs tool:
#   /opt/ome/{docs,oeps,skills}   from this repository
#   /opt/ome/upstream/...         the kubectl-ome user docs and README
#                                 from the same clone the CLI is built from
# They are root-owned so the runtime user can read but never rewrite them.
#
# The runtime base is a real distro for the same reason as
# ome-quota-manager.Dockerfile: the exec-credential provider the platform
# injects into the pod is dynamically linked and needs a C runtime.
ARG BASE_IMAGE=oraclelinux:10-slim
ARG GO_BUILDER_IMAGE=golang:1.26

FROM ${GO_BUILDER_IMAGE} AS cli-builder
ARG KUBECTL_OME_REPO=https://github.com/ome-projects/ome.git
# The kubectl-ome commit the CLI is built from; see .kubectl-ome-oss-ref. Passed
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

# The docs trees are rebuilt with cp so every file is created in one RUN,
# where setting modes works on every builder; files copied from the context
# keep modes the builder may refuse to change in a later step.
FROM ${GO_BUILDER_IMAGE} AS docs-tree
COPY docs/ /src/docs/
COPY oeps/ /src/oeps/
COPY .agents/skills/ /src/skills/
COPY --from=cli-builder /src/upstream/site/content/en/docs/ /src/upstream/site/content/en/docs/
COPY --from=cli-builder /src/upstream/cmd/kubectl-ome/README.md /src/upstream/cmd/kubectl-ome/README.md
RUN mkdir -p /opt/ome \
    && cp -R /src/. /opt/ome/ \
    && find /opt/ome -type d -exec chmod 0755 {} + \
    && find /opt/ome -type f -exec chmod 0644 {} +

ARG BASE_IMAGE
FROM ${BASE_IMAGE}
RUN if [ -f /usr/bin/microdnf ]; then \
        microdnf update -y && \
        microdnf install -y glibc libgcc libstdc++ openssl-libs python3 && \
        microdnf clean all; \
    elif [ -f /usr/bin/apt-get ]; then \
        apt-get update && \
        apt-get install -y ca-certificates libc6 libgcc-s1 libstdc++6 libssl3 python3 && \
        apt-get upgrade -y && \
        apt-get clean && rm -rf /var/lib/apt/lists/*; \
    fi
WORKDIR /
COPY --from=cli-builder /out/kubectl-ome /usr/local/bin/kubectl-ome
COPY --from=bot-builder /ome-bot /ome-bot
COPY --from=docs-tree /opt/ome /opt/ome
RUN python3 --version
USER 65532:65532
ENTRYPOINT ["/ome-bot"]
