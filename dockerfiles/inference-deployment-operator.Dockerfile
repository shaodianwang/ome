ARG BASE_IMAGE=oraclelinux:10-slim

FROM golang:1.25 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

COPY go.mod go.mod
COPY go.sum go.sum
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd/ cmd/
COPY pkg/ pkg/

ARG GIT_TAG
ARG GIT_COMMIT

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build \
    -ldflags "-X github.com/ome-projects/ome/pkg/version.GitVersion=${GIT_TAG} -X github.com/ome-projects/ome/pkg/version.GitCommit=${GIT_COMMIT}" \
    -o inference-deployment-operator ./cmd/inference-deployment-operator

ARG BASE_IMAGE
FROM ${BASE_IMAGE}

WORKDIR /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /workspace/inference-deployment-operator .
USER 65532:65532

ENTRYPOINT ["/inference-deployment-operator"]
