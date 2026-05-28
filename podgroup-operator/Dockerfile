FROM --platform=$BUILDPLATFORM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /podgroup-operator ./cmd/podgroup-operator

FROM gcr.io/distroless/static:nonroot
COPY --from=build /podgroup-operator /podgroup-operator
USER 65532:65532
ENTRYPOINT ["/podgroup-operator"]
