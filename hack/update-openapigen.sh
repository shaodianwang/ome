#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

KNOWN_VIOLATION_EXCEPTIONS=hack/violation_exceptions.list
CURRENT_VIOLATION_EXCEPTIONS=hack/current_violation_exceptions.list
OPENAPI_SPEC_FILE=pkg/openapi/openapi_generated.go

# openapi-gen requires the working directory to live on a Go import path that
# matches the module name declared in go.mod (github.com/ome-projects/ome).
# Rather than mutating the user's real GOPATH (the legacy approach renamed any
# existing checkout at $GOPATH/src/github.com/ome-projects/ome with a timestamp
# suffix and accumulated symlink cruft), we synthesize a throwaway GOPATH in a
# tmpdir whose only job is to satisfy that path requirement. This works
# uniformly from git worktrees, arbitrary clone paths, and the canonical
# $GOPATH/src/github.com/ome-projects/ome path, without touching anything
# outside the tmpdir.

ORIG_GOPATH=$(go env GOPATH)
if [[ -z $ORIG_GOPATH ]]
then
    echo >&2 "Error: GOPATH is not set. Please configure your GOPATH environment variable."
    exit 1
fi

CURRENT_DIR=$(pwd)

# Synthesize a throwaway path layout in a tmpdir. We do NOT touch GOPATH here:
# the repo uses Go modules, so import resolution is governed by go.mod, not
# GOPATH/src. The only thing openapi-gen cares about is that its cwd matches
# the module path declared in go.mod, which is satisfied by the symlink below.
SYMLINK_ROOT=$(mktemp -d -t ome-openapigen.XXXXXX)
trap 'rm -rf "$SYMLINK_ROOT"' EXIT

SYMLINK_PATH="$SYMLINK_ROOT/src/github.com/ome-projects/ome"
mkdir -p "$(dirname "$SYMLINK_PATH")"
ln -s "$CURRENT_DIR" "$SYMLINK_PATH"

# Redirect stdout to /dev/null but keep stderr
exec 3>&1 # Save the current stdout to file descriptor 3
exec 1>/dev/null # Redirect stdout to /dev/null

pushd "$SYMLINK_PATH" > /dev/null

# Generating OpenAPI specification
go run k8s.io/kube-openapi/cmd/openapi-gen \
    --output-pkg github.com/ome-projects/ome/pkg/openapi --output-dir "./pkg/openapi" \
    --output-file "openapi_generated.go" \
    -v 5 --go-header-file hack/boilerplate.go.txt \
    -r $CURRENT_VIOLATION_EXCEPTIONS \
    "knative.dev/pkg/apis" \
    "knative.dev/pkg/apis/duck/v1" \
    "./pkg/apis/ome/v1beta1" 2>&1

# Hack, the name is required in openAPI specification even if set "+optional" for v1.Container in PredictorExtensionSpec.
sed -i'.bak' -e 's/Required: \[\]string{\"name\"},//g' $OPENAPI_SPEC_FILE && rm -rf $OPENAPI_SPEC_FILE.bak
sed -i'.bak' -e 's/Required: \[\]string{\"modelFormat\", \"name\"},/Required: \[\]string{\"modelFormat\"},/g' $OPENAPI_SPEC_FILE && rm -rf $OPENAPI_SPEC_FILE.bak

test -f $CURRENT_VIOLATION_EXCEPTIONS || touch $CURRENT_VIOLATION_EXCEPTIONS

# The API rule fails if generated API rule violation report differs from the
# checked-in violation file, prints error message to request developer to
# fix either the API source code, or the known API rule violation file.
if ! diff $CURRENT_VIOLATION_EXCEPTIONS $KNOWN_VIOLATION_EXCEPTIONS > /dev/null; then
    echo >&2 "ERROR: API rule check failed. Reported violations in file $CURRENT_VIOLATION_EXCEPTIONS differ from known violations in file $KNOWN_VIOLATION_EXCEPTIONS."
    exit 1
fi

# Generating swagger file
go run cmd/spec-gen/main.go 0.1 > pkg/openapi/swagger.json 2>&1

popd > /dev/null

# Restore stdout
exec 1>&3
