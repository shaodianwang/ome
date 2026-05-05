#!/usr/bin/env bash
# Generate and upload the Buildkite pipeline.
# Captures the generator output into a variable first so that if
# generate_pipeline.py fails, we abort instead of uploading an empty pipeline.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

pipeline_yaml=$(python3 "$SCRIPT_DIR/generate_pipeline.py")
echo "$pipeline_yaml" | buildkite-agent pipeline upload
