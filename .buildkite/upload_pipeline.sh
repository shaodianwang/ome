#!/usr/bin/env bash
# Generate and upload the Buildkite pipeline.
# Captures the generator output into a variable first so that if
# generate_pipeline.py fails, we abort instead of uploading an empty pipeline.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# First arg picks which pipeline to render (ci, images, ...).
# Defaults to 'ci' to keep the existing 'ome' BuildKite pipeline working
# without configuration changes.
PIPELINE="${1:-ci}"

pipeline_yaml=$(python3 "$SCRIPT_DIR/generate_pipeline.py" "$PIPELINE")
echo "$pipeline_yaml" | buildkite-agent pipeline upload
