#!/bin/sh
# Upload the pipeline documents that follow the first one.
#
# The entrypoint uploads what generate_pipeline.py prints: every step that is
# not an eval lane, ending with the step that runs this script. Buildkite caps
# the jobs of one upload (UPLOAD_JOB_LIMIT in the generator), so the eval
# groups and the release marker follow here as separate uploads, in the
# generator's order. Dependencies by key hold across the uploads of one build:
# the lanes keep their wait on the package step and the marker, uploaded last,
# names every gate. The documents and the manifest are kept under the batch
# directory and attached to the step as artifacts.
#
# A partial upload is not resumed: a rerun that uploads a document the build
# already carries fails on its duplicate keys, so a half-uploaded build never
# reaches the marker.
#
# Usage: sh .buildkite/upload_pipeline_batches.sh
#
# Environment:
#   OME_PIPELINE_BATCH_DIR  where the documents are written
#                           (default .buildkite/artifacts/pipeline-batches)
set -eu

cd "$(dirname "$0")/.."
dir="${OME_PIPELINE_BATCH_DIR:-.buildkite/artifacts/pipeline-batches}"
mkdir -p "$dir"
rm -f "$dir"/batch-*.yml "$dir/manifest.txt"

echo "--- generate every upload document"
python3 .buildkite/generate_pipeline.py --batch-dir "$dir" > "$dir/manifest.txt"
cat "$dir/manifest.txt"

uploads=0
jobs=0
first=1
while read -r file count; do
  if [ "$first" = 1 ]; then
    first=0
    echo "skip ${file} (${count} jobs): the entrypoint uploaded it"
    continue
  fi
  echo "--- upload ${file} (${count} jobs)"
  buildkite-agent pipeline upload "$dir/$file"
  uploads=$((uploads + 1))
  jobs=$((jobs + count))
done < "$dir/manifest.txt"
echo "uploaded ${uploads} documents, ${jobs} jobs"
