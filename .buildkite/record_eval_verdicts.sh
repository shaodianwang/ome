#!/bin/sh
# Record a full eval round: gather the lanes' result archives of this build,
# write the round's verdict file with eval_verdicts.py, attach it to the
# build and mark the build as a recorded round in its meta-data, in that
# order, so a build that carries the mark always carries the file. The
# targeted eval mode of generate_pipeline.py reads both.
#
# Usage: sh .buildkite/record_eval_verdicts.sh
#
# Environment (set by the step the generator emits):
#   OME_EVAL_RESULTS_DIR        where the lanes' archives are downloaded to
#   OME_EVAL_VERDICTS_FILE      the verdict file to write and upload
#   OME_EVAL_ROUND_META_KEY     the meta-data key that marks a recorded round
#   OME_EVAL_ROUND_META_VALUE   its value
set -eu

cd "$(dirname "$0")/.."
results_dir="${OME_EVAL_RESULTS_DIR:?OME_EVAL_RESULTS_DIR is required}"
verdicts="${OME_EVAL_VERDICTS_FILE:?OME_EVAL_VERDICTS_FILE is required}"
meta_key="${OME_EVAL_ROUND_META_KEY:?OME_EVAL_ROUND_META_KEY is required}"
meta_value="${OME_EVAL_ROUND_META_VALUE:?OME_EVAL_ROUND_META_VALUE is required}"

echo "--- download the lanes' result archives"
mkdir -p "$results_dir"
buildkite-agent artifact download "${results_dir}/*.tgz" .

echo "--- record the round's verdicts"
python3 .buildkite/eval_verdicts.py --archives "$results_dir" --out "$verdicts"

echo "--- attach the verdict file and mark the round as recorded"
buildkite-agent artifact upload "$verdicts"
buildkite-agent meta-data set "$meta_key" "$meta_value"
