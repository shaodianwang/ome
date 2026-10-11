#!/bin/sh
# Record a full eval round: gather the lanes' verdict files of this build and
# the result archives of the lanes without one, write the round's verdict
# file with eval_verdicts.py, attach it to the build and mark the build as a
# recorded round in its meta-data, in that order, so a build that carries the
# mark always carries the file. The targeted eval mode of generate_pipeline.py
# reads both.
#
# Usage: sh .buildkite/record_eval_verdicts.sh
#
# Environment (set by the step the generator emits):
#   OME_EVAL_RESULTS_DIR          where the lanes' artifacts are downloaded to
#   OME_EVAL_JOB_VERDICTS_SUFFIX  what a lane's verdict file ends with
#   OME_EVAL_VERDICTS_FILE        the verdict file to write and upload
#   OME_EVAL_ROUND_META_KEY       the meta-data key that marks a recorded round
#   OME_EVAL_ROUND_META_VALUE     its value
set -eu

cd "$(dirname "$0")/.."
results_dir="${OME_EVAL_RESULTS_DIR:?OME_EVAL_RESULTS_DIR is required}"
job_suffix="${OME_EVAL_JOB_VERDICTS_SUFFIX:?OME_EVAL_JOB_VERDICTS_SUFFIX is required}"
verdicts="${OME_EVAL_VERDICTS_FILE:?OME_EVAL_VERDICTS_FILE is required}"
meta_key="${OME_EVAL_ROUND_META_KEY:?OME_EVAL_ROUND_META_KEY is required}"
meta_value="${OME_EVAL_ROUND_META_VALUE:?OME_EVAL_ROUND_META_VALUE is required}"

echo "--- download the lanes' verdict files"
mkdir -p "$results_dir"
# A lane that stopped before its summary has no verdict file; when no lane
# has one the download finds nothing and every lane is read from its archive.
buildkite-agent artifact download "${results_dir}/*${job_suffix}" . \
  || echo "no verdict file downloaded; the lanes are read from their archives"

echo "--- download the result archives of the lanes without a verdict file"
# An archive carries the lane's diagnostics beside its results, so only a
# lane whose verdict file is missing here is read from its archive. Without
# the artifact list, every archive is downloaded as a round without verdict
# files needs. The list is one path per word.
if listing=$(buildkite-agent artifact search --format '%p ' --allow-empty-results "${results_dir}/*.tgz"); then
  for archive in $listing; do
    [ -s "${archive%.tgz}${job_suffix}" ] && continue
    buildkite-agent artifact download "$archive" . \
      || echo "WARNING: ${archive} did not download; its lane yields no row" >&2
  done
else
  echo "the artifact list could not be searched; downloading every archive" >&2
  buildkite-agent artifact download "${results_dir}/*.tgz" . \
    || echo "no result archive downloaded"
fi

echo "--- record the round's verdicts"
python3 .buildkite/eval_verdicts.py --archives "$results_dir" --out "$verdicts"

echo "--- attach the verdict file and mark the round as recorded"
buildkite-agent artifact upload "$verdicts"
buildkite-agent meta-data set "$meta_key" "$meta_value"
