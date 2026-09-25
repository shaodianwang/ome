#!/bin/sh
# Collect the Buildkite REST sweeps consumed by queue_report.py.
#
# Usage: collect_sweeps.sh [DATA_DIR] [HOURS] [--org-history]
#   DATA_DIR       default $OME_CAPACITY_SCRATCH/data
#                  (OME_CAPACITY_SCRATCH defaults to ~/.cache/ome-agent.noindex/scratch/buildkite-capacity-report)
#   HOURS          build-history window, default 25
#   --org-history  also sweep the whole org's build history (capped; for the shared queue)
#
# Token: $BUILDKITE_API_TOKEN or ~/.buildkite/token (mode 600), passed to curl
# through a config on stdin so it never shows in a process list. Scopes:
# read_agents, read_builds. POSIX sh + curl + python3, nothing else.
set -eu

ORG="msl"
PIPELINE="ome"
API="https://api.buildkite.com/v2/organizations/$ORG"
SCRATCH="${OME_CAPACITY_SCRATCH:-$HOME/.cache/ome-agent.noindex/scratch/buildkite-capacity-report}"
DATA_DIR=""
HOURS=""
ORG_HISTORY=0
ORG_HISTORY_MAX_PAGES="${ORG_HISTORY_MAX_PAGES:-30}"
BATCH=6

for arg in "$@"; do
  case "$arg" in
    --org-history) ORG_HISTORY=1 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) if [ -z "$DATA_DIR" ]; then DATA_DIR="$arg"; elif [ -z "$HOURS" ]; then HOURS="$arg"; else echo "collect_sweeps: unexpected argument $arg" >&2; exit 2; fi ;;
  esac
done
DATA_DIR="${DATA_DIR:-$SCRATCH/data}"
HOURS="${HOURS:-25}"

if [ -n "${BUILDKITE_API_TOKEN:-}" ]; then
  TOKEN="$BUILDKITE_API_TOKEN"
elif [ -r "$HOME/.buildkite/token" ]; then
  TOKEN="$(cat "$HOME/.buildkite/token")"
else
  echo "collect_sweeps: no Buildkite API token: export BUILDKITE_API_TOKEN or create ~/.buildkite/token (chmod 600) with read_agents and read_builds" >&2
  exit 1
fi
[ -n "$TOKEN" ] || { echo "collect_sweeps: token is empty" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "collect_sweeps: curl not found" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "collect_sweeps: python3 not found" >&2; exit 1; }

# One GET; the bearer header travels in the curl config, not in argv.
get() {
  printf 'header = "Authorization: Bearer %s"\nheader = "Accept: application/json"\n' "$TOKEN" |
    curl -sSg -m 90 -K - -o "$2" "$1" || echo "collect_sweeps: GET failed: $1" >&2
}

page_empty() {
  python3 - "$1" <<'PY'
import json, sys
try:
    data = json.load(open(sys.argv[1]), strict=False)
except Exception:
    sys.exit(0)
sys.exit(0 if not isinstance(data, list) or not data else 1)
PY
}

# fetch_pages URL_PREFIX OUT_PREFIX MAX_PAGES: bounded-parallel ?page=N sweeps, stop after an empty page.
fetch_pages() {
  url="$1"; out="$2"; max="$3"; p=1
  while [ "$p" -le "$max" ]; do
    last=$((p + BATCH - 1))
    [ "$last" -gt "$max" ] && last="$max"
    i="$p"
    while [ "$i" -le "$last" ]; do
      get "${url}page=${i}&per_page=100" "${out}p${i}.json" &
      i=$((i + 1))
    done
    wait
    if page_empty "${out}p${last}.json"; then
      return 0
    fi
    p=$((p + BATCH))
  done
  echo "collect_sweeps: page cap ($max) reached for $url" >&2
  return 3
}

mkdir -p "$DATA_DIR/agents" "$DATA_DIR/live" "$DATA_DIR/hist"
rm -f "$DATA_DIR"/agents/*.json "$DATA_DIR"/live/*.json "$DATA_DIR"/hist/*.json
COLLECTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SINCE="$(python3 -c 'import datetime, sys; print((datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=float(sys.argv[1]))).strftime("%Y-%m-%dT%H:%M:%SZ"))' "$HOURS")"

echo "[1/3] org agents..."
fetch_pages "$API/agents?" "$DATA_DIR/agents/" 60 || true

echo "[2/3] live builds (running, failing, scheduled)..."
fetch_pages "$API/builds?state[]=running&state[]=failing&state[]=scheduled&" "$DATA_DIR/live/" 12 || true

echo "[3/3] $PIPELINE build history since $SINCE..."
fetch_pages "$API/pipelines/$PIPELINE/builds?created_from=$SINCE&" "$DATA_DIR/hist/${PIPELINE}_" 12 || true

ORG_HISTORY_CAPPED=0
if [ "$ORG_HISTORY" -eq 1 ]; then
  echo "[+] org-wide build history since $SINCE (cap $ORG_HISTORY_MAX_PAGES pages)..."
  fetch_pages "$API/builds?created_from=$SINCE&" "$DATA_DIR/hist/org_" "$ORG_HISTORY_MAX_PAGES" || ORG_HISTORY_CAPPED=1
fi

printf '{"collected_at": "%s", "hours": %s, "org": "%s", "pipeline": "%s", "org_history": %s, "org_history_capped": %s}\n' \
  "$COLLECTED_AT" "$HOURS" "$ORG" "$PIPELINE" "$ORG_HISTORY" "$ORG_HISTORY_CAPPED" > "$DATA_DIR/meta.json"
echo "done: $DATA_DIR"
