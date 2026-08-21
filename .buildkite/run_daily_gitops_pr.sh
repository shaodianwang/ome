#!/bin/sh
# Open or refresh the GitOps PR for a release that passed the RC smoke test.
set -eu

OME_CHECKOUT="$(pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

CANDIDATE_FILE="$WORK_DIR/candidate.json"
buildkite-agent meta-data get daily-candidate-b64 | base64 -d >"$CANDIDATE_FILE"

if [ -z "${GH_TOKEN:-}" ]; then
  GH_TOKEN="$(buildkite-agent secret get GH_TOKEN 2>/dev/null || true)"
fi
if [ -z "${GH_TOKEN:-}" ]; then
  echo "GH_TOKEN is required to update mslsrc/argo-gitops and open a PR" >&2
  exit 1
fi
export GH_TOKEN

ARGO_GITOPS_REPO="${ARGO_GITOPS_REPO:-mslsrc/argo-gitops}"
ARGO_GITOPS_REPO_URL="${ARGO_GITOPS_REPO_URL:-https://github.com/${ARGO_GITOPS_REPO}.git}"
ARGO_GITOPS_BASE_BRANCH="${ARGO_GITOPS_BASE_BRANCH:-main}"

SHORT_SHA="$(python3 - "$CANDIDATE_FILE" <<'PY'
import json
import sys

print(json.load(open(sys.argv[1]))["short_sha"])
PY
)"
case "$SHORT_SHA" in
  ????????) ;;
  *) echo "invalid candidate SHA: $SHORT_SHA" >&2; exit 1 ;;
esac
PROMOTION_BRANCH="${ARGO_GITOPS_PROMOTION_BRANCH:-automation/ome-rc-${SHORT_SHA}}"
git check-ref-format --branch "$PROMOTION_BRANCH" >/dev/null

ASKPASS="$WORK_DIR/git-askpass.sh"
cat >"$ASKPASS" <<'EOF'
#!/bin/sh
case "$1" in
  *Username*) printf '%s\n' x-access-token ;;
  *) printf '%s\n' "$GH_TOKEN" ;;
esac
EOF
chmod 700 "$ASKPASS"
export GIT_ASKPASS="$ASKPASS"
export GIT_TERMINAL_PROMPT=0
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=credential.helper
export GIT_CONFIG_VALUE_0=''

echo "--- clone argo-gitops/${ARGO_GITOPS_BASE_BRANCH}"
git clone --depth 1 --branch "$ARGO_GITOPS_BASE_BRANCH" \
  "$ARGO_GITOPS_REPO_URL" "$WORK_DIR/argo-gitops"
git -C "$WORK_DIR/argo-gitops" switch -c "$PROMOTION_BRANCH"

OLD_SHA="$(python3 "$OME_CHECKOUT/.buildkite/daily_gitops.py" \
  --repo-root "$WORK_DIR/argo-gitops" \
  --print-current-sha)"

echo "--- pin the RC-tested OME release"
python3 "$OME_CHECKOUT/.buildkite/daily_gitops.py" \
  --candidate "$CANDIDATE_FILE" \
  --repo-root "$WORK_DIR/argo-gitops"

if git -C "$WORK_DIR/argo-gitops" diff --quiet; then
  echo "argo-gitops main already pins OME release $SHORT_SHA; no PR needed"
  exit 0
fi

TARGET_ONE='cluster/workload/msl-gcp-usc1-1-tpu/boostrap-override/values.yaml'
TARGET_TWO='cluster/workload/msl-gcp-usc1-2-tpu/boostrap-override/values.yaml'
TARGET_THREE='cluster/workload/msl-gcp-usc1-3-tpu/boostrap-override/values.yaml'
EXPECTED_PATHS="$(printf '%s\n%s\n%s' "$TARGET_ONE" "$TARGET_TWO" "$TARGET_THREE")"
ACTUAL_PATHS="$(git -C "$WORK_DIR/argo-gitops" diff --name-only | sort)"
if [ "$ACTUAL_PATHS" != "$EXPECTED_PATHS" ]; then
  echo "unexpected GitOps paths changed:" >&2
  printf '%s\n' "$ACTUAL_PATHS" >&2
  exit 1
fi
git -C "$WORK_DIR/argo-gitops" diff --check
git -C "$WORK_DIR/argo-gitops" diff --stat

git -C "$WORK_DIR/argo-gitops" config user.name "OME RC release bot"
git -C "$WORK_DIR/argo-gitops" config user.email "ome-rc-release-bot@users.noreply.github.com"
git -C "$WORK_DIR/argo-gitops" add -- "$TARGET_ONE" "$TARGET_TWO" "$TARGET_THREE"
git -C "$WORK_DIR/argo-gitops" commit \
  -m "chore(ome): promote RC-tested release $SHORT_SHA"

echo "--- push $PROMOTION_BRANCH"
REMOTE_SHA="$(git -C "$WORK_DIR/argo-gitops" ls-remote --heads origin \
  "refs/heads/$PROMOTION_BRANCH" | awk '{print $1}')"
if [ -n "$REMOTE_SHA" ]; then
  git -C "$WORK_DIR/argo-gitops" push \
    "--force-with-lease=refs/heads/$PROMOTION_BRANCH:$REMOTE_SHA" \
    origin "HEAD:refs/heads/$PROMOTION_BRANCH"
else
  git -C "$WORK_DIR/argo-gitops" push origin "HEAD:refs/heads/$PROMOTION_BRANCH"
fi

read_candidate_field() {
  python3 - "$CANDIDATE_FILE" "$1" "$2" <<'PY'
import json
import sys

print(json.load(open(sys.argv[1]))[sys.argv[2]][sys.argv[3]])
PY
}
CRD_VERSION="$(read_candidate_field charts ome-crd)"
RESOURCES_VERSION="$(read_candidate_field charts ome-resources)"
SCHEDULER_VERSION="$(read_candidate_field charts ome-scheduler)"
BUILD_URL="${BUILDKITE_BUILD_URL:-Buildkite build ${BUILDKITE_BUILD_NUMBER:-unknown}}"
PR_TITLE="chore(ome): promote RC-tested release $SHORT_SHA"
PR_BODY_FILE="$WORK_DIR/pr-body.md"
OME_REPO="${OME_REPO:-mslsrc/ome}"
OME_COMPARE_RESPONSE="$WORK_DIR/ome-compare.json"
OME_COMPARE_CODE="$(curl -sS -o "$OME_COMPARE_RESPONSE" -w '%{http_code}' \
  -H "Authorization: Bearer $GH_TOKEN" \
  -H 'Accept: application/vnd.github+json' \
  "https://api.github.com/repos/$OME_REPO/compare/$OLD_SHA...$SHORT_SHA")"
if [ "$OME_COMPARE_CODE" != "200" ]; then
  cat "$OME_COMPARE_RESPONSE" >&2
  echo "OME comparison lookup failed with HTTP $OME_COMPARE_CODE" >&2
  exit 1
fi
CHANGESET_FILE="$WORK_DIR/changeset.md"
python3 "$OME_CHECKOUT/.buildkite/daily_changeset.py" \
  --comparison "$OME_COMPARE_RESPONSE" \
  --output "$CHANGESET_FILE" \
  --repository "$OME_REPO" \
  --base-sha "$OLD_SHA" \
  --head-sha "$SHORT_SHA"

cat >"$PR_BODY_FILE" <<EOF
## Summary

Promote OME release \`$SHORT_SHA\` after it passed the daily RC smoke test.

| Chart | Version |
|---|---|
| ome-crd | \`$CRD_VERSION\` |
| ome-resources | \`$RESOURCES_VERSION\` |
| ome-scheduler | \`$SCHEDULER_VERSION\` |

The controller and omeAgent image tags are also pinned to \`$SHORT_SHA\` in all three cluster overrides.
EOF
cat "$CHANGESET_FILE" >>"$PR_BODY_FILE"
cat >>"$PR_BODY_FILE" <<EOF

## Clusters

- \`msl-gcp-usc1-1-tpu\`
- \`msl-gcp-usc1-2-tpu\`
- \`msl-gcp-usc1-3-tpu\`

## Validation

- RC connectivity passed
- BaseModel metadata extraction through ome-agent passed
- One CPU OMENative InferenceService became Ready through ome-scheduler
- One production-shaped, two-node TPU gang became Ready with its engine pods using default-scheduler
- One production-shaped, two-node TPU gang became Ready with its engine pods using ome-scheduler
- Both TPU routers used default-scheduler
- Build: $BUILD_URL

This PR does not merge automatically. Merging it allows Argo CD to reconcile the tested release.
EOF

API_ROOT="https://api.github.com/repos/$ARGO_GITOPS_REPO"
OWNER="${ARGO_GITOPS_REPO%%/*}"
ENCODED_HEAD="$(python3 - "$OWNER:$PROMOTION_BRANCH" <<'PY'
import sys
import urllib.parse

print(urllib.parse.quote(sys.argv[1], safe=""))
PY
)"
API_RESPONSE="$WORK_DIR/github-response.json"
HTTP_CODE="$(curl -sS -o "$API_RESPONSE" -w '%{http_code}' \
  -H "Authorization: Bearer $GH_TOKEN" \
  -H 'Accept: application/vnd.github+json' \
  "$API_ROOT/pulls?state=open&head=$ENCODED_HEAD")"
if [ "$HTTP_CODE" != "200" ]; then
  cat "$API_RESPONSE" >&2
  echo "GitHub pull lookup failed with HTTP $HTTP_CODE" >&2
  exit 1
fi

EXISTING_PR_NUMBER="$(python3 - "$API_RESPONSE" <<'PY'
import json
import sys

pulls = json.load(open(sys.argv[1]))
if pulls:
    print(pulls[0]["number"])
PY
)"
if [ -n "$EXISTING_PR_NUMBER" ]; then
  PAYLOAD="$(python3 - "$PR_TITLE" "$ARGO_GITOPS_BASE_BRANCH" "$PR_BODY_FILE" <<'PY'
import json
import sys

title, base, body_path = sys.argv[1:]
print(json.dumps({"title": title, "base": base, "body": open(body_path).read()}))
PY
)"
  HTTP_CODE="$(curl -sS -o "$API_RESPONSE" -w '%{http_code}' \
    -X PATCH \
    -H "Authorization: Bearer $GH_TOKEN" \
    -H 'Accept: application/vnd.github+json' \
    -H 'Content-Type: application/json' \
    "$API_ROOT/pulls/$EXISTING_PR_NUMBER" \
    -d "$PAYLOAD")"
  EXPECTED_CODE=200
else
  PAYLOAD="$(python3 - "$PR_TITLE" "$PROMOTION_BRANCH" "$ARGO_GITOPS_BASE_BRANCH" "$PR_BODY_FILE" <<'PY'
import json
import sys

title, head, base, body_path = sys.argv[1:]
print(json.dumps({
    "title": title,
    "head": head,
    "base": base,
    "body": open(body_path).read(),
}))
PY
)"
  HTTP_CODE="$(curl -sS -o "$API_RESPONSE" -w '%{http_code}' \
    -X POST \
    -H "Authorization: Bearer $GH_TOKEN" \
    -H 'Accept: application/vnd.github+json' \
    -H 'Content-Type: application/json' \
    "$API_ROOT/pulls" \
    -d "$PAYLOAD")"
  EXPECTED_CODE=201
fi
if [ "$HTTP_CODE" != "$EXPECTED_CODE" ]; then
  cat "$API_RESPONSE" >&2
  echo "GitHub pull request operation failed with HTTP $HTTP_CODE" >&2
  exit 1
fi

PR_URL="$(python3 - "$API_RESPONSE" <<'PY'
import json
import sys

print(json.load(open(sys.argv[1]))["html_url"])
PY
)"
buildkite-agent meta-data set daily-gitops-pr-url "$PR_URL"
buildkite-agent annotate "GitOps promotion PR: [$PR_URL]($PR_URL)" \
  --style success --context daily-gitops-pr
echo "GitOps promotion PR: $PR_URL"
