# Track A — the GKE Buildkite agent queue for `integration-test-kind-gke`

This is the setup to stand up a dedicated **`gke-kind-x86`** Buildkite queue on
the MSL GCP cluster for the grouped OMENative KIND suite. The suite already runs
in CI: `generate_pipeline.py` emits `integration-test-kind-gke` **by default**,
targeting the live `gke-tpu-v7x` queue (via the per-step `kubernetes` plugin,
which lands the job on that pool's CPU nodes). The package step and every named
test job gate the build. Disable the group only as an emergency measure with
`GKE_KIND_ENABLED=0`; retarget it with `GKE_KIND_QUEUE=<queue>` once this
dedicated CPU queue exists. Keep any queue you point it at live—a step targeting
a queue with no agents waits indefinitely.

## Why GKE (recap)

The KIND suite needs real `kind` node containers, which need bridge networking
and writable net sysctls (`net.ipv4.ip_forward=1`, …). The TW dind forbids that
(read-only `/proc/sys/net`), so the suite runs on GKE. **We proved** on
`msl-gcp-usc1-rc-1` that a **privileged** pod there *can* boot kind offline and
its pod networking works — so GKE is the path.

## The hard constraint that shapes everything: no pod egress

The RC cluster has **no public egress from pods** (Docker Hub, `dl.k8s.io`,
`proxy.golang.org`, `crates.io`, `gcr.io` all time out — measured). This splits
the run into two egress domains:

| Needs egress? | How it's satisfied |
|---|---|
| kind node image, Go modules, cargo crates, fake-serving bases, tools | **None** — all baked into the `kind-ci` image (built on TW, pulled from prod ECR, `docker load`ed into dind). |
| Buildkite agent ↔ Buildkite API (`agent.buildkite.com`) | **Corp proxy** (the `with-proxy` / `HTTPS_PROXY` mechanism). The controller + agent must be proxy-configured. |
| Git checkout of `mslsrc/ome` | **Corp proxy** (or an in-cluster git mirror). |

So: the **test workload is fully offline**; only the **agent plumbing**
(job polling + checkout) needs egress, via the corp proxy. This is the single
most important thing to confirm with Cloud Foundation — see the checklist.

## Architecture

```
agent-stack-k8s controller (Deployment on the GKE cluster)
  • polls Buildkite API via the corp proxy for `queue=gke-kind-x86` jobs
  • for each job, creates a Job pod from the queue's pod-spec-patch
        │
        ▼
  Job pod (one per named semantic group)
   ├─ checkout (agent-stack-k8s init): git clone mslsrc/ome via proxy
   ├─ container "container-0": image = kind-ci (prod ECR); runs
   │     sh .buildkite/run_kind_tests_gke.sh  (offline; DOCKER_HOST→dind)
   └─ sidecar "dind": docker:28-dind, privileged, shares pod netns
```

## Prerequisites to confirm with Cloud Foundation (the open questions)

1. **Privileged pods** allowed in the agent namespace — PodSecurity `privileged`
   (not `baseline`/`restricted`) and no Gatekeeper/Policy-Controller block on
   `privileged` / `hostPath`. (We ran a privileged Job in `def-sandbox` for the
   validation, so this is likely already OK there.)
2. **A node pool** sized for the fan-out. Each job pod runs one three-node KIND
   cluster and requests 5 vCPU, 14 GiB RAM, and 24 GiB ephemeral storage across
   its workload and dind containers. `KIND_SPEC_GROUPS` controls the number of
   concurrent jobs; leave room for jobs to queue when the full fan-out exceeds
   available capacity.
3. **Corp proxy egress** for the controller + agent to reach
   `agent.buildkite.com` (job polling) and the git remote for `mslsrc/ome`
   (checkout). Confirm the proxy endpoint/env (`HTTPS_PROXY`, `NO_PROXY`) or the
   in-cluster git mirror to use. **Without this the agents cannot run.**
4. **ECR pull for job pods**: the node already pulls prod ECR (proven — the
   validation pod pulled `168653207203…`). So the `kind-ci` image pulls with the
   node's existing creds; no imagePullSecret expected. Confirm this holds in the
   agent namespace.
5. **Buildkite agent token** available as a cluster Secret (org/cluster the
   `ome` pipeline lives in).

## agent-stack-k8s install (sketch — adapt to your chart version)

Deploy a **dedicated controller** for this queue so its pod-spec-patch only
affects kind jobs. Helm values:

```yaml
# values-gke-kind.yaml  (helm install agent-stack-k8s buildkite/agent-stack-k8s -f …)
config:
  org: <buildkite-org>
  tags:
    - queue=gke-kind-x86
  # Controller needs the Buildkite API via the corp proxy.
  # (set on the controller Deployment env too, depending on chart version)
  agent-token-secret: buildkite-agent-token      # k8s Secret with the token

  # Applied to every job pod on this queue: workload image + privileged dind.
  pod-spec-patch:
    containers:
      # agent-stack-k8s injects the checkout + your step command into the
      # first ("container-0") container; point it at the offline kind-ci image.
      - name: container-0
        image: 168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/kind-ci:latest
        env:
          - { name: DOCKER_HOST, value: "tcp://localhost:2375" }
          # proxy so the in-container git ops / any agent calls can egress
          - { name: HTTPS_PROXY, value: "<corp-proxy>" }
          - { name: HTTP_PROXY,  value: "<corp-proxy>" }
          - { name: NO_PROXY,    value: "localhost,127.0.0.1,.svc,.cluster.local,169.254.169.254" }
        resources:
          requests: { cpu: "3", memory: 8Gi, ephemeral-storage: 8Gi }
          limits:   { cpu: "6", memory: 12Gi, ephemeral-storage: 16Gi }
      - name: dind
        image: 168653207203.dkr.ecr.us-east-2.amazonaws.com/pt_dockerio/library/docker:28-dind
        securityContext: { privileged: true }
        env:
          - { name: DOCKER_TLS_CERTDIR, value: "" }   # listen on tcp://…:2375, no TLS
        volumeMounts:
          - { name: docker-graph, mountPath: /var/lib/docker }
        resources:
          requests: { cpu: "2", memory: 6Gi, ephemeral-storage: 16Gi }
          limits:   { cpu: "4", memory: 8Gi, ephemeral-storage: 24Gi }
    volumes:
      - { name: docker-graph, emptyDir: {} }

  # The checkout/agent image itself must reach Buildkite + git via the proxy —
  # set HTTPS_PROXY/HTTP_PROXY/NO_PROXY on the controller + the agent sidecar
  # (chart-version-specific; see agent-stack-k8s docs for `agent` env).
```

Notes:
- `container-0` image = `kind-ci` means the step's command (`sh
  .buildkite/run_kind_tests_gke.sh`) runs with the full offline toolchain and
  `DOCKER_HOST` pointed at the sidecar — exactly what the by-hand shard Job proves.
- The dind + kind-ci images are referenced on **prod ECR** so the node pulls
  them with its existing creds (no public pull).
- `docker:28-dind` via the `pt_dockerio` pull-through (adjust region/path if your
  cluster's pull-through differs — ours resolved `pt_dockerio/library/...`).

## Secrets to create

```bash
kubectl -n <agent-ns> create secret generic buildkite-agent-token \
  --from-literal=token=<BUILDKITE_AGENT_TOKEN>
```
(ECR pull is via node creds; no image-pull secret expected — confirm per #4.)

## Going live

1. Confirm the 5 prerequisites (esp. #3 proxy egress) with Cloud Foundation.
2. `helm install` the controller with the values above; verify agents register
   under `queue=gke-kind-x86` in Buildkite.
3. Point the step at the new queue with **`GKE_KIND_QUEUE=gke-kind-x86`** in the
   `ome` pipeline env (the step is already emitted by default on `gke-tpu-v7x`).
4. Open a PR touching `.buildkite/`; confirm every named semantic job owns a
   disjoint file set and all jobs pass. The package and test jobs are hard
   gates, so any failure blocks the merge.

## How this was de-risked before the queue existed

- `gke-kind-validation-job.yaml` / the v2 Job: proved privileged kind boots +
  networks offline on the cluster (`PASS`).
- `gke-kind-shard-job.yaml` (by-hand): proves the real suite compiles + runs a
  spec offline in the `kind-ci` image. Run it while this queue is being stood up.

## Open follow-ups

- **Repo checkout source**: confirm whether the agent clones `mslsrc/ome` from
  github via proxy or an in-cluster mirror (#3). If neither is available, bake
  the repo into a CI image variant (heavier; only if unavoidable).
- **Image freshness**: `kind-ci` bakes the Go module + cargo caches at build
  time. Its build step rebuilds when one of its inputs changes (`go.mod`,
  `go.sum`, `pkg/xet`, its Dockerfile, `.dockerignore`; see `IMAGE_INPUTS` in
  `generate_pipeline.py`) and otherwise retags the image already published for
  those inputs, so the caches track the module files exactly. A PR that changes
  deps but doesn't rebuild `kind-ci` first could miss modules (`GOPROXY=off`
  fails fast, so it's visible, not silent).

## Image reuse

Every image the pipeline builds is tagged with a hash of what its build reads
(`inputs-<hex>`) as well as with the commit. The step that owns `build-<label>`
runs `build_image.sh`: it probes the dev registry for the hash tag and, when it
exists, retags that image for the commit (`<sha8>`, plus `latest` on main) and
uploads no build; on a miss it uploads the plugin build step behind itself,
and because the mirror, package and chart steps wait on `build-<label>`, they
wait for the uploaded build too. The log of each probe names the hash, the
paths hashed, the registry's answer and the tags published, and the build
carries an `image-reuse` annotation with one line per image.

`package-kind-eval-image` reuses the same way: its hash (`eval-inputs-<hex>`)
is made of its source images' input hashes and the packaging script, so a
merge that touches neither the Go trees nor `kind-ci` retags the previous eval
image. `run_omenative_eval_gke.sh` therefore reads the runtime image refs from
`/images/runtime-image-refs.txt` rather than assuming the commit tag.

`OME_IMAGE_REBUILD=1` on a build is the kill switch: every image step builds,
the eval image is packaged again, and each republishes its hash tag, so the
rebuilt images are the ones later builds reuse. That is also how base-image,
package and tool refreshes reach an image whose inputs have not changed.

## What the eval runs

The OMENative eval group (`omenative-e2e-kind-gke`) and the
`package-kind-eval-image` step it waits on are in the build whenever the eval
mode is not `off`, on any branch. `OME_EVAL_MODE` names the mode outright;
unset, the trailer decides: `OME_EVAL_SELECT` when set, else the newest
`Eval: <value>` line of the commit message (`BUILDKITE_MESSAGE`,
case-insensitive); with none of them, nothing runs, on `main` as on every
other branch. `GKE_EVAL_ENABLED=0` forces `off`.

The eval shares its GKE cluster with production work, and a round of it can
crowd that work out, so three rules bound what one build may run:

1. A merge to `main` runs no eval unless its build names a selection: the
   default mode is `off` everywhere.
2. The heavy modes, `smoke`, `targeted`, `full` and `families=`, fail
   generation unless the build carries `OME_EVAL_ALLOW_HEAVY=1`. Everything
   behind them (the recorded round, the targeted selection, the trailer forms)
   stays in place for a build that sets the override, such as a scheduled
   full round.
3. A build may run a small named selection on its own, `cases=` or `lanes=`
   from the trailer or `OME_EVAL_SELECT` on any branch, as long as its case
   runs (cases times the schedulers they are named under) stay within
   `OME_EVAL_MAX_CASES` (default 24). Above the cap generation fails, naming
   the count, the cap and the override; a lane with more cases than the cap
   can still be named for a subset with `cases=`. `OME_EVAL_ALLOW_HEAVY=1`
   lifts the cap too.

The refusals read, for a heavy mode, `OME_EVAL_MODE, OME_EVAL_SELECT or the
Eval: trailer names 'full', which runs more than a small named selection: ...
unless it carries OME_EVAL_ALLOW_HEAVY=1`, and above the cap `the Eval: trailer
or OME_EVAL_SELECT names 154 case runs (cases times schedulers), above the 24
of OME_EVAL_MAX_CASES: ...`. `OME_EVAL_MAX_CASES` must be a positive integer.

| value | runs |
|---|---|
| `off` | nothing: neither the image step nor the group; the default on every branch |
| `smoke` | the first shard of every feature/topology lane, default scheduler only; needs `OME_EVAL_ALLOW_HEAVY=1` |
| `targeted` | needs `OME_EVAL_ALLOW_HEAVY=1`; from the newest recorded full round: every (case, scheduler) that failed (any verdict but `PASS`, `SKIP` or an `XFAIL` form: a skipped lane recurs every build for the same reason, a parked case is debt the ledger already carries, and neither is rerun nor sampled), under the scheduler it ran in; every corpus case the round has no row for; every case the trailer's `families=` or `cases=` name, default scheduler, and every case of its `lanes=`, under their schedulers; and a sample of the round's passing lanes. The group label reads `(targeted from round <build>: <n> cases)`. With no round to read it runs smoke plus the trailer, and the label says why: `(targeted: smoke, no results token)`, `no recorded round` or `round lookup failed` |
| `full` | every case on every scheduler; needs `OME_EVAL_ALLOW_HEAVY=1` |
| `families=<a,b>` | every case of those folders under `hack/omenative-testing/perm/isvcs/`, every scheduler; needs `OME_EVAL_ALLOW_HEAVY=1` |
| `cases=<p1,p2>` | exactly those cases in their lanes, default scheduler; a case needing the force-delete policy runs in its lane's policy-on twin; at most `OME_EVAL_MAX_CASES` of them without the override |
| `lanes=<feature>/<topology>[@<scheduler>],...` | every case of those lanes (`gen-heal/engine`), each under the scheduler its suffix names: no suffix or `@default` is the cluster default scheduler, `@ome-scheduler` the gang-scheduler twin; the same lane may be named once per scheduler; the cases of every named lane and scheduler together stay within `OME_EVAL_MAX_CASES` without the override |

`families=`, `cases=` and `lanes=` are trailer forms; `OME_EVAL_MODE` takes
the four words, and a trailer selection stays beside a mode the variable set,
which is how `targeted` gets its extra cases. A value naming nothing known, an
unknown folder, case id or lane, a suffix naming no scheduler, the twin while
the build does not run it (`GKE_EVAL_SCHEDULER_ENABLED=0`), or an empty list
fails generation with a message naming it.

`OME_EVAL_SELECT` is the trailer's twin for a build created without a message
(triggered through the API or on a schedule): it carries the text an `Eval:`
line would, a mode word or one of the three selection forms. Set, it replaces
the message's trailer, whatever each says. Beside `OME_EVAL_MODE` it behaves
like that mode plus a selection trailer, so `OME_EVAL_MODE=targeted` with
`OME_EVAL_SELECT=lanes=gen-heal/engine@ome-scheduler` adds that lane to the
targeted selection and `OME_EVAL_SELECT=smoke` beside a mode changes nothing;
alone it behaves exactly like the trailer. Empty or blank, it is unset. A
value it carries is refused as a trailer's would be.
The group label names the mode unless it is `full`, and the upload plan in the
entrypoint's log ends with it. `_eval_selection()` in `generate_pipeline.py`
is the one place a mode chooses cases; the emission packs and shards whatever
it returns, so `targeted` can read a recorded round without touching the lanes.

A `full` round records itself: the soft-failing step `eval-verdicts`, after
every eval group, downloads the lanes' verdict files
(`hack/omenative-testing/perm/results/<build>-<job>.verdicts.json`, the
harness's own record of the job: one object per case with its verdict, reason,
seconds, fault and log path, and the lane the job ran) and, for a lane that
uploaded none because it stopped before its summary, its result archive
(`<build>-<job>.tgz`, carrying the job's `lane.tsv`, the harness's per-case
result files and the diagnostics), writes `eval-verdicts.tsv` with the
columns build, commit, feature, topology, scheduler, case, verdict (one row
per case and scheduler the round ran; a lane with neither artifact yields no
row), attaches it to the build and sets the build meta-data
`ome:eval-round=full`. `.buildkite/eval_verdicts.py` defines both formats;
`record_eval_verdicts.sh` is the step's command.

A `targeted` build reads that record through the Buildkite REST API when
`OME_EVAL_RESULTS_TOKEN` holds a read-only API token (`read_builds`,
`read_artifacts`), stored as a pipeline secret: the newest `main` build of the
pipeline in state passed or failed (never canceled or running) whose meta-data
carries the mark and whose `eval-verdicts` job attached the file.
`OME_EVAL_RESULTS_PIPELINE` names the pipeline the rounds run in when it is
not this one. The sample is `OME_EVAL_SAMPLE_PERCENT` of the round's passing
lanes, rounded up (default 5; 0 runs no sample; a value outside 0 to 100 or
not a number fails generation), drawn with the commit as seed so a build is
reproducible. Without the token, a recorded round or a working lookup the
build falls back as the table says, logs the reason to the generator's stderr
and never fails for it.

Every lane carries `concurrency_group: "ome/gke-evals"` with
`concurrency: <GKE_EVAL_MAX_CONCURRENCY>` (default 40), so across every build
of the organization at most that many lanes hold a GKE agent pod at once and
the rest wait in order; the default leaves the cluster the eval shares with
production work room for it. The variable must be a positive integer; unset
or empty means the default, and generation refuses anything else.

### Running the eval from a branch

A branch build whose eval mode is not `off` (an `Eval:` trailer or
`OME_EVAL_SELECT` naming `cases=` or `lanes=` within the cap, or a heavy mode
with `OME_EVAL_ALLOW_HEAVY=1`) publishes what the eval needs for its commit,
and nothing more. `_images_published()` in `generate_pipeline.py` is the one gate: true on
`main` and on a branch whose eval runs. Every image step and the eval image
step carry its decision as `OME_PUBLISH_IMAGES` (`1` publishes, `0` builds for
validation only); `build_image.sh` and `package_kind_eval_image.sh` honour the
variable and, when it is unset, take `main` as publishing and every other
branch as not.

On such a branch the image steps push their commit tag (`<sha8>`, plus the
`inputs-<hex>` reuse tag) to the dev registry: never `latest`, never a branch
name, and an image whose inputs are already published is retagged rather than
rebuilt. `package-kind-eval-image` waits on those builds instead of the mirror
steps, pulls the commit tags from the dev registry (`OME_EVAL_SOURCE_REGISTRY`)
and publishes `kind-ci:eval-<sha8>` to the prod registry, the one the GKE queue
pulls the runner image from; its label says the branch publishes its images
for the eval. The mirror steps keep `--only-main`, `push_chart.sh` keeps its
`main` gate and the release marker its branch condition, so the prod registry
receives nothing from the branch but that runner image (as it already receives
`kind-ci:test-<sha8>-<build>` from every branch), no chart publishes and no
marker is written. With the eval off, a branch build is as it always was: images are
built for validation and nothing is pushed. `GKE_EVAL_ENABLED=0` turns the
eval, and with it the branch publish, off.

## Version pins

`.kind-node-image` at the repo root is the single source of truth for the node
image, the way `.envtest-k8s-version` is for the envtest control plane. The kind
binary comes from the `sigs.k8s.io/kind` require in `hack/internal/tools/go.mod`
— the same pin `make kind` installs. Two constraints when bumping: the node
image's minor must match `.envtest-k8s-version`, so the KIND and envtest lanes
exercise the same Kubernetes API; and the pinned `kind` release must publish a
node image for that tag (its release notes list them).

`package_kind_test_image.sh` reads both files and appends the binary and the
node tarball to the runner image, shadowing the copies `kind-ci` bakes. That
indirection is load-bearing, not tidiness: `kind-ci` only mirrors to the prod
registry from `main`, and a PR branch's runner is assembled on top of
`kind-ci:latest`.
A pin baked only into the Dockerfile therefore cannot reach the branch that
raises it — the runner asks for a node image it has no egress to fetch and dies
in `kind create cluster`. The `ARG`s in `dockerfiles/kind-ci.Dockerfile` are
fallbacks for standalone use of the image; keep them tracking the two files.

## Exit codes, retries, and the scheduler twins

Every KIND-backed job carries the infra-retry preset (`exit_status: 125` and
`agent_stop`, never exit 1), so what a runner reports as 125 is the whole
retry policy. `run_kind_tests_gke.sh` reports 125 when the dind sidecar never
answers, or when the suite dies in BeforeSuite creating its cluster or
installing the gang scheduler with no spec run; a suite that reached even one
spec keeps its own status. `run_omenative_eval_gke.sh` reports 125 for any
status-1 failure before its first case runs (dind, image load, `kind create`,
the settle gate, the OME deploy) and keeps the shard's own status from the
first case on. A red spec or case is a defect to fix, quarantine, or park, and
is never retried.

The `ome-scheduler` twins of the KIND groups and of the eval shards gate the
build like their default-scheduler jobs. A twin that is red on main while its
default job is green is a gang-scheduler defect with an owner; it goes into
`KIND_SCHEDULER_SOFT_GROUPS` in `generate_pipeline.py` with an owner, a
one-line reason and an expiry, and its job label says `advisory until <date>`.
Generation fails on an unknown group, a missing owner or reason, and the day
after the expiry, on every branch. The eval shards have the same list keyed by
topology, `EVAL_SCHEDULER_SOFT_TOPOLOGIES`, because a gang-scheduler defect
shows on every feature of the shape it breaks. `GKE_EVAL_SCHEDULER_HARD_FAIL=0` and
`GKE_KIND_SCHEDULER_HARD_FAIL=0` make every twin advisory again; they are
emergency switches, to be reported the moment they are flipped.

## Alfred lane

`integration-test-kind-alfred-gke` runs `tests/integration/singlecluster/controller/alfred_kind`
from `/kind-test/alfred-kind-test.tgz`, one four-node cluster (three workers)
per group in `ALFRED_KIND_SPEC_GROUPS`. The suite deploys the alfred image this
pipeline built from `.alfred-oss-ref`: `build-alfred` pushes its commit tag on
every branch (`latest` stays main-only), `package_kind_test_image.sh` pulls that
tag into `/images/alfred.tar`, and `run_kind_tests_gke.sh` exports
`OME_ALFRED_IMAGE` after loading it. Kill switch: `GKE_ALFRED_ENABLED=0` drops
the lane, the package step's wait on and requirement for the alfred image, and
the every-branch push, so the image publishes on main only like every other.
Soft-fail until it has soaked; promote by removing `soft_fail` in
`_alfred_kind_group`.
