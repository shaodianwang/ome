#!/usr/bin/env python3
"""Generate the Buildkite pipeline for OME.

Image builds and chart packaging always run; the per-step scripts only
push to ECR when BUILDKITE_BRANCH=main, so PR builds validate without
publishing. The exception is _push_every_branch_images(): those images
push their commit tag from every branch because a KIND lane pulls them
back out of the dev registry.

Image steps reuse before they build. The generator hashes the tracked
content each image build reads (IMAGE_INPUTS) and every published image
carries that hash as a tag next to its commit tag. The step that owns
build-<label> probes the dev registry for the hash tag: when it exists the
image is retagged for this commit and nothing is built; otherwise the
plugin build step is uploaded behind the probe, and the steps waiting on
the key wait for it too. The eval image package step reuses the same way,
keyed on its source images' hashes. OME_IMAGE_REBUILD=1 is the kill
switch: every image step builds and republishes its hash tag, which is
also how base-image and tool refreshes reach an image whose inputs have
not changed.

Buildkite caps the jobs of one pipeline upload (UPLOAD_JOB_LIMIT), so the
build is uploaded as several documents. The entrypoint uploads what this
script prints: every step that is not an eval lane, ending with the step
that runs upload_pipeline_batches.sh. That step writes every document
(--batch-dir) and uploads the rest in order: one eval group per document,
then the release marker, which names every other step and group of the
build as a dependency.

Usage:
    python3 .buildkite/generate_pipeline.py > generated-pipeline.yml
    python3 .buildkite/generate_pipeline.py --batch-dir <dir>

Environment variables read at runtime:
    BUILDKITE_BRANCH          – current branch name
    BUILDKITE_COMMIT          – full commit SHA
    BUILDKITE_BUILD_NUMBER    – pipeline-unique build number
    BUILDKITE_PULL_REQUEST    – pull request number, or "false"
    BUILDKITE_PULL_REQUEST_BASE_BRANCH – pull request base branch
    AWS_ACCOUNT_ID            – dev AWS account for ECR (default: 588845226011)
    PROD_AWS_ACCOUNT_ID       – prod AWS account for ECR (default: 168653207203)
    AWS_REGION                – AWS region for ECR (default: us-east-2)
    ECR_REGISTRY              – full dev ECR registry override (takes precedence)
    OME_IMAGE_REBUILD         – "1" forces every image step to rebuild instead
                                of reusing a published image with the same inputs
    GKE_EVAL_ENABLED          – "0" drops the OMENative eval from the build
    OME_EVAL_MODE             – what the OMENative eval runs: off, smoke, targeted
                                or full. Unset, the newest "Eval: <value>" line of
                                BUILDKITE_MESSAGE decides, the value a mode,
                                families=<a,b> or cases=<p1,p2>; with neither,
                                main runs targeted and every other branch nothing
    GKE_EVAL_MAX_CONCURRENCY  – eval lanes running at once across the organization,
                                the cap of their one concurrency group (default: 120)
    OME_EVAL_RESULTS_TOKEN    – read-only Buildkite API token (read_builds,
                                read_artifacts) targeted mode reads the newest
                                recorded full round with; unset, targeted runs
                                smoke plus the trailer's selection
    OME_EVAL_RESULTS_PIPELINE – the pipeline the full rounds run in, when it is
                                not this one (default: BUILDKITE_PIPELINE_SLUG)
    OME_EVAL_SAMPLE_PERCENT   – the share of the round's passing lanes a targeted
                                build reruns, 0 to 100 (default: 5)
"""

from __future__ import annotations

import argparse
import base64
import datetime
import functools
import hashlib
import importlib.util
import itertools
import json
import math
import os
import random
import re
import subprocess
import sys
import textwrap
import urllib.error
import urllib.parse
import urllib.request
from fractions import Fraction
from pathlib import Path
from typing import NamedTuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "588845226011")
# Prod ECR is the cluster-foundation managed registry; cf-replace-image-registry
# admission policy fast-paths images already on this registry, so consumers in
# policy-restricted clusters need the prod-tagged copy.
PROD_AWS_ACCOUNT_ID = os.environ.get("PROD_AWS_ACCOUNT_ID", "168653207203")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
BUILD_NUMBER = os.environ.get("BUILDKITE_BUILD_NUMBER", "local")
ECR_REGISTRY = os.environ.get(
    "ECR_REGISTRY", f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com"
)
PROD_ECR_REGISTRY = f"{PROD_AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com"

CI_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/golang:1.26-bookworm"
MUSE_RUST_CI_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/rust:1.95-bookworm"
GO_BUILDER_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/golang:1.26"
RUST_BUILDER_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/library/rust:1-bookworm"

# cf-docker-image plugin handles ECR auth + multi-arch builds. The plugin
# is fetched from mslsrc/dev at job-submit time. See prod_inference for
# precedent.
CF_DOCKER_IMAGE_PLUGIN = (
    "https://github.com/mslsrc/dev.git/buildkite/plugins/cf-docker-image#main"
)
# Runtime image for the cf-docker-image plugin's command hook. Needs
# bash + docker CLI + buildx + aws CLI + uv. We reuse prod_inference's
# ci-base since it has all of those, until we build our own.
CF_DOCKER_PLUGIN_RUNTIME_IMAGE = (
    f"{ECR_REGISTRY}/msl_infra/prod-inference-ci-base"
    "@sha256:fd2e3c825cd063a8b4cd3d7440bf00696c9f8a7f8132e5a7d1e38a3df2f542e8"
)
DOCKER_PLUGIN_REF = "https://github.com/buildkite-plugins/docker-buildkite-plugin.git#v5.13.0"

DEFAULT_QUEUE = "tw-cpu-x86"
BUILD_QUEUE = "tw-cpu-x86"
# GKE-backed queue for the sharded KIND suite. Unlike the TW dind, GKE agents
# run on real VMs / privileged dind where kind can create bridge networks
# (writable /proc/sys/net + ip_forward=1). The queue's agents must provide a
# reachable, privileged Docker daemon — see .buildkite/README-gke-kind.md.
# Default targets the live agent-stack-k8s TPU queue, which lands kind jobs on
# its CPU node pool via the per-step kubernetes plugin. Flip GKE_KIND_QUEUE to a
# dedicated GKE CPU queue (e.g. gke-kind-x86) once infra provisions one.
GKE_QUEUE = os.environ.get("GKE_KIND_QUEUE", "gke-tpu-v7x")
# Buildkite rejects one pipeline upload carrying more jobs than the
# organization's limit of 1500. Every upload stays at or below this count; the
# gap to the limit is headroom for whatever else Buildkite counts against it
# (the uploading job, retried jobs, the steps other jobs upload into the build).
UPLOAD_JOB_LIMIT = 1000
# The step in the first upload that uploads the others, the script it runs and
# where that script writes every document (attached as artifacts).
UPLOAD_STEP_KEY = "upload-eval-lanes"
UPLOAD_SCRIPT = ".buildkite/upload_pipeline_batches.sh"
UPLOAD_BATCH_DIR = ".buildkite/artifacts/pipeline-batches"
# The image the pipeline entrypoint generates in; the upload step runs the
# same generator, so it runs in the same image.
PIPELINE_UPLOAD_IMAGE = f"{ECR_REGISTRY}/pt_dockerio/astral/uv:python3.12-bookworm"


def _load_sibling(name: str):
    """A script beside this one as a module, so the format it defines is
    read from its one definition."""
    path = Path(__file__).resolve().with_name(name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# The round's verdict file: eval_verdicts.py defines its columns, its name,
# the lane record the runner writes and the meta-data that marks a recorded
# round; the full round's recording step and the targeted lookup use these.
eval_verdicts = _load_sibling("eval_verdicts")
# The step of a full round that records its verdicts, the last top-level
# step before the release marker.
EVAL_VERDICTS_STEP_KEY = "eval-verdicts"
# Targeted mode reads the newest recorded full round through the Buildkite
# REST API with a read-only token (read_builds, read_artifacts) from
# OME_EVAL_RESULTS_TOKEN; without one it runs smoke plus the trailer's
# selection. The rounds are read from this pipeline unless
# OME_EVAL_RESULTS_PIPELINE names the one they run in; the organization is
# the build's own.
EVAL_RESULTS_TOKEN_VAR = "OME_EVAL_RESULTS_TOKEN"
EVAL_RESULTS_PIPELINE_VAR = "OME_EVAL_RESULTS_PIPELINE"
EVAL_ROUND_API = "https://api.buildkite.com/v2"
EVAL_ROUND_FETCH_TIMEOUT_SECONDS = 30
# Recorded rounds are few among main builds and the query asks for marked
# ones only, so the newest few are enough; each build comes with its jobs.
EVAL_ROUND_PAGE = 10
# The share of the round's passing lanes a targeted build reruns, rounded
# up, drawn with the commit as seed; unset or empty means the default.
EVAL_SAMPLE_PERCENT_VAR = "OME_EVAL_SAMPLE_PERCENT"
EVAL_SAMPLE_PERCENT_DEFAULT = 5
REPO_ROOT = Path(__file__).resolve().parents[1]
KIND_SUITE_DIR = REPO_ROOT / "tests/integration/singlecluster/controller/omenative_coordination_kind"
MC_KIND_SUITE_DIR = REPO_ROOT / "tests/integration/multicluster/scaleout_kind"
ALFRED_KIND_SUITE_DIR = REPO_ROOT / "tests/integration/singlecluster/controller/alfred_kind"
EVAL_CASE_DIR = REPO_ROOT / "hack/omenative-testing/perm/isvcs"
# Upstream ome-projects/ome commit the alfred image is built from; the
# Dockerfile fetches that source itself, so the pin is passed as a build-arg.
ALFRED_OSS_REF_FILE = REPO_ROOT / ".alfred-oss-ref"
# Upstream commit kubectl-ome is built from inside the ome-bot image; the CLI
# lives on upstream main, so the pin is a commit rather than a release tag.
KUBECTL_OME_OSS_REF_FILE = REPO_ROOT / ".kubectl-ome-oss-ref"

# Multi-cluster groups, same shape and same exact-ownership rule as the
# coordination groups below. Every job here stands up three KIND clusters, so a
# group is only worth adding once its wall-clock justifies paying for another
# set. The quota specs are split out on that basis: they drive real Kueue
# admission, so they wait on quota transitions the placement specs never hit.
MC_KIND_SPEC_GROUPS = (
    ("placement", "Multi-cluster placement", (
        "fanout", "lifecycle", "endpoint", "ownership", "reshard", "scale",
        "trafficmap_gc", "capacity", "backend", "all_plan", "member_recreation",
        "legacy_compatibility",
    )),
    ("quota", "Multi-cluster Kueue quota", (
        "admission", "split_quota", "queue_routing", "single_race", "single_move", "single_loss", "single_zero_floor", "all_zero_floor", "gang",
    )),
    ("scheduling", "Multi-cluster gang physical fit", ("scheduling",)),
)

# The alfred lane: one three-worker KIND cluster per group, exact ownership like
# the coordination groups. _spec_groups_for rejects a mapped stem whose file
# does not exist, so this tuple lists only the spec files present on the branch;
# a new alfred spec is classified here in the commit that adds it.
ALFRED_KIND_SPEC_GROUPS = (
    ("observe", "Alfred observe and advise", ("observe", "recommend_only", "config_reload", "advisory", "ineligible")),
    ("execute", "Alfred guarded execution", ("evacuate_single", "guard", "columnar_status", "no_capacity")),
    ("budgets", "Alfred budgets and cooldowns", ("budget", "cooldown", "priority", "suspicion")),
    ("maintenance-defrag", "Alfred maintenance and defrag", ("maintenance", "maintenance_window", "defrag")),
    ("resilience", "Alfred resilience", ("ack_timeout", "leader_failover", "journal")),
    ("shapes", "Alfred workload shapes", ("gang_evacuate", "partial_gang", "pd", "multi_instance")),
)

# Each group gets one three-node KIND cluster. Groups balance observed runtime
# while keeping related specs together. Exact ownership makes a new spec fail
# pipeline generation until it is classified.
KIND_SPEC_GROUPS = (
    ("foundation-lifecycle", "Foundation and lifecycle", (
        "baseline_no_coord", "diagnostic", "lifecycle_basic", "lifecycle",
        "single_replica", "regression", "watches", "manager_restart_identity",
        "standalone_replica", "standalone_replica_refs", "referenced_engine",
    )),
    ("status-events-services", "Status, events, and services", (
        "events", "status", "status_surface", "status_encoding",
        "producer_services", "producer_traffic", "peer_discovery",
        "multipod_rank0_service", "concurrent_isvcs",
    )),
    ("runtime-retry-recovery", "Runtime, retry, and recovery", (
        "bad_image", "crashloop_dark_rolls_first", "crashloop_new_revision_bound",
        "crashloop_new_revision_gang_bound",
        "crashloop_off_target_roll", "crashloop_one_down_no_surge", "crashloop_open_repair_retarget", "disposition", "retry_block",
        "runtime_corrective_recovery", "runtime_owned_runner_shape",
        "runtime_resolution", "failed_repair_heal",
    )),
    ("pd-topology-isolation", "PD topology and isolation", (
        "pd", "pd_isolation", "pd_three_way", "pd_concurrent", "independent",
        "pairing_protocol", "pd_peer_revision",
    )),
    ("bluegreen-core", "BlueGreen rollouts", (
        "bluegreen", "bluegreen_n3", "bluegreen_inplaceonly_happy",
        "bluegreen_revision_status", "bluegreen_crashloop_repair",
        "inplace_maxunavailable",
    )),
    ("canary-rollouts", "Canary rollouts", (
        "canary_retarget_rollback", "canary_secondary_only", "rolloutpolicy_run",
        "canary_per_unit", "policyref_canary_coordination",
    )),
    ("canary-lifecycle", "Canary lifecycle", (
        "canary_lifecycle_promote", "canary_lifecycle_pausefreeze",
        "canary_lifecycle_podloss", "canary_cutover_last_instance",
        "projection_freeze",
    )),
    ("ratio-core", "RatioBalanced coordination", (
        "ratiobalanced", "ratio_n3", "ratio_pause_resume",
    )),
    ("ratio-edges", "RatioBalanced edge states", (
        "ratio_anchor_scaledown", "ratio_late_member_anchor",
    )),
    ("ratio-load", "RatioBalanced load", (
        "loadtest",
    )),
    ("sequential-core", "Sequential coordination", (
        "sequential", "n3_sequential",
    )),
    ("sequential-variants", "Sequential rollout variants", (
        "sequential_n3_engine_first", "sequential_soak", "sequential_inplace",
        "sequential_recreatepod", "runtime_partition_sequential",
    )),
    ("sequential-interleavings", "Sequential rollout interleavings", (
        "sequential_pause_mid", "sequential_scale_during",
    )),
    ("sequential-holds", "Sequential hold and release", (
        "sequential_failure_hold", "sequential_later_only_bump",
    )),
    ("update-core", "Core update strategies", (
        "update", "inplace", "inplace_retag", "recreate_pod", "mixed_strategy",
        "percomponent_surge", "minreadyseconds", "strategy_switch_midsurge", "placement_surge", "placement_floors",
    )),
    ("percomponent-maxsurge", "Multi-instance maxSurge", (
        "percomponent_maxsurge_multiinstance",
    )),
    ("rolling-updates", "Rolling update strategies", (
        "rolling_recreatepod", "rolling_inplace", "rolling_inplaceonly_reject",
        "rolling_cancel_revert",
    )),
    ("gang-recovery", "Gang recovery and wedge", (
        "gang_corrective_rollout", "gang_wedge_nonready", "crashloop_gang_coordination_gate",
        "gang_unpullable_push",
        "packed_pool_gang_vacated_room",
    )),
    ("gang-surge-multipod", "Gang surge and multi-pod", (
        "concurrent_gang_surge", "gang_surge_floor", "multipod_inplace",
        "multipod_recreatepod", "multipod_surge_fallback", "topology_podgroup",
    )),
    ("migration-relocation", "Migration and relocation", (
        "auto_migrate", "auto_migrate_multiinstance", "migrate", "interleave_extra",
        "packed_pool_bad_push",
    )),
    ("migration-lifecycle", "Migration gang lifecycle", (
        "migration_gang",
    )),
    ("pause-cancel-interleavings", "Pause, cancel, and interleavings", (
        "pause", "pause_midrollout", "pause_recovery", "inference_replica_pause",
        "cancel_midrollout", "interleave",
    )),
    ("scale-transitions", "Scale transitions", (
        "bounded_scale_down", "scale_during_rollout", "bluegreen_scale_down_during",
        "scale_up_beside_open_repair",
    )),
    ("scale-transition-boundaries", "Scale transition boundaries", (
        "scale_tr_oversized_gang", "scale_tr_orphan_refusal",
    )),
    ("scale-transition-races", "Scale transition races", (
        "scale_tr_update_race", "scale_tr_drain_rebound",
    )),
    ("deletion-disruption", "Deletion and disruption", (
        "cleanup_lifecycle", "force_delete", "pdb_eviction", "teardown",
        "partial_failure",
    )),
)

# Quarantine: the escape hatch that keeps a red KIND lane meaningful.
#
# A spec that fails on `main` without an engine change, or that passes on retry
# without one, is a defect with an owner — not noise to retry away. A retry is
# never the fix. The spec moves here: it leaves its gating group above (a
# quarantined spec listed in a group fails generation) and runs in the one
# soft-fail "Quarantined KIND specs" group, so a PR author is never gated on a
# failure nobody is fixing and the failure stays visible.
#
# This pipeline has no nightly schedule, so the quarantine group runs on every
# build as a soft-fail. Give it a nightly-only trigger once one exists.
#
# Every entry carries an owner, a one-line reason and an expiry (two weeks is
# the norm). Generation fails on an unknown spec, an empty owner or reason, and
# on the day after `expires`: an expired quarantine blocks the build until
# someone fixes the spec or extends the entry with a reason. To un-quarantine,
# delete the entry and put the stem back in its group above, in the fix PR.
KIND_QUARANTINE: list[dict[str, str]] = []

# Scheduler-twin soft list: the escape hatch that keeps the gating twins
# meaningful, the way KIND_QUARANTINE does for a spec. A group whose twin is
# red on main while its default-scheduler job is green is a gang-scheduler
# defect with an owner; it stays advisory here, visibly and briefly, instead of
# turning every main build red or being retried away. Same rules as the
# quarantine: owner, one-line reason, expiry (two weeks is the norm);
# generation fails on an unknown group, an empty owner or reason, and on the
# day after `expires`. To promote the twin, delete the entry in the fix PR.
KIND_SCHEDULER_SOFT_GROUPS: list[dict[str, str]] = [
    {
        "group": "gang-surge-multipod",
        "owner": "slin1237",
        "expires": "2026-10-12",
        "reason": (
            "the required leader topology spread is not honored under the "
            "gang scheduler: two instances' leaders never land on distinct "
            "nodes within the spec's budget; the default-scheduler job passes"
        ),
    },
    {
        "group": "migration-relocation",
        "owner": "slin1237",
        "expires": "2026-10-12",
        "reason": (
            "auto-relocation of a stuck Instance does not complete under the "
            "gang scheduler; the default-scheduler job passes"
        ),
    },
]

# Eval-twin soft list: the same escape hatch for the eval shards under the gang
# scheduler, keyed by topology because a gang-scheduler defect shows on every
# feature of the shape it breaks. A topology whose scheduler twins are red on
# main while their default-scheduler shards are green stays advisory here,
# visibly and briefly, with an owner, a one-line reason and an expiry;
# generation fails on an unknown topology, an empty owner or reason, and on the
# day after `expires`. To promote the twins, delete the entry in the fix PR.
EVAL_SCHEDULER_SOFT_TOPOLOGIES: list[dict[str, str]] = [
    {
        "topology": "pd-mn",
        "owner": "slin1237",
        "expires": "2026-10-14",
        "reason": (
            "a gang the gang scheduler rejects for a full domain while the "
            "previous case's pods terminate is not retried when the domain "
            "empties, so a multi-node P/D case times out in its init window; "
            "the default-scheduler shards pass"
        ),
    },
]

# Eval-feature soft list: the escape hatch for whole families. A family that
# landed this week is run on every main build so its findings are classified,
# but it stays advisory while those findings are turned into issues and fixes,
# so a release gates on the lanes that were green before the family existed.
# Same rules as the other lists: a feature the corpus does not carry, an empty
# owner or reason, or an entry past `expires` fails generation. To promote a
# family, delete its entry in the PR that makes its first main round green.
EVAL_SOFT_FEATURES: list[dict[str, str]] = [
    {
        "feature": feature,
        "owner": "slin1237",
        "expires": "2026-10-15",
        "reason": (
            "marathon family: its first main rounds are classified into "
            "issues and fixes; advisory until they are green"
        ),
    }
    for feature in (
        "gen-switch",
        "gen-crashloop",
        "gen-crashloop-idle",
        "gen-crashloop-sibling",
        "gen-heal",
        "gen-imagepull",
        "gen-node",
        "gen-ctrlrestart",
        "gen-canary-crash",
        "gen-canary-second-rev",
        "gen-canary-podkill",
        "gen-canary-fault-old",
        "gen-canary-fault-new",
        "gen-canary-fault-gate",
        "gen-rollback",
        "gen-crashloop-new",
        "gen-crashloop-group",
        "gen-pause-repair",
        "gen-size",
        "gen-router-gang",
        "gen-ops-held",
        "gen-runtime-edit-repair",
        "gen-migrate-bump",
        "gen-readiness",
        "gen-gang-member",
        "gen-tight",
        "gen-evict",
        "gen-preempt",
        "gen-pending",
        "gen-budget",
        "gen-inplaceonly",
        "gen-fault-scale",
        "gen-fault-delete",
        "gen-fault-move",
        "gen-fault-edit",
        "gen-packed-pool",
        "gen-repeat",
        "gen-webhook",
        "gen-conflict",
        "gen-crash-once",
        "gen-lift",
    )
]


def _truthy(val: str) -> bool:
    return val.strip().lower() in ("1", "true", "yes", "on")


# The GKE KIND step runs by default and gates the build. GKE_QUEUE targets a
# live queue so the step does not wait indefinitely for a nonexistent agent.
# Kill switch: set
# GKE_KIND_ENABLED=0 to drop the step entirely (e.g. if the queue goes away).
# See .buildkite/README-gke-kind.md.
GKE_KIND_ENABLED = _truthy(os.environ.get("GKE_KIND_ENABLED", "1"))
GKE_EVAL_ENABLED = _truthy(os.environ.get("GKE_EVAL_ENABLED", "1"))
# Second scheduler dimension of the eval: every feature/topology job again with
# the fixtures stamped schedulerName=ome-scheduler and the gang scheduler
# installed in the KIND cluster. The twins gate the build like the default
# jobs except the topologies in EVAL_SCHEDULER_SOFT_TOPOLOGIES;
# GKE_EVAL_SCHEDULER_HARD_FAIL=0 is the emergency switch that makes every twin
# advisory again, to be reported the moment it is flipped.
GKE_EVAL_SCHEDULER_ENABLED = _truthy(os.environ.get("GKE_EVAL_SCHEDULER_ENABLED", "1"))
GKE_EVAL_SCHEDULER_HARD_FAIL = _truthy(os.environ.get("GKE_EVAL_SCHEDULER_HARD_FAIL", "1"))
# The gang scheduler places one gang member per node, so a multi-node case needs
# as many worker nodes as its peak pod count. Its scheduler twin gets a cluster
# sized to the shard, capped here; cases above the cap are left out of the twin
# and named in OME_EVAL_SCHEDULER_SKIPPED.
GKE_EVAL_SCHEDULER_MAX_WORKERS = int(os.environ.get("GKE_EVAL_SCHEDULER_MAX_WORKERS", "12"))
# Every eval lane is in one Buildkite concurrency group, so across every
# build of the organization at most GKE_EVAL_MAX_CONCURRENCY of them hold a
# GKE agent pod at once and the rest wait in order. The cap is read at
# generation by _eval_max_concurrency; unset or empty means the default.
EVAL_CONCURRENCY_GROUP = "ome/gke-evals"
EVAL_MAX_CONCURRENCY_VAR = "GKE_EVAL_MAX_CONCURRENCY"
EVAL_MAX_CONCURRENCY_DEFAULT = 120
EVAL_GANG_SCHEDULER = "ome-scheduler"
# Scheduler dimension of the KIND suite lanes: every semantic group again with
# the fixture runtime selecting ome-scheduler, installed into the suite's
# cluster. Main-only (the baked scheduler image is a main artifact). The twins
# gate the build except the groups in KIND_SCHEDULER_SOFT_GROUPS;
# GKE_KIND_SCHEDULER_HARD_FAIL=0 is the emergency switch that makes every twin
# advisory again.
GKE_KIND_SCHEDULER_ENABLED = _truthy(os.environ.get("GKE_KIND_SCHEDULER_ENABLED", "1"))
GKE_KIND_SCHEDULER_HARD_FAIL = _truthy(os.environ.get("GKE_KIND_SCHEDULER_HARD_FAIL", "1"))
# "" is the cluster default scheduler; every eval job is emitted once per entry.
EVAL_SCHEDULERS = [""] + ([EVAL_GANG_SCHEDULER] if GKE_EVAL_SCHEDULER_ENABLED else [])
# Force-delete dimension of the eval: a case whose header carries
# needs=forcedelete reads the outcome the controller's lifecycle.forceDelete
# policy produces, so it runs in a twin of its lane that deploys the policy
# (deploy-ome-kind.sh reads the switch) and nowhere else; every other case
# of the lane stays on the plain deploy, where the runner would otherwise
# SKIP the policy-bound ones. The twin exists only for lanes that have such
# cases and is emitted per scheduler like any lane.
EVAL_FORCE_DELETE_NEED = "forcedelete"
EVAL_FORCE_DELETE_ENV = "OME_EVAL_FORCE_DELETE"
EVAL_FORCE_DELETE_SLUG = "forcedelete"
# batch.py runs a case whose fault apparatus reaches past its own pods (a
# namespace-wide quota, a cordoned or stopped node, a manager restart, every
# node tainted or filled, the admission webhook made unreachable) on its own
# after the concurrent pool drains, so the planner gives such a case a wave
# of its own instead of packing it by its peak pods. The kinds mirror
# batch.py's cluster_wide_fault; the test suite keeps the two rules in step
# over the committed corpus.
EVAL_EXCLUSIVE_FAULT_KINDS = frozenset({"quota", "cordon", "ctrlrestart", "node", "taint", "preempt", "webhook"})
EVAL_EXCLUSIVE_VIA = "preempt"
EVAL_EXCLUSIVE_POOL = "packed"
# Wall-clock minutes one exclusive case of a family takes on the GKE agents,
# the figure that sizes the serial part of its shards: measured from the main
# builds where the family has run, expected from the runner's budgets where
# it has not yet. An unlisted family gets the default.
EVAL_EXCLUSIVE_CASE_MINUTES: dict[str, float] = {
    # measured 6-11: the node is stopped or made NotReady, the pod-eviction
    # toleration runs out, then the repair or relocation converges
    "gen-node": 9,
    # the node family's loss landing at the moment of a move
    "gen-fault-move": 9,
    # measured 2-3.5: the manager restarts at a hook and the roll completes
    "gen-ctrlrestart": 3,
    # expected: a squeezed roll waits on capacity through its hold window,
    # then gets the roll's whole convergence budget
    "gen-tight": 12,
    # expected: a placement or admission wait, its lift, the roll's completion
    "gen-pending": 6,
    # expected: fillers take every node, the scheduler preempts the victim,
    # the fillers are dropped
    "gen-preempt": 6,
    # expected: fillers take every node, the bad push is held through its
    # escalation and the believe window, then the fix rolls one Instance at
    # a time
    "gen-packed-pool": 7,
    # expected: a manager restarted three times under a roll or a scale (each
    # restart about a minute to Ready, then the operation's convergence), and
    # a preemption's fill-and-press cycle twice over with its repair between
    "gen-repeat": 8,
    # expected: the fleet settles, the webhook is unreachable for the hold
    # window or until the status says why (up to 90 s either way), the push
    # or the scale completes once it is back, the rows settle
    "gen-webhook": 6,
    # measured: quota waits of 7 minutes among restart and cordon cells of 1-2
    "fault": 4,
    # measured: manager restarts at canary hooks of 2 minutes, two quota
    # waits of 7
    "canary": 3,
}
# A quota wait, the longest exclusive cell the hand-written families carry.
EVAL_EXCLUSIVE_CASE_MINUTES_DEFAULT = 7
# Kill switch for the deployed-mode operator-upgrade identity gate (see
# _gke_upgrade_identity_step). The step resolves its own previous-image
# reference at run time, so this only needs to gate whether the step exists.
GKE_UPGRADE_ENABLED = _truthy(os.environ.get("GKE_UPGRADE_ENABLED", "1"))
# Kill switch for the alfred lane. Off also drops the package step's wait on
# the alfred image build, so a broken alfred build cannot block the other lanes.
GKE_ALFRED_ENABLED = _truthy(os.environ.get("GKE_ALFRED_ENABLED", "1"))

BRANCH = os.environ.get("BUILDKITE_BRANCH", "")
COMMIT = os.environ.get("BUILDKITE_COMMIT", "HEAD")
# What the OMENative eval runs on this build. OME_EVAL_MODE names the mode;
# unset, the newest "Eval: <value>" line of the commit message decides; with
# neither, main runs targeted and every other branch nothing. GKE_EVAL_ENABLED=0
# forces off. The trailer may name a selection instead of a mode.
#   off             no eval image step and no eval group
#   smoke           the first shard of every feature/topology lane, default scheduler
#   targeted        smoke, plus every case the trailer's families or cases name
#   full            every case on every scheduler
#   families=<a,b>  every case of those corpus folders, every scheduler
#   cases=<p1,p2>   exactly those cases in their lanes, default scheduler
EVAL_MODES = ("off", "smoke", "targeted", "full")
EVAL_SELECTION_KINDS = ("families", "cases")
EVAL_TRAILER = re.compile(r"^[ \t]*eval:[ \t]*(\S.*?)[ \t]*$", re.IGNORECASE | re.MULTILINE)


def _resolve_eval_mode(
    env_mode: str | None, message: str, branch: str, enabled: bool
) -> tuple[str, tuple[str, list[str]] | None]:
    """(mode, selection) for a build: the mode from OME_EVAL_MODE, else from
    the newest Eval: trailer of the message, else the branch default; the
    selection from the trailer whenever it names families or cases. A value
    naming nothing known is kept as written for generation to refuse."""
    trailer = None
    for match in EVAL_TRAILER.finditer(message):
        trailer = match.group(1).lower()
    selection = None
    if trailer is not None:
        kind, _, names = trailer.partition("=")
        kind = kind.strip()
        if kind in EVAL_SELECTION_KINDS:
            selection = (kind, [name.strip() for name in names.split(",") if name.strip()])
            trailer = kind
    if env_mode is not None and env_mode.strip():
        mode = env_mode.strip().lower()
    elif trailer is not None:
        mode = trailer
    else:
        mode = "targeted" if branch == "main" else "off"
    return (mode if enabled else "off"), selection


EVAL_MODE, EVAL_SELECTION = _resolve_eval_mode(
    os.environ.get("OME_EVAL_MODE"), os.environ.get("BUILDKITE_MESSAGE", ""), BRANCH, GKE_EVAL_ENABLED
)
# (label-suffix, ECR repo name under msl_infra/, Dockerfile)
# Drop redundant "ome-" prefix in ECR names so we don't get "ome-ome-*".
# model-agent is not built locally; we consume upstream-published images.
IMAGES = [
    ("ome-manager", "ome-manager", "dockerfiles/manager.Dockerfile"),
    ("ome-agent", "ome-agent", "dockerfiles/ome-agent.Dockerfile"),
    # CPU-only metrics workload for the OMENative canary-analysis e2e tests
    # (hack/omenative-testing). Pulled by runtime/csr-metrics.yaml.
    ("canary-metrics", "canary-metrics", "dockerfiles/canary-metrics.Dockerfile"),
    # MUSE console: one image serving the read-plane API + the built web UI.
    ("muse-server", "muse-server", "dockerfiles/muse-server.Dockerfile"),
    # ome-scheduler: kube-scheduler + OMEGangPack. Separate Go module
    # under scheduler/; pure-Go static binary, no xet.
    ("ome-scheduler", "ome-scheduler", "dockerfiles/ome-scheduler.Dockerfile"),
    # kind-ci: offline toolchain and image archives for the GKE-backed KIND
    # suites, including cert-manager and the OMENative eval runtime fixture.
    # It is built on TW (with egress) and mirrored to the registry reachable
    # from the restricted GKE queue. See dockerfiles/kind-ci.Dockerfile.
    ("kind-ci", "kind-ci", "dockerfiles/kind-ci.Dockerfile"),
    # ome-quota-manager: the fleet-quota control plane (OEP-0024). Shares the
    # root Go module (unlike ome-scheduler) but ships as its own image so its
    # Kueue write grant is scoped to its own ServiceAccount.
    ("ome-quota-manager", "ome-quota-manager", "dockerfiles/ome-quota-manager.Dockerfile"),
    # alfred: the upstream GPU cluster caretaker (node-health evacuation +
    # defragmentation via the OMENative migration-request annotation), built
    # unmodified from the upstream commit in .alfred-oss-ref. Pure Go, no xet.
    # An optional ome-resources component (alfred.enabled), so it is not part
    # of the release marker.
    ("alfred", "alfred", "dockerfiles/alfred.Dockerfile"),
    # ome-bot: the OME Slack bot core (meta-internal/omebot) plus kubectl-ome
    # built from the pinned upstream ref. Deployed to the management cluster
    # by the ome-bot chart; not part of the release marker.
    ("ome-bot", "ome-bot", "dockerfiles/ome-bot.Dockerfile"),
]

IMAGE_PLATFORMS = ("linux/amd64", "linux/arm64")
# Kill switch for image reuse: "1" makes every image step build and republish
# its inputs tag, and the eval image package step package and retag, so later
# builds reuse the refreshed images. Base images, apt packages and downloaded
# tools are not inputs: they refresh when an input changes or on a forced
# rebuild, not on every merge.
IMAGE_REBUILD = _truthy(os.environ.get("OME_IMAGE_REBUILD", "0"))
IMAGE_INPUTS_TAG_PREFIX = "inputs-"
EVAL_IMAGE_INPUTS_TAG_PREFIX = "eval-inputs-"
# Hashed into every image: the context filter decides what each COPY sees.
COMMON_IMAGE_INPUTS = (".dockerignore",)
# Everything an image build reads from the tree, by label: its Dockerfile, the
# paths the Dockerfile's COPY lines name (a directory covers its whole tracked
# tree) and the pin files its build args come from. The build args and the
# platforms are hashed with the listing. The one-line reason says why the set
# is what it is; the unit tests fail on a COPY source outside its image's set
# and on a path that matches no tracked file.
IMAGE_INPUTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "ome-manager": (
        "copies go.mod/go.sum, cmd/, pkg/ (the xet crate it links) and meta-internal/",
        ("dockerfiles/manager.Dockerfile", "go.mod", "go.sum", "cmd", "pkg", "meta-internal"),
    ),
    "ome-agent": (
        "copies go.mod/go.sum, cmd/, pkg/, internal/ and the agent config file",
        (
            "dockerfiles/ome-agent.Dockerfile", "go.mod", "go.sum", "cmd", "pkg", "internal",
            "config/ome-agent/ome-agent.yaml",
        ),
    ),
    "canary-metrics": (
        "a stdlib-only Go program; its own directory is all the build copies",
        ("dockerfiles/canary-metrics.Dockerfile", "hack/omenative-testing/metrics-server"),
    ),
    "muse-server": (
        "copies the muse tree and the Rust client crate it depends on by path",
        ("dockerfiles/muse-server.Dockerfile", "muse", "ome-client-rs"),
    ),
    "ome-scheduler": (
        "its own Go module; the scheduler/ tree is all the build copies",
        ("dockerfiles/ome-scheduler.Dockerfile", "scheduler"),
    ),
    "kind-ci": (
        "caches the root module graph and the xet crate; every tool, node and fixture image pin is an ARG in the Dockerfile",
        ("dockerfiles/kind-ci.Dockerfile", "go.mod", "go.sum", "pkg/xet"),
    ),
    "ome-quota-manager": (
        "copies go.mod/go.sum, pkg/ and only its own cmd/ directory",
        ("dockerfiles/ome-quota-manager.Dockerfile", "go.mod", "go.sum", "pkg", "cmd/ome-quota-manager"),
    ),
    "alfred": (
        "built from upstream source at the pinned commit; nothing from this tree but the pin",
        ("dockerfiles/alfred.Dockerfile", ".alfred-oss-ref"),
    ),
    "ome-bot": (
        "copies go.mod/go.sum and meta-internal/omebot/, ships docs/, oeps/ and the skills, and builds the CLI from the pinned upstream commit",
        (
            "dockerfiles/ome-bot.Dockerfile", "go.mod", "go.sum", "meta-internal/omebot", "docs", "oeps",
            ".agents/skills", ".kubectl-ome-oss-ref",
        ),
    ),
}
# The eval image is these images' published tags appended onto kind-ci by the
# packaging script, so its inputs are their inputs tags plus that script.
EVAL_IMAGE_SOURCES = ("kind-ci", "ome-manager", "ome-agent", "ome-scheduler", "canary-metrics")
EVAL_IMAGE_SCRIPT = ".buildkite/package_kind_eval_image.sh"


def _push_every_branch_images() -> frozenset[str]:
    """Images whose build pushes on every branch.

    The alfred KIND lane loads its image from a tarball the package step
    pulls out of the dev registry, so a PR must be able to publish it. With
    the lane off nothing consumes a branch build, and the image pushes on
    main only like every other. The prod mirror stays main-only for all.
    """
    if GKE_KIND_ENABLED and GKE_ALFRED_ENABLED:
        return frozenset({"alfred"})
    return frozenset()


CHARTS = ["ome-crd", "ome-resources", "ome-isvc", "muse", "ome-scheduler", "ome-quota-manager", "ome-bot"]

# The atomic marker is the Kargo-facing contract for an OME control-plane
# release. Keep this deliberately narrower than CHARTS: only components that
# must advance together belong in the marker.
RELEASE_MARKER_CHARTS = (
    "ome-crd",
    "ome-resources",
    "ome-scheduler",
    "ome-quota-manager",
)
RELEASE_MARKER_IMAGES = (
    "ome-manager",
    "ome-agent",
    "ome-scheduler",
    "ome-quota-manager",
)

# Charts whose published values carry an image ref, so their publish step has to
# wait for that image to exist in the prod registry. Mirrors the per-chart
# stamping blocks in push_chart.sh; a chart that stamps but is missing here
# publishes ahead of its own image and installs as ErrImagePull.
IMAGE_STAMPING_CHARTS = ("ome-resources", "ome-scheduler", "ome-quota-manager", "muse", "ome-bot")


# ---------------------------------------------------------------------------
# Step helpers
# ---------------------------------------------------------------------------


def _yaml_block(obj, indent: int = 0) -> str:
    """Serialize a dict/list/scalar to an indented YAML *block* fragment.

    Lets pipeline snippets be written as plain Python data instead of
    hand-aligned f-strings — easier to read and write, and impossible to
    misalign. Stdlib only (no PyYAML): scalars go through json.dumps, which
    quotes/escapes correctly and stays valid YAML (YAML is a JSON superset).
    Dict/list values render as nested blocks; empty ones render inline ({} / []).
    """
    pad = " " * indent
    if isinstance(obj, dict):
        lines = []
        for k, v in obj.items():
            if isinstance(v, (dict, list)) and v:
                lines.append(f"{pad}{k}:")
                lines.append(_yaml_block(v, indent + 2))
            else:
                lines.append(f"{pad}{k}: {json.dumps(v)}")
        return "\n".join(lines)
    if isinstance(obj, list):
        lines = []
        for item in obj:
            if isinstance(item, (dict, list)) and item:
                sub = _yaml_block(item, indent + 2).split("\n")
                # First child line carries the "- " marker; the rest stay aligned.
                lines.append(f"{pad}- " + sub[0][indent + 2:])
                lines.extend(sub[1:])
            else:
                lines.append(f"{pad}- {json.dumps(item)}")
        return "\n".join(lines)
    return f"{pad}{json.dumps(obj)}"


def _job_count(yaml_text: str) -> int:
    """The jobs Buildkite counts in a pipeline document: every command step
    (the generator starts each one with its label) and every wait step. A
    group is a container for its children, not a job of its own."""
    return len(re.findall(r"^\s*- (?:label: |wait\s*$)", yaml_text, re.M))


def _top_level_keys(yaml_text: str) -> list[str]:
    """The keys of a document's top-level steps and groups, in order: the
    generator renders those at four spaces and a group's children at eight."""
    return re.findall(r'^    key: "([^"]+)"$', yaml_text, re.M)


def _gh_status(key: str, indent: int = 12) -> str:
    """Per-step GitHub commit status block; without it BuildKite publishes only the pipeline rollup."""
    return _yaml_block({"notify": [{"github_commit_status": {"context": key}}]}, indent)


def _retry(automatic: list, indent: int = 12) -> str:
    """The general, reusable `retry.automatic` builder: pass a list of condition
    dicts (plain data), e.g. [{"exit_status": 125, "limit": 2},
    {"signal_reason": "agent_stop", "limit": 2}]. _auto_retry / _infra_retry are
    the named presets below; call _retry directly for any other retry policy.
    """
    return _yaml_block({"retry": {"automatic": automatic}}, indent)


def _auto_retry(limit: int = 2, indent: int = 12) -> str:
    """Preset: retry on ANY non-zero exit — for the network-heavy image build
    (Go module fetch from proxy.golang.org, then ECR push) and mirror (crane
    copy + cross-account auth) steps, which flake on transient blips (e.g. the
    HTTP/2 INTERNAL_ERROR that sank build 2445). A real Dockerfile/compile error
    still fails once the retries are spent, so it masks nothing, and the bound
    stops a genuinely broken build from looping.
    """
    return _retry([{"exit_status": "*", "limit": limit}], indent)


def _infra_retry(limit: int = 2, indent: int = 12) -> str:
    """Preset: retry ONLY on infrastructure failure — the TW dind daemon dying
    (exit_status 125: docker.sock gone mid-run, or the pre-command docker
    health-repair hook failing before the step runs), the agent being lost
    (signal_reason agent_stop), or the job's process being lost under it
    (signal_reason process_run_error: the container died and the job was
    recorded with exit -1, which no exit_status condition matches), or the
    agent stack failing to run the job at all (signal_reason stack_error, the
    same exit -1). It never
    matches exit 1, so a real test/lint/-race failure still blocks merge —
    safe on merge gates precisely because it cannot match a code failure.
    Seen on build 2523 (scheduler-test: dind
    vanished during `go mod download`) and 2548 (helm-lint: pre-command docker
    health failed before the lint ran).
    """
    return _retry(_infra_retry_automatic(limit), indent)


def _infra_retry_automatic(limit: int = 2) -> list:
    """The _infra_retry conditions as plain data, for steps assembled as
    dicts and rendered by _yaml_block rather than as YAML text.
    """
    return [
        {"exit_status": 125, "limit": limit},
        {"signal_reason": "agent_stop", "limit": limit},
        {"signal_reason": "process_run_error", "limit": limit},
        {"signal_reason": "stack_error", "limit": limit},
    ]


def _tw_docker_plugin(
    image: str,
    indent: int = 6,
    extra_volumes: tuple[str, ...] = (),
) -> str:
    """Emit the TW docker plugin block for a command step.

    TW agents run the Buildkite job on the host and use this plugin to launch the
    command in the requested workload image. The identity/cert mounts mirror the
    shared MSL TW helper so cloud_oss, curl, Go, Helm, and crane can authenticate
    from the TW host identity and cert mounts.

    extra_volumes appends host mounts beyond the identity/cert set (e.g. the
    Docker daemon socket for the KIND step). Never set the plugin's network
    option here: the TW agent's docker CLI wrapper
    (fbsource msl_infra/buildkite buildkite-docker-wrapper.sh) already injects
    --network host into every workload container it launches and hard-fails
    `docker network create`, so the plugin's create-if-missing network path
    must stay unused.
    """
    pad = " " * indent
    extra_volume_lines = "".join(f"{pad}      - {v}\n" for v in extra_volumes)
    return (
        f'{pad}- "{DOCKER_PLUGIN_REF}":\n'
        f'{pad}    image: "{image}"\n'
        f"{pad}    mount-buildkite-agent: true\n"
        f"{pad}    propagate-environment: true\n"
        f"{pad}    environment:\n"
        f"{pad}      - GIT_TERMINAL_PROMPT=0\n"
        f"{pad}      - SSL_CERT_FILE=/etc/meta/certs/ca-bundle.crt\n"
        f"{pad}      - SSL_CERT_DIR=/etc/ssl/certs:/etc/meta/certs\n"
        # Bare names pass the value from the TW agent's process env into
        # the container (propagate-environment only carries the Buildkite
        # job env, not agent env). These wire the TW-provisioned AWS
        # credential chain (cloud_oss path + config-file/credential-
        # process + S32M endpoint) through to _ecr_login.sh — mirrors
        # prod_inference's TW CPU plugin config.
        f"{pad}      - CLOUD_OSS_PATH\n"
        f"{pad}      - AWS_CONFIG_FILE\n"
        f"{pad}      - AWS_SHARED_CREDENTIALS_FILE\n"
        f"{pad}      - S32M_ENDPOINT\n"
        f"{pad}      - AWS_ENDPOINT_URL_S3\n"
        f"{pad}      - PATH=/opt/cloud.cli:/opt/cloud.cli/bin:/go/bin:/usr/local/go/bin:/usr/local/cargo/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin\n"
        f"{pad}    volumes:\n"
        f"{pad}      - /etc/pki/tls/certs:/etc/meta/certs:ro\n"
        f"{pad}      - /etc/pki/ca-trust:/etc/pki/ca-trust:ro\n"
        f"{pad}      - /etc/fbwhoami:/etc/fbwhoami:ro\n"
        f"{pad}      - /etc/smc.tiers:/etc/smc.tiers:ro\n"
        f"{pad}      - /var/facebook/rootcanal:/var/facebook/rootcanal:ro\n"
        f"{pad}      - /var/facebook/x509_identities:/var/facebook/x509_identities:ro\n"
        f"{pad}      - /var/facebook/tupperware/tls:/var/facebook/tupperware/tls:ro\n"
        f"{pad}      - /data/buildkite/cloud.cli:/opt/cloud.cli:ro\n"
        f"{extra_volume_lines}"
    )


def _kind_ci_image(tag: str) -> str:
    return f"{PROD_ECR_REGISTRY}/msl_infra/kind-ci:{tag}"


def _gke_kind_plugins(image: str, shared_model: bool = False) -> list[dict]:
    """Agent-stack pod: offline runner plus a privileged Docker sidecar."""
    runner_mounts = []
    dind_mounts = [{"name": "docker-graph", "mountPath": "/var/lib/docker"}]
    volumes = [{"name": "docker-graph", "emptyDir": {}}]
    if shared_model:
        model_mount = {"name": "eval-model", "mountPath": "/ome-eval-model"}
        runner_mounts.append(model_mount)
        dind_mounts.append(model_mount)
        volumes.append({"name": "eval-model", "emptyDir": {}})

    pod_spec = {
        "containers": [
            {
                "name": "container-0",
                "image": image,
                "env": [{"name": "DOCKER_HOST", "value": "tcp://localhost:2375"}],
                "volumeMounts": runner_mounts,
                "resources": {
                    "requests": {"cpu": "3", "memory": "8Gi", "ephemeral-storage": "8Gi"},
                    "limits": {"cpu": "6", "memory": "12Gi", "ephemeral-storage": "16Gi"},
                },
            },
            {
                "name": "dind",
                "image": f"{PROD_ECR_REGISTRY}/pt_dockerio/library/docker:28-dind",
                "securityContext": {"privileged": True},
                "env": [{"name": "DOCKER_TLS_CERTDIR", "value": ""}],
                "volumeMounts": dind_mounts,
                "resources": {
                    "requests": {"cpu": "2", "memory": "6Gi", "ephemeral-storage": "16Gi"},
                    "limits": {"cpu": "4", "memory": "8Gi", "ephemeral-storage": "24Gi"},
                },
            },
        ],
        "volumes": volumes,
    }
    pod_spec["nodeSelector"] = {"kubernetes.io/arch": "amd64"}
    return [{"kubernetes": {"podSpecPatch": pod_spec}}]


def _spec_files_in(suite_dir) -> list[str]:
    """Every spec file in a KIND suite directory that declares a Ginkgo tree."""
    pattern = re.compile(r"ginkgo\.(?:Describe|DescribeTable)\(")
    return [
        path.name
        for path in sorted(suite_dir.glob("*_kind_test.go"))
        if pattern.search(path.read_text())
    ]


def _kind_spec_files() -> list[str]:
    return _spec_files_in(KIND_SUITE_DIR)


def _mc_kind_spec_files() -> list[str]:
    return _spec_files_in(MC_KIND_SUITE_DIR)


def _alfred_kind_spec_files() -> list[str]:
    return _spec_files_in(ALFRED_KIND_SUITE_DIR)


def _today() -> datetime.date:
    """Indirection so the expiry check is testable without freezing the clock."""
    return datetime.date.today()


def _kind_quarantine_files() -> list[str]:
    """Validate KIND_QUARANTINE and return its spec filenames, in order.

    Every rejection here is a quarantine that has stopped carrying information:
    a spec that no longer exists, an entry nobody owns or explains, or one whose
    two weeks are up. The last is the point of the expiry — an abandoned
    quarantine blocks the build instead of silently un-testing a spec forever.
    """
    actual = set(_spec_files_in(KIND_SUITE_DIR))
    files: list[str] = []
    for entry in KIND_QUARANTINE:
        spec = str(entry.get("spec", "")).strip()
        if not spec:
            raise RuntimeError("KIND quarantine entry without a spec")
        filename = f"{spec}.go"
        if filename not in actual:
            raise RuntimeError(f"quarantined KIND spec does not exist: {filename}")
        if filename in files:
            raise RuntimeError(f"duplicate KIND quarantine entry: {filename}")
        _owned_until(
            entry, f"KIND quarantine entry for {spec}",
            expired=lambda raw, owner: (
                f"KIND quarantine for {spec} expired on {raw} (owner {owner}): "
                "fix the spec and return it to its group, or extend the entry "
                "with a reason"
            ),
        )
        files.append(filename)
    return files


def _owned_until(entry: dict, what: str, expired) -> str:
    """Check the owner/reason/expires triple every dated escape hatch carries
    and return the expiry as written. `expired(raw, owner)` words the error for
    an entry whose time is up.
    """
    for field in ("owner", "reason"):
        if not str(entry.get(field, "")).strip():
            raise RuntimeError(f"{what} has no {field}")
    raw_expiry = str(entry.get("expires", "")).strip()
    try:
        expires = datetime.date.fromisoformat(raw_expiry)
    except ValueError:
        raise RuntimeError(
            f"{what} needs an expires date (YYYY-MM-DD), got {raw_expiry!r}"
        ) from None
    if expires < _today():
        raise RuntimeError(expired(raw_expiry, entry["owner"]))
    return raw_expiry


def _kind_scheduler_soft_groups() -> dict[str, str]:
    """Validate KIND_SCHEDULER_SOFT_GROUPS; return {group slug: expiry}.

    Same rejections as the quarantine, for the same reason: an entry that
    names no group, that nobody owns or explains, or whose time is up has
    stopped carrying information, and an abandoned soft twin would otherwise
    stay advisory forever.
    """
    known = {slug for slug, _, _ in KIND_SPEC_GROUPS}
    soft: dict[str, str] = {}
    for entry in KIND_SCHEDULER_SOFT_GROUPS:
        group = str(entry.get("group", "")).strip()
        if not group:
            raise RuntimeError("KIND scheduler soft-list entry without a group")
        if group not in known:
            raise RuntimeError(f"soft-listed KIND scheduler twin is not a spec group: {group}")
        if group in soft:
            raise RuntimeError(f"duplicate KIND scheduler soft-list entry: {group}")
        soft[group] = _owned_until(
            entry, f"KIND scheduler soft-list entry for {group}",
            expired=lambda raw, owner: (
                f"KIND scheduler twin {group} was soft until {raw} (owner {owner}): "
                "fix it under the gang scheduler, or extend the entry with a reason"
            ),
        )
    return soft


def _eval_soft_features() -> dict[str, str]:
    """Validate EVAL_SOFT_FEATURES; return {feature: expiry}.

    Same rejections as the scheduler soft list: an entry that names no feature
    the corpus carries, that nobody owns or explains, or whose time is up has
    stopped carrying information.
    """
    known = {feature for feature, _, _ in _eval_case_groups()}
    soft: dict[str, str] = {}
    for entry in EVAL_SOFT_FEATURES:
        feature = str(entry.get("feature", "")).strip()
        if not feature:
            raise RuntimeError("eval soft-list entry without a feature")
        if feature not in known:
            raise RuntimeError(f"soft-listed eval feature is not a corpus feature: {feature}")
        if feature in soft:
            raise RuntimeError(f"duplicate eval soft-list entry: {feature}")
        soft[feature] = _owned_until(
            entry, f"eval soft-list entry for {feature}",
            expired=lambda raw, owner: (
                f"eval lanes of {feature} were soft until {raw} (owner {owner}): "
                "make the family green, or extend the entry with a reason"
            ),
        )
    return soft


def _eval_scheduler_soft_topologies() -> dict[str, str]:
    """Validate EVAL_SCHEDULER_SOFT_TOPOLOGIES; return {topology: expiry}.

    Same rejections as the KIND soft list: an entry that names no topology the
    corpus carries, that nobody owns or explains, or whose time is up has
    stopped carrying information.
    """
    known = {topology for _, topology, _ in _eval_case_groups()}
    soft: dict[str, str] = {}
    for entry in EVAL_SCHEDULER_SOFT_TOPOLOGIES:
        topology = str(entry.get("topology", "")).strip()
        if not topology:
            raise RuntimeError("eval scheduler soft-list entry without a topology")
        if topology not in known:
            raise RuntimeError(f"soft-listed eval scheduler twin is not a corpus topology: {topology}")
        if topology in soft:
            raise RuntimeError(f"duplicate eval scheduler soft-list entry: {topology}")
        soft[topology] = _owned_until(
            entry, f"eval scheduler soft-list entry for {topology}",
            expired=lambda raw, owner: (
                f"eval scheduler twins of {topology} were soft until {raw} (owner {owner}): "
                "fix them under the gang scheduler, or extend the entry with a reason"
            ),
        )
    return soft


def _spec_groups_for(
    suite_dir, declarations, quarantined: tuple[str, ...] = ()
) -> list[tuple[str, str, list[str]]]:
    """Return a suite's groups after proving exact ownership of every spec.

    Ownership is what makes a group list load-bearing rather than documentation:
    each job runs only the files its group names, so an unlisted spec would
    silently never run in CI. Failing generation is the cheapest place to catch
    that. Quarantine is the one other home a spec may have, and it is exclusive:
    a spec is in exactly one gating group or in quarantine, never both, never
    neither.
    """
    actual = set(_spec_files_in(suite_dir))
    owners: dict[str, str] = {}
    groups = []
    slugs = set()
    for slug, label, stems in declarations:
        if not stems or slug in slugs:
            raise RuntimeError(f"invalid or duplicate KIND spec group: {slug}")
        slugs.add(slug)
        files = [f"{stem}_kind_test.go" for stem in stems]
        for filename in files:
            if filename in owners:
                raise RuntimeError(
                    f"KIND spec {filename} belongs to both {owners[filename]} and {slug}"
                )
            owners[filename] = slug
        groups.append((slug, label, files))

    mapped = set(owners)
    if both := sorted(mapped & set(quarantined)):
        raise RuntimeError(
            "quarantined KIND specs are still in a gating group: "
            + ", ".join(f"{name} in {owners[name]}" for name in both)
        )
    mapped |= set(quarantined)
    if missing := sorted(actual - mapped):
        raise RuntimeError(f"unclassified KIND specs: {', '.join(missing)}")
    if stale := sorted(set(owners) - actual):
        raise RuntimeError(f"mapped KIND specs do not exist: {', '.join(stale)}")
    return groups


def _kind_spec_groups() -> list[tuple[str, str, list[str]]]:
    return _spec_groups_for(
        KIND_SUITE_DIR, KIND_SPEC_GROUPS, tuple(_kind_quarantine_files())
    )


def _mc_kind_spec_groups() -> list[tuple[str, str, list[str]]]:
    return _spec_groups_for(MC_KIND_SUITE_DIR, MC_KIND_SPEC_GROUPS)


def _alfred_kind_spec_groups() -> list[tuple[str, str, list[str]]]:
    return _spec_groups_for(ALFRED_KIND_SUITE_DIR, ALFRED_KIND_SPEC_GROUPS)


def _kind_focus(files: list[str]) -> str:
    """A -ginkgo.focus-file regex matching exactly the named spec files."""
    return "/(" + "|".join(re.escape(filename) for filename in files) + ")$"


def _mc_kind_group() -> str:
    """The multi-cluster suite: its own group, one job per shard.

    Separate from the coordination group so its soft-fail status cannot muddy
    that lane's gating GitHub context, and so each lane's shards stay
    independently countable.

    Soft-fail while the suite settles: these are the first jobs to stand up more
    than one KIND cluster per job, so their memory envelope under the dind
    sidecar's limits is unproven, and a flaky hard gate on main is expensive to
    unwind.
    """
    plugins = _gke_kind_plugins(_kind_ci_image(f"test-{COMMIT[:8]}-{BUILD_NUMBER}"))
    steps = []
    for slug, label, files in _mc_kind_spec_groups():
        scheduler_env = {}
        if slug == "scheduling":
            scheduler_env = {"OME_TEST_SCHEDULER": EVAL_GANG_SCHEDULER, "OME_TEST_SCHEDULER_WORKERS": "2"}
        file_label = "file" if len(files) == 1 else "files"
        steps.append(
            {
                "label": f":test_tube: {label} ({len(files)} {file_label})",
                "key": f"kind-gke-mc-{slug}",
                "depends_on": "package-kind-test-image",
                "soft_fail": True,
                "env": {
                    "OME_KIND_TEST_BUNDLE": "/kind-test/multicluster-kind-test.tgz",
                    "OME_KIND_FOCUS": _kind_focus(files),
                    "OME_KIND_TIMEOUT": "60m",
                    **scheduler_env,
                },
                "agents": {"queue": GKE_QUEUE},
                "command": "sh .buildkite/run_kind_tests_gke.sh",
                "plugins": plugins,
            }
        )
    group = {
        "group": ":test_tube: Integration Tests (multi-cluster KIND on GKE)",
        "key": "integration-test-kind-mc-gke",
        "notify": [{"github_commit_status": {"context": "integration-test-kind-mc-gke"}}],
        "steps": steps,
    }
    return _yaml_block([group], 2)


def _alfred_kind_group() -> str:
    """The alfred lane: one job per group, each on its own KIND cluster.

    Soft-fail: the lane deploys an externally built component into the
    cluster and its real-scheduler scenarios pay one-minute cooldown
    floors, so its flake shape is unproven.
    """
    plugins = _gke_kind_plugins(_kind_ci_image(f"test-{COMMIT[:8]}-{BUILD_NUMBER}"))
    steps = []
    for slug, label, files in _alfred_kind_spec_groups():
        file_label = "file" if len(files) == 1 else "files"
        steps.append(
            {
                "label": f":test_tube: {label} ({len(files)} {file_label})",
                "key": f"kind-gke-alfred-{slug}",
                "depends_on": "package-kind-test-image",
                "soft_fail": True,
                "env": {
                    "OME_KIND_TEST_BUNDLE": "/kind-test/alfred-kind-test.tgz",
                    "OME_KIND_FOCUS": _kind_focus(files),
                    "OME_KIND_TIMEOUT": "75m",
                },
                "agents": {"queue": GKE_QUEUE},
                "command": "sh .buildkite/run_kind_tests_gke.sh",
                "retry": {"automatic": _infra_retry_automatic()},
                "plugins": plugins,
            }
        )
    group = {
        "group": ":test_tube: Integration Tests (alfred KIND on GKE)",
        "key": "integration-test-kind-alfred-gke",
        "notify": [{"github_commit_status": {"context": "integration-test-kind-alfred-gke"}}],
        "steps": steps,
    }
    return _yaml_block([group], 2)


def _gke_kind_group() -> str:
    """A small set of named semantic jobs, each with its own KIND cluster.

    Children carry the infra-retry preset. `kind create cluster` is the one
    part of these jobs that fails for reasons the commit cannot cause: on a
    contended agent the node containers take minutes to boot and the create is
    abandoned in BeforeSuite, before any spec runs. run_kind_tests_gke.sh
    translates exactly that shape to exit 125, so the preset picks it up while
    still never matching exit 1 — a red spec is not retryable here, by design.
    """
    plugins = _gke_kind_plugins(
        _kind_ci_image(f"test-{COMMIT[:8]}-{BUILD_NUMBER}")
    )
    children = []
    for slug, label, files in _kind_spec_groups():
        focus = _kind_focus(files)
        file_label = "file" if len(files) == 1 else "files"
        children.append(
            {
                "label": f":test_tube: {label} ({len(files)} {file_label})",
                "key": f"kind-gke-{slug}",
                "depends_on": "package-kind-test-image",
                "env": {"OME_KIND_FOCUS": focus},
                "agents": {"queue": GKE_QUEUE},
                "command": "sh .buildkite/run_kind_tests_gke.sh",
                "retry": {"automatic": _infra_retry_automatic()},
                "plugins": plugins,
            }
        )
    group = {
        "group": ":test_tube: Integration Tests (KIND on GKE)",
        "key": "integration-test-kind-gke",
        "notify": [{"github_commit_status": {"context": "integration-test-kind-gke"}}],
        "steps": children,
    }
    return _yaml_block([group], 2)


def _gke_kind_scheduler_group() -> str:
    """The KIND suite lanes once more under the gang scheduler (main only).

    Same bundle, same semantic groups, one extra env var: the suite installs
    ome-scheduler into its cluster and the fixture runtime selects it. Its own
    group and status context keep the default-scheduler gate untouched. Every
    twin gates the build except the soft-listed groups, which stay advisory
    until their entry expires (and all of them while
    GKE_KIND_SCHEDULER_HARD_FAIL=0).
    """
    plugins = _gke_kind_plugins(
        _kind_ci_image(f"test-{COMMIT[:8]}-{BUILD_NUMBER}")
    )
    soft = _kind_scheduler_soft_groups()
    children = []
    for slug, label, files in _kind_spec_groups():
        focus = _kind_focus(files)
        file_label = "file" if len(files) == 1 else "files"
        text = f":test_tube: {label} [{EVAL_GANG_SCHEDULER}] ({len(files)} {file_label})"
        if slug in soft:
            text += f" (advisory until {soft[slug]})"
        child = {
            "label": text,
            "key": f"kind-gke-{slug}-{EVAL_GANG_SCHEDULER}",
            "depends_on": "package-kind-test-image",
            "env": {"OME_KIND_FOCUS": focus, "OME_TEST_SCHEDULER": EVAL_GANG_SCHEDULER},
            "agents": {"queue": GKE_QUEUE},
            "command": "sh .buildkite/run_kind_tests_gke.sh",
            "retry": {"automatic": _infra_retry_automatic()},
            "plugins": plugins,
        }
        if slug in soft or not GKE_KIND_SCHEDULER_HARD_FAIL:
            child["soft_fail"] = True
        children.append(child)
    group = {
        "group": f":test_tube: Integration Tests (KIND on GKE, {EVAL_GANG_SCHEDULER})",
        "key": "integration-test-kind-scheduler-gke",
        "notify": [{"github_commit_status": {"context": "integration-test-kind-scheduler-gke"}}],
        "steps": children,
    }
    return _yaml_block([group], 2)


def _kind_quarantine_group() -> str:
    """The quarantined specs: one soft-fail job, or nothing when none are.

    Soft-fail by construction — a quarantined spec is a known defect with an
    owner, so gating a PR author on it only teaches people to retry. It still
    runs, and on the same bundle as the gating lane, so the failure stays
    visible and a fix shows up as green before the spec moves back.
    """
    files = _kind_quarantine_files()
    if not files:
        return ""
    plugins = _gke_kind_plugins(_kind_ci_image(f"test-{COMMIT[:8]}-{BUILD_NUMBER}"))
    file_label = "file" if len(files) == 1 else "files"
    owners = sorted({str(entry["owner"]) for entry in KIND_QUARANTINE})
    step = {
        "label": f":warning: Quarantined KIND specs ({len(files)} {file_label})",
        "key": "kind-gke-quarantine",
        "depends_on": "package-kind-test-image",
        "soft_fail": True,
        "env": {
            "OME_KIND_FOCUS": _kind_focus(files),
            "OME_KIND_QUARANTINE_OWNERS": ",".join(owners),
        },
        "agents": {"queue": GKE_QUEUE},
        "command": "sh .buildkite/run_kind_tests_gke.sh",
        "plugins": plugins,
    }
    group = {
        "group": ":warning: Quarantined KIND specs (soft-fail)",
        "key": "integration-test-kind-quarantine",
        "steps": [step],
    }
    return _yaml_block([group], 2)


def _gke_upgrade_identity_step() -> str:
    """The operator-upgrade identity gate: one deployed-mode KIND job.

    Only meaningful on main, where package_kind_test_image.sh resolves and
    bakes the previous commit's manager+agent images plus this commit's own
    canary-metrics image (see the comments there). run_omenative_upgrade_gke.sh
    skips loudly, without failing the step, whenever the manager+agent pair
    was not baked — so this step existing on a build does not by itself mean
    an upgrade was actually exercised.

    shared_model=True gives the runner and dind containers the same
    /ome-eval-model emptyDir the eval lane uses. The model fixture's
    hostPath PV is bind-mounted into the KIND node from dind's filesystem,
    not the runner's, so the suite's pre-cluster chmod of that path (see
    suite_test.go) only reaches the directory dind and KIND actually use
    when this volume is shared between the two containers.
    """
    plugins = _gke_kind_plugins(
        _kind_ci_image(f"test-{COMMIT[:8]}-{BUILD_NUMBER}"), shared_model=True
    )
    step = {
        "label": ":test_tube: Operator-upgrade identity (deployed KIND)",
        "key": "kind-gke-upgrade-identity",
        "depends_on": "package-kind-test-image",
        "env": {
            "OME_KIND_TEST_BUNDLE": "/kind-test/omenative-upgrade-kind-test.tgz",
            # Lets the runner derive this commit's canary-metrics ref
            # (${PROD_ECR_REGISTRY}/msl_infra/canary-metrics:<short-sha>)
            # the same way package_kind_test_image.sh resolved it.
            "PROD_ECR_REGISTRY": PROD_ECR_REGISTRY,
        },
        "agents": {"queue": GKE_QUEUE},
        "command": "sh .buildkite/run_omenative_upgrade_gke.sh",
        "plugins": plugins,
    }
    group = {
        "group": ":test_tube: Operator-Upgrade Identity (KIND on GKE)",
        "key": "integration-test-kind-upgrade",
        "notify": [{"github_commit_status": {"context": "integration-test-kind-upgrade"}}],
        "steps": [step],
    }
    return _yaml_block([group], 2)


def _package_kind_test_image_step() -> str:
    """Put the exact Go test bundle in a commit-tagged runner image for GKE."""
    deps = ["test"]
    if BRANCH == "main":
        deps += ["mirror-kind-ci", "mirror-canary-metrics", "mirror-ome-scheduler"]
    if GKE_ALFRED_ENABLED:
        # The alfred lane's image is pulled from the dev registry at package
        # time; the build must finish first and the script must treat a
        # missing image as a failure.
        deps.append("build-alfred")
    dependencies = "    depends_on:\n" + "".join(f'      - "{d}"\n' for d in deps)
    alfred_env = '      OME_ALFRED_IMAGE_REQUIRED: "1"\n' if GKE_ALFRED_ENABLED else ""
    return (
        '  - label: ":package: Package exact Go KIND runner"\n'
        '    key: "package-kind-test-image"\n'
        f"{_gh_status('package-kind-test-image', indent=4)}\n"
        f"{dependencies}"
        f"{_auto_retry(indent=4)}\n"
        "    env:\n"
        f'      AWS_REGION: "{AWS_REGION}"\n'
        f'      ECR_REGISTRY: "{ECR_REGISTRY}"\n'
        f'      PROD_ECR_REGISTRY: "{PROD_ECR_REGISTRY}"\n'
        f"{alfred_env}"
        "    secrets:\n"
        '      - "ECR_AUTH_TOKEN"\n'
        '      - "PROD_ECR_AUTH_TOKEN"\n'
        "    agents:\n"
        f'      queue: "{BUILD_QUEUE}"\n'
        "    command: |\n"
        "      sh .buildkite/package_kind_test_image.sh\n"
        "    plugins:\n"
        f"{_tw_docker_plugin(CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=6)}\n"
    )


def _eval_feature_is_canary(feature: str) -> bool:
    """Whether an eval feature folder holds canary-mode cases: the hand-written
    canary/ folder and the generated gen-canary-* families. They run a ladder
    per case, so they get the canary pod budget and wave count, and Prometheus
    for the analysis-gated cases among them."""
    return feature == "canary" or feature.startswith("gen-canary-")


def _eval_case_groups() -> list[tuple[str, str, list[str]]]:
    """Group every committed eval case by its stable feature/topology axes."""
    groups: dict[tuple[str, str], list[str]] = {}
    seen: set[str] = set()
    for path in sorted(EVAL_CASE_DIR.glob("*/p*.yaml")):
        case_id = path.stem
        if case_id in seen:
            raise RuntimeError(f"duplicate OMENative eval case id: {case_id}")
        seen.add(case_id)
        header = path.read_text().splitlines()[0].lstrip("#").strip()
        fields = dict(token.split("=", 1) for token in header.split() if "=" in token)
        topology = fields.get("topo")
        if not topology:
            raise RuntimeError(f"OMENative eval case has no topo field: {path}")
        groups.setdefault((path.parent.name, topology), []).append(case_id)

    def case_number(case_id: str) -> int:
        return int(case_id.removeprefix("p"))

    return [
        (feature, topology, sorted(cases, key=case_number))
        for (feature, topology), cases in sorted(groups.items())
    ]


def _eval_case_header_fields(case_id: str) -> dict[str, str]:
    """The case's line-1 key=value fields (last wins), {} when the case is
    not exactly one committed file."""
    matches = sorted(EVAL_CASE_DIR.glob(f"*/{case_id}.yaml"))
    if len(matches) != 1:
        return {}
    header = matches[0].read_text().splitlines()[0].lstrip("#").strip()
    return dict(token.split("=", 1) for token in header.split() if "=" in token)


def _eval_case_runs_exclusively(fields: dict[str, str]) -> bool:
    """batch.py's rule for a case it runs on its own after the pool drains."""
    fault = fields.get("fault", "").split(":", 1)[0]
    return (fault in EVAL_EXCLUSIVE_FAULT_KINDS or fields.get("via") == EVAL_EXCLUSIVE_VIA
            or fields.get("pool") == EVAL_EXCLUSIVE_POOL)


def _eval_exclusive_case_minutes(feature: str, cases: list[str]) -> dict[str, float]:
    """{case: expected minutes} for the cases of a lane that batch.py runs on
    their own, at the family's figure from EVAL_EXCLUSIVE_CASE_MINUTES."""
    minutes = EVAL_EXCLUSIVE_CASE_MINUTES.get(feature, EVAL_EXCLUSIVE_CASE_MINUTES_DEFAULT)
    return {
        case_id: minutes
        for case_id in cases
        if _eval_case_runs_exclusively(_eval_case_header_fields(case_id))
    }


def _eval_lane_variants(cases: list[str]) -> list[tuple[str, dict[str, str], list[str]]]:
    """[(variant slug, extra env, cases)] for one feature/topology group: the
    plain lane with every case that reads the default deploy, and, when any
    case needs the force-delete policy, the policy-on twin with exactly
    those cases."""
    policy = [c for c in cases if _eval_case_header_fields(c).get("needs") == EVAL_FORCE_DELETE_NEED]
    plain = [c for c in cases if c not in policy]
    variants: list[tuple[str, dict[str, str], list[str]]] = []
    if plain:
        variants.append(("", {}, plain))
    if policy:
        variants.append((EVAL_FORCE_DELETE_SLUG, {EVAL_FORCE_DELETE_ENV: "1"}, policy))
    return variants


def _eval_lanes() -> list[tuple[str, str, str, dict[str, str], list[str]]]:
    """Every eval lane before sharding: (feature, topology, variant, env,
    cases), each committed case in exactly one."""
    return [
        (feature, topology, variant, env, cases)
        for feature, topology, group in _eval_case_groups()
        for variant, env, cases in _eval_lane_variants(group)
    ]


def _eval_case_peak_pods(case_id: str) -> int | None:
    """Return the case's declared peak pod count, including runtime workers."""
    matches = sorted(EVAL_CASE_DIR.glob(f"*/{case_id}.yaml"))
    if len(matches) != 1:
        return None
    text = matches[0].read_text()
    header = text.splitlines()[0].lstrip("#").strip()
    fields = dict(token.split("=", 1) for token in header.split() if "=" in token)
    topology = fields.get("topo", "")
    documents = re.split(r"(?m)^---\s*$", text)
    isvc = next(
        (doc for doc in documents if re.search(r"(?m)^kind:\s*InferenceService\s*$", doc)),
        "",
    )
    if not isvc:
        return None

    total = 0
    for component, suffix in (("engine", "e"), ("decoder", "d"), ("router", "r")):
        replicas = int(fields.get(f"ceil_{suffix}", fields.get(f"N{suffix}", "0")))
        if replicas == 0:
            continue
        block_match = re.search(
            rf"(?ms)^  {component}:\s*\n(.*?)(?=^  [A-Za-z][A-Za-z0-9_-]*:\s*(?:\n|$)|\Z)",
            isvc,
        )
        block = block_match.group(1) if block_match else ""
        inline = re.search(r"(?m)^    worker:\s*\{[^}]*\bsize:\s*(\d+)", block)
        nested = re.search(
            r"(?m)^    worker:\s*\n(?:^      .*\n)*?^      size:\s*(\d+)", block
        )
        worker_size = int((inline or nested).group(1)) if (inline or nested) else 0
        if topology.endswith("-mn") and component != "router" and worker_size == 0:
            return None
        total += replicas * (1 + worker_size)
    return total


def _split_eval_cases(
    cases: list[str],
    case_peaks: dict[str, int | None],
    concurrency: int,
    pod_budget: int,
    target_waves: int,
    exclusive_minutes: dict[str, float] | None = None,
    exclusive_budget: float | None = None,
) -> list[list[str]]:
    """Pack cases into feasible scheduler waves, then bound waves per job.

    A case in `exclusive_minutes` is one batch.py runs on its own after the
    pool drains, so it is a wave of its own and counts against the shard's
    `exclusive_budget` of minutes instead of its `target_waves` of pool
    waves; a shard always carries at least one such case, so a case rated
    above the budget still runs, alone."""
    if concurrency < 1 or pod_budget < 1 or target_waves < 1:
        raise RuntimeError(
            "OMENative eval concurrency, pod budget, and target waves must be positive"
        )
    exclusive_minutes = exclusive_minutes or {}
    if exclusive_minutes and (exclusive_budget is None or exclusive_budget <= 0):
        raise RuntimeError("OMENative eval exclusive-case minutes per shard must be positive")
    if not cases:
        return []

    # batch.py runs an unknown or over-budget case exclusively, so either shape
    # consumes one full pod-capacity wave for planning purposes, as does a
    # case it runs exclusively by its fault.
    def wave_weight(case_id: str) -> int:
        peak = case_peaks.get(case_id)
        if peak is None or case_id in exclusive_minutes:
            return pod_budget
        return min(max(peak, 0), pod_budget)

    weights = {case_id: wave_weight(case_id) for case_id in cases}
    order = {case_id: index for index, case_id in enumerate(cases)}
    waves: list[list[str]] = []
    pod_loads: list[int] = []
    for case_id in sorted(cases, key=lambda item: (-weights[item], order[item])):
        wave = next(
            (
                index
                for index, members in enumerate(waves)
                if len(members) < concurrency
                and pod_loads[index] + weights[case_id] <= pod_budget
            ),
            None,
        )
        if wave is None:
            wave = len(waves)
            waves.append([])
            pod_loads.append(0)
        waves[wave].append(case_id)
        pod_loads[wave] += weights[case_id]

    shards: list[list[str]] = []
    pool_waves = 0
    used_minutes = 0.0
    for wave in waves:
        # An exclusive case fills its wave, so the wave's first member says
        # which bound the wave counts against.
        minutes = exclusive_minutes.get(wave[0])
        if minutes is None:
            fits = bool(shards) and pool_waves < target_waves
        else:
            fits = bool(shards) and (
                used_minutes == 0 or used_minutes + minutes <= exclusive_budget
            )
        if not fits:
            shards.append([])
            pool_waves = 0
            used_minutes = 0.0
        shards[-1].extend(wave)
        if minutes is None:
            pool_waves += 1
        else:
            used_minutes += minutes
    for shard in shards:
        shard.sort(key=order.get)
    return shards


def _eval_mode() -> str:
    """EVAL_MODE, refused when it names neither a mode nor the kind of the
    trailer's selection; off while the kill switch is on."""
    known = EVAL_MODE in EVAL_MODES or (
        EVAL_SELECTION is not None and EVAL_MODE == EVAL_SELECTION[0]
    )
    if not known:
        raise RuntimeError(
            "OME_EVAL_MODE or the Eval: trailer must name one of "
            + ", ".join(EVAL_MODES)
            + " (the trailer also families=<folders> or cases=<ids>), not "
            + repr(EVAL_MODE)
        )
    return EVAL_MODE if GKE_EVAL_ENABLED else "off"


def _eval_on() -> bool:
    """Whether the build carries the eval image step and the eval group."""
    return _eval_mode() != "off"


class EvalPacking(NamedTuple):
    """The lane packing knobs, read from the environment at generation."""

    pod_budget: str | None
    canary_pod_budget: str | None
    concurrency: int
    target_waves: int
    canary_target_waves: int
    exclusive_budget: float


def _eval_packing() -> EvalPacking:
    return EvalPacking(
        pod_budget=(
            os.environ.get("GKE_EVAL_POD_BUDGET")
            or os.environ.get("GKE_EVAL_EXCLUSIVE_PODS")
        ),
        canary_pod_budget=os.environ.get("GKE_EVAL_CANARY_POD_BUDGET"),
        concurrency=int(os.environ.get("GKE_EVAL_CASE_CONCURRENCY", "4")),
        target_waves=int(os.environ.get("GKE_EVAL_TARGET_WAVES", "4")),
        canary_target_waves=int(os.environ.get("GKE_EVAL_CANARY_TARGET_WAVES", "2")),
        # The expected minutes of exclusive cases one shard runs back to back
        # after its pool: with the bring-up and the pool waves this keeps every
        # lane expected well under the hour, and two cases of ten minutes or
        # more never share a shard.
        exclusive_budget=float(os.environ.get("GKE_EVAL_EXCLUSIVE_MINUTES", "20")),
    )


def _eval_max_concurrency() -> int:
    """The eval lanes allowed to run at once, from GKE_EVAL_MAX_CONCURRENCY;
    anything but a positive integer is refused."""
    raw = os.environ.get(EVAL_MAX_CONCURRENCY_VAR, "").strip() or str(EVAL_MAX_CONCURRENCY_DEFAULT)
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value < 1:
        raise RuntimeError(EVAL_MAX_CONCURRENCY_VAR + " must be a positive integer, not " + repr(raw))
    return value


def _eval_lane_shards(
    feature: str, topology: str, cases: list[str], scheduler: str, packing: EvalPacking
) -> tuple[list[list[str]], str, int | None, list[str]]:
    """Today's packing of one lane under one scheduler: (shards, pod budget,
    workers, skipped cases). The gang twin of a multi-node lane keeps the
    cases within its node cap, names the rest and is sized to the kept ones;
    no shard at all means nothing fit."""
    canary = _eval_feature_is_canary(feature)
    pod_budget = packing.pod_budget or ("35" if canary else "64")
    if canary and packing.canary_pod_budget is not None:
        pod_budget = packing.canary_pod_budget
    target_waves = packing.canary_target_waves if canary else packing.target_waves
    peaks = {case_id: _eval_case_peak_pods(case_id) for case_id in cases}
    workers = None
    skipped: list[str] = []
    if scheduler and topology.endswith("-mn"):
        # Whole-node gang placement: every pod of a running case needs its
        # own worker, so the node count is the pod budget for this twin.
        kept = [c for c in cases if peaks.get(c) is not None and peaks[c] <= GKE_EVAL_SCHEDULER_MAX_WORKERS]
        skipped = [c for c in cases if c not in kept]
        cases = kept
        if not cases:
            return [], pod_budget, None, skipped
        workers = max(2, max(peaks[c] for c in cases))
        pod_budget = str(workers)
    shards = _split_eval_cases(
        cases,
        peaks,
        packing.concurrency,
        int(pod_budget),
        target_waves,
        _eval_exclusive_case_minutes(feature, cases),
        packing.exclusive_budget,
    )
    return shards, pod_budget, workers, skipped


class EvalRound(NamedTuple):
    """The recorded full round a targeted build reads: its build number and
    verdicts, or the reason there is none in the few words the label shows."""

    build: str
    rows: tuple
    reason: str


class _StopAtRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect is returned to the caller instead of followed, so the
    token sent to the API never travels to the storage it redirects to."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _http_get(url: str, headers: dict[str, str]) -> tuple[int, dict[str, str], bytes]:
    """One GET with redirects not followed: (status, response headers,
    body). The generator's only network access; the round lookup calls it
    through EVAL_ROUND_FETCH, which tests replace with a fake."""
    request = urllib.request.Request(url, headers=headers)
    opener = urllib.request.build_opener(_StopAtRedirect)
    try:
        with opener.open(request, timeout=EVAL_ROUND_FETCH_TIMEOUT_SECONDS) as response:
            return response.status, dict(response.headers.items()), response.read()
    except urllib.error.HTTPError as error:
        body = error.read() if error.fp is not None else b""
        return error.code, dict(error.headers.items()), body


EVAL_ROUND_FETCH = _http_get


def _header(headers: dict[str, str], name: str) -> str:
    return next((value for key, value in headers.items() if key.lower() == name.lower()), "")


def _eval_api_json(fetch, url: str, headers: dict[str, str]):
    status, _, body = fetch(url, headers)
    if status != 200:
        raise RuntimeError(f"HTTP {status} from {url.split('?')[0]}")
    return json.loads(body.decode("utf-8"))


def _eval_download(fetch, url: str, headers: dict[str, str]) -> bytes:
    """The artifact behind an API download URL: the API answers with a
    redirect to its storage, which is fetched without the token."""
    for _ in range(4):
        status, response_headers, body = fetch(url, headers)
        if status in (301, 302, 303, 307, 308):
            url, headers = _header(response_headers, "Location"), {}
            if not url:
                raise RuntimeError("the artifact download redirected without a location")
            continue
        if status == 200:
            return body
        raise RuntimeError(f"HTTP {status} from the artifact download")
    raise RuntimeError("too many redirects on the artifact download")


def _eval_round_lookup(fetch) -> EvalRound:
    """The newest finished main build of the pipeline (passed or failed,
    never canceled or running) whose meta-data marks it as a recorded round
    and whose recording step attached the verdict file."""
    token = os.environ.get(EVAL_RESULTS_TOKEN_VAR, "").strip()
    if not token:
        return EvalRound("", (), "no results token")
    organization = os.environ.get("BUILDKITE_ORGANIZATION_SLUG", "").strip()
    pipeline = (
        os.environ.get(EVAL_RESULTS_PIPELINE_VAR, "").strip()
        or os.environ.get("BUILDKITE_PIPELINE_SLUG", "").strip()
    )
    if not organization or not pipeline:
        raise RuntimeError("BUILDKITE_ORGANIZATION_SLUG and BUILDKITE_PIPELINE_SLUG name the pipeline to read")
    headers = {"Authorization": "Bearer " + token, "Accept": "application/json"}
    quote = urllib.parse.quote
    builds_url = f"{EVAL_ROUND_API}/organizations/{quote(organization)}/pipelines/{quote(pipeline)}/builds"
    query = urllib.parse.urlencode([
        ("branch", "main"),
        ("state[]", "passed"),
        ("state[]", "failed"),
        (f"meta_data[{eval_verdicts.ROUND_META_KEY}]", eval_verdicts.ROUND_META_VALUE),
        ("per_page", str(EVAL_ROUND_PAGE)),
    ])
    builds = _eval_api_json(fetch, builds_url + "?" + query, headers)
    recorded = [
        build for build in builds
        if build.get("branch") == "main"
        and build.get("state") in ("passed", "failed")
        and (build.get("meta_data") or {}).get(eval_verdicts.ROUND_META_KEY) == eval_verdicts.ROUND_META_VALUE
    ]
    recorded.sort(key=lambda build: int(build["number"]), reverse=True)
    for build in recorded:
        number = str(build["number"])
        for job in build.get("jobs") or []:
            if job.get("step_key") != EVAL_VERDICTS_STEP_KEY:
                continue
            artifacts_url = f"{builds_url}/{number}/jobs/{quote(str(job['id']))}/artifacts"
            for artifact in _eval_api_json(fetch, artifacts_url, headers):
                if artifact.get("filename") == eval_verdicts.VERDICTS_FILE and artifact.get("download_url"):
                    text = _eval_download(fetch, artifact["download_url"], headers).decode("utf-8")
                    return EvalRound(number, tuple(eval_verdicts.read_verdicts(text)), "")
    return EvalRound("", (), "no recorded round")


@functools.lru_cache(maxsize=None)
def _eval_round() -> EvalRound:
    """The round targeted mode reads, looked up once per generation. A
    lookup that fails for any reason leaves targeted on smoke plus the
    trailer's selection and says why on stderr; it never fails generation."""
    try:
        found = _eval_round_lookup(EVAL_ROUND_FETCH)
    except Exception as error:
        sys.stderr.write(f"eval targeted: round lookup failed: {type(error).__name__}: {error}\n")
        return EvalRound("", (), "round lookup failed")
    if found.reason:
        sys.stderr.write(f"eval targeted: {found.reason}; running smoke plus the trailer's selection\n")
    else:
        sys.stderr.write(f"eval targeted: reading round {found.build} ({len(found.rows)} verdicts)\n")
    return found


def _eval_sample_percent() -> Fraction:
    """OME_EVAL_SAMPLE_PERCENT as an exact number from 0 to 100; unset or
    empty means the default, anything else is refused."""
    raw = os.environ.get(EVAL_SAMPLE_PERCENT_VAR, "")
    if not raw.strip():
        return Fraction(EVAL_SAMPLE_PERCENT_DEFAULT)
    try:
        percent = Fraction(raw.strip())
    except (ValueError, ZeroDivisionError):
        percent = None
    if percent is None or percent < 0 or percent > 100:
        raise RuntimeError(f"{EVAL_SAMPLE_PERCENT_VAR} must be a percentage from 0 to 100, not {raw!r}")
    return percent


def _eval_round_selection(
    groups: list[tuple[str, str, list[str]]], found: EvalRound, percent: Fraction
) -> dict[tuple[str, str, str], set[str]]:
    """{(feature, topology, scheduler): cases} a targeted build runs from a
    recorded round: every lane that failed, under the scheduler it ran in
    (the default one when that twin is off); every corpus case the round
    has no row for; every case the trailer names; and `percent` of the
    passing lanes, rounded up, drawn with the commit as seed so a build is
    reproducible. A lane the harness skipped or parked
    (eval_verdicts.is_set_aside) is neither a failure nor a passing lane,
    and a row naming a case absent from the corpus is ignored."""
    homes = {case: (feature, topology) for feature, topology, cases in groups for case in cases}
    chosen: dict[tuple[str, str, str], set[str]] = {}
    seen: set[str] = set()
    passing: set[tuple[str, str]] = set()
    for row in found.rows:
        if row.case not in homes:
            continue
        seen.add(row.case)
        if eval_verdicts.is_set_aside(row.verdict):
            continue
        scheduler = row.scheduler if row.scheduler in EVAL_SCHEDULERS else ""
        if row.verdict == eval_verdicts.PASS:
            passing.add((row.case, scheduler))
        else:
            chosen.setdefault(homes[row.case] + (scheduler,), set()).add(row.case)
    for case, home in homes.items():
        if case not in seen:
            chosen.setdefault(home + ("",), set()).add(case)
    if EVAL_SELECTION is not None:
        for home, cases in _eval_named_cases(groups).items():
            chosen.setdefault(home + ("",), set()).update(cases)
    candidates = sorted(
        pair for pair in passing if pair[0] not in chosen.get(homes[pair[0]] + (pair[1],), ())
    )
    count = math.ceil(len(candidates) * percent / 100)
    for case, scheduler in random.Random(COMMIT).sample(candidates, count):
        chosen.setdefault(homes[case] + (scheduler,), set()).add(case)
    return chosen


def _eval_named_families(groups: list[tuple[str, str, list[str]]]) -> set[str]:
    """The corpus folders the trailer's families= names; an unknown or
    empty list is refused."""
    _, names = EVAL_SELECTION
    known = {feature for feature, _, _ in groups}
    unknown = [name for name in names if name not in known]
    if not names:
        raise RuntimeError("the Eval: trailer names no family")
    if unknown:
        raise RuntimeError("the Eval: trailer names no corpus family: " + ", ".join(unknown))
    return set(names)


def _eval_named_cases(groups: list[tuple[str, str, list[str]]]) -> dict[tuple[str, str], set[str]]:
    """The trailer's selection as {(feature, topology): cases}: cases= as
    named, families= as every case of the folders; an unknown or empty list
    is refused."""
    kind, names = EVAL_SELECTION
    if kind == "families":
        wanted = _eval_named_families(groups)
        return {(f, t): set(cases) for f, t, cases in groups if f in wanted}
    homes = {case_id: (f, t) for f, t, cases in groups for case_id in cases}
    unknown = [name for name in names if name not in homes]
    if not names:
        raise RuntimeError("the Eval: trailer names no case")
    if unknown:
        raise RuntimeError("the Eval: trailer names no corpus case: " + ", ".join(unknown))
    chosen: dict[tuple[str, str], set[str]] = {}
    for name in names:
        chosen.setdefault(homes[name], set()).add(name)
    return chosen


def _eval_selection() -> list[tuple[str, str, list[str], str]]:
    """What the eval group runs under the build's mode, as (feature,
    topology, cases, scheduler) in emission order: scheduler-major, then the
    corpus order of the lanes. The one place a mode chooses cases;
    _eval_children packs and emits whatever comes back.

    full      every lane on every scheduler
    smoke     each lane's first shard of today's packing, default scheduler
    targeted  from the newest recorded full round: every lane that failed
              under its scheduler, every case the round never saw and a
              sample of the passing lanes, plus the trailer's selection;
              smoke plus the trailer's selection while no round can be read
    families  every lane of the named folders on every scheduler
    cases     exactly the named cases in their lanes, default scheduler
    """
    mode = _eval_mode()
    if mode == "off":
        return []
    groups = _eval_case_groups()
    if mode in ("full", "families"):
        features = {feature for feature, _, _ in groups}
        if mode == "families":
            features = _eval_named_families(groups)
        return [
            (feature, topology, list(cases), scheduler)
            for scheduler in EVAL_SCHEDULERS
            for feature, topology, cases in groups
            if feature in features
        ]
    if mode == "targeted":
        percent = _eval_sample_percent()
        found = _eval_round()
        if not found.reason:
            by_lane = _eval_round_selection(groups, found, percent)
            return [
                (feature, topology, [c for c in cases if c in by_lane[(feature, topology, scheduler)]], scheduler)
                for scheduler in EVAL_SCHEDULERS
                for feature, topology, cases in groups
                if by_lane.get((feature, topology, scheduler))
            ]
    chosen: dict[tuple[str, str], set[str]] = {}
    if mode in ("smoke", "targeted"):
        packing = _eval_packing()
        for feature, topology, cases in groups:
            # The group's first lane is its plain one whenever it has plain
            # cases; that lane's first shard is the shape's smoke.
            lane = _eval_lane_variants(cases)[0][2]
            shards = _eval_lane_shards(feature, topology, lane, "", packing)[0]
            chosen[(feature, topology)] = set(shards[0])
    if mode == "cases" or (mode == "targeted" and EVAL_SELECTION is not None):
        for lane_key, cases in _eval_named_cases(groups).items():
            chosen.setdefault(lane_key, set()).update(cases)
    return [
        (feature, topology, [c for c in cases if c in chosen[(feature, topology)]], "")
        for feature, topology, cases in groups
        if chosen.get((feature, topology))
    ]


def _eval_children() -> list[tuple[str, str, dict]]:
    """Every job of the deployed OMENative eval as (scheduler, feature,
    step), in upload order: scheduler-major, then the named feature/topology
    lanes with their shards. The mode's selection says which lanes and
    cases; a selected case needing the force-delete policy still runs in
    its lane's policy-on twin."""
    plugins = _gke_kind_plugins(_kind_ci_image(f"eval-{COMMIT[:8]}"), shared_model=True)
    packing = _eval_packing()
    cap = _eval_max_concurrency()
    soft = _eval_scheduler_soft_topologies()
    soft_features = _eval_soft_features()
    children = []
    for feature, topology, selected, scheduler in _eval_selection():
      for variant, variant_env, cases in _eval_lane_variants(selected):
        slug = f"{feature}-{topology}".replace("_", "-")
        if scheduler:
            slug += "-" + scheduler.replace("_", "-")
        if variant:
            slug += "-" + variant
        variant_part = f" [{variant}]" if variant else ""
        prometheus = "true" if _eval_feature_is_canary(feature) or feature == "reject" else "false"
        subshards, pod_budget, workers, skipped = _eval_lane_shards(
            feature, topology, cases, scheduler, packing
        )
        if not subshards:
            # Nothing fits the cap: say so in the build instead of dropping
            # the twin silently.
            children.append((scheduler, feature, {
                "label": f":information_source: {feature} / {topology} [{scheduler}]{variant_part} needs more than {GKE_EVAL_SCHEDULER_MAX_WORKERS} nodes ({len(skipped)} cases skipped)",
                "key": f"omenative-e2e-{slug}-skipped",
                "command": "echo 'whole-node gang placement: cases above the node cap are not run in this twin: "
                + " ".join(skipped) + "'",
                "agents": {"queue": BUILD_QUEUE},
            }))
            continue
        for index, shard_cases in enumerate(subshards, start=1):
            part = f" [{index}/{len(subshards)}]" if len(subshards) > 1 else ""
            key_part = f"-{index}-of-{len(subshards)}" if len(subshards) > 1 else ""
            sched_part = f" [{scheduler}]" if scheduler else ""
            env = {
                "OME_KIND_PROMETHEUS_ENABLED": prometheus,
                "OME_EVAL_CASE_CONCURRENCY": str(packing.concurrency),
                "OME_EVAL_POD_BUDGET": pod_budget,
                "PROD_ECR_REGISTRY": PROD_ECR_REGISTRY,
                **variant_env,
            }
            if scheduler:
                env["OME_EVAL_SCHEDULER"] = scheduler
            if workers is not None:
                env["OME_EVAL_KIND_WORKERS"] = str(workers)
            if skipped:
                env["OME_EVAL_SCHEDULER_SKIPPED"] = " ".join(skipped)
            child = {
                "label": f":test_tube: {feature} / {topology}{part}{sched_part}{variant_part} ({len(shard_cases)} cases)",
                "key": f"omenative-e2e-{slug}{key_part}",
                "depends_on": "package-kind-eval-image",
                "timeout_in_minutes": 180,
                # The lanes of every build share one group, so together they
                # hold at most the cap in GKE agent pods; the rest wait in order.
                "concurrency_group": EVAL_CONCURRENCY_GROUP,
                "concurrency": cap,
                "env": env,
                "agents": {"queue": GKE_QUEUE},
                "command": "sh .buildkite/run_omenative_eval_gke.sh "
                + " ".join([feature, topology, *shard_cases]),
                # The runner leaves with 125 when the shard never reached its
                # cases (cluster bring-up or the OME deploy on a contended
                # agent), so the infra preset reruns exactly that and never a
                # red case.
                "retry": {"automatic": _infra_retry_automatic()},
                "plugins": plugins,
            }
            if feature in soft_features:
                child["label"] += f" (advisory until {soft_features[feature]})"
            elif scheduler and topology in soft:
                child["label"] += f" (advisory until {soft[topology]})"
            if feature in soft_features or (
                scheduler and (topology in soft or not GKE_EVAL_SCHEDULER_HARD_FAIL)
            ):
                child["soft_fail"] = True
            children.append((scheduler, feature, child))
    return children


def _eval_batches(
    children: list[tuple[str, str, dict]], limit: int = UPLOAD_JOB_LIMIT
) -> list[list[tuple[str, str, dict]]]:
    """Cut the eval jobs, order kept, into runs of at most `limit`: one per
    scheduler dimension, and a dimension above the limit into near-equal
    parts cut between families (a family alone above the limit is cut
    between its jobs)."""
    if limit < 1:
        raise RuntimeError("the jobs per pipeline upload must be positive")
    batches: list[list[tuple[str, str, dict]]] = []
    for _, dimension in itertools.groupby(children, key=lambda item: item[0]):
        jobs = list(dimension)
        parts = -(-len(jobs) // limit)
        target = -(-len(jobs) // parts)
        current: list[tuple[str, str, dict]] = []
        for _, family in itertools.groupby(jobs, key=lambda item: item[1]):
            members = list(family)
            if current and len(current) + len(members) > target:
                batches.append(current)
                current = []
            while len(members) > limit:
                batches.append(members[:limit])
                members = members[limit:]
            current.extend(members)
        if current:
            batches.append(current)
    return batches


def _gke_eval_groups() -> list[dict]:
    """The eval jobs as group steps, one per pipeline upload. A single group
    keeps the plain key; several are numbered, and each label names its
    scheduler and, when the scheduler's jobs span more than one group, the
    families it carries. Every label names the mode unless it is full."""
    children = _eval_children()
    batches = _eval_batches(children, UPLOAD_JOB_LIMIT)
    per_scheduler = {
        scheduler: len(list(jobs))
        for scheduler, jobs in itertools.groupby(children, key=lambda item: item[0])
    }
    groups = []
    for index, batch in enumerate(batches, start=1):
        key = "omenative-e2e-kind-gke"
        label = ":test_tube: OMENative YAML E2E (KIND on GKE)" + _eval_mode_label()
        if len(batches) > 1:
            scheduler = batch[0][0]
            part = scheduler or "default scheduler"
            if len(batch) < per_scheduler[scheduler]:
                first, last = batch[0][1], batch[-1][1]
                part += f", {first}" if first == last else f", {first} to {last}"
            key += f"-{index}-of-{len(batches)}"
            label += f" [{index}/{len(batches)}: {part}]"
        groups.append({
            "group": label,
            "key": key,
            "notify": [{"github_commit_status": {"context": key}}],
            "steps": [child for _, _, child in batch],
        })
    return groups


def _eval_mode_label() -> str:
    """The group label's suffix naming what the mode runs; full needs none."""
    mode = _eval_mode()
    if mode == "full":
        return ""
    if mode == "targeted":
        found = _eval_round()
        if found.reason:
            label = " (targeted: smoke, " + found.reason + ")"
        else:
            runs = sum(len(cases) for _, _, cases, _ in _eval_selection())
            label = f" (targeted from round {found.build}: {runs} cases)"
        if EVAL_SELECTION is not None:
            label += " (plus " + EVAL_SELECTION[0] + ": " + ", ".join(EVAL_SELECTION[1]) + ")"
        return label
    if mode in EVAL_SELECTION_KINDS:
        return " (" + mode + ": " + ", ".join(EVAL_SELECTION[1]) + ")"
    return " (" + mode + ")"


def _gke_eval_group() -> str:
    """Every eval group rendered one after another, as the uploads carry them."""
    return "\n".join(_yaml_block([group], 2) for group in _gke_eval_groups())


def _drift_check_step(key: str, label: str, make_target: str, apt_packages: str = "") -> str:
    """Emit a CI step that runs `make <target>` then `git diff --exit-code`.

    `git diff --exit-code` returns non-zero if the make target produced
    any tracked-file changes, which fails the BK step — that's the whole
    point: catches PRs that forgot to regenerate (fmt, tidy, manifests,
    generate, dep-crds) before pushing.

    apt_packages: space-separated list passed to `apt-get install` for
    targets that need extra system tooling (e.g. `make manifests` needs
    `jq` for CRD post-processing). Empty (the common case) skips apt.

    Retries on any non-zero exit: every regen target installs its tooling
    with `go install`, so the step is network-heavy before it checks
    anything, and a proxy.golang.org blip reads as drift. `_infra_retry`
    is too narrow — a failed module fetch exits 1 or 2, never 125. Drift
    itself is deterministic and so survives the retries, which is what
    makes the broader preset safe on a merge gate here.

    The block is indented for direct concatenation onto `ci_steps()`'s
    dedented output (steps at 2-space indent under `steps:`).
    """
    apt_block = ""
    if apt_packages:
        apt_block = (
            f"      apt-get update\n"
            f"      apt-get install -y --no-install-recommends {apt_packages}\n"
        )
    return (
        f'  - label: "{label}"\n'
        f'    key: "{key}"\n'
        f"{_gh_status(key, indent=4)}\n"
        f"{_auto_retry(indent=4)}\n"
        f"    command: |\n"
        f"{apt_block}"
        f"      make {make_target}\n"
        f"      git diff --exit-code\n"
        f"    agents:\n"
        f'      queue: "{DEFAULT_QUEUE}"\n'
        f"    plugins:\n"
        f"{_tw_docker_plugin(CI_IMAGE)}\n"
    )


# ---------------------------------------------------------------------------
# CI pipeline (runs on every commit / PR)
# ---------------------------------------------------------------------------


def ci_steps() -> str:
    base = textwrap.dedent(
        f"""\
        steps:
          - label: ":github: Git Author Email"
            key: "git-author-email"
{_gh_status("git-author-email")}
{_infra_retry()}
            command: |
              bash .buildkite/check_git_author_email.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":lock: Internal Identifiers"
            key: "internal-tokens"
{_gh_status("internal-tokens")}
{_infra_retry()}
            command: |
              bash .buildkite/check_internal_tokens.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":abc: Spelling"
            key: "spelling"
{_gh_status("spelling")}
{_infra_retry()}
            command: |
              bash .buildkite/check_spelling.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":golangci-lint: Lint"
            key: "lint"
{_gh_status("lint")}
{_infra_retry()}
            command: |
              make ci-lint
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":golang: Vet"
            key: "vet"
{_gh_status("vet")}
{_infra_retry()}
            command: |
              make vet
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":test_tube: Unit Tests + Coverage"
            key: "test"
{_gh_status("test")}
{_infra_retry()}
            command: |
              sh .buildkite/run_tests.sh
            artifact_paths:
              - ".buildkite/artifacts/omenative-kind-test.tgz"
              - ".buildkite/artifacts/multicluster-kind-test.tgz"
              - ".buildkite/artifacts/omenative-upgrade-kind-test.tgz"
              - ".buildkite/artifacts/alfred-kind-test.tgz"
              - ".buildkite/artifacts/kind"
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":lock: Rollout Behavior Lock"
            key: "rollout-behavior-lock"
{_gh_status("rollout-behavior-lock")}
{_infra_retry()}
            # The rollout engine's behavior lock as one visible status. It
            # runs the table checkers (every history sidecar entry selects
            # a real grid cell; every coverage name and `// Cells:` id
            # resolves to a Go test that exists; under `frozen: true` no
            # decided cell is without one) and then the lock's own test
            # packages: the machine tables, the replay goldens with their
            # arrow and determinism checks, and the engine packages the
            # decided-cell tests live in. The unit-test lane runs the same
            # packages for coverage; this step exists so a golden diff or
            # an uncovered cell reports under its own name instead of as
            # one line among the unit tests.
            #
            # Same image as the unit-test lane, which is a Go image without
            # python3; the checkers are python3 + PyYAML.
            command: |
              apt-get update
              apt-get install -y --no-install-recommends python3 python3-yaml
              make rollout-behavior-lock MACHINE=instance
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":test_tube: Integration Tests"
            key: "integration-test"
{_gh_status("integration-test")}
{_infra_retry()}
            # No depends_on: integration tests share no state with unit
            # tests (each step bootstraps its own envtest + xet under a
            # fresh container) so they run in parallel with everything
            # else in the CI fan-out.
            command: |
              sh .buildkite/run_integration_tests.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":racing_car: Perf Tests"
            key: "perf-test"
{_gh_status("perf-test")}
            # The InferenceService scale harness (envtest + fake executor)
            # plus the perf-harness unit tests. Separate from integration-test
            # because the perf packages are plain testing.T (not Ginkgo, so
            # they can't take -ginkgo.trace) and the harness is slower. Runs in
            # parallel with everything else; bootstraps its own envtest + xet.
            #
            # soft_fail: the baseline rangespec is still placeholder/uncalibrated
            # (see configs/baseline/rangespec.yaml), so a threshold breach must
            # not block merges yet — the step's value today is producing the
            # numbers + exercising the harness. Flip to hard fail once the
            # rangespec is calibrated on CI hardware.
            soft_fail: true
            command: |
              sh .buildkite/run_perf_tests.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          - label: ":test_tube: Scheduler Module"
            key: "scheduler-test"
{_gh_status("scheduler-test")}
{_infra_retry()}
            # ome-scheduler (scheduler/go.mod) is a separate Go module, so
            # the root pipeline's `go test ./...` never reaches it. This is
            # the only step that builds + tests the OMEGangPack plugin, its
            # pure topology/placement packages, and the envtest integration
            # harness. Bootstraps its own envtest (1.35, matching the
            # module's k8s pin); runs in parallel with the rest of the fan-out.
            command: |
              sh .buildkite/run_scheduler_tests.sh
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}

          # The muse-server image build owns web install/check/build in its
          # cached Node stage. Keep this independent step Rust-native.
          - label: ":rust: MUSE Rust Checks"
            key: "muse-rust-checks"
{_gh_status("muse-rust-checks")}
{_infra_retry()}
            command: |
              apt-get update
              apt-get install -y --no-install-recommends build-essential cmake perl pkg-config python3-yaml
              rustup component add rustfmt clippy
              rustc --version
              make -C ome-client-rs kopium-install ci-placement
              make -C muse ci-rust
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(MUSE_RUST_CI_IMAGE, indent=14)}

          - label: ":helm: Helm Lint"
            key: "helm-lint"
{_gh_status("helm-lint")}
{_infra_retry()}
            command: |
              make helm-lint
            agents:
              queue: "{DEFAULT_QUEUE}"
            plugins:
{_tw_docker_plugin(CI_IMAGE, indent=14)}
        """
    )
    # Drift checks: each runs a regen target and fails if the working
    # tree differs from HEAD — catches PRs that forgot to regenerate.
    drift_checks = [
        _drift_check_step("fmt-drift", ":broom: Fmt Drift", "fmt"),
        _drift_check_step("tidy-drift", ":package: Tidy Drift", "tidy"),
        # `make manifests` post-processes CRD YAML through `jq`; the
        # default golang:bookworm image doesn't ship it. perl is already
        # present (pulled in by git in the base image).
        _drift_check_step(
            "manifests-drift",
            ":memo: Manifests Drift",
            "manifests",
            apt_packages="jq",
        ),
        _drift_check_step(
            "generate-drift",
            ":arrows_counterclockwise: Generate Drift",
            "generate",
        ),
        _drift_check_step(
            "dep-crds-drift",
            ":truck: Dep CRDs Drift",
            "dep-crds",
        ),
        # oeps/STATUS.md is rendered from every oeps/*/oep.yaml; a status
        # edit without a regen leaves the table lying about the code.
        _drift_check_step(
            "oep-status-drift",
            ":clipboard: OEP Status Drift",
            "oep-status",
        ),
    ]
    steps = base + "\n" + "\n".join(drift_checks)
    if GKE_KIND_ENABLED:
        # The twins are main-only; validate their soft list on every branch so
        # an expired or misnamed entry blocks the PR that carries it, not the
        # next main build.
        _kind_scheduler_soft_groups()
        steps += "\n" + _package_kind_test_image_step()
        steps += "\n" + _gke_kind_group()
        if BRANCH == "main" and GKE_KIND_SCHEDULER_ENABLED:
            steps += "\n" + _gke_kind_scheduler_group()
        if quarantine := _kind_quarantine_group():
            steps += "\n" + quarantine
        steps += "\n" + _mc_kind_group()
        if GKE_ALFRED_ENABLED:
            steps += "\n" + _alfred_kind_group()
    return steps


# ---------------------------------------------------------------------------
# CD pipeline (build always; push only when run_*.sh sees BUILDKITE_BRANCH=main)
# ---------------------------------------------------------------------------


def _image_entry(label: str) -> tuple[str, str]:
    """The (ECR name, Dockerfile) IMAGES declares for a label."""
    for name, ecr_name, dockerfile in IMAGES:
        if name == label:
            return ecr_name, dockerfile
    raise RuntimeError(f"the pipeline builds no image named {label}")


def _image_inputs(label: str) -> tuple[str, ...]:
    """The tracked paths hashed for an image: the common set plus its entry."""
    if label not in IMAGE_INPUTS:
        raise RuntimeError(f"image {label} has no IMAGE_INPUTS entry")
    return COMMON_IMAGE_INPUTS + tuple(IMAGE_INPUTS[label][1])


def _check_image_inputs_table() -> None:
    """Every built image has exactly one inputs entry, and no entry is stale."""
    labels = {label for label, _, _ in IMAGES}
    if stale := sorted(set(IMAGE_INPUTS) - labels):
        raise RuntimeError(
            "IMAGE_INPUTS names images the pipeline does not build: " + ", ".join(stale)
        )
    if missing := sorted(labels - set(IMAGE_INPUTS)):
        raise RuntimeError("images without an IMAGE_INPUTS entry: " + ", ".join(missing))


@functools.lru_cache(maxsize=None)
def _tracked_listing(repo_root: str, commit: str, paths: tuple[str, ...]) -> tuple[str, ...] | None:
    """`git ls-tree` entries (mode, type, blob, path) under paths at commit.

    Tracked content only, so build outputs and ignored files never enter a
    hash. None when git cannot answer where the pipeline is generated: reuse
    is an optimisation, so the caller then builds as if nothing were
    published and says so on stderr. Cached per commit, so pass a resolved
    SHA when the same process looks at a moving ref.
    """
    command = [
        "git", "-c", "safe.directory=*", "-C", repo_root,
        "ls-tree", "-r", "-z", "--full-tree", commit, "--", *paths,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as exc:
        sys.stderr.write(f"image reuse off: git is not runnable here ({exc})\n")
        return None
    if result.returncode != 0:
        sys.stderr.write(f"image reuse off: git ls-tree failed: {result.stderr.strip()}\n")
        return None
    return tuple(entry for entry in result.stdout.split("\0") if entry)


def _covers(path: str, entry_path: str) -> bool:
    """Whether an input path names this tracked file or a directory above it."""
    return entry_path == path or entry_path.startswith(path.rstrip("/") + "/")


def _inputs_tag(prefix: str, preimage: list[str]) -> str:
    return prefix + hashlib.sha256("\n".join(preimage).encode()).hexdigest()[:32]


def _image_inputs_tag(label: str, dockerfile: str) -> str | None:
    """The registry tag naming everything this image's build reads.

    The preimage is the image's recipe as the plugin is told it (Dockerfile,
    context, platforms, build args) and the tracked listing of its inputs,
    so a bump to a builder image or a pin changes the tag as surely as a
    source edit. None when git is unavailable; a RuntimeError when an input
    matches no tracked file, which is a table mistake to fix, not a miss.
    """
    inputs = _image_inputs(label)
    listing = _tracked_listing(str(REPO_ROOT), COMMIT, inputs)
    if listing is None:
        return None
    entry_paths = [entry.split("\t", 1)[1] for entry in listing]
    for path in inputs:
        if not any(_covers(path, entry_path) for entry_path in entry_paths):
            raise RuntimeError(
                f"image input {path} of {label} matches no tracked file at {COMMIT}"
            )
    recipe = {
        "dockerfile": dockerfile,
        "build-context": ".",
        "platforms": list(IMAGE_PLATFORMS),
        "args": _image_build_args(label),
    }
    preimage = ["ome-image-inputs/1", label, json.dumps(recipe, sort_keys=True), *listing]
    return _inputs_tag(IMAGE_INPUTS_TAG_PREFIX, preimage)


def _eval_image_inputs() -> tuple[str, dict[str, str]] | None:
    """The eval image's inputs tag and the source-image tags it is made from."""
    sources: dict[str, str] = {}
    for label in EVAL_IMAGE_SOURCES:
        _, dockerfile = _image_entry(label)
        tag = _image_inputs_tag(label, dockerfile)
        if tag is None:
            return None
        sources[label] = tag
    script = _tracked_listing(str(REPO_ROOT), COMMIT, (EVAL_IMAGE_SCRIPT,))
    if script is None:
        return None
    if not script:
        raise RuntimeError(f"{EVAL_IMAGE_SCRIPT} is not a tracked file at {COMMIT}")
    preimage = ["ome-eval-image-inputs/1", json.dumps(sources, sort_keys=True), *script]
    return _inputs_tag(EVAL_IMAGE_INPUTS_TAG_PREFIX, preimage), sources


def _image_tags(label: str) -> list[str]:
    """The commit-facing tags a build publishes.

    `latest` is main-only: the mirror step copies it to the prod registry, so
    a branch build of an every-branch image publishes its commit tag only.
    """
    every_branch = label in _push_every_branch_images()
    return [COMMIT[:8]] if every_branch and BRANCH != "main" else ["latest", COMMIT[:8]]


def _image_pushes_from_this_branch(label: str) -> bool:
    return BRANCH == "main" or label in _push_every_branch_images()


def _image_build_plugin_step(
    label: str,
    ecr_name: str,
    dockerfile: str,
    *,
    key: str,
    tags: list[str],
    depends_on: str | None = None,
) -> str:
    """The cf-docker-image step for one image, rendered at column 0.

    The plugin's command hook generates the real build job and uploads it
    behind this step, so this step is thin and the build runs as the job it
    uploads. Builds on every branch (validates the Dockerfile); pushes only on
    main via push-branches, except the images whose build pushes on every
    branch (_push_every_branch_images). Mirror to prod ECR is handled by
    _mirror_image_step, which copies the manifest at the registry layer.
    """
    every_branch = label in _push_every_branch_images()
    push_line = "push: true" if every_branch else 'push-branches: "main"'
    lines = [
        f'- label: ":docker: Build {label}"',
        f'  key: "{key}"',
        _gh_status(key, indent=2),
    ]
    if depends_on:
        lines.append(f'  depends_on: "{depends_on}"')
    lines += [
        _auto_retry(indent=2),
        "  agents:",
        f'    queue: "{BUILD_QUEUE}"',
        "  env:",
        '    BUILDX_NO_DEFAULT_ATTESTATIONS: "1"',
        "  plugins:",
        f'    - "{CF_DOCKER_IMAGE_PLUGIN}":',
        f'        ecr-name: "msl_infra/{ecr_name}"',
        '        environment: "dev"',
        f'        region: "{AWS_REGION}"',
        f'        dockerfile: "{dockerfile}"',
        '        build-context: "."',
    ]
    build_args = _image_build_args(label)
    if build_args:
        lines.append("        args:")
        lines += [f'          - "{arg}"' for arg in build_args]
    lines += [
        "        platforms: [" + ", ".join(f'"{platform}"' for platform in IMAGE_PLATFORMS) + "]",
        f"        {push_line}",
        "        tags: [" + ", ".join(f'"{tag}"' for tag in tags) + "]",
        "        login: true",
        _auto_retry(indent=8),
        "        tw-buildkit: true",
    ]
    return "\n".join(lines) + "\n"


def _build_image_step(label: str, ecr_name: str, dockerfile: str) -> str:
    """Emit the step that owns the key build-<label>.

    With an inputs hash it is the reuse probe (build_image.sh): it retags the
    dev image already published under that hash for this commit, or uploads
    the plugin build step behind itself on a miss. A step that waits on this
    key also waits for the steps it uploads, so the mirror, package and chart
    steps need no other dependency. The uploaded step carries the hash as a
    tag, which is what makes the next build's probe hit. Without git, or
    under OME_IMAGE_REBUILD, the plugin build step itself owns the key.
    """
    inputs_tag = _image_inputs_tag(label, dockerfile)
    tags = _image_tags(label) + ([inputs_tag] if inputs_tag else [])
    if inputs_tag is None or IMAGE_REBUILD:
        return _image_build_plugin_step(
            label, ecr_name, dockerfile, key=f"build-{label}", tags=tags, depends_on="test"
        )
    uploaded = _image_build_plugin_step(
        label, ecr_name, dockerfile, key=f"build-{label}-image", tags=tags
    )
    # One line of base64 so the fragment survives the docker plugin's env file.
    fragment = base64.b64encode(("steps:\n" + textwrap.indent(uploaded, "  ")).encode()).decode()
    publish = " ".join(_image_tags(label)) if _image_pushes_from_this_branch(label) else ""
    return textwrap.dedent(
        f"""\
          - label: ":docker: Build {label}"
            key: "build-{label}"
{_gh_status(f"build-{label}")}
            depends_on: "test"
{_auto_retry()}
            # The dev-registry probe and retag use the same Buildkite cluster
            # secret as the mirror steps (_ecr_login.sh tier 2).
            secrets:
              - "ECR_AUTH_TOKEN"
            env:
              AWS_REGION: "{AWS_REGION}"
              ECR_REGISTRY: "{ECR_REGISTRY}"
              OME_IMAGE_INPUTS_TAG: "{inputs_tag}"
              OME_IMAGE_INPUTS: "{' '.join(_image_inputs(label))}"
              OME_IMAGE_PUBLISH_TAGS: "{publish}"
              OME_IMAGE_BUILD_STEP_B64: "{fragment}"
            agents:
              queue: "{BUILD_QUEUE}"
            command: |
              sh .buildkite/build_image.sh msl_infra/{ecr_name}
            plugins:
{_tw_docker_plugin(CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=14)}
        """
    )


def _image_build_args(label: str) -> list[str]:
    if label == "muse-server":
        return [f"RUST_BUILDER_IMAGE={RUST_BUILDER_IMAGE}"]
    args = [f"GO_BUILDER_IMAGE={GO_BUILDER_IMAGE}"]
    if label == "alfred":
        args.append(f"ALFRED_OSS_REF={_alfred_oss_ref()}")
    if label == "ome-bot":
        args.append(f"KUBECTL_OME_REF={_kubectl_ome_oss_ref()}")
    return args


def _alfred_oss_ref() -> str:
    return ALFRED_OSS_REF_FILE.read_text().strip()


def _kubectl_ome_oss_ref() -> str:
    return KUBECTL_OME_OSS_REF_FILE.read_text().strip()


def _mirror_image_step(label: str, ecr_name: str) -> str:
    """Copy the dev-ECR image to prod ECR via `crane copy` — pure-registry
    manifest+blob copy, no rebuild and no docker daemon required (the
    ci-base image doesn't ship one). The script downloads crane on first
    run and uses cloud_oss for cross-account ECR auth via the shared
    _ecr_login.sh helper. --only-main skips on PR branches: prod receives
    main-qualified images only, whatever a branch build pushed to dev.
    """
    short_sha = "${BUILDKITE_COMMIT:0:8}"
    return textwrap.dedent(
        f"""\
          - label: ":docker: Mirror {label} → prod ECR"
            key: "mirror-{label}"
{_gh_status(f"mirror-{label}")}
            depends_on: "build-{label}"
{_auto_retry()}
            # ECR credentials on TW: the agent resolves these Buildkite
            # cluster secrets into the job env, propagate-environment
            # carries them into the container, and _ecr_login.sh consumes
            # them (tier 2). The MKS-era aws-CLI fallback doesn't exist in
            # TW workload containers.
            secrets:
              - "ECR_AUTH_TOKEN"
              - "PROD_ECR_AUTH_TOKEN"
            agents:
              queue: "{BUILD_QUEUE}"
            command: |
              sh .buildkite/mirror_image.sh --only-main msl_infra/{ecr_name} latest "{short_sha}"
            plugins:
{_tw_docker_plugin(CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=14)}
        """
    )


def _package_kind_eval_image_step() -> str:
    """Add exact main runtime-image archives to the offline KIND runner.

    The step reuses like the image steps: the generator hands it the tag
    naming its source images' inputs, and the script retags the eval image
    already published under it instead of packaging again. Under
    OME_IMAGE_REBUILD the script packages and retags, so the refreshed
    runtime images reach the eval image too.
    """
    env: dict[str, str] = {}
    if (eval_inputs := _eval_image_inputs()) is not None:
        tag, sources = eval_inputs
        env["OME_EVAL_IMAGE_INPUTS_TAG"] = tag
        env["OME_EVAL_IMAGE_INPUTS"] = " ".join(
            [f"{label}={source_tag}" for label, source_tag in sources.items()] + [EVAL_IMAGE_SCRIPT]
        )
        if IMAGE_REBUILD:
            env["OME_IMAGE_REBUILD"] = "1"
    env_block = _yaml_block({"env": env}, indent=12) if env else ""
    return textwrap.dedent(
        f"""\
          - label: ":package: Package exact OMENative eval images"
            key: "package-kind-eval-image"
{_gh_status("package-kind-eval-image")}
            depends_on:
              - "mirror-kind-ci"
              - "mirror-ome-manager"
              - "mirror-ome-agent"
              - "mirror-ome-scheduler"
              - "mirror-canary-metrics"
{_auto_retry()}
{env_block}
            secrets:
              - "PROD_ECR_AUTH_TOKEN"
            agents:
              queue: "{BUILD_QUEUE}"
            command: |
              sh .buildkite/package_kind_eval_image.sh
            plugins:
{_tw_docker_plugin(CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=14)}
        """
    )


def _push_chart_step(chart: str, depends_on: str | list[str] = "test") -> str:
    if isinstance(depends_on, list):
        deps = "\n".join(f'              - "{d}"' for d in depends_on)
        depends_block = f"depends_on:\n{deps}"
    else:
        depends_block = f'depends_on: "{depends_on}"'
    return textwrap.dedent(
        f"""\
          - label: ":helm: Package {chart}"
            key: "chart-{chart}"
{_gh_status(f"chart-{chart}")}
            {depends_block}
            # Same Buildkite cluster-secret ECR auth as the mirror steps;
            # push_chart.sh logs in to both the dev and prod registries.
            secrets:
              - "ECR_AUTH_TOKEN"
              - "PROD_ECR_AUTH_TOKEN"
            command: |
              sh .buildkite/push_chart.sh {chart}
            artifact_paths:
              - ".buildkite/artifacts/chart-versions/{chart}"
            agents:
              queue: "{BUILD_QUEUE}"
            plugins:
{_tw_docker_plugin(CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=14)}
        """
    )


def _upload_batches_step(uploads: int, jobs: int) -> str:
    """The last step of the first upload: it generates every document again
    and uploads those after the first, the eval groups and then the release
    marker, so they exist in the build before anything waits on them."""
    return textwrap.dedent(
        f"""\
          - label: ":gear: Upload eval lanes and release marker ({uploads} uploads, {jobs} jobs)"
            key: "{UPLOAD_STEP_KEY}"
{_gh_status(UPLOAD_STEP_KEY)}
{_infra_retry()}
            command: |
              sh {UPLOAD_SCRIPT}
            artifact_paths:
              - "{UPLOAD_BATCH_DIR}/*"
            agents:
              queue: "{BUILD_QUEUE}"
            plugins:
{_tw_docker_plugin(PIPELINE_UPLOAD_IMAGE, indent=14)}
        """
    )


def _eval_verdicts_step(depends_on: list[str]) -> str:
    """Record the full round once every eval group has finished, passed or
    not: one verdict per (case, scheduler) lane from the lanes' result
    archives, attached as the artifact targeted builds read, and the
    meta-data that marks the build as a recorded round. Soft: a round that
    cannot be recorded leaves the marker alone, and targeted builds keep
    reading the newest round that was."""
    if not depends_on:
        raise RuntimeError("the verdict record needs the eval groups it reads")
    deps = "\n".join(f'              - "{key}"' for key in depends_on)
    return textwrap.dedent(
        f"""\
          - label: ":memo: Record the round's verdicts"
            key: "{EVAL_VERDICTS_STEP_KEY}"
{_gh_status(EVAL_VERDICTS_STEP_KEY)}
            depends_on:
{deps}
            allow_dependency_failure: true
            soft_fail: true
            env:
              OME_EVAL_RESULTS_DIR: "{eval_verdicts.RESULTS_DIR}"
              OME_EVAL_VERDICTS_FILE: "{eval_verdicts.VERDICTS_FILE}"
              OME_EVAL_ROUND_META_KEY: "{eval_verdicts.ROUND_META_KEY}"
              OME_EVAL_ROUND_META_VALUE: "{eval_verdicts.ROUND_META_VALUE}"
            command: |
              sh .buildkite/record_eval_verdicts.sh
            agents:
              queue: "{BUILD_QUEUE}"
            plugins:
{_tw_docker_plugin(PIPELINE_UPLOAD_IMAGE, indent=14)}
        """
    )


def _release_marker_step(depends_on: list[str]) -> str:
    """Publish the Kargo-facing marker once every gate it names has passed.

    The marker is the last step of the last upload and names every other
    top-level step and group of the build as a dependency. That list is the
    whole gate, because a wait step would only order it after the steps
    uploaded before it: a hard failure anywhere skips the marker, and a
    soft-failing step completes before it without blocking it.
    """
    if not depends_on:
        raise RuntimeError("the release marker needs the keys it gates on")
    deps = "\n".join(f'              - "{key}"' for key in depends_on)
    return textwrap.dedent(
        f"""\
          - label: ":helm: Publish qualified OME release marker"
            key: "chart-ome-release"
            if: build.branch == "main"
{_gh_status("chart-ome-release")}
{_auto_retry()}
            depends_on:
{deps}
            secrets:
              - "PROD_ECR_AUTH_TOKEN"
            env:
              AWS_ACCOUNT_ID: "{PROD_AWS_ACCOUNT_ID}"
              ECR_REGISTRY: "{PROD_ECR_REGISTRY}"
              OME_RELEASE_CHARTS: "{' '.join(RELEASE_MARKER_CHARTS)}"
              OME_RELEASE_IMAGES: "{' '.join(RELEASE_MARKER_IMAGES)}"
            command: |
              sh .buildkite/push_release_marker.sh
            agents:
              queue: "{BUILD_QUEUE}"
            plugins:
{_tw_docker_plugin(CF_DOCKER_PLUGIN_RUNTIME_IMAGE, indent=14)}
        """
    )


def cd_steps() -> str:
    _check_image_inputs_table()
    parts: list[str] = []
    # A chart that stamps an image ref (prod-ECR hub + tag) into its values at
    # publish time must wait for both the build (dev push) AND the mirror (prod
    # push) — otherwise a consumer installing the freshly published chart pulls a
    # tag that does not exist yet and sits in ErrImagePull until the mirror
    # catches up. Which charts stamp is decided by push_chart.sh, and
    # IMAGE_STAMPING_CHARTS has to agree with it; test_generate_pipeline.py reads
    # that script and fails if the two drift.
    image_keys = []
    for label, ecr_name, dockerfile in IMAGES:
        parts.append(_build_image_step(label, ecr_name, dockerfile))
        parts.append(_mirror_image_step(label, ecr_name))
        # kind-ci is CI tooling, not a runtime image any chart stamps, so keep it
        # out of the chart depends_on set — otherwise chart publishing would
        # needlessly block on its (large, slow) build.
        if label == "kind-ci":
            continue
        image_keys.append(f"build-{label}")
        image_keys.append(f"mirror-{label}")
    if _eval_on():
        parts.append(_package_kind_eval_image_step())
    for chart in CHARTS:
        if chart in IMAGE_STAMPING_CHARTS:
            # These charts stamp an image ref, so wait for the image build + mirror.
            parts.append(_push_chart_step(chart, depends_on=image_keys))
        else:
            parts.append(_push_chart_step(chart))
    return textwrap.indent("\n".join(parts), "  ")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def generate_pipeline_batches() -> list[str]:
    """The pipeline as the documents the build uploads, in upload order.

    The first document is the entrypoint's upload: every step that is not an
    eval lane and, when there are eval lanes, the step that uploads the rest.
    Each eval group is a document of its own, and the last document carries
    a full round's verdict record and then the release marker, so every key
    the marker depends on exists in the build before it is uploaded. A
    dependency may only name a key from the same or an earlier document.
    """
    first = ci_steps()
    first += "\n" + cd_steps()
    # GKE_KIND_ENABLED too: this step depends on package-kind-test-image,
    # which only exists in the pipeline when that switch is on.
    if BRANCH == "main" and GKE_UPGRADE_ENABLED and GKE_KIND_ENABLED:
        first += "\n" + _gke_upgrade_identity_step()
    groups = _gke_eval_groups() if _eval_on() else []
    later = ["steps:\n" + _yaml_block([group], 2) for group in groups]
    tail = ""
    if groups and _eval_mode() == "full":
        tail = textwrap.indent(_eval_verdicts_step([group["key"] for group in groups]), "  ")
    if later:
        # The upload step's label counts what follows it: the groups and the
        # last document, the verdict record if any and the marker.
        jobs = sum(_job_count(document) for document in later) + _job_count(tail) + 1
        first += "\n" + textwrap.indent(_upload_batches_step(len(later) + 1, jobs), "  ")
    gates = _top_level_keys(first) + [group["key"] for group in groups] + _top_level_keys(tail)
    tail += textwrap.indent(_release_marker_step(gates), "  ")
    if later:
        later.append("steps:\n" + tail)
    else:
        first += "\n" + tail
    batches = [first, *later]
    for index, document in enumerate(batches, start=1):
        count = _job_count(document)
        if count > UPLOAD_JOB_LIMIT:
            raise RuntimeError(
                f"pipeline upload {index} of {len(batches)} carries {count} jobs, "
                f"above the {UPLOAD_JOB_LIMIT} one upload may carry"
            )
    return batches


def generate_pipeline() -> str:
    """The first document: what the pipeline entrypoint uploads."""
    return generate_pipeline_batches()[0]


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}" + ("" if count == 1 else "s")


def _upload_plan(batches: list[str]) -> str:
    """One line per upload for the log: its job count and what it carries."""
    total = sum(_job_count(document) for document in batches)
    lines = [
        f"pipeline upload plan: {_plural(len(batches), 'upload')}, {_plural(total, 'job')}, "
        f"at most {UPLOAD_JOB_LIMIT} per upload, eval {_eval_mode()}"
    ]
    for index, document in enumerate(batches, start=1):
        if index == 1:
            carries = "the entrypoint: every step that is not an eval lane"
        else:
            carries = f"{UPLOAD_STEP_KEY}: " + ", ".join(_top_level_keys(document))
        jobs = _plural(_job_count(document), "job")
        lines.append(f"  {index}/{len(batches)}  {jobs:>9}  {carries}")
    return "\n".join(lines) + "\n"


def _priority_prefix() -> str:
    """Return ``"priority: N\\n"`` if ``MSL_BK_PIPELINE_PRIORITY`` is set.

    The value is set by mslsrc/dev's static pipeline (see
    ``pulumi/deployments/buildkite/prod/pipelines.py``) and propagated to
    every step in the build via Buildkite's top-level ``env:`` mechanism.
    Re-emitting it as top-level YAML here makes the dynamically uploaded
    steps inherit the same priority instead of running at the implicit
    Buildkite default 0. Per-step priorities, if any, still win.
    """
    raw = os.environ.get("MSL_BK_PIPELINE_PRIORITY")
    if not raw:
        return ""
    try:
        return f"priority: {int(raw)}\n"
    except ValueError:
        return ""


def main(argv: list[str] | None = None) -> None:
    if os.environ.get("OME_PIPELINE_MODE"):
        sys.stderr.write("OME_PIPELINE_MODE is no longer supported\n")
        raise SystemExit(2)
    parser = argparse.ArgumentParser(description="Generate the Buildkite pipeline for OME.")
    parser.add_argument(
        "--batch-dir",
        metavar="DIR",
        help="write every upload document to DIR as batch-NN.yml and print one "
        "manifest line per document (file name, job count) instead of the "
        "first document",
    )
    args = parser.parse_args(argv)
    batches = generate_pipeline_batches()
    sys.stderr.write(_upload_plan(batches))
    if args.batch_dir is None:
        sys.stdout.write(_priority_prefix() + batches[0])
        return
    directory = Path(args.batch_dir)
    directory.mkdir(parents=True, exist_ok=True)
    for index, document in enumerate(batches, start=1):
        name = f"batch-{index:02d}.yml"
        (directory / name).write_text(_priority_prefix() + document)
        sys.stdout.write(f"{name} {_job_count(document)}\n")


if __name__ == "__main__":
    main()
