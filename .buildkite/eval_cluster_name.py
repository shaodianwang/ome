#!/usr/bin/env python3
"""Name the KIND cluster of one OMENative eval lane so every node fits.

KIND names each node ``<cluster>-<role>`` (``-control-plane``, ``-worker``,
``-worker2``, ...) and docker sets that name as the container's hostname, so
a cluster named after a long lane fails before any case runs. The plain
``ome-eval-<lane>`` name is kept whenever its longest node fits the limit;
otherwise a readable prefix of it is joined with a short stable hash of the
whole lane, so the shortened name is unique and the same on every run.

Only the cluster name shortens: the lane label, the results directory and
the JUnit report keep the full lane.

Usage: eval_cluster_name.py LANE WORKERS
"""

from __future__ import annotations

import argparse
import hashlib
import sys

# The Linux hostname limit; a KIND node with a longer name never starts.
HOSTNAME_MAX = 63
CLUSTER_PREFIX = "ome-eval-"
CONTROL_PLANE_SUFFIX = "-control-plane"
WORKER_SUFFIX = "-worker"
HASH_LENGTH = 6


def node_suffixes(workers: int) -> list[str]:
    """The suffixes KIND appends to the cluster name for one control plane
    and ``workers`` workers; the first node of a role carries no ordinal."""
    if workers < 1:
        raise ValueError(f"a cluster needs at least one worker, got {workers}")
    return [CONTROL_PLANE_SUFFIX] + [
        WORKER_SUFFIX + (str(ordinal) if ordinal > 1 else "")
        for ordinal in range(1, workers + 1)
    ]


def node_names(cluster: str, workers: int) -> list[str]:
    """Every node name, and so every hostname, KIND derives from the cluster name."""
    return [cluster + suffix for suffix in node_suffixes(workers)]


def plain_cluster_name(lane: str) -> str:
    """The cluster name a lane gets when it fits."""
    return CLUSTER_PREFIX + lane.replace("_", "-")


def cluster_name(lane: str, workers: int) -> str:
    """The KIND cluster name for ``lane`` with ``workers`` worker nodes."""
    plain = plain_cluster_name(lane)
    budget = HOSTNAME_MAX - max(len(suffix) for suffix in node_suffixes(workers))
    if len(plain) <= budget:
        return plain
    head_length = budget - HASH_LENGTH - 1
    if head_length < len(CLUSTER_PREFIX):
        raise ValueError(f"no room for a cluster name with {workers} workers")
    digest = hashlib.sha256(lane.encode("utf-8")).hexdigest()[:HASH_LENGTH]
    return plain[:head_length].rstrip("-") + "-" + digest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("lane", help="the lane: feature-topology[-scheduler][-variant]")
    parser.add_argument("workers", type=int, help="worker nodes the lane creates")
    args = parser.parse_args(argv)
    try:
        print(cluster_name(args.lane, args.workers))
    except ValueError as err:
        parser.error(str(err))
    return 0


if __name__ == "__main__":
    sys.exit(main())
