#!/usr/bin/env python3
"""Render a single-user devbox Pod manifest as JSON on stdout.

The shape mirrors the two-container job the GKE KIND CI lane runs
(.buildkite/README-gke-kind.md): a "dev" container from the kind-ci image that
carries the OME toolchain, plus an optional privileged docker:dind sidecar the
dev container reaches over DOCKER_HOST. Standard library only; nothing is
written to disk. Pipe the output into `kubectl --kubeconfig ... create -f -`.

    render_devbox_pod.py --name devbox-me --namespace playground \
        --image <registry>/msl_infra/kind-ci:latest --dind --gpus 4 > pod.json
"""
import argparse
import getpass
import json
import re
import sys

DEFAULT_DIND_IMAGE = (
    "168653207203.dkr.ecr.us-east-2.amazonaws.com/pt_dockerio/library/docker:28-dind"
)
DEFAULT_NO_PROXY = "localhost,127.0.0.1,::1,10.0.0.0/8,.svc,.svc.cluster.local"
# Keep the shell as PID 1 with a trap: a bare `sleep` as PID 1 ignores SIGTERM
# and the pod lingers for the whole grace period on delete.
HOLD_COMMAND = "trap 'exit 0' TERM INT; while :; do sleep 3600 & wait $!; done"
DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
VALID_EFFECTS = ("NoSchedule", "PreferNoSchedule", "NoExecute")


def key_value(text):
    if "=" not in text:
        raise argparse.ArgumentTypeError(f"expected key=value, got {text!r}")
    key, value = text.split("=", 1)
    if not key:
        raise argparse.ArgumentTypeError(f"empty key in {text!r}")
    return key, value


def toleration(text):
    """key[=value]:Effect -> a Toleration dict."""
    if ":" not in text:
        raise argparse.ArgumentTypeError(f"expected key[=value]:Effect, got {text!r}")
    head, effect = text.rsplit(":", 1)
    if effect not in VALID_EFFECTS:
        raise argparse.ArgumentTypeError(f"effect must be one of {VALID_EFFECTS}, got {effect!r}")
    if "=" in head:
        key, value = head.split("=", 1)
        return {"key": key, "operator": "Equal", "value": value, "effect": effect}
    return {"key": head, "operator": "Exists", "effect": effect}


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--name", required=True, help="pod name (DNS label)")
    p.add_argument("--namespace", required=True)
    p.add_argument("--image", required=True, help="dev container image (kind-ci)")
    p.add_argument("--owner", default=getpass.getuser(), help="value of the `user` label (default: current login)")
    p.add_argument("--workdir", default="/work", help="emptyDir mounted here in every container")
    p.add_argument("--cpu", default="4")
    p.add_argument("--memory", default="16Gi")
    p.add_argument("--ephemeral-storage", default="50Gi")
    p.add_argument("--gpus", type=int, default=0, help="number of accelerator devices to request")
    p.add_argument("--gpu-resource", default="nvidia.com/gpu")
    p.add_argument("--dind", action="store_true", help="add a privileged docker:dind sidecar")
    p.add_argument("--dind-image", default=DEFAULT_DIND_IMAGE)
    p.add_argument("--dind-cpu", default="2")
    p.add_argument("--dind-memory", default="8Gi")
    p.add_argument("--dind-storage", default="40Gi")
    p.add_argument("--proxy", help="HTTP(S)_PROXY for the dev container")
    p.add_argument("--no-proxy", default=DEFAULT_NO_PROXY)
    p.add_argument("--env", action="append", default=[], type=key_value, metavar="K=V")
    p.add_argument("--annotation", action="append", default=[], type=key_value, metavar="K=V")
    p.add_argument("--label", action="append", default=[], type=key_value, metavar="K=V")
    p.add_argument("--node-selector", action="append", default=[], type=key_value, metavar="K=V")
    p.add_argument("--toleration", action="append", default=[], type=toleration, metavar="KEY[=VALUE]:EFFECT")
    p.add_argument("--image-pull-secret", action="append", default=[], metavar="NAME")
    p.add_argument("--service-account")
    p.add_argument("--grace-period", type=int, default=30)
    args = p.parse_args(argv)
    if not DNS_LABEL.match(args.name) or len(args.name) > 63:
        p.error(f"--name {args.name!r} is not a DNS label of at most 63 characters")
    if args.gpus < 0:
        p.error("--gpus must be >= 0")
    return args


def env_list(args):
    env = []
    if args.proxy:
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            env.append({"name": key, "value": args.proxy})
        for key in ("NO_PROXY", "no_proxy"):
            env.append({"name": key, "value": args.no_proxy})
    if args.dind:
        env.append({"name": "DOCKER_HOST", "value": "tcp://localhost:2375"})
    for key, value in args.env:
        env.append({"name": key, "value": value})
    return env


def render(args):
    quantities = {"cpu": args.cpu, "memory": args.memory, "ephemeral-storage": args.ephemeral_storage}
    resources = {"requests": dict(quantities), "limits": dict(quantities)}
    if args.gpus:
        resources["limits"][args.gpu_resource] = str(args.gpus)
        resources["requests"][args.gpu_resource] = str(args.gpus)

    work_mount = {"name": "work", "mountPath": args.workdir}
    dev = {
        "name": "dev",
        "image": args.image,
        "command": ["/bin/sh", "-c", HOLD_COMMAND],
        "workingDir": args.workdir,
        "stdin": True,
        "tty": True,
        "env": env_list(args),
        "resources": resources,
        "volumeMounts": [work_mount],
    }
    containers = [dev]
    volumes = [{"name": "work", "emptyDir": {}}]

    if args.dind:
        dind_q = {"cpu": args.dind_cpu, "memory": args.dind_memory, "ephemeral-storage": args.dind_storage}
        containers.append({
            "name": "dind",
            "image": args.dind_image,
            "securityContext": {"privileged": True},
            # Plain tcp://localhost:2375 inside the pod's network namespace; no TLS material to manage.
            "env": [{"name": "DOCKER_TLS_CERTDIR", "value": ""}],
            "resources": {"requests": dict(dind_q), "limits": dict(dind_q)},
            "volumeMounts": [{"name": "docker-graph", "mountPath": "/var/lib/docker"}, work_mount],
        })
        volumes.append({"name": "docker-graph", "emptyDir": {}})

    labels = {"app": "devbox", "user": args.owner}
    labels.update(dict(args.label))
    metadata = {"name": args.name, "namespace": args.namespace, "labels": labels}
    if args.annotation:
        metadata["annotations"] = dict(args.annotation)

    spec = {
        "restartPolicy": "Never",
        "terminationGracePeriodSeconds": args.grace_period,
        "containers": containers,
        "volumes": volumes,
    }
    if args.service_account:
        spec["serviceAccountName"] = args.service_account
    if args.node_selector:
        spec["nodeSelector"] = dict(args.node_selector)
    if args.toleration:
        spec["tolerations"] = args.toleration
    if args.image_pull_secret:
        spec["imagePullSecrets"] = [{"name": n} for n in args.image_pull_secret]

    return {"apiVersion": "v1", "kind": "Pod", "metadata": metadata, "spec": spec}


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    json.dump(render(args), sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
