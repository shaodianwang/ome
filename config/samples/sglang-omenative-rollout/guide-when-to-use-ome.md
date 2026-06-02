# To OME, or not to OME

*OME user guide — page 1 of 4. Next: [OMENative](guide-omenative.md) ·
[Runtimes](guide-runtimes.md) ·
[Models & InferenceService](guide-models-and-inferenceservice.md)*

This page is for deciding whether you should use OME at all. The honest
answer is: not every model server needs it. Use it when it earns its keep,
and skip it when a plain Deployment would do.

On this page:
- What OME is, in one paragraph
- The problem it solves
- When you do *not* need OME
- When you *do* need OME
- Rules of thumb
- FAQ

## What OME is, in one paragraph

OME is a Kubernetes operator for serving large models. You describe *what*
you want — a model, a runtime, how many replicas — and OME creates and keeps
alive the pods, services, and autoscalers that serve it. You do not hand-write
Deployments, Services, or rollout scripts. The unit you manage is an
**InferenceService** (short name `isvc`).

## The problem it solves

Serving a large model on raw Kubernetes means solving the same handful of
problems every time:

- **Scheduling on accelerators.** GPUs, RDMA, node selectors, topology — easy
  to get subtly wrong.
- **Running more than one pod as one unit.** Prefill/decode disaggregation and
  multi-node tensor parallelism are several pods that must come up, stay, and
  roll *together*.
- **Upgrading without dropping traffic.** A new image should roll out in a
  controlled way, not all at once.

You can do all of this by hand. OME's claim is that you shouldn't have to, and
that doing it by hand is where outages come from.

## When you do *not* need OME

Be honest with yourself. If all of the following are true, a plain Deployment
is simpler and you should use that:

- one model, one pod per replica (no prefill/decode split, no multi-node),
- the weights are small enough to pull normally, or already on the node,
- you don't need coordinated, paced rollouts,
- you are the only team using this runtime.

OME does not make a simple thing simpler. It makes a *hard* thing safe.

## When you *do* need OME

Reach for OME when one or more of these is true:

- **Prefill/decode disaggregation (PD).** Engine and decoder are separate pools
  that talk to each other and must roll together. (See
  [OMENative](guide-omenative.md).)
- **Multi-node serving.** One model instance spans several pods (leader +
  workers) that must be scheduled and restarted as a gang.
- **Safe upgrades matter.** You want surge-then-drain, or to roll prefill and
  decode in lockstep, or to validate one component before the next. (See
  [OMENative](guide-omenative.md).)
- **You share runtimes across teams.** A platform team publishes one hardware
  profile; many teams inherit it. (See [Runtimes](guide-runtimes.md).)

## Rules of thumb

- *Don't reach for OME to run a single small model server.* A Deployment is
  fine, and fewer moving parts is a feature.
- *Do reach for OME the moment "one instance" is more than one pod.* That is
  exactly the case Kubernetes' built-in controllers handle badly.
- *Let the platform own the hardware, let yourself own the workload.* If you
  find yourself editing GPU counts and NCCL settings to ship a model, the split
  between base and tenant runtime is wrong (see
  [Runtimes](guide-runtimes.md)).
- *Prefer boring rollouts.* The point of the rollout knobs is that an upgrade
  is uneventful. If a rollout is exciting, simplify it.

## FAQ

**Do I still write Deployments and Services?**
No. You write an InferenceService (and usually a runtime + a model reference).
OME creates the pods, services, and autoscalers.

**Is OME just a Helm chart?**
No. A Helm chart renders YAML once, at install time. OME is a running
controller: it keeps reconciling, reacts to a new image or a failed pod, and
runs the rollout. There *is* a Helm chart (`ome-isvc`) that generates the OME
resources for you, but the chart is a convenience on top of the operator, not
the operator itself.

**What does OME give me that `kubectl apply` of a Deployment doesn't?**
Three things a Deployment can't express: (1) one *instance* made of several
pods, scheduled and restarted as a gang; (2) rollouts coordinated *across*
components (roll prefill and decode together); (3) in-place updates that change
a pod without rescheduling it. If you need none of those, you don't need OME.

**Is it tied to one cloud?**
No. The cloud-specific parts (image registry, accelerator type, networking)
live in a base runtime, one per cloud. Your workload definition stays the same
across CoreWeave, GCP, and OCI. (See [Runtimes](guide-runtimes.md).)

**Doesn't OME also manage model artifacts and pick the runtime for me?**
It can — OME has features for model management and runtime auto-selection — but
we don't use them here. Weights are fetched at runtime by cluster cache (see
[Models & InferenceService](guide-models-and-inferenceservice.md)), and you
name the runtime explicitly (see [Runtimes](guide-runtimes.md)). So this guide
treats neither as a reason to adopt OME.

**Where do I start once I've decided to use it?**
Read [OMENative](guide-omenative.md) next — it's the deployment mode that
unlocks the features above — then [Runtimes](guide-runtimes.md) and
[Models & InferenceService](guide-models-and-inferenceservice.md). The YAML in
this folder is a complete, working example.
