# OEP-0021.1: MUSE write plane — create and edit OME resources via pull request

<!-- toc -->

## Summary

**Tl;dr:** MUSE's write plane turns a console form into a reviewed pull request.
Every create or edit of an OME resource — an `InferenceService`, a
`ServingRuntime`, or a scale/traffic change to an existing one — is rendered to a
Kubernetes CR and committed to the GitOps repository as a PR
**authored by the requesting user**; Argo CD applies it on merge. MUSE writes to no
cluster: the write component holds a GitHub credential, not kube write-RBAC.

This is a follow-up to OEP-0021, and it makes two of that document's deferrals
concrete. It **resolves open fork #2 (direct-write vs GitOps) in favor of a pull
request on every write** — reversing OEP-0021's v1 lean toward direct-write — and
it **resolves the Auth/identity Open Question**. Everything else in OEP-0021 (the
read plane; model onboarding/convert/transfer) is unchanged and out of scope here.

Define the load-bearing terms up front; the rest of the doc keeps them apart:

- **PR-per-write** — the contract that *every* state change MUSE makes is a Git
  pull request, never a direct apply. There is no direct-to-cluster path, not even
  for dev. Consequence: MUSE never needs cluster write-RBAC, and Git is the audit
  log.
- **`muse-writer`** — a process separate from the read-plane `muse-server`. It is
  the *only* component that holds GitHub credentials and reaches `github.com`.
  Consequence: the broadly-exposed read plane never inherits the write trust
  domain.
- **The change pipeline** — the single code path all write features feed:
  `{target, intent, object-or-patch, actor}` → validate → render CR YAML → open
  PR. One sink (a PR), so there is exactly one place writes are produced and
  audited.
- **Actor** — the authenticated human on whose behalf a write happens, established
  from the gateway identity (below) and attributed on the PR. An *unattributed*
  write is a defect, not a convenience.
- **gateway `jwt_header`** — the ingress-gateway plugin that injects a signed JWT
  (a request header carrying the caller's internal user ID) so the backend knows who
  the user is. It is the *identity* source; it is **not** a GitHub credential.
- **GitHub App user-access-token** — a token (`ghu_`) that lets `muse-writer` act
  on GitHub *as a specific user*, so the PR is attributed to them. Distinct from an
  installation token (`ghs_`), which is bot-attributed.

## Motivation

OEP-0021 shipped MUSE's read plane and a Deploy form that is, verbatim, a "design
preview … [that] doesn't create anything" (`muse/web/src/panels/Deploy.tsx`). There
is no self-service create or edit at all. The gap this OEP closes is turning that
preview into real writes — safely.

The failure mode we are designing *against* is the one OEP-0021 itself named in
fork #2: **a console "Deploy" button that writes to production is an unaudited
outage waiting to happen.** OEP-0021 leaned toward direct-write-with-provenance for
v1 and deferred GitOps. We are reversing that lean, for three reasons the
intervening feasibility work made concrete:

1. **Git is a better audit log than anything MUSE would record.** A PR carries the
   author, the diff, the review, and the merge — for free, in the system operators
   already trust. Provenance-in-a-field (OEP-0021's mitigation) reimplements a
   worse version of that.
2. **PR-per-write lets MUSE hold zero cluster write-RBAC.** If MUSE never applies,
   its blast radius is "can open a PR," not "can mutate any cluster." That is
   strictly smaller than direct-write, and it keeps the read plane's read-only
   guarantee intact across the whole product.
3. **The sanctioned internal pattern is already GitOps.** The OME
   `InferenceService`/`ServingRuntime` deploy path today is a Helm chart committed
   to the GitOps repository and reconciled by Argo CD, and the internal design of
   record for programmatic deploys is "convert input → CR → commit to
   GitHub → Argo applies". PR-per-write *is* that pattern; direct-write
   would fork it.

The honest cost is stated in Non-Goals and Drawbacks: **day-2 operations become PR
round-trips.** A replica bump is open-PR → review → merge → Argo-sync, not an
instant change.

### Goals

1. **Self-service create and edit, each as a proper PR.** Create
   `InferenceService`, create `ServingRuntime`, and scale/edit an existing
   `InferenceService`, each producing a Git PR authored by the requesting user.
2. **MUSE holds zero cluster write-RBAC.** Every write is a human-reviewed PR that
   Argo applies. No cluster is ever mutated by a MUSE service account.
3. **Per-user attribution and authorization.** Every write is tied to a verified
   human identity, and MUSE can gate *who* may open *which* PR before the PR exists.
4. **Validate before the PR.** Bad input fails in the form, not after merge —
   without granting MUSE any cluster-write verb (not even a server dry-run).

### Non-Goals

- **Not direct-apply.** There is no dev/prod split and no fast path that bypasses
  the PR. This explicitly supersedes OEP-0021 fork #2 option (a).
- **Not a merge/apply bot.** `muse-writer` *opens* PRs; humans review and merge, and
  Argo applies. MUSE does not auto-merge and does not apply.
- **Not a new placement or serving path.** The CR MUSE emits rides OEP-0018
  placement (or Argo) unchanged; MUSE does not bin-pack or fan out.
- **Not model onboarding/convert/transfer.** Those writes (OEP-0021's other
  write-plane work) are out of scope here; this OEP is CR create/edit only.

## Proposal

### The shape, in one breath

A console action becomes a `ChangeRequest`; `muse-writer` validates it, renders the
CR to YAML at the configured repository path, and opens a pull request as the
requesting user; Argo applies it on merge. Create-ISVC, create-SR, and scale/edit
are three thin front-ends onto that one pipeline — they differ only in which object
or patch they emit.

### Why a separate `muse-writer` process

The write plane is a different trust domain than the read plane: it holds a GitHub
App private key and per-user OAuth tokens, and it egresses to `github.com`. The read
plane (`muse-server`) holds neither — only read-RBAC and the metrics-proxy mTLS
cert — and it is the broadly-exposed, whole-fleet surface. Folding writes into
`muse-server` would give the most-exposed component the ability to author PRs with
everyone's tokens.

We therefore run `muse-writer` as its own binary/deployment. This extends the
process-split logic OEP-0019 established (a named subset of the control plane as its
own process) and OEP-0021 applied (MUSE separate from the OME manager) by one level:
read and write are separate processes *because their credentials differ*. Identity
parsing is a small shared library both use; only `muse-writer` holds write
credentials. Tradeoff: a second deployment and gateway routing writes to it —
accepted, because the alternative co-locates secrets with the read plane.

### User stories

- *As an ML engineer*, I fill in the Deploy form, click "Open PR," and MUSE opens a
  pull request in the GitOps repository — authored by me — that adds one
  `InferenceService`; I review the rendered YAML, merge, and Argo applies it.
- *As an SRE*, I scale a deployment from 4 to 8 replicas; MUSE opens a one-line PR
  editing `spec…replicas`; my teammate approves it and it rolls out through Argo.
- *As a reviewer*, I see exactly who requested each change and what it does, in the
  PR — no separate audit tool.

### Risks (named so they are reviewable)

- **W1 — Identity spoofing.** The gateway JWT is trivially forgeable if `muse-writer`
  does not verify its signature, or if `muse-writer` is reachable off-gateway.
  Mitigation in Design Details (verify + NetworkPolicy).
- **W2 — GitHub token custody.** Per-user OAuth tokens are user-scoped secrets. Held
  wrong, they are an impersonation vector. Mitigation: short-lived, encrypted,
  server-side only, derived from an authenticated OAuth login — never a
  user-supplied GitHub↔identity mapping (a self-reported mapping is itself an
  impersonation surface).
- **W3 — Bad CR merged (validation gap).** With no cluster apply, MUSE cannot
  server-dry-run, so a semantically-invalid CR could pass the form and only fail at
  Argo-apply. Mitigation, not full fix: pre-PR schema validation + the OME admission
  webhook as post-merge backstop.
- **W4 — Source-vs-concrete ambiguity.** Whether the PR should carry a single
  placement-driven *source* `InferenceService` (OEP-0018 fans it out) or a
  *concrete* per-cluster CR depends on how the GitOps/Argo flow is wired — which
  is not yet confirmed. Open Question #1.

## Design Details

> Order within: identity → the change pipeline (what, then how) → what CR we emit →
> configuration → where it runs → the feasibility anchors that make this
> mostly-assembly. Proposals are marked; facts are anchored.

### Identity: who is making this write (resolves OEP-0021's Auth Open Question)

**What must be true.** Every write is attributable to a real person, and that
attribution must be unspoofable. Authorization (who *may* write) builds on it, but
identity comes first.

**The failure mode.** A raw header like `X-Gateway-Authorization: Bearer <anything>`
is settable by any client. If `muse-writer` trusts the header's mere presence, or is
reachable off the gateway, anyone can impersonate anyone. So identity is only as
good as its verification.

**Terminology.** The gateway's *`jwt_header`* plugin injects a JWT signed with the
gateway's signing key, carrying the caller's internal user ID; resolve the
username server-side. It authenticates the *user*; it is not a GitHub credential. Do **not** conflate
identity ("who you are") with GitHub authorization ("act on GitHub as you"), which
is a separate token obtained separately (below).

**Approach (how).** Options:

  a) **Verify the gateway `jwt_header`.** MUSE is already fronted by the gateway.
     Verify the JWT signature against the gateway's published public key (accepting
     current+next `kid`, since the key rotates monthly), validate `exp` yourself
     (the sanctioned in-house crypto libraries verify the signature but *not*
     claims), and restrict `muse-writer` to the gateway with a CiliumNetworkPolicy
     so the header cannot be injected off-path. *Tradeoff:* smallest new surface,
     but the sanctioned verifier libraries have no Rust implementation — so
     `muse-writer` verifies against the exported public key with a Rust JOSE
     library, a real, bounded cost, and it must get rotation right or identity
     breaks for a few milliseconds each month.
  b) **App-run OIDC** (the ArgoCD/Kargo precedent): `muse-writer` runs its own
     corporate-IdP OIDC login (PKCE), reading `email`/`username`/`groups`.
     *Tradeoff:* richer claims (group-based authz for free) and path-independent,
     but a full OIDC + session flow to build.

We choose **(a) for v1, with (b) as the graduation target** once we need
group-based RBAC — because MUSE is already behind the gateway, so (a) reuses the
existing path and ships identity fastest. This is a lean, flagged for reviewers,
not a settled fact.

**Attribution ≠ GitHub authorization.** Verifying who you are does not grant a
GitHub token. The PR-author credential is obtained separately, in the pipeline
below.

### The change pipeline (what)

The contract: a `ChangeRequest` is `{repo + path, intent: create|patch, the CR
object or a patch, actor}`. The pipeline validates it, renders it, and opens exactly
one PR. It has a single sink — a pull request — so there is exactly one place a
write is produced, and it is always reviewable.

### The change pipeline (how)

Two stages, both anchored to sanctioned patterns:

1. **Validate without cluster-write.** We deliberately grant `muse-writer` no
   cluster verbs — not even a server dry-run, which requires `create`/`patch`
   authorization even when it does not persist. So validation is: check the rendered
   CR against the CRD's OpenAPI schema (the read plane already reads CRDs) and reuse
   OME's pure validation helpers (`pkg/validation`, which are K8s-dependency-free by
   design) where practical. The OME admission webhook is the backstop when Argo
   applies post-merge. Name the anti-pattern: **the silent bad-merge** — a CR that
   passes the form's structural checks but fails a semantic webhook rule only at
   apply time. We mitigate it (structural pre-check + webhook backstop); we do
   *not* fully eliminate it without a dry-run, and that is the stated cost of
   holding no cluster-write.
   - *Rust caveat:* `pkg/validation` is Go; `muse-writer` is Rust. v1 validates
     structurally against the CRD OpenAPI schema (a Rust JSON-schema check) and
     leaves semantic rules to the webhook; reimplementing OME's semantic validation
     in Rust is explicitly out of scope (drift risk).
2. **Render + open the PR as the user.** Use a **first-party GitHub App with the
   OAuth user-access-token flow** — a pattern an existing internal app already
   ships in production: the user does a one-time "Login with GitHub," MUSE stores
   their `ghu_` token (encrypted, server-side, short-lived), and opens the
   branch/commit/PR *as them*, so the PR is attributed to the user. If a user has
   not linked GitHub, the fallback is an installation token (`ghs_`, the existing
   bot-committer pattern) — but that PR is **bot-attributed**, which loses goal #3,
   so it is a fallback, not the default.

### What CR do we emit — source vs concrete (proposal + open question)

OEP-0021's thesis is "deploy = create one *source* `InferenceService` on the control
plane and let OEP-0018 placement fan it out." Carried into PR-per-write, that means
the PR commits a single source CR to the path Argo syncs to the control-plane
cluster, and placement does the rest — the user's "auto-place vs pin a cluster"
choice becomes placement fields on that source CR.

I recommend the **source-CR model**, because it keeps the write plane consistent
with OEP-0018 and avoids MUSE learning per-cluster layout. But it depends on a fact
I cannot yet confirm: whether the GitOps/Argo flow reconciles into a control-plane
cluster *running OEP-0018 placement*, or applies `InferenceService`/`ServingRuntime`
directly to workload clusters (as the existing deploy chart appears to). If the
latter, MUSE must emit a *concrete* per-cluster CR instead. This is Open Question #1;
it blocks nothing in slices 0–1 except the exact render target.

### Configuration (the no-magic constraint)

Per the repo's hard rule against baked-in behavioral values, everything
environment-specific is config (the `inferenceservice-config` ConfigMap / Helm
values / a CRD-style struct), with **no in-code defaults**:

- the GitOps repo and a path template per (kind, environment) — where the CR YAML
  lands;
- the gateway JWT header name + the verification-key source + the
  issuer/audience to enforce;
- the GitHub App id and OAuth client config;
- the authorization allowlist (who may open PRs, to which environments).

**Absent config, the write feature is disabled and MUSE stays read-only** — never a
magic fallback. That degradation is the safe default: a misconfigured MUSE cannot
write.

### Where `muse-writer` runs

A separate deployment in the management-plane cluster, beside `muse-server`. It
holds the GitHub App key + per-user OAuth tokens and egresses to
`github.com`/`api.github.com`. Egress is already available there: the cluster's
egress policy imports the platform's shared CI/CD domain list, whose
`(.*\.)?github\.com` regex covers both hosts — so no allowlist diff is needed.
The one requirement is that `muse-writer`'s pod opts into the cluster's pod
identity (a pod annotation); MUSE's chart today mints only the
chat-proxy mTLS cert, a *separate* credential. Verify allow/deny in the egress
proxy's audit log; a 403 after opting in is an identity gap, not a missing domain.
Do **not** set an explicit `HTTPS_PROXY` — egress on this platform is transparent,
and pointing at a forward proxy breaks it (a prior bring-up hit exactly this).

### Why this is mostly assembly

Every non-trivial piece has a sanctioned internal precedent, which is why the risk
is integration, not invention:

- identity verification → the gateway's `jwt_header` plugin + its signing-key
  infrastructure (an existing production service is a working consumer);
- PR-as-user → an existing internal app's user-token flow; bot fallback → the
  existing installation-token bot committer;
- CR-commit-for-Argo → the internal design of record and the existing deploy
  chart;
- egress → already covered by the platform's shared CI/CD domain list.

The one genuinely new piece is the Rust-side gateway-JWT verification plus the
pipeline glue.

## Test Plan

- **Identity (unit).** Verify a good gateway JWT; reject a forged signature, an
  expired token, and a wrong-issuer token; exercise `kid` rotation (current+next).
  This is the security-critical path — test the rejections, not just the happy path.
- **Pipeline mapping (unit).** `ChangeRequest` → exact CR YAML, including the create
  vs patch (scale/edit) shapes and the target path. Schema-validation rejects a
  structurally-bad CR before any PR is opened.
- **PR authoring (integration).** Against a recorded/faked GitHub API: a change
  opens one PR, on a fresh branch, with the rendered CR at the configured path,
  authored by the actor's token (and bot-attributed on the link-less fallback).
  Assert the authz allowlist blocks an unpermitted actor *before* a PR is created.
- **No browser test.** Follow OEP-0021's stance: a thin API-contract test, not an
  end-to-end UI test.
- `TODO:` a post-merge loop test (webhook rejects a semantically-bad merged CR) is
  desirable but depends on Open Question #1 (what cluster/webhook the PR targets) —
  `TBD`.

## Graduation Criteria

Sliced so the GitHub-free work ships first and the GitHub-dependent work follows
once the App + egress land:

- **Slice 0 — Identity.** Gateway `jwt_header` verification + `/v1/me` + the authz
  gate. No GitHub, no egress. Behind an internal flag.
- **Slice 1 — Create `InferenceService` → PR.** The full pipeline (validate → render
  → PR-as-user) + the GitHub App + OAuth flow. First real write. Single environment.
- **Slice 2 — Scale/edit → PR.** Patch an existing `InferenceService`.
- **Slice 3 — Create `ServingRuntime` → PR.** The expert form onto the same
  pipeline.
- **Beta / Stable:** app-run OIDC + group-based authz if identity fork (a) proves
  insufficient; Open Question #1 resolved; the authz model documented. `TBD`
  thresholds.

## Drawbacks

- **Day-2 ops are PR round-trips.** Scaling and edits are no longer instant. The
  counter: autoscaling (KEDA/HPA) handles the frequent scaling, manual edits are
  comparatively rare, and the audit/rollback Git gives is worth the latency — but it
  is a real UX cost, stated plainly.
- **`muse-writer` holds GitHub write credentials.** A new secret-custody surface
  (App key + per-user tokens) that the read-only product did not have. Isolating it
  in its own process bounds, but does not remove, that surface.
- **Another process to run.** One more deployment + gateway route. The honest
  counter is that co-locating it with the read plane trades operational simplicity
  for a worse security posture.

## Alternatives

1. **Direct-write to the control plane (OEP-0021 fork #2(a)).** *Tradeoff:* fastest
   UX, but MUSE holds cluster write-RBAC and the audit trail is whatever MUSE
   records. Rejected — it is the exact "unaudited button" failure mode this OEP
   exists to avoid.
2. **Bot-attributed PRs only (installation token).** *Tradeoff:* simplest (no
   per-user OAuth), but every PR is `[bot]`, losing goal #3. The team behind the
   precedent app hit this and added the user-token flow specifically to fix it.
   Rejected as the default; kept as the link-less fallback.
3. **OIDC → github.com token exchange.** *Tradeoff:* would avoid per-user GitHub
   OAuth entirely, but no such exchange exists today (the GitHub org shares the
   corporate SAML IdP for interactive login, but there is no OIDC→github-API token
   exchange), so it is net-new token-exchange (crypto) work. Rejected for v1.
4. **Fold the write plane into `muse-server`.** *Tradeoff:* one fewer process, but
   the read plane inherits GitHub tokens + egress. Rejected on trust-domain grounds.

## Open Questions

1. **Source vs concrete CR** (blocks the render target, not the slice 0–1
   scaffolding). Does the GitOps/Argo flow reconcile into a control-plane cluster
   running OEP-0018 placement (→ emit a source CR), or apply directly to workload
   clusters (→ emit a concrete per-cluster CR)? Owner: `TBD` (confirm with the
   deploy-chart owners).
2. **Which repo + path.** The repo the existing bot committer writes to today, or a
   dedicated config repo? Owner: `TBD`.
3. **Identity: gateway JWT vs app-run OIDC — final call.** (a) ships v1; (b) is the
   group-RBAC upgrade. When do we actually need groups? Owner: `TBD`.
4. **GitHub App onboarding owner.** A first-party App (secret-store ACLs, scheduled
   token rotation, `github.com` egress policy) must be filed with the owning
   oncall. Owner: `TBD`.
5. **Authz model.** Who may open a PR, to which environments/namespaces? v1 can start
   with an allowlist; the fuller model is `TBD`.

See also: OEP-0021 (the MUSE read+write layer this extends; this doc resolves its
fork #2 and Auth Open Question), OEP-0018 (the placement the source CR rides),
OEP-0019 (the process-split precedent for `muse-writer`).
