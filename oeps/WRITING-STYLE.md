# OME OEP Writing Style

**Status: required.** Every OEP in this directory must follow this guide. Clarity
is a correctness property of a design doc, not a nicety — a reader who misreads
the design builds the wrong thing.

**Tl;dr:** Write so the next engineer cannot misread you. Lead with the failure
mode, define terms before using them, pair every rule with its rationale, anchor
every claim to a concrete case, and calibrate your confidence to the evidence.
Examples below.

## The essence

Good design writing is trustworthy because it never asks the reader to take the
author's word for it: every claim is anchored to a definition, a reason, a real
incident, or an explicit "I don't know yet." You are writing to prevent the next
engineer's mistake — so you lead with what breaks, you define terms before you
use them, and you price the cost of skipping the hard work. Be calibrated, not
confident: hedge exactly as much as the evidence warrants, and emphasize the one
word that changes a sentence's meaning. Clarity comes from structure and precise
wording — not from polish, hype, or a tidy conclusion.

## Core principles

Each is a rule plus its rationale. A bare directive with no rationale is itself a
defect — the reader can't evaluate it, only obey it.

1. **Define the term before you use it.** *Rationale:* a reader who guesses your
   vocabulary will misread your design. OME is full of load-bearing terms
   (`OMENative`, `Component`, `Instance`, `RatioBalanced`, "IR-managed") — define
   each on first use, with the consequence baked in.
2. **State the failure mode, then the mitigation.** *Rationale:* a mechanism
   described without naming what breaks if it's wrong is unreviewable. Name the
   outage, the data loss, the stale state, the deadlock.
3. **Give the "why" before the "what," and the "what" before the "how."**
   *Rationale:* motivation and required properties let the reader judge whether
   the mechanism is even the right shape. Defer implementation explicitly
   ("mechanism in the next section").
4. **Separate "what" from "how", and proposal from fact.** *Rationale:*
   conflating the contract with its implementation, or a wish with a shipped
   behavior, hides exactly where the real gaps are. Keep them in visibly separate
   sections.
5. **Anchor every abstract claim to a concrete case** — a number, a mechanism, or
   a named PR/issue. *Rationale:* "e.g." is load-bearing, not decoration; an
   unanchored principle can't be checked.
6. **Quantify instead of waving.** *Rationale:* "fast", "scalable", "a lot",
   "soon" are not specs. Use a number, a range, or an explicit `TBD`.
7. **Calibrate confidence explicitly.** *Rationale:* state contestable choices as
   choices, not settled fact. Hedge when you're unsure ("probably", "to a first
   approximation"); mark gaps `TBD`/`TODO` rather than papering over them.
8. **Bound your claims.** *Rationale:* reserve absolutes for things backed by an
   incident or an existing standard; otherwise scope with "usually", "for this
   use case", "in most cases."
9. **Anticipate the objection and answer it head-on.** *Rationale:* voicing the
   smart reader's hardest question — in their words — and then answering it
   (even with "we don't have an answer yet") is how a doc earns trust.
10. **Present alternatives as an enumerated menu with per-option tradeoffs, then
    state your pick and why.** *Rationale:* a single silver bullet hides the
    decision; the reader needs to see what was rejected and at what cost.
11. **Name the anti-pattern so it becomes reusable.** *Rationale:* a labeled
    failure mode is one the team can recognize next time.
12. **Don't hide the document's state.** *Rationale:* leave visible `TODO`/`TBD`
    markers and date claims that can go stale, so readers can judge freshness and
    know what's still open.
13. **Close on the next action or the open question, not a summary flourish.**
    *Rationale:* a recap adds nothing; an owner + action, or a named open
    question, moves the work forward.

## Voice & sentence mechanics

- **Use a wide dynamic range.** Punch a long, qualified explanatory sentence
  closed with a short flat verdict. The contrast is the rhythm.
- **First person for opinion, "we" for shared responsibility.** Tag judgment
  explicitly ("I recommend", "I'm skeptical that…"). Use "we"/"our users" to fold
  the reader into the duty.
- **Active voice, named actor.** Avoid "it was decided"; say who decided, or which
  PR/issue did. The reconciler does X — not "X is done."
- **Em-dashes and parentheticals carry precision, not rambling.** Insert the
  caveat, the gloss, or the example mid-sentence — then get out.
- **Make "i.e." and "e.g." do real work.** "i.e." restates the prior sentence
  more precisely or more bluntly; "e.g." grounds it in a concrete instance.
- **Use the colon as setup-then-payoff.** State the principle, colon, deliver the
  sharp version.
- **Emphasize the one load-bearing word** ("not", "only", "all", "must",
  "never") so the reader can't miss the qualifier that changes the meaning.
- **Open a sentence with a signpost at the argument's joints:** "However,",
  "Therefore,", "In particular,", "Note that", "Worse:".
- **No emoji, no hype, no throat-clearing, no exclamation marks.** Start in the
  middle of the problem. Emphasis comes from word choice and structure. A
  sentence either carries weight or gets cut.

## How to structure an OEP

The house format (Summary → Motivation/Goals/Non-Goals → Proposal → Design
Details → …) is the scaffolding. Within each section, order ideas this way:

> definitions → why (the failure mode / the stakes) → what (required properties /
> the contract) → how (mechanism) → tradeoffs → anticipated objections → open
> questions.

Use the right tool for each job:

- **Numbered / nested lists** for taxonomies, principles, and option menus — each
  item is claim + rationale + a concrete example.
- **Tables** only for genuinely tabular reasoning: option-vs-tradeoff matrices,
  A-vs-B deltas, a probability×impact grid.
- **Analogies** sparingly and functionally — to anchor the unfamiliar in the
  familiar, never as decoration.
- **Anticipated objections** as explicit Q&A when you're persuading.

Open with a `Tl;dr:` one-liner or a one-sentence statement of scope — skip the
throat-clearing. Close by routing forward: the next action and its owner, a
`See also:` pointer, or an honest open question — and scope the claim's domain on
the way out ("this applies to OMENative; RawDeployment is unaffected").

### Reusable section skeleton

```
## <Plain noun-phrase title; imperative if it states a principle>

Tl;dr: <the single sharp claim>

### Goal and context
<Why this matters — the failure mode / the stakes — before any solution.>
<What is IN and OUT of scope.>

### Terminology
<Term : precise definition, with the consequence baked in.>

### What must be true
<Required properties / the contract. Keep "what" separate from "how".>

### Approach (how)
Options:
  a) <option> — Tradeoff: <downside>
  b) <option> — Tradeoff: <downside>
We choose (a), because <reason>.   [Mechanism; defer deep detail, signposted.]

### Failure modes & mitigations
<For each: what breaks -> mitigation. Say "mitigation, not full fix" where true.>

### Anticipated objections
Q. <the reader's hardest question, in their words>
A. <answer, or an honest "we don't have one yet">

### Open questions / next steps
<Owner + action, or a labeled TODO/TBD. Scope of applicability.>
```

## Signature moves worth borrowing

Each illustrated with an OME-native example.

- **Blunt verdict, then the consequence.** Don't just say a design is wrong — say
  what it costs.
  > "A single-replica `RecreatePod` with `maxUnavailable: 0` doesn't roll — it
  > deadlocks: the gate won't take the only pod offline, and there's no surge to
  > replace it. So the default is 25%, not 0."

- **Price the cost of skipping the work.** A conditional that punctures a
  comforting assumption.
  > "If a component's deployment mode is only inferred and never validated, a
  > typo in `spec.deploymentMode` silently falls back to RawDeployment — and every
  > `lifecycle`/`rollout` knob is ignored with no error. Convenience becomes a
  > silent footgun."

- **Voice the objection, then concede honestly.** Credibility comes from admitted
  gaps.
  > "A reader will ask: 'If coordination already does blue-green and sequential,
  > why reserve a separate canary field at all?' Honest answer: we don't have the
  > progressive-traffic executor yet — the field is shape-only, admission-rejected,
  > and reserved precisely so we don't fork the rollout surface later."

- **Definition-first, with the consequence baked in.**
  > "An *atomic multi-pod group* is a set of pods that must start, stop, and
  > migrate together (an OMENative Instance, or a legacy LWS group). Evicting one
  > without the others leaves a half-broken workload — which is why eviction is
  > unsafe here."

- **Separate the accident from the guarantee.**
  > "Today every Instance lands on one node, but that's an observation of common
  > practice, not a requirement the design depends on."

- **Generalize, then immediately bound the cost.**
  > "Auto-selecting the runtime is fully general — and that generality has a cost:
  > it needs `spec.model` to match against, so a model-less service must name its
  > runtime explicitly."

## Anti-patterns to drop

Mapped onto habits visible in our current OEPs:

- **Jargon as gatekeeping.** Dropping `OMENative`/`IR-managed`/`RatioBalanced`
  with no definition. → Never drop a term without a definition or an "i.e."
  restatement.
- **Mechanism with no failure mode.** Describing *how* the controller behaves but
  burying *what breaks* if it's wrong. → Name the consequence alongside every
  mechanism.
- **Directive with no rationale.** "The reconciler does X" with no "because". →
  Pair every rule with its rationale.
- **Unanchored abstraction.** Sweeping claims with no number, mechanism, or
  PR/issue. → Drag every principle down to a concrete case.
- **False certainty.** Stating contestable choices as settled fact. → Hedge to
  match the evidence; mark unknowns `TBD`.
- **Conflating "what" with "how" and proposal with fact.** → Keep them in
  visibly separate sections.
- **Walls of undifferentiated prose.** → Chunk with headings, lists, and emphasis
  on the load-bearing word.
- **Burying the lede / tidy reassuring conclusions.** → Lead with `Tl;dr:`; close
  on the open question or next action.
- **Vague quantifiers and adjectival hype** ("fast", "seamless", "robust"). → A
  number, a range, or `TBD`; claims are sourced, not sold.
- **Pretending the doc is finished.** → Leave visible `TODO`s and date claims that
  can go stale.

## Before / after

A real paragraph from an early draft of OEP-0008, rewritten to this guide.

**Before:**

> The central design choice: Alfred is primarily an observer + recommender.
> Execution is delegated to OMENative (for multi-pod workloads) or the K8s
> Eviction API (for single-pod workloads). Alfred does not itself orchestrate pod
> lifecycle beyond that narrow surface. Rationale: Eviction is destructive and
> races with other cluster activity. For atomic multi-pod groups (OMENative
> Instances and legacy LWS groups), eviction is unsafe. Safe migration requires
> the workload controller (OMENative), which owns the lifecycle. Alfred's failure
> modes are bounded: a bad recommendation can be rejected by OMENative; a bad
> eviction results in normal K8s pod restart.

**After:**

> **Tl;dr:** Alfred observes and recommends; it does not move pods itself. It
> hands execution to whoever already owns the workload's lifecycle — because pod
> eviction is destructive, and getting it wrong on a multi-pod group breaks the
> workload. Alfred should never be the thing that does the breaking.
>
> **Definition.** An *atomic multi-pod group* is a set of pods that must start,
> stop, and migrate together (an OMENative Instance, or a legacy LWS group);
> evicting one without the others leaves a half-broken workload.
>
> **The choice, and why.** Alfred is an observer and recommender. Execution is
> delegated:
> - Multi-pod workloads → **OMENative**, the controller that owns their lifecycle.
> - Single-pod workloads → the **K8s Eviction API**.
>
> Why not let Alfred orchestrate lifecycle directly? Eviction races with other
> cluster activity, and for an atomic group it is *unsafe*: evicting one pod out
> from under the group corrupts the workload. Safe migration of a group requires
> the controller that owns it — so that's where the work belongs.
>
> **Failure modes are bounded — by design.** A bad *recommendation* is rejected
> by OMENative: nothing happens. A bad *eviction* triggers a normal K8s pod
> restart: existing recovery handles it. Note: this bound holds *because* Alfred
> never touches multi-pod lifecycle directly. The day Alfred starts orchestrating
> pods itself, the guarantee is gone.

What changed: the load-bearing reason ("getting eviction wrong breaks the
workload") moved to the front; the term is defined before it's used, with its
consequence; the run-on became a delegation list and an explicit failure-mode
list; and the design's hidden assumption — that the safety bound depends on the
narrow surface — is now stated out loud.
