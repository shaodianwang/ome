# OME Enhancement Proposals (OEPs)

An OEP is the design record for a substantial change to OME — a new CRD or API
surface, a controller, a cross-cutting behavior, or anything whose blast radius
or ambiguity warrants agreement *before* implementation. Small, obvious changes
don't need one; if a change would be hard to reverse or would surprise a reader
of the code later, it does.

## Layout

Each proposal is a directory `oeps/<number>-<slug>/` with two files:

```
oeps/<number>-<slug>/
├── oep.yaml      # metadata: oep-number, title, authors, status, dates, reviewers, approvers
└── README.md     # the proposal itself
```

- `oep-number` is a quoted string. New proposals get `"NNNN"` (`"0016"`,
  `"0017"`); a follow-up to an existing OEP gets `"NNNN.M"` (e.g. `"0007.1"` is a
  follow-up to OEP-0007).
- Branch the work off the slug: `feat/oep-0016-<slug>` (branch names follow
  `<type>/<desc>` or `<username>/<desc>`, enforced by the pre-commit hook).
- Status moves `provisional` → `implementable` → `implemented`; supersession is
  recorded with a `see-also`/`replaces` entry on both ends so the trail is
  bidirectional. `withdrawn` marks a proposal the owner decided not to pursue:
  the document stays as a record, nothing in it is built or planned.
- Every `oep.yaml` carries a `# state:` comment under `status:` naming the
  evidence (a code path or PR number) the status was checked against.
  [`STATUS.md`](STATUS.md) is generated from those files by `make oep-status`;
  do not hand-edit it.

## README structure

Follow the house format (see existing OEPs, e.g. `0014-runtime-revisions/`):

```
# OEP-NNNN: <Title>
<!-- toc -->
## Summary
## Motivation
### Goals
### Non-Goals
## Proposal
### <Parts / user stories / risks>
## Design Details
## Test Plan
## Graduation Criteria
## Drawbacks
## Alternatives
## Open Questions
```

## Writing style is required

**Every OEP MUST follow [`WRITING-STYLE.md`](WRITING-STYLE.md).** A design doc the
reader misreads is worse than no doc — clarity is a correctness property here, not
a matter of taste. The short version: lead with the failure mode, define terms
before using them, pair every rule with its rationale, anchor every claim to a
concrete case, calibrate your confidence, and close on the open question. Reviewers
should push back on prose that violates the guide just as they would on a design
flaw.

## See also

- [`WRITING-STYLE.md`](WRITING-STYLE.md) — the required style guide, with a
  worked before/after.
- The OEP and PR conventions in the repository's developer documentation.
