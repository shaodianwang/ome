# Rollout state-machine review tooling

Renders one rollout state machine from `tests/rolloutspec/machines/`
for human review. The tables under `rolloutspec` are the source of truth; everything
here is a projection of them. What the tables mean, how cells are decided, and how
tests derive from them is in `oeps/0020.1-rollout-state-machine-spec/README.md`.

`review/` holds the per-family review tables, the roll-up
(`review/REVIEW-instance.md`) and the open-PR cross-check; `audits/` holds the
per-machine event audits. They live here rather than next to the OEP because
they cite PR numbers, which the OEP tree keeps out.

## Render

    make rolloutspec-render MACHINE=instance

Writes four files under `out/` (git-ignored):

| File | What it is |
|---|---|
| `<machine>.grid.json` | The expanded (state × event × variant × window × transition) grid plus the declared states, events, and transitions. Produced by `cmd/rolloutspec-grid --format json`. |
| `<machine>.mmd` | A Mermaid `stateDiagram-v2` of the transitions. Paste into any Mermaid renderer. |
| `<machine>.md` | A narrative document: one section per state, covering how you get there, how you leave, and everything else authored for it. Meant to be read top to bottom instead of scanned like the grid or the heatmap. Produced by `cmd/rolloutspec-grid --format markdown`. |
| `<machine>.html` | The heatmap. Open in a browser. |

The CLI can be run on its own: `go run ./hack/omenative-testing/statemachine/cmd/rolloutspec-grid --machine instance --format json`.

## History sidecars

`--history <file>` (repeatable) attaches PR references to grid cells without
touching the machine tables themselves. A sidecar file looks like:

```yaml
history:
  - {state: Update/Surge, event: spec.revision, variants: [], windows: [boundary], transition: "", prs: ["#720", "#1423"]}
```

An entry whose provenance is not a merged change — an owner's review session,
say — carries a `decision: <reference>` string instead of, or alongside, `prs`;
every entry must carry at least one of the two.

An entry matches a grid cell when its `state` and `event` match exactly and its
`variants`/`windows`/`transition` selector matches (empty means "all"), the same
rule an authored cell in the machine tables uses. An entry that matches no grid
cell at all is treated as a typo and fails the render. `make rolloutspec-render`
picks up `history/<machine>.yaml` and `history/<machine>-*.yaml` next to this
README, if they exist.

`check_history.py` is the stricter pair of that check: it reads a rendered grid
JSON plus the sidecars and fails when a `review` cell has no sidecar entry, when
a sidecar entry carries neither `prs` nor a `decision` reference, or
when a sidecar entry selects only cells that are neither `review` nor `decided`
(its PRs would then never reach a reviewer, since the heatmap only shows
history on those two statuses). A `decided` cell may cite a merged PR as
provenance for its decision but is not required to.

    python3 check_history.py --grid out/instance.grid.json \
        $(for f in history/instance-*.yaml; do printf -- '--history %s ' "$f"; done)

## Test coverage

A cell and the test that proves it name each other, and `check_coverage.py`
resolves both directions against something real:

* the cell names the check, in
  `coverage: {unit: [...], perm: [...], kind: [...], replay: [...]}`.
  A `unit` entry is a Go test function name; it must exist as `func <Name>(` in
  some `*_test.go` under `pkg/` or `tests/`. A `replay` entry is a replay
  scenario name and must exist as `tests/rolloutspec/replay/scenarios/<name>.yaml`;
  that scenario's own `cells:` list is the other half of the pair, and the
  replay suite checks those ids against the grid. `perm` and `kind` entries name
  suite specs rather than Go functions and are not resolved.
* the test names the cells, in a `// Cells:` comment directly above the test
  function, ids comma-separated. An id is the grid coordinate
  `<machine>/<state>/<event>[<variant>]/<window>[@<transition>]`, exactly as the
  heatmap and the grid JSON show it. Every id cited from a `*_test.go` under
  `pkg/` must exist in the rendered grid.

```go
// Cells: instance/Update/Surge/pod.ready[target]/boundary@T-surge-target-ready
func TestSurgeHandoffPromotesOnTargetReady(t *testing.T) {
```

Both checks run together against a rendered grid:

    make rolloutspec-check MACHINE=instance

CI runs them through the "Rollout Behavior Lock" step, which is the same
checks followed by the lock's test packages:

    make rollout-behavior-lock MACHINE=instance

A name or id that does not resolve fails the target. The third output is a
report, not a gate: per machine, how many decided cells carry coverage and
which ones do not (n/a cells are left out — there is nothing to exercise).
That gap becomes an error only once a machine sets `frozen: true`, which the
`rolloutspec` validator enforces cell by cell.

## Machine invariants

`machine.yaml` may declare the machine-wide properties its cells cite:

```yaml
invariants:
  - id: surge.le.budget
    statement: the number of surge pods never exceeds the configured surge budget
```

A cell cites one by id in `invariants: [surge.le.budget]` instead of restating
it. Once a machine declares at least one, every id its cells cite must be
declared; a machine that has not written its list yet keeps validating with the
free-form ids already in its cells. The reader's view renders the list as a
table after the states overview.

## Reading the heatmap

One table per window. Rows are states in their declared order; in the `boundary`
and `after` tables a state with several outgoing transitions has one sub-row per
transition id. Columns are events, one per variant (`pod.ready[target]`).

Cell color is the outcome: green `transition`, grey `ignore`, amber `defer`,
purple `reject`, red `escalate`, white `·` for `n/a`, bright yellow `DECIDE`.
A black cell means either the grid has no entry for that coordinate (which the
well-formedness test should have rejected) or the cell's outcome isn't one the
renderer recognizes.

Cell border is the status: hatched = `review` (the bugfix history touched it;
confirm or change it), dashed = `current` (what the code does today, unconfirmed),
solid = `decided` (an owner chose it). Hover a cell for its id, target state,
transition, note, reason, guard, invariants, placement, and history. An empty
placement (synthesized cells carry none) shows as `—`.

The state and transition-sub-row columns stay pinned while you scroll a wide
table horizontally; a thicker top border marks the first row of each new
state's block. If a render reports that grid entries fall outside every
state/column/window coordinate, that's a coordinate bug in the source
machine tables, not a rendering choice — it exits non-zero rather than
silently dropping the cell.

The counts above each table are the review backlog for that window. A machine is
frozen when `DECIDE` is zero everywhere and every `review` has become `decided`.

## Tests

    go test ./hack/omenative-testing/statemachine/... -v
    python3 -m unittest discover -s hack/omenative-testing/statemachine -p '*_test.py'
