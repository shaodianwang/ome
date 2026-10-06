package rolloutspec

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := FindRepoRoot()
	require.NoError(t, err)
	return root
}

func joined(errs []error) string {
	var b strings.Builder
	for _, e := range errs {
		b.WriteString(e.Error())
		b.WriteString("\n")
	}
	return b.String()
}

func TestValidateFixtureIsClean(t *testing.T) {
	assert.Empty(t, Validate(fixture(t), repoRoot(t)))
}

func TestValidateViolations(t *testing.T) {
	cases := map[string]struct {
		mutate func(m *Machine)
		want   string
	}{
		"duplicate state id": {
			mutate: func(m *Machine) { m.States = append(m.States, m.States[0]) },
			want:   `duplicate state id "Idle"`,
		},
		"cell references unknown event": {
			mutate: func(m *Machine) { m.Cells[0].Event = "pod.gone" },
			want:   `unknown event "pod.gone"`,
		},
		"cell references unknown variant": {
			mutate: func(m *Machine) { m.Cells[0].Variants = []string{"sidecar"} },
			want:   `unknown variant "sidecar"`,
		},
		"outcome outside vocabulary": {
			mutate: func(m *Machine) { m.Cells[0].Outcome = "maybe" },
			want:   `outcome "maybe"`,
		},
		"transition outcome without declared to": {
			mutate: func(m *Machine) { m.Cells[0].To = "Nowhere" },
			want:   `to "Nowhere"`,
		},
		"n/a without reason": {
			mutate: func(m *Machine) { m.Cells[3].Outcome = OutcomeNA },
			want:   "n/a requires a reason",
		},
		"decided without placement": {
			mutate: func(m *Machine) { m.Cells[3].Placement = "" },
			want:   "decided cell requires placement",
		},
		"decided_in path missing": {
			mutate: func(m *Machine) { m.States[0].DecidedIn = "pkg/does/not/exist.go" },
			want:   "pkg/does/not/exist.go",
		},
		"state decided_in empty": {
			mutate: func(m *Machine) { m.States[0].DecidedIn = "" },
			want:   "state Idle: decided_in is empty",
		},
		"transition decided_in empty": {
			mutate: func(m *Machine) { m.Transitions[0].DecidedIn = "" },
			want:   "transition idle-to-working: decided_in is empty",
		},
		"frozen machine with DECIDE": {
			mutate: func(m *Machine) { m.Cells[3].Outcome = OutcomeDecide },
			want:   "frozen machine has DECIDE",
		},
		"DECIDE without the open question": {
			mutate: func(m *Machine) {
				m.Frozen = false
				m.Cells[3].Outcome = OutcomeDecide
				m.Cells[3].Note = ""
			},
			want: "DECIDE requires a note",
		},
		"not_applicable unknown event": {
			mutate: func(m *Machine) { m.States[0].NotApplicable["timer.never"] = "x" },
			want:   `not_applicable "timer.never"`,
		},
		"transition from unknown state": {
			mutate: func(m *Machine) { m.Transitions[0].From = "Ghost" },
			want:   `unknown state "Ghost"`,
		},
		"cell names transition of another state": {
			mutate: func(m *Machine) { m.Cells[0].Transition = "working-to-idle" },
			want:   `transition "working-to-idle" does not leave state "Idle"`,
		},
		"op present with blank type is invalid": {
			mutate: func(m *Machine) { m.States[1].Op = &OpRef{} },
			want:   "op: {}",
		},
		"applies present with both lists empty is invalid": {
			mutate: func(m *Machine) { m.States[1].Applies = &Applies{} },
			want:   "applies: {}",
		},
		"machine with no states": {
			mutate: func(m *Machine) { m.States = nil },
			want:   "must declare at least one state",
		},
		"machine with no events": {
			mutate: func(m *Machine) { m.Events = nil },
			want:   "must declare at least one event",
		},
		"duplicate event id": {
			mutate: func(m *Machine) { m.Events = append(m.Events, m.Events[0]) },
			want:   `duplicate event id "pod.ready"`,
		},
		"blank event id": {
			mutate: func(m *Machine) { m.Events[0].ID = "" },
			want:   "blank event id",
		},
		"duplicate transition id": {
			mutate: func(m *Machine) { m.Transitions = append(m.Transitions, m.Transitions[0]) },
			want:   `duplicate transition id "idle-to-working"`,
		},
		"blank transition id": {
			mutate: func(m *Machine) { m.Transitions[0].ID = "" },
			want:   "blank transition id",
		},
		"transition unknown to": {
			mutate: func(m *Machine) { m.Transitions[0].To = "GhostTo" },
			want:   `unknown state "GhostTo"`,
		},
		"transition unknown event": {
			mutate: func(m *Machine) { m.Transitions[0].Event = "no.such.event" },
			want:   `unknown event "no.such.event"`,
		},
		"transition unknown variant": {
			mutate: func(m *Machine) { m.Transitions[0].Variant = "ghostvariant" },
			want:   `unknown variant "ghostvariant"`,
		},
		"transition decided_in missing": {
			mutate: func(m *Machine) { m.Transitions[0].DecidedIn = "pkg/rollout/does-not-exist.go" },
			want:   "pkg/rollout/does-not-exist.go",
		},
		"not_applicable unknown variant": {
			mutate: func(m *Machine) { m.States[0].NotApplicable["pod.ready[bogus]"] = "some reason" },
			want:   `not_applicable "pod.ready[bogus]" references unknown variant`,
		},
		"not_applicable empty reason": {
			mutate: func(m *Machine) { m.States[0].NotApplicable["timer.deadline"] = "" },
			want:   `not_applicable "timer.deadline" has no reason`,
		},
		"cell unknown state": {
			mutate: func(m *Machine) { m.Cells[0].State = "GhostState" },
			want:   `unknown state "GhostState"`,
		},
		"cell window outside vocabulary": {
			mutate: func(m *Machine) { m.Cells[0].Windows = []string{"before", "someday"} },
			want:   `window "someday" is not before|boundary|after`,
		},
		"cell unknown transition id": {
			mutate: func(m *Machine) { m.Cells[0].Transition = "ghost-transition" },
			want:   `unknown transition "ghost-transition"`,
		},
		"placement outside vocabulary": {
			mutate: func(m *Machine) { m.Cells[0].Placement = "moon" },
			want:   `placement "moon" is not unit|perm|kind|sim`,
		},
		"status outside vocabulary": {
			mutate: func(m *Machine) { m.Cells[0].Status = "guessed2" },
			want:   `status "guessed2" is not current|review|decided`,
		},
		"expand failure surfaces through validate": {
			mutate: func(m *Machine) { m.Cells = m.Cells[1:] },
			want:   "Idle/pod.ready[source]/before",
		},
		"frozen machine decided cell without coverage": {
			mutate: func(m *Machine) { m.Cells[0].Coverage = nil },
			want:   "decided cell has no test coverage",
		},
		"frozen machine decided cell with empty coverage lists": {
			mutate: func(m *Machine) { m.Cells[0].Coverage = &Coverage{} },
			want:   "decided cell has no test coverage",
		},
		"uncovered cell is named in selector form": {
			mutate: func(m *Machine) { m.Cells[0].Coverage = nil },
			want:   "cell Idle/pod.ready windows=[before] variants=[source] transition=",
		},
		"blank invariant id": {
			mutate: func(m *Machine) { m.Invariants[0].ID = "" },
			want:   "blank invariant id",
		},
		"duplicate invariant id": {
			mutate: func(m *Machine) { m.Invariants = append(m.Invariants, m.Invariants[0]) },
			want:   "duplicate invariant id",
		},
		"cell cites an undeclared invariant": {
			mutate: func(m *Machine) { m.Cells[0].Invariants = []string{"no.such.invariant"} },
			want:   `invariant "no.such.invariant" is not declared by the machine`,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			m := fixture(t)
			tc.mutate(m)
			got := joined(Validate(m, repoRoot(t)))
			assert.Contains(t, got, tc.want)
		})
	}
}

// A planned state is declared ahead of the engine, so a frozen machine
// keeps its DECIDE cells there; the "frozen machine with DECIDE" case
// above holds the rule for a state the engine resolves. The fixture's two
// states form a cycle, so both are marked planned to keep every arrow
// legal.
func TestValidateFrozenAllowsDecideOnPlannedState(t *testing.T) {
	m := fixture(t)
	m.States[0].Planned, m.States[1].Planned = true, true
	m.Cells[3].Outcome = OutcomeDecide
	assert.Empty(t, Validate(m, repoRoot(t)))
}

// Only another planned state may name a planned state as its to: an
// arrow the engine takes lands in a state the engine resolves.
func TestValidatePlannedStateIsEnteredOnlyFromPlannedStates(t *testing.T) {
	m := fixture(t)
	m.States[1].Planned = true
	got := joined(Validate(m, repoRoot(t)))
	assert.Contains(t, got, `transition idle-to-working: lands in planned state "Working" from "Idle", which the engine resolves`)
	assert.NotContains(t, got, "transition working-to-idle")

	m = fixture(t)
	m.States[0].Planned = true
	m.States[1].Planned = true
	assert.Empty(t, Validate(m, repoRoot(t)))
}

func TestValidateNonFrozenAllowsDecide(t *testing.T) {
	m := fixture(t)
	m.Frozen = false
	m.Cells[3].Outcome = OutcomeDecide
	assert.Empty(t, Validate(m, repoRoot(t)))
}

// A non-frozen machine is still being authored, so an uncovered decided
// cell is a gap the coverage report surfaces, not a well-formedness error.
func TestValidateNonFrozenAllowsUncoveredDecidedCell(t *testing.T) {
	m := fixture(t)
	m.Frozen = false
	m.Cells[0].Coverage = nil
	assert.Empty(t, Validate(m, repoRoot(t)))
}

// An n/a cell describes a key that cannot occur, so there is nothing for a
// test to exercise.
func TestValidateFrozenExemptsNAFromCoverage(t *testing.T) {
	m := fixture(t)
	m.Cells[2].Outcome = OutcomeNA
	m.Cells[2].Reason = "no target pod exists while Idle"
	m.Cells[2].Coverage = nil
	assert.Empty(t, Validate(m, repoRoot(t)))
}

// A machine that has not written its invariant list yet keeps validating
// with the free-form ids its cells already cite.
func TestValidateCellInvariantsUncheckedWhenMachineDeclaresNone(t *testing.T) {
	m := fixture(t)
	m.Invariants = nil
	m.Cells[0].Invariants = []string{"surge.le.budget"}
	assert.Empty(t, Validate(m, repoRoot(t)))
}

func TestValidateReportsAllViolations(t *testing.T) {
	m := fixture(t)
	m.Cells[0].Outcome = "maybe"
	m.Cells[1].Status = "guessed"
	errs := Validate(m, repoRoot(t))
	assert.GreaterOrEqual(t, len(errs), 2)
}

// embeddedMachines lists the machine directories under machines/ that the
// gate validates one by one, beside the fixture. Whether a machine is frozen
// is read from its machine.yaml, not restated here: a machine still being
// authored may carry DECIDE cells and uncovered decided cells, a frozen one
// may not. A directory missing from this list, or a name with no directory,
// fails the test so the list and the tree cannot drift apart.
var embeddedMachines = []string{"instance", "run", "coordination", "canary", "revision"}

func TestEmbeddedMachinesValidate(t *testing.T) {
	root := repoRoot(t)
	for _, name := range embeddedMachines {
		t.Run(name, func(t *testing.T) {
			m, err := Load(name)
			require.NoError(t, err)
			assert.Equal(t, name, m.Machine)
			assert.Empty(t, joined(Validate(m, root)))
			planned := plannedStates(m)
			decide := 0
			for _, c := range m.Cells {
				if c.Outcome == OutcomeDecide && !planned[c.State] {
					decide++
				}
			}
			if m.Frozen {
				assert.Zero(t, decide, "a frozen machine carries no DECIDE cell outside a planned state")
			}
			t.Logf("%s: frozen=%v states=%d events=%d transitions=%d cells=%d decide=%d",
				name, m.Frozen, len(m.States), len(m.Events), len(m.Transitions), len(m.Cells), decide)
		})
	}
}

func TestEmbeddedMachinesListMatchesTree(t *testing.T) {
	machines, err := LoadAll()
	require.NoError(t, err)
	var found []string
	for _, m := range machines {
		if strings.HasPrefix(m.Machine, "_") {
			continue
		}
		found = append(found, m.Machine)
	}
	assert.ElementsMatch(t, embeddedMachines, found)
}
