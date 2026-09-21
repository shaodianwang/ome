package rolloutspec

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// splitEvent parses an ownership event id into the machine's event id and
// variant: "pod.unschedulable[source]" is the source variant of the
// pod.unschedulable ev, "api.quotaDenied" is the whole ev.
func splitEvent(ev types.Event) (event, variant string) {
	s := string(ev)
	open := strings.IndexByte(s, '[')
	if open < 0 {
		return s, ""
	}
	return s[:open], strings.TrimSuffix(s[open+1:], "]")
}

// rowForState builds the InstanceStatus one state of the machine describes,
// from the state's own declared phase and operation.
func rowForState(s State) *types.InstanceStatus {
	row := &types.InstanceStatus{Phase: types.InstancePhase(s.Phase)}
	if s.Op != nil {
		row.Operation = &types.InstanceOperation{
			Type: types.InstanceOperationType(s.Op.Type),
			Step: s.Op.Step,
		}
	}
	return row
}

// TestOwnerTableMatchesInstanceMachine holds the engine's ownership table
// and the Instance machine to the same vocabulary: every state the machine
// declares is a state the table answers for and resolves to from the
// state's own (phase, operation), every event the table names is an event
// the machine declares, and neither side carries an id the other does not.
func TestOwnerTableMatchesInstanceMachine(t *testing.T) {
	m, err := Load("instance")
	require.NoError(t, err)

	declared := map[string]State{}
	for _, s := range m.States {
		declared[s.ID] = s
	}
	answered := map[string]bool{}
	for _, state := range types.OwnershipStates() {
		answered[string(state)] = true
		s, ok := declared[string(state)]
		require.Truef(t, ok, "ownership table names state %q, which the instance machine does not declare", state)
		require.Equalf(t, state, types.StateOf(rowForState(s)),
			"state %q: the machine's own phase and operation do not resolve to it", state)
	}
	for id := range declared {
		require.Truef(t, answered[id], "instance machine declares state %q, which the ownership table does not answer for", id)
	}

	events := map[string]Event{}
	for _, e := range m.Events {
		events[e.ID] = e
	}
	for _, ev := range types.OwnershipEvents() {
		event, variant := splitEvent(ev)
		e, ok := events[event]
		require.Truef(t, ok, "ownership event %q names event %q, which the instance machine does not declare", ev, event)
		if variant == "" {
			continue
		}
		require.Containsf(t, e.Variants, variant,
			"ownership event %q names a variant %q the instance machine does not declare", ev, variant)
	}
}

// TestInterruptibleMatchesBeforeWindow holds the table's interrupt column to
// the machine's before-window cells. The before window is the row's state
// with no transition of its own mid-commit: a cell that fires a transition
// or an escalation there is an event acting on the row while its owner's
// step is still in flight, which is what Interruptible reports. Any other
// outcome — the observation is recorded, deferred, or cannot occur — leaves
// the row to its owner until the step reaches a boundary.
func TestInterruptibleMatchesBeforeWindow(t *testing.T) {
	m, err := Load("instance")
	require.NoError(t, err)
	grid, err := m.Expand()
	require.NoError(t, err)

	events := map[string]types.Event{}
	for _, ev := range types.OwnershipEvents() {
		event, variant := splitEvent(ev)
		events[event+"\x00"+variant] = ev
	}

	checked := 0
	for _, gc := range grid.Cells {
		if gc.Window != WindowBefore {
			continue
		}
		ev, ok := events[gc.Event+"\x00"+gc.Variant]
		if !ok {
			continue
		}
		acts := gc.Cell.Outcome == OutcomeTransition || gc.Cell.Outcome == OutcomeEscalate
		require.Equalf(t, acts, types.StateInterruptible(types.RowState(gc.State), ev),
			"%s: before-window outcome %q disagrees with the ownership table", gc.Key(), gc.Cell.Outcome)
		checked++
	}
	require.Equal(t, len(types.OwnershipStates())*len(types.OwnershipEvents()), checked,
		"every (state, event) of the ownership table must have a before-window cell")
}

// TestInterruptingEventHasAnArrow ties the interrupt column back to
// transitions.yaml: an event that takes a row out from under its owner does
// so through an outgoing arrow of that state, so the table can never claim
// an interrupt the machine has no edge for.
func TestInterruptingEventHasAnArrow(t *testing.T) {
	m, err := Load("instance")
	require.NoError(t, err)

	arrows := map[string]bool{}
	for _, tr := range m.Transitions {
		arrows[tr.From+"\x00"+tr.Event] = true
	}
	for _, state := range types.OwnershipStates() {
		for _, ev := range types.OwnershipEvents() {
			if !types.StateInterruptible(state, ev) {
				continue
			}
			event, _ := splitEvent(ev)
			require.Truef(t, arrows[string(state)+"\x00"+event],
				"ownership table says %s/%s interrupts, but the machine declares no arrow", state, ev)
		}
	}
}
