package rolloutspec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fixture(t *testing.T) *Machine {
	t.Helper()
	m, err := Load("_fixture")
	require.NoError(t, err)
	return m
}

func TestExpandFixtureGrid(t *testing.T) {
	g, err := fixture(t).Expand()
	require.NoError(t, err)
	assert.Equal(t, "_fixture", g.Machine)
	assert.Len(t, g.Cells, 18)

	synthesized := 0
	for _, gc := range g.Cells {
		if gc.Synthesized {
			synthesized++
			assert.Equal(t, OutcomeNA, gc.Cell.Outcome)
			assert.Equal(t, "no operation is in flight while Idle", gc.Cell.Reason)
		}
	}
	assert.Equal(t, 3, synthesized, "Idle x timer.deadline is n/a in every window")

	// Ordering: states as declared, events as declared, variant, window, transition.
	first := g.Cells[0]
	assert.Equal(t, GridCell{State: "Idle", Event: "pod.ready", Variant: "source", Window: WindowBefore, Transition: "", Cell: first.Cell}, first)
	assert.Equal(t, OutcomeTransition, first.Cell.Outcome)
	assert.Equal(t, "idle-to-working", g.Cells[1].Transition)
	assert.Equal(t, WindowBoundary, g.Cells[1].Window)
	assert.Equal(t, "Idle/pod.ready[source]/boundary@idle-to-working", g.Cells[1].Key())
}

func TestExpandMissingKeyIsAnError(t *testing.T) {
	m := fixture(t)
	m.Cells = m.Cells[1:] // drop Idle/pod.ready[source]/before
	_, err := m.Expand()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing cell")
	assert.Contains(t, err.Error(), "Idle/pod.ready[source]/before")
}

func TestExpandAmbiguousKeyIsAnError(t *testing.T) {
	m := fixture(t)
	m.Cells = append(m.Cells, Cell{State: "Working", Event: "pod.ready", Outcome: OutcomeIgnore, Status: StatusDecided, Placement: PlacementUnit})
	_, err := m.Expand()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous cell")
	assert.Contains(t, err.Error(), "_fixture/Working/pod.ready[source]/before")
}

func TestExpandRowNAConflictsWithAuthoredCell(t *testing.T) {
	m := fixture(t)
	m.Cells = append(m.Cells, Cell{State: "Idle", Event: "timer.deadline", Outcome: OutcomeIgnore, Status: StatusDecided, Placement: PlacementUnit})
	_, err := m.Expand()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous cell")
	assert.Contains(t, err.Error(), "_fixture/Idle/timer.deadline/before")
	assert.Contains(t, err.Error(), "conflicts with not_applicable")
}

func TestExpandFixtureGridKeyOrder(t *testing.T) {
	g, err := fixture(t).Expand()
	require.NoError(t, err)
	want := []string{
		"Idle/pod.ready[source]/before",
		"Idle/pod.ready[source]/boundary@idle-to-working",
		"Idle/pod.ready[source]/after@idle-to-working",
		"Idle/pod.ready[target]/before",
		"Idle/pod.ready[target]/boundary@idle-to-working",
		"Idle/pod.ready[target]/after@idle-to-working",
		"Idle/timer.deadline/before",
		"Idle/timer.deadline/boundary@idle-to-working",
		"Idle/timer.deadline/after@idle-to-working",
		"Working/pod.ready[source]/before",
		"Working/pod.ready[source]/boundary@working-to-idle",
		"Working/pod.ready[source]/after@working-to-idle",
		"Working/pod.ready[target]/before",
		"Working/pod.ready[target]/boundary@working-to-idle",
		"Working/pod.ready[target]/after@working-to-idle",
		"Working/timer.deadline/before",
		"Working/timer.deadline/boundary@working-to-idle",
		"Working/timer.deadline/after@working-to-idle",
	}
	var got []string
	for _, gc := range g.Cells {
		got = append(got, gc.Key())
	}
	assert.Equal(t, want, got)
}

func TestExpandDeadCellIsAnError(t *testing.T) {
	t.Run("cell names a transition but only windows before", func(t *testing.T) {
		m := fixture(t)
		m.Cells = append(m.Cells, Cell{
			State: "Idle", Event: "pod.ready", Variants: []string{"source"}, Windows: []string{WindowBefore},
			Transition: "idle-to-working", Outcome: OutcomeIgnore, Status: StatusDecided, Placement: PlacementUnit,
		})
		_, err := m.Expand()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dead cell Idle/pod.ready windows=[before] variants=[source] transition=idle-to-working: matches no grid key")
	})

	t.Run("boundary/after cell on a state with no outgoing transitions", func(t *testing.T) {
		m := fixture(t)
		m.Transitions = m.Transitions[:1] // Working keeps no outgoing edge
		_, err := m.Expand()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dead cell Working/timer.deadline windows=[boundary after] variants=[] transition=: matches no grid key")
	})

	t.Run("cell names a transition id that does not exist", func(t *testing.T) {
		m := fixture(t)
		m.Cells = append(m.Cells, Cell{
			State: "Working", Event: "pod.ready", Transition: "no-such-transition",
			Outcome: OutcomeIgnore, Status: StatusDecided, Placement: PlacementUnit,
		})
		_, err := m.Expand()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dead cell Working/pod.ready windows=[] variants=[] transition=no-such-transition: matches no grid key")
	})
}

func TestExpandStateWithoutOutgoingTransitions(t *testing.T) {
	m := fixture(t)
	m.Transitions = m.Transitions[:1] // Working keeps no outgoing edge
	// Working cells that named windows boundary/after now cover nothing authored; drop them.
	var kept []Cell
	for _, c := range m.Cells {
		if c.State == "Working" && c.Event == "timer.deadline" {
			continue
		}
		kept = append(kept, c)
	}
	m.Cells = append(kept, Cell{State: "Working", Event: "timer.deadline", Windows: []string{WindowBefore}, Outcome: OutcomeIgnore, Status: StatusDecided, Placement: PlacementUnit})
	g, err := m.Expand()
	require.NoError(t, err)
	synthesized := 0
	for _, gc := range g.Cells {
		if gc.State == "Working" && gc.Window != WindowBefore {
			synthesized++
			assert.True(t, gc.Synthesized)
			assert.Equal(t, ReasonNoOutgoingTransition, gc.Cell.Reason)
		}
	}
	assert.Equal(t, 6, synthesized, "Working x (pod.ready[source,target] + timer.deadline) x (boundary,after)")
}

func TestExpandTerminalStateNarrowsAuthoredCell(t *testing.T) {
	m := fixture(t)
	m.Transitions = m.Transitions[:1] // Working becomes terminal (no outgoing transition)
	var kept []Cell
	for _, c := range m.Cells {
		if c.State == "Working" && c.Event == "timer.deadline" {
			continue
		}
		kept = append(kept, c)
	}
	// Working/pod.ready (empty Windows, authored across all three) stays;
	// timer.deadline is narrowed to a single before-window cell so its
	// former boundary/after cell does not also need this update.
	m.Cells = append(kept, Cell{
		State: "Working", Event: "timer.deadline", Windows: []string{WindowBefore},
		Outcome: OutcomeIgnore, Status: StatusDecided, Placement: PlacementUnit,
	})

	g, err := m.Expand()
	require.NoError(t, err)

	byWindow := map[string]GridCell{}
	for _, gc := range g.Cells {
		if gc.State == "Working" && gc.Event == "pod.ready" && gc.Variant == "source" {
			byWindow[gc.Window] = gc
		}
	}
	require.Len(t, byWindow, 3)

	before := byWindow[WindowBefore]
	assert.False(t, before.Synthesized)
	assert.Equal(t, OutcomeIgnore, before.Cell.Outcome)

	for _, w := range []string{WindowBoundary, WindowAfter} {
		gc := byWindow[w]
		assert.True(t, gc.Synthesized)
		assert.Equal(t, OutcomeNA, gc.Cell.Outcome)
		assert.Equal(t, ReasonNoOutgoingTransition, gc.Cell.Reason)
	}
}

func TestExpandVariantQualifiedNotApplicable(t *testing.T) {
	m := fixture(t)
	// Replace the authored Idle/pod.ready[target] cell with a
	// not_applicable entry scoped to that variant only.
	var kept []Cell
	for _, c := range m.Cells {
		if c.State == "Idle" && c.Event == "pod.ready" && len(c.Variants) == 1 && c.Variants[0] == "target" {
			continue
		}
		kept = append(kept, c)
	}
	m.Cells = kept
	m.States[0].NotApplicable["pod.ready[target]"] = "no target pod exists while Idle"

	g, err := m.Expand()
	require.NoError(t, err)

	var sourceCells, targetCells int
	for _, gc := range g.Cells {
		if gc.State != "Idle" || gc.Event != "pod.ready" {
			continue
		}
		switch gc.Variant {
		case "source":
			sourceCells++
			assert.False(t, gc.Synthesized)
		case "target":
			targetCells++
			assert.True(t, gc.Synthesized)
			assert.Equal(t, OutcomeNA, gc.Cell.Outcome)
			assert.Equal(t, "no target pod exists while Idle", gc.Cell.Reason)
		}
	}
	assert.Equal(t, 3, sourceCells)
	assert.Equal(t, 3, targetCells)
}
