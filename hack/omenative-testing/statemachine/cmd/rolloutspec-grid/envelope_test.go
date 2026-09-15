package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ome-projects/ome/tests/rolloutspec"
)

var validWindows = map[string]bool{"before": true, "boundary": true, "after": true}

func TestEmitJSONEnvelopeShape(t *testing.T) {
	m, err := rolloutspec.Load("_fixture")
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, emitJSON(&buf, m))

	var env envelope
	require.NoError(t, json.Unmarshal(buf.Bytes(), &env))

	require.Equal(t, m.Machine, env.Machine)
	require.Len(t, env.States, len(m.States))
	require.Len(t, env.Events, len(m.Events))
	require.Len(t, env.Transitions, len(m.Transitions))
	require.NotEmpty(t, env.Grid)

	stateIDs := map[string]bool{}
	for _, s := range env.States {
		stateIDs[s.ID] = true
	}
	for _, g := range env.Grid {
		require.True(t, validWindows[g.Window], "window %q", g.Window)
		require.True(t, stateIDs[g.State], "grid state %q not declared", g.State)
		require.NotEmpty(t, g.Outcome, "grid cell %s/%s/%s has no outcome", g.State, g.Event, g.Window)
		require.NotNil(t, g.Invariants, "invariants must serialize as [] not null")
		require.NotNil(t, g.History, "history must serialize as [] not null")
	}
}

// The coverage checker reads test names straight off the grid, so every
// entry carries the three lists and each serializes as [] rather than null.
func TestEmitJSONCarriesCoverage(t *testing.T) {
	m, err := rolloutspec.Load("_fixture")
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, emitJSON(&buf, m))

	var env envelope
	require.NoError(t, json.Unmarshal(buf.Bytes(), &env))

	var named int
	for _, g := range env.Grid {
		require.NotNil(t, g.Coverage.Unit, "coverage.unit must serialize as [] not null")
		require.NotNil(t, g.Coverage.Perm)
		require.NotNil(t, g.Coverage.Kind)
		named += len(g.Coverage.Unit)
	}
	require.Positive(t, named, "the fixture records unit coverage on its decided cells")
}

func TestEmitJSONSynthesizedCellsAreNA(t *testing.T) {
	m, err := rolloutspec.Load("_fixture")
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, emitJSON(&buf, m))

	var env envelope
	require.NoError(t, json.Unmarshal(buf.Bytes(), &env))
	for _, g := range env.Grid {
		if g.Synthesized {
			require.Equal(t, "n/a", g.Outcome)
			require.Equal(t, "current", g.Status)
		}
	}
}
