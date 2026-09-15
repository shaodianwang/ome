package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ome-projects/ome/tests/rolloutspec"
)

const sidecarYAML = `
history:
  - {state: Idle, event: pod.ready, variants: [source], windows: [before], transition: "", prs: ["#720", "#1423"]}
`

func TestApplyHistoryMatchesBySelector(t *testing.T) {
	m, err := rolloutspec.Load("_fixture")
	require.NoError(t, err)

	sidecar := filepath.Join(t.TempDir(), "fixture.history.yaml")
	require.NoError(t, os.WriteFile(sidecar, []byte(sidecarYAML), 0o644))

	var buf bytes.Buffer
	require.NoError(t, emitJSONWithHistory(&buf, m, []string{sidecar}))

	var env envelope
	require.NoError(t, json.Unmarshal(buf.Bytes(), &env))

	var matched, unmatched int
	for _, g := range env.Grid {
		if g.State == "Idle" && g.Event == "pod.ready" && g.Variant == "source" && g.Window == "before" {
			require.Equal(t, []string{"#720", "#1423"}, g.History)
			matched++
			continue
		}
		require.Empty(t, g.History, "cell %s should not receive unrelated history", g.ID)
		unmatched++
	}
	require.Equal(t, 1, matched)
	require.Greater(t, unmatched, 0)
}

const unmatchedSidecarYAML = `
history:
  - {state: NoSuchState, event: pod.ready, variants: [], windows: [], transition: "", prs: ["#1"]}
`

const overlappingSidecarYAML = `
history:
  - {state: Idle, event: pod.ready, variants: [source], windows: [before], transition: "", prs: ["#720", "#900"]}
  - {state: Idle, event: pod.ready, variants: [], windows: [], transition: "", prs: ["#900", "#720", "#1000"]}
`

func TestApplyHistoryDedupesPRsPreservingFirstSeenOrder(t *testing.T) {
	m, err := rolloutspec.Load("_fixture")
	require.NoError(t, err)

	sidecar := filepath.Join(t.TempDir(), "overlap.history.yaml")
	require.NoError(t, os.WriteFile(sidecar, []byte(overlappingSidecarYAML), 0o644))

	var buf bytes.Buffer
	require.NoError(t, emitJSONWithHistory(&buf, m, []string{sidecar}))

	var env envelope
	require.NoError(t, json.Unmarshal(buf.Bytes(), &env))

	var found bool
	for _, g := range env.Grid {
		if g.State == "Idle" && g.Event == "pod.ready" && g.Variant == "source" && g.Window == "before" {
			require.Equal(t, []string{"#720", "#900", "#1000"}, g.History)
			found = true
		}
	}
	require.True(t, found, "expected cell not present in grid")
}

func TestApplyHistoryUnmatchedEntryErrors(t *testing.T) {
	m, err := rolloutspec.Load("_fixture")
	require.NoError(t, err)

	sidecar := filepath.Join(t.TempDir(), "typo.history.yaml")
	require.NoError(t, os.WriteFile(sidecar, []byte(unmatchedSidecarYAML), 0o644))

	var buf bytes.Buffer
	err = emitJSONWithHistory(&buf, m, []string{sidecar})
	require.Error(t, err)
	require.Contains(t, err.Error(), sidecar)
	require.Contains(t, err.Error(), "state=NoSuchState")
	require.Contains(t, err.Error(), "event=pod.ready")
	require.Contains(t, err.Error(), "matches no grid cell")
}
