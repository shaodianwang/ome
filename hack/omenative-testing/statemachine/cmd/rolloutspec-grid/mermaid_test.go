package main

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/ome-projects/ome/tests/rolloutspec"
)

func mermaidFixture() *rolloutspec.Machine {
	return &rolloutspec.Machine{
		Machine: "demo",
		States: []rolloutspec.State{
			{ID: "Ready"},
			{ID: "Update/Surge"},
			{ID: "Update/SurgeDrain"},
		},
		Transitions: []rolloutspec.Transition{
			{ID: "t-start", From: "Ready", Event: "spec.revision", Guard: "gate allows", To: "Update/Surge"},
			{ID: "t-ready", From: "Update/Surge", Event: "pod.ready", Variant: "target",
				Guard: "target Ready and serving gate on and the source still serves traffic for the whole drain window",
				To:    "Update/SurgeDrain"},
		},
	}
}

func TestEmitMermaidDeclaresAliasesAndEdges(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, emitMermaid(&buf, mermaidFixture()))
	out := buf.String()

	require.True(t, strings.HasPrefix(out, "stateDiagram-v2\n"), out)
	require.Contains(t, out, `    state "Ready" as Ready`)
	require.Contains(t, out, `    state "Update/Surge" as Update_Surge`)
	require.Contains(t, out, `    state "Update/SurgeDrain" as Update_SurgeDrain`)
	require.Contains(t, out, "    Ready --> Update_Surge : spec.revision / gate allows")
	require.Contains(t, out, "    Update_Surge --> Update_SurgeDrain : pod.ready[target] / ")
}

func TestEmitMermaidTruncatesLongGuards(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, emitMermaid(&buf, mermaidFixture()))
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.Contains(line, " / ") {
			guard := line[strings.Index(line, " / ")+3:]
			require.LessOrEqual(t, len(guard), maxGuardChars+1, "guard summary too long: %q", guard)
		}
	}
}

func TestEmitMermaidTruncatesMultibyteGuardToValidUTF8(t *testing.T) {
	m := &rolloutspec.Machine{
		Machine: "demo",
		States:  []rolloutspec.State{{ID: "A"}, {ID: "B"}},
		Transitions: []rolloutspec.Transition{
			{ID: "t1", From: "A", Event: "e", Guard: strings.Repeat("café ", 20), To: "B"},
		},
	}
	var buf bytes.Buffer
	require.NoError(t, emitMermaid(&buf, m))
	require.True(t, utf8.ValidString(buf.String()), "output must be valid UTF-8: %q", buf.String())
}

func TestEmitMermaidSanitizesLabelSyntax(t *testing.T) {
	m := &rolloutspec.Machine{
		Machine: "demo",
		States:  []rolloutspec.State{{ID: "A"}, {ID: "B"}},
		Transitions: []rolloutspec.Transition{
			{ID: "t1", From: "A", Event: "e", Guard: `set is complete; gate "on" {x} #1 %% note`, To: "B"},
		},
	}
	var buf bytes.Buffer
	require.NoError(t, emitMermaid(&buf, m))
	out := buf.String()
	require.Contains(t, out, "A --> B : e / set is complete, gate 'on' (x) 1  note")
	for _, forbidden := range []string{";", `"on"`, "#", "{", "}", "%%"} {
		require.NotContains(t, out[strings.Index(out, "A --> B"):], forbidden)
	}
}

func TestMermaidNodeID(t *testing.T) {
	require.Equal(t, "Update_Surge", mermaidNodeID("Update/Surge"))
	require.Equal(t, "Restart_CreatePods", mermaidNodeID("Restart/CreatePods"))
	require.Equal(t, "a_b_c", mermaidNodeID("a-b c"))
}
