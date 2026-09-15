package main

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/ome-projects/ome/tests/rolloutspec"
)

// maxGuardChars bounds the guard text on an edge label. Mermaid renders
// every label inline, so a full guard sentence makes the diagram unreadable;
// the JSON envelope carries the complete guard.
const maxGuardChars = 60

// mermaidNodeID maps a state id onto a Mermaid-safe identifier. Mermaid
// node ids allow only word characters, so "Update/Surge" becomes
// "Update_Surge" and the original id is kept as the display alias.
func mermaidNodeID(stateID string) string {
	var b strings.Builder
	for _, r := range stateID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// ellipsis is multi-byte in UTF-8, so the cutoff leaves room for it: the
// truncated result never exceeds maxGuardChars+1 bytes.
const ellipsis = "…"

// labelSanitizer drops the characters Mermaid's stateDiagram lexer treats
// as syntax inside a transition description: ';' ends a statement, '"' and
// '#' open quoting and entity forms, '{' '}' open blocks, and "%%" starts a
// comment. Replacing them keeps the description as plain text.
var labelSanitizer = strings.NewReplacer(";", ",", "\"", "'", "#", "", "{", "(", "}", ")", "%%", "")

func guardSummary(guard string) string {
	guard = labelSanitizer.Replace(strings.Join(strings.Fields(guard), " "))
	if len(guard) <= maxGuardChars {
		return guard
	}
	cut := maxGuardChars - (len(ellipsis) - 1)
	for cut > 0 && !utf8.RuneStart(guard[cut]) {
		cut--
	}
	return guard[:cut] + ellipsis
}

// emitMermaid writes the machine's transitions as a stateDiagram-v2. States
// are declared in their authored order so the diagram's layout is stable
// across renders.
func emitMermaid(w io.Writer, m *rolloutspec.Machine) error {
	var b strings.Builder
	b.WriteString("stateDiagram-v2\n")
	for _, s := range m.States {
		fmt.Fprintf(&b, "    state %q as %s\n", s.ID, mermaidNodeID(s.ID))
	}
	for _, t := range m.Transitions {
		label := t.Event
		if t.Variant != "" {
			label += "[" + t.Variant + "]"
		}
		if g := guardSummary(t.Guard); g != "" {
			label += " / " + g
		}
		fmt.Fprintf(&b, "    %s --> %s : %s\n", mermaidNodeID(t.From), mermaidNodeID(t.To), label)
	}
	_, err := io.WriteString(w, b.String())
	return err
}
