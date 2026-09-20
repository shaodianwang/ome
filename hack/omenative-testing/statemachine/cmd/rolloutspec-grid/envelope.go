package main

import (
	"encoding/json"
	"io"

	"github.com/ome-projects/ome/tests/rolloutspec"
)

// envelope is the JSON contract consumed by heatmap.py. It carries the
// declared states, events, and transitions alongside the expanded grid so
// the renderer never has to parse machine YAML.
type envelope struct {
	Machine     string          `json:"machine"`
	Scope       string          `json:"scope"`
	Frozen      bool            `json:"frozen"`
	States      []stateOut      `json:"states"`
	Events      []eventOut      `json:"events"`
	Transitions []transitionOut `json:"transitions"`
	Grid        []gridEntry     `json:"grid"`
}

type stateOut struct {
	ID        string `json:"id"`
	Phase     string `json:"phase"`
	DecidedIn string `json:"decidedIn"`
	Note      string `json:"note"`
}

type eventOut struct {
	ID          string   `json:"id"`
	Family      string   `json:"family"`
	Variants    []string `json:"variants"`
	Description string   `json:"description"`
}

type transitionOut struct {
	ID        string `json:"id"`
	From      string `json:"from"`
	On        string `json:"on"`
	Variant   string `json:"variant"`
	Guard     string `json:"guard"`
	To        string `json:"to"`
	DecidedIn string `json:"decidedIn"`
	// Lane is resolved, never blank: workload for an edge the replay
	// lane drives, adapter for one only a tagged adapter test can.
	Lane string `json:"lane"`
	Why  string `json:"why"`
}

// gridEntry flattens one expanded grid cell with the fields of the
// authored cell that produced it. Id is the canonical
// "<machine>/" + GridCell.Key() form used in reports and error messages.
type gridEntry struct {
	ID          string      `json:"id"`
	State       string      `json:"state"`
	Event       string      `json:"event"`
	Variant     string      `json:"variant"`
	Window      string      `json:"window"`
	Transition  string      `json:"transition"`
	Outcome     string      `json:"outcome"`
	To          string      `json:"to"`
	Status      string      `json:"status"`
	Note        string      `json:"note"`
	Reason      string      `json:"reason"`
	Guard       string      `json:"guard"`
	Invariants  []string    `json:"invariants"`
	Placement   string      `json:"placement"`
	Coverage    coverageOut `json:"coverage"`
	History     []string    `json:"history"`
	Synthesized bool        `json:"synthesized"`
}

// coverageOut names the checks that prove a cell, one list per coverage
// kind. Every list serializes, empty rather than absent, so a consumer
// never has to distinguish "no coverage" from "field missing".
type coverageOut struct {
	Unit   []string `json:"unit"`
	Perm   []string `json:"perm"`
	Kind   []string `json:"kind"`
	Replay []string `json:"replay"`
}

func coverageOf(c *rolloutspec.Coverage) coverageOut {
	if c == nil {
		return coverageOut{Unit: []string{}, Perm: []string{}, Kind: []string{}, Replay: []string{}}
	}
	return coverageOut{Unit: nonNil(c.Unit), Perm: nonNil(c.Perm), Kind: nonNil(c.Kind), Replay: nonNil(c.Replay)}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// buildEnvelope flattens a machine and its expanded grid into the JSON
// contract. Every GridCell carries a non-nil Cell, including synthesized
// n/a cells, so each grid entry reads its outcome fields straight off it.
// History is filled separately by applyHistory: the rolloutspec Cell type
// carries no history of its own.
func buildEnvelope(m *rolloutspec.Machine, g *rolloutspec.Grid) envelope {
	env := envelope{
		Machine:     m.Machine,
		Scope:       m.Scope,
		Frozen:      m.Frozen,
		States:      make([]stateOut, 0, len(m.States)),
		Events:      make([]eventOut, 0, len(m.Events)),
		Transitions: make([]transitionOut, 0, len(m.Transitions)),
		Grid:        make([]gridEntry, 0, len(g.Cells)),
	}
	for _, s := range m.States {
		env.States = append(env.States, stateOut{ID: s.ID, Phase: s.Phase, DecidedIn: s.DecidedIn, Note: s.Note})
	}
	for _, e := range m.Events {
		env.Events = append(env.Events, eventOut{ID: e.ID, Family: e.Family, Variants: nonNil(e.Variants), Description: e.Description})
	}
	for _, t := range m.Transitions {
		env.Transitions = append(env.Transitions, transitionOut{
			ID: t.ID, From: t.From, On: t.Event, Variant: t.Variant, Guard: t.Guard, To: t.To, DecidedIn: t.DecidedIn,
			Lane: t.LaneOf(), Why: t.Why,
		})
	}
	for _, gc := range g.Cells {
		c := gc.Cell
		entry := gridEntry{
			ID:          m.Machine + "/" + gc.Key(),
			State:       gc.State,
			Event:       gc.Event,
			Variant:     gc.Variant,
			Window:      gc.Window,
			Transition:  gc.Transition,
			Outcome:     c.Outcome,
			To:          c.To,
			Status:      c.Status,
			Note:        c.Note,
			Reason:      c.Reason,
			Guard:       c.Guard,
			Invariants:  nonNil(c.Invariants),
			Placement:   c.Placement,
			Coverage:    coverageOf(c.Coverage),
			History:     []string{},
			Synthesized: gc.Synthesized,
		}
		env.Grid = append(env.Grid, entry)
	}
	return env
}

// emitJSON renders a machine's expanded grid with no history sidecar
// applied; every grid entry's history is the empty list.
func emitJSON(w io.Writer, m *rolloutspec.Machine) error {
	return emitJSONWithHistory(w, m, nil)
}

// emitJSONWithHistory renders a machine's expanded grid and merges in every
// matching entry from the given history sidecar files.
func emitJSONWithHistory(w io.Writer, m *rolloutspec.Machine, historyPaths []string) error {
	g, err := m.Expand()
	if err != nil {
		return err
	}
	env := buildEnvelope(m, g)
	entries, err := loadHistory(historyPaths)
	if err != nil {
		return err
	}
	if err := applyHistory(&env, entries); err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(env)
}
