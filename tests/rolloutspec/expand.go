package rolloutspec

import (
	"errors"
	"fmt"
	"strings"
)

// ReasonNoOutgoingTransition is the synthesized reason for a boundary or
// after cell on a state with no outgoing transition: there is no guard to
// have passed or landed, so the window cannot occur.
const ReasonNoOutgoingTransition = "no outgoing transition"

// GridCell is one fully keyed cell of the expanded grid. Transition is empty
// for the before window and names one outgoing transition otherwise.
// Synthesized marks cells the expander produced from a row-level
// not_applicable entry or from a state with no outgoing transition.
type GridCell struct {
	State      string
	Event      string
	Variant    string
	Window     string
	Transition string
	// Cell points into the Machine's Cells for an authored key, or at a
	// cell Expand synthesized for this key otherwise. Callers must not
	// mutate it through the grid.
	Cell        *Cell
	Synthesized bool
}

// Grid is the exhaustive expansion of one machine, in deterministic order:
// states as declared, events as declared, variants as declared, windows in
// before/boundary/after order, transitions as declared.
type Grid struct {
	Machine string
	Cells   []GridCell
}

// Key renders the grid coordinate the way error messages and reports show it.
func (g GridCell) Key() string {
	s := g.State + "/" + g.Event
	if g.Variant != "" {
		s += "[" + g.Variant + "]"
	}
	s += "/" + g.Window
	if g.Transition != "" {
		s += "@" + g.Transition
	}
	return s
}

// Expand materializes every (state, event, variant, window, transition) key.
// It fails when any key is covered by no cell or by more than one, and when
// any authored cell matches no key at all, so neither an unconsidered
// combination nor a cell that can never fire can hide behind a default. A
// row-level not_applicable entry takes precedence over the ReasonNoOutgoingTransition
// synthesis for boundary and after windows, so one row never carries two
// different reasons for the same unreachable key. An authored cell on a
// state with no outgoing transitions contributes only its before key: any
// boundary or after window it names is discarded without error, since
// expansion synthesizes those windows as n/a directly.
func (m *Machine) Expand() (*Grid, error) {
	outgoing := map[string][]string{}
	for _, t := range m.Transitions {
		outgoing[t.From] = append(outgoing[t.From], t.ID)
	}
	g := &Grid{Machine: m.Machine}
	matched := make([]bool, len(m.Cells))
	var errs []error
	for si := range m.States {
		s := &m.States[si]
		for _, e := range m.Events {
			variants := e.Variants
			if len(variants) == 0 {
				variants = []string{""}
			}
			for _, v := range variants {
				for _, w := range Windows {
					transitions := []string{""}
					if w != WindowBefore {
						transitions = outgoing[s.ID]
						if len(transitions) == 0 {
							reason := ReasonNoOutgoingTransition
							if r, _, ok := rowNA(s, e.ID, v); ok {
								reason = r
							}
							g.Cells = append(g.Cells, synthesizedNA(s.ID, e.ID, v, w, "", reason))
							continue
						}
					}
					for _, tr := range transitions {
						gc, err := m.resolve(s, e.ID, v, w, tr, matched)
						if err != nil {
							errs = append(errs, err)
							continue
						}
						g.Cells = append(g.Cells, gc)
					}
				}
			}
		}
	}
	for i, c := range m.Cells {
		if matched[i] {
			continue
		}
		errs = append(errs, fmt.Errorf("rolloutspec: %s: dead cell %s/%s %s: matches no grid key", m.Machine, c.State, c.Event, selectorOf(&c)))
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return g, nil
}

func (m *Machine) resolve(s *State, event, variant, window, transition string, matched []bool) (GridCell, error) {
	key := GridCell{State: s.ID, Event: event, Variant: variant, Window: window, Transition: transition}
	var matches []*Cell
	for i := range m.Cells {
		c := &m.Cells[i]
		if c.State == s.ID && c.Event == event && Selects(c.Variants, c.Windows, c.Transition, key) {
			matches = append(matches, c)
			matched[i] = true
		}
	}
	naReason, naKey, isNA := rowNA(s, event, variant)
	switch {
	case isNA && len(matches) > 0:
		return GridCell{}, fmt.Errorf("rolloutspec: %s: ambiguous cell %s: authored cell conflicts with not_applicable %q", m.Machine, m.cellID(key), naKey)
	case isNA:
		return synthesizedNA(s.ID, event, variant, window, transition, naReason), nil
	case len(matches) == 0:
		return GridCell{}, fmt.Errorf("rolloutspec: %s: missing cell %s", m.Machine, m.cellID(key))
	case len(matches) > 1:
		return GridCell{}, fmt.Errorf("rolloutspec: %s: ambiguous cell %s: %d authored cells match: %s", m.Machine, m.cellID(key), len(matches), selectorList(matches))
	}
	key.Cell = matches[0]
	return key, nil
}

// cellID renders the canonical id of one grid key: "<machine>/" + Key().
func (m *Machine) cellID(key GridCell) string {
	return m.Machine + "/" + key.Key()
}

// selectorList renders each colliding authored cell in selector form, for
// the ambiguity error.
func selectorList(cells []*Cell) string {
	parts := make([]string, len(cells))
	for i, c := range cells {
		parts[i] = selectorOf(c)
	}
	return strings.Join(parts, "; ")
}

// rowNA looks up "event[variant]" first, then the unqualified "event",
// returning the reason and the not_applicable key that matched.
func rowNA(s *State, event, variant string) (reason, key string, ok bool) {
	if variant != "" {
		if r, ok := s.NotApplicable[event+"["+variant+"]"]; ok {
			return r, event + "[" + variant + "]", true
		}
	}
	if r, ok := s.NotApplicable[event]; ok {
		return r, event, true
	}
	return "", "", false
}

func synthesizedNA(state, event, variant, window, transition, reason string) GridCell {
	c := &Cell{State: state, Event: event, Windows: []string{window}, Transition: transition, Outcome: OutcomeNA, Reason: reason, Status: StatusCurrent}
	if variant != "" {
		c.Variants = []string{variant}
	}
	return GridCell{State: state, Event: event, Variant: variant, Window: window, Transition: transition, Cell: c, Synthesized: true}
}
