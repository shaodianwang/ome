package rolloutspec

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Validate returns every well-formedness violation in m: closed
// vocabularies, dangling references, decided_in paths relative to
// repoRoot, the frozen contract (no DECIDE cell outside a planned state,
// and every decided cell that is not n/a names at least one check), the
// planned-state contract (an arrow from a state the engine resolves never
// lands in a planned one), the transition lane vocabulary and the why an
// adapter-lane edge owes, declared invariant ids, the pointer-field
// spellings that are present-but-empty rather than absent, and that
// Expand succeeds. It collects every violation rather than stopping at
// the first, so a table can be fixed in one pass instead of one error at
// a time.
func Validate(m *Machine, repoRoot string) []error {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("rolloutspec: %s: "+format, append([]any{m.Machine}, args...)...))
	}
	pathExists := func(what, p string) {
		if p == "" {
			fail("%s: decided_in is empty", what)
			return
		}
		if _, err := os.Stat(filepath.Join(repoRoot, filepath.FromSlash(p))); err != nil {
			fail("%s: decided_in %q does not exist", what, p)
		}
	}

	if len(m.States) == 0 {
		fail("must declare at least one state")
	}
	if len(m.Events) == 0 {
		fail("must declare at least one event")
	}

	states := map[string]*State{}
	for i := range m.States {
		s := &m.States[i]
		if s.ID == "" {
			fail("blank state id")
			continue
		}
		if _, dup := states[s.ID]; dup {
			fail("duplicate state id %q", s.ID)
			continue
		}
		states[s.ID] = s
		pathExists("state "+s.ID, s.DecidedIn)
		if s.Op != nil && s.Op.Type == "" {
			fail("state %s: op: {} is invalid (type is required)", s.ID)
		}
		if s.Applies != nil && len(s.Applies.Strategies) == 0 && len(s.Applies.Shape) == 0 {
			fail("state %s: applies: {} is invalid (strategies or shape is required)", s.ID)
		}
	}
	invariants := map[string]struct{}{}
	for _, inv := range m.Invariants {
		if inv.ID == "" {
			fail("blank invariant id")
			continue
		}
		if _, dup := invariants[inv.ID]; dup {
			fail("duplicate invariant id %q", inv.ID)
			continue
		}
		invariants[inv.ID] = struct{}{}
	}
	events := map[string]*Event{}
	for i := range m.Events {
		e := &m.Events[i]
		if e.ID == "" {
			fail("blank event id")
			continue
		}
		if _, dup := events[e.ID]; dup {
			fail("duplicate event id %q", e.ID)
			continue
		}
		events[e.ID] = e
	}
	hasVariant := func(e *Event, v string) bool {
		if v == "" {
			return len(e.Variants) == 0
		}
		for _, x := range e.Variants {
			if x == v {
				return true
			}
		}
		return false
	}
	transitions := map[string]*Transition{}
	for i := range m.Transitions {
		t := &m.Transitions[i]
		if t.ID == "" {
			fail("blank transition id")
			continue
		}
		if _, dup := transitions[t.ID]; dup {
			fail("duplicate transition id %q", t.ID)
			continue
		}
		transitions[t.ID] = t
		if states[t.From] == nil {
			fail("transition %s: unknown state %q", t.ID, t.From)
		}
		if states[t.To] == nil {
			fail("transition %s: unknown state %q", t.ID, t.To)
		}
		e := events[t.Event]
		if e == nil {
			fail("transition %s: unknown event %q", t.ID, t.Event)
		} else if t.Variant != "" && !hasVariant(e, t.Variant) {
			fail("transition %s: unknown variant %q", t.ID, t.Variant)
		}
		pathExists("transition "+t.ID, t.DecidedIn)
		// The engine's own arrows reach only states it resolves; a planned
		// state is entered through the parent arrow it refines, so only
		// another planned state may name it as to.
		if from, to := states[t.From], states[t.To]; from != nil && to != nil && to.Planned && !from.Planned {
			fail("transition %s: lands in planned state %q from %q, which the engine resolves", t.ID, t.To, t.From)
		}
		if _, ok := lanes[t.LaneOf()]; !ok {
			fail("transition %s: lane %q is not workload|adapter", t.ID, t.Lane)
		}
		switch {
		case t.LaneOf() == LaneAdapter && t.Why == "":
			fail("transition %s: an adapter-lane transition must state the adapter input it depends on in why", t.ID)
		case t.LaneOf() == LaneWorkload && t.Why != "":
			fail("transition %s: why states the adapter input an adapter-lane transition depends on; a workload-lane transition has none", t.ID)
		}
	}
	for _, s := range m.States {
		naKeys := make([]string, 0, len(s.NotApplicable))
		for key := range s.NotApplicable {
			naKeys = append(naKeys, key)
		}
		sort.Strings(naKeys)
		for _, key := range naKeys {
			reason := s.NotApplicable[key]
			ev, variant, qualified := splitNAKey(key)
			e := events[ev]
			switch {
			case e == nil:
				fail("state %s: not_applicable %q references unknown event", s.ID, key)
			case qualified && !hasVariant(e, variant):
				fail("state %s: not_applicable %q references unknown variant", s.ID, key)
			}
			if reason == "" {
				fail("state %s: not_applicable %q has no reason", s.ID, key)
			}
		}
	}
	for i := range m.Cells {
		c := &m.Cells[i]
		id := cellSelector(c)
		if states[c.State] == nil {
			fail("%s: unknown state %q", id, c.State)
		}
		e := events[c.Event]
		if e == nil {
			fail("%s: unknown event %q", id, c.Event)
		} else {
			for _, v := range c.Variants {
				if !hasVariant(e, v) {
					fail("%s: unknown variant %q", id, v)
				}
			}
		}
		for _, w := range c.Windows {
			if w != WindowBefore && w != WindowBoundary && w != WindowAfter {
				fail("%s: window %q is not before|boundary|after", id, w)
			}
		}
		if c.Transition != "" {
			t := transitions[c.Transition]
			switch {
			case t == nil:
				fail("%s: unknown transition %q", id, c.Transition)
			case t.From != c.State:
				fail("%s: transition %q does not leave state %q", id, c.Transition, c.State)
			}
		}
		if _, ok := outcomes[c.Outcome]; !ok {
			fail("%s: outcome %q is not in the closed vocabulary", id, c.Outcome)
		}
		if _, ok := statuses[c.Status]; !ok {
			fail("%s: status %q is not current|review|decided", id, c.Status)
		}
		if c.Placement != "" {
			if _, ok := placements[c.Placement]; !ok {
				fail("%s: placement %q is not unit|perm|kind|sim", id, c.Placement)
			}
		}
		if c.Outcome == OutcomeTransition && states[c.To] == nil {
			fail("%s: to %q is not a declared state", id, c.To)
		}
		if c.Outcome == OutcomeNA && c.Reason == "" {
			fail("%s: n/a requires a reason", id)
		}
		if c.Status == StatusDecided && c.Outcome != OutcomeNA && c.Placement == "" {
			fail("%s: decided cell requires placement", id)
		}
		// A planned state is declared ahead of the engine, so its open
		// questions are allowed to stand on a frozen machine.
		if m.Frozen && c.Outcome == OutcomeDecide && (states[c.State] == nil || !states[c.State].Planned) {
			fail("%s: frozen machine has DECIDE cell", id)
		}
		if m.Frozen && c.Status == StatusDecided && c.Outcome != OutcomeNA && !hasCoverage(c.Coverage) {
			fail("%s: decided cell has no test coverage", id)
		}
		// Citations are only checkable once the machine has written its
		// invariant list; until then cells may cite free-form ids.
		if len(m.Invariants) > 0 {
			for _, ref := range c.Invariants {
				if _, ok := invariants[ref]; !ok {
					fail("%s: invariant %q is not declared by the machine", id, ref)
				}
			}
		}
	}
	if _, err := m.Expand(); err != nil {
		if j, ok := err.(interface{ Unwrap() []error }); ok {
			errs = append(errs, j.Unwrap()...)
		} else {
			errs = append(errs, err)
		}
	}
	return errs
}

// hasCoverage reports whether a cell names at least one check, in any of
// the coverage kinds.
func hasCoverage(c *Coverage) bool {
	return c != nil && (len(c.Unit) > 0 || len(c.Perm) > 0 || len(c.Kind) > 0 || len(c.Replay) > 0)
}

// splitNAKey parses "event" or "event[variant]".
func splitNAKey(key string) (event, variant string, qualified bool) {
	open := strings.IndexByte(key, '[')
	if open < 0 || !strings.HasSuffix(key, "]") {
		return key, "", false
	}
	return key[:open], key[open+1 : len(key)-1], true
}
