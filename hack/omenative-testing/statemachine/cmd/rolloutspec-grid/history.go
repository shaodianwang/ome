package main

import (
	"errors"
	"fmt"
	"os"

	"sigs.k8s.io/yaml"

	"github.com/ome-projects/ome/tests/rolloutspec"
)

// historyEntry is one record of a history sidecar file: a cell selector
// (state and event match exactly; variants, windows, and transition follow
// the same empty-means-all rule as an authored Cell) plus the references
// that decided every grid cell it selects. file records which sidecar it
// came from, for error messages; it is not part of the sidecar's own
// schema.
type historyEntry struct {
	State      string   `json:"state"`
	Event      string   `json:"event"`
	Variants   []string `json:"variants"`
	Windows    []string `json:"windows"`
	Transition string   `json:"transition"`
	PRs        []string `json:"prs"`
	// Decision is a non-PR provenance reference, for a cell an owner
	// decided outside any merged change.
	Decision string `json:"decision"`
	file     string
}

// refs renders the entry's provenance as the flat list the grid carries:
// its PRs, then its decision reference when it has one.
func (h historyEntry) refs() []string {
	if h.Decision == "" {
		return h.PRs
	}
	return append(append([]string{}, h.PRs...), h.Decision)
}

type historyFile struct {
	History []historyEntry `json:"history"`
}

// loadHistory reads and concatenates every history sidecar file, in the
// order given.
func loadHistory(paths []string) ([]historyEntry, error) {
	var entries []historyEntry
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("read history %s: %w", p, err)
		}
		var f historyFile
		if err := yaml.UnmarshalStrict(data, &f); err != nil {
			return nil, fmt.Errorf("parse history %s: %w", p, err)
		}
		for _, h := range f.History {
			h.file = p
			entries = append(entries, h)
		}
	}
	return entries, nil
}

// applyHistory fills each grid entry's History with the references of
// every sidecar entry that selects it, matching state and event exactly and
// variant/window/transition by the same rule as an authored Cell. An entry
// that selects no grid cell at all is a typo in its selector, not a
// legitimately empty match, so it fails the run instead of vanishing
// silently.
func applyHistory(env *envelope, entries []historyEntry) error {
	if len(entries) == 0 {
		return nil
	}
	matched := make([]bool, len(entries))
	for i := range env.Grid {
		g := &env.Grid[i]
		key := rolloutspec.GridCell{Variant: g.Variant, Window: g.Window, Transition: g.Transition}
		for ei, h := range entries {
			if h.State != g.State || h.Event != g.Event {
				continue
			}
			if rolloutspec.Selects(h.Variants, h.Windows, h.Transition, key) {
				g.History = appendUniqueRefs(g.History, h.refs())
				matched[ei] = true
			}
		}
	}
	var errs []error
	for i, h := range entries {
		if matched[i] {
			continue
		}
		errs = append(errs, fmt.Errorf("history %s: entry state=%s event=%s windows=%v variants=%v transition=%s matches no grid cell",
			h.file, h.State, h.Event, h.Windows, h.Variants, h.Transition))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// appendUniqueRefs appends every reference not already present in
// existing, preserving first-seen order: a narrow and a broad sidecar entry
// both citing the same reference against one cell is the normal case, not a
// duplicate to keep.
func appendUniqueRefs(existing []string, refs []string) []string {
	seen := make(map[string]bool, len(existing))
	for _, p := range existing {
		seen[p] = true
	}
	for _, p := range refs {
		if seen[p] {
			continue
		}
		seen[p] = true
		existing = append(existing, p)
	}
	return existing
}
