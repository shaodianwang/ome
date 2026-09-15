package rolloutspec

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

// machinesFS holds every machine directory. The all: prefix admits
// underscore-prefixed directories such as the test fixture.
//
//go:embed all:machines
var machinesFS embed.FS

const machinesDir = "machines"

// One struct per fragment file so a key in the wrong file is a parse error.
type machineFile struct {
	Machine    string      `json:"machine"`
	Scope      string      `json:"scope"`
	Frozen     bool        `json:"frozen"`
	Invariants []Invariant `json:"invariants"`
}

type statesFile struct {
	States []State `json:"states"`
}

type eventsFile struct {
	Events []Event `json:"events"`
}

type transitionsFile struct {
	Transitions []Transition `json:"transitions"`
}

type cellsFile struct {
	Cells []Cell `json:"cells"`
}

// errUnrecognizedFragment names the fragment filenames a machine directory
// may contain, so a mistyped filename fails loudly instead of vanishing.
var errUnrecognizedFragment = errors.New("unrecognized fragment file (expected machine.yaml, states.yaml, events.yaml, transitions.yaml, or cells-*.yaml)")

// Load merges the fragment files of machines/<name> into one Machine.
// Fragments merge in filename order, so cells-*.yaml files concatenate
// deterministically.
func Load(name string) (*Machine, error) {
	return loadFrom(machinesFS, name)
}

// LoadAll loads every machine directory, in name order.
func LoadAll() ([]*Machine, error) {
	entries, err := fs.ReadDir(machinesFS, machinesDir)
	if err != nil {
		return nil, fmt.Errorf("rolloutspec: %w", err)
	}
	var out []*Machine
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		m, err := Load(e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func loadFrom(fsys fs.FS, name string) (*Machine, error) {
	if strings.Contains(name, "/") || strings.HasPrefix(name, ".") {
		return nil, fmt.Errorf("rolloutspec: %s: invalid machine name %q", name, name)
	}
	dir := path.Join(machinesDir, name)
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("rolloutspec: %s: %w", name, err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	m := &Machine{}
	var sawMachine, sawStates, sawEvents bool
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			return nil, fmt.Errorf("rolloutspec: %s: unexpected entry %q", name, e.Name())
		}
		data, err := fs.ReadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("rolloutspec: %s: %s: %w", name, e.Name(), err)
		}
		if err := mergeFragment(m, e.Name(), data); err != nil {
			return nil, fmt.Errorf("rolloutspec: %s: %s: %w", name, e.Name(), err)
		}
		switch e.Name() {
		case "machine.yaml":
			sawMachine = true
		case "states.yaml":
			sawStates = true
		case "events.yaml":
			sawEvents = true
		}
	}
	switch {
	case !sawMachine:
		return nil, fmt.Errorf("rolloutspec: %s: missing machine.yaml", name)
	case !sawStates:
		return nil, fmt.Errorf("rolloutspec: %s: missing states.yaml", name)
	case !sawEvents:
		return nil, fmt.Errorf("rolloutspec: %s: missing events.yaml", name)
	}
	if m.Machine != name {
		return nil, fmt.Errorf("rolloutspec: %s: directory declares machine %q", name, m.Machine)
	}
	return m, nil
}

func mergeFragment(m *Machine, filename string, data []byte) error {
	switch {
	case filename == "machine.yaml":
		var f machineFile
		if err := yaml.UnmarshalStrict(data, &f); err != nil {
			return err
		}
		m.Machine, m.Scope, m.Frozen, m.Invariants = f.Machine, f.Scope, f.Frozen, f.Invariants
	case filename == "states.yaml":
		var f statesFile
		if err := yaml.UnmarshalStrict(data, &f); err != nil {
			return err
		}
		m.States = append(m.States, f.States...)
	case filename == "events.yaml":
		var f eventsFile
		if err := yaml.UnmarshalStrict(data, &f); err != nil {
			return err
		}
		m.Events = append(m.Events, f.Events...)
	case filename == "transitions.yaml":
		var f transitionsFile
		if err := yaml.UnmarshalStrict(data, &f); err != nil {
			return err
		}
		m.Transitions = append(m.Transitions, f.Transitions...)
	case strings.HasPrefix(filename, "cells-"):
		var f cellsFile
		if err := yaml.UnmarshalStrict(data, &f); err != nil {
			return err
		}
		m.Cells = append(m.Cells, f.Cells...)
	default:
		return errUnrecognizedFragment
	}
	return nil
}
