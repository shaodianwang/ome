// Package config defines the data-driven schema for the OMENative /
// InferenceReplica performance harness. A single YAML file (see
// tests/perf/omenative/configs/<scenario>/generator.yaml) fully describes
// a run: how many ServingRuntimes and BaseModels to seed, and how many
// OMENative InferenceServices to create across one or more size classes,
// with staggered creation so the load ramps instead of arriving as a
// single burst.
//
// Unlike the InferenceService harness (whose fake executor sets
// Deployment status at a constant per-ISVC cost), OMENative manages pods
// directly: the ISVC reconciler creates one InferenceReplica per
// Component and the IR reconciler drives the pods. Cost therefore scales
// with the POD count, so a class declares both an Instance count
// (replicas) and an optional leader/worker gang size (workerSize) — the
// two knobs that multiply into the pod fan-out the harness is built to
// measure.
//
// Nothing in this package carries an in-code default for a behavioral
// value (replica counts, intervals, class names, image refs). Every such
// value comes from the YAML the operator hands the runner — the harness
// fails loudly when a required field is missing rather than silently
// substituting a magic constant.
package config

import (
	"errors"
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

// SizeClass describes one bucket of OMENative InferenceServices that
// share a shape. The harness records time-to-Ready bucketed by
// ClassName, so the checker can hold each class to its own threshold (a
// 1-pod "single" class should converge far faster than a multi-pod
// leader/worker "gang" one).
//
// A class is realized as Count InferenceServices, each referencing one of
// the run's seeded BaseModels and ServingRuntimes (round-robined by index
// so load spreads across the seeded catalog). Pods ARE real (created by
// the IR reconciler); a fake kubelet — the integration PodKubeletSimulator
// + EndpointSliceSimulator — drives them to Ready, so no real container
// is pulled.
type SizeClass struct {
	// ClassName labels every InferenceService in this class via ClassLabel
	// and is the key under which the recorder aggregates time-to-Ready.
	// Must be unique within a config.
	ClassName string `json:"className"`

	// Count is the number of InferenceServices to create for this class.
	Count int `json:"count"`

	// Replicas is the desired Instance count stamped onto each
	// InferenceService's Engine component (minReplicas == maxReplicas so
	// the InferenceReplica has a fixed, known target). For a single-pod
	// class each Instance is one pod; for a gang class each Instance is
	// WorkerSize+1 pods. Must be >= 1.
	Replicas int32 `json:"replicas"`

	// WorkerSize, when > 0, promotes the engine Component to a multi-pod
	// gang: each Instance materializes one Leader pod plus WorkerSize
	// Worker pods (total WorkerSize+1 pods per Instance). Zero (the
	// default) leaves the engine single-pod. This is the knob that drives
	// the pod fan-out the OMENative path's cost scales with.
	WorkerSize int `json:"workerSize"`

	// CreationIntervalMs is the delay between creating successive
	// InferenceServices in this class. Staggers load so the apiserver and
	// reconcilers see a ramp, not a thundering herd. Zero means "create as
	// fast as the client allows" — only set that deliberately.
	CreationIntervalMs uint `json:"creationIntervalMs"`

	// Image is the engine container image stamped onto the produced
	// InferenceService (and, for a gang, onto the leader + worker pods).
	// No in-code default: the YAML supplies it (any pullable-looking ref
	// works; the fake kubelet never pulls).
	Image string `json:"image"`
}

// PodsPerISVC returns the number of pods one InferenceService in this
// class produces: Replicas Instances, each Instance being a single pod
// (WorkerSize == 0) or a leader + WorkerSize workers (WorkerSize > 0).
func (s SizeClass) PodsPerISVC() int {
	perInstance := 1
	if s.WorkerSize > 0 {
		perInstance = s.WorkerSize + 1
	}
	return int(s.Replicas) * perInstance
}

// Catalog declares how many shared ServingRuntimes and BaseModels to seed
// before the InferenceService load starts. Every InferenceService
// references one entry from each pool (round-robined), so a run with a
// large catalog exercises the runtimeselector's fetch/match path across
// many candidates rather than a single hot pair.
type Catalog struct {
	// ServingRuntimes is the number of ClusterServingRuntimes to seed.
	// Must be >= 1 (each InferenceService needs a runtime to resolve).
	ServingRuntimes int `json:"servingRuntimes"`

	// BaseModels is the number of ClusterBaseModels to seed. Must be >= 1.
	BaseModels int `json:"baseModels"`

	// ModelFormat / ModelArchitecture / ModelFramework are stamped onto
	// every seeded ClusterBaseModel and mirrored onto every seeded
	// ClusterServingRuntime's SupportedModelFormats so the strict
	// runtimeselector matches. Config-driven (no magic literal) so a run
	// can target whatever format/arch pairing it wants to stress.
	ModelFormat       string `json:"modelFormat"`
	ModelArchitecture string `json:"modelArchitecture"`
	ModelFramework    string `json:"modelFramework"`

	// StorageURI is the (never-fetched) storage URI stamped onto every
	// seeded ClusterBaseModel. The fake kubelet does not download; the URI
	// only needs to satisfy CRD validation.
	StorageURI string `json:"storageUri"`
}

// Config is the full, data-driven description of one harness run.
type Config struct {
	// Catalog seeds the shared runtime/model pool.
	Catalog Catalog `json:"catalog"`

	// Classes is the set of InferenceService size buckets to generate.
	// Must be non-empty.
	Classes []SizeClass `json:"classes"`
}

// Total returns the total number of InferenceServices the config
// generates across all classes. Used by the recorder to know when every
// expected object has reported (so it can stop without waiting out the
// full timeout).
func (c *Config) Total() int {
	n := 0
	for _, cl := range c.Classes {
		n += cl.Count
	}
	return n
}

// TotalPods returns the total number of pods the config drives to Ready
// across all classes. Surfaced into the summary so a scenario's apiserver
// op counts and conflict rate can be read against the pod count they
// scale with (the whole point of the OMENative harness vs. the
// constant-cost ISVC one).
func (c *Config) TotalPods() int {
	n := 0
	for _, cl := range c.Classes {
		n += cl.Count * cl.PodsPerISVC()
	}
	return n
}

// LoadConfig reads and validates a generator config YAML. It returns a
// descriptive error (not a defaulted Config) when a required field is
// absent — the harness must never silently substitute a behavioral value.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file %q: %w", path, err)
	}

	cfg := &Config{}
	if err := yaml.UnmarshalStrict(raw, cfg); err != nil {
		return nil, fmt.Errorf("parsing config %q: %w", path, err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config %q: %w", path, err)
	}
	return cfg, nil
}

// validate enforces that every behavioral value the harness needs is
// present in the file. Missing-config is a hard error so a typo can never
// degrade into a silent magic default.
func (c *Config) validate() error {
	var errs []error

	if c.Catalog.ServingRuntimes < 1 {
		errs = append(errs, errors.New("catalog.servingRuntimes must be >= 1"))
	}
	if c.Catalog.BaseModels < 1 {
		errs = append(errs, errors.New("catalog.baseModels must be >= 1"))
	}
	for _, f := range []struct {
		name, val string
	}{
		{"catalog.modelFormat", c.Catalog.ModelFormat},
		{"catalog.modelArchitecture", c.Catalog.ModelArchitecture},
		{"catalog.modelFramework", c.Catalog.ModelFramework},
		{"catalog.storageUri", c.Catalog.StorageURI},
	} {
		if f.val == "" {
			errs = append(errs, fmt.Errorf("%s must be set", f.name))
		}
	}

	if len(c.Classes) == 0 {
		errs = append(errs, errors.New("classes must contain at least one entry"))
	}
	seen := map[string]struct{}{}
	for i, cl := range c.Classes {
		if cl.ClassName == "" {
			errs = append(errs, fmt.Errorf("classes[%d].className must be set", i))
			continue
		}
		if _, dup := seen[cl.ClassName]; dup {
			errs = append(errs, fmt.Errorf("classes[%d].className %q is duplicated", i, cl.ClassName))
		}
		seen[cl.ClassName] = struct{}{}
		if cl.Count < 1 {
			errs = append(errs, fmt.Errorf("classes[%q].count must be >= 1", cl.ClassName))
		}
		if cl.Replicas < 1 {
			errs = append(errs, fmt.Errorf("classes[%q].replicas must be >= 1", cl.ClassName))
		}
		if cl.WorkerSize < 0 {
			errs = append(errs, fmt.Errorf("classes[%q].workerSize must be >= 0", cl.ClassName))
		}
		if cl.Image == "" {
			errs = append(errs, fmt.Errorf("classes[%q].image must be set", cl.ClassName))
		}
	}

	return errors.Join(errs...)
}
