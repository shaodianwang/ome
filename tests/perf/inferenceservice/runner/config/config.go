// Package config defines the data-driven schema for the InferenceService
// performance/scale harness. A single YAML file (see
// tests/perf/inferenceservice/configs/<scenario>/generator.yaml) fully
// describes a run: how many ServingRuntimes and BaseModels to seed, and
// how many InferenceServices to create across one or more size classes,
// with staggered creation so the load ramps instead of arriving as a
// single burst.
//
// Nothing in this package carries an in-code default for a behavioral
// value (replica counts, intervals, class names, image refs). Every
// such value comes from the YAML the operator hands the runner — the
// harness fails loudly when a required field is missing rather than
// silently substituting a magic constant.
package config

import (
	"errors"
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

// SizeClass describes one bucket of InferenceServices that share a
// shape. The harness records time-to-Ready bucketed by ClassName, so the
// checker can hold each class to its own threshold (a 1-replica "small"
// class should converge far faster than a multi-replica "large" one).
//
// A class is realized as Count InferenceServices, each referencing one
// of the run's seeded BaseModels and ServingRuntimes (round-robined by
// index so load spreads across the seeded catalog). Pods are not real:
// the executor flips the produced Deployment to Ready after a per-class
// simulated delay (ReadyAfterMs) so no kubelet is needed.
type SizeClass struct {
	// ClassName labels every InferenceService in this class via
	// ClassLabel and is the key under which the recorder aggregates
	// time-to-Ready. Must be unique within a config.
	ClassName string `json:"className"`

	// Count is the number of InferenceServices to create for this class.
	Count int `json:"count"`

	// Replicas is the desired replica count stamped onto each
	// InferenceService's Engine component (minReplicas == maxReplicas so
	// the Deployment has a fixed, known target the executor flips to
	// Ready). Drives the "small/medium/large" distinction.
	Replicas int32 `json:"replicas"`

	// CreationIntervalMs is the delay between creating successive
	// InferenceServices in this class. Staggers load so the apiserver and
	// reconciler see a ramp, not a thundering herd. Zero means "create as
	// fast as the client allows" — only set that deliberately.
	CreationIntervalMs uint `json:"creationIntervalMs"`

	// ReadyAfterMs is how long the fake executor waits, after first
	// observing a class's Deployment, before flipping it to Ready. Models
	// the runtime's container-pull + model-load latency without a real
	// kubelet. Larger classes typically carry a larger value.
	ReadyAfterMs uint `json:"readyAfterMs"`

	// Image is the engine container image stamped onto the produced
	// InferenceService. No in-code default: the YAML supplies it (any
	// pullable-looking ref works; envtest never pulls).
	Image string `json:"image"`
}

// Catalog declares how many shared ServingRuntimes and BaseModels to
// seed before the InferenceService load starts. Every InferenceService
// references one entry from each pool (round-robined), so a run with a
// large catalog exercises the runtimeselector's fetch/match path across
// many candidates rather than a single hot pair.
type Catalog struct {
	// ServingRuntimes is the number of ClusterServingRuntimes to seed.
	// Must be >= 1 (each InferenceService needs a runtime to resolve).
	ServingRuntimes int `json:"servingRuntimes"`

	// BaseModels is the number of ClusterBaseModels to seed. Must be
	// >= 1.
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
	// seeded ClusterBaseModel. envtest does not download; the URI only
	// needs to satisfy CRD validation.
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

// LoadConfig reads and validates a generator config YAML. It returns a
// descriptive error (not a defaulted Config) when a required field is
// absent — the harness must never silently substitute a behavioral
// value.
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
// present in the file. Missing-config is a hard error so a typo can
// never degrade into a silent magic default.
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
		if cl.Image == "" {
			errs = append(errs, fmt.Errorf("classes[%q].image must be set", cl.ClassName))
		}
	}

	return errors.Join(errs...)
}
