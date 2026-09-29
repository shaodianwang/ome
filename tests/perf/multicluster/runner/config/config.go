// Package config defines the data-driven schema for the multi-cluster
// PLACEMENT performance harness. A single YAML file (see
// tests/perf/multicluster/configs/<scenario>/generator.yaml) fully
// describes a run: how many member (workload) clusters to stand up, how
// each member admits derived InferenceServices (its simulated admission
// delay, which is what lets a deterministic winner emerge), and how many
// source InferenceServices to fan out across one or more size classes.
//
// Nothing in this package carries an in-code default for a behavioral
// value (member count, admit delays, class names, dispatcher mode). Every
// such value comes from the YAML the operator hands the runner — the
// harness fails loudly when a required field is missing rather than
// silently substituting a magic constant.
//
// Cluster count is the dominant cost: each member is a full in-process
// envtest apiserver+etcd, so it drives boot time and memory. The CI
// scenario keeps it small (a few members, few source ISVCs) so a
// constrained agent can boot and converge; the baseline scenario can be
// larger.
package config

import (
	"errors"
	"fmt"
	"math"
	"os"

	"sigs.k8s.io/yaml"
)

// DispatcherMode mirrors placement.DispatcherMode as a config-surface
// string. It is validated against the known set (so a typo fails the run
// rather than silently degrading to all-at-once) and handed to the
// placement reconciler verbatim.
type DispatcherMode string

const (
	// DispatcherAllAtOnce clones onto every matched candidate at once.
	DispatcherAllAtOnce DispatcherMode = "AllAtOnce"
	// DispatcherIncremental clones onto candidates in sorted batches.
	DispatcherIncremental DispatcherMode = "Incremental"
)

// Member describes one workload (member) cluster the harness stands up and
// the admission behavior the member-side admitter simulates for it. The
// admitter watches the member's derived InferenceServices and flips them
// AllComponentsAdmitted after AdmitDelayMs, standing in for Kueue + the
// worker reconciler. The fastest-admitting member that is also a candidate
// is the placement winner, so spreading AdmitDelayMs across members makes
// the race outcome deterministic and gives the harness a known winner.
type Member struct {
	// Name is the member cluster's WorkloadCluster name on the control
	// plane. Must be unique within a config and DNS-1123-safe; the
	// placement winner is reported by this name so the checker / a human
	// can attribute it.
	Name string `json:"name"`

	// AdmitDelayMs is how long the member-side admitter waits, after first
	// observing a derived ISVC, before marking it admitted. Models that
	// member's Kueue admission + worker-reconcile latency without a real
	// kubelet. A small value makes the member a likely winner; a large one
	// makes it a likely loser whose derived is swept. Zero means "admit
	// immediately".
	AdmitDelayMs uint `json:"admitDelayMs"`
}

// SizeClass describes one bucket of source InferenceServices that share a
// shape. The harness records placement latency bucketed by ClassName so
// the checker can hold each class to its own threshold.
//
// A class is realized as Count source InferenceServices on the control
// plane, each declaring the accelerator requirement so it fans out onto
// the matching members.
type SizeClass struct {
	// ClassName labels every source InferenceService in this class and is
	// the key under which the recorder aggregates placement latency. Must
	// be unique within a config.
	ClassName string `json:"className"`

	// Count is the number of source InferenceServices to create for this
	// class.
	Count int `json:"count"`

	// CreationIntervalMs is the delay between creating successive source
	// InferenceServices in this class. Staggers load so the control-plane
	// apiserver and the placement reconciler see a ramp, not a thundering
	// herd. Zero means "create as fast as the client allows".
	CreationIntervalMs uint `json:"creationIntervalMs"`

	// Image is the engine container image stamped onto the produced source
	// InferenceService. No in-code default: the YAML supplies it (any
	// pullable-looking ref works; envtest never pulls).
	Image string `json:"image"`

	// RuntimeName identifies the OMENative runtime seeded on every member.
	RuntimeName string `json:"runtimeName"`
	// MinReplicas is the complete Engine floor for each generated service.
	MinReplicas int `json:"minReplicas"`
}

// Placement carries the placement-reconciler knobs the harness drives.
// They are config-surface mirrors of the reconciler's own fields/options,
// supplied by the YAML so a run can sweep them without a code change.
type Placement struct {
	// ClientQPS and ClientBurst set the per-member request budget for this workload.
	// Zero preserves client-go defaults; both must be configured together.
	ClientQPS   float32 `json:"clientQPS"`
	ClientBurst int     `json:"clientBurst"`

	// AcceleratorLabel / AcceleratorValue is the capability label the
	// members advertise and the source ISVCs require (e.g. "gpu" /
	// "test-accel"). The harness stamps the label on every member's
	// WorkloadCluster and the matching requirement on every source ISVC so
	// every member is a candidate. Config-driven (no magic literal); in
	// production the control plane resolves the requirement from
	// spec.model + the selected runtime.
	AcceleratorLabel string `json:"acceleratorLabel"`
	AcceleratorValue string `json:"acceleratorValue"`

	// ControlPlaneID is this control plane's identity, stamped onto every
	// derived ISVC and used to scope the status funnel's watch. Required so
	// the funnel-vs-poll comparison watches exactly this run's deriveds.
	ControlPlaneID string `json:"controlPlaneId"`

	// DispatcherMode selects the fan-out breadth policy: AllAtOnce (clone
	// onto every candidate at once) or Incremental (probe in batches).
	// Required; validated against the known set.
	DispatcherMode DispatcherMode `json:"dispatcherMode"`

	// DispatcherStepSize / DispatcherRoundTimeoutMs configure the
	// Incremental dispatcher (additional candidates per round, and the
	// dwell before the next batch). Ignored in AllAtOnce mode. Optional —
	// the reconciler applies its own graceful-degradation fallbacks for
	// non-positive values, so the harness passes them through as-is.
	DispatcherStepSize       int  `json:"dispatcherStepSize"`
	DispatcherRoundTimeoutMs uint `json:"dispatcherRoundTimeoutMs"`

	// RequeueMs is the placement status-refresh poll cadence. Drives the
	// reconciler's re-evaluation loop (fan-out / winner flip / re-place).
	// Required and positive — a slow poll would make the race resolve in
	// poll cycles rather than promptly.
	RequeueMs uint `json:"requeueMs"`

	// PlaceTimeoutMs bounds a single per-cluster fan-out apply so one slow
	// member cannot stall the apply to the healthy ones. Optional; a
	// non-positive value lets the reconciler apply its own fallback.
	PlaceTimeoutMs uint `json:"placeTimeoutMs"`

	// MaxConcurrentReconciles caps parallel placement reconciles (distinct
	// source ISVCs). Each reconcile fans out across members, so the
	// single-worker default serializes the whole fleet behind one slow
	// remote; raising it lets independent source ISVCs make progress
	// concurrently. Optional; zero preserves the controller-runtime
	// single-worker default.
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles"`

	// ClusterHealthIntervalMs is the WorkloadCluster reachability re-probe
	// cadence. Drives how fast a member's WorkloadCluster reaches
	// Ready=True (and thus becomes an eligible candidate) after the harness
	// creates it. Required and positive so members become candidates
	// promptly at the start of a run.
	ClusterHealthIntervalMs uint `json:"clusterHealthIntervalMs"`
}

// Funnel carries the two timing knobs that distinguish the headline
// funnel-ENABLED vs funnel-DISABLED comparison the harness runs. The
// harness runs the SAME scenario twice: once with the funnel on (long
// safety requeue, so convergence can only happen on events) and once with
// it off (short safety requeue, poll-only). Both windows come from config
// so the comparison is reproducible and there is no magic literal.
type Funnel struct {
	// EnabledSafetyRequeueMs is the steady-state status re-read backstop
	// used in the funnel-ENABLED pass. Set LONG so status convergence is
	// attributable to funnel events, not the poll backstop. Required and
	// positive.
	EnabledSafetyRequeueMs uint `json:"enabledSafetyRequeueMs"`

	// DisabledSafetyRequeueMs is the status re-read cadence used in the
	// funnel-DISABLED (poll-only) pass — there is no event path, so this IS
	// the convergence cadence. Set SHORT enough that the poll-only pass
	// converges within the recorder window, but representative of a real
	// poll cadence. Required and positive.
	DisabledSafetyRequeueMs uint `json:"disabledSafetyRequeueMs"`

	// BatchPeriodMs debounces funnel events before they enqueue a source
	// reconcile (folds a burst of derived-status updates for one ISVC into
	// a single reconcile). Only consulted in the funnel-ENABLED pass.
	// Required and positive.
	BatchPeriodMs uint `json:"batchPeriodMs"`

	// ResyncIntervalMs is how often the funnel reconciles its per-cluster watch
	// set against the connected clusters — i.e. how quickly it NOTICES a
	// newly-connected member and establishes its derived-status watch. It is NOT
	// the convergence latency (events flow as they happen once a watch exists),
	// but it bounds the startup gap before any events flow: members connect
	// shortly AFTER the control-plane manager (and the funnel) start, so a slow
	// resync leaves the funnel blind to the first members for that long. Required
	// and positive; set small so watches are established promptly at pass start.
	// Only consulted in the funnel-ENABLED pass.
	ResyncIntervalMs uint `json:"resyncIntervalMs"`
}

// Config is the full, data-driven description of one harness run.
type Config struct {
	// Members are the workload clusters to stand up. Must be non-empty; the
	// fan-out has nowhere to go with zero members.
	Members []Member `json:"members"`

	// Placement carries the placement-reconciler knobs.
	Placement Placement `json:"placement"`

	// Funnel carries the funnel-on / funnel-off comparison windows.
	Funnel Funnel `json:"funnel"`

	// Classes is the set of source-InferenceService size buckets to
	// generate. Must be non-empty.
	Classes []SizeClass `json:"classes"`
}

// Total returns the total number of source InferenceServices the config
// generates across all classes. Used by the recorder to know when every
// expected source has reported so it can stop without waiting out the
// timeout.
func (c *Config) Total() int {
	n := 0
	for _, cl := range c.Classes {
		n += cl.Count
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

	if len(c.Members) == 0 {
		errs = append(errs, errors.New("members must contain at least one entry"))
	}
	seenMember := map[string]struct{}{}
	for i, m := range c.Members {
		if m.Name == "" {
			errs = append(errs, fmt.Errorf("members[%d].name must be set", i))
			continue
		}
		if _, dup := seenMember[m.Name]; dup {
			errs = append(errs, fmt.Errorf("members[%d].name %q is duplicated", i, m.Name))
		}
		seenMember[m.Name] = struct{}{}
	}

	errs = append(errs, c.Placement.validate()...)
	errs = append(errs, c.Funnel.validate()...)

	if len(c.Classes) == 0 {
		errs = append(errs, errors.New("classes must contain at least one entry"))
	}
	seenClass := map[string]struct{}{}
	for i, cl := range c.Classes {
		if cl.ClassName == "" {
			errs = append(errs, fmt.Errorf("classes[%d].className must be set", i))
			continue
		}
		if _, dup := seenClass[cl.ClassName]; dup {
			errs = append(errs, fmt.Errorf("classes[%q].className is duplicated", cl.ClassName))
		}
		seenClass[cl.ClassName] = struct{}{}
		if cl.Count < 1 {
			errs = append(errs, fmt.Errorf("classes[%q].count must be >= 1", cl.ClassName))
		}
		if cl.RuntimeName == "" {
			errs = append(errs, fmt.Errorf("classes[%q].runtimeName must be set", cl.ClassName))
		}
		if cl.MinReplicas > math.MaxInt32 {
			errs = append(errs, fmt.Errorf("classes[%q].minReplicas exceeds the replica API range", cl.ClassName))
		}
		if cl.MinReplicas <= 0 {
			errs = append(errs, fmt.Errorf("classes[%q].minReplicas must be positive", cl.ClassName))
		}
		if cl.Image == "" {
			errs = append(errs, fmt.Errorf("classes[%q].image must be set", cl.ClassName))
		}
	}

	return errors.Join(errs...)
}

func (p *Placement) validate() []error {
	var errs []error
	if p.ClientQPS < 0 || p.ClientBurst < 0 || (p.ClientQPS == 0) != (p.ClientBurst == 0) {
		errs = append(errs, errors.New("placement.clientQPS and clientBurst must both be positive or both zero"))
	}
	for _, f := range []struct {
		name, val string
	}{
		{"placement.acceleratorLabel", p.AcceleratorLabel},
		{"placement.acceleratorValue", p.AcceleratorValue},
		{"placement.controlPlaneId", p.ControlPlaneID},
	} {
		if f.val == "" {
			errs = append(errs, fmt.Errorf("%s must be set", f.name))
		}
	}
	switch p.DispatcherMode {
	case DispatcherAllAtOnce, DispatcherIncremental:
	case "":
		errs = append(errs, errors.New("placement.dispatcherMode must be set (AllAtOnce or Incremental)"))
	default:
		errs = append(errs, fmt.Errorf("placement.dispatcherMode %q is not one of AllAtOnce, Incremental", p.DispatcherMode))
	}
	if p.RequeueMs == 0 {
		errs = append(errs, errors.New("placement.requeueMs must be > 0"))
	}
	if p.ClusterHealthIntervalMs == 0 {
		errs = append(errs, errors.New("placement.clusterHealthIntervalMs must be > 0"))
	}
	return errs
}

func (f *Funnel) validate() []error {
	var errs []error
	if f.EnabledSafetyRequeueMs == 0 {
		errs = append(errs, errors.New("funnel.enabledSafetyRequeueMs must be > 0"))
	}
	if f.DisabledSafetyRequeueMs == 0 {
		errs = append(errs, errors.New("funnel.disabledSafetyRequeueMs must be > 0"))
	}
	if f.BatchPeriodMs == 0 {
		errs = append(errs, errors.New("funnel.batchPeriodMs must be > 0"))
	}
	if f.ResyncIntervalMs == 0 {
		errs = append(errs, errors.New("funnel.resyncIntervalMs must be > 0"))
	}
	return errs
}
