package routing

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/validation"
)

// PublisherOptionsResolver is provided by the operator-selected publisher. It
// combines operator-level and per-InferenceService options and validates the
// result against publisher-specific semantics, including the authority scope
// an InferenceService may target. Both input maps are private copies. The
// returned map is copied again before it enters the effective Config.
type PublisherOptionsResolver func(
	globalOptions map[string]string,
	inlineOptions map[string]string,
) (map[string]string, error)

// ResolvedProbePolicy is one InferenceService's effective endpoint probe
// policy. Enabled requires both the operator-level routing gate and a configured
// effective probe. PolicyDigest identifies every setting that can affect probe
// observations or routing decisions and is empty when Enabled is false.
type ResolvedProbePolicy struct {
	Enabled      bool
	Probe        ProbeConfig
	PolicyDigest string
}

// IsEnabled reports whether this policy should run endpoint probes.
func (p ResolvedProbePolicy) IsEnabled() bool {
	return p.Enabled
}

// ResolveProbePolicy resolves only the probe portion of an InferenceService's
// routing policy. Capacity and publisher overrides are independent and do not
// participate in probe resolution.
func ResolveProbePolicy(global Config, spec *v1beta1.InferenceServiceSpec) (ResolvedProbePolicy, error) {
	probe := cloneProbeConfig(global.Probe)
	routingEnabled := global.Enabled
	var inline *v1beta1.RoutingProbeSpec
	if spec != nil && spec.Routing != nil {
		routing := spec.Routing
		if routing.Enabled != nil && !*routing.Enabled {
			routingEnabled = false
		}
		inline = routing.Probe
	}

	if inline != nil {
		if err := validation.ValidateRouting(&v1beta1.InferenceServiceSpec{
			Routing: &v1beta1.RoutingSpec{Probe: inline},
		}); err != nil {
			return ResolvedProbePolicy{}, err
		}
		if inline.Disabled {
			probe = ProbeConfig{}
		} else {
			probe = probeConfigFromSpec(inline)
		}
	}

	if !routingEnabled {
		return ResolvedProbePolicy{Probe: probe}, nil
	}
	if err := probe.Validate(); err != nil {
		if inline != nil {
			return ResolvedProbePolicy{}, fmt.Errorf("spec.%w", err)
		}
		return ResolvedProbePolicy{}, err
	}
	if !probe.IsEnabled() {
		return ResolvedProbePolicy{Probe: probe}, nil
	}
	if global.Observer.MinPeriod > 0 && probe.Period < global.Observer.MinPeriod {
		err := fmt.Errorf("routing.probe.period (%s) must be at least routing.observer.minPeriod (%s)",
			probe.Period, global.Observer.MinPeriod)
		if inline != nil {
			return ResolvedProbePolicy{}, fmt.Errorf("spec.%w", err)
		}
		return ResolvedProbePolicy{}, err
	}

	digest, err := probePolicyDigest(probe)
	if err != nil {
		return ResolvedProbePolicy{}, err
	}
	return ResolvedProbePolicy{
		Enabled:      true,
		Probe:        probe,
		PolicyDigest: digest,
	}, nil
}

type probePolicyDigestPayload struct {
	Version            int             `json:"version"`
	Path               string          `json:"path"`
	Method             string          `json:"method"`
	AcceptStatuses     []int           `json:"acceptStatuses"`
	GateStatuses       []int           `json:"gateStatuses"`
	PeriodNanoseconds  int64           `json:"periodNanoseconds"`
	TimeoutNanoseconds int64           `json:"timeoutNanoseconds"`
	FailureThreshold   int             `json:"failureThreshold"`
	SuccessThreshold   int             `json:"successThreshold"`
	AllFailedPolicy    AllFailedPolicy `json:"allFailedPolicy"`
}

func probePolicyDigest(probe ProbeConfig) (string, error) {
	payload, err := json.Marshal(probePolicyDigestPayload{
		Version:            1,
		Path:               probe.Path,
		Method:             probe.Method,
		AcceptStatuses:     canonicalStatuses(probe.AcceptStatuses),
		GateStatuses:       canonicalStatuses(probe.GateStatuses),
		PeriodNanoseconds:  int64(probe.Period),
		TimeoutNanoseconds: int64(probe.Timeout),
		FailureThreshold:   probe.FailureThreshold,
		SuccessThreshold:   probe.SuccessThreshold,
		AllFailedPolicy:    probe.AllFailedPolicy,
	})
	if err != nil {
		return "", fmt.Errorf("encode probe policy digest: %w", err)
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("sha256:%x", sum), nil
}

func canonicalStatuses(statuses []int) []int {
	if len(statuses) == 0 {
		return nil
	}
	canonical := cloneInts(statuses)
	sort.Ints(canonical)
	n := 1
	for _, status := range canonical[1:] {
		if status != canonical[n-1] {
			canonical[n] = status
			n++
		}
	}
	return canonical[:n]
}

// ResolveConfig combines the operator routing configuration with one
// InferenceService's inline overrides. The returned config shares no mutable
// maps or slices with either input.
//
// The operator-level enabled flag is a hard gate: an InferenceService may opt
// out, but it cannot turn on routing for an installation where it is disabled.
// Probe and capacity blocks replace their global counterparts as a whole.
// Publisher identity stays global; its implementation owns the merge and
// semantic validation of per-InferenceService options.
func ResolveConfig(
	global Config,
	spec *v1beta1.InferenceServiceSpec,
	resolvePublisherOptions PublisherOptionsResolver,
) (Config, error) {
	resolved := cloneConfig(global)
	if err := validation.ValidateRouting(spec); err != nil {
		return Config{}, err
	}
	if spec == nil || spec.Routing == nil {
		if err := resolved.Validate(); err != nil {
			return Config{}, err
		}
		return resolved, nil
	}
	routingSpec := spec.Routing

	if routingSpec.Enabled != nil && !*routingSpec.Enabled {
		resolved.Enabled = false
	}
	if !resolved.Enabled {
		return resolved, nil
	}

	if routingSpec.Probe != nil {
		if routingSpec.Probe.Disabled {
			resolved.Probe = ProbeConfig{}
		} else {
			resolved.Probe = probeConfigFromSpec(routingSpec.Probe)
		}
	}
	if routingSpec.Capacity != nil {
		if routingSpec.Capacity.Disabled {
			resolved.Capacity = CapacityConfig{}
		} else {
			resolved.Capacity = capacityConfigFromSpec(routingSpec.Capacity)
		}
	}
	if err := resolved.Validate(); err != nil {
		var configErr *configValidationError
		if errors.As(err, &configErr) &&
			((configErr.section == "probe" && routingSpec.Probe != nil) ||
				(configErr.section == "capacity" && routingSpec.Capacity != nil)) {
			return Config{}, fmt.Errorf("spec.%w", err)
		}
		return Config{}, err
	}

	if routingSpec.Publisher == nil || len(routingSpec.Publisher.Options) == 0 {
		return resolved, nil
	}
	if resolvePublisherOptions == nil {
		return Config{}, fmt.Errorf(
			"spec.routing.publisher.options cannot be set because the selected publisher does not support per-InferenceService options")
	}
	options, err := resolvePublisherOptions(
		cloneStringMap(global.Publisher.Options),
		cloneStringMap(routingSpec.Publisher.Options),
	)
	if err != nil {
		return Config{}, fmt.Errorf("spec.routing.publisher.options: %w", err)
	}
	resolved.Publisher.Options = cloneStringMap(options)

	return resolved, nil
}

func cloneConfig(in Config) Config {
	out := in
	out.Probe = cloneProbeConfig(in.Probe)
	out.Capacity.Options = cloneStringMap(in.Capacity.Options)
	out.Publisher.Options = cloneStringMap(in.Publisher.Options)
	return out
}

func cloneProbeConfig(in ProbeConfig) ProbeConfig {
	out := in
	out.AcceptStatuses = cloneInts(in.AcceptStatuses)
	out.GateStatuses = cloneInts(in.GateStatuses)
	return out
}

func probeConfigFromSpec(in *v1beta1.RoutingProbeSpec) ProbeConfig {
	return ProbeConfig{
		Path:             in.Path,
		Method:           in.Method,
		AcceptStatuses:   int32sToInts(in.AcceptStatuses),
		GateStatuses:     int32sToInts(in.GateStatuses),
		Period:           in.Period.Duration,
		Timeout:          in.Timeout.Duration,
		FailureThreshold: int(in.FailureThreshold),
		SuccessThreshold: int(in.SuccessThreshold),
		AllFailedPolicy:  AllFailedPolicy(in.AllFailedPolicy),
	}
}

func capacityConfigFromSpec(in *v1beta1.RoutingCapacitySpec) CapacityConfig {
	return CapacityConfig{
		Path:    in.Path,
		Method:  in.Method,
		Format:  CapacityFormat(in.Format),
		Options: cloneStringMap(in.Options),
		Period:  in.Period.Duration,
		Timeout: in.Timeout.Duration,
		Samples: int(in.Samples),
		Quorum:  int(in.Quorum),
		MaxAge:  in.MaxAge.Duration,
	}
}

func cloneInts(in []int) []int {
	if in == nil {
		return nil
	}
	return append([]int(nil), in...)
}

func int32sToInts(in []int32) []int {
	if in == nil {
		return nil
	}
	out := make([]int, len(in))
	for i, value := range in {
		out[i] = int(value)
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
