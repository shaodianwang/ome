package routing

import (
	"errors"
	"fmt"

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
	out.Probe.AcceptStatuses = cloneInts(in.Probe.AcceptStatuses)
	out.Probe.GateStatuses = cloneInts(in.Probe.GateStatuses)
	out.Capacity.Options = cloneStringMap(in.Capacity.Options)
	out.Publisher.Options = cloneStringMap(in.Publisher.Options)
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
