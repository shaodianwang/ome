package autoscaler

import (
	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// SpecSource identifies the layer that produced the resolved
// ComponentAutoscaler. Mirrored onto status.components.<comp>.autoscaler
// .specSource by Slice 4 so operators can see which layer of the
// inheritance chain won for a given Component.
//
// Per OEP-0013 §"Inheritance chain", ISVC takes priority over the
// resolved ServingRuntime; if neither sets a block we fall back to
// the universal hpa-with-CPU=80% default (filled in by the HPA
// generator downstream when HPA is nil).
//
// Alpha (OEP-0013). The constants may change without notice.
type SpecSource string

const (
	// SpecSourceISVC indicates the resolved ComponentAutoscaler was
	// taken from isvc.Spec.<component>.ComponentExtensionSpec.Autoscaler.
	SpecSourceISVC SpecSource = "isvc"

	// SpecSourceRuntime indicates the resolved ComponentAutoscaler was
	// taken from runtime.Spec.<componentConfig>.ComponentExtensionSpec.Autoscaler.
	SpecSourceRuntime SpecSource = "runtime"

	// SpecSourceDefault indicates neither the ISVC nor the resolved
	// ServingRuntime declared a Component-level Autoscaler block.
	// ResolveComponentAutoscaler returns the universal default —
	// {Class: hpa, HPA: nil} — which the HPA generator downstream
	// expands to a single CPU=80% Resource metric. Per OEP-0013
	// §"Backwards compatibility": "default ALWAYS produces an HPA".
	SpecSourceDefault SpecSource = "default"
)

// ResolveComponentAutoscaler picks the authoritative ComponentAutoscaler
// for a single Component, considering the ISVC and the resolved
// ServingRuntime. The decision tree mirrors OEP-0013 §"Inheritance
// chain" + §"Generated autoscaler resources":
//
//  1. If isvc.Spec.<component>.Autoscaler != nil → return
//     (deep-copied isvc block, SpecSourceISVC).
//  2. Else if runtime.<componentConfig>.Autoscaler != nil → return
//     (deep-copied runtime block, SpecSourceRuntime).
//  3. Else → return ({Class: hpa, HPA: nil}, SpecSourceDefault).
//     The HPA generator (Slice 2) materializes this into a single
//     CPU=80% Resource metric.
//
// Decisions (locked 2026-05-29):
//
//   - Q1: The default is ALWAYS Class=hpa with CPU=80%. There is no
//     "only when Component declares a Resource metric" carve-out.
//     The operator-visible default unconditionally produces an HPA.
//
//   - Q2: Legacy ScaleTarget / ScaleMetric / ome.io/autoscalerClass
//     fields are IGNORED. They will be DELETED in Slice 6 (no
//     deprecation period). The new Autoscaler block is fully
//     authoritative. No deprecation log.
//
// Returned pointer is always non-nil and always a deep copy of the
// source (so callers can mutate freely without affecting the input
// objects). The component argument selects which Component-level
// Autoscaler field to consult on each spec.
//
// runtime may be nil — when it is, only the ISVC + default branches
// run. Unknown component values fall through to the default branch.
//
// Alpha (OEP-0013). The signature may change without notice.
func ResolveComponentAutoscaler(
	runtime *v1beta1.ServingRuntimeSpec,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
) (*v1beta1.ComponentAutoscaler, SpecSource) {
	if a := isvcAutoscaler(isvc, component); a != nil {
		return a.DeepCopy(), SpecSourceISVC
	}
	if a := runtimeAutoscaler(runtime, component); a != nil {
		return a.DeepCopy(), SpecSourceRuntime
	}
	return defaultAutoscaler(), SpecSourceDefault
}

// isvcAutoscaler dereferences the Component-level Autoscaler block on
// the InferenceService. Returns nil when the ISVC, the Component, or
// the Autoscaler field is unset.
func isvcAutoscaler(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *v1beta1.ComponentAutoscaler {
	if isvc == nil {
		return nil
	}
	switch component {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine == nil {
			return nil
		}
		return isvc.Spec.Engine.ComponentExtensionSpec.Autoscaler
	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder == nil {
			return nil
		}
		return isvc.Spec.Decoder.ComponentExtensionSpec.Autoscaler
	case v1beta1.RouterComponent:
		if isvc.Spec.Router == nil {
			return nil
		}
		return isvc.Spec.Router.ComponentExtensionSpec.Autoscaler
	default:
		return nil
	}
}

// runtimeAutoscaler dereferences the Component-level Autoscaler block
// on the resolved ServingRuntimeSpec. Returns nil when the runtime, the
// Component config, or the Autoscaler field is unset.
func runtimeAutoscaler(runtime *v1beta1.ServingRuntimeSpec, component v1beta1.ComponentType) *v1beta1.ComponentAutoscaler {
	if runtime == nil {
		return nil
	}
	switch component {
	case v1beta1.EngineComponent:
		if runtime.EngineConfig == nil {
			return nil
		}
		return runtime.EngineConfig.ComponentExtensionSpec.Autoscaler
	case v1beta1.DecoderComponent:
		if runtime.DecoderConfig == nil {
			return nil
		}
		return runtime.DecoderConfig.ComponentExtensionSpec.Autoscaler
	case v1beta1.RouterComponent:
		if runtime.RouterConfig == nil {
			return nil
		}
		return runtime.RouterConfig.ComponentExtensionSpec.Autoscaler
	default:
		return nil
	}
}

// defaultAutoscaler returns the universal default ComponentAutoscaler.
// Always a fresh allocation — never shares memory across calls — so a
// caller mutating the returned pointer cannot affect a subsequent
// resolution.
//
// The HPA generator (Slice 2) treats HPA=nil as a sentinel and
// materializes a single CPU=80% Resource metric. Per OEP-0013
// §"Backwards compatibility": every Component without an explicit
// Autoscaler block gets the same baseline HPA.
func defaultAutoscaler() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerHPA,
	}
}
