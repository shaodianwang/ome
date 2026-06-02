package utils

import (
	"fmt"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// DetermineEngineDeploymentMode determines the deployment mode for the engine based on its configuration.
func DetermineEngineDeploymentMode(engine *v1beta1.EngineSpec) constants.DeploymentModeType {
	if engine == nil {
		return constants.RawDeployment
	}

	if mode, found := GetDeploymentModeFromAnnotations(engine.Annotations); found {
		return mode
	}

	// Multi-node if leader and worker are defined
	if engine.Leader != nil || engine.Worker != nil {
		return constants.MultiNode
	}

	// Default to raw deployment
	return constants.RawDeployment
}

// DetermineDeploymentModes determines the deployment modes for all components based on their specs.
func DetermineDeploymentModes(engine *v1beta1.EngineSpec, decoder *v1beta1.DecoderSpec, router *v1beta1.RouterSpec, runtime *v1beta1.ServingRuntimeSpec) (engineMode, decoderMode, routerMode constants.DeploymentModeType, err error) {
	engineMode = determineComponentDeploymentMode(engine, runtime)
	decoderMode = determineComponentDeploymentMode(decoder, runtime)
	routerMode = determineComponentDeploymentMode(router, runtime)

	if engine == nil {
		return "", "", "", fmt.Errorf("engine component is required")
	}

	return engineMode, decoderMode, routerMode, nil
}

// determineComponentDeploymentMode determines deployment mode for a generic component.
// Annotation takes precedence; otherwise infer MultiNode from Leader/Worker presence and
// default to RawDeployment. Symmetric for Engine, Decoder, and Router so PD-disaggregated
// ISVCs can opt each component into OMENative independently.
func determineComponentDeploymentMode(spec interface{}, runtime *v1beta1.ServingRuntimeSpec) constants.DeploymentModeType {
	switch s := spec.(type) {
	case *v1beta1.EngineSpec:
		return DetermineEngineDeploymentMode(s)
	case *v1beta1.DecoderSpec:
		if s == nil {
			return constants.RawDeployment
		}
		if mode, found := GetDeploymentModeFromAnnotations(s.Annotations); found {
			return mode
		}
		if s.Leader != nil || s.Worker != nil {
			return constants.MultiNode
		}
		return constants.RawDeployment
	case *v1beta1.RouterSpec:
		if s == nil {
			return constants.RawDeployment
		}
		if mode, found := GetDeploymentModeFromAnnotations(s.Annotations); found {
			return mode
		}
		return constants.RawDeployment
	}

	return constants.RawDeployment
}

// DetermineEntrypointComponent determines which component should be the main entrypoint for the InferenceService.
// Priority: Router (if present) > Engine (always present)
// This function implements the automatic routing logic: router if present, else engine.
func DetermineEntrypointComponent(isvc *v1beta1.InferenceService) v1beta1.ComponentType {
	// Auto-determine: Router takes precedence if present
	if isvc.Spec.Router != nil {
		return v1beta1.RouterComponent
	}

	// Default to engine
	return v1beta1.EngineComponent
}

// IsMultiPodComponent reports whether the named Component spawns more
// than one pod per Instance (Leader + N workers, where N = Worker.Size).
// Returns true when `Worker != nil && Worker.Size != nil && *Worker.Size > 0`.
//
// The admission webhook (inference_service_defaults.defaultWorkerSize)
// ensures Worker.Size is populated when Leader+Worker are paired, and
// the validator rejects orphan Leader/Worker shapes, so this predicate
// captures the only valid multi-pod shape: a Worker block with a
// positive Size.
//
// Used by per-Component Service builders to decide whether to add the
// `ome.io/pod-ordinal=0` selector. Multi-pod Components route customer
// traffic only to the rank-0 (leader) pod; workers run distributed-init
// peers and cannot serve. Single-pod Components keep the broader
// selector because SurgeThenDrain alternates the pod-naming ordinal
// between 0 and 1 across surges — pinning ordinal=0 there would
// zero-endpoint the Service during the surge phase.
//
// Router is always single-pod (has no Leader/Worker fields).
func IsMultiPodComponent(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) bool {
	if isvc == nil {
		return false
	}
	switch component {
	case v1beta1.EngineComponent:
		return engineSpawnsMultiplePods(isvc.Spec.Engine)
	case v1beta1.DecoderComponent:
		return decoderSpawnsMultiplePods(isvc.Spec.Decoder)
	default:
		// Router and any future single-pod-only Component types.
		return false
	}
}

// engineSpawnsMultiplePods is the Engine-specific tail of
// IsMultiPodComponent. Mirrors the webhook's engineIsMultiPod but does
// NOT treat a Leader without Worker as multi-pod — the validator rejects
// that shape before reconcile, but we stay strict on the
// `Worker.Size > 0` half so the selector tightening only fires when
// at least 2 pods exist per Instance.
func engineSpawnsMultiplePods(engine *v1beta1.EngineSpec) bool {
	if engine == nil {
		return false
	}
	return engine.Worker != nil && engine.Worker.Size != nil && *engine.Worker.Size > 0
}

// decoderSpawnsMultiplePods is the Decoder-specific tail of
// IsMultiPodComponent. See engineSpawnsMultiplePods for rationale.
func decoderSpawnsMultiplePods(decoder *v1beta1.DecoderSpec) bool {
	if decoder == nil {
		return false
	}
	return decoder.Worker != nil && decoder.Worker.Size != nil && *decoder.Worker.Size > 0
}
