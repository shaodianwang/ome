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
// ISVCs can opt each component into OMENative independently (OEP-0007.1).
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
