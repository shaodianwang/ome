package validation

import (
	"errors"
	"fmt"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

const (
	multiNodeConfigurationError     = "for MultiNode deployment, both leader and worker must be defined and worker.size must be greater than 0"
	rawDeploymentConfigurationError = "for RawDeployment, leader and worker must not be defined"
	priorityIsNotSameError          = "different priorities assigned for the model format %s"
)

func ValidateSupportedModelFormats(formats []v1beta1.SupportedModelFormat) error {
	if len(formats) == 0 {
		return fmt.Errorf("spec.supportedModelFormats is required")
	}
	for i, f := range formats {
		if f.ModelFormat == nil || f.ModelFormat.Name == "" {
			return fmt.Errorf("spec.supportedModelFormats[%d]: modelFormat.name is required", i)
		}
		if f.ModelFramework == nil || f.ModelFramework.Name == "" {
			return fmt.Errorf("spec.supportedModelFormats[%d]: modelFramework.name is required", i)
		}
	}
	return nil
}

func ValidateServingRuntimeConfiguration(spec *v1beta1.ServingRuntimeSpec) error {
	hasEngineConfig := spec.EngineConfig != nil
	hasDecoderConfig := spec.DecoderConfig != nil

	if hasEngineConfig && hasDecoderConfig {
		return nil
	}

	if spec.WorkerPodSpec != nil && spec.WorkerPodSpec.Size != nil && *spec.WorkerPodSpec.Size <= 0 {
		return errors.New(multiNodeConfigurationError)
	}

	isExplicitMultiNode := false
	isExplicitRawDeployment := false
	for _, container := range spec.Containers {
		for _, env := range container.Env {
			if env.Name == "DEPLOYMENT_MODE" {
				if env.Value == string(constants.MultiNode) {
					isExplicitMultiNode = true
				} else if env.Value == string(constants.RawDeployment) {
					isExplicitRawDeployment = true
				}
			}
		}
	}

	if hasEngineConfig && !hasDecoderConfig {
		isMultiNode := isExplicitMultiNode

		if !isExplicitMultiNode && !isExplicitRawDeployment {
			if spec.WorkerPodSpec != nil && spec.WorkerPodSpec.Size != nil && *spec.WorkerPodSpec.Size > 0 {
				isMultiNode = true
			}
		}

		if isMultiNode {
			if spec.WorkerPodSpec == nil || spec.WorkerPodSpec.Size == nil || *spec.WorkerPodSpec.Size <= 0 {
				return errors.New(multiNodeConfigurationError)
			}
		}

		if isExplicitRawDeployment {
			if spec.WorkerPodSpec != nil && spec.WorkerPodSpec.Size != nil && *spec.WorkerPodSpec.Size > 0 {
				return errors.New(rawDeploymentConfigurationError)
			}
		} else if !isMultiNode {
			if spec.WorkerPodSpec != nil && spec.WorkerPodSpec.Size != nil && *spec.WorkerPodSpec.Size > 0 {
				return errors.New(rawDeploymentConfigurationError)
			}
		}
	}

	return nil
}

func ValidateModelFormatPrioritySame(spec *v1beta1.ServingRuntimeSpec) error {
	nameToPriority := make(map[string]*int32)

	for _, f := range spec.SupportedModelFormats {
		if f.IsAutoSelectEnabled() {
			if existingPriority, ok := nameToPriority[f.Name]; ok {
				if existingPriority != nil && f.Priority != nil && (*existingPriority != *f.Priority) {
					return fmt.Errorf(priorityIsNotSameError, f.Name)
				}
			} else {
				nameToPriority[f.Name] = f.Priority
			}
		}
	}
	return nil
}
