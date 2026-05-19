package validation

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func ValidateInferenceServiceName(name string) error {
	if !IsvcNameRegex.MatchString(name) {
		return fmt.Errorf("invalid InferenceService name %q, must match %q", name, IsvcNameFmt)
	}
	return nil
}

func ValidateEngineDecoderConfig(spec *v1beta1.InferenceServiceSpec) error {
	if spec.Decoder != nil && spec.Engine == nil {
		return fmt.Errorf("decoder cannot be specified without engine")
	}
	return nil
}

// ValidateLeaderWorkerPairing enforces that the Leader / Worker spec
// pair on each Component (Engine, Decoder) is internally consistent:
// either both are absent (single-pod) or both are present with
// Worker.Size > 0 (multi-pod). Rejects orphan-leader, orphan-worker, and
// declared-worker-with-no-positive-size configurations.
func ValidateLeaderWorkerPairing(spec *v1beta1.InferenceServiceSpec) error {
	if spec.Engine != nil {
		if err := validateLeaderWorkerPairing("engine", spec.Engine.Leader, spec.Engine.Worker); err != nil {
			return err
		}
	}
	if spec.Decoder != nil {
		if err := validateLeaderWorkerPairing("decoder", spec.Decoder.Leader, spec.Decoder.Worker); err != nil {
			return err
		}
	}
	return nil
}

func validateLeaderWorkerPairing(component string, leader *v1beta1.LeaderSpec, worker *v1beta1.WorkerSpec) error {
	if leader == nil && worker == nil {
		return nil
	}
	if leader != nil && worker == nil {
		return fmt.Errorf(
			"InvalidLeaderWorkerPairing: %s.leader is set but %s.worker is not; multi-pod configurations require both",
			component, component,
		)
	}
	if leader == nil && worker != nil {
		return fmt.Errorf(
			"InvalidLeaderWorkerPairing: %s.worker is set but %s.leader is not; multi-pod configurations require both",
			component, component,
		)
	}
	if worker.Size == nil || *worker.Size <= 0 {
		return fmt.Errorf(
			"InvalidLeaderWorkerPairing: %s.worker is set but worker.size is not a positive integer; multi-pod configurations require worker.size > 0",
			component,
		)
	}
	return nil
}

// ValidateEngineDecoderDeploymentMode enforces that when both Engine and
// Decoder are present and either carries ome.io/deploymentMode=OMENative,
// both components carry the same value for that annotation.
func ValidateEngineDecoderDeploymentMode(spec *v1beta1.InferenceServiceSpec) error {
	if spec.Engine == nil || spec.Decoder == nil {
		return nil
	}
	engineMode := spec.Engine.Annotations[constants.DeploymentMode]
	decoderMode := spec.Decoder.Annotations[constants.DeploymentMode]
	if engineMode != string(constants.OMENative) && decoderMode != string(constants.OMENative) {
		return nil
	}
	if engineMode != decoderMode {
		return fmt.Errorf(
			"InvalidDeploymentModeCombination: engine %s=%q does not match decoder %s=%q; when either is OMENative, both must use the same value",
			constants.DeploymentMode, engineMode,
			constants.DeploymentMode, decoderMode,
		)
	}
	return nil
}

func ValidateReplicaBounds(min, max *int) error {
	if min != nil && *min < 0 {
		return fmt.Errorf("minReplicas must be >= 0")
	}
	if max != nil && *max < 1 {
		return fmt.Errorf("maxReplicas must be >= 1")
	}
	if min != nil && max != nil && *min > *max {
		return fmt.Errorf("minReplicas (%d) must be <= maxReplicas (%d)", *min, *max)
	}
	return nil
}

// ValidateAutoscalerConfig validates autoscaler-related annotations and spec
// fields. It returns warnings (non-fatal messages) and an error if validation
// fails.
func ValidateAutoscalerConfig(isvc *v1beta1.InferenceService) ([]string, error) {
	annotations := isvc.ObjectMeta.Annotations
	value, ok := annotations[constants.AutoscalerClass]
	class := constants.AutoscalerClassType(value)
	if ok {
		for _, item := range constants.AutoscalerAllowedClassList {
			if class == item {
				switch class {
				case constants.AutoscalerClassHPA:
					if metric, ok := annotations[constants.AutoscalerMetrics]; ok {
						return nil, ValidateHPAMetrics(v1beta1.ScaleMetric(metric))
					}
					return nil, nil
				case constants.AutoscalerClassKEDA:
					return ValidateKEDAConfig(isvc)
				case constants.AutoscalerClassExternal:
					return nil, nil
				default:
					return nil, fmt.Errorf("unknown autoscaler class [%s]", class)
				}
			}
		}
		return nil, fmt.Errorf("[%s] is not a supported autoscaler class type", value)
	}
	return nil, nil
}

func ValidateAutoscalerTargetUtilizationPercentage(isvc *v1beta1.InferenceService) error {
	annotations := isvc.ObjectMeta.Annotations
	if value, ok := annotations[constants.TargetUtilizationPercentage]; ok {
		t, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("the target utilization percentage should be a [1-100] integer")
		}
		if t < 1 || t > 100 {
			return fmt.Errorf("the target utilization percentage should be a [1-100] integer")
		}
	}
	return nil
}

func ValidateHPAMetrics(metric v1beta1.ScaleMetric) error {
	for _, item := range constants.AutoscalerAllowedMetricsList {
		if item == constants.AutoscalerMetricsType(metric) {
			return nil
		}
	}
	return fmt.Errorf("[%s] is not a supported metric", metric)
}

var kedaValidScalingOperators = []string{
	"GreaterThan",
	"GreaterThanOrEqual",
	"LessThan",
	"LessThanOrEqual",
}

var kedaValidAuthModes = []string{
	"basic",
	"tls",
	"bearer",
	"custom",
}

func ValidateKEDAConfig(isvc *v1beta1.InferenceService) ([]string, error) {
	var warnings []string
	kedaConfig := isvc.Spec.KedaConfig
	annotations := isvc.ObjectMeta.Annotations

	if kedaConfig != nil && kedaConfig.ScalingOperator != "" {
		if err := ValidateKEDAScalingOperator(kedaConfig.ScalingOperator); err != nil {
			return warnings, err
		}
	}

	if operatorAnnotation, ok := annotations[constants.KedaScalingOperator]; ok {
		if err := ValidateKEDAScalingOperator(operatorAnnotation); err != nil {
			return warnings, err
		}
	}

	if kedaConfig != nil && kedaConfig.ScalingThreshold != "" {
		if err := ValidateKEDAScalingThreshold(kedaConfig.ScalingThreshold); err != nil {
			return warnings, err
		}
	}

	if thresholdAnnotation, ok := annotations[constants.KedaScalingThreshold]; ok {
		if err := ValidateKEDAScalingThreshold(thresholdAnnotation); err != nil {
			return warnings, err
		}
	}

	if kedaConfig != nil && kedaConfig.PromServerAddress != "" {
		if err := ValidateKEDAPrometheusServerAddress(kedaConfig.PromServerAddress); err != nil {
			return warnings, err
		}
	}

	if promAddrAnnotation, ok := annotations[constants.KedaPrometheusServerAddress]; ok {
		if err := ValidateKEDAPrometheusServerAddress(promAddrAnnotation); err != nil {
			return warnings, err
		}
	}

	if kedaConfig != nil && kedaConfig.AuthModes != "" {
		if err := ValidateKEDAAuthModes(kedaConfig.AuthModes); err != nil {
			return warnings, err
		}
		if kedaConfig.AuthenticationRef == nil {
			warnings = append(warnings, "KEDA authModes is specified but authenticationRef is not set; authModes will be ignored by KEDA")
		}
	}

	return warnings, nil
}

func ValidateKEDAScalingOperator(operator string) error {
	for _, valid := range kedaValidScalingOperators {
		if operator == valid {
			return nil
		}
	}
	return fmt.Errorf("invalid KEDA scaling operator %q, must be one of: %s", operator, strings.Join(kedaValidScalingOperators, ", "))
}

func ValidateKEDAScalingThreshold(threshold string) error {
	_, err := strconv.ParseFloat(threshold, 64)
	if err != nil {
		return fmt.Errorf("invalid KEDA scaling threshold %q: must be a valid number", threshold)
	}
	return nil
}

func ValidateKEDAPrometheusServerAddress(address string) error {
	parsedURL, err := url.Parse(address)
	if err != nil {
		return fmt.Errorf("invalid KEDA Prometheus server address %q: %v", address, err)
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("invalid KEDA Prometheus server address %q: scheme must be http or https", address)
	}
	if parsedURL.Host == "" {
		return fmt.Errorf("invalid KEDA Prometheus server address %q: host is required", address)
	}
	return nil
}

func ValidateKEDAAuthModes(authModes string) error {
	modes := strings.Split(authModes, ",")
	for _, mode := range modes {
		mode = strings.TrimSpace(mode)
		if mode == "" {
			continue
		}
		valid := false
		for _, validMode := range kedaValidAuthModes {
			if mode == validMode {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("invalid KEDA auth mode %q, must be one of: %s", mode, strings.Join(kedaValidAuthModes, ", "))
		}
	}
	return nil
}
