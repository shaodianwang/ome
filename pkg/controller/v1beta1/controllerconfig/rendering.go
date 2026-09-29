package controllerconfig

import (
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// RenderingConfig parses the same configuration used by component rendering
// from an explicitly identified ConfigMap, without namespace or client lookup.
func RenderingConfig(cm *corev1.ConfigMap) (*InferenceServicesConfig, *DeployConfig, error) {
	if cm == nil {
		return nil, nil, fmt.Errorf("rendering requires the member configuration")
	}
	if strings.TrimSpace(cm.Data[DeployConfigName]) == "null" {
		return nil, nil, fmt.Errorf("deploy configuration must be an object")
	}
	cfg, err := inferenceServicesConfigFromConfigMap(cm)
	if err != nil {
		return nil, nil, err
	}
	if data, ok := cm.Data[ModelCacheConfigName]; ok {
		if err := json.Unmarshal([]byte(data), &cfg.ModelCache); err != nil {
			return nil, nil, fmt.Errorf("unable to parse model cache config json: %w", err)
		}
	}
	deploy, err := parseDeployConfig(cm)
	if err != nil {
		return nil, nil, err
	}
	return cfg, deploy, nil
}

// DeploymentConfig parses replica and lifecycle defaults without requiring
// unrelated ingress, storage, or model rendering configuration.
func DeploymentConfig(cm *corev1.ConfigMap) (*DeployConfig, error) {
	if cm == nil || strings.TrimSpace(cm.Data[DeployConfigName]) == "null" {
		return nil, fmt.Errorf("deploy configuration must be an object")
	}
	return parseDeployConfig(cm)
}
