package controllerconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"text/template"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
)

const (
	IngressConfigKeyName = "ingress"
	DeployConfigName     = "deploy"
	OmeAgentConfigName   = "omeAgent"
	ModelCacheConfigName = "modelCache"

	DefaultDomainTemplate = "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}"
	DefaultIngressDomain  = "example.com"

	DefaultUrlScheme = "http"
)

var DefaultConsistentHashHeaders = []string{"x-routing-key", "X-SMG-Routing-Key"}

type SecretConfig struct {
	WriteToCommonNamespace bool   `json:"writeToCommonNamespace"`
	Namespace              string `json:"namespace"`
	SecretName             string `json:"secretName"`
}

// +kubebuilder:object:generate=false
type InferenceServicesConfig struct {
	// ModelCache contains the configured model cache provider used by
	// sharded model-serving runtimes.
	ModelCache sharded.ProviderConfig `json:"modelCache,omitempty"`
}

// +kubebuilder:object:generate=false
type IngressConfig struct {
	IngressGateway           string    `json:"ingressGateway,omitempty"`
	IngressServiceName       string    `json:"ingressService,omitempty"`
	OmeIngressGateway        string    `json:"omeIngressGateway,omitempty"`
	IngressDomain            string    `json:"ingressDomain,omitempty"`
	IngressClassName         *string   `json:"ingressClassName,omitempty"`
	AdditionalIngressDomains *[]string `json:"additionalIngressDomains,omitempty"`
	DomainTemplate           string    `json:"domainTemplate,omitempty"`
	UrlScheme                string    `json:"urlScheme,omitempty"`
	PathTemplate             string    `json:"pathTemplate,omitempty"`
	DisableIngressCreation   bool      `json:"disableIngressCreation,omitempty"`
	EnableGatewayAPI         bool      `json:"enableGatewayAPI,omitempty"`
	ConsistentHashHeaders    []string  `json:"consistentHashHeaders,omitempty"`
	// PerISVCSubdomain opts into a per-ISVC subdomain ingress scheme:
	// each ISVC is reached at its own host (matching status.url, rendered
	// from DomainTemplate) with a root path, instead of the default shared
	// "llm.<IngressDomain>" host plus a "/<namespace>/<service>/" path
	// prefix. Default false preserves the shared-host scheme.
	PerISVCSubdomain bool `json:"perISVCSubdomain,omitempty"`
}

// +kubebuilder:object:generate=false
type DeployConfig struct {
	DefaultDeploymentMode string `json:"defaultDeploymentMode,omitempty"`
}

// +kubebuilder:object:generate=false
// OmeAgentConfig configures the metadata-extraction Job spawned by the
// BaseModel/ClusterBaseModel controller for PVC-backed models. The Job runs
// `ome-agent model-metadata` against the PVC.
type OmeAgentConfig struct {
	// Image is the ome-agent container image. Required.
	Image string `json:"image"`
	// ServiceAccount the metadata Job pod runs as. Must have RBAC to
	// get/list/create/update/patch ConfigMaps in the OME namespace —
	// the agent surfaces extracted metadata via a per-model status
	// ConfigMap, not via direct CR updates.
	ServiceAccount string `json:"serviceAccount,omitempty"`
	// CPURequest/MemoryRequest/CPULimit/MemoryLimit follow the standard
	// resource shape; empty strings fall back to the K8s default.
	CPURequest    string `json:"cpuRequest,omitempty"`
	MemoryRequest string `json:"memoryRequest,omitempty"`
	CPULimit      string `json:"cpuLimit,omitempty"`
	MemoryLimit   string `json:"memoryLimit,omitempty"`
	// BackoffLimit caps Job retries. Default 2 if zero.
	BackoffLimit int32 `json:"backoffLimit,omitempty"`
	// TTLSecondsAfterFinished controls cleanup of completed Jobs.
	// Default 3600 if zero.
	TTLSecondsAfterFinished int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// NodeSelector / Tolerations / Affinity / PriorityClassName are
	// pass-through scheduling hints applied to the metadata Job pod.
	// Necessary when the PVC's CSI driver only mounts on a subset of
	// nodes, or when the cluster taints GPU nodes that hold the models.
	NodeSelector      map[string]string `json:"nodeSelector,omitempty"`
	Tolerations       []v1.Toleration   `json:"tolerations,omitempty"`
	Affinity          *v1.Affinity      `json:"affinity,omitempty"`
	PriorityClassName string            `json:"priorityClassName,omitempty"`
}

// NewInferenceServicesConfig verifies that the inferenceservice ConfigMap
// exists (callers fail fast otherwise) and returns an empty config struct.
// ModelCache is loaded separately via NewModelCacheConfig.
func NewInferenceServicesConfig(clientset kubernetes.Interface) (*InferenceServicesConfig, error) {
	if _, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(context.TODO(), constants.InferenceServiceConfigMapName, metav1.GetOptions{}); err != nil {
		return nil, err
	}
	return &InferenceServicesConfig{}, nil
}

// NewOmeAgentConfig loads the ome-agent block from the inferenceservice
// ConfigMap. Returns a zero-value config (with no Image) if the block is
// absent so the BaseModel reconciler can surface a clearer error to the
// user via the model status when a PVC-backed model needs the Job.
func NewOmeAgentConfig(clientset kubernetes.Interface) (*OmeAgentConfig, error) {
	configMap, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(context.TODO(), constants.InferenceServiceConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	cfg := &OmeAgentConfig{}
	if data, ok := configMap.Data[OmeAgentConfigName]; ok {
		if err := json.Unmarshal([]byte(data), cfg); err != nil {
			return nil, fmt.Errorf("unable to parse omeAgent config json: %w", err)
		}
	}
	return cfg, nil
}

func NewIngressConfig(clientset kubernetes.Interface) (*IngressConfig, error) {
	configMap, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(context.TODO(), constants.InferenceServiceConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	ingressConfig := &IngressConfig{}
	if ingress, ok := configMap.Data[IngressConfigKeyName]; ok {
		err := json.Unmarshal([]byte(ingress), &ingressConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to parse ingress config json: %w", err)
		}

		if ingressConfig.IngressGateway == "" || ingressConfig.IngressServiceName == "" {
			return nil, fmt.Errorf("invalid ingress config - ingressGateway and ingressService are required")
		}
		if ingressConfig.PathTemplate != "" {
			// TODO: ensure that the generated path is valid, that is:
			// * both Name and Namespace are used to avoid collisions
			// * starts with a /
			// For now simply check that this is a valid template.
			_, err := template.New("path-template").Parse(ingressConfig.PathTemplate)
			if err != nil {
				return nil, fmt.Errorf("invalid ingress config, unable to parse pathTemplate: %w", err)
			}
			if ingressConfig.IngressDomain == "" {
				return nil, fmt.Errorf("invalid ingress config - ingressDomain is required if pathTemplate is given")
			}
		}
	}

	if ingressConfig.DomainTemplate == "" {
		ingressConfig.DomainTemplate = DefaultDomainTemplate
	}

	if ingressConfig.IngressDomain == "" {
		ingressConfig.IngressDomain = DefaultIngressDomain
	}

	if ingressConfig.UrlScheme == "" {
		ingressConfig.UrlScheme = DefaultUrlScheme
	}

	if len(ingressConfig.ConsistentHashHeaders) == 0 {
		ingressConfig.ConsistentHashHeaders = DefaultConsistentHashHeaders
	}

	return ingressConfig, nil
}

func getComponentConfig(key string, configMap *v1.ConfigMap, componentConfig interface{}) error {
	if data, ok := configMap.Data[key]; ok {
		err := json.Unmarshal([]byte(data), componentConfig)
		if err != nil {
			return fmt.Errorf("unable to unmarshall %v json string due to %w ", key, err)
		}
	}
	return nil
}

func NewDeployConfig(clientset kubernetes.Interface) (*DeployConfig, error) {
	configMap, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(context.TODO(), constants.InferenceServiceConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	deployConfig := &DeployConfig{}
	if deploy, ok := configMap.Data[DeployConfigName]; ok {
		err := json.Unmarshal([]byte(deploy), &deployConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to parse deploy config json: %w", err)
		}

		if deployConfig.DefaultDeploymentMode == "" {
			return nil, fmt.Errorf("invalid deploy config, defaultDeploymentMode is required")
		}

		if deployConfig.DefaultDeploymentMode != string(constants.RawDeployment) {
			return nil, fmt.Errorf("invalid deployment mode. Supported default mode is %s", constants.RawDeployment)
		}
	}
	return deployConfig, nil
}

func NewModelCacheConfig(clientset kubernetes.Interface) (*sharded.ProviderConfig, error) {
	configMap, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(context.TODO(), constants.InferenceServiceConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	modelCacheConfig := &sharded.ProviderConfig{}
	if modelCache, ok := configMap.Data[ModelCacheConfigName]; ok {
		err := json.Unmarshal([]byte(modelCache), modelCacheConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to parse model cache config json: %w", err)
		}
	}
	return modelCacheConfig, nil
}
