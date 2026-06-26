package controllerconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"text/template"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
)

const (
	IngressConfigKeyName     = "ingress"
	DeployConfigName         = "deploy"
	OmeAgentConfigName       = "omeAgent"
	ModelCacheConfigName     = "modelCache"
	CanaryAnalysisConfigName = "canaryAnalysis"
	CoordinationConfigName   = "coordination"

	DefaultDomainTemplate = "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}"
	DefaultIngressDomain  = "example.com"

	DefaultUrlScheme = "http"

	// Operational fallbacks for the canary metric-analysis sampler, applied by the
	// loader only when the ConfigMap omits them (the chart sets the real values).
	// These tune the sampler, not deployment identity, so an in-loader fallback is
	// appropriate (mirroring DefaultDomainTemplate); the source address has no such
	// fallback (see CanaryAnalysisConfig.BundledPrometheusAddress).
	DefaultAnalysisQueryTimeout   = "5s"
	DefaultAnalysisCacheTTL       = "2m"
	DefaultAnalysisMaxConcurrency = 8
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
	// "<SharedHostPrefix>.<IngressDomain>" host plus a "/<namespace>/<service>/"
	// path prefix. Default false preserves the shared-host scheme.
	PerISVCSubdomain bool `json:"perISVCSubdomain,omitempty"`
	// SharedHostPrefix is the subdomain label prepended to IngressDomain in the
	// shared-host scheme (PerISVCSubdomain=false): "<SharedHostPrefix>.<IngressDomain>".
	// There is intentionally NO in-code default — the value is supplied via the
	// inferenceservice-config ConfigMap (set by the Helm chart / GitOps), so the
	// host label is never a hardcoded magic string. Empty yields the bare
	// IngressDomain (no prefix).
	SharedHostPrefix string `json:"sharedHostPrefix,omitempty"`
	// AdditionalIngressGateways attaches each generated HTTPRoute to extra
	// gateways beyond the primary OmeIngressGateway — e.g. an external gateway
	// alongside the internal one. Each entry contributes its own parentRef and a
	// hostname rendered from its own IngressDomain. Empty preserves the
	// single-gateway behavior.
	AdditionalIngressGateways []IngressGatewaySpec `json:"additionalIngressGateways,omitempty"`
}

// IngressGatewaySpec identifies one gateway an HTTPRoute attaches to, plus the
// domain used to build that gateway's hostname for the route.
type IngressGatewaySpec struct {
	// OmeIngressGateway is the gateway parentRef in "namespace/name" form.
	OmeIngressGateway string `json:"omeIngressGateway"`
	// IngressDomain is the domain used to render this gateway's route hostname
	// (same scheme as the primary: per-ISVC subdomain or shared-host prefix).
	IngressDomain string `json:"ingressDomain"`
}

// +kubebuilder:object:generate=false
type DeployConfig struct {
	DefaultDeploymentMode string `json:"defaultDeploymentMode,omitempty"`
}

// +kubebuilder:object:generate=false
// CanaryAnalysisConfig holds operator-level defaults for metric-gated canary
// promotion: the default Prometheus source and the sampler's tuning. A per-CR
// spec.rollout.canary.prometheus.serverAddress overrides BundledPrometheusAddress.
type CanaryAnalysisConfig struct {
	// BundledPrometheusAddress is the metrics source queried when a canary does not
	// set its own spec.rollout.canary.prometheus.serverAddress. There is
	// intentionally NO in-code default — the value is supplied via the
	// inferenceservice-config ConfigMap (set by the Helm chart / GitOps), so the
	// address is never a hardcoded magic string. Empty means a canary with no
	// per-CR source has no source, and its samples read as inconclusive — it does
	// not silently fall back to a baked-in address.
	BundledPrometheusAddress string `json:"bundledPrometheusAddress,omitempty"`
	// QueryTimeout bounds one sampling pass (all of a step's metric queries). A
	// duration string (e.g. "5s"); the loader falls back to DefaultAnalysisQueryTimeout.
	QueryTimeout string `json:"queryTimeout,omitempty"`
	// MaxConcurrency caps how many analysis queries run in the background at once,
	// across all canaries. It is a fleet-wide ceiling: with many concurrent canaries
	// queries queue behind it and the effective sample interval degrades (watch the
	// ome_canary_sampler_queue_depth / _starved_total metrics), so scale this with
	// fleet size. The loader falls back to DefaultAnalysisMaxConcurrency.
	MaxConcurrency int32 `json:"maxConcurrency,omitempty"`
	// CacheTTL is how long a sampled result stays usable before the sampler evicts
	// it. A duration string; the loader falls back to DefaultAnalysisCacheTTL.
	CacheTTL string `json:"cacheTTL,omitempty"`
}

// +kubebuilder:object:generate=false
// CoordinationConfig holds operator-level tuning for the OMENative cross-Component
// coordination layer (the pod-proportional per-revision traffic producer).
type CoordinationConfig struct {
	// TrafficWeightDeadbandPercent is the per-revision hysteresis band applied to
	// the pod-proportional traffic writer: a recomputed weight set whose every
	// per-revision percent moves by strictly less than this from what is already in
	// Status.Components.<c>.Traffic is treated as pod-count jitter and the status
	// write (plus the HTTPRoute re-enqueue it triggers) is suppressed. Dampens the
	// continuous churn from pods momentarily Pending between reconciles at scale.
	//
	// There is intentionally NO in-code default — the value is supplied via the
	// inferenceservice-config ConfigMap (set by the Helm chart / GitOps). Zero
	// (absent config) disables the band and preserves the exact prior
	// write-on-any-diff behavior, so eventual correctness is never traded away
	// silently.
	TrafficWeightDeadbandPercent int32 `json:"trafficWeightDeadbandPercent,omitempty"`
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

// getInferenceServiceConfigMap fetches the inferenceservice-config ConfigMap
// directly from the apiserver via the clientset. This is the single point all
// config constructors below resolve through; a ConfigCache (see below) wraps it
// with a short TTL so a single reconcile pass does not issue one uncached GET
// per config block.
func getInferenceServiceConfigMap(clientset kubernetes.Interface) (*v1.ConfigMap, error) {
	return clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(context.TODO(), constants.InferenceServiceConfigMapName, metav1.GetOptions{})
}

// ConfigCache caches the inferenceservice-config ConfigMap for a configurable
// TTL so the per-reconcile config constructors share one apiserver GET instead
// of issuing one each. The TTL is supplied by the caller (flag/chart-driven —
// no in-code behavioral default); a short TTL preserves the property that a
// ConfigMap edit takes effect without a controller restart, since the entry is
// re-fetched once the TTL elapses.
//
// A zero (or negative) TTL disables caching: every call falls through to the
// apiserver, exactly reproducing the pre-cache behavior.
//
// +kubebuilder:object:generate=false
type ConfigCache struct {
	ttl time.Duration
	now func() time.Time

	mu        sync.Mutex
	cm        *v1.ConfigMap
	fetchedAt time.Time
}

// NewConfigCache returns a ConfigCache with the given TTL.
func NewConfigCache(ttl time.Duration) *ConfigCache {
	return &ConfigCache{ttl: ttl, now: time.Now}
}

// get returns the cached ConfigMap when the entry is fresh, otherwise fetches
// it from the apiserver and refreshes the entry. A non-positive TTL never
// caches. The returned ConfigMap is shared (read-only): the config constructors
// only read .Data, never mutate it.
func (c *ConfigCache) get(clientset kubernetes.Interface) (*v1.ConfigMap, error) {
	if c == nil || c.ttl <= 0 {
		return getInferenceServiceConfigMap(clientset)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cm != nil && c.now().Sub(c.fetchedAt) < c.ttl {
		return c.cm, nil
	}
	cm, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		// On error, drop any stale entry so the next call retries rather than
		// serving a config that may no longer exist.
		c.cm = nil
		return nil, err
	}
	c.cm = cm
	c.fetchedAt = c.now()
	return cm, nil
}

// NewInferenceServicesConfig verifies that the inferenceservice ConfigMap
// exists (callers fail fast otherwise) and returns an empty config struct.
// ModelCache is loaded separately via NewModelCacheConfig.
func NewInferenceServicesConfig(clientset kubernetes.Interface) (*InferenceServicesConfig, error) {
	if _, err := getInferenceServiceConfigMap(clientset); err != nil {
		return nil, err
	}
	return &InferenceServicesConfig{}, nil
}

// NewInferenceServicesConfigCached is the ConfigCache-backed variant of
// NewInferenceServicesConfig used on the reconcile hot path.
func NewInferenceServicesConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*InferenceServicesConfig, error) {
	if _, err := cache.get(clientset); err != nil {
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
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseIngressConfig(configMap)
}

// NewIngressConfigCached is the ConfigCache-backed variant of NewIngressConfig
// used on the reconcile hot path.
func NewIngressConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*IngressConfig, error) {
	configMap, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return parseIngressConfig(configMap)
}

func parseIngressConfig(configMap *v1.ConfigMap) (*IngressConfig, error) {
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
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseDeployConfig(configMap)
}

// NewDeployConfigCached is the ConfigCache-backed variant of NewDeployConfig
// used on the reconcile hot path.
func NewDeployConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*DeployConfig, error) {
	configMap, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return parseDeployConfig(configMap)
}

func parseDeployConfig(configMap *v1.ConfigMap) (*DeployConfig, error) {
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
	configMap, err := getInferenceServiceConfigMap(clientset)
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

// NewCanaryAnalysisConfig loads the canary metric-analysis defaults from the
// inferenceservice-config ConfigMap, applying operational fallbacks for the
// sampler tunables. BundledPrometheusAddress has no fallback (the chart is the
// source of truth); empty leaves canaries without a default metrics source.
func NewCanaryAnalysisConfig(clientset kubernetes.Interface) (*CanaryAnalysisConfig, error) {
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseCanaryAnalysisConfig(configMap)
}

// NewCanaryAnalysisConfigCached is the ConfigCache-backed variant of
// NewCanaryAnalysisConfig used on the reconcile hot path.
func NewCanaryAnalysisConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*CanaryAnalysisConfig, error) {
	configMap, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return parseCanaryAnalysisConfig(configMap)
}

func parseCanaryAnalysisConfig(configMap *v1.ConfigMap) (*CanaryAnalysisConfig, error) {
	cfg := &CanaryAnalysisConfig{}
	if err := getComponentConfig(CanaryAnalysisConfigName, configMap, cfg); err != nil {
		return nil, fmt.Errorf("unable to parse canaryAnalysis config json: %w", err)
	}
	if cfg.QueryTimeout == "" {
		cfg.QueryTimeout = DefaultAnalysisQueryTimeout
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = DefaultAnalysisMaxConcurrency
	}
	if cfg.CacheTTL == "" {
		cfg.CacheTTL = DefaultAnalysisCacheTTL
	}
	return cfg, nil
}

// NewCoordinationConfig loads the OMENative coordination tuning from the
// inferenceservice-config ConfigMap. There are no in-code defaults: an absent
// "coordination" key yields a zero-valued config (deadband disabled), preserving
// the prior write-on-any-diff behavior.
func NewCoordinationConfig(clientset kubernetes.Interface) (*CoordinationConfig, error) {
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseCoordinationConfig(configMap)
}

// NewCoordinationConfigCached is the ConfigCache-backed variant of
// NewCoordinationConfig used on the reconcile hot path.
func NewCoordinationConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*CoordinationConfig, error) {
	configMap, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return parseCoordinationConfig(configMap)
}

func parseCoordinationConfig(configMap *v1.ConfigMap) (*CoordinationConfig, error) {
	cfg := &CoordinationConfig{}
	if err := getComponentConfig(CoordinationConfigName, configMap, cfg); err != nil {
		return nil, fmt.Errorf("unable to parse coordination config json: %w", err)
	}
	return cfg, nil
}

// QueryTimeoutDuration parses QueryTimeout, falling back to the known-good
// DefaultAnalysisQueryTimeout when an operator wrote an unparseable or
// non-positive value. The timeout MUST be positive: a zero/negative value would
// leave a background analysis query unbounded, so a hung Prometheus could park a
// sampler goroutine indefinitely.
func (c *CanaryAnalysisConfig) QueryTimeoutDuration() time.Duration {
	return parseDurationOr(c.QueryTimeout, DefaultAnalysisQueryTimeout)
}

// CacheTTLDuration parses CacheTTL, falling back to the known-good
// DefaultAnalysisCacheTTL when an operator wrote an unparseable or non-positive
// value. The TTL MUST be positive: a zero/negative value disables cache eviction
// (unbounded growth).
func (c *CanaryAnalysisConfig) CacheTTLDuration() time.Duration {
	return parseDurationOr(c.CacheTTL, DefaultAnalysisCacheTTL)
}

// parseDurationOr parses s, returning the parse of fallback (a compile-time
// constant known to parse) when s is empty, malformed, or non-positive — callers
// rely on a strictly positive result.
func parseDurationOr(s, fallback string) time.Duration {
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	d, _ := time.ParseDuration(fallback)
	return d
}
