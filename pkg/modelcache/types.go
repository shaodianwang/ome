package modelcache

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

const (
	// ProviderClusterCache is the provider label for Meta's cluster_cache implementation.
	ProviderClusterCache = "cluster_cache"

	// BackendClusterCache is kept for compatibility with earlier status code.
	// Deprecated: use ProviderClusterCache.
	BackendClusterCache = ProviderClusterCache
)

// Client is the high-level OME control-plane contract for a model cache.
type Client interface {
	CheckSource(ctx context.Context, source Source) (*SourceStatus, error)
	InspectSource(ctx context.Context, source Source) (*SourceInspection, error)
	StartPrewarm(ctx context.Context, req PrewarmRequest) (*PrewarmOperation, error)
	GetPrewarmStatus(ctx context.Context, operation PrewarmOperation) (*PrewarmStatus, error)
}

// ProviderConfig identifies one model cache provider instance.
type ProviderConfig struct {
	Provider string            `json:"provider,omitempty"`
	Endpoint string            `json:"endpoint,omitempty"`
	Options  map[string]string `json:"options,omitempty"`
}

// Provider binds a provider name to the client implementation used by OME.
type Provider struct {
	Name   string
	Client Client
}

// NewProvider returns a configured model cache provider.
func NewProvider(name string, client Client) Provider {
	return Provider{
		Name:   name,
		Client: client,
	}
}

// Configured reports whether the provider can serve controller requests.
func (p Provider) Configured() bool {
	return p.Name != "" && p.Client != nil
}

// ProviderFactory builds a provider client from controller configuration.
type ProviderFactory func(config ProviderConfig) (Client, error)

// Registry stores provider factories keyed by provider name.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]ProviderFactory
}

// NewRegistry returns an empty provider registry.
func NewRegistry() *Registry {
	return &Registry{
		factories: map[string]ProviderFactory{},
	}
}

// Register adds a provider factory to the registry.
func (r *Registry) Register(provider string, factory ProviderFactory) error {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return fmt.Errorf("model cache provider name is required")
	}
	if factory == nil {
		return fmt.Errorf("model cache provider %q factory is required", provider)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.factories[provider]; ok {
		return fmt.Errorf("model cache provider %q is already registered", provider)
	}
	r.factories[provider] = factory
	return nil
}

// Build constructs a model cache provider from configuration.
func (r *Registry) Build(config ProviderConfig) (Provider, error) {
	provider := strings.TrimSpace(config.Provider)
	if provider == "" {
		return Provider{}, nil
	}

	r.mu.RLock()
	factory, ok := r.factories[provider]
	r.mu.RUnlock()
	if !ok {
		return Provider{}, fmt.Errorf("model cache provider %q is not registered", provider)
	}

	config.Provider = provider
	client, err := factory(config)
	if err != nil {
		return Provider{}, fmt.Errorf("create model cache provider %q: %w", provider, err)
	}
	if client == nil {
		return Provider{}, fmt.Errorf("model cache provider %q returned nil client", provider)
	}

	return NewProvider(provider, client), nil
}

var defaultRegistry = NewRegistry()

// RegisterProvider adds a provider factory to the default registry.
func RegisterProvider(provider string, factory ProviderFactory) error {
	return defaultRegistry.Register(provider, factory)
}

// BuildProvider constructs a provider from the default registry.
func BuildProvider(config ProviderConfig) (Provider, error) {
	return defaultRegistry.Build(config)
}

// Source identifies a model source that should be handled by a cache backend.
type Source struct {
	URI        string
	AuthRef    *AuthRef
	Parameters map[string]string
}

// AuthRef identifies authentication material without prescribing how a backend resolves it.
type AuthRef struct {
	Namespace string
	Name      string
	Key       string
}

// SourceStatus reports source reachability without exposing backend transport details.
type SourceStatus struct {
	Reachable bool
	Reason    string
	Message   string
}

// SourceInspection is a provider-neutral description of a model source plus
// the small metadata payloads OME needs for model parsing.
type SourceInspection struct {
	Objects  []Object
	Metadata ModelMetadataInputs
}

// ModelMetadataInputs contains small model metadata blobs returned by source
// inspection. Providers must not include tensor bodies here.
type ModelMetadataInputs struct {
	ConfigJSON           []byte
	ModelIndexJSON       []byte
	SafetensorsIndexJSON []byte
	TokenizerConfigJSON  []byte
	SafetensorHeaders    []SafetensorHeader
}

// SafetensorHeader describes one safetensors shard and its JSON header.
type SafetensorHeader struct {
	Path       string
	SizeBytes  int64
	Version    string
	HeaderJSON []byte
}

// ObjectRef identifies one object within a model source.
type ObjectRef struct {
	Source Source
	Path   string
}

// Object describes one source object using backend-neutral metadata.
type Object struct {
	Source    Source
	Path      string
	SizeBytes int64
	Version   string
}

// PrewarmRequest asks the model cache to prewarm a source or known objects.
type PrewarmRequest struct {
	Source Source
	// IdempotencyKey identifies one logical prewarm request. Implementations should
	// return the existing operation when the same key is submitted again.
	IdempotencyKey string
	Objects        []ObjectRef
}

// PrewarmOperation identifies an async prewarm operation.
type PrewarmOperation struct {
	ID string
}

// PrewarmPhase is the lifecycle phase of a prewarm operation.
type PrewarmPhase string

const (
	PrewarmPhasePending   PrewarmPhase = "Pending"
	PrewarmPhaseRunning   PrewarmPhase = "Running"
	PrewarmPhaseSucceeded PrewarmPhase = "Succeeded"
	PrewarmPhaseFailed    PrewarmPhase = "Failed"
	PrewarmPhaseCanceled  PrewarmPhase = "Canceled"
)

// PrewarmStatus reports progress for an async prewarm operation.
type PrewarmStatus struct {
	Operation PrewarmOperation
	Phase     PrewarmPhase
	Progress  PrewarmProgress
	Reason    string
	Message   string
}

// PrewarmProgress reports byte and chunk progress when known.
type PrewarmProgress struct {
	TotalBytes   *int64
	CachedBytes  *int64
	TotalChunks  *int32
	CachedChunks *int32
}
