// Package sharded is the BaseModel controller's sharded backend: the
// modelcache abstraction (Client interface, Provider, Registry) plus
// the cluster_cache HTTP client that implements it. Both live in one
// package because the abstraction has a single production
// implementation; the interface exists so tests can substitute a fake.
package sharded

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// ErrSourceUnparseable means the model source layout is structurally
// incompatible with the provider's parsing requirements and retrying
// will not help. The controller surfaces this as a terminal
// MetadataUnparseable condition + state=Failed; the operator must
// change the spec or the source bytes before the model can become
// Ready.
//
// Providers signal this by wrapping their structural errors:
//
//	fmt.Errorf("source missing config.json: %w", sharded.ErrSourceUnparseable)
//
// Only use for errors guaranteed not to be transient — the bytes are
// visible and well-formed, but the layout doesn't match what the
// parser needs. Network errors, auth failures, and truncated reads
// must NOT be wrapped with this sentinel.
var ErrSourceUnparseable = errors.New("model cache source has unparsable layout")

const (
	// ProviderClusterCache is the provider label for the cluster_cache
	// implementation. Also the value operators set in the ome-config
	// configmap's modelCache.provider field.
	ProviderClusterCache = "cluster_cache"
)

// Client is the high-level OME control-plane contract for a model
// cache backend. The lazy-fetch flow uses only CheckSource
// (reachability probe) and InspectSource (metadata + object listing
// for runtime selection); bytes are pulled on demand by the provider
// at first inference, so there is no StartPrewarm / GetPrewarmStatus
// equivalent.
type Client interface {
	CheckSource(ctx context.Context, source Source) (*SourceStatus, error)
	InspectSource(ctx context.Context, source Source) (*SourceInspection, error)
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
	Config ProviderConfig
}

// NewProvider returns a configured model cache provider.
func NewProvider(name string, client Client, configs ...ProviderConfig) Provider {
	config := ProviderConfig{Provider: name}
	if len(configs) > 0 {
		config = configs[0]
		if strings.TrimSpace(config.Provider) == "" {
			config.Provider = name
		}
	}
	return Provider{
		Name:   name,
		Client: client,
		Config: config,
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

	return NewProvider(provider, client, config), nil
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

// AuthRef identifies authentication material without prescribing how
// a backend resolves it.
type AuthRef struct {
	Namespace string
	Name      string
	Key       string
}

// SourceStatus reports source reachability without exposing backend
// transport details.
type SourceStatus struct {
	Reachable bool
	Reason    string
	Message   string
}

// SourceInspection is a provider-neutral description of a model source
// plus the small metadata payloads OME needs for model parsing.
type SourceInspection struct {
	RootURI  string
	Objects  []Object
	Metadata ModelMetadataInputs
}

// ModelMetadataInputs contains small model metadata blobs returned by
// source inspection. Providers must not include tensor bodies here.
type ModelMetadataInputs struct {
	ConfigJSON           []byte
	ConfigPath           string
	ModelIndexJSON       []byte
	ModelIndexPath       string
	SafetensorsIndexJSON []byte
	SafetensorsIndexPath string
	TokenizerConfigJSON  []byte
	SafetensorHeaders    []SafetensorHeader
	// HFQuantConfigJSON carries the contents of hf_quant_config.json
	// when the source has it. NVIDIA ModelOpt and TensorRT-LLM emit
	// this sibling file for models whose quantization scheme isn't
	// declared in config.json's quantization_config block (e.g.,
	// NVFP4 PTQ models). Fetched best-effort — absence is normal and
	// not an error.
	HFQuantConfigJSON []byte
	HFQuantConfigPath string
}

// SafetensorHeader describes one safetensors shard and its JSON header.
type SafetensorHeader struct {
	Path         string
	RelativePath string
	SizeBytes    int64
	Version      string
	HeaderJSON   []byte
}

// Object describes one source object using backend-neutral metadata.
type Object struct {
	Source       Source
	Path         string
	RelativePath string
	SizeBytes    int64
	Version      string
}
