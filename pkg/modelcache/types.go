package modelcache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// ErrSourceUnparseable means the model source layout is structurally
// incompatible with the provider's parsing requirements and retrying will
// not help. The controller surfaces this as a terminal MetadataUnparseable
// condition + state=Failed; the operator must change the spec or the
// source bytes before the model can become Ready.
//
// Providers signal this by wrapping their structural errors:
//
//	fmt.Errorf("source missing config.json: %w", modelcache.ErrSourceUnparseable)
//
// Only use this for errors that are guaranteed not to be transient — i.e.
// the bytes are visible and well-formed, but the layout doesn't match what
// the parser needs. Network errors, auth failures, and truncated reads
// must NOT be wrapped with this sentinel.
var ErrSourceUnparseable = errors.New("model cache source has unparseable layout")

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
	RootURI  string
	Objects  []Object
	Metadata ModelMetadataInputs
}

// ModelMetadataInputs contains small model metadata blobs returned by source
// inspection. Providers must not include tensor bodies here.
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
	// NVFP4 PTQ models). Providers should fetch it best-effort —
	// absence is normal and not an error.
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

// ObjectRef identifies one object within a model source.
type ObjectRef struct {
	Source Source
	Path   string
}

// Object describes one source object using backend-neutral metadata.
type Object struct {
	Source       Source
	Path         string
	RelativePath string
	SizeBytes    int64
	Version      string
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
	// FailedObjects is the structured per-object failure list observed in
	// the most recent prewarm round. Provider-populated; the controller
	// merges across rounds (preserving FirstSeenAt, accumulating Attempts).
	// Empty when the phase is Succeeded.
	FailedObjects []ChunkFailure
}

// PrewarmProgress reports byte and chunk progress when known.
type PrewarmProgress struct {
	TotalBytes   *int64
	CachedBytes  *int64
	TotalChunks  *int32
	CachedChunks *int32
}

// ChunkFailureClass mirrors v1beta1.ErrorClass but lives in the
// provider-neutral types so providers don't import the apis package. The
// controller maps these 1:1 into the CRD status surface.
type ChunkFailureClass string

const (
	ChunkFailureClassUnknown             ChunkFailureClass = "Unknown"
	ChunkFailureClassSourceObjectMissing ChunkFailureClass = "SourceObjectMissing"
	ChunkFailureClassAuthDenied          ChunkFailureClass = "AuthDenied"
	ChunkFailureClassBackendUnreachable  ChunkFailureClass = "BackendUnreachable"
	ChunkFailureClassChecksumMismatch    ChunkFailureClass = "ChecksumMismatch"
	ChunkFailureClassThrottled           ChunkFailureClass = "Throttled"
)

// ChunkFailure is a single object's failure reported by a provider during
// prewarm. Keyed by Path. If a provider sees multiple chunks of one file
// fail, they MUST be coalesced into a single entry with Attempts summed.
type ChunkFailure struct {
	// Path is the object path relative to the model source root (e.g.
	// "config.json", "model-00001-of-00002.safetensors"). Required.
	Path string
	// SizeBytes is the source object's size if the provider knows it.
	// Optional — zero/unknown is fine.
	SizeBytes int64
	// Class is the coarse remediation taxonomy. Providers SHOULD classify;
	// unclassified errors get ChunkFailureClassUnknown.
	Class ChunkFailureClass
	// Code is a short provider-specific identifier (e.g. "NoSuchKey",
	// "AccessDenied", "ConnectionRefused"). Optional but encouraged.
	Code string
	// Message is a one-line human-readable summary. Must NOT contain XML
	// bodies, request IDs, or stack traces — keep those out of CR status.
	Message string
	// Attempts is the number of chunk-fetch failures the provider observed
	// for this object in the current round. Multiple chunks of the same
	// file each count as one attempt.
	Attempts int32
}
