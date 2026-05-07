package modelcache

import "context"

const (
	// BackendClusterCache is the status backend label for cluster_cache.
	BackendClusterCache = "cluster_cache"
)

// Client is the high-level OME control-plane contract for a model cache.
type Client interface {
	CheckSource(ctx context.Context, source Source) (*SourceStatus, error)
	ListSource(ctx context.Context, source Source) ([]Object, error)
	GetObject(ctx context.Context, ref ObjectRef) (*Object, error)
	ReadMetadataRange(ctx context.Context, req MetadataRangeRequest) ([]byte, error)
	StartPrewarm(ctx context.Context, req PrewarmRequest) (*PrewarmOperation, error)
	GetPrewarmStatus(ctx context.Context, operation PrewarmOperation) (*PrewarmStatus, error)
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

// MetadataRangeRequest asks for a small metadata byte range from an object.
type MetadataRangeRequest struct {
	Object Object
	Offset int64
	Length int64
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
