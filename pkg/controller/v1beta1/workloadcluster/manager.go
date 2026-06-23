package workloadcluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Manager holds one live remote client per registered WorkloadCluster, keyed by
// cluster name. It is the OEP-0018 cross-cluster transport: later phases create
// and watch remote objects via ClientFor(name).
//
// Safe for concurrent use across distinct cluster names; Connect/Disconnect for
// the SAME name must be serialized by the caller (the WorkloadCluster reconciler
// is single-worker per key, so this holds in practice).
type Manager struct {
	scheme *runtime.Scheme

	mu    sync.RWMutex
	conns map[string]*remoteConn

	// newClient builds a caching-capable client (+ a cancel for its background
	// watch context) from raw kubeconfig bytes. Injected for tests; defaults to
	// buildRemoteClient.
	newClient func(ctx context.Context, raw []byte, scheme *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error)
}

type remoteConn struct {
	client     SelectivelyCachingClient
	cancel     context.CancelFunc
	configHash string
}

// NewManager returns an empty Manager whose default builder constructs a
// controller-runtime client (never-caching for now; Phase 3 registers cached
// kinds) from the validated kubeconfig.
func NewManager(scheme *runtime.Scheme) *Manager {
	return &Manager{
		scheme:    scheme,
		conns:     map[string]*remoteConn{},
		newClient: buildRemoteClient,
	}
}

// ClientFor returns the live client for a cluster, or (nil, false) if none is
// connected.
func (m *Manager) ClientFor(name string) (SelectivelyCachingClient, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.conns[name]
	if !ok {
		return nil, false
	}
	return c.client, true
}

// Connect ensures a live client for the named cluster built from raw. It is a
// no-op when the kubeconfig is unchanged; on change it builds a fresh client
// and disposes the old one (cancels its watch context).
func (m *Manager) Connect(ctx context.Context, name string, raw []byte) error {
	hash := hashBytes(raw)

	m.mu.RLock()
	if cur, ok := m.conns[name]; ok && cur.configHash == hash {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	cl, cancel, err := m.newClient(ctx, raw, m.scheme)
	if err != nil {
		return fmt.Errorf("workloadcluster %q: build remote client: %w", name, err)
	}

	m.mu.Lock()
	if old, ok := m.conns[name]; ok && old.cancel != nil {
		old.cancel()
	}
	m.conns[name] = &remoteConn{client: cl, cancel: cancel, configHash: hash}
	m.mu.Unlock()
	return nil
}

// Disconnect disposes the client for a cluster (cancels its watch context) and
// drops it from the registry. No-op if absent.
func (m *Manager) Disconnect(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.conns[name]; ok {
		if c.cancel != nil {
			c.cancel()
		}
		delete(m.conns, name)
	}
}

// Connected returns the names of all currently-connected clusters.
func (m *Manager) Connected() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.conns))
	for name := range m.conns {
		names = append(names, name)
	}
	return names
}

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// buildRemoteClient is the default Manager.newClient: validate+parse the
// kubeconfig (Phase 1's RESTConfigFromKubeConfig), build a watch-capable
// controller-runtime client, and wrap it never-caching (Phase 3 will switch to
// NewSelectivelyCachingClient with the kinds it watches). The returned cancel
// tears down the (currently unused) watch context.
func buildRemoteClient(ctx context.Context, raw []byte, scheme *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
	restConfig, err := RESTConfigFromKubeConfig(raw)
	if err != nil {
		return nil, nil, err
	}
	wc, err := client.NewWithWatch(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, nil, fmt.Errorf("new watch client: %w", err)
	}
	// The never-caching client has no background goroutines, so there is
	// nothing to tear down: return a no-op cancel. Phase 5 will switch to a
	// selective-caching client — when it does, this MUST derive a context via
	// context.WithCancel(ctx), pass THAT context to NewSelectivelyCachingClient
	// (which starts the cache informers on it), and return its cancel so
	// Disconnect tears the cache down. (Returning a cancel from an unused
	// derived context, as before, was a silent no-op that invited a leak.)
	return NewNeverCachingClient(wc), func() {}, nil
}
