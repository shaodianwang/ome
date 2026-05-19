package runtimeselector

import (
	"context"
	"sort"

	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/runtimeinheritance"
)

// DefaultRuntimeFetcher implements RuntimeFetcher using the controller-runtime client.
// It leverages the client's built-in caching for efficient runtime retrieval.
type DefaultRuntimeFetcher struct {
	client client.Client
}

// NewDefaultRuntimeFetcher creates a new DefaultRuntimeFetcher.
func NewDefaultRuntimeFetcher(client client.Client) RuntimeFetcher {
	return &DefaultRuntimeFetcher{
		client: client,
	}
}

// FetchRuntimes returns both namespace and cluster scoped runtimes.
// The results are sorted by creation timestamp (newest first) and then by name.
func (f *DefaultRuntimeFetcher) FetchRuntimes(ctx context.Context, namespace string) (*RuntimeCollection, error) {
	logger := log.FromContext(ctx)

	// Fetch namespace-scoped runtimes
	logger.V(1).Info("Fetching namespace-scoped runtimes", "namespace", namespace)
	runtimes := &v1beta1.ServingRuntimeList{}
	if err := f.client.List(ctx, runtimes, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	// Fetch cluster-scoped runtimes
	logger.V(1).Info("Fetching cluster-scoped runtimes")
	clusterRuntimes := &v1beta1.ClusterServingRuntimeList{}
	if err := f.client.List(ctx, clusterRuntimes); err != nil {
		return nil, err
	}

	// Sort namespace-scoped runtimes
	sortServingRuntimeList(runtimes)

	// Sort cluster-scoped runtimes
	sortClusterServingRuntimeList(clusterRuntimes)

	logger.V(1).Info("Fetched runtimes",
		"namespaceRuntimes", len(runtimes.Items),
		"clusterRuntimes", len(clusterRuntimes.Items))

	return &RuntimeCollection{
		NamespaceRuntimes: runtimes.Items,
		ClusterRuntimes:   clusterRuntimes.Items,
	}, nil
}

// GetRuntime fetches a runtime by name and returns its OEP-0012
// merged spec. Namespace-scoped takes precedence over cluster-scoped
// on name collision.
func (f *DefaultRuntimeFetcher) GetRuntime(ctx context.Context, name string, namespace string) (*v1beta1.ServingRuntimeSpec, bool, error) {
	logger := log.FromContext(ctx)

	if _, err := f.getNamespacedHead(ctx, namespace, name); err == nil {
		logger.V(1).Info("Found namespace-scoped runtime", "name", name, "namespace", namespace)
		spec, _, resolveErr := runtimeinheritance.ResolveNamespacedRuntime(ctx, f.client, namespace, name)
		return spec, false, resolveErr
	} else if !errors.IsNotFound(err) {
		return nil, false, err
	}

	if _, err := f.getClusterHead(ctx, name); err == nil {
		logger.V(1).Info("Found cluster-scoped runtime", "name", name)
		spec, _, resolveErr := runtimeinheritance.ResolveClusterRuntime(ctx, f.client, name)
		return spec, true, resolveErr
	} else if errors.IsNotFound(err) {
		return nil, false, &RuntimeNotFoundError{
			RuntimeName: name,
			Namespace:   namespace,
		}
	} else {
		return nil, false, err
	}
}

func (f *DefaultRuntimeFetcher) getNamespacedHead(ctx context.Context, namespace, name string) (*v1beta1.ServingRuntime, error) {
	sr := &v1beta1.ServingRuntime{}
	if err := f.client.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, sr); err != nil {
		return nil, err
	}
	return sr, nil
}

func (f *DefaultRuntimeFetcher) getClusterHead(ctx context.Context, name string) (*v1beta1.ClusterServingRuntime, error) {
	csr := &v1beta1.ClusterServingRuntime{}
	if err := f.client.Get(ctx, client.ObjectKey{Name: name}, csr); err != nil {
		return nil, err
	}
	return csr, nil
}

// sortServingRuntimeList sorts a list of ServingRuntimes by creation timestamp (desc) and name (asc).
func sortServingRuntimeList(runtimes *v1beta1.ServingRuntimeList) {
	sort.Slice(runtimes.Items, func(i, j int) bool {
		// First sort by creation timestamp (newer first)
		if !runtimes.Items[i].CreationTimestamp.Equal(&runtimes.Items[j].CreationTimestamp) {
			return runtimes.Items[i].CreationTimestamp.After(runtimes.Items[j].CreationTimestamp.Time)
		}
		// Then by name (alphabetically)
		return runtimes.Items[i].Name < runtimes.Items[j].Name
	})
}

// sortClusterServingRuntimeList sorts a list of ClusterServingRuntimes by creation timestamp (desc) and name (asc).
func sortClusterServingRuntimeList(runtimes *v1beta1.ClusterServingRuntimeList) {
	sort.Slice(runtimes.Items, func(i, j int) bool {
		// First sort by creation timestamp (newer first)
		if !runtimes.Items[i].CreationTimestamp.Equal(&runtimes.Items[j].CreationTimestamp) {
			return runtimes.Items[i].CreationTimestamp.After(runtimes.Items[j].CreationTimestamp.Time)
		}
		// Then by name (alphabetically)
		return runtimes.Items[i].Name < runtimes.Items[j].Name
	})
}
