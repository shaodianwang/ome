// Package countingclusters wraps the placement reconciler's view of the
// cross-cluster transport (placement.ClusterClients) so every remote
// round-trip the placer makes — fan-out Creates/Updates, findWinner Gets,
// loser Deletes, status reads — is counted per verb and resource kind.
//
// The placer reaches the member apiservers ONLY through
// ClusterClients.ClientFor(name); wrapping that one seam captures the entire
// remote op profile without touching the reconciler. The per-cluster clients
// are SelectivelyCachingClients (WithWatch + cache event handlers), so the
// wrapper embeds the real client for the non-counted surface (Watch,
// AddCacheEventHandler, Scheme, RESTMapper, ...) and overrides only the verbs
// worth counting, delegating each to the embedded client after bumping the
// shared counter. Counts come from tests/perf/internal/opcount so the count
// schema matches the rest of the perf harnesses.
package countingclusters

import (
	"context"

	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workloadcluster"
	"github.com/ome-projects/ome/tests/perf/internal/opcount"
)

// Counting wraps a placement.ClusterClients, counting every op on the
// per-cluster clients it hands out into a shared opcount.Counts.
type Counting struct {
	inner  placement.ClusterClients
	counts *opcount.Counts
}

var _ placement.ClusterClients = (*Counting)(nil)

// New wraps inner so the per-cluster clients it returns accumulate their ops
// into counts. The same counts can be Snapshot/Flat'd after a pass.
func New(inner placement.ClusterClients, counts *opcount.Counts) *Counting {
	return &Counting{inner: inner, counts: counts}
}

// Connected passes through; it makes no remote call.
func (c *Counting) Connected() []string { return c.inner.Connected() }

// ClientFor returns a counting wrapper around the live per-cluster client, or
// (nil, false) when the cluster is not connected.
func (c *Counting) ClientFor(name string) (workloadcluster.SelectivelyCachingClient, bool) {
	cl, ok := c.inner.ClientFor(name)
	if !ok {
		return nil, false
	}
	return &countingRemote{SelectivelyCachingClient: cl, counts: c.counts}, true
}

// countingRemote is a SelectivelyCachingClient that counts the verbs the
// placer uses. Everything not overridden (Watch, AddCacheEventHandler,
// Scheme, RESTMapper, GroupVersionKindFor, IsObjectNamespaced) is served by
// the embedded real client.
type countingRemote struct {
	workloadcluster.SelectivelyCachingClient
	counts *opcount.Counts
}

func (c *countingRemote) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.counts.Add("get", opcount.KindOf(obj))
	return c.SelectivelyCachingClient.Get(ctx, key, obj, opts...)
}

func (c *countingRemote) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.counts.Add("list", opcount.ListKindOf(list))
	return c.SelectivelyCachingClient.List(ctx, list, opts...)
}

func (c *countingRemote) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.counts.Add("create", opcount.KindOf(obj))
	return c.SelectivelyCachingClient.Create(ctx, obj, opts...)
}

func (c *countingRemote) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.counts.Add("update", opcount.KindOf(obj))
	return c.SelectivelyCachingClient.Update(ctx, obj, opts...)
}

func (c *countingRemote) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.counts.Add("delete", opcount.KindOf(obj))
	return c.SelectivelyCachingClient.Delete(ctx, obj, opts...)
}

func (c *countingRemote) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.counts.Add("patch", opcount.KindOf(obj))
	return c.SelectivelyCachingClient.Patch(ctx, obj, patch, opts...)
}

func (c *countingRemote) Status() client.SubResourceWriter {
	return &countingStatus{SubResourceWriter: c.SelectivelyCachingClient.Status(), counts: c.counts}
}

type countingStatus struct {
	client.SubResourceWriter
	counts *opcount.Counts
}

func (s *countingStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	s.counts.Add("status-update", opcount.KindOf(obj))
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

func (s *countingStatus) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	s.counts.Add("status-patch", opcount.KindOf(obj))
	return s.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

// AddCacheEventHandler is inherited from the embedded SelectivelyCachingClient
// but restated here to make the funnel dependency explicit: when the harness
// enables the status funnel + derived-ISVC cache, the funnel registers a cache
// event handler on the per-cluster client, and that registration must reach
// the REAL cache, not a no-op wrapper. The embedded delegation does exactly
// that.
func (c *countingRemote) AddCacheEventHandler(ctx context.Context, obj client.Object, h toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	return c.SelectivelyCachingClient.AddCacheEventHandler(ctx, obj, h)
}
