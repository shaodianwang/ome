// Package opcount provides counting client.Client / client.Reader wrappers for
// the perf harnesses. They record per-verb, per-resource-kind apiserver op
// counts so a harness can report reconcile op-amplification — the OMENative
// status-write 409 churn and pod-LIST fan-out, and the multi-cluster fan-out /
// loser-sweep remote round-trips, that the perf work targets.
//
// Usage: wrap the client a reconciler is constructed with (and, separately, its
// APIReader for live reads), sharing one *Counts; read the totals via Snapshot
// after a run. The wrappers are safe for concurrent use.
package opcount

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Counts accumulates op counts keyed by verb then resource kind (e.g.
// counts["list"]["Pod"]). Verbs are lowercase; cached reads use get/list while
// live (APIReader) reads use live-get/live-list so the two can be told apart.
type Counts struct {
	mu sync.Mutex
	m  map[string]map[string]int64
}

// NewCounts returns an empty, ready-to-use counter.
func NewCounts() *Counts { return &Counts{m: map[string]map[string]int64{}} }

func (c *Counts) add(verb, kind string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[verb] == nil {
		c.m[verb] = map[string]int64{}
	}
	c.m[verb][kind]++
}

// Add records one op under (verb, kind). Exported so a harness that wraps a
// non-client.Client transport (e.g. the multi-cluster placer's per-cluster
// SelectivelyCachingClient) can count into the same schema without
// re-implementing the wrappers.
func (c *Counts) Add(verb, kind string) { c.add(verb, kind) }

// Snapshot returns a deep copy of the current counts.
func (c *Counts) Snapshot() map[string]map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]map[string]int64, len(c.m))
	for v, km := range c.m {
		out[v] = make(map[string]int64, len(km))
		for k, n := range km {
			out[v][k] = n
		}
	}
	return out
}

// Total returns the sum of every counted op.
func (c *Counts) Total() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var t int64
	for _, km := range c.m {
		for _, n := range km {
			t += n
		}
	}
	return t
}

// TotalByVerb returns the sum across resource kinds for one verb.
func (c *Counts) TotalByVerb(verb string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var t int64
	for _, n := range c.m[verb] {
		t += n
	}
	return t
}

// Flat renders the counts as sorted "verb/Kind=n" lines, for logging / a YAML
// scalar map. Stable order so a diff/baseline is deterministic.
func (c *Counts) Flat() map[string]int64 {
	out := map[string]int64{}
	for verb, km := range c.Snapshot() {
		for kind, n := range km {
			out[verb+"/"+kind] = n
		}
	}
	return out
}

// SortedKeys returns the Flat() keys in stable order (helper for printing).
func (c *Counts) SortedKeys() []string {
	f := c.Flat()
	keys := make([]string, 0, len(f))
	for k := range f {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// kindOf derives a stable resource kind label from an object's Go type, e.g.
// *corev1.Pod -> "Pod", *corev1.PodList -> "Pod". No scheme needed.
func kindOf(obj client.Object) string { return trimList(typeName(obj)) }

func listKindOf(list client.ObjectList) string { return trimList(typeName(list)) }

// KindOf / ListKindOf are the exported forms of the kind-label helpers, for
// harnesses counting into a *Counts via Add without using the Client wrapper.
func KindOf(obj client.Object) string          { return kindOf(obj) }
func ListKindOf(list client.ObjectList) string { return listKindOf(list) }

func typeName(obj any) string {
	t := reflect.TypeOf(obj)
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil {
		return "Unknown"
	}
	return t.Name()
}

func trimList(name string) string { return strings.TrimSuffix(name, "List") }

// Client wraps a client.Client and counts every op through it. Construct
// reconcilers with this so their cached reads + all writes are counted.
type Client struct {
	client.Client
	counts *Counts
}

// NewClient wraps c so its ops accumulate into counts.
func NewClient(c client.Client, counts *Counts) *Client {
	return &Client{Client: c, counts: counts}
}

var (
	_ client.Client = (*Client)(nil)
	_ client.Reader = (*Reader)(nil)
)

func (c *Client) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.counts.add("get", kindOf(obj))
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *Client) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.counts.add("list", listKindOf(list))
	return c.Client.List(ctx, list, opts...)
}

func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.counts.add("create", kindOf(obj))
	return c.Client.Create(ctx, obj, opts...)
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.counts.add("update", kindOf(obj))
	return c.Client.Update(ctx, obj, opts...)
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.counts.add("delete", kindOf(obj))
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.counts.add("patch", kindOf(obj))
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	c.counts.add("deleteallof", kindOf(obj))
	return c.Client.DeleteAllOf(ctx, obj, opts...)
}

// Status returns a counting status writer (status-update / status-patch).
func (c *Client) Status() client.StatusWriter {
	return &statusWriter{SubResourceWriter: c.Client.Status(), counts: c.counts}
}

type statusWriter struct {
	client.SubResourceWriter
	counts *Counts
}

func (s *statusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	s.counts.add("status-update", kindOf(obj))
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

func (s *statusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	s.counts.add("status-patch", kindOf(obj))
	return s.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

// Reader wraps a client.Reader (e.g. mgr.GetAPIReader()) and counts its reads as
// live-get / live-list so cache-bypassing reads are distinguishable from cached.
type Reader struct {
	client.Reader
	counts *Counts
}

// NewReader wraps r so its live reads accumulate into counts.
func NewReader(r client.Reader, counts *Counts) *Reader {
	return &Reader{Reader: r, counts: counts}
}

func (r *Reader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.counts.add("live-get", kindOf(obj))
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (r *Reader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	r.counts.add("live-list", listKindOf(list))
	return r.Reader.List(ctx, list, opts...)
}
