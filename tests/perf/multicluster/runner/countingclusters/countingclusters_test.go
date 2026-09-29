package countingclusters

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workloadcluster"
	"github.com/ome-projects/ome/tests/perf/internal/opcount"
)

type registry struct {
	remote workloadcluster.SelectivelyCachingClient
	uid    types.UID
	calls  []types.UID
}

func (r *registry) Connected() []string { return []string{"member-a"} }
func (r *registry) ClientFor(string) (workloadcluster.SelectivelyCachingClient, bool) {
	return r.remote, r.remote != nil
}
func (r *registry) ClientForUID(_ string, uid types.UID) (workloadcluster.SelectivelyCachingClient, bool) {
	r.calls = append(r.calls, uid)
	if uid != r.uid {
		return nil, false
	}
	return r.ClientFor("member-a")
}

type cachedTransport struct {
	workloadcluster.SelectivelyCachingClient
	live    client.WithWatch
	watches int
}

func (c *cachedTransport) DirectClient() client.WithWatch { return c.live }
func (c *cachedTransport) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("cached read")
}
func (c *cachedTransport) AddCacheEventHandler(context.Context, client.Object, toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	c.watches++
	return nil, nil
}

func TestIdentifiedCountingTransport(t *testing.T) {
	for _, tt := range []struct {
		name            string
		uid             types.UID
		connected, want bool
	}{
		{name: "accepted identity", uid: "uid-a", connected: true, want: true},
		{name: "replaced registration", uid: "uid-b", connected: true},
		{name: "missing identity", connected: true},
		{name: "disconnected", uid: "uid-a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			live := fake.NewClientBuilder().WithObjects(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "config-a"}}).Build()
			remote := &cachedTransport{SelectivelyCachingClient: workloadcluster.NewNeverCachingClient(live), live: live}
			inner := &registry{uid: "uid-a"}
			if tt.connected {
				inner.remote = remote
			}
			counts := opcount.NewCounts()
			c := New(inner, counts)
			got, ok := c.ClientForUID("member-a", tt.uid)
			if diff := cmp.Diff(tt.want, ok); diff != "" {
				t.Fatalf("available (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff([]types.UID{tt.uid}, inner.calls); diff != "" {
				t.Fatalf("identity checks (-want +got):\n%s", diff)
			}
			if !ok {
				if got != nil {
					t.Fatal("unverified transport returned")
				}
				return
			}
			again, _ := c.ClientFor("member-a")
			identified, _ := c.ClientForUID("member-a", tt.uid)
			if got != again || got != identified {
				t.Fatal("same connection received different wrapper identities")
			}
			direct, ok := workloadcluster.DirectClient(got)
			if !ok {
				t.Fatal("live reader unavailable")
			}
			if err := direct.Get(t.Context(), client.ObjectKey{Name: "config-a"}, &corev1.ConfigMap{}); err != nil {
				t.Fatal(err)
			}
			if err := direct.List(t.Context(), &corev1.ConfigMapList{}); err != nil {
				t.Fatal(err)
			}
			if err := got.Get(t.Context(), client.ObjectKey{Name: "config-a"}, &corev1.ConfigMap{}); err == nil || err.Error() != "cached read" {
				t.Fatalf("cached path: %v", err)
			}
			if _, err := got.AddCacheEventHandler(t.Context(), &corev1.ConfigMap{}, nil); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(1, remote.watches); diff != "" {
				t.Error(diff)
			}
			if diff := cmp.Diff(map[string]int64{"get/ConfigMap": 2, "list/ConfigMap": 1}, counts.Flat()); diff != "" {
				t.Errorf("counts (-want +got):\n%s", diff)
			}
			inner.remote = workloadcluster.NewNeverCachingClient(live)
			rotated, _ := c.ClientForUID("member-a", tt.uid)
			if rotated == got {
				t.Fatal("changed connection retained wrapper identity")
			}
		})
	}
}

func TestCountingRejectsUnverifiableCapabilities(t *testing.T) {
	for _, tt := range []struct {
		name     string
		identity bool
	}{
		{name: "unidentified registry"},
		{name: "opaque transport", identity: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opaque := struct {
				workloadcluster.SelectivelyCachingClient
			}{workloadcluster.NewNeverCachingClient(fake.NewClientBuilder().Build())}
			inner := &registry{remote: opaque, uid: "uid-a"}
			c := New(inner, opcount.NewCounts())
			if !tt.identity {
				c.inner = struct{ interfaceRegistry }{inner}
			}
			got, ok := c.ClientForUID("member-a", "uid-a")
			if diff := cmp.Diff(tt.identity, ok); diff != "" {
				t.Fatal(diff)
			}
			if ok {
				direct, available := workloadcluster.DirectClient(got)
				if direct != nil || available {
					t.Fatal("opaque client supplied direct authority")
				}
			}
		})
	}
}

type interfaceRegistry interface {
	Connected() []string
	ClientFor(string) (workloadcluster.SelectivelyCachingClient, bool)
}
