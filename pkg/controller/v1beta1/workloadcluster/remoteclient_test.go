package workloadcluster

import (
	"context"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type recordingClient struct {
	client.WithWatch
	getCalls  int
	listCalls int
}

func (c *recordingClient) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	c.getCalls++
	return nil
}

func (c *recordingClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	c.listCalls++
	return nil
}

type recordingReader struct {
	getCalls  int
	listCalls int
}

func (r *recordingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	r.getCalls++
	return nil
}

func (r *recordingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	r.listCalls++
	return nil
}

func TestNeverCachingClient_NoCacheHandler(t *testing.T) {
	fc := fake.NewClientBuilder().Build()
	c := NewNeverCachingClient(fc)
	if _, err := c.AddCacheEventHandler(context.Background(), &corev1.Pod{}, nil); err == nil {
		t.Errorf("AddCacheEventHandler on NeverCachingClient: got nil error, want error")
	}
}

// NewSelectivelyCachingClient must refuse to cache Pods before any cluster
// contact: a cluster-wide Pod informer is exactly the OOM the remote cache
// guards against. The guard returns before cache.New, so a nil restConfig
// proves it short-circuits.
func TestNewSelectivelyCachingClient_RejectsPodKind(t *testing.T) {
	podGK := schema.GroupKind{Group: "", Kind: "Pod"}
	_, err := NewSelectivelyCachingClient(
		context.Background(),
		(*rest.Config)(nil),
		fake.NewClientBuilder().Build(),
		nil,
		sets.New(podGK),
		nil,
		nil,
	)
	if err == nil {
		t.Fatalf("NewSelectivelyCachingClient with Pod in cachedKinds: got nil error, want rejection")
	}
}

func TestSelectivelyCachingClient_MetadataOnlyReadsBypassCache(t *testing.T) {
	direct := &recordingClient{WithWatch: fake.NewClientBuilder().Build()}
	cached := &recordingReader{}
	metadataGK := schema.GroupKind{Group: "ome.io", Kind: "InferenceService"}
	var synced atomic.Bool
	synced.Store(true)
	c := &selectivelyCachingClient{
		WithWatch:    direct,
		cachedReader: cached,
		cachedKinds:  sets.New(metadataGK),
		synced:       &synced,
	}

	metadata := &metav1.PartialObjectMetadata{}
	metadata.SetGroupVersionKind(metadataGK.WithVersion("v1beta1"))
	if err := c.Get(context.Background(), client.ObjectKey{Name: "example"}, metadata); err != nil {
		t.Fatalf("Get PartialObjectMetadata: %v", err)
	}

	metadataList := &metav1.PartialObjectMetadataList{}
	metadataList.SetGroupVersionKind(metadataGK.WithVersion("v1beta1").GroupVersion().WithKind("InferenceServiceList"))
	if err := c.List(context.Background(), metadataList); err != nil {
		t.Fatalf("List PartialObjectMetadataList: %v", err)
	}

	if direct.getCalls != 1 || direct.listCalls != 1 {
		t.Fatalf("direct metadata reads: got Get=%d List=%d, want Get=1 List=1", direct.getCalls, direct.listCalls)
	}
	if cached.getCalls != 0 || cached.listCalls != 0 {
		t.Fatalf("cached metadata reads: got Get=%d List=%d, want Get=0 List=0", cached.getCalls, cached.listCalls)
	}
}
