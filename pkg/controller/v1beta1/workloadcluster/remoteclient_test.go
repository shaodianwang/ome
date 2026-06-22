package workloadcluster

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNeverCachingClient_NoCacheHandler(t *testing.T) {
	fc := fake.NewClientBuilder().Build()
	c := NewNeverCachingClient(fc)
	if _, err := c.AddCacheEventHandler(context.Background(), &corev1.Pod{}, nil); err == nil {
		t.Errorf("AddCacheEventHandler on NeverCachingClient: got nil error, want error")
	}
}
