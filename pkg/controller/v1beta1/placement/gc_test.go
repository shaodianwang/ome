package placement

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workloadcluster"
)

func derivedWithOrigin(name, originUID string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: name,
		Labels:      map[string]string{PlacementOriginLabel: originUID},
		Annotations: map[string]string{PlacementOriginUIDAnnotation: originUID},
	}}
}

func TestOrphanedDeriveds(t *testing.T) {
	live := map[string]bool{"uid-live": true}
	withOrigin := func(name, originUID string) metav1.PartialObjectMetadata {
		return metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: name,
			Annotations: map[string]string{PlacementOriginUIDAnnotation: originUID},
		}}
	}
	derived := []metav1.PartialObjectMetadata{
		withOrigin("a", "uid-live"), // source exists -> keep
		withOrigin("b", "uid-gone"), // source gone -> orphan
	}
	orphans := OrphanedDeriveds(live, derived)
	require.Len(t, orphans, 1)
	assert.Equal(t, "b", orphans[0].Name)
}

func TestGCSweep_DeletesOrphansOnly(t *testing.T) {
	s := testScheme(t)
	cp := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "live", UID: "uid-live"},
	}).Build()
	worker := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(
		derivedWithOrigin("live", "uid-live"),
		derivedWithOrigin("orphan", "uid-gone"),
	).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"w": workloadcluster.NewNeverCachingClient(worker),
	}}
	gc := &GCReconciler{APIReader: cp, Log: log.Log, Clusters: clusters}

	require.NoError(t, gc.sweep(context.Background()))

	err := worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "orphan"}, &v1beta1.InferenceService{})
	assert.True(t, apierrors.IsNotFound(err), "orphan deleted")
	require.NoError(t, worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "live"}, &v1beta1.InferenceService{}), "live kept")
}
