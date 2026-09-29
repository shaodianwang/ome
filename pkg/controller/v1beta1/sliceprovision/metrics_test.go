package sliceprovision

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// TestMetricNames pins the counters' names, which dashboards and alerts key on.
func TestMetricNames(t *testing.T) {
	slicesCreated.WithLabelValues("type-a", "2x2x1")
	slicesReleased.WithLabelValues("type-a", "2x2x1")
	for _, name := range []string{"ome_tpu_slice_created_total", "ome_tpu_slice_released_total"} {
		if got, err := testutil.GatherAndCount(ctrlmetrics.Registry, name); err != nil || got == 0 {
			t.Errorf("%s: %d series (err %v), want it registered", name, got, err)
		}
	}
}

func TestObserveDeleted(t *testing.T) {
	p := newProvisioner(t, newFakeClient(), testOwner)
	d := demand(t, "2x2x1")
	notSlice := &unstructured.Unstructured{}
	notSlice.SetGroupVersionKind(schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"})
	notSlice.SetLabels(map[string]string{LabelOwnerUID: "uid-a"})
	unparseable := stored(t, p, d, Slot{}, func(u *unstructured.Unstructured) {
		u.Object["spec"].(map[string]interface{})["topology"] = int64(2)
	})
	for _, tc := range []struct {
		name string
		obj  client.Object
		want bool
	}{
		{name: "provisioned slice", obj: stored(t, p, d, Slot{}), want: true},
		{name: "provisioned slice being released", obj: stored(t, p, d, Slot{}, withFinalizer, deleting), want: true},
		{name: "slice without the owner label", obj: stored(t, p, d, Slot{}, withLabel(LabelOwnerUID, ""))},
		{name: "unparseable slice", obj: unparseable},
		{name: "another kind", obj: notSlice},
		{name: "typed object", obj: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelOwnerUID: "uid-a"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slicesReleased.Reset()
			ObserveDeleted(tc.obj)
			wantSeries := 0
			if tc.want {
				wantSeries = 1
				if got := testutil.ToFloat64(slicesReleased.WithLabelValues("type-a", "2x2x1")); got != 1 {
					t.Fatalf("released{type-a,2x2x1} = %v, want 1", got)
				}
			}
			if got := testutil.CollectAndCount(slicesReleased); got != wantSeries {
				t.Fatalf("released series = %d, want %d", got, wantSeries)
			}
		})
	}
}
