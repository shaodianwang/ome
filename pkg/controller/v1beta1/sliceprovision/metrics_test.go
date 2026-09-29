package sliceprovision

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

// collectStates gathers the slice gauge, keyed type/topology/state.
func collectStates(t *testing.T) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(sliceStates)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	got := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			l := map[string]string{}
			for _, p := range m.GetLabel() {
				l[p.GetName()] = p.GetValue()
			}
			got[l["slice_type"]+"/"+l["topology"]+"/"+l["state"]] = m.GetGauge().GetValue()
		}
	}
	return got
}

// stateSource reads the slices c holds for testOwner's kind. Only the owners
// in live exist.
func stateSource(c client.Reader, live ...types.UID) StateSource {
	return StateSource{
		Slices:    c,
		Config:    testConfig(),
		OwnerKind: testOwner.Kind,
		OwnerExists: func(_ context.Context, uid types.UID) (bool, error) {
			return slices.Contains(live, uid), nil
		},
	}
}

// report reports src on the slice gauge until the test ends.
func report(t *testing.T, src StateSource) {
	t.Cleanup(ReportStates(src))
}

func TestSliceStates(t *testing.T) {
	p := newProvisioner(t, newFakeClient(), testOwner)
	other := newProvisioner(t, newFakeClient(), Owner{Kind: "Other", Namespace: "ns", Name: "svc-engine", UID: "uid-b"})
	d := demand(t, "2x2x1")
	for _, tc := range []struct {
		name      string
		slice     *unstructured.Unstructured
		ownerGone bool
		want      string
	}{
		{name: "ready", slice: stored(t, p, d, Slot{}, withState("ACTIVE", "")), want: stateReady},
		{name: "ready in another ready state", slice: stored(t, p, d, Slot{}, withState("ACTIVE_DEGRADED", "")), want: stateReady},
		{name: "pending without a state", slice: stored(t, p, d, Slot{}), want: statePending},
		{name: "pending in a state that is not ready", slice: stored(t, p, d, Slot{}, withState("FAILED", "")), want: statePending},
		{name: "terminating", slice: stored(t, p, d, Slot{}, withState("ACTIVE", ""), withFinalizer, deleting), want: stateTerminating},
		{name: "terminating after its owner is gone", slice: stored(t, p, d, Slot{}, withState("ACTIVE", ""), withFinalizer, deleting), ownerGone: true, want: stateTerminating},
		{name: "orphaned", slice: stored(t, p, d, Slot{}, withState("ACTIVE", "")), ownerGone: true, want: stateOrphaned},
		{name: "slice without the owner label", slice: stored(t, p, d, Slot{}, withLabel(LabelOwnerUID, ""))},
		{name: "slice of another owner kind", slice: stored(t, other, d, Slot{})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var live []types.UID
			if !tc.ownerGone {
				live = append(live, testOwner.UID)
			}
			report(t, stateSource(newFakeClient(tc.slice), live...))
			want := map[string]float64{}
			if tc.want != "" {
				want["type-a/2x2x1/"+tc.want] = 1
			}
			if diff := cmp.Diff(want, collectStates(t)); diff != "" {
				t.Fatalf("slice gauge (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSliceStatesCountsEachSeriesAndLooksUpEachOwnerOnce(t *testing.T) {
	a := newProvisioner(t, newFakeClient(), testOwner)
	b := newProvisioner(t, newFakeClient(), Owner{Kind: testOwner.Kind, Namespace: "ns", Name: "svc-other", UID: "uid-b"})
	small, large := demand(t, "2x2x1"), demand(t, "2x2x2")
	src := stateSource(newFakeClient(
		stored(t, a, small, Slot{Instance: 0}, withState("ACTIVE", "")),
		stored(t, a, small, Slot{Instance: 1}, withState("ACTIVE", "")),
		stored(t, a, large, Slot{Instance: 2}),
		stored(t, a, small, Slot{Instance: 3}, withFinalizer, deleting),
		stored(t, b, small, Slot{Instance: 0}, withState("ACTIVE", "")),
		stored(t, b, large, Slot{Instance: 1}),
	), testOwner.UID)
	exists, lookups := src.OwnerExists, 0
	src.OwnerExists = func(ctx context.Context, uid types.UID) (bool, error) {
		lookups++
		return exists(ctx, uid)
	}
	report(t, src)
	want := map[string]float64{
		"type-a/2x2x1/ready":       2,
		"type-a/2x2x2/pending":     1,
		"type-a/2x2x1/terminating": 1,
		"type-a/2x2x1/orphaned":    1,
		"type-a/2x2x2/orphaned":    1,
	}
	if diff := cmp.Diff(want, collectStates(t)); diff != "" {
		t.Fatalf("slice gauge (-want +got):\n%s", diff)
	}
	if lookups != 2 {
		t.Fatalf("owner lookups = %d, want one for each owner", lookups)
	}
}

func TestReportStates(t *testing.T) {
	p := newProvisioner(t, newFakeClient(), testOwner)
	c := newFakeClient(stored(t, p, demand(t, "2x2x1"), Slot{}, withState("ACTIVE", "")))
	if got := collectStates(t); len(got) != 0 {
		t.Fatalf("without a source the gauge reports %v, want nothing", got)
	}
	stopEarlier := ReportStates(stateSource(c))
	stopLater := ReportStates(stateSource(c, testOwner.UID))
	t.Cleanup(stopLater)
	stopEarlier()
	if diff := cmp.Diff(map[string]float64{"type-a/2x2x1/ready": 1}, collectStates(t)); diff != "" {
		t.Fatalf("after the replaced source stopped (-want +got):\n%s", diff)
	}
	stopLater()
	if got := collectStates(t); len(got) != 0 {
		t.Fatalf("after its source stopped the gauge reports %v, want nothing", got)
	}
}

func TestSliceStatesReportNothingWhenAReadFails(t *testing.T) {
	a := newProvisioner(t, newFakeClient(), testOwner)
	b := newProvisioner(t, newFakeClient(), Owner{Kind: testOwner.Kind, Namespace: "ns", Name: "svc-other", UID: "uid-b"})
	d := demand(t, "2x2x1")
	objs := []client.Object{stored(t, a, d, Slot{}, withState("ACTIVE", "")), stored(t, b, d, Slot{}, withState("ACTIVE", ""))}
	listFails := stateSource(fake.NewClientBuilder().WithObjects(objs...).WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return errors.New("injected list failure")
		},
	}).Build(), testOwner.UID, "uid-b")
	lookupFails := stateSource(newFakeClient(objs...), testOwner.UID)
	exists := lookupFails.OwnerExists
	lookupFails.OwnerExists = func(ctx context.Context, uid types.UID) (bool, error) {
		if uid == "uid-b" {
			return false, errors.New("injected owner lookup failure")
		}
		return exists(ctx, uid)
	}
	for _, tc := range []struct {
		name string
		src  StateSource
	}{
		{name: "listing the slices fails", src: listFails},
		{name: "one owner lookup fails", src: lookupFails},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report(t, tc.src)
			if got := collectStates(t); len(got) != 0 {
				t.Fatalf("the gauge reports %v, want nothing rather than a partial count", got)
			}
		})
	}
}
