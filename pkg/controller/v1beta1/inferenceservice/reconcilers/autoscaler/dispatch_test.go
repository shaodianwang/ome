package autoscaler

import (
	"context"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// dispatchScheme registers autoscalingv2 + kedav1 so the fake client can
// store both kinds. Mirrors the manager-startup scheme (KEDA is a
// hard dependency; always registered).
func dispatchScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, autoscalingv2.AddToScheme(scheme))
	require.NoError(t, kedav1.AddToScheme(scheme))
	return scheme
}

// dispatchOwner is the canonical owner-ref the dispatch test fixtures
// stamp on emitted HPAs / SOs. The IR-managed path supplies the live
// InferenceReplica's owner-ref; here we synthesize a stable one so
// assertions on OwnerReferences are deterministic.
func dispatchOwner(name string) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       "InferenceReplica",
		Name:       name,
		UID:        "ir-uid-12345",
		Controller: ptr.To(true),
	}
}

// dispatchScaleTargetRef builds the autoscalingv2 ref the IR-managed
// path supplies on every dispatch. Both HPA + KEDA branches pass this
// through to their generators verbatim.
func dispatchScaleTargetRef(name string) autoscalingv2.CrossVersionObjectReference {
	return autoscalingv2.CrossVersionObjectReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       "InferenceReplica",
		Name:       name,
	}
}

// existingHPA materializes a stand-in HPA at the canonical key for a
// given Component (Namespace, Name) so cross-class delete tests can
// assert it gets removed.
func existingHPA(namespace, name string) *autoscalingv2.HorizontalPodAutoscaler {
	maxR := int32(3)
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: dispatchScaleTargetRef(name),
			MaxReplicas:    maxR,
		},
	}
}

// existingScaledObject materializes a stand-in ScaledObject at the
// canonical KEDA key (utils.GetScaledObjectName(name)) for cross-class
// delete tests.
func existingScaledObject(namespace, name string) *kedav1.ScaledObject {
	maxR := int32(3)
	return &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      utils.GetScaledObjectName(name),
		},
		Spec: kedav1.ScaledObjectSpec{
			ScaleTargetRef: &kedav1.ScaleTarget{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceReplica",
				Name:       name,
			},
			MaxReplicaCount: &maxR,
			Triggers: []kedav1.ScaleTriggers{
				{Type: "cron", Metadata: map[string]string{"timezone": "UTC", "start": "0 0 * * *", "end": "0 12 * * *", "desiredReplicas": "2"}},
			},
		},
	}
}

// kedaAutoscalerBlock builds a minimal KedaAutoscaler with one trigger
// — the KEDA generator's shouldCreateScaledObject gate rejects a zero-
// trigger SO, so every "create SO" case must supply at least one.
func kedaAutoscalerBlock() *v1beta1.KedaAutoscaler {
	return &v1beta1.KedaAutoscaler{
		Triggers: []kedav1.ScaleTriggers{
			{Type: "cron", Metadata: map[string]string{"timezone": "UTC", "start": "0 9 * * *", "end": "0 17 * * *", "desiredReplicas": "2"}},
		},
	}
}

// TestDispatchAutoscaler_Branches pins every Class branch — hpa, keda,
// external, none — both in the "no pre-existing autoscaler" steady state
// and with a pre-existing OME-managed sibling that must be deleted. This
// is the load-bearing test for the twin rule (none + external are status-field
// twins, share dispatch) and the always-on rule (even when class=none we
// still run the reconcile to ensure no stale OME-managed object lingers).
func TestDispatchAutoscaler_Branches(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	cases := []struct {
		name string
		// autoscaler is the resolved ComponentAutoscaler the dispatch
		// reads off Params. nil exercises the "treat as none" branch.
		autoscaler *v1beta1.ComponentAutoscaler
		// preexisting describes any OME-managed sibling planted in the
		// fake client before the dispatch runs. Tests cross-class
		// delete + the steady-state "no sibling" branch.
		preExistHPA bool
		preExistSO  bool
		// Post-conditions asserted after a single DispatchAutoscaler.
		wantHPAExists bool
		wantSOExists  bool
	}{
		{
			name:          "class=hpa, no preexisting → HPA created, no SO",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			wantHPAExists: true,
			wantSOExists:  false,
		},
		{
			name:          "class=hpa, preexisting SO → SO deleted + HPA created (cross-class switch keda→hpa)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			preExistSO:    true,
			wantHPAExists: true,
			wantSOExists:  false,
		},
		{
			name:          "class=keda with one trigger, no preexisting → SO created, no HPA",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			wantHPAExists: false,
			wantSOExists:  true,
		},
		{
			name:          "class=keda with one trigger, preexisting HPA → HPA deleted + SO created (cross-class switch hpa→keda)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			preExistHPA:   true,
			wantHPAExists: false,
			wantSOExists:  true,
		},
		{
			name:          "class=external, preexisting HPA → HPA deleted, no SO (twin)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
			preExistHPA:   true,
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "class=external, preexisting SO → SO deleted, no HPA (twin)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
			preExistSO:    true,
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "class=external, preexisting both → both deleted",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
			preExistHPA:   true,
			preExistSO:    true,
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "class=none, preexisting both → both deleted (behaves identically to external)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			preExistHPA:   true,
			preExistSO:    true,
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "Autoscaler == nil, preexisting both → both deleted (treat-as-none)",
			autoscaler:    nil,
			preExistHPA:   true,
			preExistSO:    true,
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "class=none, no preexisting → no-op (idempotent on empty state)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "class=hpa, preexisting HPA → idempotent re-reconcile (no spurious delete)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			preExistHPA:   true,
			wantHPAExists: true,
			wantSOExists:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := dispatchScheme(t)
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.preExistHPA {
				builder = builder.WithObjects(existingHPA(ns, name))
			}
			if tc.preExistSO {
				builder = builder.WithObjects(existingScaledObject(ns, name))
			}
			cl := builder.Build()

			err := DispatchAutoscaler(context.Background(), DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          dispatchOwner(name),
				Namespace:      ns,
				Name:           name,
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     tc.autoscaler,
				MinReplicas:    1,
				MaxReplicas:    5,
			})
			require.NoError(t, err)

			// HPA existence assertion.
			gotHPA := &autoscalingv2.HorizontalPodAutoscaler{}
			hpaErr := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, gotHPA)
			if tc.wantHPAExists {
				require.NoError(t, hpaErr, "expected HPA to exist")
				assert.Equal(t, dispatchScaleTargetRef(name), gotHPA.Spec.ScaleTargetRef, "HPA scaleTargetRef should target the IR")
				// Owner-ref should pin the HPA to the IR (GC cascade).
				require.Len(t, gotHPA.OwnerReferences, 1, "HPA should have exactly one ownerRef")
				assert.Equal(t, "InferenceReplica", gotHPA.OwnerReferences[0].Kind)
				assert.Equal(t, name, gotHPA.OwnerReferences[0].Name)
			} else {
				assert.True(t, apierrors.IsNotFound(hpaErr), "expected HPA to be absent, got err=%v", hpaErr)
			}

			// ScaledObject existence assertion.
			gotSO := &kedav1.ScaledObject{}
			soErr := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, gotSO)
			if tc.wantSOExists {
				require.NoError(t, soErr, "expected ScaledObject to exist")
				require.NotNil(t, gotSO.Spec.ScaleTargetRef, "SO scaleTargetRef should be set")
				assert.Equal(t, name, gotSO.Spec.ScaleTargetRef.Name, "SO target name should be the IR name")
				assert.Equal(t, "InferenceReplica", gotSO.Spec.ScaleTargetRef.Kind, "SO target kind should be InferenceReplica")
				require.Len(t, gotSO.OwnerReferences, 1, "SO should have exactly one ownerRef")
				assert.Equal(t, "InferenceReplica", gotSO.OwnerReferences[0].Kind)
			} else {
				assert.True(t, apierrors.IsNotFound(soErr), "expected SO to be absent, got err=%v", soErr)
			}
		})
	}
}

// TestDispatchAutoscaler_ClassSwitch_HPAToKEDA exercises the full
// class-switch lifecycle on the SAME fake client: first DispatchAutoscaler
// with class=hpa creates the HPA, the second with class=keda must delete
// that HPA and create the SO. Mirrors the operator workflow of editing
// isvc.spec.engine.autoscaler.class.
func TestDispatchAutoscaler_ClassSwitch_HPAToKEDA(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)
	scheme := dispatchScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	// First reconcile: class=hpa.
	err := DispatchAutoscaler(context.Background(), DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
		MinReplicas:    1,
		MaxReplicas:    5,
	})
	require.NoError(t, err)

	// HPA exists, SO does not.
	hpaExists := func() bool {
		err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{})
		return err == nil
	}
	soExists := func() bool {
		err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, &kedav1.ScaledObject{})
		return err == nil
	}
	assert.True(t, hpaExists(), "after class=hpa reconcile, HPA should exist")
	assert.False(t, soExists(), "after class=hpa reconcile, SO should not exist")

	// Second reconcile: class=keda — must delete the prior HPA and create the SO.
	err = DispatchAutoscaler(context.Background(), DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
		MinReplicas:    1,
		MaxReplicas:    5,
	})
	require.NoError(t, err)

	assert.False(t, hpaExists(), "after class=keda reconcile, prior HPA should be deleted")
	assert.True(t, soExists(), "after class=keda reconcile, SO should exist")
}

// TestDispatchAutoscaler_ClassSwitch_KEDAToHPA covers the reverse:
// first class=keda creates the SO, second class=hpa must delete the SO
// and create the HPA.
func TestDispatchAutoscaler_ClassSwitch_KEDAToHPA(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)
	scheme := dispatchScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	// First reconcile: class=keda.
	err := DispatchAutoscaler(context.Background(), DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
		MinReplicas:    1,
		MaxReplicas:    5,
	})
	require.NoError(t, err)

	hpaExists := func() bool {
		err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{})
		return err == nil
	}
	soExists := func() bool {
		err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, &kedav1.ScaledObject{})
		return err == nil
	}
	assert.False(t, hpaExists(), "after class=keda reconcile, HPA should not exist")
	assert.True(t, soExists(), "after class=keda reconcile, SO should exist")

	// Second reconcile: class=hpa — must delete the prior SO and create the HPA.
	err = DispatchAutoscaler(context.Background(), DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
		MinReplicas:    1,
		MaxReplicas:    5,
	})
	require.NoError(t, err)

	assert.True(t, hpaExists(), "after class=hpa reconcile, HPA should exist")
	assert.False(t, soExists(), "after class=hpa reconcile, prior SO should be deleted")
}

// TestDispatchAutoscaler_HPA_DefaultMetric verifies that when the
// resolved Autoscaler has Class=hpa but HPA=nil (the resolver's default
// branch), the generated HPA carries the default of a single CPU=80%
// Resource metric.
func TestDispatchAutoscaler_HPA_DefaultMetric(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)
	scheme := dispatchScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	err := DispatchAutoscaler(context.Background(), DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}, // HPA: nil → default CPU=80%
		MinReplicas:    1,
		MaxReplicas:    5,
	})
	require.NoError(t, err)

	got := &autoscalingv2.HorizontalPodAutoscaler{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got))

	require.Len(t, got.Spec.Metrics, 1, "default metric list should have exactly one entry")
	require.Equal(t, autoscalingv2.ResourceMetricSourceType, got.Spec.Metrics[0].Type)
	require.NotNil(t, got.Spec.Metrics[0].Resource)
	assert.EqualValues(t, "cpu", got.Spec.Metrics[0].Resource.Name)
	assert.Equal(t, autoscalingv2.UtilizationMetricType, got.Spec.Metrics[0].Resource.Target.Type)
	require.NotNil(t, got.Spec.Metrics[0].Resource.Target.AverageUtilization)
	assert.Equal(t, int32(80), *got.Spec.Metrics[0].Resource.Target.AverageUtilization)
}

// TestDispatchAutoscaler_GuardErrors enforces the input-validation
// contract: nil client, empty owner UID, empty namespace, and empty
// name all return wrapped errors before any cluster I/O. The IR-managed
// dispatch path captures the IR's UID from EnsureInferenceReplica's
// return value; refusing to dispatch on an empty UID guards against
// the dispatch site running before the IR is committed.
func TestDispatchAutoscaler_GuardErrors(t *testing.T) {
	scheme := dispatchScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	validOwner := dispatchOwner("demo-engine")
	validRef := dispatchScaleTargetRef("demo-engine")

	cases := []struct {
		name string
		p    DispatchParams
		want string
	}{
		{
			name: "nil client",
			p:    DispatchParams{Owner: validOwner, Namespace: "ns", Name: "x", ScaleTargetRef: validRef},
			want: "nil client",
		},
		{
			name: "empty owner UID",
			p:    DispatchParams{Client: cl, Owner: metav1.OwnerReference{Name: "x"}, Namespace: "ns", Name: "x", ScaleTargetRef: validRef},
			want: "empty Owner.UID",
		},
		{
			name: "empty namespace",
			p:    DispatchParams{Client: cl, Owner: validOwner, Namespace: "", Name: "x", ScaleTargetRef: validRef},
			want: "empty namespace",
		},
		{
			name: "empty name",
			p:    DispatchParams{Client: cl, Owner: validOwner, Namespace: "ns", Name: "", ScaleTargetRef: validRef},
			want: "empty namespace or name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := DispatchAutoscaler(context.Background(), tc.p)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
