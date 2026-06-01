package autoscaler

import (
	"context"
	"testing"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// newStatusTestScheme builds the minimal scheme the status writer needs:
// autoscalingv2 (for HPA), kedav1 (for ScaledObject), v1beta1 (defensive,
// in case future iterations of the writer need to Get an InferenceReplica).
// Reused across every table case so a missing scheme manifest surfaces
// once at suite-setup time rather than burying as a runtime panic per
// case.
func newStatusTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	g := gomega.NewWithT(t)
	g.Expect(autoscalingv2.AddToScheme(s)).To(gomega.Succeed())
	g.Expect(kedav1.AddToScheme(s)).To(gomega.Succeed())
	g.Expect(v1beta1.AddToScheme(s)).To(gomega.Succeed())
	return s
}

// TestWriteAutoscalerStatus_ManagedByMatrix pins the three-way mapping
// Class → ManagedBy specified in OEP-0013 + Q5:
//
//   - nil resolved block               → ManagedBy=none
//   - Class=hpa | keda                 → ManagedBy=ome
//   - Class=external                   → ManagedBy=external
//   - Class=none                       → ManagedBy=none
//   - unknown Class (defensive guard)  → ManagedBy=none
//
// Empty client (no live HPA / SO) — every case lands on zero counters
// and nil Conditions; the mapping isolation is the assertion target.
func TestWriteAutoscalerStatus_ManagedByMatrix(t *testing.T) {
	cases := []struct {
		name      string
		resolved  *v1beta1.ComponentAutoscaler
		wantClass v1beta1.AutoscalerClass
		wantMB    v1beta1.AutoscalerManagedBy
	}{
		{
			name:      "nil resolved block → none",
			resolved:  nil,
			wantClass: v1beta1.AutoscalerNone,
			wantMB:    v1beta1.AutoscalerManagedByNone,
		},
		{
			name:      "class=hpa → ome",
			resolved:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			wantClass: v1beta1.AutoscalerHPA,
			wantMB:    v1beta1.AutoscalerManagedByOME,
		},
		{
			name:      "class=keda → ome",
			resolved:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA},
			wantClass: v1beta1.AutoscalerKEDA,
			wantMB:    v1beta1.AutoscalerManagedByOME,
		},
		{
			name:      "class=external → external",
			resolved:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
			wantClass: v1beta1.AutoscalerExternal,
			wantMB:    v1beta1.AutoscalerManagedByExternal,
		},
		{
			name:      "class=none → none",
			resolved:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			wantClass: v1beta1.AutoscalerNone,
			wantMB:    v1beta1.AutoscalerManagedByNone,
		},
		{
			name:      "unrecognized class → none (defensive)",
			resolved:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerClass("mystery")},
			wantClass: v1beta1.AutoscalerClass("mystery"),
			wantMB:    v1beta1.AutoscalerManagedByNone,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			cli := fake.NewClientBuilder().WithScheme(newStatusTestScheme(t)).Build()

			ref := v1beta1.ScaleTargetRef{
				APIVersion: "ome.io/v1beta1",
				Kind:       "InferenceReplica",
				Name:       "isvc-engine",
			}
			status, stRef, err := WriteAutoscalerStatus(
				context.Background(), cli, "ns1", "isvc-engine",
				tc.resolved, SpecSourceISVC, ref,
			)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(status).NotTo(gomega.BeNil())
			g.Expect(status.Class).To(gomega.Equal(tc.wantClass))
			g.Expect(status.ManagedBy).To(gomega.Equal(tc.wantMB))
			g.Expect(status.SpecSource).To(gomega.Equal(string(SpecSourceISVC)),
				"SpecSource must echo the input verbatim")

			// No live HPA / SO planted → counters stay zero, conditions nil,
			// LastScaleTime nil. Mirrors the "dispatch not yet wired" state.
			g.Expect(status.CurrentReplicas).To(gomega.BeZero())
			g.Expect(status.DesiredReplicas).To(gomega.BeZero())
			g.Expect(status.LastScaleTime).To(gomega.BeNil())
			g.Expect(status.Conditions).To(gomega.BeNil())

			// ScaleTargetRef passes through verbatim — operator visibility
			// MUST survive every Class branch (including none / external).
			g.Expect(stRef).NotTo(gomega.BeNil())
			g.Expect(*stRef).To(gomega.Equal(ref))
		})
	}
}

// TestWriteAutoscalerStatus_SpecSourcePropagation pins that the SpecSource
// input is echoed verbatim onto status.specSource for all three layers
// (isvc / runtime / default) — operators rely on this to debug which layer
// of the inheritance chain produced the live class.
func TestWriteAutoscalerStatus_SpecSourcePropagation(t *testing.T) {
	sources := []SpecSource{SpecSourceISVC, SpecSourceRuntime, SpecSourceDefault}
	for _, src := range sources {
		src := src
		t.Run(string(src), func(t *testing.T) {
			g := gomega.NewWithT(t)
			cli := fake.NewClientBuilder().WithScheme(newStatusTestScheme(t)).Build()
			status, _, err := WriteAutoscalerStatus(
				context.Background(), cli, "ns1", "name1",
				&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
				src, v1beta1.ScaleTargetRef{},
			)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(status.SpecSource).To(gomega.Equal(string(src)))
		})
	}
}

// TestWriteAutoscalerStatus_HPAMirror pins the live-HPA mirror path:
// CurrentReplicas / DesiredReplicas / LastScaleTime / Conditions are all
// stamped verbatim from the HPA the writer Gets from the live client.
// Without this guard a refactor of the HPA mirror could silently drop
// fields and the operator would see "scaler exists, but I have no idea
// what it's doing".
func TestWriteAutoscalerStatus_HPAMirror(t *testing.T) {
	g := gomega.NewWithT(t)

	scaleTime := metav1.NewTime(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	transitionTime := metav1.NewTime(time.Date(2026, 5, 29, 11, 59, 30, 0, time.UTC))
	livehpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "isvc-engine"},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{
			CurrentReplicas: 4,
			DesiredReplicas: 7,
			LastScaleTime:   &scaleTime,
			Conditions: []autoscalingv2.HorizontalPodAutoscalerCondition{
				{
					Type:               autoscalingv2.ScalingActive,
					Status:             corev1.ConditionTrue,
					Reason:             "ValidMetricFound",
					Message:            "the HPA was able to successfully calculate a replica count",
					LastTransitionTime: transitionTime,
				},
				{
					Type:               autoscalingv2.AbleToScale,
					Status:             corev1.ConditionTrue,
					Reason:             "ReadyForNewScale",
					Message:            "recommended size matches current size",
					LastTransitionTime: transitionTime,
				},
				{
					Type:               autoscalingv2.ScalingLimited,
					Status:             corev1.ConditionFalse,
					Reason:             "DesiredWithinRange",
					Message:            "the desired count is within the acceptable range",
					LastTransitionTime: transitionTime,
				},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(newStatusTestScheme(t)).
		WithObjects(livehpa).
		Build()

	resolved := &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
	status, _, err := WriteAutoscalerStatus(
		context.Background(), cli, "ns1", "isvc-engine",
		resolved, SpecSourceISVC, v1beta1.ScaleTargetRef{
			APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "isvc-engine",
		},
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(status.ManagedBy).To(gomega.Equal(v1beta1.AutoscalerManagedByOME))
	g.Expect(status.CurrentReplicas).To(gomega.Equal(int32(4)))
	g.Expect(status.DesiredReplicas).To(gomega.Equal(int32(7)))
	g.Expect(status.LastScaleTime).NotTo(gomega.BeNil())
	g.Expect(status.LastScaleTime.Time.Equal(scaleTime.Time)).To(gomega.BeTrue(),
		"LastScaleTime must mirror the HPA's value (got %v, want %v)",
		status.LastScaleTime.Time, scaleTime.Time)

	g.Expect(status.Conditions).To(gomega.HaveLen(3))
	g.Expect(status.Conditions[0].Type).To(gomega.Equal("ScalingActive"))
	g.Expect(status.Conditions[0].Status).To(gomega.Equal(metav1.ConditionTrue))
	g.Expect(status.Conditions[0].Reason).To(gomega.Equal("ValidMetricFound"))
	g.Expect(status.Conditions[0].LastTransitionTime.Time.Equal(transitionTime.Time)).To(gomega.BeTrue(),
		"LastTransitionTime must propagate verbatim from the HPA's condition (got %v, want %v)",
		status.Conditions[0].LastTransitionTime.Time, transitionTime.Time)
	g.Expect(status.Conditions[2].Type).To(gomega.Equal("ScalingLimited"))
	g.Expect(status.Conditions[2].Status).To(gomega.Equal(metav1.ConditionFalse))
}

// TestWriteAutoscalerStatus_HPAMirror_NotFound pins the graceful-degradation
// requirement: when the live HPA hasn't been created yet (dispatch not
// wired, or the HPA was just submitted and the cache hasn't caught up),
// the writer returns the static fields populated and counters at zero.
// NO error, no crash, no stale state.
func TestWriteAutoscalerStatus_HPAMirror_NotFound(t *testing.T) {
	g := gomega.NewWithT(t)
	cli := fake.NewClientBuilder().WithScheme(newStatusTestScheme(t)).Build()

	resolved := &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
	status, _, err := WriteAutoscalerStatus(
		context.Background(), cli, "ns1", "isvc-engine",
		resolved, SpecSourceISVC, v1beta1.ScaleTargetRef{},
	)
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"NotFound on HPA Get must degrade gracefully — no error")
	g.Expect(status.ManagedBy).To(gomega.Equal(v1beta1.AutoscalerManagedByOME))
	g.Expect(status.CurrentReplicas).To(gomega.BeZero())
	g.Expect(status.DesiredReplicas).To(gomega.BeZero())
	g.Expect(status.LastScaleTime).To(gomega.BeNil())
	g.Expect(status.Conditions).To(gomega.BeNil())
}

// TestWriteAutoscalerStatus_ScaledObjectMirror pins the KEDA mirror path:
// Conditions + LastScaleTime come from the ScaledObject; CurrentReplicas
// + DesiredReplicas come from the derived HPA the SO writes (looked up via
// SO.Status.HpaName when present, falling back to the SO name otherwise).
//
// The SO name itself is the utils.GetScaledObjectName(name) of the IR — we
// plant both objects under that name so the writer's two-step lookup
// resolves cleanly.
func TestWriteAutoscalerStatus_ScaledObjectMirror(t *testing.T) {
	g := gomega.NewWithT(t)

	soName := utils.GetScaledObjectName("isvc-engine")
	derivedHpaName := "keda-hpa-" + soName

	lastActive := metav1.NewTime(time.Date(2026, 5, 29, 9, 30, 0, 0, time.UTC))
	so := &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: soName},
		Status: kedav1.ScaledObjectStatus{
			HpaName:        derivedHpaName,
			LastActiveTime: &lastActive,
			Conditions: kedav1.Conditions{
				{
					Type:    kedav1.ConditionReady,
					Status:  metav1.ConditionTrue,
					Reason:  kedav1.ScaledObjectConditionReadySucccesReason,
					Message: kedav1.ScaledObjectConditionReadySuccessMessage,
				},
				{
					Type:    kedav1.ConditionActive,
					Status:  metav1.ConditionFalse,
					Reason:  "ScalerDeactivated",
					Message: "Scaler deactivated",
				},
			},
		},
	}
	derivedHpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: derivedHpaName},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{
			CurrentReplicas: 2,
			DesiredReplicas: 5,
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(newStatusTestScheme(t)).
		WithObjects(so, derivedHpa).
		Build()

	resolved := &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA}
	status, _, err := WriteAutoscalerStatus(
		context.Background(), cli, "ns1", "isvc-engine",
		resolved, SpecSourceRuntime, v1beta1.ScaleTargetRef{
			APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "isvc-engine",
		},
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(status.ManagedBy).To(gomega.Equal(v1beta1.AutoscalerManagedByOME))
	g.Expect(status.SpecSource).To(gomega.Equal(string(SpecSourceRuntime)))

	g.Expect(status.LastScaleTime).NotTo(gomega.BeNil())
	g.Expect(status.LastScaleTime.Time.Equal(lastActive.Time)).To(gomega.BeTrue(),
		"LastScaleTime must mirror the SO's LastActiveTime (got %v, want %v)",
		status.LastScaleTime.Time, lastActive.Time)
	g.Expect(status.CurrentReplicas).To(gomega.Equal(int32(2)))
	g.Expect(status.DesiredReplicas).To(gomega.Equal(int32(5)))

	g.Expect(status.Conditions).To(gomega.HaveLen(2))
	g.Expect(status.Conditions[0].Type).To(gomega.Equal("Ready"))
	g.Expect(status.Conditions[0].Status).To(gomega.Equal(metav1.ConditionTrue))
	g.Expect(status.Conditions[1].Type).To(gomega.Equal("Active"))
	g.Expect(status.Conditions[1].Status).To(gomega.Equal(metav1.ConditionFalse))
}

// TestWriteAutoscalerStatus_ScaledObjectMirror_NoDerivedHPA pins the
// fallback where SO.Status.HpaName is empty (older KEDA versions stamp it
// lazily) AND no HPA exists under the SO's own name — counters land at
// zero, no error. The Conditions + LastScaleTime still mirror because
// those live on the SO directly.
func TestWriteAutoscalerStatus_ScaledObjectMirror_NoDerivedHPA(t *testing.T) {
	g := gomega.NewWithT(t)

	soName := utils.GetScaledObjectName("isvc-engine")
	lastActive := metav1.NewTime(time.Date(2026, 5, 29, 9, 30, 0, 0, time.UTC))
	so := &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: soName},
		Status: kedav1.ScaledObjectStatus{
			LastActiveTime: &lastActive,
			Conditions: kedav1.Conditions{
				{Type: kedav1.ConditionReady, Status: metav1.ConditionTrue},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(newStatusTestScheme(t)).
		WithObjects(so).
		Build()

	resolved := &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA}
	status, _, err := WriteAutoscalerStatus(
		context.Background(), cli, "ns1", "isvc-engine",
		resolved, SpecSourceDefault, v1beta1.ScaleTargetRef{},
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(status.CurrentReplicas).To(gomega.BeZero(),
		"no derived HPA found → counters stay zero")
	g.Expect(status.DesiredReplicas).To(gomega.BeZero())
	g.Expect(status.LastScaleTime).NotTo(gomega.BeNil(),
		"LastActiveTime on the SO still mirrors despite missing HPA")
	g.Expect(status.Conditions).To(gomega.HaveLen(1))
}

// TestWriteAutoscalerStatus_ScaleTargetRef_EmptyOmitsField pins that an
// empty ScaleTargetRef returns a nil pointer (so the caller stamps "field
// absent" onto status), not a zero-valued struct. Without this guard the
// rendered status would contain `{"apiVersion":"","kind":"","name":""}`
// which (a) violates the CRD's "always populated when active" comment and
// (b) makes external scalers think there's a target named "" they should
// scale.
func TestWriteAutoscalerStatus_ScaleTargetRef_EmptyOmitsField(t *testing.T) {
	g := gomega.NewWithT(t)
	cli := fake.NewClientBuilder().WithScheme(newStatusTestScheme(t)).Build()

	_, stRef, err := WriteAutoscalerStatus(
		context.Background(), cli, "ns1", "name1",
		nil, SpecSourceDefault, v1beta1.ScaleTargetRef{},
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(stRef).To(gomega.BeNil(),
		"empty input ref must return nil so the caller omits scaleTargetRef from status")
}

// TestWriteAutoscalerStatus_NilClient pins the "writer must not crash
// when called without a live client" guard — useful for unit tests that
// only want to exercise the static field mapping.
func TestWriteAutoscalerStatus_NilClient(t *testing.T) {
	g := gomega.NewWithT(t)

	resolved := &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
	status, stRef, err := WriteAutoscalerStatus(
		context.Background(),
		client.Client(nil),
		"ns1", "name1",
		resolved, SpecSourceISVC, v1beta1.ScaleTargetRef{
			APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "name1",
		},
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(status.ManagedBy).To(gomega.Equal(v1beta1.AutoscalerManagedByOME))
	g.Expect(status.CurrentReplicas).To(gomega.BeZero(),
		"nil client → skip mirror, counters stay zero")
	g.Expect(stRef).NotTo(gomega.BeNil())
}

// TestWriteAutoscalerStatus_ExternalManagedSkipsMirror pins that
// ManagedBy=external does NOT issue a live HPA / SO lookup — even if the
// caller plants one in the fake client, status conditions / counters stay
// empty. ManagedBy=external means OME is hands-off; mirroring would
// misrepresent ownership.
func TestWriteAutoscalerStatus_ExternalManagedSkipsMirror(t *testing.T) {
	g := gomega.NewWithT(t)

	// Plant an HPA that, if naively read, would surface stale counters.
	livehpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "isvc-engine"},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{
			CurrentReplicas: 99,
			DesiredReplicas: 99,
		},
	}
	cli := fake.NewClientBuilder().
		WithScheme(newStatusTestScheme(t)).
		WithObjects(livehpa).
		Build()

	resolved := &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
	status, _, err := WriteAutoscalerStatus(
		context.Background(), cli, "ns1", "isvc-engine",
		resolved, SpecSourceISVC, v1beta1.ScaleTargetRef{},
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(status.ManagedBy).To(gomega.Equal(v1beta1.AutoscalerManagedByExternal))
	g.Expect(status.CurrentReplicas).To(gomega.BeZero(),
		"external-managed → MUST NOT mirror; counters stay zero")
	g.Expect(status.Conditions).To(gomega.BeNil())
}
