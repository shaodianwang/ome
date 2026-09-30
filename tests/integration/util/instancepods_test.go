package util

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestMaterializeSinglePodInstances(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*v1beta1.InferenceReplica)
		foreign bool
		wantErr bool
	}{
		{name: "owned sparse instances are idempotent"},
		{name: "missing parent reference", edit: func(ir *v1beta1.InferenceReplica) { ir.Spec.ParentRef = nil }, wantErr: true},
		{name: "missing parent name", edit: func(ir *v1beta1.InferenceReplica) { ir.Spec.ParentRef.Name = "" }, wantErr: true},
		{name: "missing component identity", edit: func(ir *v1beta1.InferenceReplica) { ir.UID = "" }, wantErr: true},
		{name: "multiple runners require real execution", edit: func(ir *v1beta1.InferenceReplica) { ir.Spec.Runners = append(ir.Spec.Runners, ir.Spec.Runners[0]) }, wantErr: true},
		{name: "multiple Pods require real execution", edit: func(ir *v1beta1.InferenceReplica) { ir.Spec.Runners[0].Size = 2 }, wantErr: true},
		{name: "foreign Pod cannot be adopted", foreign: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "service-engine", UID: "component-a"},
				Spec: v1beta1.InferenceReplicaSpec{ParentRef: &v1beta1.ParentReference{Name: "service"}, Runners: []v1beta1.Runner{{Name: "default", Size: 1,
					Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example/model-server:test"}}}}}}}}
			if tt.edit != nil {
				tt.edit(ir)
			}
			rows := []v1beta1.OMENativeInstanceStatus{{Index: 2, Admitted: true}, {Index: 7}}
			before := append([]v1beta1.OMENativeInstanceStatus{}, rows...)
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			if tt.foreign {
				if err := c.Create(t.Context(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ir.Namespace, Name: ir.Name + "-2"}}); err != nil {
					t.Fatal(err)
				}
			}
			got, err := MaterializeSinglePodInstances(t.Context(), c, ir, rows)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error (-want +got):\n%s; %v", diff, err)
			}
			if diff := cmp.Diff(before, rows); diff != "" {
				t.Fatalf("input rows changed:\n%s", diff)
			}
			if err != nil {
				return
			}
			want := []v1beta1.OMENativeInstanceStatus{{Index: 2, Admitted: true, RunningRevision: ir.Name + "-fixture", PodCount: 1}, {Index: 7, RunningRevision: ir.Name + "-fixture", PodCount: 1}}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatal(diff)
			}
			pods := &corev1.PodList{}
			if err := c.List(t.Context(), pods, client.InNamespace(ir.Namespace)); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(2, len(pods.Items)); diff != "" {
				t.Fatal(diff)
			}
			for _, pod := range pods.Items {
				if !metav1.IsControlledBy(&pod, ir) {
					t.Fatal("Pod has no component owner")
				}
			}
			if _, err := MaterializeSinglePodInstances(t.Context(), c, ir, rows); err != nil {
				t.Fatal(err)
			}
			again := &corev1.PodList{}
			if err := c.List(t.Context(), again, client.InNamespace(ir.Namespace)); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(pods, again); diff != "" {
				t.Errorf("idempotent materialization changed Pods:\n%s", diff)
			}
		})
	}
}
