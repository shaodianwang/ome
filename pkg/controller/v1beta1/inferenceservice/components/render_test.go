package components

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestReplicaRenderingRequiresInputs(t *testing.T) {
	for _, component := range []struct {
		name   string
		render func(context.Context, *ComponentDeps, ComponentInputs, *v1beta1.InferenceService, bool) (ReplicaTemplates, error)
	}{
		{name: "engine", render: func(ctx context.Context, deps *ComponentDeps, in ComponentInputs, service *v1beta1.InferenceService, missing bool) (ReplicaTemplates, error) {
			var spec *v1beta1.EngineSpec
			if !missing {
				spec = &v1beta1.EngineSpec{}
			}
			return RenderEngineReplica(ctx, deps, in, service, spec)
		}},
		{name: "decoder", render: func(ctx context.Context, deps *ComponentDeps, in ComponentInputs, service *v1beta1.InferenceService, missing bool) (ReplicaTemplates, error) {
			var spec *v1beta1.DecoderSpec
			if !missing {
				spec = &v1beta1.DecoderSpec{}
			}
			return RenderDecoderReplica(ctx, deps, in, service, spec)
		}},
	} {
		t.Run(component.name, func(t *testing.T) {
			for _, name := range []string{"dependencies", "client", "service", "spec"} {
				t.Run(name, func(t *testing.T) {
					scheme := runtime.NewScheme()
					if err := corev1.AddToScheme(scheme); err != nil {
						t.Fatal(err)
					}
					deps := &ComponentDeps{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
					service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a"}}
					switch name {
					case "dependencies":
						deps = nil
					case "client":
						deps.Client = nil
					case "service":
						service = nil
					}
					got, err := component.render(t.Context(), deps, ComponentInputs{}, service, name == "spec")
					if err == nil {
						t.Fatal("incomplete rendering input was accepted")
					}
					if diff := cmp.Diff(ReplicaTemplates{}, got); diff != "" {
						t.Fatalf("partial templates (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func TestRenderDecoderRejectsInvalidInputs(t *testing.T) {
	for _, tt := range []struct {
		name   string
		weight bool
		object bool
		worker bool
	}{
		{name: "fine-tuned weight unavailable", weight: true},
		{name: "fine-tuned strategy unavailable", weight: true, object: true},
		{name: "worker template has no container", worker: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, v1beta1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a"}, Spec: v1beta1.InferenceServiceSpec{Decoder: &v1beta1.DecoderSpec{}}}
			spec := &v1beta1.DecoderSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example.com/serving:v1"}}}
			if tt.worker {
				spec.Worker = &v1beta1.WorkerSpec{}
			}
			if tt.weight {
				service.Spec.Model = &v1beta1.ModelRef{FineTunedWeights: []string{"adapter"}}
			}
			var objects []client.Object
			if tt.object {
				objects = append(objects, &v1beta1.FineTunedWeight{ObjectMeta: metav1.ObjectMeta{Name: "adapter"}, Spec: v1beta1.FineTunedWeightSpec{
					Configuration:   runtime.RawExtension{Raw: []byte(`{}`)},
					HyperParameters: runtime.RawExtension{Raw: []byte(`{}`)},
				}})
			}
			deps := &ComponentDeps{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
			got, err := RenderDecoderReplica(t.Context(), deps, ComponentInputs{}, service, spec)
			if err == nil {
				t.Fatal("incomplete decoder rendering was accepted")
			}
			if diff := cmp.Diff(ReplicaTemplates{}, got); diff != "" {
				t.Fatalf("partial templates (-want +got):\n%s", diff)
			}
		})
	}
}
