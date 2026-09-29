package generator

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/placement/resolution"
	"github.com/ome-projects/ome/tests/perf/multicluster/runner/config"
)

func TestGeneratedSourceResolvesMemberFloor(t *testing.T) {
	for _, floor := range []int{1, 3} {
		t.Run(strconv.Itoa(floor), func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := v1beta1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			member := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				obj.SetUID(types.UID(obj.GetName()))
				obj.SetGeneration(1)
				return c.Create(ctx, obj, opts...)
			}}).Build()
			cls := config.SizeClass{ClassName: "small", RuntimeName: "runtime-a", MinReplicas: floor, Image: "example/model-server:test"}
			cfg := &config.Config{Classes: []config.SizeClass{cls, cls}, Placement: config.Placement{AcceleratorLabel: "gpu", AcceleratorValue: "test-accel"}}
			if err := SeedRuntimes(t.Context(), member, cfg, "team-a"); err != nil {
				t.Fatal(err)
			}
			source := New(nil, cfg, "team-a", "funnel", nil).buildSource(cls, 0, 0)
			source.Spec.Placement = nil
			resolved, floors, err := (resolution.Resolver{Client: member}).ResolveHome(t.Context(), source, nil)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff([]v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: int32(floor)}}, floors); diff != "" {
				t.Errorf("resolved floors (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(cls.Image, resolved.Engine.Runner.Image); diff != "" {
				t.Error(diff)
			}
			runtimes := &v1beta1.ServingRuntimeList{}
			if err := member.List(t.Context(), runtimes); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(1, len(runtimes.Items)); diff != "" {
				t.Errorf("shared runtime count: %s", diff)
			}
		})
	}
}
