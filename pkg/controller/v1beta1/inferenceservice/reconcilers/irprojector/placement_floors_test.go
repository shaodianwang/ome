package irprojector

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/placement/protocol"
)

func TestPlacementFloorProjection(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial projection", true: "standing projection"}[existing], func(t *testing.T) {
			for _, tt := range []struct {
				name                   string
				floor                  *int
				local, router, wantErr bool
			}{
				{name: "accepted floor", floor: ptr.To(2)},
				{name: "larger floor", floor: ptr.To(3), wantErr: true},
				{name: "smaller floor", floor: ptr.To(1), wantErr: true},
				{name: "unresolved floor", wantErr: true},
				{name: "component absent from authority", floor: ptr.To(2), router: true, wantErr: true},
				{name: "local floor remains independent", floor: ptr.To(3), local: true},
			} {
				t.Run(tt.name, func(t *testing.T) {
					service := baselineISVC("service-a", "team-a")
					if !tt.local {
						service.Labels = map[string]string{constants.PlacementOrigin: "source-a"}
					}
					cl := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
					p := minimalParams(t, service, cl)
					var before *v1beta1.InferenceReplica
					if existing {
						var err error
						before, err = EnsureInferenceReplica(t.Context(), p)
						if err != nil {
							t.Fatal(err)
						}
					}
					policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", PauseSurge: true,
						ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}}}
					raw, err := protocol.Encode(policy)
					if err != nil {
						t.Fatal(err)
					}
					service.Annotations = map[string]string{constants.PlacementExecution: raw}
					p.ComponentExt.MinReplicas = tt.floor
					if tt.router {
						p.Component = v1beta1.RouterComponent
					}
					_, err = EnsureInferenceReplica(t.Context(), p)
					if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
						t.Fatalf("projection: %s: %v", diff, err)
					}
					list := &v1beta1.InferenceReplicaList{}
					if err := cl.List(t.Context(), list, client.InNamespace(service.Namespace)); err != nil {
						t.Fatal(err)
					}
					if tt.wantErr {
						var want []v1beta1.InferenceReplica
						if before != nil {
							want = []v1beta1.InferenceReplica{*before}
						}
						if len(list.Items) == 0 {
							list.Items = nil
						}
						if diff := cmp.Diff(want, list.Items); diff != "" {
							t.Fatalf("rejected projection changed workloads: %s", diff)
						}
					} else {
						if diff := cmp.Diff(1, len(list.Items)); diff != "" {
							t.Fatal(diff)
						}
						want := policy
						if tt.local {
							want = nil
						}
						if diff := cmp.Diff(want, list.Items[0].Spec.PlacementExecution); diff != "" {
							t.Fatal(diff)
						}
					}
				})
			}
		})
	}
}
