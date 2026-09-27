package irprojector

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/placement/protocol"
)

func TestPlacementExecutionProjection(t *testing.T) {
	for _, tt := range []struct {
		name    string
		derived bool
		edit    func(*v1beta1.PlacementExecutionPolicy)
		remove  bool
		wantErr bool
	}{
		{name: "ordinary local ignores policy"},
		{name: "same policy", derived: true},
		{name: "new policy", derived: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.PlanID = "plan-b"; p.Revision++; p.PauseSurge = false }},
		{name: "stale policy", derived: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision-- }, wantErr: true},
		{name: "same revision conflicting plan", derived: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.PlanID = "plan-b" }, wantErr: true},
		{name: "same revision conflicting pause", derived: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.PauseSurge = false }, wantErr: true},
		{name: "missing policy cannot clear pause", derived: true, remove: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			service := baselineISVC("service-a", "team-a")
			policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 2, SourceUID: "source-a", ClusterUID: "cluster-a", PauseSurge: true}
			stamp := func() {
				raw, err := protocol.Encode(policy)
				if err != nil {
					t.Fatal(err)
				}
				service.Annotations = map[string]string{constants.PlacementExecution: raw}
			}
			stamp()
			if tt.derived {
				service.Labels = map[string]string{constants.PlacementOrigin: "source-a"}
			}
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
			p := minimalParams(t, service, c)
			created, err := EnsureInferenceReplica(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			var want *v1beta1.PlacementExecutionPolicy
			if tt.derived {
				want = policy.DeepCopy()
			}
			if diff := cmp.Diff(want, created.Spec.PlacementExecution); diff != "" {
				t.Fatalf("created policy (-want +got):\n%s", diff)
			}
			if tt.edit != nil {
				tt.edit(policy)
				stamp()
			}
			if tt.remove {
				delete(service.Annotations, constants.PlacementExecution)
			}
			_, err = EnsureInferenceReplica(ctx, p)
			if (err != nil) != tt.wantErr {
				t.Fatalf("projection error = %v, want error %t", err, tt.wantErr)
			}
			stored := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(created), stored); err != nil {
				t.Fatal(err)
			}
			if tt.wantErr {
				if diff := cmp.Diff(created, stored); diff != "" {
					t.Errorf("rejected projection mutated IR (-want +got):\n%s", diff)
				}
			} else if tt.derived {
				if diff := cmp.Diff(policy, stored.Spec.PlacementExecution); diff != "" {
					t.Errorf("updated policy (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestPlacementProjectionCannotOverwriteRacingAuthority(t *testing.T) {
	ctx := context.Background()
	service := baselineISVC("service-a", "team-a")
	service.Labels = map[string]string{constants.PlacementOrigin: "source-a"}
	policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", PauseSurge: true}
	raw, err := protocol.Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	service.Annotations = map[string]string{constants.PlacementExecution: raw}
	base := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	created, err := EnsureInferenceReplica(ctx, minimalParams(t, service, base))
	if err != nil {
		t.Fatal(err)
	}
	policy.PlanID, policy.Revision = "plan-b", 2
	raw, err = protocol.Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	service.Annotations[constants.PlacementExecution] = raw
	newer := policy.DeepCopy()
	newer.PlanID, newer.Revision = "plan-c", 3
	injected := false
	c := interceptor.NewClient(base, interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
		if !injected {
			injected = true
			live := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(created), live); err != nil {
				return err
			}
			live.Spec.PlacementExecution = newer.DeepCopy()
			if err := c.Update(ctx, live); err != nil {
				return err
			}
		}
		return c.Patch(ctx, obj, patch, opts...)
	}})
	if _, err := EnsureInferenceReplica(ctx, minimalParams(t, service, c)); err == nil {
		t.Fatal("expected racing authority rejection")
	}
	stored := &v1beta1.InferenceReplica{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(created), stored); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(newer, stored.Spec.PlacementExecution); diff != "" {
		t.Errorf("authority regressed (-want +got):\n%s", diff)
	}
}
