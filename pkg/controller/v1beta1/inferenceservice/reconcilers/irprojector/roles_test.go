package irprojector

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestRoleHelpers(t *testing.T) {
	inline := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc"},
		Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}, Router: &v1beta1.RouterSpec{}},
	}
	referenced := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc"},
		Spec:       v1beta1.InferenceServiceSpec{ReplicaRefs: &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}, Router: []string{"router-a"}}},
	}
	if ReferencesReplicas(inline) || !ReferencesReplicas(referenced) {
		t.Fatal("ReferencesReplicas must follow spec.replicaRefs.engine")
	}
	if got := ReferencedRoles(referenced); !reflect.DeepEqual(got, []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent}) {
		t.Fatalf("ReferencedRoles = %v", got)
	}
	for _, tc := range []struct {
		isvc         *v1beta1.InferenceService
		c            v1beta1.ComponentType
		declared     bool
		name, prefix string
	}{
		{inline, v1beta1.EngineComponent, true, "svc-engine", "svc"},
		{inline, v1beta1.DecoderComponent, false, "svc-decoder", "svc"},
		{inline, v1beta1.RouterComponent, true, "svc-router", "svc"},
		{referenced, v1beta1.EngineComponent, true, "pool-a", "pool-a"},
		{referenced, v1beta1.DecoderComponent, false, "svc-decoder", "svc"},
		{referenced, v1beta1.RouterComponent, true, "router-a", "router-a"},
	} {
		if got := RoleDeclared(tc.isvc, tc.c); got != tc.declared {
			t.Errorf("RoleDeclared(%s) = %v, want %v", tc.c, got, tc.declared)
		}
		if got := RoleReplicaName(tc.isvc, tc.c); got != tc.name {
			t.Errorf("RoleReplicaName(%s) = %q, want %q", tc.c, got, tc.name)
		}
		if got := RoleReplicaPrefix(tc.isvc, tc.c); got != tc.prefix {
			t.Errorf("RoleReplicaPrefix(%s) = %q, want %q", tc.c, got, tc.prefix)
		}
	}
}
