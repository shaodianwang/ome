package components

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func TestRouterReconcileDeployment_OMENativeReturnsNotImplemented(t *testing.T) {
	r := &Router{
		BaseComponentFields: BaseComponentFields{DeploymentMode: constants.OMENative},
		routerSpec:          &v1beta1.RouterSpec{},
	}
	_, err := r.reconcileDeployment(nil, metav1.ObjectMeta{}, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "OMENative") || !strings.Contains(err.Error(), "not implemented") || !strings.Contains(err.Error(), "router") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
