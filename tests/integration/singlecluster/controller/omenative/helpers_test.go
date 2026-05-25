package omenative

import (
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/util"
)

// The kubelet + EndpointSlice simulators and the pod binder moved to
// tests/integration/util/simulators.go so the coordination integration
// suite can share the same fixtures without duplicating them. This file
// keeps only the assertion helpers that are omenative-test-specific.

// listOMENativeRevisions returns every ControllerRevision in namespace
// tagged with the OMENative managed-by label. Used by revision-history
// assertions.
func listOMENativeRevisions(g gomega.Gomega, namespace string) []appsv1.ControllerRevision {
	crs := &appsv1.ControllerRevisionList{}
	g.Expect(k8sClient.List(suiteCtx, crs,
		client.InNamespace(namespace),
		client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
	)).To(gomega.Succeed())
	return crs.Items
}

// crNames returns just the .Name fields of a CR list — useful in test
// failure messages where the full CR objects would be too noisy.
func crNames(items []appsv1.ControllerRevision) []string {
	out := make([]string, 0, len(items))
	for _, cr := range items {
		out = append(out, cr.Name)
	}
	return out
}
