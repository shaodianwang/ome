package omenative_eval_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// prepareEvalCase removes leftovers from a reused KIND cluster and registers
// the same bounded cleanup for the end of the spec.
func prepareEvalCase(name string) {
	ginkgo.GinkgoHelper()
	deleteEvalCase(name)
	ginkgo.DeferCleanup(deleteEvalCase, name)
}

func deleteEvalCase(name string) {
	ginkgo.GinkgoHelper()
	key := client.ObjectKey{Namespace: evalNamespace(), Name: name}
	isvc := &v1beta1.InferenceService{}
	err := k8sClient.Get(suiteCtx, key, isvc)
	if err == nil {
		err = k8sClient.Delete(suiteCtx, isvc)
	}
	Expect(err == nil || apierrors.IsNotFound(err)).To(BeTrue(),
		"delete eval ISVC %s/%s: %v", key.Namespace, key.Name, err)

	Eventually(func(g Gomega) {
		current := &v1beta1.InferenceService{}
		err := k8sClient.Get(suiteCtx, key, current)
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
			"ISVC %s/%s still exists or cannot be read: %v", key.Namespace, key.Name, err)

		labels := client.MatchingLabels{constants.InferenceServicePodLabelKey: name}
		pods := &corev1.PodList{}
		g.Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(key.Namespace), labels)).To(Succeed())
		g.Expect(pods.Items).To(BeEmpty(), "pods for %s still exist", name)

		irs := &v1beta1.InferenceReplicaList{}
		g.Expect(k8sClient.List(suiteCtx, irs, client.InNamespace(key.Namespace), labels)).To(Succeed())
		g.Expect(irs.Items).To(BeEmpty(), "InferenceReplicas for %s still exist", name)

		revisions := &appsv1.ControllerRevisionList{}
		g.Expect(k8sClient.List(suiteCtx, revisions, client.InNamespace(key.Namespace), labels)).To(Succeed())
		g.Expect(revisions.Items).To(BeEmpty(), "ControllerRevisions for %s still exist", name)

		services := &corev1.ServiceList{}
		g.Expect(k8sClient.List(suiteCtx, services, client.InNamespace(key.Namespace))).To(Succeed())
		for _, service := range services.Items {
			g.Expect(service.Spec.Selector[constants.InferenceServicePodLabelKey]).NotTo(Equal(name),
				"Service %s for %s still exists", service.Name, name)
		}
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

func listEvalIRs(g Gomega, isvcName string) []v1beta1.InferenceReplica {
	list := &v1beta1.InferenceReplicaList{}
	g.Expect(k8sClient.List(suiteCtx, list,
		client.InNamespace(evalNamespace()),
		client.MatchingLabels{constants.InferenceServicePodLabelKey: isvcName},
	)).To(Succeed())
	return list.Items
}
