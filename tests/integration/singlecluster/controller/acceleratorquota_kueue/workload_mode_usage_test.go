package acceleratorquota_kueue

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	podtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	kueuebackend "github.com/ome-projects/ome/pkg/quota/backend/kueue"
)

var _ = ginkgo.Describe("A workload-mode quota plane against a real Kueue", func() {
	// One spec rather than several: the steps are a single causal chain, and
	// splitting them would mean either re-admitting workloads per spec or
	// sharing state between specs through the apiserver.
	ginkgo.It("reports what Kueue admits against the tree that authorized it", func() {
		ginkgo.By("self-creating the reserved root and measuring the cluster")
		// Never authored by this suite. A workload-mode manager brings the root
		// into being itself and derives capacity onto it, so asserting that is
		// asserting the mode.
		gomega.Eventually(func(g gomega.Gomega) {
			root := &v1beta1.AcceleratorQuota{}
			g.Expect(k8sClient.Get(suiteCtx,
				types.NamespacedName{Name: v1beta1.AcceleratorQuotaRootName}, root)).To(gomega.Succeed())
			g.Expect(root.Spec.Role).To(gomega.Equal(v1beta1.AcceleratorQuotaRoleCohort))
			g.Expect(root.Spec.ParentRef).To(gomega.BeNil())
			// Bare: a budget on the root is the admin's fleet total to write,
			// and the controller inventing one would be authorizing work.
			g.Expect(root.Spec.Budgets).To(gomega.BeEmpty())
			g.Expect(capacityFor(root)).To(gomega.Equal(int64(8)),
				"the root must report the accelerator the fake node carries")
		}, timeout, interval).Should(gomega.Succeed())

		ginkgo.By("authoring a grouping tier and a leaf beneath the root")
		gomega.Expect(k8sClient.Create(suiteCtx,
			quota(tenantsName, v1beta1.AcceleratorQuotaRootName,
				v1beta1.AcceleratorQuotaRoleCohort, nil))).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx,
			quota(leafName, tenantsName,
				v1beta1.AcceleratorQuotaRoleClusterQueue, []string{tenantNS}))).To(gomega.Succeed())

		ginkgo.By("materializing a ClusterQueue that Kueue accepts")
		gomega.Eventually(func(g gomega.Gomega) {
			cq := &kueuev1beta2.ClusterQueue{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: leafName}, cq)).To(gomega.Succeed())
			// The reader selects queues by these labels, so a queue rendered
			// without them would leave every figure at zero with nothing to say
			// why. Asserting them here names that failure at its cause.
			g.Expect(cq.Labels).To(gomega.HaveKeyWithValue(v1beta1.AcceleratorQuotaNodeLabel, leafName))
			g.Expect(cq.Labels).To(gomega.HaveKeyWithValue(v1beta1.AcceleratorQuotaManagedByLabel, fieldManager))
			g.Expect(activeCondition(cq)).To(gomega.BeTrue(), "Kueue must accept the rendered queue")
		}, timeout, interval).Should(gomega.Succeed())

		ginkgo.By("materializing a Cohort for the grouping tier")
		gomega.Eventually(func(g gomega.Gomega) {
			cohort := &kueuev1beta2.Cohort{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: tenantsName}, cohort)).To(gomega.Succeed())
		}, timeout, interval).Should(gomega.Succeed())

		ginkgo.By("materializing a LocalQueue in the bound namespace")
		gomega.Eventually(func(g gomega.Gomega) {
			lq := &kueuev1beta2.LocalQueue{}
			g.Expect(k8sClient.Get(suiteCtx,
				types.NamespacedName{Namespace: tenantNS, Name: kueuebackend.LocalQueueName}, lq)).
				To(gomega.Succeed())
		}, timeout, interval).Should(gomega.Succeed())

		ginkgo.By("charging an admitted workload to the leaf and its ancestor")
		admit("w1")
		expectAdmitted(leafName, perPod)
		expectAdmitted(tenantsName, perPod)

		// The second admission is the one that matters: it moves a figure the
		// first pass already wrote, so a status written once and then frozen
		// fails here where a single-workload assertion would pass.
		ginkgo.By("moving the figure again on a second admission")
		admit("w2")
		expectAdmitted(leafName, 2*perPod)
		expectAdmitted(tenantsName, 2*perPod)

		ginkgo.By("leaving a workload Kueue has not admitted uncharged")
		// The budget is exhausted, so Kueue holds this one. Consumption must
		// stay put rather than counting work that is only waiting.
		gomega.Expect(k8sClient.Create(suiteCtx, workload("w3"))).To(gomega.Succeed())
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(admittedOn(g, leafName)).To(gomega.Equal(int64(2 * perPod)))
		}, "10s", interval).Should(gomega.Succeed())
	})
})

// admit creates a queue-labeled workload and waits for Kueue to let it through.
//
// Gated then ungated is what admission looks like with no kubelet: the pod
// webhook adds a scheduling gate on create and the scheduler removes it once
// the workload fits the quota. Asserting the gate first means a pod Kueue never
// managed at all fails here, rather than passing the ungated check for the
// wrong reason.
func admit(name string) {
	ginkgo.GinkgoHelper()
	gomega.Expect(k8sClient.Create(suiteCtx, workload(name))).To(gomega.Succeed())

	gomega.Eventually(func(g gomega.Gomega) {
		pod := &corev1.Pod{}
		g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: tenantNS, Name: name}, pod)).To(gomega.Succeed())
		g.Expect(pod.Spec.SchedulingGates).NotTo(gomega.BeEmpty())
	}, timeout, interval).Should(gomega.Succeed(), "%s was never gated by Kueue", name)

	gomega.Eventually(func(g gomega.Gomega) {
		pod := &corev1.Pod{}
		g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: tenantNS, Name: name}, pod)).To(gomega.Succeed())
		g.Expect(pod.Spec.SchedulingGates).To(gomega.BeEmpty())
	}, timeout, interval).Should(gomega.Succeed(), "%s was never admitted against the quota", name)
}

// expectAdmitted waits for a node to report the given consumption.
func expectAdmitted(node string, want int64) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		g.Expect(admittedOn(g, node)).To(gomega.Equal(want))
	}, timeout, interval).Should(gomega.Succeed(), "%s never reported %d admitted", node, want)
}

// admittedOn reads one node's admitted figure for the accelerator budget.
func admittedOn(g gomega.Gomega, node string) int64 {
	aq := &v1beta1.AcceleratorQuota{}
	g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: node}, aq)).To(gomega.Succeed())
	for _, b := range aq.Status.Budgets {
		if b.ResourceName == acceleratorResource && b.ResourceFlavor == flavorName {
			return b.Admitted.Value()
		}
	}
	g.Expect(aq.Status.Budgets).To(gomega.BeEmpty(),
		"node %s carries budget status, but none for %s/%s", node, acceleratorResource, flavorName)
	// Unreached once the assertion above fires; a node with no budget status at
	// all is holding nothing as far as this read is concerned.
	return 0
}

// capacityFor reads the root's derived allocatable for the accelerator.
func capacityFor(root *v1beta1.AcceleratorQuota) int64 {
	for _, c := range root.Status.Capacity {
		if c.ResourceName == acceleratorResource && c.ResourceFlavor == flavorName {
			return c.Allocatable.Value()
		}
	}
	return 0
}

// workload is a pod that charges perPod of the accelerator to the tenant queue.
//
// RequestAndLimit rather than Request: an extended resource is not
// overcommittable, so the apiserver rejects a container asking for one without
// an equal limit.
func workload(name string) *corev1.Pod {
	return podtesting.MakePod(name, tenantNS).
		Queue(kueuebackend.LocalQueueName).
		RequestAndLimit(acceleratorResource, fmt.Sprintf("%d", perPod)).
		Obj()
}

func quota(name, parent string, role v1beta1.AcceleratorQuotaRole, namespaces []string) *v1beta1.AcceleratorQuota {
	q := &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:       role,
			Namespaces: namespaces,
			Budgets: []v1beta1.AcceleratorBudget{{
				ResourceName:   acceleratorResource,
				ResourceFlavor: flavorName,
				Nominal:        *resource.NewQuantity(nominal, resource.DecimalSI),
			}},
		},
	}
	if parent != "" {
		q.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: parent}
	}
	return q
}

func activeCondition(cq *kueuev1beta2.ClusterQueue) bool {
	for _, c := range cq.Status.Conditions {
		if c.Type == kueuev1beta2.ClusterQueueActive {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}
