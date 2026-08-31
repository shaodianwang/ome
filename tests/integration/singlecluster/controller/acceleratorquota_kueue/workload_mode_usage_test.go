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

		ginkgo.By("authoring a grouping tier and two leaves beneath the root")
		tier := quota(tenantsName, v1beta1.AcceleratorQuotaRootName,
			v1beta1.AcceleratorQuotaRoleCohort, nil)
		tier.Spec.Budgets[0].Nominal = *resource.NewQuantity(tierNominal, resource.DecimalSI)
		gomega.Expect(k8sClient.Create(suiteCtx, tier)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx,
			quota(leafName, tenantsName,
				v1beta1.AcceleratorQuotaRoleClusterQueue, []string{tenantNS}))).To(gomega.Succeed())
		// The idle sibling. Its namespace stays empty, so its whole share is
		// available for the cohort to lend and nothing of its own ever competes
		// for it.
		gomega.Expect(k8sClient.Create(suiteCtx,
			quota(idleName, tenantsName,
				v1beta1.AcceleratorQuotaRoleClusterQueue, []string{idleNS}))).To(gomega.Succeed())

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

		ginkgo.By("refusing work past the budget, with an idle sibling in the cohort")
		// The real test of a ceiling. The budget is exhausted and the sibling is
		// holding its whole share idle, so a queue allowed to borrow would admit
		// this and report more than the tree authorized. Nominal has to mean
		// nominal: consumption stays put rather than counting work that is only
		// waiting, and the queue never reaches past its own share to run it.
		gomega.Expect(k8sClient.Create(suiteCtx, workload("w3"))).To(gomega.Succeed())
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(admittedOn(g, leafName)).To(gomega.Equal(int64(2 * perPod)))
		}, "10s", interval).Should(gomega.Succeed())

		// Deliberately last, and deliberately in this chain: a freeze is only
		// meaningful once there is a good materialization to hold, and this is
		// the only suite that has one standing in a real Kueue.
		ginkgo.By("freezing rather than materializing a budget the cluster cannot honour")
		// The parent moves first. Raising only the leaf would break containment
		// and the node would freeze for that instead -- a different violation
		// reaching the same condition, which would let this pass while proving
		// nothing about capacity.
		for _, name := range []string{tenantsName, leafName} {
			raiseBudget(name, unaffordable)
		}

		gomega.Eventually(func(g gomega.Gomega) {
			leaf := &v1beta1.AcceleratorQuota{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: leafName}, leaf)).To(gomega.Succeed())
			g.Expect(degradedReason(leaf)).To(gomega.Equal(v1beta1.AcceleratorQuotaReasonCapacityExceeded),
				"a budget above what the cluster reports must say so, not fail quietly")
		}, timeout, interval).Should(gomega.Succeed())

		// The queue keeps the number it was last able to honour. Writing the new
		// one would let Kueue admit against hardware that is not there; deleting
		// the queue would take a working tenant's grant away over a number
		// nobody has spent yet. Consistently, not Eventually: the failure this
		// guards against is a later pass quietly applying it.
		gomega.Consistently(func(g gomega.Gomega) {
			cq := &kueuev1beta2.ClusterQueue{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: leafName}, cq)).To(gomega.Succeed())
			g.Expect(nominalQuotaOf(cq)).To(gomega.Equal(int64(nominal)))
		}, "10s", interval).Should(gomega.Succeed())

		ginkgo.By("keeping the work the frozen queue had already admitted")
		expectAdmitted(leafName, 2*perPod)
	})
})

// raiseBudget sets one node's accelerator budget, retrying the read-modify-write
// against a controller writing status to the same object.
func raiseBudget(name string, chips int64) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		q := &v1beta1.AcceleratorQuota{}
		g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: name}, q)).To(gomega.Succeed())
		q.Spec.Budgets[0].Nominal = *resource.NewQuantity(chips, resource.DecimalSI)
		g.Expect(k8sClient.Update(suiteCtx, q)).To(gomega.Succeed())
	}, timeout, interval).Should(gomega.Succeed())
}

// degradedReason is the reason carried on a node's Degraded condition, or ""
// when the node is not degraded.
func degradedReason(q *v1beta1.AcceleratorQuota) string {
	for _, c := range q.Status.Conditions {
		if c.Type != v1beta1.AcceleratorQuotaDegraded {
			continue
		}
		if c.Status == metav1.ConditionTrue {
			return c.Reason
		}
		return ""
	}
	return ""
}

// nominalQuotaOf reads the accelerator's nominalQuota out of a rendered
// ClusterQueue, or -1 when the queue covers no such resource.
func nominalQuotaOf(cq *kueuev1beta2.ClusterQueue) int64 {
	for _, group := range cq.Spec.ResourceGroups {
		for _, flavor := range group.Flavors {
			for _, r := range flavor.Resources {
				if string(r.Name) == acceleratorResource {
					return r.NominalQuota.Value()
				}
			}
		}
	}
	return -1
}

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
