package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("OMENative multi-pod topology contract", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-topology-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("uses the configured topology key for initial and surge PodGroups and worker affinity", func() {
		const isvcName = "topology-gang"
		topologyKey := corev1.LabelHostname
		var initialRevision string
		fix := newCoordFixture(ns.Name,
			WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineWorkerSize(1),
			WithEngineTopologyKey(topologyKey),
		)
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		initialPodGroup := query.PodGroupName(isvcName, workload.ComponentEngine, 0)
		ginkgo.By("projecting the topology key onto the IR, PodGroup, and worker's required affinity")
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Spec.TopologyKey).NotTo(BeNil())
			g.Expect(*ir.Spec.TopologyKey).To(Equal(topologyKey))
			g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
			g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))
			initialRevision = ir.Status.CurrentRevision

			pg := &schedulingv1alpha1.PodGroup{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: initialPodGroup}, pg)).To(Succeed())
			g.Expect(pg.Spec.MinMember).To(Equal(int32(2)))
			g.Expect(pg.Annotations).To(HaveKeyWithValue(query.AnnotationTopologyKey, topologyKey))

			pods := listOMENativePods(g, ns.Name, isvcName)
			g.Expect(pods).To(HaveLen(2))
			var worker *corev1.Pod
			for i := range pods {
				if pods[i].Labels[query.LabelRunner] == string(v1beta1.RunnerNameWorker) {
					worker = &pods[i]
				}
			}
			g.Expect(worker).NotTo(BeNil())
			g.Expect(worker.Labels).To(HaveKeyWithValue(query.LabelPodGroup, initialPodGroup))
			g.Expect(requiredAffinityTopologyKeys(worker)).To(ContainElement(topologyKey))
			g.Expect(worker.Spec.SchedulerName).To(Or(BeEmpty(), Equal(corev1.DefaultSchedulerName)),
				"installing the PodGroup API must not opt pods into a missing custom scheduler")
			g.Expect(podreadiness.IsContainersReady(worker)).To(BeTrue())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("changing only the topology key to create a new revision and surge PodGroup")
		updatedTopologyKey := corev1.LabelOSStable
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.TopologyKey = &updatedTopologyKey
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			pgs := &schedulingv1alpha1.PodGroupList{}
			g.Expect(k8sClient.List(suiteCtx, pgs, client.InNamespace(ns.Name))).To(Succeed())
			var surge *schedulingv1alpha1.PodGroup
			for i := range pgs.Items {
				pg := &pgs.Items[i]
				if pg.Name != initialPodGroup {
					surge = pg
					break
				}
			}
			g.Expect(surge).NotTo(BeNil(), "expected a fresh-index PodGroup during gang surge")
			g.Expect(surge.Spec.MinMember).To(Equal(int32(2)))
			g.Expect(surge.Annotations).To(HaveKeyWithValue(query.AnnotationTopologyKey, updatedTopologyKey))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("converging the topology-only rollout with new worker affinity and revision")
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
			g.Expect(ir.Status.CurrentRevision).NotTo(Equal(initialRevision))
			g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))

			pods := listOMENativePods(g, ns.Name, isvcName)
			g.Expect(pods).To(HaveLen(2))
			var worker *corev1.Pod
			for i := range pods {
				if pods[i].Labels[query.LabelRunner] == string(v1beta1.RunnerNameWorker) {
					worker = &pods[i]
				}
			}
			g.Expect(worker).NotTo(BeNil())
			g.Expect(requiredAffinityTopologyKeys(worker)).To(ContainElement(updatedTopologyKey))
			g.Expect(requiredAffinityTopologyKeys(worker)).NotTo(ContainElement(topologyKey))

			pgs := &schedulingv1alpha1.PodGroupList{}
			g.Expect(k8sClient.List(suiteCtx, pgs, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(pgs.Items).To(HaveLen(1), "source PodGroup must be collected after surge promotion")
			g.Expect(pgs.Items[0].Annotations).To(HaveKeyWithValue(query.AnnotationTopologyKey, updatedTopologyKey))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})

func requiredAffinityTopologyKeys(pod *corev1.Pod) []string {
	if pod == nil || pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAffinity == nil {
		return nil
	}
	terms := pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	keys := make([]string, 0, len(terms))
	for i := range terms {
		keys = append(keys, terms[i].TopologyKey)
	}
	return keys
}
