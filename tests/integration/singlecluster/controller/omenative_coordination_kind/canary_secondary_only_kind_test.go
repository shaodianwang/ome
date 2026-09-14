package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A canary run is identified by the complete group target set. An update to a
// secondary must start a fresh canary even when the externally routed primary
// remains on its existing revision.
var _ = ginkgo.Describe("behavior — canary secondary-only update", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "canary-secondary-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("restarts the canary when only the engine revision changes", func() {
		const isvcName = "canary-secondary"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithRouter(),
			WithoutDecoder(),
			WithEngineReplicas(4),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var stableEngineHash, routerHash string
		Eventually(func(g Gomega) {
			engine := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			router := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.RouterComponent)
			stableEngineHash = query.RevisionFromName(engine.Status.CurrentRevision).Hash()
			routerHash = query.RevisionFromName(router.Status.CurrentRevision).Hash()
			g.Expect(stableEngineHash).NotTo(BeEmpty())
			g.Expect(routerHash).NotTo(BeEmpty())
			g.Expect(engine.Status.UpdatedReplicas).To(Equal(engine.Status.Replicas))
			g.Expect(router.Status.UpdatedReplicas).To(Equal(router.Status.Replicas))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("adding a canary group and changing only the engine")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 25, Pause: &v1beta1.RolloutPause{}},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			}}}
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Rollout).NotTo(BeNil())
			g.Expect(c.Status.Rollout.ActiveRun).NotTo(BeNil(), "the engine target must open a group run")
			g.Expect(c.Status.Canary).NotTo(BeNil(), "the group run must initialize the canary state machine")
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Components[v1beta1.RouterComponent].RolloutPhase).To(Equal(v1beta1.RolloutPhasePaused))

			engineTarget := query.RevisionFromName(
				freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent).Status.UpdateRevision,
			).Hash()
			g.Expect(engineTarget).NotTo(BeEmpty())
			g.Expect(engineTarget).NotTo(Equal(stableEngineHash))

			engineByRevision := map[string]int{}
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp == nil && podIsReady(&pod) && pod.Labels["component"] == string(v1beta1.EngineComponent) {
					engineByRevision[pod.Labels[query.LabelRevisionHash]]++
				}
			}
			g.Expect(engineByRevision[stableEngineHash]).To(Equal(2))
			g.Expect(engineByRevision[engineTarget]).To(Equal(2))

			traffic := trafficForComponent(c, v1beta1.RouterComponent)
			g.Expect(traffic).To(HaveLen(1), "an unchanged primary has one routable revision: %s", describeTraffic(traffic))
			g.Expect(traffic[0].Percent).To(Equal(int32(100)))
			g.Expect(query.RevisionFromName(traffic[0].RevisionName).Hash()).To(Equal(routerHash))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
