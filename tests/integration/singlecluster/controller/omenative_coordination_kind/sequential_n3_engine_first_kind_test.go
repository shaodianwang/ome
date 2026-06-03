package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Sequential coordination N=3 with engine-first order.
//
// n3_sequential_kind covers Order=[router, decoder, engine] (the
// production-default decoder-first chain). This spec exercises the
// other direction: Order=[engine, decoder, router]. The Sequential
// state machine must not be biased to a specific Component — any
// permutation declared by the operator should serialize correctly.
//
// Asserts the two handoff windows analogous to the router-first spec:
//   - engine done while decoder + router still v1.
//   - decoder done while router still v1.
var _ = ginkgo.Describe("permutation — Sequential N=3 (engine-first order)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-seq-n3-ef-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("Sequential N=3 with Order=[engine, decoder, router] chains in declared direction", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-seq-n3-ef"),
			WithRouter(),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{
					v1beta1.EngineComponent,
					v1beta1.DecoderComponent,
					v1beta1.RouterComponent,
				},
				Policy: v1beta1.CoordinationPolicySequential,
				Order: []v1beta1.ComponentType{
					v1beta1.EngineComponent,
					v1beta1.DecoderComponent,
					v1beta1.RouterComponent,
				},
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-n3-ef")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent,
				v1beta1.DecoderComponent,
				v1beta1.RouterComponent,
			} {
				cs := c.Status.Components[comp]
				g.Expect(cs.Lifecycle).NotTo(BeNil(), "Component %s missing status", comp)
				g.Expect(cs.Lifecycle.ServingReplicas).To(BeNumerically(">=", 1))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-seq-n3-ef")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
		v1DecoderRev := preBump.Status.Components[v1beta1.DecoderComponent].Lifecycle.CurrentRevision
		v1RouterRev := preBump.Status.Components[v1beta1.RouterComponent].Lifecycle.CurrentRevision

		// Sample handoff windows. Same pattern as the router-first N=3
		// spec (n3_sequential_kind) but rotated for the engine-first
		// order:
		//   (a) engine done while decoder + router still v1.
		//   (b) decoder done while router still v1.
		sawEngineDoneWhileOthersV1 := false
		sawDecoderDoneWhileRouterV1 := false
		var lastE, lastD, lastR string
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(_ Gomega) {
			c := &v1beta1.InferenceService{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "perm-seq-n3-ef"}, c); err != nil {
				return
			}
			e := c.Status.Components[v1beta1.EngineComponent].Lifecycle
			d := c.Status.Components[v1beta1.DecoderComponent].Lifecycle
			r := c.Status.Components[v1beta1.RouterComponent].Lifecycle
			if e == nil || d == nil || r == nil {
				return
			}
			lastE, lastD, lastR = e.CurrentRevision, d.CurrentRevision, r.CurrentRevision
			if e.CurrentRevision != v1EngineRev &&
				d.CurrentRevision == v1DecoderRev &&
				r.CurrentRevision == v1RouterRev {
				sawEngineDoneWhileOthersV1 = true
			}
			if d.CurrentRevision != v1DecoderRev && r.CurrentRevision == v1RouterRev {
				sawDecoderDoneWhileRouterV1 = true
			}
		})
		defer stop()

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-seq-n3-ef")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Router.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-n3-ef")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.CurrentRevision).NotTo(Equal(v1DecoderRev))
			g.Expect(c.Status.Components[v1beta1.RouterComponent].Lifecycle.CurrentRevision).NotTo(Equal(v1RouterRev))
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		Expect(sawEngineDoneWhileOthersV1).To(BeTrue(),
			"never observed engine rolled-off-v1 while decoder+router still v1 — handoff 1 (engine → decoder) not enforced. lastE=%s lastD=%s lastR=%s",
			lastE, lastD, lastR)
		Expect(sawDecoderDoneWhileRouterV1).To(BeTrue(),
			"never observed decoder rolled-off-v1 while router still v1 — handoff 2 (decoder → router) not enforced. lastD=%s lastR=%s",
			lastD, lastR)
	})
})
