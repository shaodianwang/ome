package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A pairing protocol bumped again while the first bump is half-landed
// converges, and the engine stranded on the middle protocol moves first.
//
// Two engines and two decoders pair on protocol v1 in one blueGreen group.
// The engines update by SurgeThenDrain, one at a time with none allowed
// out of rotation; the decoders update one at a time by RecreatePod, and
// their containers start late. The operator bumps the protocol to v2. The
// first engine gets its v2 replacement in seconds and is promoted, since
// a drain-first decoder is never waited on, while the decoder's v2 pod is
// still coming up. The operator then bumps the protocol to v3. The decoder
// withdraws its half-built v2 pod and builds a v3 one, so the fleet is
// engines {v2, v1} and decoders on v1 with v3 the target and no v2 decoder
// ever serving: the v2 engine pairs with nothing, and the v1 engine is the
// last engine that pairs with a decoder.
//
// The gate must charge each start to the protocol its own row runs: the
// v2 engine may move to v3 at once, because the (v1, v1) pair stays; the
// v1 engine is held until a v3 decoder serves. A gate that charges every
// start to every old protocol holds both engines, the decoder's second
// move waits for a v3 engine that never comes, and the roll wedges with
// one engine rotated and no decoder.
//
// The spec asserts: a v3 engine serves while the original v1 engine pod is
// still in rotation; that pod stays in rotation until a decoder serves v3,
// and the engine's hold names the pairing gate meanwhile; a pairable
// engine+decoder traffic pair stands at every sample; and both Components
// converge on v3 on every surface. The shared sampler's per-Component
// floor reads the stranded engine as one pairable pod short while it
// stands, the capacity picture of a re-target over a half-landed protocol,
// and reports it without failing the spec.
const (
	rebumpISVC = "pairing-rebump"
	// rebumpDecoderDelaySec keeps a new decoder pod NotReady long enough for
	// the first engine to be promoted on v2, and for the protocol to move
	// again, before any decoder serves v2.
	rebumpDecoderDelaySec = 60
)

var _ = ginkgo.Describe("behavior — a protocol bumped again over a half-landed cohort converges", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pairing-rebump-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("moves the engine stranded on the middle protocol first and lands both sides on the final one", func() {
		fix := newCoordFixture(ns.Name, WithISVCName(rebumpISVC),
			WithEngineReplicas(2),
			WithDecoderReplicas(2),
			WithDecoderUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent),
			WithPairingProtocol("nixl-v1"),
			WithEngineImage("fake-serving:v1"),
			WithDecoderStartupDelay(rebumpDecoderDelaySec))
		// Engines surge one at a time with none out of rotation; decoders
		// are recreated one at a time.
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type: v1beta1.UpdateStrategySurgeThenDrain,
				RollingUpdate: &v1beta1.RollingUpdate{
					MaxSurge:       ptr.To(intstr.FromInt(1)),
					MaxUnavailable: ptr.To(intstr.FromInt(0)),
				},
			},
		}
		fix.ISVC.Spec.Decoder.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{
			MaxUnavailable: ptr.To(intstr.FromInt(1)),
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// expectSteadyOn asserts the group idle with both Components fully
		// serving, a pairable pair published, and every surface on protocol.
		expectSteadyOn := func(g Gomega, protocol string) {
			c := freshISVC(g, ns.Name, rebumpISVC)
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				g.Expect(c.Status.Components[comp].Lifecycle).NotTo(BeNil())
				g.Expect(c.Status.Components[comp].Lifecycle.ServingReplicas).To(Equal(int32(2)),
					"%s must serve on both Instances", comp)
			}
			g.Expect(pairableTrafficPairExists(c)).To(BeTrue(),
				"no pairable engine+decoder traffic pair: engine=%v decoder=%v",
				c.Status.Components[v1beta1.EngineComponent].Traffic,
				c.Status.Components[v1beta1.DecoderComponent].Traffic)
			assertPairingSurface(g, ns.Name, rebumpISVC, protocol)
		}

		setProtocol := func(protocol string) {
			Eventually(func(g Gomega) {
				cur := freshISVC(g, ns.Name, rebumpISVC)
				cur.Spec.Rollout.PairingProtocol = ptr.To(protocol)
				g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}

		ginkgo.By("waiting for both Components to serve on v1")
		Eventually(func(g Gomega) {
			expectSteadyOn(g, "nixl-v1")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// A pairable traffic pair must be published at every sample of the
		// whole walk, through both bumps.
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(g Gomega) {
			c := freshISVC(g, ns.Name, rebumpISVC)
			g.Expect(pairableTrafficPairExists(c)).To(BeTrue(),
				"no pairable engine+decoder traffic pair: engine=%v decoder=%v",
				c.Status.Components[v1beta1.EngineComponent].Traffic,
				c.Status.Components[v1beta1.DecoderComponent].Traffic)
		})
		defer stop()

		ginkgo.By("bumping the protocol to v2")
		setProtocol("nixl-v2")

		ginkgo.By("waiting for one engine to be promoted on v2 while no decoder serves v2")
		Eventually(func(g Gomega) {
			g.Expect(servingPodsOfCohort(g, ns.Name, rebumpISVC, v1beta1.DecoderComponent, "nixl-v2")).To(BeZero(),
				"a decoder served v2 before the protocol moved again; the story needs the decoder's new pod still coming up")
			promoted := promotedEngineCohorts(g, ns.Name, rebumpISVC)
			g.Expect(promoted).To(ContainElement("nixl-v2"), "an engine must be promoted on v2: %v", promoted)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping the protocol to v3 while the decoder's v2 pod is still coming up")
		setProtocol("nixl-v3")

		// The v1 engine's pod is the last engine that pairs with a decoder;
		// it is identified now and watched by identity from here on.
		var lastPairable types.UID
		Eventually(func(g Gomega) {
			pods := cohortPods(g, ns.Name, rebumpISVC, v1beta1.EngineComponent, "nixl-v1")
			g.Expect(pods).To(HaveLen(1), "exactly one engine must still run v1 after the first engine moved")
			lastPairable = pods[0].UID
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the engine on v2 to move to v3 first, while the v1 engine stays in rotation")
		Eventually(func(g Gomega) {
			g.Expect(podInRotationByUID(g, ns.Name, rebumpISVC, lastPairable)).To(BeTrue(),
				"the v1 engine, the last engine that pairs with a decoder, left rotation before any decoder served v3")
			g.Expect(servingPodsOfCohort(g, ns.Name, rebumpISVC, v1beta1.EngineComponent, "nixl-v3")).To(BeNumerically(">=", 1),
				"no engine serves v3: the engine stranded on v2 was not admitted to move")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("holding the last pairable engine on the pairing gate until a decoder serves v3")
		Eventually(func(g Gomega) {
			if servingPodsOfCohort(g, ns.Name, rebumpISVC, v1beta1.DecoderComponent, "nixl-v3") > 0 {
				return
			}
			promoted := promotedEngineCohorts(g, ns.Name, rebumpISVC)
			g.Expect(promoted).To(ContainElement("nixl-v3"), "the engine that ran v2 must be promoted on v3: %v", promoted)
			g.Expect(podInRotationByUID(g, ns.Name, rebumpISVC, lastPairable)).To(BeTrue(),
				"the v1 engine left rotation before any decoder served v3")
			ir := freshInferenceReplica(g, ns.Name, rebumpISVC, v1beta1.EngineComponent)
			g.Expect(ir.Status.RolloutHold).NotTo(BeNil(), "the v1 engine's start must be held while no decoder serves v3")
			g.Expect(ir.Status.RolloutHold.Gate).To(Equal(v1beta1.RolloutHoldGatePairing))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Consistently(func(g Gomega) {
			if servingPodsOfCohort(g, ns.Name, rebumpISVC, v1beta1.DecoderComponent, "nixl-v3") > 0 {
				return
			}
			g.Expect(podInRotationByUID(g, ns.Name, rebumpISVC, lastPairable)).To(BeTrue(),
				"the v1 engine left rotation before any decoder served v3")
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("converging both Components on v3")
		Eventually(func(g Gomega) {
			expectSteadyOn(g, "nixl-v3")
			ir := freshInferenceReplica(g, ns.Name, rebumpISVC, v1beta1.EngineComponent)
			if ir.Status.RolloutHold != nil {
				g.Expect(ir.Status.RolloutHold.Gate).NotTo(Equal(v1beta1.RolloutHoldGatePairing),
					"the Pairing hold must clear once the transition converged")
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		samples := stop()
		Expect(samples).To(BeNumerically(">", 0), "the pair-alive invariant must have been sampled during the walk")
	})
})

// cohortPods lists the live pods of one Component that carry protocol.
func cohortPods(g Gomega, namespace, isvcName string, comp v1beta1.ComponentType, protocol string) []corev1.Pod {
	var out []corev1.Pod
	for _, pod := range listOMENativePods(g, namespace, isvcName) {
		if pod.Labels[constants.OMEComponentLabel] != string(comp) ||
			pod.Labels[query.LabelPairingProtocol] != protocol || pod.DeletionTimestamp != nil {
			continue
		}
		out = append(out, pod)
	}
	return out
}

// promotedEngineCohorts lists, per engine Instance that is Ready with no
// attempt open, the pairing protocol its live Ready pod carries.
func promotedEngineCohorts(g Gomega, namespace, isvcName string) map[int32]string {
	byIndex := map[int32]string{}
	for _, pod := range listOMENativePods(g, namespace, isvcName) {
		if pod.Labels[constants.OMEComponentLabel] != string(v1beta1.EngineComponent) ||
			pod.DeletionTimestamp != nil || !podIsReady(&pod) {
			continue
		}
		if idx, ok := query.InstanceIdxFromLabels(&pod); ok {
			byIndex[idx] = pod.Labels[query.LabelPairingProtocol]
		}
	}
	out := map[int32]string{}
	for _, row := range instanceStatuses(g, namespace, isvcName, v1beta1.EngineComponent) {
		if row.Phase != v1beta1.OMENativeInstanceReady || row.Operation != nil {
			continue
		}
		if protocol, ok := byIndex[row.Index]; ok {
			out[row.Index] = protocol
		}
	}
	return out
}

// podInRotationByUID reports whether the pod with uid still exists, is not
// on its way out, and is Ready with the serving gate.
func podInRotationByUID(g Gomega, namespace, isvcName string, uid types.UID) bool {
	for _, pod := range listOMENativePods(g, namespace, isvcName) {
		if pod.UID != uid {
			continue
		}
		return pod.DeletionTimestamp == nil && podIsReady(&pod) && podreadiness.IsServing(&pod)
	}
	return false
}
