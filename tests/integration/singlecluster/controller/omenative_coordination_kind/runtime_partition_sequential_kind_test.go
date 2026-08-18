package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Runtime-inherited partition + Sequential handoff.
//
// The staged partition lives ONLY on the runtime's decoderConfig — the raw
// ISVC carries no decoder lifecycle. The projected InferenceReplica spec
// holds the merged value, the workload stages the decoder there (one
// Instance rolls, one is held on the prior revision), and coordination must
// read the SAME effective value: the decoder staged at its inherited
// partition is done-for-handoff, so the engine (next in Order) is released.
//
// A coordination layer that re-derives the partition from the raw ISVC sees
// partition 0, treats the held decoder Instance as an incomplete rollout,
// and wedges the Sequential run forever — the engine Eventually below then
// times out.
var _ = ginkgo.Describe("behavior — runtime-inherited partition + Sequential handoff", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "runtime-partition-seq-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// componentPodHashes returns live (non-terminating) pod counts per
	// revision hash for one Component.
	componentPodHashes := func(g Gomega, namespace, isvcName string, c v1beta1.ComponentType) map[string]int {
		out := map[string]int{}
		for _, pod := range listOMENativePods(g, namespace, isvcName) {
			if pod.Labels[constants.OMEComponentLabel] != string(c) || pod.DeletionTimestamp != nil {
				continue
			}
			out[pod.Labels[query.LabelRevisionHash]]++
		}
		return out
	}

	ginkgo.It("stages the decoder at the runtime partition and releases the engine", func() {
		const isvcName = "runtime-partition-seq"
		partition := int32(1)

		fix := newCoordFixture(ns.Name,
			WithISVCName(isvcName),
			WithDecoderReplicas(2),
			WithSequentialGroup(v1beta1.DecoderComponent, v1beta1.EngineComponent),
			WithEngineImage("fake-serving:v1"))
		// The partition comes from the runtime only. The ISVC decoder keeps
		// a nil lifecycle, so the raw ISVC never shows the staged value.
		fix.CSR.Spec.DecoderConfig = &v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Lifecycle: &v1beta1.LifecycleSpec{
					UpdateStrategy: &v1beta1.UpdateStrategy{
						Type:          v1beta1.UpdateStrategySurgeThenDrain,
						RollingUpdate: &v1beta1.RollingUpdate{Partition: &partition},
					},
				},
			},
		}

		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Pre-bump: the merged runtime lifecycle lands on the projected IR
		// spec (the effective value workload + coordination share) while the
		// stored ISVC still carries no decoder lifecycle. Capture the v1
		// revision hashes once every pod is up.
		var v1DecoderHash, v1EngineHash string
		Eventually(func(g Gomega) {
			stored := freshISVC(g, ns.Name, isvcName)
			g.Expect(stored.Spec.Decoder).NotTo(BeNil())
			if stored.Spec.Decoder != nil {
				g.Expect(stored.Spec.Decoder.Lifecycle).To(BeNil())
			}

			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.DecoderComponent)
			lc := ir.Spec.Lifecycle
			g.Expect(lc).NotTo(BeNil())
			if lc == nil || lc.UpdateStrategy == nil || lc.UpdateStrategy.RollingUpdate == nil {
				return
			}
			g.Expect(lc.UpdateStrategy.RollingUpdate.Partition).To(HaveValue(Equal(partition)))

			decoderHashes := componentPodHashes(g, ns.Name, isvcName, v1beta1.DecoderComponent)
			engineHashes := componentPodHashes(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(decoderHashes).To(HaveLen(1))
			g.Expect(engineHashes).To(HaveLen(1))
			for hash, n := range decoderHashes {
				g.Expect(n).To(Equal(2))
				v1DecoderHash = hash
			}
			for hash := range engineHashes {
				v1EngineHash = hash
			}
			g.Expect(v1DecoderHash).NotTo(BeEmpty())
			g.Expect(v1EngineHash).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Bump BOTH images. Sequential order is [decoder, engine]: the
		// decoder rolls first and stages at the inherited partition — one
		// Instance to v2, one held on v1.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The staged decoder must release the engine. Pre-fix this is the
		// wedge: coordination reads partition 0 off the raw ISVC, keeps
		// waiting for the held decoder Instance, and the engine never rolls.
		Eventually(func(g Gomega) {
			decoderHashes := componentPodHashes(g, ns.Name, isvcName, v1beta1.DecoderComponent)
			g.Expect(decoderHashes).To(HaveLen(2), "decoder must be staged across exactly two revisions")
			g.Expect(decoderHashes[v1DecoderHash]).To(Equal(1), "one decoder Instance held on v1")

			engineHashes := componentPodHashes(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(engineHashes).To(HaveLen(1))
			g.Expect(engineHashes).NotTo(HaveKey(v1EngineHash),
				"engine must have been released by the staged decoder and rolled off v1")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The hold is intentional and stable: the decoder keeps its held
		// Instance on v1 even after the engine finished.
		Consistently(func(g Gomega) {
			decoderHashes := componentPodHashes(g, ns.Name, isvcName, v1beta1.DecoderComponent)
			g.Expect(decoderHashes[v1DecoderHash]).To(Equal(1))
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
