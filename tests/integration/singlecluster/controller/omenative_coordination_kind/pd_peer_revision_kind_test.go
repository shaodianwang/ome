package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior — PD per-revision peer endpoints.
//
// Every engine pod carries OME_DECODER_REVISION_ENDPOINT naming the
// per-revision Service of the decoder revision it is paired with, and every
// decoder pod the reverse. The pairing rule: a pod minted for its
// Component's roll-target revision pairs with the peer's roll-target
// revision; pods minted before an update keep pairing with the peer
// revision that was current when they were rendered. The named Service
// exists whenever the pod does and selects pods of that one peer revision
// only, so a runtime preferring the revision endpoint never crosses
// revisions mid-roll. The generic OME_<PEER>_ENDPOINT stays on the stable
// per-Component Service.
var _ = ginkgo.Describe("behavior — PD per-revision peer endpoints", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pd-peer-rev-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	const isvcName = "pd-peer-rev"

	targetHash := func(g Gomega, component v1beta1.ComponentType) string {
		ir := freshInferenceReplica(g, ns.Name, isvcName, component)
		g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty(), "%s IR must report its roll target", component)
		return query.RevisionFromName(ir.Status.UpdateRevision).Hash()
	}
	envOf := func(ctr corev1.Container, name string) (string, bool) {
		for _, e := range ctr.Env {
			if e.Name == name {
				return e.Value, true
			}
		}
		return "", false
	}

	// assertPairing checks every pod of `component` against `pairing`, which
	// maps the pod's own revision hash to the peer revision hash it must be
	// paired with: each container names that peer revision's Service. The
	// env var names and DNS shapes are the coordination contract
	// (BuildPeerEndpointEnv); the spec chooses the expected hash. For pods
	// whose own hash is in `serviceRequired` the named Service must also
	// exist and select only peers of that revision. Pods of a revision
	// being drained are exempt from the Service check: their peer revision
	// may already be fully gone (and its Services swept) while they drain.
	assertPairing := func(g Gomega, component, peer v1beta1.ComponentType, pairing map[string]string, serviceRequired ...string) {
		required := map[string]bool{}
		for _, h := range serviceRequired {
			required[h] = true
		}
		seen := 0
		for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
			if pod.Labels[constants.OMEComponentLabel] != string(component) {
				continue
			}
			seen++
			own := pod.Labels[query.LabelRevisionHash]
			want, known := pairing[own]
			g.Expect(known).To(BeTrue(), "%s pod %s carries revision %q outside the expected set %v", component, pod.Name, own, pairing)
			expected := coordination.BuildPeerEndpointEnv(isvcName, isvcName, ns.Name, peer, want)
			for _, ctr := range pod.Spec.Containers {
				got, ok := envOf(ctr, expected.RevisionName)
				g.Expect(ok).To(BeTrue(), "pod %s container %s must carry %s", pod.Name, ctr.Name, expected.RevisionName)
				g.Expect(got).To(Equal(expected.RevisionValue),
					"pod %s (revision %s) must pair with %s revision %s", pod.Name, own, peer, want)
				generic, ok := envOf(ctr, expected.GenericName)
				g.Expect(ok).To(BeTrue(), "pod %s container %s must keep %s", pod.Name, ctr.Name, expected.GenericName)
				g.Expect(generic).To(Equal(expected.GenericValue))
			}
			if !required[own] {
				continue
			}
			svc := &corev1.Service{}
			svcName := coordination.PerRevisionServiceName(isvcName, peer, want)
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: svcName}, svc)).
				To(Succeed(), "Service %s named by pod %s must exist while the pod does", svcName, pod.Name)
			g.Expect(svc.Spec.Selector).To(HaveKeyWithValue(constants.OMEComponentLabel, string(peer)))
			g.Expect(svc.Spec.Selector).To(HaveKeyWithValue(query.LabelRevisionHash, want),
				"Service %s must select only %s pods of revision %s", svcName, peer, want)
		}
		g.Expect(seen).To(BeNumerically(">", 0), "no %s pods observed", component)
	}

	ginkgo.It("engine and decoder pods pair with each other's revision before, during, and after a blueGreen roll of both", func() {
		// Config: 2 engines + 2 decoders in one blueGreen group.
		// Action: bump BOTH images in one update.
		// Invariants: v1 pods pair v1<->v1 and v2 pods pair v2<->v2 at every
		// sample of the roll; the named per-revision Services exist whenever
		// a pod names them; convergence leaves every pod paired on v2.
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithEngineReplicas(2),
			WithDecoderReplicas(2),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		var engineV1, decoderV1 string
		Eventually(func(g Gomega) {
			engineV1 = targetHash(g, v1beta1.EngineComponent)
			decoderV1 = targetHash(g, v1beta1.DecoderComponent)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("v1 engines pair with v1 decoders and v1 decoders with v1 engines")
		Eventually(func(g Gomega) {
			assertPairing(g, v1beta1.EngineComponent, v1beta1.DecoderComponent, map[string]string{engineV1: decoderV1}, engineV1)
			assertPairing(g, v1beta1.DecoderComponent, v1beta1.EngineComponent, map[string]string{decoderV1: engineV1}, decoderV1)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping both images in one update")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		var engineV2, decoderV2 string
		Eventually(func(g Gomega) {
			engineV2 = targetHash(g, v1beta1.EngineComponent)
			decoderV2 = targetHash(g, v1beta1.DecoderComponent)
			g.Expect(engineV2).NotTo(Equal(engineV1), "engine roll target must move")
			g.Expect(decoderV2).NotTo(Equal(decoderV1), "decoder roll target must move")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("throughout the roll every pod pairs with its own revision's peers; v2 pods find their peer Service already present")
		enginePairing := map[string]string{engineV1: decoderV1, engineV2: decoderV2}
		decoderPairing := map[string]string{decoderV1: engineV1, decoderV2: engineV2}
		Consistently(func(g Gomega) {
			assertPairing(g, v1beta1.EngineComponent, v1beta1.DecoderComponent, enginePairing, engineV2)
			assertPairing(g, v1beta1.DecoderComponent, v1beta1.EngineComponent, decoderPairing, decoderV2)
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("after convergence every pod is on v2 and paired with v2 peers")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
			assertPairing(g, v1beta1.EngineComponent, v1beta1.DecoderComponent, map[string]string{engineV2: decoderV2}, engineV2)
			assertPairing(g, v1beta1.DecoderComponent, v1beta1.EngineComponent, map[string]string{decoderV2: engineV2}, decoderV2)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
