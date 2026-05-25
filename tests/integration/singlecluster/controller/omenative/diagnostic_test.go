package omenative

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Diagnostic specs — exploratory probes promoted to regression gates.
// Each one started as a "dump state to see what's happening" spec that
// surfaced a bug, then tightened to a positive assertion catching the
// regression. The diag dumps (ginkgo.By(formatInstanceStatus(...))) stay
// so a future failure reproduces the context that made the bug obvious.
var _ = ginkgo.Describe("OMENative diagnostic", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-diag-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("post-create: InstanceStatus reaches Ready with RunningRevision set", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("diag-llama-7b"),
			WithCSRName("diag-runtime"),
			WithISVCName("diag-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs, ok := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(ok).To(BeTrue(), "engine ComponentStatus missing")
			g.Expect(cs.OMENative).NotTo(BeNil(), "OMENative ComponentStatus missing")
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1), "exactly one InstanceStatus expected for MinReplicas=1")

			s := cs.OMENative.InstanceStatuses[0]
			// Surface the diag dump as ginkgo.By so a future failure
			// has the same context that originally made the bug obvious.
			ginkgo.By(formatInstanceStatus(s))
			ginkgo.By(formatOMENativeStatus(cs.OMENative))

			g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"InstanceStatus[0] should be Ready")
			g.Expect(s.Operation).To(BeNil(), "Operation must be cleared on Ready promote")
			g.Expect(s.RunningRevision).NotTo(BeEmpty(),
				"RunningRevision must be recorded on Create promote")
			// Once rollout converges the per-Instance RunningRevision
			// should equal the Component-level CurrentRevision pointer
			// the status reconcile writes.
			g.Expect(s.RunningRevision).To(Equal(cs.OMENative.CurrentRevision),
				"InstanceStatus.RunningRevision should equal Component.CurrentRevision after convergence")

			// Pod-level sanity: containers ready + serving=True.
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			g.Expect(pods.Items).To(HaveLen(1), "expected exactly one OMENative pod for MinReplicas=1")
			for i := range pods.Items {
				p := &pods.Items[i]
				ginkgo.By(formatPodDiag(p))
				g.Expect(util.HasConditionTrue(p.Status.Conditions, corev1.ContainersReady)).To(BeTrue(),
					"pod %s ContainersReady should be True (kubelet sim)", p.Name)
				g.Expect(util.HasConditionTrue(p.Status.Conditions, "ome.io/serving")).To(BeTrue(),
					"pod %s ome.io/serving should be True (Create's MarkPodServing)", p.Name)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// formatInstanceStatus renders an InstanceStatus into a single line
// suitable for ginkgo.By output. Aimed at readability over
// completeness — fields the diagnostic doesn't surface are
// deliberately omitted to keep the line scannable.
func formatInstanceStatus(s v1beta1.OMENativeInstanceStatus) string {
	op := "none"
	if s.Operation != nil {
		op = string(s.Operation.Type) + "/" + s.Operation.Step
		if s.Operation.Reason != "" {
			op += "(" + s.Operation.Reason + ")"
		}
	}
	return fmt.Sprintf(
		"idx=%d phase=%s inc=%d podCount=%d ready=%d avail=%d running=%q target=%q op=%s",
		s.Index, s.Phase, s.Incarnation,
		s.PodCount, s.ReadyPodCount, s.AvailablePodCount,
		s.RunningRevision, s.TargetRevision, op,
	)
}

func formatPodDiag(p *corev1.Pod) string {
	servingState := "absent"
	for _, c := range p.Status.Conditions {
		if c.Type == "ome.io/serving" {
			servingState = string(c.Status) + " reason=" + c.Reason + " msg=" + c.Message
			break
		}
	}
	containersReady := "false"
	if util.HasConditionTrue(p.Status.Conditions, corev1.ContainersReady) {
		containersReady = "true"
	}
	return fmt.Sprintf(
		"name=%s node=%s image=%s containersReady=%s ome.io/serving=%s",
		p.Name, p.Spec.NodeName, p.Spec.Containers[0].Image, containersReady, servingState,
	)
}

func formatOMENativeStatus(s *v1beta1.OMENativeComponentStatus) string {
	return fmt.Sprintf(
		"replicas=%d ready=%d avail=%d updated=%d updatedReady=%d updateRev=%q currentRev=%q",
		s.Replicas, s.ReadyReplicas, s.AvailableReplicas,
		s.UpdatedReplicas, s.UpdatedReadyReplicas,
		s.UpdateRevision, s.CurrentRevision,
	)
}
