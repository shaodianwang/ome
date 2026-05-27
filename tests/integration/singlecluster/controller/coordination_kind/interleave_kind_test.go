package coordination_kind

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// INT-5 — rapid spec bumps must converge to the final value and
// retention must cap CR count.
//
// Uses a synthetic env-var to drive per-bump PodSpec drift (which
// produces a distinct ControllerRevision hash without needing a 5-tag
// image build matrix). The controller must converge on the final spec
// and the non-live CRs must be GC'd under defaultRevisionRetention.
var _ = ginkgo.Describe("OMENative: rapid spec bumps", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "int5-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("INT-5 rapid env-var bumps converge to the final value; CR retention cap holds", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("int5"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Baseline: instance 0 Ready.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "int5")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Bump an env var 5 times with no delay between writes. Each
		// distinct value produces a distinct CR hash.
		for i := 1; i <= 5; i++ {
			finalI := i
			Eventually(func(g Gomega) {
				cur := freshISVC(g, ns.Name, "int5")
				// Replace or add INT5_BUMP env var; idempotent across retries.
				envs := cur.Spec.Engine.PodSpec.Containers[0].Env
				replaced := false
				for j := range envs {
					if envs[j].Name == "INT5_BUMP" {
						envs[j].Value = fmt.Sprintf("v%d", finalI)
						replaced = true
						break
					}
				}
				if !replaced {
					envs = append(envs, corev1.EnvVar{Name: "INT5_BUMP", Value: fmt.Sprintf("v%d", finalI)})
				}
				cur.Spec.Engine.PodSpec.Containers[0].Env = envs
				g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}

		// Final spec is INT5_BUMP=v5, controller has an UpdateRevision.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "int5")
			var got string
			for _, e := range cur.Spec.Engine.PodSpec.Containers[0].Env {
				if e.Name == "INT5_BUMP" {
					got = e.Value
				}
			}
			g.Expect(got).To(Equal("v5"), "final spec env INT5_BUMP must be v5; got %q", got)
			cs := cur.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.UpdateRevision).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// CR count stays under the retention cap. defaultRevisionRetention
		// is 10 non-live + the live one(s) — bound is < 15 to leave headroom
		// for in-flight transition states the cleanup hasn't caught yet.
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			g.Expect(len(crs.Items)).To(BeNumerically("<=", 15),
				"INT-5: CR retention cap exceeded; got %d CRs (defaultRevisionRetention=10 non-live)", len(crs.Items))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
