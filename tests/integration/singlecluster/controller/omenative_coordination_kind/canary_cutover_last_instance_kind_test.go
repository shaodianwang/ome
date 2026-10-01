package omenative_coordination_kind

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior test — the canary cutover rolls the last held Instance.
//
// The final step keeps one Instance on the stable revision until the 100%
// traffic write has landed and drained, and the rollout is not Stable until
// that Instance has rolled too. An auto ladder is driven on a real kubelet:
// its timed pause means no operator verb touches the InferenceService after
// the bump, so the controller must carry the released Instance through on
// its own wake-ups. Two and four Instances, with and without a drain window,
// must all end with every Instance on the canary revision and serving, the
// unit Stable at the done sentinel, and the run closed Completed; and the
// unit must never read Stable while a serving Instance is still on the
// stable revision.
var _ = ginkgo.Describe("behavior — canary cutover rolls the last held Instance", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "canary-co-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.DescribeTable("ends with every Instance on the canary revision and the unit Stable",
		func(isvcName string, replicas int, scaleDownDelay *int32) {
			// Config: converge fully on v1 so the plan added later observes a
			// clean stable revision.
			fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
				WithoutDecoder(),
				WithEngineReplicas(replicas),
				WithEngineImage("fake-serving:v1"))
			applyFixture(Default, fix)
			deferFixtureCleanup(fix)

			var stableHash string
			Eventually(func(g Gomega) {
				c := freshISVC(g, ns.Name, isvcName)
				engCS := c.Status.Components[v1beta1.EngineComponent]
				g.Expect(engCS.Lifecycle).NotTo(BeNil())
				g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(replicas)))
				rev := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent).Status.CurrentRevision
				g.Expect(rev).NotTo(BeEmpty())
				stableHash = query.RevisionFromName(rev).Hash()
			}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

			// Action: add an auto two-step canary (a short timed pause, then
			// 100%) and bump the image so a canary revision is created.
			Eventually(func(g Gomega) {
				cur := freshISVC(g, ns.Name, isvcName)
				cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					Canary: &v1beta1.GroupCanary{
						Steps: []v1beta1.RolloutGroupStep{
							{Capacity: intstr.FromString("50%"), Traffic: 25, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 5 * time.Second}}},
							{Capacity: intstr.FromString("100%"), Traffic: 100},
						},
						ScaleDownDelaySeconds: scaleDownDelay,
					},
				}}}
				cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
				g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

			var canaryHash string
			Eventually(func(g Gomega) {
				c := freshISVC(g, ns.Name, isvcName)
				g.Expect(c.Status.Canary).NotTo(BeNil())
				canaryHash = c.Status.Canary.CanaryRevisionHash
				g.Expect(canaryHash).NotTo(BeEmpty())
				g.Expect(canaryHash).NotTo(Equal(stableHash))
			}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

			// Invariant, sampled on every poll until completion: the unit never
			// reads Stable while a serving pod is still on the stable revision.
			stableWithOldServing := ""
			Eventually(func(g Gomega) {
				c := freshISVC(g, ns.Name, isvcName)
				phase := c.Status.Components[v1beta1.EngineComponent].RolloutPhase
				byRev := map[string]int{}
				total := 0
				for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
					if pod.DeletionTimestamp != nil {
						continue
					}
					total++
					if podIsReady(&pod) {
						byRev[pod.Labels[query.LabelRevisionHash]]++
					}
				}
				if phase == v1beta1.RolloutPhaseStable && byRev[stableHash] > 0 && stableWithOldServing == "" {
					stableWithOldServing = fmt.Sprintf("phase Stable with %d ready pod(s) still on the stable revision (%d ready on the canary)", byRev[stableHash], byRev[canaryHash])
				}

				// Completion: the done sentinel, 100% on the canary, the pre-canary
				// identity dropped, the unit Stable, and every pod on the canary
				// revision and Ready.
				g.Expect(c.Status.Canary).NotTo(BeNil())
				g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(2)),
					"a finished canary keeps its status with the done sentinel")
				g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(100)))
				g.Expect(c.Status.Canary.StableRevisionHash).To(BeEmpty(),
					"completion must drop the pre-canary stable identity")
				g.Expect(phase).To(Equal(v1beta1.RolloutPhaseStable))
				g.Expect(total).To(Equal(replicas), "no surge or stable pod may remain once the cutover is complete")
				g.Expect(byRev[canaryHash]).To(Equal(replicas), "every Instance must be Ready on the canary revision, got %v", byRev)
				g.Expect(byRev[stableHash]).To(BeZero(), "no Instance may still serve the stable revision")

				traffic := trafficForComponent(c, v1beta1.EngineComponent)
				g.Expect(traffic).To(HaveLen(1), "completed canary must carry a single full-weight target, got %s", describeTraffic(traffic))
				g.Expect(traffic[0].Tag).To(Equal("canary"))
				g.Expect(traffic[0].Percent).To(Equal(int32(100)))

				// The release reached the workload: nothing is held any more.
				ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
				if ir.Spec.Pacing != nil && ir.Spec.Pacing.Partition != nil {
					g.Expect(*ir.Spec.Pacing.Partition).To(BeZero(), "a completed canary holds no Instance")
				}
				g.Expect(ir.Status.UpdatedReplicas).To(Equal(int32(replicas)))

				// The run closed on the completed cutover.
				g.Expect(c.Status.Rollout).NotTo(BeNil())
				g.Expect(c.Status.Rollout.ActiveRun).To(BeNil(), "the run must close once the cutover is complete")
				g.Expect(c.Status.Rollout.LastRun).NotTo(BeNil())
				g.Expect(c.Status.Rollout.LastRun.Outcome).To(Equal(v1beta1.RolloutRunCompleted))
			}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
			Expect(stableWithOldServing).To(BeEmpty(), "the unit read Stable before the last held Instance rolled: %s", stableWithOldServing)
		},
		ginkgo.Entry("two Instances, no drain window", "canary-co2", 2, nil),
		ginkgo.Entry("four Instances, a drain window", "canary-co4", 4, func() *int32 { d := int32(10); return &d }()),
	)
})
