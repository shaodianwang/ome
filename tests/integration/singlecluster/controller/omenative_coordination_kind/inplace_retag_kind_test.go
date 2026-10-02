package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	corev1apis "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// InPlaceOnly roll to a second tag of the running image.
//
// The node holds fake-serving:v1-retag as a second reference to the image
// fake-serving:v1 runs, so after the in-place patch the kubelet restarts
// the container and keeps reporting the old name for it. The roll must
// read the restart as the kubelet acting on the patch: every one of the
// four Instances is patched where it runs, one at a time within
// maxUnavailable 1, and ends Ready on the new revision with its pod UID
// unchanged.
var _ = ginkgo.Describe("behavior — InPlaceOnly rolls to a second tag of the running image", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "inplace-retag-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("patches every Instance in place one at a time and ends on the new revision", func() {
		const name = "inplace-retag"
		const replicas = 4
		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithoutDecoder(),
			WithEngineReplicas(replicas),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyInPlaceOnly),
			WithEngineImage("fake-serving:v1"))
		maxSurge := intstr.FromInt32(0)
		maxUnavailable := intstr.FromInt32(1)
		fix.ISVC.Spec.Engine.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{
			MaxSurge:       &maxSurge,
			MaxUnavailable: &maxUnavailable,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var v1EngineRev string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, name)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(replicas)),
				"every engine Instance should be serving on v1")
			v1EngineRev = engCS.Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the push")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		v1UIDs := map[string]corev1apis.UID{}
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, name)
			g.Expect(err).NotTo(HaveOccurred())
			pods := snap.ByComponent[v1beta1.EngineComponent]
			g.Expect(pods).To(HaveLen(replicas))
			for _, p := range pods {
				v1UIDs[p.Name] = p.UID
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, name)
			g.Expect(err).NotTo(HaveOccurred())
			serving := snap.Serving(v1beta1.EngineComponent)
			alive := snap.Alive(v1beta1.EngineComponent)
			g.Expect(serving).To(BeNumerically(">=", replicas-1),
				"maxUnavailable 1 must keep %d of %d engine pods serving (observed serving=%d alive=%d)", replicas-1, replicas, serving, alive)
			g.Expect(alive).To(BeNumerically("<=", replicas),
				"an in-place roll must not create extra pods (observed alive=%d)", alive)
		})
		defer stop()

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v1-retag"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, name)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision should advance off v1")
			g.Expect(engCS.Lifecycle.UpdatedReplicas).To(Equal(int32(replicas)),
				"every engine Instance should be on the new revision")
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(replicas)),
				"every engine Instance should be back to serving")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		stop()

		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(snap.ByComponent[v1beta1.EngineComponent]).To(HaveLen(replicas))
		for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
			origUID, ok := v1UIDs[p.Name]
			Expect(ok).To(BeTrue(),
				"engine pod %s should have been present before the push (same name)", p.Name)
			Expect(p.UID).To(Equal(origUID),
				"the roll must patch pod %s where it runs (got UID %s, want %s)", p.Name, p.UID, origUID)
			Expect(p.Spec.Containers[0].Image).To(Equal("fake-serving:v1-retag"),
				"pod %s must run the pushed image", p.Name)
		}
	})
})
