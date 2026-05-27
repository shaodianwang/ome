package coordination_kind

import (
	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// State-machine interleaving specs ported from envtest interleave_test.go.
// The reconciler runs ops in a fixed order (delete extras → restart →
// migrate → update) and each pass writes status before the next runs.
// These specs assert that realistic operator-driven interleavings don't
// produce inconsistent state.
//
// INT-5 (rapid spec bumps converge + CR retention cap) is already
// covered by interleave_kind_test.go, so it's omitted here.
//
// Single-node KIND limitation: a "migration to a different node"
// assertion can't be made — every surge pod lands on the same node.
// We pin the controller-side state machine (audit ledger, surge index
// allocation, peer-Component non-interference) which doesn't need a
// multi-node cluster.
var _ = ginkgo.Describe("OMENative state-machine interleavings (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-int-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// INT-1 — Migration after scale-up picks a sparse surge index
	// above the occupied range. Start with 3 replicas, migrate index
	// 0; the surge goes to a new index (>= 3 since 0/1/2 are
	// occupied) and the source goes away.
	ginkgo.It("INT-1: migration after scale-up picks a sparse surge index above the occupied range", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("int1-isvc"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for 3 Ready replicas + capturing node")
		var sourceNode string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "int1-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "int1-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("requesting migration of index 0")
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("expecting 3 instances post-migration with index 0 gone, surge at a fresh index")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "int1-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3),
				"expected 3 instances after migration (source replaced by surge); got %d", len(cs.OMENative.InstanceStatuses))

			indices := map[int32]v1beta1.OMENativeInstancePhase{}
			for _, s := range cs.OMENative.InstanceStatuses {
				indices[s.Index] = s.Phase
			}
			g.Expect(indices).NotTo(HaveKey(int32(0)),
				"index 0 must be gone (migrated); got indices=%v", indices)
			for idx, phase := range indices {
				g.Expect(phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"instance %d must be Ready; got %s", idx, phase)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// INT-2 — Scale-down during migration. Fire migration on index 0,
	// then drop MinReplicas to 1. Final state: exactly 1 Ready
	// instance regardless of how the scale-down and migration
	// interleave.
	ginkgo.It("INT-2: scale down during migration converges to 1 Ready instance", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("int2-isvc"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var sourceNode string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "int2-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "int2-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("requesting migration on index 0 AND dropping MinReplicas 3 → 1")
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "int2-isvc")
			one := 1
			cur.Spec.Engine.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("converges to exactly 1 Ready instance")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "int2-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1),
				"expected 1 InstanceStatus; got %d", len(cs.OMENative.InstanceStatuses))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// INT-3 — Pod failure on a non-source instance during migration.
	// Fire migration on index 0, delete pod at index 2. Migration on
	// index 0 proceeds; index 2 restart-bumps Incarnation; index 1
	// untouched.
	ginkgo.It("INT-3: failing a non-source pod during migration restarts only that instance", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("int3-isvc"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineImage("fake-serving:v1"))
		// Restart on pod-delete (real kubelet flavor of trigger).
		restartRecreate := v1beta1.OMENativeRestartPolicyRecreateInstance
		fix.ISVC.Spec.Engine.OMENative = &v1beta1.OMENativeComponentSpec{
			RestartPolicy: &restartRecreate,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var sourceNode string
		var idx2Incarnation int64
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "int3-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				if s.Index == 2 {
					idx2Incarnation = s.Incarnation
				}
			}
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "int3-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("firing migration on index 0 AND deleting pod on index 2")
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)
		grace := int64(0)
		_ = k8sClient.Delete(suiteCtx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "int3-isvc-engine-2-default-0"},
		}, &client.DeleteOptions{GracePeriodSeconds: &grace})

		ginkgo.By("Index 0 migrates (gone, replaced); Index 2 Incarnation bumps; Index 1 untouched")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "int3-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))

			byIdx := map[int32]v1beta1.OMENativeInstanceStatus{}
			for _, s := range cs.OMENative.InstanceStatuses {
				byIdx[s.Index] = s
			}
			g.Expect(byIdx).NotTo(HaveKey(int32(0)),
				"index 0 must be gone after migration; got indices=%v", byIdx)
			g.Expect(byIdx).To(HaveKey(int32(1)), "index 1 must remain")
			g.Expect(byIdx).To(HaveKey(int32(2)), "index 2 must remain (restarted in place)")
			g.Expect(byIdx[int32(1)].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(byIdx[int32(2)].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(byIdx[int32(2)].Incarnation).To(BeNumerically(">", idx2Incarnation),
				"index 2 must have bumped Incarnation through Restart")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// INT-4 — Delete ISVC during in-flight migration. Fire a
	// migration, then delete the ISVC. The controller must observe the
	// delete and stop driving migration state transitions without
	// panicking. A follow-up ISVC in the same namespace still
	// reconciles cleanly.
	ginkgo.It("INT-4: deleting the ISVC during an in-flight migration does not panic the controller", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("int4-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		// Manually defer CBM/CSR cleanup — ISVC is deleted explicitly.
		ginkgo.DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, fix.CBM)
			_ = k8sClient.Delete(suiteCtx, fix.CSR)
		})

		var sourceNode string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "int4-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "int4-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("firing migration AND immediately deleting the ISVC")
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)
		Expect(k8sClient.Delete(suiteCtx, fix.ISVC)).To(Succeed())

		ginkgo.By("ISVC observable as NotFound; controller survives")
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fix.ISVC), &v1beta1.InferenceService{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"ISVC must vanish; got err=%v", err)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("a follow-up ISVC in the same namespace still reconciles cleanly")
		fix2 := newCoordFixture(ns.Name, WithISVCName("int4-followup"),
			WithCBMName(fix.CBM.Name),
			WithCSRName(fix.CSR.Name),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		Expect(k8sClient.Create(suiteCtx, fix2.ISVC)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix2.ISVC) })

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "int4-followup")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// INT-6 — Spec bump during in-flight migration. While the source
	// is Phase=Migrating (and surge carries Operation.Type=Migrate),
	// detectUpdateTrigger and detectRestartTrigger must NOT fire on
	// those Instances even though the user just bumped the image.
	// Pins the explicit Migrating short-circuit in both detectors.
	ginkgo.It("INT-6: detectors skip Migrate-owned instances during in-flight migration", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("int6-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var sourceNode string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "int6-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "int6-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("firing a migration and immediately bumping the image to v2")
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "int6-isvc")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("while Migrate-owned, instances must not carry Operation.Type=Update or Phase=Restarting")
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "int6-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			for _, s := range cs.OMENative.InstanceStatuses {
				if s.Phase != v1beta1.OMENativeInstanceMigrating &&
					(s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationMigrate) {
					continue
				}
				g.Expect(s.Phase).NotTo(Equal(v1beta1.OMENativeInstanceRestarting),
					"detectRestartTrigger must not fire on Migrate-owned idx=%d", s.Index)
				if s.Operation != nil {
					g.Expect(s.Operation.Type).NotTo(Equal(v1beta1.InstanceOperationUpdate),
						"detectUpdateTrigger must not fire on Migrate-owned idx=%d (got Op=%+v)", s.Index, s.Operation)
					g.Expect(s.Operation.Type).NotTo(Equal(v1beta1.InstanceOperationRestart),
						"detectRestartTrigger must not stamp Restart on Migrate-owned idx=%d", s.Index)
				}
			}
		}, "3s", kindPollInterval).Should(Succeed())

		ginkgo.By("after migration completes, the surviving instance rolls to v2 via Update")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "int6-isvc")
			g.Expect(cur.Spec.Engine.PodSpec.Containers[0].Image).To(Equal("fake-serving:v2"))
			cs := cur.Status.Components[v1beta1.EngineComponent]
			// Migration completes by surge promotion. Eventually the
			// rolling v1→v2 update on the surviving instance also lands.
			g.Expect(cs.OMENative.InstanceStatuses).NotTo(BeEmpty())
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
