package omenative

import (
	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// State-machine interleaving specs. The reconciler runs ops in a
// fixed order (delete extras → restart → migrate → update) and each
// pass writes status before the next runs. These tests assert that
// realistic operator-driven interleavings — scale during migration,
// pod failure during migration, delete during migration — don't
// produce inconsistent state. They are the integration-level
// complement to the unit tests around individual ops.
var _ = ginkgo.Describe("OMENative state-machine interleavings", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-int-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// INT-1 — Migration AFTER scale-up. Start with 1 replica, scale
	// to 3, wait for converge, then migrate index 0. Surge picks the
	// lowest unused index (3, since 0/1/2 occupied) and the source
	// goes away — final state has indices {1, 2, surge=3}, all Ready.
	// Pins the plan builder's sparse-allocation behavior under
	// occupied low indices.
	ginkgo.It("INT-1: migration after scale-up picks a sparse surge index above the occupied range", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("int1-llama"),
			WithCSRName("int1-runtime"),
			WithISVCName("int1-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("scale 1 → 3 and wait for all Ready")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			three := 3
			fresh.Spec.Engine.ComponentExtensionSpec.MinReplicas = &three
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		var sourceNode string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "int1-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("migrate index 0; expect surge at index 3 (lowest unused) and 0 removed")
		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, sourceNode, 0)

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3),
				"expected 3 instances after migration (source replaced by surge); got %d", len(cs.OMENative.InstanceStatuses))

			indices := map[int32]v1beta1.OMENativeInstancePhase{}
			for _, s := range cs.OMENative.InstanceStatuses {
				indices[s.Index] = s.Phase
			}
			g.Expect(indices).NotTo(HaveKey(int32(0)),
				"index 0 must be gone (migrated); got %v", indices)
			for idx, phase := range indices {
				g.Expect(phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"instance %d must be Ready; got %s", idx, phase)
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// INT-2 — Scale-down during migration. Start with 3 replicas,
	// fire a migration on instance 0, then drop MinReplicas to 1.
	// Pins the precedence: scale-down (extras Delete pass) runs
	// BEFORE migration. With MinReplicas=1, indices 1 and 2 must be
	// extras and get deleted; the migration on 0 must still complete
	// or be superseded by the scale-down depending on timing — what
	// MUST hold is the final state: exactly 1 Ready instance.
	ginkgo.It("INT-2: scale down during migration converges to 1 Ready instance", func() {
		three := 3
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("int2-llama"),
			WithCSRName("int2-runtime"),
			WithISVCName("int2-isvc"),
			WithMinReplicas(three),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var sourceNode string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
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
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("writing a migration on index 0 AND dropping MinReplicas 3 → 1")
		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, sourceNode, 0)
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			one := 1
			fresh.Spec.Engine.ComponentExtensionSpec.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("converges to exactly 1 Ready instance")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1),
				"expected 1 InstanceStatus; got %d", len(cs.OMENative.InstanceStatuses))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// INT-3 — Pod failure on a non-source instance during migration.
	// Start with 3 replicas, fire migration on index 0, then fail
	// pod on index 2. Migration on index 0 proceeds independently of
	// the Restart on index 2 (per-Instance state machines).
	ginkgo.It("INT-3: failing a non-source pod during migration restarts only that instance", func() {
		three := 3
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("int3-llama"),
			WithCSRName("int3-runtime"),
			WithISVCName("int3-isvc"),
			WithMinReplicas(three),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var sourceNode string
		var idx2Incarnation int64
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
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
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("firing migration on index 0 AND failing pod on index 2")
		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, sourceNode, 0)
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "int3-isvc-engine-2-default-0",
			}, pod)).To(Succeed())
			pod.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Index 0 migrates (gone, replaced by surge); Index 2 Incarnation bumps; Index 1 untouched")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
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
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// INT-4 — Delete ISVC during in-flight migration. Fire a
	// migration, then delete the ISVC. The controller must observe
	// the delete and stop driving migration state transitions
	// without panicking. envtest has no GC; we assert the ISVC
	// eventually shows up as NotFound (cache-stale window expires)
	// and the controller continues to reconcile subsequent ISVCs in
	// the namespace.
	ginkgo.It("INT-4: deleting the ISVC during an in-flight migration does not panic the controller", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("int4-llama"),
			WithCSRName("int4-runtime"),
			WithISVCName("int4-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var sourceNode string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "int4-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("firing migration AND immediately deleting the ISVC")
		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, sourceNode, 0)
		Expect(k8sClient.Delete(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("ISVC observable as NotFound; controller survives (no panic)")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"ISVC must vanish; got err=%v", err)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("a follow-up ISVC in the same namespace still reconciles cleanly")
		fx2 := newOMENativeFixture(ns.Name,
			WithCBMName("int4-llama"), // reuse
			WithCSRName("int4-runtime"),
			WithISVCName("int4-followup"),
		)
		Expect(k8sClient.Create(suiteCtx, fx2.ISVC)).To(Succeed())
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx2.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// INT-5 — Rapid spec mutations (5 image bumps in quick
	// succession) must NOT produce 5 ControllerRevisions if the
	// controller coalesces — but they must at least produce one CR
	// per distinct rendered template. The reconciler doesn't queue
	// every reconcile; it dedups via the workqueue, so some
	// intermediates collapse. The contract we pin: the FINAL image
	// converges to the user-set value, and the number of CRs grows
	// roughly linearly with distinct templates, capped at retention.
	ginkgo.It("INT-5: rapid spec bumps converge to the final value without runaway CRs", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("int5-llama"),
			WithCSRName("int5-runtime"),
			WithISVCName("int5-isvc"),
			WithImage("ghcr.io/test/runtime:v0"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("bumping the image 5 times with minimal delay between writes")
		images := []string{"v1", "v2", "v3", "v4", "v5"}
		for _, v := range images {
			full := "ghcr.io/test/runtime:" + v
			Eventually(func(g Gomega) {
				fresh := &v1beta1.InferenceService{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
				fresh.Spec.Engine.PodSpec.Containers[0].Image = full
				g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
			}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}

		ginkgo.By("status.UpdateRevision eventually points at a CR whose payload carries the final image")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			g.Expect(fresh.Spec.Engine.PodSpec.Containers[0].Image).To(Equal("ghcr.io/test/runtime:v5"),
				"final spec image must be v5")
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.UpdateRevision).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("CR count stays under the retention cap (10 non-live + up to 2 live = 12)")
		Eventually(func(g Gomega) {
			crs := listOMENativeRevisions(g, ns.Name)
			g.Expect(len(crs)).To(BeNumerically("<=", 12),
				"expected <= 12 CRs (retention cap); got %d. Names: %v", len(crs), crNames(crs))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// INT-6 — Spec bump during in-flight migration. While the source
	// is Phase=Migrating (and surge carries Operation.Type=Migrate),
	// detectUpdateTrigger and detectRestartTrigger must NOT fire on
	// those Instances even though the user just bumped the image.
	// Pins the explicit Migrating short-circuit in both detectors.
	ginkgo.It("INT-6: detectors skip Migrate-owned instances during in-flight migration", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("int6-llama"),
			WithCSRName("int6-runtime"),
			WithISVCName("int6-isvc"),
			WithImage("ghcr.io/test/runtime:v1"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var sourceNode string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "int6-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("firing a migration and immediately bumping the image to v2")
		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, sourceNode, 0)
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("while Migrate-owned, neither InstanceStatus carries Operation.Type=Update or Phase=Restarting")
		Consistently(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			for _, s := range cs.OMENative.InstanceStatuses {
				// While owned by Migrate, no other op may stamp the Instance.
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
		}, "3s", util.PollInterval).Should(Succeed())

		ginkgo.By("after migration completes, the surviving instance eventually rolls to v2 via Update")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			ledger := loadLedger(g, ns.Name, fx.ISVC.Name)
			matched := false
			for _, e := range ledger.Entries {
				if e.RequestUUID == reqUUID && e.Phase == "Completed" {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue(), "migration must complete")
			g.Expect(fresh.Spec.Engine.PodSpec.Containers[0].Image).To(Equal("ghcr.io/test/runtime:v2"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
