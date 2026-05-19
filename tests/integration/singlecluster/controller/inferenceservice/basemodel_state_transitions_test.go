package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Cross-controller / BaseModel state transitions affecting the ISVC
// reconciler. negative_path_test.go covers create-time "BM missing" /
// "BM disabled" — this file covers what happens when those states
// change AFTER an ISVC is already in flight, plus the inverse recovery
// scenarios. Pre-existing coverage was zero for transitions.
var _ = ginkgo.Describe("InferenceService responds to BaseModel state transitions", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-bmstate-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// makeRuntime registers a ClusterServingRuntime for these tests.
	// Each spec uses a unique prefix; the namespace cleanup in AfterEach
	// is per-spec but the CSR is cluster-scoped so we defer its delete
	// directly here.
	makeRuntime := func(prefix string) {
		csr := v1beta1testing.MakeClusterServingRuntime(prefix+"-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })
	}

	ginkgo.It("create-order race: ISVC created BEFORE its BaseModel → Deployment appears after the BaseModel is added later", func() {
		// reconcileBaseModel returns an error when the named BM doesn't
		// exist (reconciliation.go:43). The controller-runtime backs
		// off and re-queues. As soon as the BM is created later, the
		// next reconcile resolves and the Deployment is produced.
		// Pins that race-resolution behavior — catches a regression
		// where the controller stops re-queuing on missing-BM (which
		// would permanently strand any ISVC created before its BM in
		// a GitOps pipeline).
		prefix := "bmrace"
		makeRuntime(prefix)

		ginkgo.By("create ISVC first — BaseModel does NOT exist yet")
		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			NamespacedModel(prefix + "-bm").
			Runtime(prefix + "-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("no Deployment yet — controller correctly stuck on missing BM")
		Consistently(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(BeEmpty(), "ISVC must NOT produce a Deployment while its BaseModel doesn't exist")
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())

		ginkgo.By("create the BaseModel — controller should now reconcile successfully on next attempt")
		bm := v1beta1testing.MakeBaseModel(prefix+"-bm", ns.Name).
			StorageURI("oci://n/test/b/models/o/" + prefix).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		ginkgo.By("Deployment appears once the BM is resolvable")
		Eventually(func(g Gomega) {
			dep := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-engine", Namespace: ns.Name}, dep)).To(Succeed())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("recovery: BaseModel starts Disabled=true → no Deployment; flip Disabled=false → Deployment appears", func() {
		// negative_path_test.go pins that a Disabled BM at create
		// time produces no Deployment. This pins the INVERSE
		// transition: an operator un-disabling a BM must allow ISVCs
		// referencing it to proceed without manual ISVC poking.
		// reconciliation.go:76-78 errors when spec.Disabled is true;
		// after the flip, GetBaseModelWithStatus returns cleanly and
		// reconcile proceeds.
		prefix := "bmrecov"
		makeRuntime(prefix)

		bm := v1beta1testing.MakeBaseModel(prefix+"-bm", ns.Name).
			StorageURI("oci://n/test/b/models/o/" + prefix).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Disabled(true).Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			NamespacedModel(prefix + "-bm").
			Runtime(prefix + "-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("Disabled=true → no Deployment")
		Consistently(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(BeEmpty())
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())

		ginkgo.By("flip Disabled=false via read-modify-write to avoid losing a concurrent status update")
		Eventually(func(g Gomega) {
			latest := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: bm.Name, Namespace: ns.Name}, latest)).To(Succeed())
			f := false
			latest.Spec.Disabled = &f
			g.Expect(k8sClient.Update(suiteCtx, latest)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Deployment appears now that the BM is no longer disabled")
		Eventually(func(g Gomega) {
			dep := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-engine", Namespace: ns.Name}, dep)).To(Succeed())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("mid-flight disable: BaseModel disabled while ISVC is running → existing Deployment is NOT torn down", func() {
		// The controller surfaces an error from
		// ReconcileBaseModelWithStatus when Disabled flips to true,
		// but it does NOT delete the existing Deployment — only a
		// failed reconcile bubbles up, which controller-runtime
		// re-queues. The Deployment stays alive serving traffic
		// while an operator investigates. This is the deliberate
		// safety behavior; a regression that started deleting child
		// resources on any reconcile failure would silently take
		// down live serving.
		prefix := "bmdis"
		makeRuntime(prefix)

		bm := v1beta1testing.MakeBaseModel(prefix+"-bm", ns.Name).
			StorageURI("oci://n/test/b/models/o/" + prefix).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			NamespacedModel(prefix + "-bm").
			Runtime(prefix + "-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("baseline: Deployment exists")
		Eventually(func(g Gomega) {
			dep := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-engine", Namespace: ns.Name}, dep)).To(Succeed())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("flip BaseModel.spec.Disabled=true")
		Eventually(func(g Gomega) {
			latest := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: bm.Name, Namespace: ns.Name}, latest)).To(Succeed())
			t := true
			latest.Spec.Disabled = &t
			g.Expect(k8sClient.Update(suiteCtx, latest)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Deployment must remain — Consistently across the controller's poll window")
		Consistently(func(g Gomega) {
			dep := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-engine", Namespace: ns.Name}, dep)).To(Succeed(),
				"existing Deployment must survive a BaseModel disable — the controller must NOT proactively tear down live serving on reconcile errors")
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})

	ginkgo.It("mid-flight deletion: BaseModel deleted while ISVC is running → existing Deployment is NOT torn down", func() {
		// Same safety invariant as the disable case but at the
		// stronger boundary: BM is fully gone. The next reconcile
		// errors with "no BaseModel or ClusterBaseModel with the
		// name" (reconciliation.go:43); the controller does NOT
		// touch the existing Deployment. Operators can recover by
		// re-creating the BM (which the create-order race spec
		// already covers from the other direction).
		prefix := "bmdel"
		makeRuntime(prefix)

		bm := v1beta1testing.MakeBaseModel(prefix+"-bm", ns.Name).
			StorageURI("oci://n/test/b/models/o/" + prefix).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			NamespacedModel(prefix + "-bm").
			Runtime(prefix + "-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("baseline: Deployment exists")
		Eventually(func(g Gomega) {
			dep := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-engine", Namespace: ns.Name}, dep)).To(Succeed())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("delete the BaseModel")
		Expect(k8sClient.Delete(suiteCtx, bm)).To(Succeed())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: bm.Name, Namespace: ns.Name}, &v1beta1.BaseModel{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "BaseModel must be gone before we assert ISVC behavior")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Deployment must remain — operator-recoverable failure, not auto-teardown")
		Consistently(func(g Gomega) {
			dep := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-engine", Namespace: ns.Name}, dep)).To(Succeed(),
				"existing Deployment must survive a BaseModel deletion — the controller must NOT proactively tear down live serving on reconcile errors")
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})
})
