package integration_tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/onsi/gomega"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	v1beta1basemodelcontroller "github.com/sgl-project/ome/pkg/controller/v1beta1/basemodel"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/sgl-project/ome/pkg/modelagent"
)

const (
	testReconcileTimeout = 30 * time.Second
	testPollInterval     = 200 * time.Millisecond
)

// Tier-2 integration test: drive the BaseModel reconciler against a real
// apiserver (envtest). Envtest has no kubelet (so the Job pod doesn't run)
// and no kube-controller-manager (so owner-ref GC doesn't fire), so we:
//   - simulate the agent by writing the per-PVC ConfigMap directly
//   - verify the controller's explicit Delete on PVC ConfigMap during
//     deletion (not GC)
//
// What this catches that the in-package envtest tests don't:
//   - Manager + Reconciler wiring against the real apiserver
//   - Watch/predicate semantics for PVCs and ConfigMaps
//   - applyPVCStatusFromConfigMap translating the per-PVC ConfigMap
//     entry into BaseModel state (Ready / Failed / InTransit)
//   - Status subresource semantics on BaseModel updates
//   - Real finalizer add/remove flow including handlePVCModelDeletion's
//     explicit ConfigMap cleanup (envtest has no GC controller)
//
// Subtests share a single envtest + manager because controller-runtime's
// process-wide metrics registry rejects re-registering a controller of the
// same name (would happen if each test stood up its own manager).

func TestBaseModelPVC_Envtest(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))
	cfg, kc, stop := startPVCEnvTest(t, g)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startBaseModelManager(t, g, ctx, cfg)

	t.Run("happy path: pending PVC → bound → Job → ConfigMap → Ready", func(t *testing.T) {
		g := NewGomegaWithT(t)
		ns := "happy"
		makeNamespace(g, ctx, kc, ns)

		makePVC(g, ctx, kc, "shared-pvc", ns)

		bm := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-7b", Namespace: ns},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
				StorageUri: ptr.To("pvc://shared-pvc/models/llama"),
			}},
		}
		g.Expect(kc.Create(ctx, bm)).To(Succeed())

		// Pending PVC → SourceReachable reason should be PVCNotBound.
		g.Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(kc.Get(ctx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonPVCNotBound))
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())

		// Bind the PVC. PVC watch should re-enqueue and the Job should appear.
		bindPVC(g, ctx, kc, "shared-pvc", ns)

		g.Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(kc.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1), "expected exactly one metadata Job")
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())

		// Simulate the agent writing Status=Ready ConfigMap.
		simulateAgentReady(g, ctx, kc, "llama-7b", ns, false)

		// State should flip to Ready and spec should pick up llama metadata.
		// Status.NodesReady stays empty for PVC — there is no node concept here.
		g.Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(kc.Get(ctx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
			g.Expect(got.Status.NodesReady).To(BeEmpty(), "PVC models must not populate NodesReady (no node concept)")
			g.Expect(got.Spec.ModelType).NotTo(BeNil())
			g.Expect(*got.Spec.ModelType).To(Equal("llama"))
			g.Expect(got.Spec.ModelArchitecture).NotTo(BeNil())
			g.Expect(*got.Spec.ModelArchitecture).To(Equal("LlamaForCausalLM"))
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())
	})

	t.Run("ClusterBaseModel + cross-namespace PVC: status flows through Owns(Job)", func(t *testing.T) {
		g := NewGomegaWithT(t)
		// PVC lives in `shared`; ClusterBaseModel is cluster-scoped.
		// Owns(&Job{}) must enqueue the cluster-scoped owner correctly
		// (Namespace="" via RESTMapper) when the cross-namespace Job's
		// status changes.
		makeNamespace(g, ctx, kc, "shared")
		makePVC(g, ctx, kc, "shared-pvc", "shared")
		bindPVC(g, ctx, kc, "shared-pvc", "shared")

		cbm := &v1beta1.ClusterBaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "shared-llama"},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
				StorageUri: ptr.To("pvc://shared:shared-pvc/models/llama"),
			}},
		}
		g.Expect(kc.Create(ctx, cbm)).To(Succeed())

		// Job appears in the PVC's namespace, not "" (ClusterBaseModel has no namespace).
		g.Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(kc.List(ctx, jobs, client.InNamespace("shared"))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1), "Job must land in the PVC's namespace")
			g.Expect(jobs.Items[0].OwnerReferences).To(HaveLen(1))
			g.Expect(jobs.Items[0].OwnerReferences[0].Kind).To(Equal("ClusterBaseModel"))
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())

		simulateAgentReady(g, ctx, kc, "shared-llama", "", true)

		g.Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(kc.Get(ctx, types.NamespacedName{Name: "shared-llama"}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
			g.Expect(got.Status.NodesReady).To(BeEmpty())
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())
	})

	t.Run("URI change yields a fresh Job (deterministic naming)", func(t *testing.T) {
		g := NewGomegaWithT(t)
		ns := "uri-change"
		makeNamespace(g, ctx, kc, ns)
		makePVC(g, ctx, kc, "first-pvc", ns)
		bindPVC(g, ctx, kc, "first-pvc", ns)
		makePVC(g, ctx, kc, "second-pvc", ns)
		bindPVC(g, ctx, kc, "second-pvc", ns)

		bm := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-7b", Namespace: ns},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
				StorageUri: ptr.To("pvc://first-pvc/models/llama"),
			}},
		}
		g.Expect(kc.Create(ctx, bm)).To(Succeed())

		var firstJobName string
		g.Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(kc.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			firstJobName = jobs.Items[0].Name
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())

		// Change the URI — should yield a Job with a different name.
		got := &v1beta1.BaseModel{}
		g.Expect(kc.Get(ctx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
		got.Spec.Storage.StorageUri = ptr.To("pvc://second-pvc/models/llama")
		g.Expect(kc.Update(ctx, got)).To(Succeed())

		g.Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(kc.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
			names := make(map[string]bool)
			for _, j := range jobs.Items {
				names[j.Name] = true
			}
			g.Expect(names).To(HaveKey(Not(Equal(firstJobName))), "URI change must produce a Job with a new deterministic name")
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())
	})

	// PVC-deletion → state=Failed(PVCNotFound) is exercised at the unit-
	// test layer; envtest runs no PV-protection controller and the
	// resulting half-deleted PVC behaves differently than a real cluster.
	// Tracked as a Tier-3 (Kind) test in the follow-up.

	t.Run("manually deleted Job is recreated via Owns watch", func(t *testing.T) {
		g := NewGomegaWithT(t)
		ns := "job-recreate"
		makeNamespace(g, ctx, kc, ns)
		makePVC(g, ctx, kc, "p", ns)
		bindPVC(g, ctx, kc, "p", ns)

		bm := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-7b", Namespace: ns},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
				StorageUri: ptr.To("pvc://p/models/llama"),
			}},
		}
		g.Expect(kc.Create(ctx, bm)).To(Succeed())

		var firstUID string
		g.Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(kc.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			firstUID = string(jobs.Items[0].UID)
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())

		// Operator deletes the Job; reconciler should recreate it.
		jobs := &batchv1.JobList{}
		g.Expect(kc.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
		propagation := metav1.DeletePropagationBackground
		g.Expect(kc.Delete(ctx, &jobs.Items[0], &client.DeleteOptions{PropagationPolicy: &propagation})).To(Succeed())

		g.Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(kc.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			g.Expect(string(jobs.Items[0].UID)).NotTo(Equal(firstUID), "recreated Job must have a new UID")
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())
	})

	t.Run("agent Failed → BaseModel state Failed with extraction reason", func(t *testing.T) {
		g := NewGomegaWithT(t)
		ns := "agent-fail"
		makeNamespace(g, ctx, kc, ns)
		makePVC(g, ctx, kc, "p", ns)
		bindPVC(g, ctx, kc, "p", ns)

		bm := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-7b", Namespace: ns},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
				StorageUri: ptr.To("pvc://p/models/llama"),
			}},
		}
		g.Expect(kc.Create(ctx, bm)).To(Succeed())

		// Wait for finalizer (proxy: reconciler ran).
		g.Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(kc.Get(ctx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer))
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())

		// Simulate the agent reporting Failed.
		simulateAgentFailed(g, ctx, kc, "llama-7b", ns, false, "config.json missing")

		g.Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(kc.Get(ctx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed))
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonPVCMetadataExtractionFailed))
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())
	})

	t.Run("deletion clears PVC ConfigMap and finalizer without an agent", func(t *testing.T) {
		g := NewGomegaWithT(t)
		ns := "deletion"
		makeNamespace(g, ctx, kc, ns)
		makePVC(g, ctx, kc, "shared-pvc", ns)
		bindPVC(g, ctx, kc, "shared-pvc", ns)

		bm := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-7b", Namespace: ns},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
				StorageUri: ptr.To("pvc://shared-pvc/models/llama"),
			}},
		}
		g.Expect(kc.Create(ctx, bm)).To(Succeed())

		// Wait for finalizer to be added.
		g.Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(kc.Get(ctx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer))
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())

		simulateAgentReady(g, ctx, kc, "llama-7b", ns, false)

		cmName := constants.GetPVCMetadataConfigMapName("llama-7b", ns, false)
		cmKey := types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}
		g.Eventually(func() error {
			return kc.Get(ctx, cmKey, &corev1.ConfigMap{})
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())

		// Delete: controller must delete PVC ConfigMap directly (no agent will mark it Deleted)
		// and clear the finalizer.
		g.Expect(kc.Delete(ctx, bm)).To(Succeed())

		g.Eventually(func() error {
			err := kc.Get(ctx, cmKey, &corev1.ConfigMap{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed(),
			"PVC ConfigMap should be deleted by handleModelDeletion")

		g.Eventually(func() error {
			err := kc.Get(ctx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, &v1beta1.BaseModel{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed(),
			"BaseModel should be fully gone after finalizer clears")
	})

	t.Run("Job exhausts BackoffLimit without writing ConfigMap → state=Failed", func(t *testing.T) {
		// Regression guard for the agent-pod-died-without-SIGTERM-handler
		// case (e.g. OOMKill, eviction). The metadata Job's terminal
		// JobFailed condition must drive the BaseModel to LifeCycleStateFailed
		// even when no per-PVC ConfigMap was ever written, otherwise the
		// model would sit at InTransit forever.
		g := NewGomegaWithT(t)
		ns := "job-failure"
		makeNamespace(g, ctx, kc, ns)
		makePVC(g, ctx, kc, "p", ns)
		bindPVC(g, ctx, kc, "p", ns)

		bm := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-7b", Namespace: ns},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
				StorageUri: ptr.To("pvc://p/models/llama"),
			}},
		}
		g.Expect(kc.Create(ctx, bm)).To(Succeed())

		// Wait for the controller to create the Job.
		var jobName string
		g.Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(kc.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			jobName = jobs.Items[0].Name
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())

		// Simulate "BackoffLimit exhausted, no ConfigMap": flip the Job
		// to JobFailed=True via the status subresource. Crucially we do
		// NOT write a per-PVC ConfigMap — that's the whole point of the
		// scenario.
		job := &batchv1.Job{}
		g.Expect(kc.Get(ctx, types.NamespacedName{Name: jobName, Namespace: ns}, job)).To(Succeed())
		job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
			Type:    batchv1.JobFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "BackoffLimitExceeded",
			Message: "Job has reached the specified backoff limit",
		})
		g.Expect(kc.Status().Update(ctx, job)).To(Succeed())

		g.Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(kc.Get(ctx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed))
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonPVCMetadataExtractionFailed))
			g.Expect(ready.Message).To(ContainSubstring("BackoffLimitExceeded"),
				"Failed message must include the Job's terminal reason for operator debugging")
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())
	})

	t.Run("ClusterBaseModel deletion clears cross-namespace PVC ConfigMap", func(t *testing.T) {
		// Cluster-scoped owner deletion path: the per-PVC ConfigMap lives
		// in the OME namespace while the CBM has no namespace and the
		// PVC sits in a third namespace ("shared-del"). handlePVCModelDeletion
		// must explicitly Delete the ConfigMap (envtest has no GC controller
		// and even if it did, cross-namespace owner refs wouldn't trigger GC).
		g := NewGomegaWithT(t)
		makeNamespace(g, ctx, kc, "shared-del")
		makePVC(g, ctx, kc, "del-pvc", "shared-del")
		bindPVC(g, ctx, kc, "del-pvc", "shared-del")

		cbm := &v1beta1.ClusterBaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "del-llama"},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
				StorageUri: ptr.To("pvc://shared-del:del-pvc/models/llama"),
			}},
		}
		g.Expect(kc.Create(ctx, cbm)).To(Succeed())

		g.Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(kc.Get(ctx, types.NamespacedName{Name: "del-llama"}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.ClusterBaseModelFinalizer))
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())

		simulateAgentReady(g, ctx, kc, "del-llama", "", true)

		cmName := constants.GetPVCMetadataConfigMapName("del-llama", "", true)
		cmKey := types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}
		g.Eventually(func() error {
			return kc.Get(ctx, cmKey, &corev1.ConfigMap{})
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed())

		g.Expect(kc.Delete(ctx, cbm)).To(Succeed())

		g.Eventually(func() error {
			err := kc.Get(ctx, cmKey, &corev1.ConfigMap{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed(),
			"per-PVC ConfigMap must be explicitly deleted by handlePVCModelDeletion for cluster-scoped owners too")

		g.Eventually(func() error {
			err := kc.Get(ctx, types.NamespacedName{Name: "del-llama"}, &v1beta1.ClusterBaseModel{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}).WithTimeout(testReconcileTimeout).WithPolling(testPollInterval).Should(Succeed(),
			"ClusterBaseModel should be fully gone after finalizer clears")
	})
}

// --- helpers --------------------------------------------------------------

func startBaseModelManager(t *testing.T, g *gomega.GomegaWithT, ctx context.Context, restCfg *rest.Config) {
	t.Helper()
	scheme := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(scheme)).NotTo(HaveOccurred())
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(HaveOccurred())
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(HaveOccurred())

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:         scheme,
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
	})
	g.Expect(err).NotTo(HaveOccurred())

	ome := &controllerconfig.OmeAgentConfig{
		Image:          "ghcr.io/test/ome-agent:test",
		ServiceAccount: "ome-model-metadata",
	}

	g.Expect((&v1beta1basemodelcontroller.BaseModelReconciler{
		Client:         mgr.GetClient(),
		Log:            ctrl.Log.WithName("BaseModel"),
		Scheme:         mgr.GetScheme(),
		OmeAgentConfig: ome,
	}).SetupWithManager(mgr)).To(Succeed())

	g.Expect((&v1beta1basemodelcontroller.ClusterBaseModelReconciler{
		Client:         mgr.GetClient(),
		Log:            ctrl.Log.WithName("ClusterBaseModel"),
		Scheme:         mgr.GetScheme(),
		OmeAgentConfig: ome,
	}).SetupWithManager(mgr)).To(Succeed())

	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Logf("manager stopped: %v", err)
		}
	}()
	g.Eventually(func() bool {
		return mgr.GetCache().WaitForCacheSync(ctx)
	}).WithTimeout(testReconcileTimeout).Should(BeTrue())
}

func makeNamespace(g *gomega.GomegaWithT, ctx context.Context, kc client.Client, name string) {
	err := kc.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		g.Expect(err).NotTo(HaveOccurred())
	}
}

func makePVC(g *gomega.GomegaWithT, ctx context.Context, kc client.Client, name, namespace string) {
	g.Expect(kc.Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resourceMustParse("1Gi")},
			},
		},
	})).To(Succeed())
}

// bindPVC manually flips PVC.Status.Phase to Bound. Envtest has no
// volume controller to do this for us.
func bindPVC(g *gomega.GomegaWithT, ctx context.Context, kc client.Client, name, namespace string) {
	pvc := &corev1.PersistentVolumeClaim{}
	g.Expect(kc.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, pvc)).To(Succeed())
	pvc.Status.Phase = corev1.ClaimBound
	g.Expect(kc.Status().Update(ctx, pvc)).To(Succeed())
}

// simulateAgentReady writes the per-PVC ConfigMap that the metadata Job
// would normally produce, mimicking a successful extraction. NOTE: we
// deliberately omit the per-node `models.ome/basemodel-status` label —
// the controller reads this ConfigMap by exact name (not via the per-
// node List), and labelling it would just confuse `processModelStatus`
// for non-PVC models in the same namespace.
func simulateAgentReady(g *gomega.GomegaWithT, ctx context.Context, kc client.Client, modelName, modelNamespace string, isClusterScoped bool) {
	cmName := constants.GetPVCMetadataConfigMapName(modelName, modelNamespace, isClusterScoped)
	modelKey := constants.GetModelConfigMapKey(modelNamespace, modelName, isClusterScoped)

	cfg := &modelagent.ModelConfig{
		ModelType:         "llama",
		ModelArchitecture: "LlamaForCausalLM",
		MaxTokens:         4096,
	}
	entry := modelagent.ModelEntry{
		Name:   modelName,
		Status: modelagent.ModelStatusReady,
		Config: cfg,
	}
	data, err := json.Marshal(entry)
	g.Expect(err).NotTo(HaveOccurred())

	scope := "namespaced"
	if isClusterScoped {
		scope = "cluster"
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: constants.OMENamespace,
			Labels: map[string]string{
				constants.PVCStorageConfigMapLabel:  "true",
				constants.PVCMetadataModelNameLabel: modelName,
				constants.PVCMetadataScopeLabel:     scope,
			},
		},
		Data: map[string]string{modelKey: string(data)},
	}
	err = kc.Create(ctx, cm)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		g.Expect(err).NotTo(HaveOccurred())
	}
}

// simulateAgentFailed writes a Failed-status PVC ConfigMap with an error
// annotation so we can drive the reconciler's agent-failure path.
func simulateAgentFailed(g *gomega.GomegaWithT, ctx context.Context, kc client.Client, modelName, modelNamespace string, isClusterScoped bool, errMsg string) {
	cmName := constants.GetPVCMetadataConfigMapName(modelName, modelNamespace, isClusterScoped)
	modelKey := constants.GetModelConfigMapKey(modelNamespace, modelName, isClusterScoped)

	entry := modelagent.ModelEntry{Name: modelName, Status: modelagent.ModelStatusFailed}
	data, err := json.Marshal(entry)
	g.Expect(err).NotTo(HaveOccurred())

	scope := "namespaced"
	if isClusterScoped {
		scope = "cluster"
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: constants.OMENamespace,
			Labels: map[string]string{
				constants.PVCStorageConfigMapLabel:  "true",
				constants.PVCMetadataModelNameLabel: modelName,
				constants.PVCMetadataScopeLabel:     scope,
			},
			Annotations: map[string]string{
				constants.PVCMetadataLastErrorAnnotation: errMsg,
			},
		},
		Data: map[string]string{modelKey: string(data)},
	}
	err = kc.Create(ctx, cm)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		g.Expect(err).NotTo(HaveOccurred())
	}
}

func resourceMustParse(s string) resource.Quantity {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		panic(err)
	}
	return q
}
