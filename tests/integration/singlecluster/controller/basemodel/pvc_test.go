package basemodel

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/shared"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// BaseModel + ClusterBaseModel PVC reconcile flow against envtest.
// `By(...)` markers expose multi-step assertions in long flows so a
// failing CI log says exactly which step regressed.
var _ = ginkgo.Describe("BaseModel PVC reconcile", ginkgo.Ordered, func() {

	ginkgo.It("happy path: pending PVC → bound → Job → ConfigMap → Ready", func() {
		ns := "happy"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "shared-pvc", ns)

		bm := v1beta1testing.MakeBaseModel("llama-7b", ns).
			StorageURI("pvc://shared-pvc/models/llama").Obj()

		ginkgo.By("creating the BaseModel before the PVC is bound")
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		ginkgo.By("expecting SourceReachable=PVCNotBound while the PVC is still Pending")
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonPVCNotBound))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("binding the PVC and expecting exactly one metadata Job to appear")
		bindPVC(suiteCtx, k8sClient, "shared-pvc", ns)
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1), "expected exactly one metadata Job")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("simulating the agent writing Status=Ready and expecting state→Ready + spec populated")
		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient, "llama-7b", ns, false,
			&shared.ModelConfig{
				ModelType:         "llama",
				ModelArchitecture: "LlamaForCausalLM",
				MaxTokens:         4096,
			})).To(Succeed())
		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
			g.Expect(got.Status.NodesReady).To(BeEmpty(), "PVC models must not populate NodesReady (no node concept)")
			g.Expect(got.Spec.ModelType).NotTo(BeNil())
			g.Expect(*got.Spec.ModelType).To(Equal("llama"))
			g.Expect(got.Spec.ModelArchitecture).NotTo(BeNil())
			g.Expect(*got.Spec.ModelArchitecture).To(Equal("LlamaForCausalLM"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ClusterBaseModel + cross-namespace PVC: status flows through Owns(Job)", func() {
		makeNamespace(suiteCtx, k8sClient, "shared")
		makePVC(suiteCtx, k8sClient, "shared-pvc", "shared")
		bindPVC(suiteCtx, k8sClient, "shared-pvc", "shared")

		cbm := v1beta1testing.MakeClusterBaseModel("shared-llama").
			StorageURI("pvc://shared:shared-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())

		ginkgo.By("expecting the Job to land in the PVC's namespace with a ClusterBaseModel owner ref")
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace("shared"))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			g.Expect(jobs.Items[0].OwnerReferences).To(HaveLen(1))
			g.Expect(jobs.Items[0].OwnerReferences[0].Kind).To(Equal("ClusterBaseModel"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("simulating Ready and expecting the cluster-scoped CR to flip to Ready")
		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient, "shared-llama", "", true,
			&shared.ModelConfig{ModelType: "llama", ModelArchitecture: "LlamaForCausalLM", MaxTokens: 4096})).To(Succeed())
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "shared-llama"}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
			g.Expect(got.Status.NodesReady).To(BeEmpty())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("URI change yields a fresh Job (deterministic naming)", func() {
		ns := "uri-change"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "first-pvc", ns)
		bindPVC(suiteCtx, k8sClient, "first-pvc", ns)
		makePVC(suiteCtx, k8sClient, "second-pvc", ns)
		bindPVC(suiteCtx, k8sClient, "second-pvc", ns)

		bm := v1beta1testing.MakeBaseModel("llama-7b", ns).
			StorageURI("pvc://first-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		var firstJobName string
		ginkgo.By("waiting for the first Job to appear")
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			firstJobName = jobs.Items[0].Name
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("changing the URI and expecting a Job with a different deterministic name")
		updateBaseModelStorageURI(types.NamespacedName{Name: "llama-7b", Namespace: ns}, "pvc://second-pvc/models/llama")

		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			names := make(map[string]bool)
			for _, j := range jobs.Items {
				names[j.Name] = true
			}
			g.Expect(names).To(HaveKey(Not(Equal(firstJobName))), "URI change must produce a Job with a new deterministic name")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("manually deleted Job is recreated via Owns watch", func() {
		ns := "job-recreate"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p", ns)
		bindPVC(suiteCtx, k8sClient, "p", ns)

		bm := v1beta1testing.MakeBaseModel("llama-7b", ns).
			StorageURI("pvc://p/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		var firstUID string
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			firstUID = string(jobs.Items[0].UID)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("operator deletes the Job; reconciler must recreate it")
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
		propagation := metav1.DeletePropagationBackground
		Expect(k8sClient.Delete(suiteCtx, &jobs.Items[0], &client.DeleteOptions{PropagationPolicy: &propagation})).To(Succeed())

		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			g.Expect(string(jobs.Items[0].UID)).NotTo(Equal(firstUID), "recreated Job must have a new UID")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("agent Failed → BaseModel state Failed with extraction reason", func() {
		ns := "agent-fail"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p", ns)
		bindPVC(suiteCtx, k8sClient, "p", ns)

		bm := v1beta1testing.MakeBaseModel("llama-7b", ns).
			StorageURI("pvc://p/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(v1beta1testing.SimulatePVCAgentFailed(suiteCtx, k8sClient, "llama-7b", ns, false, "config.json missing")).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed))
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonPVCMetadataExtractionFailed))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("deletion clears PVC ConfigMap and finalizer without an agent", func() {
		ns := "deletion"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "shared-pvc", ns)
		bindPVC(suiteCtx, k8sClient, "shared-pvc", ns)

		bm := v1beta1testing.MakeBaseModel("llama-7b", ns).
			StorageURI("pvc://shared-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.BaseModelFinalizer))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient, "llama-7b", ns, false,
			&shared.ModelConfig{ModelType: "llama", ModelArchitecture: "LlamaForCausalLM", MaxTokens: 4096})).To(Succeed())

		cmName := constants.GetPVCMetadataConfigMapName("llama-7b", ns, false)
		cmKey := types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}
		Eventually(func() error {
			return k8sClient.Get(suiteCtx, cmKey, &corev1.ConfigMap{})
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(k8sClient.Delete(suiteCtx, bm)).To(Succeed())

		ginkgo.By("PVC ConfigMap should be deleted by handleModelDeletion")
		Eventually(func() error {
			err := k8sClient.Get(suiteCtx, cmKey, &corev1.ConfigMap{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("BaseModel should be fully gone after finalizer clears")
		Eventually(func() error {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, &v1beta1.BaseModel{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Job exhausts BackoffLimit without writing ConfigMap → state=Failed", func() {
		ns := "job-failure"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p", ns)
		bindPVC(suiteCtx, k8sClient, "p", ns)

		bm := v1beta1testing.MakeBaseModel("llama-7b", ns).
			StorageURI("pvc://p/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		var jobName string
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(ns))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			jobName = jobs.Items[0].Name
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("flipping the Job's terminal Failed condition without writing a ConfigMap")
		// Retry to absorb UID/Conflict races with the basemodel
		// controller, which may delete-and-recreate the Job while we
		// patch status (envtest does not run kube-controller-manager,
		// but the basemodel reconciler still owns the Job lifecycle).
		Eventually(func(g Gomega) {
			job := &batchv1.Job{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: jobName, Namespace: ns}, job)).To(Succeed())
			// The apiserver rejects a finished Job whose status lacks a
			// startTime, and rejects Failed=True unless the interim
			// FailureTarget=True is set alongside it — mirror what
			// kube-controller-manager writes on a real cluster.
			now := metav1.Now()
			job.Status.StartTime = &now
			job.Status.Conditions = append(job.Status.Conditions,
				batchv1.JobCondition{
					Type:    batchv1.JobFailureTarget,
					Status:  corev1.ConditionTrue,
					Reason:  "BackoffLimitExceeded",
					Message: "Job has reached the specified backoff limit",
				},
				batchv1.JobCondition{
					Type:    batchv1.JobFailed,
					Status:  corev1.ConditionTrue,
					Reason:  "BackoffLimitExceeded",
					Message: "Job has reached the specified backoff limit",
				})
			g.Expect(k8sClient.Status().Update(suiteCtx, job)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "llama-7b", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed))
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonPVCMetadataExtractionFailed))
			g.Expect(ready.Message).To(ContainSubstring("BackoffLimitExceeded"),
				"Failed message must include the Job's terminal reason for operator debugging")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ClusterBaseModel deletion clears cross-namespace PVC ConfigMap", func() {
		makeNamespace(suiteCtx, k8sClient, "shared-del")
		makePVC(suiteCtx, k8sClient, "del-pvc", "shared-del")
		bindPVC(suiteCtx, k8sClient, "del-pvc", "shared-del")

		cbm := v1beta1testing.MakeClusterBaseModel("del-llama").
			StorageURI("pvc://shared-del:del-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "del-llama"}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(constants.ClusterBaseModelFinalizer))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient, "del-llama", "", true,
			&shared.ModelConfig{ModelType: "llama", ModelArchitecture: "LlamaForCausalLM", MaxTokens: 4096})).To(Succeed())

		cmName := constants.GetPVCMetadataConfigMapName("del-llama", "", true)
		cmKey := types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}
		Eventually(func() error {
			return k8sClient.Get(suiteCtx, cmKey, &corev1.ConfigMap{})
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(k8sClient.Delete(suiteCtx, cbm)).To(Succeed())

		ginkgo.By("per-PVC ConfigMap must be explicitly deleted by handlePVCModelDeletion for cluster-scoped owners too")
		Eventually(func() error {
			err := k8sClient.Get(suiteCtx, cmKey, &corev1.ConfigMap{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("ClusterBaseModel should be fully gone after finalizer clears")
		Eventually(func() error {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "del-llama"}, &v1beta1.ClusterBaseModel{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// --- helpers --------------------------------------------------------------
//
// makeNamespace / makePVC / bindPVC are package-private utilities used
// only by the PVC suite. They're not promoted into util/ because their
// signatures (corev1 reuse, fixed name) are specific to these tests.

func makeNamespace(ctx context.Context, kc client.Client, name string) {
	ginkgo.GinkgoHelper()
	err := kc.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}
}

func makePVC(ctx context.Context, kc client.Client, name, namespace string) {
	ginkgo.GinkgoHelper()
	Expect(kc.Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	})).To(Succeed())
}

// bindPVC manually flips PVC.Status.Phase to Bound. Envtest has no
// volume controller to do this for us.
func bindPVC(ctx context.Context, kc client.Client, name, namespace string) {
	ginkgo.GinkgoHelper()
	pvc := &corev1.PersistentVolumeClaim{}
	Expect(kc.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, pvc)).To(Succeed())
	pvc.Status.Phase = corev1.ClaimBound
	Expect(kc.Status().Update(ctx, pvc)).To(Succeed())
}

func updateBaseModelStorageURI(key types.NamespacedName, uri string) {
	Eventually(func(g Gomega) {
		latest := &v1beta1.BaseModel{}
		g.Expect(k8sClient.Get(suiteCtx, key, latest)).To(Succeed())
		latest.Spec.Storage.StorageUri = ptr.To(uri)
		g.Expect(k8sClient.Update(suiteCtx, latest)).To(Succeed())
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
}
