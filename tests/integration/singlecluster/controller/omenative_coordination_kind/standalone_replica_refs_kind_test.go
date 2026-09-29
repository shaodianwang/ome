package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// modelHostPath is the node path a model is served from. It is where a
// model with a storage path mounts, and what MODEL_PATH names; the spec
// relies on the container runtime creating the missing host directory for
// the read-only model volume.
func modelHostPath(model string) string {
	return "/mnt/models/" + model
}

// refsFixture is the suite's standard model and runtime pair, with the
// runtime carrying the engine piece the standalone specs run: the fixture
// engine container as its runner and a short grace period on its pod spec.
// The model has a storage path, so the rendered container names it in
// MODEL_PATH. The runtime names the suite's scheduler and the controller
// carries it into every pod.
func refsFixture(model, runtime, image string) (*v1beta1.ClusterBaseModel, *v1beta1.ClusterServingRuntime) {
	cbm := kindfix.StandardClusterBaseModel(model, kindfix.StandardModelStorageURI)
	cbm.Spec.Storage.Path = ptr.To(modelHostPath(model))
	csr := kindfix.StandardClusterServingRuntime(runtime)
	csr.Spec.EngineConfig = &v1beta1.EngineSpec{
		// A short grace bounds each pod delete, keeping the roll and the
		// teardown quick.
		PodSpec: v1beta1.PodSpec{TerminationGracePeriodSeconds: ptr.To(int64(1))},
		Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
			Name:  constants.MainContainerName,
			Image: image,
			Ports: fakeServingPorts(),
		}},
	}
	return cbm, csr
}

// refsReplica is a user-created InferenceReplica with no InferenceService
// that names its model and runtime instead of carrying pod templates: the
// controller renders the runtime's engine piece for the model.
func refsReplica(namespace, name, model, runtime string, replicas int32) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.InferenceReplicaSpec{
			Component:  v1beta1.EngineComponent,
			Replicas:   ptr.To(replicas),
			ModelRef:   &v1beta1.ModelRef{Name: model, Kind: ptr.To("ClusterBaseModel")},
			RuntimeRef: &v1beta1.ServingRuntimeRef{Name: runtime, Kind: ptr.To("ClusterServingRuntime")},
		},
	}
}

var _ = ginkgo.Describe("standalone InferenceReplica from refs", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "standalone-refs-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		if ns != nil {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		}
	})

	ginkgo.It("renders from its model and runtime and rolls when the runtime image changes", func() {
		const (
			name    = "pool-a"
			model   = "model-a"
			runtime = "runtime-a"
		)
		cbm, csr := refsFixture(model, runtime, "fake-serving:v1")
		kindfix.ApplyModelRuntime(suiteCtx, Default, k8sClient, cbm, csr, kindfix.ApplyModelRuntimeOpts{
			CachedReader: kindutil.Suite().Manager.GetClient(),
		})
		ir := refsReplica(ns.Name, name, model, runtime, 2)
		ginkgo.DeferCleanup(func() {
			// The replica goes before the model and runtime it renders from,
			// so nothing renders against a deleted ref while the namespace
			// winds down; a replica that was never created is already gone.
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, ir))).To(Succeed())
			Eventually(func() bool {
				return apierrors.IsNotFound(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{}))
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(BeTrue(),
				"replica %s/%s should be fully deleted before its model and runtime", ir.Namespace, ir.Name)
			kindfix.CleanupModelRuntime(suiteCtx, k8sClient, cbm, csr, nil)
		})
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

		// ReadyReplicas counts ContainersReady pods, but PodReady also waits for
		// the kubelet to fold in the serving gate, so the pods get their own poll.
		// Their controller reference is the replica itself; their image is the
		// runtime's, and the model reaches them as MODEL_PATH and as the
		// model-Ready node selector.
		expectPods := func(image string) {
			Eventually(func(g Gomega) {
				pods := listOMENativePods(g, ns.Name, name)
				g.Expect(pods).To(HaveLen(2))
				for i := range pods {
					pod := &pods[i]
					g.Expect(pod.Name).To(HavePrefix(name + "-engine-"))
					g.Expect(pod.DeletionTimestamp).To(BeNil(), "pod %s is terminating", pod.Name)
					g.Expect(pod.Spec.Containers).To(ContainElement(SatisfyAll(
						HaveField("Image", image),
						HaveField("Env", ContainElement(corev1.EnvVar{Name: constants.ModelPathEnvVarKey, Value: modelHostPath(model)})),
					)))
					g.Expect(pod.Spec.NodeSelector).To(HaveKeyWithValue(constants.GetClusterBaseModelLabel(model), "Ready"))
					g.Expect(podIsReady(pod)).To(BeTrue(), "pod %s should be Ready", pod.Name)
					owner := metav1.GetControllerOf(pod)
					g.Expect(owner).NotTo(BeNil(), "pod %s has no controller", pod.Name)
					g.Expect(owner.UID).To(Equal(ir.UID), "pod %s must be controlled by the replica", pod.Name)
				}
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}

		ginkgo.By("waiting for both instances to become Ready on the first revision")
		var firstRevision string
		var generation int64
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
			g.Expect(fresh.Spec.Runners).To(BeEmpty(), "a refs replica stores no rendered runners")
			g.Expect(fresh.Status.ReadyReplicas).To(Equal(int32(2)))
			g.Expect(fresh.Status.CurrentRevision).NotTo(BeEmpty())
			g.Expect(fresh.Status.CurrentRevision).To(Equal(fresh.Status.UpdateRevision))
			firstRevision = fresh.Status.CurrentRevision
			generation = fresh.Generation
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		expectPods("fake-serving:v1")

		ginkgo.By("changing the runtime's engine image")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.ClusterServingRuntime{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(csr), fresh)).To(Succeed())
			fresh.Spec.EngineConfig.Runner.Container.Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for both instances to converge on the new revision")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
			g.Expect(fresh.Status.CurrentRevision).NotTo(Equal(firstRevision))
			g.Expect(fresh.Status.CurrentRevision).To(Equal(fresh.Status.UpdateRevision))
			g.Expect(fresh.Status.UpdatedReplicas).To(Equal(int32(2)))
			g.Expect(fresh.Status.ReadyReplicas).To(Equal(int32(2)))
			g.Expect(fresh.Generation).To(Equal(generation), "a runtime change rolls the replica without a change to its spec")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		expectPods("fake-serving:v2")
	})
})
