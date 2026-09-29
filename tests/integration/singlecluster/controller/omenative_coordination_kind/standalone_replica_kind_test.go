package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/util"
)

// standaloneReplica is a user-created InferenceReplica with no InferenceService:
// one single-pod runner shaped like the fixture runtime's engine container. Its
// template is the whole pod spec, so it also names the suite's scheduler.
func standaloneReplica(namespace, name, image string, replicas int32) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: v1beta1.EngineComponent,
			Replicas:  ptr.To(replicas),
			Runners: []v1beta1.Runner{{
				Name: v1beta1.RunnerNameDefault,
				Size: 1,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					SchedulerName: kindfix.SchedulerName(),
					// A short grace bounds each pod delete, keeping the roll and the
					// teardown quick.
					TerminationGracePeriodSeconds: ptr.To(int64(1)),
					Containers: []corev1.Container{{
						Name:  "ome-container",
						Image: image,
						Ports: fakeServingPorts(),
					}},
				}},
			}},
		},
	}
}

var _ = ginkgo.Describe("standalone InferenceReplica", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "standalone-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		if ns != nil {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		}
	})

	ginkgo.It("reaches Ready without an InferenceService and rolls to a new image", func() {
		const name = "pool-a"
		ir := standaloneReplica(ns.Name, name, "fake-serving:v1", 2)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

		// ReadyReplicas counts ContainersReady pods, but PodReady also waits for
		// the kubelet to fold in the serving gate, so the pods get their own poll.
		// Their controller reference is the replica itself.
		expectPods := func(image string) {
			Eventually(func(g Gomega) {
				pods := listOMENativePods(g, ns.Name, name)
				g.Expect(pods).To(HaveLen(2))
				for i := range pods {
					pod := &pods[i]
					g.Expect(pod.Name).To(HavePrefix(name + "-engine-"))
					g.Expect(pod.DeletionTimestamp).To(BeNil(), "pod %s is terminating", pod.Name)
					g.Expect(pod.Spec.Containers).To(ContainElement(HaveField("Image", image)))
					g.Expect(podIsReady(pod)).To(BeTrue(), "pod %s should be Ready", pod.Name)
					owner := metav1.GetControllerOf(pod)
					g.Expect(owner).NotTo(BeNil(), "pod %s has no controller", pod.Name)
					g.Expect(owner.UID).To(Equal(ir.UID), "pod %s must be controlled by the replica", pod.Name)
				}
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}

		ginkgo.By("waiting for both instances to become Ready on the first revision")
		var firstRevision string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
			g.Expect(fresh.Status.ReadyReplicas).To(Equal(int32(2)))
			g.Expect(fresh.Status.CurrentRevision).NotTo(BeEmpty())
			g.Expect(fresh.Status.CurrentRevision).To(Equal(fresh.Status.UpdateRevision))
			firstRevision = fresh.Status.CurrentRevision
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		expectPods("fake-serving:v1")

		ginkgo.By("rolling the runner to a new image")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
			fresh.Spec.Runners[0].Template.Spec.Containers[0].Image = "fake-serving:v2"
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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		expectPods("fake-serving:v2")
	})
})
