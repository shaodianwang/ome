package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A corrective gang rollout must keep the replacement alive even when the
// source pods are already in ImagePullBackOff. The stuck-pod escalator owns
// failure detection for the replacement; attributing the old source failure
// to that operation makes OMENative create and delete a fresh gang every
// reconcile without ever letting the corrected image start.
var _ = ginkgo.Describe("behavior - corrective gang rollout", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gang-corrective-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("keeps the corrected replacement gang instead of abandoning it", func() {
		const (
			name      = "gang-corrective"
			badImage  = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
			goodImage = "fake-serving:v2"
		)

		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithoutDecoder(),
			WithEngineReplicas(1),
			WithEngineWorkerSize(1),
			WithEngineImage(badImage))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		enginePodsForImage := func(g Gomega, image string) []*corev1.Pod {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, name)
			g.Expect(err).NotTo(HaveOccurred())
			var matched []*corev1.Pod
			for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
				if pod.DeletionTimestamp != nil {
					continue
				}
				for _, container := range pod.Spec.Containers {
					if container.Image == image {
						matched = append(matched, pod)
						break
					}
				}
			}
			return matched
		}

		// Wait until kubelet proves the source image is terminally broken.
		Eventually(func(g Gomega) {
			pods := enginePodsForImage(g, badImage)
			stuck := 0
			for _, pod := range pods {
				if pod.Labels[query.LabelInstanceIdx] != "0" {
					continue
				}
				for _, status := range pod.Status.ContainerStatuses {
					if status.State.Waiting == nil {
						continue
					}
					if status.State.Waiting.Reason == "ErrImagePull" || status.State.Waiting.Reason == "ImagePullBackOff" {
						stuck++
						break
					}
				}
			}
			g.Expect(stuck).To(Equal(2))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Publish a corrective revision for both halves of the gang.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = goodImage
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = goodImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture one complete replacement gang, then prove the exact UIDs
		// survive across multiple reconciles. The regression replaces both
		// UIDs every one to two seconds, so an eventual-count assertion alone
		// can pass while the rollout is still churning.
		var replacementUIDs map[string]types.UID
		Eventually(func(g Gomega) {
			pods := enginePodsForImage(g, goodImage)
			g.Expect(pods).To(HaveLen(2))
			replacementUIDs = make(map[string]types.UID, len(pods))
			for _, pod := range pods {
				replacementUIDs[pod.Name] = pod.UID
			}
		}).WithTimeout(kindLongTimeout).WithPolling(100 * time.Millisecond).Should(Succeed())

		Consistently(func(g Gomega) {
			pods := enginePodsForImage(g, goodImage)
			g.Expect(pods).To(HaveLen(2))
			for _, pod := range pods {
				g.Expect(replacementUIDs).To(HaveKeyWithValue(pod.Name, pod.UID))
			}
		}).WithTimeout(10 * time.Second).WithPolling(200 * time.Millisecond).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			status := cur.Status.Components[v1beta1.EngineComponent]
			g.Expect(status.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(status.Lifecycle.ServingReplicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
