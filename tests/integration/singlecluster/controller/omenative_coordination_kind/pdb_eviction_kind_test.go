package omenative_coordination_kind

import (
	"sort"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("OMENative PodDisruptionBudget", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pdb-evict-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("PDB eviction allows one disruption and rejects a concurrent second disruption", func() {
		fix := newCoordFixture(
			ns.Name,
			WithISVCName("pdb-eviction"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithStartupDelay(15),
		)
		maximum := intstr.FromInt(1)
		fix.ISVC.Spec.Engine.MaxUnavailable = &maximum
		fix.ISVC.Spec.Engine.PodSpec.Containers[0].ReadinessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
				Path: "/healthz",
				Port: intstr.FromInt(8080),
			}},
			PeriodSeconds:    1,
			FailureThreshold: 1,
		}

		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		kubeClient, err := kubernetes.NewForConfig(kindutil.Suite().RestConfig)
		Expect(err).NotTo(HaveOccurred())

		pdbKey := client.ObjectKey{Namespace: ns.Name, Name: fix.ISVC.Name + "-engine"}
		selector := map[string]string{
			constants.InferenceServicePodLabelKey: fix.ISVC.Name,
			constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		}
		var originalPods []corev1.Pod

		ginkgo.By("waiting for three healthy pods and one allowed disruption")
		Eventually(func(g Gomega) {
			budget := &policyv1.PodDisruptionBudget{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, budget)).To(Succeed())
			g.Expect(budget.Status.ObservedGeneration).To(Equal(budget.Generation))
			g.Expect(budget.Spec.MinAvailable).NotTo(BeNil())
			g.Expect(*budget.Spec.MinAvailable).To(Equal(intstr.FromInt(2)))
			g.Expect(budget.Spec.MaxUnavailable).To(BeNil())
			g.Expect(budget.Spec.Selector).NotTo(BeNil())
			g.Expect(budget.Spec.Selector.MatchLabels).To(Equal(selector))
			g.Expect(budget.Status.ExpectedPods).To(Equal(int32(3)))
			g.Expect(budget.Status.CurrentHealthy).To(Equal(int32(3)))
			g.Expect(budget.Status.DesiredHealthy).To(Equal(int32(2)))
			g.Expect(budget.Status.DisruptionsAllowed).To(Equal(int32(1)))

			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels(selector),
			)).To(Succeed())
			g.Expect(pods.Items).To(HaveLen(3))
			for i := range pods.Items {
				g.Expect(podIsReady(&pods.Items[i])).To(BeTrue(), "pod %s is not Ready", pods.Items[i].Name)
			}
			sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
			originalPods = append([]corev1.Pod(nil), pods.Items...)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		evict := func(pod corev1.Pod) error {
			uid := pod.UID
			return kubeClient.CoreV1().Pods(ns.Name).EvictV1(suiteCtx, &policyv1.Eviction{
				ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: ns.Name},
				DeleteOptions: &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
					UID: &uid,
				}},
			})
		}

		first, second := originalPods[0], originalPods[1]
		ginkgo.By("evicting the first healthy pod")
		Expect(evict(first)).To(Succeed())

		ginkgo.By("waiting for the disruption budget to close while replacement is unavailable")
		Eventually(func(g Gomega) {
			budget := &policyv1.PodDisruptionBudget{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, budget)).To(Succeed())
			g.Expect(budget.Status.DisruptionsAllowed).To(Equal(int32(0)))

			live := &corev1.Pod{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: first.Name}, live)
			if apierrors.IsNotFound(err) {
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			if live.UID == first.UID {
				g.Expect(live.DeletionTimestamp != nil || !podIsReady(live)).To(BeTrue())
				return
			}
			g.Expect(podIsReady(live)).To(BeFalse(), "replacement pod became Ready before the second eviction")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		secondLive := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: second.Name}, secondLive)).To(Succeed())
		Expect(secondLive.UID).To(Equal(second.UID))
		Expect(secondLive.DeletionTimestamp).To(BeNil())
		Expect(podIsReady(secondLive)).To(BeTrue())

		ginkgo.By("rejecting a concurrent eviction of a second healthy pod")
		err = evict(second)
		Expect(apierrors.IsTooManyRequests(err)).To(BeTrue(), "second eviction returned %v", err)

		preserved := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: second.Name}, preserved)).To(Succeed())
		Expect(preserved.UID).To(Equal(second.UID))
		Expect(preserved.DeletionTimestamp).To(BeNil())
	})
})
