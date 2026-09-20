package routing

import (
	"context"
	"slices"

	"github.com/go-logr/logr"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

type statusOwnershipPlan struct {
	claims []string
}

func (p statusOwnershipPlan) Claims() []string { return append([]string(nil), p.claims...) }

type statusOwnershipPublisher struct {
	gatewayRef  *v1beta1.TrafficMapGatewayRef
	stateful    bool
	claims      []string
	unpublished [][]string
}

func (*statusOwnershipPublisher) Name() string { return "status-test-publisher" }
func (p *statusOwnershipPublisher) Stateful() bool {
	return p.stateful
}
func (*statusOwnershipPublisher) ResolveOptions(_, _ map[string]string) (map[string]string, error) {
	return map[string]string{}, nil
}
func (p *statusOwnershipPublisher) Plan(
	*v1beta1.InferenceService,
	*v1beta1.TrafficMap,
	map[string]string,
) (placementendpoint.TrafficMapPublishPlan, error) {
	return statusOwnershipPlan{claims: append([]string(nil), p.claims...)}, nil
}
func (*statusOwnershipPublisher) Drain(context.Context, types.NamespacedName, []string) error {
	return nil
}
func (p *statusOwnershipPublisher) Apply(
	_ context.Context,
	_ placementendpoint.TrafficMapPublishPlan,
) (placementendpoint.TrafficMapPublishResult, error) {
	return placementendpoint.TrafficMapPublishResult{GatewayRef: p.gatewayRef.DeepCopy()}, nil
}
func (p *statusOwnershipPublisher) Unpublish(
	_ context.Context,
	_ types.NamespacedName,
	journal v1beta1.TrafficMapPublisherStatus,
) error {
	p.unpublished = append(p.unpublished, append([]string(nil), journal.ClaimedTargets...))
	return nil
}

var _ placementendpoint.TrafficMapPublisher = (*statusOwnershipPublisher)(nil)

var _ = ginkgo.Describe("TrafficMap status ownership", func() {
	ginkgo.BeforeEach(func() {
		ometesting.SetDefaultEventually()
	})

	ginkgo.It("keeps Routable and Programmed under separate field owners", func() {
		namespace := uniqueName("status-ownership")
		gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(gomega.Succeed())

		name := uniqueName("svc")
		isvc := createISVC(namespace, name, &v1beta1.PlacementSpec{
			Mode:         v1beta1.PlacementModeSingle,
			Requirements: "accelerator=test",
		})
		placeISVC(isvc, candidate("cluster-a", "cluster-a.example.com", 1, 1))

		key := types.NamespacedName{Namespace: namespace, Name: name}
		gomega.Eventually(func(g gomega.Gomega) {
			trafficMap := &v1beta1.TrafficMap{}
			g.Expect(k8sClient.Get(suiteCtx, key, trafficMap)).To(gomega.Succeed())
			routable := apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
			)
			g.Expect(routable).NotTo(gomega.BeNil())
			g.Expect(routable.Status).To(gomega.Equal(metav1.ConditionTrue))
		}).Should(gomega.Succeed())

		publisher := &statusOwnershipPublisher{gatewayRef: &v1beta1.TrafficMapGatewayRef{
			Group: "networking.example.com", Kind: "GlobalRoute", Name: name,
		}}
		reconciler := &placementendpoint.TrafficMapPublisherReconciler{
			Client:       k8sClient,
			APIReader:    k8sClient,
			Log:          logr.Discard(),
			Publisher:    publisher,
			Active:       true,
			RequeueAfter: 0,
		}

		ginkgo.By("publishing a gateway reference through sparse status apply")
		result, err := reconciler.Reconcile(suiteCtx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result).To(gomega.Equal(ctrl.Result{}))

		gomega.Eventually(func(g gomega.Gomega) {
			trafficMap := &v1beta1.TrafficMap{}
			g.Expect(k8sClient.Get(suiteCtx, key, trafficMap)).To(gomega.Succeed())
			g.Expect(trafficMap.Status.Programmed).To(gomega.BeTrue())
			g.Expect(trafficMap.Status.GatewayRef).NotTo(gomega.BeNil())
			g.Expect(trafficMap.Status.GatewayRef.Name).To(gomega.Equal(name))
			g.Expect(trafficMap.Status.Publisher).NotTo(gomega.BeNil())
			g.Expect(trafficMap.Status.Publisher.ObservedOptionsDigest).NotTo(gomega.BeEmpty())
			g.Expect(apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
			)).NotTo(gomega.BeNil())
			programmed := apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapProgrammed,
			)
			g.Expect(programmed).NotTo(gomega.BeNil())
			g.Expect(programmed.Status).To(gomega.Equal(metav1.ConditionTrue))

			owners := map[string]string{}
			for _, fields := range trafficMap.ManagedFields {
				if fields.Operation == metav1.ManagedFieldsOperationApply && fields.Subresource == "status" {
					owners[fields.Manager] += string(fields.FieldsV1.Raw)
				}
			}
			g.Expect(owners).To(gomega.HaveKey("ome-trafficmap-publisher"))
			g.Expect(owners).To(gomega.HaveKey("ome-trafficmap-routing"))
			g.Expect(owners["ome-trafficmap-publisher"]).To(gomega.ContainSubstring("Programmed"))
			g.Expect(owners["ome-trafficmap-publisher"]).NotTo(gomega.ContainSubstring("Routable"))
			g.Expect(owners["ome-trafficmap-routing"]).To(gomega.ContainSubstring("Routable"))
			g.Expect(owners["ome-trafficmap-routing"]).NotTo(gomega.ContainSubstring("Programmed"))
		}).Should(gomega.Succeed())

		ginkgo.By("removing an optional publisher field by omitting the owned value")
		publisher.gatewayRef = nil
		result, err = reconciler.Reconcile(suiteCtx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result).To(gomega.Equal(ctrl.Result{}))
		gomega.Eventually(func(g gomega.Gomega) {
			trafficMap := getTrafficMap(g, namespace, name)
			g.Expect(trafficMap.Status.GatewayRef).To(gomega.BeNil())
			g.Expect(apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
			)).NotTo(gomega.BeNil())
			g.Expect(apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapProgrammed,
			)).NotTo(gomega.BeNil())
		}).Should(gomega.Succeed())

		ginkgo.By("letting the routing writer update Routable independently")
		updateISVCStatus(isvc, func(current *v1beta1.InferenceService) {
			current.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
		})
		gomega.Eventually(func(g gomega.Gomega) {
			trafficMap := getTrafficMap(g, namespace, name)
			routable := apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
			)
			g.Expect(routable).NotTo(gomega.BeNil())
			g.Expect(routable.Status).To(gomega.Equal(metav1.ConditionFalse))
			g.Expect(routable.Reason).To(gomega.Equal(v1beta1.TrafficMapReasonNotPlaced))
			programmed := apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapProgrammed,
			)
			g.Expect(programmed).NotTo(gomega.BeNil())
			g.Expect(programmed.Status).To(gomega.Equal(metav1.ConditionTrue))
			g.Expect(trafficMap.Status.Publisher).NotTo(gomega.BeNil())
		}).Should(gomega.Succeed())
	})

	ginkgo.It("keeps a deleting map until every claimed target is unpublished", func() {
		namespace := uniqueName("publisher-finalizer")
		gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(gomega.Succeed())

		name := uniqueName("svc")
		isvc := createISVC(namespace, name, &v1beta1.PlacementSpec{
			Mode:         v1beta1.PlacementModeSingle,
			Requirements: "accelerator=test",
		})
		placeISVC(isvc, candidate("cluster-a", "cluster-a.example.com", 1, 1))
		key := types.NamespacedName{Namespace: namespace, Name: name}
		gomega.Eventually(func(g gomega.Gomega) {
			trafficMap := getTrafficMap(g, namespace, name)
			g.Expect(apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
			)).NotTo(gomega.BeNil())
		}).Should(gomega.Succeed())

		publisher := &statusOwnershipPublisher{
			stateful: true,
			claims:   []string{"target-a"},
			gatewayRef: &v1beta1.TrafficMapGatewayRef{
				Group: "networking.example.com", Kind: "GlobalRoute", Name: name,
			},
		}
		reconciler := &placementendpoint.TrafficMapPublisherReconciler{
			Client: k8sClient, APIReader: k8sClient, Log: logr.Discard(), Publisher: publisher, Active: true,
		}

		ginkgo.By("persisting the finalizer before claims or external effects")
		result, err := reconciler.Reconcile(suiteCtx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result.Requeue).To(gomega.BeTrue())
		result, err = reconciler.Reconcile(suiteCtx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result).To(gomega.Equal(ctrl.Result{}))
		gomega.Eventually(func(g gomega.Gomega) {
			trafficMap := getTrafficMap(g, namespace, name)
			g.Expect(trafficMap.Finalizers).To(gomega.ContainElement(
				placementendpoint.TrafficMapPublisherFinalizer,
			))
			g.Expect(trafficMap.Status.Publisher).NotTo(gomega.BeNil())
			g.Expect(trafficMap.Status.Publisher.ClaimedTargets).To(gomega.Equal([]string{"target-a"}))
			g.Expect(trafficMap.Status.Programmed).To(gomega.BeTrue())
		}).Should(gomega.Succeed())

		const holdFinalizer = "example.com/status-test-hold"
		gomega.Eventually(func(g gomega.Gomega) {
			trafficMap := getTrafficMap(g, namespace, name)
			if !slices.Contains(trafficMap.Finalizers, holdFinalizer) {
				trafficMap.Finalizers = append(trafficMap.Finalizers, holdFinalizer)
			}
			g.Expect(k8sClient.Update(suiteCtx, trafficMap)).To(gomega.Succeed())
		}).Should(gomega.Succeed())

		ginkgo.By("opting out so the routing controller requests map deletion")
		disabled := false
		gomega.Eventually(func(g gomega.Gomega) {
			current := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: namespace, Name: name,
			}, current)).To(gomega.Succeed())
			current.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &disabled}
			g.Expect(k8sClient.Update(suiteCtx, current)).To(gomega.Succeed())
		}).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			trafficMap := getTrafficMap(g, namespace, name)
			g.Expect(trafficMap.DeletionTimestamp.IsZero()).To(gomega.BeFalse())
		}).Should(gomega.Succeed())

		ginkgo.By("unpublishing from the durable journal before releasing the map")
		result, err = reconciler.Reconcile(suiteCtx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result).To(gomega.Equal(ctrl.Result{}))
		gomega.Eventually(func(g gomega.Gomega) {
			trafficMap := getTrafficMap(g, namespace, name)
			g.Expect(trafficMap.Finalizers).To(gomega.Equal([]string{holdFinalizer}))
			g.Expect(trafficMap.Status.Publisher).To(gomega.BeNil())
			g.Expect(trafficMap.Status.GatewayRef).To(gomega.BeNil())
			g.Expect(trafficMap.Status.Programmed).To(gomega.BeFalse())
			g.Expect(trafficMap.Status.ObservedTrafficMapGeneration).To(gomega.BeZero())
			programmed := apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapProgrammed,
			)
			g.Expect(programmed).NotTo(gomega.BeNil())
			g.Expect(programmed.Status).To(gomega.Equal(metav1.ConditionFalse))
			g.Expect(apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
			)).NotTo(gomega.BeNil())
		}).Should(gomega.Succeed())
		gomega.Expect(publisher.unpublished).To(gomega.Equal([][]string{{"target-a"}}))

		ginkgo.By("releasing the test hold after publisher cleanup")
		gomega.Eventually(func(g gomega.Gomega) {
			trafficMap := getTrafficMap(g, namespace, name)
			trafficMap.Finalizers = slices.DeleteFunc(trafficMap.Finalizers, func(value string) bool {
				return value == holdFinalizer
			})
			g.Expect(k8sClient.Update(suiteCtx, trafficMap)).To(gomega.Succeed())
		}).Should(gomega.Succeed())
		gomega.Eventually(func() bool {
			err := k8sClient.Get(suiteCtx, key, &v1beta1.TrafficMap{})
			return apierrors.IsNotFound(err)
		}).Should(gomega.BeTrue())
	})
})
