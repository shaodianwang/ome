package scaleout_kind

import (
	"context"
	"errors"
	"slices"

	"github.com/go-logr/logr"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
	"github.com/ome-projects/ome/tests/integration/util"
)

var errTrafficMapCleanupHeld = errors.New("test publisher cleanup is held")

const trafficMapGCTestHoldFinalizer = "kind-test.ome.io/trafficmap-gc-hold"

type trafficMapGCTestPlan struct {
	claims []string
}

func (p trafficMapGCTestPlan) Claims() []string {
	return slices.Clone(p.claims)
}

type trafficMapGCTestPublisher struct {
	claims         []string
	holdCleanup    bool
	unpublishCalls []trafficMapGCUnpublishCall
}

type trafficMapGCUnpublishCall struct {
	key     types.NamespacedName
	journal v1beta1.TrafficMapPublisherStatus
}

func (*trafficMapGCTestPublisher) Name() string { return "gc-test-publisher" }

func (*trafficMapGCTestPublisher) Stateful() bool { return true }

func (*trafficMapGCTestPublisher) ResolveOptions(_, _ map[string]string) (map[string]string, error) {
	return map[string]string{}, nil
}

func (p *trafficMapGCTestPublisher) Plan(
	*v1beta1.InferenceService,
	*v1beta1.TrafficMap,
	map[string]string,
) (placementendpoint.TrafficMapPublishPlan, error) {
	return trafficMapGCTestPlan{claims: slices.Clone(p.claims)}, nil
}

func (*trafficMapGCTestPublisher) Drain(context.Context, types.NamespacedName, []string) error {
	return nil
}

func (*trafficMapGCTestPublisher) Apply(
	context.Context,
	placementendpoint.TrafficMapPublishPlan,
) (placementendpoint.TrafficMapPublishResult, error) {
	return placementendpoint.TrafficMapPublishResult{}, nil
}

func (p *trafficMapGCTestPublisher) Unpublish(
	_ context.Context,
	key types.NamespacedName,
	journal v1beta1.TrafficMapPublisherStatus,
) error {
	p.unpublishCalls = append(p.unpublishCalls, trafficMapGCUnpublishCall{
		key:     key,
		journal: *journal.DeepCopy(),
	})
	if p.holdCleanup {
		return errTrafficMapCleanupHeld
	}
	return nil
}

var _ placementendpoint.TrafficMapPublisher = (*trafficMapGCTestPublisher)(nil)

// These specs exercise lifecycle behavior that envtest cannot supply: the
// kube-controller-manager follows the TrafficMap's owner reference and applies
// the requested dependent propagation policy. Reconciliation is invoked
// directly so the assertions can hold publisher cleanup at deterministic
// boundaries while retaining the real API server and garbage collector.
var _ = ginkgo.Describe("TrafficMap owner garbage collection (KIND)", func() {
	var namespace string

	ginkgo.BeforeEach(func() {
		namespace = uniqueName("mc-workload-trafficmap-gc")
		createNamespace(controlCluster, namespace)
	})

	ginkgo.AfterEach(func() {
		clearTrafficMapGCTestFinalizers(namespace)
		gomega.Expect(util.DeleteNamespace(controlCluster.ctx, controlCluster.client, namespace)).
			To(gomega.Succeed())
	})

	ginkgo.It("keeps the publisher journal while owner GC deletes the TrafficMap", func() {
		isvc, trafficMap, publisher, reconciler := createPublishedTrafficMapForGC(namespace)
		key := client.ObjectKeyFromObject(trafficMap)

		ginkgo.By("deleting the owner with background propagation")
		propagation := metav1.DeletePropagationBackground
		gomega.Expect(controlCluster.client.Delete(controlCluster.ctx, isvc,
			&client.DeleteOptions{PropagationPolicy: &propagation})).To(gomega.Succeed())
		waitForISVCDeletionTimestamp(key)

		// The placement finalizer normally keeps the source alive until routing
		// teardown completes. Removing it here isolates the owner-reference GC
		// fallback and lets the real garbage collector observe a missing owner.
		releasePlacementFinalizer(key)
		gomega.Eventually(func() bool {
			err := controlCluster.client.Get(controlCluster.ctx, key, &v1beta1.InferenceService{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())

		ginkgo.By("waiting for Kubernetes GC to request dependent deletion")
		assertTrafficMapCleanupHeld(key, isvc.UID, publisher.claims)

		ginkgo.By("proving a failed unpublish preserves the finalizer and durable journal")
		publisher.holdCleanup = true
		_, err := reconciler.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring(errTrafficMapCleanupHeld.Error())))
		assertTrafficMapCleanupHeld(key, isvc.UID, publisher.claims)
		gomega.Expect(publisher.unpublishCalls).To(gomega.HaveLen(1))
		gomega.Expect(publisher.unpublishCalls[0].key).To(gomega.Equal(key))
		gomega.Expect(publisher.unpublishCalls[0].journal.ClaimedTargets).To(gomega.Equal(publisher.claims))

		ginkgo.By("releasing cleanup and waiting for the TrafficMap to disappear")
		publisher.holdCleanup = false
		_, err = reconciler.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		assertPublisherCleanupComplete(key)
		gomega.Expect(publisher.unpublishCalls).To(gomega.HaveLen(2))
		gomega.Expect(publisher.unpublishCalls[1].key).To(gomega.Equal(key))
		gomega.Expect(publisher.unpublishCalls[1].journal.ClaimedTargets).To(gomega.Equal(publisher.claims))
		releaseTrafficMapGCTestHold(key)
	})

	ginkgo.It("uses source provenance after orphan propagation removes the owner reference", func() {
		isvc, trafficMap, publisher, publisherReconciler := createPublishedTrafficMapForGC(namespace)
		key := client.ObjectKeyFromObject(trafficMap)

		ginkgo.By("deleting the owner with orphan propagation")
		propagation := metav1.DeletePropagationOrphan
		gomega.Expect(controlCluster.client.Delete(controlCluster.ctx, isvc,
			&client.DeleteOptions{PropagationPolicy: &propagation})).To(gomega.Succeed())
		waitForISVCDeletionTimestamp(key)

		ginkgo.By("waiting for Kubernetes GC to orphan the dependent")
		gomega.Eventually(func(g gomega.Gomega) {
			current := &v1beta1.TrafficMap{}
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, current)).To(gomega.Succeed())
			g.Expect(current.OwnerReferences).To(gomega.BeEmpty())
			g.Expect(current.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
			g.Expect(current.Status.SourceUID).To(gomega.Equal(isvc.UID))
			g.Expect(current.Status.Publisher).NotTo(gomega.BeNil())
			g.Expect(current.Status.Publisher.ClaimedTargets).To(gomega.Equal(publisher.claims))
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("reaping the orphaned map by its durable source UID")
		routingReconciler := &routing.Reconciler{
			Client:    controlCluster.client,
			APIReader: controlCluster.client,
			Log:       logr.Discard(),
			Config:    routing.Config{Enabled: true},
		}
		_, err := routingReconciler.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		assertTrafficMapCleanupHeld(key, isvc.UID, publisher.claims)

		ginkgo.By("cleaning from the journal without restoring the owner reference")
		_, err = publisherReconciler.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		assertPublisherCleanupComplete(key)
		gomega.Expect(publisher.unpublishCalls).To(gomega.HaveLen(1))
		gomega.Expect(publisher.unpublishCalls[0].key).To(gomega.Equal(key))
		gomega.Expect(publisher.unpublishCalls[0].journal.ClaimedTargets).To(gomega.Equal(publisher.claims))
		releaseTrafficMapGCTestHold(key)

		ginkgo.By("waiting for placement to release the source after routing cleanup")
		gomega.Eventually(func() bool {
			err := controlCluster.client.Get(controlCluster.ctx, key, &v1beta1.InferenceService{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
	})
})

func createPublishedTrafficMapForGC(
	namespace string,
) (*v1beta1.InferenceService, *v1beta1.TrafficMap, *trafficMapGCTestPublisher,
	*placementendpoint.TrafficMapPublisherReconciler) {
	name := uniqueName("trafficmap-gc")
	isvc := runnableSourceISVC(namespace, name, "unused-model", "unused-runtime",
		v1beta1.PlacementModeSingle, 1)
	isvc.Spec.Placement.Requirements = "trafficmap-gc=true"
	mustCreate(controlCluster, isvc)
	key := client.ObjectKeyFromObject(isvc)

	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		current := &v1beta1.InferenceService{}
		g.Expect(controlCluster.client.Get(controlCluster.ctx, key, current)).To(gomega.Succeed())
		g.Expect(current.Finalizers).To(gomega.ContainElement(placement.PlacementFinalizer))
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

	trafficMap := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Finalizers: []string{trafficMapGCTestHoldFinalizer},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
		Spec: v1beta1.TrafficMapSpec{
			Service:                name,
			Mode:                   v1beta1.PlacementModeSingle,
			ObservedISVCGeneration: isvc.Generation,
		},
	}
	mustCreate(controlCluster, trafficMap)
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		current := &v1beta1.TrafficMap{}
		g.Expect(controlCluster.client.Get(controlCluster.ctx, key, current)).To(gomega.Succeed())
		current.Status.SourceUID = isvc.UID
		g.Expect(controlCluster.client.Status().Update(controlCluster.ctx, current)).To(gomega.Succeed())
		trafficMap = current
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

	publisher := &trafficMapGCTestPublisher{
		claims: []string{"test-target:" + namespace + "/" + name},
	}
	reconciler := &placementendpoint.TrafficMapPublisherReconciler{
		Client:       controlCluster.client,
		APIReader:    controlCluster.client,
		Log:          logr.Discard(),
		Publisher:    publisher,
		Active:       true,
		RequeueAfter: 0,
	}

	ginkgo.By("persisting the publisher finalizer before its claim journal")
	result, err := reconciler.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, result.Requeue).To(gomega.BeTrue())
	result, err = reconciler.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, result).To(gomega.Equal(ctrl.Result{}))
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		current := &v1beta1.TrafficMap{}
		g.Expect(controlCluster.client.Get(controlCluster.ctx, key, current)).To(gomega.Succeed())
		g.Expect(current.Finalizers).To(gomega.ContainElement(
			placementendpoint.TrafficMapPublisherFinalizer,
		))
		g.Expect(current.Status.SourceUID).To(gomega.Equal(isvc.UID))
		g.Expect(current.Status.Publisher).NotTo(gomega.BeNil())
		g.Expect(current.Status.Publisher.ClaimedTargets).To(gomega.Equal(publisher.claims))
		g.Expect(current.Status.Programmed).To(gomega.BeTrue())
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

	return isvc, trafficMap, publisher, reconciler
}

func waitForISVCDeletionTimestamp(key types.NamespacedName) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		current := &v1beta1.InferenceService{}
		g.Expect(controlCluster.client.Get(controlCluster.ctx, key, current)).To(gomega.Succeed())
		g.Expect(current.DeletionTimestamp.IsZero()).To(gomega.BeFalse())
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
}

func releasePlacementFinalizer(key types.NamespacedName) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		current := &v1beta1.InferenceService{}
		err := controlCluster.client.Get(controlCluster.ctx, key, current)
		if apierrors.IsNotFound(err) {
			return
		}
		g.Expect(err).NotTo(gomega.HaveOccurred())
		controllerutil.RemoveFinalizer(current, placement.PlacementFinalizer)
		g.Expect(controlCluster.client.Update(controlCluster.ctx, current)).To(gomega.Succeed())
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
}

func assertTrafficMapCleanupHeld(key types.NamespacedName, sourceUID types.UID, claims []string) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		current := &v1beta1.TrafficMap{}
		g.Expect(controlCluster.client.Get(controlCluster.ctx, key, current)).To(gomega.Succeed())
		g.Expect(current.DeletionTimestamp.IsZero()).To(gomega.BeFalse())
		g.Expect(current.Finalizers).To(gomega.ContainElement(
			placementendpoint.TrafficMapPublisherFinalizer,
		))
		g.Expect(current.Status.SourceUID).To(gomega.Equal(sourceUID))
		g.Expect(current.Status.Publisher).NotTo(gomega.BeNil())
		g.Expect(current.Status.Publisher.ClaimedTargets).To(gomega.Equal(claims))
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
}

func assertPublisherCleanupComplete(key types.NamespacedName) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		current := &v1beta1.TrafficMap{}
		g.Expect(controlCluster.client.Get(controlCluster.ctx, key, current)).To(gomega.Succeed())
		g.Expect(current.Finalizers).To(gomega.ContainElement(trafficMapGCTestHoldFinalizer))
		g.Expect(current.Finalizers).NotTo(gomega.ContainElement(
			placementendpoint.TrafficMapPublisherFinalizer,
		))
		g.Expect(current.Status.Publisher).To(gomega.BeNil())
		g.Expect(current.Status.GatewayRef).To(gomega.BeNil())
		g.Expect(current.Status.Programmed).To(gomega.BeFalse())
		g.Expect(current.Status.ObservedTrafficMapGeneration).To(gomega.BeZero())
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
}

func releaseTrafficMapGCTestHold(key types.NamespacedName) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		current := &v1beta1.TrafficMap{}
		g.Expect(controlCluster.client.Get(controlCluster.ctx, key, current)).To(gomega.Succeed())
		controllerutil.RemoveFinalizer(current, trafficMapGCTestHoldFinalizer)
		g.Expect(controlCluster.client.Update(controlCluster.ctx, current)).To(gomega.Succeed())
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	gomega.EventuallyWithOffset(1, func() bool {
		err := controlCluster.client.Get(controlCluster.ctx, key, &v1beta1.TrafficMap{})
		return apierrors.IsNotFound(err)
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
}

func clearTrafficMapGCTestFinalizers(namespace string) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		trafficMaps := &v1beta1.TrafficMapList{}
		g.Expect(controlCluster.client.List(controlCluster.ctx, trafficMaps,
			client.InNamespace(namespace))).To(gomega.Succeed())
		for i := range trafficMaps.Items {
			trafficMap := &trafficMaps.Items[i]
			trafficMap.Finalizers = nil
			g.Expect(controlCluster.client.Update(controlCluster.ctx, trafficMap)).To(gomega.Succeed())
		}

		isvcs := &v1beta1.InferenceServiceList{}
		g.Expect(controlCluster.client.List(controlCluster.ctx, isvcs,
			client.InNamespace(namespace))).To(gomega.Succeed())
		for i := range isvcs.Items {
			isvc := &isvcs.Items[i]
			isvc.Finalizers = nil
			g.Expect(controlCluster.client.Update(controlCluster.ctx, isvc)).To(gomega.Succeed())
		}
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
}
