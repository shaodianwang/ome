package multicluster

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// These specs exercise the placement orphan-GC Runnable end-to-end against a real
// member apiserver: a derived ISVC whose source no longer exists is swept, while a
// derived stamped for a DIFFERENT control plane is left untouched (ownership
// scoping). A live "keeper" source on the control plane keeps the sweep's
// live-UID set non-empty so the safety guard (skip-on-empty) does not fire.
var _ = ginkgo.Describe("Multi-cluster placement GC", func() {
	var (
		secretNS   string
		isvcNS     string
		wcName     string
		secretName string
		keeperName string
	)

	ginkgo.BeforeEach(func() {
		skipIfRealKueue()
		secretNS = uniqueName("gc-secrets")
		isvcNS = uniqueName("gc-workload")
		createNamespace(controlCluster, secretNS)
		createNamespace(controlCluster, isvcNS)
		createNamespace(member1Cluster, isvcNS)

		secretName = uniqueName("kubeconfig-m1")
		wcName = uniqueName("gc-member1")
		mustCreate(controlCluster, kubeconfigSecretFor(member1Cluster, secretNS, secretName))
		ginkgo.By("connecting member1 as a Ready WorkloadCluster")
		ometesting.CreateAndWaitReady(controlCluster.ctx, controlCluster.client,
			WorkloadClusterFor(wcName, secretNS, secretName, nil),
			"WorkloadCluster Ready=True",
			func(o *v1beta1.WorkloadCluster) bool {
				return apimeta.IsStatusConditionTrue(o.Status.Conditions, v1beta1.WorkloadClusterReady)
			})

		// A live source so the GC sweep's live-UID set is non-empty.
		keeperName = uniqueName("gc-keeper")
		mustCreate(controlCluster, keeperISVC(isvcNS, keeperName))
	})

	ginkgo.AfterEach(func() {
		deleteIfExists(controlCluster, keeperISVC(isvcNS, keeperName))
		deleteIfExists(controlCluster, WorkloadClusterFor(wcName, secretNS, secretName, nil))
	})

	ginkgo.It("reaps an orphan derived whose source is gone, and leaves another control plane's derived", func() {
		orphanName := uniqueName("orphan")
		foreignName := uniqueName("foreign")

		ginkgo.By("planting an orphan derived (this control plane, no live source) on member1")
		mustCreate(member1Cluster, orphanDerived(isvcNS, orphanName, controlPlaneID, "11111111-1111-1111-1111-111111111111"))

		ginkgo.By("planting a derived owned by a DIFFERENT control plane on member1")
		mustCreate(member1Cluster, orphanDerived(isvcNS, foreignName, "other-control-plane", "22222222-2222-2222-2222-222222222222"))

		ginkgo.By("asserting the GC reaps this control plane's orphan")
		gomega.Eventually(func() error {
			return member1Cluster.client.Get(member1Cluster.ctx,
				types.NamespacedName{Namespace: isvcNS, Name: orphanName}, &v1beta1.InferenceService{})
		}, eventuallyTimeout, eventuallyPoll).Should(ometesting.BeNotFoundError(),
			"the GC should delete the orphan derived (its source UID is not live)")

		ginkgo.By("asserting the other control plane's derived is left untouched across a sweep")
		gomega.Consistently(func() bool {
			_, ok := getDerivedOn(member1Cluster, isvcNS, foreignName)
			return ok
		}, gcInterval+eventuallyPoll, eventuallyPoll).Should(gomega.BeTrue(),
			"the GC must not reap a derived stamped for another control plane")
	})
})
